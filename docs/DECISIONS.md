# Design decisions

This file explains the important choices in this project in plain language — what was chosen,
what the alternatives were, and why. It is meant to be readable without opening the code.
Things that went wrong, and how they were fixed, are recorded too.

Entries are added as the project is built, grouped by topic.

---

## 1. Project layout

### One repository, one backend package, three frontends
- **Backend:** a single Python package (`backend/src/rad`) holds the API, processing, alerting,
  the event generator and the benchmark. They run as separate processes (separate containers),
  but they share the same event schema code. If the generator and the API each had their own copy
  of "what a valid event is", the two copies would drift apart.
- **Frontends:** the same dashboard is built three times — React (web), Angular (web) and
  React Native (mobile) — to show the same live data in three ecosystems. The tricky client-side
  logic (reconnecting with backoff, merging updates into state, message types) is written **once**
  in a small framework-free TypeScript package, `@rad/core`, and each app only does rendering.
  They live in one npm workspace so all three can import that package directly.

### Line endings are forced to LF
The code is edited on Windows but runs in Linux containers. A shell script saved with Windows
line endings (CRLF) fails inside Linux with confusing errors such as `/bin/sh^M: not found`.
`.gitattributes` forces LF for all text files so this can't happen.

## 2. Technology choices

### PostgreSQL 18, no extra infrastructure
Everything — raw events, aggregates, alerts, dead letters — lives in PostgreSQL. No Kafka, no
Redis. For the volumes a single machine handles (thousands of events per second), Postgres with
good batching and small aggregate tables is enough. Fewer moving parts means a system that is
easier to run, test and explain. The points where a message broker *would* become necessary are
listed in the README's limitations section.

Postgres 18 detail that matters for Docker: its official image stores data in
`/var/lib/postgresql` instead of `/var/lib/postgresql/data`, so the volume is mounted there.

### Python 3.14 + uv
`uv` manages the virtual environment and a lock file (`uv.lock`), so every machine and the CI
install exactly the same dependency versions. Python 3.14 is the version already installed on
the development machine and available as an official Docker image.

### asyncpg instead of an ORM
The hot path is "insert thousands of rows per second and update counters". `asyncpg` talks the
PostgreSQL binary protocol directly and can send a whole batch as a handful of arrays in one
statement. An ORM would add object creation per row and hide the exact SQL — and the SQL *is*
the interesting part of this project, so it is kept visible.

---

## 3. Ingestion

### Every event is validated on its own
A batch of 500 events with one bad event should store 499 events, not zero. The API parses the
batch, validates each item separately with Pydantic, stores the valid ones, and reports the
invalid ones (with their position in the batch and a readable error) in the response.

What "valid" means:
- all fields present, known enum values (event type, category, city, payment method);
- `amount_mad` > 0, ≤ 1,000,000, at most 2 decimals; JSON `true` is refused (Python would
  otherwise treat it as the number 1);
- `occurred_at` must carry a timezone (a timestamp without one is ambiguous) and is converted
  to UTC; it may be at most 5 minutes in the future (producers' clocks drift a bit) and at most
  7 days in the past (older than that is bad data, not "late" data).
- Unknown extra fields are **ignored**, not rejected: a producer that starts sending a new field
  should not suddenly have every event refused.

### Nothing is silently dropped: the dead-letter table
Anything that cannot be ingested goes to `dead_letter_events` with a reason
(`malformed_json`, `invalid_envelope`, `validation_error`, `payload_too_large`), the error detail
and the raw payload (truncated to 8,000 characters). You can later inspect what went wrong,
fix the producer, and even replay the events.

To make this possible the request body is read and parsed **by hand** instead of letting FastAPI
turn it into a model automatically. Otherwise, broken JSON would be refused by the framework
before our code ever sees it, and could not be recorded.

Things that must not crash the server, each covered by a test: invalid JSON, an empty body,
binary garbage containing NUL bytes (PostgreSQL text columns cannot store NUL, so it is
replaced), JSON nested 100,000 levels deep (the parser raises `RecursionError`), `NaN`,
a wrong top-level shape, too many events, a body that is too large.

Money is parsed with `parse_float=Decimal`: the JSON number `249.90` becomes the exact decimal
`249.90`, never the binary float `249.89999999999998`.

### Retries are safe: the event id is the idempotency key
`event_id` is the primary key of the `events` table and inserts use `ON CONFLICT DO NOTHING`.
If a producer times out and sends the same batch again, the second copy is ignored and the
response says how many events were duplicates. Later, aggregates are computed only from rows
that were actually inserted, so a retry can never double-count revenue.

The generator creates **UUIDv7** ids (built into Python 3.14). Their first bits are a
timestamp, so new ids are always "bigger" than old ones and land at the right end of the
primary-key index. Random UUIDv4 ids would land at random places in the index, splitting pages
and touching more of the disk on every insert.

### Group commit: many requests, one database write
The simplest design is "each HTTP request inserts its events". Under load that means hundreds of
tiny transactions competing for connections, each paying for its own network round trip and its
own commit (a disk flush).

Instead, request handlers put their valid events in an in-memory queue and **wait**. A single
background writer task loops:
1. wait for at least one request in the queue;
2. grab everything else already waiting (up to 5,000 events), without waiting for more;
3. insert all of it in **one** SQL statement (`INSERT … SELECT FROM unnest(arrays)`);
4. wake each waiting handler with its own result (inserted / duplicates).

When traffic is low, step 2 finds nothing extra, so a request is written immediately — no added
delay. When traffic is high, requests pile up during step 3, so the next write carries many of
them. Batching grows exactly when it is useful, with no timer to tune. Because the handler
answers only after the commit, `200 OK` really means "stored in PostgreSQL".

### Backpressure: 429 instead of running out of memory
The queue has a limit (50,000 waiting events by default). When a request would go over it, the
API immediately answers `429 Too Many Requests` with a `Retry-After` header, and the producer
waits and retries. This keeps memory bounded and pushes the problem back to the producer instead
of slowly degrading everyone.

Two details:
- On a 429 **nothing** from that request is stored — not even its invalid events in the
  dead-letter table — because the producer will resend the whole batch; storing them now would
  create duplicate dead letters.
- The limit counts only **valid** events, because invalid ones never enter the queue. (A test
  first got this wrong — it expected a 429 for 2 valid + 1 invalid events against a limit of 2.
  The code was right; the test's arithmetic was fixed.)

If a client disconnects while waiting, its events are still written; the writer just has nobody
to report the result to.

### Indexes on the raw events table: as few as possible
Every index makes every insert slower, and the raw table is write-heavy. It has only:
- the **primary key on `event_id`** — required for idempotency;
- a **BRIN index on `occurred_at`** — a "block range" index stores just the min/max timestamp of
  each group of disk pages. It is a few kilobytes even for millions of rows and nearly free to
  maintain. It works because events arrive roughly in time order. It serves occasional
  time-range queries (audits, backfills, tests). The dashboard never queries this table.

---

## 4. Problems met and how they were solved

### The Python virtual environment was very slow to install
The project folder is on the Windows drive, while Python runs inside WSL (Linux). Creating the
`.venv` inside the project meant thousands of small files written across the Windows/Linux
boundary: installing 14 packages took **2 minutes**. Fix: keep the project where it is but put
the environment on the Linux filesystem with `UV_PROJECT_ENVIRONMENT=$HOME/.venvs/rad`.
The same sync then took about a second.
