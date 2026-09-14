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

## 4. Processing layer: time-bucketed aggregates

### Why pre-computed buckets instead of querying the raw events
The naive dashboard query is `SELECT sum(amount) FROM events WHERE occurred_at > now() - 1 hour`.
It is correct, but its cost grows with the data: at 500 events/second an hour is 1.8 million rows,
and the dashboard wants it **every second**, for every chart, for every connected client.

Instead, the system keeps small **summary tables**, one row per minute:

| bucket | placed | paid | shipped | cancelled | revenue |
|---|---|---|---|---|---|
| 12:04 | 1,210 | 980 | 640 | 95 | 402,118.50 |

"Revenue in the last hour" becomes a sum over **60 rows**, whatever the traffic. "Revenue per hour
over 24 hours" is 1,440 rows. Reading the dashboard costs the same with a thousand or a billion
events stored. The raw table is still kept (for audits and for recomputing aggregates if a bug
is ever found), but the dashboard never reads it. A test checks this: it runs `EXPLAIN` on every
snapshot query and fails if a plan touches `events`, `orders` or `dead_letter_events`.

Tables:
- `agg_minute` — per minute: counts per event type, value of orders placed, revenue (paid).
- `agg_minute_dimension` — per minute *and* per category / city / payment method. One table for
  the three breakdowns (with a `dimension` column) keeps the write statement short. Its primary
  key is `(dimension, bucket, value)` because every read says "this dimension, this time range".
- `orders` + `order_status_counts` — current status of each order, and a counter per status.
- `ingest_minute` — per minute of *arrival*: accepted, duplicates, dead letters (for throughput and
  the dead-letter alert).

Hours are **not** stored separately: they are summed from minute rows when asked (24 hours =
1,440 tiny rows read through the primary key). A separate hourly table would be one more thing to
keep in sync for no measurable gain at this size.

### Incremental updates: the aggregates are updated in the same statement as the insert
The aggregates must never re-scan the events table. Options considered:

| Option | Why not (or why) |
|---|---|
| Recompute buckets on a timer (`GROUP BY` over recent events) | Re-scans rows repeatedly; late events outside the rescanned range are missed |
| Materialized views | `REFRESH` recomputes the whole view every time |
| Database triggers per row | Runs one upsert per event: slow, and hides logic inside the DB |
| Background job reading "new rows since id X" | Rows with lower ids can commit *after* higher ids, so a watermark can skip rows — a classic subtle bug |
| **Update aggregates in the same statement that inserts the batch** ✅ | Exactly-once, always consistent, one round trip |

The writer sends a single SQL statement built from **writable CTEs** (`WITH x AS (INSERT …)`):

1. `inserted`: insert the batch into `events` with `ON CONFLICT DO NOTHING RETURNING …` — this
   returns only rows that were really new;
2. `minute_totals`: group those rows by minute and **add** the counts to `agg_minute`
   (`INSERT … ON CONFLICT (bucket) DO UPDATE SET paid_count = paid_count + excluded.paid_count`);
3. `dimension_totals`: same for category / city / payment method;
4. `order_updates` + `status_counts`: update each order's status and adjust the status counters;
5. `ingest_totals`: add accepted and duplicate counts for the current arrival minute.

Because it is one statement, it is one transaction: either the events *and* all their
aggregates are stored, or nothing is. Because the aggregates read from step 1's `RETURNING`, a
retried batch adds nothing. A test generates 500 random orders, shuffles their events, sends
them in overlapping batches with duplicates, then checks every aggregate row against a
brute-force `GROUP BY` over the raw events table: they must be identical.

Two PostgreSQL rules that shaped the SQL:
- An `ON CONFLICT DO UPDATE` may not touch the same row twice in one statement. A batch can hold
  three events for the same order, or thousands for the same minute, so each upsert input is
  first reduced to one row per key (`GROUP BY`, or `DISTINCT ON (order_id)`). A dedicated test
  sends `shipped`, `placed` and `paid` of one order in a single statement.
- All CTEs see the database as it was when the statement started, so steps cannot read each
  other's table changes — only each other's `RETURNING` output. That's why every step feeds on
  `inserted`.

### Late and out-of-order events
Buckets use **event time** (`occurred_at`), not arrival time. An event that arrives three hours
late simply adds to the bucket of three hours ago — the upsert does not care which bucket it is.

Order status only moves **forward**: `placed (1) → paid (2) → shipped (3)`, and `cancelled (4)`
wins over everything. When an order's `paid` event arrives before its `placed` event, the late
`placed` cannot move it back to "placed". The final status is the same whatever the arrival order.

### Orders per status without counting all orders
`SELECT status, count(*) FROM orders GROUP BY status` grows with the number of orders. Instead a
4-row table `order_status_counts` is adjusted by +1 / −1 whenever an order changes status.

To know the *previous* status of each order the statement uses a PostgreSQL 18 feature:
`RETURNING old.status, new.status` on the `INSERT … ON CONFLICT DO UPDATE`. For a new order
`old.status` is NULL (so only +1 on the new status). For an order going `placed → paid`: −1 placed,
+1 paid. Before PostgreSQL 18 this would have needed an extra read of the orders first.

This counter is **all-time** (every order ever seen), not a rolling window. A rolling "last 24h"
version would need expiring old orders; it is listed as a next step.

### Details that matter
- **Time zone:** `date_trunc('minute', timestamptz)` truncates in the *session* time zone. Every
  connection sets `timezone=UTC`, so buckets never depend on server configuration. (This matters
  for hours and days; Morocco's offset is a whole hour, but other zones are not.)
- **Consistent snapshot:** the ~6 read queries of a snapshot run in one read-only
  `REPEATABLE READ` transaction, so the KPI cards and the chart can never disagree by one batch.
- **Zero-filled series:** minutes without events are returned as zeros (`generate_series` +
  `LEFT JOIN`), so charts get a regular time axis.
- **Money:** stored as exact `numeric` in PostgreSQL, sent to clients as floats because they are
  only displayed (JavaScript has no decimal type).
- **Definitions:** revenue = sum of **paid** amounts; AOV = revenue ÷ paid orders; cancellation
  rate = cancellations ÷ orders placed in the same window. The last one can in theory exceed 100%
  if many orders placed before the window are cancelled inside it — acceptable for an alerting
  signal, and documented.
- **The one write that can contend:** dead letters are stored by request handlers, not the
  writer, and both bump the same `ingest_minute` row. Each statement holds that single row lock for
  a moment and takes no other lock that the other needs, so they can queue but never deadlock.

---

## 5. Event generator

### What it simulates
Each **order** has fixed attributes (amount, category, city, payment method) and produces a small
**lifecycle** of events over the next seconds or minutes:

```
placed ──► paid ──► shipped
   │         │
   └────────►└──► cancelled
```

- Cities, categories and payment methods are drawn with realistic weights (Casablanca has the
  most orders; cash on delivery is the most common payment method, as in Moroccan e-commerce).
- Amounts follow a log-normal distribution per category (many cheap books, a few very expensive
  electronics), which looks like real basket values.
- Cash-on-delivery orders are cancelled much more often than card orders.

### How the rate is controlled
New orders arrive as a **Poisson process**: the gap before the next order is random
(exponentially distributed), like real independent customers. Since each order produces about
2.9 events on average, the order rate is `target events/s ÷ 2.9`, which makes the total event
rate match the setting. A test runs 10 simulated minutes at 50 events/s and checks the measured
rate is within 10%.

On top of the base rate:
- **Daily curve** — quiet at night (≈0.2×), a lunch bump, an evening peak (≈2.2×), with a daily
  average of exactly 1×, in Casablanca time. `GEN_TIME_COMPRESSION=1440` squeezes a day into one
  real minute to show the curve in a demo.
- **Bursts** (flash sales) — a few times per hour, traffic is multiplied by 4 for 30 seconds.
- **Anomalies** — about twice per hour, for 2 minutes, either a **payment outage** (card payments
  fail, so those orders become cancellations: the cancellation-rate alert should fire) or a
  **traffic drop** (traffic ÷ 4: the revenue-drop alert should fire).
- **Malformed events** — about 1% extra events are deliberately broken (unknown city, negative
  amount, missing field, bad date…) to exercise the dead-letter path continuously.

### Why a pure simulator + a separate runner
The simulator has no clock and no network: you give it a time, it returns the events that
happened up to that time. The runner calls it every 250 ms with the real clock and sends the
result. This split means tests can simulate ten minutes of traffic in a fraction of a second,
with a fixed random seed that always produces exactly the same stream.

Event ids are **UUIDv7** built from the event's own timestamp plus seeded random bits, so they
are both reproducible and time-ordered (good for the database index, see Ingestion).

### A well-behaved producer
- **Waits** for `GET /health` before sending anything (the API may still be starting).
- **Retries** on `429`, `502/503/504` and network errors with **exponential backoff and full
  jitter**: attempt *n* waits a random time between 0 and `min(10 s, 0.25 s × 2ⁿ)`, and at least
  the server's `Retry-After`. The randomness matters: if 50 producers all fail at the same moment
  and all retry exactly 1 s later, they hit the recovering server at the same moment again
  ("thundering herd"). Retrying is safe because the API ignores duplicate `event_id`s.
- **Does not retry** other errors (e.g. `413 too large`): sending the same bytes again cannot work.
- **Bounds its own buffer**: if the API is down for a long time, new batches are dropped and
  counted instead of filling memory until the process crashes.

---

## 6. Packaging and running

### One image for the API and the generator
Both run from the same Docker image with a different command. They share the code (the event
schema), so building it once avoids two images drifting apart.

The Dockerfile installs dependencies **before** copying the source code. Docker caches each step,
so changing a line of Python rebuilds only the last layer in a second instead of reinstalling
every package. The container runs as a non-root user. The health check uses Python (the slim
image has no `curl`), and compose starts the generator only once the API reports healthy, and
the API only once PostgreSQL is healthy.

The API runs as **one** process. The WebSocket broadcaster (next milestone) keeps its client
list in memory, so several API processes would each only know their own clients. How to scale
past one process is listed in the README's limitations.

Uvicorn's access log is turned off: at several ingestion requests per second it's noise, and
formatting and writing each line costs CPU on the hot path. Errors are still logged.

---

## 7. Live push: the WebSocket layer

### Why WebSockets instead of polling
With **polling**, each browser asks `GET /metrics/snapshot` every second. Every client triggers
its own set of database queries, so 200 open dashboards = 200 snapshot builds per second, most of
them returning the same data. Updates also arrive up to one polling interval late.

With a **WebSocket**, the connection stays open and the server pushes. The server builds the
state **once per second whatever the number of clients**, serializes it **once**, and sends that
same string to everyone. Database load is constant; adding a client only adds a socket write.

Server-Sent Events (SSE) would also work, since data flows one way. WebSockets were chosen
because they are the stated requirement, and because the same connection can later carry
messages from the client (e.g. subscribing to one city) without a second channel.

### Push once per second, not on every commit
The writer commits several times per second under load. Pushing after each commit would send
more updates than a human can see and would multiply work. The hub **ticks at 1 Hz**: it reads
the aggregate tables once and pushes the result. Everything that happened during that second is
coalesced into one message.

An update is sent every tick even when nothing changed. It doubles as a **heartbeat**: a client
that hears nothing for a few seconds knows the connection is dead, even when TCP hasn't noticed.

### Message design: snapshot, small updates, periodic resync
| Type | When | Content |
|---|---|---|
| `snapshot` | on connect, then every 30 ticks | the full state, including 60 minute points and 24 hour points |
| `update` | every other tick | KPIs, breakdowns and statuses, but only the **last 3 minute points and the last hour point** |
| `events` | when new events were stored | up to 20 newest events, for the live feed |

Only the newest time buckets change from second to second, so resending 84 chart points every
second would be waste. The client replaces or adds the points it receives, matched by bucket
time. A **late event** can change an older bucket that no update carries, so a full snapshot every
30 seconds corrects any drift. Every message has a `seq` number that increases each tick.

### Backpressure: slow clients never slow down the others
A client on a bad mobile connection may read slowly. If the server waited for each socket before
moving on, one slow client would delay everyone; if it queued every message per client, memory
would grow without limit. The hub does neither:

1. **Hand-over never waits.** Each tick, the hub gives the message to every client object
   synchronously. Each client has its **own sender task** that does the actual network write.
2. **State is conflated.** Each client keeps at most **one** pending state message. If a newer one
   arrives before the old one was sent, the old one is simply replaced. A slow client skips
   intermediate states and always gets the latest, which is all a dashboard needs.
3. **The feed is lossy.** Feed messages go into a small queue (20) that drops the oldest.
4. **Stuck clients are evicted.** If a single write takes more than 5 seconds, the client is
   closed with WebSocket code **1013 "try again later"** and removed.

Memory per client is bounded (one state + 20 feed messages), whatever the client does. Unit tests
use a fake socket that blocks on purpose: they check that the blocked client receives only the
first and the latest state, that the feed drops the oldest items, and that a healthy client keeps
receiving every tick while the stuck one is evicted.

### Reconnection storms and capacity
When the API restarts, every client reconnects at nearly the same time. Two protections:
- The first `snapshot` a new client receives is the one **already built by the last tick** (at
  most one second old), not a new database query per connection.
- Above `ws_max_clients` (500 by default), new connections are accepted and immediately closed
  with **1013**, telling well-behaved clients to back off and retry.

Spreading those retries out is the client's job: exponential backoff with random jitter (see the
shared TypeScript client).

### Noticing disconnects
The server never expects messages from the client, but it keeps **reading** the socket: reading
is how a closed connection is detected. When the read loop ends, the client's sender task is
cancelled and it is removed from the hub. A test opens three clients, closes two, and checks that
`/health` reports one.

### Freshness and throughput in every message
Each message carries a `pipeline` block:
- `last_event_occurred_at`: the newest event time among committed events. The dashboard can show
  "data is 0.8 s old", and the benchmark uses it to measure end-to-end latency.
- `events_per_second`: committed events per second over the last ~5 seconds, measured in memory
  from the writer's counter. The per-minute table average reacts too slowly right after start-up.
- `connected_clients`.

### Limitation: one API process
The hub keeps its client list in memory. With several API processes behind a load balancer, each
process would only push to its own clients. That's still correct here, because every process
reads the same aggregate tables. What would break is the live feed and the freshness watermark,
which come from the process's own writer. The planned fix without new infrastructure is
PostgreSQL `LISTEN/NOTIFY`, so every process hears about every commit.

---

## 8. Alerting

### The rules
| Rule | Fires when (default) | Guard against noise | Severity |
|---|---|---|---|
| `cancellation_rate` | cancellations ÷ orders placed in the last 3 min **> 15%** | at least 30 orders in the window | warning |
| `revenue_drop` | revenue per minute in the last 3 min is **more than 50% below** the 3 min before | previous window ≥ 5,000 MAD/min | critical |
| `dead_letter_ratio` | rejected ÷ (accepted + rejected) in the last 3 min **> 5%** | at least 100 events in the window | warning |
| `pipeline_stalled` | no event stored for **30 s** | — | critical |

Every threshold is an environment variable (`RAD_ALERT_*`).

The "minimum volume" guards matter: at 3 a.m., 4 orders with 1 cancellation is a 25% cancellation
rate that means nothing. Below the floor a rule simply reports "not enough data".

### Rules are pure functions
A rule takes numbers (window totals) and returns "breached or not, value, message". It does not
query the database or read the clock. Each rule's boundaries (exactly at the threshold, just
above, below the volume floor) are unit-tested with plain values. A small store module reads the
window totals from the **aggregate tables** (two tiny queries per second, never the raw events).

### Comparing a partial window fairly
The current window ends *now*, so its last minute is only partly filled. Comparing raw totals would
make revenue look lower at the start of every minute and cause false "drops". The revenue rule
compares **rates** (MAD per minute) using the real elapsed seconds of each window. A test checks
that half a window with half the revenue counts as "no drop".

### Choosing the window: why 3 minutes (a tuning story)
The first design used 5-minute windows. Working through the numbers against the generator's
anomalies showed two problems:
- **Missed incidents.** A 2-minute traffic drop to 25% only lowers a 5-minute window's revenue to
  (3 × 100% + 2 × 25%) ÷ 5 = 70%, a 30% drop. The 50% rule would never fire. A shorter window
  reacts faster: with 3 minutes, a sustained drop crosses 50% about two minutes in.
- **False alarms from flash sales.** A 30-second burst at 4× traffic inflates the *previous* window.
  When traffic returns to normal it looks like a drop. With a 3-minute window the burst raises the
  baseline by about 50%, so normal traffic looks like a ~33% drop, still below the 50% threshold.

Order amounts have a heavy tail (a few very expensive electronics orders), so revenue over a short
window is noisy. That's why the threshold is a large 50%, and why an alert must hold for 10
seconds before firing (next section). The generator's anomalies last **4 minutes**, longer than the
window, so they're actually visible to the rules. A trade-off: shorter windows detect faster but
get noisier.

### No flapping: hysteresis in time
A value hovering around its threshold would fire and resolve every few seconds, and people stop
reading alerts that do that. The alert engine is a small state machine:
- an alert **fires** only after its rule has been breached **continuously for 10 s**;
- it **resolves** only after the rule has been healthy **continuously for 30 s**;
- a single healthy second during an incident resets the "healthy for" timer, and vice versa.

Unit tests feed one result per second: breached for 9 s → nothing; 10 s → fires once; a pattern
of 8 s breached / 1 s healthy repeated → never fires; healthy 29 s → still firing; 30 s → resolved.

The engine returns only **transitions** (newly firing, newly resolved). Each change is therefore
stored and pushed exactly once, not re-sent every second.

### Delivery
- Each transition is **saved to the `alerts` table first, then pushed** over the WebSocket. A client
  that reacts to the push by calling `GET /alerts` always finds it.
- Alerts have their **own per-client queue that is never trimmed**, unlike the lossy feed. If a
  client falls so far behind that 50 alerts are waiting, it is disconnected instead. When it
  reconnects it receives the snapshot and then every **currently firing** alert. An integration test
  checks this with a client that connects in the middle of an incident.
- The dashboards load the recent alert history once over REST (`GET /alerts`), then apply live
  changes. If a stale "firing" copy from REST arrives after the live "resolved" message, the client
  keeps the resolution (alerts only move firing → resolved).

### Restarts
The engine's state lives in memory. After a restart it cannot resolve alerts that the previous
process fired, so on startup the API marks any alert still `firing` in the table as resolved. If
the problem is still there, the new engine fires a fresh alert within seconds. The alternative,
rebuilding engine state from the table, would be more code for little benefit.

### Limitations
- Evaluation happens in the API process, once per second. If the database is down, rules cannot be
  evaluated. `pipeline_stalled` then can't fire either, which is exactly when you'd want it. A
  production system would add an external health check (e.g. an uptime monitor on `/health`).
- Thresholds are static. Comparing with the same hour last week would handle daily seasonality
  better; listed as a next step.

---

## 9. Clients: one core, three apps

### What lives in `@rad/core` (and why)
The same dashboard exists three times: React (web), Angular (web), React Native (mobile). The
parts that are easy to get subtly wrong are written **once**, in a framework-free TypeScript
package, and tested once:

| Module | Responsibility |
|---|---|
| `messages.ts` | TypeScript types mirroring the server's Pydantic models; `parseServerMessage` returns `null` for anything unknown, so a newer server never crashes an older client |
| `backoff.ts` | exponential backoff with full jitter |
| `connection.ts` | `LiveConnection`: keeps a WebSocket alive (below) |
| `reducer.ts` | merges server messages into dashboard state, preserving object identity |
| `format.ts` | MAD amounts, percentages, "data is 0.8 s old", connection status wording |
| `styles/dashboard.css` | the look shared by both web apps (same class names) |

Each app only renders. A bug fixed in the reconnection logic is fixed for all three clients.

### Reconnection with backoff (client side)
`LiveConnection` wraps a WebSocket and never gives up unless told to:
- After any disconnect it waits `random(0.25 s, min(30 s, 0.5 s × 2^attempt))` and tries again.
  The random part spreads out thousands of clients that lost the server at the same instant.
- The attempt counter resets only when a **message** arrives, not when the socket opens. A server
  that accepts and immediately closes (e.g. 1013 "busy") would otherwise reset the backoff on
  every attempt, and clients would hammer it.
- **Dead connections that look alive:** after a laptop sleeps or a mobile network changes, a
  socket can stay "open" while nothing flows. The server sends something every second, so
  5 seconds of silence means dead. The client closes the socket itself and reconnects.
- **Late events from an old socket are ignored.** Every handler checks it still belongs to the
  current socket; otherwise a delayed `close` from a replaced socket would trigger a second,
  parallel reconnection. A test covers this case.
- **Offline / online:** browsers (and apps) report network changes. While offline, retries stop
  (they cannot succeed and would only burn battery). When the network comes back, the client
  connects immediately with a fresh backoff.
- **Background tabs:** browsers throttle timers in hidden tabs. When the tab becomes visible
  again during a backoff wait, the web apps reconnect immediately.

All of this is tested with a fake socket and fake timers: the delays grow 500 → 1,000 → 2,000 →
4,000 ms while the server keeps failing, reset after a real message, a silent socket is dropped
after exactly 5 s, `stop()` never reconnects, and "offline" pauses retries.

### Degraded mode: polling when the WebSocket is blocked
Some corporate proxies block WebSockets. While the connection is not open, both web apps poll
`GET /api/metrics/snapshot` every 5 s and show the newest of the two sources. Polling stops the
moment the live connection is back. Users see slightly older numbers instead of a blank page.

### Smooth charts without flicker or full re-renders
Two things cause flicker in live dashboards: re-creating or re-animating a whole chart on every
update, and re-rendering every component on every message. Both are addressed:

1. **Identity-preserving reducer.** When an update arrives, the reducer compares each part of the
   state with the previous one *by value* and keeps the **old object** when nothing changed. If
   the category breakdown is identical, the new state holds the same array as before.
2. **Components re-render only when their own slice changes.**
   - React: every dashboard section is wrapped in `React.memo` and receives only its slice.
     Unchanged slice = same reference = no re-render.
   - Angular: `OnPush` components with signal inputs. Angular skips a component whose inputs are
     the same objects; `computed` signals only notify readers when the value's identity changes.
3. **Charts update in place.**
   - Recharts: animations are turned off for live series. Otherwise each update would replay the
     "draw the line" animation on the whole chart, which reads as flicker.
   - ECharts (Angular): the chart receives its static options once and then only `[merge]` with
     the new data, so the chart instance persists and moves smoothly to the new values.
4. **Stable list keys.** Feed rows are keyed by `event_id`, so only new rows are created; the
   short highlight animation plays only on them.

This is **tested, not assumed**. In the React app a test-only counter records how many times each
section renders (Vite removes the counter from production builds). The test renders the dashboard,
applies an update where only the current minute's revenue changed, and asserts that the revenue
chart rendered once while the KPI cards, category chart, status bar, city table and feed rendered
zero times. A second update changes the categories and asserts that only the category chart
re-renders. The Angular service test checks the same identity guarantees on its signals.

### React specifics
- **React Query where it fits:** the alert history (loaded once) and the degraded-mode snapshot
  polling (`enabled` only while disconnected, `refetchInterval` 5 s). Live data does not go through
  React Query: a WebSocket stream is not a request/response cache.
- `useReducer` + the core reducer; the WebSocket lives in a hook tied to the component's lifetime.
  React's development StrictMode deliberately mounts effects twice, and `start()`/`stop()` handle it.

### Angular specifics
- Angular 22: standalone components, zoneless change detection (the default for new projects),
  signals, and the new control flow (`@if`, `@for` with `track`).
- **Chart library: ECharts via `ngx-echarts`.** Recharts is React-only. ECharts was chosen over
  Chart.js because its `setOption` merge model maps directly onto live updates, it handles large
  series well, and it can be **tree-shaken**: the app registers only the line and bar charts,
  grid, tooltip and canvas renderer instead of shipping all of ECharts.
- Degraded-mode polling is an RxJS pipeline: `connection status → switchMap(open ? nothing :
  timer every 5 s → GET snapshot)`, turned back into a signal.

### Serving the web apps: same origin through nginx
Each web app is built into static files and served by nginx. The same nginx forwards `/api/*` and
`/ws/*` to the API container, and the Vite and Angular dev servers do the same during development.
The apps therefore only ever call their own origin:
- no CORS configuration, and no API hostname baked into the build;
- the WebSocket proxy needs `proxy_http_version 1.1`, the `Upgrade`/`Connection` headers, and a
  long `proxy_read_timeout`. Without them nginx would refuse the upgrade or cut connections that
  look idle.

### Workspace details that caused friction
- **TypeScript 6.0, not 7.** TypeScript 7 was the newest release, but Angular 22's compiler and
  typescript-eslint both require `>=6.0 <6.1`. The whole workspace pins `~6.0.3` so a single
  compiler version is installed.
- **`npm ci` in Docker needs every workspace's `package.json`,** even when building only one app,
  because it checks the lock file against all of them. Each Dockerfile copies all manifests first
  (a cached layer), then only the sources it needs.
- `@rad/core` is **compiled** to JavaScript before the apps use it (`dist/`). Bundlers can import
  TypeScript from a workspace package, but Angular's builder and React Native's Metro treat
  `node_modules` as already-compiled code, so shipping built JS is what works for all three.

### React Native specifics (Expo)
- **Expo** (SDK 57) rather than a bare React Native project: the app runs on a real phone through
  the free Expo Go app with no Android Studio or Xcode setup, and Expo configures Metro for
  monorepos automatically (no custom `metro.config.js`).
- **Not part of `docker compose`.** A phone app does not run in a container. It is started with
  `npx expo start` and talks to the API over the local network. `EXPO_PUBLIC_API_URL` must be the
  computer's LAN IP, because on the phone `localhost` is the phone itself. Expo inlines
  `EXPO_PUBLIC_*` variables at bundle time, so they must be read as `process.env.EXPO_PUBLIC_API_URL`
  (dot notation only).
- **Exactly one React in the workspace.** Expo 57 pins React 19.2.3, while the web apps would
  happily install the newer 19.3. Two copies of React in one app crash at runtime ("invalid hook
  call"), so the workspace root has npm `overrides` forcing a single version everywhere.
- **Charts without a chart library:** the revenue sparkline is two SVG paths drawn with
  `react-native-svg` (included in Expo Go). The path computation is a pure function with unit
  tests; bars and the status breakdown are plain `View`s with percentage widths.
- **Mobile lifecycle instead of browser events:** when the app goes to the background, the OS may
  suspend it and silently kill sockets. The app closes the connection on purpose (no retries
  draining the battery) and reconnects as soon as it returns to the foreground. Stale-connection
  detection from `@rad/core` covers network switches (Wi-Fi to mobile data) while in use.
- **What it shows:** KPI grid, revenue sparkline, category bars, orders by status, firing alerts
  and the live feed. It reuses the same memoized-sections approach as the web apps.

---

## 10. Problems met and how they were solved

### The Python virtual environment was very slow to install
The project folder is on the Windows drive, while Python runs inside WSL (Linux). Creating the
`.venv` inside the project meant thousands of small files written across the Windows/Linux
boundary: installing 14 packages took **2 minutes**. Fix: keep the project where it is but put
the environment on the Linux filesystem with `UV_PROJECT_ENVIRONMENT=$HOME/.venvs/rad`.
The same sync then took about a second.

### Tests failed with "connection refused"
After a restart of the machine, the whole DB test suite errored with `ConnectionRefusedError` on
port 5432. Nothing was wrong with the code: Docker Desktop was not running, so neither was
PostgreSQL. Starting it fixed it. Lesson: when *every* database test fails at once in fixture
setup, check the infrastructure before the code.

### Logs drowned in noise
The first `docker compose up` printed a line for every HTTP request from both the generator
(httpx logs at INFO level) and the API (uvicorn access log) — several per second — hiding the
generator's useful statistics line. httpx is now set to WARNING in the generator and the API runs
with `--no-access-log`.

### Docker Desktop failures during development, and what they showed
Twice, Docker Desktop's engine stopped answering (`500 Internal Server Error` on every API call),
taking PostgreSQL down. Two useful observations:
- The API container crashed at startup (it could not resolve the database host) and came back on
  its own once the database returned, thanks to `restart: unless-stopped`.
- The generator kept producing while the database was down. Its bounded buffer and retries with
  backoff held the events, and when everything came back it flushed the backlog. For a few
  seconds the dashboard showed about 2,600 events/s being committed, then it went back to normal.
  Nothing was lost, and duplicates from retries were ignored by the API.

### A Docker build failed because of a comment line
The Dockerfiles started with `# syntax=docker/dockerfile:1`. That line tells BuildKit to download
its Dockerfile parser image from Docker Hub before building. When the network had a TLS
timeout, every build failed at that step, even though all the base images were already cached
locally. The line was removed: the parser built into current Docker already supports the
`RUN --mount=type=cache` feature it was there for, so builds now work offline.

### Unit tests that were wrong, not the code
- A formatting test expected `850 ms` to display as `0.9 s`. JavaScript's `(0.85).toFixed(1)` gives
  `"0.8"`, because 0.85 cannot be stored exactly in binary floating point (it is really
  0.84999…). The test now uses a value that is not on a rounding boundary.
- An Angular test checked that the fallback snapshot request was sent, but the polling starts on
  an RxJS `timer(0)`, a macrotask. The assertion ran before the timer fired. The test now lets
  one macrotask pass first.
