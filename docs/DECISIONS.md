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
