# realtime-analytics-dashboard

A real-time analytics pipeline for e-commerce order events (amounts in MAD, Moroccan cities):
a synthetic event generator, a FastAPI ingestion API, incremental time-bucketed aggregates in
PostgreSQL, a WebSocket push layer with alerting, and live dashboards in **React**, **Angular**
and **React Native**.

> 🚧 Work in progress. See [PLAN.md](PLAN.md) for the milestones and
> [docs/DECISIONS.md](docs/DECISIONS.md) for the reasoning behind each design choice.

## Progress

- [x] M0 — Repo skeleton
- [x] M1 — Ingestion API (validation, dead letters, group-commit writer, 429 backpressure)
- [x] M2 — Processing layer (incremental minute buckets updated in the insert statement, snapshot API)
- [x] M3 — Event generator (order lifecycles, daily curve, bursts, anomalies, malformed events)
- [x] M4 — WebSocket layer (1 Hz fan-out, conflation, slow-client eviction, periodic resync)
- [x] M5 — Alerting (4 threshold rules, anti-flapping state machine, pushed live and stored)
- [x] M6 — Shared client core (`@rad/core`: types, reconnecting WebSocket, identity-preserving reducer)
- [x] M7 — React dashboard (Recharts, React Query, memoized sections)
- [x] M8 — Angular dashboard (signals, OnPush, tree-shaken ECharts)
- [ ] M9 — React Native app
- [ ] M10 — CI
- [ ] M11 — Benchmark & final README

## Run it (so far)

```bash
docker compose up --build        # postgres + api + generator + both web dashboards
```

| What | URL |
|---|---|
| React dashboard | http://localhost:5173 |
| Angular dashboard | http://localhost:4200 |
| API (OpenAPI docs) | http://localhost:8000/docs |
| Live snapshot (REST) | http://localhost:8000/metrics/snapshot |

The generator is tuned with environment variables (see [.env.example](.env.example)), e.g.
`GEN_EVENTS_PER_SECOND=100 GEN_TIME_COMPRESSION=1440 docker compose up`.

## Development

```bash
docker compose up -d postgres            # tests use a rad_test database on localhost:5432
cd backend && uv sync
uv run ruff check . && uv run mypy && uv run pytest

cd frontends && npm install
npm run build:core                       # the apps import the compiled @rad/core
npm run test && npm run lint && npm run typecheck
npm run dev -w @rad/web-react            # http://localhost:5173 (proxies /api and /ws to :8000)
npm run start -w @rad/web-angular        # http://localhost:4200
```
