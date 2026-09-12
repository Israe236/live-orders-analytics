# realtime-analytics-dashboard

A real-time analytics pipeline for e-commerce order events (amounts in MAD, Moroccan cities):
a synthetic event generator, a FastAPI ingestion API, incremental time-bucketed aggregates in
PostgreSQL, a WebSocket push layer with alerting, and live dashboards in **React**, **Angular**
and **React Native**.

> 🚧 Work in progress. See [PLAN.md](PLAN.md) for the milestones and
> [docs/DECISIONS.md](docs/DECISIONS.md) for the reasoning behind each design choice.

## Progress

- [x] M0 — Repo skeleton
- [ ] M1 — Ingestion API
- [ ] M2 — Processing layer (incremental aggregates)
- [ ] M3 — Event generator
- [ ] M4 — WebSocket layer
- [ ] M5 — Alerting
- [ ] M6 — Shared client core
- [ ] M7 — React dashboard
- [ ] M8 — Angular dashboard
- [ ] M9 — React Native app
- [ ] M10 — CI
- [ ] M11 — Benchmark & final README

## Development (so far)

```bash
docker compose up -d postgres          # PostgreSQL 18 on localhost:5432
cd backend && uv sync && uv run pytest # backend tests
```
