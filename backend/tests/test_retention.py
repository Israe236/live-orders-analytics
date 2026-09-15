"""Data retention: expired rows go, everything recent stays, counters stay consistent."""

import random
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest

from rad.common.config import Settings
from rad.common.events import EventType
from rad.db.pool import DbPool
from rad.processing.retention import RETENTION_LOCK_ID, RetentionPolicy, prune
from tests.helpers import count_rows, make_event, write_all

NOW = datetime(2026, 9, 15, 12, 0, tzinfo=UTC)
POLICY = RetentionPolicy(
    raw_events=timedelta(days=8),
    orders=timedelta(days=8),
    minute_buckets=timedelta(days=35),
    dead_letters=timedelta(days=14),
    resolved_alerts=timedelta(days=90),
    batch_size=3,  # tiny batches, so every table needs several delete statements
)


async def seed(pool: DbPool) -> None:
    rng = random.Random(9)
    old = NOW - timedelta(days=10)  # past raw-event and order retention, within bucket retention
    recent = NOW - timedelta(hours=1)
    events = []
    for _ in range(5):  # old orders: placed then paid
        order = UUID(int=rng.getrandbits(128), version=4)
        events.append(
            make_event(
                rng, order_id=order, event_type=EventType.ORDER_PLACED, occurred_at=old, amount="10"
            )
        )
        events.append(
            make_event(
                rng,
                order_id=order,
                event_type=EventType.ORDER_PAID,
                occurred_at=old + timedelta(minutes=1),
                amount="10",
            )
        )
    for _ in range(4):  # recent orders: placed only
        order = UUID(int=rng.getrandbits(128), version=4)
        events.append(
            make_event(
                rng,
                order_id=order,
                event_type=EventType.ORDER_PLACED,
                occurred_at=recent,
                amount="10",
            )
        )
    await write_all(pool, [events], batch_max=1_000)

    await pool.execute(
        "INSERT INTO agg_minute (bucket, placed_count) VALUES ($1, 7)", NOW - timedelta(days=40)
    )
    await pool.execute(
        "INSERT INTO dead_letter_events (received_at, reason, raw_payload) "
        "VALUES ($1, 'malformed_json', 'old'), ($2, 'malformed_json', 'recent')",
        NOW - timedelta(days=20),
        recent,
    )
    alert_sql = (
        "INSERT INTO alerts "
        "(id, rule, severity, status, message, threshold, started_at, resolved_at) "
        "VALUES ($1, 'revenue_drop', 'critical', $2, 'm', 0.6, $3, $4)"
    )
    long_ago = NOW - timedelta(days=100)
    await pool.execute(alert_sql, uuid4(), "resolved", long_ago, long_ago + timedelta(minutes=5))
    await pool.execute(alert_sql, uuid4(), "firing", long_ago, None)  # still firing: must stay
    await pool.execute(alert_sql, uuid4(), "resolved", recent, recent + timedelta(minutes=5))


async def test_prune_deletes_expired_rows_and_keeps_status_counts_consistent(
    db_pool: DbPool,
) -> None:
    await seed(db_pool)

    result = await prune(db_pool, POLICY, now=NOW)

    assert result.deleted == {
        "events": 10,
        "dead_letter_events": 1,
        "agg_minute": 1,
        "agg_minute_dimension": 0,
        "ingest_minute": 0,
        "alerts": 1,
        "orders": 5,
    }
    assert await count_rows(db_pool, "events") == 4
    assert await count_rows(db_pool, "orders") == 4
    # The 5 old orders were "paid": their counter went back to 0, the recent ones stay counted.
    rows = await db_pool.fetch("SELECT status, order_count FROM order_status_counts")
    assert {r["status"]: r["order_count"] for r in rows if r["order_count"]} == {"placed": 4}
    # Buckets of the 10-day-old events are within the 35-day bucket retention and stay.
    assert await count_rows(db_pool, "agg_minute") == 3
    assert await db_pool.fetchval("SELECT count(*) FROM alerts WHERE status = 'firing'") == 1

    again = await prune(db_pool, POLICY, now=NOW)
    assert not any(again.deleted.values())


async def test_prune_skips_when_another_process_holds_the_lock(db_pool: DbPool) -> None:
    async with db_pool.acquire() as other_process:
        await other_process.execute("SELECT pg_advisory_lock($1)", RETENTION_LOCK_ID)
        try:
            result = await prune(db_pool, POLICY, now=NOW)
        finally:
            await other_process.execute("SELECT pg_advisory_unlock($1)", RETENTION_LOCK_ID)
    assert result.skipped_because_locked
    assert result.deleted == {}


@pytest.mark.parametrize("field", ["retention_raw_events_days", "retention_minute_buckets_days"])
def test_retention_shorter_than_the_late_event_limit_is_refused(field: str) -> None:
    # The API accepts events up to 7 days old; keeping them for less would break deduplication.
    with pytest.raises(ValueError, match="max_event_age"):
        Settings.model_validate({field: 6}).retention_policy()
