"""Alert persistence and window statistics read from the aggregate tables."""

import random
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

from rad.alerts.rules import Alert, AlertStatus, RuleName, Severity
from rad.alerts.store import fetch_alerts, fetch_window_stats, resolve_stale_alerts, save_alert
from rad.common.events import EventType
from rad.db.pool import DbPool
from tests.helpers import make_event, write_all

NOW = datetime(2026, 9, 13, 20, 0, 30, tzinfo=UTC)


def at(hh: int, mm: int) -> datetime:
    return datetime(2026, 9, 13, hh, mm, tzinfo=UTC)


async def test_window_stats_split_recent_and_previous_windows(db_pool: DbPool) -> None:
    rng = random.Random(5)

    def order() -> UUID:
        return UUID(int=rng.getrandbits(128), version=4)

    events = [
        make_event(
            rng,
            order_id=order(),
            event_type=EventType.ORDER_PAID,
            occurred_at=at(19, 52),
            amount="100",
        ),
        make_event(
            rng,
            order_id=order(),
            event_type=EventType.ORDER_PAID,
            occurred_at=at(19, 58),
            amount="30",
        ),
        make_event(
            rng,
            order_id=order(),
            event_type=EventType.ORDER_CANCELLED,
            occurred_at=at(19, 59),
            amount="5",
        ),
        # Outside both windows: before 19:51 and after the current minute.
        make_event(
            rng,
            order_id=order(),
            event_type=EventType.ORDER_PAID,
            occurred_at=at(19, 50),
            amount="999",
        ),
        make_event(
            rng,
            order_id=order(),
            event_type=EventType.ORDER_PAID,
            occurred_at=at(20, 1),
            amount="999",
        ),
    ]
    events += [
        make_event(
            rng,
            order_id=order(),
            event_type=EventType.ORDER_PLACED,
            occurred_at=at(19, 57),
            amount="1",
        )
        for _ in range(4)
    ]
    await write_all(db_pool, [events], batch_max=100)
    await db_pool.execute(
        "INSERT INTO ingest_minute (bucket, accepted_count, dead_letter_count) "
        "VALUES ($1, 200, 20), ($2, 999, 999)",
        at(19, 57),
        at(19, 50),
    )

    stats = await fetch_window_stats(db_pool, now=NOW, window_minutes=5, last_commit_at=None)

    # Current window: 19:56:00 → 20:00:30 (270 s). Previous window: 19:51 → 19:55:59.
    assert (stats.recent_seconds, stats.previous_seconds) == (270.0, 300.0)
    assert (stats.placed, stats.cancelled) == (4, 1)
    assert (stats.revenue_recent, stats.revenue_previous) == (30.0, 100.0)
    assert (stats.accepted, stats.dead_letters) == (200, 20)


async def test_save_update_list_and_resolve_stale(db_pool: DbPool) -> None:
    first = Alert(
        id=uuid4(),
        rule=RuleName.REVENUE_DROP,
        severity=Severity.CRITICAL,
        status=AlertStatus.FIRING,
        message="Revenue down 70%",
        value=0.7,
        threshold=0.5,
        started_at=NOW,
    )
    second = replace(
        first,
        id=uuid4(),
        rule=RuleName.CANCELLATION_RATE,
        severity=Severity.WARNING,
        message="Cancellation rate 22%",
        value=0.22,
        threshold=0.15,
        started_at=NOW + timedelta(minutes=1),
    )
    await save_alert(db_pool, first)
    await save_alert(db_pool, second)
    # Same id again: an update of the existing row, not a second alert.
    await save_alert(
        db_pool,
        replace(first, status=AlertStatus.RESOLVED, resolved_at=NOW + timedelta(minutes=3)),
    )

    listed = await fetch_alerts(db_pool, limit=10)
    assert [a.id for a in listed] == [second.id, first.id]  # newest first
    assert listed[1].status == "resolved"
    assert [a.id for a in await fetch_alerts(db_pool, limit=10, status="firing")] == [second.id]

    assert await resolve_stale_alerts(db_pool, now=NOW + timedelta(hours=1)) == 1
    assert await fetch_alerts(db_pool, limit=10, status="firing") == []
