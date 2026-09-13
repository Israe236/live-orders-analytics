"""The incremental aggregates must always equal a brute-force recomputation from raw events."""

import asyncio
import json
import random
from collections import Counter
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from uuid import UUID

import pytest

from rad.common.events import Category, City, EventType, OrderEvent, PaymentMethod
from rad.db.pool import DbPool
from rad.processing.snapshot import SNAPSHOT_QUERIES, fetch_snapshot
from rad.processing.writer import BatchWriter

STATUS_OF = {
    EventType.ORDER_PLACED: "placed",
    EventType.ORDER_PAID: "paid",
    EventType.ORDER_SHIPPED: "shipped",
    EventType.ORDER_CANCELLED: "cancelled",
}
RANK = {"placed": 1, "paid": 2, "shipped": 3, "cancelled": 4}


def make_event(
    rng: random.Random,
    *,
    order_id: UUID,
    event_type: EventType,
    occurred_at: datetime,
    amount: str | Decimal,
    category: Category = Category.ELECTRONICS,
    city: City = City.CASABLANCA,
    payment: PaymentMethod = PaymentMethod.CARD,
) -> OrderEvent:
    return OrderEvent.model_validate(
        {
            "event_id": UUID(int=rng.getrandbits(128), version=4),
            "order_id": order_id,
            "event_type": event_type,
            "occurred_at": occurred_at,
            "amount_mad": amount,
            "category": category,
            "city": city,
            "payment_method": payment,
        }
    )


def random_lifecycles(rng: random.Random, start: datetime, orders: int) -> list[OrderEvent]:
    """Realistic mixes: paid+shipped, cancelled before or after payment, still open."""
    events: list[OrderEvent] = []
    for _ in range(orders):
        order_id = UUID(int=rng.getrandbits(128), version=4)
        amount = Decimal(rng.randint(1_000, 500_000)) / 100
        dims: dict[str, Any] = {
            "category": rng.choice(list(Category)),
            "city": rng.choice(list(City)),
            "payment": rng.choice(list(PaymentMethod)),
        }
        roll = rng.random()
        steps = [EventType.ORDER_PLACED]
        if roll < 0.55:
            steps.append(EventType.ORDER_PAID)
            if rng.random() < 0.7:
                steps.append(EventType.ORDER_SHIPPED)
        elif roll < 0.75:
            steps.append(EventType.ORDER_CANCELLED)
        elif roll < 0.85:
            steps += [EventType.ORDER_PAID, EventType.ORDER_CANCELLED]
        t = start + timedelta(seconds=rng.uniform(0, 1_800))
        for step in steps:
            events.append(
                make_event(
                    rng, order_id=order_id, event_type=step, occurred_at=t, amount=amount, **dims
                )
            )
            t += timedelta(seconds=rng.uniform(1, 120))
    return events


async def write_all(pool: DbPool, submissions: list[list[OrderEvent]], batch_max: int) -> None:
    writer = BatchWriter(pool, max_queued_events=1_000_000, batch_max_events=batch_max)
    writer.start()
    try:
        await asyncio.gather(*(writer.submit(chunk) for chunk in submissions))
    finally:
        await writer.stop()


BRUTE_FORCE_MINUTE_SQL = """
SELECT date_trunc('minute', occurred_at) AS bucket,
       count(*) FILTER (WHERE event_type = 'order_placed')    AS placed_count,
       count(*) FILTER (WHERE event_type = 'order_paid')      AS paid_count,
       count(*) FILTER (WHERE event_type = 'order_shipped')   AS shipped_count,
       count(*) FILTER (WHERE event_type = 'order_cancelled') AS cancelled_count,
       coalesce(sum(amount_mad) FILTER (WHERE event_type = 'order_placed'), 0) AS placed_value_mad,
       coalesce(sum(amount_mad) FILTER (WHERE event_type = 'order_paid'), 0)   AS revenue_mad
FROM events
GROUP BY 1
ORDER BY 1
"""

BRUTE_FORCE_DIMENSION_SQL = """
SELECT d.dimension, date_trunc('minute', e.occurred_at) AS bucket, d.value,
       count(*) FILTER (WHERE e.event_type = 'order_placed')    AS placed_count,
       count(*) FILTER (WHERE e.event_type = 'order_paid')      AS paid_count,
       count(*) FILTER (WHERE e.event_type = 'order_cancelled') AS cancelled_count,
       coalesce(sum(e.amount_mad) FILTER (WHERE e.event_type = 'order_paid'), 0) AS revenue_mad
FROM events e
CROSS JOIN LATERAL (VALUES ('category', e.category), ('city', e.city),
                           ('payment_method', e.payment_method)) AS d(dimension, value)
GROUP BY 1, 2, 3
HAVING count(*) FILTER (WHERE e.event_type <> 'order_shipped') > 0
ORDER BY 1, 2, 3
"""


async def assert_aggregates_match_raw_events(pool: DbPool, events: list[OrderEvent]) -> None:
    expected_minutes = [tuple(r) for r in await pool.fetch(BRUTE_FORCE_MINUTE_SQL)]
    actual_minutes = [
        tuple(r)
        for r in await pool.fetch(
            "SELECT bucket, placed_count, paid_count, shipped_count, cancelled_count, "
            "placed_value_mad, revenue_mad FROM agg_minute ORDER BY bucket"
        )
    ]
    assert actual_minutes == expected_minutes

    expected_dims = [tuple(r) for r in await pool.fetch(BRUTE_FORCE_DIMENSION_SQL)]
    actual_dims = [
        tuple(r)
        for r in await pool.fetch(
            "SELECT dimension, bucket, value, placed_count, paid_count, cancelled_count, "
            "revenue_mad FROM agg_minute_dimension "
            "WHERE placed_count + paid_count + cancelled_count > 0 ORDER BY 1, 2, 3"
        )
    ]
    assert actual_dims == expected_dims

    # Final status per order, computed independently in Python: the highest-ranked status seen.
    final_status: dict[UUID, str] = {}
    for event in events:
        status = STATUS_OF[event.event_type]
        current = final_status.get(event.order_id)
        if current is None or RANK[status] > RANK[current]:
            final_status[event.order_id] = status
    expected_counts = dict(Counter(final_status.values()))
    rows = await pool.fetch("SELECT status, order_count FROM order_status_counts")
    actual_counts = {r["status"]: r["order_count"] for r in rows if r["order_count"] != 0}
    assert actual_counts == expected_counts
    assert await pool.fetchval("SELECT count(*) FROM orders") == len(final_status)


async def test_aggregates_match_brute_force_with_shuffled_batches_and_duplicates(
    db_pool: DbPool,
) -> None:
    rng = random.Random(20260912)
    start = datetime.now(UTC).replace(second=0, microsecond=0) - timedelta(hours=1)
    events = random_lifecycles(rng, start, orders=500)
    rng.shuffle(events)  # lifecycle steps of an order arrive in any order, across batches
    duplicates = rng.sample(events, 60)
    chunks = [events[i : i + 37] for i in range(0, len(events), 37)]
    # Each chunk also re-sends an event from its own chunk: duplicates *inside* one statement.
    chunks = [[*chunk, chunk[0]] for chunk in chunks]

    await write_all(db_pool, [*chunks, duplicates], batch_max=150)

    assert await db_pool.fetchval("SELECT count(*) FROM events") == len(events)
    await assert_aggregates_match_raw_events(db_pool, events)
    ingest = await db_pool.fetchrow(
        "SELECT sum(accepted_count) AS accepted, sum(duplicate_count) AS duplicates "
        "FROM ingest_minute"
    )
    assert ingest is not None
    assert ingest["accepted"] == len(events)
    assert ingest["duplicates"] == len(duplicates) + len(chunks)


async def test_same_order_with_several_events_in_one_statement(db_pool: DbPool) -> None:
    """ON CONFLICT DO UPDATE fails if one statement touches a row twice; the writer must
    collapse events per order before upserting `orders`."""
    rng = random.Random(1)
    order_id = UUID(int=rng.getrandbits(128), version=4)
    t = datetime.now(UTC)
    events = [
        make_event(rng, order_id=order_id, event_type=step, occurred_at=t, amount="120.00")
        for step in (EventType.ORDER_SHIPPED, EventType.ORDER_PLACED, EventType.ORDER_PAID)
    ]
    await write_all(db_pool, [events], batch_max=100)
    assert await db_pool.fetchval("SELECT status FROM orders") == "shipped"
    await assert_aggregates_match_raw_events(db_pool, events)


async def test_late_events_update_old_buckets(db_pool: DbPool) -> None:
    rng = random.Random(2)
    now = datetime.now(UTC)
    old = now.replace(second=0, microsecond=0) - timedelta(hours=3)
    order_id = UUID(int=rng.getrandbits(128), version=4)
    first = make_event(
        rng, order_id=order_id, event_type=EventType.ORDER_PAID, occurred_at=old, amount="10.00"
    )
    await write_all(db_pool, [[first]], batch_max=10)
    late = make_event(
        rng, order_id=order_id, event_type=EventType.ORDER_PLACED, occurred_at=old, amount="10.00"
    )
    await write_all(db_pool, [[late]], batch_max=10)

    row = await db_pool.fetchrow("SELECT placed_count, paid_count FROM agg_minute")
    assert row is not None
    assert (row["placed_count"], row["paid_count"]) == (1, 1)
    # A late "placed" must not move the order back from "paid".
    assert await db_pool.fetchval("SELECT status FROM orders") == "paid"
    await assert_aggregates_match_raw_events(db_pool, [first, late])


async def test_snapshot_values_for_a_known_scenario(db_pool: DbPool) -> None:
    rng = random.Random(3)
    now = datetime(2026, 9, 12, 12, 30, 30, tzinfo=UTC)

    def at(hh: int, mm: int) -> datetime:
        return datetime(2026, 9, 12, hh, mm, tzinfo=UTC)

    a, b, c, d = (UUID(int=rng.getrandbits(128), version=4) for _ in range(4))
    ev = EventType
    events = [
        make_event(
            rng, order_id=a, event_type=ev.ORDER_PLACED, occurred_at=at(12, 10), amount="100"
        ),
        make_event(rng, order_id=a, event_type=ev.ORDER_PAID, occurred_at=at(12, 11), amount="100"),
        make_event(
            rng,
            order_id=b,
            event_type=ev.ORDER_PLACED,
            occurred_at=at(12, 20),
            amount="300",
            category=Category.FASHION,
            city=City.RABAT,
            payment=PaymentMethod.CASH_ON_DELIVERY,
        ),
        make_event(
            rng,
            order_id=b,
            event_type=ev.ORDER_CANCELLED,
            occurred_at=at(12, 25),
            amount="300",
            category=Category.FASHION,
            city=City.RABAT,
            payment=PaymentMethod.CASH_ON_DELIVERY,
        ),
        # Order C was placed and paid outside the 60-minute window, shipped inside it.
        make_event(rng, order_id=c, event_type=ev.ORDER_PLACED, occurred_at=at(11, 0), amount="50"),
        make_event(rng, order_id=c, event_type=ev.ORDER_PAID, occurred_at=at(11, 5), amount="50"),
        make_event(
            rng, order_id=c, event_type=ev.ORDER_SHIPPED, occurred_at=at(12, 29), amount="50"
        ),
        # Order D is one minute in the future: not visible yet.
        make_event(
            rng, order_id=d, event_type=ev.ORDER_PLACED, occurred_at=at(12, 31), amount="999"
        ),
    ]
    await write_all(db_pool, [events], batch_max=100)

    snap = await fetch_snapshot(db_pool, now=now)

    k = snap.kpis
    assert (k.orders_placed, k.orders_paid, k.orders_shipped, k.orders_cancelled) == (2, 1, 1, 1)
    assert (k.revenue_mad, k.avg_order_value_mad, k.cancellation_rate) == (100.0, 100.0, 0.5)

    assert len(snap.revenue_per_minute) == 60
    assert snap.revenue_per_minute[-1].bucket == at(12, 30)
    by_minute = {p.bucket: p for p in snap.revenue_per_minute}
    assert by_minute[at(12, 11)].revenue_mad == 100.0
    assert at(11, 5) not in by_minute

    assert len(snap.revenue_per_hour) == 24
    assert [(p.revenue_mad, p.orders_placed) for p in snap.revenue_per_hour[-2:]] == [
        (50.0, 1),
        (100.0, 2),
    ]

    assert [(c.value, c.revenue_mad, c.orders_cancelled) for c in snap.categories] == [
        ("electronics", 100.0, 0),
        ("fashion", 0.0, 1),
    ]
    assert snap.orders_by_status == {"placed": 1, "paid": 1, "shipped": 1, "cancelled": 1}


def _relations(plan: Any) -> set[str]:
    found: set[str] = set()
    if isinstance(plan, dict):
        if "Relation Name" in plan:
            found.add(plan["Relation Name"])
        for value in plan.values():
            found |= _relations(value)
    elif isinstance(plan, list):
        for item in plan:
            found |= _relations(item)
    return found


@pytest.mark.parametrize("sql", SNAPSHOT_QUERIES)
async def test_snapshot_queries_never_read_raw_tables(db_pool: DbPool, sql: str) -> None:
    params: list[Any] = [datetime.now(UTC), 60] if "$1" in sql else []
    explain = await db_pool.fetchval(f"EXPLAIN (FORMAT JSON) {sql}", *params)
    relations = _relations(json.loads(explain))
    assert relations, "expected the plan to read at least one table"
    assert not relations & {"events", "orders", "dead_letter_events"}
