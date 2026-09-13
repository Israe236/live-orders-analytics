"""Read side: dashboard metrics built from the aggregate tables only.

No query here touches the raw ``events`` table. Each reads at most a few thousand small rows
(one per minute, or per minute and dimension value), so the cost of building a snapshot does
not grow with the total number of events ever stored.

Every function takes ``now`` as a parameter so tests can pin the clock.
"""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import asyncpg

from rad.common.metrics import (
    ORDER_STATUSES,
    Breakdown,
    Granularity,
    IngestStats,
    Kpis,
    MetricsSnapshot,
    TimePoint,
)
from rad.db.pool import DbPool

# All windows include the current, still-filling minute. `$1` is now, `$2` a size in minutes.
_WINDOW = """
    bucket >= date_trunc('minute', $1::timestamptz) - ($2::int - 1) * interval '1 minute'
    AND bucket <= date_trunc('minute', $1::timestamptz)
"""

KPI_SQL = f"""
SELECT coalesce(sum(placed_count), 0)::bigint    AS placed,
       coalesce(sum(paid_count), 0)::bigint      AS paid,
       coalesce(sum(shipped_count), 0)::bigint   AS shipped,
       coalesce(sum(cancelled_count), 0)::bigint AS cancelled,
       coalesce(sum(revenue_mad), 0)             AS revenue_mad
FROM agg_minute
WHERE {_WINDOW}
"""

# Zero-filled: minutes without events still appear, so charts have a regular x axis.
MINUTE_SERIES_SQL = """
SELECT s.bucket,
       coalesce(a.revenue_mad, 0)     AS revenue_mad,
       coalesce(a.placed_count, 0)    AS placed,
       coalesce(a.paid_count, 0)      AS paid,
       coalesce(a.cancelled_count, 0) AS cancelled
FROM generate_series(
         date_trunc('minute', $1::timestamptz) - ($2::int - 1) * interval '1 minute',
         date_trunc('minute', $1::timestamptz),
         interval '1 minute'
     ) AS s(bucket)
LEFT JOIN agg_minute a ON a.bucket = s.bucket
ORDER BY s.bucket
"""

# Hours are rolled up from minute rows at read time: 24 h = 1,440 rows, a trivial range scan
# on the primary key. A separate hourly table would be one more thing to keep in sync.
HOUR_SERIES_SQL = """
SELECT s.bucket,
       coalesce(sum(a.revenue_mad), 0)::numeric       AS revenue_mad,
       coalesce(sum(a.placed_count), 0)::bigint       AS placed,
       coalesce(sum(a.paid_count), 0)::bigint         AS paid,
       coalesce(sum(a.cancelled_count), 0)::bigint    AS cancelled
FROM generate_series(
         date_trunc('hour', $1::timestamptz) - ($2::int - 1) * interval '1 hour',
         date_trunc('hour', $1::timestamptz),
         interval '1 hour'
     ) AS s(bucket)
LEFT JOIN agg_minute a
       ON a.bucket >= s.bucket
      AND a.bucket < s.bucket + interval '1 hour'
      AND a.bucket <= date_trunc('minute', $1::timestamptz)
GROUP BY s.bucket
ORDER BY s.bucket
"""

BREAKDOWN_SQL = f"""
SELECT dimension,
       value,
       sum(placed_count)::bigint    AS placed,
       sum(paid_count)::bigint      AS paid,
       sum(cancelled_count)::bigint AS cancelled,
       sum(revenue_mad)             AS revenue_mad
FROM agg_minute_dimension
WHERE dimension IN ('category', 'city', 'payment_method')
  AND {_WINDOW}
GROUP BY dimension, value
ORDER BY dimension, revenue_mad DESC, placed DESC, value
"""

STATUS_SQL = "SELECT status, order_count FROM order_status_counts"

INGEST_SQL = f"""
SELECT coalesce(sum(accepted_count), 0)::bigint    AS accepted,
       coalesce(sum(duplicate_count), 0)::bigint   AS duplicates,
       coalesce(sum(dead_letter_count), 0)::bigint AS dead_letters
FROM ingest_minute
WHERE {_WINDOW}
"""

SNAPSHOT_QUERIES = (
    KPI_SQL,
    MINUTE_SERIES_SQL,
    HOUR_SERIES_SQL,
    BREAKDOWN_SQL,
    STATUS_SQL,
    INGEST_SQL,
)


@dataclass(frozen=True, slots=True)
class SnapshotConfig:
    kpi_window_minutes: int = 60
    minute_points: int = 60
    hour_points: int = 24
    ingest_window_minutes: int = 5
    top_n: int = 10


def _time_point(row: asyncpg.Record) -> TimePoint:
    return TimePoint(
        bucket=row["bucket"],
        revenue_mad=float(row["revenue_mad"]),
        orders_placed=row["placed"],
        orders_paid=row["paid"],
        orders_cancelled=row["cancelled"],
    )


def _kpis(row: asyncpg.Record | None, window_minutes: int) -> Kpis:
    placed = int(row["placed"]) if row else 0
    paid = int(row["paid"]) if row else 0
    revenue: Decimal = row["revenue_mad"] if row else Decimal(0)
    cancelled = int(row["cancelled"]) if row else 0
    return Kpis(
        window_minutes=window_minutes,
        orders_placed=placed,
        orders_paid=paid,
        orders_shipped=int(row["shipped"]) if row else 0,
        orders_cancelled=cancelled,
        revenue_mad=float(revenue),
        avg_order_value_mad=float(round(revenue / paid, 2)) if paid else None,
        cancellation_rate=round(cancelled / placed, 4) if placed else None,
    )


def _ingest_stats(row: asyncpg.Record | None, now: datetime, window_minutes: int) -> IngestStats:
    accepted = int(row["accepted"]) if row else 0
    window_start = now.replace(second=0, microsecond=0) - timedelta(minutes=window_minutes - 1)
    elapsed_s = max((now - window_start).total_seconds(), 1.0)
    return IngestStats(
        window_minutes=window_minutes,
        accepted=accepted,
        duplicates=int(row["duplicates"]) if row else 0,
        dead_letters=int(row["dead_letters"]) if row else 0,
        events_per_second=round(accepted / elapsed_s, 2),
    )


async def fetch_snapshot(
    pool: DbPool, *, now: datetime | None = None, config: SnapshotConfig | None = None
) -> MetricsSnapshot:
    now = now or datetime.now(UTC)
    config = config or SnapshotConfig()
    # One read-only REPEATABLE READ transaction: every part of the snapshot sees the same
    # committed data, so e.g. KPIs and the chart can never disagree by one batch.
    async with (
        pool.acquire() as conn,
        conn.transaction(isolation="repeatable_read", readonly=True),
    ):
        kpi_row = await conn.fetchrow(KPI_SQL, now, config.kpi_window_minutes)
        minute_rows = await conn.fetch(MINUTE_SERIES_SQL, now, config.minute_points)
        hour_rows = await conn.fetch(HOUR_SERIES_SQL, now, config.hour_points)
        breakdown_rows = await conn.fetch(BREAKDOWN_SQL, now, config.kpi_window_minutes)
        status_rows = await conn.fetch(STATUS_SQL)
        ingest_row = await conn.fetchrow(INGEST_SQL, now, config.ingest_window_minutes)

    breakdowns: dict[str, list[Breakdown]] = {"category": [], "city": [], "payment_method": []}
    for row in breakdown_rows:
        items = breakdowns[row["dimension"]]
        if len(items) < config.top_n:
            items.append(
                Breakdown(
                    value=row["value"],
                    orders_placed=row["placed"],
                    orders_paid=row["paid"],
                    orders_cancelled=row["cancelled"],
                    revenue_mad=float(row["revenue_mad"]),
                )
            )

    orders_by_status: dict[str, int] = dict.fromkeys(ORDER_STATUSES, 0)
    for row in status_rows:
        orders_by_status[row["status"]] = int(row["order_count"])

    return MetricsSnapshot(
        generated_at=now,
        kpis=_kpis(kpi_row, config.kpi_window_minutes),
        revenue_per_minute=[_time_point(r) for r in minute_rows],
        revenue_per_hour=[_time_point(r) for r in hour_rows],
        orders_by_status=orders_by_status,
        categories=breakdowns["category"],
        cities=breakdowns["city"],
        payment_methods=breakdowns["payment_method"],
        ingest=_ingest_stats(ingest_row, now, config.ingest_window_minutes),
    )


async def fetch_timeseries(
    pool: DbPool, *, granularity: Granularity, points: int, now: datetime | None = None
) -> list[TimePoint]:
    now = now or datetime.now(UTC)
    sql = MINUTE_SERIES_SQL if granularity == "minute" else HOUR_SERIES_SQL
    rows: list[Any] = await pool.fetch(sql, now, points)
    return [_time_point(r) for r in rows]
