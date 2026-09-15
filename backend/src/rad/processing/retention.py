"""Data retention: delete what is older than its retention period, in small batches.

Dashboards read minute buckets; raw events are only kept for audits, deduplication and
recomputation. Each kind of data has its own period. Deletes run a few thousand rows per
statement, so a cleanup never holds locks for long, and a PostgreSQL advisory lock makes sure
only one API process prunes at a time.
"""

import asyncio
import contextlib
import logging
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

from rad.db.pool import DbPool

log = logging.getLogger(__name__)

RETENTION_LOCK_ID = 7_240_002  # arbitrary, unique to this job


@dataclass(frozen=True, slots=True)
class RetentionPolicy:
    raw_events: timedelta
    orders: timedelta
    minute_buckets: timedelta
    dead_letters: timedelta
    resolved_alerts: timedelta
    batch_size: int = 5_000


@dataclass(slots=True)
class PruneResult:
    deleted: dict[str, int] = field(default_factory=dict)
    skipped_because_locked: bool = False
    seconds: float = 0.0


# (label, table, time column, policy attribute, extra condition)
_TABLES: tuple[tuple[str, str, str, str, str], ...] = (
    ("events", "events", "occurred_at", "raw_events", ""),
    ("dead_letter_events", "dead_letter_events", "received_at", "dead_letters", ""),
    ("agg_minute", "agg_minute", "bucket", "minute_buckets", ""),
    ("agg_minute_dimension", "agg_minute_dimension", "bucket", "minute_buckets", ""),
    ("ingest_minute", "ingest_minute", "bucket", "minute_buckets", ""),
    # A firing alert is never deleted, however old.
    ("alerts", "alerts", "started_at", "resolved_alerts", "AND status = 'resolved'"),
)

# Orders are deleted together with the matching decrement of the per-status counters, in one
# statement, so "orders by status" always equals the orders actually stored.
_DELETE_ORDERS_SQL = """
WITH doomed AS (
    SELECT ctid FROM orders WHERE first_event_at < $1 LIMIT $2
),
deleted AS (
    DELETE FROM orders WHERE ctid IN (SELECT ctid FROM doomed) RETURNING status
),
per_status AS (
    SELECT status, count(*) AS removed FROM deleted GROUP BY status
),
adjusted AS (
    UPDATE order_status_counts AS s
    SET order_count = s.order_count - p.removed
    FROM per_status AS p
    WHERE s.status = p.status
)
SELECT coalesce(sum(removed), 0)::bigint FROM per_status
"""


def _delete_batch_sql(table: str, column: str, extra: str) -> str:
    # Table and column names come from the constant list above, never from input.
    return (
        f"DELETE FROM {table} WHERE ctid = ANY(ARRAY("
        f"SELECT ctid FROM {table} WHERE {column} < $1 {extra} LIMIT $2))"
    )


async def _delete_in_batches(pool: DbPool, sql: str, cutoff: datetime, batch_size: int) -> int:
    total = 0
    while True:
        status = await pool.execute(sql, cutoff, batch_size)  # command tag, e.g. "DELETE 5000"
        deleted = int(status.split()[-1])
        total += deleted
        if deleted < batch_size:
            return total


async def _delete_orders(pool: DbPool, cutoff: datetime, batch_size: int) -> int:
    total = 0
    while True:
        deleted = int(await pool.fetchval(_DELETE_ORDERS_SQL, cutoff, batch_size))
        total += deleted
        if deleted < batch_size:
            return total


async def prune(pool: DbPool, policy: RetentionPolicy, *, now: datetime) -> PruneResult:
    result = PruneResult()
    started = time.monotonic()
    async with pool.acquire() as lock_conn:
        if not await lock_conn.fetchval("SELECT pg_try_advisory_lock($1)", RETENTION_LOCK_ID):
            result.skipped_because_locked = True
            return result
        try:
            for label, table, column, attribute, extra in _TABLES:
                period: timedelta = getattr(policy, attribute)
                result.deleted[label] = await _delete_in_batches(
                    pool, _delete_batch_sql(table, column, extra), now - period, policy.batch_size
                )
            result.deleted["orders"] = await _delete_orders(
                pool, now - policy.orders, policy.batch_size
            )
        finally:
            await lock_conn.execute("SELECT pg_advisory_unlock($1)", RETENTION_LOCK_ID)
    result.seconds = time.monotonic() - started
    return result


class RetentionJob:
    """Runs ``prune`` at startup and then every ``interval_s`` seconds."""

    def __init__(self, pool: DbPool, policy: RetentionPolicy, *, interval_s: float) -> None:
        self._pool = pool
        self._policy = policy
        self._interval_s = interval_s
        self._task: asyncio.Task[None] | None = None

    def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="retention")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None

    async def _run(self) -> None:
        while True:
            try:
                result = await prune(self._pool, self._policy, now=datetime.now(UTC))
                if any(result.deleted.values()):
                    log.info("retention deleted %s in %.1f s", result.deleted, result.seconds)
            except Exception:
                log.exception("retention run failed; retrying at the next interval")
            await asyncio.sleep(self._interval_s)
