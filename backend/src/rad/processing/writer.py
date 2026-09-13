"""Group-commit batch writer.

HTTP handlers never write valid events to the database themselves. They hand their events to
this writer and wait. One background task takes *everything* that is waiting, writes it with a
single SQL statement (one transaction), then wakes every waiting handler with its own result.

* Low load: a request is written as soon as it arrives (a batch of one) — latency stays low.
* High load: requests pile up while the previous write is running, so the next write carries
  many of them at once — throughput rises without any timer or tuning knob.
* Bounded: when more than ``max_queued_events`` are already waiting, ``submit`` fails fast with
  ``QueueFullError`` and the API answers 429. Producers slow down; memory stays bounded.
* Durable answers: a handler only gets its result after the transaction committed, so a
  success response really means "stored".
"""

import asyncio
import contextlib
import logging
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from uuid import UUID

from rad.common.events import OrderEvent
from rad.db.pool import DbPool

log = logging.getLogger(__name__)


class QueueFullError(Exception):
    """Too many events are already waiting to be written."""


class WriterUnavailableError(Exception):
    """The writer is shutting down, or the database write failed."""


@dataclass(frozen=True, slots=True)
class WriteResult:
    inserted: int
    duplicates: int


@dataclass(frozen=True, slots=True)
class CommittedBatch:
    """Events that were newly stored by one transaction (duplicates already removed)."""

    events: list[OrderEvent]
    committed_at: float  # unix time, seconds


@dataclass(slots=True)
class WriterStats:
    batches: int = 0
    events_inserted: int = 0
    duplicates: int = 0
    failed_batches: int = 0
    last_batch_events: int = 0
    last_commit_at: float | None = None


@dataclass(slots=True)
class _Submission:
    events: Sequence[OrderEvent]
    future: asyncio.Future[WriteResult]


type CommitListener = Callable[[CommittedBatch], None]

# One statement = one transaction = one round trip. Raw events and every aggregate are updated
# together, so the aggregates can never disagree with the events table.
#
# Aggregates are fed only from `inserted` — the rows that were really inserted — so duplicate
# events (retries) never count twice. Each upsert target gets at most one row per key from this
# statement (GROUP BY / DISTINCT ON): PostgreSQL refuses an ON CONFLICT DO UPDATE that would
# touch the same row twice in one command.
INSERT_EVENTS_SQL = """
WITH input AS (
    SELECT *
    FROM unnest(
        $1::uuid[], $2::uuid[], $3::text[], $4::timestamptz[],
        $5::numeric[], $6::text[], $7::text[], $8::text[]
    ) AS t(event_id, order_id, event_type, occurred_at, amount_mad, category, city, payment_method)
),
inserted AS (
    INSERT INTO events (
        event_id, order_id, event_type, occurred_at, amount_mad, category, city, payment_method
    )
    SELECT event_id, order_id, event_type, occurred_at, amount_mad, category, city, payment_method
    FROM input
    ON CONFLICT (event_id) DO NOTHING
    RETURNING event_id, order_id, occurred_at, amount_mad, category, city, payment_method,
              date_trunc('minute', occurred_at) AS bucket,
              replace(event_type, 'order_', '') AS status
),
minute_totals AS (
    INSERT INTO agg_minute AS a (
        bucket, placed_count, paid_count, shipped_count, cancelled_count,
        placed_value_mad, revenue_mad
    )
    SELECT bucket,
           count(*) FILTER (WHERE status = 'placed'),
           count(*) FILTER (WHERE status = 'paid'),
           count(*) FILTER (WHERE status = 'shipped'),
           count(*) FILTER (WHERE status = 'cancelled'),
           coalesce(sum(amount_mad) FILTER (WHERE status = 'placed'), 0),
           coalesce(sum(amount_mad) FILTER (WHERE status = 'paid'), 0)
    FROM inserted
    GROUP BY bucket
    ON CONFLICT (bucket) DO UPDATE SET
        placed_count     = a.placed_count     + excluded.placed_count,
        paid_count       = a.paid_count       + excluded.paid_count,
        shipped_count    = a.shipped_count    + excluded.shipped_count,
        cancelled_count  = a.cancelled_count  + excluded.cancelled_count,
        placed_value_mad = a.placed_value_mad + excluded.placed_value_mad,
        revenue_mad      = a.revenue_mad      + excluded.revenue_mad
),
dimension_totals AS (
    INSERT INTO agg_minute_dimension AS d (
        dimension, bucket, value, placed_count, paid_count, cancelled_count, revenue_mad
    )
    SELECT dim.dimension, i.bucket, dim.value,
           count(*) FILTER (WHERE i.status = 'placed'),
           count(*) FILTER (WHERE i.status = 'paid'),
           count(*) FILTER (WHERE i.status = 'cancelled'),
           coalesce(sum(i.amount_mad) FILTER (WHERE i.status = 'paid'), 0)
    FROM inserted AS i
    -- Fan each event out to its three dimensions.
    CROSS JOIN LATERAL (
        VALUES ('category', i.category), ('city', i.city), ('payment_method', i.payment_method)
    ) AS dim(dimension, value)
    WHERE i.status <> 'shipped'
    GROUP BY dim.dimension, i.bucket, dim.value
    ON CONFLICT (dimension, bucket, value) DO UPDATE SET
        placed_count    = d.placed_count    + excluded.placed_count,
        paid_count      = d.paid_count      + excluded.paid_count,
        cancelled_count = d.cancelled_count + excluded.cancelled_count,
        revenue_mad     = d.revenue_mad     + excluded.revenue_mad
),
order_updates AS (
    INSERT INTO orders AS o (
        order_id, status, amount_mad, category, city, payment_method,
        first_event_at, last_event_at
    )
    -- Several events of the same order may be in this batch: keep the highest-ranked one.
    SELECT DISTINCT ON (order_id)
           order_id, status, amount_mad, category, city, payment_method,
           min(occurred_at) OVER (PARTITION BY order_id),
           max(occurred_at) OVER (PARTITION BY order_id)
    FROM inserted
    ORDER BY order_id, order_status_rank(status) DESC
    ON CONFLICT (order_id) DO UPDATE SET
        status = CASE
                     WHEN order_status_rank(excluded.status) > order_status_rank(o.status)
                     THEN excluded.status
                     ELSE o.status
                 END,
        first_event_at = least(o.first_event_at, excluded.first_event_at),
        last_event_at  = greatest(o.last_event_at, excluded.last_event_at)
    -- PostgreSQL 18: RETURNING can read the row before (old) and after (new) the change.
    -- old.status is NULL for a brand-new order.
    RETURNING old.status AS old_status, new.status AS new_status
),
status_counts AS (
    INSERT INTO order_status_counts AS s (status, order_count)
    SELECT status, sum(delta)
    FROM (
        SELECT new_status AS status, 1 AS delta
        FROM order_updates
        WHERE old_status IS DISTINCT FROM new_status
        UNION ALL
        SELECT old_status, -1
        FROM order_updates
        WHERE old_status IS NOT NULL AND old_status <> new_status
    ) AS changes
    GROUP BY status
    ON CONFLICT (status) DO UPDATE SET order_count = s.order_count + excluded.order_count
),
ingest_totals AS (
    INSERT INTO ingest_minute AS m (bucket, accepted_count, duplicate_count)
    SELECT date_trunc('minute', now()),
           (SELECT count(*) FROM inserted),
           (SELECT count(*) FROM input) - (SELECT count(*) FROM inserted)
    ON CONFLICT (bucket) DO UPDATE SET
        accepted_count  = m.accepted_count  + excluded.accepted_count,
        duplicate_count = m.duplicate_count + excluded.duplicate_count
)
SELECT event_id FROM inserted
"""


class BatchWriter:
    def __init__(
        self,
        pool: DbPool,
        *,
        max_queued_events: int,
        batch_max_events: int,
        failure_pause_s: float = 0.5,
    ) -> None:
        self._pool = pool
        self._max_queued_events = max_queued_events
        self._batch_max_events = batch_max_events
        self._failure_pause_s = failure_pause_s
        # `None` is the stop sentinel. The queue itself is unbounded in *items*; the bound that
        # matters is the number of *events*, tracked in `_queued_events`.
        self._queue: asyncio.Queue[_Submission | None] = asyncio.Queue()
        self._queued_events = 0
        self._accepting = False
        self._task: asyncio.Task[None] | None = None
        self._listeners: list[CommitListener] = []
        self.stats = WriterStats()

    @property
    def queued_events(self) -> int:
        return self._queued_events

    def add_listener(self, listener: CommitListener) -> None:
        """Register a callback invoked (synchronously) after each successful commit."""
        self._listeners.append(listener)

    def start(self) -> None:
        self._accepting = True
        self._task = asyncio.create_task(self._run(), name="batch-writer")

    async def stop(self, timeout_s: float = 10.0) -> None:
        """Stop accepting new events, write what is already queued, then finish."""
        if self._task is None:
            return
        self._accepting = False
        self._queue.put_nowait(None)
        try:
            async with asyncio.timeout(timeout_s):
                await self._task
        except TimeoutError:
            log.error("batch writer did not drain within %.1fs; cancelling", timeout_s)
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        self._task = None

    async def submit(self, events: Sequence[OrderEvent]) -> WriteResult:
        """Queue events and wait until they are committed."""
        if not events:
            return WriteResult(inserted=0, duplicates=0)
        if not self._accepting:
            raise WriterUnavailableError("writer is not running")
        if self._queued_events + len(events) > self._max_queued_events:
            raise QueueFullError
        future: asyncio.Future[WriteResult] = asyncio.get_running_loop().create_future()
        self._queued_events += len(events)
        self._queue.put_nowait(_Submission(events, future))
        # If the HTTP client disconnects, this await is cancelled (which cancels the future).
        # The events are still written; the writer just skips reporting to a cancelled future.
        return await future

    async def _run(self) -> None:
        while True:
            first = await self._queue.get()
            if first is None:
                return
            batch = [first]
            size = len(first.events)
            stop_after_batch = False
            # Take whatever else is already waiting — no waiting for more.
            while size < self._batch_max_events:
                try:
                    item = self._queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                if item is None:
                    stop_after_batch = True
                    break
                batch.append(item)
                size += len(item.events)
            await self._write(batch, size)
            if stop_after_batch:
                return

    async def _write(self, batch: list[_Submission], size: int) -> None:
        events = [event for submission in batch for event in submission.events]
        try:
            inserted_ids = await self._insert(events)
        except Exception:
            self._queued_events -= size
            self.stats.failed_batches += 1
            log.exception("writing a batch of %d events failed", size)
            for submission in batch:
                if not submission.future.done():
                    submission.future.set_exception(WriterUnavailableError("database write failed"))
            # Avoid a hot loop of failures while the database is down.
            await asyncio.sleep(self._failure_pause_s)
            return

        self._queued_events -= size
        # Attribute each inserted id to exactly one submission, so that the same event sent
        # twice in one batch counts as one insert and one duplicate.
        remaining: set[UUID] = set(inserted_ids)
        new_events: list[OrderEvent] = []
        for submission in batch:
            inserted = 0
            for event in submission.events:
                if event.event_id in remaining:
                    remaining.discard(event.event_id)
                    inserted += 1
                    new_events.append(event)
            if not submission.future.done():
                submission.future.set_result(
                    WriteResult(inserted=inserted, duplicates=len(submission.events) - inserted)
                )

        now = time.time()
        self.stats.batches += 1
        self.stats.events_inserted += len(new_events)
        self.stats.duplicates += size - len(new_events)
        self.stats.last_batch_events = size
        self.stats.last_commit_at = now
        if new_events:
            committed = CommittedBatch(events=new_events, committed_at=now)
            for listener in self._listeners:
                try:
                    listener(committed)
                except Exception:
                    log.exception("commit listener failed")

    async def _insert(self, events: list[OrderEvent]) -> set[UUID]:
        rows = await self._pool.fetch(
            INSERT_EVENTS_SQL,
            [e.event_id for e in events],
            [e.order_id for e in events],
            [e.event_type.value for e in events],
            [e.occurred_at for e in events],
            [e.amount_mad for e in events],
            [e.category.value for e in events],
            [e.city.value for e in events],
            [e.payment_method.value for e in events],
        )
        return {row["event_id"] for row in rows}
