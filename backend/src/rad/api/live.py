"""WebSocket fan-out: one hub, many clients, and slow clients never slow down the others.

The hub ticks once per second: it builds the dashboard state from the aggregate tables *once*,
serializes it *once*, and hands the same string to every client. Handing it over never waits
on the network — each client has its own sender task that does the actual socket writes.

Per client:
* **State is conflated.** A client holds at most one pending state message. If a newer one
  arrives before the previous was sent, the old one is replaced: a slow client simply skips
  intermediate states and always receives the latest.
* **The feed is lossy.** Recent-event messages go to a small queue that drops its oldest items.
* **Alerts are never dropped.** They have their own queue; a client so far behind that it
  cannot take them is disconnected (it gets the active alerts again when it reconnects).
* **Stuck clients are evicted.** If one socket write takes longer than ``send_timeout_s`` the
  client is closed with code 1013 ("try again later") and removed.

Memory per client is therefore bounded, whatever the client does.

The same tick also evaluates the alert rules (see ``rad.alerts``).
"""

import asyncio
import contextlib
import logging
import time
from collections import deque
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Protocol

from fastapi import WebSocket

from rad.alerts.rules import Alert, AlertEngine, evaluate_rules
from rad.alerts.store import AlertOut, fetch_window_stats, save_alert
from rad.common.config import Settings
from rad.common.events import OrderEvent
from rad.common.live import (
    AlertMessage,
    EventsMessage,
    FeedEvent,
    LiveUpdate,
    PipelineStats,
    SnapshotMessage,
    UpdateMessage,
)
from rad.db.pool import DbPool
from rad.processing.snapshot import fetch_snapshot
from rad.processing.writer import BatchWriter, CommittedBatch

log = logging.getLogger(__name__)

CLOSE_GOING_AWAY = 1001
CLOSE_TRY_AGAIN_LATER = 1013
_RATE_WINDOW_S = 5.0


class MessageSink(Protocol):
    """The part of a WebSocket a client connection needs (lets tests use fakes)."""

    async def send_text(self, data: str) -> None: ...

    async def close(self, code: int = 1000, reason: str | None = None) -> None: ...


class ClientConnection:
    def __init__(
        self,
        sink: MessageSink,
        *,
        send_timeout_s: float,
        on_closed: Callable[[ClientConnection], None],
        feed_queue_size: int = 20,
        max_pending_alerts: int = 50,
    ) -> None:
        self._sink = sink
        self._send_timeout_s = send_timeout_s
        self._on_closed = on_closed
        self._state: str | None = None
        self._feed: deque[str] = deque(maxlen=feed_queue_size)
        self._alerts: deque[str] = deque()
        self._max_pending_alerts = max_pending_alerts
        self._alerts_overflowed = False
        self._wakeup = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self.closed = False
        self.sent = 0
        self.conflated = 0
        self.feed_dropped = 0

    def start(self) -> None:
        self._task = asyncio.create_task(self._send_loop(), name="ws-client-sender")

    def push_state(self, message: str) -> None:
        if self.closed:
            return
        if self._state is not None:
            self.conflated += 1  # the previous state was never sent: skip it
        self._state = message
        self._wakeup.set()

    def push_feed(self, message: str) -> None:
        if self.closed:
            return
        if len(self._feed) == self._feed.maxlen:
            self.feed_dropped += 1  # deque drops the oldest item on append
        self._feed.append(message)
        self._wakeup.set()

    def push_alert(self, message: str) -> None:
        if self.closed:
            return
        if len(self._alerts) >= self._max_pending_alerts:
            self._alerts_overflowed = True  # the sender task closes the connection
        else:
            self._alerts.append(message)
        self._wakeup.set()

    async def _send_loop(self) -> None:
        try:
            while True:
                await self._wakeup.wait()
                self._wakeup.clear()
                if self._alerts_overflowed:
                    log.warning("closing a WebSocket client that cannot keep up with alerts")
                    await self._close_sink(CLOSE_TRY_AGAIN_LATER, "client too slow")
                    return
                while (message := self._next_message()) is not None:
                    async with asyncio.timeout(self._send_timeout_s):
                        await self._sink.send_text(message)
                    self.sent += 1
        except TimeoutError:
            log.warning("closing a WebSocket client that is too slow")
            await self._close_sink(CLOSE_TRY_AGAIN_LATER, "client too slow")
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # the peer went away mid-send
            log.debug("WebSocket send failed: %r", exc)
        finally:
            self._mark_closed()

    def _next_message(self) -> str | None:
        # Most important first: alerts, then the latest state, then the feed.
        if self._alerts:
            return self._alerts.popleft()
        if self._state is not None:
            message, self._state = self._state, None
            return message
        if self._feed:
            return self._feed.popleft()
        return None

    async def close(self, code: int = CLOSE_GOING_AWAY, reason: str = "") -> None:
        if self._task is not None and not self._task.done():
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        if not self.closed:
            await self._close_sink(code, reason)
        self._mark_closed()

    async def _close_sink(self, code: int, reason: str) -> None:
        # Closing sends a frame too, which a stuck client may never accept: bound it.
        with contextlib.suppress(Exception):
            async with asyncio.timeout(1.0):
                await self._sink.close(code=code, reason=reason)

    def _mark_closed(self) -> None:
        if not self.closed:
            self.closed = True
            self._on_closed(self)


class LiveHub:
    def __init__(self, pool: DbPool, writer: BatchWriter, settings: Settings) -> None:
        self._pool = pool
        self._writer = writer
        self._settings = settings
        self._clients: set[ClientConnection] = set()
        self._recent: deque[OrderEvent] = deque(maxlen=settings.ws_feed_max_events)
        self._last_event_occurred_at: datetime | None = None
        self._last_commit_at: float | None = None
        self._rate_samples: deque[tuple[float, int]] = deque()
        self._seq = 0
        self._latest_snapshot: str | None = None
        self._task: asyncio.Task[None] | None = None
        self._thresholds = settings.alert_thresholds()
        self._alerts = AlertEngine(
            fire_after=timedelta(seconds=settings.alert_fire_after_s),
            resolve_after=timedelta(seconds=settings.alert_resolve_after_s),
        )
        self._started_at = datetime.now(UTC)
        self.tick_errors = 0
        self.rejected_clients = 0

    @property
    def client_count(self) -> int:
        return len(self._clients)

    @property
    def active_alerts(self) -> list[Alert]:
        return self._alerts.active_alerts

    # --- writer side -----------------------------------------------------------------------

    def on_commit(self, batch: CommittedBatch) -> None:
        """Writer callback (same event loop): remember what just got stored. Never blocks."""
        self._recent.extend(batch.events)
        newest = max(event.occurred_at for event in batch.events)
        if self._last_event_occurred_at is None or newest > self._last_event_occurred_at:
            self._last_event_occurred_at = newest
        self._last_commit_at = batch.committed_at

    # --- lifecycle -------------------------------------------------------------------------

    def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="live-hub")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        await asyncio.gather(
            *(
                client.close(CLOSE_GOING_AWAY, "server shutting down")
                for client in list(self._clients)
            )
        )

    async def _run(self) -> None:
        loop = asyncio.get_running_loop()
        tick = 0
        while True:
            started = loop.time()
            try:
                await self._tick(tick)
            except Exception:
                self.tick_errors += 1
                log.exception("live hub tick failed")
            try:
                await self._evaluate_alerts()
            except Exception:
                self.tick_errors += 1
                log.exception("alert evaluation failed")
            tick += 1
            await asyncio.sleep(
                max(0.0, self._settings.ws_tick_interval_s - (loop.time() - started))
            )

    async def _tick(self, tick: int) -> None:
        full = await self._snapshot_message()
        if tick % self._settings.ws_full_snapshot_every_ticks == 0:
            state = full
        else:
            snapshot = SnapshotMessage.model_validate_json(full)
            state = UpdateMessage(
                seq=snapshot.seq,
                sent_at=snapshot.sent_at,
                pipeline=snapshot.pipeline,
                data=LiveUpdate.from_snapshot(snapshot.data),
            ).model_dump_json()

        feed: str | None = None
        if self._recent:
            items = [FeedEvent.from_event(event) for event in reversed(self._recent)]
            self._recent.clear()
            feed = EventsMessage(seq=self._seq, items=items).model_dump_json()

        # Fan-out: synchronous hand-over to each client, no awaiting on any socket.
        for client in list(self._clients):
            client.push_state(state)
            if feed is not None:
                client.push_feed(feed)

    async def _evaluate_alerts(self) -> None:
        now = datetime.now(UTC)
        stats = await fetch_window_stats(
            self._pool,
            now=now,
            window_minutes=self._thresholds.window_minutes,
            last_commit_at=self._last_commit_datetime(),
        )
        results = evaluate_rules(stats, self._thresholds, started_at=self._started_at)
        for alert in self._alerts.update(results, now):
            # Store before pushing, so GET /alerts never disagrees with what clients were told.
            await save_alert(self._pool, alert)
            log.warning("alert %s %s: %s", alert.status, alert.rule, alert.message)
            message = self._alert_message(alert)
            for client in list(self._clients):
                client.push_alert(message)

    def _alert_message(self, alert: Alert) -> str:
        return AlertMessage(seq=self._seq, alert=AlertOut.from_alert(alert)).model_dump_json()

    async def _snapshot_message(self) -> str:
        snapshot = await fetch_snapshot(self._pool)
        self._seq += 1
        message = SnapshotMessage(
            seq=self._seq,
            sent_at=datetime.now(UTC),
            pipeline=self._pipeline_stats(),
            data=snapshot,
        ).model_dump_json()
        self._latest_snapshot = message
        return message

    def _last_commit_datetime(self) -> datetime | None:
        if self._last_commit_at is None:
            return None
        return datetime.fromtimestamp(self._last_commit_at, UTC)

    def _pipeline_stats(self) -> PipelineStats:
        now = time.monotonic()
        total = self._writer.stats.events_inserted
        samples = self._rate_samples
        samples.append((now, total))
        while len(samples) > 2 and now - samples[1][0] >= _RATE_WINDOW_S:
            samples.popleft()
        first_time, first_total = samples[0]
        elapsed = now - first_time
        return PipelineStats(
            events_per_second=round((total - first_total) / elapsed, 1) if elapsed > 0 else 0.0,
            last_event_occurred_at=self._last_event_occurred_at,
            last_commit_at=self._last_commit_datetime(),
            connected_clients=len(self._clients),
        )

    # --- client side -----------------------------------------------------------------------

    async def serve(self, websocket: WebSocket) -> None:
        await websocket.accept()
        if len(self._clients) >= self._settings.ws_max_clients:
            self.rejected_clients += 1
            await websocket.close(code=CLOSE_TRY_AGAIN_LATER, reason="server busy, retry later")
            return

        # The first message is always a complete snapshot. Reuse the one built by the last tick
        # (at most one tick old) so a reconnect storm does not become a database query storm.
        first = self._latest_snapshot or await self._snapshot_message()
        await websocket.send_text(first)
        # Then whatever is currently firing: a client that connects mid-incident must see it.
        for alert in self._alerts.active_alerts:
            await websocket.send_text(self._alert_message(alert))

        client = ClientConnection(
            websocket,
            send_timeout_s=self._settings.ws_send_timeout_s,
            on_closed=self._clients.discard,
        )
        self._clients.add(client)
        client.start()
        try:
            # Clients do not need to send anything; reading is how a disconnect is noticed.
            while not client.closed:
                message = await websocket.receive()
                if message["type"] == "websocket.disconnect":
                    break
        except Exception as exc:
            log.debug("WebSocket receive ended: %r", exc)
        finally:
            await client.close()
