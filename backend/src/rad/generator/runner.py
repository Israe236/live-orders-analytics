"""Runs the simulator in real time and ships its events to the ingestion API.

Producer-side resilience:
* waits for the API to be healthy before starting;
* retries 429 / 5xx / network errors with exponential backoff and full jitter, honouring
  ``Retry-After``; retries are safe because the API deduplicates on ``event_id``;
* bounds its own buffer: if the API stays unavailable for long, the newest batches are dropped
  (and counted) instead of growing memory forever.
"""

import asyncio
import contextlib
import logging
import random
import time
from typing import Any, Protocol

import httpx
from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from rad.generator.simulator import Anomaly, EventPayload, OrderStreamSimulator, SimulatorConfig

log = logging.getLogger("rad.generator")


class GeneratorSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="RAD_GEN_", extra="ignore")

    api_url: str = "http://localhost:8000"
    events_per_second: float = 20.0
    daily_curve: bool = True
    time_compression: float = 1.0
    invalid_ratio: float = 0.01
    bursts_per_hour: float = 4.0
    burst_multiplier: float = 4.0
    burst_duration_s: float = 30.0
    anomalies_per_hour: float = 1.0
    anomaly_duration_s: float = 120.0
    seed: int | None = None
    # Demo helper: trigger this anomaly once, this many seconds after start (for screenshots or
    # to watch an alert fire without waiting for a random anomaly).
    force_anomaly: Anomaly | None = None
    force_anomaly_after_s: float = 60.0
    # Seed the dashboards with this many minutes of simulated history at startup, only when the
    # last hour has no orders (so restarting the generator never duplicates history).
    backfill_minutes: float = 0.0

    tick_interval_s: float = 0.25
    batch_size: int = 1_000
    senders: int = 2
    max_buffered_events: int = 200_000
    stats_interval_s: float = 10.0

    @field_validator("force_anomaly", mode="before")
    @classmethod
    def _empty_means_none(cls, value: object) -> object:
        # docker compose passes an unset variable as an empty string.
        return None if value == "" else value

    def simulator_config(self) -> SimulatorConfig:
        return SimulatorConfig(
            base_events_per_second=self.events_per_second,
            daily_curve=self.daily_curve,
            time_compression=self.time_compression,
            invalid_ratio=self.invalid_ratio,
            bursts_per_hour=self.bursts_per_hour,
            burst_multiplier=self.burst_multiplier,
            burst_duration_s=self.burst_duration_s,
            anomalies_per_hour=self.anomalies_per_hour,
            anomaly_duration_s=self.anomaly_duration_s,
            seed=self.seed,
        )


def backoff_delay(
    attempt: int, *, rng: random.Random, base_s: float = 0.25, cap_s: float = 10.0
) -> float:
    """Exponential backoff with *full jitter*: a random delay in [0, min(cap, base * 2^attempt)].

    The randomness spreads retries out, so many producers that failed at the same moment do
    not all retry at the same moment again.
    """
    return rng.uniform(0, min(cap_s, base_s * 2**attempt))


class Shipper:
    """Bounded queue of batches drained by a few concurrent HTTP senders."""

    def __init__(self, client: httpx.AsyncClient, settings: GeneratorSettings) -> None:
        self._client = client
        self._settings = settings
        self._rng = random.Random()
        self._queue: asyncio.Queue[list[EventPayload]] = asyncio.Queue()
        self.buffered = 0
        self.inserted = 0
        self.duplicates = 0
        self.rejected = 0
        self.dropped = 0
        self.retries = 0

    def enqueue(self, events: list[EventPayload]) -> None:
        size = self._settings.batch_size
        for i in range(0, len(events), size):
            batch = events[i : i + size]
            if self.buffered + len(batch) > self._settings.max_buffered_events:
                self.dropped += len(batch)
                continue
            self.buffered += len(batch)
            self._queue.put_nowait(batch)

    async def run_sender(self) -> None:
        while True:
            batch = await self._queue.get()
            try:
                await self._send_with_retries(batch)
            finally:
                self.buffered -= len(batch)

    async def _send_with_retries(self, batch: list[EventPayload]) -> None:
        attempt = 0
        while True:
            retry_after = 0.0
            try:
                response = await self._client.post("/events/batch", json=batch)
            except httpx.HTTPError as exc:
                log.warning("send failed (%s); retrying", type(exc).__name__)
            else:
                if response.status_code == 200:
                    body: dict[str, Any] = response.json()
                    self.inserted += body["inserted"]
                    self.duplicates += body["duplicates"]
                    self.rejected += body["rejected"]
                    return
                if response.status_code not in (429, 502, 503, 504):
                    # Not retryable (e.g. 413): retrying the same bytes cannot succeed.
                    log.error(
                        "batch refused with %s: %s", response.status_code, response.text[:200]
                    )
                    self.rejected += len(batch)
                    return
                with contextlib.suppress(ValueError):
                    retry_after = float(response.headers.get("retry-after", "0"))
            self.retries += 1
            await asyncio.sleep(max(retry_after, backoff_delay(attempt, rng=self._rng)))
            attempt += 1


async def wait_for_api(client: httpx.AsyncClient) -> None:
    attempt = 0
    rng = random.Random()
    while True:
        try:
            if (await client.get("/health")).status_code == 200:
                return
        except httpx.HTTPError:
            pass
        delay = backoff_delay(attempt, rng=rng, base_s=0.5, cap_s=5.0)
        log.info("waiting for the API at %s ...", client.base_url)
        await asyncio.sleep(delay)
        attempt += 1


class EventSink(Protocol):
    """What backfill needs from a shipper (lets tests use a fake)."""

    buffered: int

    def enqueue(self, events: list[EventPayload]) -> None: ...


async def backfill(
    simulator: OrderStreamSimulator,
    sink: EventSink,
    *,
    until: float,
    max_buffered: int,
    step_s: float = 30.0,
) -> int:
    """Replay simulated history up to ``until`` as fast as the API accepts it.

    Simulated time moves in small steps, and production waits whenever too many events are
    buffered: backpressure instead of dropping events or growing memory.
    """
    produced = 0
    t = simulator.current_time
    while t < until:
        t = min(t + step_s, until)
        events = simulator.advance(t)
        sink.enqueue(events)
        produced += len(events)
        # Short poll of a counter owned by the sender tasks; an Event would only add coupling.
        while sink.buffered > max_buffered:  # noqa: ASYNC110
            await asyncio.sleep(0.05)
    return produced


async def last_hour_has_orders(client: httpx.AsyncClient) -> bool:
    try:
        response = await client.get("/metrics/snapshot")
        response.raise_for_status()
        return int(response.json()["kpis"]["orders_placed"]) > 0
    except httpx.HTTPError, KeyError, TypeError, ValueError:
        return True  # when unsure, do not risk duplicating history


async def run(settings: GeneratorSettings) -> None:
    async with httpx.AsyncClient(base_url=settings.api_url, timeout=15) as client:
        await wait_for_api(client)
        now = time.time()
        start = now
        if settings.backfill_minutes > 0:
            if await last_hour_has_orders(client):
                log.info("recent orders found: skipping the history backfill")
            else:
                start = now - settings.backfill_minutes * 60
        simulator = OrderStreamSimulator(settings.simulator_config(), start=start)
        log.info(
            "generating ~%.1f events/s (daily curve: %s, compression x%g)",
            settings.events_per_second,
            settings.daily_curve,
            settings.time_compression,
        )
        shipper = Shipper(client, settings)
        senders = [asyncio.create_task(shipper.run_sender()) for _ in range(settings.senders)]
        last_events = 0
        forced = False
        try:
            if start < now:
                produced = await backfill(
                    simulator,
                    shipper,
                    until=time.time(),
                    max_buffered=settings.batch_size * settings.senders * 4,
                )
                log.info(
                    "backfilled %d events (%.0f minutes of history)",
                    produced,
                    settings.backfill_minutes,
                )
                last_events = simulator.stats.events
            started = time.monotonic()
            next_stats = started + settings.stats_interval_s
            while True:
                tick_started = time.monotonic()
                if (
                    settings.force_anomaly is not None
                    and not forced
                    and tick_started - started >= settings.force_anomaly_after_s
                ):
                    simulator.force_anomaly(
                        settings.force_anomaly,
                        at=time.time(),
                        duration_s=settings.anomaly_duration_s,
                    )
                    forced = True
                    log.warning(
                        "forced anomaly %s for %.0f s",
                        settings.force_anomaly.value,
                        settings.anomaly_duration_s,
                    )
                events = simulator.advance(time.time())
                if events:
                    shipper.enqueue(events)
                if tick_started >= next_stats:
                    stats = simulator.stats
                    rate = (stats.events - last_events) / settings.stats_interval_s
                    last_events = stats.events
                    next_stats = tick_started + settings.stats_interval_s
                    log.info(
                        "rate=%.1f ev/s multiplier=%.2f anomaly=%s | inserted=%d dup=%d "
                        "rejected=%d retries=%d dropped=%d buffered=%d",
                        rate,
                        simulator.traffic_multiplier(time.time()),
                        simulator.anomaly_at(time.time()),
                        shipper.inserted,
                        shipper.duplicates,
                        shipper.rejected,
                        shipper.retries,
                        shipper.dropped,
                        shipper.buffered,
                    )
                elapsed = time.monotonic() - tick_started
                await asyncio.sleep(max(0.0, settings.tick_interval_s - elapsed))
        finally:
            for task in senders:
                task.cancel()
            await asyncio.gather(*senders, return_exceptions=True)
