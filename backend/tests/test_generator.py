"""Unit tests for the synthetic event generator (no database, no network)."""

import random
from collections import defaultdict
from dataclasses import replace
from datetime import UTC, datetime
from uuid import UUID

import pytest
from pydantic import ValidationError

from rad.common.events import EventType, OrderEvent
from rad.generator.runner import GeneratorSettings, backoff_delay
from rad.generator.simulator import (
    Anomaly,
    EventPayload,
    OrderStreamSimulator,
    SimulatorConfig,
    daily_multiplier,
    uuid7_from,
)

START = datetime(2026, 9, 12, 10, 0, tzinfo=UTC).timestamp()
FLAT = SimulatorConfig(
    base_events_per_second=50,
    daily_curve=False,
    invalid_ratio=0.0,
    bursts_per_hour=0,
    anomalies_per_hour=0,
    seed=7,
)
WARM_UP_S = 200  # follow-up events need a couple of minutes to reach a steady state


def drive(sim: OrderStreamSimulator, seconds: float, tick: float = 0.25) -> list[EventPayload]:
    out: list[EventPayload] = []
    for step in range(1, int(seconds / tick) + 1):
        out += sim.advance(START + step * tick)
    return out


def steady_rate(payloads: list[EventPayload], seconds: float) -> float:
    times = [datetime.fromisoformat(p["occurred_at"]).timestamp() for p in payloads]
    return sum(1 for t in times if t >= START + WARM_UP_S) / (seconds - WARM_UP_S)


def test_events_are_valid_and_lifecycles_are_consistent() -> None:
    payloads = drive(OrderStreamSimulator(FLAT, start=START), 600)
    events = [OrderEvent.model_validate(p) for p in payloads]  # raises if any is invalid

    allowed_next: dict[EventType | None, set[EventType]] = {
        None: {EventType.ORDER_PLACED},
        EventType.ORDER_PLACED: {EventType.ORDER_PAID, EventType.ORDER_CANCELLED},
        EventType.ORDER_PAID: {EventType.ORDER_SHIPPED, EventType.ORDER_CANCELLED},
        EventType.ORDER_SHIPPED: set(),
        EventType.ORDER_CANCELLED: set(),
    }
    by_order: dict[UUID, list[OrderEvent]] = defaultdict(list)
    for event in events:
        by_order[event.order_id].append(event)
    assert len(by_order) > 1_000
    for order_events in by_order.values():
        order_events.sort(key=lambda e: e.occurred_at)
        previous: EventType | None = None
        for event in order_events:
            assert event.event_type in allowed_next[previous]
            previous = event.event_type
        # Every event of an order carries the same order attributes.
        assert (
            len({(e.amount_mad, e.category, e.city, e.payment_method) for e in order_events}) == 1
        )


def test_steady_rate_matches_configuration() -> None:
    payloads = drive(OrderStreamSimulator(FLAT, start=START), 600)
    assert steady_rate(payloads, 600) == pytest.approx(50, rel=0.1)


def test_burst_multiplies_and_traffic_drop_divides_the_rate() -> None:
    bursting = OrderStreamSimulator(FLAT, start=START)
    bursting.force_burst(at=START, duration_s=10_000)
    dropping = OrderStreamSimulator(FLAT, start=START)
    dropping.force_anomaly(Anomaly.TRAFFIC_DROP, at=START, duration_s=10_000)

    assert steady_rate(drive(bursting, 400), 400) == pytest.approx(200, rel=0.1)
    assert steady_rate(drive(dropping, 600), 600) == pytest.approx(12.5, rel=0.15)


def test_daily_curve_is_quiet_at_night_and_peaks_in_the_evening() -> None:
    assert daily_multiplier(4.0) < 0.5
    assert daily_multiplier(21.0) > 1.5
    assert daily_multiplier(12.5) > daily_multiplier(9.0)
    mean = sum(daily_multiplier(minute / 60) for minute in range(1_440)) / 1_440
    assert mean == pytest.approx(1.0, abs=1e-9)


def test_time_compression_moves_the_simulated_clock_faster() -> None:
    sim = OrderStreamSimulator(replace(FLAT, time_compression=1_440), start=START)
    assert sim.simulated_local_time(START + 60).date() > sim.simulated_local_time(START).date()


def test_payment_outage_turns_card_payments_into_cancellations() -> None:
    sim = OrderStreamSimulator(FLAT, start=START)
    sim.force_anomaly(Anomaly.PAYMENT_OUTAGE, at=START, duration_s=10_000)
    payloads = drive(sim, 300)

    def count(method: str, event_type: str) -> int:
        return sum(
            1 for p in payloads if p["payment_method"] == method and p["event_type"] == event_type
        )

    assert count("card", "order_paid") == 0
    assert count("card", "order_cancelled") > 100
    assert count("cash_on_delivery", "order_paid") > 100


def test_invalid_events_are_really_invalid_and_match_the_ratio() -> None:
    payloads = drive(OrderStreamSimulator(replace(FLAT, invalid_ratio=0.05), start=START), 300)
    invalid = 0
    for payload in payloads:
        try:
            OrderEvent.model_validate(payload)
        except ValidationError:
            invalid += 1
    assert invalid / (len(payloads) - invalid) == pytest.approx(0.05, rel=0.2)


def test_same_seed_gives_the_same_stream() -> None:
    first = drive(OrderStreamSimulator(FLAT, start=START), 60)
    second = drive(OrderStreamSimulator(FLAT, start=START), 60)
    assert first == second


def test_event_ids_are_uuid7_with_the_event_timestamp() -> None:
    assert uuid7_from(START, random.Random(1)).version == 7
    for payload in drive(OrderStreamSimulator(FLAT, start=START), 5):
        millis = UUID(payload["event_id"]).int >> 80
        occurred = datetime.fromisoformat(payload["occurred_at"]).timestamp()
        assert millis == int(occurred * 1000)


def test_backoff_uses_full_jitter_below_an_exponential_cap() -> None:
    rng = random.Random(0)
    for attempt in range(12):
        ceiling = min(10.0, 0.25 * 2**attempt)
        delays = [backoff_delay(attempt, rng=rng) for _ in range(200)]
        assert all(0 <= d <= ceiling for d in delays)
        assert max(delays) > ceiling * 0.8  # really spread over the whole range


def test_forced_anomaly_setting_accepts_empty_value_from_compose() -> None:
    # model_validate takes raw values exactly as they come from the environment.
    assert GeneratorSettings.model_validate({"force_anomaly": ""}).force_anomaly is None
    forced = GeneratorSettings.model_validate({"force_anomaly": "payment_outage"})
    assert forced.force_anomaly is Anomaly.PAYMENT_OUTAGE
