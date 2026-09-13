"""Synthetic order-event stream: order lifecycles, a daily traffic curve, bursts and anomalies.

This module is pure logic with an injected clock and random generator. Tests can simulate
many minutes of traffic in milliseconds, and the same seed always gives the same stream.

How the rate works: new orders are placed as a Poisson process (random exponential gaps).
Every order then produces follow-up events (paid, shipped or cancelled) after short random
delays. The order rate is chosen so that *total* events per second match the target.
"""

import heapq
import math
import random
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
from enum import StrEnum
from typing import Any
from uuid import UUID

from rad.common.events import Category, City, EventType, PaymentMethod

type EventPayload = dict[str, Any]

# Morocco is UTC+1 most of the year (UTC+0 during Ramadan). A fixed offset is enough for a
# traffic curve and avoids depending on the tz database inside slim containers.
CASABLANCA_TZ = timezone(timedelta(hours=1), "UTC+01")

CITY_WEIGHTS: dict[City, float] = {
    City.CASABLANCA: 30,
    City.RABAT: 13,
    City.MARRAKECH: 11,
    City.TANGIER: 10,
    City.FES: 9,
    City.AGADIR: 7,
    City.KENITRA: 6,
    City.MEKNES: 5,
    City.TETOUAN: 5,
    City.OUJDA: 4,
}

# (weight, median amount in MAD, spread of the log-normal distribution)
CATEGORY_PROFILE: dict[Category, tuple[float, float, float]] = {
    Category.FASHION: (22, 450, 0.6),
    Category.ELECTRONICS: (16, 3_200, 0.8),
    Category.HOME: (14, 900, 0.7),
    Category.BEAUTY: (13, 260, 0.5),
    Category.GROCERY: (12, 180, 0.5),
    Category.SPORTS: (9, 700, 0.6),
    Category.TOYS: (8, 350, 0.5),
    Category.BOOKS: (6, 120, 0.4),
}

PAYMENT_WEIGHTS: dict[PaymentMethod, float] = {
    PaymentMethod.CASH_ON_DELIVERY: 45,
    PaymentMethod.CARD: 35,
    PaymentMethod.WALLET: 12,
    PaymentMethod.BANK_TRANSFER: 8,
}

# Cash-on-delivery orders are cancelled far more often before they are paid.
CANCEL_BEFORE_PAYMENT: dict[PaymentMethod, float] = {
    PaymentMethod.CASH_ON_DELIVERY: 0.12,
    PaymentMethod.CARD: 0.04,
    PaymentMethod.WALLET: 0.05,
    PaymentMethod.BANK_TRANSFER: 0.06,
}
CANCEL_AFTER_PAYMENT = 0.03

# Delays between lifecycle steps, in seconds (short, so a demo shows full lifecycles quickly).
PAY_DELAY_S = (2.0, 20.0)
SHIP_DELAY_S = (10.0, 90.0)
CANCEL_DELAY_S = (5.0, 60.0)

# placed + (paid | cancelled) + [if paid: (shipped | cancelled)]
_TOTAL_PAYMENT_WEIGHT = sum(PAYMENT_WEIGHTS.values())
EXPECTED_EVENTS_PER_ORDER = 2.0 + sum(
    weight / _TOTAL_PAYMENT_WEIGHT * (1 - CANCEL_BEFORE_PAYMENT[method])
    for method, weight in PAYMENT_WEIGHTS.items()
)


def _bump(hour: float, center: float, width: float, height: float) -> float:
    distance = abs(hour - center) % 24
    distance = min(distance, 24 - distance)  # the day wraps around midnight
    return height * math.exp(-(distance**2) / (2 * width**2))


def _raw_curve(hour: float) -> float:
    # Quiet night, a lunch bump, an after-work shoulder and a strong evening peak.
    return (
        0.12
        + _bump(hour, 12.5, 2.0, 0.55)
        + _bump(hour, 17.0, 3.0, 0.35)
        + _bump(hour, 21.0, 2.2, 1.0)
    )


_CURVE_MEAN = sum(_raw_curve(minute / 60) for minute in range(1_440)) / 1_440


def daily_multiplier(local_hour: float) -> float:
    """Relative traffic at a local hour of the day, normalised so the daily mean is 1."""
    return _raw_curve(local_hour) / _CURVE_MEAN


def uuid7_from(timestamp_s: float, rng: random.Random) -> UUID:
    """A UUIDv7 (millisecond timestamp prefix) whose random part comes from ``rng``.

    ``uuid.uuid7()`` would use the system clock and OS randomness; this keeps the stream
    reproducible while giving the database time-ordered primary keys.
    """
    millis = int(timestamp_s * 1000) & ((1 << 48) - 1)
    value = (
        (millis << 80)
        | (0x7 << 76)
        | (rng.getrandbits(12) << 64)
        | (0b10 << 62)
        | rng.getrandbits(62)
    )
    return UUID(int=value)


class Anomaly(StrEnum):
    PAYMENT_OUTAGE = "payment_outage"  # card payments fail: those orders get cancelled
    TRAFFIC_DROP = "traffic_drop"  # traffic falls to a quarter: revenue drops


@dataclass(frozen=True, slots=True)
class SimulatorConfig:
    base_events_per_second: float = 20.0
    daily_curve: bool = True
    # 1.0 = real time. 1440 = a simulated day per real minute (to watch the curve in a demo).
    time_compression: float = 1.0
    # Roughly this fraction of extra, deliberately broken events (to exercise dead letters).
    invalid_ratio: float = 0.01
    bursts_per_hour: float = 4.0
    burst_multiplier: float = 4.0
    burst_duration_s: float = 30.0
    anomalies_per_hour: float = 1.0
    anomaly_duration_s: float = 120.0
    seed: int | None = None


@dataclass(slots=True)
class SimulatorStats:
    events: int = 0
    invalid_events: int = 0
    orders: int = 0
    bursts: int = 0
    anomalies: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class _Order:
    order_id: UUID
    amount_mad: Decimal
    category: Category
    city: City
    payment: PaymentMethod


@dataclass(order=True, slots=True)
class _Due:
    at: float
    seq: int
    order: _Order = field(compare=False)
    event_type: EventType = field(compare=False)


class OrderStreamSimulator:
    def __init__(self, config: SimulatorConfig, *, start: float) -> None:
        self.config = config
        self.stats = SimulatorStats()
        self._rng = random.Random(config.seed)
        self._start = start
        self._now = start
        self._pending: list[_Due] = []
        self._seq = 0
        self._burst_until = -math.inf
        self._anomaly: Anomaly | None = None
        self._anomaly_until = -math.inf
        self._next_placement = start + self._placement_gap(start)

    # --- traffic shape ---------------------------------------------------------------------

    def simulated_local_time(self, t: float) -> datetime:
        elapsed = (t - self._start) * self.config.time_compression
        return datetime.fromtimestamp(self._start + elapsed, CASABLANCA_TZ)

    def traffic_multiplier(self, t: float) -> float:
        multiplier = 1.0
        if self.config.daily_curve:
            local = self.simulated_local_time(t)
            multiplier *= daily_multiplier(local.hour + local.minute / 60 + local.second / 3600)
        if t < self._burst_until:
            multiplier *= self.config.burst_multiplier
        if self.anomaly_at(t) is Anomaly.TRAFFIC_DROP:
            multiplier *= 0.25
        return multiplier

    def anomaly_at(self, t: float) -> Anomaly | None:
        return self._anomaly if t < self._anomaly_until else None

    def force_burst(self, *, at: float, duration_s: float) -> None:
        self._burst_until = at + duration_s
        self.stats.bursts += 1

    def force_anomaly(self, kind: Anomaly, *, at: float, duration_s: float) -> None:
        self._anomaly = kind
        self._anomaly_until = at + duration_s
        self.stats.anomalies[kind.value] = self.stats.anomalies.get(kind.value, 0) + 1

    def _placement_gap(self, t: float) -> float:
        orders_per_second = (
            self.config.base_events_per_second
            * self.traffic_multiplier(t)
            / EXPECTED_EVENTS_PER_ORDER
        )
        return self._rng.expovariate(max(orders_per_second, 1e-9))

    def _maybe_start_effects(self, now: float) -> None:
        dt = max(now - self._now, 0.0)
        # Probability that a Poisson event with this hourly rate happened during dt.
        if now >= self._burst_until and self._happens(self.config.bursts_per_hour, dt):
            self.force_burst(at=now, duration_s=self.config.burst_duration_s)
        if now >= self._anomaly_until and self._happens(self.config.anomalies_per_hour, dt):
            kind = self._rng.choice(list(Anomaly))
            self.force_anomaly(kind, at=now, duration_s=self.config.anomaly_duration_s)

    def _happens(self, per_hour: float, dt: float) -> bool:
        return per_hour > 0 and self._rng.random() < 1 - math.exp(-per_hour / 3600 * dt)

    # --- stream ----------------------------------------------------------------------------

    def advance(self, now: float) -> list[EventPayload]:
        """Return the wire payloads of everything that happened since the previous call."""
        self._maybe_start_effects(now)
        out: list[EventPayload] = []
        while True:
            next_due = self._pending[0].at if self._pending else math.inf
            if self._next_placement <= now and self._next_placement <= next_due:
                t = self._next_placement
                self._place_order(t, out)
                self._next_placement = t + self._placement_gap(t)
            elif next_due <= now:
                self._transition(heapq.heappop(self._pending), out)
            else:
                break
        self._now = now
        return out

    def _place_order(self, t: float, out: list[EventPayload]) -> None:
        rng = self._rng
        category = rng.choices(list(CATEGORY_PROFILE), [p[0] for p in CATEGORY_PROFILE.values()])[0]
        _, median, spread = CATEGORY_PROFILE[category]
        amount = min(max(median * rng.lognormvariate(0, spread), 20.0), 999_999.0)
        order = _Order(
            order_id=uuid7_from(t, rng),
            amount_mad=Decimal(f"{amount:.2f}"),
            category=category,
            city=rng.choices(list(CITY_WEIGHTS), list(CITY_WEIGHTS.values()))[0],
            payment=rng.choices(list(PAYMENT_WEIGHTS), list(PAYMENT_WEIGHTS.values()))[0],
        )
        self.stats.orders += 1
        self._emit(order, EventType.ORDER_PLACED, t, out)
        if rng.random() < CANCEL_BEFORE_PAYMENT[order.payment]:
            self._schedule(t + rng.uniform(*CANCEL_DELAY_S), order, EventType.ORDER_CANCELLED)
        else:
            self._schedule(t + rng.uniform(*PAY_DELAY_S), order, EventType.ORDER_PAID)

    def _transition(self, due: _Due, out: list[EventPayload]) -> None:
        if due.event_type is not EventType.ORDER_PAID:
            self._emit(due.order, due.event_type, due.at, out)
            return
        if (
            self.anomaly_at(due.at) is Anomaly.PAYMENT_OUTAGE
            and due.order.payment is PaymentMethod.CARD
        ):
            # The card payment failed: the order is cancelled instead of paid.
            self._emit(due.order, EventType.ORDER_CANCELLED, due.at, out)
            return
        self._emit(due.order, EventType.ORDER_PAID, due.at, out)
        if self._rng.random() < CANCEL_AFTER_PAYMENT:
            next_step = EventType.ORDER_CANCELLED
            delay = self._rng.uniform(*CANCEL_DELAY_S)
        else:
            next_step = EventType.ORDER_SHIPPED
            delay = self._rng.uniform(*SHIP_DELAY_S)
        self._schedule(due.at + delay, due.order, next_step)

    def _schedule(self, at: float, order: _Order, event_type: EventType) -> None:
        self._seq += 1  # tie-breaker so the heap never compares orders
        heapq.heappush(self._pending, _Due(at, self._seq, order, event_type))

    def _emit(
        self, order: _Order, event_type: EventType, t: float, out: list[EventPayload]
    ) -> None:
        payload: EventPayload = {
            "event_id": str(uuid7_from(t, self._rng)),
            "order_id": str(order.order_id),
            "event_type": event_type.value,
            "occurred_at": datetime.fromtimestamp(t, UTC).isoformat(),
            "amount_mad": str(order.amount_mad),
            "category": order.category.value,
            "city": order.city.value,
            "payment_method": order.payment.value,
        }
        out.append(payload)
        self.stats.events += 1
        if self.config.invalid_ratio > 0 and self._rng.random() < self.config.invalid_ratio:
            out.append(self._corrupt(payload, t))
            self.stats.invalid_events += 1

    def _corrupt(self, payload: EventPayload, t: float) -> EventPayload:
        """A broken copy of a real event, with its own id so it never collides with it."""
        bad = dict(payload, event_id=str(uuid7_from(t, self._rng)))
        match self._rng.randrange(6):
            case 0:
                bad["city"] = "Paris"
            case 1:
                bad["amount_mad"] = "-" + str(bad["amount_mad"])
            case 2:
                del bad["order_id"]
            case 3:
                bad["occurred_at"] = "not-a-date"
            case 4:
                bad["event_type"] = "order_refunded"
            case _:
                bad["amount_mad"] = True
        return bad
