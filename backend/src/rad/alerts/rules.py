"""Alert rules and the state machine that turns rule results into alerts.

Everything here is pure: rules take plain numbers, the engine takes rule results and a time.
No database, no clock, no network — every threshold and transition is unit-tested directly.
"""

from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from enum import StrEnum
from uuid import UUID, uuid4


class Severity(StrEnum):
    WARNING = "warning"
    CRITICAL = "critical"


class RuleName(StrEnum):
    CANCELLATION_RATE = "cancellation_rate"
    REVENUE_DROP = "revenue_drop"
    ORDERS_DROP = "orders_drop"
    DEAD_LETTER_RATIO = "dead_letter_ratio"
    PIPELINE_STALLED = "pipeline_stalled"


@dataclass(frozen=True, slots=True)
class Thresholds:
    # Short enough to catch a few-minute incident, long enough to average out noise
    # (see docs/DECISIONS.md, "Choosing the window").
    window_minutes: int = 3
    # Cancellations / orders placed in the window. 15% raised ~40 false alerts per simulated
    # day; 20% was chosen with the backtest (python -m rad.alerts.backtest, docs/DECISIONS.md).
    cancellation_rate: float = 0.20
    cancellation_min_orders: int = 30
    # Fire when the recent revenue rate is more than this fraction below the previous window.
    revenue_drop_ratio: float = 0.6
    revenue_min_baseline_mad_per_min: float = 5_000.0
    # Same comparison on the number of orders placed: counts are not swung by a few expensive
    # orders, so some night-time drops that revenue misses become visible. Backtest: 50% added
    # ~6 false alerts/day (flash sales inflate the previous window), 60% added 0.2.
    orders_drop_ratio: float = 0.6
    orders_min_baseline_per_min: float = 10.0
    # Dead letters / (accepted + dead letters) in the window.
    dead_letter_ratio: float = 0.05
    dead_letter_min_events: int = 100
    # No event committed for this long.
    stall_after_s: float = 30.0


@dataclass(frozen=True, slots=True)
class WindowStats:
    """Totals for the current window and the window just before it, read from aggregates.

    The current window ends *now*, so its last minute is only partly filled; ``recent_seconds``
    is its real elapsed length and rates are computed per second to stay comparable.
    """

    now: datetime
    recent_seconds: float
    previous_seconds: float
    placed: int
    placed_previous: int
    cancelled: int
    revenue_recent: float
    revenue_previous: float
    accepted: int
    dead_letters: int
    last_commit_at: datetime | None


@dataclass(frozen=True, slots=True)
class RuleResult:
    rule: RuleName
    severity: Severity
    breached: bool
    value: float | None
    threshold: float
    message: str


def cancellation_rate_rule(stats: WindowStats, t: Thresholds) -> RuleResult:
    def result(breached: bool, value: float | None, message: str) -> RuleResult:
        return RuleResult(
            RuleName.CANCELLATION_RATE,
            Severity.WARNING,
            breached,
            value,
            t.cancellation_rate,
            message,
        )

    if stats.placed < t.cancellation_min_orders:
        # With 4 orders, 1 cancellation is 25%: too few orders to mean anything.
        return result(False, None, f"only {stats.placed} orders in the window")
    rate = stats.cancelled / stats.placed
    return result(
        rate > t.cancellation_rate,
        rate,
        f"Cancellation rate {rate:.1%} over the last {t.window_minutes} min "
        f"(threshold {t.cancellation_rate:.0%})",
    )


def _drop(
    recent: float, previous: float, recent_seconds: float, previous_seconds: float
) -> tuple[float, float] | None:
    """Per-minute rates of two windows of different elapsed lengths; None if not comparable."""
    if recent_seconds <= 0 or previous_seconds <= 0:
        return None
    return recent / recent_seconds * 60, previous / previous_seconds * 60


def revenue_drop_rule(stats: WindowStats, t: Thresholds) -> RuleResult:
    def result(breached: bool, value: float | None, message: str) -> RuleResult:
        return RuleResult(
            RuleName.REVENUE_DROP, Severity.CRITICAL, breached, value, t.revenue_drop_ratio, message
        )

    rates = _drop(
        stats.revenue_recent, stats.revenue_previous, stats.recent_seconds, stats.previous_seconds
    )
    if rates is None:
        return result(False, None, "windows not available yet")
    recent_per_min, previous_per_min = rates
    if previous_per_min < t.revenue_min_baseline_mad_per_min:
        return result(False, None, f"baseline too small ({previous_per_min:,.0f} MAD/min)")
    drop = 1 - recent_per_min / previous_per_min
    return result(
        drop > t.revenue_drop_ratio,
        drop,
        f"Revenue down {drop:.0%} vs the previous {t.window_minutes} min "
        f"({recent_per_min:,.0f} vs {previous_per_min:,.0f} MAD/min)",
    )


def orders_drop_rule(stats: WindowStats, t: Thresholds) -> RuleResult:
    def result(breached: bool, value: float | None, message: str) -> RuleResult:
        return RuleResult(
            RuleName.ORDERS_DROP, Severity.CRITICAL, breached, value, t.orders_drop_ratio, message
        )

    rates = _drop(stats.placed, stats.placed_previous, stats.recent_seconds, stats.previous_seconds)
    if rates is None:
        return result(False, None, "windows not available yet")
    recent_per_min, previous_per_min = rates
    if previous_per_min < t.orders_min_baseline_per_min:
        return result(False, None, f"baseline too small ({previous_per_min:,.0f} orders/min)")
    drop = 1 - recent_per_min / previous_per_min
    return result(
        drop > t.orders_drop_ratio,
        drop,
        f"Orders down {drop:.0%} vs the previous {t.window_minutes} min "
        f"({recent_per_min:,.0f} vs {previous_per_min:,.0f} orders/min)",
    )


def dead_letter_ratio_rule(stats: WindowStats, t: Thresholds) -> RuleResult:
    def result(breached: bool, value: float | None, message: str) -> RuleResult:
        return RuleResult(
            RuleName.DEAD_LETTER_RATIO,
            Severity.WARNING,
            breached,
            value,
            t.dead_letter_ratio,
            message,
        )

    total = stats.accepted + stats.dead_letters
    if total < t.dead_letter_min_events:
        return result(False, None, f"only {total} events in the window")
    ratio = stats.dead_letters / total
    return result(
        ratio > t.dead_letter_ratio,
        ratio,
        f"{ratio:.1%} of incoming events rejected over the last {t.window_minutes} min "
        f"(threshold {t.dead_letter_ratio:.0%})",
    )


def pipeline_stalled_rule(stats: WindowStats, t: Thresholds, *, started_at: datetime) -> RuleResult:
    # Before the first commit, measure silence from process start.
    reference = stats.last_commit_at or started_at
    silent_s = max((stats.now - reference).total_seconds(), 0.0)
    return RuleResult(
        RuleName.PIPELINE_STALLED,
        Severity.CRITICAL,
        silent_s > t.stall_after_s,
        silent_s,
        t.stall_after_s,
        f"No events stored for {silent_s:.0f}s",
    )


def evaluate_rules(stats: WindowStats, t: Thresholds, *, started_at: datetime) -> list[RuleResult]:
    return [
        cancellation_rate_rule(stats, t),
        revenue_drop_rule(stats, t),
        orders_drop_rule(stats, t),
        dead_letter_ratio_rule(stats, t),
        pipeline_stalled_rule(stats, t, started_at=started_at),
    ]


# --- state machine --------------------------------------------------------------------------


class AlertStatus(StrEnum):
    FIRING = "firing"
    RESOLVED = "resolved"


@dataclass(frozen=True, slots=True)
class Alert:
    id: UUID
    rule: RuleName
    severity: Severity
    status: AlertStatus
    message: str
    value: float | None
    threshold: float
    started_at: datetime
    resolved_at: datetime | None = None


@dataclass(slots=True)
class _RuleTrack:
    breached_since: datetime | None = None
    healthy_since: datetime | None = None
    active: Alert | None = None


class AlertEngine:
    """Hysteresis in time, to avoid flapping alerts.

    * An alert **fires** only after its rule has been breached *continuously* for ``fire_after``.
    * A firing alert **resolves** only after its rule has been healthy *continuously* for
      ``resolve_after``.

    ``update`` returns only transitions (newly firing or newly resolved alerts), so the caller
    can push and store each change exactly once.
    """

    def __init__(
        self,
        *,
        fire_after: timedelta = timedelta(seconds=10),
        resolve_after: timedelta = timedelta(seconds=30),
    ) -> None:
        self._fire_after = fire_after
        self._resolve_after = resolve_after
        self._tracks: dict[RuleName, _RuleTrack] = {}

    @property
    def active_alerts(self) -> list[Alert]:
        return [track.active for track in self._tracks.values() if track.active is not None]

    def update(self, results: list[RuleResult], now: datetime) -> list[Alert]:
        transitions: list[Alert] = []
        for result in results:
            track = self._tracks.setdefault(result.rule, _RuleTrack())
            if result.breached:
                track.healthy_since = None
                if track.breached_since is None:
                    track.breached_since = now
                if track.active is None:
                    if now - track.breached_since >= self._fire_after:
                        track.active = Alert(
                            id=uuid4(),
                            rule=result.rule,
                            severity=result.severity,
                            status=AlertStatus.FIRING,
                            message=result.message,
                            value=result.value,
                            threshold=result.threshold,
                            started_at=now,
                        )
                        transitions.append(track.active)
                else:
                    # Still firing: keep the latest numbers, but it is not a new transition.
                    track.active = replace(track.active, message=result.message, value=result.value)
            else:
                track.breached_since = None
                if track.active is None:
                    continue
                if track.healthy_since is None:
                    track.healthy_since = now
                if now - track.healthy_since >= self._resolve_after:
                    transitions.append(
                        replace(track.active, status=AlertStatus.RESOLVED, resolved_at=now)
                    )
                    track.active = None
                    track.healthy_since = None
        return transitions
