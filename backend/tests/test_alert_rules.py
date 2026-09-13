"""Alert rules and anti-flapping state machine (pure unit tests)."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from rad.alerts.rules import (
    AlertEngine,
    AlertStatus,
    RuleName,
    RuleResult,
    Severity,
    Thresholds,
    WindowStats,
    cancellation_rate_rule,
    dead_letter_ratio_rule,
    evaluate_rules,
    pipeline_stalled_rule,
    revenue_drop_rule,
)

NOW = datetime(2026, 9, 13, 20, 0, 30, tzinfo=UTC)
T = Thresholds()
BASE = WindowStats(
    now=NOW,
    recent_seconds=300,
    previous_seconds=300,
    placed=1_000,
    cancelled=50,
    revenue_recent=500_000,
    revenue_previous=500_000,
    accepted=3_000,
    dead_letters=30,
    last_commit_at=NOW - timedelta(seconds=1),
)


def test_healthy_traffic_breaches_nothing() -> None:
    results = evaluate_rules(BASE, T, started_at=NOW - timedelta(hours=1))
    assert [r.rule for r in results] == list(RuleName)
    assert not any(r.breached for r in results)


# --- cancellation rate ------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("placed", "cancelled", "breached"),
    [
        (1_000, 150, False),  # exactly at the threshold: not above it
        (1_000, 151, True),
        (29, 29, False),  # 100% but below the minimum volume
        (30, 5, True),  # 16.7% at the minimum volume
        (0, 0, False),
    ],
)
def test_cancellation_rate_rule(placed: int, cancelled: int, breached: bool) -> None:
    result = cancellation_rate_rule(replace(BASE, placed=placed, cancelled=cancelled), T)
    assert result.breached is breached
    assert result.severity is Severity.WARNING


# --- revenue drop -----------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("recent", "previous", "breached"),
    [
        (250_000, 500_000, False),  # exactly a 50% drop: not above the threshold
        (249_000, 500_000, True),
        (900_000, 500_000, False),  # an increase is never an alert
        (0, 20_000, False),  # previous 4,000 MAD/min is below the 5,000 baseline
    ],
)
def test_revenue_drop_rule(recent: float, previous: float, breached: bool) -> None:
    result = revenue_drop_rule(replace(BASE, revenue_recent=recent, revenue_previous=previous), T)
    assert result.breached is breached


def test_revenue_drop_compares_rates_so_a_partial_window_is_fair() -> None:
    # Only 150 s of the current window have elapsed and it earned half the previous revenue:
    # that is the same rate per second, so no drop.
    stats = replace(BASE, recent_seconds=150, revenue_recent=250_000)
    result = revenue_drop_rule(stats, T)
    assert result.value == pytest.approx(0.0)
    assert not result.breached


# --- dead letters ----------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("accepted", "dead", "breached"),
    [(950, 50, False), (949, 51, True), (50, 49, False)],  # last: under 100 events in total
)
def test_dead_letter_ratio_rule(accepted: int, dead: int, breached: bool) -> None:
    result = dead_letter_ratio_rule(replace(BASE, accepted=accepted, dead_letters=dead), T)
    assert result.breached is breached


# --- stall -----------------------------------------------------------------------------------


def test_pipeline_stalled_rule_uses_last_commit_or_start_time() -> None:
    started = NOW - timedelta(hours=1)
    quiet_29 = replace(BASE, last_commit_at=NOW - timedelta(seconds=29))
    quiet_31 = replace(BASE, last_commit_at=NOW - timedelta(seconds=31))
    assert not pipeline_stalled_rule(quiet_29, T, started_at=started).breached
    assert pipeline_stalled_rule(quiet_31, T, started_at=started).breached

    never_committed = replace(BASE, last_commit_at=None)
    assert not pipeline_stalled_rule(
        never_committed, T, started_at=NOW - timedelta(seconds=10)
    ).breached
    assert pipeline_stalled_rule(
        never_committed, T, started_at=NOW - timedelta(seconds=60)
    ).breached


# --- engine ----------------------------------------------------------------------------------


def result(breached: bool, message: str = "msg") -> list[RuleResult]:
    return [RuleResult(RuleName.CANCELLATION_RATE, Severity.WARNING, breached, 0.2, 0.15, message)]


def run(
    engine: AlertEngine, pattern: list[bool], start: datetime = NOW
) -> list[tuple[int, AlertStatus]]:
    """Feed one result per second; return (second, status) for each transition."""
    transitions = []
    for second, breached in enumerate(pattern):
        for alert in engine.update(result(breached), start + timedelta(seconds=second)):
            transitions.append((second, alert.status))
    return transitions


def test_fires_only_after_a_continuous_breach() -> None:
    assert run(AlertEngine(), [True] * 10) == []  # seconds 0..9: breached for 9 s
    assert run(AlertEngine(), [True] * 11) == [(10, AlertStatus.FIRING)]


def test_a_firing_alert_is_reported_once() -> None:
    engine = AlertEngine()
    assert run(engine, [True] * 60) == [(10, AlertStatus.FIRING)]
    assert len(engine.active_alerts) == 1


def test_flapping_rule_never_fires() -> None:
    pattern = ([True] * 8 + [False]) * 10
    assert run(AlertEngine(), pattern) == []


def test_resolves_only_after_being_healthy_long_enough() -> None:
    engine = AlertEngine()
    pattern = [True] * 11 + [False] * 30
    assert run(engine, pattern) == [(10, AlertStatus.FIRING)]  # healthy for 29 s only
    engine = AlertEngine()
    pattern = [True] * 11 + [False] * 31
    assert run(engine, pattern) == [(10, AlertStatus.FIRING), (41, AlertStatus.RESOLVED)]
    assert engine.active_alerts == []


def test_a_short_relapse_restarts_the_resolve_timer() -> None:
    pattern = [True] * 11 + [False] * 20 + [True] + [False] * 30
    assert run(AlertEngine(), pattern) == [(10, AlertStatus.FIRING)]


def test_refiring_after_resolution_is_a_new_alert() -> None:
    engine = AlertEngine()
    alerts = []
    pattern = [True] * 11 + [False] * 31 + [True] * 11
    for second, breached in enumerate(pattern):
        alerts += engine.update(result(breached), NOW + timedelta(seconds=second))
    firing = [a for a in alerts if a.status is AlertStatus.FIRING]
    assert len(firing) == 2
    assert firing[0].id != firing[1].id


def test_resolved_alert_keeps_its_identity_and_times() -> None:
    engine = AlertEngine(fire_after=timedelta(0), resolve_after=timedelta(0))
    fired = engine.update(result(True, "first"), NOW)
    engine.update(result(True, "updated numbers"), NOW + timedelta(seconds=5))
    resolved = engine.update(result(False), NOW + timedelta(seconds=9))
    assert fired[0].status is AlertStatus.FIRING
    assert resolved[0].id == fired[0].id
    assert resolved[0].message == "updated numbers"
    assert (resolved[0].started_at, resolved[0].resolved_at) == (NOW, NOW + timedelta(seconds=9))
