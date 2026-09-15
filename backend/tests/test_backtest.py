"""The alert backtest harness: its numbers drive the default thresholds, so it is tested too."""

from datetime import UTC, datetime
from uuid import uuid4

from rad.alerts.backtest import Incident, evaluate, score, simulate
from rad.alerts.rules import Alert, AlertStatus, RuleName, Severity, Thresholds
from rad.generator.simulator import Anomaly, SimulatorConfig

START = datetime(2026, 9, 1, 12, 0, tzinfo=UTC).timestamp()
STEADY = SimulatorConfig(
    base_events_per_second=50,
    daily_curve=False,
    bursts_per_hour=0,
    anomalies_per_hour=0,
    invalid_ratio=0,
    seed=11,
)
NO_FALSE_ALERTS = {"cancellation_rate": 0, "revenue_drop": 0, "orders_drop": 0}


def firing(rule: RuleName) -> Alert:
    return Alert(
        id=uuid4(),
        rule=rule,
        severity=Severity.WARNING,
        status=AlertStatus.FIRING,
        message="",
        value=1.0,
        threshold=0.5,
        started_at=datetime.now(UTC),
    )


def test_score_separates_false_alerts_from_detected_incidents() -> None:
    incidents = [
        Incident(Anomaly.PAYMENT_OUTAGE, start_s=1_000, duration_s=240),
        Incident(Anomaly.TRAFFIC_DROP, start_s=5_000, duration_s=240),
    ]
    transitions = [
        (500, firing(RuleName.CANCELLATION_RATE)),  # before any incident: false
        (1_100, firing(RuleName.CANCELLATION_RATE)),  # during the outage: detection after 100 s
        (1_150, firing(RuleName.REVENUE_DROP)),  # during the outage, other rule: not false
        (5_090, firing(RuleName.ORDERS_DROP)),  # traffic drop detected by the orders rule
        (5_130, firing(RuleName.REVENUE_DROP)),  # ...and later by the revenue rule
        (9_000, firing(RuleName.REVENUE_DROP)),  # long after: false
    ]
    result = score(transitions, incidents, attribution_s=180)
    assert result.false_alerts == {"cancellation_rate": 1, "revenue_drop": 1, "orders_drop": 0}
    assert [(d.kind, d.detected, d.delay_s) for d in result.detections] == [
        ("payment_outage", True, 100),
        ("traffic_drop", True, 90),
    ]
    assert result.detections[1].delay_by_rule == {"orders_drop": 90, "revenue_drop": 130}


def test_an_injected_payment_outage_is_detected_on_steady_traffic() -> None:
    incident = Incident(Anomaly.PAYMENT_OUTAGE, start_s=1_500, duration_s=300)
    traffic = simulate(STEADY, start=START, seconds=2_400, incidents=[incident])

    transitions = evaluate(traffic, Thresholds(), fire_after_s=10, resolve_after_s=30)
    result = score(transitions, [incident], attribution_s=180)

    assert result.detections[0].detected
    assert result.detections[0].delay_s is not None and result.detections[0].delay_s < 180
    assert result.false_alerts == NO_FALSE_ALERTS


def test_an_injected_traffic_drop_is_detected_by_the_orders_rule() -> None:
    incident = Incident(Anomaly.TRAFFIC_DROP, start_s=1_500, duration_s=300)
    traffic = simulate(STEADY, start=START, seconds=2_400, incidents=[incident])

    transitions = evaluate(traffic, Thresholds(), fire_after_s=10, resolve_after_s=30)
    result = score(transitions, [incident], attribution_s=180)

    assert "orders_drop" in result.detections[0].delay_by_rule
    assert result.false_alerts == NO_FALSE_ALERTS
