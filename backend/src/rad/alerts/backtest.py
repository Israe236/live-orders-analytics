"""Backtest the alert rules on simulated traffic with known incidents.

Two questions, answered with numbers instead of intuition:

* **Noise:** how many alerts do the rules raise on normal traffic (daily curve and flash-sale
  bursts included)?
* **Detection:** are injected incidents (payment outage, traffic drop) detected, and how fast?

The generator's simulator is replayed offline (no database, no network), so the ground truth is
known: every incident is injected at a chosen time. Events are summed per second of event time,
and the *production* rule functions and ``AlertEngine`` are evaluated once per simulated second
with the same windows as the live hub: aligned on minute buckets, current minute partly filled.

    python -m rad.alerts.backtest --days 2 --seeds 2
"""

import argparse
import json
import math
import sys
import time
from collections.abc import Iterable, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from itertools import accumulate
from statistics import median
from typing import Any

from rad.alerts.rules import (
    Alert,
    AlertEngine,
    AlertStatus,
    RuleName,
    RuleResult,
    Thresholds,
    WindowStats,
    cancellation_rate_rule,
    revenue_drop_rule,
)
from rad.common.events import EventType
from rad.generator.simulator import CASABLANCA_TZ, Anomaly, OrderStreamSimulator, SimulatorConfig

# The rule each kind of incident is expected to trigger.
EXPECTED_RULE = {
    Anomaly.PAYMENT_OUTAGE: RuleName.CANCELLATION_RATE,
    Anomaly.TRAFFIC_DROP: RuleName.REVENUE_DROP,
}
EVALUATED_RULES = (RuleName.CANCELLATION_RATE, RuleName.REVENUE_DROP)


@dataclass(frozen=True, slots=True)
class Incident:
    kind: Anomaly
    start_s: int
    duration_s: int


@dataclass(frozen=True, slots=True)
class Traffic:
    """Per-second prefix sums over event time: element ``i`` is the total over seconds [0, i)."""

    start: float
    placed: list[int]
    cancelled: list[int]
    revenue: list[float]

    @property
    def seconds(self) -> int:
        return len(self.placed) - 1


def simulate(
    config: SimulatorConfig, *, start: float, seconds: int, incidents: Iterable[Incident]
) -> Traffic:
    if start % 60 != 0:
        raise ValueError("start must be on a minute boundary, like the live minute buckets")
    by_start = {incident.start_s: incident for incident in incidents}
    placed = [0] * seconds
    cancelled = [0] * seconds
    revenue = [0.0] * seconds
    simulator = OrderStreamSimulator(config, start=start)
    for second in range(seconds):
        incident = by_start.get(second)
        if incident is not None:
            simulator.force_anomaly(
                incident.kind, at=start + second, duration_s=incident.duration_s
            )
        for event in simulator.advance(start + second + 1):
            index = int(datetime.fromisoformat(event["occurred_at"]).timestamp() - start)
            if not 0 <= index < seconds:
                continue
            kind = event["event_type"]
            if kind == EventType.ORDER_PLACED:
                placed[index] += 1
            elif kind == EventType.ORDER_CANCELLED:
                cancelled[index] += 1
            elif kind == EventType.ORDER_PAID:
                revenue[index] += float(event["amount_mad"])
    return Traffic(
        start=start,
        placed=list(accumulate(placed, initial=0)),
        cancelled=list(accumulate(cancelled, initial=0)),
        revenue=list(accumulate(revenue, initial=0.0)),
    )


def evaluate(
    traffic: Traffic,
    thresholds: Thresholds,
    *,
    fire_after_s: float,
    resolve_after_s: float,
) -> list[tuple[int, Alert]]:
    """Run the production rules and engine once per second; return (second, transition)."""
    engine = AlertEngine(
        fire_after=timedelta(seconds=fire_after_s),
        resolve_after=timedelta(seconds=resolve_after_s),
    )
    window_s = thresholds.window_minutes * 60
    placed, cancelled, revenue = traffic.placed, traffic.cancelled, traffic.revenue
    transitions: list[tuple[int, Alert]] = []
    for second in range(2 * window_s, traffic.seconds + 1):
        now = traffic.start + second
        # Same window arithmetic as rad.alerts.store.fetch_window_stats.
        recent_start = math.floor(now / 60) * 60 - (thresholds.window_minutes - 1) * 60
        recent_index = int(recent_start - traffic.start)
        previous_index = recent_index - window_s
        if previous_index < 0:
            continue
        moment = datetime.fromtimestamp(now, UTC)
        stats = WindowStats(
            now=moment,
            recent_seconds=now - recent_start,
            previous_seconds=float(window_s),
            placed=placed[second] - placed[recent_index],
            cancelled=cancelled[second] - cancelled[recent_index],
            revenue_recent=revenue[second] - revenue[recent_index],
            revenue_previous=revenue[recent_index] - revenue[previous_index],
            accepted=0,
            dead_letters=0,
            last_commit_at=moment,
        )
        results: list[RuleResult] = [
            cancellation_rate_rule(stats, thresholds),
            revenue_drop_rule(stats, thresholds),
        ]
        transitions.extend((second, alert) for alert in engine.update(results, moment))
    return transitions


@dataclass(frozen=True, slots=True)
class Detection:
    kind: str
    start_s: int
    detected: bool
    delay_s: int | None


@dataclass(frozen=True, slots=True)
class Score:
    false_alerts: dict[str, int]
    detections: list[Detection]


def score(
    transitions: Sequence[tuple[int, Alert]],
    incidents: Sequence[Incident],
    *,
    attribution_s: int,
) -> Score:
    """Alerts starting during an incident (or up to ``attribution_s`` after it) are attributed to
    it; any other alert is a false alert. An incident counts as detected when its expected rule
    fires during that period."""
    firing = [
        (second, alert) for second, alert in transitions if alert.status is AlertStatus.FIRING
    ]

    def during(second: int, incident: Incident) -> bool:
        return incident.start_s <= second <= incident.start_s + incident.duration_s + attribution_s

    false_alerts = {rule.value: 0 for rule in EVALUATED_RULES}
    for second, alert in firing:
        if not any(during(second, incident) for incident in incidents):
            false_alerts[alert.rule.value] += 1

    detections = []
    for incident in incidents:
        hits = [
            second
            for second, alert in firing
            if alert.rule is EXPECTED_RULE[incident.kind] and during(second, incident)
        ]
        detections.append(
            Detection(
                kind=incident.kind.value,
                start_s=incident.start_s,
                detected=bool(hits),
                delay_s=hits[0] - incident.start_s if hits else None,
            )
        )
    return Score(false_alerts=false_alerts, detections=detections)


# Incidents start at these local hours every simulated day, alternating kinds, so detection is
# measured at night, at the lunch bump and at the evening peak.
INCIDENT_HOURS = (1.5, 4.5, 7.5, 10.5, 13.5, 16.5, 19.5, 22.5)


@dataclass(frozen=True, slots=True)
class Candidate:
    name: str
    thresholds: Thresholds
    fire_after_s: float


def candidate(window: int, cancel: float, drop: float, fire_after_s: float) -> Candidate:
    return Candidate(
        name=f"{window} min, cancel>{cancel:.0%}, drop>{drop:.0%}, fire after {fire_after_s:.0f}s",
        thresholds=Thresholds(
            window_minutes=window, cancellation_rate=cancel, revenue_drop_ratio=drop
        ),
        fire_after_s=fire_after_s,
    )


CANDIDATES: list[Candidate] = [
    candidate(3, 0.15, 0.50, 10),  # the first defaults
    candidate(3, 0.20, 0.60, 10),
    candidate(3, 0.20, 0.60, 30),
    candidate(3, 0.25, 0.60, 30),
    candidate(3, 0.20, 0.70, 30),
    candidate(4, 0.20, 0.60, 30),
    candidate(5, 0.15, 0.50, 10),
    candidate(5, 0.20, 0.60, 10),
]


def schedule(days: int, duration_s: int) -> list[Incident]:
    kinds = (Anomaly.PAYMENT_OUTAGE, Anomaly.TRAFFIC_DROP)
    return [
        Incident(kind=kinds[i % 2], start_s=int((day * 24 + hour) * 3600), duration_s=duration_s)
        for day in range(days)
        for i, hour in enumerate(INCIDENT_HOURS)
    ]


def summarize(name: str, days: int, scores: list[Score]) -> dict[str, Any]:
    detections = [d for s in scores for d in s.detections]
    by_kind: dict[str, dict[str, Any]] = {}
    for kind in (a.value for a in Anomaly):
        of_kind = [d for d in detections if d.kind == kind]
        delays = [d.delay_s for d in of_kind if d.delay_s is not None]
        by_kind[kind] = {
            "incidents": len(of_kind),
            "detected": sum(d.detected for d in of_kind),
            "median_delay_s": median(delays) if delays else None,
            "max_delay_s": max(delays) if delays else None,
            "missed_at_start_s": [d.start_s for d in of_kind if not d.detected],
        }
    simulated_days = days * len(scores)
    false_total = {
        rule.value: sum(s.false_alerts[rule.value] for s in scores) for rule in EVALUATED_RULES
    }
    return {
        "candidate": name,
        "simulated_days": simulated_days,
        "false_alerts_per_day": {
            rule: round(count / simulated_days, 2) for rule, count in false_total.items()
        },
        "detection": by_kind,
    }


def run(
    *,
    days: int,
    seeds: int,
    events_per_second: float,
    incident_duration_s: int,
    resolve_after_s: float,
) -> dict[str, Any]:
    start = datetime(2026, 9, 1, tzinfo=CASABLANCA_TZ).timestamp()  # local midnight
    incidents = schedule(days, incident_duration_s)
    scores: dict[str, list[Score]] = {c.name: [] for c in CANDIDATES}
    for seed in range(seeds):
        began = time.monotonic()
        config = SimulatorConfig(
            base_events_per_second=events_per_second,
            invalid_ratio=0.0,
            anomalies_per_hour=0.0,  # only the injected, labelled incidents
            seed=seed,
        )
        traffic = simulate(config, start=start, seconds=days * 86_400, incidents=incidents)
        print(
            f"seed {seed}: simulated {days} day(s) in {time.monotonic() - began:.0f} s",
            file=sys.stderr,
        )
        for c in CANDIDATES:
            transitions = evaluate(
                traffic, c.thresholds, fire_after_s=c.fire_after_s, resolve_after_s=resolve_after_s
            )
            scores[c.name].append(
                score(transitions, incidents, attribution_s=c.thresholds.window_minutes * 60)
            )
    return {
        "settings": {
            "days_per_seed": days,
            "seeds": seeds,
            "events_per_second": events_per_second,
            "incident_duration_s": incident_duration_s,
            "incidents_per_day": len(INCIDENT_HOURS),
            "resolve_after_s": resolve_after_s,
            "bursts": asdict(SimulatorConfig())["bursts_per_hour"],
        },
        "results": [summarize(name, days, runs) for name, runs in scores.items()],
    }


def print_table(results: list[dict[str, Any]]) -> None:
    """Compact human summary on stderr (the JSON on stdout has every detail)."""

    def detection(entry: dict[str, Any]) -> str:
        return (
            f"{entry['detected']}/{entry['incidents']} "
            f"(median {entry['median_delay_s']} s, max {entry['max_delay_s']} s)"
        )

    for row in results:
        false_per_day = row["false_alerts_per_day"]
        print(
            f"{row['candidate']:<48} | false/day cancel {false_per_day['cancellation_rate']:>5}"
            f" revenue {false_per_day['revenue_drop']:>5}"
            f" | outage {detection(row['detection']['payment_outage']):<34}"
            f" | traffic drop {detection(row['detection']['traffic_drop'])}",
            file=sys.stderr,
        )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="python -m rad.alerts.backtest")
    parser.add_argument("--days", type=int, default=2)
    parser.add_argument("--seeds", type=int, default=2)
    parser.add_argument("--events-per-second", type=float, default=20.0)
    parser.add_argument("--incident-duration", type=int, default=240)
    parser.add_argument("--resolve-after", type=float, default=30.0)
    args = parser.parse_args(argv)
    result = run(
        days=args.days,
        seeds=args.seeds,
        events_per_second=args.events_per_second,
        incident_duration_s=args.incident_duration,
        resolve_after_s=args.resolve_after,
    )
    print_table(result["results"])
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
