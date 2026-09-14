"""The benchmark's own helpers: numbers in the README depend on them being right."""

import random

import pytest

from rad.bench.benchmark import make_batch, percentile, summarize_ms
from rad.common.events import OrderEvent


def test_percentile_interpolates_between_ranks() -> None:
    values = [10.0, 20.0, 30.0, 40.0]
    assert percentile(values, 0) == 10.0
    assert percentile(values, 50) == pytest.approx(25.0)
    assert percentile(values, 100) == 40.0
    assert percentile([7.0], 99) == 7.0
    assert percentile([], 50) is None


def test_percentile_does_not_depend_on_input_order() -> None:
    values = [float(v) for v in range(1, 101)]
    shuffled = values[:]
    random.Random(1).shuffle(shuffled)
    assert percentile(shuffled, 95) == percentile(values, 95) == pytest.approx(95.05)


def test_summary_of_no_samples_has_no_made_up_numbers() -> None:
    assert summarize_ms([]) == {"count": 0, "p50": None, "p95": None, "p99": None, "max": None}


def test_generated_batches_are_valid_and_unique() -> None:
    events, created = make_batch(200, random.Random(3))
    parsed = [OrderEvent.model_validate(event) for event in events]
    assert len({event.event_id for event in parsed}) == 200
    assert all(abs(event.occurred_at.timestamp() - created) < 1 for event in parsed)
