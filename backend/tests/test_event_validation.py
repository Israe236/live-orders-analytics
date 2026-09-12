"""Unit tests for the event schema (no database)."""

from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import pytest
from pydantic import ValidationError

from rad.common.events import City, EventType, OrderEvent, TimeWindow, validate_events
from tests.helpers import event_payload

NOW = datetime(2026, 9, 12, 12, 0, tzinfo=UTC)
WINDOW = TimeWindow(now=NOW, max_future=timedelta(minutes=5), max_age=timedelta(days=7))


def payload(**overrides: Any) -> dict[str, Any]:
    return event_payload(**{"occurred_at": NOW.isoformat(), **overrides})


def validate(raw: Any) -> OrderEvent:
    return OrderEvent.model_validate(raw, context={"window": WINDOW})


def test_valid_event_is_parsed() -> None:
    event = validate(payload())
    assert event.event_type is EventType.ORDER_PLACED
    assert event.amount_mad == Decimal("349.90")
    assert event.city is City.CASABLANCA


def test_timestamp_is_normalized_to_utc() -> None:
    casablanca_summer = timezone(timedelta(hours=1))
    event = validate(payload(occurred_at=datetime(2026, 9, 12, 13, 0, tzinfo=casablanca_summer)))
    assert event.occurred_at == NOW
    assert event.occurred_at.tzinfo is UTC


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("event_type", "order_refunded"),
        ("category", "weapons"),
        ("city", "Paris"),
        ("payment_method", "bitcoin"),
        ("amount_mad", 0),
        ("amount_mad", -10),
        ("amount_mad", "12.345"),
        ("amount_mad", Decimal("1000000.01")),
        ("amount_mad", True),
        ("amount_mad", "abc"),
        ("amount_mad", None),
        ("event_id", "not-a-uuid"),
        ("occurred_at", "2026-09-12T12:00:00"),  # no timezone: ambiguous, rejected
        ("occurred_at", "yesterday"),
    ],
)
def test_invalid_field_values_are_rejected(field: str, value: Any) -> None:
    with pytest.raises(ValidationError) as exc_info:
        validate(payload(**{field: value}))
    assert any(err["loc"][0] == field for err in exc_info.value.errors())


def test_missing_field_is_rejected() -> None:
    raw = payload()
    del raw["city"]
    with pytest.raises(ValidationError):
        validate(raw)


def test_small_clock_skew_is_tolerated() -> None:
    validate(payload(occurred_at=(NOW + timedelta(minutes=4)).isoformat()))


def test_timestamp_far_in_future_is_rejected() -> None:
    with pytest.raises(ValidationError, match="in the future"):
        validate(payload(occurred_at=(NOW + timedelta(minutes=6)).isoformat()))


def test_timestamp_too_old_is_rejected() -> None:
    with pytest.raises(ValidationError, match="in the past"):
        validate(payload(occurred_at=(NOW - timedelta(days=8)).isoformat()))


def test_unknown_fields_are_ignored() -> None:
    event = validate(payload(coupon_code="RAMADAN10"))
    assert not hasattr(event, "coupon_code")


@pytest.mark.parametrize(
    ("value", "expected"), [(250, "250"), ("99.5", "99.50"), (Decimal("1.25"), "1.25")]
)
def test_amount_accepts_integers_strings_and_decimals(value: Any, expected: str) -> None:
    assert validate(payload(amount_mad=value)).amount_mad == Decimal(expected)


def test_validate_events_keeps_valid_items_and_reports_invalid_ones() -> None:
    items = [payload(), payload(city="Paris"), "not an object", None, payload()]
    valid, invalid = validate_events(items, WINDOW)
    assert len(valid) == 2
    assert [item.index for item in invalid] == [1, 2, 3]
    assert invalid[0].errors[0]["field"] == "city"
