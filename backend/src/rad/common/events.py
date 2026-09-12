"""Order event schema: the single definition of "a valid event".

Used by the ingestion API (validation), the generator (building payloads), tests and benchmarks.
"""

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from typing import Annotated, Any
from uuid import UUID

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    ValidationInfo,
    field_validator,
)


class EventType(StrEnum):
    ORDER_PLACED = "order_placed"
    ORDER_PAID = "order_paid"
    ORDER_SHIPPED = "order_shipped"
    ORDER_CANCELLED = "order_cancelled"


class Category(StrEnum):
    ELECTRONICS = "electronics"
    FASHION = "fashion"
    HOME = "home"
    BEAUTY = "beauty"
    GROCERY = "grocery"
    SPORTS = "sports"
    BOOKS = "books"
    TOYS = "toys"


class City(StrEnum):
    CASABLANCA = "Casablanca"
    RABAT = "Rabat"
    MARRAKECH = "Marrakech"
    FES = "Fes"
    TANGIER = "Tangier"
    AGADIR = "Agadir"
    MEKNES = "Meknes"
    OUJDA = "Oujda"
    KENITRA = "Kenitra"
    TETOUAN = "Tetouan"


class PaymentMethod(StrEnum):
    CARD = "card"
    CASH_ON_DELIVERY = "cash_on_delivery"
    WALLET = "wallet"
    BANK_TRANSFER = "bank_transfer"


@dataclass(frozen=True, slots=True)
class TimeWindow:
    """Accepted range for ``occurred_at``, relative to the server clock at validation time."""

    now: datetime
    max_future: timedelta
    max_age: timedelta


AmountMAD = Annotated[Decimal, Field(gt=0, le=1_000_000, max_digits=12, decimal_places=2)]


class OrderEvent(BaseModel):
    """One lifecycle transition of an order."""

    # Unknown fields are ignored rather than rejected: a producer that starts sending an extra
    # field should not suddenly have every event dead-lettered.
    model_config = ConfigDict(frozen=True, extra="ignore")

    event_id: UUID
    order_id: UUID
    event_type: EventType
    occurred_at: AwareDatetime
    amount_mad: AmountMAD
    category: Category
    city: City
    payment_method: PaymentMethod

    @field_validator("amount_mad", mode="before")
    @classmethod
    def _reject_booleans(cls, value: Any) -> Any:
        # In Python `True` is an int, so JSON `true` would otherwise be accepted as 1.00 MAD.
        if isinstance(value, bool):
            raise ValueError("amount_mad must be a number, not a boolean")
        return value

    @field_validator("occurred_at")
    @classmethod
    def _normalize_and_check_window(cls, value: datetime, info: ValidationInfo) -> datetime:
        value = value.astimezone(UTC)
        window = info.context.get("window") if isinstance(info.context, dict) else None
        if isinstance(window, TimeWindow):
            if value > window.now + window.max_future:
                limit = int(window.max_future.total_seconds())
                raise ValueError(f"occurred_at is more than {limit}s in the future")
            if value < window.now - window.max_age:
                limit = int(window.max_age.total_seconds())
                raise ValueError(f"occurred_at is more than {limit}s in the past")
        return value


@dataclass(frozen=True, slots=True)
class InvalidEvent:
    """An item of a batch that failed validation, with its position and readable errors."""

    index: int
    raw: Any
    errors: list[dict[str, str]]


def simplify_errors(exc: ValidationError) -> list[dict[str, str]]:
    """Turn Pydantic errors into small JSON-safe dicts (no echo of the input values)."""
    return [
        {
            "field": ".".join(str(part) for part in err["loc"]) or "(root)",
            "message": err["msg"],
            "type": err["type"],
        }
        for err in exc.errors(include_url=False, include_input=False, include_context=False)
    ]


def validate_events(
    items: Sequence[Any], window: TimeWindow | None
) -> tuple[list[OrderEvent], list[InvalidEvent]]:
    """Validate each item independently, so one bad event never sinks the whole batch."""
    valid: list[OrderEvent] = []
    invalid: list[InvalidEvent] = []
    context = {"window": window}
    for index, item in enumerate(items):
        try:
            valid.append(OrderEvent.model_validate(item, context=context))
        except ValidationError as exc:
            invalid.append(InvalidEvent(index=index, raw=item, errors=simplify_errors(exc)))
    return valid, invalid
