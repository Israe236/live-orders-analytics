"""Metric payloads sent to clients over REST and WebSocket.

Money is stored exactly (``numeric``) in PostgreSQL; here it is a float, because these values
are only displayed and JavaScript clients have no decimal type anyway.
"""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel

ORDER_STATUSES = ("placed", "paid", "shipped", "cancelled")

type Granularity = Literal["minute", "hour"]


class Kpis(BaseModel):
    window_minutes: int
    orders_placed: int
    orders_paid: int
    orders_shipped: int
    orders_cancelled: int
    revenue_mad: float
    # None when there is nothing to divide by (no paid / no placed orders in the window).
    avg_order_value_mad: float | None
    cancellation_rate: float | None


class TimePoint(BaseModel):
    bucket: datetime
    revenue_mad: float
    orders_placed: int
    orders_paid: int
    orders_cancelled: int


class Breakdown(BaseModel):
    value: str
    orders_placed: int
    orders_paid: int
    orders_cancelled: int
    revenue_mad: float


class IngestStats(BaseModel):
    window_minutes: int
    accepted: int
    duplicates: int
    dead_letters: int
    events_per_second: float


class MetricsSnapshot(BaseModel):
    generated_at: datetime
    kpis: Kpis
    revenue_per_minute: list[TimePoint]
    revenue_per_hour: list[TimePoint]
    orders_by_status: dict[str, int]
    categories: list[Breakdown]
    cities: list[Breakdown]
    payment_methods: list[Breakdown]
    ingest: IngestStats
