"""Messages pushed over the ``/ws/live`` WebSocket.

Every message is a JSON object with a ``type`` field:

* ``snapshot`` — the complete dashboard state. Sent first on connect, then periodically.
* ``update``   — the same state without the full time series: only their last points, which
  are the ones that change second to second. Clients merge it into their state.
* ``events``   — a sample of the newest ingested events, for the live feed.

``seq`` increases by one per server tick; ``pipeline`` describes freshness and throughput.
"""

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel

from rad.alerts.store import AlertOut
from rad.common.events import OrderEvent
from rad.common.metrics import Breakdown, IngestStats, Kpis, MetricsSnapshot, TimePoint

MINUTE_TAIL_POINTS = 3
HOUR_TAIL_POINTS = 1


class PipelineStats(BaseModel):
    # Events committed per second, measured over the last ~5 seconds.
    events_per_second: float
    # Newest event time among committed events: "how fresh is the data I am looking at".
    last_event_occurred_at: datetime | None
    last_commit_at: datetime | None
    connected_clients: int


class LiveUpdate(BaseModel):
    generated_at: datetime
    kpis: Kpis
    revenue_per_minute_tail: list[TimePoint]
    revenue_per_hour_tail: list[TimePoint]
    orders_by_status: dict[str, int]
    categories: list[Breakdown]
    cities: list[Breakdown]
    payment_methods: list[Breakdown]
    ingest: IngestStats

    @classmethod
    def from_snapshot(cls, snapshot: MetricsSnapshot) -> LiveUpdate:
        return cls(
            generated_at=snapshot.generated_at,
            kpis=snapshot.kpis,
            revenue_per_minute_tail=snapshot.revenue_per_minute[-MINUTE_TAIL_POINTS:],
            revenue_per_hour_tail=snapshot.revenue_per_hour[-HOUR_TAIL_POINTS:],
            orders_by_status=snapshot.orders_by_status,
            categories=snapshot.categories,
            cities=snapshot.cities,
            payment_methods=snapshot.payment_methods,
            ingest=snapshot.ingest,
        )


class FeedEvent(BaseModel):
    event_id: UUID
    order_id: UUID
    event_type: str
    occurred_at: datetime
    amount_mad: float
    category: str
    city: str
    payment_method: str

    @classmethod
    def from_event(cls, event: OrderEvent) -> FeedEvent:
        return cls(
            event_id=event.event_id,
            order_id=event.order_id,
            event_type=event.event_type.value,
            occurred_at=event.occurred_at,
            amount_mad=float(event.amount_mad),
            category=event.category.value,
            city=event.city.value,
            payment_method=event.payment_method.value,
        )


class SnapshotMessage(BaseModel):
    type: Literal["snapshot"] = "snapshot"
    seq: int
    sent_at: datetime
    pipeline: PipelineStats
    data: MetricsSnapshot


class UpdateMessage(BaseModel):
    type: Literal["update"] = "update"
    seq: int
    sent_at: datetime
    pipeline: PipelineStats
    data: LiveUpdate


class EventsMessage(BaseModel):
    type: Literal["events"] = "events"
    seq: int
    items: list[FeedEvent]  # newest first


class AlertMessage(BaseModel):
    """An alert started firing or was resolved (sent once per transition)."""

    type: Literal["alert"] = "alert"
    seq: int
    alert: AlertOut
