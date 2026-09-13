"""Read-only metrics endpoints (initial load for dashboards; live updates come over WebSocket)."""

from typing import Annotated

from fastapi import APIRouter, Query

from rad.api.services import ServicesDep
from rad.common.metrics import Granularity, MetricsSnapshot, TimePoint
from rad.processing.snapshot import fetch_snapshot, fetch_timeseries

router = APIRouter(prefix="/metrics", tags=["metrics"])


@router.get("/snapshot")
async def snapshot(services: ServicesDep) -> MetricsSnapshot:
    return await fetch_snapshot(services.pool)


@router.get("/timeseries")
async def timeseries(
    services: ServicesDep,
    granularity: Granularity = "minute",
    points: Annotated[int, Query(ge=1, le=1440)] = 60,
) -> list[TimePoint]:
    return await fetch_timeseries(services.pool, granularity=granularity, points=points)
