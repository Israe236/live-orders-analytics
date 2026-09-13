"""Alert history (live alert changes are pushed over the WebSocket)."""

from typing import Annotated, Literal

from fastapi import APIRouter, Query

from rad.alerts.store import AlertOut, fetch_alerts
from rad.api.services import ServicesDep

router = APIRouter(tags=["alerts"])


@router.get("/alerts")
async def list_alerts(
    services: ServicesDep,
    limit: Annotated[int, Query(ge=1, le=500)] = 50,
    status: Literal["firing", "resolved"] | None = None,
) -> list[AlertOut]:
    return await fetch_alerts(services.pool, limit=limit, status=status)
