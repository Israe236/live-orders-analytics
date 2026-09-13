"""Liveness / readiness endpoint."""

import asyncio
from dataclasses import asdict

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from rad.api.services import ServicesDep

router = APIRouter(tags=["health"])


@router.get("/health")
async def health(services: ServicesDep) -> JSONResponse:
    try:
        async with asyncio.timeout(2):
            await services.pool.fetchval("SELECT 1")
        database_ok = True
    except Exception:
        database_ok = False
    body = {
        "status": "ok" if database_ok else "degraded",
        "database": database_ok,
        "queued_events": services.writer.queued_events,
        "writer": asdict(services.writer.stats),
        "live_clients": services.hub.client_count,
    }
    return JSONResponse(body, status_code=200 if database_ok else 503)
