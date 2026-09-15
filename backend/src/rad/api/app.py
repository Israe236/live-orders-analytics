"""FastAPI application factory.

Run with: ``uvicorn rad.api.app:create_app --factory``
"""

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime

from fastapi import FastAPI

from rad import __version__
from rad.alerts.store import resolve_stale_alerts
from rad.api import routes_alerts, routes_health, routes_ingest, routes_live, routes_metrics
from rad.api.live import LiveHub
from rad.api.services import Services
from rad.common.config import Settings
from rad.db.migrate import apply_migrations
from rad.db.pool import create_pool
from rad.processing.retention import RetentionJob
from rad.processing.writer import BatchWriter

log = logging.getLogger(__name__)


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        pool = await create_pool(
            settings.database_url,
            min_size=settings.db_pool_min_size,
            max_size=settings.db_pool_max_size,
        )
        try:
            await apply_migrations(pool)
            closed = await resolve_stale_alerts(pool, now=datetime.now(UTC))
            if closed:
                log.info("resolved %d alerts left firing by a previous run", closed)
            writer = BatchWriter(
                pool,
                max_queued_events=settings.ingest_queue_max_events,
                batch_max_events=settings.writer_batch_max_events,
            )
            hub = LiveHub(pool, writer, settings)
            writer.add_listener(hub.on_commit)
            retention = (
                RetentionJob(
                    pool, settings.retention_policy(), interval_s=settings.retention_interval_s
                )
                if settings.retention_enabled
                else None
            )
            writer.start()
            hub.start()
            if retention is not None:
                retention.start()
            app.state.services = Services(settings=settings, pool=pool, writer=writer, hub=hub)
            try:
                yield
            finally:
                # Stop pushing first, then drain the write queue, then close the pool.
                if retention is not None:
                    await retention.stop()
                await hub.stop()
                await writer.stop()
        finally:
            await pool.close()

    app = FastAPI(title="realtime-analytics-dashboard", version=__version__, lifespan=lifespan)
    app.include_router(routes_health.router)
    app.include_router(routes_ingest.router)
    app.include_router(routes_metrics.router)
    app.include_router(routes_live.router)
    app.include_router(routes_alerts.router)
    return app
