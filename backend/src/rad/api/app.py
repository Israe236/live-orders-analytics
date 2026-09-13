"""FastAPI application factory.

Run with: ``uvicorn rad.api.app:create_app --factory``
"""

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from rad import __version__
from rad.api import routes_health, routes_ingest, routes_metrics
from rad.api.services import Services
from rad.common.config import Settings
from rad.db.migrate import apply_migrations
from rad.db.pool import create_pool
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
            writer = BatchWriter(
                pool,
                max_queued_events=settings.ingest_queue_max_events,
                batch_max_events=settings.writer_batch_max_events,
            )
            writer.start()
            app.state.services = Services(settings=settings, pool=pool, writer=writer)
            try:
                yield
            finally:
                await writer.stop()
        finally:
            await pool.close()

    app = FastAPI(title="realtime-analytics-dashboard", version=__version__, lifespan=lifespan)
    app.include_router(routes_health.router)
    app.include_router(routes_ingest.router)
    app.include_router(routes_metrics.router)
    return app
