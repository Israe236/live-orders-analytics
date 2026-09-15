"""Fixtures. DB tests need PostgreSQL (``docker compose up -d postgres``).

The test database defaults to ``rad_test`` on localhost and is created if missing;
override with ``RAD_TEST_DATABASE_URL``.
"""

import asyncio
import os
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import asyncpg
import httpx
import pytest
import uvicorn
from fastapi import FastAPI

from rad.api.app import create_app
from rad.common.config import Settings
from rad.db.migrate import apply_migrations
from rad.db.pool import DbPool, create_pool
from tests.helpers import free_port, truncate_all

TEST_DATABASE_URL = os.environ.get(
    "RAD_TEST_DATABASE_URL", "postgresql://rad:rad@localhost:5432/rad_test"
)


@dataclass(frozen=True, slots=True)
class LiveServer:
    base_url: str
    app: FastAPI

    @property
    def ws_base_url(self) -> str:
        return self.base_url.replace("http://", "ws://", 1)


type ServerFactory = Callable[..., Awaitable[LiveServer]]


def _ensure_database(url: str) -> None:
    parts = urlsplit(url)
    name = parts.path.lstrip("/")
    admin_url = urlunsplit(parts._replace(path="/postgres"))

    async def run() -> None:
        conn = await asyncpg.connect(admin_url)
        try:
            if not await conn.fetchval("SELECT 1 FROM pg_database WHERE datname = $1", name):
                await conn.execute(f'CREATE DATABASE "{name}"')
        finally:
            await conn.close()

    asyncio.run(run())


@pytest.fixture(scope="session")
def database_url() -> str:
    _ensure_database(TEST_DATABASE_URL)
    return TEST_DATABASE_URL


@pytest.fixture
async def db_pool(database_url: str) -> AsyncIterator[DbPool]:
    """A pool on a migrated, empty test database."""
    pool = await create_pool(database_url, min_size=1, max_size=4)
    await apply_migrations(pool)
    await truncate_all(pool)
    yield pool
    await pool.close()


@pytest.fixture
async def start_server(database_url: str, db_pool: DbPool) -> AsyncIterator[ServerFactory]:
    """Start real uvicorn servers (on free ports) with optional Settings overrides."""
    running: list[tuple[uvicorn.Server, asyncio.Task[None]]] = []

    async def start(**overrides: Any) -> LiveServer:
        # Retention is tested on its own (test_retention.py); keep it out of the server tests.
        options: dict[str, Any] = {"retention_enabled": False, **overrides}
        settings = Settings(
            database_url=database_url, db_pool_min_size=1, db_pool_max_size=4, **options
        )
        app = create_app(settings)
        port = free_port()
        server = uvicorn.Server(
            uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
        )
        task = asyncio.create_task(server.serve())
        running.append((server, task))
        async with asyncio.timeout(15):
            while not server.started:
                if task.done():
                    task.result()
                    raise RuntimeError("server exited during startup")
                await asyncio.sleep(0.02)
        return LiveServer(base_url=f"http://127.0.0.1:{port}", app=app)

    yield start

    for server, _ in running:
        server.should_exit = True
    for _, task in running:
        await task


@pytest.fixture
async def server(start_server: ServerFactory) -> LiveServer:
    return await start_server()


@pytest.fixture
async def client(server: LiveServer) -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient(base_url=server.base_url, timeout=15) as http:
        yield http
