"""asyncpg connection pool helpers."""

import asyncpg

type DbPool = asyncpg.Pool[asyncpg.Record]


async def create_pool(dsn: str, *, min_size: int, max_size: int) -> DbPool:
    pool = await asyncpg.create_pool(
        dsn,
        min_size=min_size,
        max_size=max_size,
        command_timeout=30,
        # date_trunc() on timestamptz truncates in the *session* time zone. Pin it to UTC so
        # buckets never depend on server configuration.
        server_settings={"timezone": "UTC"},
    )
    if pool is None:  # only happens when the pool is created lazily; guards the Optional type
        raise RuntimeError("could not create the database pool")
    return pool
