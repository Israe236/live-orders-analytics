"""Minimal migration runner: applies numbered ``migrations/*.sql`` files once, in order.

A Postgres advisory lock makes it safe when several API processes start at the same time:
only one of them applies migrations, the others wait and then find nothing left to do.
"""

import logging
from pathlib import Path

from rad.db.pool import DbPool

log = logging.getLogger(__name__)

MIGRATIONS_DIR = Path(__file__).parent / "migrations"
_ADVISORY_LOCK_ID = 7_240_001  # arbitrary, just unique to this application


async def apply_migrations(pool: DbPool) -> list[str]:
    applied_now: list[str] = []
    async with pool.acquire() as conn:
        await conn.execute("SELECT pg_advisory_lock($1)", _ADVISORY_LOCK_ID)
        try:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version    text PRIMARY KEY,
                    applied_at timestamptz NOT NULL DEFAULT now()
                )
                """
            )
            done = {r["version"] for r in await conn.fetch("SELECT version FROM schema_migrations")}
            for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
                if path.stem in done:
                    continue
                async with conn.transaction():
                    await conn.execute(path.read_text(encoding="utf-8"))
                    await conn.execute(
                        "INSERT INTO schema_migrations (version) VALUES ($1)", path.stem
                    )
                log.info("applied migration %s", path.stem)
                applied_now.append(path.stem)
        finally:
            await conn.execute("SELECT pg_advisory_unlock($1)", _ADVISORY_LOCK_ID)
    return applied_now
