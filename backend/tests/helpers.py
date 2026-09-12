"""Shared test helpers (importable as ``tests.helpers``)."""

import socket
from datetime import UTC, datetime
from typing import Any
from uuid import uuid7

from rad.db.pool import DbPool


def event_payload(**overrides: Any) -> dict[str, Any]:
    """A valid wire-format event; override any field to make it invalid or specific."""
    payload: dict[str, Any] = {
        "event_id": str(uuid7()),
        "order_id": str(uuid7()),
        "event_type": "order_placed",
        "occurred_at": datetime.now(UTC).isoformat(),
        "amount_mad": "349.90",
        "category": "electronics",
        "city": "Casablanca",
        "payment_method": "card",
    }
    payload.update(overrides)
    return payload


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port: int = sock.getsockname()[1]
        return port


async def count_rows(pool: DbPool, table: str) -> int:
    return int(await pool.fetchval(f"SELECT count(*) FROM {table}"))


async def truncate_all(pool: DbPool) -> None:
    rows = await pool.fetch(
        "SELECT format('%I', tablename) AS name FROM pg_tables "
        "WHERE schemaname = 'public' AND tablename <> 'schema_migrations'"
    )
    if rows:
        await pool.execute(f"TRUNCATE {', '.join(r['name'] for r in rows)} RESTART IDENTITY")
