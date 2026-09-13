"""Shared test helpers (importable as ``tests.helpers``)."""

import asyncio
import json
import random
import socket
from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid7

from websockets.asyncio.client import ClientConnection

from rad.common.events import Category, City, EventType, OrderEvent, PaymentMethod
from rad.db.pool import DbPool
from rad.processing.writer import BatchWriter


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


def make_event(
    rng: random.Random,
    *,
    order_id: UUID,
    event_type: EventType,
    occurred_at: datetime,
    amount: str | Decimal,
    category: Category = Category.ELECTRONICS,
    city: City = City.CASABLANCA,
    payment: PaymentMethod = PaymentMethod.CARD,
) -> OrderEvent:
    """An already-validated event with a reproducible id (for writer-level tests)."""
    return OrderEvent.model_validate(
        {
            "event_id": UUID(int=rng.getrandbits(128), version=4),
            "order_id": order_id,
            "event_type": event_type,
            "occurred_at": occurred_at,
            "amount_mad": amount,
            "category": category,
            "city": city,
            "payment_method": payment,
        }
    )


async def write_all(pool: DbPool, submissions: list[list[OrderEvent]], batch_max: int) -> None:
    """Submit everything concurrently through a real BatchWriter, then stop it."""
    writer = BatchWriter(pool, max_queued_events=1_000_000, batch_max_events=batch_max)
    writer.start()
    try:
        await asyncio.gather(*(writer.submit(chunk) for chunk in submissions))
    finally:
        await writer.stop()


def ws_url(base_url: str) -> str:
    """The live WebSocket URL of a server given its http:// base URL."""
    return base_url.replace("http://", "ws://", 1) + "/ws/live"


async def receive_until(
    ws: ClientConnection, predicate: Callable[[dict[str, Any]], bool], within_s: float = 5.0
) -> dict[str, Any]:
    """Read messages until one matches, failing after ``within_s`` seconds."""
    async with asyncio.timeout(within_s):
        while True:
            message: dict[str, Any] = json.loads(await ws.recv())
            if predicate(message):
                return message


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
