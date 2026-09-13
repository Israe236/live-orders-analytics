"""WebSocket layer: unit tests for slow-client handling, integration tests with real clients."""

import asyncio
import json
from datetime import UTC, datetime
from typing import Any

import httpx
import pytest
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

from rad.api.live import CLOSE_TRY_AGAIN_LATER, ClientConnection
from tests.conftest import LiveServer, ServerFactory
from tests.helpers import event_payload, receive_until, ws_url

# --- fakes for unit tests ------------------------------------------------------------------


class RecordingSink:
    def __init__(self, *, blocked: bool = False) -> None:
        self.sent: list[str] = []
        self.closed_with: int | None = None
        self.release = asyncio.Event()
        if not blocked:
            self.release.set()

    async def send_text(self, data: str) -> None:
        await self.release.wait()
        self.sent.append(data)

    async def close(self, code: int = 1000, reason: str | None = None) -> None:
        self.closed_with = code


def make_client(
    sink: RecordingSink, closed: list[ClientConnection], timeout: float = 5.0
) -> ClientConnection:
    client = ClientConnection(
        sink, send_timeout_s=timeout, on_closed=closed.append, feed_queue_size=5
    )
    client.start()
    return client


async def test_slow_client_only_receives_the_latest_state() -> None:
    sink = RecordingSink(blocked=True)
    closed: list[ClientConnection] = []
    client = make_client(sink, closed)

    client.push_state("state-0")
    await asyncio.sleep(0.01)  # the sender picks up state-0 and blocks on the socket
    for i in range(1, 10):
        client.push_state(f"state-{i}")
    sink.release.set()
    await asyncio.sleep(0.05)

    assert sink.sent == ["state-0", "state-9"]
    assert client.conflated == 8
    await client.close()
    assert closed == [client]


async def test_feed_queue_is_bounded_and_drops_oldest() -> None:
    sink = RecordingSink(blocked=True)
    client = make_client(sink, [])
    client.push_feed("feed-0")
    await asyncio.sleep(0.01)  # feed-0 is in flight
    for i in range(1, 11):
        client.push_feed(f"feed-{i}")
    sink.release.set()
    await asyncio.sleep(0.05)

    assert sink.sent == ["feed-0", "feed-6", "feed-7", "feed-8", "feed-9", "feed-10"]
    assert client.feed_dropped == 5
    await client.close()


async def test_stuck_client_is_evicted_without_delaying_others() -> None:
    stuck_sink, fast_sink = RecordingSink(blocked=True), RecordingSink()
    closed: list[ClientConnection] = []
    stuck = make_client(stuck_sink, closed, timeout=0.2)
    fast = make_client(fast_sink, closed, timeout=0.2)

    for i in range(5):
        # This is exactly what the hub does on each tick: hand over, never await a socket.
        stuck.push_state(f"tick-{i}")
        fast.push_state(f"tick-{i}")
        await asyncio.sleep(0.02)
    assert fast_sink.sent == [f"tick-{i}" for i in range(5)]

    await asyncio.sleep(0.3)
    assert closed == [stuck]
    assert stuck_sink.closed_with == CLOSE_TRY_AGAIN_LATER
    assert not fast.closed
    await fast.close()


# --- integration: real server, real WebSocket clients ----------------------------------------


@pytest.fixture
async def fast_server(start_server: ServerFactory) -> LiveServer:
    return await start_server(ws_tick_interval_s=0.1, ws_full_snapshot_every_ticks=5)


async def test_every_client_gets_a_snapshot_then_live_updates(fast_server: LiveServer) -> None:
    clients = [await connect(ws_url(fast_server.base_url)) for _ in range(5)]
    try:
        for ws in clients:
            first = json.loads(await ws.recv())
            assert first["type"] == "snapshot"
            assert len(first["data"]["revenue_per_minute"]) == 60

        async with httpx.AsyncClient(base_url=fast_server.base_url) as http:
            response = await http.post("/events/batch", json=[event_payload() for _ in range(10)])
            assert response.json()["inserted"] == 10

        def shows_ten_orders(message: dict[str, Any]) -> bool:
            return (
                message["type"] in ("snapshot", "update")
                and message["data"]["kpis"]["orders_placed"] == 10
            )

        updates = await asyncio.gather(*(receive_until(ws, shows_ten_orders) for ws in clients))
        assert all(u["pipeline"]["connected_clients"] == 5 for u in updates)
        partial = await receive_until(clients[0], lambda m: m["type"] == "update")
        assert len(partial["data"]["revenue_per_minute_tail"]) == 3
    finally:
        await asyncio.gather(*(ws.close() for ws in clients))


async def test_feed_and_freshness_follow_new_events(fast_server: LiveServer) -> None:
    async with connect(ws_url(fast_server.base_url)) as ws:
        await ws.recv()  # snapshot
        occurred_at = datetime.now(UTC)
        events = [event_payload(occurred_at=occurred_at.isoformat()) for _ in range(3)]
        async with httpx.AsyncClient(base_url=fast_server.base_url) as http:
            await http.post("/events/batch", json=events)

        wanted = {e["event_id"] for e in events}
        seen: set[str] = set()

        def collect(message: dict[str, Any]) -> bool:
            if message["type"] == "events":
                seen.update(item["event_id"] for item in message["items"])
            return wanted <= seen

        await receive_until(ws, collect)

        def fresh(message: dict[str, Any]) -> bool:
            watermark = message.get("pipeline", {}).get("last_event_occurred_at")
            return watermark is not None and datetime.fromisoformat(watermark) >= occurred_at

        await receive_until(ws, fresh)


async def test_disconnected_clients_are_removed(fast_server: LiveServer) -> None:
    clients = [await connect(ws_url(fast_server.base_url)) for _ in range(3)]
    for ws in clients:
        await ws.recv()
    await clients[0].close()
    await clients[1].close()

    async with httpx.AsyncClient(base_url=fast_server.base_url) as http:
        for _ in range(100):  # up to 5 s
            if (await http.get("/health")).json()["live_clients"] == 1:
                break
            await asyncio.sleep(0.05)
        else:
            pytest.fail("closed clients were not removed")
    await clients[2].close()


async def test_clients_over_capacity_are_told_to_retry_later(start_server: ServerFactory) -> None:
    server = await start_server(ws_max_clients=1, ws_tick_interval_s=0.1)
    async with connect(ws_url(server.base_url)) as first:
        await first.recv()
        async with connect(ws_url(server.base_url)) as second:
            with pytest.raises(ConnectionClosed) as exc_info:
                await second.recv()
            assert exc_info.value.rcvd is not None
            assert exc_info.value.rcvd.code == CLOSE_TRY_AGAIN_LATER
