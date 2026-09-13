"""Alerting end to end: breach a threshold over HTTP, receive the alert over the WebSocket."""

from collections.abc import Callable
from typing import Any

import httpx
from websockets.asyncio.client import connect

from tests.conftest import LiveServer, ServerFactory
from tests.helpers import event_payload, receive_until, ws_url


def is_alert(rule: str, status: str) -> Callable[[dict[str, Any]], bool]:
    def matches(message: dict[str, Any]) -> bool:
        return bool(
            message["type"] == "alert"
            and message["alert"]["rule"] == rule
            and message["alert"]["status"] == status
        )

    return matches


async def alerting_server(start_server: ServerFactory) -> LiveServer:
    # No hysteresis delays and a low volume floor, so the test runs in about a second.
    return await start_server(
        ws_tick_interval_s=0.1,
        alert_fire_after_s=0,
        alert_resolve_after_s=0,
        alert_cancellation_min_orders=5,
    )


async def post_cancellation_spike(http: httpx.AsyncClient) -> None:
    placed = [event_payload() for _ in range(10)]
    cancelled = [
        event_payload(order_id=order["order_id"], event_type="order_cancelled")
        for order in placed[:5]
    ]
    response = await http.post("/events/batch", json=[*placed, *cancelled])
    assert response.json()["inserted"] == 15


async def test_cancellation_spike_fires_then_resolves(start_server: ServerFactory) -> None:
    server = await alerting_server(start_server)
    async with (
        httpx.AsyncClient(base_url=server.base_url) as http,
        connect(ws_url(server.base_url)) as ws,
    ):
        await post_cancellation_spike(http)  # 5 cancelled / 10 placed = 50%
        fired = await receive_until(ws, is_alert("cancellation_rate", "firing"))
        assert fired["alert"]["value"] == 0.5
        assert fired["alert"]["severity"] == "warning"

        # Stored before it was pushed: the REST history already agrees.
        stored = (await http.get("/alerts", params={"status": "firing"})).json()
        assert [alert["id"] for alert in stored] == [fired["alert"]["id"]]

        # 90 more healthy orders: 5 / 100 = 5%, below the 15% threshold.
        await http.post("/events/batch", json=[event_payload() for _ in range(90)])
        resolved = await receive_until(ws, is_alert("cancellation_rate", "resolved"))
        assert resolved["alert"]["id"] == fired["alert"]["id"]
        assert resolved["alert"]["resolved_at"] is not None
        assert (await http.get("/alerts", params={"status": "firing"})).json() == []


async def test_a_client_connecting_mid_incident_receives_active_alerts(
    start_server: ServerFactory,
) -> None:
    server = await alerting_server(start_server)
    async with (
        httpx.AsyncClient(base_url=server.base_url) as http,
        connect(ws_url(server.base_url)) as first,
    ):
        await post_cancellation_spike(http)
        fired = await receive_until(first, is_alert("cancellation_rate", "firing"))

        async with connect(ws_url(server.base_url)) as late:
            snapshot = await receive_until(late, lambda m: True, within_s=2)
            assert snapshot["type"] == "snapshot"
            active = await receive_until(late, is_alert("cancellation_rate", "firing"), within_s=2)
            assert active["alert"]["id"] == fired["alert"]["id"]
