"""Metrics endpoints, end to end: ingest over HTTP, read back the aggregates."""

import httpx

from tests.helpers import event_payload


async def test_snapshot_reflects_ingested_events(client: httpx.AsyncClient) -> None:
    placed = [event_payload(amount_mad="200.00") for _ in range(4)]
    paid = [
        event_payload(order_id=e["order_id"], event_type="order_paid", amount_mad="200.00")
        for e in placed[:2]
    ]
    cancelled = event_payload(order_id=placed[2]["order_id"], event_type="order_cancelled")
    response = await client.post("/events/batch", json=[*placed, *paid, cancelled])
    assert response.json()["inserted"] == 7
    await client.post(
        "/events/batch", content=b"{broken", headers={"content-type": "application/json"}
    )

    snap = (await client.get("/metrics/snapshot")).json()

    kpis = snap["kpis"]
    assert (kpis["orders_placed"], kpis["orders_paid"], kpis["orders_cancelled"]) == (4, 2, 1)
    assert kpis["revenue_mad"] == 400.0
    assert kpis["avg_order_value_mad"] == 200.0
    assert kpis["cancellation_rate"] == 0.25
    assert snap["orders_by_status"] == {"placed": 1, "paid": 2, "shipped": 0, "cancelled": 1}
    assert snap["categories"][0]["value"] == "electronics"
    assert (snap["ingest"]["accepted"], snap["ingest"]["dead_letters"]) == (7, 1)
    assert snap["ingest"]["events_per_second"] > 0


async def test_timeseries_endpoint(client: httpx.AsyncClient) -> None:
    hourly = await client.get("/metrics/timeseries", params={"granularity": "hour", "points": 24})
    assert hourly.status_code == 200
    assert len(hourly.json()) == 24
    bad = await client.get("/metrics/timeseries", params={"granularity": "week"})
    assert bad.status_code == 422
