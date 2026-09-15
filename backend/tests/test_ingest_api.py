"""Integration tests for the ingestion endpoints (real HTTP server + PostgreSQL)."""

import asyncio

import httpx
import pytest

from rad.db.pool import DbPool
from tests.conftest import ServerFactory
from tests.helpers import count_rows, event_payload


async def test_valid_batch_is_stored(client: httpx.AsyncClient, db_pool: DbPool) -> None:
    events = [event_payload() for _ in range(25)]
    response = await client.post("/events/batch", json={"events": events})
    assert response.status_code == 200, response.text
    body = response.json()
    assert (body["received"], body["inserted"], body["duplicates"], body["rejected"]) == (
        25,
        25,
        0,
        0,
    )
    assert await count_rows(db_pool, "events") == 25


async def test_bare_json_array_is_accepted(client: httpx.AsyncClient, db_pool: DbPool) -> None:
    response = await client.post("/events/batch", json=[event_payload(), event_payload()])
    assert response.status_code == 200
    assert await count_rows(db_pool, "events") == 2


async def test_duplicates_are_not_stored_twice(client: httpx.AsyncClient, db_pool: DbPool) -> None:
    events = [event_payload() for _ in range(5)]
    first = await client.post("/events/batch", json=[*events, events[0]])  # dup inside a batch
    second = await client.post("/events/batch", json=events)  # retry of the whole batch
    assert (first.json()["inserted"], first.json()["duplicates"]) == (5, 1)
    assert (second.json()["inserted"], second.json()["duplicates"]) == (0, 5)
    assert await count_rows(db_pool, "events") == 5


async def test_mixed_batch_keeps_valid_events_and_dead_letters_invalid_ones(
    client: httpx.AsyncClient, db_pool: DbPool
) -> None:
    events = [
        event_payload(),
        event_payload(city="Paris"),
        event_payload(amount_mad=-5),
        event_payload(),
    ]
    response = await client.post("/events/batch", json={"events": events})
    body = response.json()
    assert response.status_code == 200
    assert (body["inserted"], body["rejected"]) == (2, 2)
    assert [error["index"] for error in body["errors"]] == [1, 2]
    assert await count_rows(db_pool, "events") == 2

    rows = await db_pool.fetch("SELECT reason, raw_payload FROM dead_letter_events ORDER BY id")
    assert [row["reason"] for row in rows] == ["validation_error", "validation_error"]
    assert "Paris" in rows[0]["raw_payload"]


@pytest.mark.parametrize(
    "body",
    [
        b"{not json",
        b"",
        b"\xff\xfe\x00\x00garbage\x00",
        b"[" * 100_000,  # deep nesting: RecursionError in the JSON parser
        b'{"events": [NaN]}',
    ],
    ids=["broken", "empty", "binary-with-nul", "deeply-nested", "nan"],
)
async def test_malformed_body_is_dead_lettered_without_crashing(
    client: httpx.AsyncClient, db_pool: DbPool, body: bytes
) -> None:
    response = await client.post(
        "/events/batch", content=body, headers={"content-type": "application/json"}
    )
    assert response.status_code == 400
    assert await count_rows(db_pool, "dead_letter_events") == 1
    assert (await client.get("/health")).status_code == 200


@pytest.mark.parametrize("payload", [{"events": "nope"}, 42, {"orders": []}])
async def test_wrong_envelope_is_rejected(
    client: httpx.AsyncClient, db_pool: DbPool, payload: object
) -> None:
    response = await client.post("/events/batch", json=payload)
    assert response.status_code == 400
    reason = await db_pool.fetchval("SELECT reason FROM dead_letter_events")
    assert reason == "invalid_envelope"


async def test_too_many_events_in_one_request(start_server: ServerFactory, db_pool: DbPool) -> None:
    server = await start_server(max_events_per_request=3)
    async with httpx.AsyncClient(base_url=server.base_url) as http:
        response = await http.post("/events/batch", json=[event_payload() for _ in range(4)])
    assert response.status_code == 413
    assert await count_rows(db_pool, "events") == 0
    assert await db_pool.fetchval("SELECT reason FROM dead_letter_events") == "payload_too_large"


async def test_oversized_body(start_server: ServerFactory, db_pool: DbPool) -> None:
    server = await start_server(max_request_bytes=1_000)
    async with httpx.AsyncClient(base_url=server.base_url) as http:
        response = await http.post("/events/batch", json=[event_payload() for _ in range(20)])
    assert response.status_code == 413
    assert await count_rows(db_pool, "events") == 0
    assert await count_rows(db_pool, "dead_letter_events") == 1


async def test_full_queue_answers_429_and_stores_nothing(
    start_server: ServerFactory, db_pool: DbPool
) -> None:
    # The bound counts valid events only (invalid ones never reach the queue): 3 > 2.
    server = await start_server(ingest_queue_max_events=2)
    events = [event_payload(), event_payload(), event_payload(), event_payload(city="Paris")]
    async with httpx.AsyncClient(base_url=server.base_url) as http:
        response = await http.post("/events/batch", json=events)
    assert response.status_code == 429
    assert response.headers["retry-after"] == "1"
    assert await count_rows(db_pool, "events") == 0
    assert await count_rows(db_pool, "dead_letter_events") == 0


async def test_single_event_endpoint(client: httpx.AsyncClient, db_pool: DbPool) -> None:
    ok = await client.post("/events", json=event_payload())
    invalid = await client.post("/events", json=event_payload(payment_method="bitcoin"))
    wrong_shape = await client.post("/events", json=[event_payload()])
    assert (ok.status_code, ok.json()["inserted"]) == (200, 1)
    assert invalid.status_code == 422
    assert invalid.json()["errors"][0]["errors"][0]["field"] == "payment_method"
    assert wrong_shape.status_code == 400
    assert await count_rows(db_pool, "events") == 1
    assert await count_rows(db_pool, "dead_letter_events") == 2


async def test_concurrent_requests_are_all_committed(
    client: httpx.AsyncClient, db_pool: DbPool
) -> None:
    batches = [[event_payload() for _ in range(50)] for _ in range(20)]
    responses = await asyncio.gather(*(client.post("/events/batch", json=b) for b in batches))
    assert all(r.status_code == 200 for r in responses)
    assert sum(r.json()["inserted"] for r in responses) == 1_000
    assert await count_rows(db_pool, "events") == 1_000
    writer = (await client.get("/health")).json()["writer"]
    assert writer["batches"] >= 1
    assert writer["insert_seconds_total"] > 0
