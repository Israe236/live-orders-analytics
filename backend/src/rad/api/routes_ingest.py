"""Ingestion endpoints.

Rule: bad input is recorded in ``dead_letter_events`` and answered with a 4xx — never a 500.
The body is read and parsed by hand (instead of letting FastAPI parse it into a model) so that
malformed JSON, wrong shapes and oversized payloads can all be captured and stored.
"""

import json
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from rad.api.services import Services, ServicesDep
from rad.common.events import TimeWindow, validate_events
from rad.processing.dead_letter import (
    DeadLetter,
    DeadLetterReason,
    store_dead_letters,
    to_raw_text,
)
from rad.processing.writer import QueueFullError, WriterUnavailableError

router = APIRouter(tags=["ingestion"])

_MAX_ERRORS_IN_RESPONSE = 100


class IngestError(BaseModel):
    index: int
    errors: list[dict[str, str]]


class IngestResponse(BaseModel):
    received: int = 0
    inserted: int = 0
    duplicates: int = 0
    rejected: int = 0
    errors: list[IngestError] = Field(default_factory=list)
    message: str | None = None


class _RejectedRequestError(Exception):
    """The request as a whole is unusable; it is dead-lettered and answered with a 4xx."""

    def __init__(self, status: int, letter: DeadLetter, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.letter = letter
        self.message = message


def _preview(body: bytes, max_chars: int) -> str:
    # Decoding is lossy on purpose: the point is to keep something readable for debugging.
    return body[: max_chars * 4].decode("utf-8", errors="replace")[:max_chars]


def _reject_json_constant(name: str) -> Any:
    raise ValueError(f"{name} is not valid JSON")


async def _read_json(request: Request, services: Services) -> Any:
    settings = services.settings
    limit = settings.max_request_bytes
    preview_chars = settings.dead_letter_payload_max_chars

    declared = request.headers.get("content-length", "")
    too_large_message = f"request body exceeds {limit} bytes"
    if declared.isdigit() and int(declared) > limit:
        letter = DeadLetter(DeadLetterReason.PAYLOAD_TOO_LARGE, "", {"content_length": declared})
        raise _RejectedRequestError(413, letter, too_large_message)

    body = bytearray()
    async for chunk in request.stream():
        body += chunk
        if len(body) > limit:
            letter = DeadLetter(
                DeadLetterReason.PAYLOAD_TOO_LARGE, _preview(bytes(body), preview_chars)
            )
            raise _RejectedRequestError(413, letter, too_large_message)

    try:
        # parse_float=Decimal keeps amounts exact (no binary floating point rounding).
        return json.loads(bytes(body), parse_float=Decimal, parse_constant=_reject_json_constant)
    # JSONDecodeError and UnicodeDecodeError are ValueErrors; absurd nesting is a RecursionError.
    except (ValueError, RecursionError) as exc:
        letter = DeadLetter(
            DeadLetterReason.MALFORMED_JSON,
            _preview(bytes(body), preview_chars),
            {"error": str(exc)[:500]},
        )
        raise _RejectedRequestError(400, letter, "request body is not valid JSON") from exc


async def _ingest_items(
    services: Services, items: list[Any]
) -> tuple[int, dict[str, str], IngestResponse]:
    settings = services.settings
    window = TimeWindow(
        now=datetime.now(UTC),
        max_future=settings.max_future_skew,
        max_age=settings.max_event_age,
    )
    valid, invalid = validate_events(items, window)

    try:
        result = await services.writer.submit(valid)
    except QueueFullError:
        # Nothing is stored (not even dead letters): the client is expected to retry the batch.
        response = IngestResponse(
            received=len(items), message="ingestion queue is full, retry later"
        )
        return 429, {"Retry-After": "1"}, response
    except WriterUnavailableError as exc:
        return 503, {"Retry-After": "2"}, IngestResponse(received=len(items), message=str(exc))

    await store_dead_letters(
        services.pool,
        [
            DeadLetter(
                DeadLetterReason.VALIDATION_ERROR,
                to_raw_text(item.raw),
                {"index": item.index, "errors": item.errors},
            )
            for item in invalid
        ],
        max_payload_chars=settings.dead_letter_payload_max_chars,
    )
    response = IngestResponse(
        received=len(items),
        inserted=result.inserted,
        duplicates=result.duplicates,
        rejected=len(invalid),
        errors=[
            IngestError(index=item.index, errors=item.errors)
            for item in invalid[:_MAX_ERRORS_IN_RESPONSE]
        ],
    )
    return 200, {}, response


def _json_response(
    status: int, body: IngestResponse, headers: dict[str, str] | None = None
) -> JSONResponse:
    return JSONResponse(status_code=status, content=body.model_dump(mode="json"), headers=headers)


async def _rejected(services: Services, exc: _RejectedRequestError) -> JSONResponse:
    await store_dead_letters(
        services.pool,
        [exc.letter],
        max_payload_chars=services.settings.dead_letter_payload_max_chars,
    )
    return _json_response(exc.status, IngestResponse(message=exc.message))


@router.post("/events/batch", response_model=IngestResponse)
async def ingest_batch(request: Request, services: ServicesDep) -> JSONResponse:
    """Ingest many events: ``{"events": [...]}`` or a bare JSON array.

    Valid events are stored even when others in the same batch are invalid.
    """
    settings = services.settings
    try:
        payload = await _read_json(request, services)
        items = payload.get("events") if isinstance(payload, dict) else payload
        if not isinstance(items, list):
            raise _RejectedRequestError(
                400,
                DeadLetter(
                    DeadLetterReason.INVALID_ENVELOPE,
                    to_raw_text(payload)[: settings.dead_letter_payload_max_chars],
                ),
                'expected {"events": [...]} or a JSON array of events',
            )
        if len(items) > settings.max_events_per_request:
            raise _RejectedRequestError(
                413,
                DeadLetter(
                    DeadLetterReason.PAYLOAD_TOO_LARGE,
                    to_raw_text(items[:10]),
                    {"events": len(items), "limit": settings.max_events_per_request},
                ),
                f"batch has {len(items)} events; the limit is {settings.max_events_per_request}",
            )
    except _RejectedRequestError as exc:
        return await _rejected(services, exc)

    status, headers, response = await _ingest_items(services, items)
    return _json_response(status, response, headers)


@router.post("/events", response_model=IngestResponse)
async def ingest_one(request: Request, services: ServicesDep) -> JSONResponse:
    """Ingest a single event object. Answers 422 (and dead-letters it) when it is invalid."""
    try:
        payload = await _read_json(request, services)
        if not isinstance(payload, dict):
            raise _RejectedRequestError(
                400,
                DeadLetter(DeadLetterReason.INVALID_ENVELOPE, to_raw_text(payload)[:1000]),
                "expected a single JSON object; use /events/batch for many events",
            )
    except _RejectedRequestError as exc:
        return await _rejected(services, exc)

    status, headers, response = await _ingest_items(services, [payload])
    if status == 200 and response.rejected:
        status = 422
    return _json_response(status, response, headers)
