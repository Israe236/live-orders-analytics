"""Dead-letter storage: anything that cannot be ingested is kept, with the reason."""

import json
import logging
from collections.abc import Sequence
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from typing import Any

from rad.db.pool import DbPool

log = logging.getLogger(__name__)


class DeadLetterReason(StrEnum):
    MALFORMED_JSON = "malformed_json"
    INVALID_ENVELOPE = "invalid_envelope"
    VALIDATION_ERROR = "validation_error"
    PAYLOAD_TOO_LARGE = "payload_too_large"


@dataclass(frozen=True, slots=True)
class DeadLetter:
    reason: DeadLetterReason
    raw_payload: str
    detail: Any = None


def _json_default(value: object) -> object:
    return float(value) if isinstance(value, Decimal) else str(value)


def to_raw_text(value: Any) -> str:
    """Serialize an already-parsed JSON value back to text for storage."""
    return json.dumps(value, default=_json_default, ensure_ascii=False)


def _strip_nul(text: str) -> str:
    # PostgreSQL text and jsonb cannot store the NUL character; garbage input may contain it.
    return text.replace("\x00", "�").replace("\\u0000", "\\ufffd")


_INSERT_SQL = """
INSERT INTO dead_letter_events (reason, error_detail, raw_payload)
SELECT * FROM unnest($1::text[], $2::jsonb[], $3::text[])
"""


async def store_dead_letters(
    pool: DbPool, letters: Sequence[DeadLetter], *, max_payload_chars: int
) -> bool:
    """Best effort: a failure is logged, never raised. Rejecting bad input must not crash."""
    if not letters:
        return True
    try:
        await pool.execute(
            _INSERT_SQL,
            [letter.reason.value for letter in letters],
            [_strip_nul(json.dumps(letter.detail, default=_json_default)) for letter in letters],
            [_strip_nul(letter.raw_payload[:max_payload_chars]) for letter in letters],
        )
    except Exception:
        log.exception("failed to store %d dead letters", len(letters))
        return False
    return True
