"""Database side of alerting: read window totals from aggregates, persist alert history."""

from datetime import datetime, timedelta
from typing import Literal
from uuid import UUID

from pydantic import BaseModel

from rad.alerts.rules import Alert, WindowStats
from rad.db.pool import DbPool

# Two consecutive windows of N minutes on the minute table. The current one ends with the
# still-filling minute. Ranges are half-open on the left so no bucket is counted twice.
_EVENT_WINDOWS_SQL = """
SELECT coalesce(sum(placed_count)    FILTER (WHERE bucket >= $2), 0)::bigint AS placed,
       coalesce(sum(cancelled_count) FILTER (WHERE bucket >= $2), 0)::bigint AS cancelled,
       coalesce(sum(revenue_mad)     FILTER (WHERE bucket >= $2), 0)         AS revenue_recent,
       coalesce(sum(revenue_mad)     FILTER (WHERE bucket <  $2), 0)         AS revenue_previous
FROM agg_minute
WHERE bucket >= $1 AND bucket <= $3
"""

_INGEST_WINDOW_SQL = """
SELECT coalesce(sum(accepted_count), 0)::bigint    AS accepted,
       coalesce(sum(dead_letter_count), 0)::bigint AS dead_letters
FROM ingest_minute
WHERE bucket >= $1 AND bucket <= $2
"""


async def fetch_window_stats(
    pool: DbPool, *, now: datetime, window_minutes: int, last_commit_at: datetime | None
) -> WindowStats:
    current_minute = now.replace(second=0, microsecond=0)
    recent_start = current_minute - timedelta(minutes=window_minutes - 1)
    previous_start = recent_start - timedelta(minutes=window_minutes)
    async with pool.acquire() as conn:
        events = await conn.fetchrow(
            _EVENT_WINDOWS_SQL, previous_start, recent_start, current_minute
        )
        ingest = await conn.fetchrow(_INGEST_WINDOW_SQL, recent_start, current_minute)
    assert events is not None and ingest is not None  # aggregates always return one row
    return WindowStats(
        now=now,
        recent_seconds=(now - recent_start).total_seconds(),
        previous_seconds=window_minutes * 60.0,
        placed=int(events["placed"]),
        cancelled=int(events["cancelled"]),
        revenue_recent=float(events["revenue_recent"]),
        revenue_previous=float(events["revenue_previous"]),
        accepted=int(ingest["accepted"]),
        dead_letters=int(ingest["dead_letters"]),
        last_commit_at=last_commit_at,
    )


class AlertOut(BaseModel):
    """Wire format of an alert (REST and WebSocket)."""

    id: UUID
    rule: str
    severity: Literal["warning", "critical"]
    status: Literal["firing", "resolved"]
    message: str
    value: float | None
    threshold: float
    started_at: datetime
    resolved_at: datetime | None

    @classmethod
    def from_alert(cls, alert: Alert) -> AlertOut:
        return cls(
            id=alert.id,
            rule=alert.rule.value,
            severity=alert.severity.value,
            status=alert.status.value,
            message=alert.message,
            value=alert.value,
            threshold=alert.threshold,
            started_at=alert.started_at,
            resolved_at=alert.resolved_at,
        )


_SAVE_SQL = """
INSERT INTO alerts (id, rule, severity, status, message, value, threshold, started_at, resolved_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (id) DO UPDATE SET
    status      = excluded.status,
    message     = excluded.message,
    value       = excluded.value,
    resolved_at = excluded.resolved_at
"""


async def save_alert(pool: DbPool, alert: Alert) -> None:
    await pool.execute(
        _SAVE_SQL,
        alert.id,
        alert.rule.value,
        alert.severity.value,
        alert.status.value,
        alert.message,
        alert.value,
        alert.threshold,
        alert.started_at,
        alert.resolved_at,
    )


async def resolve_stale_alerts(pool: DbPool, *, now: datetime) -> int:
    """Close alerts left 'firing' by a previous process.

    The alert engine keeps its state in memory, so after a restart it cannot resolve alerts it
    did not fire. If the condition still holds, the new engine simply fires a fresh alert.
    """
    result = await pool.execute(
        "UPDATE alerts SET status = 'resolved', resolved_at = $1 WHERE status = 'firing'", now
    )
    return int(result.split()[-1])  # asyncpg returns the command tag, e.g. "UPDATE 3"


async def fetch_alerts(
    pool: DbPool, *, limit: int, status: Literal["firing", "resolved"] | None = None
) -> list[AlertOut]:
    rows = await pool.fetch(
        "SELECT id, rule, severity, status, message, value, threshold, started_at, resolved_at "
        "FROM alerts WHERE ($1::text IS NULL OR status = $1) "
        "ORDER BY started_at DESC LIMIT $2",
        status,
        limit,
    )
    return [AlertOut.model_validate(dict(row)) for row in rows]
