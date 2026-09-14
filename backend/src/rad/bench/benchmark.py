"""Load and latency benchmark for the running stack.

Run it inside the compose network, so the benchmark and the API share the same clock (the
latency measurement compares client and server timestamps):

    docker compose stop generator
    docker compose run --rm --no-deps generator python -m rad.bench > result.json

Two phases:

1. **Sustained throughput (closed loop).** N senders post batches as fast as the API accepts
   them for D seconds, after a warm-up. Reported: events committed per second (counted from the
   API's "inserted" answers, by completion time), HTTP acknowledgement latency, 429 responses.

2. **End-to-end latency (open loop, fixed rate).** Events are sent at a steady offered rate while
   a WebSocket client listens like a dashboard. For each batch:
   ``end-to-end = time the first dashboard update that includes it is received - event creation``.
   A state message includes the batch when its ``generated_at`` is later than the moment the API
   acknowledged the batch (the acknowledgement comes after the commit, so this is a slightly
   pessimistic upper bound). The value includes the wait for the next 1-second push tick.
"""

import argparse
import asyncio
import contextlib
import json
import os
import platform
import random
import sys
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid7

import httpx
from websockets.asyncio.client import connect

from rad.common.events import Category, City, EventType, PaymentMethod

type Payload = dict[str, Any]

_TYPES = [t.value for t in EventType]
_CATEGORIES = [c.value for c in Category]
_CITIES = [c.value for c in City]
_PAYMENTS = [p.value for p in PaymentMethod]


def make_batch(size: int, rng: random.Random) -> tuple[list[Payload], float]:
    """A batch of valid events created now. Returns the events and their creation time."""
    created = time.time()
    occurred_at = datetime.fromtimestamp(created, UTC).isoformat()
    events = [
        {
            "event_id": str(uuid7()),
            "order_id": str(uuid7()),
            "event_type": rng.choice(_TYPES),
            "occurred_at": occurred_at,
            "amount_mad": f"{rng.uniform(50, 5_000):.2f}",
            "category": rng.choice(_CATEGORIES),
            "city": rng.choice(_CITIES),
            "payment_method": rng.choice(_PAYMENTS),
        }
        for _ in range(size)
    ]
    return events, created


def percentile(values: list[float], pct: float) -> float | None:
    """Linear-interpolated percentile; None for an empty list."""
    if not values:
        return None
    ordered = sorted(values)
    rank = (len(ordered) - 1) * pct / 100
    low = int(rank)
    high = min(low + 1, len(ordered) - 1)
    return ordered[low] + (ordered[high] - ordered[low]) * (rank - low)


def summarize_ms(values: list[float]) -> dict[str, float | int | None]:
    def rounded(value: float | None) -> float | None:
        return None if value is None else round(value, 1)

    return {
        "count": len(values),
        "p50": rounded(percentile(values, 50)),
        "p95": rounded(percentile(values, 95)),
        "p99": rounded(percentile(values, 99)),
        "max": rounded(max(values)) if values else None,
    }


@dataclass
class _Tally:
    accepted: int = 0
    requests: int = 0
    throttled: int = 0
    errors: int = 0
    ack_ms: list[float] = field(default_factory=list)


async def run_throughput(
    client: httpx.AsyncClient, *, senders: int, batch_size: int, duration_s: float, warmup_s: float
) -> dict[str, Any]:
    tally = _Tally()
    measure_from = time.monotonic() + warmup_s
    stop_at = measure_from + duration_s

    async def sender(seed: int) -> None:
        rng = random.Random(seed)
        while time.monotonic() < stop_at:
            events, _ = make_batch(batch_size, rng)
            sent = time.monotonic()
            try:
                response = await client.post("/events/batch", json=events)
            except httpx.HTTPError:
                tally.errors += 1
                continue
            done = time.monotonic()
            in_window = measure_from <= done <= stop_at
            if response.status_code == 200:
                if in_window:
                    tally.accepted += int(response.json()["inserted"])
                    tally.requests += 1
                    tally.ack_ms.append((done - sent) * 1000)
            elif response.status_code == 429:
                if in_window:
                    tally.throttled += 1
                await asyncio.sleep(float(response.headers.get("retry-after", "1")))
            else:
                tally.errors += 1

    await asyncio.gather(*(sender(i) for i in range(senders)))
    return {
        "senders": senders,
        "batch_size": batch_size,
        "warmup_s": warmup_s,
        "measured_s": duration_s,
        "events_committed": tally.accepted,
        "events_per_second": round(tally.accepted / duration_s, 1),
        "requests": tally.requests,
        "throttled_429": tally.throttled,
        "errors": tally.errors,
        "ack_latency_ms": summarize_ms(tally.ack_ms),
    }


@dataclass
class _LatencyState:
    # (acknowledged_at, created_at) in acknowledgement order, waiting for a dashboard update.
    pending: deque[tuple[float, float]] = field(default_factory=deque)
    end_to_end_ms: list[float] = field(default_factory=list)
    ack_ms: list[float] = field(default_factory=list)
    failed: int = 0
    state_messages: int = 0


async def run_latency(
    client: httpx.AsyncClient,
    ws_url: str,
    *,
    rate_eps: float,
    batch_size: int,
    duration_s: float,
) -> dict[str, Any]:
    state = _LatencyState()

    async def listen() -> None:
        async with connect(ws_url, max_size=None) as ws:
            async for raw in ws:
                received = time.time()
                message = json.loads(raw)
                if message.get("type") not in ("snapshot", "update"):
                    continue
                state.state_messages += 1
                generated = datetime.fromisoformat(message["data"]["generated_at"]).timestamp()
                while state.pending and state.pending[0][0] < generated:
                    _, created = state.pending.popleft()
                    state.end_to_end_ms.append((received - created) * 1000)

    async def send_one(rng: random.Random) -> None:
        events, created = make_batch(batch_size, rng)
        try:
            response = await client.post("/events/batch", json=events)
        except httpx.HTTPError:
            state.failed += 1
            return
        acknowledged = time.time()
        if response.status_code != 200:
            state.failed += 1
            return
        state.ack_ms.append((acknowledged - created) * 1000)
        state.pending.append((acknowledged, created))

    listener = asyncio.create_task(listen())
    await asyncio.sleep(1.5)  # connected and first snapshot received
    rng = random.Random(42)
    interval = batch_size / rate_eps
    loop = asyncio.get_running_loop()
    start = loop.time()
    sent = 0
    async with asyncio.TaskGroup() as group:
        while start + sent * interval < start + duration_s:
            await asyncio.sleep(max(0.0, start + sent * interval - loop.time()))
            group.create_task(send_one(rng))
            sent += 1
    for _ in range(50):  # give the last batches up to 5 s to reach a dashboard update
        if not state.pending:
            break
        await asyncio.sleep(0.1)
    listener.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await listener

    return {
        "offered_events_per_second": rate_eps,
        "batch_size": batch_size,
        "duration_s": duration_s,
        "batches_sent": sent,
        "batches_failed": state.failed,
        "batches_never_seen_on_dashboard": len(state.pending),
        "dashboard_messages_received": state.state_messages,
        "ack_latency_ms": summarize_ms(state.ack_ms),
        "end_to_end_latency_ms": summarize_ms(state.end_to_end_ms),
    }


def machine_info() -> dict[str, Any]:
    info: dict[str, Any] = {
        "python": platform.python_version(),
        "platform": platform.platform(),
        "logical_cpus": os.cpu_count(),
    }
    cpuinfo = Path("/proc/cpuinfo")
    if cpuinfo.exists():
        for line in cpuinfo.read_text().splitlines():
            if line.startswith("model name"):
                info["cpu_model"] = line.split(":", 1)[1].strip()
                break
    meminfo = Path("/proc/meminfo")
    if meminfo.exists():
        for line in meminfo.read_text().splitlines():
            if line.startswith("MemTotal"):
                info["memory_gb"] = round(int(line.split()[1]) / 1024 / 1024, 1)
                break
    return info


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="python -m rad.bench", description=__doc__.split("\n")[0])
    parser.add_argument("--api-url", default=os.environ.get("RAD_BENCH_API_URL", "http://api:8000"))
    parser.add_argument("--ws-url", default=None, help="defaults to the API URL + /ws/live")
    parser.add_argument("--senders", type=int, default=8)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--duration", type=float, default=60.0)
    parser.add_argument("--warmup", type=float, default=10.0)
    parser.add_argument("--latency-rate", type=float, default=500.0, help="offered events/s")
    parser.add_argument("--latency-batch-size", type=int, default=50)
    parser.add_argument("--latency-duration", type=float, default=60.0)
    parser.add_argument("--skip-throughput", action="store_true")
    parser.add_argument("--skip-latency", action="store_true")
    return parser.parse_args(argv)


def _log(message: str) -> None:
    print(message, file=sys.stderr, flush=True)


async def _run(args: argparse.Namespace) -> dict[str, Any]:
    ws_url = args.ws_url or args.api_url.replace("http", "ws", 1).rstrip("/") + "/ws/live"
    limits = httpx.Limits(
        max_connections=args.senders + 16, max_keepalive_connections=args.senders + 16
    )
    result: dict[str, Any] = {
        "started_at": datetime.now(UTC).isoformat(),
        "machine": machine_info(),
        "settings": vars(args) | {"ws_url": ws_url},
    }
    async with httpx.AsyncClient(base_url=args.api_url, timeout=30, limits=limits) as client:
        health = await client.get("/health")
        health.raise_for_status()
        if not args.skip_throughput:
            _log(
                f"throughput: {args.senders} senders x {args.batch_size} events, "
                f"{args.duration:.0f} s"
            )
            result["throughput"] = await run_throughput(
                client,
                senders=args.senders,
                batch_size=args.batch_size,
                duration_s=args.duration,
                warmup_s=args.warmup,
            )
            _log(json.dumps(result["throughput"]))
            await asyncio.sleep(5)  # let the write queue drain before the latency phase
        if not args.skip_latency:
            _log(
                f"latency: {args.latency_rate:.0f} events/s offered for "
                f"{args.latency_duration:.0f} s"
            )
            result["latency"] = await run_latency(
                client,
                ws_url,
                rate_eps=args.latency_rate,
                batch_size=args.latency_batch_size,
                duration_s=args.latency_duration,
            )
            _log(json.dumps(result["latency"]))
    result["finished_at"] = datetime.now(UTC).isoformat()
    return result


def main(argv: list[str] | None = None) -> None:
    result = asyncio.run(_run(_parse_args(argv)))
    print(json.dumps(result, indent=2))
