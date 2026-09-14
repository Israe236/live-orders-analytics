import { describe, expect, it } from "vitest";

import type { ConnectionState } from "./connection";
import { dataAgeMs, describeConnection, formatAge, formatPercent } from "./format";

const base: ConnectionState = { status: "open", attempt: 0, nextRetryAt: null, lastMessageAt: null };

describe("describeConnection", () => {
  const now = 1_000_000;

  it.each<[Partial<ConnectionState>, string]>([
    [{ status: "connecting" }, "Connecting…"],
    [{ status: "open" }, "Live"],
    [{ status: "reconnecting", attempt: 2, nextRetryAt: now + 2_500 }, "Reconnecting in 3s (attempt 2)"],
    [{ status: "reconnecting", attempt: 1, nextRetryAt: null }, "Reconnecting… (attempt 1)"],
    [{ status: "offline" }, "Offline, waiting for the network"],
    [{ status: "closed" }, "Disconnected"],
  ])("%o → %s", (patch, expected) => {
    expect(describeConnection({ ...base, ...patch }, now)).toBe(expected);
  });
});

describe("formatting helpers", () => {
  it("formats percentages and missing values", () => {
    expect(formatPercent(0.1234)).toBe("12.3%");
    expect(formatPercent(null)).toBe("—");
  });

  it("formats data age", () => {
    expect(formatAge(870)).toBe("0.9 s");
    expect(formatAge(75_000)).toBe("1 min");
    expect(formatAge(null)).toBe("—");
  });

  it("computes the age of the newest committed event", () => {
    const pipeline = {
      events_per_second: 1,
      last_event_occurred_at: "2026-09-13T20:00:00.000Z",
      last_commit_at: null,
      connected_clients: 1,
    };
    expect(dataAgeMs(pipeline, Date.parse("2026-09-13T20:00:01.500Z"))).toBe(1_500);
    expect(dataAgeMs(null)).toBeNull();
  });
});
