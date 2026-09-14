import { describe, expect, it } from "vitest";

import type {
  Alert,
  Breakdown,
  FeedEvent,
  MetricsSnapshot,
  PipelineStats,
  SnapshotMessage,
  TimePoint,
  UpdateMessage,
} from "./messages";
import { initialDashboardState, mergeTail, reduce, type DashboardState } from "./reducer";

const pipeline: PipelineStats = {
  events_per_second: 40,
  last_event_occurred_at: "2026-09-13T20:00:00Z",
  last_commit_at: "2026-09-13T20:00:00Z",
  connected_clients: 1,
};

function minute(m: number): string {
  return new Date(Date.UTC(2026, 8, 13, 20, m)).toISOString();
}

function point(m: number, revenue: number): TimePoint {
  return { bucket: minute(m), revenue_mad: revenue, orders_placed: 1, orders_paid: 1, orders_cancelled: 0 };
}

function breakdown(value: string, revenue: number): Breakdown {
  return { value, revenue_mad: revenue, orders_placed: 1, orders_paid: 1, orders_cancelled: 0 };
}

function snapshotData(overrides: Partial<MetricsSnapshot> = {}): MetricsSnapshot {
  return {
    generated_at: minute(2),
    kpis: {
      window_minutes: 60,
      orders_placed: 10,
      orders_paid: 8,
      orders_shipped: 5,
      orders_cancelled: 1,
      revenue_mad: 1_000,
      avg_order_value_mad: 125,
      cancellation_rate: 0.1,
    },
    revenue_per_minute: [point(0, 10), point(1, 20), point(2, 30)],
    revenue_per_hour: [point(0, 60)],
    orders_by_status: { placed: 2, paid: 3, shipped: 5, cancelled: 1 },
    categories: [breakdown("electronics", 700), breakdown("fashion", 300)],
    cities: [breakdown("Casablanca", 600)],
    payment_methods: [breakdown("card", 1_000)],
    ingest: { window_minutes: 5, accepted: 100, duplicates: 0, dead_letters: 1, events_per_second: 0.3 },
    ...overrides,
  };
}

function snapshot(seq: number, overrides: Partial<MetricsSnapshot> = {}): SnapshotMessage {
  return { type: "snapshot", seq, sent_at: minute(2), pipeline, data: snapshotData(overrides) };
}

function update(seq: number, tail: TimePoint[], overrides: Partial<MetricsSnapshot> = {}): UpdateMessage {
  const data = snapshotData(overrides);
  return {
    type: "update",
    seq,
    sent_at: minute(2),
    pipeline,
    data: {
      generated_at: data.generated_at,
      kpis: data.kpis,
      revenue_per_minute_tail: tail,
      revenue_per_hour_tail: data.revenue_per_hour.slice(-1),
      orders_by_status: data.orders_by_status,
      categories: data.categories,
      cities: data.cities,
      payment_methods: data.payment_methods,
      ingest: data.ingest,
    },
  };
}

function started(): DashboardState {
  return reduce(initialDashboardState, snapshot(1));
}

describe("reduce", () => {
  it("ignores updates until a snapshot has arrived", () => {
    const state = reduce(initialDashboardState, update(1, [point(2, 99)]));
    expect(state).toBe(initialDashboardState);
  });

  it("merges the tail: replaces the current minute, appends a new one, keeps 60-style length", () => {
    const state = reduce(started(), update(2, [point(1, 20), point(2, 35), point(3, 5)]));
    expect(state.snapshot?.revenue_per_minute.map((p) => [p.bucket, p.revenue_mad])).toEqual([
      [minute(1), 20],
      [minute(2), 35],
      [minute(3), 5],
    ]);
    expect(state.lastSeq).toBe(2);
  });

  it("keeps the same object for parts that did not change (so UIs skip re-rendering)", () => {
    const before = started();
    const after = reduce(before, update(2, [point(2, 30)], { categories: [breakdown("electronics", 700), breakdown("fashion", 300)] }));
    expect(after.snapshot?.revenue_per_minute).toBe(before.snapshot?.revenue_per_minute);
    expect(after.snapshot?.categories).toBe(before.snapshot?.categories);
    expect(after.snapshot?.kpis).toBe(before.snapshot?.kpis);
    expect(after.snapshot?.orders_by_status).toBe(before.snapshot?.orders_by_status);

    const changed = reduce(after, update(3, [point(2, 31)], { categories: [breakdown("electronics", 701)] }));
    expect(changed.snapshot?.revenue_per_minute).not.toBe(after.snapshot?.revenue_per_minute);
    expect(changed.snapshot?.categories).not.toBe(after.snapshot?.categories);
    expect(changed.snapshot?.cities).toBe(after.snapshot?.cities);
  });

  it("drops out-of-order updates", () => {
    const state = reduce(started(), update(5, [point(2, 50)]));
    expect(reduce(state, update(4, [point(2, 40)]))).toBe(state);
  });

  it("a periodic snapshot also preserves identity of equal parts", () => {
    const before = started();
    const after = reduce(before, snapshot(2, { revenue_per_hour: [point(0, 61)] }));
    expect(after.snapshot?.revenue_per_minute).toBe(before.snapshot?.revenue_per_minute);
    expect(after.snapshot?.revenue_per_hour).not.toBe(before.snapshot?.revenue_per_hour);
  });

  it("prepends new feed events, skips duplicates and caps the list", () => {
    const event = (id: string): FeedEvent => ({
      event_id: id,
      order_id: "o",
      event_type: "order_paid",
      occurred_at: minute(2),
      amount_mad: 10,
      category: "books",
      city: "Rabat",
      payment_method: "card",
    });
    let state = reduce(started(), { type: "events", seq: 1, items: [event("b"), event("a")] });
    state = reduce(state, { type: "events", seq: 2, items: [event("c"), event("b")] }, { feedLimit: 2 });
    expect(state.feed.map((e) => e.event_id)).toEqual(["c", "b"]);
  });

  it("upserts alerts by id and lists firing ones first", () => {
    const alert = (id: string, status: Alert["status"], m: number): Alert => ({
      id,
      rule: "revenue_drop",
      severity: "critical",
      status,
      message: id,
      value: 0.6,
      threshold: 0.5,
      started_at: minute(m),
      resolved_at: status === "resolved" ? minute(m + 1) : null,
    });
    let state = started();
    state = reduce(state, { type: "alert", seq: 1, alert: alert("old", "firing", 0) });
    state = reduce(state, { type: "alert", seq: 2, alert: alert("new", "firing", 5) });
    state = reduce(state, { type: "alert", seq: 3, alert: alert("new", "resolved", 5) });
    expect(state.alerts.map((a) => [a.id, a.status])).toEqual([
      ["old", "firing"],
      ["new", "resolved"],
    ]);

    // A stale "firing" copy of an alert already known as resolved is ignored.
    expect(reduce(state, { type: "alert", seq: 4, alert: alert("new", "firing", 5) })).toBe(state);
  });
});

describe("mergeTail", () => {
  it("returns the same array when nothing changes", () => {
    const series = [point(0, 1), point(1, 2)];
    expect(mergeTail(series, [point(1, 2)])).toBe(series);
    expect(mergeTail(series, [])).toBe(series);
  });

  it("ignores unknown points older than the window", () => {
    const series = [point(5, 1), point(6, 2)];
    expect(mergeTail(series, [point(1, 100)])).toBe(series);
  });
});
