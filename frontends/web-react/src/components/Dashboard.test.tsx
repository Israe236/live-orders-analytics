import {
  initialDashboardState,
  reduce,
  type MetricsSnapshot,
  type SnapshotMessage,
  type TimePoint,
  type UpdateMessage,
} from "@rad/core";
import { render } from "@testing-library/react";
import { beforeEach, describe, expect, it } from "vitest";

import { renderTrace, resetRenderTrace } from "../renderTrace";
import { Dashboard } from "./Dashboard";

const pipeline = {
  events_per_second: 40,
  last_event_occurred_at: null,
  last_commit_at: null,
  connected_clients: 1,
};

function point(minute: number, revenue: number): TimePoint {
  return {
    bucket: new Date(Date.UTC(2026, 8, 13, 20, minute)).toISOString(),
    revenue_mad: revenue,
    orders_placed: 1,
    orders_paid: 1,
    orders_cancelled: 0,
  };
}

function data(revenueAtMinute2: number, fashionRevenue: number): MetricsSnapshot {
  return {
    generated_at: "2026-09-13T20:02:00Z",
    kpis: {
      window_minutes: 60,
      orders_placed: 10,
      orders_paid: 8,
      orders_shipped: 4,
      orders_cancelled: 1,
      revenue_mad: 1_000,
      avg_order_value_mad: 125,
      cancellation_rate: 0.1,
    },
    revenue_per_minute: [point(0, 10), point(1, 20), point(2, revenueAtMinute2)],
    revenue_per_hour: [point(0, 60)],
    orders_by_status: { placed: 2, paid: 3, shipped: 4, cancelled: 1 },
    categories: [
      { value: "electronics", orders_placed: 5, orders_paid: 4, orders_cancelled: 0, revenue_mad: 700 },
      { value: "fashion", orders_placed: 5, orders_paid: 4, orders_cancelled: 1, revenue_mad: fashionRevenue },
    ],
    cities: [{ value: "Rabat", orders_placed: 10, orders_paid: 8, orders_cancelled: 1, revenue_mad: 1_000 }],
    payment_methods: [],
    ingest: { window_minutes: 5, accepted: 100, duplicates: 0, dead_letters: 0, events_per_second: 1 },
  };
}

function snapshotMessage(seq: number, snapshot: MetricsSnapshot): SnapshotMessage {
  return { type: "snapshot", seq, sent_at: snapshot.generated_at, pipeline, data: snapshot };
}

function updateMessage(seq: number, snapshot: MetricsSnapshot): UpdateMessage {
  return {
    type: "update",
    seq,
    sent_at: snapshot.generated_at,
    pipeline,
    data: {
      generated_at: snapshot.generated_at,
      kpis: snapshot.kpis,
      revenue_per_minute_tail: snapshot.revenue_per_minute.slice(-3),
      revenue_per_hour_tail: snapshot.revenue_per_hour.slice(-1),
      orders_by_status: snapshot.orders_by_status,
      categories: snapshot.categories,
      cities: snapshot.cities,
      payment_methods: snapshot.payment_methods,
      ingest: snapshot.ingest,
    },
  };
}

describe("Dashboard re-rendering", () => {
  beforeEach(resetRenderTrace);

  it("re-renders only the sections whose data changed", () => {
    let state = reduce(initialDashboardState, snapshotMessage(1, data(30, 300)));
    const view = (s: typeof state) => (
      <Dashboard snapshot={s.snapshot!} eventsPerSecond={40} feed={s.feed} alerts={s.alerts} />
    );
    const { rerender } = render(view(state));
    resetRenderTrace();

    // New revenue for the current minute; everything else identical (as JSON, not by reference).
    state = reduce(state, updateMessage(2, data(45, 300)));
    rerender(view(state));

    expect(renderTrace.revenue).toBe(1);
    expect(renderTrace.categories).toBeUndefined();
    expect(renderTrace.kpis).toBeUndefined();
    expect(renderTrace.status).toBeUndefined();
    expect(renderTrace.cities).toBeUndefined();
    expect(renderTrace.feed).toBeUndefined();

    // Now the category breakdown changes too.
    resetRenderTrace();
    state = reduce(state, updateMessage(3, data(45, 310)));
    rerender(view(state));
    expect(renderTrace.categories).toBe(1);
    expect(renderTrace.revenue).toBeUndefined();
  });
});
