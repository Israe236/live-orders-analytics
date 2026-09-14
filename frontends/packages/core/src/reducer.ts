import type {
  Alert,
  Breakdown,
  FeedEvent,
  Kpis,
  MetricsSnapshot,
  PipelineStats,
  ServerMessage,
  TimePoint,
} from "./messages";

/**
 * Client-side dashboard state built from server messages.
 *
 * Framework-agnostic and pure: `reduce(state, message)` returns a new state and never mutates.
 * It also preserves object identity for everything that did not change — if the category
 * breakdown is identical to the previous one, the new state holds the *same array*. UI layers
 * that compare by reference (React.memo, Angular OnPush / signals) then skip re-rendering those
 * parts, which is what keeps charts from flickering.
 */
export interface DashboardState {
  snapshot: MetricsSnapshot | null;
  pipeline: PipelineStats | null;
  lastSeq: number;
  /** Newest first. */
  feed: FeedEvent[];
  /** Firing alerts first, then most recent first. */
  alerts: Alert[];
}

export interface ReduceOptions {
  feedLimit: number;
  alertLimit: number;
}

const DEFAULT_OPTIONS: ReduceOptions = { feedLimit: 50, alertLimit: 20 };

export const initialDashboardState: DashboardState = {
  snapshot: null,
  pipeline: null,
  lastSeq: 0,
  feed: [],
  alerts: [],
};

export function reduce(
  state: DashboardState,
  message: ServerMessage,
  options: Partial<ReduceOptions> = {},
): DashboardState {
  const { feedLimit, alertLimit } = { ...DEFAULT_OPTIONS, ...options };
  switch (message.type) {
    case "snapshot": {
      // A snapshot replaces everything, but unchanged parts keep their previous identity.
      const previous = state.snapshot;
      const next = message.data;
      const snapshot: MetricsSnapshot = previous
        ? {
            ...next,
            kpis: sameKpis(previous.kpis, next.kpis) ? previous.kpis : next.kpis,
            revenue_per_minute: sameList(previous.revenue_per_minute, next.revenue_per_minute, samePoint)
              ? previous.revenue_per_minute
              : next.revenue_per_minute,
            revenue_per_hour: sameList(previous.revenue_per_hour, next.revenue_per_hour, samePoint)
              ? previous.revenue_per_hour
              : next.revenue_per_hour,
            orders_by_status: sameRecord(previous.orders_by_status, next.orders_by_status)
              ? previous.orders_by_status
              : next.orders_by_status,
            categories: keepList(previous.categories, next.categories),
            cities: keepList(previous.cities, next.cities),
            payment_methods: keepList(previous.payment_methods, next.payment_methods),
          }
        : next;
      return { ...state, snapshot, pipeline: message.pipeline, lastSeq: message.seq };
    }
    case "update": {
      // An update only makes sense on top of a snapshot, and never goes back in time.
      const previous = state.snapshot;
      if (previous === null || message.seq <= state.lastSeq) return state;
      const update = message.data;
      const snapshot: MetricsSnapshot = {
        generated_at: update.generated_at,
        kpis: sameKpis(previous.kpis, update.kpis) ? previous.kpis : update.kpis,
        revenue_per_minute: mergeTail(previous.revenue_per_minute, update.revenue_per_minute_tail),
        revenue_per_hour: mergeTail(previous.revenue_per_hour, update.revenue_per_hour_tail),
        orders_by_status: sameRecord(previous.orders_by_status, update.orders_by_status)
          ? previous.orders_by_status
          : update.orders_by_status,
        categories: keepList(previous.categories, update.categories),
        cities: keepList(previous.cities, update.cities),
        payment_methods: keepList(previous.payment_methods, update.payment_methods),
        ingest: update.ingest,
      };
      return { ...state, snapshot, pipeline: message.pipeline, lastSeq: message.seq };
    }
    case "events": {
      if (message.items.length === 0) return state;
      const known = new Set(state.feed.map((event) => event.event_id));
      const fresh = message.items.filter((event) => !known.has(event.event_id));
      if (fresh.length === 0) return state;
      return { ...state, feed: [...fresh, ...state.feed].slice(0, feedLimit) };
    }
    case "alert": {
      const existing = state.alerts.find((alert) => alert.id === message.alert.id);
      // Alerts only move firing → resolved. A "firing" copy arriving after the "resolved" one
      // (e.g. REST history loaded after the WebSocket pushed the resolution) is stale.
      if (existing?.status === "resolved" && message.alert.status === "firing") return state;
      const others = state.alerts.filter((alert) => alert.id !== message.alert.id);
      const alerts = [message.alert, ...others].sort(compareAlerts).slice(0, alertLimit);
      return { ...state, alerts };
    }
  }
}

/**
 * Merge the newest points of a time series into the current one, matching by bucket time.
 * Existing buckets are replaced, newer buckets are appended and the oldest points dropped so the
 * window length stays the same. Returns the *same array* when nothing changed.
 */
export function mergeTail(series: TimePoint[], tail: TimePoint[]): TimePoint[] {
  if (tail.length === 0) return series;
  const result = series.slice();
  let changed = false;
  for (const point of tail) {
    const time = Date.parse(point.bucket);
    const index = findIndexFromEnd(result, (p) => Date.parse(p.bucket) === time);
    if (index >= 0) {
      if (!samePoint(result[index]!, point)) {
        result[index] = point;
        changed = true;
      }
    } else if (result.length === 0 || time > Date.parse(result[result.length - 1]!.bucket)) {
      result.push(point);
      changed = true;
    }
    // Otherwise the point is older than the window and unknown: ignore it.
  }
  if (!changed) return series;
  if (series.length > 0 && result.length > series.length) {
    result.splice(0, result.length - series.length);
  }
  return result;
}

function findIndexFromEnd<T>(items: T[], predicate: (item: T) => boolean): number {
  for (let i = items.length - 1; i >= 0; i--) {
    if (predicate(items[i]!)) return i;
  }
  return -1;
}

function compareAlerts(a: Alert, b: Alert): number {
  if (a.status !== b.status) return a.status === "firing" ? -1 : 1;
  return Date.parse(b.started_at) - Date.parse(a.started_at);
}

function samePoint(a: TimePoint, b: TimePoint): boolean {
  return (
    a.bucket === b.bucket &&
    a.revenue_mad === b.revenue_mad &&
    a.orders_placed === b.orders_placed &&
    a.orders_paid === b.orders_paid &&
    a.orders_cancelled === b.orders_cancelled
  );
}

function sameBreakdown(a: Breakdown, b: Breakdown): boolean {
  return (
    a.value === b.value &&
    a.revenue_mad === b.revenue_mad &&
    a.orders_placed === b.orders_placed &&
    a.orders_paid === b.orders_paid &&
    a.orders_cancelled === b.orders_cancelled
  );
}

function sameKpis(a: Kpis, b: Kpis): boolean {
  return sameRecord(a as unknown as Record<string, unknown>, b as unknown as Record<string, unknown>);
}

function sameRecord<T extends Record<string, unknown>>(a: T, b: T): boolean {
  const keys = Object.keys(a);
  return keys.length === Object.keys(b).length && keys.every((key) => a[key] === b[key]);
}

function sameList<T>(a: T[], b: T[], same: (x: T, y: T) => boolean): boolean {
  return a.length === b.length && a.every((item, i) => same(item, b[i]!));
}

function keepList(previous: Breakdown[], next: Breakdown[]): Breakdown[] {
  return sameList(previous, next, sameBreakdown) ? previous : next;
}
