/**
 * Types of the messages pushed by the API over `/ws/live`, mirroring the Pydantic models in
 * `backend/src/rad/common/{metrics,live}.py` and `backend/src/rad/alerts/store.py`.
 */

/** ISO-8601 timestamp string, always UTC. */
export type IsoDate = string;

export type OrderStatus = "placed" | "paid" | "shipped" | "cancelled";

export interface Kpis {
  window_minutes: number;
  orders_placed: number;
  orders_paid: number;
  orders_shipped: number;
  orders_cancelled: number;
  revenue_mad: number;
  /** null when no order was paid in the window. */
  avg_order_value_mad: number | null;
  /** 0..1; null when no order was placed in the window. */
  cancellation_rate: number | null;
}

export interface TimePoint {
  bucket: IsoDate;
  revenue_mad: number;
  orders_placed: number;
  orders_paid: number;
  orders_cancelled: number;
}

export interface Breakdown {
  value: string;
  orders_placed: number;
  orders_paid: number;
  orders_cancelled: number;
  revenue_mad: number;
}

export interface IngestStats {
  window_minutes: number;
  accepted: number;
  duplicates: number;
  dead_letters: number;
  events_per_second: number;
}

export interface MetricsSnapshot {
  generated_at: IsoDate;
  kpis: Kpis;
  revenue_per_minute: TimePoint[];
  revenue_per_hour: TimePoint[];
  orders_by_status: Record<OrderStatus, number>;
  categories: Breakdown[];
  cities: Breakdown[];
  payment_methods: Breakdown[];
  ingest: IngestStats;
}

export interface LiveUpdate {
  generated_at: IsoDate;
  kpis: Kpis;
  revenue_per_minute_tail: TimePoint[];
  revenue_per_hour_tail: TimePoint[];
  orders_by_status: Record<OrderStatus, number>;
  categories: Breakdown[];
  cities: Breakdown[];
  payment_methods: Breakdown[];
  ingest: IngestStats;
}

export interface PipelineStats {
  events_per_second: number;
  last_event_occurred_at: IsoDate | null;
  last_commit_at: IsoDate | null;
  connected_clients: number;
}

export type EventType = "order_placed" | "order_paid" | "order_shipped" | "order_cancelled";

export interface FeedEvent {
  event_id: string;
  order_id: string;
  event_type: EventType;
  occurred_at: IsoDate;
  amount_mad: number;
  category: string;
  city: string;
  payment_method: string;
}

export interface Alert {
  id: string;
  rule: string;
  severity: "warning" | "critical";
  status: "firing" | "resolved";
  message: string;
  value: number | null;
  threshold: number;
  started_at: IsoDate;
  resolved_at: IsoDate | null;
}

export interface SnapshotMessage {
  type: "snapshot";
  seq: number;
  sent_at: IsoDate;
  pipeline: PipelineStats;
  data: MetricsSnapshot;
}

export interface UpdateMessage {
  type: "update";
  seq: number;
  sent_at: IsoDate;
  pipeline: PipelineStats;
  data: LiveUpdate;
}

export interface EventsMessage {
  type: "events";
  seq: number;
  /** Newest first. */
  items: FeedEvent[];
}

export interface AlertMessage {
  type: "alert";
  seq: number;
  alert: Alert;
}

export type ServerMessage = SnapshotMessage | UpdateMessage | EventsMessage | AlertMessage;

const MESSAGE_TYPES = new Set<ServerMessage["type"]>(["snapshot", "update", "events", "alert"]);

/**
 * Parse a raw WebSocket frame. Returns null for anything that is not a known message, so a
 * newer server sending a new message type never breaks an older client.
 */
export function parseServerMessage(raw: unknown): ServerMessage | null {
  if (typeof raw !== "string") return null;
  let value: unknown;
  try {
    value = JSON.parse(raw);
  } catch {
    return null;
  }
  if (typeof value !== "object" || value === null) return null;
  const type = (value as { type?: unknown }).type;
  if (typeof type !== "string" || !MESSAGE_TYPES.has(type as ServerMessage["type"])) return null;
  return value as ServerMessage;
}
