import type { ConnectionState } from "./connection";
import type { PipelineStats } from "./messages";

/** Human-readable connection status, e.g. "Reconnecting in 3s (attempt 2)". */
export function describeConnection(connection: ConnectionState, now: number = Date.now()): string {
  switch (connection.status) {
    case "connecting":
      return "Connecting…";
    case "open":
      return "Live";
    case "reconnecting": {
      const seconds =
        connection.nextRetryAt === null ? null : Math.max(0, Math.ceil((connection.nextRetryAt - now) / 1000));
      const when = seconds === null || seconds === 0 ? "…" : ` in ${seconds}s`;
      return `Reconnecting${when} (attempt ${connection.attempt})`;
    }
    case "offline":
      return "Offline, waiting for the network";
    case "closed":
      return "Disconnected";
  }
}

const madFormatter = new Intl.NumberFormat("fr-MA", {
  style: "currency",
  currency: "MAD",
  maximumFractionDigits: 0,
});

const compactFormatter = new Intl.NumberFormat("en", {
  notation: "compact",
  maximumFractionDigits: 1,
});

const integerFormatter = new Intl.NumberFormat("en");

/** "12 345 MAD" style amount, no decimals (dashboard figures, not invoices). */
export function formatMad(value: number): string {
  return madFormatter.format(value);
}

/** 1234567 → "1.2M" (chart axes). */
export function formatCompact(value: number): string {
  return compactFormatter.format(value);
}

export function formatInteger(value: number): string {
  return integerFormatter.format(value);
}

/** 0.1234 → "12.3%"; null → "—". */
export function formatPercent(value: number | null, fractionDigits = 1): string {
  return value === null ? "—" : `${(value * 100).toFixed(fractionDigits)}%`;
}

/** "HH:MM" in the viewer's local time. */
export function formatClock(iso: string): string {
  const date = new Date(iso);
  return `${String(date.getHours()).padStart(2, "0")}:${String(date.getMinutes()).padStart(2, "0")}`;
}

/** Age of the newest committed event, in milliseconds; null when unknown. */
export function dataAgeMs(pipeline: PipelineStats | null, now: number = Date.now()): number | null {
  if (pipeline?.last_event_occurred_at == null) return null;
  return Math.max(0, now - Date.parse(pipeline.last_event_occurred_at));
}

/** 850 → "0.9 s", 75_000 → "1 min". */
export function formatAge(ms: number | null): string {
  if (ms === null) return "—";
  if (ms < 60_000) return `${(ms / 1000).toFixed(1)} s`;
  return `${Math.round(ms / 60_000)} min`;
}

export const EVENT_LABELS: Record<string, string> = {
  order_placed: "Placed",
  order_paid: "Paid",
  order_shipped: "Shipped",
  order_cancelled: "Cancelled",
};
