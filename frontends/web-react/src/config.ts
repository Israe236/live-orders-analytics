/**
 * By default the app talks to its own origin: `/api/*` and `/ws/*` are proxied to the API by the
 * Vite dev server (see vite.config.ts) and by nginx in Docker. No CORS, no hard-coded host.
 */
export const API_BASE: string = import.meta.env.VITE_API_BASE ?? "/api";

export function liveUrl(): string {
  if (import.meta.env.VITE_WS_URL) return import.meta.env.VITE_WS_URL;
  const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
  return `${protocol}//${window.location.host}/ws/live`;
}

/** Chart colours (SVG attributes cannot use CSS variables reliably). */
export const COLORS = {
  accent: "#4f8cff",
  muted: "#8a94a7",
  grid: "#252c3a",
  placed: "#8a94a7",
  paid: "#4f8cff",
  shipped: "#34c38f",
  cancelled: "#f46a6a",
} as const;
