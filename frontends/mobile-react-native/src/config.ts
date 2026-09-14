// Expo inlines EXPO_PUBLIC_* variables when bundling, so they must be read with plain dot
// notation (no destructuring, no process.env[name]).
const configured = process.env.EXPO_PUBLIC_API_URL ?? "http://localhost:8000";

export const API_URL = configured.replace(/\/+$/, "");
export const LIVE_URL = `${API_URL.replace(/^http/, "ws")}/ws/live`;

export const COLORS = {
  bg: "#0b0e14",
  surface: "#121722",
  surface2: "#171d2a",
  border: "#222938",
  text: "#e6e9ef",
  muted: "#8a94a7",
  accent: "#4f8cff",
  ok: "#34c38f",
  warn: "#f1b44c",
  bad: "#f46a6a",
  placed: "#8a94a7",
  paid: "#4f8cff",
  shipped: "#34c38f",
  cancelled: "#f46a6a",
} as const;
