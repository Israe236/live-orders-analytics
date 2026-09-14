import type { Alert, MetricsSnapshot } from "@rad/core";

import { API_BASE } from "./config";

async function getJson<T>(path: string, signal?: AbortSignal): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`, { signal });
  if (!response.ok) throw new Error(`${response.status} ${response.statusText} for ${path}`);
  return (await response.json()) as T;
}

export function fetchSnapshot(signal?: AbortSignal): Promise<MetricsSnapshot> {
  return getJson<MetricsSnapshot>("/metrics/snapshot", signal);
}

export function fetchAlerts(signal?: AbortSignal): Promise<Alert[]> {
  return getJson<Alert[]>("/alerts?limit=20", signal);
}
