import type { MetricsSnapshot } from "@rad/core";
import { useQuery } from "@tanstack/react-query";
import { useEffect } from "react";

import { fetchAlerts, fetchSnapshot } from "./api";
import { ConnectionBadge } from "./components/ConnectionBadge";
import { Dashboard } from "./components/Dashboard";
import { liveUrl } from "./config";
import { useLiveDashboard } from "./hooks/useLiveDashboard";

const FALLBACK_POLL_MS = 5_000;

function newest(a: MetricsSnapshot | null, b: MetricsSnapshot | undefined): MetricsSnapshot | null {
  if (!b) return a;
  if (!a) return b;
  return Date.parse(b.generated_at) > Date.parse(a.generated_at) ? b : a;
}

export function App() {
  const { state, connection, dispatch } = useLiveDashboard(liveUrl());
  const live = connection.status === "open";

  // Degraded mode: while the WebSocket is down, poll the REST snapshot so the page keeps
  // showing recent numbers. Polling stops as soon as the live connection is back.
  const fallback = useQuery({
    queryKey: ["snapshot"],
    queryFn: ({ signal }) => fetchSnapshot(signal),
    enabled: !live,
    refetchInterval: live ? false : FALLBACK_POLL_MS,
  });

  // Alert history comes from REST once; live changes then arrive over the WebSocket.
  const alertHistory = useQuery({
    queryKey: ["alerts"],
    queryFn: ({ signal }) => fetchAlerts(signal),
    staleTime: Infinity,
  });
  useEffect(() => {
    for (const alert of alertHistory.data ?? []) dispatch({ type: "alert", seq: 0, alert });
  }, [alertHistory.data, dispatch]);

  const snapshot = live ? state.snapshot : newest(state.snapshot, fallback.data);

  return (
    <div className="app">
      <header className="topbar">
        <div>
          <h1>Live Orders</h1>
          <p className="muted">E-commerce analytics · Morocco · React</p>
        </div>
        <ConnectionBadge connection={connection} lastEventAt={state.pipeline?.last_event_occurred_at ?? null} />
      </header>
      {snapshot ? (
        <Dashboard
          snapshot={snapshot}
          eventsPerSecond={live ? (state.pipeline?.events_per_second ?? null) : null}
          feed={state.feed}
          alerts={state.alerts}
        />
      ) : (
        <p className="empty loading">
          {fallback.isError ? "Cannot reach the API yet, retrying…" : "Loading the dashboard…"}
        </p>
      )}
    </div>
  );
}
