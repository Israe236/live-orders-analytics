import {
  initialDashboardState,
  LiveConnection,
  reduce,
  type Alert,
  type ConnectionState,
  type DashboardState,
  type ServerMessage,
} from "@rad/core";
import { useEffect, useReducer, useState } from "react";
import { AppState } from "react-native";

import { API_URL, LIVE_URL } from "./config";

const INITIAL_CONNECTION: ConnectionState = {
  status: "connecting",
  attempt: 0,
  nextRetryAt: null,
  lastMessageAt: null,
};

function reducer(state: DashboardState, message: ServerMessage): DashboardState {
  return reduce(state, message);
}

/** Same state and reconnection logic as the web apps, via @rad/core; mobile lifecycle on top. */
export function useLiveDashboard(): { state: DashboardState; connection: ConnectionState } {
  const [state, dispatch] = useReducer(reducer, initialDashboardState);
  const [connection, setConnection] = useState<ConnectionState>(INITIAL_CONNECTION);

  useEffect(() => {
    const live = new LiveConnection({
      url: LIVE_URL,
      onMessage: dispatch,
      onStateChange: setConnection,
    });
    live.start();

    // In the background the OS may suspend the app and silently kill its sockets. Close the
    // connection on purpose (no retries burning battery) and reconnect when the user returns.
    const subscription = AppState.addEventListener("change", (next) => {
      if (next === "active") live.start();
      else if (next === "background") live.stop();
    });

    return () => {
      subscription.remove();
      live.stop();
    };
  }, []);

  useEffect(() => {
    const controller = new AbortController();
    fetch(`${API_URL}/alerts?limit=20`, { signal: controller.signal })
      .then((response) => (response.ok ? (response.json() as Promise<Alert[]>) : []))
      .then((alerts) => {
        for (const alert of alerts) dispatch({ type: "alert", seq: 0, alert });
      })
      .catch(() => {
        // History is optional; live alerts still arrive over the WebSocket.
      });
    return () => controller.abort();
  }, []);

  return { state, connection };
}
