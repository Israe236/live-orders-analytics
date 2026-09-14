import {
  initialDashboardState,
  LiveConnection,
  reduce,
  type ConnectionState,
  type DashboardState,
  type ServerMessage,
} from "@rad/core";
import { useEffect, useReducer, useState, type Dispatch } from "react";

const INITIAL_CONNECTION: ConnectionState = {
  status: "connecting",
  attempt: 0,
  nextRetryAt: null,
  lastMessageAt: null,
};

function reducer(state: DashboardState, message: ServerMessage): DashboardState {
  return reduce(state, message);
}

export interface LiveDashboard {
  state: DashboardState;
  connection: ConnectionState;
  dispatch: Dispatch<ServerMessage>;
}

/**
 * Owns the WebSocket for the lifetime of the component. All reconnection logic lives in
 * `@rad/core`; this hook only wires it to React and to browser events.
 */
export function useLiveDashboard(url: string): LiveDashboard {
  const [state, dispatch] = useReducer(reducer, initialDashboardState);
  const [connection, setConnection] = useState<ConnectionState>(INITIAL_CONNECTION);

  useEffect(() => {
    const live = new LiveConnection({ url, onMessage: dispatch, onStateChange: setConnection });

    const handleOnline = () => live.setNetworkAvailable(true);
    const handleOffline = () => live.setNetworkAvailable(false);
    // Browsers throttle timers in background tabs: when the tab comes back, don't wait for the
    // remaining backoff delay.
    const handleVisibility = () => {
      if (document.visibilityState === "visible" && live.currentState.status === "reconnecting") {
        live.reconnectNow();
      }
    };

    window.addEventListener("online", handleOnline);
    window.addEventListener("offline", handleOffline);
    document.addEventListener("visibilitychange", handleVisibility);
    if (!navigator.onLine) live.setNetworkAvailable(false);
    live.start();

    return () => {
      window.removeEventListener("online", handleOnline);
      window.removeEventListener("offline", handleOffline);
      document.removeEventListener("visibilitychange", handleVisibility);
      live.stop();
    };
  }, [url]);

  return { state, connection, dispatch };
}
