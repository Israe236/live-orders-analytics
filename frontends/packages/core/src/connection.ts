import { backoffDelay, type BackoffOptions } from "./backoff";
import { parseServerMessage, type ServerMessage } from "./messages";

export type ConnectionStatus =
  /** First connection attempt in progress. */
  | "connecting"
  /** Connected and receiving messages. */
  | "open"
  /** Connection lost; waiting before the next attempt, or attempting again. */
  | "reconnecting"
  /** The device reported no network; waiting for it to come back. */
  | "offline"
  /** stop() was called. */
  | "closed";

export interface ConnectionState {
  status: ConnectionStatus;
  /** Consecutive failed attempts since the last successful message. */
  attempt: number;
  /** When the next attempt is scheduled (epoch ms), if any. */
  nextRetryAt: number | null;
  /** When the last message arrived (epoch ms), if any. */
  lastMessageAt: number | null;
}

/** The subset of the browser / React Native WebSocket API this client relies on. */
export interface WebSocketLike {
  onopen: ((event: unknown) => void) | null;
  onmessage: ((event: { data: unknown }) => void) | null;
  onclose: ((event: { code: number; reason: string }) => void) | null;
  onerror: ((event: unknown) => void) | null;
  close(code?: number, reason?: string): void;
}

export interface LiveConnectionOptions {
  url: string;
  onMessage: (message: ServerMessage) => void;
  onStateChange?: (state: ConnectionState) => void;
  /** Defaults to the global WebSocket (browsers, React Native). */
  createSocket?: (url: string) => WebSocketLike;
  backoff?: Partial<BackoffOptions>;
  /**
   * The server sends a message every second. If nothing arrives for this long the connection
   * is treated as dead even if the socket has not noticed (e.g. a laptop waking from sleep).
   */
  staleAfterMs?: number;
  now?: () => number;
}

/** Close codes sent by this client. 4000-4999 are reserved for applications. */
export const CLOSE_STALE = 4000;
export const CLOSE_NORMAL = 1000;

/**
 * A WebSocket that keeps itself connected.
 *
 * - Reconnects with exponential backoff + jitter after any disconnect.
 * - The attempt counter is reset only when a *message* arrives, not when the socket opens: a
 *   server that accepts and immediately closes (e.g. code 1013 "busy") keeps backing off.
 * - Detects silent dead connections with a staleness timer.
 * - `setNetworkAvailable(false)` pauses retries while the device is offline;
 *   `setNetworkAvailable(true)` reconnects immediately.
 */
export class LiveConnection {
  private socket: WebSocketLike | null = null;
  private retryTimer: ReturnType<typeof setTimeout> | null = null;
  private staleTimer: ReturnType<typeof setTimeout> | null = null;
  private stopped = true;
  private networkAvailable = true;
  private state: ConnectionState = {
    status: "closed",
    attempt: 0,
    nextRetryAt: null,
    lastMessageAt: null,
  };

  private readonly createSocket: (url: string) => WebSocketLike;
  private readonly staleAfterMs: number;
  private readonly now: () => number;

  constructor(private readonly options: LiveConnectionOptions) {
    this.createSocket =
      options.createSocket ?? ((url) => new WebSocket(url) as unknown as WebSocketLike);
    this.staleAfterMs = options.staleAfterMs ?? 5_000;
    this.now = options.now ?? Date.now;
  }

  get currentState(): ConnectionState {
    return this.state;
  }

  start(): void {
    if (!this.stopped) return;
    this.stopped = false;
    this.setState({ status: "connecting", attempt: 0, nextRetryAt: null });
    this.connect();
  }

  stop(): void {
    this.stopped = true;
    this.clearTimers();
    this.dropSocket(CLOSE_NORMAL, "client stopped");
    this.setState({ status: "closed", nextRetryAt: null });
  }

  /** Skip the remaining backoff delay and try now (e.g. the app came back to the foreground). */
  reconnectNow(): void {
    if (this.stopped || !this.networkAvailable) return;
    this.clearTimers();
    this.dropSocket(CLOSE_NORMAL, "reconnecting");
    this.connect();
  }

  setNetworkAvailable(available: boolean): void {
    if (available === this.networkAvailable) return;
    this.networkAvailable = available;
    if (this.stopped) return;
    if (!available) {
      this.clearTimers();
      this.dropSocket(CLOSE_NORMAL, "network lost");
      this.setState({ status: "offline", nextRetryAt: null });
    } else {
      this.setState({ status: "reconnecting", attempt: 0, nextRetryAt: null });
      this.connect();
    }
  }

  private connect(): void {
    let socket: WebSocketLike;
    try {
      socket = this.createSocket(this.options.url);
    } catch {
      this.scheduleReconnect();
      return;
    }
    this.socket = socket;
    // Every handler checks it still belongs to the current socket: events from a socket we
    // already replaced (e.g. a late `close`) must not trigger a second reconnect.
    socket.onopen = () => {
      if (socket === this.socket) this.armStaleTimer();
    };
    socket.onmessage = (event) => {
      if (socket !== this.socket) return;
      const message = parseServerMessage(event.data);
      if (message === null) return;
      this.armStaleTimer();
      this.setState({ status: "open", attempt: 0, nextRetryAt: null, lastMessageAt: this.now() });
      this.options.onMessage(message);
    };
    socket.onclose = () => {
      if (socket !== this.socket) return;
      this.socket = null;
      this.scheduleReconnect();
    };
    socket.onerror = () => {
      // A `close` event always follows an error; reconnection is handled there.
    };
  }

  private scheduleReconnect(): void {
    this.clearTimers();
    if (this.stopped || !this.networkAvailable) return;
    const attempt = this.state.attempt;
    const delay = backoffDelay(attempt, this.options.backoff);
    this.setState({ status: "reconnecting", attempt: attempt + 1, nextRetryAt: this.now() + delay });
    this.retryTimer = setTimeout(() => {
      this.retryTimer = null;
      this.connect();
    }, delay);
  }

  private armStaleTimer(): void {
    if (this.staleTimer !== null) clearTimeout(this.staleTimer);
    this.staleTimer = setTimeout(() => {
      this.staleTimer = null;
      // Do not wait for the socket to report the close: it may never do so.
      this.dropSocket(CLOSE_STALE, "no message received in time");
      this.scheduleReconnect();
    }, this.staleAfterMs);
  }

  private dropSocket(code: number, reason: string): void {
    const socket = this.socket;
    this.socket = null;
    if (socket === null) return;
    try {
      socket.close(code, reason);
    } catch {
      // Closing a socket that is already closing can throw in some runtimes; nothing to do.
    }
  }

  private clearTimers(): void {
    if (this.retryTimer !== null) clearTimeout(this.retryTimer);
    if (this.staleTimer !== null) clearTimeout(this.staleTimer);
    this.retryTimer = null;
    this.staleTimer = null;
  }

  private setState(patch: Partial<ConnectionState>): void {
    this.state = { ...this.state, ...patch };
    this.options.onStateChange?.(this.state);
  }
}
