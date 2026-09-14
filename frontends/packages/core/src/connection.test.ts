import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import {
  CLOSE_STALE,
  LiveConnection,
  type ConnectionState,
  type LiveConnectionOptions,
  type WebSocketLike,
} from "./connection";
import type { ServerMessage } from "./messages";

class FakeSocket implements WebSocketLike {
  onopen: WebSocketLike["onopen"] = null;
  onmessage: WebSocketLike["onmessage"] = null;
  onclose: WebSocketLike["onclose"] = null;
  onerror: WebSocketLike["onerror"] = null;
  closedWith: { code?: number; reason?: string } | null = null;

  constructor(readonly url: string) {}

  close(code?: number, reason?: string): void {
    this.closedWith = { code, reason };
  }

  // Helpers that play the server's part.
  open(): void {
    this.onopen?.({});
  }

  send(message: unknown): void {
    this.onmessage?.({ data: typeof message === "string" ? message : JSON.stringify(message) });
  }

  drop(code = 1006): void {
    this.onclose?.({ code, reason: "" });
  }
}

const EVENTS: ServerMessage = { type: "events", seq: 1, items: [] };

describe("LiveConnection", () => {
  let sockets: FakeSocket[];
  let received: ServerMessage[];
  let states: ConnectionState[];

  function connection(options: Partial<LiveConnectionOptions> = {}): LiveConnection {
    return new LiveConnection({
      url: "ws://test/ws/live",
      createSocket: (url) => {
        const socket = new FakeSocket(url);
        sockets.push(socket);
        return socket;
      },
      onMessage: (message) => received.push(message),
      onStateChange: (state) => states.push(state),
      // Deterministic backoff: always the ceiling (500, 1000, 2000, ...).
      backoff: { random: () => 0.999999, minMs: 0 },
      staleAfterMs: 5_000,
      ...options,
    });
  }

  const latest = () => states[states.length - 1]!;
  const current = () => sockets[sockets.length - 1]!;

  beforeEach(() => {
    vi.useFakeTimers();
    sockets = [];
    received = [];
    states = [];
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("delivers parsed messages and ignores garbage", () => {
    const live = connection();
    live.start();
    expect(latest().status).toBe("connecting");

    current().open();
    current().send("not json");
    current().send({ type: "unknown-future-type" });
    current().send(EVENTS);

    expect(received).toEqual([EVENTS]);
    expect(latest().status).toBe("open");
    live.stop();
  });

  it("reconnects with a growing delay while the server keeps failing", () => {
    const live = connection();
    live.start();

    const delays: number[] = [];
    for (let i = 0; i < 4; i++) {
      const before = sockets.length;
      current().open(); // accepted, then closed at once: must NOT reset the backoff
      current().drop();
      const state = latest();
      expect(state.status).toBe("reconnecting");
      delays.push(state.nextRetryAt! - Date.now());

      vi.advanceTimersByTime(delays[i]! - 1);
      expect(sockets.length).toBe(before);
      vi.advanceTimersByTime(1);
      expect(sockets.length).toBe(before + 1);
    }
    expect(delays).toEqual([500, 1_000, 2_000, 4_000]);
    live.stop();
  });

  it("resets the backoff once a message gets through", () => {
    const live = connection();
    live.start();
    current().drop();
    vi.advanceTimersByTime(500);
    current().drop();
    vi.advanceTimersByTime(1_000);

    current().send(EVENTS);
    expect(latest().attempt).toBe(0);
    current().drop();
    expect(latest().nextRetryAt! - Date.now()).toBe(500);
    live.stop();
  });

  it("treats a silent connection as dead and ignores its late close event", () => {
    const live = connection();
    live.start();
    const first = current();
    first.open();
    first.send(EVENTS);

    vi.advanceTimersByTime(4_999);
    expect(first.closedWith).toBeNull();
    vi.advanceTimersByTime(1);
    expect(first.closedWith?.code).toBe(CLOSE_STALE);
    expect(latest().status).toBe("reconnecting");

    first.drop(); // arrives late, after we already moved on
    vi.advanceTimersByTime(500);
    expect(sockets.length).toBe(2); // exactly one reconnection, not two
    live.stop();
  });

  it("stop() closes the socket and never reconnects", () => {
    const live = connection();
    live.start();
    const socket = current();
    live.stop();
    socket.drop();
    vi.advanceTimersByTime(60_000);
    expect(sockets.length).toBe(1);
    expect(latest().status).toBe("closed");
  });

  it("pauses while offline and reconnects immediately when the network returns", () => {
    const live = connection();
    live.start();
    current().drop();
    live.setNetworkAvailable(false);
    expect(latest().status).toBe("offline");

    vi.advanceTimersByTime(60_000);
    expect(sockets.length).toBe(1);

    live.setNetworkAvailable(true);
    expect(sockets.length).toBe(2);
    expect(latest().attempt).toBe(0);
    live.stop();
  });
});
