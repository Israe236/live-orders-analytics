import { HttpClient } from '@angular/common/http';
import { computed, DestroyRef, inject, Injectable, InjectionToken, signal } from '@angular/core';
import { toObservable, toSignal } from '@angular/core/rxjs-interop';
import {
  initialDashboardState,
  LiveConnection,
  reduce,
  type Alert,
  type ConnectionState,
  type DashboardState,
  type MetricsSnapshot,
  type ServerMessage,
  type WebSocketLike,
} from '@rad/core';
import { catchError, EMPTY, switchMap, timer } from 'rxjs';

import { API_BASE, liveUrl } from './config';

export const LIVE_URL = new InjectionToken<string>('LIVE_URL', { factory: liveUrl });

/** Replaceable in tests with a fake socket. */
export const SOCKET_FACTORY = new InjectionToken<(url: string) => WebSocketLike>('SOCKET_FACTORY', {
  factory: () => (url: string) => new WebSocket(url) as unknown as WebSocketLike,
});

const FALLBACK_POLL_MS = 5_000;

function newest(a: MetricsSnapshot | null, b: MetricsSnapshot | null): MetricsSnapshot | null {
  if (b === null) return a;
  if (a === null) return b;
  return Date.parse(b.generated_at) > Date.parse(a.generated_at) ? b : a;
}

/**
 * The live dashboard state as Angular signals.
 *
 * Reconnection, backoff and message merging all come from `@rad/core`. Signals fit it well:
 * `computed` only notifies its readers when the value's identity changes, and the core reducer
 * keeps unchanged slices at the same identity — so OnPush components whose data did not change
 * are not re-rendered on a live update.
 */
@Injectable({ providedIn: 'root' })
export class LiveDashboardService {
  private readonly http = inject(HttpClient);
  private readonly state = signal<DashboardState>(initialDashboardState);

  readonly connection = signal<ConnectionState>({
    status: 'connecting',
    attempt: 0,
    nextRetryAt: null,
    lastMessageAt: null,
  });
  readonly live = computed(() => this.connection().status === 'open');
  readonly pipeline = computed(() => this.state().pipeline);
  readonly feed = computed(() => this.state().feed);
  readonly alerts = computed(() => this.state().alerts);
  readonly firingAlerts = computed(() => this.alerts().filter((alert) => alert.status === 'firing'));

  /** Degraded mode: poll the REST snapshot only while the WebSocket is down. */
  private readonly fallback = toSignal(
    toObservable(this.live).pipe(
      switchMap((live) =>
        live
          ? EMPTY
          : timer(0, FALLBACK_POLL_MS).pipe(
              switchMap(() =>
                this.http
                  .get<MetricsSnapshot>(`${API_BASE}/metrics/snapshot`)
                  .pipe(catchError(() => EMPTY)),
              ),
            ),
      ),
    ),
    { initialValue: null },
  );

  readonly snapshot = computed<MetricsSnapshot | null>(() => {
    const current = this.state().snapshot;
    return this.live() ? current : newest(current, this.fallback());
  });

  constructor() {
    const connection = new LiveConnection({
      url: inject(LIVE_URL),
      createSocket: inject(SOCKET_FACTORY),
      onMessage: (message) => this.dispatch(message),
      onStateChange: (state) => this.connection.set(state),
    });

    const online = () => connection.setNetworkAvailable(true);
    const offline = () => connection.setNetworkAvailable(false);
    const visibility = () => {
      if (document.visibilityState === 'visible' && connection.currentState.status === 'reconnecting') {
        connection.reconnectNow();
      }
    };
    window.addEventListener('online', online);
    window.addEventListener('offline', offline);
    document.addEventListener('visibilitychange', visibility);
    connection.start();

    inject(DestroyRef).onDestroy(() => {
      window.removeEventListener('online', online);
      window.removeEventListener('offline', offline);
      document.removeEventListener('visibilitychange', visibility);
      connection.stop();
    });

    // Alert history once over REST; live changes arrive over the WebSocket.
    this.http
      .get<Alert[]>(`${API_BASE}/alerts?limit=20`)
      .pipe(catchError(() => EMPTY))
      .subscribe((alerts) => {
        for (const alert of alerts) this.dispatch({ type: 'alert', seq: 0, alert });
      });
  }

  dispatch(message: ServerMessage): void {
    this.state.update((state) => reduce(state, message));
  }
}
