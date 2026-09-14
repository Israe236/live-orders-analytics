import { provideHttpClient } from '@angular/common/http';
import { provideHttpClientTesting } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';
import type { MetricsSnapshot, ServerMessage, WebSocketLike } from '@rad/core';

import { LIVE_URL, LiveDashboardService, SOCKET_FACTORY } from './live-dashboard.service';

class FakeSocket implements WebSocketLike {
  onopen: WebSocketLike['onopen'] = null;
  onmessage: WebSocketLike['onmessage'] = null;
  onclose: WebSocketLike['onclose'] = null;
  onerror: WebSocketLike['onerror'] = null;
  close(): void {}
  send(message: ServerMessage): void {
    this.onmessage?.({ data: JSON.stringify(message) });
  }
}

const pipeline = {
  events_per_second: 12,
  last_event_occurred_at: '2026-09-13T20:02:00Z',
  last_commit_at: '2026-09-13T20:02:00Z',
  connected_clients: 1,
};

function snapshot(revenueNow: number): MetricsSnapshot {
  const point = (minute: number, revenue: number) => ({
    bucket: `2026-09-13T20:0${minute}:00.000Z`,
    revenue_mad: revenue,
    orders_placed: 1,
    orders_paid: 1,
    orders_cancelled: 0,
  });
  return {
    generated_at: '2026-09-13T20:02:00Z',
    kpis: {
      window_minutes: 60,
      orders_placed: 3,
      orders_paid: 3,
      orders_shipped: 1,
      orders_cancelled: 0,
      revenue_mad: 60,
      avg_order_value_mad: 20,
      cancellation_rate: 0,
    },
    revenue_per_minute: [point(0, 10), point(1, 20), point(2, revenueNow)],
    revenue_per_hour: [point(0, 60)],
    orders_by_status: { placed: 0, paid: 2, shipped: 1, cancelled: 0 },
    categories: [{ value: 'books', orders_placed: 3, orders_paid: 3, orders_cancelled: 0, revenue_mad: 60 }],
    cities: [{ value: 'Fes', orders_placed: 3, orders_paid: 3, orders_cancelled: 0, revenue_mad: 60 }],
    payment_methods: [],
    ingest: { window_minutes: 5, accepted: 7, duplicates: 0, dead_letters: 0, events_per_second: 0.1 },
  };
}

describe('LiveDashboardService', () => {
  let socket: FakeSocket;

  beforeEach(() => {
    socket = new FakeSocket();
    TestBed.configureTestingModule({
      providers: [
        provideHttpClient(),
        provideHttpClientTesting(),
        { provide: LIVE_URL, useValue: 'ws://test/ws/live' },
        { provide: SOCKET_FACTORY, useValue: () => socket },
      ],
    });
  });

  it('turns WebSocket messages into signals and keeps unchanged slices identical', () => {
    const service = TestBed.inject(LiveDashboardService);
    expect(service.connection().status).toBe('connecting');

    socket.send({ type: 'snapshot', seq: 1, sent_at: '2026-09-13T20:02:00Z', pipeline, data: snapshot(30) });
    expect(service.live()).toBe(true);
    const first = service.snapshot();
    expect(first?.kpis.orders_placed).toBe(3);

    const next = snapshot(45);
    socket.send({
      type: 'update',
      seq: 2,
      sent_at: '2026-09-13T20:02:01Z',
      pipeline,
      data: {
        generated_at: next.generated_at,
        kpis: next.kpis,
        revenue_per_minute_tail: next.revenue_per_minute.slice(-1),
        revenue_per_hour_tail: next.revenue_per_hour,
        orders_by_status: next.orders_by_status,
        categories: next.categories,
        cities: next.cities,
        payment_methods: next.payment_methods,
        ingest: next.ingest,
      },
    });

    const second = service.snapshot();
    expect(second?.revenue_per_minute.at(-1)?.revenue_mad).toBe(45);
    // Same values → same objects → OnPush children bound to them are not re-rendered.
    expect(second?.categories).toBe(first?.categories);
    expect(second?.kpis).toBe(first?.kpis);
    expect(second?.revenue_per_minute).not.toBe(first?.revenue_per_minute);
  });
});
