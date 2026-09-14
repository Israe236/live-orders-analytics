import { provideHttpClient } from '@angular/common/http';
import { HttpTestingController, provideHttpClientTesting } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';
import type { WebSocketLike } from '@rad/core';

import { App } from './app';
import { LIVE_URL, SOCKET_FACTORY } from './live-dashboard.service';

class SilentSocket implements WebSocketLike {
  onopen = null;
  onmessage = null;
  onclose = null;
  onerror = null;
  close(): void {}
}

describe('App', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [App],
      providers: [
        provideHttpClient(),
        provideHttpClientTesting(),
        { provide: LIVE_URL, useValue: 'ws://test/ws/live' },
        { provide: SOCKET_FACTORY, useValue: () => new SilentSocket() },
      ],
    });
  });

  it('shows a connecting badge and a loading message before any data', async () => {
    const fixture = TestBed.createComponent(App);
    await fixture.whenStable();
    const element = fixture.nativeElement as HTMLElement;

    expect(element.querySelector('h1')?.textContent).toContain('Live Orders');
    expect(element.querySelector('[role="status"]')?.textContent).toContain('Connecting');
    expect(element.textContent).toContain('Loading the dashboard');

    // Nothing answered yet: the alert history and the fallback snapshot were requested.
    // The fallback poll starts on an RxJS timer(0), so let one macrotask pass first.
    await new Promise((resolve) => setTimeout(resolve, 20));
    const http = TestBed.inject(HttpTestingController);
    expect(http.match('/api/alerts?limit=20').length).toBe(1);
    expect(http.match('/api/metrics/snapshot').length).toBeGreaterThanOrEqual(1);
  });
});
