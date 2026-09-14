import { TestBed } from '@angular/core/testing';
import type { Kpis } from '@rad/core';

import { KpiCardsComponent } from './kpi-cards.component';

const kpis: Kpis = {
  window_minutes: 60,
  orders_placed: 1_200,
  orders_paid: 1_000,
  orders_shipped: 700,
  orders_cancelled: 200,
  revenue_mad: 1_234_567,
  avg_order_value_mad: null,
  cancellation_rate: 0.1667,
};

describe('KpiCardsComponent', () => {
  it('formats values and flags a high cancellation rate', async () => {
    const fixture = TestBed.createComponent(KpiCardsComponent);
    fixture.componentRef.setInput('kpis', kpis);
    fixture.componentRef.setInput('eventsPerSecond', 41.26);
    await fixture.whenStable();

    const element = fixture.nativeElement as HTMLElement;
    const text = element.textContent ?? '';
    expect(text).toContain('Revenue · last 60 min');
    expect(text).toContain('16.7%');
    expect(text).toContain('41.3');
    expect(text).toContain('1,000 paid · 700 shipped');
    expect(element.querySelector('.kpi-bad')?.textContent).toContain('Cancellation rate');
  });
});
