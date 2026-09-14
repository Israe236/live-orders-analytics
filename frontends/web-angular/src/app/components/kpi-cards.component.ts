import { ChangeDetectionStrategy, Component, computed, input } from '@angular/core';
import { formatInteger, formatMad, formatPercent, type Kpis } from '@rad/core';

interface Card {
  label: string;
  value: string;
  detail?: string;
  bad?: boolean;
}

const CANCELLATION_WARNING = 0.15;

@Component({
  selector: 'app-kpi-cards',
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `
    <div class="kpis">
      @for (card of cards(); track card.label) {
        <div class="kpi" [class.kpi-bad]="card.bad">
          <div class="kpi-label">{{ card.label }}</div>
          <div class="kpi-value">{{ card.value }}</div>
          @if (card.detail) {
            <div class="kpi-detail">{{ card.detail }}</div>
          }
        </div>
      }
    </div>
  `,
})
export class KpiCardsComponent {
  readonly kpis = input.required<Kpis>();
  readonly eventsPerSecond = input<number | null>(null);

  protected readonly cards = computed<Card[]>(() => {
    const k = this.kpis();
    const eps = this.eventsPerSecond();
    return [
      { label: `Revenue · last ${k.window_minutes} min`, value: formatMad(k.revenue_mad) },
      {
        label: 'Orders placed',
        value: formatInteger(k.orders_placed),
        detail: `${formatInteger(k.orders_paid)} paid · ${formatInteger(k.orders_shipped)} shipped`,
      },
      {
        label: 'Average order value',
        value: k.avg_order_value_mad === null ? '—' : formatMad(k.avg_order_value_mad),
      },
      {
        label: 'Cancellation rate',
        value: formatPercent(k.cancellation_rate),
        detail: `${formatInteger(k.orders_cancelled)} cancelled`,
        bad: (k.cancellation_rate ?? 0) > CANCELLATION_WARNING,
      },
      { label: 'Events / second', value: eps === null ? '—' : eps.toFixed(1) },
    ];
  });
}
