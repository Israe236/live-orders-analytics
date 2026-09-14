import { ChangeDetectionStrategy, Component, computed, input } from '@angular/core';
import { formatInteger, formatMad, type Breakdown, type OrderStatus } from '@rad/core';

import { COLORS } from '../config';

const STATUSES: OrderStatus[] = ['placed', 'paid', 'shipped', 'cancelled'];

@Component({
  selector: 'app-status-bar',
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `
    <div class="card-header">
      <h2>Orders by status</h2>
      <span class="muted">{{ total() }} orders</span>
    </div>
    <div class="status-bar" role="img" aria-label="Share of orders per status">
      @for (segment of segments(); track segment.status) {
        <!-- flex-grow is animated in CSS, so segments slide instead of jumping. -->
        <div [style.flex-grow]="segment.count" [style.background]="segment.color"></div>
      }
    </div>
    <ul class="legend">
      @for (segment of segments(); track segment.status) {
        <li>
          <span class="swatch" [style.background]="segment.color"></span>
          {{ segment.status }}
          <strong>{{ segment.label }}</strong>
        </li>
      }
    </ul>
  `,
})
export class StatusBarComponent {
  readonly counts = input.required<Record<OrderStatus, number>>();

  protected readonly segments = computed(() =>
    STATUSES.map((status) => ({
      status,
      count: this.counts()[status],
      label: formatInteger(this.counts()[status]),
      color: COLORS[status],
    })),
  );
  protected readonly total = computed(() =>
    formatInteger(STATUSES.reduce((sum, status) => sum + this.counts()[status], 0)),
  );
}

@Component({
  selector: 'app-city-table',
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `
    <div class="card-header">
      <h2>Top cities</h2>
      <span class="muted">last 60 min</span>
    </div>
    <table class="table">
      <thead>
        <tr>
          <th scope="col">City</th>
          <th scope="col" class="num">Orders</th>
          <th scope="col" class="num">Revenue</th>
        </tr>
      </thead>
      <tbody>
        @for (row of rows(); track row.city) {
          <tr>
            <td>
              {{ row.city }}
              <div class="inline-bar" [style.width.%]="row.share"></div>
            </td>
            <td class="num">{{ row.orders }}</td>
            <td class="num">{{ row.revenue }}</td>
          </tr>
        }
      </tbody>
    </table>
  `,
})
export class CityTableComponent {
  readonly items = input.required<Breakdown[]>();

  protected readonly rows = computed(() => {
    const items = this.items().slice(0, 6);
    const max = Math.max(1, ...items.map((item) => item.revenue_mad));
    return items.map((item) => ({
      city: item.value,
      orders: formatInteger(item.orders_placed),
      revenue: formatMad(item.revenue_mad),
      share: (item.revenue_mad / max) * 100,
    }));
  });
}
