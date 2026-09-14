import { ChangeDetectionStrategy, Component, computed, input } from '@angular/core';
import type { Alert } from '@rad/core';

function time(iso: string | null): string {
  return iso === null ? '—' : new Date(iso).toLocaleTimeString([], { hour12: false });
}

@Component({
  selector: 'app-alert-banner',
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `
    @if (alerts().length > 0) {
      <div class="alert-banner" role="alert">
        @for (alert of alerts(); track alert.id) {
          <div [class]="'alert-item alert-' + alert.severity">
            <strong>{{ alert.severity === 'critical' ? 'Critical' : 'Warning' }}</strong>
            <span>{{ alert.message }}</span>
            <span class="muted">since {{ time(alert.started_at) }}</span>
          </div>
        }
      </div>
    }
  `,
})
export class AlertBannerComponent {
  readonly alerts = input.required<Alert[]>();
  protected readonly time = time;
}

@Component({
  selector: 'app-alert-history',
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `
    <div class="card-header">
      <h2>Alerts</h2>
      <span class="muted">{{ firing() }} firing</span>
    </div>
    @if (alerts().length === 0) {
      <p class="empty">No alerts. Thresholds are checked every second.</p>
    } @else {
      <table class="table">
        <thead>
          <tr>
            <th scope="col">Status</th>
            <th scope="col">Rule</th>
            <th scope="col">Message</th>
            <th scope="col" class="num">Started</th>
            <th scope="col" class="num">Resolved</th>
          </tr>
        </thead>
        <tbody>
          @for (alert of recent(); track alert.id) {
            <tr>
              <td><span [class]="'chip chip-alert-' + alert.status">{{ alert.status }}</span></td>
              <td>{{ alert.rule.replaceAll('_', ' ') }}</td>
              <td>{{ alert.message }}</td>
              <td class="num">{{ time(alert.started_at) }}</td>
              <td class="num">{{ time(alert.resolved_at) }}</td>
            </tr>
          }
        </tbody>
      </table>
    }
  `,
})
export class AlertHistoryComponent {
  readonly alerts = input.required<Alert[]>();
  protected readonly firing = computed(() => this.alerts().filter((a) => a.status === 'firing').length);
  /** Most recent alerts only (firing ones are always listed first). */
  protected readonly recent = computed(() => this.alerts().slice(0, 8));
  protected readonly time = time;
}
