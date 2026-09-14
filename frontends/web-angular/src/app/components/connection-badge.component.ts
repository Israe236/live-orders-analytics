import { ChangeDetectionStrategy, Component, computed, input } from '@angular/core';
import { toSignal } from '@angular/core/rxjs-interop';
import { describeConnection, formatAge, type ConnectionState } from '@rad/core';
import { interval, map } from 'rxjs';

const STALE_AFTER_MS = 10_000;

@Component({
  selector: 'app-connection-badge',
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `
    <div [class]="'badge badge-' + tone()" role="status" aria-live="polite">
      <span class="badge-dot" aria-hidden="true"></span>
      <span>{{ label() }}</span>
      @if (connection().status === 'open' && ageMs() !== null) {
        <span class="badge-detail">data {{ age() }} old</span>
      }
    </div>
  `,
})
export class ConnectionBadgeComponent {
  readonly connection = input.required<ConnectionState>();
  readonly lastEventAt = input<string | null>(null);

  /** Ticks every second for the countdown and the data age; only this component updates. */
  private readonly now = toSignal(interval(1_000).pipe(map(() => Date.now())), {
    initialValue: Date.now(),
  });

  protected readonly ageMs = computed(() => {
    const at = this.lastEventAt();
    return at === null ? null : Math.max(0, this.now() - Date.parse(at));
  });
  protected readonly age = computed(() => formatAge(this.ageMs()));
  protected readonly label = computed(() => describeConnection(this.connection(), this.now()));
  protected readonly tone = computed(() => {
    const status = this.connection().status;
    if (status === 'open') return (this.ageMs() ?? 0) > STALE_AFTER_MS ? 'warn' : 'ok';
    return status === 'offline' ? 'bad' : 'warn';
  });
}
