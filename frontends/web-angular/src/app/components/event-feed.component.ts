import { ChangeDetectionStrategy, Component, input } from '@angular/core';
import { EVENT_LABELS, formatMad, type FeedEvent } from '@rad/core';

@Component({
  selector: 'app-event-feed',
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `
    <div class="card-header">
      <h2>Live events</h2>
      <span class="muted">newest first</span>
    </div>
    @if (events().length === 0) {
      <p class="empty">Waiting for events…</p>
    } @else {
      <ul class="feed">
        <!-- track by event id: only new rows are created (and play the highlight animation). -->
        @for (event of events(); track event.event_id) {
          <li class="feed-row">
            <span class="feed-time">{{ clock(event.occurred_at) }}</span>
            <span [class]="'chip chip-' + event.event_type">{{ labels[event.event_type] }}</span>
            <span class="feed-amount">{{ mad(event.amount_mad) }}</span>
            <span class="feed-meta">
              {{ event.category }} · {{ event.city }} · {{ event.payment_method.replaceAll('_', ' ') }}
            </span>
          </li>
        }
      </ul>
    }
  `,
})
export class EventFeedComponent {
  readonly events = input.required<FeedEvent[]>();

  protected readonly labels = EVENT_LABELS;
  protected readonly mad = formatMad;

  protected clock(iso: string): string {
    return new Date(iso).toLocaleTimeString([], { hour12: false });
  }
}
