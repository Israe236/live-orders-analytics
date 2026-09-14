import { EVENT_LABELS, formatMad, type FeedEvent } from "@rad/core";
import { memo } from "react";

import { useRenderTrace } from "../renderTrace";

function clock(iso: string): string {
  return new Date(iso).toLocaleTimeString([], { hour12: false });
}

export const EventFeed = memo(function EventFeed({ events }: { events: FeedEvent[] }) {
  useRenderTrace("feed");
  return (
    <>
      <div className="card-header">
        <h2>Live events</h2>
        <span className="muted">newest first</span>
      </div>
      {events.length === 0 ? (
        <p className="empty">Waiting for events…</p>
      ) : (
        <ul className="feed">
          {events.map((event) => (
            // Stable keys: only new rows mount (and play the highlight animation).
            <li key={event.event_id} className="feed-row">
              <span className="feed-time">{clock(event.occurred_at)}</span>
              <span className={`chip chip-${event.event_type}`}>{EVENT_LABELS[event.event_type]}</span>
              <span className="feed-amount">{formatMad(event.amount_mad)}</span>
              <span className="feed-meta">
                {event.category} · {event.city} · {event.payment_method.replaceAll("_", " ")}
              </span>
            </li>
          ))}
        </ul>
      )}
    </>
  );
});
