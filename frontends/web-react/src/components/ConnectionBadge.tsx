import { describeConnection, formatAge, type ConnectionState } from "@rad/core";

import { useNow } from "../hooks/useNow";

interface Props {
  connection: ConnectionState;
  /** Newest committed event time, from the server's pipeline stats. */
  lastEventAt: string | null;
}

const STALE_AFTER_MS = 10_000;

export function ConnectionBadge({ connection, lastEventAt }: Props) {
  // Ticks every second so the countdown and the data age stay current; only this badge re-renders.
  const now = useNow(1_000);
  const ageMs = lastEventAt === null ? null : Math.max(0, now - Date.parse(lastEventAt));
  const stale = connection.status === "open" && ageMs !== null && ageMs > STALE_AFTER_MS;
  const tone =
    connection.status === "open" ? (stale ? "warn" : "ok") : connection.status === "offline" ? "bad" : "warn";

  return (
    <div className={`badge badge-${tone}`} role="status" aria-live="polite">
      <span className="badge-dot" aria-hidden="true" />
      <span>{describeConnection(connection, now)}</span>
      {connection.status === "open" && ageMs !== null && (
        <span className="badge-detail">data {formatAge(ageMs)} old</span>
      )}
    </div>
  );
}
