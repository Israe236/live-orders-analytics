import type { Alert } from "@rad/core";
import { memo } from "react";

import { useRenderTrace } from "../renderTrace";

function time(iso: string | null): string {
  return iso === null ? "—" : new Date(iso).toLocaleTimeString([], { hour12: false });
}

export const AlertBanner = memo(function AlertBanner({ alerts }: { alerts: Alert[] }) {
  useRenderTrace("alertBanner");
  if (alerts.length === 0) return null;
  return (
    <div className="alert-banner" role="alert">
      {alerts.map((alert) => (
        <div key={alert.id} className={`alert-item alert-${alert.severity}`}>
          <strong>{alert.severity === "critical" ? "Critical" : "Warning"}</strong>
          <span>{alert.message}</span>
          <span className="muted">since {time(alert.started_at)}</span>
        </div>
      ))}
    </div>
  );
});

export const AlertHistory = memo(function AlertHistory({ alerts }: { alerts: Alert[] }) {
  useRenderTrace("alertHistory");
  return (
    <>
      <div className="card-header">
        <h2>Alerts</h2>
        <span className="muted">{alerts.filter((a) => a.status === "firing").length} firing</span>
      </div>
      {alerts.length === 0 ? (
        <p className="empty">No alerts. Thresholds are checked every second.</p>
      ) : (
        <table className="table">
          <thead>
            <tr>
              <th>Status</th>
              <th>Rule</th>
              <th>Message</th>
              <th className="num">Started</th>
              <th className="num">Resolved</th>
            </tr>
          </thead>
          <tbody>
            {alerts.map((alert) => (
              <tr key={alert.id}>
                <td>
                  <span className={`chip chip-alert-${alert.status}`}>{alert.status}</span>
                </td>
                <td>{alert.rule.replaceAll("_", " ")}</td>
                <td>{alert.message}</td>
                <td className="num">{time(alert.started_at)}</td>
                <td className="num">{time(alert.resolved_at)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </>
  );
});
