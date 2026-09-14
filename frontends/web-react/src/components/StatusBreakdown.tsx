import { formatInteger, formatMad, type Breakdown, type OrderStatus } from "@rad/core";
import { memo } from "react";

import { COLORS } from "../config";
import { useRenderTrace } from "../renderTrace";

const STATUSES: OrderStatus[] = ["placed", "paid", "shipped", "cancelled"];

export const StatusBar = memo(function StatusBar({ counts }: { counts: Record<OrderStatus, number> }) {
  useRenderTrace("status");
  const total = STATUSES.reduce((sum, status) => sum + counts[status], 0);
  return (
    <>
      <div className="card-header">
        <h2>Orders by status</h2>
        <span className="muted">{formatInteger(total)} orders</span>
      </div>
      <div className="status-bar" role="img" aria-label="Share of orders per status">
        {STATUSES.map((status) => (
          // flex-grow is animated in CSS, so segments slide smoothly instead of jumping.
          <div key={status} style={{ flexGrow: counts[status], background: COLORS[status] }} />
        ))}
      </div>
      <ul className="legend">
        {STATUSES.map((status) => (
          <li key={status}>
            <span className="swatch" style={{ background: COLORS[status] }} />
            {status}
            <strong>{formatInteger(counts[status])}</strong>
          </li>
        ))}
      </ul>
    </>
  );
});

export const CityTable = memo(function CityTable({ items }: { items: Breakdown[] }) {
  useRenderTrace("cities");
  const maxRevenue = Math.max(1, ...items.map((item) => item.revenue_mad));
  return (
    <>
      <div className="card-header">
        <h2>Top cities</h2>
        <span className="muted">last 60 min</span>
      </div>
      <table className="table">
        <thead>
          <tr>
            <th>City</th>
            <th className="num">Orders</th>
            <th className="num">Revenue</th>
          </tr>
        </thead>
        <tbody>
          {items.slice(0, 6).map((item) => (
            <tr key={item.value}>
              <td>
                {item.value}
                <div className="inline-bar" style={{ width: `${(item.revenue_mad / maxRevenue) * 100}%` }} />
              </td>
              <td className="num">{formatInteger(item.orders_placed)}</td>
              <td className="num">{formatMad(item.revenue_mad)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </>
  );
});
