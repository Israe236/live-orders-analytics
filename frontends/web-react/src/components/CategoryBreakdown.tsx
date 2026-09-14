import { formatCompact, formatMad, type Breakdown } from "@rad/core";
import { memo } from "react";
import { Bar, BarChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";

import { COLORS } from "../config";
import { useRenderTrace } from "../renderTrace";

function titleCase(value: string): string {
  return value.charAt(0).toUpperCase() + value.slice(1);
}

export const CategoryBreakdown = memo(function CategoryBreakdown({ items }: { items: Breakdown[] }) {
  useRenderTrace("categories");
  return (
    <>
      <div className="card-header">
        <h2>Revenue by category</h2>
        <span className="muted">last 60 min</span>
      </div>
      <div className="chart chart-bars">
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={items} layout="vertical" margin={{ top: 4, right: 16, bottom: 4, left: 8 }}>
            <XAxis
              type="number"
              tickFormatter={(value: number) => formatCompact(value)}
              stroke={COLORS.muted}
              tickLine={false}
              axisLine={false}
            />
            <YAxis
              type="category"
              dataKey="value"
              width={92}
              tickFormatter={(value: string) => titleCase(value)}
              stroke={COLORS.muted}
              tickLine={false}
              axisLine={false}
            />
            <Tooltip
              cursor={{ fill: "rgba(255,255,255,0.04)" }}
              contentStyle={{ background: "#141925", border: `1px solid ${COLORS.grid}`, borderRadius: 8 }}
              formatter={(value) => [formatMad(Number(value)), "Revenue"]}
              labelFormatter={(label) => titleCase(String(label))}
            />
            <Bar dataKey="revenue_mad" fill={COLORS.accent} radius={[0, 4, 4, 0]} isAnimationActive={false} />
          </BarChart>
        </ResponsiveContainer>
      </div>
    </>
  );
});
