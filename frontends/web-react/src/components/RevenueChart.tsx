import { formatClock, formatCompact, formatMad, type TimePoint } from "@rad/core";
import { memo, useState } from "react";
import { Area, AreaChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";

import { COLORS } from "../config";
import { useRenderTrace } from "../renderTrace";

type Granularity = "minute" | "hour";

interface Props {
  minute: TimePoint[];
  hour: TimePoint[];
}

const LABELS: Record<Granularity, string> = { minute: "Per minute · 1 h", hour: "Per hour · 24 h" };

export const RevenueChart = memo(function RevenueChart({ minute, hour }: Props) {
  useRenderTrace("revenue");
  const [granularity, setGranularity] = useState<Granularity>("minute");
  const data = granularity === "minute" ? minute : hour;

  return (
    <>
      <div className="card-header">
        <h2>Revenue</h2>
        <div className="segmented" role="tablist" aria-label="Granularity">
          {(Object.keys(LABELS) as Granularity[]).map((g) => (
            <button
              key={g}
              type="button"
              role="tab"
              aria-selected={g === granularity}
              className={g === granularity ? "active" : undefined}
              onClick={() => setGranularity(g)}
            >
              {LABELS[g]}
            </button>
          ))}
        </div>
      </div>
      <div className="chart">
        <ResponsiveContainer width="100%" height="100%">
          <AreaChart data={data} margin={{ top: 8, right: 12, bottom: 0, left: 0 }}>
            <defs>
              <linearGradient id="revenueFill" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stopColor={COLORS.accent} stopOpacity={0.4} />
                <stop offset="100%" stopColor={COLORS.accent} stopOpacity={0} />
              </linearGradient>
            </defs>
            <CartesianGrid stroke={COLORS.grid} vertical={false} />
            <XAxis
              dataKey="bucket"
              tickFormatter={(value: string) => formatClock(value)}
              minTickGap={40}
              stroke={COLORS.muted}
              tickLine={false}
            />
            <YAxis
              tickFormatter={(value: number) => formatCompact(value)}
              width={52}
              stroke={COLORS.muted}
              tickLine={false}
              axisLine={false}
            />
            <Tooltip
              contentStyle={{ background: "#141925", border: `1px solid ${COLORS.grid}`, borderRadius: 8 }}
              labelFormatter={(label) => formatClock(String(label))}
              formatter={(value) => [formatMad(Number(value)), "Revenue"]}
            />
            {/* No animation: each live update would otherwise replay the drawing animation
                on the whole line, which reads as flicker. */}
            <Area
              type="monotone"
              dataKey="revenue_mad"
              stroke={COLORS.accent}
              strokeWidth={2}
              fill="url(#revenueFill)"
              dot={false}
              isAnimationActive={false}
            />
          </AreaChart>
        </ResponsiveContainer>
      </div>
    </>
  );
});
