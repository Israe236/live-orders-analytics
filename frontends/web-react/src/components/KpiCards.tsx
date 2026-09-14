import { formatInteger, formatMad, formatPercent, type Kpis } from "@rad/core";
import { memo } from "react";

import { useRenderTrace } from "../renderTrace";

interface Props {
  kpis: Kpis;
  eventsPerSecond: number | null;
}

const CANCELLATION_WARNING = 0.15;

export const KpiCards = memo(function KpiCards({ kpis, eventsPerSecond }: Props) {
  useRenderTrace("kpis");
  const cancellationHigh = (kpis.cancellation_rate ?? 0) > CANCELLATION_WARNING;

  return (
    <div className="kpis">
      <Kpi label={`Revenue · last ${kpis.window_minutes} min`} value={formatMad(kpis.revenue_mad)} />
      <Kpi
        label="Orders placed"
        value={formatInteger(kpis.orders_placed)}
        detail={`${formatInteger(kpis.orders_paid)} paid · ${formatInteger(kpis.orders_shipped)} shipped`}
      />
      <Kpi
        label="Average order value"
        value={kpis.avg_order_value_mad === null ? "—" : formatMad(kpis.avg_order_value_mad)}
      />
      <Kpi
        label="Cancellation rate"
        value={formatPercent(kpis.cancellation_rate)}
        detail={`${formatInteger(kpis.orders_cancelled)} cancelled`}
        tone={cancellationHigh ? "bad" : undefined}
      />
      <Kpi label="Events / second" value={eventsPerSecond === null ? "—" : eventsPerSecond.toFixed(1)} />
    </div>
  );
});

function Kpi({ label, value, detail, tone }: { label: string; value: string; detail?: string; tone?: "bad" }) {
  return (
    <div className={tone ? `kpi kpi-${tone}` : "kpi"}>
      <div className="kpi-label">{label}</div>
      <div className="kpi-value">{value}</div>
      {detail && <div className="kpi-detail">{detail}</div>}
    </div>
  );
}
