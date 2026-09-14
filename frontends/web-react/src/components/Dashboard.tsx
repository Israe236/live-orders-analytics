import type { Alert, FeedEvent, MetricsSnapshot } from "@rad/core";
import { useMemo } from "react";

import { AlertBanner, AlertHistory } from "./Alerts";
import { CategoryBreakdown } from "./CategoryBreakdown";
import { EventFeed } from "./EventFeed";
import { KpiCards } from "./KpiCards";
import { RevenueChart } from "./RevenueChart";
import { CityTable, StatusBar } from "./StatusBreakdown";

export interface DashboardProps {
  snapshot: MetricsSnapshot;
  eventsPerSecond: number | null;
  feed: FeedEvent[];
  alerts: Alert[];
}

/**
 * Pure layout. Every section is memoized and receives only its own slice of the state; the
 * reducer in @rad/core keeps unchanged slices at the same object identity, so a live update
 * re-renders only the sections whose data actually changed.
 */
export function Dashboard({ snapshot, eventsPerSecond, feed, alerts }: DashboardProps) {
  const firing = useMemo(() => alerts.filter((alert) => alert.status === "firing"), [alerts]);
  return (
    <>
      <AlertBanner alerts={firing} />
      <main className="grid">
        <section className="area-kpis">
          <KpiCards kpis={snapshot.kpis} eventsPerSecond={eventsPerSecond} />
        </section>
        <section className="card area-revenue">
          <RevenueChart minute={snapshot.revenue_per_minute} hour={snapshot.revenue_per_hour} />
        </section>
        <section className="card area-categories">
          <CategoryBreakdown items={snapshot.categories} />
        </section>
        <section className="card area-status">
          <StatusBar counts={snapshot.orders_by_status} />
        </section>
        <section className="card area-cities">
          <CityTable items={snapshot.cities} />
        </section>
        <section className="card area-feed">
          <EventFeed events={feed} />
        </section>
        <section className="card area-alerts">
          <AlertHistory alerts={alerts} />
        </section>
      </main>
    </>
  );
}
