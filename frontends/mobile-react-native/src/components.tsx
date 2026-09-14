import {
  describeConnection,
  EVENT_LABELS,
  formatAge,
  formatInteger,
  formatMad,
  formatPercent,
  type Alert,
  type Breakdown,
  type ConnectionState,
  type FeedEvent,
  type Kpis,
  type OrderStatus,
  type TimePoint,
} from "@rad/core";
import { memo, useEffect, useMemo, useState } from "react";
import { StyleSheet, Text, View, type LayoutChangeEvent } from "react-native";
import Svg, { Defs, LinearGradient, Path, Stop } from "react-native-svg";

import { COLORS } from "./config";
import { sparklinePaths } from "./sparkline";

function useNow(intervalMs: number): number {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const timer = setInterval(() => setNow(Date.now()), intervalMs);
    return () => clearInterval(timer);
  }, [intervalMs]);
  return now;
}

export function ConnectionPill({ connection, lastEventAt }: { connection: ConnectionState; lastEventAt: string | null }) {
  const now = useNow(1_000);
  const age = lastEventAt === null ? null : Math.max(0, now - Date.parse(lastEventAt));
  const color =
    connection.status === "open" ? COLORS.ok : connection.status === "offline" ? COLORS.bad : COLORS.warn;
  return (
    <View style={styles.pill} accessibilityRole="text" accessibilityLiveRegion="polite">
      <View style={[styles.dot, { backgroundColor: color }]} />
      <Text style={styles.pillText}>{describeConnection(connection, now)}</Text>
      {connection.status === "open" && age !== null && <Text style={styles.muted}> · {formatAge(age)}</Text>}
    </View>
  );
}

export const AlertBanner = memo(function AlertBanner({ alerts }: { alerts: Alert[] }) {
  const firing = alerts.filter((alert) => alert.status === "firing");
  if (firing.length === 0) return null;
  return (
    <View style={styles.section}>
      {firing.map((alert) => (
        <View
          key={alert.id}
          style={[styles.alert, { borderColor: alert.severity === "critical" ? COLORS.bad : COLORS.warn }]}
        >
          <Text style={[styles.alertTitle, { color: alert.severity === "critical" ? COLORS.bad : COLORS.warn }]}>
            {alert.severity === "critical" ? "Critical" : "Warning"}
          </Text>
          <Text style={styles.text}>{alert.message}</Text>
        </View>
      ))}
    </View>
  );
});

export const KpiGrid = memo(function KpiGrid({ kpis, eventsPerSecond }: { kpis: Kpis; eventsPerSecond: number | null }) {
  const cards = [
    { label: `Revenue · ${kpis.window_minutes} min`, value: formatMad(kpis.revenue_mad) },
    { label: "Orders placed", value: formatInteger(kpis.orders_placed) },
    { label: "Avg order value", value: kpis.avg_order_value_mad === null ? "—" : formatMad(kpis.avg_order_value_mad) },
    {
      label: "Cancellation rate",
      value: formatPercent(kpis.cancellation_rate),
      bad: (kpis.cancellation_rate ?? 0) > 0.15,
    },
    { label: "Events / s", value: eventsPerSecond === null ? "—" : eventsPerSecond.toFixed(1) },
    { label: "Paid · shipped", value: `${formatInteger(kpis.orders_paid)} · ${formatInteger(kpis.orders_shipped)}` },
  ];
  return (
    <View style={styles.grid}>
      {cards.map((card) => (
        <View key={card.label} style={styles.kpi}>
          <Text style={styles.muted}>{card.label}</Text>
          <Text style={[styles.kpiValue, card.bad ? { color: COLORS.bad } : null]} numberOfLines={1} adjustsFontSizeToFit>
            {card.value}
          </Text>
        </View>
      ))}
    </View>
  );
});

export const RevenueSparkline = memo(function RevenueSparkline({ points }: { points: TimePoint[] }) {
  const [width, setWidth] = useState(0);
  const height = 140;
  const paths = useMemo(
    () => sparklinePaths(points.map((p) => p.revenue_mad), width, height),
    [points, width],
  );
  const last = points[points.length - 1];
  return (
    <View style={styles.card}>
      <View style={styles.cardHeader}>
        <Text style={styles.cardTitle}>Revenue per minute</Text>
        <Text style={styles.muted}>{last ? formatMad(last.revenue_mad) : "—"} now</Text>
      </View>
      <View onLayout={(e: LayoutChangeEvent) => setWidth(e.nativeEvent.layout.width)} style={{ height }}>
        {width > 0 && (
          <Svg width={width} height={height}>
            <Defs>
              <LinearGradient id="fill" x1="0" y1="0" x2="0" y2="1">
                <Stop offset="0" stopColor={COLORS.accent} stopOpacity={0.4} />
                <Stop offset="1" stopColor={COLORS.accent} stopOpacity={0} />
              </LinearGradient>
            </Defs>
            <Path d={paths.area} fill="url(#fill)" />
            <Path d={paths.line} stroke={COLORS.accent} strokeWidth={2} fill="none" />
          </Svg>
        )}
      </View>
    </View>
  );
});

export const CategoryBars = memo(function CategoryBars({ items }: { items: Breakdown[] }) {
  const max = Math.max(1, ...items.map((item) => item.revenue_mad));
  return (
    <View style={styles.card}>
      <Text style={styles.cardTitle}>Revenue by category</Text>
      {items.map((item) => (
        <View key={item.value} style={styles.barRow}>
          <Text style={[styles.text, styles.barLabel]}>{item.value}</Text>
          <View style={styles.barTrack}>
            <View style={[styles.barFill, { width: `${(item.revenue_mad / max) * 100}%` }]} />
          </View>
          <Text style={[styles.muted, styles.barValue]}>{formatMad(item.revenue_mad)}</Text>
        </View>
      ))}
    </View>
  );
});

const STATUSES: OrderStatus[] = ["placed", "paid", "shipped", "cancelled"];

export const StatusRow = memo(function StatusRow({ counts }: { counts: Record<OrderStatus, number> }) {
  return (
    <View style={styles.card}>
      <Text style={styles.cardTitle}>Orders by status</Text>
      <View style={styles.statusBar}>
        {STATUSES.map((status) => (
          <View key={status} style={{ flexGrow: counts[status], backgroundColor: COLORS[status] }} />
        ))}
      </View>
      <View style={styles.legend}>
        {STATUSES.map((status) => (
          <Text key={status} style={styles.text}>
            <Text style={{ color: COLORS[status] }}>● </Text>
            {status} {formatInteger(counts[status])}
          </Text>
        ))}
      </View>
    </View>
  );
});

export const EventFeed = memo(function EventFeed({ events }: { events: FeedEvent[] }) {
  return (
    <View style={styles.card}>
      <Text style={styles.cardTitle}>Live events</Text>
      {events.length === 0 && <Text style={styles.muted}>Waiting for events…</Text>}
      {/* A plain map, not a FlatList: nested inside the screen's ScrollView and capped at 15 rows. */}
      {events.slice(0, 15).map((event) => (
        <View key={event.event_id} style={styles.feedRow}>
          <Text style={[styles.text, { color: COLORS[event.event_type.replace("order_", "") as OrderStatus] }]}>
            {EVENT_LABELS[event.event_type]}
          </Text>
          <Text style={[styles.muted, styles.feedMeta]} numberOfLines={1}>
            {event.category} · {event.city}
          </Text>
          <Text style={styles.text}>{formatMad(event.amount_mad)}</Text>
        </View>
      ))}
    </View>
  );
});

const styles = StyleSheet.create({
  text: { color: COLORS.text, fontSize: 14 },
  muted: { color: COLORS.muted, fontSize: 12 },
  section: { gap: 8, marginBottom: 12 },
  pill: {
    flexDirection: "row",
    alignItems: "center",
    alignSelf: "flex-start",
    backgroundColor: COLORS.surface,
    borderColor: COLORS.border,
    borderWidth: 1,
    borderRadius: 999,
    paddingHorizontal: 12,
    paddingVertical: 6,
    marginTop: 8,
  },
  dot: { width: 8, height: 8, borderRadius: 4, marginRight: 8 },
  pillText: { color: COLORS.text, fontSize: 13 },
  alert: { borderWidth: 1, borderRadius: 12, padding: 12, backgroundColor: COLORS.surface, gap: 4 },
  alertTitle: { fontWeight: "700" },
  grid: { flexDirection: "row", flexWrap: "wrap", gap: 10, marginBottom: 12 },
  kpi: {
    flexBasis: "47%",
    flexGrow: 1,
    backgroundColor: COLORS.surface,
    borderColor: COLORS.border,
    borderWidth: 1,
    borderRadius: 12,
    padding: 12,
  },
  kpiValue: { color: COLORS.text, fontSize: 20, fontWeight: "700", marginTop: 4, fontVariant: ["tabular-nums"] },
  card: {
    backgroundColor: COLORS.surface,
    borderColor: COLORS.border,
    borderWidth: 1,
    borderRadius: 12,
    padding: 14,
    marginBottom: 12,
    gap: 8,
  },
  cardHeader: { flexDirection: "row", justifyContent: "space-between", alignItems: "baseline" },
  cardTitle: { color: COLORS.text, fontSize: 15, fontWeight: "600" },
  barRow: { flexDirection: "row", alignItems: "center", gap: 8 },
  barLabel: { width: 84, textTransform: "capitalize" },
  barTrack: { flex: 1, height: 8, borderRadius: 4, backgroundColor: COLORS.surface2, overflow: "hidden" },
  barFill: { height: 8, borderRadius: 4, backgroundColor: COLORS.accent },
  barValue: { width: 92, textAlign: "right" },
  statusBar: { flexDirection: "row", height: 12, borderRadius: 6, overflow: "hidden", backgroundColor: COLORS.surface2 },
  legend: { flexDirection: "row", flexWrap: "wrap", columnGap: 16, rowGap: 4 },
  feedRow: {
    flexDirection: "row",
    alignItems: "center",
    gap: 8,
    paddingVertical: 6,
    borderBottomColor: COLORS.border,
    borderBottomWidth: StyleSheet.hairlineWidth,
  },
  feedMeta: { flex: 1, textTransform: "capitalize" },
});
