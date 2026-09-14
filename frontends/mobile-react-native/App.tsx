import { StatusBar } from "expo-status-bar";
import { ScrollView, StyleSheet, Text, View } from "react-native";
import { SafeAreaProvider, SafeAreaView } from "react-native-safe-area-context";

import {
  AlertBanner,
  CategoryBars,
  ConnectionPill,
  EventFeed,
  KpiGrid,
  RevenueSparkline,
  StatusRow,
} from "./src/components";
import { API_URL, COLORS } from "./src/config";
import { useLiveDashboard } from "./src/useLiveDashboard";

export default function App() {
  const { state, connection } = useLiveDashboard();
  const snapshot = state.snapshot;
  const live = connection.status === "open";

  return (
    <SafeAreaProvider>
      <SafeAreaView style={styles.screen} edges={["top", "left", "right"]}>
        <StatusBar style="light" />
        <ScrollView contentContainerStyle={styles.content}>
          <View style={styles.header}>
            <Text style={styles.title}>Live Orders</Text>
            <Text style={styles.subtitle}>E-commerce analytics · Morocco · React Native</Text>
            <ConnectionPill
              connection={connection}
              lastEventAt={state.pipeline?.last_event_occurred_at ?? null}
            />
          </View>

          {snapshot === null ? (
            <Text style={styles.subtitle}>Connecting to {API_URL}…</Text>
          ) : (
            <>
              {/* Each section gets its own slice; unchanged slices keep their identity, so the
                  memoized sections skip re-rendering on live updates. */}
              <AlertBanner alerts={state.alerts} />
              <KpiGrid
                kpis={snapshot.kpis}
                eventsPerSecond={live ? (state.pipeline?.events_per_second ?? null) : null}
              />
              <RevenueSparkline points={snapshot.revenue_per_minute} />
              <CategoryBars items={snapshot.categories} />
              <StatusRow counts={snapshot.orders_by_status} />
              <EventFeed events={state.feed} />
            </>
          )}
        </ScrollView>
      </SafeAreaView>
    </SafeAreaProvider>
  );
}

const styles = StyleSheet.create({
  screen: { flex: 1, backgroundColor: COLORS.bg },
  content: { padding: 16, paddingBottom: 40 },
  header: { marginBottom: 16 },
  title: { color: COLORS.text, fontSize: 24, fontWeight: "700" },
  subtitle: { color: COLORS.muted, fontSize: 13, marginTop: 2 },
});
