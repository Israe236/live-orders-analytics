import type { Kpis } from "@rad/core";
import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { KpiCards } from "./KpiCards";

const kpis: Kpis = {
  window_minutes: 60,
  orders_placed: 1_200,
  orders_paid: 1_000,
  orders_shipped: 700,
  orders_cancelled: 200,
  revenue_mad: 1_234_567,
  avg_order_value_mad: null,
  cancellation_rate: 0.1667,
};

describe("KpiCards", () => {
  it("formats values and flags a high cancellation rate", () => {
    const { container } = render(<KpiCards kpis={kpis} eventsPerSecond={41.26} />);
    expect(screen.getByText("Revenue · last 60 min")).toBeTruthy();
    expect(screen.getByText("16.7%")).toBeTruthy();
    expect(screen.getByText("41.3")).toBeTruthy();
    expect(screen.getByText("1,000 paid · 700 shipped")).toBeTruthy();
    // No paid order → no average, shown as a dash rather than "NaN" or "0".
    expect(screen.getAllByText("—").length).toBe(1);
    expect(container.querySelector(".kpi-bad")?.textContent).toContain("Cancellation rate");
  });
});
