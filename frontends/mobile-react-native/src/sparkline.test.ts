import { describe, expect, it } from "vitest";

import { sparklinePaths } from "./sparkline";

describe("sparklinePaths", () => {
  it("returns empty paths when there is nothing to draw", () => {
    expect(sparklinePaths([], 100, 50)).toEqual({ line: "", area: "" });
    expect(sparklinePaths([1, 2], 0, 50)).toEqual({ line: "", area: "" });
  });

  it("maps the lowest value to the bottom and the highest to the top", () => {
    const { line, area } = sparklinePaths([0, 10], 100, 50, 0);
    expect(line).toBe("M0.0 50.0 L100.0 0.0");
    expect(area).toBe("M0.0 50.0 L100.0 0.0 L100.0 50.0 L0.0 50.0 Z");
  });

  it("draws a flat series at the baseline instead of dividing by zero", () => {
    expect(sparklinePaths([0, 0, 0], 100, 50, 0).line).toBe("M0.0 50.0 L50.0 50.0 L100.0 50.0");
  });

  it("keeps the y axis anchored at zero", () => {
    // 90 and 100 are both near the top: the chart does not exaggerate a 10% difference.
    const { line } = sparklinePaths([90, 100], 100, 100, 0);
    expect(line).toBe("M0.0 10.0 L100.0 0.0");
  });
});
