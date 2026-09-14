import { describe, expect, it } from "vitest";

import { backoffDelay } from "./backoff";

describe("backoffDelay", () => {
  it("doubles the ceiling on each attempt, up to the cap", () => {
    const ceiling = (attempt: number) => backoffDelay(attempt, { random: () => 0.999999, minMs: 0 });
    expect([0, 1, 2, 3, 4, 5, 6, 20].map(ceiling)).toEqual([
      500, 1_000, 2_000, 4_000, 8_000, 16_000, 30_000, 30_000,
    ]);
  });

  it("picks a random delay between the floor and the ceiling", () => {
    expect(backoffDelay(3, { random: () => 0 })).toBe(250); // floor, never an immediate retry
    expect(backoffDelay(3, { random: () => 0.5 })).toBe(2_000);
  });

  it("spreads many clients over the whole range", () => {
    const delays = Array.from({ length: 1_000 }, () => backoffDelay(4));
    expect(Math.min(...delays)).toBeGreaterThanOrEqual(250);
    expect(Math.max(...delays)).toBeLessThanOrEqual(8_000);
    expect(new Set(delays).size).toBeGreaterThan(500);
  });
});
