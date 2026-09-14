export interface SparklinePaths {
  /** SVG path of the line itself. */
  line: string;
  /** Same line closed down to the baseline, for a filled area under it. */
  area: string;
}

/**
 * SVG paths for a small line chart. Pure (no React Native imports) so it is unit-tested directly.
 * The y axis starts at 0 (or the lowest value if negative) so a flat day does not look volatile.
 */
export function sparklinePaths(
  values: readonly number[],
  width: number,
  height: number,
  padding = 4,
): SparklinePaths {
  if (values.length === 0 || width <= 0 || height <= 0) return { line: "", area: "" };

  const max = Math.max(...values);
  const min = Math.min(0, ...values);
  const span = max - min || 1;
  const innerWidth = width - padding * 2;
  const innerHeight = height - padding * 2;
  const step = values.length > 1 ? innerWidth / (values.length - 1) : 0;
  const fmt = (n: number) => n.toFixed(1);

  const points = values.map((value, i) => ({
    x: padding + i * step,
    y: padding + innerHeight - ((value - min) / span) * innerHeight,
  }));
  const line = points.map((p, i) => `${i === 0 ? "M" : "L"}${fmt(p.x)} ${fmt(p.y)}`).join(" ");
  const baseline = fmt(padding + innerHeight);
  const first = points[0]!;
  const last = points[points.length - 1]!;
  const area = `${line} L${fmt(last.x)} ${baseline} L${fmt(first.x)} ${baseline} Z`;
  return { line, area };
}
