/**
 * Counts renders per dashboard section, in tests only.
 *
 * It lets a test prove that when a live update leaves e.g. the category breakdown unchanged,
 * that section does not re-render at all. Outside tests `import.meta.env.MODE` is replaced at
 * build time, so the bundler removes the counting code entirely.
 */
export const renderTrace: Record<string, number> = {};

export function useRenderTrace(section: string): void {
  if (import.meta.env.MODE === "test") {
    renderTrace[section] = (renderTrace[section] ?? 0) + 1;
  }
}

export function resetRenderTrace(): void {
  for (const key of Object.keys(renderTrace)) delete renderTrace[key];
}
