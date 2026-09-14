// Captures README screenshots of the running dashboards with a headless Chromium.
//
//   docker compose up -d --build          (optionally GEN_FORCE_ANOMALY=payment_outage)
//   cd frontends && node scripts/capture-screenshots.mjs
//
// Environment: REACT_URL, ANGULAR_URL, OUT_DIR, SETTLE_MS.
import { mkdir } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { chromium } from "playwright";

const here = path.dirname(fileURLToPath(import.meta.url));
const outDir = process.env.OUT_DIR ?? path.resolve(here, "../../docs/images");
const settleMs = Number(process.env.SETTLE_MS ?? 6000);

const targets = [
  { name: "react-dashboard", url: process.env.REACT_URL ?? "http://localhost:5173" },
  { name: "angular-dashboard", url: process.env.ANGULAR_URL ?? "http://localhost:4200" },
];

await mkdir(outDir, { recursive: true });
const browser = await chromium.launch();
try {
  for (const target of targets) {
    const page = await browser.newPage({
      viewport: { width: 1440, height: 1000 },
      deviceScaleFactor: 1,
      colorScheme: "dark",
    });
    await page.goto(target.url, { waitUntil: "domcontentloaded" });
    // Live data has arrived: KPI values and at least one feed row are rendered.
    await page.waitForSelector(".kpi-value", { timeout: 30_000 });
    await page.waitForSelector(".feed-row", { timeout: 30_000 });
    // Let charts receive a few live updates and the arrival highlight fade.
    await page.waitForTimeout(settleMs);

    const status = await page.locator('[role="status"]').first().innerText();
    const alerts = await page.locator(".alert-item").count();
    const file = path.join(outDir, `${target.name}.png`);
    await page.screenshot({ path: file, fullPage: true });
    console.log(`${target.name}: ${file} (status: ${status.replace(/\s+/g, " ")}, firing alerts: ${alerts})`);
    await page.close();
  }
} finally {
  await browser.close();
}
