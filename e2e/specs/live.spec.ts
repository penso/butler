import { expect, test } from "../fixtures";

test("live counts and the live chart update over server-sent events", async ({ page, startServer }) => {
  const server = await startServer();
  await page.goto(server.url + "/");
  await expect(page.locator("[data-live-label]")).toHaveText("live");
  await expect(page.locator("#live-chart canvas")).toBeVisible();

  // The server finishes a job every 100 ms: the rate and the total move.
  await expect(page.locator('[data-rate="processed"]')).not.toHaveText(/^0(\.0)?$/);
  const processed = page.locator('[data-stat="processed_total"]');
  const before = Number((await processed.textContent())!.replace(/,/g, ""));
  await expect
    .poll(async () => Number((await processed.textContent())!.replace(/,/g, "")))
    .toBeGreaterThan(before);

  // The chart's newest point is drawn from those snapshots: its legend shows
  // a non-zero rate once the cursor is on it.
  const chart = page.locator("#live-chart .u-over");
  const box = (await chart.boundingBox())!;
  await page.mouse.move(box.x + box.width - 2, box.y + box.height / 2);
  await expect(page.locator("#live-chart .u-legend .u-series").nth(1).locator(".u-value")).not.toHaveText(/^(-|0(\.0)?)$/);

  await expect(page.locator("#history-chart canvas")).toBeVisible();
  await expect(page.locator("#duration-chart canvas")).toBeVisible();
});

test("the live stream reconnects after the server restarts", async ({ page, startServer }) => {
  const server = await startServer();
  await page.goto(server.url + "/");
  const label = page.locator("[data-live-label]");
  await expect(label).toHaveText("live");

  await server.stop();
  await expect(label).toHaveText("reconnecting");

  await server.restart();
  // EventSource retries on its own (every few seconds): no reload.
  await expect(label).toHaveText("live", { timeout: 15_000 });
  const processed = page.locator('[data-stat="processed_total"]');
  const before = Number((await processed.textContent())!.replace(/,/g, ""));
  await expect
    .poll(async () => Number((await processed.textContent())!.replace(/,/g, "")))
    .not.toBe(before);
});
