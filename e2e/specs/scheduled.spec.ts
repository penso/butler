import { expect, test } from "../fixtures";

test("run all now starts every scheduled job on a queue, after confirming", async ({ page, startServer }) => {
  const server = await startServer();
  await page.goto(server.url + "/jobs?state=scheduled&queue=mailers");
  const rows = page.getByRole("link", { name: "send_reminder" });
  await expect(rows).toHaveCount(2);

  page.once("dialog", async (dialog) => {
    expect(dialog.message()).toBe("Run every scheduled job on mailers now?");
    await dialog.dismiss();
  });
  await page.getByRole("button", { name: "Run all now" }).click();
  await expect(rows).toHaveCount(2);

  page.once("dialog", (dialog) => dialog.accept());
  await page.getByRole("button", { name: "Run all now" }).click();
  await expect(page.getByText("No scheduled jobs on mailers.")).toBeVisible();
  await expect(page).toHaveURL(server.url + "/jobs?state=scheduled&queue=mailers");

  await page.goto(server.url + "/jobs?state=pending&queue=mailers");
  await expect(page.getByRole("link", { name: "send_reminder" })).toHaveCount(2);
  // The other queue's scheduled job wasn't touched.
  await page.goto(server.url + "/jobs?state=scheduled&queue=default");
  await expect(page.getByRole("link", { name: "send_reminder" })).toHaveCount(1);
});

test("cancel all cancels every scheduled job, after confirming", async ({ page, startServer }) => {
  const server = await startServer();
  await page.goto(server.url + "/jobs?state=scheduled");
  await expect(page.getByRole("link", { name: "send_reminder" })).toHaveCount(3);

  page.once("dialog", async (dialog) => {
    expect(dialog.message()).toBe("Cancel every scheduled job? They won't run.");
    await dialog.accept();
  });
  await page.getByRole("button", { name: "Cancel all" }).click();
  await expect(page.getByText("No scheduled jobs.")).toBeVisible();

  await page.goto(server.url + "/jobs?state=cancelled");
  await expect(page.getByRole("link", { name: "send_reminder" })).toHaveCount(3);
  // Pending work stays.
  await page.goto(server.url + "/jobs?state=pending&queue=default");
  await expect(page.getByRole("link", { name: "resize" })).toBeVisible();
});

test("queue and job pages draw their own charts and live counts", async ({ page, startServer }) => {
  const server = await startServer();
  // The busy queue's runs, in its narrowed series: once there are some, the
  // dashboard's busiest-jobs table lists it.
  await expect
    .poll(async () => {
      const series = await page.request.get(server.url + "/api/metrics?minutes=5&queue=live");
      const processed: number[] = (await series.json()).processed;
      return processed.reduce((a, b) => a + b, 0);
    })
    .toBeGreaterThan(0);
  await page.goto(server.home);
  await page.getByRole("link", { name: "live", exact: true }).first().click();
  await expect(page).toHaveURL(server.url + "/queues/live");
  await expect(page.locator("[data-live-label]")).toHaveText("live");
  await expect(page.locator("#history-chart")).toHaveAttribute("data-queue", "live");
  await expect(page.locator("#history-chart canvas")).toBeVisible();
  await expect(page.locator("#duration-chart canvas")).toBeVisible();

  await page.getByRole("link", { name: "tick" }).click();
  await expect(page).toHaveURL(server.url + "/metrics/tick?queue=live");
  await expect(page.locator("#history-chart")).toHaveAttribute("data-job", "tick");
  await expect(page.locator("#history-chart canvas")).toBeVisible();
});
