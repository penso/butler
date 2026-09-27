import { expect, test } from "../fixtures";

test("under a nested base path, navigation, live updates and actions stay inside it", async ({
  page,
  startServer,
}) => {
  const server = await startServer({ base: "/admin/jobs" });
  await page.goto(server.home);
  await expect(page.locator("[data-live-label]")).toHaveText("live");
  await expect(page.locator("#live-chart canvas")).toBeVisible();

  for (const name of ["Jobs", "Recurring", "Workers", "Dashboard"]) {
    await page.getByRole("navigation").getByRole("link", { name, exact: true }).click();
    await expect(page.locator("main")).not.toContainText("page not found");
    expect(new URL(page.url()).pathname.startsWith("/admin/jobs")).toBe(true);
  }
  await expect(page).toHaveURL(server.home);

  // An error page links back into the dashboard too.
  const response = await page.goto(server.url + "/jobs/nope");
  expect(response!.status()).toBe(404);
  await page.getByRole("link", { name: "Back to the dashboard" }).click();
  await expect(page).toHaveURL(server.home);
  await expect(page.locator("[data-live-label]")).toHaveText("live");

  // A queue page, and back.
  await page.goto(server.url + "/queues/default");
  await expect(page.locator("#history-chart canvas")).toBeVisible();
  await page.getByRole("link", { name: "← Dashboard" }).click();
  await expect(page).toHaveURL(server.home);
});
