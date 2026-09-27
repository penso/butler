import { expect, test } from "../fixtures";

test("without credentials every page asks for them", async ({ browser, startServer }) => {
  const server = await startServer({ auth: "admin:s3cret" });
  const context = await browser.newContext();
  const page = await context.newPage();
  const response = await page.goto(server.url + "/");
  expect(response!.status()).toBe(401);
  expect(response!.headers()["www-authenticate"]).toContain("Basic");
  for (const path of ["/assets/app.css", "/events", "/api/stats"]) {
    const answer = await context.request.get(server.url + path);
    expect(answer.status(), path).toBe(401);
  }
  await context.close();

  const wrong = await browser.newContext({ httpCredentials: { username: "admin", password: "nope" } });
  const denied = await (await wrong.newPage()).goto(server.url + "/");
  expect(denied!.status()).toBe(401);
  await wrong.close();
});

for (const base of ["", "/admin/jobs"]) {
  test(`with credentials the dashboard, its live stream and actions work${base ? " under " + base : ""}`, async ({
    browser,
    startServer,
  }) => {
    const server = await startServer({ auth: "admin:s3cret", base });
    const context = await browser.newContext({ httpCredentials: { username: "admin", password: "s3cret" } });
    const page = await context.newPage();
    const response = await page.goto(server.url + "/");
    expect(response!.status()).toBe(200);
    // Styles, script and SSE all went through the same credentials.
    await expect(page.locator("[data-live-label]")).toHaveText("live");
    await expect(page.locator("#live-chart canvas")).toBeVisible();

    await page.goto(server.url + "/jobs?state=scheduled&queue=default");
    page.once("dialog", (dialog) => dialog.accept());
    await page.getByRole("button", { name: "Run all now" }).click();
    await expect(page.getByText("No scheduled jobs on default.")).toBeVisible();
    await expect(page).toHaveURL(server.url + "/jobs?state=scheduled&queue=default");
    await context.close();
  });
}
