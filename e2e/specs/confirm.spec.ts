import { expect, test } from "../fixtures";

test("discard all asks first, and does nothing when dismissed", async ({ page, startServer }) => {
  const server = await startServer();
  // Only the seeded dead job on default: the busy queue's failures land elsewhere.
  await page.goto(server.url + "/jobs?state=dead&queue=default");
  const row = page.getByRole("link", { name: "charge_card" });
  await expect(row).toBeVisible();

  page.once("dialog", async (dialog) => {
    expect(dialog.type()).toBe("confirm");
    expect(dialog.message()).toBe("Discard every dead job on default? This deletes them.");
    await dialog.dismiss();
  });
  await page.getByRole("button", { name: "Discard all" }).click();
  await expect(row).toBeVisible();

  page.once("dialog", (dialog) => dialog.accept());
  await page.getByRole("button", { name: "Discard all" }).click();
  await expect(page.getByText("No dead jobs on default.")).toBeVisible();
  await expect(page).toHaveURL(server.url + "/jobs?state=dead&queue=default");
});

test("pausing a queue asks first, resuming doesn't", async ({ page, startServer }) => {
  const server = await startServer();
  await page.goto(server.home);
  const row = page.locator("tr", { has: page.getByRole("link", { name: "mailers", exact: true }) }).first();

  page.once("dialog", (dialog) => dialog.dismiss());
  await row.getByRole("button", { name: "Pause" }).click();
  await expect(row.getByRole("button", { name: "Pause" })).toBeVisible();

  page.once("dialog", async (dialog) => {
    expect(dialog.message()).toContain("Pause mailers?");
    await dialog.accept();
  });
  await row.getByRole("button", { name: "Pause" }).click();
  await expect(row.getByText("paused")).toBeVisible();

  let asked = false;
  page.once("dialog", (dialog) => {
    asked = true;
    return dialog.accept();
  });
  await row.getByRole("button", { name: "Resume" }).click();
  await expect(row.getByRole("button", { name: "Pause" })).toBeVisible();
  expect(asked).toBe(false);
});
