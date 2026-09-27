import { expect, test } from "../fixtures";

test.use({ colorScheme: "dark" });

test("the theme toggle switches, redraws the charts, and is remembered", async ({ page, startServer }) => {
  const server = await startServer();
  await page.goto(server.url + "/");
  const html = page.locator("html");
  await expect(html).toHaveAttribute("data-theme", "dark");
  const background = () => page.evaluate(() => getComputedStyle(document.body).backgroundColor);
  const dark = await background();

  await page.getByRole("button", { name: "Theme" }).click();
  await expect(html).toHaveAttribute("data-theme", "light");
  expect(await background()).not.toBe(dark);
  // Charts are drawn again in the new colors, not left blank.
  await expect(page.locator("#live-chart canvas")).toBeVisible();
  await expect(page.locator("#history-chart canvas")).toBeVisible();

  await page.reload();
  await expect(html).toHaveAttribute("data-theme", "light");
  await page.goto(server.url + "/workers");
  await expect(html).toHaveAttribute("data-theme", "light");

  await page.getByRole("button", { name: "Theme" }).click();
  await expect(html).toHaveAttribute("data-theme", "dark");
});

test.describe("with a light system theme", () => {
  test.use({ colorScheme: "light" });

  test("follows the system until toggled", async ({ page, startServer }) => {
    const server = await startServer();
    await page.goto(server.url + "/");
    await expect(page.locator("html")).toHaveAttribute("data-theme", "light");
  });
});
