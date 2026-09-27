import { defineConfig, devices } from "@playwright/test";

// Each test starts its own dashboard (see fixtures.ts), so tests run in
// parallel without sharing jobs.
export default defineConfig({
  testDir: "./specs",
  timeout: 30_000,
  expect: { timeout: 10_000 },
  forbidOnly: !!process.env.CI,
  retries: 0,
  reporter: process.env.CI ? [["list"], ["github"], ["html", { open: "never" }]] : "list",
  use: {
    trace: "retain-on-failure",
    screenshot: "only-on-failure",
  },
  projects: [{ name: "chromium", use: { ...devices["Desktop Chrome"] } }],
});
