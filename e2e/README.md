# Dashboard browser tests

Playwright tests for `butler-web` in Chromium: the live chart and counts over
server-sent events, reconnecting after a restart, the theme toggle, confirm
dialogs, bulk actions on scheduled jobs, the queue and job pages, basic auth,
and a nested base path.

Each test starts its own `butler-e2e-server` (in `server/`): the dashboard
over an in-memory queue seeded with known jobs, finishing a job every 100 ms so
the live chart has data. Tests don't share jobs, so they run in parallel.

```sh
just e2e                 # from the repository root: install, build, run
# or, from here:
npm ci && npx playwright install chromium
npm test                 # builds the server, then runs every spec
npx playwright test specs/live.spec.ts --headed
```

Nothing here is published: the server crate has `publish = false`, and the
Rust crates don't depend on it.
