# butler-web

A web dashboard for [butler](https://crates.io/crates/butler-jobs)'s background jobs:
live counts over server-sent events, throughput and duration charts, queues,
workers, and job details. Retry or discard failed jobs, cancel pending work,
run scheduled jobs now, pause and resume queues, see recurring schedules'
next and last run, and inspect arguments, results, errors, and saved progress.

![The dashboard](https://raw.githubusercontent.com/penso/butler/main/docs/images/dashboard-dark.png)

## Run it

As its own server, reading `butler.toml` (or `BUTLER_*` variables) like a
worker:

```sh
butler-web                  # http://127.0.0.1:9090
butler-web 0.0.0.0:8080     # elsewhere: put authentication in front first
```

Or inside your own axum app, behind your own authentication:

```rust
let app = Router::new()
    .nest(
        "/admin/jobs",
        butler_web::Dashboard::new(queue).base_path("/admin/jobs").router(),
    )
    .layer(your_auth_layer);
```

## Pages

| Page | What it shows |
|---|---|
| `/` | Stat cards, live throughput (per second, over SSE), 24 h / 7 d history, duration (average and slowest), queues with pause and resume, busiest jobs |
| `/jobs?state=dead` | Jobs by state and queue, paged; retry, discard, cancel; retry all / discard all |
| `/jobs?state=scheduled` | Scheduled jobs and retries waiting out their backoff, soonest first, with when they run next and their last error; run now, cancel |
| `/jobs/{id}` | Arguments, attempts, the full error chain, result, saved progress |
| `/recurring` | Recurring schedules: job, queue, cron and time zone, arguments, next run, last run (linking to its job); schedules no running worker registered for three minutes are marked and can be removed |
| `/workers` | Workers, their heartbeat, and the jobs they hold |
| `/events` | The live stream (`text/event-stream`), one snapshot per second |
| `/api/stats`, `/api/metrics?minutes=1440` | The same data as JSON |

One sampler reads the backend once a second, and only while a page is open,
however many are.

## Security

By default the dashboard has no login of its own: bind it to localhost (the
default) or mount it behind your authentication.

### Optional basic auth

For running the dashboard on its own without a proxy, it can ask for one
username and password (HTTP basic auth) on every page, asset, action and the
live stream:

```rust
butler_web::Dashboard::new(queue).basic_auth("admin", &password).router()
```

The `butler-web` binary turns it on when `BUTLER_WEB_USERNAME` and
`BUTLER_WEB_PASSWORD` are both set:

```sh
BUTLER_WEB_USERNAME=admin BUTLER_WEB_PASSWORD='…' butler-web
```

This is a convenience, not a replacement for your application's
authentication or an authenticating proxy: there is one shared account, no
logout, and no rate limiting of wrong guesses, and browsers send the
credentials with every request, so serve it over HTTPS or keep it on
localhost. The credentials are compared in constant time. It still works under
`base_path`, and the cross-site check below still applies.

### Actions

Actions are POSTs, and cross-site POSTs are refused (`Sec-Fetch-Site`, or
`Origin`), so another site can't trigger them through a logged-in browser.
Redirects after an action never leave the dashboard. Everything from jobs (names, arguments, errors) is HTML-escaped.

## How it's built

Server-rendered [Askama](https://crates.io/crates/askama) templates, Tailwind
CSS v4, [uPlot](https://github.com/leeoniya/uPlot) for charts, and one small
script for the live parts, all compiled into the binary: no CDN, nothing to
deploy next to it. After changing templates or `ui/input.css`, rebuild the
stylesheet with `just web-css` (needs the standalone `tailwindcss` CLI).
