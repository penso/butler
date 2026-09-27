# butler-web

A web dashboard for [butler](https://crates.io/crates/butler-jobs)'s background jobs:
live counts over server-sent events, throughput and duration charts, queues,
workers, and job details. Retry or discard failed jobs, cancel pending work,
run scheduled jobs now, and inspect arguments, results, errors, and saved
progress.

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
| `/` | Stat cards, live throughput (per second, over SSE), 24 h / 7 d history, duration (average and slowest), queues, busiest jobs |
| `/jobs?state=dead` | Jobs by state and queue, paged; retry, discard, cancel; retry all / discard all |
| `/jobs?state=scheduled` | Scheduled jobs and retries waiting out their backoff, soonest first, with when they run next and their last error; run now, cancel |
| `/jobs/{id}` | Arguments, attempts, the full error chain, result, saved progress |
| `/workers` | Workers, their heartbeat, and the jobs they hold |
| `/events` | The live stream (`text/event-stream`), one snapshot per second |
| `/api/stats`, `/api/metrics?minutes=1440` | The same data as JSON |

One sampler reads the backend once a second, and only while a page is open,
however many are.

## Security

The dashboard has no login of its own: bind it to localhost (the default) or
mount it behind your authentication. Actions are POSTs, and cross-site POSTs
are refused (`Sec-Fetch-Site`, or `Origin`), so another site can't trigger
them through a logged-in browser. Redirects after an action never leave the
dashboard. Everything from jobs (names, arguments, errors) is HTML-escaped.

## How it's built

Server-rendered [Askama](https://crates.io/crates/askama) templates, Tailwind
CSS v4, [uPlot](https://github.com/leeoniya/uPlot) for charts, and one small
script for the live parts, all compiled into the binary: no CDN, nothing to
deploy next to it. After changing templates or `ui/input.css`, rebuild the
stylesheet with `just web-css` (needs the standalone `tailwindcss` CLI).
