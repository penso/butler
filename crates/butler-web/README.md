# butler-web

A web dashboard for [butler](https://crates.io/crates/butler-jobs)'s background jobs:
live counts over server-sent events, throughput and duration charts (overall,
per queue, and per job), queues, workers, and job details. Retry or discard
failed jobs, cancel pending work, run scheduled jobs now (one or all), pause
and resume queues, see recurring schedules'
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
| `/jobs?state=scheduled` | Scheduled jobs and retries waiting out their backoff, soonest first, with when they run next and their last error; run now, cancel; run all now / cancel all (on the selected queue, if any) |
| `/jobs/{id}` | Arguments, attempts, the full error chain, result, saved progress |
| `/queues/{queue}` | One queue: live pending and running counts, pause or resume, its history and duration charts, and its jobs over the day |
| `/metrics/{job}` | One job name: its totals, history and duration charts, on every queue or one (`?queue=`) |
| `/recurring` | Recurring schedules: job, queue, cron and time zone, arguments, next run, last run (linking to its job); schedules no running worker registered for three minutes are marked and can be removed |
| `/workers` | Workers, their heartbeat, and the jobs they hold |
| `/events` | The live stream (`text/event-stream`), one snapshot per second |
| `/api/stats`, `/api/metrics?minutes=1440` | The same data as JSON; `/api/metrics` takes `queue=` and `job=` to narrow its series |

One sampler reads the backend once a second, and only while a page is open,
however many are. Queue and job pages use the same stream for their live
counts, and read their charts' history once per load and once a minute.

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
