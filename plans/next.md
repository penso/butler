# What's next

Everything noted as missing or worth improving so far, in the order I'd do it.
Each item: what's missing, why it matters, and how I'd approach it. Items
marked **(needs 1)** build on an earlier one.

Status as of 2026-09-26: `main` at `1083452`, CI green. butler has queues with
priorities and limits, crash recovery, typed results and job states, instant
wake-ups, continuations, bulk enqueuing, inline testing, four backends (Redis,
SQLite, file, memory) and the `butler-web` dashboard.

## Priority 1: production failure handling

### 1. Scheduled jobs (ActiveJob `set(wait:)`, `set(wait_until:)`)

- **Missing:** every job runs as soon as a worker is free. There is no way to
  say "in 5 minutes" or "at 03:00".
- **Why:** reminders, delayed follow-ups, and it is the foundation for retry
  backoff (2) and recurring jobs (5).
- **Approach:**
  - API: `send_email::prepare(..)?.run_in(Duration::from_secs(300))` and
    `.run_at(SystemTime)`, then `.enqueue().await` (or `enqueue_all`).
  - New state `Scheduled`, with `Job<Scheduled>` in the typestate.
  - Backends keep a "scheduled" set ordered by run time, and move due jobs into
    their queue:
    - Redis: a sorted set `butler:scheduled` (score = run time), moved by a Lua
      script (`ZRANGEBYSCORE` + `LPUSH`, atomic).
    - SQLite: a `run_at` column, and claims only take rows where `run_at` has
      passed.
    - File: a `scheduled/` directory named by run time.
    - Memory: a `BTreeMap` keyed by run time.
  - The worker's keeper promotes due jobs every tick, and claims wake at the
    next due time rather than the poll interval.
  - `butler-web`: a "Scheduled" tab, with "run now" and cancel.
- **Tests:** contract checks on every backend (not claimable before its time,
  claimable after, cancel works while scheduled), and inline mode (run at once,
  or respect the delay?).

### 2. Retry policies and backoff (ActiveJob `retry_on`, `discard_on`) (needs 1)

- **Missing:**
  - Failed jobs retry immediately, which hammers a flaky API.
  - `max_retries` is one global number.
  - Every error is retried, even ones that can never succeed.
- **Approach:**
  - Per job: `#[job(retries = 10, backoff = "exponential")]`, with
    `"exponential" | "polynomial" | "fixed:30s"`, plus jitter. Defaults come
    from `[worker]`.
  - Per error: a trait the job's error type can implement,
    `fn retry(&self) -> Retry { Retry::Default | Retry::After(Duration) | Retry::Never }`.
    `Never` sends the job straight to dead (ActiveJob's `discard_on`), and
    `After` honours `Retry-After`-style hints.
  - The worker's `fail` schedules the retry through (1) instead of requeuing
    at once. Record the next attempt time and show it in `butler-web`.
- **Tests:**
  - backoff delays grow as specified, jitter stays within bounds;
  - `Retry::Never` goes straight to dead;
  - a job's own retry count overrides the worker's;
  - inline mode keeps one attempt.

## Priority 2: ergonomics

### 3. Callbacks and middleware (ActiveJob `before_enqueue`, `around_perform`, `after_discard`)

- **Missing:** there are no hooks around enqueueing or running.
- **Why:** logging context, tenant or database setup, metrics, alerting when a
  job dies.
- **Approach:** tower-style layers.
  - Worker side: `Worker::wrap(layer)`. A layer gets the job (name, queue,
    args, attempt) and the next step, and can act before and after it or
    short-circuit.
  - Enqueue side: `butler::configure_enqueue(layer)`, which can add metadata
    or veto a job.
  - Built in: a `tracing` span per job run, with the job id, name, queue and
    attempt. This replaces the scattered log lines, like ActiveJob's
    instrumentation events.
  - An `on_dead` hook for alerting.

### 4. `perform_now`

- **Missing:** running a job synchronously in this process, bypassing the
  queue. Only the testing helper can do that today.
- **Approach:** the enqueue future returned by `send_email(..)` becomes a small
  builder that still `.await`s to enqueue, plus
  `send_email(..).now().await -> Result<T, JobError>`, which runs the body
  directly and returns its real output. The macro already has everything it
  needs: the job's `JobDef` and its dispatch function.

## Priority 3: scheduling and flow control

### 5. Recurring jobs (Solid Queue `recurring.yml`) (needs 1)

- **Missing:** cron-style schedules, such as a nightly report.
- **Approach:**
  - `[[recurring]]` entries in `butler.toml` (`job`, `cron`, `args`, `queue`),
    or `Worker::recurring(job::prepare(..)?, "0 3 * * *")` in code.
  - Exactly one enqueue per tick across all workers: key each tick by
    `(schedule, tick time)` and make the backend's push idempotent on that key
    (`SET NX` in Redis, a unique index in SQLite).
  - Shown in `butler-web`: next run and last run.

### 6. Per-key concurrency and unique jobs (Solid Queue `limits_concurrency`, Sidekiq Enterprise unique jobs)

- **Missing:**
  - "At most N at a time per key", e.g. one sync per account.
  - "Don't enqueue this if an identical one is already waiting".
- **Approach:**
  - `#[job(concurrency_key = "account_id", limit = 1)]`, where the key comes
    from named arguments.
  - Claims skip jobs whose key is at its limit, which needs backend-side
    counters (Redis `INCR`/`DECR` with expiry, a SQLite table).
  - Uniqueness: `#[job(unique = "until_started")]`, backed by a key set on
    push and cleared on claim or finish.

### 7. Global queue limits

- **Missing:** `[worker.queue_limits]` caps each worker process separately.
  Three workers with `mailers = 20` can run 60 mailer jobs at once. Sidekiq
  Enterprise's limits are global.
- **Approach:** an optional backend-side counter per queue, checked
  atomically in the claim (a Lua script in Redis, a transaction in SQLite).
  Keep today's per-process limits as the cheap default, and document the
  difference clearly.

### 8. Priority within a queue (ActiveJob `queue_with_priority`)

- **Missing:** a queue is strictly first in, first out.
- **Approach:** an optional priority on the job (`#[job(priority = 10)]`, or
  `prepare(..)?.priority(..)`):
  - Redis: a sorted set per queue (score = priority, then time).
  - SQLite: order by `priority, seq`.
  - Memory: a heap.

  Weighed against complexity: weighted queues already cover most cases.

### 9. Pausing a queue (Mission Control)

- **Missing:** stopping workers from taking jobs from a queue, without
  stopping the workers.
- **Approach:** a paused set in the backend that workers read before building
  their claim order, and Pause / Resume buttons in `butler-web`.

## Priority 4: operations and scale

### 10. Cleanup of finished jobs (Solid Queue `clear_finished_jobs_after`)

- **Missing:** SQLite, file and memory keep every finished job forever. Redis
  expires them after 24 hours.
- **Approach:**
  - `[queue] keep_finished = "7d"` (and a separate setting for dead jobs, or
    keep those until discarded).
  - The worker's keeper deletes older finished jobs in batches. SQLite needs
    an index on the finish time.

### 11. SQLite dashboard queries at scale

- **Problem:** `stats()` runs `GROUP BY queue, state` over the whole table
  every second while the dashboard is open. That is fine for thousands of
  rows and slow for millions.
- **Approach:** keep counts in a small table, updated in the same
  transactions as state changes (or computed from indexed partial counts),
  and use (10) to keep the table small.

### 12. Redis details

- **Checkpoint cost:** the checkpoint script uses `LPOS` on the worker's
  processing list, which grows with the number of jobs a worker holds. Fine
  for hundreds; consider a per-worker set of held ids.
- **Redis Cluster:** the recovery and checkpoint scripts build keys inside Lua
  instead of declaring them, so they don't work there. Use hash tags
  (`{butler}:...`) and declared keys if Cluster support matters.
- **Connection pool:** `RedisQueue`'s idle pool is unbounded; cap it and
  expose the size.
- **Per-queue running counts:** Redis only knows running jobs per worker, not
  per queue. The dashboard would benefit if the worker recorded them.

### 13. Continuation details

- **Loop guard:** a limit on how many times a job can be interrupted and
  resumed (ActiveJob has a `max_resumptions` option), in case a job never
  reaches its next checkpoint before the deadline.
- **Isolated steps (ActiveJob `isolated: true`):** a checkpoint that forces
  the job back on its queue, so a long step starts in a fresh execution.
- **`shutdown_timeout`:** on shutdown, how long to wait for jobs without
  checkpoints before giving up. Their jobs are recovered anyway, but a bound
  keeps deploys predictable.

### 14. Test assertions for enqueued jobs (ActiveJob `assert_enqueued_with`, `assert_no_enqueued_jobs`)

- **Missing:** a test mode that records what was enqueued without running it.
  `InlineJobs` runs everything.
- **Approach:** `testing::RecordedJobs`, a scope that captures enqueues
  (name, queue, args, schedule time), with helpers like
  `assert_enqueued::<send_email>(|args| ..)` and `jobs.enqueued_names()`.

## butler-web

- **Browser tests:** end-to-end tests in a real browser (moltis uses
  Playwright) for the live chart, SSE reconnect, the theme toggle and the
  confirm dialogs. Today they are only checked by hand and with screenshots.
- **Stale CSS check:** a CI step that rebuilds `assets/app.css` with a pinned
  `tailwindcss` and fails if it differs from the committed file.
- **Pages to add:** scheduled jobs (1), recurring schedules (5), pause and
  resume (9), per-queue pages with their own charts, and per-job charts.
- **Optional login:** built-in HTTP basic auth
  (`Dashboard::basic_auth(user, pass)`) for people running the binary without
  a proxy.
- **Error page links:** error pages don't know the base path, so their links
  point to the root. Pass it through when mounted under a prefix.

## Publishing to crates.io

- **Name:** `butler` is taken on crates.io. Choose a name (`butler-jobs`,
  `butler-queue`, ...) and keep `butler` as the library name, so user code is
  unchanged.
- **Metadata:** add `license` and `repository` under `[workspace.package]`.
- **Publish order:** `butler-macros`, then `butler`, then `butler-web`.
  `just publish-dry-run` checks the first two today; add `butler-web`.

## Smaller notes

- **Job names are the contract:** renaming a job function strands the jobs
  already queued under the old name. Document `#[job(name = "...")]`
  prominently, or add aliases (`#[job(aliases = ["old_name"])]`).
- **Inline testing gap:** jobs enqueued from work that was `tokio::spawn`ed
  inside `perform_enqueued_jobs` are not inline. A tokio task-local could
  carry the inline scope into spawned tasks.
- **`Backend` is growing:** it now mixes storage, monitoring and signalling.
  Consider splitting it into traits (`Storage`, `Monitor`) before publishing,
  to make custom backends easier to write.
- **Benchmark numbers** in the README come from one 16-CPU machine; note the
  hardware next to them, or generate them in CI.
