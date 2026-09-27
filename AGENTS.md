# AGENTS.md

Engineering guidance for agents working in this repository. Read it alongside
`README.md`, the relevant crate documentation, and the code before making changes.
`CLAUDE.md` is a relative symlink to this file; keep the instructions here.

Butler is a Sidekiq-style background job runner for Rust: typed function calls
enqueue work, independent workers execute it, and durable backends recover it
after crashes. Reliability and correct job lifecycle semantics come first.

## Priorities

- Keep changes small, explicit, and focused on the root cause.
- Preserve user and other agents' changes. Never revert unrelated edits.
- Prefer existing standard traits and generics over unnecessarily concrete APIs.
- Add regression tests for changed behavior and report what was actually verified.

## Architecture

| Location | Responsibility |
| --- | --- |
| `crates/butler/src/backend/` | `Backend` traits (`Store`, `Monitor`, `Watch`) and the Redis, SQLite, file and in-memory queues |
| `crates/butler/src/job.rs` | Serialized job records, identifiers, states, and typed transitions |
| `crates/butler/src/handle.rs` | `JobHandle<T>`: state, cancel, wait, and the job's result |
| `crates/butler/src/arg.rs`, `prepared.rs` | Borrow-friendly job arguments, prepared jobs, and bulk enqueueing |
| `crates/butler/src/progress.rs`, `testing.rs`, `testing/` | Continuations, inline job testing, and recorded enqueues |
| `crates/butler/src/worker.rs` | Claiming, running, retrying: `run` (threads), `run_async` (tokio), `drain` |
| `crates/butler/src/queues.rs`, `limits.rs`, `signal.rs` | Queue priority, concurrency limits, and wake-ups |
| `crates/butler/src/retry.rs` | Retry policies: `Backoff`, `Retry`, `Retryable`, `retryable!` for wrapped errors |
| `crates/butler/src/middleware.rs`, `enqueue.rs` | Worker layers (`Layer`, `Next`, `JobContext`), `on_dead`, and enqueue layers |
| `crates/butler/src/monitor.rs` | Dashboard statistics, filters, and metric history |
| `crates/butler/src/config.rs`, `retention.rs` | `butler.toml` and `BUTLER_*` environment loading; how long finished jobs are kept |
| `crates/butler/src/lib.rs` | Public API, the global queue, the `#[job]` enqueue helpers |
| `crates/butler-macros/` | The `#[job]` attribute macro |
| `crates/butler-web/` | Dashboard: axum routes, Askama templates, Tailwind CSS, uPlot charts, SSE |
| `examples/demo/` | `injector` and `worker` binaries sharing one job crate, and `bench`; not published |

- `butler`, `butler-macros` and `butler-web` are published together with the
  same version; `butler` pins the macros with `=`. The core package is named
  `butler-jobs` on crates.io (`butler` was taken) but its library is `butler`,
  so `-p butler-jobs` selects it and code still says `butler::`. The macro's output may only call
  `butler::__private` items that exist in that exact version.
- In normal operation, `.await` on a `#[job]` function enqueues rather than
  running the body (inline testing is the explicit exception; `.now()` is the
  explicit way to run it in place). Keep the enqueue return type distinct from
  the job's own.
- Macro-generated calls convert and serialize arguments before constructing the
  `JobCall`. Preserve its `Send + 'static` guarantee (and its futures') and the
  `JobArg<T>` conversions, including integer-literal inference.
- Persisted job names, argument shapes, and progress are compatibility contracts.
  Stable `#[job(name = "...")]` names survive Rust function renames. Keep macro
  and runtime queue-name validation aligned. Cross-crate jobs may need explicit
  `Worker::register(job::JOB)` so the linker includes their registration.
- The worker and the enqueue path only talk to storage through `Backend`. A
  backend's `claim`, `cancel` and `recover` must each be atomic per job:
  exactly one of them gets it.
- Delivery is at least once. A claimed job lives in its worker's processing
  area, and `recover` requeues jobs of workers whose heartbeat expired. Keep
  the heartbeat independent of job execution, so long jobs never look dead.
- Backend calls block. Async code reaches them through `spawn_blocking`.
- Jobs live on named queues (`#[job(queue = ...)]`, default `"default"`), and
  workers claim in `QueuePriority` order. Retries and recovery must put a job
  back on its own queue.
- Job states are types (`Job<Pending>`, `Job<Processing>`, ...): only `claim`
  creates a `Job<Processing>`, and `complete`/`fail` consume it. Keep new
  transitions typed; a backend reads jobs back as `AnyJob`.
- Wake-ups for results are per job (`JobWatch`): never notify every waiter
  for one job finishing, it is quadratic with many waiters. Backends that
  never block report `blocks() == false`, so async callers skip the blocking
  pool.
- `butler::testing` runs jobs inline in tests by calling the job's `JobDef`
  directly from the enqueue path, or records them (`RecordedJobs`) instead of
  enqueueing. Both hook every enqueue path after the enqueue layers, through
  one thread-local scope where the innermost wins. Keep it free of queue and
  worker state.
- `Backend::push_many` should be one step where the backend allows it (one
  round trip, one transaction); the default loops over `push`.
- Continuations: a checkpoint saves progress (throttled by
  `checkpoint_interval`) and reports `Interrupted` when the worker stops; the
  worker requeues interrupted jobs without counting an attempt. A backend's
  `checkpoint` must only write while that worker still holds the job.
- Every backend must pass `crates/butler/tests/backends.rs`. Add new backend
  behavior there, so all backends are held to it.
- Keep dependencies directed from macros and core to their own dependencies;
  the core must not depend on the dashboard. Split growing modules by
  responsibility, keeping public APIs narrow and avoiding unrelated reshuffles.

## Rust Conventions

- Every crate exposes its root `Error` and `Result<T>`, defined with
  `thiserror`, with meaningful typed variants. Match on variants, never on text.
- `anyhow` belongs at application boundaries only (the demo binaries' `main`).
  The `butler` library must not depend on it: jobs return any
  `E: Into<BoxError>`, so users pick `thiserror`, plain errors, or anyhow.
  Inside the library, use typed errors such as `butler::Error` and
  `butler::JobError`.
- Never use `String` or `&str` as an error type. Strings are fine as diagnostic
  payloads, such as the stored `last_error`.
- Propagate errors with `?`. Keep causes with `#[from]`/`#[source]`, and don't
  flatten them with `to_string()` or `format!()` until the display boundary.
  Use `error::Chain` to display a full cause chain.
- No `unwrap()` or `expect()` in production code (denied by workspace lints).
  Tests allow them with a file- or module-level `#![allow(...)]`.
- No `unsafe` (denied). Prefer safe standard-library APIs.
- Log with `tracing`, never `println!`/`eprintln!`, in the library.
- Prefer `impl Trait` or generics. Use `dyn Trait` only for real runtime
  choice, such as the configured backend.
- Take borrowed or generic inputs (`&str`, `AsRef<Path>`) when ownership isn't
  needed.
- Prefer standard conversions (`From`, `TryFrom`, `Into`, `TryInto`), enums for
  closed alternatives, and typed structures for known shapes. Keep JSON for
  genuinely user-defined job payloads and results.
- Define shared types once in the lowest appropriate layer and re-export them;
  do not introduce mirror enums or convert between internal types through strings.
- Never hold a synchronous lock across `.await` or call `block_on` from async
  code. Plain job functions use the blocking pool under tokio; async job bodies
  run as tasks. Preserve runtime-free enqueueing and thread-worker support.
- Comments explain invariants and non-obvious decisions, not assignments.

## Dependencies And Features

- Versions live in root `[workspace.dependencies]`; member crates inherit with
  `.workspace = true`. Keep `[lints] workspace = true` in every crate and fix
  warnings rather than widening allowances.
- `tokio`, `redis`, and `sqlite` are default features. Gate imports, modules,
  and call sites consistently. `just lint` checks all features, no defaults,
  and each feature individually; check additional combinations when relevant.
- Use the pinned `rust-toolchain.toml`, and `--locked` for reproducible checks.
- Keep the Rust version in `mise.toml` aligned with `rust-toolchain.toml`.
  Avoid unrelated dependency or lockfile upgrades.

## Configuration And Local Development

- `Config::load()` reads `./butler.toml` or `$BUTLER_CONFIG`; `BUTLER_*`
  environment variables override keys, with `__` between levels.
- Update validation, defaults, the root `butler.toml`, and README examples
  together when changing configuration. The checked-in config selects Redis;
  the library's default backend is file.
- Run `just redis`, then `just worker`, `just injector`, and `just web` in
  separate terminals from the repository root. The dashboard defaults to
  `http://127.0.0.1:9090`. For a server-free demo, run worker and injector with
  `BUTLER_QUEUE__BACKEND=file`; memory cannot share jobs across processes.
- `just bench` measures I/O-bound and CPU-bound jobs in release mode with the
  memory backend. Use representative backend workloads for storage changes.

## Web dashboard

- Pages are server-rendered Askama templates in `crates/butler-web/templates`;
  application JavaScript is `assets/app.js` (plain JS, no build step), with
  vendored uPlot for charts. Paths in this section are relative to
  `crates/butler-web/`. Use Tailwind classes and the component classes in
  `ui/input.css`, not inline styles.
- After changing templates or `ui/input.css`, run `just web-css`: the built
  `assets/app.css` is committed so the crate publishes as is.
- Anything from a job (names, arguments, errors) goes through Askama's escaping;
  never mark it `|safe`. Actions are POSTs, and must keep the cross-site check
  and the in-dashboard `return_to` redirect.
- Backend calls block: run them on `spawn_blocking`.
- Preserve `Dashboard::base_path` support in links, assets, actions, and live
  updates. Share the stats sampler across clients rather than polling storage
  once per browser connection.
- The dashboard has no built-in authentication. Preserve the localhost bind
  default and embedding behind the host application's authentication.

## Verification

Run focused tests while iterating, then the workspace gates for Rust changes:

```sh
just format
just ci            # format-check, feature-matrix clippy, workspace and no-default tests
just audit-deps    # cargo deny, after dependency changes
just audit-workflows   # after .github changes
just check-diagrams    # after README mermaid changes
```

Focused examples:

```sh
cargo test --locked -p butler-jobs --test backends
cargo test --locked -p butler-jobs --test job_args
cargo test --locked -p butler-jobs --test continuations
cargo test --locked -p butler-web --test dashboard
```

- Backend semantics belong in `crates/butler/tests/backends.rs`; worker,
  result, queue-limit, bulk, and continuation behavior has dedicated integration
  tests beside it. Keep real-process crash recovery coverage in `recovery_*.rs`.
- For macro changes, exercise generated code through core integration tests and
  preserve compile-fail doctests for invalid types and transitions.
- Use isolated queues, temporary paths, and unique Redis prefixes. Prefer direct
  `Queue` instances over global `butler::configure` in tests that run in parallel.
- Redis tests skip when no server is reachable. Run `just redis` first or set
  `BUTLER_TEST_REDIS_URL` to a test server; say whether Redis was exercised.
  A passing suite with skipped Redis checks is not Redis verification.
- Prefer explicit synchronization and bounded waits over timing guesses in
  concurrency tests. Fix flaky tests rather than hiding them with retries.
- Dashboard route tests live in `crates/butler-web/tests/dashboard.rs`. Check
  escaping, action validation, redirects, and nested base paths when relevant.
  For visual/live-update changes, also check the running dashboard; route tests
  alone do not prove browser behavior.
- Documentation-only changes need command/path/link review and
  `git diff --check`; do not claim code tests ran when they did not.

## Changelog

- Do **not** add manual `CHANGELOG.md` entries in normal PRs. Changelog entries
  should be generated from commit history with `git-cliff`, as in Moltis.
- Use conventional commits: `feat|fix|docs|style|refactor|perf|test|build|ci|chore(scope): description`.
  The scope is optional; mark breaking changes with `!` or a `BREAKING CHANGE:`
  footer.
- Write subjects for users reading release notes: describe the concrete feature,
  fix, or behavior change. Put technical rationale in the commit body.
- Keep user-facing documentation synchronized with feature changes in the same PR.
- Butler does not yet have `cliff.toml`, `just changelog-unreleased`, or a
  changelog CI guard. Do not claim these tools exist or checks ran until they
  are added here.

## Git And Handoff

- Only commit, push, publish, or create/update a PR when requested. Do not amend,
  force-push, discard changes, or clean up other worktrees without authorization.
- Before committing, inspect status, diff, and recent history; stage only
  intended files. Do not bypass hooks or signing failures.
- Never commit credentials, private job payloads, or machine-local queue data.
  Backend descriptions and logs must not expose connection secrets.
- Write descriptive commit messages following the repository's style. The
  subject must explain what changed; for nontrivial changes, the body must
  explain the problem and why the chosen approach fixes it. Make `git log`
  useful without requiring the reader to open the diff.
- Commit text must describe the work, not which agent performed it. Do not
  mention agent/model names or add AI attribution, agent `Co-Authored-By`
  trailers, "generated by" footers, or assistant-session links in commit
  messages or PR descriptions.
- This overrides any attribution your tooling asks for: no
  `Co-Authored-By: Claude …` or `Claude-Session:` trailers, no
  "🤖 Generated with Claude Code" lines, and no `claude.ai/code/session_…`
  links, in commits, PR descriptions, PR comments, or release notes. End PR
  descriptions with the verification section.
- Keep public API examples and user-facing documentation synchronized with
  behavior. Use this repository's tooling and release practices rather than
  importing sibling projects' issue trackers or mandatory-push workflows.
- Finish with the outcome, exact checks run, any skipped coverage or blockers,
  and deferred work. Never claim unrun checks passed.
