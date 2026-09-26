# AGENTS.md

Engineering guidance for agents working in this repository. Read it alongside
`README.md` and the code before making changes.

## Priorities

- Keep changes small, explicit, and focused on the root cause.
- Preserve user and other agents' changes. Never revert unrelated edits.
- Add regression tests for changed behavior and report what was actually verified.

## Architecture

| Location | Responsibility |
| --- | --- |
| `crates/butler/src/backend/` | `Backend` trait and the file and Redis queues |
| `crates/butler/src/worker.rs` | Claiming, running, retrying: `run` (threads), `run_async` (tokio), `drain` |
| `crates/butler/src/config.rs` | `config.toml` and `BUTLER_*` environment loading |
| `crates/butler/src/lib.rs` | Public API, the global queue, the `#[job]` enqueue helpers |
| `crates/butler-macros/` | The `#[job]` attribute macro |
| `examples/demo/` | `injector` and `worker` binaries sharing one job crate; not published |

- `butler` and `butler-macros` are published together with the same version;
  `butler` pins the macros with `=`. The macro's output may only call
  `butler::__private` items that exist in that exact version.
- `.await` on a `#[job]` function enqueues; it never runs the body. Keep that
  contract, and keep the enqueue return type distinct from the job's own.
- The worker and the enqueue path only talk to storage through `Backend`. A
  backend's `claim` must be atomic: exactly one worker gets each job.
- Backend calls block. Async code reaches them through `spawn_blocking`.

## Rust Conventions

- Every crate exposes its root `Error` and `Result<T>`, defined with
  `thiserror`, with meaningful typed variants. Match on variants, never on text.
- `anyhow` belongs at application boundaries (the demo binaries) and in job
  bodies, which are user code. Inside the library, use typed errors such as
  `butler::Error` and `butler::JobError`.
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
- Comments explain invariants and non-obvious decisions, not assignments.

## Dependencies And Features

- Versions live in root `[workspace.dependencies]`; member crates inherit with
  `.workspace = true`. Keep `[lints] workspace = true` in every crate and fix
  warnings rather than widening allowances.
- `tokio` and `redis` are default features. Gate imports and call sites
  consistently; `just lint` checks every feature combination.
- Use the pinned `rust-toolchain.toml`, and `--locked` for reproducible checks.

## Verification

```sh
just format
just ci            # format-check, clippy on every feature combination, tests
just audit-deps    # cargo deny, after dependency changes
just audit-workflows   # after .github changes
```

The Redis test skips itself when no server is reachable. Run `just redis` first
to exercise it, and say so when you report results.
