//! Jobs shared by the `injector` (which enqueues them) and the `worker` (which
//! runs them). Both binaries link this crate, so they agree on job names and
//! argument types.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

/// Why a tick can fail: an ordinary `thiserror` enum. The injector sees its
/// message in `Error::JobFailed` when the job runs out of retries.
#[derive(Debug, thiserror::Error)]
pub enum TickError {
    #[error("tick #{0} is divisible by 7, failing on purpose")]
    DivisibleBySeven(u64),
}

/// What the worker sends back for each tick: the other direction of the trip.
#[derive(Debug, Serialize, Deserialize)]
pub struct TickReport {
    pub tick: u64,
    pub worker_pid: u32,
    /// How long the job waited in the queue before a worker started it.
    pub queued_ms: u64,
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Takes ~1.5s of async work on the tokio timer. The injector sends one per
/// second, so several run at the same time on the worker.
#[butler::job]
pub async fn process_tick(
    tick: u64,
    enqueued_at_ms: u64,
    from_pid: u32,
) -> Result<TickReport, TickError> {
    let picked_up_after = now_ms().saturating_sub(enqueued_at_ms);
    println!(
        "[worker {}] tick #{tick} from injector {from_pid}: started {picked_up_after}ms after enqueue",
        std::process::id()
    );

    tokio::time::sleep(Duration::from_millis(1500)).await;

    if tick.is_multiple_of(7) {
        return Err(TickError::DivisibleBySeven(tick));
    }
    println!(
        "[worker {}] tick #{tick}: done after 1.5s of tokio::time::sleep",
        std::process::id()
    );
    Ok(TickReport {
        tick,
        worker_pid: std::process::id(),
        queued_ms: picked_up_after,
    })
}

/// Goes on the "critical" queue, which `config.toml` weights 3:1 over
/// "default", so the worker picks these ahead of ticks when both are waiting.
#[butler::job(queue = "critical")]
pub async fn alert(tick: u64) {
    println!(
        "[worker {}] ALERT for tick #{tick} (critical queue)",
        std::process::id()
    );
}
