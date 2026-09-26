//! Jobs shared by the `injector` (which enqueues them) and the `worker` (which
//! runs them). Both binaries link this crate, so they agree on job names and
//! argument types.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Takes ~1.5s of async work on the tokio timer. The injector sends one per
/// second, so several run at the same time on the worker.
#[butler::job]
pub async fn process_tick(tick: u64, enqueued_at_ms: u64, from_pid: u32) -> anyhow::Result<()> {
    let picked_up_after = now_ms().saturating_sub(enqueued_at_ms);
    println!(
        "[worker {}] tick #{tick} from injector {from_pid}: started {picked_up_after}ms after enqueue",
        std::process::id()
    );

    tokio::time::sleep(Duration::from_millis(1500)).await;

    if tick.is_multiple_of(7) {
        anyhow::bail!("tick #{tick} is divisible by 7, failing on purpose");
    }
    println!(
        "[worker {}] tick #{tick}: done after 1.5s of tokio::time::sleep",
        std::process::id()
    );
    Ok(())
}
