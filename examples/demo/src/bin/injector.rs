//! Enqueues a butler job every second from inside a tokio main loop, alongside
//! ordinary tokio work.
//!
//!   cargo run -p demo --bin injector

use std::time::{Duration, Instant};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_target(false).init();
    let queue = butler::Config::load()?.connect()?;
    println!(
        "[injector {}] queue: {}",
        std::process::id(),
        queue.describe()
    );
    butler::configure(queue);

    // An ordinary background tokio task that runs alongside the enqueues.
    tokio::spawn(async {
        let mut beats = 0u64;
        loop {
            tokio::time::sleep(Duration::from_millis(2500)).await;
            beats += 1;
            println!("[injector] heartbeat #{beats} (background tokio task)");
        }
    });

    let mut every_second = tokio::time::interval(Duration::from_secs(1));
    let mut tick = 0u64;
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => break,
            _ = every_second.tick() => {}
        }
        tick += 1;

        // Ordinary .await: runs right here, on the tokio timer.
        let started = Instant::now();
        tokio::time::sleep(Duration::from_millis(200)).await;
        let slept = started.elapsed();

        // butler .await: only writes the job to the queue; the worker runs it.
        let started = Instant::now();
        let job = demo::process_tick(tick, demo::now_ms(), std::process::id()).await?;
        println!(
            "[injector] tick #{tick}: tokio sleep took {slept:.0?}, enqueue took {:.1?} -> job {job}",
            started.elapsed()
        );

        // The result comes back through the queue: wait for it in its own task,
        // so the loop keeps enqueueing.
        let pending = job.clone();
        tokio::spawn(async move {
            match pending.wait_result(Duration::from_millis(100)).await {
                Ok(report) => println!(
                    "[injector] tick #{tick}: result from worker {}: {report:?}",
                    report.worker_pid
                ),
                Err(err) => println!("[injector] tick #{tick}: no result: {err}"),
            }
        });

        // Every third tick also raises an alert, on the "critical" queue.
        if tick.is_multiple_of(3) {
            let alert = demo::alert(tick).await?;
            println!("[injector] tick #{tick}: alert -> job {alert} on the critical queue");
        }

        // The handle can cancel a job that no worker has claimed yet.
        if tick.is_multiple_of(5) {
            if job.cancel().await? {
                println!("[injector] tick #{tick}: cancelled before a worker picked it up");
            } else {
                println!("[injector] tick #{tick}: too late to cancel, a worker already has it");
            }
        }
    }
    println!("[injector] stopped after {tick} jobs");
    Ok(())
}
