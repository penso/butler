//! Enqueues a butler job every second from inside a tokio main loop, alongside
//! ordinary tokio work.
//!
//!   cargo run -p demo --bin injector

use std::time::{Duration, Instant};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let queue = butler::Config::load()?.connect()?;
    println!("[injector {}] queue: {}", std::process::id(), queue.describe());
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
        let id = demo::process_tick(tick, demo::now_ms(), std::process::id()).await?;
        println!(
            "[injector] tick #{tick}: tokio sleep took {slept:.0?}, enqueue took {:.1?} -> job {id}",
            started.elapsed()
        );
    }
    println!("[injector] stopped after {tick} jobs");
    Ok(())
}
