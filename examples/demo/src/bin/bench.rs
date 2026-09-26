//! How the worker uses cores, measured in one process with the in-memory
//! backend (no Redis, no files):
//!
//!   cargo run --release -p demo --bin bench
//!
//! 1. I/O-bound async jobs: each one is a tokio task, so thousands can wait at
//!    once on a handful of threads.
//! 2. CPU-bound sync jobs: each one runs on tokio's blocking pool, so with
//!    `concurrency` = CPU count they spread across every core.

use std::{
    convert::Infallible,
    hint::black_box,
    time::{Duration, Instant},
};

use butler::{JobHandle, MemoryQueue, Queue, Worker};

/// Waits on the tokio timer, like a job calling an API or a database.
#[butler::job]
async fn io_bound(wait_ms: u64) -> Result<u64, Infallible> {
    tokio::time::sleep(Duration::from_millis(wait_ms)).await;
    Ok(wait_ms)
}

/// Pure computation. A plain `fn`, so the worker runs it on the blocking pool.
#[butler::job]
fn cpu_bound(rounds: u64) -> Result<u64, Infallible> {
    let mut x = 0x9e37_79b9_7f4a_7c15_u64;
    for i in 0..rounds {
        x = x.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(i);
        x ^= x >> 33;
    }
    Ok(black_box(x))
}

/// Starts a worker, waits for every result, stops the worker, and returns the
/// wall time from start to the last result.
async fn run<T: serde::de::DeserializeOwned + Send + 'static>(
    queue: &Queue,
    concurrency: usize,
    jobs: Vec<JobHandle<T>>,
) -> anyhow::Result<Duration> {
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let worker = Worker::new(queue.clone())
        .concurrency(concurrency)
        .poll_interval(Duration::from_millis(20));
    let started = Instant::now();
    let running = tokio::spawn(worker.run_async(async {
        let _ = stopped.await;
    }));
    // Read the results side by side, so reading them doesn't limit the measure.
    let mut waiting = tokio::task::JoinSet::new();
    for job in jobs {
        waiting.spawn(async move { job.wait_result(Duration::from_secs(5)).await });
    }
    while let Some(result) = waiting.join_next().await {
        result??;
    }
    let took = started.elapsed();
    let _ = stop.send(());
    running.await?;
    Ok(took)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_target(false).init();
    let queue: Queue = MemoryQueue::new().into();
    butler::configure(queue.clone());
    let cores = std::thread::available_parallelism().map_or(4, |n| n.get());
    println!("{cores} CPUs\n");

    // 1. I/O-bound.
    let (count, wait_ms) = (2_000, 50);
    let mut jobs = Vec::with_capacity(count);
    for _ in 0..count {
        jobs.push(io_bound(wait_ms).await?);
    }
    let took = run(&queue, 1_000, jobs).await?;
    println!("I/O-bound: {count} async jobs x {wait_ms}ms of waiting, concurrency 1000");
    let sequential = (count as u64 * wait_ms) as f64 / 1000.0;
    println!(
        "  {took:.2?} ({:.0} jobs/s); one at a time would take {sequential:.0}s, so {:.0}x faster\n",
        count as f64 / took.as_secs_f64(),
        sequential / took.as_secs_f64()
    );

    // 1b. Very high concurrency: 100,000 jobs allowed to run at once, each
    // waiting 1s. Tokio tasks are cheap, so the cap can be this high.
    let (count, wait_ms) = (100_000, 1_000);
    let mut jobs = Vec::with_capacity(count);
    for _ in 0..count {
        jobs.push(io_bound(wait_ms).await?);
    }
    let took = run(&queue, count, jobs).await?;
    println!("High concurrency: {count} async jobs x {wait_ms}ms, concurrency {count}");
    println!(
        "  {took:.2?} ({:.0} jobs/s); one at a time would take {:.0} hours\n",
        count as f64 / took.as_secs_f64(),
        (count as u64 * wait_ms) as f64 / 3_600_000.0
    );

    // 2. CPU-bound, first on one core, then on all of them.
    let (count, rounds) = (cores * 2, 60_000_000);
    let mut timings = Vec::new();
    for concurrency in [1, cores] {
        let mut jobs = Vec::with_capacity(count);
        for _ in 0..count {
            jobs.push(cpu_bound(rounds).await?);
        }
        timings.push(run(&queue, concurrency, jobs).await?);
    }
    println!("CPU-bound: {count} sync jobs x {rounds} rounds");
    println!("  concurrency 1:  {:.2?}", timings[0]);
    println!("  concurrency {cores}: {:.2?}", timings[1]);
    println!(
        "  speedup {:.1}x on {cores} CPUs",
        timings[0].as_secs_f64() / timings[1].as_secs_f64()
    );
    Ok(())
}
