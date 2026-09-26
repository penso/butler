#![allow(clippy::unwrap_used, clippy::expect_used)]

//! Failed and panicking jobs are retried and end up in `dead/`.

mod common;

use std::sync::atomic::{AtomicU32, Ordering};

use butler::{JobState, Worker};

static FLAKY_CALLS: AtomicU32 = AtomicU32::new(0);

#[butler::job]
async fn flaky(succeed_on_attempt: u32) -> anyhow::Result<()> {
    let n = FLAKY_CALLS.fetch_add(1, Ordering::SeqCst) + 1;
    anyhow::ensure!(n >= succeed_on_attempt, "attempt {n} failed");
    Ok(())
}

#[butler::job]
async fn read_missing(path: String) -> anyhow::Result<()> {
    use anyhow::Context;
    std::fs::read(&path).with_context(|| format!("reading {path}"))?;
    Ok(())
}

#[butler::job(name = "always_panics")]
async fn boom() {
    panic!("kaboom");
}

#[test]
fn retries_then_succeeds_or_dies() {
    let (queue, _dir) = common::temp_queue("retries");
    let worker = Worker::new(queue.clone()).max_retries(2);
    assert_eq!(
        worker.job_names(),
        ["always_panics", "flaky", "read_missing"]
    );

    let ok = butler::block_on(flaky(3)).unwrap();
    let dead = butler::block_on(boom()).unwrap();
    let missing = butler::block_on(read_missing("/nonexistent/butler")).unwrap();

    // flaky: 3 runs. boom and read_missing: 1 run + 2 retries each.
    assert_eq!(worker.drain().unwrap(), 9);

    let (state, job) = queue.get(&ok).unwrap().unwrap();
    assert_eq!(state, JobState::Done);
    assert_eq!(job.attempts, 2);

    let (state, job) = queue.get(&dead).unwrap().unwrap();
    assert_eq!(state, JobState::Dead);
    assert_eq!(job.attempts, 3);
    assert_eq!(job.last_error.as_deref(), Some("job panicked: kaboom"));

    // The whole anyhow context chain is kept, not just the outer message.
    let (_, job) = queue.get(&missing).unwrap().unwrap();
    let err = job.last_error.unwrap();
    assert!(err.starts_with("reading /nonexistent/butler: "), "{err}");
    assert!(err.contains("No such file or directory"), "{err}");
}
