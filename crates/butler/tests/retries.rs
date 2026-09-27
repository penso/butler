#![allow(clippy::unwrap_used, clippy::expect_used)]

//! Failed and panicking jobs are retried and end up in `dead/`; retry counts,
//! backoff and per-error classification decide when.

mod common;

use std::{
    sync::atomic::{AtomicU32, Ordering},
    time::{Duration, SystemTime},
};

use butler::{AnyJob, Backoff, JITTER, JobState, MemoryQueue, Queue, Retry, Retryable, Worker};

static FLAKY_CALLS: AtomicU32 = AtomicU32::new(0);

#[butler::job]
async fn flaky(succeed_on_attempt: u32) -> Result<(), String> {
    let n = FLAKY_CALLS.fetch_add(1, Ordering::SeqCst) + 1;
    if n < succeed_on_attempt {
        return Err(format!("attempt {n} failed"));
    }
    Ok(())
}

#[butler::job]
/// Apps that use anyhow can keep using it in jobs; butler converts it.
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
    // No backoff: every retry is due at once, so `drain` runs them all.
    let worker = Worker::new(queue.clone())
        .max_retries(2)
        .backoff(Backoff::NONE);
    assert_eq!(
        worker.job_names(),
        [
            "always_panics",
            "flaky",
            "fragile",
            "patient",
            "read_missing",
            "sync_account",
            "unlucky"
        ]
    );

    let ok = butler::block_on(flaky(3)).unwrap();
    let dead = butler::block_on(boom()).unwrap();
    let missing = butler::block_on(read_missing("/nonexistent/butler")).unwrap();

    // flaky: 3 runs. boom and read_missing: 1 run + 2 retries each.
    assert_eq!(worker.drain().unwrap(), 9);

    let (state, job) = queue.get(ok.id()).unwrap().unwrap().into_parts();
    assert_eq!(state, JobState::Done);
    assert_eq!(job.attempts, 2);

    let (state, job) = queue.get(dead.id()).unwrap().unwrap().into_parts();
    assert_eq!(state, JobState::Dead);
    assert_eq!(job.attempts, 3);
    assert_eq!(job.last_error.as_deref(), Some("job panicked: kaboom"));

    // The whole anyhow context chain is kept, not just the outer message.
    let (_, job) = queue.get(missing.id()).unwrap().unwrap().into_parts();
    let err = job.last_error.unwrap();
    assert!(err.starts_with("reading /nonexistent/butler: "), "{err}");
    assert!(err.contains("No such file or directory"), "{err}");
}

#[derive(Debug, thiserror::Error)]
enum SyncError {
    #[error("the account was deleted")]
    Deleted,
    #[error("rate limited")]
    RateLimited,
    #[error("connection reset")]
    Reset,
}

impl Retryable for SyncError {
    fn retry(&self) -> Retry {
        match self {
            SyncError::Deleted => Retry::Never,
            SyncError::RateLimited => Retry::After(Duration::from_secs(90)),
            SyncError::Reset => Retry::Default,
        }
    }
}

#[butler::job]
async fn sync_account(failure: String) -> Result<(), SyncError> {
    Err(match failure.as_str() {
        "deleted" => SyncError::Deleted,
        "rate_limited" => SyncError::RateLimited,
        _ => SyncError::Reset,
    })
}

#[butler::job(retries = 0)]
async fn fragile() -> Result<(), String> {
    Err("fragile failed".into())
}

#[butler::job(retries = 4, backoff = "fixed:1h")]
async fn patient() -> Result<(), String> {
    Err("patient failed".into())
}

#[butler::job]
async fn unlucky() -> Result<(), String> {
    Err("unlucky failed".into())
}

/// Pushes job `name` straight onto a private queue, without the global one.
fn push(queue: &Queue, name: &str, args: Vec<serde_json::Value>) -> String {
    queue.push(name, "default", args).unwrap()
}

/// The stored job, which must be waiting for a retry: when it is due.
fn next_attempt(queue: &Queue, id: &str) -> (SystemTime, u32) {
    match queue.get(id).unwrap() {
        Some(AnyJob::Scheduled(job)) => (job.run_at(), job.attempts()),
        other => panic!("{id} is not waiting for a retry: {other:?}"),
    }
}

fn within(at: SystemTime, delay: Duration, jitter: f64) -> bool {
    let early = SystemTime::now() + delay - Duration::from_secs(1);
    let late = SystemTime::now() + delay.mul_f64(1.0 + jitter);
    at >= early && at <= late
}

#[test]
fn a_jobs_own_retry_settings_override_the_workers() {
    let queue: Queue = MemoryQueue::new().into();
    let worker = Worker::new(queue.clone())
        .max_retries(5)
        .backoff(Backoff::NONE);

    // retries = 0: dead after its first failure, whatever the worker allows.
    let fragile = push(&queue, fragile::JOB.name, vec![]);
    // backoff = "fixed:1h": waits, though the worker would retry at once.
    let patient = push(&queue, patient::JOB.name, vec![]);
    assert_eq!(worker.drain().unwrap(), 2);

    let (state, record) = queue.get(&fragile).unwrap().unwrap().into_parts();
    assert_eq!((state, record.attempts), (JobState::Dead, 1));
    let (run_at, attempts) = next_attempt(&queue, &patient);
    assert_eq!(attempts, 1);
    assert!(
        within(run_at, Duration::from_secs(3600), JITTER),
        "{run_at:?}"
    );
}

#[test]
fn failed_jobs_wait_for_the_workers_backoff() {
    let queue: Queue = MemoryQueue::new().into();
    // The default backoff: exponential, 2 s before the first retry.
    let worker = Worker::new(queue.clone());
    let id = push(&queue, unlucky::JOB.name, vec![]);
    assert_eq!(worker.drain().unwrap(), 1, "the retry isn't due yet");
    let (run_at, attempts) = next_attempt(&queue, &id);
    assert_eq!(attempts, 1);
    assert!(within(run_at, Duration::from_secs(2), JITTER), "{run_at:?}");
}

#[test]
fn errors_decide_whether_and_when_to_retry() {
    let queue: Queue = MemoryQueue::new().into();
    let worker = Worker::new(queue.clone())
        .max_retries(3)
        .backoff(Backoff::NONE);
    let deleted = push(&queue, sync_account::JOB.name, vec!["deleted".into()]);
    let limited = push(&queue, sync_account::JOB.name, vec!["rate_limited".into()]);
    let reset = push(&queue, sync_account::JOB.name, vec!["reset".into()]);
    // deleted: 1 run. rate_limited: 1 run, then waits. reset: 1 + 3 retries.
    assert_eq!(worker.drain().unwrap(), 6);

    let (state, record) = queue.get(&deleted).unwrap().unwrap().into_parts();
    assert_eq!(
        (state, record.attempts),
        (JobState::Dead, 1),
        "Retry::Never"
    );
    assert_eq!(
        record.last_error.as_deref(),
        Some("the account was deleted")
    );

    let (run_at, attempts) = next_attempt(&queue, &limited);
    assert_eq!(attempts, 1);
    assert!(
        within(run_at, Duration::from_secs(90), 0.0),
        "Retry::After, no jitter"
    );

    let (state, record) = queue.get(&reset).unwrap().unwrap().into_parts();
    assert_eq!(
        (state, record.attempts),
        (JobState::Dead, 4),
        "Retry::Default"
    );
}
