#![allow(clippy::unwrap_used, clippy::expect_used)]

//! `.now()` on a job call, like ActiveJob's `perform_now`: the body runs in
//! this process, once, and the call returns its real output or error.

use std::{
    sync::atomic::{AtomicU32, Ordering},
    thread,
};

use butler::{JobError, Progress, Retry, Retryable};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Receipt {
    to: String,
    words: u32,
}

#[butler::job]
async fn send_email(to: String, subject: String, words: u32) -> Result<Receipt, std::io::Error> {
    Ok(Receipt {
        to: format!("{to}: {subject}"),
        words,
    })
}

/// A plain `fn` job: it reports the thread it ran on.
#[butler::job]
fn crunch(n: u64) -> Result<(u64, String), std::convert::Infallible> {
    Ok((n * 2, format!("{:?}", thread::current().id())))
}

#[derive(Debug, PartialEq, thiserror::Error)]
enum ChargeError {
    #[error("card declined")]
    Declined,
}

impl Retryable for ChargeError {
    fn retry(&self) -> Retry {
        Retry::Default
    }
}

static CHARGES: AtomicU32 = AtomicU32::new(0);

/// Would be retried ten times by a worker; `.now()` runs it once.
#[butler::job(retries = 10, backoff = "fixed:1ms")]
async fn charge(cents: i64) -> Result<(), ChargeError> {
    CHARGES.fetch_add(1, Ordering::SeqCst);
    if cents > 100 {
        return Err(ChargeError::Declined);
    }
    Ok(())
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct Steps {
    done: u32,
}

#[derive(Debug, thiserror::Error)]
enum StepsError {
    #[error(transparent)]
    Interrupted(#[from] butler::Interrupted),
}

/// Checkpoints run through, and are never interrupted.
#[butler::job]
async fn walk(steps: u32, mut progress: Progress<Steps>) -> Result<u32, StepsError> {
    while progress.done < steps {
        let done = progress.done + 1;
        progress.set(Steps { done }).await?;
    }
    Ok(progress.done)
}

#[butler::job]
async fn boom() {
    panic!("async boom");
}

#[butler::job]
fn sync_boom() {
    panic!("sync boom");
}

#[test]
fn runs_the_body_and_returns_its_output_without_a_runtime() {
    // Borrowed arguments and bare integer literals, as for enqueueing.
    let receipt = butler::block_on(send_email("ada@example.com", "Hi", 3).now()).unwrap();
    assert_eq!(
        receipt,
        Receipt {
            to: "ada@example.com: Hi".into(),
            words: 3
        }
    );

    // Without a runtime, a plain `fn` job runs right here.
    let (doubled, ran_on) = butler::block_on(crunch(21).now()).unwrap();
    assert_eq!(doubled, 42);
    assert_eq!(ran_on, format!("{:?}", thread::current().id()));
}

#[test]
fn a_failure_is_the_jobs_own_error_after_one_attempt() {
    CHARGES.store(0, Ordering::SeqCst);
    let err = butler::block_on(charge(500).now()).unwrap_err();
    assert_eq!(CHARGES.load(Ordering::SeqCst), 1, "no retries");
    let JobError::Failed(failure) = err else {
        panic!("expected the job's failure, got {err:?}");
    };
    assert_eq!(failure.retry(), Retry::Default);
    let own = failure.into_inner().downcast::<ChargeError>().unwrap();
    assert_eq!(*own, ChargeError::Declined);

    butler::block_on(charge(50).now()).unwrap();
    assert_eq!(CHARGES.load(Ordering::SeqCst), 2);
}

#[test]
fn progress_starts_fresh_and_is_never_interrupted() {
    assert_eq!(butler::block_on(walk(5).now()).unwrap(), 5);
}

#[test]
fn prepared_jobs_run_now_too_ignoring_their_schedule() {
    let prepared = crunch::prepare(4u64)
        .unwrap()
        .on_queue("elsewhere")
        .unwrap()
        .run_in(std::time::Duration::from_secs(3600));
    assert_eq!(butler::block_on(prepared.now()).unwrap().0, 8);
}

#[test]
fn a_panic_in_an_async_job_reaches_the_caller() {
    let panicked = std::panic::catch_unwind(|| butler::block_on(boom().now()));
    assert!(panicked.is_err());
}

#[cfg(feature = "tokio")]
mod under_tokio {
    use butler::{JobState, MemoryQueue, Queue, monitor::ListFilter};

    use super::*;

    #[tokio::test]
    async fn plain_jobs_go_to_the_blocking_pool() {
        let (doubled, ran_on) = crunch(5).now().await.unwrap();
        assert_eq!(doubled, 10);
        assert_ne!(
            ran_on,
            format!("{:?}", thread::current().id()),
            "not on the async thread"
        );
    }

    #[tokio::test]
    async fn a_panic_in_a_plain_job_is_a_job_error() {
        let err = sync_boom().now().await.unwrap_err();
        assert!(
            matches!(&err, JobError::Panicked { message } if message == "sync boom"),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn now_futures_can_be_spawned() {
        let receipt = tokio::spawn(send_email("bob@example.com", "Yo", 1).now())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(receipt.words, 1);
    }

    /// The only test here that touches the global queue: nothing else in
    /// this binary enqueues.
    #[tokio::test]
    async fn now_enqueues_nothing_and_await_still_enqueues() {
        let queue: Queue = MemoryQueue::new().into();
        butler::configure(queue.clone());

        send_email("ada@example.com", "Hi", 1).now().await.unwrap();
        for state in [JobState::Pending, JobState::Done, JobState::Dead] {
            assert!(
                queue.list(&ListFilter::new(state)).unwrap().is_empty(),
                "{state:?}"
            );
        }

        let job = send_email("ada@example.com", "Hi", 2).await.unwrap();
        assert_eq!(queue.state(job.id()), Some(JobState::Pending));
        let spawned = tokio::spawn(send_email("ada@example.com", "Hi", 3).enqueue())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(queue.state(spawned.id()), Some(JobState::Pending));
    }
}
