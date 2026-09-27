#![cfg(feature = "tokio")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

//! `butler::testing`: jobs enqueued inside `perform_enqueued_jobs` run
//! immediately, like ActiveJob's test helper of the same name.

use std::sync::Mutex;

use butler::{
    Error, JobHandle, JobState, MemoryQueue, Queue,
    testing::{InlineJobs, perform_enqueued_jobs},
};

/// Stands in for "the user row" a Rails test would reload.
static PROCESSED: Mutex<Vec<u64>> = Mutex::new(Vec::new());

fn processed(user_id: u64) -> bool {
    PROCESSED.lock().unwrap().contains(&user_id)
}

#[butler::job]
async fn process_user(user_id: u64) -> Result<u64, std::io::Error> {
    tokio::task::yield_now().await; // a real await inside the job
    PROCESSED.lock().unwrap().push(user_id);
    Ok(user_id * 10)
}

#[butler::job(retries = 5, backoff = "fixed:1h")]
async fn always_fails(user_id: u64) -> Result<(), String> {
    Err(format!("user {user_id} is locked"))
}

/// Enqueues another job from inside a job.
#[butler::job]
async fn signup(user_id: u64) -> Result<(), Error> {
    process_user(user_id).await?;
    Ok(())
}

/// A plain `fn` job: runs on the blocking pool, inline too.
#[butler::job]
fn crunch(n: u32) -> Result<u32, std::convert::Infallible> {
    Ok(n * 2)
}

#[tokio::test]
async fn it_processes_the_job_immediately() {
    perform_enqueued_jobs(async {
        let job = process_user(1).await.unwrap();
        // Already ran, before the block even ends.
        assert!(processed(1));
        assert_eq!(job.result().await.unwrap(), Some(10));
    })
    .await;

    assert!(processed(1));
}

#[tokio::test]
async fn outside_the_block_jobs_are_only_enqueued() {
    let queue: Queue = MemoryQueue::new().into();
    butler::configure(queue.clone());

    let job = process_user(2).await.unwrap();
    assert!(!processed(2), "no worker ran it");
    assert_eq!(queue.state(job.id()), Some(JobState::Pending));

    perform_enqueued_jobs(async {
        process_user(3).await.unwrap();
    })
    .await;
    assert!(processed(3));
    assert!(!processed(2), "the block only runs jobs enqueued inside it");
}

#[tokio::test]
async fn a_failing_job_is_dead_after_one_attempt() {
    let jobs = InlineJobs::new();
    let handle: JobHandle<()> = jobs.perform(async { always_fails(7).await.unwrap() }).await;

    let err = handle
        .wait_result(std::time::Duration::ZERO)
        .await
        .unwrap_err();
    assert!(
        matches!(&err, Error::JobFailed { error, .. } if error == "user 7 is locked"),
        "{err:?}"
    );
    let [performed] = jobs.performed().try_into().unwrap();
    assert_eq!(performed.state(), JobState::Dead);
    assert_eq!(
        performed.record().attempts,
        1,
        "no retries inline, even with retries = 5"
    );
}

#[tokio::test]
async fn jobs_enqueued_by_jobs_run_inline_too() {
    let jobs = InlineJobs::new();
    jobs.perform(async { signup(4).await.unwrap() }).await;

    assert!(processed(4));
    // The inner job finishes first: it runs inside the outer one.
    assert_eq!(jobs.performed_names(), ["process_user", "signup"]);
    assert!(
        jobs.performed()
            .iter()
            .all(|job| job.state() == JobState::Done)
    );
}

#[tokio::test]
async fn plain_fn_jobs_run_inline() {
    let jobs = InlineJobs::new();
    let value = jobs
        .perform(async { crunch(21).await.unwrap().result().await.unwrap() })
        .await;
    assert_eq!(value, Some(42));
}

#[test]
fn no_runtime_needed() {
    let value = butler::block_on(perform_enqueued_jobs(async {
        crunch(5).await.unwrap().result().await.unwrap()
    }));
    assert_eq!(value, Some(10));
}
