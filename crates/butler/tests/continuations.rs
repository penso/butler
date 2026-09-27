#![cfg(feature = "tokio")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

//! Job continuations: a job with a `Progress` resumes from its last checkpoint
//! after a worker shutdown, a failed attempt, or (tests/recovery_progress.rs) a
//! crash. One test at a time uses the global queue, so they are serialized.

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU32, Ordering},
    },
    time::Duration,
};

use butler::{
    Backoff, DeadJob, Interrupted, JobError, JobHandle, JobState, MemoryQueue, Progress, Queue,
    QueuePriority, Worker, testing::InlineJobs,
};
use serde::{Deserialize, Serialize};

/// The steps of an import, each with its own cursor.
#[derive(Debug, Default, Serialize, Deserialize)]
enum Import {
    #[default]
    Start,
    Items {
        next: u32,
    },
    Finalize,
}

#[derive(Debug, thiserror::Error)]
enum ImportError {
    #[error("item {0} failed")]
    Item(u32),
    #[error(transparent)]
    Interrupted(#[from] Interrupted),
}

/// Every item processed, in order: each must appear exactly once.
static PROCESSED: Mutex<Vec<u32>> = Mutex::new(Vec::new());
static STARTS: Mutex<u32> = Mutex::new(0);
static FAIL_ON_7_ONCE: AtomicBool = AtomicBool::new(false);

/// Tests share the statics and the global queue: one at a time. A tokio
/// mutex, since the guard is held across `.await`.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn serial() -> tokio::sync::MutexGuard<'static, ()> {
    let guard = SERIAL.lock().await;
    PROCESSED.lock().unwrap().clear();
    *STARTS.lock().unwrap() = 0;
    guard
}

#[butler::job]
async fn import(
    items: u32,
    pause_ms: u64,
    mut progress: Progress<Import>,
) -> Result<u32, ImportError> {
    loop {
        match *progress {
            Import::Start => {
                *STARTS.lock().unwrap() += 1;
                progress.set(Import::Items { next: 0 }).await?;
            }
            Import::Items { next } if next < items => {
                if next == 7 && FAIL_ON_7_ONCE.swap(false, Ordering::SeqCst) {
                    return Err(ImportError::Item(7));
                }
                tokio::time::sleep(Duration::from_millis(pause_ms)).await;
                PROCESSED.lock().unwrap().push(next);
                progress.set(Import::Items { next: next + 1 }).await?;
            }
            Import::Items { .. } => progress.set(Import::Finalize).await?,
            Import::Finalize => return Ok(items),
        }
    }
}

fn processed() -> Vec<u32> {
    PROCESSED.lock().unwrap().clone()
}

async fn run_until_done(queue: &Queue, job: &JobHandle<u32>) -> u32 {
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let worker = Worker::new(queue.clone()).poll_interval(Duration::from_millis(10));
    let running = tokio::spawn(worker.run_async(async {
        let _ = stopped.await;
    }));
    let value = tokio::time::timeout(
        Duration::from_secs(10),
        job.wait_result(Duration::from_secs(1)),
    )
    .await
    .expect("job finished")
    .unwrap();
    stop.send(()).unwrap();
    running.await.unwrap();
    value
}

#[tokio::test(flavor = "multi_thread")]
async fn a_worker_shutdown_interrupts_and_the_next_worker_resumes() {
    let _serial = serial().await;
    let queue: Queue = MemoryQueue::new().into();
    butler::configure(queue.clone());
    let job = import(40, 10).await.unwrap();

    // First worker: stop it partway through.
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let worker = Worker::new(queue.clone()).poll_interval(Duration::from_millis(10));
    let running = tokio::spawn(worker.run_async(async {
        let _ = stopped.await;
    }));
    while processed().len() < 5 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    stop.send(()).unwrap();
    running.await.unwrap(); // returns promptly: the job stops at its next checkpoint

    let done_before = processed().len() as u32;
    assert!(done_before < 40, "interrupted, not run to the end");
    let (state, record) = queue.get(job.id()).unwrap().unwrap().into_parts();
    assert_eq!(state, JobState::Pending, "back on its queue");
    assert_eq!(record.attempts, 0, "an interruption is not a failure");
    assert_eq!(
        record.progress,
        Some(serde_json::json!({ "Items": { "next": done_before } }))
    );

    // Second worker: resumes where the first stopped.
    assert_eq!(run_until_done(&queue, &job).await, 40);
    assert_eq!(
        processed(),
        (0..40).collect::<Vec<_>>(),
        "each item once, in order"
    );
    assert_eq!(*STARTS.lock().unwrap(), 1, "Start didn't run again");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_attempt_retries_from_its_last_checkpoint() {
    let _serial = serial().await;
    let queue: Queue = MemoryQueue::new().into();
    butler::configure(queue.clone());
    FAIL_ON_7_ONCE.store(true, Ordering::SeqCst);
    let job = import(10, 0).await.unwrap();

    assert_eq!(run_until_done(&queue, &job).await, 10);
    assert_eq!(
        processed(),
        (0..10).collect::<Vec<_>>(),
        "items 0-6 weren't redone"
    );
    let record = job.job().await.unwrap().unwrap().record().clone();
    assert_eq!(record.attempts, 1);
    assert_eq!(record.last_error.as_deref(), Some("item 7 failed"));
}

#[tokio::test(flavor = "multi_thread")]
async fn inline_tests_can_interrupt_at_a_checkpoint() {
    let _serial = serial().await;
    let jobs = InlineJobs::new().interrupt_at_checkpoint(4);
    let handle = jobs.perform(async { import(6, 0).await.unwrap() }).await;

    assert_eq!(jobs.interruptions(), 1);
    assert_eq!(handle.result().await.unwrap(), Some(6));
    assert_eq!(processed(), (0..6).collect::<Vec<_>>());
    assert_eq!(*STARTS.lock().unwrap(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn progress_that_no_longer_fits_its_type_fails_clearly() {
    let _serial = serial().await;
    let queue: Queue = MemoryQueue::new().into();
    butler::configure(queue.clone());
    let job = import(3, 0).await.unwrap();
    // As if a deploy changed the Import type under a saved job.
    let claimed = queue
        .claim("w", &["default"], Duration::ZERO)
        .unwrap()
        .unwrap();
    let mut record = claimed.into_record();
    record.progress = Some(serde_json::json!({ "Unknown": 1 }));
    queue.checkpoint("w", &record).unwrap();
    queue.recover().unwrap(); // "w" never sent a heartbeat: its job goes back

    let worker = Worker::new(queue.clone()).max_retries(0);
    worker.drain().unwrap();
    let dead = job.job().await.unwrap().unwrap();
    assert_eq!(dead.state(), JobState::Dead);
    let error = dead.record().last_error.clone().unwrap_or_default();
    assert!(
        error.starts_with("saved job progress doesn't match"),
        "{error}"
    );
}

/// Runs of `endless` so far: it checkpoints forever and never finishes, so
/// only a worker shutdown ends a run.
static ENDLESS_RUNS: AtomicU32 = AtomicU32::new(0);
static STUBBORN_RUNS: AtomicU32 = AtomicU32::new(0);

#[butler::job(max_resumptions = 1, retries = 0)]
async fn endless(mut progress: Progress<u32>) -> Result<(), Interrupted> {
    ENDLESS_RUNS.fetch_add(1, Ordering::SeqCst);
    loop {
        tokio::time::sleep(Duration::from_millis(1)).await;
        progress.set(*progress + 1).await?;
    }
}

/// Like `endless`, without a limit of its own: the worker's applies.
#[butler::job]
async fn stubborn(mut progress: Progress<u32>) -> Result<(), Interrupted> {
    STUBBORN_RUNS.fetch_add(1, Ordering::SeqCst);
    loop {
        tokio::time::sleep(Duration::from_millis(1)).await;
        progress.set(*progress + 1).await?;
    }
}

/// Starts `worker`, waits until `runs` goes past `before` (a run of the job
/// has started), then shuts the worker down and waits for it to return.
async fn interrupt_one_run(worker: Worker, runs: &AtomicU32, before: u32) {
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let running = tokio::spawn(worker.run_async(async {
        let _ = stopped.await;
    }));
    tokio::time::timeout(Duration::from_secs(10), async {
        while runs.load(Ordering::SeqCst) <= before {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the job started");
    stop.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(10), running)
        .await
        .expect("the job stopped at a checkpoint")
        .unwrap();
}

fn worker_for(queue: &Queue) -> Worker {
    Worker::new(queue.clone()).poll_interval(Duration::from_millis(10))
}

#[tokio::test(flavor = "multi_thread")]
async fn a_job_interrupted_past_max_resumptions_counts_a_failed_attempt() {
    assert_eq!(endless::JOB.max_resumptions, Some(1));
    assert_eq!(stubborn::JOB.max_resumptions, None);
    let queue: Queue = MemoryQueue::new().into();
    let id = queue.push("endless", "default", vec![]).unwrap();
    let dead = Arc::new(Mutex::new(None));
    let worker = || {
        let dead = Arc::clone(&dead);
        worker_for(&queue).on_dead(move |job: DeadJob| {
            let dead = Arc::clone(&dead);
            async move {
                let limit = matches!(job.error(), JobError::ResumeLimit { max: 1 });
                *dead.lock().unwrap() = Some(limit);
            }
        })
    };

    // The first shutdown is within the limit: back on the queue, no attempt.
    interrupt_one_run(worker(), &ENDLESS_RUNS, 0).await;
    let (state, record) = queue.get(&id).unwrap().unwrap().into_parts();
    assert_eq!(state, JobState::Pending);
    assert_eq!((record.attempts, record.resumptions), (0, 1));
    let saved = record.progress.and_then(|p| p.as_u64()).expect("progress");

    // The second is past it: a failed attempt, and with `retries = 0`, dead.
    interrupt_one_run(worker(), &ENDLESS_RUNS, 1).await;
    let (state, record) = queue.get(&id).unwrap().unwrap().into_parts();
    assert_eq!(state, JobState::Dead);
    assert_eq!((record.attempts, record.resumptions), (1, 1));
    let error = record.last_error.unwrap_or_default();
    assert!(error.contains("max_resumptions"), "{error}");
    let progress = record.progress.and_then(|p| p.as_u64()).expect("progress");
    assert!(
        progress > saved,
        "the second run resumed and kept its progress"
    );
    assert_eq!(*dead.lock().unwrap(), Some(true), "on_dead saw ResumeLimit");
    assert_eq!(ENDLESS_RUNS.load(Ordering::SeqCst), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_workers_max_resumptions_applies_through_the_retry_policy() {
    let queue: Queue = MemoryQueue::new().into();
    let id = queue.push("stubborn", "default", vec![]).unwrap();
    let worker = worker_for(&queue)
        .max_resumptions(0)
        .max_retries(3)
        .backoff(Backoff::NONE);

    interrupt_one_run(worker, &STUBBORN_RUNS, 0).await;
    let (state, record) = queue.get(&id).unwrap().unwrap().into_parts();
    assert_eq!(state, JobState::Pending, "retried, not dead: {record:?}");
    assert_eq!((record.attempts, record.resumptions), (1, 0));
    assert!(
        record.progress.is_some(),
        "a retry resumes from its progress"
    );
}

/// Each execution of `in_two_steps`, and of `bystander`, in order.
static STEPS: Mutex<Vec<&'static str>> = Mutex::new(Vec::new());

#[derive(Debug, Default, Serialize, Deserialize)]
enum TwoSteps {
    #[default]
    First,
    Second,
}

#[butler::job]
async fn in_two_steps(mut progress: Progress<TwoSteps>) -> Result<u32, Interrupted> {
    match *progress {
        TwoSteps::First => {
            STEPS.lock().unwrap().push("first");
            progress.requeue(TwoSteps::Second)?;
            unreachable!("requeue always interrupts")
        }
        TwoSteps::Second => {
            STEPS.lock().unwrap().push("second");
            Ok(2)
        }
    }
}

#[butler::job(queue = "isolated")]
fn bystander() {
    STEPS.lock().unwrap().push("bystander");
}

#[tokio::test(flavor = "multi_thread")]
async fn requeue_starts_the_next_step_in_a_fresh_execution() {
    let _serial = serial().await;
    STEPS.lock().unwrap().clear();
    let queue: Queue = MemoryQueue::new().into();
    let id = queue.push("in_two_steps", "isolated", vec![]).unwrap();
    queue.push("bystander", "isolated", vec![]).unwrap();

    let worker = Worker::new(queue.clone())
        .queues(QueuePriority::strict(["isolated"]))
        .max_resumptions(0);
    let drained = tokio::task::spawn_blocking(move || worker.drain().unwrap());
    assert_eq!(
        drained.await.unwrap(),
        3,
        "two executions and the bystander"
    );
    assert_eq!(
        *STEPS.lock().unwrap(),
        ["first", "bystander", "second"],
        "in memory, the second step waits its turn behind the job queued before it"
    );
    let job = queue.get(&id).unwrap().unwrap();
    assert_eq!(job.state(), JobState::Done);
    let record = job.record();
    assert_eq!(
        (record.attempts, record.resumptions),
        (0, 0),
        "neither an attempt nor a resumption, even with max_resumptions = 0"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn requeue_resumes_at_once_when_run_now_or_inline() {
    let _serial = serial().await;
    STEPS.lock().unwrap().clear();
    assert_eq!(in_two_steps().now().await.unwrap(), 2);

    let jobs = InlineJobs::new();
    let handle = jobs.perform(async { in_two_steps().await.unwrap() }).await;
    assert_eq!(handle.result().await.unwrap(), Some(2));
    assert_eq!(
        *STEPS.lock().unwrap(),
        ["first", "second", "first", "second"]
    );
}
