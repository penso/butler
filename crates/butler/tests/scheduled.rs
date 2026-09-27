#![cfg(feature = "tokio")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

//! Scheduled jobs through the public API: `prepare(..).run_in(..)`, a worker
//! that runs them once they are due, the keeper promoting them while every
//! slot is busy, and inline tests that run them at once.

use std::time::{Duration, Instant, SystemTime};

use butler::{JobState, MemoryQueue, Queue, QueuePriority, Worker, testing::InlineJobs};
use serde_json::json;

const HOUR: Duration = Duration::from_secs(3600);

#[butler::job(queue = "mailers")]
async fn send_reminder(user_id: u64) -> Result<u64, std::io::Error> {
    Ok(user_id)
}

#[butler::job]
async fn hold(millis: u64) {
    tokio::time::sleep(Duration::from_millis(millis)).await;
}

/// Runs `worker` until the returned sender is used.
fn start(
    worker: Worker,
) -> (
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<()>,
) {
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let running = tokio::spawn(worker.run_async(async {
        let _ = stopped.await;
    }));
    (stop, running)
}

// The only test here that uses the global queue, so tests can run in parallel.
#[tokio::test(flavor = "multi_thread")]
async fn a_worker_runs_a_scheduled_job_once_it_is_due() {
    let queue: Queue = MemoryQueue::new().into();
    butler::configure(queue.clone());

    let delay = Duration::from_millis(300);
    let enqueued = Instant::now();
    let job = send_reminder::prepare(7)
        .unwrap()
        .run_in(delay)
        .enqueue()
        .await
        .unwrap();
    assert_eq!(job.state().await.unwrap(), Some(JobState::Scheduled));

    // Scheduled through `enqueue_all` too, and cancelled while it waits.
    let [later] = butler::enqueue_all([send_reminder::prepare(8).unwrap().run_in(HOUR)])
        .await
        .unwrap()
        .try_into()
        .unwrap();
    assert_eq!(later.state().await.unwrap(), Some(JobState::Scheduled));
    assert!(later.cancel().await.unwrap());
    assert_eq!(later.state().await.unwrap(), Some(JobState::Cancelled));

    // Idle claims wait up to a second: the job must not wait for that.
    let worker = Worker::new(queue.clone())
        .queues(QueuePriority::strict(["mailers"]))
        .poll_interval(Duration::from_secs(1));
    let (stop, running) = start(worker);
    let output = tokio::time::timeout(
        Duration::from_secs(5),
        job.wait_result(Duration::from_millis(10)),
    )
    .await
    .expect("the scheduled job never ran")
    .unwrap();
    let elapsed = enqueued.elapsed();
    assert_eq!(output, 7);
    assert!(
        elapsed >= delay - Duration::from_millis(5),
        "ran early: {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_millis(900),
        "waited for a poll: {elapsed:?}"
    );
    stop.send(()).unwrap();
    running.await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn the_keeper_promotes_due_jobs_while_every_slot_is_busy() {
    let queue: Queue = MemoryQueue::new().into();
    // Takes the worker's only slot, so no claim runs while the other job
    // comes due: only the keeper can move it onto its queue.
    let busy = queue.push("hold", "default", vec![json!(800)]).unwrap();
    let due = queue
        .schedule(
            "send_reminder",
            "mailers",
            vec![json!(1)],
            SystemTime::now() + Duration::from_millis(100),
        )
        .unwrap();

    let worker = Worker::new(queue.clone())
        .queues(QueuePriority::strict(["default", "mailers"]))
        .concurrency(1)
        .poll_interval(Duration::from_millis(20));
    let (stop, running) = start(worker);
    let deadline = Instant::now() + Duration::from_secs(5);
    while queue.state(&due) != Some(JobState::Pending) {
        assert!(
            Instant::now() < deadline,
            "never promoted: {:?}",
            queue.state(&due)
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        queue.state(&busy),
        Some(JobState::Processing),
        "slot still busy"
    );
    stop.send(()).unwrap();
    running.await.unwrap();
}

#[tokio::test]
async fn inline_tests_run_scheduled_jobs_at_once() {
    let jobs = InlineJobs::new();
    let output = jobs
        .perform(async {
            let job = send_reminder::prepare(9)
                .unwrap()
                .run_in(HOUR)
                .enqueue()
                .await
                .unwrap();
            job.result().await.unwrap()
        })
        .await;
    assert_eq!(output, Some(9));
    assert_eq!(jobs.performed_names(), ["send_reminder"]);
}
