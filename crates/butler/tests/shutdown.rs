#![allow(clippy::unwrap_used, clippy::expect_used)]

//! Worker shutdown: the heartbeat outlives the jobs still finishing, and
//! `shutdown_timeout` bounds how long the worker waits for them. Each test
//! has its own job and queue, so they run in parallel.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use butler::{JobState, MemoryQueue, Queue, Worker};

/// A job that runs, without checkpoints, until the test releases it.
struct Gate {
    started: AtomicBool,
    release: AtomicBool,
}

impl Gate {
    const fn new() -> Self {
        Self {
            started: AtomicBool::new(false),
            release: AtomicBool::new(false),
        }
    }

    fn hold(&self) {
        self.started.store(true, Ordering::SeqCst);
        while !self.release.load(Ordering::SeqCst) {
            thread::sleep(Duration::from_millis(2));
        }
    }
}

static THREADS_GIVE_UP: Gate = Gate::new();
static THREADS_HEARTBEAT: Gate = Gate::new();

#[butler::job]
fn held_by_threads_give_up() {
    THREADS_GIVE_UP.hold();
}

#[butler::job]
fn held_by_threads_heartbeat() {
    THREADS_HEARTBEAT.hold();
}

/// Waits up to 10 seconds for `condition`.
fn wait_until(what: &str, mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !condition() {
        assert!(Instant::now() < deadline, "timed out waiting: {what}");
        thread::sleep(Duration::from_millis(5));
    }
}

fn worker(queue: &Queue) -> Worker {
    Worker::new(queue.clone())
        .concurrency(1)
        .poll_interval(Duration::from_millis(10))
        .heartbeat_ttl(Duration::from_secs(1))
}

/// Runs `worker` with `run_until` on a thread; returns its stop flag and a
/// receiver that gets a message once `run_until` returns.
fn run_threads(worker: Worker) -> (Arc<AtomicBool>, mpsc::Receiver<()>) {
    let stop = Arc::new(AtomicBool::new(false));
    let (returned, returns) = mpsc::channel();
    let flag = Arc::clone(&stop);
    thread::spawn(move || {
        worker.run_until(flag);
        let _ = returned.send(());
    });
    (stop, returns)
}

/// When the worker's heartbeat expires, in ms since the epoch, or `None` if
/// it isn't registered.
fn heartbeat_expiry(queue: &Queue, worker: &str) -> Option<i128> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    let stats = queue.stats().unwrap();
    let found = stats.workers.iter().find(|w| w.id == worker)?;
    Some(now.as_millis() as i128 + i128::from(found.expires_in_ms))
}

#[test]
fn run_until_gives_up_after_the_shutdown_timeout_and_leaves_the_job_to_recovery() {
    let queue: Queue = MemoryQueue::new().into();
    let id = queue
        .push("held_by_threads_give_up", "default", vec![])
        .unwrap();
    let worker = worker(&queue).shutdown_timeout(Duration::from_millis(100));
    let worker_id = worker.id().to_owned();
    let (stop, returns) = run_threads(worker);

    wait_until("the job started", || {
        THREADS_GIVE_UP.started.load(Ordering::SeqCst)
    });
    stop.store(true, Ordering::SeqCst);
    returns
        .recv_timeout(Duration::from_secs(10))
        .expect("run_until returned without waiting for the job");

    // Still running, still held, and the worker didn't retire.
    assert_eq!(queue.state(&id), Some(JobState::Processing));
    assert!(heartbeat_expiry(&queue, &worker_id).is_some());
    // Its heartbeat stopped, so recovery puts the job back once it expires.
    wait_until("the job was recovered", || queue.recover().unwrap() == 1);
    assert_eq!(queue.state(&id), Some(JobState::Pending));

    THREADS_GIVE_UP.release.store(true, Ordering::SeqCst);
}

#[test]
fn run_until_keeps_the_heartbeat_while_jobs_finish_after_stop() {
    let queue: Queue = MemoryQueue::new().into();
    let id = queue
        .push("held_by_threads_heartbeat", "default", vec![])
        .unwrap();
    let worker = worker(&queue).shutdown_timeout(Duration::MAX);
    let worker_id = worker.id().to_owned();
    let (stop, returns) = run_threads(worker);

    wait_until("the job started", || {
        THREADS_HEARTBEAT.started.load(Ordering::SeqCst)
    });
    stop.store(true, Ordering::SeqCst);
    let first = heartbeat_expiry(&queue, &worker_id).expect("registered");
    // The keeper refreshes it at least every half second while the job runs.
    wait_until("the heartbeat was refreshed after stop", || {
        heartbeat_expiry(&queue, &worker_id).is_some_and(|expiry| expiry > first + 100)
    });
    assert_eq!(
        queue.recover().unwrap(),
        0,
        "the running job isn't recovered"
    );
    assert!(
        returns.try_recv().is_err(),
        "no timeout: still waiting for the job"
    );

    THREADS_HEARTBEAT.release.store(true, Ordering::SeqCst);
    returns
        .recv_timeout(Duration::from_secs(10))
        .expect("run_until returned once the job finished");
    assert_eq!(queue.state(&id), Some(JobState::Done));
    assert_eq!(heartbeat_expiry(&queue, &worker_id), None, "retired");
}

#[cfg(feature = "tokio")]
mod tokio_worker {
    use super::*;

    static ASYNC_GIVE_UP: Gate = Gate::new();
    static ASYNC_FINISHES: Gate = Gate::new();

    #[butler::job]
    async fn held_by_tasks_give_up() {
        ASYNC_GIVE_UP.started.store(true, Ordering::SeqCst);
        while !ASYNC_GIVE_UP.release.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }

    #[butler::job]
    async fn held_by_tasks_finishes() {
        ASYNC_FINISHES.started.store(true, Ordering::SeqCst);
        while !ASYNC_FINISHES.release.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }

    async fn wait_until_async(what: &str, mut condition: impl FnMut() -> bool) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while !condition() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("timed out waiting: {what}"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn run_async_gives_up_after_the_shutdown_timeout_and_leaves_the_job_to_recovery() {
        let queue: Queue = MemoryQueue::new().into();
        let id = queue
            .push("held_by_tasks_give_up", "default", vec![])
            .unwrap();
        let worker = worker(&queue).shutdown_timeout(Duration::from_millis(100));
        let worker_id = worker.id().to_owned();
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let running = tokio::spawn(worker.run_async(async {
            let _ = stopped.await;
        }));

        wait_until_async("the job started", || {
            ASYNC_GIVE_UP.started.load(Ordering::SeqCst)
        })
        .await;
        stop.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(10), running)
            .await
            .expect("run_async returned without waiting for the job")
            .unwrap();

        assert_eq!(queue.state(&id), Some(JobState::Processing));
        assert!(
            heartbeat_expiry(&queue, &worker_id).is_some(),
            "not retired"
        );
        wait_until_async("the job was recovered", || queue.recover().unwrap() == 1).await;
        assert_eq!(queue.state(&id), Some(JobState::Pending));

        ASYNC_GIVE_UP.release.store(true, Ordering::SeqCst);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn run_async_waits_for_a_job_that_finishes_within_the_timeout() {
        let queue: Queue = MemoryQueue::new().into();
        let id = queue
            .push("held_by_tasks_finishes", "default", vec![])
            .unwrap();
        let worker = worker(&queue).shutdown_timeout(Duration::from_secs(60));
        let worker_id = worker.id().to_owned();
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let mut running = tokio::spawn(worker.run_async(async {
            let _ = stopped.await;
        }));

        wait_until_async("the job started", || {
            ASYNC_FINISHES.started.load(Ordering::SeqCst)
        })
        .await;
        stop.send(()).unwrap();
        let first = heartbeat_expiry(&queue, &worker_id).expect("registered");
        wait_until_async("the heartbeat was refreshed after shutdown", || {
            heartbeat_expiry(&queue, &worker_id).is_some_and(|expiry| expiry > first + 100)
        })
        .await;
        assert!(
            !running.is_finished(),
            "still waiting for the job, within the timeout"
        );

        ASYNC_FINISHES.release.store(true, Ordering::SeqCst);
        tokio::time::timeout(Duration::from_secs(10), &mut running)
            .await
            .expect("run_async returned once the job finished")
            .unwrap();
        assert_eq!(queue.state(&id), Some(JobState::Done));
        assert_eq!(heartbeat_expiry(&queue, &worker_id), None, "retired");
    }
}
