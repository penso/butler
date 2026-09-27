#![cfg(feature = "tokio")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

//! Global queue limits through real workers: several workers with the same
//! global limit never run more than it between them. That the backends count
//! and free slots atomically is in `backends.rs`.

use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

use butler::{JobState, MemoryQueue, Queue, QueuePriority, Worker};
use serde_json::json;

static RUNNING: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

#[butler::job(queue = "shared")]
async fn shared_job(ms: u64) {
    let now = RUNNING.fetch_add(1, Ordering::SeqCst) + 1;
    PEAK.fetch_max(now, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(ms)).await;
    RUNNING.fetch_sub(1, Ordering::SeqCst);
}

#[tokio::test(flavor = "multi_thread")]
async fn three_workers_share_one_global_limit() {
    let queue: Queue = MemoryQueue::new().into();
    let ids: Vec<_> = (0..15)
        .map(|_| queue.push("shared_job", "shared", vec![json!(60)]).unwrap())
        .collect();

    // Each worker alone could run all 15 at once.
    let (stop, stopped) = tokio::sync::watch::channel(false);
    let workers: Vec<_> = (0..3)
        .map(|_| {
            let worker = Worker::new(queue.clone())
                .concurrency(100)
                .poll_interval(Duration::from_millis(5))
                .queues(QueuePriority::strict(["shared"]))
                .global_queue_limit("shared", 3);
            assert_eq!(worker.global_queue_limits(), [("shared", 3)]);
            let mut stopped = stopped.clone();
            tokio::spawn(worker.run_async(async move {
                let _ = stopped.wait_for(|stop| *stop).await;
            }))
        })
        .collect();

    let all_done = async {
        loop {
            if ids.iter().all(|id| queue.state(id) == Some(JobState::Done)) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(10), all_done)
        .await
        .expect("every job ran");
    stop.send(true).unwrap();
    for worker in workers {
        worker.await.unwrap();
    }
    assert_eq!(
        PEAK.load(Ordering::SeqCst),
        3,
        "the limit is reached, and never exceeded, across the workers"
    );
}
