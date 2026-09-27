#![allow(clippy::unwrap_used, clippy::expect_used)]

//! Pausing a queue through a worker: it stops claiming from it, keeps
//! serving its other queues, and picks the paused jobs up once resumed.

use std::sync::atomic::{AtomicUsize, Ordering};

use butler::{JobState, MemoryQueue, Queue, QueuePriority, Worker};
use serde_json::json;

static RAN: AtomicUsize = AtomicUsize::new(0);

#[butler::job]
fn count(_n: u32) {
    RAN.fetch_add(1, Ordering::SeqCst);
}

#[test]
fn a_worker_skips_a_paused_queue_until_it_is_resumed() {
    let queue: Queue = MemoryQueue::new().into();
    let worker = Worker::new(queue.clone()).queues(QueuePriority::strict(["mailers", "default"]));
    let mailers: Vec<_> = (0..3)
        .map(|n| queue.push("count", "mailers", vec![json!(n)]).unwrap())
        .collect();
    queue.push("count", "default", vec![json!(9)]).unwrap();

    queue.pause_queue("mailers").unwrap();
    // `drain` reads the paused queues first, like the keeper does.
    assert_eq!(worker.drain().unwrap(), 1, "only the unpaused queue runs");
    for id in &mailers {
        assert_eq!(queue.state(id), Some(JobState::Pending));
    }

    // Still accepting jobs while paused.
    queue.push("count", "mailers", vec![json!(3)]).unwrap();
    assert_eq!(worker.drain().unwrap(), 0);

    queue.resume_queue("mailers").unwrap();
    assert_eq!(worker.drain().unwrap(), 4);
    assert_eq!(RAN.load(Ordering::SeqCst), 5);
}
