#![allow(clippy::unwrap_used, clippy::expect_used)]

//! `#[job(queue = "...")]` plus worker queue priorities, end to end on the
//! in-memory backend. One test: `butler::configure` is process-global.

use std::sync::Mutex;

use butler::{MemoryQueue, Queue, QueuePriority, Worker, block_on};

static RAN: Mutex<Vec<String>> = Mutex::new(Vec::new());

fn ran(label: String) {
    RAN.lock().unwrap().push(label);
}

fn take_ran() -> Vec<String> {
    std::mem::take(&mut *RAN.lock().unwrap())
}

#[butler::job(queue = "critical")]
fn urgent(n: u32) {
    ran(format!("critical-{n}"));
}

#[butler::job]
fn normal(n: u32) {
    ran(format!("default-{n}"));
}

#[butler::job(queue = "low")]
fn chore(n: u32) {
    ran(format!("low-{n}"));
}

#[test]
fn queues_and_priorities() {
    let queue: Queue = MemoryQueue::new().into();
    butler::configure(queue.clone());

    // Enqueued lowest priority first, so FIFO alone would run them backwards.
    for n in 0..2 {
        block_on(chore(n)).unwrap();
    }
    for n in 0..2 {
        block_on(normal(n)).unwrap();
    }
    for n in 0..2 {
        block_on(urgent(n)).unwrap();
    }
    let job = block_on(urgent(99)).unwrap();
    assert_eq!(block_on(job.job()).unwrap().unwrap().queue, "critical");
    assert!(block_on(job.cancel()).unwrap());

    // Strict: a queue only runs once every queue before it is empty.
    let strict =
        Worker::new(queue.clone()).queues(QueuePriority::strict(["critical", "default", "low"]));
    assert_eq!(strict.drain().unwrap(), 6);
    assert_eq!(
        take_ran(),
        [
            "critical-0",
            "critical-1",
            "default-0",
            "default-1",
            "low-0",
            "low-1"
        ]
    );

    // A worker only serves the queues it lists; the default worker, "default".
    block_on(urgent(1)).unwrap();
    block_on(chore(1)).unwrap();
    let low_only = Worker::new(queue.clone()).queues(QueuePriority::strict(["low"]));
    assert_eq!(low_only.drain().unwrap(), 1);
    assert_eq!(take_ran(), ["low-1"]);
    assert_eq!(Worker::new(queue.clone()).drain().unwrap(), 0);
    let critical_only = Worker::new(queue.clone()).queues(QueuePriority::strict(["critical"]));
    assert_eq!(critical_only.drain().unwrap(), 1);
    take_ran();

    // Weighted 9:1: while both queues have work, about 9 in 10 claims go to
    // "critical", but "low" still gets turns instead of starving.
    for n in 0..300 {
        block_on(urgent(n)).unwrap();
        block_on(chore(n)).unwrap();
    }
    let weighted =
        Worker::new(queue.clone()).queues(QueuePriority::weighted([("critical", 9), ("low", 1)]));
    for _ in 0..200 {
        assert!(weighted.work_one().unwrap());
    }
    let first = take_ran();
    let critical = first.iter().filter(|l| l.starts_with("critical")).count();
    let low = first.len() - critical;
    // Expected 180 / 20. These bounds fail by chance about once in 10^5 runs.
    assert!(
        (150..=195).contains(&critical),
        "critical {critical}, low {low}"
    );
    assert!(low >= 5, "low starved: {low}");
    weighted.drain().unwrap();
    assert_eq!(take_ran().len(), 400);
}
