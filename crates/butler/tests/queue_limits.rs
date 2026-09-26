#![cfg(feature = "tokio")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

//! Per-queue limits: a capped queue never runs more than its limit at once,
//! and being full never holds back the worker's other queues.

use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::{Duration, Instant},
};

use butler::{JobHandle, MemoryQueue, Queue, QueuePriority, Worker};

/// Jobs running right now, and the most seen at once, per queue.
struct Gauge {
    running: AtomicUsize,
    peak: AtomicUsize,
}

impl Gauge {
    const fn new() -> Self {
        Self {
            running: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
        }
    }

    async fn measure(&self, work: Duration) {
        let now = self.running.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(now, Ordering::SeqCst);
        tokio::time::sleep(work).await;
        self.running.fetch_sub(1, Ordering::SeqCst);
    }
}

static LIMITED: Gauge = Gauge::new();
static FREE: Gauge = Gauge::new();

#[butler::job(queue = "limited")]
async fn limited(ms: u64) {
    LIMITED.measure(Duration::from_millis(ms)).await;
}

#[butler::job(queue = "free")]
async fn free(ms: u64) {
    FREE.measure(Duration::from_millis(ms)).await;
}

const WORK_MS: u64 = 80;
const POLL: Duration = Duration::from_millis(5);

#[tokio::test(flavor = "multi_thread")]
async fn a_full_queue_is_capped_and_never_blocks_the_others() {
    let queue: Queue = MemoryQueue::new().into();
    butler::configure(queue.clone());

    let mut capped: Vec<JobHandle<()>> = Vec::new();
    for _ in 0..12 {
        capped.push(limited(WORK_MS).await.unwrap());
    }
    let mut uncapped: Vec<JobHandle<()>> = Vec::new();
    for _ in 0..40 {
        uncapped.push(free(WORK_MS).await.unwrap());
    }

    // "limited" is first in strict priority, and full almost all the time: the
    // worker must skip it rather than wait on it.
    let worker = Worker::new(queue.clone())
        .concurrency(1_000)
        .queues(QueuePriority::strict(["limited", "free"]))
        .queue_limit("limited", 3);
    assert_eq!(worker.queue_limits(), [("limited", 3)]);

    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let started = Instant::now();
    let running = tokio::spawn(worker.run_async(async {
        let _ = stopped.await;
    }));

    let all_free = async {
        for job in &uncapped {
            job.wait_result(POLL).await.unwrap();
        }
        started.elapsed()
    };
    let free_done = tokio::time::timeout(Duration::from_secs(10), all_free)
        .await
        .expect("free jobs");
    let all_limited = async {
        for job in &capped {
            job.wait_result(POLL).await.unwrap();
        }
        started.elapsed()
    };
    let limited_done = tokio::time::timeout(Duration::from_secs(10), all_limited)
        .await
        .expect("limited jobs");
    stop.send(()).unwrap();
    running.await.unwrap();

    let (limited_peak, free_peak) = (
        LIMITED.peak.load(Ordering::SeqCst),
        FREE.peak.load(Ordering::SeqCst),
    );
    println!(
        "limited: peak {limited_peak}, all done in {limited_done:?}; free: peak {free_peak}, all done in {free_done:?}"
    );
    assert_eq!(limited_peak, 3, "the cap is reached but never exceeded");
    // 40 uncapped jobs run together, not a few at a time behind the full queue.
    assert!(free_peak >= 30, "free peak {free_peak}");
    // 12 capped jobs of 80ms, 3 at a time, take at least 4 rounds.
    assert!(limited_done >= Duration::from_millis(4 * WORK_MS));
    assert!(
        free_done < limited_done,
        "the full queue held the others back"
    );
}
