#![allow(clippy::unwrap_used, clippy::expect_used)]

//! What a `JobHandle` can do with the file backend: inspect, cancel, rebuild
//! from an id, and wait. Its own test binary: `butler::configure` is global.

mod common;

use std::time::Duration;

use butler::{Error, JobState, Worker, block_on};

#[butler::job]
async fn noop(label: String) {
    let _ = label;
}

#[test]
// One test: `butler::configure` is process-global, and tests run in parallel.
fn cancel_rebuild_and_wait() {
    let (queue, _dir) = common::temp_queue("handle");
    let worker = Worker::new(queue.clone());

    let kept = block_on(noop("kept")).unwrap();
    let cancelled = block_on(noop("cancelled")).unwrap();

    assert!(block_on(cancelled.cancel()).unwrap());
    assert_eq!(
        block_on(cancelled.state()).unwrap(),
        Some(JobState::Cancelled)
    );
    assert!(
        !block_on(cancelled.cancel()).unwrap(),
        "a second cancel changes nothing"
    );

    // Only the job that wasn't cancelled runs.
    assert_eq!(worker.drain().unwrap(), 1);
    assert_eq!(block_on(kept.state()).unwrap(), Some(JobState::Done));
    assert!(
        !block_on(kept.cancel()).unwrap(),
        "a finished job can't be cancelled"
    );

    // The stored record survives cancellation, for inspection.
    let record = block_on(cancelled.job()).unwrap().unwrap().record().clone();
    assert_eq!(record.args[0], "cancelled");

    // A handle rebuilt from a stored id, and `wait` without a tokio runtime.
    let id = block_on(noop("stored")).unwrap().into_id();
    let handle = queue.handle(id);
    assert_eq!(block_on(handle.state()).unwrap(), Some(JobState::Pending));

    worker.drain().unwrap();
    assert_eq!(
        block_on(handle.wait(Duration::from_millis(1))).unwrap(),
        JobState::Done
    );

    let missing = queue.handle("no-such-job");
    assert_eq!(block_on(missing.state()).unwrap(), None);
    assert!(matches!(
        block_on(missing.wait(Duration::from_millis(1))),
        Err(Error::JobNotFound { .. })
    ));
}
