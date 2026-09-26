#![cfg(feature = "tokio")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

//! Enqueue from a tokio runtime, run the job with `Worker::run_async`, and
//! check that the job body could use tokio's timer.

mod common;

use std::time::Duration;

use butler::{JobState, Worker};

#[butler::job]
async fn sleepy_write(path: String, millis: u64) -> std::io::Result<()> {
    // Panics outside a tokio runtime, so this proves the job ran on tokio.
    tokio::time::sleep(Duration::from_millis(millis)).await;
    std::fs::write(path, format!("slept {millis}ms"))
}

#[tokio::test(flavor = "multi_thread")]
async fn tokio_worker_runs_job_enqueued_from_tokio() {
    let (queue, dir) = common::temp_queue("tokio");
    let out = dir.join("out.txt");

    let job = sleepy_write(out.display().to_string(), 50).await.unwrap();
    assert_eq!(job.state().await.unwrap(), Some(JobState::Pending));

    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let worker = Worker::new(queue.clone()).poll_interval(Duration::from_millis(10));
    let running = tokio::spawn(worker.run_async(async {
        let _ = stop_rx.await;
    }));

    let finished =
        tokio::time::timeout(Duration::from_secs(5), job.wait(Duration::from_millis(10)))
            .await
            .expect("job did not finish in time")
            .unwrap();
    assert_eq!(finished, JobState::Done);
    stop_tx.send(()).unwrap();
    running.await.unwrap();

    assert_eq!(std::fs::read_to_string(&out).unwrap(), "slept 50ms");
}
