#![cfg(all(feature = "redis", feature = "tokio"))]
#![allow(clippy::unwrap_used, clippy::expect_used)]

//! Runs a job through Redis with the tokio worker, including a retry that ends
//! in the dead list. Needs a Redis server: `$BUTLER_TEST_REDIS_URL`, or
//! `redis://127.0.0.1:6379/`. Skipped when none is reachable.

mod common;

use std::time::Duration;

use butler::{JobState, Queue, RedisQueue, Worker};

#[butler::job]
async fn redis_sleepy_write(path: String) -> std::io::Result<()> {
    tokio::time::sleep(Duration::from_millis(20)).await;
    std::fs::write(path, "via redis")
}

#[butler::job]
async fn redis_always_fails() -> std::io::Result<()> {
    Err(std::io::Error::other("nope"))
}

#[tokio::test(flavor = "multi_thread")]
async fn redis_roundtrip_with_retry() {
    let url = std::env::var("BUTLER_TEST_REDIS_URL").unwrap_or("redis://127.0.0.1:6379/".into());
    // A unique prefix, so runs don't see each other's keys.
    let prefix = format!("butler-test-{}", std::process::id());
    let queue: Queue = match RedisQueue::connect(&url, &prefix) {
        Ok(q) => q.into(),
        Err(e) => {
            eprintln!("skipping: no redis at {url}: {e}");
            return;
        }
    };
    butler::configure(queue.clone());

    let out = std::env::temp_dir().join(format!("{prefix}.txt"));
    let ok = redis_sleepy_write(out.display().to_string()).await.unwrap();
    let bad = redis_always_fails().await.unwrap();
    assert_eq!(ok.state().await.unwrap(), Some(JobState::Pending));

    // Cancelling a job no worker has claimed yet removes it from the queue.
    let doomed = redis_sleepy_write(out.display().to_string()).await.unwrap();
    assert!(doomed.cancel().await.unwrap());
    assert_eq!(doomed.state().await.unwrap(), Some(JobState::Cancelled));
    assert!(
        !doomed.cancel().await.unwrap(),
        "a second cancel changes nothing"
    );

    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let worker = Worker::new(queue.clone())
        .max_retries(1)
        .poll_interval(Duration::from_millis(10));
    let running = tokio::spawn(worker.run_async(async {
        let _ = stop_rx.await;
    }));

    let poll = Duration::from_millis(10);
    let both = async { (ok.wait(poll).await, bad.wait(poll).await) };
    let (ok_state, bad_state) = tokio::time::timeout(Duration::from_secs(5), both)
        .await
        .expect("jobs did not finish in time");
    assert_eq!(ok_state.unwrap(), JobState::Done);
    assert_eq!(bad_state.unwrap(), JobState::Dead);
    assert!(
        !ok.cancel().await.unwrap(),
        "a finished job can't be cancelled"
    );
    stop_tx.send(()).unwrap();
    running.await.unwrap();

    assert_eq!(std::fs::read_to_string(&out).unwrap(), "via redis");
    let dead = bad.job().await.unwrap().unwrap().record().clone();
    assert_eq!(dead.attempts, 2);
    assert_eq!(dead.last_error.as_deref(), Some("nope"));
    let _ = std::fs::remove_file(out);
}
