#![cfg(all(feature = "redis", feature = "tokio"))]

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
async fn redis_always_fails() -> Result<(), String> {
    Err("nope".into())
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
    assert_eq!(queue.state(&ok), Some(JobState::Pending));

    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let worker = Worker::new(queue.clone()).max_retries(1).poll_interval(Duration::from_millis(10));
    let handle = tokio::spawn(worker.run_async(async {
        let _ = stop_rx.await;
    }));

    let q = queue.clone();
    let (ok2, bad2) = (ok.clone(), bad.clone());
    tokio::task::spawn_blocking(move || {
        common::wait_for(&q, &ok2, JobState::Done);
        common::wait_for(&q, &bad2, JobState::Dead);
    })
    .await
    .unwrap();
    stop_tx.send(()).unwrap();
    handle.await.unwrap();

    assert_eq!(std::fs::read_to_string(&out).unwrap(), "via redis");
    let (_, dead) = queue.get(&bad).unwrap().unwrap();
    assert_eq!(dead.attempts, 2);
    assert_eq!(dead.last_error.as_deref(), Some("nope"));
    let _ = std::fs::remove_file(out);
}
