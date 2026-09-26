#![cfg(all(feature = "sqlite", feature = "tokio"))]
#![allow(clippy::unwrap_used, clippy::expect_used)]

//! SQLite has no pub/sub between connections, so two `SqliteQueue`s opened on
//! the same file stand in for two processes: neither sees the other's in-process
//! signals, only the `data_version` watcher can wake them.

use std::{
    thread,
    time::{Duration, Instant},
};

use butler::{JobState, Queue, SQLITE_WATCH_TICK, SqliteQueue};

fn two_connections(name: &str) -> (Queue, Queue) {
    let db = std::env::temp_dir().join(format!("butler-sqlite-{name}-{}.db", std::process::id()));
    let _ = std::fs::remove_file(&db);
    (
        SqliteQueue::open(&db).unwrap().into(),
        SqliteQueue::open(&db).unwrap().into(),
    )
}

/// Generous for a loaded CI machine; typically a few milliseconds.
const PROMPT: Duration = Duration::from_millis(500);

#[test]
fn a_push_from_another_connection_wakes_a_waiting_claim() {
    let (worker_side, enqueuer_side) = two_connections("wake");
    let mut latencies = Vec::new();
    for _ in 0..20 {
        let waiter = {
            let queue = worker_side.clone();
            thread::spawn(move || {
                let job = queue
                    .claim("w", &["default"], Duration::from_secs(30))
                    .unwrap();
                (Instant::now(), job)
            })
        };
        thread::sleep(Duration::from_millis(20)); // let the claim start waiting
        let pushed_at = Instant::now();
        let id = enqueuer_side.push("x", "default", vec![]).unwrap();
        let (woke_at, job) = waiter.join().unwrap();
        assert_eq!(job.unwrap().id(), id);
        latencies.push(woke_at.duration_since(pushed_at));
    }
    latencies.sort();
    let (median, worst) = (
        latencies[latencies.len() / 2],
        latencies[latencies.len() - 1],
    );
    println!(
        "sqlite cross-connection wake: median {median:?}, worst {worst:?} (tick {SQLITE_WATCH_TICK:?})"
    );
    assert!(worst < PROMPT, "worst wake-up {worst:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_result_finished_by_another_connection_wakes_its_waiter() {
    let (caller_side, worker_side) = two_connections("result");
    let id = caller_side.push("x", "default", vec![]).unwrap();
    let handle = caller_side.handle(&id).with_output::<u32>();

    let started = Instant::now();
    let finisher = thread::spawn(move || {
        thread::sleep(Duration::from_millis(50));
        let job = worker_side
            .claim("w", &["default"], Duration::ZERO)
            .unwrap()
            .unwrap();
        worker_side
            .complete("w", job, serde_json::json!(7))
            .unwrap();
    });
    // A 30s fallback: the result has to come from a wake-up, not a re-check.
    let value = tokio::time::timeout(
        Duration::from_secs(5),
        handle.wait_result(Duration::from_secs(30)),
    )
    .await
    .expect("no wake-up")
    .unwrap();
    finisher.join().unwrap();
    assert_eq!(value, 7);
    assert!(started.elapsed() < Duration::from_millis(50) + PROMPT);
    assert_eq!(caller_side.state(&id), Some(JobState::Done));
}
