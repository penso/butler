#![cfg(feature = "redis")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

//! Redis storage details that other backends don't share: the fields and keys
//! the scripts keep, and data left by older versions. Needs a Redis server:
//! `$BUTLER_TEST_REDIS_URL`, or `redis://127.0.0.1:6379/`. Skipped when none
//! is reachable.

use std::{
    sync::atomic::{AtomicU32, Ordering},
    time::Duration,
};

use butler::{Queue, RedisQueue};
use redis::Commands;
use serde_json::json;

const NOW: Duration = Duration::ZERO;
const DEFAULT: &[&str] = &["default"];

fn url() -> String {
    std::env::var("BUTLER_TEST_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/".into())
}

/// A queue under a prefix of its own, and a raw connection to inspect it.
fn redis(test: &str) -> Option<(Queue, redis::Connection, String)> {
    static RUN: AtomicU32 = AtomicU32::new(0);
    let prefix = format!(
        "butler-storage-{test}-{}-{}",
        std::process::id(),
        RUN.fetch_add(1, Ordering::Relaxed)
    );
    match RedisQueue::connect(&url(), &prefix) {
        Ok(queue) => {
            let raw = redis::Client::open(url())
                .unwrap()
                .get_connection()
                .unwrap();
            Some((queue.into(), raw, prefix))
        }
        Err(e) => {
            eprintln!("skipping redis: {e}");
            None
        }
    }
}

fn progress(queue: &Queue, id: &str) -> Option<serde_json::Value> {
    queue.get(id).unwrap().unwrap().record().progress.clone()
}

#[test]
fn the_job_hash_names_its_holder_only_while_it_runs() {
    let Some((queue, mut raw, prefix)) = redis("holder") else {
        return;
    };
    let job_key = |id: &str| format!("{prefix}:job:{id}");
    let id = queue.push("a", "default", vec![]).unwrap();
    let held: Option<String> = raw.hget(job_key(&id), "worker").unwrap();
    assert_eq!(held, None);

    let job = queue.claim("w1", DEFAULT, NOW).unwrap().unwrap();
    let held: Option<String> = raw.hget(job_key(&id), "worker").unwrap();
    assert_eq!(held.as_deref(), Some("w1"));
    queue.fail("w1", job, "boom".into(), 1).unwrap();
    let held: Option<String> = raw.hget(job_key(&id), "worker").unwrap();
    assert_eq!(held, None, "a retry no longer names the failed worker");

    let job = queue.claim("w2", DEFAULT, NOW).unwrap().unwrap();
    queue.complete("w2", job, json!(null)).unwrap();
    let held: Option<String> = raw.hget(job_key(&id), "worker").unwrap();
    assert_eq!(held, None);
}

/// A job claimed before the upgrade has no `worker` field: its checkpoints
/// fall back to searching the worker's processing list.
#[test]
fn checkpoints_of_jobs_claimed_by_an_older_version_still_check_the_holder() {
    let Some((queue, mut raw, prefix)) = redis("upgrade") else {
        return;
    };
    let id = queue.push("long", "default", vec![]).unwrap();
    let mut record = queue
        .claim("old", DEFAULT, NOW)
        .unwrap()
        .unwrap()
        .into_record();
    let _: () = raw.hdel(format!("{prefix}:job:{id}"), "worker").unwrap();

    record.progress = Some(json!({ "after": 1 }));
    queue.checkpoint("old", &record).unwrap();
    assert_eq!(progress(&queue, &id), Some(json!({ "after": 1 })));

    let mut stranger = record.clone();
    stranger.progress = Some(json!({ "after": 999 }));
    queue.checkpoint("someone-else", &stranger).unwrap();
    assert_eq!(progress(&queue, &id), Some(json!({ "after": 1 })));

    // Recovering it works as for any other job.
    queue.heartbeat("old", Duration::from_millis(1)).unwrap();
    std::thread::sleep(Duration::from_millis(50));
    assert_eq!(queue.recover().unwrap(), 1);
    let resumed = queue.claim("new", DEFAULT, NOW).unwrap().unwrap();
    assert_eq!(resumed.record().progress, Some(json!({ "after": 1 })));
    queue.checkpoint("old", &stranger).unwrap();
    assert_eq!(progress(&queue, &id), Some(json!({ "after": 1 })));
}
