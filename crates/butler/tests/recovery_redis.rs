#![cfg(feature = "redis")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

//! A worker that crashes mid-job doesn't lose the job: once its heartbeat
//! expires, another worker requeues and runs it. Redis backend; skipped when
//! no server is reachable (see redis_backend.rs).

mod common;
#[path = "common/crash.rs"]
mod crash;

use butler::{Queue, RedisQueue};

const URL_ENV: &str = "BUTLER_TEST_REDIS_URL";
const PREFIX_ENV: &str = "BUTLER_TEST_REDIS_PREFIX";

fn url() -> String {
    std::env::var(URL_ENV).unwrap_or_else(|_| "redis://127.0.0.1:6379/".into())
}

#[test]
#[ignore = "child process of crash_mid_job_is_recovered"]
fn crashing_worker_child() {
    if let Ok(prefix) = std::env::var(PREFIX_ENV) {
        crash::crashing_child(RedisQueue::connect(&url(), &prefix).unwrap().into());
    }
}

#[test]
fn crash_mid_job_is_recovered() {
    let prefix = format!("butler-test-recovery-{}", std::process::id());
    let queue: Queue = match RedisQueue::connect(&url(), &prefix) {
        Ok(q) => q.into(),
        Err(e) => {
            eprintln!("skipping: no redis at {}: {e}", url());
            return;
        }
    };
    butler::configure(queue.clone());
    let dir = std::env::temp_dir().join(&prefix);
    std::fs::create_dir_all(&dir).unwrap();
    crash::crash_mid_job_is_recovered(
        &queue,
        &dir,
        "crashing_worker_child",
        &[(URL_ENV, url()), (PREFIX_ENV, prefix.clone())],
    );
    let _ = std::fs::remove_dir_all(dir);
}
