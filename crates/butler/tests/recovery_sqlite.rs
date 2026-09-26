#![cfg(feature = "sqlite")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

//! A worker that crashes mid-job doesn't lose the job: once its heartbeat
//! expires, another worker requeues and runs it. SQLite backend.

mod common;
#[path = "common/crash.rs"]
mod crash;

use butler::{Queue, SqliteQueue};

const DB_ENV: &str = "BUTLER_TEST_SQLITE_DB";

#[test]
#[ignore = "child process of crash_mid_job_is_recovered"]
fn crashing_worker_child() {
    if let Some(db) = std::env::var_os(DB_ENV) {
        crash::crashing_child(SqliteQueue::open(db).unwrap().into());
    }
}

#[test]
fn crash_mid_job_is_recovered() {
    let dir = std::env::temp_dir().join(format!("butler-recovery-sqlite-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let db = dir.join("queue.db");
    let queue: Queue = SqliteQueue::open(&db).unwrap().into();
    butler::configure(queue.clone());
    crash::crash_mid_job_is_recovered(
        &queue,
        &dir,
        "crashing_worker_child",
        &[(DB_ENV, db.display().to_string())],
    );
}
