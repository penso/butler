#![allow(clippy::unwrap_used, clippy::expect_used)]

//! A worker that crashes mid-job doesn't lose the job: once its heartbeat
//! expires, another worker requeues and runs it. File backend.

mod common;
#[path = "common/crash.rs"]
mod crash;

use butler::FileQueue;

const DIR_ENV: &str = "BUTLER_TEST_QUEUE_DIR";

#[test]
#[ignore = "child process of crash_mid_job_is_recovered"]
fn crashing_worker_child() {
    if let Some(dir) = std::env::var_os(DIR_ENV) {
        crash::crashing_child(FileQueue::new(dir).unwrap().into());
    }
}

#[test]
fn crash_mid_job_is_recovered() {
    let (queue, dir) = common::temp_queue("recovery");
    crash::crash_mid_job_is_recovered(
        &queue,
        &dir,
        "crashing_worker_child",
        &[(DIR_ENV, dir.display().to_string())],
    );
}
