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

#[test]
fn interrupted_retry_transitions_keep_one_recoverable_copy() {
    use butler::JobState;
    use std::{
        fs,
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    // Stop after each persistence step of fail(Scheduled), including the
    // initial rename before the new retry record has been committed.
    for step in 0..3 {
        for cancel in [false, true] {
            let dir = std::env::temp_dir().join(format!(
                "butler-retry-transition-{}-{step}-{cancel}",
                std::process::id()
            ));
            let _ = fs::remove_dir_all(&dir);
            let queue: butler::Queue = FileQueue::new(&dir).unwrap().into();
            let id = queue.push("retry", "mailers", vec![]).unwrap();
            let job = queue
                .claim("crashed", &["mailers"], Duration::ZERO)
                .unwrap()
                .unwrap();
            let mut record = job.into_record();
            let at = SystemTime::now() + Duration::from_secs(3600);
            let at_ms = u64::try_from(at.duration_since(UNIX_EPOCH).unwrap().as_millis()).unwrap();
            let held = dir.join("processing/crashed").join(format!("{id}.json"));
            let retry = held.with_file_name(format!("{id}.retry.json"));
            fs::rename(&held, &retry).unwrap();
            if step >= 1 {
                record.attempts = 1;
                record.last_error = Some("retry me".into());
                record.run_at_ms = Some(at_ms);
                fs::write(&retry, serde_json::to_vec(&record).unwrap()).unwrap();
            }
            if step == 2 {
                fs::rename(
                    &retry,
                    dir.join("scheduled").join(format!("{at_ms:020}_{id}.json")),
                )
                .unwrap();
            } else {
                assert_eq!(queue.state(&id), Some(JobState::Processing));
            }

            assert_eq!(queue.recover().unwrap(), usize::from(step < 2));
            assert_eq!(queue.recover().unwrap(), 0);
            assert!(!held.exists());
            assert!(!retry.exists());
            assert_eq!(
                queue.state(&id),
                Some(if step == 0 {
                    JobState::Pending
                } else {
                    JobState::Scheduled
                })
            );
            if step >= 1 {
                assert!(
                    queue
                        .claim("new", &["mailers"], Duration::ZERO)
                        .unwrap()
                        .is_none()
                );
                assert_eq!(queue.get(&id).unwrap().unwrap().record().attempts, 1);
            }
            if cancel {
                assert!(queue.cancel(&id).unwrap());
                assert_eq!(queue.promote(at).unwrap().moved, 0);
            } else {
                assert_eq!(queue.promote(at).unwrap().moved, usize::from(step >= 1));
                let claimed = queue
                    .claim("new", &["mailers"], Duration::ZERO)
                    .unwrap()
                    .unwrap();
                assert_eq!(claimed.id(), id);
                queue
                    .complete("new", claimed, serde_json::Value::Null)
                    .unwrap();
            }
            assert!(
                queue
                    .claim("new", &["mailers"], Duration::ZERO)
                    .unwrap()
                    .is_none()
            );
            assert_eq!(queue.promote(at).unwrap().moved, 0);
            fs::remove_dir_all(dir).unwrap();
        }
    }
}
