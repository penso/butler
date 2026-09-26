#![allow(clippy::unwrap_used, clippy::expect_used)]

//! Progress survives a real crash: the test binary re-runs itself as a child
//! worker that saves progress at every checkpoint and aborts mid-job. The next
//! worker recovers the job and resumes it from the last saved checkpoint.

mod common;

use std::{
    io::Write,
    path::PathBuf,
    process::{Command, Stdio},
    time::Duration,
};

use butler::{FileQueue, Interrupted, JobState, Progress, Queue, Worker, block_on};

const CRASH_ENV: &str = "BUTLER_TEST_CRASH";
const DIR_ENV: &str = "BUTLER_TEST_QUEUE_DIR";
const CHILD_TTL: Duration = Duration::from_millis(500);

/// Appends each processed item to a log shared by both processes.
fn log_item(log: &PathBuf, item: u32) {
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log)
        .unwrap();
    writeln!(file, "{item}").unwrap();
}

#[butler::job]
async fn count_to(n: u32, log: PathBuf, mut progress: Progress<u32>) -> Result<(), Interrupted> {
    while *progress < n {
        let item = *progress;
        if item == 5 && std::env::var_os(CRASH_ENV).is_some() {
            std::process::abort(); // as close to kill -9 as a test gets
        }
        log_item(&log, item);
        progress.set(item + 1).await?;
    }
    Ok(())
}

#[test]
#[ignore = "child process of progress_is_resumed_after_a_crash"]
fn crashing_worker_child() {
    let Some(dir) = std::env::var_os(DIR_ENV) else {
        return;
    };
    Worker::new(FileQueue::new(dir).unwrap())
        .heartbeat_ttl(CHILD_TTL)
        .checkpoint_interval(Duration::ZERO) // save at every checkpoint
        .drain()
        .unwrap();
    unreachable!("the job should have aborted the process");
}

#[test]
fn progress_is_resumed_after_a_crash() {
    let (queue, dir): (Queue, PathBuf) = common::temp_queue("progress-crash");
    let log = dir.join("items.log");
    let job = block_on(count_to(10, &log)).unwrap();

    let status = Command::new(std::env::current_exe().unwrap())
        .args([
            "crashing_worker_child",
            "--exact",
            "--ignored",
            "--nocapture",
        ])
        .env(CRASH_ENV, "1")
        .env(DIR_ENV, &dir)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .unwrap();
    assert!(!status.success(), "the child worker should have crashed");

    let saved = block_on(job.job()).unwrap().unwrap();
    assert_eq!(saved.state(), JobState::Processing);
    assert_eq!(
        saved.record().progress,
        Some(serde_json::json!(5)),
        "saved before the crash"
    );

    std::thread::sleep(CHILD_TTL + Duration::from_millis(300));
    assert_eq!(Worker::new(queue.clone()).drain().unwrap(), 1);
    assert_eq!(block_on(job.state()).unwrap(), Some(JobState::Done));

    let items: Vec<u32> = std::fs::read_to_string(&log)
        .unwrap()
        .lines()
        .map(|line| line.parse().unwrap())
        .collect();
    assert_eq!(
        items,
        (0..10).collect::<Vec<_>>(),
        "0-4 before the crash, 5-9 after, none twice"
    );
}
