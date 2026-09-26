//! A real worker crash, shared by the per-backend recovery tests: the test
//! binary re-runs itself as a child process whose worker aborts mid-job.

use std::{
    path::PathBuf,
    process::{Command, Stdio},
    time::Duration,
};

use butler::{JobState, Queue, Worker, block_on};

const DEFAULT: &[&str] = &["default"];

/// Set in the child process only: makes `fragile` abort instead of running.
const CRASH_ENV: &str = "BUTLER_TEST_CRASH";

/// The child worker's heartbeat. The parent waits a bit longer than this.
const CHILD_TTL: Duration = Duration::from_millis(500);

#[butler::job]
pub async fn fragile(marker: PathBuf) -> std::io::Result<()> {
    if std::env::var_os(CRASH_ENV).is_some() {
        // No unwinding, no cleanup: as close to `kill -9` as a test can get.
        std::process::abort();
    }
    std::fs::write(marker, "ran")
}

/// Child side: claim the job and die inside it.
pub fn crashing_child(queue: Queue) {
    if std::env::var_os(CRASH_ENV).is_none() {
        return; // run directly with --ignored, outside the parent test
    }
    let worker = Worker::new(queue).heartbeat_ttl(CHILD_TTL);
    worker.drain().unwrap();
    unreachable!("the job should have aborted the process");
}

/// Parent side. `queue` must also be the configured global queue, and `env`
/// tells the child how to open the same queue.
pub fn crash_mid_job_is_recovered(
    queue: &Queue,
    dir: &std::path::Path,
    child: &str,
    env: &[(&str, String)],
) {
    // A job held by a live worker: recovery must leave it alone.
    let alive = format!("alive-{}", std::process::id());
    queue.heartbeat(&alive, Duration::from_secs(60)).unwrap();
    let held = block_on(fragile(dir.join("held"))).unwrap();
    let claimed = queue
        .claim(&alive, DEFAULT, Duration::ZERO)
        .unwrap()
        .unwrap();
    assert_eq!(claimed.id(), held.id());

    let marker = dir.join("marker");
    let job = block_on(fragile(&marker)).unwrap();

    let status = Command::new(std::env::current_exe().unwrap())
        .args([child, "--exact", "--ignored", "--nocapture"])
        .env(CRASH_ENV, "1")
        .envs(env.iter().map(|(k, v)| (k, v)))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .unwrap();
    assert!(!status.success(), "the child worker should have crashed");

    // The crash left the job claimed and unfinished.
    assert_eq!(block_on(job.state()).unwrap(), Some(JobState::Processing));
    assert!(!marker.exists());

    std::thread::sleep(CHILD_TTL + Duration::from_millis(300));

    // A new worker recovers the job when it starts, then runs it.
    let worker = Worker::new(queue.clone());
    assert_eq!(worker.drain().unwrap(), 1);
    assert_eq!(block_on(job.state()).unwrap(), Some(JobState::Done));
    assert_eq!(std::fs::read_to_string(&marker).unwrap(), "ran");

    // The live worker's job was not touched, and a second pass finds nothing.
    assert_eq!(block_on(held.state()).unwrap(), Some(JobState::Processing));
    assert_eq!(queue.recover().unwrap(), 0);

    queue
        .complete(&alive, claimed, serde_json::Value::Null)
        .unwrap();
    queue.retire(&alive).unwrap();
}
