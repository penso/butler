//! Failed and panicking jobs are retried and end up in `dead/`.

mod common;

use std::sync::atomic::{AtomicU32, Ordering};

use butler::{JobState, Worker};

static FLAKY_CALLS: AtomicU32 = AtomicU32::new(0);

#[butler::job]
async fn flaky(succeed_on_attempt: u32) -> Result<(), String> {
    let n = FLAKY_CALLS.fetch_add(1, Ordering::SeqCst) + 1;
    if n < succeed_on_attempt { Err(format!("attempt {n} failed")) } else { Ok(()) }
}

#[butler::job(name = "always_panics")]
async fn boom() {
    panic!("kaboom");
}

#[test]
fn retries_then_succeeds_or_dies() {
    let (queue, _dir) = common::temp_queue("retries");
    let worker = Worker::new(queue.clone()).max_retries(2);
    assert_eq!(worker.job_names(), ["always_panics", "flaky"]);

    let ok = butler::block_on(flaky(3)).unwrap();
    let dead = butler::block_on(boom()).unwrap();

    // flaky: 3 runs. boom: 1 run + 2 retries.
    assert_eq!(worker.drain().unwrap(), 6);

    let (state, job) = queue.get(&ok).unwrap().unwrap();
    assert_eq!(state, JobState::Done);
    assert_eq!(job.attempts, 2);

    let (state, job) = queue.get(&dead).unwrap().unwrap();
    assert_eq!(state, JobState::Dead);
    assert_eq!(job.attempts, 3);
    assert_eq!(job.last_error.as_deref(), Some("job panicked: kaboom"));
}
