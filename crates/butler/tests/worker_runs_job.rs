#![allow(clippy::unwrap_used, clippy::expect_used)]

//! Enqueue with `.await`, run a real worker on a background thread, and check
//! that the job ran through the queue.

mod common;

use std::{
    path::PathBuf,
    sync::{Arc, atomic::AtomicBool, atomic::Ordering},
    thread,
    time::Duration,
};

use butler::{JobState, Worker};

#[butler::job]
async fn write_greeting(path: PathBuf, name: String, times: u32) -> std::io::Result<()> {
    std::fs::write(path, format!("hello {name} x{times}"))
}

#[test]
fn awaited_job_is_executed_by_worker() {
    let (queue, dir) = common::temp_queue("roundtrip");
    let out = dir.join("greeting.txt");

    // Borrowed and literal arguments: `&PathBuf` for `PathBuf`, `&str` for
    // `String`, and a bare `3` for `u32`.
    let enqueue = write_greeting(&out, "fabien", 3);
    // `.await` only enqueues; the job does not run here.
    let job = butler::block_on(enqueue).unwrap();
    assert_eq!(queue.state(job.id()), Some(JobState::Pending));
    assert!(!out.exists(), "job must not run inline");

    let (_, record) = queue.get(job.id()).unwrap().unwrap();
    assert_eq!(record.name, "write_greeting");
    assert_eq!(record.args[1], "fabien");

    let stop = Arc::new(AtomicBool::new(false));
    let worker = {
        let stop = stop.clone();
        let w = Worker::new(queue.clone()).poll_interval(Duration::from_millis(10));
        thread::spawn(move || w.run_until(stop))
    };

    common::wait_for(&queue, job.id(), JobState::Done);
    stop.store(true, Ordering::Relaxed);
    worker.join().unwrap();

    assert_eq!(std::fs::read_to_string(&out).unwrap(), "hello fabien x3");
}
