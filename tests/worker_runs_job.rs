//! Enqueue with `.await`, run a real worker on a background thread, and check
//! that the job ran through the queue.

mod common;

use std::{
    sync::{Arc, atomic::AtomicBool, atomic::Ordering},
    thread,
    time::Duration,
};

use butler::{JobState, Worker};

#[butler::job]
async fn write_greeting(path: String, name: String, times: u32) -> std::io::Result<()> {
    std::fs::write(path, format!("hello {name} x{times}"))
}

#[test]
fn awaited_job_is_executed_by_worker() {
    let (queue, dir) = common::temp_queue("roundtrip");
    let out = dir.join("greeting.txt");

    // `.await` only enqueues; the job does not run here.
    let id = butler::block_on(write_greeting(out.display().to_string(), "fabien".into(), 3)).unwrap();
    assert_eq!(queue.state(&id), Some(JobState::Pending));
    assert!(!out.exists(), "job must not run inline");

    let (_, job) = queue.get(&id).unwrap().unwrap();
    assert_eq!(job.name, "write_greeting");
    assert_eq!(job.args[1], "fabien");

    let stop = Arc::new(AtomicBool::new(false));
    let worker = {
        let stop = stop.clone();
        let w = Worker::new(queue.clone()).poll_interval(Duration::from_millis(10));
        thread::spawn(move || w.run_until(stop))
    };

    common::wait_for(&queue, &id, JobState::Done);
    stop.store(true, Ordering::Relaxed);
    worker.join().unwrap();

    assert_eq!(std::fs::read_to_string(&out).unwrap(), "hello fabien x3");
}
