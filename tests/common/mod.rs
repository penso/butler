// Each test binary uses a different subset of these helpers.
#![allow(dead_code)]

use std::{
    path::PathBuf,
    time::{Duration, Instant},
};

use butler::{FileQueue, JobState, Queue};

pub fn temp_queue(name: &str) -> (Queue, PathBuf) {
    let dir = std::env::temp_dir().join(format!("butler-test-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let queue: Queue = FileQueue::new(&dir).unwrap().into();
    butler::configure(queue.clone());
    (queue, dir)
}

pub fn wait_for(queue: &Queue, id: &str, state: JobState) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while queue.state(id) != Some(state) {
        assert!(Instant::now() < deadline, "job {id} never reached {state:?}, now {:?}", queue.state(id));
        std::thread::sleep(Duration::from_millis(10));
    }
}
