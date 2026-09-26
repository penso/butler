#![allow(clippy::unwrap_used, clippy::expect_used)]

//! What callers may pass to a `#[job]` function, and what the returned future
//! holds on to. Its own test binary: `butler::configure` is process-global.

mod common;

use std::path::PathBuf;

#[butler::job]
async fn record(path: PathBuf, name: String, times: u32) -> std::io::Result<()> {
    std::fs::write(path, format!("{name} x{times}"))
}

fn assert_send_static<F: Future + Send + 'static>(fut: F) -> F {
    fut
}

#[test]
fn enqueue_future_does_not_borrow_its_arguments() {
    let (queue, dir) = common::temp_queue("borrowed-args");
    let path = dir.join("unused.txt");
    let name = String::from("borrowed");

    // The arguments are converted and serialized during the call, so the
    // future is 'static and can outlive them, e.g. inside `tokio::spawn`.
    let enqueue = assert_send_static(record(&path, &name, 1));
    drop((path, name));

    let id = butler::block_on(enqueue).unwrap();
    let (_, job) = queue.get(&id).unwrap().unwrap();
    assert_eq!(job.args[1], "borrowed");
    assert_eq!(job.args[2], 1);
}
