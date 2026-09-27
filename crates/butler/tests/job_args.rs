#![allow(clippy::unwrap_used, clippy::expect_used)]

//! What callers may pass to a `#[job]` function, and what the returned call
//! holds on to. Its own test binary: `butler::configure` is process-global.

mod common;

use std::path::PathBuf;

#[butler::job]
async fn record(path: PathBuf, name: String, times: u32) -> std::io::Result<()> {
    std::fs::write(path, format!("{name} x{times}"))
}

/// The call, and the futures it becomes, both enqueueing and running now.
fn assert_send_static<C>(call: C) -> C
where
    C: IntoFuture<IntoFuture: Send + 'static> + Send + 'static,
{
    call
}

fn assert_future_send_static<F: Future + Send + 'static>(fut: F) -> F {
    fut
}

#[test]
fn enqueue_future_does_not_borrow_its_arguments() {
    let (queue, dir) = common::temp_queue("borrowed-args");
    let path = dir.join("unused.txt");
    let name = String::from("borrowed");

    // The arguments are converted and serialized during the call, so the
    // call is 'static and can outlive them, e.g. inside `tokio::spawn`.
    let enqueue = assert_send_static(record(&path, &name, 1));
    let now = assert_future_send_static(record(dir.join("now.txt"), &name, 2).now());
    let spawnable = assert_future_send_static(record(&path, &name, 3).enqueue());
    drop((path, name, spawnable));

    butler::block_on(now).unwrap();
    assert_eq!(
        std::fs::read_to_string(dir.join("now.txt")).unwrap(),
        "borrowed x2"
    );

    let job = butler::block_on(enqueue).unwrap();
    let (_, job) = queue.get(job.id()).unwrap().unwrap().into_parts();
    assert_eq!(job.args[1], "borrowed");
    assert_eq!(job.args[2], 1);
}
