#![allow(clippy::unwrap_used, clippy::expect_used)]

//! Two `#[job]`s in one binary answering to the same name: a worker refuses
//! to start rather than guess which one a queued job means. In its own test
//! binary, since every worker in it sees both jobs.

use butler::{MemoryQueue, Worker};

#[butler::job(name = "invoice.send")]
async fn send_invoice(id: u64) {
    let _ = id;
}

/// Its old name is the other job's current one.
#[butler::job(aliases = ["invoice.send"])]
async fn email_invoice(id: u64) {
    let _ = id;
}

#[test]
#[should_panic(expected = "both answer to `invoice.send`")]
fn a_worker_refuses_two_jobs_sharing_a_name() {
    let _ = Worker::new(MemoryQueue::new());
}
