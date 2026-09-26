#![cfg(feature = "tokio")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

//! Bulk enqueuing, like ActiveJob's `perform_all_later`: `prepare` builds jobs
//! without enqueueing them, `enqueue_all` pushes them in one go. One test per
//! global queue use: `butler::configure` is process-global.

use std::time::Duration;

use butler::{
    Error, JobHandle, JobState, MemoryQueue, PreparedJob, Queue, QueuePriority, Worker,
    enqueue_all, testing::InlineJobs,
};

#[butler::job]
async fn welcome(email: String) -> Result<String, std::io::Error> {
    Ok(format!("welcomed {email}"))
}

#[butler::job(queue = "reports")]
async fn report(month: u32) -> Result<u32, std::io::Error> {
    Ok(month * 100)
}

const WAIT: Duration = Duration::from_secs(5);

#[tokio::test(flavor = "multi_thread")]
async fn enqueue_all_then_run_them() {
    let queue: Queue = MemoryQueue::new().into();
    butler::configure(queue.clone());

    // Typed: every job is `welcome`, so the handles are `JobHandle<String>`.
    let users = ["ada@example.com", "grace@example.com", "linus@example.com"];
    let emails = users
        .iter()
        .map(|email| welcome::prepare(*email))
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(emails[1].args(), ["grace@example.com"]);
    let handles: Vec<JobHandle<String>> = enqueue_all(emails).await.unwrap();
    assert_eq!(handles.len(), 3);
    assert!(
        handles
            .iter()
            .all(|h| queue.state(h.id()) == Some(JobState::Pending))
    );

    // Mixed kinds, like perform_all_later with several job classes.
    let mixed: Vec<PreparedJob> = vec![
        welcome::prepare("mixed@example.com").unwrap().untyped(),
        report::prepare(3).unwrap().untyped(),
        // Same job, another queue: like `MyJob.set(queue: :low)`.
        report::prepare(4)
            .unwrap()
            .on_queue("urgent")
            .unwrap()
            .untyped(),
    ];
    let mixed = enqueue_all(mixed).await.unwrap();
    assert!(
        enqueue_all(Vec::<PreparedJob>::new())
            .await
            .unwrap()
            .is_empty()
    );
    assert!(matches!(
        report::prepare(1).unwrap().on_queue("../x"),
        Err(Error::InvalidQueue { .. })
    ));

    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let worker =
        Worker::new(queue.clone()).queues(QueuePriority::strict(["default", "reports", "urgent"]));
    let running = tokio::spawn(worker.run_async(async {
        let _ = stopped.await;
    }));

    for (handle, email) in handles.iter().zip(users) {
        assert_eq!(
            handle.wait_result(WAIT).await.unwrap(),
            format!("welcomed {email}")
        );
    }
    let welcomed = mixed[0]
        .clone()
        .with_output::<String>()
        .wait_result(WAIT)
        .await
        .unwrap();
    assert_eq!(welcomed, "welcomed mixed@example.com");
    assert_eq!(
        mixed[1]
            .clone()
            .with_output::<u32>()
            .wait_result(WAIT)
            .await
            .unwrap(),
        300
    );
    let urgent = mixed[2].clone().with_output::<u32>();
    assert_eq!(urgent.wait_result(WAIT).await.unwrap(), 400);
    assert_eq!(
        urgent.job().await.unwrap().unwrap().record().queue,
        "urgent"
    );

    stop.send(()).unwrap();
    running.await.unwrap();
}

#[tokio::test]
async fn inside_perform_enqueued_jobs_a_batch_runs_inline_in_order() {
    let jobs = InlineJobs::new();
    let results = jobs
        .perform(async {
            let batch = vec![
                welcome::prepare("a@x").unwrap(),
                welcome::prepare("b@x").unwrap(),
            ];
            let handles = enqueue_all(batch).await.unwrap();
            let mut results = Vec::new();
            for handle in handles {
                results.push(handle.result().await.unwrap().unwrap());
            }
            results
        })
        .await;
    assert_eq!(results, ["welcomed a@x", "welcomed b@x"]);
    assert_eq!(jobs.performed_names(), ["welcome", "welcome"]);
}
