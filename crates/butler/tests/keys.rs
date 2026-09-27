#![cfg(feature = "tokio")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

//! `#[job(concurrency_key = ..., limit = ...)]` and `#[job(unique = ...)]`
//! through real jobs: the keys the macro computes, and a worker honoring
//! them. What each backend guarantees is in `backends.rs`. One test: it uses
//! `butler::configure`, which is process-global.

use std::{
    collections::HashMap,
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use butler::{JobState, MemoryQueue, Queue, Unique};

/// Per account, jobs running right now and the most at once.
static RUNNING: Mutex<Option<HashMap<u64, (usize, usize)>>> = Mutex::new(None);
static PEAK_ALL: AtomicUsize = AtomicUsize::new(0);
static NOW_ALL: AtomicUsize = AtomicUsize::new(0);

#[butler::job(concurrency_key = "account_id", limit = 1)]
async fn sync_account(account_id: u64, run: u32) {
    let _ = run;
    {
        let mut running = RUNNING.lock().unwrap();
        let entry = running
            .get_or_insert_with(HashMap::new)
            .entry(account_id)
            .or_default();
        entry.0 += 1;
        entry.1 = entry.1.max(entry.0);
    }
    let now = NOW_ALL.fetch_add(1, Ordering::SeqCst) + 1;
    PEAK_ALL.fetch_max(now, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(40)).await;
    NOW_ALL.fetch_sub(1, Ordering::SeqCst);
    let mut running = RUNNING.lock().unwrap();
    if let Some(entry) = running.as_mut().and_then(|all| all.get_mut(&account_id)) {
        entry.0 -= 1;
    }
}

/// Keyed on two arguments, in their own order.
#[butler::job(concurrency_key = "region, account_id", limit = 2)]
fn export(account_id: u64, region: String, full: bool) {
    let _ = (account_id, region, full);
}

#[butler::job(unique = "until_started")]
async fn refresh_feed(user_id: u64) {
    let _ = user_id;
}

#[butler::job(unique = "until_finished", queue = "low")]
async fn rebuild_index(name: String) {
    let _ = name;
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrency_keys_and_unique_jobs_through_the_macro() {
    let queue: Queue = MemoryQueue::new().into();
    butler::configure(queue.clone());

    // What the macro puts in each job's definition.
    let limit = export::JOB.concurrency.unwrap();
    assert_eq!((limit.args, limit.limit), (&[1_usize, 0][..], 2));
    assert_eq!(refresh_feed::JOB.unique, Some(Unique::UntilStarted));
    assert_eq!(rebuild_index::JOB.unique, Some(Unique::UntilFinished));
    assert!(sync_account::JOB.unique.is_none());

    // The keys stored with an enqueued job.
    let job = export(7, "eu", true).await.unwrap();
    let record = queue.get(job.id()).unwrap().unwrap().record().clone();
    let key = record.concurrency.unwrap();
    assert_eq!((key.key.as_str(), key.limit), (r#"export:["eu",7]"#, 2));
    queue.cancel(job.id()).unwrap();

    // Unique: enqueueing it again returns the job already waiting, through
    // every enqueue path.
    let first = refresh_feed(1).await.unwrap();
    assert_eq!(refresh_feed(1).await.unwrap().id(), first.id());
    let prepared = refresh_feed::prepare(1).unwrap().enqueue().await.unwrap();
    assert_eq!(prepared.id(), first.id());
    let batch = butler::enqueue_all([
        refresh_feed::prepare(1).unwrap(),
        refresh_feed::prepare(2).unwrap(),
        refresh_feed::prepare(2).unwrap(),
    ])
    .await
    .unwrap();
    assert_eq!(batch[0].id(), first.id());
    assert_ne!(batch[1].id(), first.id());
    assert_eq!(batch[1].id(), batch[2].id());
    let index = rebuild_index("users").await.unwrap();
    assert_eq!(rebuild_index("users").await.unwrap().id(), index.id());

    // Per-key concurrency: two accounts, three syncs each. Each account runs
    // one at a time, while the two accounts run side by side.
    let mut syncs = Vec::new();
    for run in 0..3 {
        for account in [1_u64, 2] {
            syncs.push(sync_account(account, run).await.unwrap());
        }
    }
    let worker = butler::Worker::new(queue.clone())
        .concurrency(10)
        .poll_interval(Duration::from_millis(5))
        .queues(butler::QueuePriority::strict(["default", "low"]));
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let running = tokio::spawn(worker.run_async(async {
        let _ = stopped.await;
    }));
    for sync in &syncs {
        tokio::time::timeout(
            Duration::from_secs(10),
            sync.wait_result(Duration::from_millis(5)),
        )
        .await
        .expect("every sync ran")
        .unwrap();
    }
    // Started, so identical feeds can be enqueued again; the index keeps its
    // key until it is done.
    tokio::time::timeout(
        Duration::from_secs(10),
        index.wait_result(Duration::from_millis(5)),
    )
    .await
    .expect("the index ran")
    .unwrap();
    assert_ne!(refresh_feed(1).await.unwrap().id(), first.id());
    assert_ne!(rebuild_index("users").await.unwrap().id(), index.id());
    stop.send(()).unwrap();
    running.await.unwrap();

    let running = RUNNING.lock().unwrap();
    for (account, (_, peak)) in running.as_ref().unwrap() {
        assert_eq!(*peak, 1, "account {account} ran one sync at a time");
    }
    assert!(
        PEAK_ALL.load(Ordering::SeqCst) >= 2,
        "accounts ran side by side"
    );
    assert_eq!(queue.state(first.id()), Some(JobState::Done));
}
