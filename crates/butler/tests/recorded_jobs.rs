#![allow(clippy::unwrap_used, clippy::expect_used)]

//! `butler::testing::RecordedJobs`: jobs enqueued inside `record` are
//! recorded, not run and not stored, like ActiveJob's `assert_enqueued_with`.

use std::{
    sync::{
        Once,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, SystemTime},
};

use butler::{
    Error, JobError, JobHandle, JobState, MemoryQueue, NewJob, Queue, Unique, enqueue_all,
    testing::{InlineJobs, RecordedJobs},
};
use serde_json::json;

/// Set if any job body runs: recording must never run one.
static RAN: AtomicBool = AtomicBool::new(false);

#[butler::job(queue = "mailers")]
async fn send_email(to: String, subject: String) -> Result<(), std::io::Error> {
    RAN.store(true, Ordering::SeqCst);
    let _ = (to, subject);
    Ok(())
}

/// A plain `fn` job.
#[butler::job]
fn crunch(n: u32) -> Result<u32, std::convert::Infallible> {
    RAN.store(true, Ordering::SeqCst);
    Ok(n * 2)
}

#[butler::job(concurrency_key = "account_id", limit = 2, unique = "until_started")]
async fn sync_account(account_id: u64, full: bool) {
    RAN.store(true, Ordering::SeqCst);
    let _ = (account_id, full);
}

/// Enqueues another job from its body.
#[butler::job]
async fn signup(email: String) -> Result<(), Error> {
    send_email(email, "Welcome").await?;
    Ok(())
}

/// Only ever run inline, so it may run.
#[butler::job]
async fn inline_only(n: u32) -> Result<u32, std::io::Error> {
    Ok(n + 1)
}

/// Enqueues another job from its body; only ever run inline.
#[butler::job]
async fn inline_signup(n: u32) -> Result<(), Error> {
    inline_only(n).await?;
    Ok(())
}

/// Changed and vetoed by the enqueue layer below.
#[butler::job]
async fn layered(n: u32) {
    RAN.store(true, Ordering::SeqCst);
    let _ = n;
}

/// Enqueue layers are process-wide: this one only touches `layered` jobs,
/// so the other tests here don't see it.
fn install_layer() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        butler::configure_enqueue(|job: &mut NewJob| {
            if job.name != "layered" {
                return Ok(());
            }
            if job.args == [json!(0)] {
                return Err(std::io::Error::other("layered(0) is not allowed"));
            }
            job.queue = "rerouted".to_owned();
            job.meta.insert("tenant".to_owned(), json!("acme"));
            Ok(())
        });
    });
}

fn assert_nothing_ran() {
    assert!(!RAN.load(Ordering::SeqCst), "recording ran a job body");
}

#[tokio::test]
async fn it_records_an_awaited_call_without_running_it() {
    let jobs = RecordedJobs::new();
    let handle: JobHandle<()> = jobs
        .record(async { send_email("ada@example.com", "Hi").await.unwrap() })
        .await;

    let [email] = jobs.enqueued().try_into().unwrap();
    assert_eq!(email.id, handle.id());
    assert!(email.is(&send_email::JOB));
    assert_eq!(email.job.name, "send_email");
    assert_eq!(email.job.queue, "mailers");
    assert_eq!(email.job.args, [json!("ada@example.com"), json!("Hi")]);
    assert_eq!(email.job.run_at, None);
    assert_eq!(email.arg::<String>(0).unwrap(), "ada@example.com");
    assert!(matches!(
        email.arg::<String>(2),
        Err(JobError::MissingArgument { index: 2, .. })
    ));
    assert!(matches!(
        email.arg::<u64>(1),
        Err(JobError::BadArgument { index: 1, .. })
    ));

    // A real handle that stays pending: nothing runs it.
    assert_eq!(handle.state().await.unwrap(), Some(JobState::Pending));
    assert_eq!(handle.result().await.unwrap(), None);
    assert!(handle.cancel().await.unwrap());
    assert_eq!(handle.state().await.unwrap(), Some(JobState::Cancelled));
    assert_eq!(jobs.enqueued_names(), ["send_email"], "still recorded");
    assert_nothing_ran();
}

#[tokio::test]
async fn it_records_every_enqueue_path() {
    let jobs = RecordedJobs::new();
    let at = SystemTime::now() + Duration::from_secs(3600);
    let (scheduled, batch) = jobs
        .record(async {
            crunch(1).enqueue().await.unwrap();
            let scheduled = send_email::prepare("b@example.com", "Later")
                .unwrap()
                .on_queue("low")
                .unwrap()
                .run_at(at)
                .enqueue()
                .await
                .unwrap();
            let batch = enqueue_all([
                crunch::prepare(2u32).unwrap(),
                crunch::prepare(3u32)
                    .unwrap()
                    .run_in(Duration::from_secs(60)),
            ])
            .await
            .unwrap();
            (scheduled, batch)
        })
        .await;

    assert_eq!(
        jobs.enqueued_names(),
        ["crunch", "send_email", "crunch", "crunch"]
    );
    let later = jobs.assert_enqueued_with(send_email("b@example.com", "Later"));
    assert_eq!(later.job.queue, "low");
    assert_eq!(later.job.run_at, Some(at));
    assert_eq!(scheduled.state().await.unwrap(), Some(JobState::Scheduled));

    let crunches = jobs.enqueued_of(&crunch::JOB);
    let args: Vec<u32> = crunches.iter().map(|job| job.arg(0).unwrap()).collect();
    assert_eq!(args, [1, 2, 3]);
    assert_eq!(crunches[1].id, batch[0].id());
    assert!(crunches[2].job.run_at.is_some());
    assert_eq!(batch[1].state().await.unwrap(), Some(JobState::Scheduled));
    assert_nothing_ran();
}

#[tokio::test]
async fn it_records_keys_without_enforcing_them() {
    let jobs = RecordedJobs::new();
    let (a, b) = jobs
        .record(async {
            let a = sync_account(7u64, true).await.unwrap();
            let b = sync_account(7u64, true).await.unwrap();
            (a, b)
        })
        .await;

    assert_ne!(a.id(), b.id(), "a duplicate unique job is recorded again");
    let [first, second] = jobs.enqueued_of(&sync_account::JOB).try_into().unwrap();
    let concurrency = first.job.concurrency.clone().unwrap();
    assert_eq!(concurrency.key, "sync_account:[7]");
    assert_eq!(concurrency.limit, 2);
    let unique = first.job.unique.clone().unwrap();
    assert_eq!(unique.until, Unique::UntilStarted);
    assert_eq!(second.job.unique, Some(unique));
    assert_nothing_ran();
}

#[tokio::test]
async fn assertions_find_matching_jobs() {
    let jobs = RecordedJobs::new();
    jobs.assert_no_enqueued_jobs();
    jobs.record(async {
        send_email("a@example.com", "One").await.unwrap();
        send_email("b@example.com", "Two").await.unwrap();
    })
    .await;

    let two = jobs.assert_enqueued(&send_email::JOB, |job| {
        job.arg::<String>(1).is_ok_and(|subject| subject == "Two")
    });
    assert_eq!(two.arg::<String>(0).unwrap(), "b@example.com");
    jobs.assert_enqueued_with(send_email("a@example.com", "One"));

    jobs.clear();
    jobs.assert_no_enqueued_jobs();
    assert!(jobs.enqueued().is_empty());
}

#[tokio::test]
#[should_panic(expected = "expected send_email(\"a@example.com\", \"Other\") to be enqueued")]
async fn assert_enqueued_with_panics_on_different_arguments() {
    let jobs = RecordedJobs::new();
    jobs.record(async { send_email("a@example.com", "One").await.unwrap() })
        .await;
    jobs.assert_enqueued_with(send_email("a@example.com", "Other"));
}

#[tokio::test]
#[should_panic(expected = "expected a matching crunch job to be enqueued")]
async fn assert_enqueued_panics_on_another_kind() {
    let jobs = RecordedJobs::new();
    jobs.record(async { send_email("a@example.com", "One").await.unwrap() })
        .await;
    jobs.assert_enqueued(&crunch::JOB, |_| true);
}

#[tokio::test]
#[should_panic(expected = "expected no enqueued jobs; recorded: \n  crunch(4) on \"default\"")]
async fn assert_no_enqueued_jobs_lists_what_was_recorded() {
    let jobs = RecordedJobs::new();
    jobs.record(async { crunch(4).await.unwrap() }).await;
    jobs.assert_no_enqueued_jobs();
}

/// The scope follows the future across `.await`s, even when a multi-thread
/// runtime moves it between threads.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn it_follows_the_future_across_threads() {
    let jobs = RecordedJobs::new();
    let spawned_jobs = jobs.clone();
    tokio::spawn(async move {
        spawned_jobs
            .record(async {
                for n in 0..20u32 {
                    tokio::task::yield_now().await;
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    crunch(n).await.unwrap();
                }
            })
            .await;
    })
    .await
    .unwrap();

    assert_eq!(jobs.enqueued_of(&crunch::JOB).len(), 20);
    assert_nothing_ran();
}

/// No runtime needed: a plain `#[test]` under `butler::block_on`.
#[test]
fn it_records_without_a_runtime() {
    let jobs = RecordedJobs::new();
    let handle = butler::block_on(jobs.record(async { crunch(5).await.unwrap() }));

    jobs.assert_enqueued_with(crunch(5u32));
    assert_eq!(
        butler::block_on(handle.state()).unwrap(),
        Some(JobState::Pending)
    );
    assert_nothing_ran();
}

#[tokio::test]
async fn recording_inside_an_inline_scope_records() {
    let inline = InlineJobs::new();
    let recorded = RecordedJobs::new();
    let ran = inline
        .perform(async {
            recorded
                .record(async { send_email("a@example.com", "Hi").await.unwrap() })
                .await;
            // The inline scope applies again once the recording ends.
            inline_only(1).await.unwrap().result().await.unwrap()
        })
        .await;

    assert_eq!(recorded.enqueued_names(), ["send_email"]);
    assert_eq!(inline.performed_names(), ["inline_only"]);
    assert_eq!(ran, Some(2));
    assert_nothing_ran();
}

#[tokio::test]
async fn an_inline_scope_inside_a_recording_runs_jobs() {
    let recorded = RecordedJobs::new();
    let inline = InlineJobs::new();
    recorded
        .record(async {
            inline
                .perform(async { inline_only(1).await.unwrap() })
                .await;
            // The recording applies again once the inline scope ends.
            signup("a@example.com").await.unwrap();
        })
        .await;

    assert_eq!(inline.performed_names(), ["inline_only"]);
    // `signup` was recorded, not run, so it enqueued nothing itself.
    assert_eq!(recorded.enqueued_names(), ["signup"]);
    assert_nothing_ran();
}

#[tokio::test]
async fn jobs_run_inline_enqueue_into_the_inline_scope() {
    let recorded = RecordedJobs::new();
    let inline = InlineJobs::new();
    // The inline `inline_signup` enqueues `inline_only` from inside the
    // inline scope, so it runs too, and the outer recording sees neither.
    recorded
        .record(inline.perform(async { inline_signup(1).await.unwrap() }))
        .await;
    assert_eq!(inline.performed_names(), ["inline_only", "inline_signup"]);
    recorded.assert_no_enqueued_jobs();
}

#[tokio::test]
async fn enqueue_layers_apply_before_recording() {
    install_layer();
    let jobs = RecordedJobs::new();
    let vetoed = jobs
        .record(async {
            layered(1).await.unwrap();
            layered(0).await
        })
        .await;

    assert!(
        matches!(vetoed, Err(Error::Vetoed { .. })),
        "{:?}",
        vetoed.err()
    );
    let [recorded] = jobs.enqueued().try_into().unwrap();
    assert_eq!(recorded.job.queue, "rerouted");
    assert_eq!(recorded.job.meta.get("tenant"), Some(&json!("acme")));
}

#[tokio::test]
async fn recording_does_not_touch_the_configured_queue() {
    let queue: Queue = MemoryQueue::new().into();
    butler::configure(queue.clone());

    let jobs = RecordedJobs::new();
    let spawned = jobs
        .record(async {
            let recorded = crunch(6).await.unwrap();
            assert_eq!(queue.state(recorded.id()), None, "not in the real queue");
            // A spawned task is outside the recording: it enqueues normally.
            tokio::spawn(crunch(7).enqueue()).await.unwrap().unwrap()
        })
        .await;

    assert_eq!(jobs.enqueued_names(), ["crunch"]);
    assert_eq!(queue.state(spawned.id()), Some(JobState::Pending));
}
