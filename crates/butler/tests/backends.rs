#![allow(clippy::unwrap_used, clippy::expect_used)]

//! The `Backend` contract, checked the same way against every backend: memory
//! and file always, Redis when a server is reachable. These tests talk to the
//! queue directly, never through `butler::configure`, so they run in parallel.

use std::{
    collections::HashSet,
    path::PathBuf,
    sync::atomic::{AtomicU32, Ordering},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use butler::{
    AnyJob, Backoff, ConcurrencyKey, Cron, Failed, FileQueue, GlobalLimit, JITTER, JobState,
    MemoryQueue, NewJob, Queue, Recurring, RecurringRecord, Retry, RetryPolicy, Unique, UniqueKey,
    monitor::{JobMetric, ListFilter},
};
use serde_json::json;

/// Whether `claim` with a wait blocks until a job arrives.
#[derive(Clone, Copy, PartialEq)]
enum Claim {
    Blocks,
    ReturnsAtOnce,
}

fn backends(test: &str) -> Vec<(Queue, Claim)> {
    static RUN: AtomicU32 = AtomicU32::new(0);
    let run = RUN.fetch_add(1, Ordering::Relaxed);
    let unique = format!("{test}-{}-{run}", std::process::id());

    let dir: PathBuf = std::env::temp_dir().join(format!("butler-backends-{unique}"));
    let _ = std::fs::remove_dir_all(&dir);
    #[cfg_attr(not(any(feature = "redis", feature = "sqlite")), allow(unused_mut))]
    let mut all: Vec<(Queue, Claim)> = vec![
        (MemoryQueue::new().into(), Claim::Blocks),
        (FileQueue::new(&dir).unwrap().into(), Claim::ReturnsAtOnce),
    ];
    #[cfg(feature = "sqlite")]
    all.push((
        butler::SqliteQueue::open(dir.with_extension("db"))
            .unwrap()
            .into(),
        Claim::Blocks,
    ));
    #[cfg(feature = "redis")]
    {
        let url = std::env::var("BUTLER_TEST_REDIS_URL")
            .unwrap_or_else(|_| "redis://127.0.0.1:6379/".into());
        match butler::RedisQueue::connect(&url, &format!("butler-backends-{unique}")) {
            Ok(q) => all.push((q.into(), Claim::Blocks)),
            Err(e) => eprintln!("skipping redis: {e}"),
        }
    }
    all
}

const NOW: Duration = Duration::ZERO;
const DEFAULT: &[&str] = &["default"];
const HOUR: Duration = Duration::from_secs(3600);

/// Backends store run times to the millisecond.
fn millis(at: SystemTime) -> u128 {
    at.duration_since(UNIX_EPOCH).unwrap().as_millis()
}

#[test]
fn claims_in_fifo_order_and_only_once() {
    for (queue, _) in backends("fifo") {
        let first = queue.push("a", "default", vec![json!(1)]).unwrap();
        let second = queue.push("b", "default", vec![json!(2)]).unwrap();

        let claimed = queue.claim("w1", DEFAULT, NOW).unwrap().unwrap();
        assert_eq!(claimed.id(), first, "{}", queue.describe());
        assert_eq!(claimed.args(), [json!(1)]);
        assert_eq!(queue.state(&first), Some(JobState::Processing));

        let next = queue.claim("w2", DEFAULT, NOW).unwrap().unwrap();
        assert_eq!(next.id(), second, "{}", queue.describe());
        assert!(
            queue.claim("w1", DEFAULT, NOW).unwrap().is_none(),
            "{}",
            queue.describe()
        );
    }
}

#[test]
fn a_waiting_claim_wakes_when_a_job_arrives_on_any_of_its_queues() {
    for (queue, claim) in backends("wake") {
        // Pushed to the worker's second queue: a backend that can only block on
        // one queue would miss it until the wait runs out.
        let pusher = {
            let queue = queue.clone();
            thread::spawn(move || {
                thread::sleep(Duration::from_millis(100));
                queue.push("late", "low", vec![]).unwrap()
            })
        };
        let started = Instant::now();
        let claimed = queue
            .claim("w", &["critical", "low"], Duration::from_secs(5))
            .unwrap();
        let waited = started.elapsed();
        let pushed = pusher.join().unwrap();
        match claim {
            Claim::Blocks => {
                assert_eq!(claimed.unwrap().id(), pushed, "{}", queue.describe());
                assert!(waited < Duration::from_secs(2), "{}", queue.describe());
            }
            Claim::ReturnsAtOnce => {
                assert!(claimed.is_none(), "{}", queue.describe());
                assert!(waited < Duration::from_millis(100), "{}", queue.describe());
            }
        }
    }
}

#[test]
fn cancel_wins_or_loses_against_claim_but_never_both() {
    for (queue, _) in backends("cancel") {
        let waiting = queue.push("x", "default", vec![]).unwrap();
        assert!(queue.cancel(&waiting).unwrap(), "{}", queue.describe());
        assert_eq!(queue.state(&waiting), Some(JobState::Cancelled));
        assert!(
            queue.claim("w", DEFAULT, NOW).unwrap().is_none(),
            "{}",
            queue.describe()
        );

        let running = queue.push("y", "default", vec![]).unwrap();
        queue.claim("w", DEFAULT, NOW).unwrap().unwrap();
        assert!(!queue.cancel(&running).unwrap(), "{}", queue.describe());
        assert_eq!(queue.state(&running), Some(JobState::Processing));
        assert!(!queue.cancel("unknown").unwrap());
    }
}

#[test]
fn completing_stores_the_result() {
    for (queue, _) in backends("result") {
        let id = queue
            .push("sum", "default", vec![json!(2), json!(3)])
            .unwrap();
        let job = queue.claim("w", DEFAULT, NOW).unwrap().unwrap();
        let done = queue.complete("w", job, json!({ "sum": 5 })).unwrap();
        assert_eq!(
            done.output::<serde_json::Value>().unwrap(),
            json!({ "sum": 5 })
        );

        let (state, stored) = queue.get(&id).unwrap().unwrap().into_parts();
        assert_eq!(state, JobState::Done, "{}", queue.describe());
        assert_eq!(
            stored.result,
            Some(json!({ "sum": 5 })),
            "{}",
            queue.describe()
        );
    }
}

#[test]
fn failures_retry_then_die() {
    for (queue, _) in backends("fail") {
        let id = queue.push("flaky", "default", vec![]).unwrap();
        let job = queue.claim("w", DEFAULT, NOW).unwrap().unwrap();
        let Failed::Retry(retry) = queue.fail("w", job, "first".into(), 1).unwrap() else {
            panic!(
                "{}: one failure with max_retries 1 is a retry",
                queue.describe()
            );
        };
        assert_eq!(retry.last_error(), Some("first"));

        let job = queue.claim("w", DEFAULT, NOW).unwrap().unwrap();
        assert_eq!(job.attempts(), 1, "{}", queue.describe());
        let Failed::Dead(dead) = queue.fail("w", job, "second".into(), 1).unwrap() else {
            panic!("{}: a second failure is dead", queue.describe());
        };
        assert_eq!(dead.error(), "second");

        let (state, stored) = queue.get(&id).unwrap().unwrap().into_parts();
        assert_eq!(state, JobState::Dead, "{}", queue.describe());
        assert_eq!(stored.last_error.as_deref(), Some("second"));
        assert!(
            queue.claim("w", DEFAULT, NOW).unwrap().is_none(),
            "{}",
            queue.describe()
        );
    }
}

#[test]
fn recover_requeues_only_workers_whose_heartbeat_expired() {
    for (queue, _) in backends("recover") {
        // Long heartbeats while setting up, so "not expired yet" doesn't depend
        // on how fast the backend is (a slow CI disk took over 50 ms here).
        queue.heartbeat("alive", Duration::from_secs(60)).unwrap();
        queue.heartbeat("crashed", Duration::from_secs(60)).unwrap();
        let held = queue.push("held", "default", vec![]).unwrap();
        queue.claim("alive", DEFAULT, NOW).unwrap().unwrap();
        let orphan = queue.push("orphan", "default", vec![]).unwrap();
        queue.claim("crashed", DEFAULT, NOW).unwrap().unwrap();
        assert_eq!(queue.recover().unwrap(), 0, "{}", queue.describe());

        // Now "crashed" stops: its last heartbeat expires almost at once.
        queue
            .heartbeat("crashed", Duration::from_millis(1))
            .unwrap();
        thread::sleep(Duration::from_millis(50));
        assert_eq!(queue.recover().unwrap(), 1, "{}", queue.describe());
        assert_eq!(queue.recover().unwrap(), 0, "each job moves once");

        assert_eq!(queue.state(&orphan), Some(JobState::Pending));
        assert_eq!(queue.state(&held), Some(JobState::Processing));
        assert_eq!(
            queue.claim("new", DEFAULT, NOW).unwrap().unwrap().id(),
            orphan
        );
    }
}

#[test]
fn a_retired_worker_leaves_nothing_to_recover() {
    for (queue, _) in backends("retire") {
        queue.heartbeat("w", Duration::from_secs(60)).unwrap();
        queue.push("x", "default", vec![]).unwrap();
        let job = queue.claim("w", DEFAULT, NOW).unwrap().unwrap();
        queue.complete("w", job, json!(null)).unwrap();
        queue.retire("w").unwrap();
        assert_eq!(queue.recover().unwrap(), 0, "{}", queue.describe());
    }
}

#[test]
fn claims_follow_the_given_queue_order_and_skip_unlisted_queues() {
    for (queue, _) in backends("queues") {
        let low = queue.push("l", "low", vec![]).unwrap();
        let critical = queue.push("c", "critical", vec![]).unwrap();
        let other = queue.push("o", "other", vec![]).unwrap();

        // Older, but on a queue checked later.
        let first = queue
            .claim("w", &["critical", "low"], NOW)
            .unwrap()
            .unwrap();
        assert_eq!(first.id(), critical, "{}", queue.describe());
        assert_eq!(first.queue(), "critical");
        let second = queue
            .claim("w", &["critical", "low"], NOW)
            .unwrap()
            .unwrap();
        assert_eq!(second.id(), low, "{}", queue.describe());

        // "other" is never served unless listed.
        assert!(
            queue
                .claim("w", &["critical", "low"], NOW)
                .unwrap()
                .is_none()
        );
        assert!(
            queue.claim("w", DEFAULT, NOW).unwrap().is_none(),
            "{}",
            queue.describe()
        );
        assert_eq!(queue.state(&other), Some(JobState::Pending));
        assert!(queue.cancel(&other).unwrap(), "cancel works on any queue");
    }
}

#[test]
fn retries_and_recovery_go_back_to_the_jobs_own_queue() {
    for (queue, _) in backends("own-queue") {
        let mailers: &[&str] = &["mailers"];
        let id = queue.push("mail", "mailers", vec![]).unwrap();

        // A failed attempt goes back on "mailers", not "default".
        let job = queue.claim("w", mailers, NOW).unwrap().unwrap();
        assert_eq!(
            queue.fail("w", job, "retry".into(), 3).unwrap().state(),
            JobState::Pending
        );
        assert!(
            queue.claim("w", DEFAULT, NOW).unwrap().is_none(),
            "{}",
            queue.describe()
        );

        // So does a job recovered from a crashed worker.
        queue
            .heartbeat("crashed", Duration::from_millis(50))
            .unwrap();
        assert_eq!(
            queue.claim("crashed", mailers, NOW).unwrap().unwrap().id(),
            id
        );
        thread::sleep(Duration::from_millis(100));
        assert_eq!(queue.recover().unwrap(), 1, "{}", queue.describe());
        assert!(
            queue.claim("w", DEFAULT, NOW).unwrap().is_none(),
            "{}",
            queue.describe()
        );
        let again = queue.claim("w", mailers, NOW).unwrap().unwrap();
        assert_eq!((again.id(), again.attempts()), (id.as_str(), 1));
    }
}

#[test]
fn push_many_keeps_order_and_every_job_is_claimable() {
    for (queue, _) in backends("push-many") {
        let ids = queue
            .push_many(vec![
                NewJob::new("a", "default", vec![json!(1)]),
                NewJob::new("b", "low", vec![json!(2)]),
                NewJob::new("c", "default", vec![json!(3)]),
            ])
            .unwrap();
        assert_eq!(ids.len(), 3, "{}", queue.describe());
        assert!(
            ids.iter()
                .all(|id| queue.state(id) == Some(JobState::Pending))
        );

        let first = queue.claim("w", DEFAULT, NOW).unwrap().unwrap();
        let second = queue.claim("w", DEFAULT, NOW).unwrap().unwrap();
        assert_eq!(
            (first.id(), second.id()),
            (ids[0].as_str(), ids[2].as_str()),
            "{}",
            queue.describe()
        );
        assert_eq!(second.args(), [json!(3)]);
        let low = queue.claim("w", &["low"], NOW).unwrap().unwrap();
        assert_eq!((low.id(), low.name()), (ids[1].as_str(), "b"));
        assert!(queue.push_many(Vec::new()).unwrap().is_empty());
    }
}

#[test]
fn saved_progress_survives_crash_recovery() {
    for (queue, _) in backends("checkpoint") {
        queue.heartbeat("crashed", Duration::from_secs(60)).unwrap();
        let id = queue.push("long", "default", vec![]).unwrap();
        let job = queue.claim("crashed", DEFAULT, NOW).unwrap().unwrap();
        let mut record = job.into_record();
        record.progress = Some(json!({ "after": 41 }));
        queue.checkpoint("crashed", &record).unwrap();
        // Another worker can't overwrite a job it doesn't hold.
        let mut stranger = record.clone();
        stranger.progress = Some(json!({ "after": 999 }));
        queue.checkpoint("someone-else", &stranger).unwrap();

        queue
            .heartbeat("crashed", Duration::from_millis(1))
            .unwrap();
        thread::sleep(Duration::from_millis(50));
        assert_eq!(queue.recover().unwrap(), 1, "{}", queue.describe());
        let resumed = queue.claim("new", DEFAULT, NOW).unwrap().unwrap();
        assert_eq!(resumed.id(), id);
        assert_eq!(
            resumed.record().progress,
            Some(json!({ "after": 41 })),
            "{}",
            queue.describe()
        );
    }
}

#[test]
fn a_checkpoint_saves_only_while_the_worker_holds_the_job() {
    for (queue, _) in backends("checkpoint-holder") {
        let name = queue.describe();
        let progress =
            |queue: &Queue, id: &str| queue.get(id).unwrap().unwrap().record().progress.clone();
        queue.heartbeat("slow", Duration::from_secs(60)).unwrap();
        let id = queue.push("long", "default", vec![]).unwrap();
        let mut stale = queue
            .claim("slow", DEFAULT, NOW)
            .unwrap()
            .unwrap()
            .into_record();

        // Its heartbeat lapses while it still runs: the job moves to another worker.
        queue.heartbeat("slow", Duration::from_millis(1)).unwrap();
        thread::sleep(Duration::from_millis(50));
        assert_eq!(queue.recover().unwrap(), 1, "{name}");
        let job = queue.claim("fresh", DEFAULT, NOW).unwrap().unwrap();
        let mut current = job.record().clone();
        current.progress = Some(json!({ "run": 2 }));
        queue.checkpoint("fresh", &current).unwrap();

        stale.progress = Some(json!({ "run": 1 }));
        queue.checkpoint("slow", &stale).unwrap();
        assert_eq!(progress(&queue, &id), Some(json!({ "run": 2 })), "{name}");

        // Nor can its holder save once the job is done.
        queue.complete("fresh", job, json!(null)).unwrap();
        let completed = progress(&queue, &id);
        current.progress = Some(json!({ "run": 3 }));
        queue.checkpoint("fresh", &current).unwrap();
        assert_eq!(progress(&queue, &id), completed, "{name}");
        assert_eq!(queue.state(&id), Some(JobState::Done), "{name}");
    }
}

#[test]
fn stats_count_running_jobs_per_queue_through_every_way_out() {
    for (queue, _) in backends("running-per-queue") {
        let name = queue.describe();
        let running = |queue: &Queue| -> Vec<(String, u64, u64)> {
            queue
                .stats()
                .unwrap()
                .queues
                .into_iter()
                // The file backend always lists `default`.
                .filter(|q| q.name != "default")
                .map(|q| (q.name, q.pending, q.running))
                .collect()
        };
        let row = |name: &str, pending, running| (name.to_owned(), pending, running);
        queue.heartbeat("w", Duration::from_secs(60)).unwrap();
        queue.heartbeat("doomed", Duration::from_secs(60)).unwrap();
        for (job, on) in [
            ("a", "mail"),
            ("b", "mail"),
            ("c", "mail"),
            ("d", "reports"),
        ] {
            queue.push(job, on, vec![]).unwrap();
        }
        let first = queue.claim("w", &["mail"], NOW).unwrap().unwrap();
        let second = queue.claim("w", &["mail"], NOW).unwrap().unwrap();
        queue.claim("doomed", &["mail"], NOW).unwrap().unwrap();
        queue.claim("doomed", &["reports"], NOW).unwrap().unwrap();
        assert_eq!(
            running(&queue),
            [row("mail", 0, 3), row("reports", 0, 1)],
            "{name}"
        );
        assert_eq!(queue.stats().unwrap().processing, 4, "{name}");

        queue.complete("w", first, json!(null)).unwrap();
        // A retry goes back to pending.
        queue.fail("w", second, "boom".into(), 1).unwrap();
        assert_eq!(
            running(&queue),
            [row("mail", 1, 1), row("reports", 0, 1)],
            "{name}"
        );

        queue.heartbeat("doomed", Duration::from_millis(1)).unwrap();
        thread::sleep(Duration::from_millis(50));
        assert_eq!(queue.recover().unwrap(), 2, "{name}");
        assert_eq!(
            running(&queue),
            [row("mail", 2, 0), row("reports", 1, 0)],
            "{name}"
        );
    }
}

#[test]
fn stats_listing_retry_and_discard_for_a_dashboard() {
    for (queue, _) in backends("monitor") {
        let name = queue.describe();
        queue.heartbeat("w", Duration::from_secs(60)).unwrap();
        let done = queue.push("a", "default", vec![]).unwrap();
        let dead = queue.push("b", "default", vec![json!("x")]).unwrap();
        let low = queue.push("c", "low", vec![]).unwrap();
        let cancelled = queue.push("d", "default", vec![]).unwrap();

        let job = queue.claim("w", DEFAULT, NOW).unwrap().unwrap();
        queue.complete("w", job, json!(1)).unwrap();
        let job = queue.claim("w", DEFAULT, NOW).unwrap().unwrap();
        queue.fail("w", job, "boom".into(), 0).unwrap();
        assert!(queue.cancel(&cancelled).unwrap());

        let stats = queue.stats().unwrap();
        assert_eq!(stats.pending(), 1, "{name}");
        assert_eq!(
            (stats.processing, stats.done, stats.dead, stats.cancelled),
            (0, 1, 1, 1),
            "{name}"
        );
        let low_queue = stats.queues.iter().find(|q| q.name == "low").unwrap();
        assert_eq!(low_queue.pending, 1, "{name}");
        let worker = stats.workers.iter().find(|w| w.id == "w").unwrap();
        assert!(worker.expires_in_ms > 0, "{name}: w is alive");

        let ids = |state, queue_name: Option<&str>| -> Vec<String> {
            let mut filter = ListFilter::new(state);
            filter.queue = queue_name.map(str::to_owned);
            queue
                .list(&filter)
                .unwrap()
                .iter()
                .map(|job| job.record().id.clone())
                .collect()
        };
        assert_eq!(ids(JobState::Dead, None), [dead.as_str()], "{name}");
        assert_eq!(ids(JobState::Done, None), [done.as_str()], "{name}");
        assert_eq!(ids(JobState::Pending, None), [low.as_str()], "{name}");
        assert_eq!(
            ids(JobState::Pending, Some("low")),
            [low.as_str()],
            "{name}"
        );
        assert!(ids(JobState::Dead, Some("low")).is_empty(), "{name}");
        let listed = queue.list(&ListFilter::new(JobState::Dead)).unwrap();
        assert_eq!(
            listed[0].record().last_error.as_deref(),
            Some("boom"),
            "{name}"
        );

        // Retry: only dead jobs, back on their queue with attempts reset.
        assert!(!queue.retry(&done).unwrap(), "{name}");
        assert!(queue.retry(&dead).unwrap(), "{name}");
        assert!(!queue.retry(&dead).unwrap(), "{name}: already retried");
        let (state, record) = queue.get(&dead).unwrap().unwrap().into_parts();
        assert_eq!((state, record.attempts), (JobState::Pending, 0), "{name}");
        assert_eq!(
            queue.claim("w", DEFAULT, NOW).unwrap().unwrap().id(),
            dead,
            "{name}"
        );

        // Discard: only finished jobs.
        assert!(!queue.discard(&low).unwrap(), "{name}: still pending");
        assert!(queue.discard(&done).unwrap(), "{name}");
        assert_eq!(queue.state(&done), None, "{name}");
        assert!(queue.discard(&cancelled).unwrap(), "{name}");
    }
}

#[test]
fn metrics_keep_per_minute_history_and_lifetime_totals() {
    for (queue, _) in backends("metrics") {
        let name = queue.describe();
        if name.starts_with("file:") {
            continue; // the file backend keeps no history
        }
        let minute = butler::monitor::current_minute();
        for (failed, ms) in [(false, 10), (false, 30), (true, 5)] {
            let metric = JobMetric {
                job: "send_email".into(),
                queue: "mailers".into(),
                failed,
                duration_ms: ms,
                minute,
            };
            queue.record_metric(&metric).unwrap();
        }
        let buckets = queue.metrics(minute).unwrap();
        let bucket = buckets
            .iter()
            .find(|b| b.job == "send_email" && b.queue == "mailers")
            .unwrap_or_else(|| panic!("{name}: no bucket in {buckets:?}"));
        assert_eq!(
            (
                bucket.minute,
                bucket.processed,
                bucket.failed,
                bucket.total_ms,
                bucket.max_ms
            ),
            (minute, 3, 1, 45, 30),
            "{name}"
        );
        assert!(queue.metrics(minute + 1).unwrap().is_empty(), "{name}");
        let stats = queue.stats().unwrap();
        assert_eq!(
            (stats.processed_total, stats.failed_total),
            (3, 1),
            "{name}"
        );
    }
}

#[test]
fn a_scheduled_job_is_not_claimable_before_its_time() {
    for (queue, _) in backends("scheduled-wait") {
        let name = queue.describe();
        let at = SystemTime::now() + HOUR;
        let id = queue
            .schedule("later", "default", vec![json!(1)], at)
            .unwrap();
        assert_eq!(queue.state(&id), Some(JobState::Scheduled), "{name}");
        let Some(AnyJob::Scheduled(job)) = queue.get(&id).unwrap() else {
            panic!("{name}: a scheduled job reads back as scheduled");
        };
        assert_eq!(millis(job.run_at()), millis(at), "{name}");
        assert_eq!(job.args(), [json!(1)]);

        assert!(queue.claim("w", DEFAULT, NOW).unwrap().is_none(), "{name}");
        let early = queue.promote(SystemTime::now()).unwrap();
        assert_eq!(early.moved, 0, "{name}");
        assert_eq!(early.next.map(millis), Some(millis(at)), "{name}");
        assert_eq!(queue.state(&id), Some(JobState::Scheduled), "{name}");
    }
}

#[test]
fn a_scheduled_job_is_claimable_once_its_time_has_come() {
    for (queue, _) in backends("scheduled-due") {
        let name = queue.describe();
        let at = SystemTime::now() + HOUR;
        let id = queue.schedule("later", "default", vec![], at).unwrap();

        // Promoting as of its run time moves it onto its queue.
        let promoted = queue.promote(at).unwrap();
        assert_eq!((promoted.moved, promoted.next), (1, None), "{name}");
        assert_eq!(queue.state(&id), Some(JobState::Pending), "{name}");
        assert_eq!(queue.promote(at).unwrap().moved, 0, "{name}: moved once");
        let job = queue.claim("w", DEFAULT, NOW).unwrap().unwrap();
        assert_eq!(job.id(), id, "{name}");
    }
}

#[test]
fn a_waiting_claim_wakes_when_a_scheduled_job_comes_due() {
    for (queue, claim) in backends("scheduled-wake") {
        let name = queue.describe();
        let delay = Duration::from_millis(200);
        // Start the clock before the run time is fixed: `schedule` itself can
        // take milliseconds (the file backend writes to disk), and that time
        // counts towards the delay.
        let started = Instant::now();
        let id = queue
            .schedule("soon", "default", vec![], SystemTime::now() + delay)
            .unwrap();
        match claim {
            // One claim: it wakes at the run time, not at the end of its wait.
            Claim::Blocks => {
                let job = queue
                    .claim("w", DEFAULT, Duration::from_secs(5))
                    .unwrap()
                    .unwrap();
                let waited = started.elapsed();
                assert_eq!(job.id(), id, "{name}");
                assert!(
                    waited >= delay - Duration::from_millis(5),
                    "{name}: {waited:?}"
                );
                assert!(waited < Duration::from_secs(2), "{name}: {waited:?}");
            }
            // The worker polls this one: due jobs appear on a later claim.
            Claim::ReturnsAtOnce => {
                assert!(
                    queue
                        .claim("w", DEFAULT, Duration::from_secs(5))
                        .unwrap()
                        .is_none()
                );
                let deadline = Instant::now() + Duration::from_secs(5);
                let job = loop {
                    if let Some(job) = queue.claim("w", DEFAULT, NOW).unwrap() {
                        break job;
                    }
                    assert!(Instant::now() < deadline, "{name}: never became claimable");
                    thread::sleep(Duration::from_millis(10));
                };
                assert_eq!(job.id(), id, "{name}");
                assert!(
                    started.elapsed() >= delay - Duration::from_millis(5),
                    "{name}"
                );
            }
        }
    }
}

#[test]
fn cancel_works_while_scheduled() {
    for (queue, _) in backends("scheduled-cancel") {
        let name = queue.describe();
        let at = SystemTime::now() + HOUR;
        let id = queue.schedule("x", "default", vec![], at).unwrap();
        assert!(queue.cancel(&id).unwrap(), "{name}");
        assert_eq!(queue.state(&id), Some(JobState::Cancelled), "{name}");
        assert!(!queue.cancel(&id).unwrap(), "{name}: cancelled once");

        let promoted = queue.promote(at).unwrap();
        assert_eq!((promoted.moved, promoted.next), (0, None), "{name}");
        assert!(queue.claim("w", DEFAULT, NOW).unwrap().is_none(), "{name}");
        assert!(!queue.run_now(&id).unwrap(), "{name}");
        assert_eq!(queue.state(&id), Some(JobState::Cancelled), "{name}");
    }
}

#[test]
fn a_waiting_claim_notices_new_and_earlier_schedules() {
    for existing in [false, true] {
        for (queue, claim) in backends("schedule-during-claim") {
            if claim == Claim::ReturnsAtOnce {
                continue;
            }
            let name = queue.describe();
            if existing {
                queue
                    .schedule("later", "default", vec![], SystemTime::now() + HOUR)
                    .unwrap();
            }
            let (send, receive) = std::sync::mpsc::channel();
            let waiter = thread::spawn({
                let queue = queue.clone();
                move || {
                    send.send(()).unwrap();
                    queue.claim("w", DEFAULT, Duration::from_secs(5)).unwrap()
                }
            });
            receive.recv_timeout(Duration::from_secs(2)).unwrap();
            // Give the claim time to enter its backend wait, as in the
            // pending-job wakeup contract test above.
            thread::sleep(Duration::from_millis(100));
            let at = SystemTime::now() + Duration::from_millis(100);
            let started = Instant::now();
            let id = queue.schedule("sooner", "default", vec![], at).unwrap();
            let job = waiter
                .join()
                .unwrap()
                .unwrap_or_else(|| panic!("{name}: missed new schedule"));
            assert_eq!(job.id(), id, "{name}");
            assert!(
                millis(SystemTime::now()) >= millis(at),
                "{name}: claimed early"
            );
            assert!(
                started.elapsed() < Duration::from_secs(2),
                "{name}: waited for stale deadline"
            );
        }
    }
}

#[test]
fn cancel_and_promotion_never_both_take_a_scheduled_job() {
    for (queue, claim) in backends("scheduled-race") {
        let name = queue.describe();
        let due = SystemTime::now() + Duration::from_millis(50);
        let ids: Vec<String> = (0..40)
            .map(|i| {
                queue
                    .schedule("race", "default", vec![json!(i)], due)
                    .unwrap()
            })
            .collect();
        // Cancels every job, starting around the time they come due, while
        // this thread promotes and claims them.
        let canceller = thread::spawn({
            let (queue, ids) = (queue.clone(), ids.clone());
            move || {
                thread::sleep(Duration::from_millis(45));
                ids.into_iter()
                    .filter(|id| queue.cancel(id).unwrap())
                    .collect::<HashSet<_>>()
            }
        });
        let mut claimed = HashSet::new();
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let wait = match claim {
                Claim::Blocks => Duration::from_millis(10),
                Claim::ReturnsAtOnce => NOW,
            };
            if let Some(job) = queue.claim("w", DEFAULT, wait).unwrap() {
                assert!(claimed.insert(job.id().to_owned()), "{name}: claimed twice");
                continue;
            }
            let settled = ids.iter().all(|id| {
                matches!(
                    queue.state(id),
                    Some(JobState::Cancelled | JobState::Processing)
                )
            });
            if canceller.is_finished() && settled {
                break;
            }
            assert!(Instant::now() < deadline, "{name}: jobs never settled");
            thread::sleep(Duration::from_millis(2));
        }
        let cancelled = canceller.join().unwrap();
        assert!(
            cancelled.is_disjoint(&claimed),
            "{name}: cancelled and claimed"
        );
        assert_eq!(
            cancelled.len() + claimed.len(),
            ids.len(),
            "{name}: lost jobs"
        );
        for id in &cancelled {
            assert_eq!(queue.state(id), Some(JobState::Cancelled), "{name}");
        }
    }
}

#[test]
fn scheduled_jobs_are_promoted_onto_their_own_queue() {
    for (queue, _) in backends("scheduled-own-queue") {
        let name = queue.describe();
        let at = SystemTime::now() + HOUR;
        let id = queue.schedule("mail", "mailers", vec![], at).unwrap();
        assert_eq!(queue.promote(at).unwrap().moved, 1, "{name}");
        assert!(queue.claim("w", DEFAULT, NOW).unwrap().is_none(), "{name}");
        let job = queue.claim("w", &["mailers"], NOW).unwrap().unwrap();
        assert_eq!((job.id(), job.queue()), (id.as_str(), "mailers"), "{name}");
    }
}

#[test]
fn promotion_moves_only_due_jobs_and_reports_the_next_run_time() {
    for (queue, _) in backends("scheduled-next") {
        let name = queue.describe();
        let base = SystemTime::now() + HOUR;
        let first = queue.schedule("a", "default", vec![], base).unwrap();
        let second = queue
            .schedule("b", "low", vec![], base + Duration::from_secs(1))
            .unwrap();
        let far = base + 24 * HOUR;
        let last = queue.schedule("c", "default", vec![], far).unwrap();

        let promoted = queue.promote(base + Duration::from_secs(2)).unwrap();
        assert_eq!(promoted.moved, 2, "{name}");
        assert_eq!(promoted.next.map(millis), Some(millis(far)), "{name}");
        assert_eq!(queue.state(&first), Some(JobState::Pending), "{name}");
        assert_eq!(queue.state(&second), Some(JobState::Pending), "{name}");
        assert_eq!(queue.state(&last), Some(JobState::Scheduled), "{name}");
    }
}

#[test]
fn push_many_can_schedule_jobs() {
    for (queue, _) in backends("scheduled-many") {
        let name = queue.describe();
        let at = SystemTime::now() + HOUR;
        let ids = queue
            .push_many(vec![
                NewJob::new("now", "default", vec![]),
                NewJob::new("later", "low", vec![]).run_at(at),
            ])
            .unwrap();
        assert_eq!(queue.state(&ids[0]), Some(JobState::Pending), "{name}");
        assert_eq!(queue.state(&ids[1]), Some(JobState::Scheduled), "{name}");
        assert!(queue.claim("w", &["low"], NOW).unwrap().is_none(), "{name}");
        assert_eq!(queue.promote(at).unwrap().moved, 1, "{name}");
        let job = queue.claim("w", &["low"], NOW).unwrap().unwrap();
        assert_eq!(job.id(), ids[1], "{name}");
    }
}

#[test]
fn a_run_time_already_past_enqueues_at_once() {
    for (queue, _) in backends("scheduled-past") {
        let name = queue.describe();
        let past = SystemTime::now() - HOUR;
        let id = queue.schedule("late", "default", vec![], past).unwrap();
        assert_eq!(queue.state(&id), Some(JobState::Pending), "{name}");
        let ids = queue
            .push_many(vec![NewJob::new("late", "default", vec![]).run_at(past)])
            .unwrap();
        assert_eq!(queue.state(&ids[0]), Some(JobState::Pending), "{name}");
    }
}

#[test]
fn scheduled_jobs_are_counted_listed_and_can_run_now() {
    for (queue, _) in backends("scheduled-monitor") {
        let name = queue.describe();
        let at = SystemTime::now() + HOUR;
        let later = queue.schedule("later", "mailers", vec![], at).unwrap();
        let sooner = queue
            .schedule("sooner", "default", vec![], at - Duration::from_secs(60))
            .unwrap();
        let pending = queue.push("now", "default", vec![]).unwrap();

        let stats = queue.stats().unwrap();
        assert_eq!((stats.scheduled, stats.pending()), (2, 1), "{name}");

        // Soonest first.
        let listed: Vec<String> = queue
            .list(&ListFilter::new(JobState::Scheduled))
            .unwrap()
            .iter()
            .map(|job| job.record().id.clone())
            .collect();
        assert_eq!(listed, [sooner.as_str(), later.as_str()], "{name}");
        let mut on_mailers = ListFilter::new(JobState::Scheduled);
        on_mailers.queue = Some("mailers".into());
        assert_eq!(queue.list(&on_mailers).unwrap().len(), 1, "{name}");

        // Run now: onto its own queue, before its time.
        assert!(!queue.run_now(&pending).unwrap(), "{name}: not scheduled");
        assert!(queue.run_now(&later).unwrap(), "{name}");
        assert!(!queue.run_now(&later).unwrap(), "{name}: moved once");
        assert_eq!(queue.state(&later), Some(JobState::Pending), "{name}");
        let job = queue.claim("w", &["mailers"], NOW).unwrap().unwrap();
        assert_eq!(job.id(), later, "{name}");
        assert_eq!(queue.stats().unwrap().scheduled, 1, "{name}");
    }
}

#[test]
fn a_failed_attempt_waits_for_its_retry_on_its_own_queue() {
    for (queue, _) in backends("retry-later") {
        let name = queue.describe();
        let mailers: &[&str] = &["mailers"];
        let id = queue.push("mail", "mailers", vec![json!(1)]).unwrap();
        let job = queue.claim("w", mailers, NOW).unwrap().unwrap();
        let before = SystemTime::now();
        let policy = RetryPolicy::new(3, Backoff::Fixed(HOUR));
        let Failed::Scheduled(waiting) = queue
            .fail_with("w", job, "timeout".into(), Retry::Default, policy)
            .unwrap()
        else {
            panic!("{name}: a retry with a backoff waits");
        };
        // The next attempt's time, backoff plus jitter, is recorded.
        let run_at = waiting.run_at();
        assert!(millis(run_at) >= millis(before + HOUR), "{name}");
        assert!(
            run_at <= SystemTime::now() + HOUR.mul_f64(1.0 + JITTER),
            "{name}"
        );
        let Some(AnyJob::Scheduled(stored)) = queue.get(&id).unwrap() else {
            panic!("{name}: stored as scheduled");
        };
        assert_eq!(millis(stored.run_at()), millis(run_at), "{name}");
        assert_eq!(stored.last_error(), Some("timeout"), "{name}");
        assert!(queue.claim("w", mailers, NOW).unwrap().is_none(), "{name}");

        // A failed worker leaves only the scheduled retry, not a processing
        // copy that recovery could make runnable before the backoff expires.
        assert_eq!(queue.recover().unwrap(), 0, "{name}");
        assert_eq!(queue.state(&id), Some(JobState::Scheduled), "{name}");

        assert_eq!(queue.promote(run_at).unwrap().moved, 1, "{name}");
        assert!(queue.claim("w", DEFAULT, NOW).unwrap().is_none(), "{name}");
        let again = queue.claim("w", mailers, NOW).unwrap().unwrap();
        assert_eq!((again.id(), again.attempts()), (id.as_str(), 1), "{name}");
    }
}

#[test]
fn an_error_can_refuse_retries_or_pick_the_delay() {
    for (queue, _) in backends("retry-classified") {
        let name = queue.describe();
        let policy = RetryPolicy::new(5, Backoff::Fixed(HOUR));

        let never = queue.push("a", "default", vec![]).unwrap();
        let job = queue.claim("w", DEFAULT, NOW).unwrap().unwrap();
        let failed = queue
            .fail_with("w", job, "gone".into(), Retry::Never, policy)
            .unwrap();
        assert_eq!(
            failed.state(),
            JobState::Dead,
            "{name}: retries left, but never"
        );
        assert_eq!(queue.state(&never), Some(JobState::Dead), "{name}");

        queue.push("b", "default", vec![]).unwrap();
        let job = queue.claim("w", DEFAULT, NOW).unwrap().unwrap();
        let before = SystemTime::now();
        let delay = Duration::from_secs(90);
        let Failed::Scheduled(waiting) = queue
            .fail_with("w", job, "slow down".into(), Retry::After(delay), policy)
            .unwrap()
        else {
            panic!("{name}: retry after a delay");
        };
        // Exactly the delay asked for: no backoff, no jitter.
        assert!(millis(waiting.run_at()) >= millis(before + delay), "{name}");
        assert!(waiting.run_at() <= SystemTime::now() + delay, "{name}");

        // At once when there's no delay; dead once out of retries anyway.
        queue.push("c", "default", vec![]).unwrap();
        let job = queue.claim("w", DEFAULT, NOW).unwrap().unwrap();
        let failed = queue
            .fail_with("w", job, "x".into(), Retry::After(Duration::ZERO), policy)
            .unwrap();
        assert_eq!(failed.state(), JobState::Pending, "{name}");
        let job = queue.claim("w", DEFAULT, NOW).unwrap().unwrap();
        let out_of_retries = RetryPolicy::new(1, Backoff::Fixed(HOUR));
        let failed = queue
            .fail_with("w", job, "x".into(), Retry::After(delay), out_of_retries)
            .unwrap();
        assert_eq!(failed.state(), JobState::Dead, "{name}");
    }
}

#[test]
fn metadata_is_kept_through_scheduling_claims_retries_and_recovery() {
    for (queue, _) in backends("meta") {
        let name = queue.describe();
        let with_meta = |job: NewJob| {
            let mut job = job;
            job.meta.insert("tenant".into(), json!("acme"));
            job.meta.insert("trace".into(), json!({ "id": 7 }));
            job
        };
        let expected = json!({ "tenant": "acme", "trace": { "id": 7 } });
        let meta_of = |id: &str| {
            let record = queue.get(id).unwrap().unwrap().record().meta.clone();
            serde_json::Value::Object(record)
        };

        let pending = queue
            .push_job(with_meta(NewJob::new("a", "default", vec![])))
            .unwrap();
        let later = SystemTime::now() + HOUR;
        let scheduled = queue
            .push_job(with_meta(NewJob::new("b", "default", vec![]).run_at(later)))
            .unwrap();
        let batch = queue
            .push_many(vec![
                with_meta(NewJob::new("c", "default", vec![])),
                with_meta(NewJob::new("d", "default", vec![]).run_at(later)),
                NewJob::new("plain", "default", vec![]),
            ])
            .unwrap();
        for id in [&pending, &scheduled, &batch[0], &batch[1]] {
            assert_eq!(meta_of(id), expected, "{name}: {id}");
        }
        assert!(
            queue
                .get(&batch[2])
                .unwrap()
                .unwrap()
                .record()
                .meta
                .is_empty(),
            "{name}"
        );
        assert_eq!(queue.state(&scheduled), Some(JobState::Scheduled), "{name}");

        // Claimed, failed back to the queue, recovered after a crash, and
        // promoted: the metadata goes wherever the job goes.
        let job = queue.claim("w1", DEFAULT, NOW).unwrap().unwrap();
        assert_eq!(job.id(), pending, "{name}");
        assert_eq!(
            serde_json::Value::Object(job.record().meta.clone()),
            expected
        );
        queue.fail("w1", job, "boom".into(), 3).unwrap();
        assert_eq!(meta_of(&pending), expected, "{name}");

        queue.heartbeat("w2", Duration::from_millis(30)).unwrap();
        let held = queue.claim("w2", DEFAULT, NOW).unwrap().unwrap();
        thread::sleep(Duration::from_millis(80));
        assert!(queue.recover().unwrap() >= 1, "{name}");
        assert_eq!(meta_of(held.id()), expected, "{name}");

        queue.promote(later).unwrap();
        assert_eq!(queue.state(&scheduled), Some(JobState::Pending), "{name}");
        assert_eq!(meta_of(&scheduled), expected, "{name}");
    }
}

/// A job whose concurrency key is `key`, with `limit`.
fn keyed(name: &str, key: &str, limit: u32) -> NewJob {
    let mut job = NewJob::new(name, "default", vec![json!(key)]);
    job.concurrency = Some(ConcurrencyKey {
        key: format!("sync:[{key:?}]"),
        limit,
    });
    job
}

#[test]
fn claims_skip_jobs_whose_concurrency_key_is_full() {
    for (queue, _) in backends("ckey-skip") {
        let name = queue.describe();
        let a1 = queue.push_job(keyed("a1", "a", 1)).unwrap();
        let a2 = queue.push_job(keyed("a2", "a", 1)).unwrap();
        let b1 = queue.push_job(keyed("b1", "b", 1)).unwrap();
        let plain = queue.push("plain", "default", vec![]).unwrap();
        let a3 = queue.push_job(keyed("a3", "a", 1)).unwrap();
        let claim = |worker: &str| queue.claim(worker, DEFAULT, NOW).unwrap();

        let first = claim("w1").unwrap();
        assert_eq!(first.id(), a1, "{name}");
        // "a" is running: a2 is skipped, and the jobs behind it are not held back.
        assert_eq!(claim("w2").unwrap().id(), b1, "{name}");
        assert_eq!(claim("w3").unwrap().id(), plain, "{name}");
        assert!(claim("w4").is_none(), "{name}: only a's jobs are left");
        assert_eq!(queue.state(&a2), Some(JobState::Pending), "{name}");

        // a1 finishes: a2, then a3, in their order.
        queue.complete("w1", first, json!(null)).unwrap();
        let second = claim("w4").unwrap();
        assert_eq!(second.id(), a2, "{name}");
        assert!(claim("w5").is_none(), "{name}");
        queue.complete("w4", second, json!(null)).unwrap();
        assert_eq!(claim("w5").unwrap().id(), a3, "{name}");
    }
}

#[test]
fn a_concurrency_limit_above_one_runs_that_many_per_key() {
    for (queue, _) in backends("ckey-limit") {
        let name = queue.describe();
        for n in 0..4 {
            queue.push_job(keyed(&format!("j{n}"), "a", 2)).unwrap();
        }
        let claim = |worker: &str| queue.claim(worker, DEFAULT, NOW).unwrap();
        assert!(claim("w1").is_some(), "{name}");
        assert!(claim("w2").is_some(), "{name}");
        assert!(claim("w3").is_none(), "{name}: two at most");
    }
}

#[test]
fn every_way_out_of_processing_frees_a_concurrency_key() {
    for (queue, _) in backends("ckey-release") {
        let name = queue.describe();
        for n in 0..6 {
            queue.push_job(keyed(&format!("j{n}"), "a", 1)).unwrap();
        }
        let claim = || queue.claim("w", DEFAULT, NOW).unwrap();
        let full = |step: &str| {
            assert!(claim().is_none(), "{name}: the key is held before {step}");
        };

        let job = claim().unwrap();
        full("a retry");
        queue.fail("w", job, "again".into(), 3).unwrap();

        let job = claim().unwrap();
        full("a delayed retry");
        let policy = RetryPolicy::new(3, Backoff::Fixed(HOUR));
        queue.fail("w", job, "later".into(), policy).unwrap();

        let job = claim().unwrap();
        full("dying");
        queue.fail("w", job, "dead".into(), 0).unwrap();

        let job = claim().unwrap();
        full("an interruption");
        queue.interrupt("w", job).unwrap();

        let job = claim().unwrap();
        full("completing");
        queue.complete("w", job, json!(null)).unwrap();
        assert!(claim().is_some(), "{name}: free again");
    }
}

#[test]
fn recovering_a_crashed_worker_frees_its_concurrency_keys() {
    for (queue, _) in backends("ckey-recover") {
        let name = queue.describe();
        let held = queue.push_job(keyed("held", "a", 1)).unwrap();
        let waiting = queue.push_job(keyed("waiting", "a", 1)).unwrap();
        queue.heartbeat("crashed", Duration::from_secs(60)).unwrap();
        queue.claim("crashed", DEFAULT, NOW).unwrap().unwrap();
        assert!(
            queue.claim("alive", DEFAULT, NOW).unwrap().is_none(),
            "{name}"
        );

        queue
            .heartbeat("crashed", Duration::from_millis(1))
            .unwrap();
        thread::sleep(Duration::from_millis(50));
        assert_eq!(queue.recover().unwrap(), 1, "{name}");
        // The key is free again: one of the two runs, and only one.
        let next = queue.claim("alive", DEFAULT, NOW).unwrap().unwrap();
        assert!([&held, &waiting].contains(&&next.id().to_owned()), "{name}");
        assert!(
            queue.claim("alive", DEFAULT, NOW).unwrap().is_none(),
            "{name}"
        );
    }
}

#[test]
fn a_job_skipped_for_its_key_stays_pending_counted_listed_and_cancellable() {
    for (queue, _) in backends("ckey-parked") {
        let name = queue.describe();
        queue.push_job(keyed("running", "a", 1)).unwrap();
        let skipped = queue.push_job(keyed("skipped", "a", 1)).unwrap();
        let job = queue.claim("w", DEFAULT, NOW).unwrap().unwrap();
        assert!(queue.claim("w", DEFAULT, NOW).unwrap().is_none(), "{name}");

        assert_eq!(queue.state(&skipped), Some(JobState::Pending), "{name}");
        assert_eq!(queue.stats().unwrap().pending(), 1, "{name}");
        let listed = queue.list(&ListFilter::new(JobState::Pending)).unwrap();
        let ids: Vec<_> = listed.iter().map(|job| job.record().id.clone()).collect();
        assert_eq!(ids, std::slice::from_ref(&skipped), "{name}");

        assert!(queue.cancel(&skipped).unwrap(), "{name}");
        assert_eq!(queue.state(&skipped), Some(JobState::Cancelled), "{name}");
        queue.complete("w", job, json!(null)).unwrap();
        assert!(queue.claim("w", DEFAULT, NOW).unwrap().is_none(), "{name}");
        assert_eq!(queue.stats().unwrap().pending(), 0, "{name}");
    }
}

#[test]
fn concurrent_claims_never_exceed_a_concurrency_key_limit() {
    for (queue, _) in backends("ckey-race") {
        let name = queue.describe();
        for n in 0..12 {
            queue.push_job(keyed(&format!("j{n}"), "a", 3)).unwrap();
        }
        let claimers: Vec<_> = (0..8)
            .map(|n| {
                let queue = queue.clone();
                thread::spawn(move || {
                    queue
                        .claim(&format!("w{n}"), DEFAULT, NOW)
                        .unwrap()
                        .is_some()
                })
            })
            .collect();
        let claimed = claimers
            .into_iter()
            .filter_map(|claimer| claimer.join().unwrap().then_some(()))
            .count();
        assert_eq!(claimed, 3, "{name}");
    }
}

/// A job that is unique on `key` until `until`.
fn unique(key: &str, until: Unique) -> NewJob {
    let mut job = NewJob::new("refresh", "default", vec![json!(key)]);
    job.unique = Some(UniqueKey {
        key: format!("refresh:[{key:?}]"),
        until,
    });
    job
}

#[test]
fn a_unique_job_until_started_is_stored_once_while_it_waits() {
    for (queue, _) in backends("unique-started") {
        let name = queue.describe();
        let first = queue.push_job(unique("a", Unique::UntilStarted)).unwrap();
        let again = queue.push_job(unique("a", Unique::UntilStarted)).unwrap();
        assert_eq!(again, first, "{name}: the waiting one");
        let other = queue.push_job(unique("b", Unique::UntilStarted)).unwrap();
        assert_ne!(other, first, "{name}: other arguments are another job");
        assert_eq!(queue.stats().unwrap().pending(), 2, "{name}");

        // Scheduled counts as waiting.
        let later = unique("c", Unique::UntilStarted).run_at(SystemTime::now() + HOUR);
        let scheduled = queue.push_job(later.clone()).unwrap();
        assert_eq!(queue.push_job(later).unwrap(), scheduled, "{name}");

        // Once a worker starts it, an identical job can wait again.
        let job = queue.claim("w", DEFAULT, NOW).unwrap().unwrap();
        assert_eq!(job.id(), first, "{name}");
        let next = queue.push_job(unique("a", Unique::UntilStarted)).unwrap();
        assert_ne!(next, first, "{name}");

        // Cancelling frees it too.
        assert!(queue.cancel(&next).unwrap(), "{name}");
        let after_cancel = queue.push_job(unique("a", Unique::UntilStarted)).unwrap();
        assert_ne!(after_cancel, next, "{name}");
    }
}

#[test]
fn a_unique_job_until_finished_keeps_its_key_through_retries() {
    for (queue, _) in backends("unique-finished") {
        let name = queue.describe();
        let job_of = |key| unique(key, Unique::UntilFinished);
        let first = queue.push_job(job_of("a")).unwrap();
        let job = queue.claim("w", DEFAULT, NOW).unwrap().unwrap();
        assert_eq!(
            queue.push_job(job_of("a")).unwrap(),
            first,
            "{name}: running"
        );
        queue.fail("w", job, "again".into(), 3).unwrap();
        assert_eq!(
            queue.push_job(job_of("a")).unwrap(),
            first,
            "{name}: retrying"
        );
        let job = queue.claim("w", DEFAULT, NOW).unwrap().unwrap();
        queue.complete("w", job, json!(null)).unwrap();
        let second = queue.push_job(job_of("a")).unwrap();
        assert_ne!(second, first, "{name}: done frees it");

        // Dying frees it as well.
        let job = queue.claim("w", DEFAULT, NOW).unwrap().unwrap();
        queue.fail("w", job, "dead".into(), 0).unwrap();
        assert_ne!(queue.push_job(job_of("a")).unwrap(), second, "{name}");
    }
}

#[test]
fn concurrent_pushes_of_a_unique_job_store_it_once() {
    for (queue, _) in backends("unique-race") {
        let name = queue.describe();
        let pushers: Vec<_> = (0..8)
            .map(|_| {
                let queue = queue.clone();
                thread::spawn(move || queue.push_job(unique("a", Unique::UntilFinished)).unwrap())
            })
            .collect();
        let ids: HashSet<_> = pushers
            .into_iter()
            .map(|pusher| pusher.join().unwrap())
            .collect();
        assert_eq!(ids.len(), 1, "{name}: every push got the same job");
        let a = ids.into_iter().next().unwrap();
        assert_eq!(queue.stats().unwrap().pending(), 1, "{name}");

        // In one batch too, duplicates of the batch included.
        let ids = queue
            .push_many(vec![
                unique("a", Unique::UntilFinished),
                unique("b", Unique::UntilFinished),
                unique("b", Unique::UntilFinished),
            ])
            .unwrap();
        assert_eq!(ids[0], a, "{name}");
        assert_eq!(ids[1], ids[2], "{name}");
        assert_ne!(ids[1], a, "{name}");
        assert_eq!(queue.stats().unwrap().pending(), 2, "{name}");
    }
}

#[test]
fn paused_queues_are_listed_until_resumed() {
    for (queue, _) in backends("pause") {
        let name = queue.describe();
        assert!(queue.paused_queues().unwrap().is_empty(), "{name}");
        assert!(queue.pause_queue("mailers").unwrap(), "{name}");
        assert!(
            !queue.pause_queue("mailers").unwrap(),
            "{name}: already paused"
        );
        assert!(queue.pause_queue("low").unwrap(), "{name}");
        assert_eq!(queue.paused_queues().unwrap(), ["low", "mailers"], "{name}");

        assert!(queue.resume_queue("mailers").unwrap(), "{name}");
        assert!(
            !queue.resume_queue("mailers").unwrap(),
            "{name}: not paused"
        );
        assert_eq!(queue.paused_queues().unwrap(), ["low"], "{name}");

        let bad = queue.pause_queue("../etc").unwrap_err();
        assert!(matches!(bad, butler::Error::InvalidQueue { .. }), "{name}");
    }
}

#[test]
fn a_paused_queue_still_accepts_jobs_and_promotions() {
    for (queue, _) in backends("pause-accepts") {
        let name = queue.describe();
        queue.pause_queue("default").unwrap();
        let first = queue.push("a", "default", vec![]).unwrap();
        let at = SystemTime::now() + HOUR;
        let scheduled = queue.schedule("b", "default", vec![], at).unwrap();
        assert_eq!(queue.promote(at).unwrap().moved, 1, "{name}");
        assert_eq!(queue.state(&first), Some(JobState::Pending), "{name}");
        assert_eq!(queue.state(&scheduled), Some(JobState::Pending), "{name}");
        assert_eq!(queue.stats().unwrap().pending(), 2, "{name}");

        // Resumed, its jobs are claimed in their order.
        queue.resume_queue("default").unwrap();
        assert_eq!(queue.claim("w", DEFAULT, NOW).unwrap().unwrap().id(), first);
        assert_eq!(
            queue.claim("w", DEFAULT, NOW).unwrap().unwrap().id(),
            scheduled
        );
    }
}

const MAILERS: &[&str] = &["mailers"];

fn at_most(max: usize) -> [GlobalLimit<'static>; 1] {
    [GlobalLimit {
        queue: "mailers",
        max,
    }]
}

#[test]
fn a_global_limit_caps_running_jobs_across_workers() {
    for (queue, _) in backends("global-limit") {
        let name = queue.describe();
        for n in 0..5 {
            queue.push("send", "mailers", vec![json!(n)]).unwrap();
        }
        let other = queue.push("other", "default", vec![]).unwrap();
        let limits = at_most(2);
        let claim = |worker: &str, queues: &[&str]| {
            queue
                .claim_within_limits(worker, queues, &limits, NOW)
                .unwrap()
        };

        let first = claim("w1", MAILERS).unwrap();
        claim("w2", MAILERS).unwrap();
        assert!(claim("w3", MAILERS).is_none(), "{name}: two are running");
        // A full queue doesn't hold back the others.
        let next = claim("w3", &["mailers", "default"]).unwrap();
        assert_eq!(next.id(), other, "{name}");

        // Finishing one frees its slot for any worker.
        queue.complete("w1", first, json!(null)).unwrap();
        claim("w3", MAILERS).unwrap();
        assert!(claim("w4", MAILERS).is_none(), "{name}");

        // Claims without the limit take no slot and aren't bounded by it.
        assert!(queue.claim("w5", MAILERS, NOW).unwrap().is_some(), "{name}");
        assert!(claim("w4", MAILERS).is_none(), "{name}");
    }
}

#[test]
fn every_way_out_of_processing_frees_a_global_slot() {
    for (queue, _) in backends("global-release") {
        let name = queue.describe();
        for n in 0..6 {
            queue.push("send", "mailers", vec![json!(n)]).unwrap();
        }
        let limits = at_most(1);
        let claim = || {
            queue
                .claim_within_limits("w", MAILERS, &limits, NOW)
                .unwrap()
        };
        let full = |step: &str| {
            assert!(claim().is_none(), "{name}: the slot is held before {step}");
        };

        let job = claim().unwrap();
        full("a retry");
        queue.fail("w", job, "again".into(), 3).unwrap();

        let job = claim().unwrap();
        full("a delayed retry");
        let policy = RetryPolicy::new(3, Backoff::Fixed(HOUR));
        queue.fail("w", job, "later".into(), policy).unwrap();

        let job = claim().unwrap();
        full("dying");
        queue.fail("w", job, "dead".into(), 0).unwrap();

        let job = claim().unwrap();
        full("an interruption");
        queue.interrupt("w", job).unwrap();

        let job = claim().unwrap();
        full("completing");
        queue.complete("w", job, json!(null)).unwrap();
        assert!(claim().is_some(), "{name}: free again");
    }
}

#[test]
fn recovering_a_crashed_worker_frees_its_global_slots() {
    for (queue, _) in backends("global-recover") {
        let name = queue.describe();
        for n in 0..3 {
            queue.push("send", "mailers", vec![json!(n)]).unwrap();
        }
        let limits = at_most(2);
        queue.heartbeat("crashed", Duration::from_secs(60)).unwrap();
        queue.heartbeat("alive", Duration::from_secs(60)).unwrap();
        queue
            .claim_within_limits("crashed", MAILERS, &limits, NOW)
            .unwrap()
            .unwrap();
        queue
            .claim_within_limits("crashed", MAILERS, &limits, NOW)
            .unwrap()
            .unwrap();
        assert!(
            queue
                .claim_within_limits("alive", MAILERS, &limits, NOW)
                .unwrap()
                .is_none(),
            "{name}"
        );

        queue
            .heartbeat("crashed", Duration::from_millis(1))
            .unwrap();
        thread::sleep(Duration::from_millis(50));
        assert_eq!(queue.recover().unwrap(), 2, "{name}");
        // Both slots came back.
        assert!(
            queue
                .claim_within_limits("alive", MAILERS, &limits, NOW)
                .unwrap()
                .is_some(),
            "{name}"
        );
        assert!(
            queue
                .claim_within_limits("alive", MAILERS, &limits, NOW)
                .unwrap()
                .is_some(),
            "{name}"
        );
        assert!(
            queue
                .claim_within_limits("alive", MAILERS, &limits, NOW)
                .unwrap()
                .is_none(),
            "{name}: still two at most"
        );
    }
}

#[test]
fn concurrent_claims_never_exceed_a_global_limit() {
    for (queue, _) in backends("global-race") {
        let name = queue.describe();
        for n in 0..12 {
            queue.push("send", "mailers", vec![json!(n)]).unwrap();
        }
        let claimers: Vec<_> = (0..8)
            .map(|n| {
                let queue = queue.clone();
                thread::spawn(move || {
                    queue
                        .claim_within_limits(&format!("w{n}"), MAILERS, &at_most(3), NOW)
                        .unwrap()
                        .is_some()
                })
            })
            .collect();
        let claimed = claimers
            .into_iter()
            .filter_map(|claimer| claimer.join().unwrap().then_some(()))
            .count();
        assert_eq!(claimed, 3, "{name}");
    }
}

/// A recurring schedule for job `name` every hour, as a worker registers it
/// at `now`.
fn hourly(name: &str, key: &str, now: SystemTime) -> RecurringRecord {
    Recurring::from_parts(
        name.into(),
        "reports".into(),
        vec![json!("summary")],
        Cron::parse("0 * * * *").unwrap(),
    )
    .unwrap()
    .with_key(key)
    .unwrap()
    .record(now)
}

#[test]
fn a_recurring_tick_is_enqueued_once_however_many_workers_push_it() {
    for (queue, _) in backends("recurring-once") {
        let name = queue.describe();
        let now = SystemTime::now();
        queue
            .register_recurring(&[hourly("report", "hourly", now)])
            .unwrap();
        let tick = UNIX_EPOCH + Duration::from_secs(1_790_000_000);

        // Eight workers reach the same tick at once: one of them enqueues it.
        let pushers: Vec<_> = (0..8)
            .map(|_| {
                let queue = queue.clone();
                thread::spawn(move || {
                    let job = NewJob::new("report", "reports", vec![json!("summary")]);
                    queue.push_recurring("hourly", tick, job).unwrap()
                })
            })
            .collect();
        let ids: Vec<_> = pushers
            .into_iter()
            .filter_map(|pusher| pusher.join().unwrap())
            .collect();
        assert_eq!(ids.len(), 1, "{name}: one job per tick");

        // Pending on its own queue, with its arguments, claimable once.
        assert_eq!(queue.state(&ids[0]), Some(JobState::Pending), "{name}");
        let job = queue.claim("w", &["reports"], NOW).unwrap().unwrap();
        assert_eq!(job.id(), ids[0], "{name}");
        assert_eq!(job.args(), [json!("summary")], "{name}");
        assert!(
            queue.claim("w", &["reports"], NOW).unwrap().is_none(),
            "{name}"
        );

        // Later tries of the same tick still find it taken; the next tick runs.
        let again = NewJob::new("report", "reports", vec![]);
        assert_eq!(
            queue.push_recurring("hourly", tick, again).unwrap(),
            None,
            "{name}"
        );
        let next = tick + HOUR;
        let next_id = queue
            .push_recurring("hourly", next, NewJob::new("report", "reports", vec![]))
            .unwrap();
        assert!(next_id.is_some(), "{name}");

        // The same tick of another schedule is its own.
        let other = queue
            .push_recurring("other", tick, NewJob::new("report", "reports", vec![]))
            .unwrap();
        assert!(other.is_some(), "{name}");

        let listed = queue.recurring().unwrap();
        assert_eq!(
            listed.len(),
            1,
            "{name}: only registered schedules are listed"
        );
        assert_eq!(listed[0].last_tick_ms, Some(millis(next) as u64), "{name}");
        assert_eq!(listed[0].last_job_id, next_id, "{name}");
    }
}

#[test]
fn registering_a_schedule_keeps_its_creation_time_and_last_run() {
    for (queue, _) in backends("recurring-register") {
        let name = queue.describe();
        let first = UNIX_EPOCH + Duration::from_secs(1_790_000_000);
        let stored = queue
            .register_recurring(&[hourly("report", "hourly", first)])
            .unwrap();
        assert_eq!(stored[0].created_at_ms, millis(first) as u64, "{name}");
        assert_eq!(stored[0].last_tick_ms, None, "{name}");
        let id = queue
            .push_recurring("hourly", first, NewJob::new("report", "reports", vec![]))
            .unwrap()
            .unwrap();

        // A later registration, with new arguments: the definition and
        // `seen_at_ms` change, the creation time and last run stay.
        let later = first + HOUR;
        let mut changed = hourly("report", "hourly", later);
        changed.args = vec![json!("detailed")];
        let stored = queue
            .register_recurring(&[changed, hourly("cleanup", "cleanup", later)])
            .unwrap();
        assert_eq!(stored.len(), 2, "{name}");
        assert_eq!(stored[0].key, "hourly", "{name}: in the order given");
        assert_eq!(stored[0].created_at_ms, millis(first) as u64, "{name}");
        assert_eq!(stored[0].seen_at_ms, millis(later) as u64, "{name}");
        assert_eq!(stored[0].last_tick_ms, Some(millis(first) as u64), "{name}");
        assert_eq!(
            stored[0].last_job_id.as_deref(),
            Some(id.as_str()),
            "{name}"
        );
        assert_eq!(stored[0].args, vec![json!("detailed")], "{name}");
        assert_eq!(stored[1].created_at_ms, millis(later) as u64, "{name}");

        let listed = queue.recurring().unwrap();
        let keys: Vec<_> = listed.iter().map(|s| s.key.as_str()).collect();
        assert_eq!(keys, ["cleanup", "hourly"], "{name}: sorted by key");
        assert_eq!(listed[1], stored[0], "{name}");
    }
}

#[test]
fn removing_a_schedule_forgets_it_and_its_last_run() {
    for (queue, _) in backends("recurring-remove") {
        let name = queue.describe();
        let now = SystemTime::now();
        queue
            .register_recurring(&[hourly("report", "hourly", now)])
            .unwrap();
        let tick = UNIX_EPOCH + Duration::from_secs(1_790_000_000);
        queue
            .push_recurring("hourly", tick, NewJob::new("report", "reports", vec![]))
            .unwrap()
            .unwrap();
        assert!(queue.remove_recurring("hourly").unwrap(), "{name}");
        assert!(!queue.remove_recurring("hourly").unwrap(), "{name}");
        assert!(queue.recurring().unwrap().is_empty(), "{name}");

        // Registered again, it is new: no last run.
        let stored = queue
            .register_recurring(&[hourly("report", "hourly", now)])
            .unwrap();
        assert_eq!(stored[0].last_tick_ms, None, "{name}");
    }
}

#[test]
fn removing_a_schedule_rejects_invalid_keys_without_changing_jobs() {
    for (queue, _) in backends("recurring-remove-invalid") {
        let id = queue.push("report", "default", vec![]).unwrap();
        queue
            .register_recurring(&[hourly("report", "hourly", SystemTime::now())])
            .unwrap();
        for key in [
            "",
            ".",
            "..",
            "../../pending",
            "/pending",
            "a/b",
            "a\\b",
            &"a".repeat(129),
        ] {
            assert!(
                matches!(
                    queue.remove_recurring(key),
                    Err(butler::Error::InvalidRecurringKey { .. })
                ),
                "{}: {key:?}",
                queue.describe()
            );
            assert_eq!(queue.state(&id), Some(JobState::Pending));
            assert_eq!(queue.recurring().unwrap().len(), 1);
        }
        assert_eq!(queue.claim("w", DEFAULT, NOW).unwrap().unwrap().id(), id);
    }
}

#[test]
fn a_global_limit_and_concurrency_keys_apply_together() {
    for (queue, _) in backends("global-and-keys") {
        let name = queue.describe();
        let on_mailers = |job: NewJob| NewJob {
            queue: "mailers".into(),
            ..job
        };
        let a1 = queue.push_job(on_mailers(keyed("a1", "a", 1))).unwrap();
        let a2 = queue.push_job(on_mailers(keyed("a2", "a", 1))).unwrap();
        let b1 = queue.push_job(on_mailers(keyed("b1", "b", 1))).unwrap();
        let c1 = queue.push_job(on_mailers(keyed("c1", "c", 1))).unwrap();
        let limits = at_most(2);
        let claim = |worker: &str| {
            queue
                .claim_within_limits(worker, MAILERS, &limits, NOW)
                .unwrap()
        };

        let first = claim("w1").unwrap();
        assert_eq!(first.id(), a1, "{name}");
        // a2 is skipped for its key, without taking a slot: b1 gets the second.
        assert_eq!(claim("w2").unwrap().id(), b1, "{name}");
        // Both slots are in use: c1 waits, though its key is free.
        assert!(claim("w3").is_none(), "{name}");

        // a1 finishes: its slot and its key are free, so a2 is next in line.
        queue.complete("w1", first, json!(null)).unwrap();
        assert_eq!(claim("w3").unwrap().id(), a2, "{name}");
        assert!(claim("w4").is_none(), "{name}");
        assert_eq!(queue.state(&c1), Some(JobState::Pending), "{name}");
    }
}
