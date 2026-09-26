#![allow(clippy::unwrap_used, clippy::expect_used)]

//! The `Backend` contract, checked the same way against every backend: memory
//! and file always, Redis when a server is reachable. These tests talk to the
//! queue directly, never through `butler::configure`, so they run in parallel.

use std::{
    path::PathBuf,
    sync::atomic::{AtomicU32, Ordering},
    thread,
    time::{Duration, Instant},
};

use butler::{Failed, FileQueue, JobState, MemoryQueue, NewJob, Queue};
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
                NewJob {
                    name: "a".into(),
                    queue: "default".into(),
                    args: vec![json!(1)],
                },
                NewJob {
                    name: "b".into(),
                    queue: "low".into(),
                    args: vec![json!(2)],
                },
                NewJob {
                    name: "c".into(),
                    queue: "default".into(),
                    args: vec![json!(3)],
                },
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
