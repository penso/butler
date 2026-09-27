#![allow(clippy::unwrap_used, clippy::expect_used)]

//! Recurring jobs through the worker: which ticks it enqueues when it
//! starts, and how `[[recurring]]` entries become schedules. That each tick
//! is enqueued once per backend is in `backends.rs`.

use std::{
    sync::Mutex,
    thread,
    time::{Duration, SystemTime},
};

use butler::{Error, MemoryQueue, Queue, Recurring, RecurringConfig, Worker};

static RUNS: Mutex<Vec<String>> = Mutex::new(Vec::new());

#[butler::job(queue = "reports")]
fn yearly_report(label: String) {
    RUNS.lock().unwrap().push(label);
}

fn runs(label: &str) -> usize {
    RUNS.lock()
        .unwrap()
        .iter()
        .filter(|run| *run == label)
        .count()
}

/// Every January 1st at midnight UTC: the latest tick is always months or
/// hours ago, never about to change while a test runs (except at New Year).
const NEW_YEAR: &str = "0 0 1 1 *";

fn schedule(label: &str) -> Recurring {
    Recurring::new(yearly_report::prepare(label).unwrap(), NEW_YEAR).unwrap()
}

#[test]
fn a_tick_missed_during_downtime_runs_once_late_across_workers() {
    let queue: Queue = MemoryQueue::new().into();
    let schedule = schedule("missed");
    // Registered two years ago, and never run since: this year's tick was
    // missed while no worker ran.
    let two_years_ago = SystemTime::now() - Duration::from_secs(2 * 366 * 24 * 3600);
    queue
        .register_recurring(&[schedule.record(two_years_ago)])
        .unwrap();

    let workers: Vec<_> = (0..3)
        .map(|_| {
            let worker = Worker::new(queue.clone())
                .queues(butler::QueuePriority::strict(["reports"]))
                .add_recurring(schedule.clone())
                .unwrap();
            thread::spawn(move || worker.drain().unwrap())
        })
        .collect();
    let ran: usize = workers.into_iter().map(|w| w.join().unwrap()).sum();
    assert_eq!(
        ran, 1,
        "one late run for the missed tick, not one per worker"
    );
    assert_eq!(runs("missed"), 1);

    let stored = &queue.recurring().unwrap()[0];
    assert_eq!(stored.key, schedule.key());
    let tick = schedule.cron().latest_until(SystemTime::now()).unwrap();
    assert_eq!(stored.last_tick_ms, Some(ms(tick)));
    assert!(stored.last_job_id.is_some());

    // A worker starting later finds this tick ran: nothing to catch up.
    let late = Worker::new(queue.clone())
        .queues(butler::QueuePriority::strict(["reports"]))
        .add_recurring(schedule)
        .unwrap();
    assert_eq!(late.drain().unwrap(), 0);
    assert_eq!(runs("missed"), 1);
}

#[test]
fn a_new_schedule_does_not_run_ticks_from_before_it_existed() {
    let queue: Queue = MemoryQueue::new().into();
    let worker = Worker::new(queue.clone())
        .queues(butler::QueuePriority::strict(["reports"]))
        .recurring(yearly_report::prepare("new").unwrap(), NEW_YEAR)
        .unwrap();
    assert_eq!(worker.drain().unwrap(), 0);
    assert_eq!(runs("new"), 0);

    // Registered, with no run yet, and its next run on January 1st.
    let stored = &queue.recurring().unwrap()[0];
    assert_eq!(stored.last_tick_ms, None);
    assert!(stored.is_active(SystemTime::now()));
    let next = stored.next_run(SystemTime::now()).unwrap();
    assert_eq!(
        Some(next),
        worker.recurring_schedules()[0]
            .cron()
            .next_after(SystemTime::now())
    );
}

#[test]
fn recurring_entries_resolve_their_job_and_queue() {
    let entry = |job: &str| RecurringConfig {
        job: job.into(),
        cron: "0 3 * * *".into(),
        args: vec![serde_json::json!("from config")],
        queue: None,
        timezone: Some("Europe/Paris".into()),
        key: None,
    };
    let worker = Worker::new(MemoryQueue::new())
        .with_recurring_config(&[entry("yearly_report")])
        .unwrap();
    let schedule = &worker.recurring_schedules()[0];
    assert_eq!(schedule.name(), "yearly_report");
    assert_eq!(schedule.queue(), "reports", "the job's own queue");
    assert_eq!(schedule.cron().time_zone(), "Europe/Paris");
    assert_eq!(schedule.args(), [serde_json::json!("from config")]);

    let on_low = RecurringConfig {
        queue: Some("low".into()),
        ..entry("yearly_report")
    };
    let worker = Worker::new(MemoryQueue::new())
        .with_recurring_config(&[on_low])
        .unwrap();
    assert_eq!(worker.recurring_schedules()[0].queue(), "low");

    let unknown = Worker::new(MemoryQueue::new())
        .with_recurring_config(&[entry("no_such_job")])
        .err()
        .unwrap();
    assert!(
        matches!(unknown, Error::UnknownRecurringJob { ref name, .. } if name == "no_such_job"),
        "{unknown:?}"
    );

    let twice = Worker::new(MemoryQueue::new())
        .with_recurring_config(&[entry("yearly_report"), entry("yearly_report")])
        .err()
        .unwrap();
    assert!(
        matches!(twice, Error::DuplicateRecurring { .. }),
        "{twice:?}"
    );
}

fn ms(at: SystemTime) -> u64 {
    at.duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
