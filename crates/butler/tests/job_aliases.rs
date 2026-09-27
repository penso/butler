#![allow(clippy::unwrap_used, clippy::expect_used)]

//! Renamed jobs: `#[job(aliases = [...])]` keeps jobs queued under earlier
//! names running, new jobs are enqueued under the current name, and no two
//! jobs in a worker may answer to the same name.

use butler::{Error, JobDef, JobState, MemoryQueue, Queue, RecurringConfig, Worker};
use serde_json::json;

/// Was `charge`, then `billing.charge`; now `billing.charge_v2`.
#[butler::job(name = "billing.charge_v2", aliases = ["charge", "billing.charge"])]
async fn charge(cents: i64) -> Result<i64, std::io::Error> {
    Ok(cents * 2)
}

#[butler::job]
async fn refund(cents: i64) -> Result<i64, std::io::Error> {
    Ok(-cents)
}

fn result(queue: &Queue, id: &str) -> serde_json::Value {
    let job = queue.get(id).unwrap().unwrap();
    assert_eq!(job.state(), JobState::Done, "{id}");
    job.record().result.clone().unwrap()
}

#[test]
fn jobs_queued_under_an_old_name_still_run() {
    let queue: Queue = MemoryQueue::new().into();
    let oldest = queue.push("charge", "default", vec![json!(1)]).unwrap();
    let older = queue
        .push("billing.charge", "default", vec![json!(2)])
        .unwrap();
    let current = queue
        .push("billing.charge_v2", "default", vec![json!(3)])
        .unwrap();

    let worker = Worker::new(queue.clone());
    assert_eq!(worker.drain().unwrap(), 3);
    assert_eq!(result(&queue, &oldest), json!(2));
    assert_eq!(result(&queue, &older), json!(4));
    assert_eq!(result(&queue, &current), json!(6));
    // Each keeps the name it was queued under.
    assert_eq!(queue.get(&oldest).unwrap().unwrap().record().name, "charge");
}

#[test]
fn new_jobs_use_the_current_name_and_workers_list_only_it() {
    assert_eq!(charge::JOB.name, "billing.charge_v2");
    assert_eq!(charge::JOB.aliases, ["charge", "billing.charge"]);
    assert_eq!(
        charge::JOB.names().collect::<Vec<_>>(),
        ["billing.charge_v2", "charge", "billing.charge"]
    );
    let prepared = charge::prepare(5).unwrap();
    assert_eq!(prepared.name(), "billing.charge_v2");

    let names = Worker::new(MemoryQueue::new()).job_names();
    assert!(names.contains(&"billing.charge_v2"));
    assert!(!names.contains(&"charge"), "aliases aren't separate jobs");
    assert!(!names.contains(&"billing.charge"));
}

#[test]
fn a_recurring_schedule_may_name_a_job_by_its_alias() {
    let entry = RecurringConfig {
        job: "charge".into(),
        cron: "0 3 * * *".into(),
        args: vec![json!(1)],
        queue: None,
        timezone: None,
        key: None,
    };
    let worker = Worker::new(MemoryQueue::new())
        .with_recurring_config(&[entry])
        .unwrap();
    assert_eq!(worker.recurring_schedules()[0].name(), "billing.charge_v2");
}

#[test]
fn registering_the_same_job_again_is_fine() {
    let queue: Queue = MemoryQueue::new().into();
    let id = queue.push("charge", "default", vec![json!(7)]).unwrap();
    let worker = Worker::new(queue.clone())
        .register(charge::JOB)
        .register(charge::JOB);
    worker.drain().unwrap();
    assert_eq!(result(&queue, &id), json!(14));
}

#[test]
fn check_names_rejects_what_the_macro_rejects() {
    let valid = JobDef {
        aliases: &["a", "b"],
        ..charge::JOB
    };
    valid.check_names().unwrap();
    for (aliases, reason) in [
        (&[""][..], "job names and aliases can't be empty"),
        (
            &["billing.charge_v2"][..],
            "an alias repeats the job's name",
        ),
        (&["a", "a"][..], "an alias is listed twice"),
    ] {
        let def = JobDef {
            aliases,
            ..charge::JOB
        };
        let err = def.check_names().unwrap_err();
        assert!(
            matches!(err, Error::InvalidJobName { reason: r, .. } if r == reason),
            "{aliases:?}: {err:?}"
        );
    }
    let unnamed = JobDef {
        name: "",
        aliases: &[],
        ..charge::JOB
    };
    assert!(matches!(
        unnamed.check_names(),
        Err(Error::InvalidJobName { .. })
    ));
}

/// The panic message of `register`ing `def` on a fresh worker.
fn register_panic(def: JobDef) -> String {
    let panic = std::panic::catch_unwind(|| Worker::new(MemoryQueue::new()).register(def))
        .err()
        .expect("register panics");
    panic.downcast_ref::<String>().cloned().unwrap_or_default()
}

#[test]
fn two_jobs_cant_answer_to_the_same_name() {
    // Another job taking this one's alias,
    let message = register_panic(JobDef {
        aliases: &["charge"],
        ..refund::JOB
    });
    assert_eq!(
        message,
        "butler: jobs `refund` and `billing.charge_v2` both answer to `charge`"
    );
    // or its name as an alias,
    let message = register_panic(JobDef {
        aliases: &["billing.charge_v2"],
        ..refund::JOB
    });
    assert!(
        message.contains("both answer to `billing.charge_v2`"),
        "{message}"
    );
    // or another job's name as its own.
    let message = register_panic(JobDef {
        name: "billing.charge",
        aliases: &[],
        ..refund::JOB
    });
    assert!(
        message.contains("both answer to `billing.charge`"),
        "{message}"
    );
    // Invalid names are refused too.
    let message = register_panic(JobDef {
        aliases: &["refund"],
        ..refund::JOB
    });
    assert!(
        message.contains("an alias repeats the job's name"),
        "{message}"
    );
}

#[test]
fn a_rejected_registration_changes_nothing() {
    let queue: Queue = MemoryQueue::new().into();
    let id = queue.push("refund", "default", vec![json!(3)]).unwrap();
    let conflicting = JobDef {
        aliases: &["charge"],
        ..refund::JOB
    };
    let worker = Worker::new(queue.clone());
    let attempt = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        worker.clone().register(conflicting)
    }));
    assert!(attempt.is_err());
    worker.drain().unwrap();
    assert_eq!(result(&queue, &id), json!(-3), "refund is still refund");
}
