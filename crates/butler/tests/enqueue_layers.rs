#![cfg(feature = "tokio")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

//! Enqueue layers (`butler::configure_enqueue`): metadata and vetoes on
//! every enqueue path. Its own test binary: enqueue layers and
//! `butler::configure` are process-wide, so every test installs the same
//! layers and queue once, and looks only at its own jobs.

use std::sync::{Mutex, Once};

use butler::{
    Error, JobContext, JobState, MemoryQueue, NewJob, Next, Queue, Worker, monitor::ListFilter,
    testing::perform_enqueued_jobs,
};
use serde_json::{Value, json};

#[derive(Debug, thiserror::Error)]
#[error("forbidden by policy")]
struct Forbidden;

#[butler::job]
async fn note(n: u32) -> Result<u32, std::io::Error> {
    Ok(n)
}

#[butler::job]
async fn forbidden() {}

#[butler::job]
async fn reroute() {}

/// Only enqueued next to a vetoed job, so it must never be stored.
#[butler::job]
async fn batch_mate() {}

#[butler::job]
async fn bad_route() {}

fn queue() -> Queue {
    static QUEUE: Mutex<Option<Queue>> = Mutex::new(None);
    static LAYERS: Once = Once::new();
    let queue = QUEUE
        .lock()
        .unwrap()
        .get_or_insert_with(|| {
            let queue: Queue = MemoryQueue::new().into();
            butler::configure(queue.clone());
            queue
        })
        .clone();
    LAYERS.call_once(|| {
        butler::configure_enqueue(|job: &mut NewJob| {
            if job.name == "forbidden" {
                return Err(Forbidden);
            }
            job.meta.insert("order".into(), json!(["first"]));
            job.meta.insert("tenant".into(), json!("acme"));
            Ok(())
        });
        butler::configure_enqueue(|job: &mut NewJob| {
            if let Some(Value::Array(order)) = job.meta.get_mut("order") {
                order.push(json!("second"));
            }
            match job.name.as_str() {
                "reroute" => job.queue = "rerouted".into(),
                "bad_route" => job.queue = ".hidden".into(),
                _ => {}
            }
            Ok::<_, butler::BoxError>(())
        });
    });
    queue
}

fn meta(queue: &Queue, id: &str) -> Value {
    Value::Object(queue.get(id).unwrap().unwrap().record().meta.clone())
}

fn stored_named(queue: &Queue, name: &str) -> usize {
    JobState::ALL
        .into_iter()
        .flat_map(|state| queue.list(&ListFilter::new(state)).unwrap())
        .filter(|job| job.record().name == name)
        .count()
}

fn expected() -> Value {
    json!({ "order": ["first", "second"], "tenant": "acme" })
}

#[tokio::test]
async fn layers_add_metadata_in_order_on_every_enqueue_path() {
    let queue = queue();
    let called = note(1).await.unwrap();
    let prepared = note::prepare(2u32).unwrap().enqueue().await.unwrap();
    let scheduled = note::prepare(3u32)
        .unwrap()
        .run_in(std::time::Duration::from_secs(3600))
        .enqueue()
        .await
        .unwrap();
    let batch = butler::enqueue_all([note::prepare(4u32).unwrap(), note::prepare(5u32).unwrap()])
        .await
        .unwrap();

    for id in [
        called.id(),
        prepared.id(),
        scheduled.id(),
        batch[0].id(),
        batch[1].id(),
    ] {
        assert_eq!(meta(&queue, id), expected(), "{id}");
    }
    assert_eq!(queue.state(scheduled.id()), Some(JobState::Scheduled));
}

#[tokio::test]
async fn a_veto_stores_nothing_and_says_why() {
    let queue = queue();
    let err = forbidden().await.unwrap_err();
    let Error::Vetoed { name, source } = &err else {
        panic!("expected a veto, got {err:?}");
    };
    assert_eq!(name, "forbidden");
    assert!(source.downcast_ref::<Forbidden>().is_some());

    assert!(matches!(
        forbidden::prepare().unwrap().enqueue().await,
        Err(Error::Vetoed { .. })
    ));
    assert_eq!(stored_named(&queue, "forbidden"), 0);
}

#[tokio::test]
async fn one_veto_fails_the_whole_batch() {
    let queue = queue();
    let batch = [
        batch_mate::prepare().unwrap().untyped(),
        forbidden::prepare().unwrap().untyped(),
    ];
    let err = butler::enqueue_all(batch).await.unwrap_err();
    assert!(matches!(err, Error::Vetoed { ref name, .. } if name == "forbidden"));
    assert_eq!(stored_named(&queue, "batch_mate"), 0, "nothing stored");
}

#[tokio::test]
async fn a_layer_can_move_a_job_to_another_valid_queue() {
    let queue = queue();
    let job = reroute().await.unwrap();
    assert_eq!(
        queue.get(job.id()).unwrap().unwrap().record().queue,
        "rerouted"
    );

    let err = bad_route().await.unwrap_err();
    assert!(
        matches!(err, Error::InvalidQueue { ref name, .. } if name == ".hidden"),
        "{err:?}"
    );
    assert_eq!(stored_named(&queue, "bad_route"), 0);
}

#[tokio::test]
async fn vetoes_apply_in_inline_test_mode_too() {
    queue();
    perform_enqueued_jobs(async {
        assert!(matches!(forbidden().await, Err(Error::Vetoed { .. })));
        let job = note(9).await.unwrap();
        assert_eq!(job.result().await.unwrap(), Some(9));
    })
    .await;
}

#[test]
fn worker_layers_read_what_enqueue_layers_stored() {
    let queue = queue();
    let job = butler::block_on(note(7)).unwrap();
    let seen = std::sync::Arc::new(Mutex::new(None));
    let worker = Worker::new(queue.clone()).wrap({
        let seen = std::sync::Arc::clone(&seen);
        let id = job.id().to_owned();
        move |context: JobContext, next: Next| {
            if context.id() == id {
                *seen.lock().unwrap() = context.meta().get("tenant").cloned();
            }
            next.run()
        }
    });
    while queue.state(job.id()) != Some(JobState::Done) {
        assert!(worker.work_one().unwrap(), "the job is still queued");
    }
    assert_eq!(*seen.lock().unwrap(), Some(json!("acme")));
}
