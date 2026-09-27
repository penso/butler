#![allow(clippy::unwrap_used, clippy::expect_used)]

//! Worker layers (`Worker::wrap`), the per-run tracing span, `on_dead`, and
//! retry classification through wrapped errors. Each test uses its own
//! memory queue and worker; the tracing subscriber is process-wide, so spans
//! are looked up by job id.

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicU32, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use butler::{
    AnyJob, BoxError, DeadJob, Failure, JobContext, JobError, JobState, MemoryQueue, Next, Queue,
    Retry, Retryable, Worker,
};
use serde_json::{Value, json};

/// What ran, in order, per test: layers and bodies push `"<step>:<tag>"`.
static LOG: Mutex<Vec<String>> = Mutex::new(Vec::new());

fn log(entry: String) {
    LOG.lock().unwrap().push(entry);
}

fn logged(tag: &str) -> Vec<String> {
    let suffix = format!(":{tag}");
    LOG.lock()
        .unwrap()
        .iter()
        .filter_map(|entry| entry.strip_suffix(&suffix).map(str::to_owned))
        .collect()
}

#[butler::job]
async fn step(tag: String) -> Result<String, std::io::Error> {
    log(format!("body:{tag}"));
    Ok(format!("ran {tag}"))
}

/// A plain `fn` job: under `run_async` its body is on the blocking pool,
/// still inside the layers.
#[butler::job]
fn plain_step(tag: String) -> Result<String, std::io::Error> {
    log(format!("body:{tag}"));
    Ok(format!("ran {tag}"))
}

fn fresh() -> (Queue, Worker) {
    let queue: Queue = MemoryQueue::new().into();
    let worker = Worker::new(queue.clone()).poll_interval(Duration::from_millis(5));
    (queue, worker)
}

/// A layer that logs `<name>-before` and `<name>-after` around the rest.
fn logging(name: &'static str) -> impl butler::Layer {
    move |job: JobContext, next: Next| async move {
        let tag = job.args()[0].as_str().unwrap().to_owned();
        log(format!("{name}-before:{tag}"));
        let result = next.run().await;
        log(format!("{name}-after:{tag}"));
        result
    }
}

fn output(queue: &Queue, id: &str) -> Option<Value> {
    queue.get(id).unwrap().unwrap().record().result.clone()
}

#[test]
fn layers_run_in_the_order_they_were_added_around_the_job() {
    let (queue, worker) = fresh();
    let worker = worker.wrap(logging("outer")).wrap(logging("inner"));
    let id = queue.push("step", "default", vec![json!("order")]).unwrap();
    let plain = queue
        .push("plain_step", "default", vec![json!("order-plain")])
        .unwrap();
    assert_eq!(worker.drain().unwrap(), 2);

    let expected = [
        "outer-before",
        "inner-before",
        "body",
        "inner-after",
        "outer-after",
    ];
    assert_eq!(logged("order"), expected);
    assert_eq!(logged("order-plain"), expected);
    assert_eq!(output(&queue, &id), Some(json!("ran order")));
    assert_eq!(output(&queue, &plain), Some(json!("ran order-plain")));
}

#[test]
fn a_layer_can_short_circuit_with_an_output_or_an_error() {
    let dead = Arc::new(Mutex::new(Vec::<DeadJob>::new()));
    let (queue, worker) = fresh();
    let worker = worker
        .wrap(logging("outer"))
        .wrap(|job: JobContext, next: Next| async move {
            match job.args()[0].as_str() {
                Some("skip") => Ok(json!("skipped")),
                Some("refuse") => Err(JobError::Failed(Failure::new(
                    BoxError::from("refused by a layer"),
                    Retry::Never,
                ))),
                _ => next.run().await,
            }
        })
        .wrap(logging("never"))
        .on_dead({
            let dead = Arc::clone(&dead);
            move |job: DeadJob| {
                dead.lock().unwrap().push(job);
                async {}
            }
        });
    let skipped = queue.push("step", "default", vec![json!("skip")]).unwrap();
    let refused = queue
        .push("step", "default", vec![json!("refuse")])
        .unwrap();
    assert_eq!(worker.drain().unwrap(), 2);

    // The outer layer saw both; nothing inside the short-circuit ran.
    assert_eq!(logged("skip"), ["outer-before", "outer-after"]);
    assert_eq!(logged("refuse"), ["outer-before", "outer-after"]);
    assert_eq!(queue.state(&skipped), Some(JobState::Done));
    assert_eq!(output(&queue, &skipped), Some(json!("skipped")));

    assert_eq!(queue.state(&refused), Some(JobState::Dead));
    let dead = dead.lock().unwrap();
    assert_eq!(dead.len(), 1);
    assert_eq!(dead[0].job().id(), refused);
    assert_eq!(dead[0].job().error(), "refused by a layer");
    assert!(dead[0].discarded());
}

static FLAKY_RUNS: AtomicU32 = AtomicU32::new(0);

/// Fails its first attempt only.
#[butler::job(queue = "flaky", retries = 3, backoff = "fixed:0s")]
async fn flaky_once() -> Result<(), std::io::Error> {
    if FLAKY_RUNS.fetch_add(1, Ordering::SeqCst) == 0 {
        return Err(std::io::Error::other("first attempt fails"));
    }
    Ok(())
}

#[test]
fn a_layer_sees_the_job_and_its_retry_state() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let (queue, worker) = fresh();
    let worker = worker
        .queues(butler::QueuePriority::strict(["flaky"]))
        .wrap({
            let seen = Arc::clone(&seen);
            move |job: JobContext, next: Next| {
                seen.lock().unwrap().push(job.clone());
                next.run()
            }
        });
    let mut new = butler::NewJob::new("flaky_once", "flaky", vec![]);
    new.meta.insert("tenant".into(), json!("acme"));
    let id = queue.push_job(new).unwrap();
    assert_eq!(worker.drain().unwrap(), 2);

    let seen = seen.lock().unwrap();
    let [first, second] = &seen[..] else {
        panic!("expected two attempts, saw {seen:?}");
    };
    for job in [first, second] {
        assert_eq!(
            (job.id(), job.name(), job.queue()),
            (id.as_str(), "flaky_once", "flaky")
        );
        assert_eq!(job.meta().get("tenant"), Some(&json!("acme")));
        assert_eq!(job.retry_policy().max_retries, 3);
        assert_eq!(job.worker_id(), worker.id());
        assert!(job.enqueued_at() <= SystemTime::now());
    }
    assert_eq!((first.attempt(), first.last_error()), (1, None));
    assert_eq!(
        (second.attempt(), second.last_error()),
        (2, Some("first attempt fails"))
    );
    assert!(!second.is_last_attempt());
}

#[derive(Debug, thiserror::Error)]
enum SyncError {
    #[error("account {0} is gone")]
    Gone(u64),
    #[error("rate limited")]
    RateLimited,
}

impl Retryable for SyncError {
    fn retry(&self) -> Retry {
        match self {
            SyncError::Gone(_) => Retry::Never,
            SyncError::RateLimited => Retry::After(Duration::from_secs(90)),
        }
    }
}

butler::retryable!(SyncError);

/// Not `Retryable` itself: it only wraps one that is.
#[derive(Debug, thiserror::Error)]
#[error("syncing failed")]
struct Wrapper(#[source] SyncError);

#[butler::job(retries = 5, backoff = "fixed:1h")]
async fn boxed_gone(id: u64) -> Result<(), BoxError> {
    Err(SyncError::Gone(id).into())
}

#[butler::job(retries = 5, backoff = "fixed:1h")]
async fn anyhow_gone(id: u64) -> anyhow::Result<()> {
    Err(anyhow::Error::new(SyncError::Gone(id)).context("while syncing"))
}

#[butler::job(retries = 5, backoff = "fixed:1h")]
async fn wrapped_gone(id: u64) -> Result<(), Wrapper> {
    Err(Wrapper(SyncError::Gone(id)))
}

#[butler::job(retries = 5, backoff = "fixed:1h")]
async fn boxed_rate_limited() -> Result<(), BoxError> {
    Err(SyncError::RateLimited.into())
}

#[butler::job(retries = 5, backoff = "fixed:1h")]
async fn boxed_unregistered() -> Result<(), BoxError> {
    Err("nothing to classify".into())
}

#[test]
fn retry_classification_looks_through_wrapped_errors() {
    let (queue, worker) = fresh();
    let dead = Arc::new(Mutex::new(Vec::<DeadJob>::new()));
    let worker = worker.on_dead({
        let dead = Arc::clone(&dead);
        move |job: DeadJob| {
            dead.lock().unwrap().push(job);
            async {}
        }
    });
    let gone: Vec<_> = ["boxed_gone", "anyhow_gone", "wrapped_gone"]
        .into_iter()
        .map(|name| queue.push(name, "default", vec![json!(7)]).unwrap())
        .collect();
    let limited = queue.push("boxed_rate_limited", "default", vec![]).unwrap();
    let plain = queue.push("boxed_unregistered", "default", vec![]).unwrap();
    let before = SystemTime::now();
    // Each runs once: the dead ones never retry, the others wait.
    assert_eq!(worker.drain().unwrap(), 5);

    for id in &gone {
        assert_eq!(queue.state(id), Some(JobState::Dead), "{id}");
    }
    let dead = dead.lock().unwrap();
    assert_eq!(dead.len(), 3);
    for job in dead.iter() {
        assert!(job.discarded(), "{:?}", job.job().name());
        assert_eq!(job.job().attempts(), 1);
    }

    let retry_in = |id: &str| {
        let Some(AnyJob::Scheduled(job)) = queue.get(id).unwrap() else {
            panic!("{id} should wait for a retry");
        };
        job.run_at().duration_since(before).unwrap()
    };
    // Retry::After(90s) from the boxed error, rather than the 1h backoff.
    let limited = retry_in(&limited);
    assert!(limited >= Duration::from_secs(89) && limited < Duration::from_secs(120));
    // No registered type in the chain: the job's own backoff.
    assert!(retry_in(&plain) >= Duration::from_secs(3599));
}

#[butler::job(retries = 1, backoff = "fixed:0s")]
async fn always_fails(tag: String) -> Result<(), std::io::Error> {
    Err(std::io::Error::other(format!("{tag} failed")))
}

#[butler::job(retries = 9)]
async fn never_again() -> Result<(), SyncError> {
    Err(SyncError::Gone(1))
}

/// Collects dead jobs through a channel.
fn dead_channel(worker: Worker) -> (Worker, std::sync::mpsc::Receiver<DeadJob>) {
    let (tx, rx) = std::sync::mpsc::channel();
    let worker = worker.on_dead(move |job: DeadJob| {
        tx.send(job).unwrap();
        async {}
    });
    (worker, rx)
}

#[test]
fn on_dead_fires_for_exhausted_retries_and_retry_never_on_threads() {
    let (queue, worker) = fresh();
    let (worker, dead) = dead_channel(worker);
    let exhausted = queue
        .push("always_fails", "default", vec![json!("threads")])
        .unwrap();
    let never = queue.push("never_again", "default", vec![]).unwrap();
    // Two attempts of the first, one of the second.
    assert_eq!(worker.drain().unwrap(), 3);

    let first = dead.try_recv().unwrap();
    let second = dead.try_recv().unwrap();
    assert!(dead.try_recv().is_err(), "each death fires once");
    let (exhausted_job, never_job) = if first.job().id() == exhausted {
        (first, second)
    } else {
        (second, first)
    };
    assert_eq!(exhausted_job.job().attempts(), 2);
    assert!(!exhausted_job.discarded());
    assert_eq!(exhausted_job.job().error(), "threads failed");
    assert_eq!(never_job.job().id(), never);
    assert!(never_job.discarded());
    let JobError::Failed(failure) = never_job.error() else {
        panic!("the job's own error");
    };
    assert_eq!(failure.retry(), Retry::Never);
}

#[cfg(feature = "tokio")]
mod under_tokio {
    use super::*;

    /// Collects dead jobs through a channel the test can await.
    fn dead_channel(worker: Worker) -> (Worker, tokio::sync::mpsc::UnboundedReceiver<DeadJob>) {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let worker = worker.on_dead(move |job: DeadJob| {
            let tx = tx.clone();
            async move {
                tx.send(job).unwrap();
            }
        });
        (worker, rx)
    }

    async fn run_until_idle(worker: Worker, until: impl Future<Output = ()>) {
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let running = tokio::spawn(worker.run_async(async {
            let _ = stopped.await;
        }));
        tokio::time::timeout(Duration::from_secs(10), until)
            .await
            .expect("timed out");
        stop.send(()).unwrap();
        running.await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn layers_wrap_async_and_plain_jobs_under_run_async() {
        let (queue, worker) = fresh();
        let worker = worker.wrap(logging("outer")).wrap(logging("inner"));
        let async_job = queue.push("step", "default", vec![json!("tokio")]).unwrap();
        let plain_job = queue
            .push("plain_step", "default", vec![json!("tokio-plain")])
            .unwrap();
        let waiting = queue.clone();
        run_until_idle(worker, async move {
            for id in [&async_job, &plain_job] {
                waiting
                    .handle(id.as_str())
                    .wait(Duration::from_millis(5))
                    .await
                    .unwrap();
            }
        })
        .await;
        let expected = [
            "outer-before",
            "inner-before",
            "body",
            "inner-after",
            "outer-after",
        ];
        assert_eq!(logged("tokio"), expected);
        assert_eq!(logged("tokio-plain"), expected);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn on_dead_fires_for_exhausted_retries_and_retry_never_under_run_async() {
        let (queue, worker) = fresh();
        let (worker, mut dead) = dead_channel(worker);
        let exhausted = queue
            .push("always_fails", "default", vec![json!("tokio")])
            .unwrap();
        let never = queue.push("never_again", "default", vec![]).unwrap();
        let mut ids = Vec::new();
        run_until_idle(worker, async {
            for _ in 0..2 {
                let job = dead.recv().await.unwrap();
                ids.push((job.job().id().to_owned(), job.discarded()));
            }
        })
        .await;
        ids.sort();
        let mut expected = vec![(exhausted, false), (never, true)];
        expected.sort();
        assert_eq!(ids, expected);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_panicking_layer_fails_the_attempt_and_the_worker_goes_on() {
        let (queue, worker) = fresh();
        let (worker, mut dead) = dead_channel(worker);
        let worker = worker.wrap(|job: JobContext, next: Next| async move {
            if job.args().first() == Some(&json!("panic")) {
                panic!("layer panicked");
            }
            next.run().await
        });
        let panics = queue.push("never_again", "default", vec![]).unwrap();
        queue.push("step", "default", vec![json!("panic")]).unwrap();
        let fine = queue.push("step", "default", vec![json!("fine")]).unwrap();
        let waiting = queue.clone();
        run_until_idle(worker, async move {
            waiting
                .handle(fine.as_str())
                .wait(Duration::from_millis(5))
                .await
                .unwrap();
            dead.recv().await.unwrap();
        })
        .await;
        assert_eq!(queue.state(&panics), Some(JobState::Dead));
        assert_eq!(logged("fine"), ["body"]);
    }
}

/// The built-in span: a process-wide subscriber that keeps the fields of
/// every `job` span, by span.
mod spans {
    use std::{
        collections::HashMap,
        fmt,
        sync::{
            Mutex, Once,
            atomic::{AtomicU64, Ordering},
        },
    };

    use tracing::{
        Event, Metadata, Subscriber,
        field::{Field, Visit},
        span,
    };

    pub type Fields = HashMap<String, String>;

    static SPANS: Mutex<Vec<(u64, Fields)>> = Mutex::new(Vec::new());
    static NEXT: AtomicU64 = AtomicU64::new(1);

    struct Recorder;

    struct Collect<'a>(&'a mut Fields);

    impl Visit for Collect<'_> {
        fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
            self.0.insert(field.name().to_owned(), format!("{value:?}"));
        }

        fn record_str(&mut self, field: &Field, value: &str) {
            self.0.insert(field.name().to_owned(), value.to_owned());
        }
    }

    impl Subscriber for Recorder {
        fn enabled(&self, _: &Metadata<'_>) -> bool {
            true
        }

        fn new_span(&self, span: &span::Attributes<'_>) -> span::Id {
            let id = NEXT.fetch_add(1, Ordering::Relaxed);
            if span.metadata().name() == "job" {
                let mut fields = Fields::new();
                span.record(&mut Collect(&mut fields));
                SPANS.lock().unwrap().push((id, fields));
            }
            span::Id::from_u64(id)
        }

        fn record(&self, span: &span::Id, values: &span::Record<'_>) {
            let mut spans = SPANS.lock().unwrap();
            if let Some((_, fields)) = spans.iter_mut().find(|(id, _)| *id == span.into_u64()) {
                values.record(&mut Collect(fields));
            }
        }

        fn record_follows_from(&self, _: &span::Id, _: &span::Id) {}
        fn event(&self, _: &Event<'_>) {}
        fn enter(&self, _: &span::Id) {}
        fn exit(&self, _: &span::Id) {}
    }

    pub fn install() {
        static ONCE: Once = Once::new();
        ONCE.call_once(|| tracing::subscriber::set_global_default(Recorder).unwrap());
    }

    /// Every run of job `id`, oldest first.
    pub fn of(id: &str) -> Vec<Fields> {
        SPANS
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, fields)| fields.get("id").map(String::as_str) == Some(id))
            .map(|(_, fields)| fields.clone())
            .collect()
    }
}

#[butler::job(queue = "spans", retries = 1, backoff = "fixed:1h")]
async fn traced(fail: bool) -> Result<(), std::io::Error> {
    if fail {
        return Err(std::io::Error::other("traced failure"));
    }
    Ok(())
}

#[test]
fn every_run_has_a_span_with_the_job_and_its_outcome() {
    spans::install();
    let (queue, worker) = fresh();
    let worker = worker.queues(butler::QueuePriority::strict(["spans"]));
    let ok = queue.push("traced", "spans", vec![json!(false)]).unwrap();
    let failing = queue.push("traced", "spans", vec![json!(true)]).unwrap();
    let before = SystemTime::now();
    assert_eq!(worker.drain().unwrap(), 2);

    let [done] = &spans::of(&ok)[..] else {
        panic!("one run of {ok}");
    };
    let get = |fields: &spans::Fields, key: &str| fields.get(key).cloned().unwrap_or_default();
    assert_eq!(get(done, "name"), "traced");
    assert_eq!(get(done, "queue"), "spans");
    assert_eq!(get(done, "attempt"), "1");
    assert_eq!(get(done, "worker"), worker.id());
    assert_eq!(get(done, "outcome"), "done");
    assert!(!done.contains_key("retry_at_ms"));

    // First attempt failed: the span says when the next one is.
    let [retry] = &spans::of(&failing)[..] else {
        panic!("one run of {failing}");
    };
    assert_eq!(get(retry, "outcome"), "retry");
    let retry_at: u128 = get(retry, "retry_at_ms").parse().unwrap();
    let hour_later = (before + Duration::from_secs(3600))
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    assert!(retry_at >= hour_later, "{retry_at} < {hour_later}");
    let Some(AnyJob::Scheduled(waiting)) = queue.get(&failing).unwrap() else {
        panic!("waits for its retry");
    };
    assert_eq!(
        retry_at,
        waiting
            .run_at()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );

    // Its second and last attempt, forced due now, dies.
    queue.run_now(&failing).unwrap();
    assert_eq!(worker.drain().unwrap(), 1);
    let runs = spans::of(&failing);
    assert_eq!(runs.len(), 2);
    assert_eq!(get(&runs[1], "attempt"), "2");
    assert_eq!(get(&runs[1], "outcome"), "dead");
}
