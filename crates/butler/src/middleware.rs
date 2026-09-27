//! Worker layers, like ActiveJob's `around_perform` and Sidekiq's server
//! middleware: code that runs around every job a worker runs, and the
//! [`on_dead`](crate::Worker::on_dead) hook for jobs that die.
//!
//! ```ignore
//! let worker = Worker::from_config(&config)?
//!     .wrap(|job: JobContext, next: Next| async move {
//!         let tenant = job.meta().get("tenant").cloned();
//!         let started = Instant::now();
//!         let result = with_tenant(tenant, next.run()).await;
//!         metrics::histogram!("job", "name" => job.name().to_owned()).record(started.elapsed());
//!         result
//!     })
//!     .on_dead(|dead: DeadJob| async move {
//!         pager::alert(format!("{} died: {}", dead.job().name(), dead.job().error())).await;
//!     });
//! ```

use std::{
    fmt,
    future::Future,
    pin::Pin,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde_json::{Map, Value};

use crate::{
    JobError, JobRecord, Retry, RetryPolicy,
    job::{Job, state::Dead},
    progress::Invocation,
};

/// A job run in progress, as layers see it: its output as JSON, or why it
/// failed.
pub type RunFuture = Pin<Box<dyn Future<Output = Result<Value, JobError>> + Send + 'static>>;

/// Code that runs around every job a [`Worker`](crate::Worker) runs. Add it
/// with [`Worker::wrap`](crate::Worker::wrap).
///
/// A layer gets the job and the [`Next`] step. It can act before and after
/// `next.run().await`, change the result, or short-circuit: return without
/// calling `next`, and the job's body never runs. An `Ok` then counts as
/// the job's output (`Value::Null` for a job that returns nothing); an `Err`
/// is a failed attempt, retried or dead like any other. To skip a job for
/// good, return `JobError::Failed(Failure::new(error, Retry::Never))`.
///
/// It is implemented for closures and functions
/// `Fn(JobContext, Next) -> impl Future<Output = Result<Value, JobError>>`,
/// as long as they and their futures are `Send + 'static`. Layers run in
/// the job's task under `run_async` and on the job's thread under `run`, so
/// a layer must not hold a synchronous lock across an `.await`.
pub trait Layer: Send + Sync + 'static {
    fn run(&self, job: JobContext, next: Next) -> RunFuture;
}

impl<F, Fut> Layer for F
where
    F: Fn(JobContext, Next) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Value, JobError>> + Send + 'static,
{
    fn run(&self, job: JobContext, next: Next) -> RunFuture {
        Box::pin(self(job, next))
    }
}

/// The job a layer runs around: everything the worker knows about it
/// before this attempt. Cheap to clone.
#[derive(Clone)]
pub struct JobContext(Arc<Context>);

struct Context {
    record: JobRecord,
    policy: RetryPolicy,
    worker: Arc<str>,
}

impl JobContext {
    pub(crate) fn new(record: JobRecord, policy: RetryPolicy, worker: Arc<str>) -> Self {
        Self(Arc::new(Context {
            record,
            policy,
            worker,
        }))
    }

    pub fn id(&self) -> &str {
        &self.0.record.id
    }

    pub fn name(&self) -> &str {
        &self.0.record.name
    }

    pub fn queue(&self) -> &str {
        &self.0.record.queue
    }

    /// The arguments, as stored: one JSON value per parameter.
    pub fn args(&self) -> &[Value] {
        &self.0.record.args
    }

    /// Which attempt this is, from 1.
    pub fn attempt(&self) -> u32 {
        self.0.record.attempts.saturating_add(1)
    }

    /// Whether the job is dead if this attempt fails, having used its
    /// retries. An error with [`Retry::Never`] kills it on any attempt.
    pub fn is_last_attempt(&self) -> bool {
        self.0.record.attempts >= self.0.policy.max_retries
    }

    /// How often it is retried and how long it waits: its
    /// `#[job(retries, backoff)]`, or the worker's.
    pub fn retry_policy(&self) -> RetryPolicy {
        self.0.policy
    }

    /// The error of the previous attempt, on a retry.
    pub fn last_error(&self) -> Option<&str> {
        self.0.record.last_error.as_deref()
    }

    /// When the job was due, if it was scheduled: with
    /// [`run_in`](crate::PreparedJob::run_in) or `run_at`, or as a retry
    /// that waited for its backoff.
    pub fn scheduled_at(&self) -> Option<SystemTime> {
        self.0.record.run_at()
    }

    pub fn enqueued_at(&self) -> SystemTime {
        UNIX_EPOCH + Duration::from_millis(self.0.record.enqueued_at_ms)
    }

    /// What [enqueue layers](crate::EnqueueLayer) stored with the job.
    pub fn meta(&self) -> &Map<String, Value> {
        &self.0.record.meta
    }

    /// The id of the worker running it.
    pub fn worker_id(&self) -> &str {
        &self.0.worker
    }

    /// The whole stored record.
    pub fn record(&self) -> &JobRecord {
        &self.0.record
    }
}

impl fmt::Debug for JobContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JobContext")
            .field("id", &self.id())
            .field("name", &self.name())
            .field("queue", &self.queue())
            .field("attempt", &self.attempt())
            .finish_non_exhaustive()
    }
}

pub(crate) type Handler = fn(Invocation) -> RunFuture;

/// The rest of the chain after a layer: the next layer, or the job itself.
/// [`run`](Next::run) it once to go on; drop it to short-circuit.
pub struct Next {
    layers: Arc<Vec<Arc<dyn Layer>>>,
    index: usize,
    job: JobContext,
    handler: Option<Handler>,
    invocation: Invocation,
}

impl Next {
    /// Starts the chain: `layers` in order, outermost first, then `handler`.
    /// Without a handler (the job isn't registered), the innermost step
    /// fails with [`JobError::UnknownJob`].
    pub(crate) fn start(
        layers: Arc<Vec<Arc<dyn Layer>>>,
        job: JobContext,
        handler: Option<Handler>,
        invocation: Invocation,
    ) -> RunFuture {
        Next {
            layers,
            index: 0,
            job,
            handler,
            invocation,
        }
        .run()
    }

    /// Runs the rest of the chain and returns the job's result.
    pub fn run(self) -> RunFuture {
        let Next {
            layers,
            index,
            job,
            handler,
            invocation,
        } = self;
        match layers.get(index).cloned() {
            Some(layer) => layer.run(
                job.clone(),
                Next {
                    layers,
                    index: index + 1,
                    job,
                    handler,
                    invocation,
                },
            ),
            None => run_handler(handler, job.name(), invocation),
        }
    }
}

impl fmt::Debug for Next {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Next")
            .field("job", &self.job)
            .field("remaining_layers", &(self.layers.len() - self.index))
            .finish_non_exhaustive()
    }
}

/// Runs the job's own generated code, or fails if it isn't registered.
pub(crate) fn run_handler(
    handler: Option<Handler>,
    name: &str,
    invocation: Invocation,
) -> RunFuture {
    match handler {
        Some(handler) => handler(invocation),
        None => {
            let error = JobError::UnknownJob {
                name: name.to_owned(),
            };
            Box::pin(async move { Err(error) })
        }
    }
}

/// A job that just died, for [`Worker::on_dead`](crate::Worker::on_dead):
/// it used all its retries, or its error said never to retry.
#[derive(Debug, Clone)]
pub struct DeadJob {
    job: Job<Dead>,
    error: Arc<JobError>,
}

impl DeadJob {
    pub(crate) fn new(job: Job<Dead>, error: JobError) -> Self {
        Self {
            job,
            error: Arc::new(error),
        }
    }

    /// The job as stored, with its attempts and final error message.
    pub fn job(&self) -> &Job<Dead> {
        &self.job
    }

    /// The error of the last attempt. For an error the job returned, it is
    /// [`JobError::Failed`], which holds that error.
    pub fn error(&self) -> &JobError {
        &self.error
    }

    /// Whether its error said never to retry ([`Retry::Never`]), rather
    /// than the job running out of retries.
    pub fn discarded(&self) -> bool {
        self.error.retry() == Retry::Never
    }
}

/// A hook added with [`Worker::on_dead`](crate::Worker::on_dead).
pub(crate) type DeadHook =
    Arc<dyn Fn(DeadJob) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>> + Send + Sync>;

/// Runs every dead hook, in the order they were added.
pub(crate) async fn run_dead_hooks(hooks: Arc<Vec<DeadHook>>, dead: DeadJob) {
    for hook in hooks.iter() {
        hook(dead.clone()).await;
    }
}
