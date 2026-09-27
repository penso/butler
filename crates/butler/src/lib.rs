//! butler: a tiny Sidekiq-style background job runner.
//!
//! ```ignore
//! #[butler::job]
//! async fn send_email(to: String, subject: String) { /* ... */ }
//!
//! // Enqueue: returns as soon as the job is persisted. Arguments accept
//! // borrowed values too (see [`JobArg`]).
//! let job = send_email("a@b.c", "hi").await?;
//! job.cancel().await?;   // or job.state(), job.wait(interval), ... (see [`JobHandle`])
//!
//! // Elsewhere (same binary/crate that defines the jobs):
//! butler::Worker::from_config(&butler::Config::load()?)?.run();
//! ```
//!
//! `butler.toml` chooses the backend: files on disk or Redis (see [`Config`]).
//!
//! Without the `tokio` feature, it needs no async runtime: enqueuing writes a file
//! and `Worker::run` drives each job with a minimal `block_on`. With `tokio` (the
//! default), enqueuing from inside a runtime goes through `spawn_blocking`, and
//! `Worker::run_async` spawns each job as a tokio task. That lets job bodies use
//! tokio timers, I/O, and so on.

mod arg;
mod backend;
mod config;
mod error;
mod executor;
mod handle;
mod job;
mod limits;
pub mod monitor;
mod prepared;
mod progress;
mod queues;
mod retry;
mod signal;
pub mod testing;
mod worker;

use std::sync::{PoisonError, RwLock};

use serde::{Serialize, de::DeserializeOwned};

pub use arg::JobArg;
#[cfg(feature = "redis")]
pub use backend::RedisQueue;
pub use backend::{
    Backend, FileQueue, MemoryQueue, Monitor, NewJob, Promoted, Queue, Store, Watch,
};
#[cfg(feature = "sqlite")]
pub use backend::{SQLITE_WATCH_TICK, SqliteQueue};
pub use butler_macros::job;
pub use config::{
    BackendKind, Config, FileConfig, QueueConfig, QueueEntry, RedisConfig, SqliteConfig,
    WorkerConfig,
};
pub use error::{BoxError, Error, Failure, JobError, Result};
pub use executor::block_on;
pub use handle::JobHandle;
pub use job::{
    AnyJob, DEFAULT_QUEUE, Failed, Job, JobId, JobRecord, JobState, is_valid_queue_name, state,
};
pub use prepared::{PreparedJob, enqueue_all};
pub use progress::{Interrupted, Progress};
pub use queues::QueuePriority;
pub use retry::{Backoff, JITTER, MAX_BACKOFF, Retry, RetryPolicy, Retryable};
pub use signal::{JobWatch, Signal};
pub use worker::Worker;

/// Converts a job's return value into its output or a failure.
///
/// Implemented for `()` and for `Result<T, E>`, where `T` is serializable (the
/// worker stores it, and [`JobHandle::result`] returns it) and `E` converts
/// into a [`BoxError`]: any `std::error::Error + Send + Sync` (a `thiserror`
/// enum, `std::io::Error`, ...), a `String` or `&str` message, or an
/// `anyhow::Error` if your app uses anyhow. butler itself doesn't depend on it.
pub trait IntoJobResult {
    /// What the job produces on success.
    type Output: Serialize + DeserializeOwned + Send + 'static;
    /// What the job fails with. The `#[job]` macro reads its
    /// [`Retryable`] classification, when it has one.
    type Error: Into<BoxError>;

    fn into_result(self) -> Result<Self::Output, Self::Error>;

    fn into_job_result(self) -> Result<Self::Output, JobError>
    where
        Self: Sized,
    {
        self.into_result()
            .map_err(|e| JobError::Failed(Failure::from(e.into())))
    }
}

impl IntoJobResult for () {
    type Output = ();
    type Error = std::convert::Infallible;

    fn into_result(self) -> Result<(), std::convert::Infallible> {
        Ok(())
    }
}

impl<T, E> IntoJobResult for Result<T, E>
where
    T: Serialize + DeserializeOwned + Send + 'static,
    E: Into<BoxError>,
{
    type Output = T;
    type Error = E;

    fn into_result(self) -> Result<T, E> {
        self
    }
}

/// A job that a worker can run: its name, the queue it goes on, its retry
/// settings, and the function that decodes the arguments and runs the body.
/// `#[job] fn foo` generates it as `foo::JOB`.
#[derive(Clone, Copy)]
pub struct JobDef {
    pub name: &'static str,
    /// Set with `#[job(queue = "...")]`; [`DEFAULT_QUEUE`] otherwise.
    pub queue: &'static str,
    /// Set with `#[job(retries = N)]`; the worker's `max_retries` otherwise.
    pub retries: Option<u32>,
    /// Set with `#[job(backoff = "...")]`; the worker's backoff otherwise.
    pub backoff: Option<Backoff>,
    #[doc(hidden)]
    pub perform: fn(progress::Invocation) -> __private::BoxFuture,
}

static QUEUE: RwLock<Option<Queue>> = RwLock::new(None);

/// Sets the queue that `#[job]` functions enqueue into. Without this call, the
/// first enqueue opens the queue described by [`Config::load`].
pub fn configure(queue: impl Into<Queue>) {
    // The slot is a plain `Option`, so a panic elsewhere can't leave it half-written.
    *QUEUE.write().unwrap_or_else(PoisonError::into_inner) = Some(queue.into());
}

/// Returns the configured queue, opening it from [`Config::load`] if needed.
pub fn queue() -> Result<Queue> {
    if let Some(q) = QUEUE
        .read()
        .unwrap_or_else(PoisonError::into_inner)
        .as_ref()
    {
        return Ok(q.clone());
    }
    let mut slot = QUEUE.write().unwrap_or_else(PoisonError::into_inner);
    if let Some(q) = slot.as_ref() {
        return Ok(q.clone());
    }
    let q = Config::load()?.connect()?;
    *slot = Some(q.clone());
    Ok(q)
}

/// Used by the `#[job]` macro. Not public API.
#[doc(hidden)]
pub mod __private {
    use std::{future::Future, pin::Pin};

    pub use inventory;
    pub use serde_json;

    pub use crate::progress::{Checkpoints, Invocation};

    use crate::JobError;

    /// A job run: its output as JSON, or why it failed.
    pub type BoxFuture = Pin<Box<dyn Future<Output = Result<serde_json::Value, JobError>> + Send>>;

    /// Turns a job body's return value into the JSON the worker stores, or
    /// its error, with what `classify` says about retrying it.
    pub fn output<R: crate::IntoJobResult>(
        returned: R,
        classify: impl FnOnce(&R::Error) -> crate::Retry,
    ) -> Result<serde_json::Value, JobError> {
        match returned.into_result() {
            Ok(value) => serde_json::to_value(value).map_err(JobError::Output),
            Err(error) => {
                let retry = classify(&error);
                Err(JobError::Failed(crate::Failure::new(error, retry)))
            }
        }
    }

    /// Reads a job error's [`Retryable`](crate::Retryable) classification
    /// when its type has one, and [`Retry::Default`](crate::Retry::Default)
    /// otherwise, without requiring the trait: the macro calls
    /// `(&Classify(&error)).retry_policy()` with both traits below in scope.
    /// Method lookup tries `Classify<E>` (needs `E: Retryable`) before
    /// `&Classify<E>` (any `E`), so the first applies whenever it can. That
    /// only works where `E` is a concrete type, as it is in generated code.
    pub struct Classify<'a, E>(pub &'a E);

    pub trait ClassifyRetry {
        fn retry_policy(&self) -> crate::Retry;
    }

    impl<E: crate::Retryable> ClassifyRetry for Classify<'_, E> {
        fn retry_policy(&self) -> crate::Retry {
            self.0.retry()
        }
    }

    pub trait DefaultRetry {
        fn retry_policy(&self) -> crate::Retry;
    }

    impl<E> DefaultRetry for &Classify<'_, E> {
        fn retry_policy(&self) -> crate::Retry {
            crate::Retry::Default
        }
    }

    /// Runs a synchronous job body. Inside a tokio runtime it goes to the
    /// blocking pool, so CPU-heavy or blocking work never stalls the async
    /// worker threads; elsewhere it runs in place.
    pub async fn run_blocking<T: Send + 'static>(
        body: impl FnOnce() -> T + Send + 'static,
    ) -> Result<T, JobError> {
        #[cfg(feature = "tokio")]
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            return handle.spawn_blocking(body).await.map_err(|e| {
                if e.is_panic() {
                    JobError::Panicked {
                        message: crate::executor::panic_message(&*e.into_panic()),
                    }
                } else {
                    JobError::Cancelled(e)
                }
            });
        }
        Ok(body())
    }

    inventory::collect!(crate::JobDef);

    /// Collects the serialized arguments of one enqueue call.
    pub fn args<const N: usize>(
        values: [serde_json::Result<serde_json::Value>; N],
    ) -> crate::Result<Vec<serde_json::Value>> {
        values.into_iter().map(|v| v.map_err(Into::into)).collect()
    }

    pub async fn enqueue<T>(
        job: &'static crate::JobDef,
        args: Vec<serde_json::Value>,
    ) -> crate::Result<crate::JobHandle<T>> {
        enqueue_on(job, job.queue, args, None).await
    }

    /// Like [`enqueue`], on `queue` rather than the job's own, and scheduled
    /// for `run_at` if given.
    pub async fn enqueue_on<T>(
        job: &'static crate::JobDef,
        queue_name: &str,
        args: Vec<serde_json::Value>,
        run_at: Option<std::time::SystemTime>,
    ) -> crate::Result<crate::JobHandle<T>> {
        // Inside `testing::perform_enqueued_jobs`: run it now, no queue, even
        // if it was scheduled for later.
        if let Some(inline) = crate::testing::current() {
            return inline.run(job, queue_name, args).await;
        }
        let queue_name = queue_name.to_owned();
        let queue = crate::queue()?;
        let pushing = queue.clone();
        let id = crate::executor::unblock(queue.blocks(), move || match run_at {
            Some(run_at) => pushing.schedule(job.name, &queue_name, args, run_at),
            None => pushing.push(job.name, &queue_name, args),
        })
        .await?;
        Ok(crate::JobHandle::new(queue, id))
    }

    pub fn arg<T: serde::de::DeserializeOwned>(
        args: &mut impl Iterator<Item = serde_json::Value>,
        job: &'static str,
        index: usize,
    ) -> Result<T, JobError> {
        let value = args
            .next()
            .ok_or(JobError::MissingArgument { job, index })?;
        serde_json::from_value(value).map_err(|source| JobError::BadArgument { job, index, source })
    }
}
