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
//! `config.toml` chooses the backend: files on disk or Redis (see [`Config`]).
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
mod prepared;
mod queues;
mod signal;
pub mod testing;
mod worker;

use std::sync::{PoisonError, RwLock};

use serde::{Serialize, de::DeserializeOwned};

pub use arg::JobArg;
#[cfg(feature = "redis")]
pub use backend::RedisQueue;
pub use backend::{Backend, FileQueue, MemoryQueue, NewJob, Queue};
#[cfg(feature = "sqlite")]
pub use backend::{SQLITE_WATCH_TICK, SqliteQueue};
pub use butler_macros::job;
pub use config::{
    BackendKind, Config, FileConfig, QueueConfig, QueueEntry, RedisConfig, SqliteConfig,
    WorkerConfig,
};
pub use error::{BoxError, Error, JobError, Result};
pub use executor::block_on;
pub use handle::JobHandle;
pub use job::{
    AnyJob, DEFAULT_QUEUE, Failed, Job, JobId, JobRecord, JobState, is_valid_queue_name, state,
};
pub use prepared::{PreparedJob, enqueue_all};
pub use queues::QueuePriority;
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

    fn into_job_result(self) -> Result<Self::Output, JobError>;
}

impl IntoJobResult for () {
    type Output = ();

    fn into_job_result(self) -> Result<(), JobError> {
        Ok(())
    }
}

impl<T, E> IntoJobResult for Result<T, E>
where
    T: Serialize + DeserializeOwned + Send + 'static,
    E: Into<BoxError>,
{
    type Output = T;

    fn into_job_result(self) -> Result<T, JobError> {
        self.map_err(|e| JobError::Failed(e.into()))
    }
}

/// A job that a worker can run: its name, the queue it goes on, and the
/// function that decodes the arguments and runs the body. `#[job] fn foo`
/// generates it as `foo::JOB`.
#[derive(Clone, Copy)]
pub struct JobDef {
    pub name: &'static str,
    /// Set with `#[job(queue = "...")]`; [`DEFAULT_QUEUE`] otherwise.
    pub queue: &'static str,
    #[doc(hidden)]
    pub perform: fn(Vec<serde_json::Value>) -> __private::BoxFuture,
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

    use crate::JobError;

    /// A job run: its output as JSON, or why it failed.
    pub type BoxFuture = Pin<Box<dyn Future<Output = Result<serde_json::Value, JobError>> + Send>>;

    /// Turns a job body's return value into the JSON the worker stores.
    pub fn output<R: crate::IntoJobResult>(returned: R) -> Result<serde_json::Value, JobError> {
        serde_json::to_value(returned.into_job_result()?).map_err(JobError::Output)
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
        enqueue_on(job, job.queue, args).await
    }

    /// Like [`enqueue`], on `queue` rather than the job's own.
    pub async fn enqueue_on<T>(
        job: &'static crate::JobDef,
        queue_name: &str,
        args: Vec<serde_json::Value>,
    ) -> crate::Result<crate::JobHandle<T>> {
        // Inside `testing::perform_enqueued_jobs`: run it now, no queue.
        if let Some(inline) = crate::testing::current() {
            return inline.run(job, queue_name, args).await;
        }
        let queue_name = queue_name.to_owned();
        let queue = crate::queue()?;
        let pushing = queue.clone();
        let id = crate::executor::unblock(queue.blocks(), move || {
            pushing.push(job.name, &queue_name, args)
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
