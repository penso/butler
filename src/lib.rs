//! butler: a tiny Sidekiq-style background job runner.
//!
//! ```ignore
//! #[butler::job]
//! async fn send_email(to: String, subject: String) { /* ... */ }
//!
//! // Enqueue: returns as soon as the job is persisted.
//! let id = send_email("a@b.c".into(), "hi".into()).await?;
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

mod backend;
mod config;
mod error;
mod executor;
mod job;
mod worker;

use std::sync::{PoisonError, RwLock};

#[cfg(feature = "redis")]
pub use backend::RedisQueue;
pub use backend::{Backend, FileQueue, Queue};
pub use butler_macros::job;
pub use config::{BackendKind, Config, FileConfig, QueueConfig, RedisConfig, WorkerConfig};
pub use error::{Error, JobError, Result};
pub use executor::block_on;
pub use job::{Job, JobId, JobState};
pub use worker::Worker;

/// Converts a job's return value into success or failure. Implemented for `()`
/// and for `Result<T, E>` where `E` converts into `anyhow::Error`: any
/// `std::error::Error + Send + Sync`, or `anyhow::Error` itself.
pub trait IntoJobResult {
    fn into_job_result(self) -> Result<(), JobError>;
}

impl IntoJobResult for () {
    fn into_job_result(self) -> Result<(), JobError> {
        Ok(())
    }
}

impl<T, E: Into<anyhow::Error>> IntoJobResult for Result<T, E> {
    fn into_job_result(self) -> Result<(), JobError> {
        self.map(|_| ()).map_err(|e| JobError::Failed(e.into()))
    }
}

/// A job that a worker can run: its name plus the function that decodes the
/// arguments and runs the body. `#[job] fn foo` generates it as `foo::JOB`.
#[derive(Clone, Copy)]
pub struct JobDef {
    pub name: &'static str,
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

    pub type BoxFuture = Pin<Box<dyn Future<Output = Result<(), JobError>> + Send>>;

    inventory::collect!(crate::JobDef);

    pub async fn enqueue(
        name: &'static str,
        args: Vec<serde_json::Value>,
    ) -> crate::Result<crate::JobId> {
        let queue = crate::queue()?;
        // Inside a tokio runtime, keep the file I/O off the async worker threads.
        #[cfg(feature = "tokio")]
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            return handle
                .spawn_blocking(move || queue.push(name, args))
                .await?;
        }
        queue.push(name, args)
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
