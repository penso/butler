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
//! butler::Worker::new(butler::FileQueue::new(".butler")?).run();
//! ```
//!
//! It does not depend on any async runtime. Enqueuing only writes a file, and the
//! worker drives each job with its own minimal `block_on`.

mod error;
mod executor;
mod queue;
mod worker;

use std::sync::RwLock;

pub use butler_macros::job;
pub use error::Error;
pub use executor::block_on;
pub use queue::{FileQueue, Job, JobId, JobState};
pub use worker::Worker;

/// Converts a job's return value into success or failure. Implemented for `()`
/// and for `Result<T, E: Display>`.
pub trait IntoJobResult {
    fn into_job_result(self) -> Result<(), String>;
}

impl IntoJobResult for () {
    fn into_job_result(self) -> Result<(), String> {
        Ok(())
    }
}

impl<T, E: std::fmt::Display> IntoJobResult for Result<T, E> {
    fn into_job_result(self) -> Result<(), String> {
        self.map(|_| ()).map_err(|e| e.to_string())
    }
}

static QUEUE: RwLock<Option<FileQueue>> = RwLock::new(None);

/// Sets the queue that `#[job]` functions enqueue into. Without this call, the
/// queue lives in `$BUTLER_DIR`, or `./.butler` when that is unset.
pub fn configure(queue: FileQueue) {
    *QUEUE.write().unwrap() = Some(queue);
}

/// Returns the configured queue, creating the default one if needed.
pub fn queue() -> Result<FileQueue, Error> {
    if let Some(q) = QUEUE.read().unwrap().as_ref() {
        return Ok(q.clone());
    }
    let dir = std::env::var_os("BUTLER_DIR").unwrap_or_else(|| ".butler".into());
    let q = FileQueue::new(dir)?;
    *QUEUE.write().unwrap() = Some(q.clone());
    Ok(q)
}

/// Used by the `#[job]` macro. Not public API.
#[doc(hidden)]
pub mod __private {
    use std::{future::Future, pin::Pin};

    pub use inventory;
    pub use serde_json;

    pub type BoxFuture = Pin<Box<dyn Future<Output = Result<(), String>>>>;

    pub struct JobDef {
        pub name: &'static str,
        pub perform: fn(Vec<serde_json::Value>) -> BoxFuture,
    }

    inventory::collect!(JobDef);

    pub fn enqueue(name: &str, args: Vec<serde_json::Value>) -> Result<crate::JobId, crate::Error> {
        crate::queue()?.push(name, args)
    }

    pub fn arg<T: serde::de::DeserializeOwned>(
        args: &mut impl Iterator<Item = serde_json::Value>,
        job: &str,
        index: usize,
    ) -> Result<T, String> {
        let value = args
            .next()
            .ok_or_else(|| format!("{job}: missing argument #{index}"))?;
        serde_json::from_value(value).map_err(|e| format!("{job}: bad argument #{index}: {e}"))
    }
}
