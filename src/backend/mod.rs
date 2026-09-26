//! Queue backends. The worker and the `#[job]` enqueue path only use the
//! [`Backend`] trait, so they work the same with every backend.

mod file;
#[cfg(feature = "redis")]
mod redis;

use std::sync::Arc;

use serde_json::Value;

pub use self::file::FileQueue;
#[cfg(feature = "redis")]
pub use self::redis::RedisQueue;
use crate::{Job, JobId, JobState, Result};

/// Storage for jobs. Methods block; async callers run them through
/// `spawn_blocking`.
pub trait Backend: Send + Sync + 'static {
    /// Stores a new pending job and returns its id.
    fn push(&self, name: &str, args: Vec<Value>) -> Result<JobId>;

    /// Atomically takes the oldest pending job, so no other worker can take it.
    /// Returns `None` if nothing is pending.
    fn claim(&self) -> Result<Option<Job>>;

    /// Marks a claimed job as done.
    fn complete(&self, job: &Job) -> Result<()>;

    /// Records a failure and returns the job's new state: `Pending` to retry,
    /// or `Dead` once it has failed more than `max_retries` times.
    fn fail(&self, job: Job, error: String, max_retries: u32) -> Result<JobState>;

    fn get(&self, id: &str) -> Result<Option<(JobState, Job)>>;

    /// Where the queue lives, for logs. Must not include secrets.
    fn describe(&self) -> String;
}

/// Records a failure on `job` and returns the state it should move to.
pub(crate) fn record_failure(job: &mut Job, error: String, max_retries: u32) -> JobState {
    job.attempts += 1;
    job.last_error = Some(error);
    if job.attempts > max_retries {
        JobState::Dead
    } else {
        JobState::Pending
    }
}

/// A cheap-to-clone handle to a backend.
#[derive(Clone)]
pub struct Queue(Arc<dyn Backend>);

impl Queue {
    pub fn new(backend: impl Backend) -> Self {
        Queue(Arc::new(backend))
    }

    pub fn push(&self, name: &str, args: Vec<Value>) -> Result<JobId> {
        self.0.push(name, args)
    }

    pub fn claim(&self) -> Result<Option<Job>> {
        self.0.claim()
    }

    pub fn complete(&self, job: &Job) -> Result<()> {
        self.0.complete(job)
    }

    pub fn fail(&self, job: Job, error: String, max_retries: u32) -> Result<JobState> {
        self.0.fail(job, error, max_retries)
    }

    pub fn get(&self, id: &str) -> Result<Option<(JobState, Job)>> {
        self.0.get(id)
    }

    /// The job's current state. Returns `None` if the job is unknown, or if the
    /// backend can't be reached.
    pub fn state(&self, id: &str) -> Option<JobState> {
        self.0.get(id).ok().flatten().map(|(state, _)| state)
    }

    pub fn describe(&self) -> String {
        self.0.describe()
    }
}

impl std::fmt::Debug for Queue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Queue({})", self.describe())
    }
}

impl From<FileQueue> for Queue {
    fn from(q: FileQueue) -> Self {
        Queue::new(q)
    }
}

#[cfg(feature = "redis")]
impl From<RedisQueue> for Queue {
    fn from(q: RedisQueue) -> Self {
        Queue::new(q)
    }
}
