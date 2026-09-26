//! Queue backends. The worker and the `#[job]` enqueue path only use the
//! [`Backend`] trait, so they work the same with every backend.

mod file;
mod memory;
#[cfg(feature = "redis")]
mod redis;

use std::{sync::Arc, time::Duration};

use serde_json::Value;

#[cfg(feature = "redis")]
pub use self::redis::RedisQueue;
pub use self::{file::FileQueue, memory::MemoryQueue};
use crate::{Job, JobId, JobState, Result};

/// Storage for jobs. Methods block; async callers run them through
/// `spawn_blocking`.
///
/// Delivery is at least once. Each worker process has an id; `claim` moves a
/// job into that worker's own processing area, and the worker keeps a
/// `heartbeat` alive while it runs. If the heartbeat lapses (the process
/// crashed or hung), `recover` puts that worker's jobs back in the queue, so a
/// job interrupted by a crash runs again. Job bodies should be safe to repeat.
pub trait Backend: Send + Sync + 'static {
    /// Stores a new pending job on `queue` and returns its id.
    fn push(&self, name: &str, queue: &str, args: Vec<Value>) -> Result<JobId>;

    /// Atomically moves the oldest pending job of the first non-empty queue in
    /// `queues` into `worker`'s processing area, so no other worker can take
    /// it. May block up to `wait` for a job to arrive; backends that can't
    /// block return `None` right away.
    fn claim(&self, worker: &str, queues: &[&str], wait: Duration) -> Result<Option<Job>>;

    /// Marks a job `worker` claimed as done.
    fn complete(&self, worker: &str, job: &Job) -> Result<()>;

    /// Records a failure and returns the job's new state: `Pending` to retry,
    /// or `Dead` once it has failed more than `max_retries` times.
    fn fail(&self, worker: &str, job: Job, error: String, max_retries: u32) -> Result<JobState>;

    fn get(&self, id: &str) -> Result<Option<(JobState, Job)>>;

    /// Atomically removes a pending job so no worker will run it, and marks it
    /// `Cancelled`. Returns `false`, changing nothing, if the job is unknown or
    /// a worker already claimed it: a running job can't be interrupted.
    fn cancel(&self, id: &str) -> Result<bool>;

    /// Declares `worker` alive for `ttl`. Workers call it well within `ttl`.
    fn heartbeat(&self, worker: &str, ttl: Duration) -> Result<()>;

    /// Clean shutdown: `worker` is gone and holds no jobs.
    fn retire(&self, worker: &str) -> Result<()>;

    /// Moves every job held by a worker whose heartbeat expired back to the
    /// front of its own queue, and returns how many. Safe to run from several workers at once:
    /// each job moves exactly once.
    fn recover(&self) -> Result<usize>;

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

    pub fn push(&self, name: &str, queue: &str, args: Vec<Value>) -> Result<JobId> {
        self.0.push(name, queue, args)
    }

    pub fn claim(&self, worker: &str, queues: &[&str], wait: Duration) -> Result<Option<Job>> {
        self.0.claim(worker, queues, wait)
    }

    pub fn complete(&self, worker: &str, job: &Job) -> Result<()> {
        self.0.complete(worker, job)
    }

    pub fn fail(
        &self,
        worker: &str,
        job: Job,
        error: String,
        max_retries: u32,
    ) -> Result<JobState> {
        self.0.fail(worker, job, error, max_retries)
    }

    pub fn heartbeat(&self, worker: &str, ttl: Duration) -> Result<()> {
        self.0.heartbeat(worker, ttl)
    }

    pub fn retire(&self, worker: &str) -> Result<()> {
        self.0.retire(worker)
    }

    pub fn recover(&self) -> Result<usize> {
        self.0.recover()
    }

    pub fn get(&self, id: &str) -> Result<Option<(JobState, Job)>> {
        self.0.get(id)
    }

    /// The job's current state. Returns `None` if the job is unknown, or if the
    /// backend can't be reached.
    pub fn state(&self, id: &str) -> Option<JobState> {
        self.0.get(id).ok().flatten().map(|(state, _)| state)
    }

    pub fn cancel(&self, id: &str) -> Result<bool> {
        self.0.cancel(id)
    }

    /// A handle to an existing job, for example from an id stored elsewhere.
    pub fn handle(&self, id: impl Into<JobId>) -> crate::JobHandle {
        crate::JobHandle::new(self.clone(), id.into())
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

impl From<MemoryQueue> for Queue {
    fn from(q: MemoryQueue) -> Self {
        Queue::new(q)
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
