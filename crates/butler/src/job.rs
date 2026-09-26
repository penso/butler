use std::{
    marker::PhantomData,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;

pub type JobId = String;

/// A job as stored by a backend: plain data, whatever state it is in. The
/// typed view is [`Job`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobRecord {
    pub id: JobId,
    pub name: String,
    /// The named queue it waits in; workers choose which queues they serve.
    #[serde(default = "default_queue")]
    pub queue: String,
    pub args: Vec<Value>,
    /// Number of failed attempts so far.
    pub attempts: u32,
    pub enqueued_at_ms: u64,
    pub last_error: Option<String>,
    /// The job's output as JSON, once it is done.
    #[serde(default)]
    pub result: Option<Value>,
    /// Saved [`Progress`](crate::Progress), for a job that resumes from its
    /// last checkpoint.
    #[serde(default)]
    pub progress: Option<Value>,
}

impl JobRecord {
    pub fn new(name: &str, queue: &str, args: Vec<Value>) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        JobRecord {
            id: new_id(now.as_nanos()),
            name: name.to_string(),
            queue: queue.to_string(),
            args,
            attempts: 0,
            enqueued_at_ms: now.as_millis() as u64,
            last_error: None,
            result: None,
            progress: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum JobState {
    Pending,
    Processing,
    Done,
    Dead,
    /// Removed from the queue by [`JobHandle::cancel`](crate::JobHandle::cancel)
    /// before a worker claimed it.
    Cancelled,
}

impl JobState {
    pub const ALL: [JobState; 5] = [
        JobState::Pending,
        JobState::Processing,
        JobState::Done,
        JobState::Dead,
        JobState::Cancelled,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            JobState::Pending => "pending",
            JobState::Processing => "processing",
            JobState::Done => "done",
            JobState::Dead => "dead",
            JobState::Cancelled => "cancelled",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|state| state.as_str() == s)
    }

    /// Whether the job has reached a state it never leaves.
    pub fn is_finished(self) -> bool {
        matches!(self, JobState::Done | JobState::Dead | JobState::Cancelled)
    }
}

/// Marker types for [`Job`]'s state parameter. Each one is a type with no
/// values; it only exists at compile time.
pub mod state {
    use super::JobState;

    mod sealed {
        pub trait Sealed {}
    }

    /// A job state known at compile time. Sealed: the states are fixed.
    pub trait State: sealed::Sealed + Send + Sync + 'static {
        const STATE: JobState;
    }

    macro_rules! states {
        ($($(#[$doc:meta])* $name:ident),* $(,)?) => {$(
            $(#[$doc])*
            #[derive(Debug, Clone, Copy)]
            pub enum $name {}
            impl sealed::Sealed for $name {}
            impl State for $name {
                const STATE: JobState = JobState::$name;
            }
        )*};
    }

    states! {
        /// Waiting on its queue.
        Pending,
        /// Claimed by a worker, which must complete or fail it.
        Processing,
        /// Finished successfully, with its output stored.
        Done,
        /// Failed more than `max_retries` times.
        Dead,
        /// Removed from its queue before any worker claimed it.
        Cancelled,
    }
}

use state::State;

/// A job whose state is part of its type, so each state only offers what
/// makes sense for it:
///
/// - Only [`Queue::claim`](crate::Queue::claim) creates a `Job<Processing>`,
///   and only a `Job<Processing>` can be completed or failed, which consumes
///   it: a job can't be finished twice, or finished without being claimed.
/// - Only a `Job<Done>` has an [`output`](Job::output), and only a
///   `Job<Dead>` a final [`error`](Job::error).
///
/// ```compile_fail
/// # fn f(queue: &butler::Queue, job: butler::Job<butler::state::Pending>) {
/// // A pending job was never claimed, so it can't be completed.
/// queue.complete("worker", job, serde_json::Value::Null);
/// # }
/// ```
///
/// ```compile_fail
/// # fn f(job: butler::Job<butler::state::Processing>) {
/// // A job that is still running has no output yet.
/// let _ = job.output::<u32>();
/// # }
/// ```
///
/// Jobs read back from a backend come as an [`AnyJob`], because their state
/// is only known at runtime: match on it to get the typed job.
pub struct Job<S: State> {
    record: JobRecord,
    state: PhantomData<fn() -> S>,
}

impl<S: State> Job<S> {
    /// Wraps a record the caller knows to be in state `S`.
    pub(crate) fn from_record(record: JobRecord) -> Self {
        Self {
            record,
            state: PhantomData,
        }
    }

    pub fn id(&self) -> &str {
        &self.record.id
    }

    pub fn name(&self) -> &str {
        &self.record.name
    }

    pub fn queue(&self) -> &str {
        &self.record.queue
    }

    pub fn args(&self) -> &[Value] {
        &self.record.args
    }

    /// Failed attempts so far.
    pub fn attempts(&self) -> u32 {
        self.record.attempts
    }

    pub fn state(&self) -> JobState {
        S::STATE
    }

    pub fn record(&self) -> &JobRecord {
        &self.record
    }

    pub fn into_record(self) -> JobRecord {
        self.record
    }
}

impl Job<state::Processing> {
    /// The same job, carrying `progress` to store with it.
    pub(crate) fn with_progress(mut self, progress: Value) -> Self {
        self.record.progress = Some(progress);
        self
    }
}

impl Job<state::Pending> {
    /// Why the previous attempt failed, if this is a retry.
    pub fn last_error(&self) -> Option<&str> {
        self.record.last_error.as_deref()
    }
}

impl Job<state::Done> {
    /// What the job returned. Jobs finished before results existed read as
    /// JSON `null`.
    pub fn output<T: DeserializeOwned>(&self) -> crate::Result<T> {
        let value = self.record.result.clone().unwrap_or(Value::Null);
        Ok(serde_json::from_value(value)?)
    }
}

impl Job<state::Dead> {
    /// Why the last attempt failed.
    pub fn error(&self) -> &str {
        self.record.last_error.as_deref().unwrap_or_default()
    }
}

impl<S: State> Clone for Job<S> {
    fn clone(&self) -> Self {
        Self::from_record(self.record.clone())
    }
}

impl<S: State> std::fmt::Debug for Job<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Job")
            .field("state", &S::STATE)
            .field("record", &self.record)
            .finish()
    }
}

/// A job read from a backend, in whichever state it was at that moment.
#[derive(Debug, Clone)]
pub enum AnyJob {
    Pending(Job<state::Pending>),
    Processing(Job<state::Processing>),
    Done(Job<state::Done>),
    Dead(Job<state::Dead>),
    Cancelled(Job<state::Cancelled>),
}

impl AnyJob {
    pub(crate) fn new(state: JobState, record: JobRecord) -> Self {
        match state {
            JobState::Pending => Self::Pending(Job::from_record(record)),
            JobState::Processing => Self::Processing(Job::from_record(record)),
            JobState::Done => Self::Done(Job::from_record(record)),
            JobState::Dead => Self::Dead(Job::from_record(record)),
            JobState::Cancelled => Self::Cancelled(Job::from_record(record)),
        }
    }

    pub fn state(&self) -> JobState {
        match self {
            Self::Pending(_) => JobState::Pending,
            Self::Processing(_) => JobState::Processing,
            Self::Done(_) => JobState::Done,
            Self::Dead(_) => JobState::Dead,
            Self::Cancelled(_) => JobState::Cancelled,
        }
    }

    pub fn record(&self) -> &JobRecord {
        match self {
            Self::Pending(job) => job.record(),
            Self::Processing(job) => job.record(),
            Self::Done(job) => job.record(),
            Self::Dead(job) => job.record(),
            Self::Cancelled(job) => job.record(),
        }
    }

    pub fn into_parts(self) -> (JobState, JobRecord) {
        let state = self.state();
        let record = match self {
            Self::Pending(job) => job.into_record(),
            Self::Processing(job) => job.into_record(),
            Self::Done(job) => job.into_record(),
            Self::Dead(job) => job.into_record(),
            Self::Cancelled(job) => job.into_record(),
        };
        (state, record)
    }
}

/// What [`Queue::fail`](crate::Queue::fail) turned a failed job into.
#[derive(Debug, Clone)]
pub enum Failed {
    /// Back on its queue for another attempt.
    Retry(Job<state::Pending>),
    /// Out of retries.
    Dead(Job<state::Dead>),
}

impl Failed {
    pub fn state(&self) -> JobState {
        match self {
            Self::Retry(_) => JobState::Pending,
            Self::Dead(_) => JobState::Dead,
        }
    }
}

/// The queue of jobs that don't name one.
pub const DEFAULT_QUEUE: &str = "default";

fn default_queue() -> String {
    DEFAULT_QUEUE.to_owned()
}

/// Whether `name` can be a queue: 1 to 64 of `A-Z a-z 0-9 _ - .`, not starting
/// with a dot. Queue names become file names and Redis keys.
pub fn is_valid_queue_name(name: &str) -> bool {
    (1..=64).contains(&name.len())
        && !name.starts_with('.')
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
}

/// Builds an id that sorts in enqueue order and stays unique across threads
/// and processes.
fn new_id(nanos: u128) -> JobId {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    format!("{nanos:020}-{}-{seq}", std::process::id())
}
