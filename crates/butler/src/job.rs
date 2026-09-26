use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub type JobId = String;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
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
}

impl Job {
    pub fn new(name: &str, queue: &str, args: Vec<Value>) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        Job {
            id: new_id(now.as_nanos()),
            name: name.to_string(),
            queue: queue.to_string(),
            args,
            attempts: 0,
            enqueued_at_ms: now.as_millis() as u64,
            last_error: None,
            result: None,
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
