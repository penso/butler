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
    pub args: Vec<Value>,
    /// Number of failed attempts so far.
    pub attempts: u32,
    pub enqueued_at_ms: u64,
    pub last_error: Option<String>,
}

impl Job {
    pub fn new(name: &str, args: Vec<Value>) -> Self {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
        Job {
            id: new_id(now.as_nanos()),
            name: name.to_string(),
            args,
            attempts: 0,
            enqueued_at_ms: now.as_millis() as u64,
            last_error: None,
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
}

impl JobState {
    pub fn as_str(self) -> &'static str {
        match self {
            JobState::Pending => "pending",
            JobState::Processing => "processing",
            JobState::Done => "done",
            JobState::Dead => "dead",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        [JobState::Pending, JobState::Processing, JobState::Done, JobState::Dead]
            .into_iter()
            .find(|state| state.as_str() == s)
    }
}

/// Builds an id that sorts in enqueue order and stays unique across threads
/// and processes.
fn new_id(nanos: u128) -> JobId {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    format!("{nanos:020}-{}-{seq}", std::process::id())
}
