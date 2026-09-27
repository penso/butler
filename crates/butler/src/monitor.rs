//! What a dashboard reads: counts, job listings, history, and the actions it
//! takes on failed jobs. Backends implement these with
//! [`Monitor`](crate::Monitor); `butler-web` renders them.

use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::JobState;

/// Jobs per state and queue, and the workers alive right now.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Stats {
    /// One entry per queue that has ever had a job, sorted by name.
    pub queues: Vec<QueueStats>,
    /// Jobs waiting for their run time, retries included.
    #[serde(default)]
    pub scheduled: u64,
    pub processing: u64,
    pub dead: u64,
    /// Finished jobs the backend still keeps (Redis: the most recent ones).
    pub done: u64,
    pub cancelled: u64,
    /// Every attempt that finished since the backend started counting.
    pub processed_total: u64,
    /// Every attempt that failed since the backend started counting.
    pub failed_total: u64,
    pub workers: Vec<WorkerStats>,
}

impl Stats {
    pub fn pending(&self) -> u64 {
        self.queues.iter().map(|queue| queue.pending).sum()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueStats {
    pub name: String,
    /// Jobs waiting to be claimed, parked ones included.
    pub pending: u64,
    /// Jobs of the queue that workers are running.
    #[serde(default)]
    pub running: u64,
}

/// A worker known to the backend: alive while its heartbeat hasn't expired.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerStats {
    pub id: String,
    /// Jobs it holds right now.
    pub running: u64,
    /// Milliseconds until its heartbeat expires; negative once it has.
    pub expires_in_ms: i64,
}

/// Which jobs [`Monitor::list`](crate::Monitor::list) returns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListFilter {
    pub state: JobState,
    /// Only this queue; every queue if `None`.
    pub queue: Option<String>,
    pub offset: usize,
    pub limit: usize,
}

impl ListFilter {
    pub fn new(state: JobState) -> Self {
        Self {
            state,
            queue: None,
            offset: 0,
            limit: 50,
        }
    }
}

/// One finished attempt, as a worker reports it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobMetric {
    pub job: String,
    pub queue: String,
    pub failed: bool,
    pub duration_ms: u64,
    /// Minutes since the Unix epoch.
    pub minute: u64,
}

impl JobMetric {
    pub fn now(job: &str, queue: &str, failed: bool, duration_ms: u64) -> Self {
        Self {
            job: job.to_owned(),
            queue: queue.to_owned(),
            failed,
            duration_ms,
            minute: current_minute(),
        }
    }
}

/// One minute of history for one job on one queue.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetricBucket {
    /// Minutes since the Unix epoch.
    pub minute: u64,
    pub queue: String,
    pub job: String,
    /// Attempts that finished (succeeded or failed).
    pub processed: u64,
    pub failed: u64,
    pub total_ms: u64,
    pub max_ms: u64,
}

impl MetricBucket {
    pub(crate) fn add(&mut self, metric: &JobMetric) {
        self.processed += 1;
        self.failed += u64::from(metric.failed);
        self.total_ms += metric.duration_ms;
        self.max_ms = self.max_ms.max(metric.duration_ms);
    }
}

/// How long backends keep per-minute history.
pub const METRICS_RETENTION_MINUTES: u64 = 7 * 24 * 60;

pub fn current_minute() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        / 60
}
