//! What templates show: plain data, already formatted.

use std::time::{SystemTime, UNIX_EPOCH};

use butler::{AnyJob, JobState, monitor::MetricBucket};

/// An error and each of its sources: `outer: inner: root`.
pub(crate) fn chain(err: &(dyn std::error::Error + 'static)) -> String {
    let mut text = err.to_string();
    let mut source = err.source();
    while let Some(inner) = source {
        text.push_str(": ");
        text.push_str(&inner.to_string());
        source = inner.source();
    }
    text
}

pub(crate) fn now_ms() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(u64::MAX)
}

/// "4s ago", "3m ago", "2h ago", "5d ago". Refreshed in the browser.
pub(crate) fn ago(ms: u64) -> String {
    format!("{} ago", span(now_ms().saturating_sub(ms) / 1000))
}

/// "in 4s", "in 3m", "in 2h", "in 5d", or "due now" once it has passed.
/// Refreshed in the browser.
pub(crate) fn until(ms: u64) -> String {
    match ms.saturating_sub(now_ms()) / 1000 {
        0 => "due now".to_owned(),
        seconds => format!("in {}", span(seconds)),
    }
}

/// "4s", "3m", "2h", "5d".
fn span(seconds: u64) -> String {
    match seconds {
        0..60 => format!("{seconds}s"),
        60..3_600 => format!("{}m", seconds / 60),
        3_600..86_400 => format!("{}h", seconds / 3_600),
        _ => format!("{}d", seconds / 86_400),
    }
}

/// "1,234,567".
pub(crate) fn thousands(n: u64) -> String {
    let digits = n.to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    for (i, digit) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(digit);
    }
    out
}

pub(crate) fn duration(ms: u64) -> String {
    match ms {
        0..1_000 => format!("{ms} ms"),
        1_000..60_000 => format!("{:.1} s", ms as f64 / 1_000.0),
        _ => format!("{:.1} min", ms as f64 / 60_000.0),
    }
}

/// One job in a table.
pub(crate) struct JobRow {
    pub id: String,
    pub short_id: String,
    pub name: String,
    pub queue: String,
    pub attempts: u32,
    pub enqueued_ms: u64,
    pub enqueued_ago: String,
    /// When a scheduled job runs, in ms since the epoch, and as "in 5m".
    pub run_at_ms: u64,
    pub runs_in: String,
    /// First line of the last error.
    pub error: Option<String>,
}

impl JobRow {
    pub fn new(job: &AnyJob) -> Self {
        let record = job.record();
        Self {
            id: record.id.clone(),
            short_id: short_id(&record.id),
            name: record.name.clone(),
            queue: record.queue.clone(),
            attempts: record.attempts,
            enqueued_ms: record.enqueued_at_ms,
            enqueued_ago: ago(record.enqueued_at_ms),
            run_at_ms: record.run_at_ms.unwrap_or_default(),
            runs_in: until(record.run_at_ms.unwrap_or_default()),
            error: record
                .last_error
                .as_deref()
                .map(|error| error.lines().next().unwrap_or_default().to_owned()),
        }
    }
}

/// Ids are `<nanos>-<pid>-<seq>`: the tail tells jobs apart at a glance.
pub(crate) fn short_id(id: &str) -> String {
    let len = id.chars().count();
    if len <= 12 {
        return id.to_owned();
    }
    id.chars().skip(len - 12).collect()
}

/// Summary of one job name over the charted period.
pub(crate) struct JobSummary {
    pub queue: String,
    pub job: String,
    pub processed: String,
    pub failed: u64,
    pub failure_rate: String,
    pub avg: String,
    pub max: String,
    sort_key: u64,
}

pub(crate) fn summarize(buckets: &[MetricBucket]) -> Vec<JobSummary> {
    let mut by_job: std::collections::BTreeMap<(&str, &str), MetricBucket> = Default::default();
    for bucket in buckets {
        let total = by_job
            .entry((bucket.queue.as_str(), bucket.job.as_str()))
            .or_default();
        total.processed += bucket.processed;
        total.failed += bucket.failed;
        total.total_ms += bucket.total_ms;
        total.max_ms = total.max_ms.max(bucket.max_ms);
    }
    let mut rows: Vec<JobSummary> = by_job
        .into_iter()
        .map(|((queue, job), total)| JobSummary {
            queue: queue.to_owned(),
            job: job.to_owned(),
            processed: thousands(total.processed),
            failed: total.failed,
            failure_rate: if total.processed == 0 {
                "0%".to_owned()
            } else {
                format!(
                    "{:.1}%",
                    total.failed as f64 * 100.0 / total.processed as f64
                )
            },
            avg: duration(total.total_ms.checked_div(total.processed).unwrap_or(0)),
            max: duration(total.max_ms),
            sort_key: total.processed,
        })
        .collect();
    rows.sort_by_key(|row| std::cmp::Reverse(row.sort_key));
    rows
}

pub(crate) const STATES: [JobState; 6] = [
    JobState::Pending,
    JobState::Scheduled,
    JobState::Processing,
    JobState::Done,
    JobState::Dead,
    JobState::Cancelled,
];

pub(crate) fn state_label(state: JobState) -> &'static str {
    match state {
        JobState::Pending => "Pending",
        JobState::Scheduled => "Scheduled",
        JobState::Processing => "Running",
        JobState::Done => "Done",
        JobState::Dead => "Dead",
        JobState::Cancelled => "Cancelled",
    }
}

/// JSON safe to embed in a `<script>`: `</` can't end the element early.
pub(crate) fn script_json(value: &impl serde::Serialize) -> String {
    serde_json::to_string(value)
        .unwrap_or_else(|_| "null".to_owned())
        .replace("</", "<\\/")
}
