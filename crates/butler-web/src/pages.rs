use std::{collections::BTreeMap, sync::Arc};

use askama::Template;
use axum::{
    Json,
    extract::{Path, Query, State},
    response::Html,
};
use butler::{
    AnyJob, JobState, Queue,
    monitor::{ListFilter, MetricBucket, current_minute},
};
use serde::{Deserialize, Serialize};

use crate::{
    AppState, WebError,
    events::{Snapshot, read_stats},
    views::{
        self, JobRow, JobSummary, STATES, ago, duration, script_json, state_label, thousands, until,
    },
};

const PAGE_SIZE: usize = 50;
/// The dashboard's history and busiest-jobs table cover a day.
const DAY_MINUTES: u64 = 24 * 60;

async fn blocking<T: Send + 'static>(
    queue: &Queue,
    call: impl FnOnce(&Queue) -> butler::Result<T> + Send + 'static,
) -> Result<T, WebError> {
    let queue = queue.clone();
    Ok(tokio::task::spawn_blocking(move || call(&queue)).await??)
}

pub(crate) struct QueueRow {
    pub name: String,
    pub pending: String,
}

#[derive(Template)]
#[template(path = "dashboard.html")]
struct DashboardPage {
    base: String,
    nav: &'static str,
    backend: String,
    processed: String,
    failed: String,
    running: String,
    pending: String,
    scheduled: String,
    dead: String,
    workers: String,
    queues: Vec<QueueRow>,
    jobs: Vec<JobSummary>,
    snapshot_json: String,
}

pub(crate) async fn dashboard(
    State(state): State<Arc<AppState>>,
) -> Result<Html<String>, WebError> {
    let stats = read_stats(&state.queue).await?;
    let since = current_minute().saturating_sub(DAY_MINUTES);
    let buckets = blocking(&state.queue, move |queue| queue.metrics(since)).await?;
    let snapshot = Snapshot::from_stats(&stats);
    let page = DashboardPage {
        base: state.base.clone(),
        nav: "dashboard",
        backend: state.queue.describe(),
        processed: thousands(stats.processed_total),
        failed: thousands(stats.failed_total),
        running: thousands(stats.processing),
        pending: thousands(stats.pending()),
        scheduled: thousands(stats.scheduled),
        dead: thousands(stats.dead),
        workers: thousands(snapshot.workers_alive as u64),
        queues: stats
            .queues
            .iter()
            .map(|queue| QueueRow {
                name: queue.name.clone(),
                pending: thousands(queue.pending),
            })
            .collect(),
        jobs: views::summarize(&buckets).into_iter().take(12).collect(),
        snapshot_json: script_json(&snapshot),
    };
    Ok(Html(page.render()?))
}

/// Per-minute totals for the charts, one entry per minute, zeros included.
#[derive(Serialize)]
pub(crate) struct Series {
    minutes: Vec<u64>,
    processed: Vec<u64>,
    failed: Vec<u64>,
    avg_ms: Vec<u64>,
    max_ms: Vec<u64>,
}

#[derive(Deserialize)]
pub(crate) struct SeriesQuery {
    minutes: Option<u64>,
}

pub(crate) async fn metrics_json(
    State(state): State<Arc<AppState>>,
    Query(query): Query<SeriesQuery>,
) -> Result<Json<Series>, WebError> {
    let span = query
        .minutes
        .unwrap_or(DAY_MINUTES)
        .clamp(5, butler::monitor::METRICS_RETENTION_MINUTES);
    let now = current_minute();
    let first = now.saturating_sub(span - 1);
    let buckets = blocking(&state.queue, move |queue| queue.metrics(first)).await?;
    Ok(Json(series(first, now, &buckets)))
}

fn series(first: u64, last: u64, buckets: &[MetricBucket]) -> Series {
    let mut totals: BTreeMap<u64, MetricBucket> = (first..=last)
        .map(|minute| (minute, MetricBucket::default()))
        .collect();
    for bucket in buckets {
        if let Some(total) = totals.get_mut(&bucket.minute) {
            total.processed += bucket.processed;
            total.failed += bucket.failed;
            total.total_ms += bucket.total_ms;
            total.max_ms = total.max_ms.max(bucket.max_ms);
        }
    }
    let mut out = Series {
        minutes: Vec::with_capacity(totals.len()),
        processed: Vec::with_capacity(totals.len()),
        failed: Vec::with_capacity(totals.len()),
        avg_ms: Vec::with_capacity(totals.len()),
        max_ms: Vec::with_capacity(totals.len()),
    };
    for (minute, total) in totals {
        out.minutes.push(minute);
        out.processed.push(total.processed);
        out.failed.push(total.failed);
        out.avg_ms
            .push(total.total_ms.checked_div(total.processed).unwrap_or(0));
        out.max_ms.push(total.max_ms);
    }
    out
}

pub(crate) struct Tab {
    pub state: &'static str,
    pub label: &'static str,
    pub count: String,
    pub active: bool,
}

#[derive(Template)]
#[template(path = "jobs.html")]
struct JobsPage {
    base: String,
    nav: &'static str,
    state: &'static str,
    label: &'static str,
    tabs: Vec<Tab>,
    queue: String,
    queues: Vec<String>,
    rows: Vec<JobRow>,
    page: usize,
    prev_url: Option<String>,
    next_url: Option<String>,
    return_to: String,
    can_retry: bool,
    can_discard: bool,
    can_cancel: bool,
    can_run_now: bool,
    /// Scheduled jobs show when they run.
    show_run_at: bool,
    /// Dead jobs, and retries waiting their turn, show their last error.
    show_error: bool,
}

#[derive(Deserialize)]
pub(crate) struct JobsQuery {
    state: Option<String>,
    queue: Option<String>,
    page: Option<usize>,
}

pub(crate) async fn jobs(
    State(state): State<Arc<AppState>>,
    Query(query): Query<JobsQuery>,
) -> Result<Html<String>, WebError> {
    let job_state = query
        .state
        .as_deref()
        .and_then(JobState::parse)
        .unwrap_or(JobState::Dead);
    let queue_name = query.queue.filter(|queue| !queue.is_empty());
    let page = query.page.unwrap_or(1).max(1);

    let stats = read_stats(&state.queue).await?;
    let mut filter = ListFilter::new(job_state);
    filter.queue = queue_name.clone();
    filter.offset = (page - 1) * PAGE_SIZE;
    filter.limit = PAGE_SIZE;
    let jobs = blocking(&state.queue, move |queue| queue.list(&filter)).await?;

    let count = |job_state: JobState| match job_state {
        JobState::Pending => stats.pending(),
        JobState::Scheduled => stats.scheduled,
        JobState::Processing => stats.processing,
        JobState::Done => stats.done,
        JobState::Dead => stats.dead,
        JobState::Cancelled => stats.cancelled,
    };
    let url = |page: usize| {
        let mut url = format!("{}/jobs?state={}", state.base, job_state.as_str());
        if let Some(queue) = &queue_name {
            url.push_str("&queue=");
            url.push_str(queue);
        }
        if page > 1 {
            url.push_str(&format!("&page={page}"));
        }
        url
    };
    let page_view = JobsPage {
        base: state.base.clone(),
        nav: "jobs",
        state: job_state.as_str(),
        label: state_label(job_state),
        tabs: STATES
            .iter()
            .map(|tab| Tab {
                state: tab.as_str(),
                label: state_label(*tab),
                count: thousands(count(*tab)),
                active: *tab == job_state,
            })
            .collect(),
        queue: queue_name.clone().unwrap_or_default(),
        queues: stats
            .queues
            .iter()
            .map(|queue| queue.name.clone())
            .collect(),
        rows: jobs.iter().map(JobRow::new).collect(),
        page,
        prev_url: (page > 1).then(|| url(page - 1)),
        next_url: (jobs.len() == PAGE_SIZE).then(|| url(page + 1)),
        return_to: url(page),
        can_retry: job_state == JobState::Dead,
        can_discard: job_state.is_finished(),
        can_cancel: matches!(job_state, JobState::Pending | JobState::Scheduled),
        can_run_now: job_state == JobState::Scheduled,
        show_run_at: job_state == JobState::Scheduled,
        show_error: matches!(job_state, JobState::Dead | JobState::Scheduled),
    };
    Ok(Html(page_view.render()?))
}

#[derive(Template)]
#[template(path = "job.html")]
struct JobPage {
    base: String,
    nav: &'static str,
    id: String,
    name: String,
    queue: String,
    state: &'static str,
    label: &'static str,
    attempts: u32,
    enqueued_ms: u64,
    enqueued_ago: String,
    /// For a scheduled job: when it runs, in ms and as "in 5m".
    run_at: Option<(u64, String)>,
    args: String,
    /// What enqueue layers stored with the job, if anything.
    meta: Option<String>,
    last_error: Option<String>,
    result: Option<String>,
    progress: Option<String>,
    return_to: String,
    can_retry: bool,
    can_discard: bool,
    can_cancel: bool,
    can_run_now: bool,
}

fn pretty(value: &impl Serialize) -> String {
    serde_json::to_string_pretty(value).unwrap_or_default()
}

pub(crate) async fn job(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Html<String>, WebError> {
    let lookup = id.clone();
    let job: AnyJob = blocking(&state.queue, move |queue| queue.get(&lookup))
        .await?
        .ok_or_else(|| WebError::NotFound(id.clone()))?;
    let job_state = job.state();
    let record = job.record();
    let page = JobPage {
        base: state.base.clone(),
        nav: "jobs",
        id: record.id.clone(),
        name: record.name.clone(),
        queue: record.queue.clone(),
        state: job_state.as_str(),
        label: state_label(job_state),
        attempts: record.attempts,
        enqueued_ms: record.enqueued_at_ms,
        enqueued_ago: ago(record.enqueued_at_ms),
        run_at: record
            .run_at_ms
            .filter(|_| job_state == JobState::Scheduled)
            .map(|ms| (ms, until(ms))),
        args: pretty(&record.args),
        meta: (!record.meta.is_empty()).then(|| pretty(&record.meta)),
        last_error: record.last_error.clone(),
        result: record.result.as_ref().map(pretty),
        progress: record.progress.as_ref().map(pretty),
        return_to: format!("{}/jobs/{}", state.base, record.id),
        can_retry: job_state == JobState::Dead,
        can_discard: job_state.is_finished(),
        can_cancel: matches!(job_state, JobState::Pending | JobState::Scheduled),
        can_run_now: job_state == JobState::Scheduled,
    };
    Ok(Html(page.render()?))
}

pub(crate) struct WorkerRow {
    pub id: String,
    pub running: String,
    pub alive: bool,
    pub heartbeat: String,
}

#[derive(Template)]
#[template(path = "workers.html")]
struct WorkersPage {
    base: String,
    nav: &'static str,
    rows: Vec<WorkerRow>,
}

pub(crate) async fn workers(State(state): State<Arc<AppState>>) -> Result<Html<String>, WebError> {
    let stats = read_stats(&state.queue).await?;
    let rows = stats
        .workers
        .iter()
        .map(|worker| {
            let alive = worker.expires_in_ms > 0;
            let ms = worker.expires_in_ms.unsigned_abs();
            WorkerRow {
                id: worker.id.clone(),
                running: thousands(worker.running),
                alive,
                heartbeat: if alive {
                    format!("expires in {}", duration(ms))
                } else {
                    "expired: its jobs will be recovered".to_owned()
                },
            }
        })
        .collect();
    let page = WorkersPage {
        base: state.base.clone(),
        nav: "workers",
        rows,
    };
    Ok(Html(page.render()?))
}
