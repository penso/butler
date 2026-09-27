//! What the dashboard changes: retry and discard failed jobs, cancel pending
//! and scheduled ones, run scheduled ones now, pause and resume queues, and
//! forget recurring schedules no worker runs anymore. Every action is a POST and
//! redirects back to the page it came from.

use std::sync::Arc;

use axum::{
    Form,
    extract::{Path, State},
    response::Redirect,
};
use butler::{JobState, monitor::ListFilter};
use serde::Deserialize;

use crate::{AppState, WebError};

#[derive(Deserialize)]
pub(crate) struct Back {
    return_to: Option<String>,
}

#[derive(Deserialize)]
pub(crate) struct Bulk {
    state: String,
    queue: Option<String>,
    return_to: Option<String>,
}

/// Only paths inside the dashboard: never an open redirect.
fn back(state: &AppState, return_to: Option<&str>) -> Redirect {
    let target = return_to
        .filter(|path| {
            path.starts_with('/') && !path.starts_with("//") && path.starts_with(&state.base)
        })
        .map_or_else(|| crate::home(&state.base), str::to_owned);
    Redirect::to(&target)
}

async fn act(
    state: &AppState,
    call: impl FnOnce(&butler::Queue) -> butler::Result<bool> + Send + 'static,
) -> Result<bool, WebError> {
    let queue = state.queue.clone();
    Ok(tokio::task::spawn_blocking(move || call(&queue)).await??)
}

pub(crate) async fn retry(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Form(form): Form<Back>,
) -> Result<Redirect, WebError> {
    act(&state, move |queue| queue.retry(&id)).await?;
    Ok(back(&state, form.return_to.as_deref()))
}

pub(crate) async fn discard(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Form(form): Form<Back>,
) -> Result<Redirect, WebError> {
    act(&state, move |queue| queue.discard(&id)).await?;
    // The job is gone: its own page would 404, so go to the list instead.
    let return_to = form
        .return_to
        .filter(|path| !path.contains("/jobs/"))
        .unwrap_or_else(|| format!("{}/jobs?state=dead", state.base));
    Ok(back(&state, Some(&return_to)))
}

pub(crate) async fn cancel(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Form(form): Form<Back>,
) -> Result<Redirect, WebError> {
    act(&state, move |queue| queue.cancel(&id)).await?;
    Ok(back(&state, form.return_to.as_deref()))
}

pub(crate) async fn run_now(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Form(form): Form<Back>,
) -> Result<Redirect, WebError> {
    act(&state, move |queue| queue.run_now(&id)).await?;
    Ok(back(&state, form.return_to.as_deref()))
}

pub(crate) async fn pause_queue(
    State(state): State<Arc<AppState>>,
    Path(queue): Path<String>,
    Form(form): Form<Back>,
) -> Result<Redirect, WebError> {
    let name = queue.clone();
    if act(&state, move |backend| backend.pause_queue(&name)).await? {
        tracing::info!(queue, "paused a queue from the dashboard");
    }
    Ok(back(&state, form.return_to.as_deref()))
}

pub(crate) async fn resume_queue(
    State(state): State<Arc<AppState>>,
    Path(queue): Path<String>,
    Form(form): Form<Back>,
) -> Result<Redirect, WebError> {
    let name = queue.clone();
    if act(&state, move |backend| backend.resume_queue(&name)).await? {
        tracing::info!(queue, "resumed a queue from the dashboard");
    }
    Ok(back(&state, form.return_to.as_deref()))
}

pub(crate) async fn remove_recurring(
    State(state): State<Arc<AppState>>,
    Path(key): Path<String>,
    Form(form): Form<Back>,
) -> Result<Redirect, WebError> {
    if !butler::recurring::is_valid_key(&key) {
        return Err(butler::Error::InvalidRecurringKey { key }.into());
    }
    let removed = act(&state, {
        let key = key.clone();
        move |queue| queue.remove_recurring(&key)
    })
    .await?;
    if removed {
        tracing::info!(
            schedule = key,
            "removed a recurring schedule from the dashboard"
        );
    }
    Ok(back(&state, form.return_to.as_deref()))
}

/// Applies `each` to every job in `bulk.state` (and queue), page by page,
/// until none is left or nothing changes.
async fn for_all(
    state: &AppState,
    bulk: &Bulk,
    each: fn(&butler::Queue, &str) -> butler::Result<bool>,
) -> Result<u64, WebError> {
    let Some(job_state) = JobState::parse(&bulk.state) else {
        return Ok(0);
    };
    let queue_name = bulk.queue.clone().filter(|queue| !queue.is_empty());
    let queue = state.queue.clone();
    Ok(tokio::task::spawn_blocking(move || -> butler::Result<u64> {
        let mut done = 0;
        loop {
            let mut filter = ListFilter::new(job_state);
            filter.queue = queue_name.clone();
            filter.limit = 500;
            let page = queue.list(&filter)?;
            let mut changed = 0;
            for job in &page {
                if each(&queue, &job.record().id)? {
                    changed += 1;
                }
            }
            done += changed;
            if page.len() < filter.limit || changed == 0 {
                return Ok(done);
            }
        }
    })
    .await??)
}

pub(crate) async fn retry_all(
    State(state): State<Arc<AppState>>,
    Form(bulk): Form<Bulk>,
) -> Result<Redirect, WebError> {
    let retried = for_all(&state, &bulk, |queue, id| queue.retry(id)).await?;
    tracing::info!(
        retried,
        state = bulk.state,
        "retried jobs from the dashboard"
    );
    Ok(back(&state, bulk.return_to.as_deref()))
}

pub(crate) async fn discard_all(
    State(state): State<Arc<AppState>>,
    Form(bulk): Form<Bulk>,
) -> Result<Redirect, WebError> {
    let discarded = for_all(&state, &bulk, |queue, id| queue.discard(id)).await?;
    tracing::info!(
        discarded,
        state = bulk.state,
        "discarded jobs from the dashboard"
    );
    Ok(back(&state, bulk.return_to.as_deref()))
}
