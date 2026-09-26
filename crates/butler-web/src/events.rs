//! Live counts over server-sent events. One sampler per dashboard reads the
//! backend once a second, only while someone is watching, and every client
//! receives its snapshots: ten open tabs don't mean ten times the load.

use std::{convert::Infallible, sync::Arc, time::Duration};

use axum::{
    Json,
    extract::State,
    response::sse::{Event, KeepAlive, Sse},
};
use butler::{Queue, monitor::Stats};
use serde::Serialize;
use tokio::sync::watch;
use tokio_stream::{Stream, StreamExt, wrappers::WatchStream};

use crate::{AppState, WebError, views::now_ms};

const SAMPLE_EVERY: Duration = Duration::from_secs(1);

/// What the dashboard shows live. Rates are computed in the browser from
/// consecutive `processed_total` and `failed_total`.
#[derive(Debug, Clone, Default, Serialize)]
pub(crate) struct Snapshot {
    pub ts_ms: u64,
    pub processed_total: u64,
    pub failed_total: u64,
    pub processing: u64,
    pub pending: u64,
    pub dead: u64,
    pub done: u64,
    pub cancelled: u64,
    pub workers_alive: usize,
    pub queues: Vec<(String, u64)>,
    /// Set when the backend couldn't be read this time.
    pub error: Option<String>,
}

impl Snapshot {
    pub fn from_stats(stats: &Stats) -> Self {
        Self {
            ts_ms: now_ms(),
            processed_total: stats.processed_total,
            failed_total: stats.failed_total,
            processing: stats.processing,
            pending: stats.pending(),
            dead: stats.dead,
            done: stats.done,
            cancelled: stats.cancelled,
            workers_alive: stats
                .workers
                .iter()
                .filter(|worker| worker.expires_in_ms > 0)
                .count(),
            queues: stats
                .queues
                .iter()
                .map(|queue| (queue.name.clone(), queue.pending))
                .collect(),
            error: None,
        }
    }
}

pub(crate) async fn read_stats(queue: &Queue) -> Result<Stats, WebError> {
    let queue = queue.clone();
    Ok(tokio::task::spawn_blocking(move || queue.stats()).await??)
}

pub(crate) async fn stream(
    State(state): State<Arc<AppState>>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let snapshots = WatchStream::new(live(&state)).map(|snapshot| {
        Ok(Event::default()
            .event("stats")
            .data(serde_json::to_string(&snapshot).unwrap_or_default()))
    });
    Sse::new(snapshots).keep_alive(KeepAlive::default())
}

pub(crate) async fn stats_json(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Snapshot>, WebError> {
    Ok(Json(Snapshot::from_stats(&read_stats(&state.queue).await?)))
}

/// The shared snapshot channel, starting its sampler on first use.
fn live(state: &Arc<AppState>) -> watch::Receiver<Snapshot> {
    state
        .live
        .get_or_init(|| {
            let (sender, receiver) = watch::channel(Snapshot::default());
            let queue = state.queue.clone();
            tokio::spawn(sample(queue, sender));
            receiver
        })
        .clone()
}

async fn sample(queue: Queue, sender: watch::Sender<Snapshot>) {
    let mut tick = tokio::time::interval(SAMPLE_EVERY);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tick.tick().await;
        // The dashboard keeps its own receiver, so count the others.
        if sender.receiver_count() <= 1 {
            continue;
        }
        let snapshot = match read_stats(&queue).await {
            Ok(stats) => Snapshot::from_stats(&stats),
            Err(err) => Snapshot {
                ts_ms: now_ms(),
                error: Some(crate::views::chain(&err)),
                ..sender.borrow().clone()
            },
        };
        sender.send_replace(snapshot);
    }
}
