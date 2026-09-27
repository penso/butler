//! A web dashboard for [butler](https://crates.io/crates/butler), like
//! Sidekiq's Web UI or Rails' Mission Control: live counts over server-sent
//! events, throughput and duration charts, and the jobs themselves, with
//! retry and discard for failed ones, run now or cancel for scheduled ones,
//! and pause or resume for queues.
//!
//! Mount it in your own axum app, behind your own authentication:
//!
//! ```ignore
//! let app = Router::new()
//!     .nest("/admin/jobs", butler_web::Dashboard::new(queue).base_path("/admin/jobs").router())
//!     .layer(your_auth_layer);
//! ```
//!
//! or run the `butler-web` binary, which reads `butler.toml` like a worker.
//!
//! The dashboard has no authentication of its own. Actions (retry, discard,
//! cancel, run now, pause and resume a queue) are POSTs, and cross-site POSTs are rejected, so another site
//! can't trigger them through a logged-in browser.

mod actions;
mod assets;
mod error;
mod events;
mod pages;
mod views;

use std::sync::{Arc, OnceLock};

use axum::{
    Router,
    extract::Request,
    http::{Method, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use butler::Queue;

pub use error::WebError;

/// The dashboard for one queue backend.
#[derive(Clone)]
pub struct Dashboard {
    queue: Queue,
    base: String,
}

/// Shared by every request.
pub(crate) struct AppState {
    pub queue: Queue,
    /// Where the dashboard is mounted, without a trailing slash ("" at the root).
    pub base: String,
    /// The live snapshot stream, started by the first SSE client.
    pub live: OnceLock<tokio::sync::watch::Receiver<events::Snapshot>>,
}

impl Dashboard {
    pub fn new(queue: impl Into<Queue>) -> Self {
        Self {
            queue: queue.into(),
            base: String::new(),
        }
    }

    /// Where the router is mounted, e.g. `"/admin/jobs"`, so its links point
    /// there. Defaults to the root.
    pub fn base_path(mut self, path: &str) -> Self {
        self.base = path.trim_end_matches('/').to_owned();
        self
    }

    pub fn router(self) -> Router {
        let state = Arc::new(AppState {
            queue: self.queue,
            base: self.base,
            live: OnceLock::new(),
        });
        Router::new()
            .route("/", get(pages::dashboard))
            .route("/jobs", get(pages::jobs))
            .route("/jobs/{id}", get(pages::job))
            .route("/jobs/{id}/retry", post(actions::retry))
            .route("/jobs/{id}/discard", post(actions::discard))
            .route("/jobs/{id}/cancel", post(actions::cancel))
            .route("/jobs/{id}/run-now", post(actions::run_now))
            .route("/jobs/retry-all", post(actions::retry_all))
            .route("/jobs/discard-all", post(actions::discard_all))
            .route("/workers", get(pages::workers))
            .route("/queues/{queue}/pause", post(actions::pause_queue))
            .route("/queues/{queue}/resume", post(actions::resume_queue))
            .route("/events", get(events::stream))
            .route("/api/stats", get(events::stats_json))
            .route("/api/metrics", get(pages::metrics_json))
            .route("/assets/{file}", get(assets::serve))
            .layer(middleware::from_fn(same_origin_posts))
            .with_state(state)
    }
}

/// The dashboard at the root of its own router.
pub fn router(queue: impl Into<Queue>) -> Router {
    Dashboard::new(queue).router()
}

/// Rejects state-changing requests sent from another site, using the
/// browser's `Sec-Fetch-Site` header, or `Origin` when that is missing.
async fn same_origin_posts(request: Request, next: Next) -> Response {
    if request.method() == Method::GET || request.method() == Method::HEAD {
        return next.run(request).await;
    }
    let headers = request.headers();
    let site = headers
        .get("sec-fetch-site")
        .and_then(|value| value.to_str().ok());
    let allowed = match site {
        Some(site) => site == "same-origin" || site == "none",
        None => match (headers.get(header::ORIGIN), headers.get(header::HOST)) {
            (Some(origin), Some(host)) => origin
                .to_str()
                .ok()
                .and_then(|origin| origin.split("://").nth(1))
                .zip(host.to_str().ok())
                .is_some_and(|(origin_host, host)| origin_host == host),
            // Not a browser form post (curl, scripts): nothing to forge.
            (None, _) => true,
            (Some(_), None) => false,
        },
    };
    if allowed {
        next.run(request).await
    } else {
        (StatusCode::FORBIDDEN, "cross-site request rejected").into_response()
    }
}
