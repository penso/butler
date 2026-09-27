use std::sync::Arc;

use askama::Template;
use axum::{
    extract::{Request, State},
    http::StatusCode,
    middleware::Next,
    response::{Html, IntoResponse, Response},
};

use crate::AppState;

/// Why a dashboard request failed.
#[derive(Debug, thiserror::Error)]
pub enum WebError {
    #[error("queue backend error")]
    Queue(#[from] butler::Error),

    #[error("job {0} not found")]
    NotFound(String),

    #[error("could not render the page")]
    Render(#[from] askama::Error),

    #[error("background task failed")]
    Join(#[from] tokio::task::JoinError),
}

#[derive(Template)]
#[template(path = "error.html")]
struct ErrorPage<'a> {
    base: &'a str,
    nav: &'a str,
    status: u16,
    message: &'a str,
}

/// What an error page shows, kept on its response so [`with_base_path`] can
/// render it again with the links of the dashboard that produced it.
#[derive(Clone)]
struct ErrorDetails {
    status: StatusCode,
    message: String,
}

impl ErrorDetails {
    fn render(&self, base: &str) -> Response {
        let page = ErrorPage {
            base,
            nav: "",
            status: self.status.as_u16(),
            message: &self.message,
        };
        match page.render() {
            Ok(html) => (self.status, Html(html)).into_response(),
            Err(_) => (self.status, self.message.clone()).into_response(),
        }
    }
}

impl IntoResponse for WebError {
    fn into_response(self) -> Response {
        let status = match &self {
            WebError::NotFound(_) => StatusCode::NOT_FOUND,
            WebError::Queue(butler::Error::InvalidRecurringKey { .. }) => StatusCode::BAD_REQUEST,
            WebError::Queue(butler::Error::Unsupported(_)) => StatusCode::NOT_IMPLEMENTED,
            WebError::Queue(butler::Error::InvalidQueue { .. }) => StatusCode::BAD_REQUEST,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        if status.is_server_error() {
            tracing::error!(error = %crate::views::chain(&self), "dashboard request failed");
        }
        let details = ErrorDetails {
            status,
            message: crate::views::chain(&self),
        };
        // Outside a dashboard router this is all there is, linking to the root.
        let mut response = details.render("");
        response.extensions_mut().insert(details);
        response
    }
}

/// Renders error pages again with the dashboard's base path, since
/// `IntoResponse` can't see where the router is mounted.
pub(crate) async fn with_base_path(
    State(state): State<Arc<AppState>>,
    request: Request,
    next: Next,
) -> Response {
    let response = next.run(request).await;
    match response.extensions().get::<ErrorDetails>() {
        Some(details) if !state.base.is_empty() => details.render(&state.base),
        _ => response,
    }
}

/// Paths the dashboard doesn't have, as an error page rather than an empty 404.
pub(crate) async fn not_found(State(state): State<Arc<AppState>>) -> Response {
    ErrorDetails {
        status: StatusCode::NOT_FOUND,
        message: "page not found".to_owned(),
    }
    .render(&state.base)
}
