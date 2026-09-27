use askama::Template;
use axum::{
    http::StatusCode,
    response::{Html, IntoResponse, Response},
};

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
    message: String,
}

impl IntoResponse for WebError {
    fn into_response(self) -> Response {
        let status = match &self {
            WebError::NotFound(_) => StatusCode::NOT_FOUND,
            WebError::Queue(butler::Error::InvalidRecurringKey { .. }) => StatusCode::BAD_REQUEST,
            WebError::Queue(butler::Error::Unsupported(_)) => StatusCode::NOT_IMPLEMENTED,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        if status.is_server_error() {
            tracing::error!(error = %crate::views::chain(&self), "dashboard request failed");
        }
        let page = ErrorPage {
            base: "",
            nav: "",
            status: status.as_u16(),
            message: crate::views::chain(&self),
        };
        match page.render() {
            Ok(html) => (status, Html(html)).into_response(),
            Err(_) => (status, crate::views::chain(&self)).into_response(),
        }
    }
}
