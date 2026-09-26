use axum::{
    extract::Path,
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};

/// Everything the pages load, compiled into the binary: nothing to deploy
/// next to it, and no CDN.
const ASSETS: &[(&str, &str, &[u8])] = &[
    (
        "app.css",
        "text/css; charset=utf-8",
        include_bytes!("../assets/app.css"),
    ),
    (
        "app.js",
        "text/javascript; charset=utf-8",
        include_bytes!("../assets/app.js"),
    ),
    (
        "uplot.min.js",
        "text/javascript; charset=utf-8",
        include_bytes!("../assets/uplot.min.js"),
    ),
    (
        "uplot.min.css",
        "text/css; charset=utf-8",
        include_bytes!("../assets/uplot.min.css"),
    ),
];

pub(crate) async fn serve(Path(file): Path<String>) -> Response {
    match ASSETS.iter().find(|(name, _, _)| *name == file) {
        Some((_, content_type, bytes)) => (
            [
                (header::CONTENT_TYPE, *content_type),
                (header::CACHE_CONTROL, "public, max-age=300"),
                (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
            ],
            *bytes,
        )
            .into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}
