//! `butler-web`: the dashboard as its own server, reading `config.toml` (or
//! `BUTLER_*` variables) like a worker does.
//!
//!   butler-web                  # http://127.0.0.1:9090
//!   butler-web 0.0.0.0:8080     # listen elsewhere (put auth in front first)

use anyhow::Context;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_target(false).init();
    let listen = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:9090".to_owned());
    let config = butler::Config::load().context("loading config.toml")?;
    let queue = config
        .connect()
        .context("connecting to the queue backend")?;
    let backend = queue.describe();

    let listener = tokio::net::TcpListener::bind(&listen)
        .await
        .with_context(|| format!("listening on {listen}"))?;
    tracing::info!(%backend, "butler dashboard on http://{listen}");
    axum::serve(listener, butler_web::router(queue))
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
        .context("serving the dashboard")?;
    Ok(())
}
