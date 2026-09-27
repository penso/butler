//! `butler-web`: the dashboard as its own server, reading `butler.toml` (or
//! `BUTLER_*` variables) like a worker does.
//!
//!   butler-web                  # http://127.0.0.1:9090
//!   butler-web 0.0.0.0:8080     # listen elsewhere (put auth in front first)
//!
//! With `BUTLER_WEB_USERNAME` and `BUTLER_WEB_PASSWORD` both set, it asks for
//! them (HTTP basic auth); see `Dashboard::basic_auth` for what that does and
//! doesn't protect.

use anyhow::Context;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_target(false).init();
    let listen = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:9090".to_owned());
    let config = butler::Config::load().context("loading Butler configuration")?;
    let queue = config
        .connect()
        .context("connecting to the queue backend")?;
    let backend = queue.describe();

    let mut dashboard = butler_web::Dashboard::new(queue);
    match (
        std::env::var("BUTLER_WEB_USERNAME"),
        std::env::var("BUTLER_WEB_PASSWORD"),
    ) {
        (Ok(user), Ok(password)) if !user.is_empty() && !password.is_empty() => {
            dashboard = dashboard.basic_auth(&user, &password);
            tracing::info!(user, "basic auth on");
        }
        (Err(_), Err(_)) => {}
        _ => anyhow::bail!(
            "set both BUTLER_WEB_USERNAME and BUTLER_WEB_PASSWORD, non-empty, or neither"
        ),
    }

    let listener = tokio::net::TcpListener::bind(&listen)
        .await
        .with_context(|| format!("listening on {listen}"))?;
    tracing::info!(%backend, "butler dashboard on http://{listen}");
    axum::serve(listener, dashboard.router())
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
        .context("serving the dashboard")?;
    Ok(())
}
