//! The dashboard over an in-memory queue seeded with known jobs, for the
//! browser tests in `e2e/`. It prints `listening on http://…` once it serves,
//! then keeps finishing a job every 100 ms so the live chart has data.
//!
//!   butler-e2e-server [--port N] [--base /admin/jobs] [--auth user:password]

use std::time::{Duration, SystemTime};

use anyhow::{Context, bail};
use butler::{MemoryQueue, Queue, monitor::JobMetric};
use serde_json::json;

const WORKER: &str = "e2e-worker";

struct Args {
    port: u16,
    base: String,
    auth: Option<(String, String)>,
}

fn args() -> anyhow::Result<Args> {
    let mut args = Args {
        port: 0,
        base: String::new(),
        auth: None,
    };
    let mut given = std::env::args().skip(1);
    while let Some(flag) = given.next() {
        let value = given
            .next()
            .with_context(|| format!("{flag} needs a value"))?;
        match flag.as_str() {
            "--port" => args.port = value.parse().context("--port")?,
            "--base" => args.base = value,
            "--auth" => {
                let (user, password) = value
                    .split_once(':')
                    .context("--auth takes user:password")?;
                args.auth = Some((user.to_owned(), password.to_owned()));
            }
            _ => bail!("unknown flag {flag}"),
        }
    }
    Ok(args)
}

/// Known jobs in every state the pages act on.
fn seed(queue: &Queue) -> anyhow::Result<()> {
    queue.heartbeat(WORKER, Duration::from_secs(60))?;
    for (name, target, error) in [
        (
            "charge_card",
            "default",
            "card declined: insufficient funds",
        ),
        ("sync_account", "mailers", "upstream timed out"),
    ] {
        queue.push(name, target, vec![json!(42)])?;
        let job = queue
            .claim(WORKER, &[target], Duration::ZERO)?
            .context("claiming a seeded job")?;
        queue.fail(WORKER, job, error.to_owned(), 0)?;
    }
    queue.push("send_email", "mailers", vec![json!("ada@example.com")])?;
    let job = queue
        .claim(WORKER, &["mailers"], Duration::ZERO)?
        .context("claiming a seeded job")?;
    queue.complete(WORKER, job, json!({ "message_id": "m-1" }))?;
    queue.push("resize", "default", vec![])?;
    let later = SystemTime::now() + Duration::from_secs(3600);
    for target in ["mailers", "mailers", "default"] {
        queue.schedule(
            "send_reminder",
            target,
            vec![json!("ada@example.com")],
            later,
        )?;
    }
    Ok(())
}

/// Finishes a job on the `live` queue every 100 ms, one in ten failing.
fn keep_busy(queue: Queue) {
    let mut tick: u64 = 0;
    loop {
        std::thread::sleep(Duration::from_millis(100));
        tick += 1;
        let failed = tick.is_multiple_of(10);
        let run = || -> butler::Result<()> {
            queue.heartbeat(WORKER, Duration::from_secs(60))?;
            queue.push("tick", "live", vec![json!(tick)])?;
            if let Some(job) = queue.claim(WORKER, &["live"], Duration::ZERO)? {
                if failed {
                    queue.fail(WORKER, job, format!("tick {tick} failed"), 0)?;
                } else {
                    queue.complete(WORKER, job, json!(tick))?;
                }
            }
            queue.record_metric(&JobMetric::now("tick", "live", failed, 5 + tick % 20))
        };
        if let Err(err) = run() {
            eprintln!("e2e server: {err}");
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = args()?;
    let queue: Queue = MemoryQueue::new().into();
    seed(&queue)?;
    std::thread::spawn({
        let queue = queue.clone();
        move || keep_busy(queue)
    });

    let mut dashboard = butler_web::Dashboard::new(queue).base_path(&args.base);
    if let Some((user, password)) = &args.auth {
        dashboard = dashboard.basic_auth(user, password);
    }
    let app = if args.base.is_empty() {
        dashboard.router()
    } else {
        axum::Router::new().nest(&args.base, dashboard.router())
    };
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", args.port)).await?;
    // The test harness waits for this line to learn the port.
    println!("listening on http://{}", listener.local_addr()?);
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}
