//! Runs butler jobs inside a tokio main loop. Ctrl-C stops claiming new jobs
//! and waits for the running ones to finish.
//!
//!   cargo run -p demo --bin worker
//!
//! Backend and worker settings come from ./butler.toml (see butler::Config).

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_target(false).init();
    let config = butler::Config::load()?;
    // The jobs live in another crate, so they are registered before the
    // `[[recurring]]` entries that name them are read.
    let worker = butler::Worker::new(config.connect()?)
        .register(demo::process_tick::JOB)
        .register(demo::alert::JOB)
        .register(demo::summary::JOB)
        .with_config(&config.worker)
        .with_recurring_config(&config.recurring)?;
    println!(
        "[worker {}] started on {}, jobs: {:?}, queues: {:?}, recurring: {:?}",
        std::process::id(),
        worker.queue().describe(),
        worker.job_names(),
        worker.served_queues(),
        worker
            .recurring_schedules()
            .iter()
            .map(|schedule| format!("{} ({})", schedule.name(), schedule.cron().expression()))
            .collect::<Vec<_>>()
    );

    worker
        .run_async(async {
            let _ = tokio::signal::ctrl_c().await;
            println!("[worker] ctrl-c: finishing running jobs");
        })
        .await;

    println!("[worker] stopped");
    Ok(())
}
