//! Runs butler jobs inside a tokio main loop. Ctrl-C stops claiming new jobs
//! and waits for the running ones to finish.
//!
//!   cargo run -p demo --bin worker
//!
//! Backend and worker settings come from ./config.toml (see butler::Config).

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = butler::Config::load()?;
    let worker = butler::Worker::from_config(&config)?.register(demo::process_tick::JOB);
    println!(
        "[worker {}] started on {}, jobs: {:?}",
        std::process::id(),
        worker.queue().describe(),
        worker.job_names()
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
