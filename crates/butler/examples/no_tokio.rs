//! The same two-process flow as `examples/demo`, without tokio: the worker
//! runs jobs on plain threads and `.await` is driven by `butler::block_on`.
//!
//!   cargo run -p butler-jobs --example no_tokio -- worker            # terminal 1
//!   cargo run -p butler-jobs --example no_tokio -- enqueue Ada 3     # terminal 2
//!
//! Both read the queue settings from ./butler.toml (see `butler::Config`).

use std::time::Duration;

#[butler::job]
async fn greet(name: String, times: u32) {
    for i in 1..=times {
        println!("[{}] hello {name} ({i}/{times})", std::process::id());
    }
}

fn main() -> Result<(), butler::Error> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("worker") => {
            let worker = butler::Worker::new(butler::queue()?).concurrency(2);
            println!("worker started, jobs: {:?}", worker.job_names());
            worker.poll_interval(Duration::from_millis(50)).run();
        }
        Some("enqueue") => {
            let name = args.get(1).cloned().unwrap_or_else(|| "world".into());
            let times = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(1);
            // Any executor works here; butler's block_on stands in for your async code.
            let id = butler::block_on(async { greet(name, times).await })?;
            println!("enqueued {id}");
        }
        _ => eprintln!("usage: demo worker | demo enqueue <name> [times]"),
    }
    Ok(())
}
