use std::{
    collections::HashMap,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};

use anyhow::{Context, anyhow};

use crate::{
    Config, Job, JobDef, JobState, Queue, WorkerConfig, block_on,
    __private::BoxFuture,
};

type Handler = fn(Vec<serde_json::Value>) -> BoxFuture;

/// Pulls jobs from a [`Queue`] and runs them. It can run every `#[job]`
/// function compiled into the current binary.
#[derive(Clone)]
pub struct Worker {
    queue: Queue,
    handlers: Arc<HashMap<&'static str, Handler>>,
    concurrency: usize,
    max_retries: u32,
    poll_interval: Duration,
}

impl Worker {
    /// Opens the configured backend and applies the `[worker]` settings.
    pub fn from_config(config: &Config) -> Result<Self, crate::Error> {
        Ok(Self::new(config.connect()?).with_config(&config.worker))
    }

    pub fn new(queue: impl Into<Queue>) -> Self {
        let defaults = WorkerConfig::default();
        let mut handlers = HashMap::new();
        for def in inventory::iter::<JobDef> {
            if handlers.insert(def.name, def.perform).is_some() {
                panic!("butler: two jobs are registered as `{}`", def.name);
            }
        }
        Self {
            queue: queue.into(),
            handlers: Arc::new(handlers),
            concurrency: defaults.concurrency,
            max_retries: defaults.max_retries,
            poll_interval: defaults.poll_interval(),
        }
    }

    pub fn with_config(self, config: &WorkerConfig) -> Self {
        self.concurrency(config.concurrency)
            .max_retries(config.max_retries)
            .poll_interval(config.poll_interval())
    }

    pub fn queue(&self) -> &Queue {
        &self.queue
    }

    /// Adds a job explicitly. Automatic registration covers jobs in the crate
    /// that builds the worker binary. For jobs in a library crate, call
    /// `.register(my_lib::my_job::JOB)`, or the linker may drop them.
    pub fn register(mut self, job: JobDef) -> Self {
        Arc::make_mut(&mut self.handlers).insert(job.name, job.perform);
        self
    }

    /// Sets how many jobs run at the same time (threads for `run`, tasks for `run_async`).
    pub fn concurrency(mut self, n: usize) -> Self {
        self.concurrency = n.max(1);
        self
    }

    pub fn max_retries(mut self, n: u32) -> Self {
        self.max_retries = n;
        self
    }

    pub fn poll_interval(mut self, d: Duration) -> Self {
        self.poll_interval = d;
        self
    }

    pub fn job_names(&self) -> Vec<&'static str> {
        let mut names: Vec<_> = self.handlers.keys().copied().collect();
        names.sort();
        names
    }

    /// Runs forever on plain threads, using the built-in `block_on`. Jobs that
    /// use tokio APIs need `run_async` instead.
    pub fn run(self) {
        self.run_until(Arc::new(AtomicBool::new(false)));
    }

    /// Runs until `stop` is set, then waits for the threads to finish their
    /// current jobs.
    pub fn run_until(self, stop: Arc<AtomicBool>) {
        let threads: Vec<_> = (0..self.concurrency)
            .map(|_| {
                let worker = self.clone();
                let stop = stop.clone();
                thread::spawn(move || {
                    while !stop.load(Ordering::Relaxed) {
                        match worker.work_one() {
                            Ok(true) => {}
                            Ok(false) => thread::sleep(worker.poll_interval),
                            Err(e) => {
                                eprintln!("butler: queue error: {:#}", anyhow::Error::from(e));
                                thread::sleep(worker.poll_interval);
                            }
                        }
                    }
                })
            })
            .collect();
        for t in threads {
            let _ = t.join();
        }
    }

    /// Runs jobs on the current thread until the queue is empty, and returns
    /// how many ran. This includes retries, so a job that always fails
    /// runs `max_retries + 1` times. Similar to `Sidekiq::Worker.drain_all`.
    pub fn drain(&self) -> Result<usize, crate::Error> {
        let mut n = 0;
        while self.work_one()? {
            n += 1;
        }
        Ok(n)
    }

    /// Claims and runs one job. Returns `Ok(false)` if the queue was empty.
    pub fn work_one(&self) -> Result<bool, crate::Error> {
        let Some(job) = self.queue.claim()? else {
            return Ok(false);
        };
        let result = match self.handler(&job) {
            Ok(handler) => {
                let fut = handler(job.args.clone());
                catch_unwind(AssertUnwindSafe(|| block_on(fut)))
                    .unwrap_or_else(|panic| Err(anyhow!("job panicked: {}", panic_message(&*panic))))
            }
            Err(e) => Err(e),
        };
        self.finish(job, result)?;
        Ok(true)
    }

    fn handler(&self, job: &Job) -> anyhow::Result<Handler> {
        self.handlers
            .get(job.name.as_str())
            .copied()
            .with_context(|| format!("no job named `{}` is registered in this worker", job.name))
    }

    /// Stores the outcome: `done/` on success, otherwise a retry or `dead/`.
    /// The error's full chain (`{:#}`) becomes the job's `last_error`.
    fn finish(&self, job: Job, result: anyhow::Result<()>) -> Result<(), crate::Error> {
        match result {
            Ok(()) => self.queue.complete(&job),
            Err(err) => {
                let err = format!("{err:#}");
                let (name, id) = (job.name.clone(), job.id.clone());
                let state = self.queue.fail(job, err.clone(), self.max_retries)?;
                let verb = if state == JobState::Dead { "is dead" } else { "will retry" };
                eprintln!("butler: job {name} ({id}) failed and {verb}: {err}");
                Ok(())
            }
        }
    }
}

#[cfg(feature = "tokio")]
impl Worker {
    /// Runs the worker inside the current tokio runtime until `shutdown`
    /// completes. Each job is a tokio task, so job bodies can use tokio timers
    /// and I/O. At most `concurrency` jobs run at once. On shutdown, the worker
    /// stops claiming jobs and waits for the running ones to finish.
    pub async fn run_async(self, shutdown: impl std::future::Future<Output = ()>) {
        use tokio::{sync::Semaphore, task::JoinSet};

        let permits = Arc::new(Semaphore::new(self.concurrency));
        let mut running = JoinSet::new();
        let mut shutdown = std::pin::pin!(shutdown);

        loop {
            let permit = tokio::select! {
                _ = &mut shutdown => break,
                permit = permits.clone().acquire_owned() => permit.expect("semaphore is never closed"),
            };

            // Not raced against `shutdown`: once a claim has started, it must
            // finish, or the job would be stranded in `processing/`.
            let queue = self.queue.clone();
            let claimed = tokio::task::spawn_blocking(move || queue.claim())
                .await
                .unwrap_or_else(|e| Err(e.into()));

            match claimed {
                Ok(Some(job)) => {
                    let worker = self.clone();
                    running.spawn(async move {
                        worker.execute_async(job).await;
                        drop(permit);
                    });
                }
                Ok(None) | Err(_) => {
                    if let Err(e) = claimed {
                        eprintln!("butler: queue error: {:#}", anyhow::Error::from(e));
                    }
                    drop(permit);
                    tokio::select! {
                        _ = &mut shutdown => break,
                        _ = tokio::time::sleep(self.poll_interval) => {}
                    }
                }
            }
            while running.try_join_next().is_some() {}
        }

        while running.join_next().await.is_some() {}
    }

    async fn execute_async(&self, job: Job) {
        let result = match self.handler(&job) {
            // A separate task, so a panic in the job surfaces as a JoinError.
            Ok(handler) => match tokio::spawn(handler(job.args.clone())).await {
                Ok(result) => result,
                Err(e) if e.is_panic() => {
                    Err(anyhow!("job panicked: {}", panic_message(&*e.into_panic())))
                }
                Err(e) => Err(anyhow::Error::from(e).context("job task was cancelled")),
            },
            Err(e) => Err(e),
        };

        let worker = self.clone();
        let stored = tokio::task::spawn_blocking(move || worker.finish(job, result))
            .await
            .map_err(crate::Error::from)
            .and_then(|r| r);
        if let Err(e) = stored {
            eprintln!("butler: could not store job result: {:#}", anyhow::Error::from(e));
        }
    }
}

fn panic_message(panic: &(dyn std::any::Any + Send)) -> String {
    panic
        .downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| panic.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown panic".into())
}
