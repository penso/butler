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

use crate::{
    FileQueue, Job, JobState, block_on,
    __private::{BoxFuture, JobDef},
};

type Handler = fn(Vec<serde_json::Value>) -> BoxFuture;

/// Pulls jobs from a [`FileQueue`] and runs them. It can run every `#[job]`
/// function compiled into the current binary.
#[derive(Clone)]
pub struct Worker {
    queue: FileQueue,
    handlers: Arc<HashMap<&'static str, Handler>>,
    concurrency: usize,
    max_retries: u32,
    poll_interval: Duration,
}

impl Worker {
    pub fn new(queue: FileQueue) -> Self {
        let mut handlers = HashMap::new();
        for def in inventory::iter::<JobDef> {
            if handlers.insert(def.name, def.perform).is_some() {
                panic!("butler: two jobs are registered as `{}`", def.name);
            }
        }
        Self {
            queue,
            handlers: Arc::new(handlers),
            concurrency: 1,
            max_retries: 3,
            poll_interval: Duration::from_millis(100),
        }
    }

    /// Sets how many worker threads run jobs at the same time.
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

    /// Runs forever.
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
                                eprintln!("butler: queue error: {e}");
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
        match self.execute(&job) {
            Ok(()) => self.queue.complete(&job)?,
            Err(err) => {
                let state = self.queue.fail(job.clone(), err.clone(), self.max_retries)?;
                let verb = if state == JobState::Dead { "is dead" } else { "will retry" };
                eprintln!("butler: job {} ({}) failed and {verb}: {err}", job.name, job.id);
            }
        }
        Ok(true)
    }

    fn execute(&self, job: &Job) -> Result<(), String> {
        let handler = self
            .handlers
            .get(job.name.as_str())
            .ok_or_else(|| format!("no job named `{}` is registered in this worker", job.name))?;
        let fut = handler(job.args.clone());
        catch_unwind(AssertUnwindSafe(|| block_on(fut)))
            .unwrap_or_else(|panic| Err(format!("job panicked: {}", panic_message(&panic))))
    }
}

fn panic_message(panic: &Box<dyn std::any::Any + Send>) -> String {
    panic
        .downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| panic.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown panic".into())
}
