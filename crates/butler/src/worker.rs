use std::{
    collections::HashMap,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use crate::{
    __private::BoxFuture,
    Config, Failed, Job, JobDef, JobError, Queue, QueuePriority, Result, WorkerConfig, block_on,
    error::Chain,
    executor::panic_message,
    limits::{Permit, QueueLimits},
    state::Processing,
};

type Handler = fn(Vec<serde_json::Value>) -> BoxFuture;

/// What one pass of a worker thread did.
enum Step {
    Ran,
    /// Nothing waiting on the queues it could take from.
    Idle,
    /// Only limited queues could have work, and they are full.
    Throttled,
}

/// The outcome of [`Worker::claim_limited`].
enum Claimed {
    /// A job, with its place in its queue's limit.
    Job(Job<Processing>, Permit),
    /// No job; `throttled` if some queue was skipped for being full.
    Nothing { throttled: bool },
}

/// `run` spends one OS thread per job slot, so it caps them here; tokio tasks
/// in `run_async` have no such cost.
const MAX_THREADS: usize = 512;

/// While a queue is skipped for being at its limit, idle claims re-check
/// this often, so a freed slot is used within about this long.
const LIMITED_RECHECK: Duration = Duration::from_millis(10);

/// How often the keeper wakes to check whether a heartbeat or a recovery pass
/// is due. The checks themselves are throttled; this only bounds the latency.
const KEEPER_TICK: Duration = Duration::from_millis(250);

/// Pulls jobs from a [`Queue`] and runs them. It can run every `#[job]`
/// function compiled into the current binary.
///
/// Each worker has an id and keeps a heartbeat in the backend while it runs.
/// If a worker process crashes, its heartbeat expires and another worker puts
/// the jobs it held back in the queue (see [`Backend`](crate::Backend)). Clones
/// share the id: they are the same worker.
#[derive(Clone)]
pub struct Worker {
    queue: Queue,
    handlers: Arc<HashMap<&'static str, Handler>>,
    id: Arc<str>,
    queues: QueuePriority,
    limits: QueueLimits,
    claimers: usize,
    concurrency: usize,
    max_retries: u32,
    poll_interval: Duration,
    heartbeat_ttl: Duration,
    recover_interval: Duration,
    upkeep: Arc<Mutex<Upkeep>>,
}

/// When this worker last refreshed its heartbeat and last recovered jobs.
#[derive(Default)]
struct Upkeep {
    beat: Option<Instant>,
    recovered: Option<Instant>,
}

impl Worker {
    /// Opens the configured backend and applies the `[worker]` settings.
    pub fn from_config(config: &Config) -> Result<Self> {
        Ok(Self::new(config.connect()?).with_config(&config.worker))
    }

    /// # Panics
    ///
    /// If two `#[job]` functions in this binary share a name. That is a
    /// build-time mistake, and a worker running either one would be wrong.
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
            id: new_worker_id().into(),
            queues: defaults.priority(),
            limits: QueueLimits::default(),
            claimers: defaults.claimers,
            concurrency: defaults.concurrency,
            max_retries: defaults.max_retries,
            poll_interval: defaults.poll_interval(),
            heartbeat_ttl: defaults.heartbeat_ttl(),
            recover_interval: defaults.recover_interval(),
            upkeep: Arc::default(),
        }
    }

    pub fn with_config(self, config: &WorkerConfig) -> Self {
        let worker = config
            .queue_limits
            .iter()
            .fold(self, |worker, (queue, max)| worker.queue_limit(queue, *max));
        worker
            .queues(config.priority())
            .concurrency(config.concurrency)
            .claimers(config.claimers)
            .max_retries(config.max_retries)
            .poll_interval(config.poll_interval())
            .heartbeat_ttl(config.heartbeat_ttl())
            .recover_interval(config.recover_interval())
    }

    pub fn queue(&self) -> &Queue {
        &self.queue
    }

    /// This worker's id in the backend, e.g. its processing list in Redis.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Adds a job explicitly. Automatic registration covers jobs in the crate
    /// that builds the worker binary. For jobs in a library crate, call
    /// `.register(my_lib::my_job::JOB)`, or the linker may drop them.
    pub fn register(mut self, job: JobDef) -> Self {
        Arc::make_mut(&mut self.handlers).insert(job.name, job.perform);
        self
    }

    /// Which queues to take jobs from, and in what priority. Defaults to just
    /// `"default"`. Jobs on queues not listed are never run by this worker.
    pub fn queues(mut self, queues: QueuePriority) -> Self {
        self.queues = queues;
        self
    }

    pub fn served_queues(&self) -> &QueuePriority {
        &self.queues
    }

    /// How many claim loops `run_async` runs side by side. Each takes one job
    /// at a time from the backend, so more of them start jobs faster: this is
    /// what lets a high `concurrency` actually fill up. Defaults to the number
    /// of CPUs; raise it for Redis, where each claim is a network round trip.
    pub fn claimers(mut self, n: usize) -> Self {
        self.claimers = n.max(1);
        self
    }

    /// Caps how many jobs from `queue` run at once, on top of the overall
    /// [`concurrency`](Worker::concurrency). When `queue` is at its limit, the
    /// worker skips it and keeps taking jobs from its other queues.
    pub fn queue_limit(mut self, queue: &str, max: usize) -> Self {
        self.limits.set(queue, max);
        self
    }

    /// The per-queue limits, sorted by queue name.
    pub fn queue_limits(&self) -> Vec<(&str, usize)> {
        let mut limits: Vec<_> = self.limits.limits().collect();
        limits.sort_unstable();
        limits
    }

    /// Sets how many jobs run at the same time (threads for `run`, tasks for
    /// `run_async`). Defaults to the number of CPUs. Async jobs that mostly wait
    /// on I/O can go much higher; for CPU-bound sync jobs, the CPU count is right.
    pub fn concurrency(mut self, n: usize) -> Self {
        self.concurrency = n.max(1);
        self
    }

    pub fn max_retries(mut self, n: u32) -> Self {
        self.max_retries = n;
        self
    }

    /// The longest an idle claim waits before checking the queues again. Redis
    /// (pub/sub) and memory wake claims the moment a job arrives, so there it
    /// is only a fallback; the file backend can't be woken, so it polls at
    /// this interval.
    pub fn poll_interval(mut self, d: Duration) -> Self {
        self.poll_interval = d;
        self
    }

    /// How long this worker counts as alive after each heartbeat. It refreshes
    /// every third of that, so a crash is noticed within about `ttl`.
    pub fn heartbeat_ttl(mut self, ttl: Duration) -> Self {
        self.heartbeat_ttl = ttl.max(Duration::from_millis(30));
        self
    }

    /// How often to look for jobs held by workers whose heartbeat expired.
    /// Also done once when the worker starts.
    pub fn recover_interval(mut self, d: Duration) -> Self {
        self.recover_interval = d;
        self
    }

    pub fn job_names(&self) -> Vec<&'static str> {
        let mut names: Vec<_> = self.handlers.keys().copied().collect();
        names.sort_unstable();
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
        self.log_upkeep(true);
        let keeper = {
            let worker = self.clone();
            let stop = Arc::clone(&stop);
            thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    worker.log_upkeep(false);
                    thread::sleep(KEEPER_TICK);
                }
            })
        };

        let threads = if self.concurrency > MAX_THREADS {
            tracing::warn!(
                concurrency = self.concurrency,
                threads = MAX_THREADS,
                "run() uses one OS thread per job slot; capping threads (use run_async for high concurrency)"
            );
            MAX_THREADS
        } else {
            self.concurrency
        };
        let threads: Vec<_> = (0..threads)
            .map(|_| {
                let worker = self.clone();
                let stop = Arc::clone(&stop);
                thread::spawn(move || {
                    while !stop.load(Ordering::Relaxed) {
                        let started = Instant::now();
                        match worker.work_one_within(worker.poll_interval) {
                            Ok(Step::Ran) => {}
                            Ok(Step::Idle) => {
                                thread::sleep(
                                    worker.poll_interval.saturating_sub(started.elapsed()),
                                );
                            }
                            Ok(Step::Throttled) => {
                                thread::sleep(LIMITED_RECHECK.saturating_sub(started.elapsed()));
                            }
                            Err(err) => {
                                tracing::error!(error = %Chain(&err), "queue error");
                                thread::sleep(worker.poll_interval);
                            }
                        }
                    }
                })
            })
            .collect();
        for thread in threads.into_iter().chain([keeper]) {
            if thread.join().is_err() {
                tracing::error!("a worker thread panicked outside of a job");
            }
        }
        self.log_retire();
    }

    /// Runs jobs on the current thread until the queue is empty, and returns
    /// how many ran. This includes retries, so a job that always fails
    /// runs `max_retries + 1` times. Similar to `Sidekiq::Worker.drain_all`.
    /// Recovers jobs from stopped workers first.
    pub fn drain(&self) -> Result<usize> {
        self.upkeep(true)?;
        let mut n = 0;
        while self.work_one()? {
            n += 1;
        }
        self.queue.retire(&self.id)?;
        Ok(n)
    }

    /// Claims and runs one job, without waiting. Returns `Ok(false)` if there
    /// was none it could start.
    pub fn work_one(&self) -> Result<bool> {
        Ok(matches!(self.work_one_within(Duration::ZERO)?, Step::Ran))
    }

    fn work_one_within(&self, wait: Duration) -> Result<Step> {
        self.upkeep(false)?;
        let (job, permit) = match self.claim_limited(wait)? {
            Claimed::Job(job, permit) => (job, permit),
            Claimed::Nothing { throttled: true } => return Ok(Step::Throttled),
            Claimed::Nothing { throttled: false } => return Ok(Step::Idle),
        };
        let result = self.handler(&job).and_then(|handler| {
            let fut = handler(job.args().to_vec());
            catch_unwind(AssertUnwindSafe(|| block_on(fut))).unwrap_or_else(|panic| {
                Err(JobError::Panicked {
                    message: panic_message(&*panic),
                })
            })
        });
        self.finish(job, result)?;
        drop(permit);
        Ok(Step::Ran)
    }

    /// Claims from the queues that have room, reserving a slot in each limited
    /// one first so a job is only taken when it can start. Keeps the slot of
    /// the job's queue and releases the others.
    fn claim_limited(&self, wait: Duration) -> Result<Claimed> {
        let mut reserved: Vec<(&str, Permit)> = Vec::new();
        let mut skipped = false;
        for queue in self.queues.claim_order() {
            match self.limits.try_acquire(queue) {
                Some(permit) => reserved.push((queue, permit)),
                None => skipped = true,
            }
        }
        if reserved.is_empty() {
            return Ok(Claimed::Nothing { throttled: true });
        }
        // A waiting claim can't see a slot free up on a skipped queue, so keep
        // waits short while any queue is full.
        let wait = if skipped {
            wait.min(LIMITED_RECHECK)
        } else {
            wait
        };
        let queues: Vec<&str> = reserved.iter().map(|(queue, _)| *queue).collect();
        let Some(job) = self.queue.claim(&self.id, &queues, wait)? else {
            return Ok(Claimed::Nothing { throttled: skipped });
        };
        let permit = reserved
            .into_iter()
            .find(|(queue, _)| *queue == job.queue())
            .map_or(Permit::Unlimited, |(_, permit)| permit);
        Ok(Claimed::Job(job, permit))
    }

    /// Refreshes the heartbeat and recovers stopped workers' jobs, each only
    /// when due unless `force`. Timestamps move only after a success, so a
    /// failed heartbeat is retried on the next call.
    fn upkeep(&self, force: bool) -> Result<()> {
        let now = Instant::now();
        let (beat, recover) = {
            let last = self.upkeep.lock().unwrap_or_else(PoisonError::into_inner);
            let due = |at: Option<Instant>, every: Duration| {
                force || at.is_none_or(|at| now.duration_since(at) >= every)
            };
            (
                due(last.beat, self.heartbeat_ttl / 3),
                due(last.recovered, self.recover_interval),
            )
        };
        if beat {
            self.queue.heartbeat(&self.id, self.heartbeat_ttl)?;
            self.upkeep
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .beat = Some(now);
        }
        if recover {
            let recovered = self.queue.recover()?;
            self.upkeep
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .recovered = Some(now);
            if recovered > 0 {
                tracing::warn!(
                    recovered,
                    "requeued jobs held by workers that stopped without finishing them"
                );
            }
        }
        Ok(())
    }

    fn log_upkeep(&self, force: bool) {
        if let Err(err) = self.upkeep(force) {
            tracing::error!(worker = %self.id, error = %Chain(&err), "heartbeat or recovery failed");
        }
    }

    fn log_retire(&self) {
        if let Err(err) = self.queue.retire(&self.id) {
            tracing::error!(worker = %self.id, error = %Chain(&err), "could not retire worker");
        }
    }

    fn handler(&self, job: &Job<Processing>) -> Result<Handler, JobError> {
        self.handlers
            .get(job.name())
            .copied()
            .ok_or_else(|| JobError::UnknownJob {
                name: job.name().to_owned(),
            })
    }

    /// Stores the outcome: `done` with the job's output, otherwise a retry or
    /// `dead`. The error's full cause chain becomes the job's `last_error`.
    fn finish(
        &self,
        job: Job<Processing>,
        result: Result<serde_json::Value, JobError>,
    ) -> Result<()> {
        let err = match result {
            Ok(output) => {
                self.queue.complete(&self.id, job, output)?;
                return Ok(());
            }
            Err(err) => err,
        };
        let error = Chain(&err).to_string();
        match self.queue.fail(&self.id, job, error, self.max_retries)? {
            Failed::Dead(job) => tracing::error!(
                job = job.name(),
                id = job.id(),
                error = %Chain(&err),
                "job failed and is dead"
            ),
            Failed::Retry(job) => tracing::warn!(
                job = job.name(),
                id = job.id(),
                error = %Chain(&err),
                "job failed and will retry"
            ),
        }
        Ok(())
    }
}

#[cfg(feature = "tokio")]
impl Worker {
    /// Runs the worker inside the current tokio runtime until `shutdown`
    /// completes. Each job is a tokio task, so job bodies can use tokio timers
    /// and I/O. At most `concurrency` jobs run at once. On shutdown, the worker
    /// stops claiming jobs, waits for the running ones to finish, and retires.
    pub async fn run_async(self, shutdown: impl Future<Output = ()>) {
        use tokio::{sync::Semaphore, task::JoinSet};

        let upkeep = |worker: Worker, force: bool| async move {
            let w = worker.clone();
            match tokio::task::spawn_blocking(move || w.upkeep(force)).await {
                Ok(Ok(())) => {}
                Ok(Err(err)) => worker.log_upkeep_error(&err),
                Err(err) => worker.log_upkeep_error(&err.into()),
            }
        };
        upkeep(self.clone(), true).await;
        // Separate from the claim loop, so a long job never lets the heartbeat lapse.
        let keeper = tokio::spawn({
            let worker = self.clone();
            async move {
                loop {
                    upkeep(worker.clone(), false).await;
                    tokio::time::sleep(KEEPER_TICK).await;
                }
            }
        });

        // One permit per job allowed to run at once. Job tasks hold theirs
        // until they finish, so getting every permit back means they all have.
        let permits = Arc::new(Semaphore::new(self.concurrency));
        let (stop, stopped) = tokio::sync::watch::channel(false);
        let mut claimers = JoinSet::new();
        for _ in 0..self.claimers.min(self.concurrency) {
            claimers.spawn(
                self.clone()
                    .claim_loop(Arc::clone(&permits), stopped.clone()),
            );
        }

        shutdown.await;
        let _ = stop.send(true);
        while claimers.join_next().await.is_some() {}
        let all = u32::try_from(self.concurrency).unwrap_or(u32::MAX);
        drop(permits.acquire_many(all).await);

        keeper.abort();
        let worker = self.clone();
        if let Err(err) = tokio::task::spawn_blocking(move || worker.log_retire()).await {
            tracing::error!(error = %err, "could not retire worker");
        }
    }

    /// Claims jobs and spawns each as its own task, until `stopped` turns true.
    /// Several of these run side by side; the global and per-queue limits are
    /// shared, so together they never exceed either.
    async fn claim_loop(
        self,
        permits: Arc<tokio::sync::Semaphore>,
        mut stopped: tokio::sync::watch::Receiver<bool>,
    ) {
        loop {
            // The semaphore is never closed, so acquiring only fails if that changes.
            let permit = tokio::select! {
                _ = stopped.wait_for(|stop| *stop) => break,
                permit = Arc::clone(&permits).acquire_owned() => match permit {
                    Ok(permit) => permit,
                    Err(_) => break,
                },
            };

            // Not raced against `stopped`: once a claim has started, it must
            // finish, or the job would sit in this worker's processing area
            // until recovery. It waits here for up to `poll_interval`.
            let started = Instant::now();
            let worker = self.clone();
            let claimed =
                tokio::task::spawn_blocking(move || worker.claim_limited(worker.poll_interval))
                    .await
                    .unwrap_or_else(|e| Err(e.into()));

            match claimed {
                Ok(Claimed::Job(job, queue_permit)) => {
                    let worker = self.clone();
                    tokio::spawn(async move {
                        worker.execute_async(job).await;
                        drop(queue_permit);
                        drop(permit);
                    });
                }
                nothing => {
                    drop(permit);
                    let pause = match nothing {
                        Ok(Claimed::Nothing { throttled: true }) => LIMITED_RECHECK,
                        Ok(_) => self.poll_interval,
                        Err(err) => {
                            tracing::error!(error = %Chain(&err), "queue error");
                            self.poll_interval
                        }
                    };
                    tokio::select! {
                        _ = stopped.wait_for(|stop| *stop) => break,
                        _ = tokio::time::sleep(pause.saturating_sub(started.elapsed())) => {}
                    }
                }
            }
        }
    }

    fn log_upkeep_error(&self, err: &crate::Error) {
        tracing::error!(worker = %self.id, error = %Chain(err), "heartbeat or recovery failed");
    }

    async fn execute_async(&self, job: Job<Processing>) {
        let result = match self.handler(&job) {
            // A separate task, so a panic in the job surfaces as a JoinError.
            Ok(handler) => match tokio::spawn(handler(job.args().to_vec())).await {
                Ok(result) => result,
                Err(e) if e.is_panic() => Err(JobError::Panicked {
                    message: panic_message(&*e.into_panic()),
                }),
                Err(e) => Err(JobError::Cancelled(e)),
            },
            Err(e) => Err(e),
        };

        let worker = self.clone();
        let stored =
            crate::executor::unblock(self.queue.blocks(), move || worker.finish(job, result)).await;
        if let Err(err) = stored {
            tracing::error!(error = %Chain(&err), "could not store job result");
        }
    }
}

/// Unique per worker, across threads and processes on one machine and, with
/// the timestamp, across restarts.
fn new_worker_id() -> String {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    format!("{}-{nanos:x}-{seq}", std::process::id())
}
