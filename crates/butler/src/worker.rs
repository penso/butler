use std::{
    collections::{HashMap, HashSet},
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc, Mutex, PoisonError, RwLock, TryLockError,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use tracing::{Instrument, Span, field};

use crate::{
    Backoff, Cleaned, Config, Cron, DeadJob, Error, Failed, GlobalLimit, Job, JobContext, JobDef,
    JobError, Layer, NewJob, Next, PreparedJob, Queue, QueuePriority, Recurring, RecurringConfig,
    Result, RetryPolicy, RunFuture, WorkerConfig, block_on,
    error::Chain,
    executor::panic_message,
    job::millis,
    limits::{Permit, QueueLimits},
    middleware::{DeadHook, Handler, run_dead_hooks, run_handler},
    monitor::JobMetric,
    progress::{Checkpoints, Interruption, Invocation},
    recurring::REGISTER_INTERVAL,
    state::Processing,
};

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
    /// Boxed: a job record is far larger than the other variant.
    Job(Box<Job<Processing>>, Permit),
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

/// How often the keeper promotes scheduled jobs that came due. Idle claims
/// promote on their own, at the due time; this covers workers too busy to be
/// idle, so a due job on a queue they serve first doesn't wait for a lull.
const PROMOTE_INTERVAL: Duration = KEEPER_TICK;

/// How often the keeper reads which queues are paused, so pausing or
/// resuming one takes effect within about this long.
const PAUSED_INTERVAL: Duration = Duration::from_secs(1);

/// How often the keeper deletes finished jobs past their retention, and how
/// many at most per call. A full batch means more are waiting: the next one
/// runs at the next keeper tick, so a backlog drains without holding the
/// backend (or delaying the heartbeat) for long at a time.
const CLEAN_INTERVAL: Duration = Duration::from_secs(60);
const CLEAN_BATCH: usize = 1_000;

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
    /// Every job this worker can run, by name.
    jobs: Arc<HashMap<&'static str, JobDef>>,
    id: Arc<str>,
    queues: QueuePriority,
    limits: QueueLimits,
    /// Per queue, the most of its jobs running at once across every worker
    /// with the same limit, enforced by the backend.
    global_limits: Arc<HashMap<String, usize>>,
    claimers: usize,
    concurrency: usize,
    max_retries: u32,
    backoff: Backoff,
    /// Worker shutdowns a job with a `Progress` may be interrupted by without
    /// counting an attempt, unless it sets its own; `None` for no limit.
    max_resumptions: Option<u32>,
    /// On shutdown, the longest to wait for running jobs before leaving them
    /// to heartbeat recovery.
    shutdown_timeout: Duration,
    poll_interval: Duration,
    heartbeat_ttl: Duration,
    recover_interval: Duration,
    upkeep: Arc<Mutex<Upkeep>>,
    /// The queues paused in the backend, as the keeper last read them: left
    /// out of every claim.
    paused: Arc<RwLock<HashSet<String>>>,
    /// Set when the worker starts shutting down: jobs with a `Progress` stop
    /// at their next checkpoint and go back on their queue.
    stopping: Arc<AtomicBool>,
    checkpoint_interval: Duration,
    /// Run around every job, outermost first.
    layers: Arc<Vec<Arc<dyn Layer>>>,
    /// Run when a job dies, in order.
    dead_hooks: Arc<Vec<DeadHook>>,
    /// Jobs this worker enqueues on a cron schedule.
    recurring: Arc<Vec<Recurring>>,
    recurring_clock: Arc<Mutex<RecurringClock>>,
}

/// When this worker last registered its recurring schedules, and when each
/// one is next due (`None` until the first registration).
#[derive(Default)]
struct RecurringClock {
    registered: Option<Instant>,
    next: Vec<Option<SystemTime>>,
}

/// When this worker last refreshed its heartbeat, recovered jobs, and
/// promoted scheduled jobs.
#[derive(Default)]
struct Upkeep {
    beat: Option<Instant>,
    recovered: Option<Instant>,
    promoted: Option<Instant>,
    paused: Option<Instant>,
    cleaned: Option<Instant>,
    /// The last cleanup deleted a full batch.
    clean_backlog: bool,
}

impl Worker {
    /// Opens the configured backend and applies the `[worker]` settings.
    /// Opens the configured backend and applies the `[worker]` settings and
    /// the `[[recurring]]` schedules. A schedule's job must be known by then:
    /// jobs that need [`register`](Worker::register) should use
    /// [`with_recurring_config`](Worker::with_recurring_config) after it.
    pub fn from_config(config: &Config) -> Result<Self> {
        Self::new(config.connect()?)
            .with_config(&config.worker)
            .with_recurring_config(&config.recurring)
    }

    /// # Panics
    ///
    /// If two `#[job]` functions in this binary share a name or an alias
    /// (`#[job(aliases = [...])]`). That is a build-time mistake, and a worker
    /// running either one would be wrong.
    pub fn new(queue: impl Into<Queue>) -> Self {
        let defaults = WorkerConfig::default();
        let mut jobs = HashMap::new();
        for def in inventory::iter::<JobDef> {
            if let Err(err) = add_job(&mut jobs, *def, false) {
                panic!("butler: {err}");
            }
        }
        Self {
            queue: queue.into(),
            jobs: Arc::new(jobs),
            id: new_worker_id().into(),
            queues: defaults.priority(),
            limits: QueueLimits::default(),
            global_limits: Arc::default(),
            claimers: defaults.claimers,
            concurrency: defaults.concurrency,
            max_retries: defaults.max_retries,
            backoff: defaults.backoff,
            max_resumptions: defaults.max_resumptions,
            shutdown_timeout: defaults.shutdown_timeout(),
            poll_interval: defaults.poll_interval(),
            heartbeat_ttl: defaults.heartbeat_ttl(),
            recover_interval: defaults.recover_interval(),
            upkeep: Arc::default(),
            paused: Arc::default(),
            stopping: Arc::default(),
            checkpoint_interval: defaults.checkpoint_interval(),
            layers: Arc::default(),
            dead_hooks: Arc::default(),
            recurring: Arc::default(),
            recurring_clock: Arc::default(),
        }
    }

    pub fn with_config(self, config: &WorkerConfig) -> Self {
        let worker = config
            .queue_limits
            .iter()
            .fold(self, |worker, (queue, max)| worker.queue_limit(queue, *max));
        let worker = config
            .global_queue_limits
            .iter()
            .fold(worker, |worker, (queue, max)| {
                worker.global_queue_limit(queue, *max)
            });
        worker
            .queues(config.priority())
            .concurrency(config.concurrency)
            .claimers(config.claimers)
            .max_retries(config.max_retries)
            .backoff(config.backoff)
            .max_resumptions(config.max_resumptions)
            .shutdown_timeout(config.shutdown_timeout())
            .poll_interval(config.poll_interval())
            .heartbeat_ttl(config.heartbeat_ttl())
            .checkpoint_interval(config.checkpoint_interval())
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
    /// Registering a job this worker already has replaces it, aliases
    /// included.
    ///
    /// # Panics
    ///
    /// If `job`'s names are invalid ([`JobDef::check_names`]), or one of its
    /// name and aliases is another job's name or alias.
    pub fn register(mut self, job: JobDef) -> Self {
        if let Err(err) = add_job(Arc::make_mut(&mut self.jobs), job, true) {
            panic!("butler: {err}");
        }
        self
    }

    /// Adds a [`Layer`] around every job this worker runs, like ActiveJob's
    /// `around_perform`. The first layer added is the outermost: with
    /// `.wrap(a).wrap(b)`, `a` starts first and finishes last.
    ///
    /// ```ignore
    /// worker.wrap(|job: JobContext, next: Next| async move {
    ///     let started = Instant::now();
    ///     let result = next.run().await;
    ///     record_duration(job.name(), started.elapsed());
    ///     result
    /// })
    /// ```
    pub fn wrap(mut self, layer: impl Layer) -> Self {
        Arc::make_mut(&mut self.layers).push(Arc::new(layer));
        self
    }

    /// Runs `hook` whenever a job dies, for alerting: when it used all its
    /// retries, and when its error said never to retry
    /// ([`Retry::Never`](crate::Retry::Never)). It runs after the job is
    /// stored as dead, in the job's tracing span; several hooks run in the
    /// order they were added. A panic in a hook is logged, and doesn't stop
    /// the worker.
    ///
    /// Under [`run_async`](Worker::run_async) it is a task on the runtime;
    /// under [`run`](Worker::run) and [`drain`](Worker::drain), it runs on
    /// the job's thread with the built-in `block_on`.
    pub fn on_dead<F, Fut>(mut self, hook: F) -> Self
    where
        F: Fn(DeadJob) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let hook: DeadHook = Arc::new(move |dead| Box::pin(hook(dead)));
        Arc::make_mut(&mut self.dead_hooks).push(hook);
        self
    }

    /// Enqueues `job`, with its arguments and queue, at every tick of `cron`,
    /// a five-field crontab expression in UTC. Every worker with the same
    /// schedule enqueues each tick exactly once between them.
    ///
    /// ```ignore
    /// worker.recurring(nightly_report::prepare("summary")?, "0 3 * * *")?
    /// ```
    ///
    /// For a time zone or a key of its own, build a [`Recurring`] and use
    /// [`add_recurring`](Worker::add_recurring).
    pub fn recurring<T>(self, job: PreparedJob<T>, cron: &str) -> Result<Self> {
        self.add_recurring(Recurring::new(job, cron)?)
    }

    /// Adds a recurring schedule. Fails if this worker already has one with
    /// the same key.
    pub fn add_recurring(mut self, schedule: Recurring) -> Result<Self> {
        if self.recurring.iter().any(|s| s.key() == schedule.key()) {
            return Err(Error::DuplicateRecurring {
                key: schedule.key().to_owned(),
            });
        }
        Arc::make_mut(&mut self.recurring).push(schedule);
        Ok(self)
    }

    /// Adds the `[[recurring]]` schedules from `butler.toml`. Each one's job
    /// must be registered with this worker, and runs on the job's own queue
    /// unless the entry names another.
    pub fn with_recurring_config(self, entries: &[RecurringConfig]) -> Result<Self> {
        entries.iter().try_fold(self, |worker, entry| {
            let Some(def) = worker.jobs.get(entry.job.as_str()) else {
                return Err(Error::UnknownRecurringJob {
                    key: entry.key.clone().unwrap_or_default(),
                    name: entry.job.clone(),
                });
            };
            let queue = entry.queue.as_deref().unwrap_or(def.queue).to_owned();
            let mut cron = Cron::parse(&entry.cron)?;
            if let Some(zone) = &entry.timezone {
                cron = cron.in_time_zone(zone)?;
            }
            let mut schedule =
                Recurring::from_parts(def.name.to_owned(), queue, entry.args.clone(), cron)?;
            if let Some(key) = &entry.key {
                schedule = schedule.with_key(key)?;
            }
            worker.add_recurring(schedule)
        })
    }

    /// This worker's recurring schedules, in the order they were added.
    pub fn recurring_schedules(&self) -> &[Recurring] {
        &self.recurring
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

    /// Caps how many jobs from `queue` run at once in this worker process, on
    /// top of the overall [`concurrency`](Worker::concurrency). When `queue`
    /// is at its limit, the worker skips it and keeps taking jobs from its
    /// other queues. Each process counts its own jobs, so limits add up
    /// across workers; see [`global_queue_limit`](Worker::global_queue_limit)
    /// for one limit shared by all of them.
    pub fn queue_limit(mut self, queue: &str, max: usize) -> Self {
        self.limits.set(queue, max);
        self
    }

    /// Caps how many jobs from `queue` run at once **across every worker**
    /// that has this limit, rather than within this one process as
    /// [`queue_limit`](Worker::queue_limit) does. The backend keeps the
    /// count, checked and taken atomically with each claim, and frees a slot
    /// when its job completes, fails, is interrupted, or is recovered from a
    /// crashed worker. Set the same limit on every worker serving `queue`:
    /// claims without it neither take a slot nor respect the limit.
    pub fn global_queue_limit(mut self, queue: &str, max: usize) -> Self {
        Arc::make_mut(&mut self.global_limits).insert(queue.to_owned(), max.max(1));
        self
    }

    /// The global per-queue limits, sorted by queue name.
    pub fn global_queue_limits(&self) -> Vec<(&str, usize)> {
        let mut limits: Vec<_> = self
            .global_limits
            .iter()
            .map(|(queue, max)| (queue.as_str(), *max))
            .collect();
        limits.sort_unstable();
        limits
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

    /// How many times a failed job is retried, unless it sets its own with
    /// `#[job(retries = N)]`. It runs at most `n + 1` times.
    pub fn max_retries(mut self, n: u32) -> Self {
        self.max_retries = n;
        self
    }

    /// How long a failed job waits before each retry, unless it sets its own
    /// with `#[job(backoff = "...")]`. Defaults to [`Backoff::Exponential`];
    /// [`Backoff::NONE`] retries at once.
    pub fn backoff(mut self, backoff: Backoff) -> Self {
        self.backoff = backoff;
        self
    }

    /// How many times a job with a [`Progress`](crate::Progress) may be
    /// interrupted at a checkpoint by a worker shutdown and put back on its
    /// queue without counting an attempt, unless it sets its own with
    /// `#[job(max_resumptions = N)]`. Past it, the next interruption counts
    /// as a failed attempt ([`JobError::ResumeLimit`]) and goes through the
    /// job's retry policy, so a job interrupted at every deploy ends up dead
    /// (and in the `on_dead` hooks) rather than cycling forever. `None`, the
    /// default, sets no limit.
    ///
    /// Only interruptions at a checkpoint count: not
    /// [`Progress::requeue`](crate::Progress::requeue), nor a job recovered
    /// after its worker crashed or [gave up on it](Worker::shutdown_timeout).
    pub fn max_resumptions(mut self, max: impl Into<Option<u32>>) -> Self {
        self.max_resumptions = max.into();
        self
    }

    /// The resumption limit for job `name`: its own, or this worker's.
    fn max_resumptions_of(&self, name: &str) -> Option<u32> {
        self.jobs
            .get(name)
            .and_then(|def| def.max_resumptions)
            .or(self.max_resumptions)
    }

    /// On shutdown, the longest [`run_until`](Worker::run_until) and
    /// [`run_async`](Worker::run_async) wait for running jobs to finish, from
    /// the moment they are asked to stop. Jobs with a `Progress` stop at
    /// their next checkpoint and go back on their queue; other jobs run to
    /// the end. Past the timeout, the worker stops its heartbeat and returns
    /// without retiring: the jobs still running are left in its processing
    /// area, and another worker puts them back on their queues once the
    /// heartbeat expires (after `heartbeat_ttl`), as after a crash. Exit the
    /// process soon after, or those jobs may run twice. Defaults to 25
    /// seconds, under the usual 30-second grace period before a `SIGKILL`;
    /// `Duration::MAX` waits for as long as the jobs take.
    pub fn shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.shutdown_timeout = timeout;
        self
    }

    /// When a shutdown that starts now stops waiting for jobs, or `None` to
    /// wait for as long as they take.
    fn shutdown_deadline(&self) -> Option<Instant> {
        Instant::now().checked_add(self.shutdown_timeout)
    }

    fn log_gave_up(&self, running: usize) {
        tracing::warn!(
            worker = %self.id,
            running,
            timeout_ms = u64::try_from(self.shutdown_timeout.as_millis()).unwrap_or(u64::MAX),
            "shutdown timeout reached with jobs still running; they go back on their queues once this worker's heartbeat expires"
        );
    }

    /// Puts back a job claimed as the worker started stopping, without
    /// running it. A claim that was waiting when the shutdown began can
    /// still return a job, often one just interrupted by this very
    /// shutdown: running it would only interrupt it again at once, and each
    /// time would count towards its `max_resumptions`.
    fn release_unstarted(&self, job: Job<Processing>) -> Result<()> {
        let id = job.id().to_owned();
        self.queue.requeue(&self.id, job)?;
        tracing::debug!(job = %id, "stopping: put back a job claimed but not started");
        Ok(())
    }

    /// The retry policy for job `name`: its own settings, or this worker's.
    fn retry_policy(&self, name: &str) -> RetryPolicy {
        let def = self.jobs.get(name);
        RetryPolicy::new(
            def.and_then(|def| def.retries).unwrap_or(self.max_retries),
            def.and_then(|def| def.backoff).unwrap_or(self.backoff),
        )
    }

    /// The longest an idle claim waits before checking the queues again. Redis
    /// (pub/sub) and memory wake claims the moment a job arrives, so there it
    /// is only a fallback; the file backend can't be woken, so it polls at
    /// this interval.
    /// For jobs with a [`Progress`](crate::Progress): the most often their
    /// progress is saved to the backend, so a crash resumes them from at most
    /// this long ago. Checkpoints in between only update it in memory.
    pub fn checkpoint_interval(mut self, d: Duration) -> Self {
        self.checkpoint_interval = d;
        self
    }

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

    /// The jobs this worker runs, by name (not their aliases), sorted.
    pub fn job_names(&self) -> Vec<&'static str> {
        let mut names: Vec<_> = self
            .jobs
            .iter()
            .filter(|(name, def)| **name == def.name)
            .map(|(name, _)| *name)
            .collect();
        names.sort_unstable();
        names
    }

    /// Runs forever on plain threads, using the built-in `block_on`. Jobs that
    /// use tokio APIs need `run_async` instead.
    pub fn run(self) {
        self.run_until(Arc::new(AtomicBool::new(false)));
    }

    /// Runs until `stop` is set, then waits for the threads to finish their
    /// current jobs, for up to the [`shutdown_timeout`](Worker::shutdown_timeout).
    pub fn run_until(mut self, stop: Arc<AtomicBool>) {
        // Checkpoints see the same flag: stopping interrupts continuable jobs.
        self.stopping = Arc::clone(&stop);
        self.log_upkeep(true);
        // Stopped only once no job runs, or the worker gives up on them: a
        // job still finishing after `stop` must keep its heartbeat, or
        // another worker would recover it while it runs.
        let keeper_stop = Arc::new(AtomicBool::new(false));
        let keeper = {
            let worker = self.clone();
            let keeper_stop = Arc::clone(&keeper_stop);
            thread::spawn(move || {
                while !keeper_stop.load(Ordering::Relaxed) {
                    worker.log_upkeep(false);
                    worker.clean_finished();
                    thread::sleep(KEEPER_TICK);
                }
            })
        };
        // Each job thread sends one message as it ends, even by a panic.
        let (ended, ends) = mpsc::channel::<()>();

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
                let ended = Ended(ended.clone());
                thread::spawn(move || {
                    let _ended = ended;
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
        drop(ended);

        let mut running = threads.len();
        // Before `stop`, only a thread that panicked outside a job ends.
        while running > 0 && !stop.load(Ordering::Relaxed) {
            if ends.recv_timeout(KEEPER_TICK).is_ok() {
                running -= 1;
            }
        }
        let deadline = self.shutdown_deadline();
        while running > 0 {
            let ended = match deadline {
                Some(deadline) => {
                    ends.recv_timeout(deadline.saturating_duration_since(Instant::now()))
                }
                None => ends.recv().map_err(mpsc::RecvTimeoutError::from),
            };
            match ended {
                Ok(()) => running -= 1,
                Err(mpsc::RecvTimeoutError::Timeout) => break,
                // Every thread has ended.
                Err(mpsc::RecvTimeoutError::Disconnected) => running = 0,
            }
        }

        keeper_stop.store(true, Ordering::Relaxed);
        if keeper.join().is_err() {
            tracing::error!("the worker's keeper thread panicked");
        }
        if running > 0 {
            // The job threads are left running, detached: joining them could
            // wait forever. Not retired, so the jobs they hold are recovered
            // once the heartbeat, now stopped, expires.
            self.log_gave_up(running);
            return;
        }
        for thread in threads {
            if thread.join().is_err() {
                tracing::error!("a worker thread panicked outside of a job");
            }
        }
        self.log_retire();
    }

    /// Runs jobs on the current thread until the queue is empty, and returns
    /// how many ran. This includes retries, so a job that always fails
    /// runs `max_retries + 1` times. Similar to `Sidekiq::Worker.drain_all`.
    /// Recovers jobs from stopped workers first. Scheduled jobs run if they
    /// are due; ones due later stay scheduled.
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
            Claimed::Job(job, permit) => (*job, permit),
            Claimed::Nothing { throttled: true } => return Ok(Step::Throttled),
            Claimed::Nothing { throttled: false } => return Ok(Step::Idle),
        };
        if self.stopping.load(Ordering::Acquire) {
            self.release_unstarted(job)?;
            drop(permit);
            return Ok(Step::Idle);
        }
        let span = self.span(&job);
        let checkpoints = self.checkpoints(&job);
        let started = Instant::now();
        let fut = self.perform(&job, &checkpoints).instrument(span.clone());
        let result = catch_unwind(AssertUnwindSafe(|| block_on(fut))).unwrap_or_else(|panic| {
            Err(JobError::Panicked {
                message: panic_message(&*panic),
            })
        });
        let dead = self.finish(job, result, &checkpoints, started, &span)?;
        drop(permit);
        if let Some(dead) = dead.filter(|_| !self.dead_hooks.is_empty()) {
            let hooks = run_dead_hooks(Arc::clone(&self.dead_hooks), dead).instrument(span.clone());
            if catch_unwind(AssertUnwindSafe(|| block_on(hooks))).is_err() {
                span.in_scope(|| tracing::error!("an on_dead hook panicked"));
            }
        }
        Ok(Step::Ran)
    }

    /// Claims from the queues that have room, reserving a slot in each limited
    /// one first so a job is only taken when it can start. Keeps the slot of
    /// the job's queue and releases the others.
    fn claim_limited(&self, wait: Duration) -> Result<Claimed> {
        let mut reserved: Vec<(&str, Permit)> = Vec::new();
        let mut skipped = false;
        let order: Vec<&str> = {
            let paused = self.paused.read().unwrap_or_else(PoisonError::into_inner);
            self.queues
                .claim_order()
                .into_iter()
                .filter(|queue| !paused.contains(*queue))
                .collect()
        };
        for queue in order {
            match self.limits.try_acquire(queue) {
                Some(permit) => reserved.push((queue, permit)),
                None => skipped = true,
            }
        }
        if reserved.is_empty() {
            // Only full queues are worth rechecking soon; paused ones wait
            // for the keeper.
            return Ok(Claimed::Nothing { throttled: skipped });
        }
        // A waiting claim can't see a slot free up on a skipped queue, so keep
        // waits short while any queue is full.
        let wait = if skipped {
            wait.min(LIMITED_RECHECK)
        } else {
            wait
        };
        let queues: Vec<&str> = reserved.iter().map(|(queue, _)| *queue).collect();
        let global: Vec<GlobalLimit<'_>> = queues
            .iter()
            .filter_map(|queue| {
                let max = *self.global_limits.get(*queue)?;
                Some(GlobalLimit { queue, max })
            })
            .collect();
        let Some(job) = self
            .queue
            .claim_within_limits(&self.id, &queues, &global, wait)?
        else {
            return Ok(Claimed::Nothing { throttled: skipped });
        };
        let permit = reserved
            .into_iter()
            .find(|(queue, _)| *queue == job.queue())
            .map_or(Permit::Unlimited, |(_, permit)| permit);
        Ok(Claimed::Job(Box::new(job), permit))
    }

    /// Refreshes the heartbeat, recovers stopped workers' jobs, and promotes
    /// scheduled jobs that came due, each only when due unless `force`.
    /// Timestamps move only after a success, so a failed heartbeat is retried
    /// on the next call.
    fn upkeep(&self, force: bool) -> Result<()> {
        let now = Instant::now();
        let (beat, recover, promote, paused) = {
            let last = self.upkeep.lock().unwrap_or_else(PoisonError::into_inner);
            let due = |at: Option<Instant>, every: Duration| {
                force || at.is_none_or(|at| now.duration_since(at) >= every)
            };
            (
                due(last.beat, self.heartbeat_ttl / 3),
                due(last.recovered, self.recover_interval),
                due(last.promoted, PROMOTE_INTERVAL),
                due(last.paused, PAUSED_INTERVAL),
            )
        };
        if paused {
            self.refresh_paused()?;
            self.upkeep
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .paused = Some(now);
        }
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
        if promote {
            let promoted = self.queue.promote(SystemTime::now())?;
            self.upkeep
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .promoted = Some(now);
            if promoted.moved > 0 {
                tracing::debug!(
                    moved = promoted.moved,
                    "moved due scheduled jobs onto their queues"
                );
            }
        }
        self.run_recurring(force)
    }

    /// Registers this worker's recurring schedules with the backend (at
    /// start, then every [`REGISTER_INTERVAL`], so the dashboard sees they
    /// are still run), and enqueues each tick once it is due. Only one thread
    /// of this worker does it at a time; the backend makes sure no other
    /// worker enqueues the same tick again.
    fn run_recurring(&self, force: bool) -> Result<()> {
        if self.recurring.is_empty() {
            return Ok(());
        }
        let mut clock = match self.recurring_clock.try_lock() {
            Ok(clock) => clock,
            Err(TryLockError::Poisoned(poisoned)) => poisoned.into_inner(),
            Err(TryLockError::WouldBlock) => return Ok(()),
        };
        let now = SystemTime::now();
        let register = force
            || clock
                .registered
                .is_none_or(|at| at.elapsed() >= REGISTER_INTERVAL);
        if register {
            // Retried at the next interval, not every keeper tick.
            clock.registered = Some(Instant::now());
            let records: Vec<_> = self.recurring.iter().map(|s| s.record(now)).collect();
            let stored = self.queue.register_recurring(&records)?;
            clock.next.resize(self.recurring.len(), None);
            for ((schedule, stored), next) in
                self.recurring.iter().zip(&stored).zip(&mut clock.next)
            {
                if next.is_none() {
                    *next = schedule.first_due(stored, now);
                }
            }
        }
        for (schedule, next) in self.recurring.iter().zip(&mut clock.next) {
            if next.is_none_or(|due| due > now) {
                continue;
            }
            // The latest tick: after a long pause, the ones in between are
            // skipped rather than run as a backlog.
            let tick = schedule.cron().latest_until(now).or(*next).unwrap_or(now);
            let mut job = NewJob::new(schedule.name(), schedule.queue(), schedule.args().to_vec());
            let pushed = crate::enqueue::apply(&mut job)
                .and_then(|()| self.queue.push_recurring(schedule.key(), tick, job));
            match pushed {
                Ok(Some(id)) => tracing::info!(
                    schedule = schedule.key(),
                    job = %id,
                    tick_ms = millis(tick),
                    "enqueued a recurring job"
                ),
                Ok(None) => tracing::debug!(
                    schedule = schedule.key(),
                    tick_ms = millis(tick),
                    "another worker already enqueued this recurring tick"
                ),
                Err(err @ Error::Vetoed { .. }) => tracing::warn!(
                    schedule = schedule.key(),
                    error = %Chain(&err),
                    "an enqueue layer vetoed a recurring job; skipping this tick"
                ),
                // Left due, so the next pass tries again.
                Err(err) => return Err(err),
            }
            *next = schedule.cron().next_after(now);
        }
        Ok(())
    }

    /// Deletes a batch of finished jobs past the backend's retention, when
    /// due: every [`CLEAN_INTERVAL`], or at the next tick while the backend
    /// reports more to look at (a full batch, or a scan budget spent). Only
    /// the keeper does it, never a thread about to run a job.
    fn clean_finished(&self) {
        let now = Instant::now();
        {
            let last = self.upkeep.lock().unwrap_or_else(PoisonError::into_inner);
            let every = if last.clean_backlog {
                Duration::ZERO
            } else {
                CLEAN_INTERVAL
            };
            if last
                .cleaned
                .is_some_and(|at| now.duration_since(at) < every)
            {
                return;
            }
        }
        let cleaned = self.queue.clean_finished(SystemTime::now(), CLEAN_BATCH);
        let mut last = self.upkeep.lock().unwrap_or_else(PoisonError::into_inner);
        last.cleaned = Some(now);
        last.clean_backlog = cleaned.as_ref().is_ok_and(|cleaned| cleaned.more);
        match cleaned {
            Ok(Cleaned { deleted: 0, .. }) => {}
            Ok(Cleaned { deleted, .. }) => {
                tracing::debug!(deleted, "deleted finished jobs past their retention")
            }
            Err(err) => tracing::warn!(
                worker = %self.id,
                error = %Chain(&err),
                "could not delete finished jobs past their retention"
            ),
        }
    }

    /// Reads the paused queues from the backend, and logs when one this
    /// worker serves is paused or resumed.
    fn refresh_paused(&self) -> Result<()> {
        let now: HashSet<String> = self.queue.paused_queues()?.into_iter().collect();
        let mut paused = self.paused.write().unwrap_or_else(PoisonError::into_inner);
        if *paused == now {
            return Ok(());
        }
        for queue in self.queues.names() {
            match (paused.contains(queue), now.contains(queue)) {
                (false, true) => tracing::info!(queue, "queue paused: not claiming its jobs"),
                (true, false) => tracing::info!(queue, "queue resumed"),
                _ => {}
            }
        }
        *paused = now;
        Ok(())
    }

    /// Records one attempt for the dashboard's history. Best effort: a
    /// metrics hiccup must never fail the job.
    fn record(&self, metric: &JobMetric) {
        if let Err(err) = self.queue.record_metric(metric) {
            tracing::warn!(error = %Chain(&err), "could not record job metric");
        }
    }

    fn log_upkeep(&self, force: bool) {
        if let Err(err) = self.upkeep(force) {
            tracing::error!(worker = %self.id, error = %Chain(&err), "heartbeat, recovery, promotion or recurring jobs failed");
        }
    }

    fn log_retire(&self) {
        if let Err(err) = self.queue.retire(&self.id) {
            tracing::error!(worker = %self.id, error = %Chain(&err), "could not retire worker");
        }
    }

    /// The span every run of `job` is in, with its id, name, queue and
    /// attempt. `outcome` (`done`, `retry`, `dead`, `interrupted` or
    /// `requeued`) and, for a retry, `retry_at_ms` (ms since the Unix epoch)
    /// are recorded when it finishes.
    fn span(&self, job: &Job<Processing>) -> Span {
        tracing::info_span!(
            "job",
            id = job.id(),
            name = job.name(),
            queue = job.queue(),
            attempt = job.attempts().saturating_add(1),
            worker = &*self.id,
            outcome = field::Empty,
            retry_at_ms = field::Empty,
        )
    }

    /// The job's run: its layers, outermost first, around its generated
    /// code, or `UnknownJob` if this worker doesn't have it.
    fn perform(&self, job: &Job<Processing>, checkpoints: &Checkpoints) -> RunFuture {
        let handler: Option<Handler> = self.jobs.get(job.name()).map(|def| def.perform);
        let invocation = Invocation {
            args: job.args().to_vec(),
            checkpoints: checkpoints.clone(),
        };
        if self.layers.is_empty() {
            return run_handler(handler, job.name(), invocation);
        }
        let context = JobContext::new(
            job.record().clone(),
            self.retry_policy(job.name()),
            Arc::clone(&self.id),
        );
        Next::start(Arc::clone(&self.layers), context, handler, invocation)
    }

    /// What a job run gets for its `Progress`: the progress to resume from,
    /// the stopping flag, and a way to save progress to the backend.
    fn checkpoints(&self, job: &Job<Processing>) -> Checkpoints {
        let stopping = Arc::clone(&self.stopping);
        let (queue, worker, record) = (
            self.queue.clone(),
            Arc::clone(&self.id),
            job.record().clone(),
        );
        Checkpoints::new(
            record.progress.clone(),
            self.checkpoint_interval,
            move || stopping.load(Ordering::Acquire),
            Box::new(move |progress| {
                let mut record = record.clone();
                record.progress = Some(progress);
                let (queue, worker) = (queue.clone(), Arc::clone(&worker));
                Box::pin(async move {
                    crate::executor::unblock(queue.blocks(), move || {
                        queue.checkpoint(&worker, &record)
                    })
                    .await
                })
            }),
        )
    }

    /// Stores the outcome: `done` with the job's output, back on its queue if
    /// a checkpoint interrupted it, otherwise a retry (after the job's
    /// backoff, or when its error asks) or `dead`. A failed or interrupted job
    /// keeps its latest progress, so the next run resumes from there. The
    /// error's full cause chain becomes the job's `last_error`. Logs in the
    /// job's `span`, and records its outcome there. Returns the job if it
    /// died, for the `on_dead` hooks.
    fn finish(
        &self,
        job: Job<Processing>,
        result: Result<serde_json::Value, JobError>,
        checkpoints: &Checkpoints,
        started: Instant,
        span: &Span,
    ) -> Result<Option<DeadJob>> {
        let _entered = span.enter();
        let (job_name, job_queue) = (job.name().to_owned(), job.queue().to_owned());
        let duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
        let metric = |failed| JobMetric::now(&job_name, &job_queue, failed, duration_ms);
        let err = match result {
            Ok(output) => {
                self.queue.complete(&self.id, job, output)?;
                span.record("outcome", "done");
                tracing::debug!(duration_ms, "job done");
                self.record(&metric(false));
                return Ok(None);
            }
            Err(err) => err,
        };
        let job = match checkpoints.latest() {
            Some(progress) => job.with_progress(progress),
            None => job,
        };
        let err = match checkpoints.interruption() {
            Some(Interruption::Requeued) => {
                self.queue.requeue(&self.id, job)?;
                span.record("outcome", "requeued");
                tracing::debug!("job requeued itself; its next step starts in a fresh execution");
                return Ok(None);
            }
            Some(Interruption::Stopping) => match self.max_resumptions_of(&job_name) {
                Some(max) if job.resumptions() >= max => {
                    // Too many already: this one is a failed attempt.
                    JobError::ResumeLimit { max }
                }
                _ => {
                    self.queue.interrupt(&self.id, job)?;
                    span.record("outcome", "interrupted");
                    tracing::info!("job interrupted at a checkpoint; it will resume from there");
                    return Ok(None);
                }
            },
            None => err,
        };
        let error = Chain(&err).to_string();
        self.record(&metric(true));
        let policy = self.retry_policy(&job_name);
        match self
            .queue
            .fail_with(&self.id, job, error, err.retry(), policy)?
        {
            Failed::Dead(job) => {
                span.record("outcome", "dead");
                tracing::error!(duration_ms, error = %Chain(&err), "job failed and is dead");
                return Ok(Some(DeadJob::new(job, err)));
            }
            Failed::Scheduled(job) => {
                span.record("outcome", "retry");
                span.record("retry_at_ms", millis(job.run_at()));
                tracing::warn!(
                    duration_ms,
                    retry_in_ms = job
                        .run_at()
                        .duration_since(SystemTime::now())
                        .unwrap_or_default()
                        .as_millis(),
                    error = %Chain(&err),
                    "job failed and will retry later"
                );
            }
            Failed::Retry(_) => {
                span.record("outcome", "retry");
                span.record("retry_at_ms", millis(SystemTime::now()));
                tracing::warn!(duration_ms, error = %Chain(&err), "job failed and will retry");
            }
        }
        Ok(None)
    }
}

#[cfg(feature = "tokio")]
impl Worker {
    /// Runs the worker inside the current tokio runtime until `shutdown`
    /// completes. Each job is a tokio task, so job bodies can use tokio timers
    /// and I/O. At most `concurrency` jobs run at once. On shutdown, the worker
    /// stops claiming jobs, waits for the running ones to finish, for up to
    /// the [`shutdown_timeout`](Worker::shutdown_timeout), and retires.
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
                    let w = worker.clone();
                    if let Err(err) = tokio::task::spawn_blocking(move || w.clean_finished()).await
                    {
                        tracing::error!(error = %err, "cleaning up finished jobs panicked");
                    }
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
        let deadline = self.shutdown_deadline();
        // Continuable jobs stop at their next checkpoint and requeue.
        self.stopping.store(true, Ordering::Release);
        let _ = stop.send(true);
        // Not bounded by the deadline: a claim that started must finish (see
        // `claim_loop`), and each waits at most `poll_interval`.
        while claimers.join_next().await.is_some() {}
        let all = u32::try_from(self.concurrency).unwrap_or(u32::MAX);
        let drained = permits.acquire_many(all);
        let drained = match deadline {
            Some(deadline) => tokio::time::timeout_at(deadline.into(), drained)
                .await
                .is_ok(),
            None => {
                drop(drained.await);
                true
            }
        };

        // The keeper ran until now, so jobs finishing after the shutdown
        // signal kept their heartbeat.
        keeper.abort();
        if !drained {
            // The job tasks keep running while the runtime does; their jobs
            // are recovered once the heartbeat, now stopped, expires.
            self.log_gave_up(self.concurrency - permits.available_permits());
            return;
        }
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
                        worker.execute_async(*job).await;
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

    fn log_upkeep_error(&self, err: &Error) {
        tracing::error!(worker = %self.id, error = %Chain(err), "heartbeat, recovery, promotion or recurring jobs failed");
    }

    async fn execute_async(&self, job: Job<Processing>) {
        if self.stopping.load(Ordering::Acquire) {
            let worker = self.clone();
            let released = crate::executor::unblock(self.queue.blocks(), move || {
                worker.release_unstarted(job)
            })
            .await;
            if let Err(err) = released {
                tracing::error!(error = %Chain(&err), "could not put back a job claimed while stopping");
            }
            return;
        }
        let span = self.span(&job);
        let checkpoints = self.checkpoints(&job);
        let started = Instant::now();
        // A separate task, so a panic in the job or a layer surfaces as a
        // JoinError.
        let run = self.perform(&job, &checkpoints).instrument(span.clone());
        let result = match tokio::spawn(run).await {
            Ok(result) => result,
            Err(e) if e.is_panic() => Err(JobError::Panicked {
                message: panic_message(&*e.into_panic()),
            }),
            Err(e) => Err(JobError::Cancelled(e)),
        };

        let worker = self.clone();
        let finishing = span.clone();
        let stored = crate::executor::unblock(self.queue.blocks(), move || {
            worker.finish(job, result, &checkpoints, started, &finishing)
        })
        .await;
        let dead = match stored {
            Ok(dead) => dead,
            Err(err) => {
                span.in_scope(
                    || tracing::error!(error = %Chain(&err), "could not store job result"),
                );
                None
            }
        };
        if let Some(dead) = dead.filter(|_| !self.dead_hooks.is_empty()) {
            let hooks = run_dead_hooks(Arc::clone(&self.dead_hooks), dead).instrument(span.clone());
            if let Err(err) = tokio::spawn(hooks).await {
                span.in_scope(|| tracing::error!(error = %err, "an on_dead hook panicked"));
            }
        }
    }
}

/// Tells [`Worker::run_until`] that a job thread ended, when dropped: at the
/// end of its loop, or while unwinding from a panic outside a job.
struct Ended(mpsc::Sender<()>);

impl Drop for Ended {
    fn drop(&mut self) {
        // The receiver is gone only if `run_until` gave up on this thread.
        let _ = self.0.send(());
    }
}

/// Adds `def` to `jobs` under its name and each alias, which all look up the
/// same `JobDef`. A name or alias already taken by another job is an error,
/// and so is the same job twice unless `replace` (explicit registration, which
/// may repeat what inventory found). Nothing changes on error.
fn add_job(jobs: &mut HashMap<&'static str, JobDef>, def: JobDef, replace: bool) -> Result<()> {
    def.check_names()?;
    for name in def.names() {
        if let Some(existing) = jobs.get(name)
            && (existing.name != def.name || !replace)
        {
            return Err(Error::JobNameTaken {
                name: name.to_owned(),
                job: def.name.to_owned(),
                other: existing.name.to_owned(),
            });
        }
    }
    jobs.retain(|_, existing| existing.name != def.name);
    jobs.extend(def.names().map(|name| (name, def)));
    Ok(())
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
