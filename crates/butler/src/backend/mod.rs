//! Queue backends. The worker and the `#[job]` enqueue path only use the
//! [`Backend`] traits, so they work the same with every backend:
//!
//! - [`Store`]: storage and the job lifecycle (push, claim, finish, recover);
//! - [`Monitor`]: what a dashboard reads, and the actions it takes;
//! - [`Watch`]: wake-ups for callers waiting on a job's result.

mod file;
mod memory;
#[cfg(feature = "redis")]
mod redis;
#[cfg(feature = "sqlite")]
mod sqlite;

use std::{
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};

use serde_json::{Map, Value};

#[cfg(feature = "redis")]
pub use self::redis::RedisQueue;
#[cfg(feature = "sqlite")]
pub use self::sqlite::{SqliteQueue, WATCH_TICK as SQLITE_WATCH_TICK};
pub use self::{file::FileQueue, memory::MemoryQueue};
use crate::{
    AnyJob, Error, Failed, Job, JobId, JobRecord, JobState, Result, Retry, RetryPolicy, Signal,
    job::{after, millis},
    monitor::{JobMetric, ListFilter, MetricBucket, Stats},
    recurring::RecurringRecord,
    state::{Done, Pending, Processing},
};

/// A complete backend: storage and the job lifecycle ([`Store`]), what a
/// dashboard reads and does ([`Monitor`]), and wake-ups for result waiters
/// ([`Watch`]). Implemented for every type that implements all three, so
/// [`Queue`] can hold any configured backend as one `dyn Backend`.
pub trait Backend: Store + Monitor + Watch {}

impl<T: Store + Monitor + Watch> Backend for T {}

/// Storage for jobs and their lifecycle: what the enqueue path and the worker
/// need. Methods block; async callers run them through `spawn_blocking`.
///
/// Delivery is at least once. Each worker process has an id; `claim` moves a
/// job into that worker's own processing area, and the worker keeps a
/// `heartbeat` alive while it runs. If the heartbeat lapses (the process
/// crashed or hung), `recover` puts that worker's jobs back in the queue, so a
/// job interrupted by a crash runs again. Job bodies should be safe to repeat.
pub trait Store: Send + Sync + 'static {
    /// Stores a new job and returns its id. Without a `run_at`, it is
    /// pending on its queue. With one, no worker can claim it before then:
    /// it stays `Scheduled` until [`promote`](Store::promote) moves it onto
    /// its queue. [`NewJob::into_record`] builds the record to store, with
    /// its metadata.
    fn push(&self, job: NewJob) -> Result<JobId>;

    /// Stores many new jobs, in order, and returns their ids in the same
    /// order, as [`push`](Store::push) would. Backends that can should do it
    /// in one step (one round trip, one transaction); the default stores
    /// them one by one.
    fn push_many(&self, jobs: Vec<NewJob>) -> Result<Vec<JobId>> {
        jobs.into_iter().map(|job| self.push(job)).collect()
    }

    /// Moves every scheduled job due by `now` onto its own queue, as pending,
    /// and reports how many moved and when the next remaining one is due.
    /// Each job moves exactly once, and never after a
    /// [`cancel`](Store::cancel) took it: several workers promote at once.
    fn promote(&self, now: SystemTime) -> Result<Promoted>;

    /// Atomically moves the oldest pending job of the first non-empty queue in
    /// `queues` into `worker`'s processing area, so no other worker can take
    /// it. May wait up to `wait` for a job to arrive on any of `queues`, and
    /// should return as soon as one does; backends that can't wait return
    /// `None` right away, and the worker polls them instead.
    fn claim(&self, worker: &str, queues: &[&str], wait: Duration) -> Result<Option<JobRecord>>;

    /// Marks a job `worker` claimed as done.
    fn complete(&self, worker: &str, job: &JobRecord) -> Result<()>;

    /// Stores a failed or interrupted attempt of a job `worker` claimed. `job`
    /// already carries the new attempt count and error; `next` is `Pending`
    /// (back on its queue), `Scheduled` (a retry that waits until the job's
    /// `run_at_ms`), or `Dead`.
    fn fail(&self, worker: &str, job: &JobRecord, next: JobState) -> Result<()>;

    fn get(&self, id: &str) -> Result<Option<(JobState, JobRecord)>>;

    /// Saves the record (in practice, its `progress`) of a job `worker`
    /// holds, so a crash resumes it from there. Does nothing if the worker no
    /// longer holds it. The default does nothing at all: progress then only
    /// survives interruptions and failures, which store the whole record.
    fn checkpoint(&self, _worker: &str, _job: &JobRecord) -> Result<()> {
        Ok(())
    }

    /// Atomically removes a pending or scheduled job so no worker will run
    /// it, and marks it `Cancelled`. Returns `false`, changing nothing, if the
    /// job is unknown or a worker already claimed it: a running job can't be
    /// interrupted.
    fn cancel(&self, id: &str) -> Result<bool>;

    /// Declares `worker` alive for `ttl`. Workers call it well within `ttl`.
    fn heartbeat(&self, worker: &str, ttl: Duration) -> Result<()>;

    /// Clean shutdown: `worker` is gone and holds no jobs.
    fn retire(&self, worker: &str) -> Result<()>;

    /// Moves every job held by a worker whose heartbeat expired back to the
    /// front of its own queue, and returns how many. Safe to run from several workers at once:
    /// each job moves exactly once.
    /// Interrupted retry transitions preserve their recorded schedule.
    fn recover(&self) -> Result<usize>;

    /// Stores the recurring schedules a worker runs, or refreshes them if
    /// the backend has them already: their definition and `seen_at_ms` come
    /// from `schedules`, while a stored schedule keeps its `created_at_ms`
    /// and last run. Returns the stored schedules, in the same order.
    /// Default: not supported.
    fn register_recurring(&self, _schedules: &[RecurringRecord]) -> Result<Vec<RecurringRecord>> {
        Err(Error::Unsupported("recurring jobs"))
    }

    /// Enqueues `job` (pending, on its queue) as the run of recurring
    /// schedule `key` for its tick at `tick`, unless that tick was already
    /// enqueued: however many workers call it, one job per `(key, tick)`.
    /// Checking and enqueueing are one atomic step. Records the job as the
    /// schedule's last run, unless a later tick already ran. Returns the new
    /// job's id, or `None` if the tick was taken. A tick is remembered for at
    /// least a day. Default: not supported.
    fn push_recurring(&self, _key: &str, _tick: SystemTime, _job: NewJob) -> Result<Option<JobId>> {
        Err(Error::Unsupported("recurring jobs"))
    }

    /// Whether calls can block on I/O (network, disk). Async callers send
    /// blocking backends' calls to tokio's blocking pool; calls to backends
    /// that never block run in place, which is much cheaper. `claim` with a
    /// `wait` always counts as blocking.
    fn blocks(&self) -> bool {
        true
    }

    /// Where the queue lives, for logs. Must not include secrets.
    fn describe(&self) -> String;
}

/// What a dashboard reads and does: counts, listings, per-minute history, and
/// the actions an operator takes on jobs. Every method has a default, so a
/// backend only implements what it can support.
pub trait Monitor: Send + Sync + 'static {
    /// Counts for a dashboard. Default: not supported.
    fn stats(&self) -> Result<Stats> {
        Err(Error::Unsupported("stats"))
    }

    /// Jobs in `filter.state`: pending and processing jobs oldest first,
    /// scheduled ones soonest first, finished ones (done, dead, cancelled)
    /// most recent first.
    fn list(&self, _filter: &ListFilter) -> Result<Vec<JobRecord>> {
        Err(Error::Unsupported("listing jobs"))
    }

    /// Puts a dead job back on its queue, with its attempts reset. Returns
    /// `false` if `id` isn't a dead job.
    fn retry(&self, _id: &str) -> Result<bool> {
        Err(Error::Unsupported("retrying jobs"))
    }

    /// Moves a scheduled job onto its queue now, ahead of its run time.
    /// Returns `false` if `id` isn't a scheduled job.
    fn run_now(&self, _id: &str) -> Result<bool> {
        Err(Error::Unsupported("running scheduled jobs now"))
    }

    /// Deletes a finished job (done, dead or cancelled). Returns `false` if
    /// `id` isn't a finished job.
    fn discard(&self, _id: &str) -> Result<bool> {
        Err(Error::Unsupported("discarding jobs"))
    }

    /// Every recurring schedule a worker registered, sorted by key.
    fn recurring(&self) -> Result<Vec<RecurringRecord>> {
        Err(Error::Unsupported("listing recurring jobs"))
    }

    /// Forgets a recurring schedule and its last run. A worker that still
    /// runs it registers it again, as new. Returns `false` if `key` is
    /// unknown. Implementations must reject invalid keys with
    /// [`Error::InvalidRecurringKey`] before touching storage (see
    /// [`recurring::is_valid_key`](crate::recurring::is_valid_key)).
    fn remove_recurring(&self, _key: &str) -> Result<bool> {
        Err(Error::Unsupported("removing recurring jobs"))
    }

    /// Records one finished attempt, for history. Default: ignored.
    fn record_metric(&self, _metric: &JobMetric) -> Result<()> {
        Ok(())
    }

    /// Per-minute history since `since_minute` (minutes since the epoch).
    /// Default: none.
    fn metrics(&self, _since_minute: u64) -> Result<Vec<MetricBucket>> {
        Ok(Vec::new())
    }
}

/// Wake-ups for callers waiting on a job's result.
pub trait Watch: Send + Sync + 'static {
    /// A signal notified when job `id` may have finished (done, dead or
    /// cancelled), so a waiting [`JobHandle`](crate::JobHandle) wakes at once.
    /// Per job, so finishing one job only wakes its own waiters. `None`, the
    /// default, makes waiters poll at the interval they were given.
    fn watch_finished(&self, _id: &str) -> Option<Arc<Signal>> {
        None
    }
}

/// A job to push, for [`Store::push`] and [`Store::push_many`]. It is also
/// what [enqueue layers](crate::EnqueueLayer) see and may change.
#[derive(Debug, Clone)]
pub struct NewJob {
    pub name: String,
    pub queue: String,
    pub args: Vec<Value>,
    /// Scheduled for this time, or pending at once if `None`.
    pub run_at: Option<SystemTime>,
    /// Stored with the job as its [`JobRecord::meta`]: what enqueue layers
    /// add, such as a tenant or a trace id, for worker layers to read.
    pub meta: Map<String, Value>,
}

impl NewJob {
    /// A job to run as soon as a worker is free.
    pub fn new(name: impl Into<String>, queue: impl Into<String>, args: Vec<Value>) -> Self {
        Self {
            name: name.into(),
            queue: queue.into(),
            args,
            run_at: None,
            meta: Map::new(),
        }
    }

    /// The same job, scheduled for `at`.
    pub fn run_at(mut self, at: SystemTime) -> Self {
        self.run_at = Some(at);
        self
    }

    /// The record a backend stores for it: a new id, its run time and
    /// metadata.
    pub fn into_record(self) -> JobRecord {
        let mut job = JobRecord::owned(self.name, self.queue, self.args);
        job.run_at_ms = self.run_at.map(millis);
        job.meta = self.meta;
        job
    }
}

/// What [`Store::promote`] did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Promoted {
    /// Jobs moved onto their queues.
    pub moved: usize,
    /// When the next job still scheduled is due, if any.
    pub next: Option<SystemTime>,
}

/// How long backends remember that a recurring tick was enqueued. A tick is
/// only ever tried around its own time, or once by a worker starting later,
/// which first checks the schedule's last run; a day covers both.
pub(crate) const TICK_RETENTION: Duration = Duration::from_secs(24 * 60 * 60);

/// The shortest a claim waits for a scheduled job to come due, so a run time
/// that slips into the past between two calls can't make it spin.
const MIN_SCHEDULE_WAIT: Duration = Duration::from_millis(1);

// Schedules can change from another process while a backend waits for pending
// work. Periodically return to promotion even when no due time was known.
const MAX_SCHEDULE_WAIT: Duration = Duration::from_millis(100);

/// Whether `at` has passed, to the millisecond backends store.
fn is_due(at: SystemTime) -> bool {
    millis(at) <= millis(SystemTime::now())
}

/// Records a failure on `job` and returns the state it should move to: dead
/// when its error said never to retry or it is out of retries, otherwise back
/// on its queue, at once or (with its `run_at_ms` set) after a delay.
pub(crate) fn record_failure(
    job: &mut JobRecord,
    error: String,
    retry: Retry,
    policy: RetryPolicy,
) -> JobState {
    job.attempts += 1;
    job.last_error = Some(error);
    let delay = match retry {
        Retry::Never => return JobState::Dead,
        _ if job.attempts > policy.max_retries => return JobState::Dead,
        Retry::After(delay) => delay,
        Retry::Default => policy.backoff.delay(job.attempts),
    };
    if delay.is_zero() {
        return JobState::Pending;
    }
    job.run_at_ms = Some(millis(after(delay)));
    JobState::Scheduled
}

/// A cheap-to-clone handle to a backend.
#[derive(Clone)]
pub struct Queue(Arc<dyn Backend>);

impl Queue {
    pub fn new(backend: impl Backend) -> Self {
        Queue(Arc::new(backend))
    }

    pub fn push(&self, name: &str, queue: &str, args: Vec<Value>) -> Result<JobId> {
        self.0.push(NewJob::new(name, queue, args))
    }

    /// Stores one job: pending, or scheduled if it has a `run_at` still in
    /// the future, with its metadata.
    pub fn push_job(&self, mut job: NewJob) -> Result<JobId> {
        job.run_at = job.run_at.filter(|at| !is_due(*at));
        self.0.push(job)
    }

    /// Stores jobs in one step where the backend allows it. A `run_at`
    /// already past enqueues that job at once.
    pub fn push_many(&self, mut jobs: Vec<NewJob>) -> Result<Vec<JobId>> {
        for job in &mut jobs {
            job.run_at = job.run_at.filter(|at| !is_due(*at));
        }
        self.0.push_many(jobs)
    }

    /// Stores a job that no worker claims before `run_at`. A time already
    /// past enqueues it at once, as [`push`](Queue::push) does.
    pub fn schedule(
        &self,
        name: &str,
        queue: &str,
        args: Vec<Value>,
        run_at: SystemTime,
    ) -> Result<JobId> {
        self.push_job(NewJob::new(name, queue, args).run_at(run_at))
    }

    /// Moves the scheduled jobs due by `now` onto their queues. Claims do it
    /// on their own when they find nothing to run; the worker's keeper also
    /// does it regularly, so due jobs move while every worker is busy.
    pub fn promote(&self, now: SystemTime) -> Result<Promoted> {
        self.0.promote(now)
    }

    /// Takes the next job for `worker`: the oldest on the first non-empty
    /// queue of `queues`, waiting up to `wait` for one. The job comes back
    /// typed as [`Processing`], the only state that can be completed or failed.
    ///
    /// When no job is waiting, it promotes the scheduled jobs that are due,
    /// and a waiting claim also wakes when the next scheduled job comes due,
    /// rather than at the end of `wait`. While blocked, it checks for newly
    /// scheduled jobs at least every 100 ms (plus backend latency).
    pub fn claim(
        &self,
        worker: &str,
        queues: &[&str],
        wait: Duration,
    ) -> Result<Option<Job<Processing>>> {
        let deadline = Instant::now() + wait;
        // Checked first: promoting costs a backend call, and a busy queue
        // rarely needs it.
        if let Some(record) = self.0.claim(worker, queues, Duration::ZERO)? {
            return Ok(Some(Job::from_record(record)));
        }
        loop {
            let promoted = self.0.promote(SystemTime::now())?;
            let left = deadline
                .saturating_duration_since(Instant::now())
                .min(MAX_SCHEDULE_WAIT);
            let until = match promoted.next {
                _ if promoted.moved > 0 => Duration::ZERO,
                Some(next) => left.min(
                    next.duration_since(SystemTime::now())
                        .unwrap_or_default()
                        .max(MIN_SCHEDULE_WAIT),
                ),
                None => left,
            };
            let started = Instant::now();
            if let Some(record) = self.0.claim(worker, queues, until)? {
                return Ok(Some(Job::from_record(record)));
            }
            // After the last wait, make one final promotion/nonblocking claim:
            // a job scheduled during that wait may now be due. Backends whose
            // claims never wait still return at once for the worker to poll.
            if left.is_zero() || started.elapsed() < until {
                return Ok(None);
            }
        }
    }

    /// Stores `output` and marks the job done. Takes the job by value, so it
    /// can't be completed twice or failed afterwards.
    pub fn complete(&self, worker: &str, job: Job<Processing>, output: Value) -> Result<Job<Done>> {
        let mut record = job.into_record();
        record.result = Some(output);
        self.0.complete(worker, &record)?;
        Ok(Job::from_record(record))
    }

    /// Saves the progress of a job `worker` holds.
    pub fn checkpoint(&self, worker: &str, job: &JobRecord) -> Result<()> {
        self.0.checkpoint(worker, job)
    }

    /// Puts a job interrupted at a checkpoint back on its queue, with its
    /// progress, to resume. Unlike [`fail`](Queue::fail), it isn't counted as
    /// a failed attempt.
    pub fn interrupt(&self, worker: &str, job: Job<Processing>) -> Result<Job<Pending>> {
        let record = job.into_record();
        self.0.fail(worker, &record, JobState::Pending)?;
        Ok(Job::from_record(record))
    }

    /// Records a failed attempt: the job is retried after its policy's
    /// backoff, or dies once it has failed more than `max_retries` times. A
    /// plain number is a policy that retries at once:
    /// `queue.fail(worker, job, error, 3)`.
    pub fn fail(
        &self,
        worker: &str,
        job: Job<Processing>,
        error: String,
        policy: impl Into<RetryPolicy>,
    ) -> Result<Failed> {
        self.fail_with(worker, job, error, Retry::Default, policy)
    }

    /// Like [`fail`](Queue::fail), with what the job's error asked for:
    /// [`Retry::Never`] makes it dead at once, and [`Retry::After`] replaces
    /// the backoff's delay. A retry that waits is
    /// [`Scheduled`](JobState::Scheduled), and promoted onto its queue when
    /// it is due.
    pub fn fail_with(
        &self,
        worker: &str,
        job: Job<Processing>,
        error: String,
        retry: Retry,
        policy: impl Into<RetryPolicy>,
    ) -> Result<Failed> {
        let mut record = job.into_record();
        let next = record_failure(&mut record, error, retry, policy.into());
        self.0.fail(worker, &record, next)?;
        Ok(match next {
            JobState::Dead => Failed::Dead(Job::from_record(record)),
            JobState::Scheduled => Failed::Scheduled(Job::from_record(record)),
            _ => Failed::Retry(Job::from_record(record)),
        })
    }

    /// Stores or refreshes recurring schedules; see
    /// [`Store::register_recurring`].
    pub fn register_recurring(
        &self,
        schedules: &[RecurringRecord],
    ) -> Result<Vec<RecurringRecord>> {
        self.0.register_recurring(schedules)
    }

    /// Enqueues `job` as tick `tick` of recurring schedule `key`, unless some
    /// worker already did. Returns its id, or `None` if the tick was taken.
    /// A run time on `job` is ignored: it is pending at once.
    pub fn push_recurring(
        &self,
        key: &str,
        tick: SystemTime,
        mut job: NewJob,
    ) -> Result<Option<JobId>> {
        job.run_at = None;
        self.0.push_recurring(key, tick, job)
    }

    /// Every registered recurring schedule, sorted by key.
    pub fn recurring(&self) -> Result<Vec<RecurringRecord>> {
        self.0.recurring()
    }

    pub fn remove_recurring(&self, key: &str) -> Result<bool> {
        self.0.remove_recurring(key)
    }

    pub fn heartbeat(&self, worker: &str, ttl: Duration) -> Result<()> {
        self.0.heartbeat(worker, ttl)
    }

    pub fn retire(&self, worker: &str) -> Result<()> {
        self.0.retire(worker)
    }

    pub fn recover(&self) -> Result<usize> {
        self.0.recover()
    }

    /// The job as it is right now, typed by its state.
    pub fn get(&self, id: &str) -> Result<Option<AnyJob>> {
        Ok(self
            .0
            .get(id)?
            .map(|(state, record)| AnyJob::new(state, record)))
    }

    /// The job's current state. Returns `None` if the job is unknown, or if the
    /// backend can't be reached.
    pub fn state(&self, id: &str) -> Option<JobState> {
        self.0.get(id).ok().flatten().map(|(state, _)| state)
    }

    pub fn cancel(&self, id: &str) -> Result<bool> {
        self.0.cancel(id)
    }

    /// A handle to an existing job, for example from an id stored elsewhere.
    pub fn handle(&self, id: impl Into<JobId>) -> crate::JobHandle {
        crate::JobHandle::new(self.clone(), id.into())
    }

    pub fn watch_finished(&self, id: &str) -> Option<Arc<Signal>> {
        self.0.watch_finished(id)
    }

    pub fn blocks(&self) -> bool {
        self.0.blocks()
    }

    pub fn stats(&self) -> Result<Stats> {
        self.0.stats()
    }

    pub fn list(&self, filter: &ListFilter) -> Result<Vec<AnyJob>> {
        let state = filter.state;
        Ok(self
            .0
            .list(filter)?
            .into_iter()
            .map(|record| AnyJob::new(state, record))
            .collect())
    }

    pub fn retry(&self, id: &str) -> Result<bool> {
        self.0.retry(id)
    }

    /// Moves a scheduled job onto its queue now, ahead of its run time.
    pub fn run_now(&self, id: &str) -> Result<bool> {
        self.0.run_now(id)
    }

    pub fn discard(&self, id: &str) -> Result<bool> {
        self.0.discard(id)
    }

    pub fn record_metric(&self, metric: &JobMetric) -> Result<()> {
        self.0.record_metric(metric)
    }

    pub fn metrics(&self, since_minute: u64) -> Result<Vec<MetricBucket>> {
        self.0.metrics(since_minute)
    }

    pub fn describe(&self) -> String {
        self.0.describe()
    }
}

impl std::fmt::Debug for Queue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Queue({})", self.describe())
    }
}

impl From<MemoryQueue> for Queue {
    fn from(q: MemoryQueue) -> Self {
        Queue::new(q)
    }
}

impl From<FileQueue> for Queue {
    fn from(q: FileQueue) -> Self {
        Queue::new(q)
    }
}

#[cfg(feature = "sqlite")]
impl From<SqliteQueue> for Queue {
    fn from(q: SqliteQueue) -> Self {
        Queue::new(q)
    }
}

#[cfg(feature = "redis")]
impl From<RedisQueue> for Queue {
    fn from(q: RedisQueue) -> Self {
        Queue::new(q)
    }
}
