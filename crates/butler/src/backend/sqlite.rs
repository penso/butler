//! A SQLite-backed queue: one database file that several processes on the same
//! machine can share, with no server to run.
//!
//! ```text
//! butler_jobs     id, queue, state, worker, seq, data (job JSON), finished_seq, run_at, slot
//! butler_workers  worker, expires_at_ms (the heartbeat)
//! butler_recurring        key, data (schedule JSON), created_at_ms, seen_at_ms, last_tick_ms, last_job_id
//! butler_recurring_ticks  key, tick_ms, job_id: primary key (key, tick_ms)
//! ```
//!
//! A claim is one `UPDATE ... RETURNING` that moves the oldest pending row of
//! a queue to `processing` under this worker. SQLite runs it under its write
//! lock, so only one worker gets each job. `seq` orders the queue: pushes and
//! retries go to the back, recovered jobs to the front.
//!
//! A scheduled job is a `scheduled` row with its run time in `run_at`
//! (milliseconds since the epoch). Promotion turns due rows into `pending`
//! ones at the back of their queue, in run-time order; claims only take
//! `pending` rows, so a scheduled job can't run early. Databases created
//! before scheduling existed get the `run_at` column when opened.
//!
//! A claim under a global queue limit sets `slot = 1`, and only succeeds
//! while fewer than the limit of that queue's `processing` rows have it: the
//! count is part of the claim's one `UPDATE`. Anything that moves the job out
//! of `processing` (done, a retry, dead, an interruption, recovery) frees the
//! slot, since only processing rows count. Databases from before global
//! limits get the `slot` column when opened.
//!
//! A recurring tick is an `INSERT OR IGNORE` into `butler_recurring_ticks`,
//! whose primary key is `(key, tick_ms)`, and the job's insert, in one
//! transaction: only the worker whose tick row went in enqueues the job.
//! Databases from before recurring jobs get both tables when opened.
//!
//! Waking waiters, without a server to publish through:
//!
//! - **Same process: instant.** Every write through a `SqliteQueue` notifies
//!   its signals directly, like the memory backend.
//! - **Across processes: within a few milliseconds.** SQLite has no pub/sub
//!   between connections (its hooks only see their own connection), but
//!   `PRAGMA data_version` changes whenever another connection commits. In WAL
//!   mode that check reads shared memory rather than the table, so a watcher
//!   thread checks it every [`WATCH_TICK`] and wakes waiters when it moves.
//!   Waiters never re-scan the queues until something actually committed.
//!
//! Use a file path: `:memory:` databases are private to one connection.

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, OnceLock, PoisonError, Weak,
        atomic::{AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};

use super::{GlobalLimit, Monitor, NewJob, Promoted, Store, TICK_RETENTION, Watch};
use crate::{
    Error, JobId, JobRecord, JobState, RecurringRecord, Result, Signal,
    job::{from_millis, millis},
    monitor::{
        JobMetric, ListFilter, METRICS_RETENTION_MINUTES, MetricBucket, QueueStats, Stats,
        WorkerStats,
    },
    signal::JobWatch,
};

/// How often the watcher checks `PRAGMA data_version` for commits made by other
/// processes. Each check is a read of shared memory, a few microseconds.
pub const WATCH_TICK: Duration = Duration::from_millis(2);

/// How long a connection waits for another process's write lock.
const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS butler_jobs (
    id     TEXT PRIMARY KEY,
    queue  TEXT NOT NULL,
    state  TEXT NOT NULL,
    worker TEXT,
    seq    INTEGER NOT NULL,
    data   TEXT NOT NULL,
    -- Set when the job is done, dead or cancelled, in finishing order, so a
    -- watcher can ask which jobs finished since it last looked.
    finished_seq INTEGER,
    -- When a scheduled job may run, in ms since the epoch.
    run_at INTEGER,
    -- 1 while claimed under a global queue limit, counted while processing.
    slot INTEGER
);
CREATE INDEX IF NOT EXISTS butler_jobs_claim ON butler_jobs (state, queue, seq);
CREATE INDEX IF NOT EXISTS butler_jobs_finished ON butler_jobs (finished_seq);
-- `MAX(seq)` on every push and `MIN(seq)` on recovery: without this, each is a
-- full scan, so enqueueing gets slower as the table grows.
CREATE INDEX IF NOT EXISTS butler_jobs_seq ON butler_jobs (seq);
CREATE TABLE IF NOT EXISTS butler_workers (
    worker        TEXT PRIMARY KEY,
    expires_at_ms INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS butler_metrics (
    minute    INTEGER NOT NULL,
    queue     TEXT NOT NULL,
    job       TEXT NOT NULL,
    processed INTEGER NOT NULL,
    failed    INTEGER NOT NULL,
    total_ms  INTEGER NOT NULL,
    max_ms    INTEGER NOT NULL,
    PRIMARY KEY (minute, queue, job)
);
CREATE TABLE IF NOT EXISTS butler_counters (
    name  TEXT PRIMARY KEY,
    value INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS butler_recurring (
    key           TEXT PRIMARY KEY,
    -- The schedule's definition, as JSON; the columns below win over it.
    data          TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    seen_at_ms    INTEGER NOT NULL,
    last_tick_ms  INTEGER,
    last_job_id   TEXT
);
-- One row per tick enqueued: the primary key is what makes a tick run once.
CREATE TABLE IF NOT EXISTS butler_recurring_ticks (
    key     TEXT NOT NULL,
    tick_ms INTEGER NOT NULL,
    job_id  TEXT NOT NULL,
    PRIMARY KEY (key, tick_ms)
);
";

/// Needs the `run_at` column, which older databases only have once
/// [`migrate`] added it.
const SCHEDULED_INDEX: &str =
    "CREATE INDEX IF NOT EXISTS butler_jobs_scheduled ON butler_jobs (state, run_at);";

/// Takes the next sequence number: the back of every queue.
const NEXT_SEQ: &str = "(SELECT COALESCE(MAX(seq), 0) + 1 FROM butler_jobs)";

pub struct SqliteQueue {
    path: PathBuf,
    /// One connection per queue: every statement is short, and waiting
    /// happens on `signals`, never while holding it.
    conn: Mutex<Connection>,
    signals: Arc<Signals>,
    /// The cross-process watcher thread, started by the first waiter.
    watcher: OnceLock<()>,
    /// The minute history was last pruned, so it's pruned once a minute.
    pruned_at_minute: AtomicU64,
}

/// A job was pushed (idle claims re-check their queues), or a job finished
/// (`JobHandle::wait` re-checks).
#[derive(Default)]
struct Signals {
    pushed: Signal,
    /// Per job: finishing one wakes only its own waiters.
    finished: JobWatch,
}

impl SqliteQueue {
    /// Opens (or creates) the database at `path` and its tables.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let conn = connect(&path)?;
        conn.execute_batch(SCHEMA)?;
        migrate(&conn)?;
        Ok(Self {
            path,
            conn: Mutex::new(conn),
            signals: Arc::default(),
            watcher: OnceLock::new(),
            pruned_at_minute: AtomicU64::new(0),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Runs `f` on the connection. A panic mid-statement leaves nothing half
    /// applied (SQLite rolls back), so a poisoned lock is still usable.
    fn with_conn<T>(&self, f: impl FnOnce(&Connection) -> rusqlite::Result<T>) -> Result<T> {
        let conn = self.conn.lock().unwrap_or_else(PoisonError::into_inner);
        Ok(f(&conn)?)
    }

    /// Starts, once, the thread that turns other processes' commits into
    /// wake-ups. Enqueue-only processes never start it.
    fn watch_other_processes(&self) {
        self.watcher.get_or_init(|| {
            let path = self.path.clone();
            let signals = Arc::downgrade(&self.signals);
            let spawned = thread::Builder::new()
                .name("butler-sqlite-watch".into())
                .spawn(move || watch(&path, &signals));
            if let Err(err) = spawned {
                tracing::error!(error = %err, "no cross-process wake-ups; waiters fall back to polling");
            }
        });
    }

    /// Takes the oldest pending job of the first non-empty queue, skipping
    /// queues at their global limit. Never waits.
    fn sweep(
        &self,
        worker: &str,
        queues: &[&str],
        limits: &[GlobalLimit<'_>],
    ) -> Result<Option<JobRecord>> {
        for queue in queues {
            let data: Option<String> = self.with_conn(|conn| {
                match GlobalLimit::of(limits, queue) {
                    None => conn.query_row(
                        "UPDATE butler_jobs SET state = 'processing', worker = ?1, slot = NULL
                         WHERE id = (SELECT id FROM butler_jobs
                                     WHERE state = 'pending' AND queue = ?2
                                     ORDER BY seq LIMIT 1)
                         RETURNING data",
                        params![worker, queue],
                        |row| row.get(0),
                    ),
                    Some(max) => conn.query_row(
                        "UPDATE butler_jobs SET state = 'processing', worker = ?1, slot = 1
                         WHERE id = (SELECT id FROM butler_jobs
                                     WHERE state = 'pending' AND queue = ?2
                                     ORDER BY seq LIMIT 1)
                           AND (SELECT COUNT(*) FROM butler_jobs
                                WHERE state = 'processing' AND queue = ?2 AND slot = 1) < ?3
                         RETURNING data",
                        params![worker, queue, i64::try_from(max).unwrap_or(i64::MAX)],
                        |row| row.get(0),
                    ),
                }
                .optional()
            })?;
            if let Some(data) = data {
                return Ok(Some(serde_json::from_str(&data)?));
            }
        }
        Ok(None)
    }
}

/// Brings a database created by an older version up to this schema. Runs
/// under the write lock, so processes opening the same file at once apply
/// each change once.
fn migrate(conn: &Connection) -> rusqlite::Result<()> {
    let tx = Transaction::new_unchecked(conn, TransactionBehavior::Immediate)?;
    let has_run_at = tx
        .prepare("SELECT 1 FROM pragma_table_info('butler_jobs') WHERE name = 'run_at'")?
        .exists([])?;
    if !has_run_at {
        tx.execute_batch("ALTER TABLE butler_jobs ADD COLUMN run_at INTEGER")?;
    }
    let has_slot = tx
        .prepare("SELECT 1 FROM pragma_table_info('butler_jobs') WHERE name = 'slot'")?
        .exists([])?;
    if !has_slot {
        tx.execute_batch("ALTER TABLE butler_jobs ADD COLUMN slot INTEGER")?;
    }
    tx.execute_batch(SCHEDULED_INDEX)?;
    tx.commit()
}

fn connect(path: &Path) -> rusqlite::Result<Connection> {
    let conn = Connection::open(path)?;
    conn.busy_timeout(BUSY_TIMEOUT)?;
    // WAL: readers don't block the writer, and `data_version` is cheap.
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(None, "synchronous", "NORMAL")?;
    Ok(conn)
}

/// Wakes both signals whenever `data_version` moves, until the queue that
/// owns `signals` is dropped. On errors it reconnects after a pause; waiters
/// still re-check at their fallback interval meanwhile.
fn watch(path: &Path, signals: &Weak<Signals>) {
    while signals.strong_count() > 0 {
        if let Err(err) = watch_once(path, signals) {
            tracing::warn!(error = %err, "sqlite change watcher failed; restarting");
            thread::sleep(Duration::from_secs(1));
        }
    }
}

fn watch_once(path: &Path, signals: &Weak<Signals>) -> rusqlite::Result<()> {
    let conn = connect(path)?;
    let version = || conn.query_row("PRAGMA data_version", [], |row| row.get::<_, i64>(0));
    let mut seen = version()?;
    let mut finished_upto: i64 = conn.query_row(
        "SELECT COALESCE(MAX(finished_seq), 0) FROM butler_jobs",
        [],
        |row| row.get(0),
    )?;
    // Anything may have happened while no watcher was running.
    if let Some(signals) = signals.upgrade() {
        signals.pushed.notify();
        signals.finished.notify_all();
    }
    let mut finished_since = conn.prepare(
        "SELECT id, finished_seq FROM butler_jobs WHERE finished_seq > ?1 ORDER BY finished_seq",
    )?;
    loop {
        thread::sleep(WATCH_TICK);
        let now = version()?;
        if now == seen {
            if signals.strong_count() == 0 {
                return Ok(());
            }
            continue;
        }
        seen = now;
        let Some(signals) = signals.upgrade() else {
            return Ok(());
        };
        signals.pushed.notify();
        // Only the jobs that finished since the last look, by id.
        let finished = finished_since.query_map(params![finished_upto], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })?;
        for row in finished {
            let (id, seq) = row?;
            finished_upto = finished_upto.max(seq);
            signals.finished.notify(&id);
        }
    }
}

/// A stored time or count as a SQLite integer.
fn int(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

/// A `butler_recurring` row: `data`, `[created_at_ms, seen_at_ms]`,
/// `last_tick_ms`, `last_job_id`.
type RecurringRow = (String, [i64; 2], Option<i64>, Option<String>);

/// Reads a `butler_recurring` row: `data`, `created_at_ms`, `seen_at_ms`,
/// `last_tick_ms`, `last_job_id`, in that order.
fn recurring_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<RecurringRow> {
    Ok((
        row.get(0)?,
        [row.get(1)?, row.get(2)?],
        row.get(3)?,
        row.get(4)?,
    ))
}

fn from_recurring_row(
    (data, [created, seen], last_tick, last_job): RecurringRow,
) -> Result<RecurringRecord> {
    let mut schedule: RecurringRecord = serde_json::from_str(&data)?;
    schedule.created_at_ms = count(created);
    schedule.seen_at_ms = count(seen);
    schedule.last_tick_ms = last_tick.map(count);
    schedule.last_job_id = last_job;
    Ok(schedule)
}

/// SQLite integers are signed; counts and durations are never negative.
fn count(value: i64) -> u64 {
    u64::try_from(value).unwrap_or(0)
}

/// Where a new job starts: scheduled if it has a run time.
fn new_state(job: &JobRecord) -> JobState {
    match job.run_at_ms {
        Some(_) => JobState::Scheduled,
        None => JobState::Pending,
    }
}

/// A run time as stored in `run_at`.
fn run_at_column(job: &JobRecord) -> Option<i64> {
    job.run_at_ms
        .map(|ms| i64::try_from(ms).unwrap_or(i64::MAX))
}

fn millis_of(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn now_ms() -> i64 {
    let ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    i64::try_from(ms).unwrap_or(i64::MAX)
}

impl Store for SqliteQueue {
    fn push(&self, job: NewJob) -> Result<JobId> {
        let job = job.into_record();
        let data = serde_json::to_string(&job)?;
        let state = new_state(&job);
        self.with_conn(|conn| {
            conn.execute(
                &format!(
                    "INSERT INTO butler_jobs (id, queue, state, seq, data, run_at)
                     VALUES (?1, ?2, ?3, {NEXT_SEQ}, ?4, ?5)"
                ),
                params![job.id, job.queue, state.as_str(), data, run_at_column(&job)],
            )
        })?;
        // No wake-up for a scheduled job: there is nothing to claim until
        // it is promoted.
        if state == JobState::Pending {
            self.signals.pushed.notify();
        }
        Ok(job.id)
    }

    /// One transaction for every job: a commit per insert is what makes
    /// SQLite slow.
    fn push_many(&self, jobs: Vec<NewJob>) -> Result<Vec<JobId>> {
        let records = jobs
            .into_iter()
            .map(|new| {
                let job = new.into_record();
                let data = serde_json::to_string(&job)?;
                Ok((job, data))
            })
            .collect::<Result<Vec<_>>>()?;
        let ids = self.with_conn(|conn| {
            let tx = conn.unchecked_transaction()?;
            {
                let mut insert = tx.prepare(&format!(
                    "INSERT INTO butler_jobs (id, queue, state, seq, data, run_at)
                     VALUES (?1, ?2, ?3, {NEXT_SEQ}, ?4, ?5)"
                ))?;
                for (job, data) in &records {
                    insert.execute(params![
                        job.id,
                        job.queue,
                        new_state(job).as_str(),
                        data,
                        run_at_column(job)
                    ])?;
                }
            }
            tx.commit()?;
            Ok(records.into_iter().map(|(job, _)| job.id).collect())
        })?;
        self.signals.pushed.notify();
        Ok(ids)
    }

    fn promote(&self, now: SystemTime) -> Result<Promoted> {
        let now = i64::try_from(millis(now)).unwrap_or(i64::MAX);
        let next_run_at = |conn: &Connection| {
            conn.query_row(
                "SELECT MIN(run_at) FROM butler_jobs WHERE state = 'scheduled'",
                [],
                |row| row.get::<_, Option<i64>>(0),
            )
        };
        // A read first: the write lock is only taken when something is due.
        let next = self.with_conn(next_run_at)?;
        let (moved, next) = match next {
            Some(at) if at <= now => self.with_conn(|conn| {
                let tx = Transaction::new_unchecked(conn, TransactionBehavior::Immediate)?;
                let due: Vec<String> = tx
                    .prepare(
                        "SELECT id FROM butler_jobs WHERE state = 'scheduled' AND run_at <= ?1
                         ORDER BY run_at, seq",
                    )?
                    .query_map(params![now], |row| row.get(0))?
                    .collect::<rusqlite::Result<_>>()?;
                let mut moved = 0;
                {
                    // One at a time, so each gets its own place in line.
                    let mut enqueue = tx.prepare(&format!(
                        "UPDATE butler_jobs SET state = 'pending', seq = {NEXT_SEQ}
                         WHERE id = ?1 AND state = 'scheduled'"
                    ))?;
                    for id in &due {
                        moved += enqueue.execute(params![id])?;
                    }
                }
                let next = next_run_at(&tx)?;
                tx.commit()?;
                Ok((moved, next))
            })?,
            next => (0, next),
        };
        if moved > 0 {
            self.signals.pushed.notify();
        }
        Ok(Promoted {
            moved,
            next: next.map(|at| from_millis(count(at))),
        })
    }

    fn claim(&self, worker: &str, queues: &[&str], wait: Duration) -> Result<Option<JobRecord>> {
        self.claim_within_limits(worker, queues, &[], wait)
    }

    fn claim_within_limits(
        &self,
        worker: &str,
        queues: &[&str],
        limits: &[GlobalLimit<'_>],
        wait: Duration,
    ) -> Result<Option<JobRecord>> {
        if queues.is_empty() {
            return Ok(None);
        }
        if !wait.is_zero() {
            self.watch_other_processes();
        }
        let deadline = Instant::now() + wait;
        loop {
            // Read before sweeping: a commit during the sweep still wakes us.
            let seen = self.signals.pushed.generation();
            if let Some(job) = self.sweep(worker, queues, limits)? {
                return Ok(Some(job));
            }
            let now = Instant::now();
            if now >= deadline {
                return Ok(None);
            }
            self.signals.pushed.wait_past(seen, deadline - now);
        }
    }

    fn complete(&self, _worker: &str, job: &JobRecord) -> Result<()> {
        let data = serde_json::to_string(job)?;
        self.with_conn(|conn| {
            conn.execute(
                "UPDATE butler_jobs
                 SET state = 'done', worker = NULL, data = ?2,
                     finished_seq = (SELECT COALESCE(MAX(finished_seq), 0) + 1 FROM butler_jobs)
                 WHERE id = ?1",
                params![job.id, data],
            )
        })?;
        self.signals.finished.notify(&job.id);
        Ok(())
    }

    fn fail(&self, _worker: &str, job: &JobRecord, next: JobState) -> Result<()> {
        let data = serde_json::to_string(job)?;
        self.with_conn(|conn| match next {
            JobState::Pending => conn.execute(
                "UPDATE butler_jobs
                 SET state = 'pending', worker = NULL, data = ?2,
                     seq = (SELECT COALESCE(MAX(seq), 0) + 1 FROM butler_jobs)
                 WHERE id = ?1",
                params![job.id, data],
            ),
            JobState::Scheduled => conn.execute(
                "UPDATE butler_jobs
                 SET state = 'scheduled', worker = NULL, data = ?2, run_at = ?3
                 WHERE id = ?1",
                params![job.id, data, run_at_column(job)],
            ),
            _ => conn.execute(
                "UPDATE butler_jobs
                 SET state = ?3, worker = NULL, data = ?2,
                     finished_seq = (SELECT COALESCE(MAX(finished_seq), 0) + 1 FROM butler_jobs)
                 WHERE id = ?1",
                params![job.id, data, next.as_str()],
            ),
        })?;
        match next {
            JobState::Pending => self.signals.pushed.notify(),
            JobState::Scheduled => {}
            _ => self.signals.finished.notify(&job.id),
        }
        Ok(())
    }

    fn get(&self, id: &str) -> Result<Option<(JobState, JobRecord)>> {
        let row: Option<(String, String)> = self.with_conn(|conn| {
            conn.query_row(
                "SELECT state, data FROM butler_jobs WHERE id = ?1",
                params![id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
        })?;
        let Some((state, data)) = row else {
            return Ok(None);
        };
        let state = JobState::parse(&state).ok_or_else(|| Error::UnknownState {
            id: id.to_owned(),
            state,
        })?;
        Ok(Some((state, serde_json::from_str(&data)?)))
    }

    fn checkpoint(&self, worker: &str, job: &JobRecord) -> Result<()> {
        let data = serde_json::to_string(job)?;
        self.with_conn(|conn| {
            conn.execute(
                "UPDATE butler_jobs SET data = ?3
                 WHERE id = ?1 AND state = 'processing' AND worker = ?2",
                params![job.id, worker, data],
            )
        })?;
        Ok(())
    }

    fn cancel(&self, id: &str) -> Result<bool> {
        let changed = self.with_conn(|conn| {
            conn.execute(
                "UPDATE butler_jobs
                 SET state = 'cancelled',
                     finished_seq = (SELECT COALESCE(MAX(finished_seq), 0) + 1 FROM butler_jobs)
                 WHERE id = ?1 AND state IN ('pending', 'scheduled')",
                params![id],
            )
        })?;
        if changed == 0 {
            return Ok(false);
        }
        self.signals.finished.notify(id);
        Ok(true)
    }

    fn heartbeat(&self, worker: &str, ttl: Duration) -> Result<()> {
        let ttl_ms = i64::try_from(ttl.as_millis()).unwrap_or(i64::MAX);
        self.with_conn(|conn| {
            conn.execute(
                "INSERT INTO butler_workers (worker, expires_at_ms) VALUES (?1, ?2)
                 ON CONFLICT (worker) DO UPDATE SET expires_at_ms = excluded.expires_at_ms",
                params![worker, now_ms().saturating_add(ttl_ms)],
            )
        })?;
        Ok(())
    }

    fn retire(&self, worker: &str) -> Result<()> {
        self.with_conn(|conn| {
            conn.execute(
                "DELETE FROM butler_workers WHERE worker = ?1",
                params![worker],
            )
        })?;
        Ok(())
    }

    fn recover(&self) -> Result<usize> {
        let now = now_ms();
        // One statement, so two workers recovering at once can't both move a job.
        let recovered = self.with_conn(|conn| {
            let recovered = conn.execute(
                "UPDATE butler_jobs
                 SET state = 'pending', worker = NULL,
                     seq = (SELECT COALESCE(MIN(seq), 0) - 1 FROM butler_jobs)
                 WHERE state = 'processing'
                   AND (worker IS NULL OR worker NOT IN
                        (SELECT worker FROM butler_workers WHERE expires_at_ms > ?1))",
                params![now],
            )?;
            conn.execute(
                "DELETE FROM butler_workers WHERE expires_at_ms <= ?1",
                params![now],
            )?;
            Ok(recovered)
        })?;
        if recovered > 0 {
            self.signals.pushed.notify();
        }
        Ok(recovered)
    }

    /// One transaction: an existing schedule keeps its creation time and
    /// last run.
    fn register_recurring(&self, schedules: &[RecurringRecord]) -> Result<Vec<RecurringRecord>> {
        let rows = schedules
            .iter()
            .map(|schedule| Ok((schedule, serde_json::to_string(schedule)?)))
            .collect::<Result<Vec<_>>>()?;
        let stored = self.with_conn(|conn| {
            let tx = conn.unchecked_transaction()?;
            let mut stored = Vec::with_capacity(rows.len());
            {
                let mut upsert = tx.prepare(
                    "INSERT INTO butler_recurring (key, data, created_at_ms, seen_at_ms)
                     VALUES (?1, ?2, ?3, ?4)
                     ON CONFLICT (key) DO UPDATE SET
                         data = excluded.data, seen_at_ms = excluded.seen_at_ms
                     RETURNING data, created_at_ms, seen_at_ms, last_tick_ms, last_job_id",
                )?;
                for (schedule, data) in &rows {
                    stored.push(upsert.query_row(
                        params![
                            schedule.key,
                            data,
                            int(schedule.created_at_ms),
                            int(schedule.seen_at_ms)
                        ],
                        recurring_row,
                    )?);
                }
            }
            tx.commit()?;
            Ok(stored)
        })?;
        stored.into_iter().map(from_recurring_row).collect()
    }

    fn push_recurring(&self, key: &str, tick: SystemTime, job: NewJob) -> Result<Option<JobId>> {
        let job = job.into_record();
        let data = serde_json::to_string(&job)?;
        let tick = int(millis(tick));
        let oldest = tick.saturating_sub(int(millis_of(TICK_RETENTION)));
        let pushed = self.with_conn(|conn| {
            let tx = Transaction::new_unchecked(conn, TransactionBehavior::Immediate)?;
            let taken = tx.execute(
                "INSERT OR IGNORE INTO butler_recurring_ticks (key, tick_ms, job_id)
                 VALUES (?1, ?2, ?3)",
                params![key, tick, job.id],
            )?;
            if taken == 0 {
                return Ok(false);
            }
            tx.execute(
                &format!(
                    "INSERT INTO butler_jobs (id, queue, state, seq, data)
                     VALUES (?1, ?2, 'pending', {NEXT_SEQ}, ?3)"
                ),
                params![job.id, job.queue, data],
            )?;
            tx.execute(
                "UPDATE butler_recurring SET last_tick_ms = ?2, last_job_id = ?3
                 WHERE key = ?1 AND (last_tick_ms IS NULL OR last_tick_ms < ?2)",
                params![key, tick, job.id],
            )?;
            tx.execute(
                "DELETE FROM butler_recurring_ticks WHERE key = ?1 AND tick_ms < ?2",
                params![key, oldest],
            )?;
            tx.commit()?;
            Ok(true)
        })?;
        if !pushed {
            return Ok(None);
        }
        self.signals.pushed.notify();
        Ok(Some(job.id))
    }

    fn describe(&self) -> String {
        format!("sqlite:{}", self.path.display())
    }
}

impl Monitor for SqliteQueue {
    fn stats(&self) -> Result<Stats> {
        self.with_conn(|conn| {
            let mut stats = Stats::default();
            let mut queues: BTreeMap<String, u64> = BTreeMap::new();
            let mut by_state = conn
                .prepare("SELECT queue, state, COUNT(*) FROM butler_jobs GROUP BY queue, state")?;
            let rows = by_state.query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    count(row.get::<_, i64>(2)?),
                ))
            })?;
            for row in rows {
                let (queue, state, count) = row?;
                let pending = queues.entry(queue).or_default();
                match JobState::parse(&state) {
                    Some(JobState::Pending) => *pending += count,
                    Some(JobState::Scheduled) => stats.scheduled += count,
                    Some(JobState::Processing) => stats.processing += count,
                    Some(JobState::Done) => stats.done += count,
                    Some(JobState::Dead) => stats.dead += count,
                    Some(JobState::Cancelled) => stats.cancelled += count,
                    None => {}
                }
            }
            stats.queues = queues
                .into_iter()
                .map(|(name, pending)| QueueStats { name, pending })
                .collect();

            let mut counters = conn.prepare("SELECT name, value FROM butler_counters")?;
            for row in counters.query_map([], |row| {
                Ok((row.get::<_, String>(0)?, count(row.get::<_, i64>(1)?)))
            })? {
                match row? {
                    (name, value) if name == "processed" => stats.processed_total = value,
                    (name, value) if name == "failed" => stats.failed_total = value,
                    _ => {}
                }
            }

            // Workers with a heartbeat, and any still holding jobs without one.
            let mut workers: BTreeMap<String, WorkerStats> = BTreeMap::new();
            let now = now_ms();
            let mut beats = conn.prepare("SELECT worker, expires_at_ms FROM butler_workers")?;
            for row in beats.query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })? {
                let (id, expires_at_ms) = row?;
                workers.insert(
                    id.clone(),
                    WorkerStats {
                        id,
                        running: 0,
                        expires_in_ms: expires_at_ms - now,
                    },
                );
            }
            let mut held = conn.prepare(
                "SELECT worker, COUNT(*) FROM butler_jobs
                 WHERE state = 'processing' AND worker IS NOT NULL GROUP BY worker",
            )?;
            for row in held.query_map([], |row| {
                Ok((row.get::<_, String>(0)?, count(row.get::<_, i64>(1)?)))
            })? {
                let (id, running) = row?;
                workers
                    .entry(id.clone())
                    .or_insert_with(|| WorkerStats {
                        id,
                        running: 0,
                        expires_in_ms: -1,
                    })
                    .running = running;
            }
            stats.workers = workers.into_values().collect();
            Ok(stats)
        })
    }

    fn list(&self, filter: &ListFilter) -> Result<Vec<JobRecord>> {
        let order = match filter.state {
            state if state.is_finished() => "finished_seq DESC",
            JobState::Scheduled => "run_at ASC, seq ASC",
            _ => "seq ASC",
        };
        let sql = format!(
            "SELECT data FROM butler_jobs
             WHERE state = ?1 AND (?2 IS NULL OR queue = ?2)
             ORDER BY {order} LIMIT ?3 OFFSET ?4"
        );
        let rows: Vec<String> = self.with_conn(|conn| {
            let mut select = conn.prepare(&sql)?;
            select
                .query_map(
                    params![
                        filter.state.as_str(),
                        filter.queue,
                        i64::try_from(filter.limit).unwrap_or(i64::MAX),
                        i64::try_from(filter.offset).unwrap_or(i64::MAX),
                    ],
                    |row| row.get(0),
                )?
                .collect()
        })?;
        rows.iter()
            .map(|data| Ok(serde_json::from_str(data)?))
            .collect()
    }

    fn retry(&self, id: &str) -> Result<bool> {
        let data: Option<String> = self.with_conn(|conn| {
            conn.query_row(
                "SELECT data FROM butler_jobs WHERE id = ?1 AND state = 'dead'",
                params![id],
                |row| row.get(0),
            )
            .optional()
        })?;
        let Some(data) = data else {
            return Ok(false);
        };
        let mut job: JobRecord = serde_json::from_str(&data)?;
        job.attempts = 0;
        let data = serde_json::to_string(&job)?;
        let changed = self.with_conn(|conn| {
            conn.execute(
                "UPDATE butler_jobs
                 SET state = 'pending', worker = NULL, finished_seq = NULL, data = ?2,
                     seq = (SELECT COALESCE(MAX(seq), 0) + 1 FROM butler_jobs)
                 WHERE id = ?1 AND state = 'dead'",
                params![id, data],
            )
        })?;
        if changed > 0 {
            self.signals.pushed.notify();
        }
        Ok(changed > 0)
    }

    fn run_now(&self, id: &str) -> Result<bool> {
        let changed = self.with_conn(|conn| {
            conn.execute(
                &format!(
                    "UPDATE butler_jobs SET state = 'pending', seq = {NEXT_SEQ}
                     WHERE id = ?1 AND state = 'scheduled'"
                ),
                params![id],
            )
        })?;
        if changed > 0 {
            self.signals.pushed.notify();
        }
        Ok(changed > 0)
    }

    fn discard(&self, id: &str) -> Result<bool> {
        let changed = self.with_conn(|conn| {
            conn.execute(
                "DELETE FROM butler_jobs
                 WHERE id = ?1 AND state IN ('done', 'dead', 'cancelled')",
                params![id],
            )
        })?;
        Ok(changed > 0)
    }

    fn recurring(&self) -> Result<Vec<RecurringRecord>> {
        let rows = self.with_conn(|conn| {
            conn.prepare(
                "SELECT data, created_at_ms, seen_at_ms, last_tick_ms, last_job_id
                 FROM butler_recurring ORDER BY key",
            )?
            .query_map([], recurring_row)?
            .collect::<rusqlite::Result<Vec<_>>>()
        })?;
        rows.into_iter().map(from_recurring_row).collect()
    }

    fn remove_recurring(&self, key: &str) -> Result<bool> {
        crate::recurring::validate_key(key)?;
        let removed = self.with_conn(|conn| {
            let tx = conn.unchecked_transaction()?;
            let removed =
                tx.execute("DELETE FROM butler_recurring WHERE key = ?1", params![key])?;
            tx.execute(
                "DELETE FROM butler_recurring_ticks WHERE key = ?1",
                params![key],
            )?;
            tx.commit()?;
            Ok(removed)
        })?;
        Ok(removed > 0)
    }

    /// One transaction: the bucket and both lifetime counters.
    fn record_metric(&self, metric: &JobMetric) -> Result<()> {
        let prune = self
            .pruned_at_minute
            .fetch_max(metric.minute, Ordering::AcqRel)
            < metric.minute;
        self.with_conn(|conn| {
            let tx = conn.unchecked_transaction()?;
            let duration = i64::try_from(metric.duration_ms).unwrap_or(i64::MAX);
            tx.execute(
                "INSERT INTO butler_metrics (minute, queue, job, processed, failed, total_ms, max_ms)
                 VALUES (?1, ?2, ?3, 1, ?4, ?5, ?5)
                 ON CONFLICT (minute, queue, job) DO UPDATE SET
                     processed = processed + 1,
                     failed = failed + excluded.failed,
                     total_ms = total_ms + excluded.total_ms,
                     max_ms = MAX(max_ms, excluded.max_ms)",
                params![
                    i64::try_from(metric.minute).unwrap_or(i64::MAX),
                    metric.queue,
                    metric.job,
                    i64::from(metric.failed),
                    duration
                ],
            )?;
            let mut count = tx.prepare(
                "INSERT INTO butler_counters (name, value) VALUES (?1, 1)
                 ON CONFLICT (name) DO UPDATE SET value = value + 1",
            )?;
            count.execute(params!["processed"])?;
            if metric.failed {
                count.execute(params!["failed"])?;
            }
            drop(count);
            if prune {
                tx.execute(
                    "DELETE FROM butler_metrics WHERE minute < ?1",
                    params![i64::try_from(metric.minute.saturating_sub(METRICS_RETENTION_MINUTES)).unwrap_or(0)],
                )?;
            }
            tx.commit()
        })
    }

    fn metrics(&self, since_minute: u64) -> Result<Vec<MetricBucket>> {
        self.with_conn(|conn| {
            let mut select = conn.prepare(
                "SELECT minute, queue, job, processed, failed, total_ms, max_ms
                 FROM butler_metrics WHERE minute >= ?1 ORDER BY minute, queue, job",
            )?;
            select
                .query_map(
                    params![i64::try_from(since_minute).unwrap_or(i64::MAX)],
                    |row| {
                        Ok(MetricBucket {
                            minute: count(row.get(0)?),
                            queue: row.get(1)?,
                            job: row.get(2)?,
                            processed: count(row.get(3)?),
                            failed: count(row.get(4)?),
                            total_ms: count(row.get(5)?),
                            max_ms: count(row.get(6)?),
                        })
                    },
                )?
                .collect()
        })
    }
}

impl Watch for SqliteQueue {
    fn watch_finished(&self, id: &str) -> Option<Arc<Signal>> {
        self.watch_other_processes();
        Some(self.signals.finished.watch(id))
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    /// `butler_jobs` as versions before scheduled jobs created it.
    const OLD_JOBS_TABLE: &str = "
    CREATE TABLE butler_jobs (
        id     TEXT PRIMARY KEY,
        queue  TEXT NOT NULL,
        state  TEXT NOT NULL,
        worker TEXT,
        seq    INTEGER NOT NULL,
        data   TEXT NOT NULL,
        finished_seq INTEGER
    );";

    /// `butler_jobs` as 0.1.0 created it, before global queue limits.
    const JOBS_TABLE_0_1: &str = "
    CREATE TABLE butler_jobs (
        id TEXT PRIMARY KEY, queue TEXT NOT NULL, state TEXT NOT NULL, worker TEXT,
        seq INTEGER NOT NULL, data TEXT NOT NULL, finished_seq INTEGER, run_at INTEGER
    );";

    #[test]
    fn a_0_1_database_gains_global_limits_and_keeps_its_jobs() {
        let path = std::env::temp_dir().join(format!(
            "butler-sqlite-migrate-slots-{}.db",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(JOBS_TABLE_0_1).unwrap();
            // Two pending jobs and one a 0.1.0 worker is running, as it wrote them.
            for (id, state, seq) in [
                ("1-1-0", "processing", 1),
                ("2-1-0", "pending", 2),
                ("3-1-0", "pending", 3),
            ] {
                let data = format!(
                    r#"{{"id":"{id}","name":"old","queue":"mailers","args":[],
                    "attempts":0,"enqueued_at_ms":1,"last_error":null}}"#
                );
                conn.execute(
                    "INSERT INTO butler_jobs (id, queue, state, worker, seq, data)
                     VALUES (?1, 'mailers', ?2, 'old-worker', ?3, ?4)",
                    params![id, state, seq, data],
                )
                .unwrap();
            }
        }
        let queue = SqliteQueue::open(&path).unwrap();
        drop(SqliteQueue::open(&path).unwrap());
        let limits = [GlobalLimit {
            queue: "mailers",
            max: 1,
        }];
        let claim = || {
            queue
                .claim_within_limits("w", &["mailers"], &limits, Duration::ZERO)
                .unwrap()
        };
        // The old worker's running job took no slot: it isn't counted.
        assert_eq!(claim().unwrap().id, "2-1-0");
        assert!(claim().is_none(), "the one slot is taken");
        let _ = std::fs::remove_file(&path);
    }

    /// The whole schema as 0.1.0 (before recurring jobs) created it.
    const SCHEMA_0_1: &str = "
    CREATE TABLE butler_jobs (
        id TEXT PRIMARY KEY, queue TEXT NOT NULL, state TEXT NOT NULL, worker TEXT,
        seq INTEGER NOT NULL, data TEXT NOT NULL, finished_seq INTEGER, run_at INTEGER
    );
    CREATE INDEX butler_jobs_claim ON butler_jobs (state, queue, seq);
    CREATE INDEX butler_jobs_finished ON butler_jobs (finished_seq);
    CREATE INDEX butler_jobs_seq ON butler_jobs (seq);
    CREATE INDEX butler_jobs_scheduled ON butler_jobs (state, run_at);
    CREATE TABLE butler_workers (worker TEXT PRIMARY KEY, expires_at_ms INTEGER NOT NULL);
    CREATE TABLE butler_metrics (
        minute INTEGER NOT NULL, queue TEXT NOT NULL, job TEXT NOT NULL,
        processed INTEGER NOT NULL, failed INTEGER NOT NULL, total_ms INTEGER NOT NULL,
        max_ms INTEGER NOT NULL, PRIMARY KEY (minute, queue, job)
    );
    CREATE TABLE butler_counters (name TEXT PRIMARY KEY, value INTEGER NOT NULL);";

    #[test]
    fn a_0_1_database_gains_recurring_jobs_and_keeps_its_jobs() {
        let path = std::env::temp_dir().join(format!(
            "butler-sqlite-migrate-0-1-{}.db",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let (pending, scheduled) = ("1-1-0", "2-1-0");
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(SCHEMA_0_1).unwrap();
            // A pending and a scheduled job, as 0.1.0 wrote them.
            let later = now_ms() + 3_600_000;
            let pending_data = r#"{"id":"1-1-0","name":"old","queue":"default","args":[],
                "attempts":0,"enqueued_at_ms":1,"last_error":null}"#;
            let scheduled_data = format!(
                r#"{{"id":"2-1-0","name":"later","queue":"default","args":[],
                "attempts":0,"enqueued_at_ms":1,"last_error":null,"run_at_ms":{later}}}"#
            );
            conn.execute(
                "INSERT INTO butler_jobs (id, queue, state, seq, data)
                 VALUES ('1-1-0', 'default', 'pending', 1, ?1)",
                params![pending_data],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO butler_jobs (id, queue, state, seq, data, run_at)
                 VALUES ('2-1-0', 'default', 'scheduled', 2, ?1, ?2)",
                params![scheduled_data, later],
            )
            .unwrap();
        }
        let queue = SqliteQueue::open(&path).unwrap();
        drop(SqliteQueue::open(&path).unwrap());
        assert_eq!(queue.get(pending).unwrap().unwrap().0, JobState::Pending);
        assert_eq!(
            queue.get(scheduled).unwrap().unwrap().0,
            JobState::Scheduled
        );

        let schedule = crate::Recurring::from_parts(
            "report".into(),
            "default".into(),
            vec![],
            crate::Cron::parse("0 * * * *").unwrap(),
        )
        .unwrap()
        .record(SystemTime::now());
        queue.register_recurring(&[schedule]).unwrap();
        let key = queue.recurring().unwrap()[0].key.clone();
        let tick = SystemTime::now();
        let job = || NewJob::new("report", "default", vec![]);
        assert!(queue.push_recurring(&key, tick, job()).unwrap().is_some());
        assert!(queue.push_recurring(&key, tick, job()).unwrap().is_none());
        // The old pending job is still first in line.
        let claimed = queue.claim("w", &["default"], Duration::ZERO).unwrap();
        assert_eq!(claimed.unwrap().id, pending);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn an_older_database_gains_scheduling_and_keeps_its_jobs() {
        let path =
            std::env::temp_dir().join(format!("butler-sqlite-migrate-{}.db", std::process::id()));
        let _ = std::fs::remove_file(&path);
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(OLD_JOBS_TABLE).unwrap();
            // A record as older versions wrote it: no run time.
            let data = r#"{"id":"1-1-0","name":"old","queue":"default","args":[1],
                "attempts":0,"enqueued_at_ms":1,"last_error":null}"#;
            conn.execute(
                "INSERT INTO butler_jobs (id, queue, state, seq, data)
                 VALUES ('1-1-0', 'default', 'pending', 1, ?1)",
                params![data],
            )
            .unwrap();
        }
        let queue = SqliteQueue::open(&path).unwrap();
        // Opening it again finds the column there and changes nothing.
        drop(SqliteQueue::open(&path).unwrap());

        let job = queue.claim("w", &["default"], Duration::ZERO).unwrap();
        let job = job.unwrap();
        assert_eq!((job.name.as_str(), job.run_at_ms), ("old", None));

        let at = SystemTime::now() + Duration::from_secs(60);
        let id = queue
            .push(NewJob::new("new", "default", vec![]).run_at(at))
            .unwrap();
        assert_eq!(queue.get(&id).unwrap().unwrap().0, JobState::Scheduled);
        assert_eq!(queue.promote(at).unwrap().moved, 1);
        let _ = std::fs::remove_file(&path);
    }
}
