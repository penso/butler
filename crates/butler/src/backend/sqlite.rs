//! A SQLite-backed queue: one database file that several processes on the same
//! machine can share, with no server to run.
//!
//! ```text
//! butler_jobs     id, queue, state, worker, seq, data (job JSON), finished_seq
//! butler_workers  worker, expires_at_ms (the heartbeat)
//! ```
//!
//! A claim is one `UPDATE ... RETURNING` that moves the oldest pending row of
//! a queue to `processing` under this worker. SQLite runs it under its write
//! lock, so only one worker gets each job. `seq` orders the queue: pushes and
//! retries go to the back, recovered jobs to the front.
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

use rusqlite::{Connection, OptionalExtension, params};
use serde_json::Value;

use super::{Monitor, NewJob, Store, Watch};
use crate::{
    Error, JobId, JobRecord, JobState, Result, Signal,
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
    finished_seq INTEGER
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
";

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

    /// Takes the oldest pending job of the first non-empty queue. Never waits.
    fn sweep(&self, worker: &str, queues: &[&str]) -> Result<Option<JobRecord>> {
        for queue in queues {
            let data: Option<String> = self.with_conn(|conn| {
                conn.query_row(
                    "UPDATE butler_jobs SET state = 'processing', worker = ?1
                     WHERE id = (SELECT id FROM butler_jobs
                                 WHERE state = 'pending' AND queue = ?2
                                 ORDER BY seq LIMIT 1)
                     RETURNING data",
                    params![worker, queue],
                    |row| row.get(0),
                )
                .optional()
            })?;
            if let Some(data) = data {
                return Ok(Some(serde_json::from_str(&data)?));
            }
        }
        Ok(None)
    }
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

/// SQLite integers are signed; counts and durations are never negative.
fn count(value: i64) -> u64 {
    u64::try_from(value).unwrap_or(0)
}

fn now_ms() -> i64 {
    let ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    i64::try_from(ms).unwrap_or(i64::MAX)
}

impl Store for SqliteQueue {
    fn push(&self, name: &str, queue: &str, args: Vec<Value>) -> Result<JobId> {
        let job = JobRecord::new(name, queue, args);
        let data = serde_json::to_string(&job)?;
        self.with_conn(|conn| {
            conn.execute(
                "INSERT INTO butler_jobs (id, queue, state, seq, data)
                 VALUES (?1, ?2, 'pending',
                         (SELECT COALESCE(MAX(seq), 0) + 1 FROM butler_jobs), ?3)",
                params![job.id, queue, data],
            )
        })?;
        self.signals.pushed.notify();
        Ok(job.id)
    }

    /// One transaction for every job: a commit per insert is what makes
    /// SQLite slow.
    fn push_many(&self, jobs: Vec<NewJob>) -> Result<Vec<JobId>> {
        let records = jobs
            .into_iter()
            .map(|new| {
                let job = JobRecord::new(&new.name, &new.queue, new.args);
                let data = serde_json::to_string(&job)?;
                Ok((job.id, new.queue, data))
            })
            .collect::<Result<Vec<_>>>()?;
        let ids = self.with_conn(|conn| {
            let tx = conn.unchecked_transaction()?;
            {
                let mut insert = tx.prepare(
                    "INSERT INTO butler_jobs (id, queue, state, seq, data)
                     VALUES (?1, ?2, 'pending',
                             (SELECT COALESCE(MAX(seq), 0) + 1 FROM butler_jobs), ?3)",
                )?;
                for (id, queue, data) in &records {
                    insert.execute(params![id, queue, data])?;
                }
            }
            tx.commit()?;
            Ok(records.into_iter().map(|(id, _, _)| id).collect())
        })?;
        self.signals.pushed.notify();
        Ok(ids)
    }

    fn claim(&self, worker: &str, queues: &[&str], wait: Duration) -> Result<Option<JobRecord>> {
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
            if let Some(job) = self.sweep(worker, queues)? {
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
                 WHERE id = ?1 AND state = 'pending'",
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
        let order = if filter.state.is_finished() {
            "finished_seq DESC"
        } else {
            "seq ASC"
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
