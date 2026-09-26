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
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock, PoisonError, Weak},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use rusqlite::{Connection, OptionalExtension, params};
use serde_json::Value;

use super::{Backend, NewJob};
use crate::{Error, JobId, JobRecord, JobState, Result, Signal, signal::JobWatch};

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
";

pub struct SqliteQueue {
    path: PathBuf,
    /// One connection per queue: every statement is short, and waiting
    /// happens on `signals`, never while holding it.
    conn: Mutex<Connection>,
    signals: Arc<Signals>,
    /// The cross-process watcher thread, started by the first waiter.
    watcher: OnceLock<()>,
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

fn now_ms() -> i64 {
    let ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    i64::try_from(ms).unwrap_or(i64::MAX)
}

impl Backend for SqliteQueue {
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

    fn watch_finished(&self, id: &str) -> Option<Arc<Signal>> {
        self.watch_other_processes();
        Some(self.signals.finished.watch(id))
    }

    fn describe(&self) -> String {
        format!("sqlite:{}", self.path.display())
    }
}
