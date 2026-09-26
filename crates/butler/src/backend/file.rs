//! A file-backed queue. Each job is one JSON file, and moving between state
//! directories with `rename` is atomic. Several workers, even in separate
//! processes, can share the same directory: when two workers try to claim a
//! job, exactly one rename succeeds.
//!
//! ```text
//! <dir>/tmp/                   files being written; never read by workers
//! <dir>/pending/<queue>/       waiting to run (includes jobs waiting for a retry)
//! <dir>/processing/<worker>/   claimed by that worker
//! <dir>/workers/<worker>       that worker's heartbeat: when it expires, in ms
//! <dir>/done/                  succeeded
//! <dir>/dead/                  failed after exhausting retries
//! <dir>/cancelled/             removed from pending/ before a worker claimed it
//! ```
//!
//! Cancelling is a rename from `pending/<queue>/` to `cancelled/`, and
//! recovering a crashed worker's job is a rename from its
//! `processing/<worker>/` back to `pending/<queue>/`. Both race with other renames the way two claims do: exactly
//! one succeeds.

use std::{
    collections::BTreeMap,
    fs, io,
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde_json::Value;

use super::{Monitor, Store, Watch};
use crate::{
    JobId, JobRecord, JobState, Result,
    job::DEFAULT_QUEUE,
    monitor::{ListFilter, QueueStats, Stats, WorkerStats},
};

/// Checked in this order. During a retry, a job is briefly in both
/// `processing/` and `pending/`, and `processing/` wins.
const LOOKUP_ORDER: [JobState; 5] = [
    JobState::Cancelled,
    JobState::Done,
    JobState::Dead,
    JobState::Processing,
    JobState::Pending,
];

const WORKERS: &str = "workers";

#[derive(Debug, Clone)]
pub struct FileQueue {
    root: PathBuf,
}

impl FileQueue {
    pub fn new(root: impl Into<PathBuf>) -> io::Result<Self> {
        let root = root.into();
        for dir in ["tmp", WORKERS] {
            fs::create_dir_all(root.join(dir))?;
        }
        for state in LOOKUP_ORDER {
            fs::create_dir_all(root.join(state.as_str()))?;
        }
        fs::create_dir_all(root.join(JobState::Pending.as_str()).join(DEFAULT_QUEUE))?;
        Ok(Self { root })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    fn dir(&self, state: JobState) -> PathBuf {
        self.root.join(state.as_str())
    }

    fn pending(&self, queue: &str) -> PathBuf {
        self.dir(JobState::Pending).join(queue)
    }

    fn processing(&self, worker: &str) -> PathBuf {
        self.dir(JobState::Processing).join(worker)
    }

    /// Subdirectories of a state's directory: queues, or workers.
    fn subdirs(&self, state: JobState) -> Result<Vec<String>> {
        let mut names = Vec::new();
        for entry in fs::read_dir(self.dir(state))? {
            let entry = entry?;
            if entry.file_type()?.is_dir()
                && let Some(name) = entry.file_name().to_str()
            {
                names.push(name.to_owned());
            }
        }
        Ok(names)
    }

    /// The job files in `dir`; none if it doesn't exist.
    fn job_files(&self, dir: &Path) -> Result<Vec<PathBuf>> {
        let entries = match fs::read_dir(dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e.into()),
        };
        let mut files = Vec::new();
        for entry in entries {
            let path = entry?.path();
            if path.extension().is_some_and(|ext| ext == "json") {
                files.push(path);
            }
        }
        Ok(files)
    }

    fn heartbeat_file(&self, worker: &str) -> PathBuf {
        self.root.join(WORKERS).join(worker)
    }

    fn find(&self, id: &str) -> Result<Option<(JobState, PathBuf)>> {
        let file = format!("{id}.json");
        for state in LOOKUP_ORDER {
            // One level of subdirectories: per worker, or per queue.
            if matches!(state, JobState::Processing | JobState::Pending) {
                for sub in fs::read_dir(self.dir(state))? {
                    let path = sub?.path().join(&file);
                    if path.exists() {
                        return Ok(Some((state, path)));
                    }
                }
                continue;
            }
            let path = self.dir(state).join(&file);
            if path.exists() {
                return Ok(Some((state, path)));
            }
        }
        Ok(None)
    }

    /// Writes `contents` under `tmp/` and then renames it to `to`, so readers
    /// never see a half-written file.
    fn write_atomic(&self, to: &Path, contents: &[u8]) -> Result<()> {
        let name = to.file_name().unwrap_or_default();
        let tmp = self.root.join("tmp").join(name);
        fs::write(&tmp, contents)?;
        fs::rename(&tmp, to)?;
        Ok(())
    }

    fn write(&self, state: JobState, job: &JobRecord) -> Result<()> {
        let dir = if state == JobState::Pending {
            let dir = self.pending(&job.queue);
            fs::create_dir_all(&dir)?;
            dir
        } else {
            self.dir(state)
        };
        let to = dir.join(format!("{}.json", job.id));
        self.write_atomic(&to, &serde_json::to_vec_pretty(job)?)
    }

    fn remove_processing(&self, worker: &str, job: &JobRecord) -> Result<()> {
        ignore_missing(fs::remove_file(
            self.processing(worker).join(format!("{}.json", job.id)),
        ))
    }

    /// Whether `worker`'s heartbeat file names a time still in the future.
    fn is_alive(&self, worker: &str, now_ms: u128) -> Result<bool> {
        match fs::read_to_string(self.heartbeat_file(worker)) {
            Ok(deadline) => Ok(deadline.trim().parse::<u128>().is_ok_and(|d| d > now_ms)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e.into()),
        }
    }
}

impl Store for FileQueue {
    fn push(&self, name: &str, queue: &str, args: Vec<Value>) -> Result<JobId> {
        let job = JobRecord::new(name, queue, args);
        self.write(JobState::Pending, &job)?;
        Ok(job.id)
    }

    /// Never blocks: there is nothing to wait on, so the worker sleeps instead.
    fn claim(&self, worker: &str, queues: &[&str], _wait: Duration) -> Result<Option<JobRecord>> {
        for queue in queues {
            let dir = self.pending(queue);
            let mut names: Vec<_> = match fs::read_dir(&dir) {
                Ok(entries) => entries
                    .filter_map(|e| e.ok())
                    .map(|e| e.file_name())
                    .collect(),
                // Nothing was ever enqueued on this queue.
                Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e.into()),
            };
            // Ids start with the enqueue time, so name order is FIFO.
            names.sort();

            let processing = self.processing(worker);
            fs::create_dir_all(&processing)?;
            for name in names {
                let to = processing.join(&name);
                match fs::rename(dir.join(&name), &to) {
                    Ok(()) => return Ok(Some(serde_json::from_slice(&fs::read(&to)?)?)),
                    // Another worker claimed it first, or it was cancelled.
                    Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                    Err(e) => return Err(e.into()),
                }
            }
        }
        Ok(None)
    }

    fn complete(&self, worker: &str, job: &JobRecord) -> Result<()> {
        self.write(JobState::Done, job)?;
        self.remove_processing(worker, job)
    }

    fn fail(&self, worker: &str, job: &JobRecord, next: JobState) -> Result<()> {
        self.write(next, job)?;
        self.remove_processing(worker, job)
    }

    fn get(&self, id: &str) -> Result<Option<(JobState, JobRecord)>> {
        let Some((state, path)) = self.find(id)? else {
            return Ok(None);
        };
        Ok(Some((state, serde_json::from_slice(&fs::read(path)?)?)))
    }

    fn checkpoint(&self, worker: &str, job: &JobRecord) -> Result<()> {
        let path = self.processing(worker).join(format!("{}.json", job.id));
        if path.exists() {
            self.write_atomic(&path, &serde_json::to_vec_pretty(job)?)?;
        }
        Ok(())
    }

    fn cancel(&self, id: &str) -> Result<bool> {
        let file = format!("{id}.json");
        for queue in fs::read_dir(self.dir(JobState::Pending))? {
            match fs::rename(
                queue?.path().join(&file),
                self.dir(JobState::Cancelled).join(&file),
            ) {
                Ok(()) => return Ok(true),
                // Not in this queue, or claimed or cancelled first.
                Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e.into()),
            }
        }
        Ok(false)
    }

    fn heartbeat(&self, worker: &str, ttl: Duration) -> Result<()> {
        let deadline = now_ms() + ttl.as_millis();
        // The heartbeat goes first: a processing directory without a live
        // heartbeat is what `recover` treats as abandoned.
        self.write_atomic(
            &self.heartbeat_file(worker),
            deadline.to_string().as_bytes(),
        )?;
        fs::create_dir_all(self.processing(worker))?;
        Ok(())
    }

    fn retire(&self, worker: &str) -> Result<()> {
        ignore_missing(fs::remove_file(self.heartbeat_file(worker)))?;
        // Only succeeds when empty; anything left there is for `recover`.
        let _ = fs::remove_dir(self.processing(worker));
        Ok(())
    }

    fn recover(&self) -> Result<usize> {
        let now = now_ms();
        let mut recovered = 0;
        for entry in fs::read_dir(self.dir(JobState::Processing))? {
            let entry = entry?;
            let Some(worker) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            if !entry.file_type()?.is_dir() || self.is_alive(&worker, now)? {
                continue;
            }
            for held in fs::read_dir(entry.path())? {
                let held = held?;
                let queue = match fs::read(held.path()) {
                    Ok(bytes) => serde_json::from_slice::<JobRecord>(&bytes)?.queue,
                    // Another recover moved it first.
                    Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                    Err(e) => return Err(e.into()),
                };
                let dir = self.pending(&queue);
                fs::create_dir_all(&dir)?;
                match fs::rename(held.path(), dir.join(held.file_name())) {
                    Ok(()) => recovered += 1,
                    Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                    Err(e) => return Err(e.into()),
                }
            }
            let _ = fs::remove_dir(entry.path());
            ignore_missing(fs::remove_file(self.heartbeat_file(&worker)))?;
        }
        Ok(recovered)
    }

    fn describe(&self) -> String {
        format!("file:{}", self.root.display())
    }
}

impl Monitor for FileQueue {
    fn stats(&self) -> Result<Stats> {
        let mut stats = Stats::default();
        for queue in self.subdirs(JobState::Pending)? {
            let pending = self.job_files(&self.pending(&queue))?.len() as u64;
            stats.queues.push(QueueStats {
                name: queue,
                pending,
            });
        }
        stats.queues.sort_by(|a, b| a.name.cmp(&b.name));
        stats.dead = self.job_files(&self.dir(JobState::Dead))?.len() as u64;
        stats.done = self.job_files(&self.dir(JobState::Done))?.len() as u64;
        stats.cancelled = self.job_files(&self.dir(JobState::Cancelled))?.len() as u64;

        let now = now_ms();
        let mut workers: BTreeMap<String, WorkerStats> = BTreeMap::new();
        for entry in fs::read_dir(self.root.join(WORKERS))? {
            let entry = entry?;
            let Some(id) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            let expires_at = fs::read_to_string(entry.path())
                .ok()
                .and_then(|deadline| deadline.trim().parse::<u128>().ok())
                .unwrap_or(0);
            let expires_in_ms = i64::try_from(expires_at).unwrap_or(i64::MAX)
                - i64::try_from(now).unwrap_or(i64::MAX);
            workers.insert(
                id.clone(),
                WorkerStats {
                    id,
                    running: 0,
                    expires_in_ms,
                },
            );
        }
        for worker in self.subdirs(JobState::Processing)? {
            let running = self.job_files(&self.processing(&worker))?.len() as u64;
            stats.processing += running;
            workers
                .entry(worker.clone())
                .or_insert_with(|| WorkerStats {
                    id: worker,
                    running: 0,
                    expires_in_ms: -1,
                })
                .running = running;
        }
        stats.workers = workers.into_values().collect();
        Ok(stats)
    }

    fn list(&self, filter: &ListFilter) -> Result<Vec<JobRecord>> {
        let dirs: Vec<PathBuf> = match filter.state {
            JobState::Pending => match &filter.queue {
                Some(queue) => vec![self.pending(queue)],
                None => self
                    .subdirs(JobState::Pending)?
                    .iter()
                    .map(|queue| self.pending(queue))
                    .collect(),
            },
            JobState::Processing => self
                .subdirs(JobState::Processing)?
                .iter()
                .map(|worker| self.processing(worker))
                .collect(),
            state => vec![self.dir(state)],
        };
        let mut files = Vec::new();
        for dir in dirs {
            files.extend(self.job_files(&dir)?);
        }
        // File names start with the enqueue time.
        files.sort_by(|a, b| a.file_name().cmp(&b.file_name()));
        if filter.state.is_finished() {
            files.reverse();
        }
        let mut page = Vec::new();
        let mut skipped = 0;
        for path in files {
            let job: JobRecord = match fs::read(&path) {
                Ok(bytes) => serde_json::from_slice(&bytes)?,
                // Moved on while we were listing.
                Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e.into()),
            };
            if filter
                .queue
                .as_deref()
                .is_some_and(|queue| queue != job.queue)
            {
                continue;
            }
            if skipped < filter.offset {
                skipped += 1;
                continue;
            }
            page.push(job);
            if page.len() == filter.limit {
                break;
            }
        }
        Ok(page)
    }

    fn retry(&self, id: &str) -> Result<bool> {
        let file = format!("{id}.json");
        // Taking it out of dead/ first makes this retry the only one moving it.
        let taken = self.root.join("tmp").join(format!("retry-{file}"));
        match fs::rename(self.dir(JobState::Dead).join(&file), &taken) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(e) => return Err(e.into()),
        }
        let mut job: JobRecord = serde_json::from_slice(&fs::read(&taken)?)?;
        job.attempts = 0;
        self.write(JobState::Pending, &job)?;
        ignore_missing(fs::remove_file(taken))?;
        Ok(true)
    }

    fn discard(&self, id: &str) -> Result<bool> {
        let file = format!("{id}.json");
        for state in [JobState::Done, JobState::Dead, JobState::Cancelled] {
            match fs::remove_file(self.dir(state).join(&file)) {
                Ok(()) => return Ok(true),
                Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e.into()),
            }
        }
        Ok(false)
    }
}

impl Watch for FileQueue {}

fn ignore_missing(result: io::Result<()>) -> Result<()> {
    match result {
        Err(e) if e.kind() != io::ErrorKind::NotFound => Err(e.into()),
        _ => Ok(()),
    }
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}
