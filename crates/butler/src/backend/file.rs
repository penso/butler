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
    fs, io,
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde_json::Value;

use super::{Backend, record_failure};
use crate::{Job, JobId, JobState, Result, job::DEFAULT_QUEUE};

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

    fn write(&self, state: JobState, job: &Job) -> Result<()> {
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

    fn remove_processing(&self, worker: &str, job: &Job) -> Result<()> {
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

impl Backend for FileQueue {
    fn push(&self, name: &str, queue: &str, args: Vec<Value>) -> Result<JobId> {
        let job = Job::new(name, queue, args);
        self.write(JobState::Pending, &job)?;
        Ok(job.id)
    }

    /// Never blocks: there is nothing to wait on, so the worker sleeps instead.
    fn claim(&self, worker: &str, queues: &[&str], _wait: Duration) -> Result<Option<Job>> {
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

    fn complete(&self, worker: &str, job: &Job) -> Result<()> {
        self.write(JobState::Done, job)?;
        self.remove_processing(worker, job)
    }

    fn fail(
        &self,
        worker: &str,
        mut job: Job,
        error: String,
        max_retries: u32,
    ) -> Result<JobState> {
        let state = record_failure(&mut job, error, max_retries);
        self.write(state, &job)?;
        self.remove_processing(worker, &job)?;
        Ok(state)
    }

    fn get(&self, id: &str) -> Result<Option<(JobState, Job)>> {
        let Some((state, path)) = self.find(id)? else {
            return Ok(None);
        };
        Ok(Some((state, serde_json::from_slice(&fs::read(path)?)?)))
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
                    Ok(bytes) => serde_json::from_slice::<Job>(&bytes)?.queue,
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
