//! A file-backed queue. Each job is one JSON file, and moving between state
//! directories with `rename` is atomic. Several workers, even in separate
//! processes, can share the same directory: when two workers try to claim a
//! job, exactly one rename succeeds.
//!
//! ```text
//! <dir>/tmp/         files being written; never read by workers
//! <dir>/pending/     waiting to run (includes jobs waiting for a retry)
//! <dir>/processing/  claimed by a worker
//! <dir>/done/        succeeded
//! <dir>/dead/        failed after exhausting retries
//! ```

use std::{
    fs, io,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::Error;

pub type JobId = String;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub id: JobId,
    pub name: String,
    pub args: Vec<Value>,
    /// Number of failed attempts so far.
    pub attempts: u32,
    pub enqueued_at_ms: u64,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobState {
    Pending,
    Processing,
    Done,
    Dead,
}

impl JobState {
    const ALL: [JobState; 4] = [
        // Checked in this order. During a retry, a job is briefly in both
        // processing/ and pending/, and processing/ wins.
        JobState::Done,
        JobState::Dead,
        JobState::Processing,
        JobState::Pending,
    ];

    fn dir_name(self) -> &'static str {
        match self {
            JobState::Pending => "pending",
            JobState::Processing => "processing",
            JobState::Done => "done",
            JobState::Dead => "dead",
        }
    }
}

#[derive(Debug, Clone)]
pub struct FileQueue {
    root: PathBuf,
}

impl FileQueue {
    pub fn new(root: impl Into<PathBuf>) -> io::Result<Self> {
        let root = root.into();
        fs::create_dir_all(root.join("tmp"))?;
        for state in JobState::ALL {
            fs::create_dir_all(root.join(state.dir_name()))?;
        }
        Ok(Self { root })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Adds a job to `pending/` and returns its id.
    pub fn push(&self, name: &str, args: Vec<Value>) -> Result<JobId, Error> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
        let job = Job {
            id: new_id(now.as_nanos()),
            name: name.to_string(),
            args,
            attempts: 0,
            enqueued_at_ms: now.as_millis() as u64,
            last_error: None,
        };
        self.write(JobState::Pending, &job)?;
        Ok(job.id)
    }

    /// Claims the oldest pending job by moving it into `processing/`.
    pub fn claim(&self) -> Result<Option<Job>, Error> {
        let mut names: Vec<_> = fs::read_dir(self.dir(JobState::Pending))?
            .filter_map(|e| e.ok())
            .map(|e| e.file_name())
            .collect();
        names.sort();

        for name in names {
            let from = self.dir(JobState::Pending).join(&name);
            let to = self.dir(JobState::Processing).join(&name);
            match fs::rename(&from, &to) {
                Ok(()) => return Ok(Some(serde_json::from_slice(&fs::read(&to)?)?)),
                // Another worker claimed it first.
                Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e.into()),
            }
        }
        Ok(None)
    }

    /// Moves a claimed job to `done/`.
    pub fn complete(&self, job: &Job) -> Result<(), Error> {
        self.write(JobState::Done, job)?;
        self.remove_processing(job)
    }

    /// Records a failure. The job goes back to `pending/` for a retry, or to
    /// `dead/` once it has failed more than `max_retries` times.
    pub fn fail(&self, mut job: Job, error: String, max_retries: u32) -> Result<JobState, Error> {
        job.attempts += 1;
        job.last_error = Some(error);
        let state = if job.attempts > max_retries {
            JobState::Dead
        } else {
            JobState::Pending
        };
        self.write(state, &job)?;
        self.remove_processing(&job)?;
        Ok(state)
    }

    pub fn state(&self, id: &str) -> Option<JobState> {
        self.find(id).map(|(state, _)| state)
    }

    /// Returns a job's current state and data, if the job exists.
    pub fn get(&self, id: &str) -> Result<Option<(JobState, Job)>, Error> {
        let Some((state, path)) = self.find(id) else {
            return Ok(None);
        };
        Ok(Some((state, serde_json::from_slice(&fs::read(path)?)?)))
    }

    fn find(&self, id: &str) -> Option<(JobState, PathBuf)> {
        JobState::ALL.into_iter().find_map(|state| {
            let path = self.dir(state).join(format!("{id}.json"));
            path.exists().then_some((state, path))
        })
    }

    fn dir(&self, state: JobState) -> PathBuf {
        self.root.join(state.dir_name())
    }

    /// Writes the file under `tmp/` and then renames it, so workers never see
    /// a half-written job.
    fn write(&self, state: JobState, job: &Job) -> Result<(), Error> {
        let file = format!("{}.json", job.id);
        let tmp = self.root.join("tmp").join(&file);
        fs::write(&tmp, serde_json::to_vec_pretty(job)?)?;
        fs::rename(&tmp, self.dir(state).join(&file))?;
        Ok(())
    }

    fn remove_processing(&self, job: &Job) -> Result<(), Error> {
        match fs::remove_file(self.dir(JobState::Processing).join(format!("{}.json", job.id))) {
            Err(e) if e.kind() != io::ErrorKind::NotFound => Err(e.into()),
            _ => Ok(()),
        }
    }
}

/// Builds an id that sorts in enqueue order and stays unique across threads
/// and processes.
fn new_id(nanos: u128) -> JobId {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    format!("{nanos:020}-{}-{seq}", std::process::id())
}
