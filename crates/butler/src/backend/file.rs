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
};

use serde_json::Value;

use super::{Backend, record_failure};
use crate::{Job, JobId, JobState, Result};

/// Checked in this order. During a retry, a job is briefly in both
/// `processing/` and `pending/`, and `processing/` wins.
const LOOKUP_ORDER: [JobState; 4] = [
    JobState::Done,
    JobState::Dead,
    JobState::Processing,
    JobState::Pending,
];

#[derive(Debug, Clone)]
pub struct FileQueue {
    root: PathBuf,
}

impl FileQueue {
    pub fn new(root: impl Into<PathBuf>) -> io::Result<Self> {
        let root = root.into();
        fs::create_dir_all(root.join("tmp"))?;
        for state in LOOKUP_ORDER {
            fs::create_dir_all(root.join(state.as_str()))?;
        }
        Ok(Self { root })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    fn dir(&self, state: JobState) -> PathBuf {
        self.root.join(state.as_str())
    }

    fn find(&self, id: &str) -> Option<(JobState, PathBuf)> {
        LOOKUP_ORDER.into_iter().find_map(|state| {
            let path = self.dir(state).join(format!("{id}.json"));
            path.exists().then_some((state, path))
        })
    }

    /// Writes the file under `tmp/` and then renames it, so workers never see
    /// a half-written job.
    fn write(&self, state: JobState, job: &Job) -> Result<()> {
        let file = format!("{}.json", job.id);
        let tmp = self.root.join("tmp").join(&file);
        fs::write(&tmp, serde_json::to_vec_pretty(job)?)?;
        fs::rename(&tmp, self.dir(state).join(&file))?;
        Ok(())
    }

    fn remove_processing(&self, job: &Job) -> Result<()> {
        match fs::remove_file(
            self.dir(JobState::Processing)
                .join(format!("{}.json", job.id)),
        ) {
            Err(e) if e.kind() != io::ErrorKind::NotFound => Err(e.into()),
            _ => Ok(()),
        }
    }
}

impl Backend for FileQueue {
    fn push(&self, name: &str, args: Vec<Value>) -> Result<JobId> {
        let job = Job::new(name, args);
        self.write(JobState::Pending, &job)?;
        Ok(job.id)
    }

    fn claim(&self) -> Result<Option<Job>> {
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

    fn complete(&self, job: &Job) -> Result<()> {
        self.write(JobState::Done, job)?;
        self.remove_processing(job)
    }

    fn fail(&self, mut job: Job, error: String, max_retries: u32) -> Result<JobState> {
        let state = record_failure(&mut job, error, max_retries);
        self.write(state, &job)?;
        self.remove_processing(&job)?;
        Ok(state)
    }

    fn get(&self, id: &str) -> Result<Option<(JobState, Job)>> {
        let Some((state, path)) = self.find(id) else {
            return Ok(None);
        };
        Ok(Some((state, serde_json::from_slice(&fs::read(path)?)?)))
    }

    fn describe(&self) -> String {
        format!("file:{}", self.root.display())
    }
}
