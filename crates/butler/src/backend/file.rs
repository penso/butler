//! A file-backed queue. Each job is one JSON file, and moving between state
//! directories with `rename` is atomic. Several workers, even in separate
//! processes, can share the same directory: when two workers try to claim a
//! job, exactly one rename succeeds.
//!
//! ```text
//! <dir>/tmp/                   files being written; never read by workers
//! <dir>/pending/<queue>/       waiting to run
//! <dir>/scheduled/             waiting for their run time (retries included),
//!                              named `<run time in ms>_<id>.json`, so name order is run order
//! <dir>/processing/<worker>/   claimed by that worker
//! <dir>/workers/<worker>       that worker's heartbeat: when it expires, in ms
//! <dir>/done/                  succeeded
//! <dir>/dead/                  failed after exhausting retries
//! <dir>/cancelled/             removed from pending/ before a worker claimed it
//! <dir>/recurring/schedules/<key>.json   a recurring schedule workers registered
//! <dir>/recurring/ticks/<key>/<tick ms>  one per tick enqueued, holding its job id
//! ```
//!
//! Cancelling is a rename from `pending/<queue>/` or `scheduled/` to
//! `cancelled/`, promoting a due job is a rename from `scheduled/` to
//! `pending/<queue>/`, and recovering a crashed worker's job is a rename from
//! its `processing/<worker>/` back to `pending/<queue>/`. Interrupted retry
//! transitions use `<id>.retry.json` in the processing directory and recover
//! to `scheduled/` when they carry a run time. They all race with
//! other renames the way two claims do: exactly one succeeds.
//!
//! A recurring tick is claimed by hard-linking a marker file, already
//! holding the job id, to `recurring/ticks/<key>/<tick>`: a link fails if the
//! name exists, so exactly one worker creates it, and only that worker then
//! renames the job file (written under `tmp/` first) into its queue. A crash
//! between the two steps loses that one tick: the marker says it ran.

use std::{
    collections::BTreeMap,
    ffi::OsString,
    fs, io,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use super::{Monitor, NewJob, Promoted, Store, TICK_RETENTION, Watch};
use crate::{
    JobId, JobRecord, JobState, RecurringRecord, Result,
    job::{DEFAULT_QUEUE, from_millis, millis},
    monitor::{ListFilter, QueueStats, Stats, WorkerStats},
};

/// Checked in this order. During a retry, a job is briefly in both
/// `processing/` and `pending/`, and `processing/` wins.
const LOOKUP_ORDER: [JobState; 6] = [
    JobState::Cancelled,
    JobState::Done,
    JobState::Dead,
    JobState::Processing,
    JobState::Scheduled,
    JobState::Pending,
];

const WORKERS: &str = "workers";
const SCHEDULES: &str = "recurring/schedules";
const TICKS: &str = "recurring/ticks";

#[derive(Debug, Clone)]
pub struct FileQueue {
    root: PathBuf,
}

impl FileQueue {
    pub fn new(root: impl Into<PathBuf>) -> io::Result<Self> {
        let root = root.into();
        for dir in ["tmp", WORKERS, SCHEDULES, TICKS] {
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

    /// The file of scheduled job `id`, if it is scheduled.
    fn find_scheduled(&self, id: &str) -> Result<Option<PathBuf>> {
        let suffix = format!("_{id}.json");
        for entry in fs::read_dir(self.dir(JobState::Scheduled))? {
            let entry = entry?;
            if entry
                .file_name()
                .to_str()
                .is_some_and(|name| name.ends_with(&suffix))
            {
                return Ok(Some(entry.path()));
            }
        }
        Ok(None)
    }

    /// Scheduled job files with their run times, soonest first.
    fn scheduled_files(&self) -> Result<Vec<(u64, OsString)>> {
        let mut files = Vec::new();
        for entry in fs::read_dir(self.dir(JobState::Scheduled))? {
            let name = entry?.file_name();
            if let Some(at) = name.to_str().and_then(scheduled_run_at) {
                files.push((at, name));
            }
        }
        files.sort();
        Ok(files)
    }

    /// Moves a scheduled job's file onto its queue. `Ok(false)` if another
    /// rename (a cancel, another promotion) took it first.
    fn enqueue_scheduled(&self, path: &Path) -> Result<bool> {
        let job: JobRecord = match fs::read(path) {
            Ok(bytes) => serde_json::from_slice(&bytes)?,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(e) => return Err(e.into()),
        };
        let dir = self.pending(&job.queue);
        fs::create_dir_all(&dir)?;
        match fs::rename(path, dir.join(format!("{}.json", job.id))) {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    fn find(&self, id: &str) -> Result<Option<(JobState, PathBuf)>> {
        let file = format!("{id}.json");
        for state in LOOKUP_ORDER {
            if state == JobState::Scheduled {
                if let Some(path) = self.find_scheduled(id)? {
                    return Ok(Some((state, path)));
                }
                continue;
            }
            // One level of subdirectories: per worker, or per queue.
            if matches!(state, JobState::Processing | JobState::Pending) {
                for sub in fs::read_dir(self.dir(state))? {
                    let path = sub?.path().join(&file);
                    if path.exists() {
                        return Ok(Some((state, path)));
                    }
                    if state == JobState::Processing {
                        let retry = path.with_file_name(format!("{id}.retry.json"));
                        if retry.exists() {
                            return Ok(Some((state, retry)));
                        }
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
        let tmp = self.staged(to.file_name().unwrap_or_default())?;
        fs::write(&tmp, contents)?;
        fs::rename(&tmp, to)?;
        Ok(())
    }

    /// A path under `tmp/` no other writer uses, even one writing a file of
    /// the same name at the same time, such as another process registering
    /// the same recurring schedule.
    fn staged(&self, name: &std::ffi::OsStr) -> Result<PathBuf> {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let mut unique = name.to_owned();
        unique.push(format!(
            ".{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        Ok(self.root.join("tmp").join(unique))
    }

    fn schedule_path(&self, key: &str) -> PathBuf {
        self.root.join(SCHEDULES).join(format!("{key}.json"))
    }

    fn ticks_dir(&self, key: &str) -> PathBuf {
        self.root.join(TICKS).join(key)
    }

    /// The tick markers of schedule `key`: `(tick ms, file name)`, oldest
    /// first.
    fn ticks(&self, key: &str) -> Result<Vec<(u64, OsString)>> {
        let entries = match fs::read_dir(self.ticks_dir(key)) {
            Ok(entries) => entries,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e.into()),
        };
        let mut ticks = Vec::new();
        for entry in entries {
            let name = entry?.file_name();
            if let Some(tick) = name.to_str().and_then(|name| name.parse().ok()) {
                ticks.push((tick, name));
            }
        }
        ticks.sort();
        Ok(ticks)
    }

    /// Schedule `key` as stored, with its last run from its newest tick
    /// marker. `None` if it isn't registered.
    fn read_schedule(&self, key: &str) -> Result<Option<RecurringRecord>> {
        let mut schedule: RecurringRecord = match fs::read(self.schedule_path(key)) {
            Ok(bytes) => serde_json::from_slice(&bytes)?,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        schedule.last_tick_ms = None;
        schedule.last_job_id = None;
        if let Some((tick, name)) = self.ticks(key)?.pop() {
            match fs::read_to_string(self.ticks_dir(key).join(name)) {
                Ok(job) => schedule.record_run(tick, job.trim()),
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
        }
        Ok(Some(schedule))
    }

    fn scheduled_path(&self, job: &JobRecord) -> PathBuf {
        self.dir(JobState::Scheduled).join(format!(
            "{:020}_{}.json",
            job.run_at_ms.unwrap_or_default(),
            job.id
        ))
    }

    fn write(&self, state: JobState, job: &JobRecord) -> Result<()> {
        let to = match state {
            JobState::Pending => {
                let dir = self.pending(&job.queue);
                fs::create_dir_all(&dir)?;
                dir.join(format!("{}.json", job.id))
            }
            JobState::Scheduled => self.scheduled_path(job),
            _ => self.dir(state).join(format!("{}.json", job.id)),
        };
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
    fn push(&self, job: NewJob) -> Result<JobId> {
        let job = job.into_record();
        let state = match job.run_at_ms {
            Some(_) => JobState::Scheduled,
            None => JobState::Pending,
        };
        self.write(state, &job)?;
        Ok(job.id)
    }

    fn promote(&self, now: SystemTime) -> Result<Promoted> {
        let now = millis(now);
        let mut promoted = Promoted::default();
        let dir = self.dir(JobState::Scheduled);
        for (at, name) in self.scheduled_files()? {
            if at > now {
                promoted.next = Some(from_millis(at));
                break;
            }
            if self.enqueue_scheduled(&dir.join(name))? {
                promoted.moved += 1;
            }
        }
        Ok(promoted)
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
        if next == JobState::Scheduled {
            // Keep exactly one recoverable copy. The first rename records the
            // transition intent; recovery can finish it at either later step.
            let held = self.processing(worker).join(format!("{}.json", job.id));
            let retry = held.with_file_name(format!("{}.retry.json", job.id));
            fs::rename(held, &retry)?;
            self.write_atomic(&retry, &serde_json::to_vec_pretty(job)?)?;
            fs::rename(&retry, self.scheduled_path(job))?;
            return Ok(());
        }
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
        // Scheduled first: promotion only moves jobs from there to pending/,
        // so checking in that order can't miss a job moving in between.
        if let Some(path) = self.find_scheduled(id)? {
            match fs::rename(path, self.dir(JobState::Cancelled).join(&file)) {
                Ok(()) => return Ok(true),
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
        }
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
                let job = match fs::read(held.path()) {
                    Ok(bytes) => serde_json::from_slice::<JobRecord>(&bytes)?,
                    // Another recover moved it first.
                    Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                    Err(e) => return Err(e.into()),
                };
                let to = if held.file_name().to_string_lossy().ends_with(".retry.json")
                    && job.run_at_ms.is_some()
                {
                    self.scheduled_path(&job)
                } else {
                    let dir = self.pending(&job.queue);
                    fs::create_dir_all(&dir)?;
                    dir.join(format!("{}.json", job.id))
                };
                match fs::rename(held.path(), to) {
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

    fn register_recurring(&self, schedules: &[RecurringRecord]) -> Result<Vec<RecurringRecord>> {
        fs::create_dir_all(self.root.join(SCHEDULES))?;
        let mut stored = Vec::with_capacity(schedules.len());
        for schedule in schedules {
            let mut schedule = schedule.clone();
            if let Some(existing) = self.read_schedule(&schedule.key)? {
                schedule.merge_stored(&existing);
            }
            let mut written = schedule.clone();
            // The last run lives in the tick markers, not here.
            written.last_tick_ms = None;
            written.last_job_id = None;
            self.write_atomic(
                &self.schedule_path(&schedule.key),
                &serde_json::to_vec_pretty(&written)?,
            )?;
            stored.push(schedule);
        }
        Ok(stored)
    }

    fn push_recurring(&self, key: &str, tick: SystemTime, job: NewJob) -> Result<Option<JobId>> {
        let job = job.into_record();
        let tick = millis(tick);
        let dir = self.ticks_dir(key);
        fs::create_dir_all(&dir)?;
        // Both written where no worker looks, before the tick is claimed.
        let file = format!("{}.json", job.id);
        let staged_job = self.staged(file.as_ref())?;
        fs::write(&staged_job, serde_json::to_vec_pretty(&job)?)?;
        let staged_marker = self.staged(format!("{key}-{tick}").as_ref())?;
        fs::write(&staged_marker, &job.id)?;
        let claimed = fs::hard_link(&staged_marker, dir.join(format!("{tick:020}")));
        ignore_missing(fs::remove_file(&staged_marker))?;
        match claimed {
            Ok(()) => {}
            Err(e) => {
                ignore_missing(fs::remove_file(&staged_job))?;
                if e.kind() == io::ErrorKind::AlreadyExists {
                    return Ok(None);
                }
                return Err(e.into());
            }
        }
        let queue = self.pending(&job.queue);
        fs::create_dir_all(&queue)?;
        fs::rename(&staged_job, queue.join(file))?;
        let oldest = tick.saturating_sub(u64::try_from(TICK_RETENTION.as_millis()).unwrap_or(0));
        for (old, name) in self.ticks(key)? {
            if old >= oldest {
                break;
            }
            ignore_missing(fs::remove_file(dir.join(name)))?;
        }
        Ok(Some(job.id))
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
        stats.scheduled = self.job_files(&self.dir(JobState::Scheduled))?.len() as u64;
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
        // File names start with the enqueue time (the run time, if scheduled).
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

    fn run_now(&self, id: &str) -> Result<bool> {
        match self.find_scheduled(id)? {
            Some(path) => self.enqueue_scheduled(&path),
            None => Ok(false),
        }
    }

    fn recurring(&self) -> Result<Vec<RecurringRecord>> {
        let mut keys = Vec::new();
        for entry in fs::read_dir(self.root.join(SCHEDULES))? {
            let name = entry?.file_name();
            if let Some(key) = name.to_str().and_then(|name| name.strip_suffix(".json")) {
                keys.push(key.to_owned());
            }
        }
        keys.sort();
        let mut schedules = Vec::with_capacity(keys.len());
        for key in keys {
            // Removed while we were listing: skip it.
            schedules.extend(self.read_schedule(&key)?);
        }
        Ok(schedules)
    }

    fn remove_recurring(&self, key: &str) -> Result<bool> {
        let removed = match fs::remove_file(self.schedule_path(key)) {
            Ok(()) => true,
            Err(e) if e.kind() == io::ErrorKind::NotFound => false,
            Err(e) => return Err(e.into()),
        };
        match fs::remove_dir_all(self.ticks_dir(key)) {
            Err(e) if e.kind() != io::ErrorKind::NotFound => return Err(e.into()),
            _ => {}
        }
        Ok(removed)
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

/// The run time in a scheduled file's name, `<run time in ms>_<id>.json`.
fn scheduled_run_at(name: &str) -> Option<u64> {
    let (at, _) = name.strip_suffix(".json")?.split_once('_')?;
    at.parse().ok()
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
