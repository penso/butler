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
//! <dir>/ckeys/<key>/<n>         a running job's concurrency slot: "<worker>\n<job id>"
//! <dir>/unique/<key>            the id of the job holding that unique key
//! <dir>/paused/<queue>          a queue workers don't claim from
//! <dir>/slots/<queue>/<n>       a global-limit slot in use: "<worker>\n<job id>"
//! <dir>/recurring/schedules/<key>.json   a recurring schedule workers registered
//! <dir>/recurring/ticks/<key>/<tick ms>  one per tick enqueued, holding its job id
//! ```
//!
//! A pending job with a concurrency key is named `<id>~<key>~<limit>.json`
//! (`<key>` being the key's hash), so a claim can skip jobs whose key is
//! full without reading them. Claiming one first takes one of the slot
//! names `0` to `limit - 1` in `ckeys/<key>/` by hard-linking a file naming
//! the worker (a link fails if the name exists, so each slot has one
//! holder), then renames the job, and gives the slot back if the rename
//! lost. Processing files are always `<id>.json`. The holding worker frees
//! the slot when the job finishes; `recover` frees a stopped worker's.
//!
//! A unique job's push hard-links a file holding its id to `unique/<key>`.
//! If that name exists and the job it names still holds the key, the push
//! stores nothing and returns that id; a lock whose job moved on is removed
//! and the link tried again. That replacement is best effort: two pushes
//! replacing the same stale lock at once could both store their job.
//!
//! Cancelling is a rename from `pending/<queue>/` or `scheduled/` to
//! `cancelled/`, promoting a due job is a rename from `scheduled/` to
//! `pending/<queue>/`, and recovering a crashed worker's job is a rename from
//! its `processing/<worker>/` back to `pending/<queue>/`. Interrupted retry
//! transitions use `<id>.retry.json` in the processing directory and recover
//! to `scheduled/` when they carry a run time. They all race with
//! other renames the way two claims do: exactly one succeeds.
//!
//! A claim under a global queue limit of `max` first takes one of the slot
//! names `0` to `max - 1` in `slots/<queue>/`, by hard-linking a file that
//! already names the worker: a link fails if the name exists, so each slot
//! has one holder. Only then does it claim a job, and it gives the slot back
//! if there was none. The worker that holds the job frees its slot when it
//! completes or fails it; `recover` frees every slot of a stopped worker.
//! After lowering a limit, slots numbered above the new one stay held until
//! their jobs finish.
//!
//! A finished job's file is written (done, dead) or touched (cancelled) when
//! it finishes, so its modification time is when it finished: cleaning up
//! deletes the files of `done/`, `cancelled/` and `dead/` older than the
//! queue's [`Retention`](crate::Retention).
//!
//! A recurring tick is claimed by hard-linking a marker file, already
//! holding the job id, to `recurring/ticks/<key>/<tick>`: a link fails if the
//! name exists, so exactly one worker creates it, and only that worker then
//! renames the job file (written under `tmp/` first) into its queue. A crash
//! between the two steps loses that one tick: the marker says it ran.

use std::{
    collections::{BTreeMap, HashSet},
    ffi::OsString,
    fs, io,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use super::{GlobalLimit, Monitor, NewJob, Promoted, Store, TICK_RETENTION, Watch};
use crate::{
    JobId, JobRecord, JobState, RecurringRecord, Result, Retention,
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
const CKEYS: &str = "ckeys";
const UNIQUE: &str = "unique";

/// How long a unique lock whose job isn't written yet counts as held: its
/// push writes the job right after taking the lock, unless it crashed.
const UNWRITTEN_LOCK_GRACE: Duration = Duration::from_secs(10);
const PAUSED: &str = "paused";
const SLOTS: &str = "slots";
const SCHEDULES: &str = "recurring/schedules";
const TICKS: &str = "recurring/ticks";

#[derive(Debug, Clone)]
pub struct FileQueue {
    root: PathBuf,
    retention: Retention,
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
        Ok(Self {
            root,
            retention: Retention::default(),
        })
    }

    /// Keeps finished jobs for `retention`.
    pub fn retention(mut self, retention: Retention) -> Self {
        self.retention = retention;
        self
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
        match fs::rename(path, dir.join(pending_name(&job))) {
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
            if state == JobState::Pending {
                for queue in fs::read_dir(self.dir(state))? {
                    if let Some(path) = self.pending_file(&queue?.path(), id)? {
                        return Ok(Some((state, path)));
                    }
                }
                continue;
            }
            // One level of subdirectories: per worker.
            if state == JobState::Processing {
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

    /// Job `id`'s file in the pending directory `dir`, keyed name or not.
    fn pending_file(&self, dir: &Path, id: &str) -> Result<Option<PathBuf>> {
        let plain = dir.join(format!("{id}.json"));
        if plain.exists() {
            return Ok(Some(plain));
        }
        let entries = match fs::read_dir(dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let prefix = format!("{id}~");
        for entry in entries {
            let entry = entry?;
            if entry
                .file_name()
                .to_str()
                .is_some_and(|name| name.starts_with(&prefix))
            {
                return Ok(Some(entry.path()));
            }
        }
        Ok(None)
    }

    fn ckey_slots(&self, hash: &str) -> PathBuf {
        self.root.join(CKEYS).join(hash)
    }

    /// Takes a free slot among `0..max` in `dir` for `worker`, or `None` if
    /// every one is in use. The slot names the worker until
    /// [`hold_slot`](Self::hold_slot) adds the job.
    fn take_slot(&self, dir: &Path, max: usize, worker: &str) -> Result<Option<PathBuf>> {
        fs::create_dir_all(dir)?;
        let staged = self.staged("slot".as_ref())?;
        fs::write(&staged, format!("{worker}\n"))?;
        let mut taken = None;
        for n in 0..max {
            let slot = dir.join(n.to_string());
            match fs::hard_link(&staged, &slot) {
                Ok(()) => {
                    taken = Some(slot);
                    break;
                }
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(e) => {
                    ignore_missing(fs::remove_file(&staged))?;
                    return Err(e.into());
                }
            }
        }
        ignore_missing(fs::remove_file(&staged))?;
        Ok(taken)
    }

    /// Records that the job in `slot` is `id`, so finishing it frees it.
    fn hold_slot(&self, slot: &Path, worker: &str, id: &str) -> Result<()> {
        self.write_atomic(slot, format!("{worker}\n{id}").as_bytes())
    }

    /// Frees the slots in `dir` that `holds` accepts, given each one's
    /// worker and job id.
    fn free_slots(&self, dir: &Path, holds: impl Fn(&str, &str) -> bool) -> Result<()> {
        let entries = match fs::read_dir(dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        for entry in entries {
            let path = entry?.path();
            let contents = match fs::read_to_string(&path) {
                Ok(contents) => contents,
                Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e.into()),
            };
            let (worker, id) = contents.split_once('\n').unwrap_or((&contents, ""));
            if holds(worker, id) {
                ignore_missing(fs::remove_file(&path))?;
            }
        }
        Ok(())
    }

    /// Frees the concurrency slot `worker` holds for `job`, if any.
    fn release_key(&self, worker: &str, job: &JobRecord) -> Result<()> {
        match &job.concurrency {
            Some(concurrency) => self.free_slots(&self.ckey_slots(&concurrency.hash()), |w, id| {
                w == worker && id == job.id
            }),
            None => Ok(()),
        }
    }

    fn unique_lock(&self, unique: &crate::UniqueKey) -> PathBuf {
        self.root.join(UNIQUE).join(unique.hash())
    }

    /// Frees `job`'s unique key, if it still holds it.
    fn unlock(&self, job: &JobRecord) -> Result<()> {
        let Some(unique) = &job.unique else {
            return Ok(());
        };
        let lock = self.unique_lock(unique);
        match fs::read_to_string(&lock) {
            Ok(holder) if holder == job.id => ignore_missing(fs::remove_file(lock)),
            Err(e) if e.kind() != io::ErrorKind::NotFound => Err(e.into()),
            _ => Ok(()),
        }
    }

    /// Takes `job`'s unique key, or returns the id of the job holding it.
    fn lock_unique(&self, job: &JobRecord, unique: &crate::UniqueKey) -> Result<Option<JobId>> {
        let lock = self.unique_lock(unique);
        fs::create_dir_all(self.root.join(UNIQUE))?;
        let staged = self.staged("unique".as_ref())?;
        fs::write(&staged, &job.id)?;
        let result = (|| {
            for _ in 0..16 {
                match fs::hard_link(&staged, &lock) {
                    Ok(()) => return Ok(None),
                    Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {}
                    Err(e) => return Err(e.into()),
                }
                let holder = match fs::read_to_string(&lock) {
                    Ok(holder) => holder,
                    Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                    Err(e) => return Err(e.into()),
                };
                let holds = match self.get(&holder)? {
                    Some((state, record)) => record
                        .unique
                        .map_or(unique.until, |key| key.until)
                        .holds_in(state),
                    // Its push took the lock and is still writing the job,
                    // unless it crashed in between long ago.
                    None => fs::metadata(&lock)
                        .and_then(|meta| meta.modified())
                        .ok()
                        .and_then(|at| at.elapsed().ok())
                        .is_none_or(|age| age < UNWRITTEN_LOCK_GRACE),
                };
                if holds {
                    return Ok(Some(holder));
                }
                // Left by a job that moved on: replace it.
                ignore_missing(fs::remove_file(&lock))?;
            }
            Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "unique lock kept changing while enqueueing",
            )
            .into())
        })();
        ignore_missing(fs::remove_file(&staged))?;
        result
    }

    fn slots(&self, queue: &str) -> PathBuf {
        self.root.join(SLOTS).join(queue)
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
                dir.join(pending_name(job))
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
        if let Some(unique) = &job.unique
            && let Some(holder) = self.lock_unique(&job, unique)?
        {
            return Ok(holder);
        }
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

    fn claim(&self, worker: &str, queues: &[&str], wait: Duration) -> Result<Option<JobRecord>> {
        self.claim_within_limits(worker, queues, &[], wait)
    }

    /// Never blocks: there is nothing to wait on, so the worker sleeps instead.
    fn claim_within_limits(
        &self,
        worker: &str,
        queues: &[&str],
        limits: &[GlobalLimit<'_>],
        _wait: Duration,
    ) -> Result<Option<JobRecord>> {
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
            if names.is_empty() {
                continue;
            }
            // Ids start with the enqueue time, so name order is FIFO.
            names.sort();

            // Created first: `recover` finds a stopped worker's slots through
            // its processing directory.
            let processing = self.processing(worker);
            fs::create_dir_all(&processing)?;
            let queue_slot = match GlobalLimit::of(limits, queue) {
                None => None,
                Some(max) => match self.take_slot(&self.slots(queue), max, worker)? {
                    Some(slot) => Some(slot),
                    None => continue,
                },
            };
            // Keys found full during this claim: their jobs are skipped.
            let mut full: HashSet<String> = HashSet::new();
            for name in names {
                let Some(name) = name.to_str() else { continue };
                let Some((id, key)) = parse_pending_name(name) else {
                    continue;
                };
                let key_slot = match key {
                    None => None,
                    Some((hash, limit)) => {
                        if full.contains(hash) {
                            continue;
                        }
                        match self.take_slot(&self.ckey_slots(hash), limit, worker)? {
                            Some(slot) => Some(slot),
                            None => {
                                full.insert(hash.to_owned());
                                continue;
                            }
                        }
                    }
                };
                let to = processing.join(format!("{id}.json"));
                match fs::rename(dir.join(name), &to) {
                    Ok(()) => {
                        let job: JobRecord = serde_json::from_slice(&fs::read(&to)?)?;
                        for slot in queue_slot.iter().chain(&key_slot) {
                            self.hold_slot(slot, worker, &job.id)?;
                        }
                        if job
                            .unique
                            .as_ref()
                            .is_some_and(|unique| unique.until == crate::Unique::UntilStarted)
                        {
                            self.unlock(&job)?;
                        }
                        return Ok(Some(job));
                    }
                    // Another worker claimed it first, or it was cancelled.
                    Err(e) if e.kind() == io::ErrorKind::NotFound => {
                        if let Some(slot) = key_slot {
                            ignore_missing(fs::remove_file(slot))?;
                        }
                    }
                    Err(e) => return Err(e.into()),
                }
            }
            // No job taken from this queue: give its slot back.
            if let Some(slot) = queue_slot {
                ignore_missing(fs::remove_file(slot))?;
            }
        }
        Ok(None)
    }

    fn complete(&self, worker: &str, job: &JobRecord) -> Result<()> {
        self.write(JobState::Done, job)?;
        self.remove_processing(worker, job)?;
        self.free_slots(&self.slots(&job.queue), |holder, id| {
            holder == worker && id == job.id
        })?;
        self.release_key(worker, job)?;
        self.unlock(job)
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
        } else {
            self.write(next, job)?;
            self.remove_processing(worker, job)?;
        }
        self.free_slots(&self.slots(&job.queue), |holder, id| {
            holder == worker && id == job.id
        })?;
        self.release_key(worker, job)?;
        if next == JobState::Dead {
            self.unlock(job)?;
        }
        Ok(())
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
        let cancelled = self.dir(JobState::Cancelled).join(format!("{id}.json"));
        // Scheduled first: promotion only moves jobs from there to pending/,
        // so checking in that order can't miss a job moving in between.
        let mut taken = false;
        if let Some(path) = self.find_scheduled(id)? {
            match fs::rename(path, &cancelled) {
                Ok(()) => taken = true,
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
        }
        if !taken {
            for queue in fs::read_dir(self.dir(JobState::Pending))? {
                let Some(path) = self.pending_file(&queue?.path(), id)? else {
                    continue;
                };
                match fs::rename(path, &cancelled) {
                    Ok(()) => {
                        taken = true;
                        break;
                    }
                    // Claimed or cancelled first.
                    Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                    Err(e) => return Err(e.into()),
                }
            }
        }
        if taken {
            let job: JobRecord = serde_json::from_slice(&fs::read(&cancelled)?)?;
            self.unlock(&job)?;
            // The rename kept the time it was written; cleaning up counts
            // from now.
            match fs::File::options().write(true).open(&cancelled) {
                Ok(file) => file.set_modified(SystemTime::now())?,
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
        }
        Ok(taken)
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
                    dir.join(pending_name(&job))
                };
                match fs::rename(held.path(), to) {
                    Ok(()) => recovered += 1,
                    Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                    Err(e) => return Err(e.into()),
                }
            }
            // Its jobs are back in line: give their concurrency slots back.
            match fs::read_dir(self.root.join(CKEYS)) {
                Ok(keys) => {
                    for key in keys {
                        self.free_slots(&key?.path(), |holder, _| holder == worker)?;
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
            // Its jobs are back in line: give their slots back too, and any
            // it took without getting a job.
            match fs::read_dir(self.root.join(SLOTS)) {
                Ok(queues) => {
                    for queue in queues {
                        if let Some(queue) = queue?.file_name().to_str() {
                            self.free_slots(&self.slots(queue), |holder, _| holder == worker)?;
                        }
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
            let _ = fs::remove_dir(entry.path());
            ignore_missing(fs::remove_file(self.heartbeat_file(&worker)))?;
        }
        Ok(recovered)
    }

    fn paused_queues(&self) -> Result<Vec<String>> {
        let entries = match fs::read_dir(self.root.join(PAUSED)) {
            Ok(entries) => entries,
            // Nothing was ever paused.
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e.into()),
        };
        let mut paused = Vec::new();
        for entry in entries {
            if let Some(queue) = entry?.file_name().to_str() {
                paused.push(queue.to_owned());
            }
        }
        paused.sort();
        Ok(paused)
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

    fn clean_finished(&self, now: SystemTime, limit: usize) -> Result<usize> {
        let mut deleted = 0;
        for (state, keep) in [
            (JobState::Done, self.retention.finished),
            (JobState::Cancelled, self.retention.finished),
            (JobState::Dead, self.retention.dead),
        ] {
            let Some(cutoff) = keep.cutoff(now) else {
                continue;
            };
            for entry in fs::read_dir(self.dir(state))? {
                if deleted == limit {
                    return Ok(deleted);
                }
                let entry = entry?;
                let finished_at = match entry.metadata() {
                    Ok(meta) if meta.is_file() => meta.modified()?,
                    Ok(_) => continue,
                    // Discarded or retried meanwhile.
                    Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                    Err(e) => return Err(e.into()),
                };
                if finished_at >= cutoff {
                    continue;
                }
                match fs::remove_file(entry.path()) {
                    Ok(()) => deleted += 1,
                    Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                    Err(e) => return Err(e.into()),
                }
            }
        }
        Ok(deleted)
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

    /// The marker is created exclusively, so of two pauses one reports it.
    fn pause_queue(&self, queue: &str) -> Result<bool> {
        let dir = self.root.join(PAUSED);
        fs::create_dir_all(&dir)?;
        match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(dir.join(queue))
        {
            Ok(_) => Ok(true),
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    fn resume_queue(&self, queue: &str) -> Result<bool> {
        match fs::remove_file(self.root.join(PAUSED).join(queue)) {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e.into()),
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
        crate::recurring::validate_key(key)?;
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

/// A pending job's file name: `<id>.json`, or `<id>~<key hash>~<limit>.json`
/// for a job with a concurrency key.
fn pending_name(job: &JobRecord) -> String {
    match &job.concurrency {
        Some(concurrency) => format!(
            "{}~{}~{}.json",
            job.id,
            concurrency.hash(),
            concurrency.limit
        ),
        None => format!("{}.json", job.id),
    }
}

/// A pending file name's job id, and its concurrency key hash and limit if
/// it has them.
fn parse_pending_name(name: &str) -> Option<(&str, Option<(&str, usize)>)> {
    let stem = name.strip_suffix(".json")?;
    let mut parts = stem.split('~');
    let id = parts.next()?;
    match (parts.next(), parts.next()) {
        (Some(hash), Some(limit)) => Some((id, Some((hash, limit.parse().ok()?)))),
        _ => Some((id, None)),
    }
}

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

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use crate::{Keep, Retention};

    const HOUR: Duration = Duration::from_secs(3600);

    #[test]
    fn a_cancelled_job_is_kept_from_when_it_was_cancelled_not_pushed() {
        let dir = std::env::temp_dir().join(format!(
            "butler-file-cancel-retention-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        let queue = FileQueue::new(&dir).unwrap().retention(Retention {
            finished: Keep::For(HOUR),
            dead: Keep::Forever,
        });
        let id = queue.push(NewJob::new("a", "default", vec![])).unwrap();
        // Pushed two hours ago.
        let file = queue.pending(DEFAULT_QUEUE).join(format!("{id}.json"));
        fs::File::options()
            .write(true)
            .open(&file)
            .unwrap()
            .set_modified(SystemTime::now() - 2 * HOUR)
            .unwrap();
        assert!(queue.cancel(&id).unwrap());

        let soon = SystemTime::now() + HOUR / 2;
        assert_eq!(queue.clean_finished(soon, 10).unwrap(), 0);
        assert_eq!(queue.get(&id).unwrap().unwrap().0, JobState::Cancelled);
        let later = SystemTime::now() + 2 * HOUR;
        assert_eq!(queue.clean_finished(later, 10).unwrap(), 1);
        assert!(queue.get(&id).unwrap().is_none());
        let _ = fs::remove_dir_all(&dir);
    }
}
