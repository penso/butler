//! An in-process queue: no files, no server. Enqueuers and workers must share
//! one `MemoryQueue` (clones share state), so it suits tests, and apps that run
//! their workers in the same process. Nothing survives a restart, and finished
//! jobs are kept for their [`Retention`], until a worker cleans them up.
//!
//! It follows the same contract as the other backends: per-worker processing,
//! heartbeats and recovery, atomic claim, cancel and promotion (here, under
//! one lock). A claim with a `wait` sleeps on a condition variable and wakes
//! as soon as a job is pushed. Scheduled jobs wait in a set ordered by run
//! time until they are promoted onto their queue. Recurring ticks are
//! remembered in a set per schedule, checked and filled under the same lock
//! as the push.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque},
    sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock, PoisonError},
    time::{Duration, Instant, SystemTime},
};

use super::{Cleaned, GlobalLimit, Monitor, NewJob, Promoted, Store, TICK_RETENTION, Watch};
use crate::{
    JobId, JobRecord, JobState, RecurringRecord, Result, Retention, Signal,
    job::{from_millis, millis},
    monitor::{
        JobMetric, ListFilter, METRICS_RETENTION_MINUTES, MetricBucket, QueueStats, Stats,
        WorkerStats,
    },
    signal::JobWatch,
};

#[derive(Clone, Default)]
pub struct MemoryQueue {
    inner: Arc<Inner>,
}

#[derive(Default)]
struct Inner {
    state: Mutex<State>,
    pushed: Condvar,
    /// Notified per job when it finishes, for `JobHandle::wait`.
    finished: JobWatch,
}

#[derive(Default)]
struct State {
    /// Per queue, oldest first.
    pending: HashMap<String, VecDeque<JobId>>,
    /// Scheduled jobs by run time (milliseconds since the epoch), soonest
    /// first; the id keeps jobs due at the same time apart.
    scheduled: BTreeSet<(u64, JobId)>,
    jobs: HashMap<JobId, (JobState, JobRecord)>,
    /// Per worker. A set: a worker can hold a great many jobs at once, and
    /// finishing one must not scan the others.
    processing: HashMap<String, HashSet<JobId>>,
    /// Per concurrency key, the jobs running with it.
    running_keys: HashMap<String, HashSet<JobId>>,
    /// Per unique key, the job holding it. Checked against that job's state,
    /// so a lock left by a job that moved on is free.
    unique: HashMap<String, JobId>,
    /// Queues an operator paused.
    paused: BTreeSet<String>,
    /// Per queue with a global limit, the jobs running in one of its slots.
    slots: HashMap<String, HashSet<JobId>>,
    /// When each worker's heartbeat expires.
    heartbeats: HashMap<String, Instant>,
    /// Recurring schedules, by key.
    recurring: BTreeMap<String, RecurringRecord>,
    /// Per recurring schedule, the ticks enqueued (ms), for a day.
    ticks: HashMap<String, BTreeSet<u64>>,
    /// History, per (minute, queue, job).
    metrics: BTreeMap<(u64, String, String), MetricBucket>,
    processed_total: u64,
    failed_total: u64,
    /// The minute history was last pruned, so it's pruned once a minute, not
    /// on every job.
    pruned_at_minute: u64,
    retention: Retention,
    /// When each finished job finished (ms), and the same in finishing
    /// order, done and cancelled jobs apart from dead ones, for cleaning up.
    finished_at: HashMap<JobId, u64>,
    finished: BTreeSet<(u64, JobId)>,
    dead: BTreeSet<(u64, JobId)>,
}

impl MemoryQueue {
    /// A new, empty queue.
    pub fn new() -> Self {
        Self::default()
    }

    /// The queue that `backend = "memory"` in `butler.toml` refers to: one per
    /// process, so an enqueuer and a worker configured separately still meet.
    pub fn shared() -> Self {
        static SHARED: OnceLock<MemoryQueue> = OnceLock::new();
        SHARED.get_or_init(MemoryQueue::new).clone()
    }

    /// Keeps finished jobs for `retention`, for every clone of this queue.
    pub fn retention(self, retention: Retention) -> Self {
        self.lock().retention = retention;
        self
    }

    /// Every operation is a few map updates that cannot panic halfway, so a
    /// poisoned lock still guards consistent state.
    fn lock(&self) -> MutexGuard<'_, State> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

impl State {
    /// Takes `job` out of `worker`'s processing area, and frees its
    /// global-limit slot and its concurrency key. Returns whether anything
    /// was freed, which lets a waiting claim take a job. A worker that no
    /// longer holds the job (it was recovered and claimed again) frees no
    /// slot: the slot is the new claim's.
    fn release(&mut self, worker: &str, job: &JobRecord) -> bool {
        let held = self
            .processing
            .get_mut(worker)
            .is_some_and(|held| held.remove(&job.id));
        let slot = held && self.free_slot(&job.queue, &job.id);
        let key = self.free_key(job);
        slot || key
    }

    fn free_slot(&mut self, queue: &str, id: &str) -> bool {
        self.slots
            .get_mut(queue)
            .is_some_and(|slots| slots.remove(id))
    }

    fn free_key(&mut self, job: &JobRecord) -> bool {
        job.concurrency.as_ref().is_some_and(|concurrency| {
            self.running_keys
                .get_mut(&concurrency.key)
                .is_some_and(|running| running.remove(&job.id))
        })
    }

    /// Records that job `id` finished now, for [`Store::clean_finished`].
    fn finish(&mut self, id: &str, dead: bool) {
        self.unfinish(id);
        let now = millis(SystemTime::now());
        self.finished_at.insert(id.to_owned(), now);
        let order = if dead {
            &mut self.dead
        } else {
            &mut self.finished
        };
        order.insert((now, id.to_owned()));
    }

    /// Forgets when job `id` finished: it was retried or discarded.
    fn unfinish(&mut self, id: &str) {
        if let Some(at) = self.finished_at.remove(id) {
            let entry = (at, id.to_owned());
            self.finished.remove(&entry);
            self.dead.remove(&entry);
        }
    }

    /// Deletes up to `limit` jobs of `order` that finished before `cutoff`.
    fn clean(&mut self, dead: bool, cutoff: u64, limit: usize) -> usize {
        let mut deleted = 0;
        while deleted < limit {
            let order = if dead {
                &mut self.dead
            } else {
                &mut self.finished
            };
            let Some((at, id)) = order.pop_first() else {
                break;
            };
            if at >= cutoff {
                order.insert((at, id));
                break;
            }
            self.jobs.remove(&id);
            self.finished_at.remove(&id);
            deleted += 1;
        }
        deleted
    }

    /// Frees `job`'s unique key, if it still holds it.
    fn unlock(&mut self, job: &JobRecord) {
        if let Some(unique) = &job.unique
            && self.unique.get(&unique.key) == Some(&job.id)
        {
            self.unique.remove(&unique.key);
        }
    }

    /// The job holding `job`'s unique key, if one does.
    fn unique_holder(&self, job: &JobRecord) -> Option<JobId> {
        let unique = job.unique.as_ref()?;
        let holder = self.unique.get(&unique.key)?;
        let (state, _) = self.jobs.get(holder)?;
        unique.until.holds_in(*state).then(|| holder.clone())
    }

    /// Whether `id` may start: its concurrency key, if any, has room.
    fn has_room(&self, id: &str) -> bool {
        let Some((_, job)) = self.jobs.get(id) else {
            return true;
        };
        job.concurrency.as_ref().is_none_or(|concurrency| {
            self.running_keys
                .get(&concurrency.key)
                .map_or(0, HashSet::len)
                < concurrency.limit as usize
        })
    }

    /// Takes the first job of `queue` that may start, skipping those whose
    /// concurrency key is full, and takes its key.
    fn take_next(&mut self, queue: &str) -> Option<JobId> {
        let pending = self.pending.get(queue)?;
        let position = pending.iter().position(|id| self.has_room(id))?;
        let id = self.pending.get_mut(queue)?.remove(position)?;
        if let Some((_, job)) = self.jobs.get(&id) {
            let job = job.clone();
            if let Some(concurrency) = &job.concurrency {
                self.running_keys
                    .entry(concurrency.key.clone())
                    .or_default()
                    .insert(id.clone());
            }
            if job
                .unique
                .as_ref()
                .is_some_and(|unique| unique.until == crate::Unique::UntilStarted)
            {
                self.unlock(&job);
            }
        }
        Some(id)
    }

    /// Stores a new job: pending on its queue, or scheduled if it has a run
    /// time. A unique job whose key is held stores nothing, and gets the
    /// holder's id.
    fn insert_new(&mut self, job: JobRecord) -> JobId {
        if let Some(holder) = self.unique_holder(&job) {
            return holder;
        }
        if let Some(unique) = &job.unique {
            self.unique.insert(unique.key.clone(), job.id.clone());
        }
        if job.run_at_ms.is_some() {
            return self.insert_scheduled(job);
        }
        let id = job.id.clone();
        self.pending
            .entry(job.queue.clone())
            .or_default()
            .push_back(id.clone());
        self.jobs.insert(id.clone(), (JobState::Pending, job));
        id
    }

    fn insert_scheduled(&mut self, job: JobRecord) -> JobId {
        let id = job.id.clone();
        self.scheduled
            .insert((job.run_at_ms.unwrap_or_default(), id.clone()));
        self.jobs.insert(id.clone(), (JobState::Scheduled, job));
        id
    }

    /// Moves a job out of the scheduled set onto the back of its queue.
    fn enqueue_scheduled(&mut self, id: JobId) {
        let Some((job_state, job)) = self.jobs.get_mut(&id) else {
            return;
        };
        *job_state = JobState::Pending;
        let queue = job.queue.clone();
        self.pending.entry(queue).or_default().push_back(id);
    }

    fn next_run_at(&self) -> Option<SystemTime> {
        self.scheduled.first().map(|(at, _)| from_millis(*at))
    }
}

impl Store for MemoryQueue {
    fn push(&self, job: NewJob) -> Result<JobId> {
        let scheduled = job.run_at.is_some();
        let id = self.lock().insert_new(job.into_record());
        // Every waiting claim rechecks: only some of them serve this queue.
        if !scheduled {
            self.inner.pushed.notify_all();
        }
        Ok(id)
    }

    fn push_many(&self, jobs: Vec<NewJob>) -> Result<Vec<JobId>> {
        let mut state = self.lock();
        let ids = jobs
            .into_iter()
            .map(|new| state.insert_new(new.into_record()))
            .collect();
        self.inner.pushed.notify_all();
        Ok(ids)
    }

    fn promote(&self, now: SystemTime) -> Result<Promoted> {
        let now = millis(now);
        let mut state = self.lock();
        let mut moved = 0;
        while state.scheduled.first().is_some_and(|(at, _)| *at <= now) {
            if let Some((_, id)) = state.scheduled.pop_first() {
                state.enqueue_scheduled(id);
                moved += 1;
            }
        }
        if moved > 0 {
            self.inner.pushed.notify_all();
        }
        Ok(Promoted {
            moved,
            next: state.next_run_at(),
        })
    }

    fn claim(&self, worker: &str, queues: &[&str], wait: Duration) -> Result<Option<JobRecord>> {
        self.claim_within_limits(worker, queues, &[], wait)
    }

    /// Counting and claiming happen under the one lock. A freed slot wakes
    /// waiting claims, like a push.
    fn claim_within_limits(
        &self,
        worker: &str,
        queues: &[&str],
        limits: &[GlobalLimit<'_>],
        wait: Duration,
    ) -> Result<Option<JobRecord>> {
        let deadline = Instant::now() + wait;
        let mut state = self.lock();
        loop {
            let locked = &mut *state;
            let next = queues.iter().find_map(|queue| {
                let limit = GlobalLimit::of(limits, queue);
                let used = locked.slots.get(*queue).map_or(0, HashSet::len);
                if limit.is_some_and(|max| used >= max) {
                    return None;
                }
                let id = locked.take_next(queue)?;
                if limit.is_some() {
                    locked
                        .slots
                        .entry((*queue).to_owned())
                        .or_default()
                        .insert(id.clone());
                }
                Some(id)
            });
            if let Some(id) = next {
                state
                    .processing
                    .entry(worker.to_owned())
                    .or_default()
                    .insert(id.clone());
                if let Some((job_state, job)) = state.jobs.get_mut(&id) {
                    *job_state = JobState::Processing;
                    return Ok(Some(job.clone()));
                }
                continue;
            }
            let now = Instant::now();
            if now >= deadline {
                return Ok(None);
            }
            state = self
                .inner
                .pushed
                .wait_timeout(state, deadline - now)
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        }
    }

    fn complete(&self, worker: &str, job: &JobRecord) -> Result<()> {
        let mut state = self.lock();
        if state.release(worker, job) {
            self.inner.pushed.notify_all();
        }
        state.unlock(job);
        state
            .jobs
            .insert(job.id.clone(), (JobState::Done, job.clone()));
        state.finish(&job.id, false);
        self.inner.finished.notify(&job.id);
        Ok(())
    }

    fn fail(&self, worker: &str, job: &JobRecord, next: JobState) -> Result<()> {
        let mut state = self.lock();
        if state.release(worker, job) {
            self.inner.pushed.notify_all();
        }
        if next == JobState::Dead {
            state.unlock(job);
        }
        if next == JobState::Scheduled {
            state.insert_scheduled(job.clone());
            return Ok(());
        }
        if next == JobState::Pending {
            state
                .pending
                .entry(job.queue.clone())
                .or_default()
                .push_back(job.id.clone());
            self.inner.pushed.notify_all();
        }
        state.jobs.insert(job.id.clone(), (next, job.clone()));
        if next == JobState::Dead {
            state.finish(&job.id, true);
            self.inner.finished.notify(&job.id);
        }
        Ok(())
    }

    fn get(&self, id: &str) -> Result<Option<(JobState, JobRecord)>> {
        Ok(self.lock().jobs.get(id).cloned())
    }

    fn checkpoint(&self, worker: &str, job: &JobRecord) -> Result<()> {
        let mut state = self.lock();
        let holds = state
            .processing
            .get(worker)
            .is_some_and(|held| held.contains(&job.id));
        if holds {
            state
                .jobs
                .insert(job.id.clone(), (JobState::Processing, job.clone()));
        }
        Ok(())
    }

    fn cancel(&self, id: &str) -> Result<bool> {
        let mut state = self.lock();
        let Some((job_state, job)) = state.jobs.get(id) else {
            return Ok(false);
        };
        match job_state {
            JobState::Scheduled => {
                let key = (job.run_at_ms.unwrap_or_default(), id.to_owned());
                if !state.scheduled.remove(&key) {
                    return Ok(false);
                }
            }
            JobState::Pending => {
                let queue = job.queue.clone();
                let Some(pending) = state.pending.get_mut(&queue) else {
                    return Ok(false);
                };
                let Some(position) = pending.iter().position(|pending| pending == id) else {
                    return Ok(false);
                };
                pending.remove(position);
            }
            _ => return Ok(false),
        }
        if let Some((job_state, job)) = state.jobs.get_mut(id) {
            *job_state = JobState::Cancelled;
            let job = job.clone();
            state.unlock(&job);
            state.finish(id, false);
        }
        self.inner.finished.notify(id);
        Ok(true)
    }

    fn heartbeat(&self, worker: &str, ttl: Duration) -> Result<()> {
        let mut state = self.lock();
        state
            .heartbeats
            .insert(worker.to_owned(), Instant::now() + ttl);
        state.processing.entry(worker.to_owned()).or_default();
        Ok(())
    }

    fn retire(&self, worker: &str) -> Result<()> {
        let mut state = self.lock();
        state.heartbeats.remove(worker);
        // Anything still held stays for `recover`, as with the other backends.
        if state.processing.get(worker).is_some_and(HashSet::is_empty) {
            state.processing.remove(worker);
        }
        Ok(())
    }

    fn recover(&self) -> Result<usize> {
        let now = Instant::now();
        let mut state = self.lock();
        let stopped: Vec<String> = state
            .processing
            .keys()
            .filter(|worker| {
                state
                    .heartbeats
                    .get(*worker)
                    .is_none_or(|expires| *expires <= now)
            })
            .cloned()
            .collect();
        let mut recovered = 0;
        for worker in stopped {
            state.heartbeats.remove(&worker);
            for id in state.processing.remove(&worker).unwrap_or_default() {
                let Some((job_state, job)) = state.jobs.get_mut(&id) else {
                    continue;
                };
                *job_state = JobState::Pending;
                let job = job.clone();
                state.free_key(&job);
                state.free_slot(&job.queue, &id);
                let queue = job.queue;
                // Claimed before anything still pending, so it goes first.
                state.pending.entry(queue).or_default().push_front(id);
                recovered += 1;
            }
        }
        if recovered > 0 {
            self.inner.pushed.notify_all();
        }
        Ok(recovered)
    }

    fn paused_queues(&self) -> Result<Vec<String>> {
        Ok(self.lock().paused.iter().cloned().collect())
    }

    fn register_recurring(&self, schedules: &[RecurringRecord]) -> Result<Vec<RecurringRecord>> {
        crate::recurring::validate_keys(schedules)?;
        let mut state = self.lock();
        Ok(schedules
            .iter()
            .map(|schedule| {
                let mut schedule = schedule.clone();
                if let Some(stored) = state.recurring.get(&schedule.key) {
                    schedule.merge_stored(stored);
                }
                state
                    .recurring
                    .insert(schedule.key.clone(), schedule.clone());
                schedule
            })
            .collect())
    }

    fn push_recurring(&self, key: &str, tick: SystemTime, job: NewJob) -> Result<Option<JobId>> {
        crate::recurring::validate_key(key)?;
        let tick = millis(tick);
        let mut state = self.lock();
        let ticks = state.ticks.entry(key.to_owned()).or_default();
        if !ticks.insert(tick) {
            return Ok(None);
        }
        let oldest = tick.saturating_sub(millis_of(TICK_RETENTION));
        *ticks = ticks.split_off(&oldest);
        let id = state.insert_new(job.into_record());
        if let Some(schedule) = state.recurring.get_mut(key) {
            schedule.record_run(tick, &id);
        }
        drop(state);
        self.inner.pushed.notify_all();
        Ok(Some(id))
    }

    /// Every call is a few map updates under a lock.
    fn clean_finished(&self, now: SystemTime, limit: usize) -> Result<Cleaned> {
        let mut state = self.lock();
        let retention = state.retention;
        let mut deleted = 0;
        for (dead, keep) in [(false, retention.finished), (true, retention.dead)] {
            if let Some(cutoff) = keep.cutoff(now) {
                deleted += state.clean(dead, millis(cutoff), limit - deleted);
            }
        }
        Ok(Cleaned {
            deleted,
            more: deleted == limit,
        })
    }

    fn blocks(&self) -> bool {
        false
    }

    fn describe(&self) -> String {
        "memory".to_owned()
    }
}

impl Monitor for MemoryQueue {
    fn stats(&self) -> Result<Stats> {
        let state = self.lock();
        let mut stats = Stats {
            processed_total: state.processed_total,
            failed_total: state.failed_total,
            ..Stats::default()
        };
        // Per queue: pending, running.
        let mut queues: BTreeMap<&str, (u64, u64)> = state
            .pending
            .iter()
            .map(|(queue, ids)| (queue.as_str(), (ids.len() as u64, 0)))
            .collect();
        for (job_state, job) in state.jobs.values() {
            let counts = queues.entry(job.queue.as_str()).or_default();
            match job_state {
                JobState::Processing => {
                    stats.processing += 1;
                    counts.1 += 1;
                }
                JobState::Done => stats.done += 1,
                JobState::Dead => stats.dead += 1,
                JobState::Cancelled => stats.cancelled += 1,
                JobState::Scheduled => stats.scheduled += 1,
                JobState::Pending => {}
            }
        }
        stats.queues = queues
            .into_iter()
            .map(|(name, (pending, running))| QueueStats {
                name: name.to_owned(),
                pending,
                running,
            })
            .collect();
        let now = Instant::now();
        stats.workers = state
            .processing
            .iter()
            .map(|(id, held)| WorkerStats {
                id: id.clone(),
                running: held.len() as u64,
                expires_in_ms: state.heartbeats.get(id).map_or(-1, |expires| {
                    if *expires >= now {
                        i64::try_from((*expires - now).as_millis()).unwrap_or(i64::MAX)
                    } else {
                        -i64::try_from((now - *expires).as_millis()).unwrap_or(i64::MAX)
                    }
                }),
            })
            .collect();
        stats.workers.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(stats)
    }

    fn list(&self, filter: &ListFilter) -> Result<Vec<JobRecord>> {
        let state = self.lock();
        let mut jobs: Vec<&JobRecord> = state
            .jobs
            .values()
            .filter(|(job_state, job)| {
                *job_state == filter.state
                    && filter
                        .queue
                        .as_deref()
                        .is_none_or(|queue| queue == job.queue)
            })
            .map(|(_, job)| job)
            .collect();
        if filter.state == JobState::Scheduled {
            jobs.sort_by(|a, b| (a.run_at_ms, &a.id).cmp(&(b.run_at_ms, &b.id)));
        } else {
            // Ids start with the enqueue time.
            jobs.sort_by(|a, b| a.id.cmp(&b.id));
        }
        if filter.state.is_finished() {
            jobs.reverse();
        }
        Ok(jobs
            .into_iter()
            .skip(filter.offset)
            .take(filter.limit)
            .cloned()
            .collect())
    }

    fn retry(&self, id: &str) -> Result<bool> {
        let mut state = self.lock();
        let Some((job_state, job)) = state.jobs.get_mut(id) else {
            return Ok(false);
        };
        if *job_state != JobState::Dead {
            return Ok(false);
        }
        *job_state = JobState::Pending;
        job.attempts = 0;
        job.resumptions = 0;
        let queue = job.queue.clone();
        state.unfinish(id);
        state
            .pending
            .entry(queue)
            .or_default()
            .push_back(id.to_owned());
        self.inner.pushed.notify_all();
        Ok(true)
    }

    fn run_now(&self, id: &str) -> Result<bool> {
        let mut state = self.lock();
        let Some((JobState::Scheduled, job)) = state.jobs.get(id) else {
            return Ok(false);
        };
        let key = (job.run_at_ms.unwrap_or_default(), id.to_owned());
        if !state.scheduled.remove(&key) {
            return Ok(false);
        }
        state.enqueue_scheduled(key.1);
        self.inner.pushed.notify_all();
        Ok(true)
    }

    fn discard(&self, id: &str) -> Result<bool> {
        let mut state = self.lock();
        let finished = state
            .jobs
            .get(id)
            .is_some_and(|(job_state, _)| job_state.is_finished());
        if finished {
            state.jobs.remove(id);
            state.unfinish(id);
        }
        Ok(finished)
    }

    fn pause_queue(&self, queue: &str) -> Result<bool> {
        Ok(self.lock().paused.insert(queue.to_owned()))
    }

    fn resume_queue(&self, queue: &str) -> Result<bool> {
        let resumed = self.lock().paused.remove(queue);
        if resumed {
            // Idle claims waiting on this queue can take its jobs now.
            self.inner.pushed.notify_all();
        }
        Ok(resumed)
    }

    fn recurring(&self) -> Result<Vec<RecurringRecord>> {
        Ok(self.lock().recurring.values().cloned().collect())
    }

    fn remove_recurring(&self, key: &str) -> Result<bool> {
        crate::recurring::validate_key(key)?;
        let mut state = self.lock();
        state.ticks.remove(key);
        Ok(state.recurring.remove(key).is_some())
    }

    fn record_metric(&self, metric: &JobMetric) -> Result<()> {
        let mut state = self.lock();
        state.processed_total += 1;
        state.failed_total += u64::from(metric.failed);
        state
            .metrics
            .entry((metric.minute, metric.queue.clone(), metric.job.clone()))
            .or_insert_with(|| MetricBucket {
                minute: metric.minute,
                queue: metric.queue.clone(),
                job: metric.job.clone(),
                ..MetricBucket::default()
            })
            .add(metric);
        if metric.minute > state.pruned_at_minute {
            state.pruned_at_minute = metric.minute;
            let oldest = metric.minute.saturating_sub(METRICS_RETENTION_MINUTES);
            state.metrics = state
                .metrics
                .split_off(&(oldest, String::new(), String::new()));
        }
        Ok(())
    }

    fn metrics(&self, since_minute: u64) -> Result<Vec<MetricBucket>> {
        let state = self.lock();
        Ok(state
            .metrics
            .range((since_minute, String::new(), String::new())..)
            .map(|(_, bucket)| bucket.clone())
            .collect())
    }
}

impl Watch for MemoryQueue {
    fn watch_finished(&self, id: &str) -> Option<Arc<Signal>> {
        Some(self.inner.finished.watch(id))
    }
}

fn millis_of(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}
