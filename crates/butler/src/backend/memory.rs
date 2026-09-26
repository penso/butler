//! An in-process queue: no files, no server. Enqueuers and workers must share
//! one `MemoryQueue` (clones share state), so it suits tests, and apps that run
//! their workers in the same process. Nothing survives a restart, and finished
//! jobs are kept until the process exits.
//!
//! It follows the same contract as the other backends: per-worker processing,
//! heartbeats and recovery, atomic claim and cancel (here, under one lock). A
//! claim with a `wait` sleeps on a condition variable and wakes as soon as a
//! job is pushed.

use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock, PoisonError},
    time::{Duration, Instant},
};

use serde_json::Value;

use super::{Backend, NewJob};
use crate::{
    JobId, JobRecord, JobState, Result, Signal,
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
    jobs: HashMap<JobId, (JobState, JobRecord)>,
    /// Per worker. A set: a worker can hold a great many jobs at once, and
    /// finishing one must not scan the others.
    processing: HashMap<String, HashSet<JobId>>,
    /// When each worker's heartbeat expires.
    heartbeats: HashMap<String, Instant>,
    /// History, per (minute, queue, job).
    metrics: BTreeMap<(u64, String, String), MetricBucket>,
    processed_total: u64,
    failed_total: u64,
    /// The minute history was last pruned, so it's pruned once a minute, not
    /// on every job.
    pruned_at_minute: u64,
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
    fn release(&mut self, worker: &str, id: &str) {
        if let Some(held) = self.processing.get_mut(worker) {
            held.remove(id);
        }
    }
}

impl Backend for MemoryQueue {
    fn push(&self, name: &str, queue: &str, args: Vec<Value>) -> Result<JobId> {
        let job = JobRecord::new(name, queue, args);
        let id = job.id.clone();
        let mut state = self.lock();
        state.jobs.insert(id.clone(), (JobState::Pending, job));
        state
            .pending
            .entry(queue.to_owned())
            .or_default()
            .push_back(id.clone());
        // Every waiting claim rechecks: only some of them serve this queue.
        self.inner.pushed.notify_all();
        Ok(id)
    }

    fn push_many(&self, jobs: Vec<NewJob>) -> Result<Vec<JobId>> {
        let mut state = self.lock();
        let ids = jobs
            .into_iter()
            .map(|new| {
                let job = JobRecord::new(&new.name, &new.queue, new.args);
                let id = job.id.clone();
                state.jobs.insert(id.clone(), (JobState::Pending, job));
                state
                    .pending
                    .entry(new.queue)
                    .or_default()
                    .push_back(id.clone());
                id
            })
            .collect();
        self.inner.pushed.notify_all();
        Ok(ids)
    }

    fn claim(&self, worker: &str, queues: &[&str], wait: Duration) -> Result<Option<JobRecord>> {
        let deadline = Instant::now() + wait;
        let mut state = self.lock();
        loop {
            let next = queues
                .iter()
                .find_map(|queue| state.pending.get_mut(*queue)?.pop_front());
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
        state.release(worker, &job.id);
        state
            .jobs
            .insert(job.id.clone(), (JobState::Done, job.clone()));
        self.inner.finished.notify(&job.id);
        Ok(())
    }

    fn fail(&self, worker: &str, job: &JobRecord, next: JobState) -> Result<()> {
        let mut state = self.lock();
        state.release(worker, &job.id);
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
        let Some(queue) = state.jobs.get(id).map(|(_, job)| job.queue.clone()) else {
            return Ok(false);
        };
        let Some(pending) = state.pending.get_mut(&queue) else {
            return Ok(false);
        };
        let Some(position) = pending.iter().position(|pending| pending == id) else {
            return Ok(false);
        };
        pending.remove(position);
        if let Some((job_state, _)) = state.jobs.get_mut(id) {
            *job_state = JobState::Cancelled;
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
                let queue = job.queue.clone();
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

    fn watch_finished(&self, id: &str) -> Option<Arc<Signal>> {
        Some(self.inner.finished.watch(id))
    }

    fn stats(&self) -> Result<Stats> {
        let state = self.lock();
        let mut stats = Stats {
            processed_total: state.processed_total,
            failed_total: state.failed_total,
            ..Stats::default()
        };
        let mut queues: BTreeMap<&str, u64> = state
            .pending
            .iter()
            .map(|(queue, ids)| (queue.as_str(), ids.len() as u64))
            .collect();
        for (job_state, job) in state.jobs.values() {
            queues.entry(job.queue.as_str()).or_default();
            match job_state {
                JobState::Processing => stats.processing += 1,
                JobState::Done => stats.done += 1,
                JobState::Dead => stats.dead += 1,
                JobState::Cancelled => stats.cancelled += 1,
                JobState::Pending => {}
            }
        }
        stats.queues = queues
            .into_iter()
            .map(|(name, pending)| QueueStats {
                name: name.to_owned(),
                pending,
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
        // Ids start with the enqueue time.
        jobs.sort_by(|a, b| a.id.cmp(&b.id));
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
        let queue = job.queue.clone();
        state
            .pending
            .entry(queue)
            .or_default()
            .push_back(id.to_owned());
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
        }
        Ok(finished)
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

    /// Every call is a few map updates under a lock.
    fn blocks(&self) -> bool {
        false
    }

    fn describe(&self) -> String {
        "memory".to_owned()
    }
}
