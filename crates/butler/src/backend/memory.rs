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
    collections::{HashMap, HashSet, VecDeque},
    sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock, PoisonError},
    time::{Duration, Instant},
};

use serde_json::Value;

use super::{Backend, NewJob};
use crate::{JobId, JobRecord, JobState, Result, Signal, signal::JobWatch};

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
}

impl MemoryQueue {
    /// A new, empty queue.
    pub fn new() -> Self {
        Self::default()
    }

    /// The queue that `backend = "memory"` in `config.toml` refers to: one per
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

    /// Every call is a few map updates under a lock.
    fn blocks(&self) -> bool {
        false
    }

    fn describe(&self) -> String {
        "memory".to_owned()
    }
}
