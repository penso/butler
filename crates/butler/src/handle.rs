use std::{fmt, marker::PhantomData, time::Duration};

use serde::de::DeserializeOwned;

use crate::{
    AnyJob, Error, JobId, JobState, Queue, Result, Signal,
    executor::{sleep, unblock},
};

/// An enqueued job. Awaiting a `#[job]` function returns one, typed by what
/// the job returns: `JobHandle<T>` for a job returning `Result<T, E>`, and
/// `JobHandle<()>` for one returning nothing.
///
/// Each method asks the backend for the job's current status, so a handle
/// stays valid across processes: store [`JobHandle::id`] and rebuild it later
/// with [`Queue::handle`].
pub struct JobHandle<T = serde_json::Value> {
    queue: Queue,
    id: JobId,
    // `fn() -> T`: the handle never holds a `T`, so it is `Send`/`Sync` for any `T`.
    output: PhantomData<fn() -> T>,
}

impl<T> JobHandle<T> {
    pub(crate) fn new(queue: Queue, id: JobId) -> Self {
        Self {
            queue,
            id,
            output: PhantomData,
        }
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn into_id(self) -> JobId {
        self.id
    }

    /// The job's current state, or `None` if the backend no longer knows it
    /// (for example, Redis expired it a day after it finished).
    pub async fn state(&self) -> Result<Option<JobState>> {
        Ok(self.job().await?.map(|job| job.state()))
    }

    /// The job as stored right now, typed by its state: match on it to reach
    /// what that state offers, like [`Job::output`](crate::Job::output) once
    /// it is done.
    pub async fn job(&self) -> Result<Option<AnyJob>> {
        let (queue, id) = (self.queue.clone(), self.id.clone());
        unblock(queue.blocks(), move || queue.get(&id)).await
    }

    /// Removes the job if no worker has claimed it yet, and marks it
    /// [`JobState::Cancelled`]. Returns `false`, changing nothing, if a worker
    /// already took it or it already finished: a running job is never
    /// interrupted.
    pub async fn cancel(&self) -> Result<bool> {
        let (queue, id) = (self.queue.clone(), self.id.clone());
        unblock(queue.blocks(), move || queue.cancel(&id)).await
    }

    /// Waits until the job is done, dead, or cancelled, and returns that state.
    ///
    /// With Redis and memory, the backend signals every finished job, so this
    /// returns as soon as it happens and `fallback` is only a safety net
    /// (a missed Redis message). The file backend can't signal, so it re-checks
    /// every `fallback`.
    pub async fn wait(&self, fallback: Duration) -> Result<JobState> {
        Ok(self.finished(fallback).await?.state())
    }

    /// Waits for the job to reach a final state, and returns it as it is then.
    async fn finished(&self, fallback: Duration) -> Result<AnyJob> {
        let signal = self.queue.watch_finished(&self.id);
        let signal = signal.as_deref();
        loop {
            // Read before checking, so a job finishing in between still wakes us.
            let seen = signal.map(Signal::generation);
            match self.job().await? {
                Some(job) if job.state().is_finished() => return Ok(job),
                Some(_) => {}
                None => {
                    return Err(Error::JobNotFound {
                        id: self.id.clone(),
                    });
                }
            }
            match (signal, seen) {
                (Some(signal), Some(seen)) => signal.changed_past(seen, fallback).await,
                _ => sleep(fallback).await,
            }
        }
    }

    /// The same job, with its output read as `U` instead.
    pub fn with_output<U>(self) -> JobHandle<U> {
        JobHandle::new(self.queue, self.id)
    }
}

impl<T: DeserializeOwned> JobHandle<T> {
    /// What the job returned, once a worker has finished it. `None` while it is
    /// pending or running, and also if it died or was cancelled: use
    /// [`JobHandle::wait_result`] to tell those apart.
    pub async fn result(&self) -> Result<Option<T>> {
        match self.job().await? {
            Some(AnyJob::Done(job)) => Ok(Some(job.output()?)),
            Some(_) => Ok(None),
            None => Err(Error::JobNotFound {
                id: self.id.clone(),
            }),
        }
    }

    /// Waits until the job finishes, and returns what it returned. A job that
    /// exhausted its retries is [`Error::JobFailed`], with its last error; a
    /// cancelled one is [`Error::JobCancelled`]. Redis and memory wake it the
    /// moment the job finishes; `fallback` is how often to re-check anyway
    /// (always, on the file backend). See [`JobHandle::wait`].
    pub async fn wait_result(&self, fallback: Duration) -> Result<T> {
        match self.finished(fallback).await? {
            AnyJob::Done(job) => job.output(),
            AnyJob::Dead(job) => Err(Error::JobFailed {
                id: self.id.clone(),
                error: job.error().to_owned(),
            }),
            // `finished` only returns final states; anything else was cancelled.
            _ => Err(Error::JobCancelled {
                id: self.id.clone(),
            }),
        }
    }
}

impl<T> Clone for JobHandle<T> {
    fn clone(&self) -> Self {
        Self::new(self.queue.clone(), self.id.clone())
    }
}

impl<T> fmt::Display for JobHandle<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.id)
    }
}

impl<T> fmt::Debug for JobHandle<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JobHandle")
            .field("id", &self.id)
            .field("queue", &self.queue)
            .finish()
    }
}
