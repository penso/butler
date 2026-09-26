use std::{fmt, marker::PhantomData, time::Duration};

use serde::de::DeserializeOwned;

use crate::{
    Error, Job, JobId, JobState, Queue, Result,
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
        Ok(self.job_with_state().await?.map(|(state, _)| state))
    }

    /// The stored job: its arguments, attempts so far, last error and output.
    pub async fn job(&self) -> Result<Option<Job>> {
        Ok(self.job_with_state().await?.map(|(_, job)| job))
    }

    /// Removes the job if no worker has claimed it yet, and marks it
    /// [`JobState::Cancelled`]. Returns `false`, changing nothing, if a worker
    /// already took it or it already finished: a running job is never
    /// interrupted.
    pub async fn cancel(&self) -> Result<bool> {
        let (queue, id) = (self.queue.clone(), self.id.clone());
        unblock(move || queue.cancel(&id)).await
    }

    /// Polls every `interval` until the job is done, dead, or cancelled, and
    /// returns that state.
    pub async fn wait(&self, interval: Duration) -> Result<JobState> {
        loop {
            match self.state().await? {
                Some(state) if state.is_finished() => return Ok(state),
                Some(_) => sleep(interval).await,
                None => {
                    return Err(Error::JobNotFound {
                        id: self.id.clone(),
                    });
                }
            }
        }
    }

    async fn job_with_state(&self) -> Result<Option<(JobState, Job)>> {
        let (queue, id) = (self.queue.clone(), self.id.clone());
        unblock(move || queue.get(&id)).await
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
        match self.job_with_state().await? {
            Some((JobState::Done, job)) => Ok(Some(Self::decode(job)?)),
            Some(_) => Ok(None),
            None => Err(Error::JobNotFound {
                id: self.id.clone(),
            }),
        }
    }

    /// Polls every `interval` until the job finishes, and returns what it
    /// returned. A job that exhausted its retries is [`Error::JobFailed`], with
    /// its last error; a cancelled one is [`Error::JobCancelled`].
    pub async fn wait_result(&self, interval: Duration) -> Result<T> {
        match self.wait(interval).await? {
            JobState::Done => self.result().await?.ok_or_else(|| Error::JobNotFound {
                id: self.id.clone(),
            }),
            JobState::Cancelled => Err(Error::JobCancelled {
                id: self.id.clone(),
            }),
            _ => {
                let error = self
                    .job()
                    .await?
                    .and_then(|job| job.last_error)
                    .unwrap_or_default();
                Err(Error::JobFailed {
                    id: self.id.clone(),
                    error,
                })
            }
        }
    }

    /// Done jobs written before results existed have none; they read as null.
    fn decode(job: Job) -> Result<T> {
        let value = job.result.unwrap_or(serde_json::Value::Null);
        Ok(serde_json::from_value(value)?)
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
