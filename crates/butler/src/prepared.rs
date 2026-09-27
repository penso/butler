use std::{
    fmt,
    marker::PhantomData,
    time::{Duration, SystemTime},
};

use serde_json::Value;

use crate::{
    Error, JobDef, JobHandle, Queue, Result,
    backend::NewJob,
    executor::unblock,
    job::{after, is_valid_queue_name},
};

/// A job ready to enqueue, with its arguments already serialized: what
/// `send_email::prepare(...)` returns. Enqueue it alone with
/// [`enqueue`](PreparedJob::enqueue), or many at once with [`enqueue_all`],
/// like ActiveJob's `perform_all_later`.
///
/// To run it later, like ActiveJob's `set(wait:)` and `set(wait_until:)`:
///
/// ```ignore
/// send_reminder::prepare(user_id)?
///     .run_in(Duration::from_secs(300))
///     .enqueue()
///     .await?;
/// ```
pub struct PreparedJob<T = Value> {
    def: &'static JobDef,
    queue: String,
    args: Vec<Value>,
    run_at: Option<SystemTime>,
    output: PhantomData<fn() -> T>,
}

impl<T> PreparedJob<T> {
    #[doc(hidden)]
    pub fn new(def: &'static JobDef, args: Vec<Value>) -> Self {
        Self {
            def,
            queue: def.queue.to_owned(),
            args,
            run_at: None,
            output: PhantomData,
        }
    }

    pub fn name(&self) -> &str {
        self.def.name
    }

    pub fn queue(&self) -> &str {
        &self.queue
    }

    pub fn args(&self) -> &[Value] {
        &self.args
    }

    /// When it is scheduled to run, if it was.
    pub fn scheduled_at(&self) -> Option<SystemTime> {
        self.run_at
    }

    /// Runs it no sooner than `delay` from now, like ActiveJob's
    /// `set(wait: ...)`. Until then it is [`Scheduled`](crate::JobState::Scheduled),
    /// and can be cancelled. Inside
    /// [`perform_enqueued_jobs`](crate::testing::perform_enqueued_jobs), it
    /// runs at once.
    pub fn run_in(self, delay: Duration) -> Self {
        self.run_at(after(delay))
    }

    /// Runs it no sooner than `at`, like ActiveJob's `set(wait_until: ...)`.
    /// A time already past enqueues it at once.
    pub fn run_at(mut self, at: SystemTime) -> Self {
        self.run_at = Some(at);
        self
    }

    /// Enqueues on `queue` instead of the one set with `#[job(queue = ...)]`,
    /// like ActiveJob's `MyJob.set(queue: :low)`. Workers only run it if they
    /// serve that queue.
    pub fn on_queue(mut self, queue: &str) -> Result<Self> {
        if !is_valid_queue_name(queue) {
            return Err(Error::InvalidQueue {
                name: queue.to_owned(),
                reason: "use 1 to 64 of A-Z a-z 0-9 _ - . (not starting with a dot)",
            });
        }
        self.queue = queue.to_owned();
        Ok(self)
    }

    /// Forgets the output type, so jobs of different kinds can go in one
    /// [`enqueue_all`]. Get it back on the handle with
    /// [`JobHandle::with_output`].
    pub fn untyped(self) -> PreparedJob<Value> {
        PreparedJob {
            def: self.def,
            queue: self.queue,
            args: self.args,
            run_at: self.run_at,
            output: PhantomData,
        }
    }

    /// Runs the job's body right now, in this process, and returns its
    /// output, like [`JobCall::now`](crate::JobCall::now): once, with no
    /// retries, ignoring its queue and any run time.
    pub async fn now(self) -> Result<T, crate::JobError>
    where
        T: serde::de::DeserializeOwned,
    {
        let output = crate::call::run_now(self.def, self.args).await?;
        serde_json::from_value(output).map_err(crate::JobError::Output)
    }

    /// Enqueues this one job, or schedules it if it has a run time.
    pub async fn enqueue(self) -> Result<JobHandle<T>> {
        crate::__private::enqueue_on(self.def, &self.queue, self.args, self.run_at).await
    }
}

impl<T> fmt::Debug for PreparedJob<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PreparedJob")
            .field("name", &self.def.name)
            .field("queue", &self.queue)
            .field("args", &self.args)
            .field("run_at", &self.run_at)
            .finish()
    }
}

/// Enqueues many jobs at once, like ActiveJob's `perform_all_later`, and
/// returns their handles in the same order.
///
/// Backends that can do it in one step do: Redis sends one pipelined
/// transaction, SQLite writes one transaction, memory takes its lock once. The
/// file backend writes one file per job, as usual. Jobs with a run time are
/// scheduled in the same step. Inside
/// [`perform_enqueued_jobs`](crate::testing::perform_enqueued_jobs), each job
/// runs inline, in order, scheduled or not; inside
/// [`RecordedJobs::record`](crate::testing::RecordedJobs::record), each is
/// recorded, in order.
///
/// ```ignore
/// let emails = users
///     .iter()
///     .map(|user| send_email::prepare(&user.email, "Welcome"))
///     .collect::<Result<Vec<_>, _>>()?;
/// let handles = butler::enqueue_all(emails).await?;
/// ```
pub async fn enqueue_all<T>(
    jobs: impl IntoIterator<Item = PreparedJob<T>>,
) -> Result<Vec<JobHandle<T>>> {
    let jobs: Vec<PreparedJob<T>> = jobs.into_iter().collect();
    if jobs.is_empty() {
        return Ok(Vec::new());
    }
    // Every job passes the enqueue layers before any is stored: one veto
    // fails the whole batch, and nothing is enqueued.
    let jobs = jobs
        .into_iter()
        .map(|job| {
            let mut new = NewJob::for_job(job.def, job.queue, job.args);
            new.run_at = job.run_at;
            crate::enqueue::apply(&mut new)?;
            Ok((job.def, new))
        })
        .collect::<Result<Vec<_>>>()?;
    if let Some(scope) = crate::testing::current() {
        let mut handles = Vec::with_capacity(jobs.len());
        for (def, new) in jobs {
            handles.push(scope.enqueue(def, new).await?);
        }
        return Ok(handles);
    }
    let queue: Queue = crate::queue()?;
    let new_jobs: Vec<NewJob> = jobs.into_iter().map(|(_, new)| new).collect();
    let pushing = queue.clone();
    let ids = unblock(queue.blocks(), move || pushing.push_many(new_jobs)).await?;
    Ok(ids
        .into_iter()
        .map(|id| JobHandle::new(queue.clone(), id))
        .collect())
}
