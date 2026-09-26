use std::{fmt, marker::PhantomData};

use serde_json::Value;

use crate::{
    Error, JobDef, JobHandle, Queue, Result, backend::NewJob, executor::unblock,
    job::is_valid_queue_name,
};

/// A job ready to enqueue, with its arguments already serialized: what
/// `send_email::prepare(...)` returns. Enqueue it alone with
/// [`enqueue`](PreparedJob::enqueue), or many at once with [`enqueue_all`],
/// like ActiveJob's `perform_all_later`.
pub struct PreparedJob<T = Value> {
    def: &'static JobDef,
    queue: String,
    args: Vec<Value>,
    output: PhantomData<fn() -> T>,
}

impl<T> PreparedJob<T> {
    #[doc(hidden)]
    pub fn new(def: &'static JobDef, args: Vec<Value>) -> Self {
        Self {
            def,
            queue: def.queue.to_owned(),
            args,
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
            output: PhantomData,
        }
    }

    /// Enqueues this one job.
    pub async fn enqueue(self) -> Result<JobHandle<T>> {
        crate::__private::enqueue_on(self.def, &self.queue, self.args).await
    }
}

impl<T> fmt::Debug for PreparedJob<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PreparedJob")
            .field("name", &self.def.name)
            .field("queue", &self.queue)
            .field("args", &self.args)
            .finish()
    }
}

/// Enqueues many jobs at once, like ActiveJob's `perform_all_later`, and
/// returns their handles in the same order.
///
/// Backends that can do it in one step do: Redis sends one pipelined
/// transaction, SQLite writes one transaction, memory takes its lock once. The
/// file backend writes one file per job, as usual. Inside
/// [`perform_enqueued_jobs`](crate::testing::perform_enqueued_jobs), each job
/// runs inline, in order.
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
    if let Some(inline) = crate::testing::current() {
        let mut handles = Vec::with_capacity(jobs.len());
        for job in jobs {
            handles.push(inline.run(job.def, &job.queue, job.args).await?);
        }
        return Ok(handles);
    }
    let queue: Queue = crate::queue()?;
    let new_jobs: Vec<NewJob> = jobs
        .into_iter()
        .map(|job| NewJob {
            name: job.def.name.to_owned(),
            queue: job.queue,
            args: job.args,
        })
        .collect();
    let pushing = queue.clone();
    let ids = unblock(queue.blocks(), move || pushing.push_many(new_jobs)).await?;
    Ok(ids
        .into_iter()
        .map(|id| JobHandle::new(queue.clone(), id))
        .collect())
}
