//! Running jobs inline in tests, like ActiveJob's `perform_enqueued_jobs`.
//!
//! ```ignore
//! #[tokio::test]
//! async fn signup_sends_the_welcome_email() {
//!     butler::testing::perform_enqueued_jobs(async {
//!         // Runs the job right here, before `.await` returns.
//!         send_welcome(user_id).await.unwrap();
//!     })
//!     .await;
//!     assert!(mailbox().contains("Welcome"));
//! }
//! ```
//!
//! Inside the block, awaiting a `#[job]` function doesn't touch any queue: the
//! job's own generated code runs at once, in the caller's task. Jobs that jobs
//! enqueue run inline too. Each job gets a real [`JobHandle`], backed by a
//! private in-memory store, so `result()` and `wait_result()` work and return
//! immediately. Failures are not retried: the job is dead after one attempt,
//! and `wait_result` returns [`Error::JobFailed`](crate::Error::JobFailed). A
//! panic in a job propagates and fails the test, like an exception inside
//! Rails' block.
//!
//! Inline mode follows the future you pass: it is on while that future is
//! being polled, on whatever thread polls it. Work you `tokio::spawn` from
//! inside is a separate task, so it enqueues normally. It needs no worker, no
//! configured queue, and no runtime (it also works under [`block_on`](crate::block_on)).

use std::{
    cell::RefCell,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex, PoisonError},
    task::{Context, Poll},
};

use serde_json::Value;

use crate::{
    AnyJob, Backend, Failed, Job, JobDef, JobHandle, MemoryQueue, Queue, Result, error::Chain,
};

thread_local! {
    /// The inline scope being polled on this thread, if any.
    static INLINE: RefCell<Option<InlineJobs>> = const { RefCell::new(None) };
}

/// Runs every job enqueued while `body` runs, immediately, like ActiveJob's
/// `perform_enqueued_jobs { ... }`. Returns `body`'s output.
///
/// To also check which jobs ran, use [`InlineJobs`].
pub fn perform_enqueued_jobs<F: Future>(body: F) -> Performing<F> {
    InlineJobs::new().perform(body)
}

/// An inline scope that also records the jobs it ran, for assertions like
/// ActiveJob's `assert_performed_jobs`.
///
/// ```ignore
/// let jobs = InlineJobs::new();
/// jobs.perform(async { signup(user).await }).await;
/// assert_eq!(jobs.performed_names(), ["send_welcome", "sync_crm"]);
/// ```
#[derive(Clone, Default)]
pub struct InlineJobs {
    store: MemoryQueue,
    performed: Arc<Mutex<Vec<AnyJob>>>,
}

impl InlineJobs {
    pub fn new() -> Self {
        Self::default()
    }

    /// Runs `body` with every job it enqueues performed immediately.
    pub fn perform<F: Future>(&self, body: F) -> Performing<F> {
        Performing {
            body: Box::pin(body),
            jobs: self.clone(),
        }
    }

    /// Every job performed so far, in the order they finished (a job that
    /// enqueues another finishes after it), with its final state and output.
    pub fn performed(&self) -> Vec<AnyJob> {
        self.performed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// The names of the jobs performed so far, in the order they finished.
    pub fn performed_names(&self) -> Vec<String> {
        self.performed()
            .iter()
            .map(|job| job.record().name.clone())
            .collect()
    }

    /// Runs one job right now, as the enqueue call's replacement.
    pub(crate) async fn run<T>(
        &self,
        def: &'static JobDef,
        queue_name: &str,
        args: Vec<Value>,
    ) -> Result<JobHandle<T>> {
        const INLINE: &str = "inline";
        let queue: Queue = self.store.clone().into();
        let id = self.store.push(def.name, queue_name, args.clone())?;
        let record = self
            .store
            .get(&id)?
            .map(|(_, record)| record)
            .ok_or_else(|| crate::Error::JobNotFound { id: id.clone() })?;
        let running: Job<crate::state::Processing> = Job::from_record(record);

        let finished = match (def.perform)(args).await {
            Ok(output) => AnyJob::Done(queue.complete(INLINE, running, output)?),
            // One attempt: tests shouldn't wait on retries.
            Err(err) => match queue.fail(INLINE, running, Chain(&err).to_string(), 0)? {
                Failed::Dead(job) => AnyJob::Dead(job),
                Failed::Retry(job) => AnyJob::Pending(job),
            },
        };
        self.performed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(finished);
        Ok(JobHandle::new(queue, id))
    }
}

/// The future returned by [`perform_enqueued_jobs`] and [`InlineJobs::perform`].
pub struct Performing<F> {
    body: Pin<Box<F>>,
    jobs: InlineJobs,
}

impl<F: Future> Future for Performing<F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        let this = &mut *self;
        let previous = INLINE.with(|inline| inline.replace(Some(this.jobs.clone())));
        // Restores the outer scope even if `body` panics.
        let _restore = Restore(previous);
        this.body.as_mut().poll(cx)
    }
}

struct Restore(Option<InlineJobs>);

impl Drop for Restore {
    fn drop(&mut self) {
        let previous = self.0.take();
        INLINE.with(|inline| *inline.borrow_mut() = previous);
    }
}

/// Used by the enqueue path: `Some` when a job enqueued now must run inline.
pub(crate) fn current() -> Option<InlineJobs> {
    INLINE.with(|inline| inline.borrow().clone())
}
