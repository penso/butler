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
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicU32, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};

use crate::{
    AnyJob, Failed, Job, JobDef, JobHandle, JobRecord, MemoryQueue, NewJob, Queue, Result, Store,
    error::Chain,
    progress::{Checkpoints, Invocation},
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
    /// Interrupt each job's first run at this checkpoint (1-based).
    interrupt_at: Option<u32>,
    interruptions: Arc<AtomicU32>,
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

    /// Interrupts each job's first run at its `n`-th checkpoint, as if the
    /// worker were shutting down, then resumes it at once from the progress
    /// it saved, like a restarted worker would. Use it to test that a job
    /// with a [`Progress`](crate::Progress) resumes correctly.
    pub fn interrupt_at_checkpoint(mut self, n: u32) -> Self {
        self.interrupt_at = Some(n.max(1));
        self
    }

    /// How many times a job was interrupted and resumed.
    pub fn interruptions(&self) -> u32 {
        self.interruptions.load(Ordering::SeqCst)
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
        mut job: NewJob,
    ) -> Result<JobHandle<T>> {
        const INLINE: &str = "inline";
        let queue: Queue = self.store.clone().into();
        let args = job.args.clone();
        // Runs at once, whatever its run time.
        job.run_at = None;
        let id = self.store.push(job)?;
        let mut record = self
            .store
            .get(&id)?
            .map(|(_, record)| record)
            .ok_or_else(|| crate::Error::JobNotFound { id: id.clone() })?;

        let mut first_run = true;
        let finished = loop {
            let checkpoints = self.checkpoints(&record, first_run);
            let outcome = (def.perform)(Invocation {
                args: args.clone(),
                checkpoints: checkpoints.clone(),
            })
            .await;
            let latest = checkpoints.latest();
            let running: Job<crate::state::Processing> = Job::from_record(record.clone());
            let running = match latest {
                Some(progress) => running.with_progress(progress),
                None => running,
            };
            break match outcome {
                Ok(output) => AnyJob::Done(queue.complete(INLINE, running, output)?),
                // Interrupted: resume right away from the saved progress.
                Err(_) if checkpoints.interrupted() => {
                    record = running.into_record();
                    self.interruptions.fetch_add(1, Ordering::SeqCst);
                    first_run = false;
                    continue;
                }
                // One attempt, whatever the job's retry settings: tests
                // shouldn't wait on retries.
                Err(err) => match queue.fail(INLINE, running, Chain(&err).to_string(), 0)? {
                    Failed::Dead(job) => AnyJob::Dead(job),
                    Failed::Scheduled(job) => AnyJob::Scheduled(job),
                    Failed::Retry(job) => AnyJob::Pending(job),
                },
            };
        };
        self.performed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(finished);
        Ok(JobHandle::new(queue, id))
    }

    /// Checkpoints for an inline run: saved to the private store at every
    /// checkpoint, and stopping at `interrupt_at` on a job's first run.
    fn checkpoints(&self, record: &JobRecord, first_run: bool) -> Checkpoints {
        let stop_at = self.interrupt_at.filter(|_| first_run);
        let seen = AtomicU32::new(0);
        Checkpoints::new(
            record.progress.clone(),
            Duration::ZERO,
            move || stop_at.is_some_and(|n| seen.fetch_add(1, Ordering::SeqCst) + 1 >= n),
            Box::new(|_| Box::pin(async { Ok(()) })),
        )
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
