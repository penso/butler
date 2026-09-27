//! Recording enqueues without running them, like ActiveJob's
//! `assert_enqueued_with` and `assert_no_enqueued_jobs`.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex, PoisonError},
    task::{Context, Poll},
};

use serde::de::DeserializeOwned;

use super::{Scope, enter};
use crate::{JobCall, JobDef, JobError, JobHandle, JobId, MemoryQueue, NewJob, Result, Store};

/// A scope that records the jobs enqueued inside it instead of enqueueing
/// them: nothing runs, and no queue is touched, configured or not.
///
/// ```
/// use butler::testing::RecordedJobs;
///
/// #[butler::job(queue = "mailers")]
/// async fn send_email(to: String, subject: String) -> Result<(), std::io::Error> {
///     Ok(())
/// }
///
/// async fn signup(email: &str) -> butler::Result<()> {
///     // ... create the account, then:
///     send_email(email, "Welcome").await?;
///     Ok(())
/// }
///
/// # fn main() -> butler::Result<()> {
/// let jobs = RecordedJobs::new();
/// butler::block_on(jobs.record(signup("ada@example.com")))?;
///
/// // Same job, same arguments, converted as the call converts them.
/// let email = jobs.assert_enqueued_with(send_email("ada@example.com", "Welcome"));
/// assert_eq!(email.job.queue, "mailers");
/// assert_eq!(jobs.enqueued_names(), ["send_email"]);
///
/// // Or check part of the arguments, decoded as the job decodes them.
/// jobs.assert_enqueued(&send_email::JOB, |job| {
///     job.arg::<String>(0).is_ok_and(|to| to.ends_with("@example.com"))
/// });
///
/// jobs.clear();
/// jobs.assert_no_enqueued_jobs();
/// # Ok(())
/// # }
/// ```
///
/// Every enqueue path is recorded: awaiting a `#[job]` call or
/// [`JobCall::enqueue`], [`PreparedJob::enqueue`](crate::PreparedJob::enqueue)
/// (with its queue and run time), and [`enqueue_all`](crate::enqueue_all),
/// one entry per job, in order. Enqueue layers run first, as they would for
/// a real enqueue, so the record is what would be stored: their
/// [`meta`](NewJob::meta), a queue they changed, and a veto, which returns
/// [`Error::Vetoed`](crate::Error::Vetoed) and records nothing. Recurring
/// schedules are enqueued by workers, not here, and aren't recorded.
///
/// Each enqueue returns a real [`JobHandle`], backed by a private in-memory
/// store: its state is [`Pending`](crate::JobState::Pending), or
/// [`Scheduled`](crate::JobState::Scheduled) with a run time, and it stays
/// so, since nothing runs it. `result()` returns `None`, [`cancel`](JobHandle::cancel)
/// works (the job stays recorded), and `wait` or `wait_result` never
/// return. The [concurrency](NewJob::concurrency) and
/// [unique](NewJob::unique) keys are recorded but not enforced: enqueueing
/// the same unique job twice records it twice, with two handles.
///
/// Like [`InlineJobs`](super::InlineJobs), recording follows the future you
/// pass to [`record`](RecordedJobs::record): it is on while that future is
/// being polled, on whatever thread polls it. Work you `tokio::spawn` or run
/// on another thread from inside is not part of it, and enqueues normally.
/// It needs no runtime.
#[derive(Clone, Default)]
pub struct RecordedJobs {
    store: MemoryQueue,
    enqueued: Arc<Mutex<Vec<EnqueuedJob>>>,
}

impl RecordedJobs {
    pub fn new() -> Self {
        Self::default()
    }

    /// Runs `body`, recording every job it enqueues instead of enqueueing
    /// it. Returns `body`'s output.
    pub fn record<F: Future>(&self, body: F) -> Recording<F> {
        Recording {
            body: Box::pin(body),
            jobs: self.clone(),
        }
    }

    /// Every job recorded so far, in the order it was enqueued.
    pub fn enqueued(&self) -> Vec<EnqueuedJob> {
        self.lock().clone()
    }

    /// The names of the jobs recorded so far, in the order they were enqueued.
    pub fn enqueued_names(&self) -> Vec<String> {
        self.lock().iter().map(|job| job.job.name.clone()).collect()
    }

    /// The recorded jobs of kind `def` (such as `&send_email::JOB`), in order.
    pub fn enqueued_of(&self, def: &JobDef) -> Vec<EnqueuedJob> {
        self.lock()
            .iter()
            .filter(|job| job.is(def))
            .cloned()
            .collect()
    }

    /// Forgets the jobs recorded so far, to check only what comes next. The
    /// handles already returned keep working.
    pub fn clear(&self) {
        self.lock().clear();
    }

    /// Returns the first recorded job of kind `def` for which `matches`
    /// holds.
    ///
    /// # Panics
    ///
    /// If there is none, listing what was recorded.
    #[track_caller]
    pub fn assert_enqueued(
        &self,
        def: &JobDef,
        matches: impl Fn(&EnqueuedJob) -> bool,
    ) -> EnqueuedJob {
        let enqueued = self.enqueued();
        match enqueued.iter().find(|job| job.is(def) && matches(job)) {
            Some(job) => job.clone(),
            None => panic!(
                "expected a matching {} job to be enqueued; recorded: {}",
                def.name,
                Listing(&enqueued)
            ),
        }
    }

    /// Returns the first recorded job with the same name and arguments as
    /// `call`, such as `send_email("ada@example.com", "Welcome")`: the
    /// arguments are converted and serialized as the call converts them, so
    /// the check is typed like the job. Its queue, run time and keys are on
    /// the returned job.
    ///
    /// # Panics
    ///
    /// If there is none, listing what was recorded, or if `call`'s arguments
    /// don't serialize.
    #[track_caller]
    pub fn assert_enqueued_with<T>(&self, call: JobCall<T>) -> EnqueuedJob {
        let (def, args) = call.into_parts();
        let args = match args {
            Ok(args) => args,
            Err(err) => panic!(
                "the expected {} call's arguments don't serialize: {err}",
                def.name
            ),
        };
        let enqueued = self.enqueued();
        match enqueued
            .iter()
            .find(|job| job.is(def) && job.job.args == args)
        {
            Some(job) => job.clone(),
            None => panic!(
                "expected {}({}) to be enqueued; recorded: {}",
                def.name,
                Args(&args),
                Listing(&enqueued)
            ),
        }
    }

    /// # Panics
    ///
    /// If any job was recorded (since the last [`clear`](RecordedJobs::clear)),
    /// listing them.
    #[track_caller]
    pub fn assert_no_enqueued_jobs(&self) {
        let enqueued = self.enqueued();
        if !enqueued.is_empty() {
            panic!(
                "expected no enqueued jobs; recorded: {}",
                Listing(&enqueued)
            );
        }
    }

    /// Records one job, as the enqueue call's replacement.
    pub(crate) fn record_job<T>(&self, def: &'static JobDef, job: NewJob) -> Result<JobHandle<T>> {
        let mut stored = job.clone();
        // Recorded, not enforced: a duplicate gets its own handle.
        stored.unique = None;
        let id = self.store.push(stored)?;
        self.lock().push(EnqueuedJob {
            id: id.clone(),
            job,
            def,
        });
        Ok(JobHandle::new(self.store.clone().into(), id))
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Vec<EnqueuedJob>> {
        self.enqueued.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// A job recorded by [`RecordedJobs`]: what would have been stored, after
/// the enqueue layers ran.
#[derive(Clone)]
pub struct EnqueuedJob {
    /// The id of the handle its enqueue returned.
    pub id: JobId,
    /// Its name, queue, arguments as JSON, run time, meta, and keys.
    pub job: NewJob,
    def: &'static JobDef,
}

impl EnqueuedJob {
    /// Whether it is a job of kind `def`, such as `&send_email::JOB`.
    pub fn is(&self, def: &JobDef) -> bool {
        self.job.name == def.name
    }

    /// The job's definition, as its enqueue call named it.
    pub fn def(&self) -> &'static JobDef {
        self.def
    }

    /// Its argument at `index` (0-based, `Progress` excluded), decoded as
    /// the job would decode it.
    pub fn arg<T: DeserializeOwned>(&self, index: usize) -> Result<T, JobError> {
        let mut args = self.job.args.iter().skip(index).cloned();
        crate::__private::arg(&mut args, self.def.name, index)
    }
}

impl fmt::Debug for EnqueuedJob {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EnqueuedJob")
            .field("id", &self.id)
            .field("job", &self.job)
            .finish()
    }
}

/// The future returned by [`RecordedJobs::record`].
pub struct Recording<F> {
    body: Pin<Box<F>>,
    jobs: RecordedJobs,
}

impl<F: Future> Future for Recording<F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        let this = &mut *self;
        let _restore = enter(Scope::Record(this.jobs.clone()));
        this.body.as_mut().poll(cx)
    }
}

/// Recorded jobs, for assertion messages: `none`, or one `name(args) on
/// queue` per line.
struct Listing<'a>(&'a [EnqueuedJob]);

impl fmt::Display for Listing<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0.is_empty() {
            return f.write_str("none");
        }
        for job in self.0 {
            let job = &job.job;
            write!(
                f,
                "\n  {}({}) on {:?}",
                job.name,
                Args(&job.args),
                job.queue
            )?;
            if let Some(at) = job.run_at {
                write!(f, " at {at:?}")?;
            }
        }
        Ok(())
    }
}

struct Args<'a>(&'a [serde_json::Value]);

impl fmt::Display for Args<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, arg) in self.0.iter().enumerate() {
            if index > 0 {
                f.write_str(", ")?;
            }
            write!(f, "{arg}")?;
        }
        Ok(())
    }
}
