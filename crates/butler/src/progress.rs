//! Job continuations, like ActiveJob's `Continuable`: a long job saves its
//! progress at checkpoints, and resumes from the last one after a worker
//! shutdown, a crash, or a failed attempt, instead of starting over.
//!
//! ```ignore
//! #[derive(Default, Serialize, Deserialize)]
//! enum Import {
//!     #[default]
//!     Start,
//!     Records { after: u64 },
//!     Finalize,
//! }
//!
//! #[butler::job]
//! async fn process_import(id: u64, mut progress: Progress<Import>) -> Result<(), ImportError> {
//!     loop {
//!         match *progress {
//!             Import::Start => {
//!                 initialize(id).await?;
//!                 progress.set(Import::Records { after: 0 }).await?;
//!             }
//!             Import::Records { after } => {
//!                 for record in records_after(id, after).await? {
//!                     record.process().await?;
//!                     progress.set(Import::Records { after: record.id }).await?;
//!                 }
//!                 progress.set(Import::Finalize).await?;
//!             }
//!             Import::Finalize => return finalize(id).await,
//!         }
//!     }
//! }
//! ```
//!
//! A worker shutdown interrupts the job at its next checkpoint. Each such
//! interruption is counted in the job's
//! [`resumptions`](crate::JobRecord::resumptions); past `max_resumptions`
//! (`#[job(max_resumptions = N)]`, or the worker's setting), one more counts
//! as a failed attempt, so a job interrupted at every deploy is bounded by its
//! retry policy instead of cycling forever.
//!
//! [`Progress::requeue`] ends the current execution on purpose, like
//! ActiveJob's isolated steps: the job goes back on its queue, and the next
//! step starts in a fresh execution, possibly on another worker.

use std::{
    fmt,
    future::Future,
    ops::Deref,
    pin::Pin,
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicU8, Ordering},
    },
    time::{Duration, Instant},
};

use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;

use crate::{JobError, error::Chain};

/// Returned by a checkpoint when the worker is stopping: propagate it with
/// `?`, and the job goes back on its queue with its progress, to resume from
/// this checkpoint. It doesn't count as a failed attempt.
///
/// A job's error type opts in with a variant such as
/// `#[error(transparent)] Interrupted(#[from] butler::Interrupted)`;
/// `anyhow::Error` and [`BoxError`](crate::BoxError) accept it as they are.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("interrupted at a checkpoint: the worker is stopping, and the job will resume from here")]
pub struct Interrupted;

/// A job's saved progress, of a type you define: typically an enum of steps,
/// each carrying its own cursor. Add it as a parameter of an `async` job, and
/// the worker provides it: `S::default()` on a first run, the last saved
/// value when the job resumes. It isn't part of what callers pass.
///
/// Read it through `Deref` (`*progress`, `progress.get()`), and move forward
/// with [`set`](Progress::set), which is a checkpoint.
///
/// `#[job(max_resumptions = N)]` bounds how many worker shutdowns may
/// interrupt the job without counting an attempt:
///
/// ```no_run
/// #[butler::job(max_resumptions = 5)]
/// async fn import(id: u64, progress: butler::Progress<u64>) {}
/// # fn main() {}
/// ```
///
/// A job without a `Progress` is never interrupted, so it can't set one:
///
/// ```compile_fail
/// #[butler::job(max_resumptions = 5)]
/// async fn import(id: u64) {}
/// # fn main() {}
/// ```
pub struct Progress<S> {
    state: S,
    checkpoints: Checkpoints,
}

impl<S: Serialize + DeserializeOwned + Default> Progress<S> {
    #[doc(hidden)]
    pub fn resume(checkpoints: &Checkpoints) -> Result<Self, JobError> {
        let state = match &checkpoints.0.saved {
            Some(saved) => serde_json::from_value(saved.clone()).map_err(JobError::BadProgress)?,
            None => S::default(),
        };
        Ok(Self {
            state,
            checkpoints: checkpoints.clone(),
        })
    }

    pub fn get(&self) -> &S {
        &self.state
    }

    /// Whether this run started from saved progress rather than the default.
    pub fn is_resumed(&self) -> bool {
        self.checkpoints.0.saved.is_some()
    }

    /// Moves to `next` and checkpoints: see [`checkpoint`](Progress::checkpoint).
    pub async fn set(&mut self, next: S) -> Result<(), Interrupted> {
        self.state = next;
        self.checkpoint().await
    }

    /// Moves to `next` and ends this execution, like ActiveJob's isolated
    /// steps: always returns [`Interrupted`], and once the job returns it
    /// (propagate it with `?`), the worker puts the job back on its queue
    /// with `next` as its progress, as for a retry. The next step then starts
    /// in a fresh execution, possibly on another worker.
    ///
    /// Use it before a long step that shouldn't share an execution with the
    /// ones before it, for example to give it a full shutdown deadline. It
    /// counts neither as a failed attempt nor towards `max_resumptions`. In
    /// [inline tests](crate::testing) and with `.now()`, the job resumes at
    /// once.
    ///
    /// ```ignore
    /// Import::Records { after } => {
    ///     // ... import the records ...
    ///     progress.requeue(Import::Finalize)?;
    /// }
    /// ```
    pub fn requeue(&mut self, next: S) -> Result<(), Interrupted> {
        self.state = next;
        // The worker stores this with the job as it requeues it: no save here.
        self.remember();
        self.checkpoints.0.stop(Interruption::Requeued);
        Err(Interrupted)
    }

    /// Serializes the current progress as the latest, and returns it.
    fn remember(&self) -> Option<Value> {
        match serde_json::to_value(&self.state) {
            Ok(value) => {
                *self
                    .checkpoints
                    .0
                    .latest
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner) = Some(value.clone());
                Some(value)
            }
            Err(err) => {
                tracing::error!(error = %err, "job progress could not be serialized");
                None
            }
        }
    }

    /// A checkpoint: remembers the current progress, saves it to the backend
    /// if the last save is older than the worker's `checkpoint_interval`, and
    /// returns [`Interrupted`] if the worker is stopping.
    ///
    /// Call it where resuming is safe: after a unit of work is done, before
    /// the next starts.
    pub async fn checkpoint(&mut self) -> Result<(), Interrupted> {
        let inner = &self.checkpoints.0;
        if let Some(value) = self.remember().filter(|_| inner.save_due()) {
            match (inner.save)(value).await {
                Ok(()) => inner.saved_now(),
                // Best effort: the worker also stores the latest progress
                // when the job fails or is interrupted.
                Err(err) => {
                    tracing::warn!(error = %Chain(&err), "could not save job progress")
                }
            }
        }
        if (inner.should_stop)() {
            inner.stop(Interruption::Stopping);
            return Err(Interrupted);
        }
        Ok(())
    }
}

impl<S> Deref for Progress<S> {
    type Target = S;

    fn deref(&self) -> &S {
        &self.state
    }
}

impl<S: fmt::Debug> fmt::Debug for Progress<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Progress").field(&self.state).finish()
    }
}

/// Saves progress to the backend.
pub(crate) type Save = Box<dyn Fn(Value) -> SaveFuture + Send + Sync>;
pub(crate) type SaveFuture = Pin<Box<dyn Future<Output = crate::Result<()>> + Send>>;

/// Why a run of a job with a [`Progress`] returned [`Interrupted`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Interruption {
    /// A checkpoint saw the worker stopping: one more resumption.
    Stopping,
    /// The job asked for it with [`Progress::requeue`].
    Requeued,
}

impl Interruption {
    fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::Stopping),
            2 => Some(Self::Requeued),
            _ => None,
        }
    }

    fn as_u8(self) -> u8 {
        match self {
            Self::Stopping => 1,
            Self::Requeued => 2,
        }
    }
}

/// What the worker gives a job run so its [`Progress`] can resume and
/// checkpoint. Shared: the worker reads back the latest progress and whether
/// the run was interrupted.
#[doc(hidden)]
#[derive(Clone)]
pub struct Checkpoints(Arc<Inner>);

struct Inner {
    /// Progress to resume from, if any.
    saved: Option<Value>,
    latest: Mutex<Option<Value>>,
    /// The last [`Interruption`] of this run, as its `as_u8`; 0 for none.
    interrupted: AtomicU8,
    should_stop: Box<dyn Fn() -> bool + Send + Sync>,
    save: Save,
    interval: Duration,
    last_save: Mutex<Option<Instant>>,
}

impl Inner {
    fn stop(&self, why: Interruption) {
        self.interrupted.store(why.as_u8(), Ordering::Release);
    }

    fn save_due(&self) -> bool {
        self.last_save
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_none_or(|at| at.elapsed() >= self.interval)
    }

    fn saved_now(&self) {
        *self
            .last_save
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(Instant::now());
    }
}

impl Checkpoints {
    pub(crate) fn new(
        saved: Option<Value>,
        interval: Duration,
        should_stop: impl Fn() -> bool + Send + Sync + 'static,
        save: Save,
    ) -> Self {
        Self(Arc::new(Inner {
            saved,
            latest: Mutex::new(None),
            interrupted: AtomicU8::new(0),
            should_stop: Box::new(should_stop),
            save,
            interval,
            last_save: Mutex::new(None),
        }))
    }

    /// Why this run returned [`Interrupted`], if it did.
    pub(crate) fn interruption(&self) -> Option<Interruption> {
        Interruption::from_u8(self.0.interrupted.load(Ordering::Acquire))
    }

    /// Whether a checkpoint or [`Progress::requeue`] returned [`Interrupted`]
    /// during this run.
    pub(crate) fn interrupted(&self) -> bool {
        self.interruption().is_some()
    }

    /// The progress at the last checkpoint of this run, if any.
    pub(crate) fn latest(&self) -> Option<Value> {
        self.0
            .latest
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

/// The arguments and checkpoint context of one job run, as the worker passes
/// them to a job's generated dispatch function.
#[doc(hidden)]
pub struct Invocation {
    pub args: Vec<Value>,
    pub checkpoints: Checkpoints,
}
