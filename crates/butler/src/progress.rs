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

use std::{
    fmt,
    future::Future,
    ops::Deref,
    pin::Pin,
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicBool, Ordering},
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

    /// A checkpoint: remembers the current progress, saves it to the backend
    /// if the last save is older than the worker's `checkpoint_interval`, and
    /// returns [`Interrupted`] if the worker is stopping.
    ///
    /// Call it where resuming is safe: after a unit of work is done, before
    /// the next starts.
    pub async fn checkpoint(&mut self) -> Result<(), Interrupted> {
        let inner = &self.checkpoints.0;
        match serde_json::to_value(&self.state) {
            Ok(value) => {
                *inner.latest.lock().unwrap_or_else(PoisonError::into_inner) = Some(value.clone());
                if inner.save_due() {
                    match (inner.save)(value).await {
                        Ok(()) => inner.saved_now(),
                        // Best effort: the worker also stores the latest
                        // progress when the job fails or is interrupted.
                        Err(err) => {
                            tracing::warn!(error = %Chain(&err), "could not save job progress")
                        }
                    }
                }
            }
            Err(err) => tracing::error!(error = %err, "job progress could not be serialized"),
        }
        if (inner.should_stop)() {
            inner.interrupted.store(true, Ordering::Release);
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
    interrupted: AtomicBool,
    should_stop: Box<dyn Fn() -> bool + Send + Sync>,
    save: Save,
    interval: Duration,
    last_save: Mutex<Option<Instant>>,
}

impl Inner {
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
            interrupted: AtomicBool::new(false),
            should_stop: Box::new(should_stop),
            save,
            interval,
            last_save: Mutex::new(None),
        }))
    }

    /// Whether a checkpoint returned [`Interrupted`] during this run.
    pub(crate) fn interrupted(&self) -> bool {
        self.0.interrupted.load(Ordering::Acquire)
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
