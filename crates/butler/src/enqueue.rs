//! Enqueue layers, like ActiveJob's `before_enqueue`: they see every job
//! before it is stored, and can add metadata to it or veto it.
//!
//! ```ignore
//! butler::configure_enqueue(|job: &mut butler::NewJob| {
//!     if maintenance_mode() && job.queue == "mailers" {
//!         return Err("mail is paused for maintenance".into());
//!     }
//!     job.meta.insert("tenant".into(), current_tenant().into());
//!     Ok::<_, butler::BoxError>(())
//! });
//! ```

use std::sync::{Arc, PoisonError, RwLock};

use crate::{BoxError, Error, NewJob, Result, is_valid_queue_name};

/// Runs before a job is enqueued: through a `#[job]` call, a
/// [`PreparedJob`](crate::PreparedJob), or [`enqueue_all`](crate::enqueue_all).
/// Register it with [`configure_enqueue`].
///
/// It may change the [`NewJob`]: add to its [`meta`](NewJob::meta), which is
/// stored with the job and read by worker layers from the
/// [`JobContext`](crate::JobContext), or even move it to another queue. An
/// `Err` vetoes the job: nothing is stored, and the enqueue returns
/// [`Error::Vetoed`] with it as the source.
///
/// It runs in the caller, before the backend call, so keep it quick. It is
/// implemented for closures taking `&mut NewJob` and returning
/// `Result<(), E>` for any `E: Into<BoxError>`.
pub trait EnqueueLayer: Send + Sync + 'static {
    fn enqueue(&self, job: &mut NewJob) -> std::result::Result<(), BoxError>;
}

impl<F, E> EnqueueLayer for F
where
    F: Fn(&mut NewJob) -> std::result::Result<(), E> + Send + Sync + 'static,
    E: Into<BoxError>,
{
    fn enqueue(&self, job: &mut NewJob) -> std::result::Result<(), BoxError> {
        self(job).map_err(Into::into)
    }
}

static LAYERS: RwLock<Vec<Arc<dyn EnqueueLayer>>> = RwLock::new(Vec::new());

/// Adds an enqueue layer for every job this process enqueues from now on,
/// after the ones added before: they run in the order they were added. Like
/// [`configure`](crate::configure), it is process-wide.
pub fn configure_enqueue(layer: impl EnqueueLayer) {
    LAYERS
        .write()
        .unwrap_or_else(PoisonError::into_inner)
        .push(Arc::new(layer));
}

/// Runs every enqueue layer on `job`, in order. The first veto stops it.
pub(crate) fn apply(job: &mut NewJob) -> Result<()> {
    // Cloned out, so a layer that enqueues or adds a layer can't deadlock.
    let layers = {
        let layers = LAYERS.read().unwrap_or_else(PoisonError::into_inner);
        if layers.is_empty() {
            return Ok(());
        }
        layers.clone()
    };
    for layer in layers {
        layer.enqueue(job).map_err(|source| Error::Vetoed {
            name: job.name.clone(),
            source,
        })?;
    }
    crate::keys::validate(job.concurrency.as_ref())?;
    if !is_valid_queue_name(&job.queue) {
        return Err(Error::InvalidQueue {
            name: job.queue.clone(),
            reason: "an enqueue layer set it; use 1 to 64 of A-Z a-z 0-9 _ - . (not starting with a dot)",
        });
    }
    Ok(())
}
