//! What calling a `#[job]` function returns: a [`JobCall`], which enqueues
//! when awaited, or runs the job right here with [`now`](JobCall::now).

use std::{future::Future, marker::PhantomData, pin::Pin, time::Duration};

use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::{
    JobDef, JobError, JobHandle, Result,
    progress::{Checkpoints, Interruption, Invocation},
};

/// The future an awaited [`JobCall`] becomes: it enqueues the job and returns
/// its handle.
pub type Enqueueing<T> = Pin<Box<dyn Future<Output = Result<JobHandle<T>>> + Send + 'static>>;

/// A call to a `#[job]` function, with its arguments already converted and
/// serialized. `.await` it to enqueue the job, like ActiveJob's
/// `perform_later`, or run the body in this process with
/// [`now`](JobCall::now), like `perform_now`:
///
/// ```no_run
/// #[butler::job]
/// async fn double(n: u32) -> Result<u32, std::io::Error> {
///     Ok(n * 2)
/// }
///
/// # async fn f() -> Result<(), Box<dyn std::error::Error>> {
/// let job: butler::JobHandle<u32> = double(21).await?; // queued for a worker
/// let n: u32 = double(21).now().await?;                // ran here, now
/// # Ok(())
/// # }
/// # fn main() {}
/// ```
///
/// The two never mix up: awaiting the call gives a handle, not the job's
/// output,
///
/// ```compile_fail
/// # #[butler::job]
/// # async fn double(n: u32) -> Result<u32, std::io::Error> { Ok(n * 2) }
/// # async fn f() {
/// let n: Result<u32, butler::JobError> = double(21).await;
/// # }
/// # fn main() {}
/// ```
///
/// and running it now gives the output, not a handle:
///
/// ```compile_fail
/// # #[butler::job]
/// # async fn double(n: u32) -> Result<u32, std::io::Error> { Ok(n * 2) }
/// # async fn f() {
/// let job: butler::JobHandle<u32> = double(21).now().await.unwrap();
/// # }
/// # fn main() {}
/// ```
///
/// A `JobCall` is `Send + 'static`, and never borrows the arguments it was
/// called with. It is not itself a future: to pass the enqueue to
/// `tokio::spawn`, use [`enqueue`](JobCall::enqueue).
#[must_use = "a job call does nothing until it is awaited (to enqueue it) or run with `.now()`"]
pub struct JobCall<T> {
    def: &'static JobDef,
    args: serde_json::Result<Vec<Value>>,
    output: PhantomData<fn() -> T>,
}

impl<T> JobCall<T> {
    #[doc(hidden)]
    pub fn new(def: &'static JobDef, args: serde_json::Result<Vec<Value>>) -> Self {
        Self {
            def,
            args,
            output: PhantomData,
        }
    }

    /// The job's name, as workers look it up.
    pub fn name(&self) -> &'static str {
        self.def.name
    }

    /// The job and its serialized arguments, for test assertions.
    pub(crate) fn into_parts(self) -> (&'static JobDef, serde_json::Result<Vec<Value>>) {
        (self.def, self.args)
    }
}

impl<T: DeserializeOwned + Send + 'static> JobCall<T> {
    /// Enqueues the job, as `.await` on the call does, as a named future:
    /// for `tokio::spawn` and other places that take a `Future` rather than
    /// an `IntoFuture`.
    pub fn enqueue(self) -> Enqueueing<T> {
        let Self { def, args, .. } = self;
        Box::pin(async move { crate::__private::enqueue(def, args?).await })
    }

    /// Runs the job's body right now, in this process, and returns its
    /// output, like ActiveJob's `perform_now`. Nothing is enqueued, and no
    /// worker is involved.
    ///
    /// It runs once: an error is returned as it is, as
    /// [`JobError::Failed`] holding the job's own error (get it back with
    /// [`Failure::into_inner`](crate::Failure::into_inner) and `downcast`),
    /// with no retry, backoff or scheduling. The arguments are serialized and
    /// decoded as a worker would, through the job's generated code. A
    /// [`Progress`](crate::Progress) starts from its default and its
    /// checkpoints are never saved or interrupted.
    ///
    /// A plain `fn` job runs on tokio's blocking pool inside a runtime, and
    /// in place elsewhere (under [`block_on`](crate::block_on), say); a panic
    /// there is returned as [`JobError::Panicked`]. An `async fn` job runs in
    /// the caller's task, so a panic in it propagates to the caller, like any
    /// function call.
    pub fn now(self) -> impl Future<Output = Result<T, JobError>> + Send + 'static {
        let Self { def, args, .. } = self;
        async move {
            let args = args.map_err(|source| JobError::Arguments {
                job: def.name,
                source,
            })?;
            let output = run_now(def, args).await?;
            serde_json::from_value(output).map_err(JobError::Output)
        }
    }
}

impl<T: DeserializeOwned + Send + 'static> IntoFuture for JobCall<T> {
    type Output = Result<JobHandle<T>>;
    type IntoFuture = Enqueueing<T>;

    fn into_future(self) -> Self::IntoFuture {
        self.enqueue()
    }
}

/// Runs a job's generated code directly: no queue, no retries, and
/// checkpoints that neither save nor stop. A step that asks to be requeued
/// ([`Progress::requeue`](crate::Progress::requeue)) resumes at once, from
/// the progress it set.
pub(crate) async fn run_now(def: &'static JobDef, args: Vec<Value>) -> Result<Value, JobError> {
    let mut saved = None;
    loop {
        let checkpoints = Checkpoints::new(
            saved,
            Duration::ZERO,
            || false,
            Box::new(|_| Box::pin(async { Ok(()) })),
        );
        let invocation = Invocation {
            args: args.clone(),
            checkpoints: checkpoints.clone(),
        };
        match (def.perform)(invocation).await {
            Err(_) if checkpoints.interruption() == Some(Interruption::Requeued) => {
                saved = checkpoints.latest();
            }
            outcome => return outcome,
        }
    }
}
