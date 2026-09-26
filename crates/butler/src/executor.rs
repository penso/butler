use std::{
    future::Future,
    pin::pin,
    sync::Arc,
    task::{Context, Poll, Wake, Waker},
    thread::{self, Thread},
};

struct ThreadWaker(Thread);

impl Wake for ThreadWaker {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
}

/// Runs a future to completion on the current thread, with no runtime needed.
/// The thread parks until the future's waker fires.
pub fn block_on<F: Future>(fut: F) -> F::Output {
    let mut fut = pin!(fut);
    let waker = Waker::from(Arc::new(ThreadWaker(thread::current())));
    let mut cx = Context::from_waker(&waker);
    loop {
        if let Poll::Ready(out) = fut.as_mut().poll(&mut cx) {
            return out;
        }
        thread::park();
    }
}

/// The message a panic carried, when it was a string.
pub(crate) fn panic_message(panic: &(dyn std::any::Any + Send)) -> String {
    panic
        .downcast_ref::<&str>()
        .map(|s| (*s).to_string())
        .or_else(|| panic.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown panic".into())
}

/// Runs a blocking backend call. Inside a tokio runtime it goes to the
/// blocking thread pool, so async worker threads never wait on I/O; elsewhere
/// it runs in place.
pub(crate) async fn unblock<T: Send + 'static>(
    f: impl FnOnce() -> crate::Result<T> + Send + 'static,
) -> crate::Result<T> {
    #[cfg(feature = "tokio")]
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        return handle.spawn_blocking(f).await?;
    }
    f()
}

/// Sleeps on tokio's timer inside a tokio runtime, or blocks the thread
/// elsewhere (e.g. under [`block_on`], where nothing else shares the thread).
pub(crate) async fn sleep(duration: std::time::Duration) {
    #[cfg(feature = "tokio")]
    if tokio::runtime::Handle::try_current().is_ok() {
        tokio::time::sleep(duration).await;
        return;
    }
    thread::sleep(duration);
}
