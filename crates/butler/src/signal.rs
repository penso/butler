use std::{
    collections::HashMap,
    sync::{Arc, Condvar, Mutex, PoisonError, Weak},
    time::Duration,
};

/// A counter that moves whenever something may have changed, so waiters wake
/// at once instead of polling. Backends notify it; waiters remember the counter
/// before checking, then wait for it to move past that value, which closes the
/// gap between the check and the wait.
///
/// Waiting works both ways: [`Signal::wait_past`] blocks the thread (a
/// condition variable), and [`Signal::changed_past`], inside a tokio runtime,
/// awaits a `tokio::sync::Notify` without holding a thread.
#[derive(Default)]
pub struct Signal {
    generation: Mutex<u64>,
    changed: Condvar,
    #[cfg(feature = "tokio")]
    notify: tokio::sync::Notify,
}

impl Signal {
    pub fn generation(&self) -> u64 {
        *self
            .generation
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Wakes every waiter.
    pub fn notify(&self) {
        *self
            .generation
            .lock()
            .unwrap_or_else(PoisonError::into_inner) += 1;
        self.changed.notify_all();
        #[cfg(feature = "tokio")]
        self.notify.notify_waiters();
    }

    /// Blocks until the counter moves past `seen`, or `timeout` passes.
    pub fn wait_past(&self, seen: u64, timeout: Duration) {
        let guard = self
            .generation
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let _ = self
            .changed
            .wait_timeout_while(guard, timeout, |generation| *generation == seen);
    }

    /// Like [`Signal::wait_past`], but inside a tokio runtime it awaits instead
    /// of blocking a thread.
    pub async fn changed_past(&self, seen: u64, timeout: Duration) {
        #[cfg(feature = "tokio")]
        if tokio::runtime::Handle::try_current().is_ok() {
            // Registered before the check: a `notify` in between still wakes it.
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.generation() != seen {
                return;
            }
            let _ = tokio::time::timeout(timeout, notified).await;
            return;
        }
        self.wait_past(seen, timeout);
    }
}

/// Per-job signals for jobs someone is waiting on, so finishing one job wakes
/// only its waiters. A single shared signal would wake every waiter on every
/// finish, and each would re-read its job: quadratic with many waiters.
#[derive(Default)]
pub struct JobWatch {
    watched: Mutex<HashMap<String, Weak<Signal>>>,
}

impl JobWatch {
    /// The signal for `id`, shared by everyone waiting on it. It stays
    /// registered while any waiter holds it.
    pub fn watch(&self, id: &str) -> Arc<Signal> {
        let mut watched = self.watched.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(signal) = watched.get(id).and_then(Weak::upgrade) {
            return signal;
        }
        // Drop entries whose waiters are gone, now and then, as the map grows.
        if watched.len() >= 64 && watched.len().is_power_of_two() {
            watched.retain(|_, signal| signal.strong_count() > 0);
        }
        let signal = Arc::new(Signal::default());
        watched.insert(id.to_owned(), Arc::downgrade(&signal));
        signal
    }

    /// Wakes whoever waits on `id`; a no-op if nobody does.
    pub fn notify(&self, id: &str) {
        let signal = self
            .watched
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(id)
            .and_then(Weak::upgrade);
        if let Some(signal) = signal {
            signal.notify();
        }
    }

    /// Wakes every waiter, for when a backend can't tell which jobs changed
    /// (say, after reconnecting).
    pub fn notify_all(&self) {
        let signals: Vec<_> = self
            .watched
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .filter_map(Weak::upgrade)
            .collect();
        for signal in signals {
            signal.notify();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        thread,
        time::{Duration, Instant},
    };

    use super::{Arc, JobWatch, Signal};

    #[test]
    fn a_notify_after_the_snapshot_is_never_missed() {
        let signal = Signal::default();
        let seen = signal.generation();
        signal.notify();
        let started = Instant::now();
        signal.wait_past(seen, Duration::from_secs(5));
        assert!(started.elapsed() < Duration::from_millis(100));
    }

    #[test]
    fn a_waiter_wakes_when_notified_from_another_thread() {
        let signal = Arc::new(Signal::default());
        let seen = signal.generation();
        let notifier = {
            let signal = Arc::clone(&signal);
            thread::spawn(move || {
                thread::sleep(Duration::from_millis(50));
                signal.notify();
            })
        };
        let started = Instant::now();
        signal.wait_past(seen, Duration::from_secs(5));
        assert!(started.elapsed() < Duration::from_secs(1));
        notifier.join().unwrap_or_default();
    }

    #[test]
    fn finishing_one_job_wakes_only_its_waiters() {
        let watch = JobWatch::default();
        let a = watch.watch("a");
        let b = watch.watch("b");
        let (seen_a, seen_b) = (a.generation(), b.generation());
        watch.notify("a");
        assert_ne!(a.generation(), seen_a);
        assert_eq!(b.generation(), seen_b, "b's waiters were not woken");
        assert!(
            Arc::ptr_eq(&a, &watch.watch("a")),
            "waiters on one job share a signal"
        );
        watch.notify("nobody-waits");
        watch.notify_all();
        assert_ne!(b.generation(), seen_b);
    }
}
