use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

/// Per-queue caps on how many jobs run at once. Each limited queue is a
/// counting semaphore (an atomic counter), usable from threads and async
/// tasks alike; queues without a limit are only bound by the worker's
/// overall `concurrency`.
#[derive(Clone, Debug, Default)]
pub(crate) struct QueueLimits {
    slots: HashMap<String, Arc<Slots>>,
}

#[derive(Debug)]
pub(crate) struct Slots {
    running: AtomicUsize,
    max: usize,
}

/// A running (or about to run) job's place in its queue's limit. Dropping it
/// frees the place.
#[derive(Debug)]
pub(crate) enum Permit {
    Unlimited,
    Limited(Arc<Slots>),
}

impl Drop for Permit {
    fn drop(&mut self) {
        if let Permit::Limited(slots) = self {
            slots.running.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

impl QueueLimits {
    pub(crate) fn set(&mut self, queue: &str, max: usize) {
        self.slots.insert(
            queue.to_owned(),
            Arc::new(Slots {
                running: AtomicUsize::new(0),
                max: max.max(1),
            }),
        );
    }

    pub(crate) fn limits(&self) -> impl Iterator<Item = (&str, usize)> {
        self.slots
            .iter()
            .map(|(queue, slots)| (queue.as_str(), slots.max))
    }

    /// Takes a place in `queue`'s limit, or `None` if it is full.
    pub(crate) fn try_acquire(&self, queue: &str) -> Option<Permit> {
        let Some(slots) = self.slots.get(queue) else {
            return Some(Permit::Unlimited);
        };
        slots
            .running
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |running| {
                (running < slots.max).then_some(running + 1)
            })
            .ok()
            .map(|_| Permit::Limited(Arc::clone(slots)))
    }

    /// Jobs running on `queue` right now, if it is limited.
    #[cfg(test)]
    fn running(&self, queue: &str) -> Option<usize> {
        self.slots
            .get(queue)
            .map(|slots| slots.running.load(Ordering::Acquire))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_limited_queue_hands_out_at_most_its_limit() {
        let mut limits = QueueLimits::default();
        limits.set("mailers", 2);

        let first = limits.try_acquire("mailers");
        let second = limits.try_acquire("mailers");
        assert!(first.is_some() && second.is_some());
        assert!(limits.try_acquire("mailers").is_none(), "limit reached");
        assert_eq!(limits.running("mailers"), Some(2));

        drop(first);
        assert!(limits.try_acquire("mailers").is_some(), "a slot freed up");
    }

    #[test]
    fn queues_without_a_limit_are_never_full() {
        let limits = QueueLimits::default();
        let permits: Vec<_> = (0..10_000).map(|_| limits.try_acquire("default")).collect();
        assert!(permits.iter().all(Option::is_some));
    }

    #[test]
    fn concurrent_acquires_never_exceed_the_limit() {
        let mut limits = QueueLimits::default();
        limits.set("q", 7);
        let limits = Arc::new(limits);
        let held: Vec<_> = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..64)
                .map(|_| {
                    let limits = Arc::clone(&limits);
                    scope.spawn(move || limits.try_acquire("q"))
                })
                .collect();
            handles
                .into_iter()
                .filter_map(|h| h.join().ok().flatten())
                .collect()
        });
        assert_eq!(held.len(), 7);
        assert_eq!(limits.running("q"), Some(7));
    }
}
