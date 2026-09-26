use std::{
    hash::{BuildHasher, Hasher, RandomState},
    sync::atomic::{AtomicU64, Ordering},
};

use crate::job::{DEFAULT_QUEUE, is_valid_queue_name};

/// Which queues a worker serves, and in what order it checks them. Jobs on
/// queues not listed here are never run by this worker.
///
/// Mirrors Sidekiq's queue configuration:
///
/// - [`Strict`](QueuePriority::Strict): always in list order. A queue only gets
///   a turn when every queue before it is empty, so a busy first queue can
///   starve the rest.
/// - [`Weighted`](QueuePriority::Weighted): each claim checks the queues in a
///   random order where a queue with weight 6 comes first six times as often
///   as one with weight 1. Every queue keeps moving; heavier ones get most of
///   the workers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum QueuePriority {
    Strict(Vec<String>),
    Weighted(Vec<(String, u32)>),
}

impl Default for QueuePriority {
    fn default() -> Self {
        Self::Strict(vec![DEFAULT_QUEUE.to_owned()])
    }
}

impl QueuePriority {
    /// Strict priority, in the given order.
    pub fn strict<S: Into<String>>(queues: impl IntoIterator<Item = S>) -> Self {
        Self::Strict(queues.into_iter().map(Into::into).collect()).checked()
    }

    /// Weighted priority: `(queue, weight)`, weight at least 1.
    pub fn weighted<S: Into<String>>(queues: impl IntoIterator<Item = (S, u32)>) -> Self {
        Self::Weighted(queues.into_iter().map(|(q, w)| (q.into(), w)).collect()).checked()
    }

    /// Every queue served, in configured order.
    pub fn names(&self) -> Vec<&str> {
        match self {
            Self::Strict(queues) => queues.iter().map(String::as_str).collect(),
            Self::Weighted(queues) => queues.iter().map(|(q, _)| q.as_str()).collect(),
        }
    }

    /// The order to check queues in for one claim.
    pub(crate) fn claim_order(&self) -> Vec<&str> {
        match self {
            Self::Strict(queues) => queues.iter().map(String::as_str).collect(),
            Self::Weighted(queues) => {
                // Efraimidis-Spirakis: key = ln(u) / w, highest key first,
                // gives each queue first place with probability w / sum(w).
                let mut keyed: Vec<(f64, &str)> = queues
                    .iter()
                    .map(|(queue, weight)| {
                        (random_unit().ln() / f64::from(*weight), queue.as_str())
                    })
                    .collect();
                keyed.sort_by(|a, b| b.0.total_cmp(&a.0));
                keyed.into_iter().map(|(_, queue)| queue).collect()
            }
        }
    }

    /// Drops entries that could never be served (bad names, zero weights),
    /// logging each. Falls back to the default queue if nothing is left.
    fn checked(self) -> Self {
        let valid = |queue: &str| {
            let ok = is_valid_queue_name(queue);
            if !ok {
                tracing::error!(queue, "ignoring invalid queue name");
            }
            ok
        };
        let checked = match self {
            Self::Strict(queues) => Self::Strict(queues.into_iter().filter(|q| valid(q)).collect()),
            Self::Weighted(queues) => Self::Weighted(
                queues
                    .into_iter()
                    .filter(|(q, w)| {
                        if *w == 0 {
                            tracing::error!(queue = q, "ignoring queue with weight 0");
                        }
                        *w > 0 && valid(q)
                    })
                    .collect(),
            ),
        };
        if checked.names().is_empty() {
            Self::default()
        } else {
            checked
        }
    }
}

/// A uniform value in (0, 1]. The standard library seeds every `RandomState`
/// randomly, which is plenty for spreading claims across queues.
fn random_unit() -> f64 {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let mut hasher = RandomState::new().build_hasher();
    hasher.write_u64(COUNTER.fetch_add(1, Ordering::Relaxed));
    // 53 random bits, shifted off zero so ln() stays finite.
    ((hasher.finish() >> 11) as f64 + 1.0) / (1u64 << 53) as f64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strict_order_never_changes() {
        let priority = QueuePriority::strict(["critical", "default", "low"]);
        for _ in 0..100 {
            assert_eq!(priority.claim_order(), ["critical", "default", "low"]);
        }
    }

    #[test]
    fn weights_decide_how_often_a_queue_goes_first() {
        let priority = QueuePriority::weighted([("critical", 6), ("default", 3), ("low", 1)]);
        let draws = 20_000;
        let mut first = [0usize; 3];
        for _ in 0..draws {
            let order = priority.claim_order();
            assert_eq!(order.len(), 3, "every queue is still checked");
            let index = ["critical", "default", "low"]
                .iter()
                .position(|q| *q == order[0])
                .unwrap_or(usize::MAX);
            first[index] += 1;
        }
        let share = |n: usize| n as f64 / draws as f64;
        // Expected 0.6 / 0.3 / 0.1; 20k draws keep each within about ±0.01.
        assert!((share(first[0]) - 0.6).abs() < 0.03, "{first:?}");
        assert!((share(first[1]) - 0.3).abs() < 0.03, "{first:?}");
        assert!((share(first[2]) - 0.1).abs() < 0.03, "{first:?}");
    }

    #[test]
    fn bad_entries_are_dropped() {
        let priority = QueuePriority::weighted([("ok", 1), ("../etc", 5), ("zero", 0)]);
        assert_eq!(priority.names(), ["ok"]);
        assert_eq!(
            QueuePriority::strict(Vec::<String>::new()),
            QueuePriority::default()
        );
    }
}
