//! Retry policies, like ActiveJob's `retry_on` and `discard_on`: how many
//! times a failed job is retried, how long it waits before each retry, and
//! which errors are never worth retrying.
//!
//! ```ignore
//! #[butler::job(retries = 10, backoff = "exponential")]
//! async fn sync_account(id: u64) -> Result<(), SyncError> { /* ... */ }
//!
//! impl butler::Retryable for SyncError {
//!     fn retry(&self) -> butler::Retry {
//!         match self {
//!             SyncError::AccountDeleted => butler::Retry::Never,
//!             SyncError::RateLimited { retry_after } => butler::Retry::After(*retry_after),
//!             _ => butler::Retry::Default,
//!         }
//!     }
//! }
//! ```

use std::{
    fmt,
    hash::{BuildHasher, Hasher, RandomState},
    str::FromStr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde::Deserialize;

use crate::Error;

/// How long a failed job waits before its next attempt. `n` below is the
/// number of attempts that failed so far, starting at 1.
///
/// Every delay gets up to [`JITTER`] more, at random, so jobs that failed
/// together don't all retry at the same moment, and none waits longer than
/// [`MAX_BACKOFF`]. In `butler.toml` and `#[job(backoff = ...)]` it is
/// written `"exponential"`, `"polynomial"` or `"fixed:30s"` (units `ms`, `s`,
/// `m`, `h`, `d`; `"fixed:0s"` retries at once).
///
/// ```compile_fail
/// // Checked when the job is compiled.
/// #[butler::job(backoff = "sometimes")]
/// async fn flaky() {}
/// # fn main() {}
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(try_from = "String")]
pub enum Backoff {
    /// 2ⁿ seconds: 2 s, 4 s, 8 s, 16 s, ... about a day by the 17th retry.
    /// The default.
    #[default]
    Exponential,
    /// n⁴ + 15 seconds, like Sidekiq: 16 s, 31 s, 96 s, 271 s, 640 s, ...
    Polynomial,
    /// The same delay every time.
    Fixed(Duration),
}

/// The most a delay grows by, at random, as a fraction of it.
pub const JITTER: f64 = 0.15;

/// No retry waits longer than this.
pub const MAX_BACKOFF: Duration = Duration::from_secs(30 * 24 * 60 * 60);

impl Backoff {
    /// Retries at once, as butler did before backoff existed.
    pub const NONE: Backoff = Backoff::Fixed(Duration::ZERO);

    /// The delay before the retry that follows `failed` failed attempts,
    /// without jitter.
    pub fn base_delay(self, failed: u32) -> Duration {
        let delay = match self {
            Backoff::Exponential => {
                Duration::from_secs(2u64.checked_pow(failed).unwrap_or(u64::MAX))
            }
            Backoff::Polynomial => Duration::from_secs(
                u64::from(failed)
                    .checked_pow(4)
                    .and_then(|n| n.checked_add(15))
                    .unwrap_or(u64::MAX),
            ),
            Backoff::Fixed(delay) => delay,
        };
        delay.min(MAX_BACKOFF)
    }

    /// The delay with jitter: `sample`, in `0.0..1.0`, picks how much of the
    /// [`JITTER`] it adds.
    pub fn delay_with(self, failed: u32, sample: f64) -> Duration {
        let base = self.base_delay(failed);
        base.saturating_add(base.mul_f64(JITTER * sample.clamp(0.0, 1.0)))
            .min(MAX_BACKOFF)
    }

    /// The delay before the next attempt, with random jitter.
    pub fn delay(self, failed: u32) -> Duration {
        self.delay_with(failed, random_unit())
    }
}

impl fmt::Display for Backoff {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Backoff::Exponential => f.write_str("exponential"),
            Backoff::Polynomial => f.write_str("polynomial"),
            Backoff::Fixed(delay) => {
                let ms = delay.as_millis();
                let (value, unit) = [
                    (86_400_000, "d"),
                    (3_600_000, "h"),
                    (60_000, "m"),
                    (1_000, "s"),
                ]
                .into_iter()
                .find(|(size, _)| ms > 0 && ms % size == 0)
                .map_or((ms, "ms"), |(size, unit)| (ms / size, unit));
                write!(f, "fixed:{value}{unit}")
            }
        }
    }
}

impl FromStr for Backoff {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self, Error> {
        let invalid = |reason| Error::InvalidBackoff {
            value: value.to_owned(),
            reason,
        };
        match value {
            "exponential" => Ok(Backoff::Exponential),
            "polynomial" => Ok(Backoff::Polynomial),
            _ => {
                let delay = value
                    .strip_prefix("fixed:")
                    .ok_or_else(|| invalid(BACKOFF_FORMS))?;
                let ms = parse_millis(delay).ok_or_else(|| invalid(DURATION_FORMS))?;
                Ok(Backoff::Fixed(Duration::from_millis(ms)))
            }
        }
    }
}

impl TryFrom<String> for Backoff {
    type Error = Error;

    fn try_from(value: String) -> Result<Self, Error> {
        value.parse()
    }
}

const BACKOFF_FORMS: &str = r#"use "exponential", "polynomial", or "fixed:<duration>""#;
const DURATION_FORMS: &str = "a fixed delay is a whole number with ms, s, m, h or d, like 30s";

/// `"30s"` in milliseconds. The `#[job]` macro accepts the same forms.
pub(crate) fn parse_millis(text: &str) -> Option<u64> {
    let split = text.find(|c: char| !c.is_ascii_digit())?;
    let (number, unit) = text.split_at(split);
    let scale = match unit {
        "ms" => 1,
        "s" => 1_000,
        "m" => 60_000,
        "h" => 3_600_000,
        "d" => 86_400_000,
        _ => return None,
    };
    number.parse::<u64>().ok()?.checked_mul(scale)
}

/// A number in `0.0..1.0` that differs on every call, for jitter. It needs no
/// quality beyond spreading retries apart: each `RandomState` has fresh keys.
fn random_unit() -> f64 {
    let mut hasher = RandomState::new().build_hasher();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    hasher.write_u32(nanos);
    (hasher.finish() >> 11) as f64 / (1u64 << 53) as f64
}

/// What a job's error asks for, through [`Retryable`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Retry {
    /// Retry after the job's backoff, while it has retries left.
    #[default]
    Default,
    /// Retry after this long instead, for hints like HTTP's `Retry-After`,
    /// while it has retries left.
    After(Duration),
    /// Never retry: the job is dead at once, like ActiveJob's `discard_on`.
    Never,
}

/// Implement it on a job's error type to decide, per error, whether and when
/// the job is retried. Error types that don't implement it get
/// [`Retry::Default`].
///
/// The `#[job]` macro reads it on the error type the job returns. For an
/// error that reaches the worker wrapped, in an `anyhow::Error`, a
/// [`BoxError`](crate::BoxError), or as the `source` of another error,
/// register its type with [`retryable!`](crate::retryable): the error and
/// its sources are then searched, outermost first, for one whose type is
/// registered.
pub trait Retryable {
    fn retry(&self) -> Retry;
}

/// Registers error types that implement [`Retryable`] (and
/// `std::error::Error`), so their classification applies when they are
/// wrapped: returned as an `anyhow::Error` or a [`BoxError`](crate::BoxError),
/// or found in the source chain of the job's error.
///
/// ```
/// #[derive(Debug, thiserror::Error)]
/// #[error("account {0} was deleted")]
/// struct AccountDeleted(u64);
///
/// impl butler::Retryable for AccountDeleted {
///     fn retry(&self) -> butler::Retry {
///         butler::Retry::Never
///     }
/// }
///
/// butler::retryable!(AccountDeleted);
///
/// #[butler::job]
/// async fn sync_account(id: u64) -> Result<(), butler::BoxError> {
///     Err(AccountDeleted(id).into()) // dead at once, not retried
/// }
/// # fn main() {}
/// ```
///
/// Like jobs, registrations are collected when the program links: one made
/// in a library crate is only seen if the binary uses something from it.
#[macro_export]
macro_rules! retryable {
    ($($error:ty),+ $(,)?) => {
        $(
            $crate::__private::inventory::submit! {
                $crate::__private::RetryableType::of::<$error>()
            }
        )+
    };
}

/// An error type registered with [`retryable!`].
#[doc(hidden)]
pub struct RetryableType {
    classify: fn(&(dyn std::error::Error + 'static)) -> Option<Retry>,
}

impl RetryableType {
    pub const fn of<E: Retryable + std::error::Error + 'static>() -> Self {
        Self {
            classify: classify_as::<E>,
        }
    }
}

inventory::collect!(RetryableType);

fn classify_as<E: Retryable + std::error::Error + 'static>(
    error: &(dyn std::error::Error + 'static),
) -> Option<Retry> {
    error.downcast_ref::<E>().map(Retryable::retry)
}

/// What the first error of a registered type in `error`'s source chain,
/// starting with `error` itself, asks for.
pub(crate) fn classify_chain(error: &(dyn std::error::Error + 'static)) -> Option<Retry> {
    let mut next = Some(error);
    while let Some(error) = next {
        for registered in inventory::iter::<RetryableType> {
            if let Some(retry) = (registered.classify)(error) {
                return Some(retry);
            }
        }
        next = error.source();
    }
    None
}

/// How many times a failed job is retried, and how long it waits before each
/// retry. A plain number converts into a policy that retries at once, which
/// is what [`Queue::fail`](crate::Queue::fail) did before backoff existed.
///
/// A job sets its own with `#[job(retries = N)]`, checked when it compiles:
///
/// ```compile_fail
/// #[butler::job(retries = -1)]
/// async fn flaky() {}
/// # fn main() {}
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Failed attempts after which the job is dead: it runs at most
    /// `max_retries + 1` times.
    pub max_retries: u32,
    pub backoff: Backoff,
}

impl RetryPolicy {
    pub fn new(max_retries: u32, backoff: Backoff) -> Self {
        Self {
            max_retries,
            backoff,
        }
    }
}

impl From<u32> for RetryPolicy {
    fn from(max_retries: u32) -> Self {
        Self::new(max_retries, Backoff::NONE)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    const SECOND: Duration = Duration::from_secs(1);

    #[test]
    fn delays_grow_as_specified() {
        let exponential: Vec<_> = (1..=4)
            .map(|n| Backoff::Exponential.base_delay(n))
            .collect();
        assert_eq!(
            exponential,
            [2 * SECOND, 4 * SECOND, 8 * SECOND, 16 * SECOND]
        );
        let polynomial: Vec<_> = (1..=4).map(|n| Backoff::Polynomial.base_delay(n)).collect();
        assert_eq!(
            polynomial,
            [16 * SECOND, 31 * SECOND, 96 * SECOND, 271 * SECOND]
        );
        let fixed = Backoff::Fixed(30 * SECOND);
        assert_eq!(
            (fixed.base_delay(1), fixed.base_delay(9)),
            (30 * SECOND, 30 * SECOND)
        );
        assert_eq!(
            Backoff::Exponential.base_delay(200),
            MAX_BACKOFF,
            "capped, no overflow"
        );
        assert_eq!(Backoff::Polynomial.base_delay(u32::MAX), MAX_BACKOFF);
    }

    #[test]
    fn jitter_stays_within_bounds() {
        let base = Backoff::Polynomial.base_delay(3);
        assert_eq!(Backoff::Polynomial.delay_with(3, 0.0), base);
        assert_eq!(
            Backoff::Polynomial.delay_with(3, 1.0),
            base.mul_f64(1.0 + JITTER)
        );
        for _ in 0..1_000 {
            let delay = Backoff::Polynomial.delay(3);
            assert!(
                delay >= base && delay <= base.mul_f64(1.0 + JITTER),
                "{delay:?}"
            );
        }
        assert_eq!(Backoff::NONE.delay(5), Duration::ZERO, "no jitter on zero");
        let spread: std::collections::HashSet<_> =
            (0..50).map(|_| Backoff::Exponential.delay(10)).collect();
        assert!(spread.len() > 1, "jitter varies");
    }

    #[test]
    fn parses_and_displays_config_values() {
        for (text, backoff) in [
            ("exponential", Backoff::Exponential),
            ("polynomial", Backoff::Polynomial),
            ("fixed:30s", Backoff::Fixed(30 * SECOND)),
            ("fixed:1500ms", Backoff::Fixed(Duration::from_millis(1500))),
            ("fixed:5m", Backoff::Fixed(300 * SECOND)),
            ("fixed:2h", Backoff::Fixed(7_200 * SECOND)),
            ("fixed:1d", Backoff::Fixed(86_400 * SECOND)),
            ("fixed:0ms", Backoff::NONE),
        ] {
            assert_eq!(text.parse::<Backoff>().unwrap(), backoff, "{text}");
            assert_eq!(backoff.to_string(), text);
        }
        for bad in [
            "",
            "linear",
            "fixed",
            "fixed:",
            "fixed:30",
            "fixed:s",
            "fixed:-1s",
            "fixed:1w",
        ] {
            let err = bad.parse::<Backoff>().unwrap_err();
            assert!(
                matches!(err, Error::InvalidBackoff { ref value, .. } if value == bad),
                "{bad}"
            );
        }
    }

    #[test]
    fn a_number_is_a_policy_that_retries_at_once() {
        assert_eq!(RetryPolicy::from(3), RetryPolicy::new(3, Backoff::NONE));
    }
}
