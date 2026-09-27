//! How long backends keep finished jobs before a worker deletes them.

use std::{
    str::FromStr,
    time::{Duration, SystemTime},
};

use serde::Deserialize;

use crate::{Error, retry::parse_millis};

/// How long a finished job is kept: `"forever"`, or a duration like `"7d"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(try_from = "String")]
pub enum Keep {
    Forever,
    /// Kept this long after it finished, and at least [`Keep::MIN`].
    For(Duration),
}

impl Keep {
    /// The shortest retention: a [`JobHandle`](crate::JobHandle) waiting on
    /// a job must still find it when it wakes or polls, so a shorter one is
    /// rounded up to this.
    pub const MIN: Duration = Duration::from_secs(60);

    /// How long it keeps a job, if not forever.
    pub fn duration(self) -> Option<Duration> {
        match self {
            Keep::Forever => None,
            Keep::For(keep) => Some(keep.max(Self::MIN)),
        }
    }

    /// Jobs that finished before this time are old enough to delete at
    /// `now`. `None` when nothing is.
    pub fn cutoff(self, now: SystemTime) -> Option<SystemTime> {
        now.checked_sub(self.duration()?)
    }
}

impl FromStr for Keep {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self, Error> {
        if value == "forever" {
            return Ok(Keep::Forever);
        }
        let invalid = |reason| Error::InvalidRetention {
            value: value.to_owned(),
            reason,
        };
        let keep = parse_millis(value)
            .map(Duration::from_millis)
            .ok_or_else(|| {
                invalid(r#"use "forever" or a whole number with ms, s, m, h or d, like "7d""#)
            })?;
        if keep < Keep::MIN {
            return Err(invalid("keep finished jobs at least 1m"));
        }
        Ok(Keep::For(keep))
    }
}

impl TryFrom<String> for Keep {
    type Error = Error;

    fn try_from(value: String) -> Result<Self, Error> {
        value.parse()
    }
}

/// How long a backend keeps finished jobs; a running worker deletes older
/// ones in batches (see [`Store::clean_finished`](crate::Store::clean_finished)).
/// Set with `[queue] keep_finished` and `keep_dead`, or each backend's
/// `retention` method.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Retention {
    /// Done and cancelled jobs, and with them their results. Default: a day.
    pub finished: Keep,
    /// Jobs that exhausted their retries. Default: forever, until discarded
    /// or retried from the dashboard.
    pub dead: Keep,
}

impl Retention {
    pub const DEFAULT_FINISHED: Keep = Keep::For(Duration::from_secs(24 * 60 * 60));
}

impl Default for Retention {
    fn default() -> Self {
        Self {
            finished: Self::DEFAULT_FINISHED,
            dead: Keep::Forever,
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn parses_forever_and_durations() {
        assert_eq!("forever".parse::<Keep>().unwrap(), Keep::Forever);
        assert_eq!(
            "7d".parse::<Keep>().unwrap(),
            Keep::For(Duration::from_secs(7 * 24 * 60 * 60))
        );
        assert_eq!(
            "90m".parse::<Keep>().unwrap(),
            Keep::For(Duration::from_secs(90 * 60))
        );
        for bad in ["", "7", "7 days", "-1d", "never", "30s", "59999ms"] {
            assert!(
                matches!(bad.parse::<Keep>(), Err(Error::InvalidRetention { .. })),
                "{bad:?}"
            );
        }
    }

    #[test]
    fn short_retentions_are_rounded_up_and_forever_never_expires() {
        let now = SystemTime::now();
        assert_eq!(Keep::Forever.cutoff(now), None);
        assert_eq!(Keep::For(Duration::ZERO).cutoff(now), Some(now - Keep::MIN));
        assert_eq!(
            Keep::For(Duration::from_secs(3600)).cutoff(now),
            Some(now - Duration::from_secs(3600))
        );
    }
}
