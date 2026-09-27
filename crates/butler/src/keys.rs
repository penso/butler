//! Per-key concurrency and unique jobs, like Solid Queue's
//! `limits_concurrency` and Sidekiq Enterprise's unique jobs:
//!
//! ```ignore
//! // At most one sync per account at a time, across every worker.
//! #[butler::job(concurrency_key = "account_id", limit = 1)]
//! async fn sync_account(account_id: u64, full: bool) { ... }
//!
//! // Enqueueing it again while one is waiting returns the waiting one.
//! #[butler::job(unique = "until_started")]
//! async fn refresh_feed(user_id: u64) { ... }
//! ```
//!
//! The keys are computed when the job is enqueued, from its name and
//! arguments, and stored with it, so retries and recovery keep them.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{Error, Result};

/// `#[job(concurrency_key = "...", limit = N)]`: at most `limit` jobs with
/// the same key run at once, across every worker. The key is the job's name
/// and the arguments at `args`, by position; the macro turns argument names
/// into positions and checks them at compile time.
///
/// ```no_run
/// #[butler::job(concurrency_key = "tenant, account_id", limit = 2)]
/// async fn sync(tenant: String, account_id: u64, full: bool) {}
/// # fn main() {}
/// ```
///
/// A name that isn't one of the job's arguments doesn't compile,
///
/// ```compile_fail
/// #[butler::job(concurrency_key = "acount_id")]
/// async fn sync(account_id: u64) {}
/// # fn main() {}
/// ```
///
/// nor does its `Progress`, which callers don't pass,
///
/// ```compile_fail
/// #[butler::job(concurrency_key = "progress")]
/// async fn sync(account_id: u64, progress: butler::Progress<u32>) {}
/// # fn main() {}
/// ```
///
/// an empty or repeated name,
///
/// ```compile_fail
/// #[butler::job(concurrency_key = "account_id, account_id")]
/// async fn sync(account_id: u64) {}
/// # fn main() {}
/// ```
///
/// ```compile_fail
/// #[butler::job(concurrency_key = "")]
/// async fn sync(account_id: u64) {}
/// # fn main() {}
/// ```
///
/// a limit of 0, or a limit without a key:
///
/// ```compile_fail
/// #[butler::job(concurrency_key = "account_id", limit = 0)]
/// async fn sync(account_id: u64) {}
/// # fn main() {}
/// ```
///
/// ```compile_fail
/// #[butler::job(limit = 2)]
/// async fn sync(account_id: u64) {}
/// # fn main() {}
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConcurrencyLimit {
    pub args: &'static [usize],
    pub limit: u32,
}

/// How long a unique job keeps out identical ones (same name, same
/// arguments): `#[job(unique = "until_started")]` or `"until_finished"`.
/// Any other mode doesn't compile:
///
/// ```compile_fail
/// #[butler::job(unique = "forever")]
/// async fn refresh(user_id: u64) {}
/// # fn main() {}
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Unique {
    /// While it waits (pending or scheduled): once a worker starts it, an
    /// identical job can be enqueued again.
    UntilStarted,
    /// Until it is done, dead or cancelled, retries included.
    UntilFinished,
}

impl Unique {
    /// Every mode, by the name `#[job(unique = "...")]` takes.
    pub const ALL: [Unique; 2] = [Unique::UntilStarted, Unique::UntilFinished];

    pub fn as_str(self) -> &'static str {
        match self {
            Unique::UntilStarted => "until_started",
            Unique::UntilFinished => "until_finished",
        }
    }

    /// The mode named `name`, as the macro accepts it.
    pub fn parse(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|mode| mode.as_str() == name)
    }

    /// Whether a job in `state` still keeps identical ones out.
    pub fn holds_in(self, state: crate::JobState) -> bool {
        use crate::JobState::{Pending, Processing, Scheduled};
        match self {
            Unique::UntilStarted => matches!(state, Pending | Scheduled),
            Unique::UntilFinished => matches!(state, Pending | Scheduled | Processing),
        }
    }
}

/// A job's concurrency key, as stored with it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConcurrencyKey {
    /// `<job name>:<the key arguments as a JSON array>`.
    pub key: String,
    /// At most this many jobs with `key` run at once.
    pub limit: u32,
}

impl ConcurrencyKey {
    /// The key for job `name` called with `args`, per `limit`.
    pub fn new(name: &str, args: &[Value], limit: &ConcurrencyLimit) -> Self {
        let values: Vec<&Value> = limit
            .args
            .iter()
            .map(|index| args.get(*index).unwrap_or(&Value::Null))
            .collect();
        Self {
            key: format!(
                "{name}:{}",
                serde_json::to_string(&values).unwrap_or_default()
            ),
            limit: limit.limit,
        }
    }

    /// A short, stable name for the key, for Redis keys and file names.
    pub fn hash(&self) -> String {
        format!("{:016x}", fnv1a(&self.key))
    }
}

/// A unique job's key, as stored with it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UniqueKey {
    /// `<job name>:<all its arguments as a JSON array>`.
    pub key: String,
    pub until: Unique,
}

impl UniqueKey {
    pub fn new(name: &str, args: &[Value], until: Unique) -> Self {
        Self {
            key: format!("{name}:{}", serde_json::to_string(args).unwrap_or_default()),
            until,
        }
    }

    /// A short, stable name for the key, for Redis keys and file names.
    pub fn hash(&self) -> String {
        format!("{:016x}", fnv1a(&self.key))
    }
}

/// Checks what enqueue layers may have changed: a limit of 0 would park the
/// job forever.
pub(crate) fn validate(concurrency: Option<&ConcurrencyKey>) -> Result<()> {
    match concurrency {
        Some(key) if key.limit == 0 => Err(Error::InvalidConcurrencyLimit {
            key: key.key.clone(),
        }),
        _ => Ok(()),
    }
}

/// FNV-1a: its output is fixed by definition, unlike the standard library's
/// hasher, so every process and version names a key the same way.
fn fnv1a(text: &str) -> u64 {
    text.bytes().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3)
    })
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn keys_come_from_the_name_and_chosen_arguments() {
        let limit = ConcurrencyLimit {
            args: &[0, 2],
            limit: 2,
        };
        let key = ConcurrencyKey::new("sync", &[json!(42), json!(true), json!("eu")], &limit);
        assert_eq!(key.key, r#"sync:[42,"eu"]"#);
        assert_eq!(key.limit, 2);
        assert_eq!(key.hash().len(), 16);
        let other = ConcurrencyKey::new("sync", &[json!(43), json!(true), json!("eu")], &limit);
        assert_ne!(key.hash(), other.hash());

        let unique = UniqueKey::new("refresh", &[json!(1), json!("a")], Unique::UntilStarted);
        assert_eq!(unique.key, r#"refresh:[1,"a"]"#);
    }

    #[test]
    fn a_limit_of_zero_set_at_runtime_is_refused() {
        // The macro refuses `limit = 0`; an enqueue layer could still set it.
        let zero = ConcurrencyKey {
            key: "sync:[1]".into(),
            limit: 0,
        };
        assert!(matches!(
            validate(Some(&zero)),
            Err(Error::InvalidConcurrencyLimit { .. })
        ));
        let one = ConcurrencyKey { limit: 1, ..zero };
        assert!(validate(Some(&one)).is_ok());
        assert!(validate(None).is_ok());
    }

    #[test]
    fn modes_parse_by_their_attribute_names() {
        assert_eq!(Unique::parse("until_started"), Some(Unique::UntilStarted));
        assert_eq!(Unique::parse("until_finished"), Some(Unique::UntilFinished));
        assert_eq!(Unique::parse("forever"), None);
        use crate::JobState::*;
        assert!(Unique::UntilStarted.holds_in(Scheduled));
        assert!(!Unique::UntilStarted.holds_in(Processing));
        assert!(Unique::UntilFinished.holds_in(Processing));
        assert!(!Unique::UntilFinished.holds_in(Dead));
    }
}
