//! Loads settings from `butler.toml`, with environment variables taking
//! precedence.
//!
//! ```toml
//! [queue]
//! backend = "redis"            # "file" (default), "redis", or "memory" (in-process)
//!
//! [queue.file]
//! dir = ".butler"
//!
//! [queue.redis]
//! url = "redis://127.0.0.1:6379/"
//! prefix = "butler"
//! max_idle_connections = 32   # idle connections kept for reuse, per process
//!
//! [worker]
//! concurrency = 4
//! max_retries = 3
//! backoff = "exponential"      # or "polynomial", "fixed:30s"; before each retry
//! poll_interval_ms = 100
//! heartbeat_ttl_secs = 30      # crashed workers' jobs are requeued after this
//! recover_interval_secs = 10
//! queues = [["critical", 6], ["default", 3], ["low", 1]]   # or ["critical", "default"]
//!
//! [worker.queue_limits]        # optional, per queue and per worker process
//! mailers = 20
//!
//! [worker.global_queue_limits] # optional, per queue across every worker
//! reports = 5
//!
//! [[recurring]]                # optional, any number: a job on a cron schedule
//! job = "nightly_report"       # the job's name
//! cron = "0 3 * * *"           # minute hour day-of-month month day-of-week
//! args = ["summary"]           # optional, default none
//! queue = "reports"            # optional, default the job's own queue
//! timezone = "Europe/Paris"    # optional, default "UTC"
//! key = "nightly"              # optional, default derived from the fields above
//! ```
//!
//! The config file is `$BUTLER_CONFIG` if set (it must then exist), otherwise
//! `./butler.toml` if present. Environment variables override individual keys,
//! using `__` between levels: `BUTLER_QUEUE__BACKEND=redis`,
//! `BUTLER_QUEUE__REDIS__URL=redis://host/`, `BUTLER_WORKER__CONCURRENCY=8`.

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    time::Duration,
};

use serde::Deserialize;
use serde_json::Value;

use crate::{
    Backoff, Cron, Error, FileQueue, Queue, QueuePriority, Result,
    job::{DEFAULT_QUEUE, is_valid_queue_name},
    recurring,
};

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct Config {
    pub queue: QueueConfig,
    pub worker: WorkerConfig,
    /// `[[recurring]]` entries: jobs enqueued on a cron schedule by the
    /// workers built with [`Worker::from_config`](crate::Worker::from_config).
    pub recurring: Vec<RecurringConfig>,
}

/// One `[[recurring]]` entry: a job enqueued at every tick of a cron
/// schedule. See [`Recurring`](crate::Recurring).
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct RecurringConfig {
    /// The job's name: its function name, or its `#[job(name = "...")]`.
    pub job: String,
    /// Five fields, as in crontab: `"0 3 * * *"` is 03:00 every day.
    pub cron: String,
    /// The job's arguments, in order.
    #[serde(default)]
    pub args: Vec<Value>,
    /// The job's own queue if not set.
    #[serde(default)]
    pub queue: Option<String>,
    /// An IANA time zone for `cron`, such as `"Europe/Paris"`; UTC if not set.
    #[serde(default)]
    pub timezone: Option<String>,
    /// What identifies the schedule in the backend; derived from the other
    /// fields if not set. Set it to keep the schedule's history when changing
    /// them.
    #[serde(default)]
    pub key: Option<String>,
}

impl RecurringConfig {
    /// Checks what can be checked without the worker's jobs: the cron
    /// expression, time zone, queue and key.
    fn validate(&self) -> Result<()> {
        let cron = Cron::parse(&self.cron)?;
        if let Some(zone) = &self.timezone {
            cron.in_time_zone(zone)?;
        }
        if let Some(queue) = self.queue.as_deref().filter(|q| !is_valid_queue_name(q)) {
            return Err(Error::InvalidQueue {
                name: queue.to_owned(),
                reason: "use 1 to 64 of A-Z a-z 0-9 _ - . (not starting with a dot)",
            });
        }
        if let Some(key) = self
            .key
            .as_deref()
            .filter(|key| !recurring::is_valid_key(key))
        {
            return Err(Error::InvalidRecurringKey {
                key: key.to_owned(),
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct QueueConfig {
    pub backend: BackendKind,
    pub file: FileConfig,
    pub redis: RedisConfig,
    pub sqlite: SqliteConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct SqliteConfig {
    /// Created if missing. Use a file, not `:memory:`: other processes and
    /// the change watcher open their own connections to it.
    pub path: PathBuf,
}

impl Default for SqliteConfig {
    fn default() -> Self {
        Self {
            path: "butler.db".into(),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BackendKind {
    #[default]
    File,
    Redis,
    /// In-process only: see [`MemoryQueue`](crate::MemoryQueue).
    Memory,
    /// One database file shared by processes on the same machine.
    Sqlite,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct FileConfig {
    pub dir: PathBuf,
}

impl Default for FileConfig {
    fn default() -> Self {
        Self {
            dir: ".butler".into(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct RedisConfig {
    pub url: String,
    /// Namespace for all keys, so several apps can share one Redis.
    pub prefix: String,
    /// How many idle connections each process keeps for reuse. Not a limit
    /// on connections in use: see
    /// [`RedisQueue::max_idle_connections`](crate::RedisQueue::max_idle_connections).
    pub max_idle_connections: usize,
}

impl RedisConfig {
    /// The default of [`max_idle_connections`](Self::max_idle_connections).
    pub const DEFAULT_MAX_IDLE_CONNECTIONS: usize = 32;
}

impl Default for RedisConfig {
    fn default() -> Self {
        Self {
            url: "redis://127.0.0.1:6379/".into(),
            prefix: "butler".into(),
            max_idle_connections: Self::DEFAULT_MAX_IDLE_CONNECTIONS,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct WorkerConfig {
    /// Jobs run at the same time (threads for `run`, tasks for `run_async`).
    /// Defaults to the number of CPUs.
    pub concurrency: usize,
    /// Claim loops running side by side in `run_async`; more start jobs
    /// faster. Defaults to the number of CPUs.
    pub claimers: usize,
    /// How many times a failed job is retried, unless it sets its own with
    /// `#[job(retries = N)]`.
    pub max_retries: u32,
    /// How long a failed job waits before each retry, unless it sets its own
    /// with `#[job(backoff = "...")]`: `"exponential"` (the default),
    /// `"polynomial"`, or `"fixed:30s"`.
    pub backoff: Backoff,
    pub poll_interval_ms: u64,
    /// A worker counts as alive this long after each heartbeat; it refreshes
    /// every third of it. After a crash, its jobs are requeued once this lapses.
    pub heartbeat_ttl_secs: u64,
    /// How often to requeue jobs held by workers whose heartbeat expired.
    pub recover_interval_secs: u64,
    /// For jobs with a `Progress`: the most often their progress is saved to
    /// the backend, so a crash resumes them from at most this long ago.
    pub checkpoint_interval_ms: u64,
    /// Queues to serve. Plain names are strict priority, in order:
    /// `["critical", "default"]`. Any `[name, weight]` pair makes it weighted,
    /// with plain names weighing 1: `[["critical", 6], ["default", 1]]`.
    pub queues: Vec<QueueEntry>,
    /// Caps on jobs running at once, per queue, on top of `concurrency`:
    /// `[worker.queue_limits]` then `mailers = 20`. Queues not listed are only
    /// bound by `concurrency`. Each worker process counts its own jobs, so
    /// three workers with `mailers = 20` can run 60 mailer jobs at once.
    pub queue_limits: HashMap<String, usize>,
    /// Caps on jobs running at once, per queue, across every worker that has
    /// the same cap: `[worker.global_queue_limits]` then `mailers = 20`. The
    /// backend keeps the count, so three workers with `mailers = 20` run 20
    /// mailer jobs at once between them. Costs a little more per claim than
    /// `queue_limits`; both can apply to the same queue.
    pub global_queue_limits: HashMap<String, usize>,
}

/// One entry of `[worker] queues`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(untagged)]
pub enum QueueEntry {
    Name(String),
    Weighted(String, u32),
}

impl QueueEntry {
    fn name(&self) -> &str {
        match self {
            Self::Name(name) | Self::Weighted(name, _) => name,
        }
    }
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            concurrency: cpus(),
            claimers: cpus(),
            max_retries: 3,
            backoff: Backoff::default(),
            poll_interval_ms: 100,
            heartbeat_ttl_secs: 30,
            recover_interval_secs: 10,
            checkpoint_interval_ms: 1_000,
            queues: vec![QueueEntry::Name(DEFAULT_QUEUE.to_owned())],
            queue_limits: HashMap::new(),
            global_queue_limits: HashMap::new(),
        }
    }
}

fn cpus() -> usize {
    std::thread::available_parallelism().map_or(4, |n| n.get())
}

impl WorkerConfig {
    pub fn poll_interval(&self) -> Duration {
        Duration::from_millis(self.poll_interval_ms)
    }

    pub fn heartbeat_ttl(&self) -> Duration {
        Duration::from_secs(self.heartbeat_ttl_secs)
    }

    pub fn recover_interval(&self) -> Duration {
        Duration::from_secs(self.recover_interval_secs)
    }

    pub fn checkpoint_interval(&self) -> Duration {
        Duration::from_millis(self.checkpoint_interval_ms)
    }

    pub fn priority(&self) -> QueuePriority {
        let weighted = self
            .queues
            .iter()
            .any(|entry| matches!(entry, QueueEntry::Weighted(..)));
        if !weighted {
            return QueuePriority::strict(self.queues.iter().map(QueueEntry::name));
        }
        QueuePriority::weighted(self.queues.iter().map(|entry| match entry {
            QueueEntry::Name(name) => (name.as_str(), 1),
            QueueEntry::Weighted(name, weight) => (name.as_str(), *weight),
        }))
    }

    /// Rejects queue names that can't be file names or Redis keys, and zero
    /// weights, which would mean a queue is listed but never served.
    fn validate(&self) -> Result<()> {
        for entry in &self.queues {
            let reason = match entry {
                _ if !is_valid_queue_name(entry.name()) => {
                    "use 1 to 64 of A-Z a-z 0-9 _ - . (not starting with a dot)"
                }
                QueueEntry::Weighted(_, 0) => "weights start at 1",
                _ => continue,
            };
            return Err(Error::InvalidQueue {
                name: entry.name().to_owned(),
                reason,
            });
        }
        for (queue, max) in self.queue_limits.iter().chain(&self.global_queue_limits) {
            let reason = if !is_valid_queue_name(queue) {
                "use 1 to 64 of A-Z a-z 0-9 _ - . (not starting with a dot)"
            } else if *max == 0 {
                "a queue limit starts at 1"
            } else {
                continue;
            };
            return Err(Error::InvalidQueue {
                name: queue.clone(),
                reason,
            });
        }
        Ok(())
    }
}

impl Config {
    /// Loads from `$BUTLER_CONFIG`, or `./butler.toml`, plus `BUTLER_*` env vars.
    pub fn load() -> Result<Self> {
        match std::env::var_os("BUTLER_CONFIG") {
            Some(path) => Self::load_from(path, true),
            None => Self::load_from("butler.toml", false),
        }
    }

    /// Loads from `path` plus `BUTLER_*` env vars. A missing file is an error
    /// only if `required` is set.
    pub fn load_from(path: impl AsRef<Path>, required: bool) -> Result<Self> {
        let config = config::Config::builder()
            .add_source(config::File::from(path.as_ref()).required(required))
            .add_source(
                config::Environment::with_prefix("BUTLER")
                    .prefix_separator("_")
                    .separator("__")
                    .try_parsing(true),
            )
            .build()?;
        let config: Self = config.try_deserialize()?;
        config.worker.validate()?;
        let mut keys = std::collections::HashSet::new();
        for entry in &config.recurring {
            entry.validate()?;
            if let Some(key) = entry.key.as_deref().filter(|key| !keys.insert(*key)) {
                return Err(Error::DuplicateRecurring {
                    key: key.to_owned(),
                });
            }
        }
        Ok(config)
    }

    /// Opens the configured backend.
    pub fn connect(&self) -> Result<Queue> {
        match self.queue.backend {
            BackendKind::File => Ok(FileQueue::new(&self.queue.file.dir)?.into()),
            BackendKind::Memory => Ok(crate::MemoryQueue::shared().into()),
            #[cfg(feature = "sqlite")]
            BackendKind::Sqlite => Ok(crate::SqliteQueue::open(&self.queue.sqlite.path)?.into()),
            #[cfg(not(feature = "sqlite"))]
            BackendKind::Sqlite => Err(Error::BackendDisabled("sqlite")),
            #[cfg(feature = "redis")]
            BackendKind::Redis => {
                let redis = &self.queue.redis;
                Ok(crate::RedisQueue::connect(&redis.url, &redis.prefix)?
                    .max_idle_connections(redis.max_idle_connections)
                    .into())
            }
            #[cfg(not(feature = "redis"))]
            BackendKind::Redis => Err(Error::BackendDisabled("redis")),
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn parses_toml_and_fills_defaults() {
        let path = std::env::temp_dir().join(format!("butler-config-{}.toml", std::process::id()));
        std::fs::write(
            &path,
            "[queue]\nbackend = \"redis\"\n[queue.redis]\nurl = \"redis://example:6380/\"\n[worker]\nconcurrency = 8\n",
        )
        .unwrap();
        let config = Config::load_from(&path, true).unwrap();
        std::fs::remove_file(&path).unwrap();

        assert_eq!(config.queue.backend, BackendKind::Redis);
        assert_eq!(config.queue.redis.url, "redis://example:6380/");
        assert_eq!(config.queue.redis.prefix, "butler");
        assert_eq!(
            config.queue.redis.max_idle_connections,
            RedisConfig::DEFAULT_MAX_IDLE_CONNECTIONS
        );
        assert_eq!(config.queue.file.dir, PathBuf::from(".butler"));
        assert_eq!(config.worker.concurrency, 8);
        assert_eq!(config.worker.max_retries, 3);
    }

    /// Loads `toml` from a file of its own: tests run in parallel.
    fn load(toml: &str) -> Result<Config> {
        static SEQ: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
        let seq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "butler-config-queues-{}-{seq}.toml",
            std::process::id(),
        ));
        std::fs::write(&path, toml).unwrap();
        let config = Config::load_from(&path, true);
        std::fs::remove_file(&path).unwrap();
        config
    }

    #[test]
    fn queues_are_strict_or_weighted() {
        let strict = load("[worker]\nqueues = [\"critical\", \"default\"]\n").unwrap();
        assert_eq!(
            strict.worker.priority(),
            QueuePriority::strict(["critical", "default"])
        );

        let weighted =
            load("[worker]\nqueues = [[\"critical\", 6], \"default\", [\"low\", 1]]\n").unwrap();
        assert_eq!(
            weighted.worker.priority(),
            QueuePriority::weighted([("critical", 6), ("default", 1), ("low", 1)])
        );

        let default = load("").unwrap();
        assert_eq!(
            default.worker.priority(),
            QueuePriority::strict(["default"])
        );
    }

    #[test]
    fn queue_limits_load_and_reject_zero() {
        let config = load("[worker.queue_limits]\nmailers = 20\nreports = 2\n").unwrap();
        assert_eq!(config.worker.queue_limits.get("mailers"), Some(&20));
        assert_eq!(config.worker.queue_limits.get("reports"), Some(&2));

        let zero = load("[worker.queue_limits]\nmailers = 0\n").unwrap_err();
        assert!(matches!(zero, Error::InvalidQueue { ref name, .. } if name == "mailers"));
    }

    #[test]
    fn global_queue_limits_are_a_separate_key() {
        let config = load(
            "[worker.queue_limits]\nmailers = 20\n[worker.global_queue_limits]\nmailers = 50\nreports = 2\n",
        )
        .unwrap();
        assert_eq!(config.worker.queue_limits.get("mailers"), Some(&20));
        assert_eq!(config.worker.global_queue_limits.get("mailers"), Some(&50));
        assert_eq!(config.worker.global_queue_limits.get("reports"), Some(&2));
        assert!(load("").unwrap().worker.global_queue_limits.is_empty());

        let zero = load("[worker.global_queue_limits]\nreports = 0\n").unwrap_err();
        assert!(matches!(zero, Error::InvalidQueue { ref name, .. } if name == "reports"));
        let bad = load("[worker.global_queue_limits]\n\"../x\" = 1\n").unwrap_err();
        assert!(matches!(bad, Error::InvalidQueue { .. }), "{bad:?}");
    }

    #[test]
    fn bad_queues_are_rejected() {
        let bad_name = load("[worker]\nqueues = [\"../etc\"]\n").unwrap_err();
        assert!(matches!(bad_name, Error::InvalidQueue { ref name, .. } if name == "../etc"));
        let zero = load("[worker]\nqueues = [[\"low\", 0]]\n").unwrap_err();
        assert!(matches!(zero, Error::InvalidQueue { ref name, .. } if name == "low"));
    }

    #[test]
    fn backoff_loads_and_rejects_unknown_forms() {
        assert_eq!(load("").unwrap().worker.backoff, Backoff::Exponential);
        let fixed = load("[worker]\nbackoff = \"fixed:30s\"\n").unwrap();
        assert_eq!(
            fixed.worker.backoff,
            Backoff::Fixed(Duration::from_secs(30))
        );
        let polynomial = load("[worker]\nbackoff = \"polynomial\"\n").unwrap();
        assert_eq!(polynomial.worker.backoff, Backoff::Polynomial);
        assert!(load("[worker]\nbackoff = \"sometimes\"\n").is_err());
    }

    #[test]
    fn recurring_entries_load_with_defaults_and_are_validated() {
        let config = load(
            "[[recurring]]\njob = \"report\"\ncron = \"0 3 * * *\"\nargs = [42, \"summary\", { Mixed = true }]\n\
             [[recurring]]\njob = \"cleanup\"\ncron = \"*/15 * * * *\"\nqueue = \"low\"\ntimezone = \"Europe/Paris\"\nkey = \"cleanup\"\n",
        )
        .unwrap();
        assert_eq!(config.recurring.len(), 2);
        let report = &config.recurring[0];
        assert_eq!(
            (report.job.as_str(), report.cron.as_str()),
            ("report", "0 3 * * *")
        );
        assert_eq!(
            report.args,
            vec![
                serde_json::json!(42),
                serde_json::json!("summary"),
                serde_json::json!({ "Mixed": true })
            ]
        );
        assert_eq!(
            (
                report.queue.as_deref(),
                report.timezone.as_deref(),
                report.key.as_deref()
            ),
            (None, None, None)
        );
        let cleanup = &config.recurring[1];
        assert_eq!(cleanup.queue.as_deref(), Some("low"));
        assert_eq!(cleanup.timezone.as_deref(), Some("Europe/Paris"));
        assert!(load("").unwrap().recurring.is_empty());

        let bad_cron = load("[[recurring]]\njob = \"x\"\ncron = \"0 25 * * *\"\n").unwrap_err();
        assert!(
            matches!(bad_cron, Error::InvalidCron { .. }),
            "{bad_cron:?}"
        );
        let bad_zone =
            load("[[recurring]]\njob = \"x\"\ncron = \"0 3 * * *\"\ntimezone = \"Paris\"\n")
                .unwrap_err();
        assert!(
            matches!(bad_zone, Error::UnknownTimeZone { .. }),
            "{bad_zone:?}"
        );
        let bad_queue =
            load("[[recurring]]\njob = \"x\"\ncron = \"0 3 * * *\"\nqueue = \"../x\"\n")
                .unwrap_err();
        assert!(
            matches!(bad_queue, Error::InvalidQueue { .. }),
            "{bad_queue:?}"
        );
        let bad_key =
            load("[[recurring]]\njob = \"x\"\ncron = \"0 3 * * *\"\nkey = \"a/b\"\n").unwrap_err();
        assert!(
            matches!(bad_key, Error::InvalidRecurringKey { .. }),
            "{bad_key:?}"
        );
        let twice = "[[recurring]]\njob = \"x\"\ncron = \"0 3 * * *\"\nkey = \"k\"\n";
        let duplicate = load(&format!("{twice}{twice}")).unwrap_err();
        assert!(
            matches!(duplicate, Error::DuplicateRecurring { .. }),
            "{duplicate:?}"
        );
    }

    #[test]
    fn redis_idle_connections_load_from_the_file() {
        let config = load("[queue.redis]\nmax_idle_connections = 4\n").unwrap();
        assert_eq!(config.queue.redis.max_idle_connections, 4);
        assert!(load("[queue.redis]\nmax_idle_connections = -1\n").is_err());
    }

    #[test]
    fn missing_optional_file_gives_defaults() {
        let config = Config::load_from("/nonexistent/butler.toml", false).unwrap();
        assert_eq!(config.queue.backend, BackendKind::File);
    }
}
