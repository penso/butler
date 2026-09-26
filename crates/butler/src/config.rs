//! Loads settings from `config.toml`, with environment variables taking
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
//!
//! [worker]
//! concurrency = 4
//! max_retries = 3
//! poll_interval_ms = 100
//! heartbeat_ttl_secs = 30      # crashed workers' jobs are requeued after this
//! recover_interval_secs = 10
//! queues = [["critical", 6], ["default", 3], ["low", 1]]   # or ["critical", "default"]
//!
//! [worker.queue_limits]        # optional, per queue, on top of `concurrency`
//! mailers = 20
//! ```
//!
//! The config file is `$BUTLER_CONFIG` if set (it must then exist), otherwise
//! `./config.toml` if present. Environment variables override individual keys,
//! using `__` between levels: `BUTLER_QUEUE__BACKEND=redis`,
//! `BUTLER_QUEUE__REDIS__URL=redis://host/`, `BUTLER_WORKER__CONCURRENCY=8`.

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    time::Duration,
};

use serde::Deserialize;

use crate::{
    Error, FileQueue, Queue, QueuePriority, Result,
    job::{DEFAULT_QUEUE, is_valid_queue_name},
};

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct Config {
    pub queue: QueueConfig,
    pub worker: WorkerConfig,
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
}

impl Default for RedisConfig {
    fn default() -> Self {
        Self {
            url: "redis://127.0.0.1:6379/".into(),
            prefix: "butler".into(),
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
    pub max_retries: u32,
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
    /// bound by `concurrency`.
    pub queue_limits: HashMap<String, usize>,
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
            poll_interval_ms: 100,
            heartbeat_ttl_secs: 30,
            recover_interval_secs: 10,
            checkpoint_interval_ms: 1_000,
            queues: vec![QueueEntry::Name(DEFAULT_QUEUE.to_owned())],
            queue_limits: HashMap::new(),
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
        for (queue, max) in &self.queue_limits {
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
    /// Loads from `$BUTLER_CONFIG`, or `./config.toml`, plus `BUTLER_*` env vars.
    pub fn load() -> Result<Self> {
        match std::env::var_os("BUTLER_CONFIG") {
            Some(path) => Self::load_from(path, true),
            None => Self::load_from("config.toml", false),
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
                Ok(crate::RedisQueue::connect(&redis.url, &redis.prefix)?.into())
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
        assert_eq!(config.queue.file.dir, PathBuf::from(".butler"));
        assert_eq!(config.worker.concurrency, 8);
        assert_eq!(config.worker.max_retries, 3);
    }

    fn load(toml: &str) -> Result<Config> {
        let path = std::env::temp_dir().join(format!(
            "butler-config-queues-{}-{}.toml",
            std::process::id(),
            toml.len()
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
    fn bad_queues_are_rejected() {
        let bad_name = load("[worker]\nqueues = [\"../etc\"]\n").unwrap_err();
        assert!(matches!(bad_name, Error::InvalidQueue { ref name, .. } if name == "../etc"));
        let zero = load("[worker]\nqueues = [[\"low\", 0]]\n").unwrap_err();
        assert!(matches!(zero, Error::InvalidQueue { ref name, .. } if name == "low"));
    }

    #[test]
    fn missing_optional_file_gives_defaults() {
        let config = Config::load_from("/nonexistent/butler.toml", false).unwrap();
        assert_eq!(config.queue.backend, BackendKind::File);
    }
}
