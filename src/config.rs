//! Loads settings from `config.toml`, with environment variables taking
//! precedence.
//!
//! ```toml
//! [queue]
//! backend = "redis"            # "file" (default) or "redis"
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
//! ```
//!
//! The config file is `$BUTLER_CONFIG` if set (it must then exist), otherwise
//! `./config.toml` if present. Environment variables override individual keys,
//! using `__` between levels: `BUTLER_QUEUE__BACKEND=redis`,
//! `BUTLER_QUEUE__REDIS__URL=redis://host/`, `BUTLER_WORKER__CONCURRENCY=8`.

use std::{path::PathBuf, time::Duration};

use serde::Deserialize;

use crate::{Error, FileQueue, Queue};

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
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BackendKind {
    #[default]
    File,
    Redis,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct FileConfig {
    pub dir: PathBuf,
}

impl Default for FileConfig {
    fn default() -> Self {
        Self { dir: ".butler".into() }
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
        Self { url: "redis://127.0.0.1:6379/".into(), prefix: "butler".into() }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct WorkerConfig {
    /// Jobs run at the same time (threads for `run`, tasks for `run_async`).
    pub concurrency: usize,
    pub max_retries: u32,
    pub poll_interval_ms: u64,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self { concurrency: 1, max_retries: 3, poll_interval_ms: 100 }
    }
}

impl WorkerConfig {
    pub fn poll_interval(&self) -> Duration {
        Duration::from_millis(self.poll_interval_ms)
    }
}

impl Config {
    /// Loads from `$BUTLER_CONFIG`, or `./config.toml`, plus `BUTLER_*` env vars.
    pub fn load() -> Result<Self, Error> {
        match std::env::var_os("BUTLER_CONFIG") {
            Some(path) => Self::load_from(PathBuf::from(path), true),
            None => Self::load_from("config.toml", false),
        }
    }

    /// Loads from `path` plus `BUTLER_*` env vars. A missing file is an error
    /// only if `required` is set.
    pub fn load_from(path: impl Into<PathBuf>, required: bool) -> Result<Self, Error> {
        let config = config::Config::builder()
            .add_source(config::File::from(path.into()).required(required))
            .add_source(
                config::Environment::with_prefix("BUTLER")
                    .prefix_separator("_")
                    .separator("__")
                    .try_parsing(true),
            )
            .build()?;
        Ok(config.try_deserialize()?)
    }

    /// Opens the configured backend.
    pub fn connect(&self) -> Result<Queue, Error> {
        match self.queue.backend {
            BackendKind::File => Ok(FileQueue::new(&self.queue.file.dir)?.into()),
            #[cfg(feature = "redis")]
            BackendKind::Redis => {
                let redis = &self.queue.redis;
                Ok(crate::RedisQueue::connect(&redis.url, &redis.prefix)?.into())
            }
            #[cfg(not(feature = "redis"))]
            BackendKind::Redis => Err(Error::Backend(
                "config selects the redis backend, but butler was built without the `redis` feature".into(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
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

    #[test]
    fn missing_optional_file_gives_defaults() {
        let config = Config::load_from("/nonexistent/butler.toml", false).unwrap();
        assert_eq!(config.queue.backend, BackendKind::File);
    }
}
