use std::fmt;

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Any error a job body can return: a `thiserror` enum, `std::io::Error`, a
/// plain `String` message, or an `anyhow::Error` from an app that uses anyhow.
pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Errors from the queue itself: storage, serialization, configuration.
/// Failures inside a job body are not `Error`s. They go into the job record as
/// `last_error` and trigger a retry.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("queue I/O failed")]
    Io(#[from] std::io::Error),

    #[error("job (de)serialization failed")]
    Json(#[from] serde_json::Error),

    #[error("invalid configuration")]
    Config(#[from] config::ConfigError),

    #[cfg(feature = "redis")]
    #[error("redis operation failed")]
    Redis(#[from] redis::RedisError),

    #[cfg(feature = "sqlite")]
    #[error("sqlite operation failed")]
    Sqlite(#[from] rusqlite::Error),

    #[cfg(feature = "tokio")]
    #[error("blocking queue task did not complete")]
    Join(#[from] tokio::task::JoinError),

    #[error("this backend doesn't support {0}")]
    Unsupported(&'static str),

    #[error("job {id} is not in the queue")]
    JobNotFound { id: String },

    /// Waiting for a result, the job exhausted its retries instead.
    #[error("job {id} failed: {error}")]
    JobFailed { id: String, error: String },

    #[error("job {id} was cancelled before it ran")]
    JobCancelled { id: String },

    #[error("job {id} has unknown state `{state}`")]
    UnknownState { id: String, state: String },

    #[error("invalid queue `{name}` in config: {reason}")]
    InvalidQueue { name: String, reason: &'static str },

    #[error("config selects the `{0}` backend, but butler was built without the `{0}` feature")]
    BackendDisabled(&'static str),
}

/// Why one run of a job failed. Its full cause chain is stored as the job's
/// `last_error`, and the job is retried or marked dead.
#[derive(Debug, thiserror::Error)]
pub enum JobError {
    #[error("no job named `{name}` is registered in this worker")]
    UnknownJob { name: String },

    #[error("{job}: missing argument #{index}")]
    MissingArgument { job: &'static str, index: usize },

    #[error("{job}: bad argument #{index}")]
    BadArgument {
        job: &'static str,
        index: usize,
        #[source]
        source: serde_json::Error,
    },

    #[error("job panicked: {message}")]
    Panicked { message: String },

    #[error("job output could not be serialized")]
    Output(#[source] serde_json::Error),

    /// The saved progress no longer fits the job's `Progress` type, for
    /// example after a deploy that changed it.
    #[error("saved job progress doesn't match the job's progress type")]
    BadProgress(#[source] serde_json::Error),

    #[cfg(feature = "tokio")]
    #[error("job task was cancelled")]
    Cancelled(#[source] tokio::task::JoinError),

    /// The job body returned an error. Its display and sources are the job's
    /// own, so the stored `last_error` shows the whole cause chain.
    #[error(transparent)]
    Failed(BoxError),
}

/// Displays an error followed by each of its sources: `outer: inner: root`.
pub(crate) struct Chain<'a>(pub &'a (dyn std::error::Error + 'static));

impl fmt::Display for Chain<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)?;
        let mut source = self.0.source();
        while let Some(err) = source {
            write!(f, ": {err}")?;
            source = err.source();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chain_shows_every_source() {
        let io = std::io::Error::new(std::io::ErrorKind::NotFound, "no such file");
        let err = Error::from(io);
        assert_eq!(Chain(&err).to_string(), "queue I/O failed: no such file");
    }

    #[derive(Debug, thiserror::Error)]
    #[error("reading config")]
    struct ReadingConfig(#[source] std::io::Error);

    #[test]
    fn failed_job_error_is_transparent() {
        let root = std::io::Error::new(std::io::ErrorKind::NotFound, "root cause");
        let err = JobError::Failed(Box::new(ReadingConfig(root)));
        assert_eq!(Chain(&err).to_string(), "reading config: root cause");

        let message = JobError::Failed("plain message".into());
        assert_eq!(Chain(&message).to_string(), "plain message");
    }
}
