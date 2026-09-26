use std::fmt;

pub type Result<T, E = Error> = std::result::Result<T, E>;

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

    #[cfg(feature = "tokio")]
    #[error("blocking queue task did not complete")]
    Join(#[from] tokio::task::JoinError),

    #[error("job {id} has unknown state `{state}`")]
    UnknownState { id: String, state: String },

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

    #[cfg(feature = "tokio")]
    #[error("job task was cancelled")]
    Cancelled(#[source] tokio::task::JoinError),

    /// The job body returned an error.
    #[error(transparent)]
    Failed(anyhow::Error),
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

    #[test]
    fn failed_job_error_is_transparent() {
        let err = JobError::Failed(anyhow::anyhow!("root cause").context("reading config"));
        assert_eq!(Chain(&err).to_string(), "reading config: root cause");
    }
}
