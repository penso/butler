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
