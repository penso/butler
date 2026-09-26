use std::fmt;

#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    Json(serde_json::Error),
    Config(config::ConfigError),
    #[cfg(feature = "redis")]
    Redis(redis::RedisError),
    Backend(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "queue io error: {e}"),
            Error::Json(e) => write!(f, "job serialization error: {e}"),
            Error::Config(e) => write!(f, "config error: {e}"),
            #[cfg(feature = "redis")]
            Error::Redis(e) => write!(f, "redis error: {e}"),
            Error::Backend(e) => write!(f, "queue error: {e}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            Error::Json(e) => Some(e),
            Error::Config(e) => Some(e),
            #[cfg(feature = "redis")]
            Error::Redis(e) => Some(e),
            Error::Backend(_) => None,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::Json(e)
    }
}

impl From<config::ConfigError> for Error {
    fn from(e: config::ConfigError) -> Self {
        Error::Config(e)
    }
}

#[cfg(feature = "redis")]
impl From<redis::RedisError> for Error {
    fn from(e: redis::RedisError) -> Self {
        Error::Redis(e)
    }
}
