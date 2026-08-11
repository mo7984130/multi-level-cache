//! Unified error type for this crate.

use std::error::Error as StdError;

/// Error returned by cache operations.
///
/// Every internal failure (backend, serialization, data-source loading) is funneled into this
/// type, so the crate stays independent of the caller's business error types. Underlying errors
/// are preserved as the error source (`Error::source`), keeping the error chain intact for
/// diagnostics.
#[derive(Debug, thiserror::Error)]
pub enum CacheError {
    /// L2 backend error (Redis connection, command execution, etc.)
    #[error("cache backend error: {0}")]
    Backend(Box<dyn StdError + Send + Sync>),

    /// Value serialization / deserialization error
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// Data-source loader error (forwards the loader's underlying error)
    #[error("data loading failed: {0}")]
    Loader(Box<dyn StdError + Send + Sync>),

    /// Caller-defined business error (e.g. "record not found"), distinguishable from internal
    /// failures so callers can decide whether to expose it to the user
    #[error("custom error: {0}")]
    Custom(Box<dyn StdError + Send + Sync>),
}

impl CacheError {
    /// Creates a backend error from any error, preserving it as the error source.
    pub fn backend<E: StdError + Send + Sync + 'static>(err: E) -> Self {
        Self::Backend(Box::new(err))
    }

    /// Creates a loader error from any error, preserving it as the error source.
    pub fn loader<E: StdError + Send + Sync + 'static>(err: E) -> Self {
        Self::Loader(Box::new(err))
    }

    /// Creates a custom/business error from any error, preserving it as the error source.
    pub fn custom<E: StdError + Send + Sync + 'static>(err: E) -> Self {
        Self::Custom(Box::new(err))
    }
}

#[cfg(any(feature = "redis-deadpool", feature = "redis-connection"))]
impl From<redis::RedisError> for CacheError {
    fn from(err: redis::RedisError) -> Self {
        Self::Backend(Box::new(err))
    }
}

#[cfg(feature = "redis-deadpool")]
impl From<deadpool_redis::PoolError> for CacheError {
    fn from(err: deadpool_redis::PoolError) -> Self {
        Self::Backend(Box::new(err))
    }
}
