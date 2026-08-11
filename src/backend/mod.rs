//! The L2 backend abstraction and its built-in implementations.
//!
//! Backends operate on "string + TTL" units: the serialization strategy for values and keys is
//! decided by the upper [`crate::MultiLevelCache`] layer, while a backend only handles
//! read/write and expiration. This keeps the type-safe generic layer decoupled from the concrete
//! storage and makes test doubles easy.
//!
//! Built-in implementations:
//! - [`InMemoryBackend`]: in-process in-memory backend, zero IO, for development, tests and
//!   benchmarks
//! - `redis-deadpool` feature: [`Backend`] for [`deadpool_redis::Pool`]
//! - `redis-connection` feature: [`Backend`] for [`redis::aio::ConnectionManager`]

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use async_trait::async_trait;

use crate::error::CacheError;

#[cfg(feature = "redis-deadpool")]
mod redis_deadpool;
#[cfg(feature = "redis-connection")]
mod redis_connection;

/// The L2 cache backend abstraction.
///
/// Implementations must be thread-safe (`Send + Sync`). All errors are unified under
/// [`CacheError`].
#[async_trait]
pub trait Backend: Send + Sync + 'static {
    /// Backend identifier, used for logging and diagnostics.
    fn name(&self) -> &'static str;

    /// Reads a single key; `None` means the key is absent or expired.
    async fn get(&self, key: &str) -> Result<Option<String>, CacheError>;

    /// Batch-reads multiple keys; the result is aligned one-to-one with `keys`.
    async fn get_many(&self, keys: &[String]) -> Result<Vec<Option<String>>, CacheError>;

    /// Writes a single key with an expiration time.
    async fn set_ex(&self, key: &str, value: String, ttl: Duration) -> Result<(), CacheError>;

    /// Batch-writes entries with an expiration time.
    async fn set_ex_many(&self, items: &[(String, String, Duration)]) -> Result<(), CacheError>;

    /// Deletes a single key.
    async fn del(&self, key: &str) -> Result<(), CacheError>;

    /// Batch-deletes multiple keys.
    async fn del_many(&self, keys: &[String]) -> Result<(), CacheError>;
}

struct Entry {
    value: String,
    expires_at: Option<Instant>,
}

/// In-process in-memory backend (zero IO).
///
/// Provides semantics identical to a real distributed backend: TTL-aware reads/writes and batch
/// operations. Used in development, unit tests and benchmarks to remove network-IO noise from
/// measurements.
#[derive(Default)]
pub struct InMemoryBackend {
    store: Mutex<HashMap<String, Entry>>,
}

impl InMemoryBackend {
    /// Creates an empty in-memory backend.
    pub fn new() -> Self {
        Self::default()
    }
}

impl Entry {
    fn is_expired(&self, now: Instant) -> bool {
        self.expires_at.is_some_and(|t| now >= t)
    }
}

#[async_trait]
impl Backend for InMemoryBackend {
    fn name(&self) -> &'static str {
        "in-memory"
    }

    async fn get(&self, key: &str) -> Result<Option<String>, CacheError> {
        let now = Instant::now();
        let mut store = self.store.lock().unwrap();
        if let Some(entry) = store.get(key) {
            if !entry.is_expired(now) {
                return Ok(Some(entry.value.clone()));
            }
        }
        store.remove(key);
        Ok(None)
    }

    async fn get_many(&self, keys: &[String]) -> Result<Vec<Option<String>>, CacheError> {
        let mut values = Vec::with_capacity(keys.len());
        for key in keys {
            values.push(self.get(key).await?);
        }
        Ok(values)
    }

    async fn set_ex(&self, key: &str, value: String, ttl: Duration) -> Result<(), CacheError> {
        self.store.lock().unwrap().insert(
            key.to_string(),
            Entry {
                value,
                expires_at: Some(Instant::now() + ttl),
            },
        );
        Ok(())
    }

    async fn set_ex_many(&self, items: &[(String, String, Duration)]) -> Result<(), CacheError> {
        let now = Instant::now();
        let mut store = self.store.lock().unwrap();
        for (key, value, ttl) in items {
            store.insert(
                key.clone(),
                Entry {
                    value: value.clone(),
                    expires_at: Some(now + *ttl),
                },
            );
        }
        Ok(())
    }

    async fn del(&self, key: &str) -> Result<(), CacheError> {
        self.store.lock().unwrap().remove(key);
        Ok(())
    }

    async fn del_many(&self, keys: &[String]) -> Result<(), CacheError> {
        let mut store = self.store.lock().unwrap();
        for key in keys {
            store.remove(key);
        }
        Ok(())
    }
}
