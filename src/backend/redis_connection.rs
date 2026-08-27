//! Direct redis-rs backend implementation (requires the `redis-connection` feature).
//!
//! [`Backend`] is implemented directly for [`redis::aio::ConnectionManager`], so a connection
//! manager can be passed straight into [`crate::MultiLevelCache::new`] without any wrapper type.
//! The connection reconnects automatically and no external connection pool is needed.
//!
//! Batch reads use `MGET` and batch writes use a pipeline.

use std::time::Duration;

use async_trait::async_trait;
use redis::AsyncCommands;
use redis::aio::ConnectionManager;

use crate::backend::Backend;
use crate::error::CacheError;

#[async_trait]
impl Backend for ConnectionManager {
    fn name(&self) -> &'static str {
        "redis-connection"
    }

    async fn get(&self, key: &str) -> Result<Option<String>, CacheError> {
        let mut conn = self.clone();
        let value: Option<String> = AsyncCommands::get(&mut conn, key).await?;
        Ok(value)
    }

    async fn get_many(&self, keys: &[String]) -> Result<Vec<Option<String>>, CacheError> {
        let mut conn = self.clone();
        let values: Vec<Option<String>> = AsyncCommands::mget(&mut conn, keys).await?;
        Ok(values)
    }

    async fn set_ex(&self, key: &str, value: String, ttl: Duration) -> Result<(), CacheError> {
        let mut conn = self.clone();
        let _: () = AsyncCommands::set_ex(&mut conn, key, value, ttl.as_secs()).await?;
        Ok(())
    }

    async fn set_ex_many(&self, items: &[(String, String, Duration)]) -> Result<(), CacheError> {
        let mut conn = self.clone();
        let mut pipe = redis::pipe();
        for (key, value, ttl) in items {
            pipe.set_ex(key, value, ttl.as_secs()).ignore();
        }
        let _: () = pipe.query_async(&mut conn).await?;
        Ok(())
    }

    async fn del(&self, key: &str) -> Result<(), CacheError> {
        let mut conn = self.clone();
        let _: usize = AsyncCommands::del(&mut conn, key).await?;
        Ok(())
    }

    async fn del_many(&self, keys: &[String]) -> Result<(), CacheError> {
        let mut conn = self.clone();
        let _: usize = AsyncCommands::del(&mut conn, keys).await?;
        Ok(())
    }
}
