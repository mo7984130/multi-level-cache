//! Backend implementation over [`deadpool_redis::Pool`] (requires the `redis-deadpool` feature).
//!
//! [`Backend`] is implemented directly for [`deadpool_redis::Pool`], so a pool can be passed
//! straight into [`crate::MultiLevelCache::new`] without any wrapper type.
//!
//! Batch reads use `MGET` and batch writes use a pipeline, reducing network round trips.

use std::time::Duration;

use async_trait::async_trait;
use deadpool_redis::Pool;
use redis::AsyncCommands;

use crate::backend::Backend;
use crate::error::CacheError;

#[async_trait]
impl Backend for Pool {
    fn name(&self) -> &'static str {
        "redis-deadpool"
    }

    async fn get(&self, key: &str) -> Result<Option<String>, CacheError> {
        let mut conn = self.get().await?;
        let value: Option<String> = conn.get(key).await?;
        Ok(value)
    }

    async fn get_many(&self, keys: &[String]) -> Result<Vec<Option<String>>, CacheError> {
        let mut conn = self.get().await?;
        let values: Vec<Option<String>> = conn.mget(keys).await?;
        Ok(values)
    }

    async fn set_ex(&self, key: &str, value: String, ttl: Duration) -> Result<(), CacheError> {
        let mut conn = self.get().await?;
        let _: () = conn.set_ex(key, value, ttl.as_secs()).await?;
        Ok(())
    }

    async fn set_ex_many(&self, items: &[(String, String, Duration)]) -> Result<(), CacheError> {
        let mut conn = self.get().await?;
        let mut pipe = redis::pipe();
        for (key, value, ttl) in items {
            pipe.set_ex(key, value, ttl.as_secs()).ignore();
        }
        let _: () = pipe.query_async(&mut conn).await?;
        Ok(())
    }

    async fn del(&self, key: &str) -> Result<(), CacheError> {
        let mut conn = self.get().await?;
        let _: usize = conn.del(key).await?;
        Ok(())
    }

    async fn del_many(&self, keys: &[String]) -> Result<(), CacheError> {
        let mut conn = self.get().await?;
        let _: usize = conn.del(keys).await?;
        Ok(())
    }
}
