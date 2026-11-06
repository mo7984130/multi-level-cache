//! Database caching: Redis backend + type-safe loader.
//!
//! Shows the generic error parameter `E` in action: the loader returns the app's own error type
//! (`AppError`) with zero conversion, while internal cache failures are logged and degrade to the
//! loader, so `AppError` needs no conversion impl.
//!
//! Requires the `redis-connection` feature: `cargo run -p multi-level-cache --features redis-connection --example database_caching`
//! A local Redis is required; the address can be set via the `REDIS_URL` environment variable.

use std::time::Duration;

use multi_level_cache::{CacheConfig, MultiLevelCache};
use redis::aio::ConnectionManager;
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
struct Product {
    id: u64,
    name: String,
}

/// App-level error type. Loader closures return `Result<T, AppError>`, so business errors are
/// expressed natively; internal cache failures are logged and degrade instead of converting.
#[derive(Debug)]
enum AppError {
    /// Business error, safe to expose to the user.
    NotFound(String),
    /// Internal failure (DB down, backend failure, ...).
    Internal(String),
}

impl std::fmt::Display for AppError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound(m) => write!(f, "not found: {m}"),
            Self::Internal(m) => write!(f, "internal: {m}"),
        }
    }
}

impl std::error::Error for AppError {}

/// Simulates a database query: fails internally (DB down) or returns the row.
async fn query_db(id: u64) -> Result<Option<Product>, AppError> {
    tokio::time::sleep(Duration::from_millis(20)).await;
    // Simulates one DB failure (internal error).
    if id == 0 {
        return Err(AppError::Internal("database connection refused".into()));
    }
    Ok(Some(Product {
        id,
        name: format!("product-{id}"),
    }))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into());
    let client = redis::Client::open(redis_url)?;
    let conn = ConnectionManager::new(client).await?;

    // E = AppError: the loader returns AppError; business errors pass through untouched.
    let cache = MultiLevelCache::<Product, AppError>::new(
        conn,
        CacheConfig::new(true, 10_000, Duration::from_secs(60)),
    );

    // Normal path.
    let product = cache
        .get_or_load("product:42", Duration::from_secs(3600), || async {
            query_db(42)
                .await?
                .ok_or_else(|| AppError::NotFound("product 42".into()))
        })
        .await?;
    println!("read: {product:?}");

    // Cache hit (the loader is not called again).
    let product2 = cache
        .get_or_load("product:42", Duration::from_secs(3600), || async {
            query_db(42)
                .await?
                .ok_or_else(|| AppError::NotFound("product 42".into()))
        })
        .await?;
    assert_eq!(product, product2);
    println!("second read hit, data consistent");

    // Business error: the loader returns AppError::NotFound, passed through untouched.
    match cache
        .get_or_load("product:999", Duration::from_secs(3600), || async {
            Err(AppError::NotFound("product 999".into()))
        })
        .await
    {
        Ok(_) => unreachable!(),
        Err(AppError::NotFound(msg)) => println!("business error passed through: {msg}"),
        Err(e) => panic!("unexpected error: {e}"),
    }
    Ok(())
}
