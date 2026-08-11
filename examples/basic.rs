//! Basic usage: in-memory backend + single-key reads/writes.
//!
//! Depends on no feature (including `metrics`); builds and runs with the default configuration:
//! `cargo run -p multi-level-cache --example basic`

use std::time::Duration;

use multi_level_cache::{CacheConfig, InMemoryBackend, MultiLevelCache};

#[derive(Clone, serde::Serialize, serde::Deserialize, Debug)]
struct User {
    id: u64,
    name: String,
}

fn user(id: u64) -> User {
    User {
        id,
        name: format!("user-{id}"),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cache = MultiLevelCache::<User>::new(
        InMemoryBackend::new(),
        CacheConfig::new(true, 10_000, Duration::from_secs(60)),
    );

    // On miss: call the loader and backfill.
    let u = cache
        .get_or_load("user:1", Duration::from_secs(3600), || async { Ok(user(1)) })
        .await?;
    println!("first read (through the loader): {u:?}");

    // On hit: the loader is not called again.
    let u = cache
        .get_or_load("user:1", Duration::from_secs(3600), || async { Ok(user(999)) })
        .await?;
    println!("second read (cache hit): {u:?}");

    // Overwrite via put.
    cache.put("user:1", User { id: 1, name: "renamed".into() }, Duration::from_secs(3600)).await?;

    // Delete via invalidate.
    cache.invalidate("user:1").await?;
    println!("deleted");
    Ok(())
}
