# multi-level-cache

[![Crates.io](https://img.shields.io/crates/v/multi-level-cache.svg)](https://crates.io/crates/multi-level-cache)
[![docs.rs](https://docs.rs/multi-level-cache/badge.svg)](https://docs.rs/multi-level-cache)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](#license)

A general-purpose async multi-level cache library (in-process L1 + distributed L2 backend + data-source L3).

**Languages: [English](README.md) · [中文](README.zh-CN.md)**

```text
Request → L1 (in-process Moka, typed objects, Arc-shared) → L2 (distributed backend) → L3 (caller loader)
            <1ms                                     2-5ms                         10-50ms
```

## Features

- **Multi-level read path**: L1 hits return with zero-copy; L2 misses backfill L1; L3 loader loads and backfills each level in turn
- **Batch pipeline**: `get_or_load_batch` hits L1 one by one, queries L2 with `MGET`, then loads the remaining keys with a single loader call
- **Parameter dedup**: multiple parameters mapping to the same key are loaded once, and the result is broadcast to all matching indices
- **Graceful degradation**: a failed L2 batch read degrades to a full load; a failed backfill write never affects the read result
- **Pluggable backends**: built-in in-memory backend (development/testing/zero-dependency start), optional Redis (deadpool or redis-rs direct connection)
- **Master switch**: `CacheConfig::enabled` bypasses to the loader entirely for benchmark comparison
- **Optional metrics**: the `metrics` feature collects `cache:{name}:{layer}:{op}` metrics

## Installation

```toml
[dependencies]
multi-level-cache = "0.1"
tokio = { version = "1", features = ["full"] }
```

## Quick start

```rust,no_run
use std::time::Duration;
use multi_level_cache::{CacheConfig, InMemoryBackend, MultiLevelCache};

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct User { id: u64, name: String }

let cache = MultiLevelCache::<User>::new(
    InMemoryBackend::new(),
    CacheConfig::new(true, 10_000, Duration::from_secs(60)),
);

// On miss the loader is called and every level is backfilled.
let user = cache
    .get_or_load("user:1", Duration::from_secs(3600), || async {
        Ok(User { id: 1, name: "alice".into() })
    })
    .await?;
```

## Redis backends

Two optional backends; pick one based on your dependency preferences. Both are implemented
directly for the native Redis types, so no wrapper is needed:

```toml
# deadpool-redis connection pool (shares an existing pool with your app)
multi-level-cache = { version = "0.1", features = ["redis-deadpool"] }

# redis-rs direct connection (built-in ConnectionManager, auto-reconnect)
multi-level-cache = { version = "0.1", features = ["redis-connection"] }
```

Pass a `deadpool_redis::Pool` straight into `new`:

```rust,no_run
use multi_level_cache::{CacheConfig, MultiLevelCache};

let pool = deadpool_redis::Config::from_url("redis://127.0.0.1:6379")
    .create_pool(Some(deadpool_redis::Runtime::Tokio1))?;

let cache = MultiLevelCache::<User>::new(
    pool,
    CacheConfig::new(true, 10_000, Duration::from_secs(60)),
);
```

Or a `redis::aio::ConnectionManager`:

```rust,no_run
use multi_level_cache::{CacheConfig, MultiLevelCache};

let client = redis::Client::open("redis://127.0.0.1:6379")?;
let conn = redis::aio::ConnectionManager::new(client).await?;

let cache = MultiLevelCache::<User>::new(
    conn,
    CacheConfig::new(true, 10_000, Duration::from_secs(60)),
);
```

## Batch reads

```rust,no_run
let ids = [1u64, 2, 1, 3]; // duplicate ids are loaded only once
let result: Vec<Option<User>> = cache
    .get_or_load_batch(
        &ids,
        |id| format!("user:{id}"),                    // key mapping
        Duration::from_secs(3600),
        |miss_ids| async { Ok(fetch_users(&miss_ids).await) }, // batch loader
        |user| user.id,                               // reverse key lookup
    )
    .await?;
```

## Writes and deletions

- **Update**: `put` overwrites L1 and L2 directly, avoiding the penetration window of "delete-then-write"; a read right after the write sees the fresh value
- **Delete**: `invalidate` / `invalidate_batch` clear both L1 and L2

```rust,no_run
cache.put("user:1", user, Duration::from_secs(3600)).await?;
cache.invalidate("user:1").await?;
```

## Configuration

| Field | Description |
|-------|-------------|
| `enabled` | Whether the cache is enabled; `false` makes reads/writes pass through to the loader (for benchmark comparison) |
| `local_capacity` | Maximum number of L1 entries |
| `local_ttl` | L1 TTL; a short TTL keeps multi-instance deployments eventually consistent |

## Error handling

Cache operations return the caller's error type `E` (default
[`CacheError`](https://docs.rs/multi-level-cache/latest/multi_level_cache/enum.CacheError.html)):

- `Backend` — L2 backend error (Redis connection/command failure)
- `Serialization` — value serialization/deserialization failure
- `Loader` — data-source loading failure (upstream/infrastructure)
- `Custom` — caller-defined business error (e.g. "record not found"), distinguishable from
  internal failures

The underlying error is preserved as the error source, so the full chain stays available via
`Error::source()`. Build these with `CacheError::backend(e)` / `CacheError::loader(e)` /
`CacheError::custom(e)`, or let `?` convert Redis/Pool errors automatically.

### Custom error type

`MultiLevelCache<T, E>` is generic over the error type `E` (default `CacheError`). Loader
closures return `Result<T, E>` with the app's own error type, and internal cache failures
([`CacheError`]) are converted into `E` via `E: From<CacheError>`. This lets callers express
business vs internal errors natively:

```rust,no_run
use multi_level_cache::{CacheConfig, CacheError, MultiLevelCache};

// The app's own error type.
#[derive(Debug)]
enum AppError { NotFound(String), Internal(String) }
impl std::fmt::Display for AppError { /* ... */ }
impl std::error::Error for AppError {}

// Internal cache failures map to a kind the app can match on.
impl From<CacheError> for AppError {
    fn from(e: CacheError) -> Self {
        match e {
            CacheError::Custom(err) => AppError::NotFound(err.to_string()),
            _ => AppError::Internal(e.to_string()),
        }
    }
}

let cache = MultiLevelCache::<User, AppError>::new(
    InMemoryBackend::new(),
    CacheConfig::new(true, 10_000, Duration::from_secs(60)),
);

// Loader uses the app error directly, zero conversion.
let user = cache
    .get_or_load("user:1", Duration::from_secs(3600), || async {
        let row = db_query(1).await?;                       // → AppError (internal)
        row.ok_or(AppError::NotFound("user:1".into()))?     // → AppError (business)
    })
    .await?;
```

## Custom backends

Implement the [`Backend`](https://docs.rs/multi-level-cache/latest/multi_level_cache/trait.Backend.html) trait (string-level read/write + TTL) to plug in any storage:

```rust,no_run
use std::time::Duration;
use async_trait::async_trait;
use multi_level_cache::{Backend, CacheError};

#[async_trait]
impl Backend for MyStore {
    fn name(&self) -> &'static str { "my-store" }
    async fn get(&self, key: &str) -> Result<Option<String>, CacheError> { /* ... */ }
    async fn get_many(&self, keys: &[String]) -> Result<Vec<Option<String>>, CacheError> { /* ... */ }
    async fn set_ex(&self, key: &str, value: String, ttl: Duration) -> Result<(), CacheError> { /* ... */ }
    async fn set_ex_many(&self, items: &[(String, String, Duration)]) -> Result<(), CacheError> { /* ... */ }
    async fn del(&self, key: &str) -> Result<(), CacheError> { /* ... */ }
    async fn del_many(&self, keys: &[String]) -> Result<(), CacheError> { /* ... */ }
}
```

## Metrics

With the `metrics` feature enabled, every cache instance collects `cache:{name}:{layer}:{op}`
metrics. The instance name is set via `MultiLevelCache::new_with_name` (default:
`multi-level-cache`); when `metrics` is disabled the name is ignored and the name-less
`MultiLevelCache::new` can be used:

```rust,no_run
let cache = MultiLevelCache::<User>::new_with_name(
    "user_info",                                  // metric prefix cache:user_info:*
    InMemoryBackend::new(),
    CacheConfig::new(true, 10_000, Duration::from_secs(60)),
);
```

Collected metrics:

- `l1:hits` / `l1:misses`, `l2:hits` / `l2:misses`
- `db:loads`
- `l1:get:duration_seconds`, `l2:get:duration_seconds`, `db:load:duration_seconds`
- `l1:entries`

## Features

| Feature | Description |
|---------|-------------|
| `redis-deadpool` | `Backend` for `deadpool_redis::Pool` |
| `redis-connection` | `Backend` for `redis::aio::ConnectionManager` |
| `metrics` | Metric instrumentation |

## Tests and benchmarks

```bash
cargo test -p multi-level-cache
cargo bench -p multi-level-cache
```

## Examples

```bash
cargo run -p multi-level-cache --example basic
cargo run -p multi-level-cache --example batch
cargo run -p multi-level-cache --features redis-connection --example database_caching  # requires a local Redis
```

## License

Dual-licensed under either of MIT or Apache-2.0, at your option.
