//! # multi-level-cache
//!
//! A general-purpose async multi-level cache library:
//!
//! ```text
//! Request → L1 (in-process Moka, typed objects, Arc-shared) → L2 (distributed backend) → L3 (caller loader)
//!             <1ms                                     2-5ms                         10-50ms
//! ```
//!
//! ## Features
//!
//! - **Multi-level read path**: L1 hits return with zero-copy; L2 misses backfill L1; L3 loader
//!   loads and backfills each level in turn
//! - **Batch pipeline**: `get_or_load_batch` hits L1 one by one, then queries L2 with MGET, and
//!   loads the remaining keys with a single loader call
//! - **Parameter dedup**: multiple parameters mapping to the same key are loaded once, and the
//!   result is broadcast to all corresponding indices
//! - **Graceful degradation**: a failed L2 batch read degrades to a full load automatically
//! - **Pluggable backends**: built-in in-memory backend (development/testing), optional Redis
//!   (deadpool or redis-rs direct connection)
//! - **Master switch**: `CacheConfig::enabled` can bypass to the loader entirely for benchmark
//!   comparison
//! - **Optional metrics**: the `metrics` feature collects `cache:{name}:{layer}:{op}` metrics
//!
//! ## Quick start
//!
//! ```no_run
//! use std::time::Duration;
//! use multi_level_cache::{CacheConfig, InMemoryBackend, MultiLevelCache};
//!
//! let cache = MultiLevelCache::<u64>::new(
//!     InMemoryBackend::new(),
//!     CacheConfig::new(true, 10_000, Duration::from_secs(60)),
//! );
//! ```
//!
//! ## Error handling
//!
//! Cache operations: read-through operations return the caller's error type `E` (default
//! [`CacheError`]), while write and invalidation operations return [`CacheError`] directly. Loader
//! closures return `Result<T, E>` with the caller's own error type. Cache read failures are logged
//! and degrade to the loader instead of being surfaced through `E`, so custom error types need no
//! conversion impl. Use `?` to propagate loader errors, or match [`CacheError`] variants on
//! mutations as needed.

pub mod backend;
pub mod cache;
pub mod error;

pub use backend::{Backend, InMemoryBackend};
pub use cache::{CacheConfig, MultiLevelCache};
pub use error::CacheError;
