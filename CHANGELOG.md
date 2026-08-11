# Changelog

This project follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- `MultiLevelCache<T, E>` is now generic over the error type `E` (default `CacheError`).
  Loader closures return `Result<T, E>` with the caller's own error type, so no manual
  conversion is needed; internal failures ([`CacheError`]) are converted via `E: From<CacheError>`.
- `CacheError::Backend` and `CacheError::Loader` now carry the underlying error as a
  `Box<dyn Error + Send + Sync>` instead of a message string, preserving the error chain via
  `Error::source`. Construct with `CacheError::backend(e)` / `CacheError::loader(e)`.
- Added `CacheError::Custom` for caller-defined business errors, distinguishable from internal
  failures. Construct with `CacheError::custom(e)`.
- `Backend` is now implemented directly for `deadpool_redis::Pool` (`redis-deadpool`) and
  `redis::aio::ConnectionManager` (`redis-connection`), so native Redis types can be passed
  straight into `MultiLevelCache::new` without a wrapper. The `RedisDeadpoolBackend` and
  `RedisConnectionBackend` wrapper types were removed.

## [0.1.0] - 2026-08-11

### Added

- `MultiLevelCache`: L1 in-process Moka + L2 distributed backend + L3 data-source loader
- Single-key `get_or_load` and batch `get_or_load_batch` (MGET + parameter dedup + result alignment)
- Write operations `put` (overwrite) and `invalidate` / `invalidate_batch` (delete)
- `Backend` trait abstraction with a built-in `InMemoryBackend`
- Redis backends: deadpool connection pool (`redis-deadpool` feature) and redis-rs direct
  connection (`redis-connection` feature)
- Unified `CacheError` (backend / serialization / data-source loading)
- `metrics` feature instrumentation
- Benchmarks, integration tests, examples and documentation
