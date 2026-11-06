//! The multi-level cache component.
//!
//! Read order: L1 (in-process Moka) → L2 (distributed backend) → L3 (caller loader), each level
//! backfills the previous one on miss.
//! Write policy: updates go through [`put`](MultiLevelCache::put) which overwrites L1 and L2
//! directly, avoiding the penetration window of "delete-then-write"; deletions go through
//! [`invalidate`](MultiLevelCache::invalidate) which clears both L1 and L2.
//!
//! L1 relies only on a short TTL and active invalidation for eventual consistency; no
//! cross-instance broadcast is performed. [`CacheConfig::enabled`] can disable the cache
//! entirely (reads/writes pass through to the loader), which is handy for benchmark comparison.

use std::fmt::Debug;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::{Duration, Instant};

use indexmap::IndexMap;
use moka::future::Cache;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tracing::warn;

use crate::backend::Backend;
use crate::error::CacheError;

/// Configuration for a cache instance.
#[derive(Debug, Clone, Copy)]
pub struct CacheConfig {
    /// Whether the cache is enabled. When disabled, reads/writes pass straight through to the
    /// loader, which is useful for benchmark comparison.
    pub enabled: bool,
    /// Maximum number of entries in the L1 local cache.
    pub local_capacity: u64,
    /// L1 local cache TTL (a short TTL keeps multi-instance deployments eventually consistent).
    pub local_ttl: Duration,
}

impl CacheConfig {
    /// Creates a cache configuration.
    pub fn new(enabled: bool, local_capacity: u64, local_ttl: Duration) -> Self {
        Self {
            enabled,
            local_capacity,
            local_ttl,
        }
    }
}

/// The multi-level cache component.
///
/// The generic parameter `T` is the cached value type; one instance caches exactly one type.
/// Different data types each hold their own independent [`MultiLevelCache`] instance.
///
/// The generic parameter `E` is the caller's error type, defaulting to [`CacheError`]. Loader
/// closures return `Result<T, E>`, so the caller can use its own error type without wrapping.
/// Internal cache failures are logged and degraded instead of being surfaced through `E`, so
/// custom error types need no conversion impl. This keeps the cache library self-contained
/// while letting the caller express business errors natively.
pub struct MultiLevelCache<T, E = CacheError> {
    /// Instance name used as the `cache:{name}:*` metric prefix (present only with the `metrics`
    /// feature).
    #[cfg(feature = "metrics")]
    name: &'static str,
    /// L1 in-process cache storing deserialized objects; hits are zero-copy.
    local: Cache<String, Arc<T>>,
    /// L2 distributed backend storing JSON strings.
    backend: Arc<dyn Backend>,
    /// Whether the cache is enabled. When disabled, reads/writes pass through to the loader.
    enabled: bool,
    /// `E` only appears in method signatures, so it needs a type marker.
    _marker: PhantomData<fn(E) -> E>,
}

impl<T, E> MultiLevelCache<T, E>
where
    T: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    /// Default instance name used as the metric prefix when none is specified.
    const DEFAULT_NAME: &'static str = "multi-level-cache";

    /// Creates a cache instance with the default instance name.
    ///
    /// # Arguments
    /// - `backend`: the L2 distributed backend (Redis or an in-memory implementation)
    /// - `config`: the cache configuration (enable switch, L1 capacity, L1 TTL)
    pub fn new(backend: impl Backend, config: CacheConfig) -> Self {
        Self::new_with_name(Self::DEFAULT_NAME, backend, config)
    }

    /// Creates a cache instance with a custom instance name.
    ///
    /// The name is only used as the `cache:{name}:*` metric prefix and is ignored when the
    /// `metrics` feature is disabled. Use this when you need to tell the metrics of distinct
    /// cache instances apart.
    ///
    /// # Arguments
    /// - `name`: the cache instance name (only takes effect with the `metrics` feature)
    /// - `backend`: the L2 distributed backend (Redis or an in-memory implementation)
    /// - `config`: the cache configuration (enable switch, L1 capacity, L1 TTL)
    pub fn new_with_name(name: &'static str, backend: impl Backend, config: CacheConfig) -> Self {
        #[cfg(not(feature = "metrics"))]
        let _ = name;

        // When disabled, set L1 capacity to 0 so no useless in-memory cache is retained.
        let capacity = if config.enabled {
            config.local_capacity
        } else {
            0
        };
        let local = Cache::builder()
            .max_capacity(capacity)
            .time_to_live(config.local_ttl)
            .build();
        Self {
            #[cfg(feature = "metrics")]
            name,
            local,
            backend: Arc::new(backend),
            enabled: config.enabled,
            _marker: PhantomData,
        }
    }

    /// Gets a value from the cache, falling back to L2 and then the loader, backfilling each
    /// level on miss or cache-read failure.
    ///
    /// The loader returns the caller's error type `E`. Cache read failures are logged and fall
    /// through to the loader; a loader failure is returned to the caller.
    ///
    /// # Arguments
    /// - `key`: the cache key
    /// - `ttl`: the L2 (distributed backend) expiration time
    /// - `loader`: the L3 data-source loader closure, called when L1/L2 miss or L2 is unavailable
    pub async fn get_or_load<F, Fut>(&self, key: &str, ttl: Duration, loader: F) -> Result<T, E>
    where
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = Result<T, E>> + Send,
    {
        // Pass straight through to the data source when the cache is disabled.
        if !self.enabled {
            return loader().await;
        }

        let key_owned = key.to_string();

        // ---- L1 hit ----
        let start = Instant::now();
        if let Some(value) = self.local.get(&key_owned).await {
            self.record_duration("l1:get:duration_seconds", start);
            self.inc_counter("l1:hits", 1);
            return Ok((*value).clone());
        }
        self.record_duration("l1:get:duration_seconds", start);
        self.inc_counter("l1:misses", 1);

        // ---- L2 hit ----
        let start = Instant::now();
        let cached = match self.backend.get(&key_owned).await {
            Ok(cached) => cached,
            Err(e) => {
                warn!(
                    "multi_level_cache: failed to read L2 key={}, degrading to L3: {e:?}",
                    key
                );
                None
            }
        };
        if let Some(json) = cached {
            match serde_json::from_str::<T>(&json) {
                Ok(value) => {
                    self.record_duration("l2:get:duration_seconds", start);
                    self.inc_counter("l2:hits", 1);
                    self.write_l1(key_owned, value.clone()).await;
                    return Ok(value);
                }
                Err(e) => {
                    warn!(
                        "multi_level_cache: failed to deserialize L2 value key={}: {e:?}",
                        key
                    );
                }
            }
        }
        self.record_duration("l2:get:duration_seconds", start);
        self.inc_counter("l2:misses", 1);

        // ---- L3 data-source load ----
        let start = Instant::now();
        let value = loader().await?;
        self.record_duration("db:load:duration_seconds", start);
        self.inc_counter("db:loads", 1);

        // Backfill L2 + L1 (best-effort, does not affect the read result).
        if let Ok(json) = serde_json::to_string(&value) {
            self.backend
                .set_ex(&key_owned, json, ttl)
                .await
                .unwrap_or_else(|e| {
                    warn!("multi_level_cache: failed to write L2 key={}: {e:?}", key);
                });
        } else {
            warn!("multi_level_cache: failed to serialize value key={}", key);
        }
        self.write_l1(key_owned, value.clone()).await;

        Ok(value)
    }

    /// Batch-fetches values, loading misses through the loader and backfilling them.
    ///
    /// L1 is hit one by one first; the remaining keys are fetched from L2 in a single MGET; any
    /// keys still missing are loaded by the loader. Parameters that map to the same key are
    /// loaded only once and the result is broadcast to all matching indices.
    ///
    /// # Arguments
    /// - `params`: the parameters to query
    /// - `key_provider`: maps a parameter to a cache key
    /// - `ttl`: the L2 (distributed backend) expiration time
    /// - `loader`: the L3 data-source batch loader closure
    /// - `result_mapper`: extracts the parameter key back from a result value
    ///
    /// # Returns
    /// A result list with the same length as `params`; entries that missed the cache and were
    /// not returned by the loader are `None`.
    pub async fn get_or_load_batch<K, F, Fut, M>(
        &self,
        params: &[K],
        key_provider: impl Fn(&K) -> String + Send + Sync,
        ttl: Duration,
        loader: F,
        result_mapper: M,
    ) -> Result<Vec<Option<T>>, E>
    where
        K: Clone + Send + Sync + std::hash::Hash + Eq + Debug,
        F: FnOnce(Vec<K>) -> Fut + Send,
        Fut: Future<Output = Result<Vec<T>, E>> + Send,
        M: Fn(&T) -> K + Send + Sync,
    {
        // When disabled, load everything directly (align results by K after the loader returns;
        // no cache keys are generated).
        if !self.enabled {
            let fresh_data = loader(params.to_vec()).await?;
            let mut index_by_k: IndexMap<K, Vec<usize>> = IndexMap::new();
            for (i, p) in params.iter().enumerate() {
                index_by_k.entry(p.clone()).or_default().push(i);
            }
            let mut results: Vec<Option<T>> = vec![None; params.len()];
            self.align_and_collect(
                fresh_data,
                result_mapper,
                |k| index_by_k.get(k).map(|indices| ("", indices)),
                &mut results,
            );
            return Ok(results);
        }

        let mut results: Vec<Option<T>> = vec![None; params.len()];

        // ---- L1 layer: hit one by one ----
        let mut l1_miss: Vec<(usize, K)> = Vec::new();
        for (i, p) in params.iter().enumerate() {
            let key = key_provider(p);
            let start = Instant::now();
            if let Some(value) = self.local.get(&key).await {
                self.record_duration("l1:get:duration_seconds", start);
                self.inc_counter("l1:hits", 1);
                results[i] = Some((*value).clone());
            } else {
                self.record_duration("l1:get:duration_seconds", start);
                self.inc_counter("l1:misses", 1);
                l1_miss.push((i, p.clone()));
            }
        }

        if l1_miss.is_empty() {
            self.update_entries_gauge();
            return Ok(results);
        }

        // ---- L2 layer: batch MGET ----
        // param -> (cache key, [original indices])
        let mut key_to_info: IndexMap<K, (String, Vec<usize>)> = IndexMap::new();
        for (i, p) in l1_miss {
            let key = key_provider(&p);
            let entry = key_to_info.entry(p).or_insert_with(|| (key, Vec::new()));
            entry.1.push(i);
        }

        let unique_keys: Vec<String> = key_to_info.values().map(|(key, _)| key.clone()).collect();
        let start = Instant::now();
        let cached_jsons: Vec<Option<String>> = self
            .backend
            .get_many(&unique_keys)
            .await
            .unwrap_or_else(|e| {
                warn!("multi_level_cache: MGET failed, degrading to full load: {e:?}");
                vec![None; unique_keys.len()]
            });
        self.record_duration("l2:get:duration_seconds", start);

        let mut l2_miss_keys: Vec<usize> = Vec::new();
        for (idx, (_, (key, orig_indices))) in key_to_info.iter().enumerate() {
            match cached_jsons.get(idx).and_then(|o| o.as_deref()) {
                Some(json) => match serde_json::from_str::<T>(json) {
                    Ok(value) => {
                        self.inc_counter("l2:hits", 1);
                        self.write_l1(key.clone(), value.clone()).await;
                        for &i in orig_indices {
                            results[i] = Some(value.clone());
                        }
                    }
                    Err(e) => {
                        warn!(
                            "multi_level_cache: failed to deserialize L2 value key={}: {e:?}",
                            key
                        );
                        self.inc_counter("l2:misses", 1);
                        l2_miss_keys.push(idx);
                    }
                },
                None => {
                    self.inc_counter("l2:misses", 1);
                    l2_miss_keys.push(idx);
                }
            }
        }

        // ---- L3 layer: batch load and write back ----
        if !l2_miss_keys.is_empty() {
            let miss_params: Vec<K> = l2_miss_keys
                .iter()
                .map(|&idx| key_to_info.get_index(idx).unwrap().0.clone())
                .collect();

            let start = Instant::now();
            let fresh_data = loader(miss_params).await?;
            self.record_duration("db:load:duration_seconds", start);
            self.inc_counter("db:loads", fresh_data.len() as u64);

            let write_back = self.align_and_collect(
                fresh_data,
                result_mapper,
                |k| {
                    key_to_info
                        .get(k)
                        .map(|(key, indices)| (key.as_str(), indices))
                },
                &mut results,
            );

            if !write_back.is_empty() {
                let mut items: Vec<(String, String, Duration)> =
                    Vec::with_capacity(write_back.len());
                for (key, item) in write_back {
                    self.write_l1(key.clone(), item.clone()).await;
                    match serde_json::to_string(&item) {
                        Ok(json) => items.push((key, json, ttl)),
                        Err(e) => warn!(
                            "multi_level_cache: failed to serialize L2 value key={}: {e:?}",
                            key
                        ),
                    }
                }
                // Backfill failure must not affect the read result (best-effort).
                if !items.is_empty() {
                    self.backend.set_ex_many(&items).await.unwrap_or_else(|e| {
                        warn!("multi_level_cache: failed to batch-write L2: {e:?}");
                    });
                }
            }
        }

        self.update_entries_gauge();
        Ok(results)
    }

    /// Overwrites L1 and L2 directly (for updating the cache after a write).
    ///
    /// Compared to "delete-then-write", overwriting avoids the penetration window during
    /// concurrent access, and a read right after the write sees the fresh value.
    pub async fn put(&self, key: &str, value: T, ttl: Duration) -> Result<(), CacheError> {
        // Writes are no-ops when the cache is disabled.
        if !self.enabled {
            return Ok(());
        }

        let json = serde_json::to_string(&value)?;

        // L1 overwrite (moka `insert` resets the TTL automatically).
        self.write_l1(key.to_string(), value).await;

        // L2 overwrite.
        self.backend.set_ex(key, json, ttl).await?;

        self.update_entries_gauge();
        Ok(())
    }

    /// Removes a single key from the cache (L1 + L2).
    ///
    /// Used for data-deletion scenarios where an overwrite cannot express "the data no longer
    /// exists".
    pub async fn invalidate(&self, key: &str) -> Result<(), CacheError> {
        // Invalidation is a no-op when the cache is disabled.
        if !self.enabled {
            return Ok(());
        }

        self.local.invalidate(&key.to_string()).await;
        self.backend.del(key).await?;
        self.update_entries_gauge();
        Ok(())
    }

    /// Batch-removes keys from the cache (L1 + L2).
    pub async fn invalidate_batch(&self, keys: &[String]) -> Result<(), CacheError> {
        // Invalidation is a no-op when the cache is disabled.
        if !self.enabled {
            return Ok(());
        }

        if keys.is_empty() {
            return Ok(());
        }
        for key in keys {
            self.local.invalidate(key).await;
        }
        self.backend.del_many(keys).await?;
        self.update_entries_gauge();
        Ok(())
    }

    /// Writes into L1. The `Arc` shares the value by reference count, avoiding a full copy.
    async fn write_l1(&self, key: String, value: T) {
        self.local.insert(key, Arc::new(value)).await;
    }

    /// Aligns the batch loader results into `results` via `result_mapper`.
    ///
    /// `index_lookup` maps a `K` to its cache key and original indices; an empty cache key means
    /// no write-back is needed (the disabled-cache path). Returns the `(key, value)` pairs that
    /// still need to be written back to L2.
    fn align_and_collect<'a, K, M, L>(
        &self,
        fresh_data: Vec<T>,
        result_mapper: M,
        index_lookup: L,
        results: &mut [Option<T>],
    ) -> Vec<(String, T)>
    where
        M: Fn(&T) -> K,
        L: Fn(&K) -> Option<(&'a str, &'a Vec<usize>)>,
    {
        let mut write_back = Vec::new();
        for item in fresh_data {
            let k = result_mapper(&item);
            match index_lookup(&k) {
                Some((cache_key, indices)) => {
                    if !cache_key.is_empty() {
                        write_back.push((cache_key.to_owned(), item.clone()));
                    }
                    for &i in indices {
                        results[i] = Some(item.clone());
                    }
                }
                None => {
                    warn!("multi_level_cache: loader returned an unrequested key");
                }
            }
        }
        write_back
    }

    // ============ Metric instrumentation ============

    #[cfg(feature = "metrics")]
    #[inline]
    fn metric_name(&self, step: &str) -> String {
        format!("cache:{}:{}", self.name, step)
    }

    #[cfg(feature = "metrics")]
    #[inline]
    fn inc_counter(&self, step: &str, value: u64) {
        metrics::counter!(self.metric_name(step)).increment(value);
    }

    #[cfg(feature = "metrics")]
    #[inline]
    fn record_duration(&self, step: &str, start: Instant) {
        metrics::histogram!(self.metric_name(step)).record(start.elapsed().as_secs_f64());
    }

    #[cfg(feature = "metrics")]
    #[inline]
    fn update_entries_gauge(&self) {
        metrics::gauge!(self.metric_name("l1:entries")).set(self.local.entry_count() as f64);
    }

    #[cfg(not(feature = "metrics"))]
    #[inline]
    fn inc_counter(&self, _step: &str, _value: u64) {}

    #[cfg(not(feature = "metrics"))]
    #[inline]
    fn record_duration(&self, _step: &str, _start: Instant) {}

    #[cfg(not(feature = "metrics"))]
    #[inline]
    fn update_entries_gauge(&self) {}
}
