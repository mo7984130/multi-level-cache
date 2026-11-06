//! Integration tests for `MultiLevelCache` (backed by the built-in `InMemoryBackend`).

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use multi_level_cache::{Backend, CacheConfig, CacheError, InMemoryBackend, MultiLevelCache};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct Item {
    id: u64,
}

impl Item {
    fn new(id: u64) -> Self {
        Self { id }
    }
}

fn make_cache(capacity: u64, enabled: bool) -> MultiLevelCache<Item> {
    MultiLevelCache::new(
        InMemoryBackend::new(),
        CacheConfig::new(enabled, capacity, Duration::from_secs(60)),
    )
}

fn key_of(id: &u64) -> String {
    format!("item:{id}")
}

fn id_of(item: &Item) -> u64 {
    item.id
}

async fn loader_of(ids: Vec<u64>, counter: &AtomicUsize) -> Result<Vec<Item>, CacheError> {
    counter.fetch_add(1, Ordering::SeqCst);
    Ok(ids.into_iter().map(Item::new).collect())
}

#[tokio::test]
async fn get_or_load_l1_hit_skips_loader() {
    let cache = make_cache(1024, true);
    cache
        .put("item:1", Item::new(1), Duration::from_secs(3600))
        .await
        .unwrap();

    let calls = AtomicUsize::new(0);
    let value = cache
        .get_or_load("item:1", Duration::from_secs(3600), || {
            let calls = &calls;
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(Item::new(999))
            }
        })
        .await
        .unwrap();

    assert_eq!(value, Item::new(1));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn get_or_load_l2_hit_backfills_l1() {
    // Capacity 0 forces L1 to never hit; only L2 is used.
    let cache = make_cache(0, true);
    cache
        .put("item:1", Item::new(1), Duration::from_secs(3600))
        .await
        .unwrap();

    let calls = AtomicUsize::new(0);
    let value = cache
        .get_or_load("item:1", Duration::from_secs(3600), || {
            let calls = &calls;
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(Item::new(999))
            }
        })
        .await
        .unwrap();

    assert_eq!(value, Item::new(1));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn get_or_load_full_miss_backfills_all_levels() {
    let cache = make_cache(1024, true);

    let value = cache
        .get_or_load("item:1", Duration::from_secs(3600), || async {
            Ok(Item::new(1))
        })
        .await
        .unwrap();
    assert_eq!(value, Item::new(1));

    // The second read no longer calls the loader.
    let calls = AtomicUsize::new(0);
    let value = cache
        .get_or_load("item:1", Duration::from_secs(3600), || {
            let calls = &calls;
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(Item::new(999))
            }
        })
        .await
        .unwrap();
    assert_eq!(value, Item::new(1));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn get_or_load_corrupted_l2_entry_is_reloaded() {
    // Write corrupt data directly into L2.
    let backend = InMemoryBackend::new();
    backend
        .set_ex("item:1", "not-json".to_string(), Duration::from_secs(3600))
        .await
        .unwrap();
    let cache =
        MultiLevelCache::<Item>::new(backend, CacheConfig::new(true, 0, Duration::from_secs(60)));

    let value = cache
        .get_or_load("item:1", Duration::from_secs(3600), || async {
            Ok(Item::new(42))
        })
        .await
        .unwrap();
    assert_eq!(value, Item::new(42));
}

#[tokio::test]
async fn get_or_load_backend_failure_degrades_to_loader() {
    let cache = MultiLevelCache::<Item>::new(
        FailingBackend,
        CacheConfig::new(true, 0, Duration::from_secs(60)),
    );
    let calls = AtomicUsize::new(0);
    let value = cache
        .get_or_load("item:1", Duration::from_secs(3600), || {
            let calls = &calls;
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(Item::new(1))
            }
        })
        .await;
    assert_eq!(value.unwrap(), Item::new(1));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn disabled_cache_passes_through_to_loader() {
    let cache = make_cache(1024, false);
    cache
        .put("item:1", Item::new(1), Duration::from_secs(3600))
        .await
        .unwrap();

    let calls = AtomicUsize::new(0);
    let value = cache
        .get_or_load("item:1", Duration::from_secs(3600), || {
            let calls = &calls;
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(Item::new(2))
            }
        })
        .await
        .unwrap();

    assert_eq!(value, Item::new(2));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn batch_deduplicates_and_aligns_results() {
    let cache = make_cache(0, true);
    let calls = AtomicUsize::new(0);

    // item:1 appears twice; the loader should be called only once.
    let params = [1u64, 2, 1];
    let result = cache
        .get_or_load_batch(
            &params,
            key_of,
            Duration::from_secs(3600),
            |ids| loader_of(ids, &calls),
            id_of,
        )
        .await
        .unwrap();

    assert_eq!(result.len(), 3);
    assert_eq!(result[0], Some(Item::new(1)));
    assert_eq!(result[1], Some(Item::new(2)));
    assert_eq!(result[2], Some(Item::new(1)));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn batch_all_l1_hits_skips_loader() {
    let cache = make_cache(1024, true);
    cache
        .put("item:1", Item::new(1), Duration::from_secs(3600))
        .await
        .unwrap();
    cache
        .put("item:2", Item::new(2), Duration::from_secs(3600))
        .await
        .unwrap();

    let calls = AtomicUsize::new(0);
    let result = cache
        .get_or_load_batch(
            &[1u64, 2],
            key_of,
            Duration::from_secs(3600),
            |ids| loader_of(ids, &calls),
            id_of,
        )
        .await
        .unwrap();

    assert_eq!(result, vec![Some(Item::new(1)), Some(Item::new(2))]);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn batch_partial_hit_merges_l1_l2_loader() {
    let cache = make_cache(1024, true);
    cache
        .put("item:1", Item::new(1), Duration::from_secs(3600))
        .await
        .unwrap();

    let calls = AtomicUsize::new(0);
    let result = cache
        .get_or_load_batch(
            &[1u64, 2, 3],
            key_of,
            Duration::from_secs(3600),
            |ids| loader_of(ids, &calls),
            id_of,
        )
        .await
        .unwrap();

    assert_eq!(
        result,
        vec![Some(Item::new(1)), Some(Item::new(2)), Some(Item::new(3))]
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn batch_loader_returns_extra_key_is_ignored() {
    let cache = make_cache(0, true);
    // The loader returns an unrequested key (99); it is ignored with a warning, no panic.
    let result = cache
        .get_or_load_batch(
            &[1u64, 2],
            key_of,
            Duration::from_secs(3600),
            |_ids| async { Ok(vec![Item::new(1), Item::new(2), Item::new(99)]) },
            id_of,
        )
        .await
        .unwrap();

    assert_eq!(result.len(), 2);
    assert_eq!(result[0], Some(Item::new(1)));
    assert_eq!(result[1], Some(Item::new(2)));
}

#[tokio::test]
async fn batch_backend_failure_degrades_to_full_load() {
    let cache = MultiLevelCache::<Item>::new(
        FailingBackend,
        CacheConfig::new(true, 0, Duration::from_secs(60)),
    );
    let calls = AtomicUsize::new(0);

    let result = cache
        .get_or_load_batch(
            &[1u64, 2],
            key_of,
            Duration::from_secs(3600),
            |ids| loader_of(ids, &calls),
            id_of,
        )
        .await
        .unwrap();

    assert_eq!(result, vec![Some(Item::new(1)), Some(Item::new(2))]);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn batch_disabled_passes_through_to_loader() {
    let cache = make_cache(0, false);
    cache
        .put("item:1", Item::new(1), Duration::from_secs(3600))
        .await
        .unwrap();

    let calls = AtomicUsize::new(0);
    let result = cache
        .get_or_load_batch(
            &[1u64, 2],
            key_of,
            Duration::from_secs(3600),
            |ids| loader_of(ids, &calls),
            id_of,
        )
        .await
        .unwrap();

    assert_eq!(result, vec![Some(Item::new(1)), Some(Item::new(2))]);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn put_overwrites_then_invalidate_removes() {
    let cache = make_cache(1024, true);
    cache
        .put("item:1", Item::new(1), Duration::from_secs(3600))
        .await
        .unwrap();

    // Overwrite
    cache
        .put("item:1", Item::new(2), Duration::from_secs(3600))
        .await
        .unwrap();
    let value = cache
        .get_or_load("item:1", Duration::from_secs(3600), || async {
            Ok(Item::new(0))
        })
        .await
        .unwrap();
    assert_eq!(value, Item::new(2));

    // After deletion, fall back to the loader
    cache.invalidate("item:1").await.unwrap();
    let value = cache
        .get_or_load("item:1", Duration::from_secs(3600), || async {
            Ok(Item::new(3))
        })
        .await
        .unwrap();
    assert_eq!(value, Item::new(3));
}

#[tokio::test]
async fn invalidate_batch_removes_all() {
    let cache = make_cache(1024, true);
    cache
        .put("item:1", Item::new(1), Duration::from_secs(3600))
        .await
        .unwrap();
    cache
        .put("item:2", Item::new(2), Duration::from_secs(3600))
        .await
        .unwrap();

    cache
        .invalidate_batch(&["item:1".to_string(), "item:2".to_string()])
        .await
        .unwrap();

    let result = cache
        .get_or_load_batch(
            &[1u64, 2],
            key_of,
            Duration::from_secs(3600),
            |ids| async { Ok(ids.into_iter().map(Item::new).collect()) },
            id_of,
        )
        .await
        .unwrap();
    assert_eq!(result, vec![Some(Item::new(1)), Some(Item::new(2))]);
}

/// A backend that always errors, used to verify error propagation and the degradation path.
struct FailingBackend;

#[async_trait::async_trait]
impl multi_level_cache::Backend for FailingBackend {
    fn name(&self) -> &'static str {
        "failing"
    }

    async fn get(&self, _key: &str) -> Result<Option<String>, CacheError> {
        Err(CacheError::backend(std::io::Error::other("boom")))
    }

    async fn get_many(&self, _keys: &[String]) -> Result<Vec<Option<String>>, CacheError> {
        Err(CacheError::backend(std::io::Error::other("boom")))
    }

    async fn set_ex(&self, _key: &str, _value: String, _ttl: Duration) -> Result<(), CacheError> {
        Err(CacheError::backend(std::io::Error::other("boom")))
    }

    async fn set_ex_many(&self, _items: &[(String, String, Duration)]) -> Result<(), CacheError> {
        Err(CacheError::backend(std::io::Error::other("boom")))
    }

    async fn del(&self, _key: &str) -> Result<(), CacheError> {
        Err(CacheError::backend(std::io::Error::other("boom")))
    }

    async fn del_many(&self, _keys: &[String]) -> Result<(), CacheError> {
        Err(CacheError::backend(std::io::Error::other("boom")))
    }
}

#[tokio::test]
async fn in_memory_backend_respects_ttl() {
    let backend = InMemoryBackend::new();
    backend
        .set_ex("k", "v".into(), Duration::from_millis(20))
        .await
        .unwrap();
    assert_eq!(backend.get("k").await.unwrap(), Some("v".into()));

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(backend.get("k").await.unwrap(), None);
}

#[tokio::test]
async fn in_memory_backend_batch_operations() {
    let backend = InMemoryBackend::new();
    let items = vec![
        ("a".to_string(), "1".to_string(), Duration::from_secs(60)),
        ("b".to_string(), "2".to_string(), Duration::from_secs(60)),
    ];
    backend.set_ex_many(&items).await.unwrap();

    let values = backend
        .get_many(&["a".to_string(), "b".to_string(), "c".to_string()])
        .await
        .unwrap();
    assert_eq!(values, vec![Some("1".into()), Some("2".into()), None]);

    backend
        .del_many(&["a".to_string(), "b".to_string()])
        .await
        .unwrap();
    assert_eq!(
        backend
            .get_many(&["a".to_string(), "b".to_string()])
            .await
            .unwrap(),
        vec![None, None]
    );
}

#[tokio::test]
async fn cache_loader_error_propagates() {
    let cache = MultiLevelCache::<Item>::new(
        FailingBackend,
        CacheConfig::new(true, 1024, Duration::from_secs(60)),
    );
    let result: Result<Item, CacheError> = cache
        .get_or_load("item:1", Duration::from_secs(3600), || async {
            Err(CacheError::loader(std::io::Error::other("db down")))
        })
        .await;
    assert!(matches!(result, Err(CacheError::Loader(_))));
}

/// Mimics an app-level error type (like AppError) to verify the generic `E` usage.
/// It intentionally does not implement `From<CacheError>`: the cache must not require it.
#[derive(Debug)]
struct AppError {
    msg: String,
}

impl AppError {
    fn bad_request(msg: &str) -> Self {
        Self { msg: msg.into() }
    }
}

impl std::fmt::Display for AppError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.msg)
    }
}

impl std::error::Error for AppError {}

#[tokio::test]
async fn custom_error_type_passthrough_loader_error() {
    // E = AppError: loader business errors pass through untouched.
    let cache = MultiLevelCache::<Item, AppError>::new(
        InMemoryBackend::new(),
        CacheConfig::new(true, 1024, Duration::from_secs(60)),
    );
    let result: Result<Item, AppError> = cache
        .get_or_load("item:1", Duration::from_secs(3600), || async {
            Err(AppError::bad_request("user_not_found"))
        })
        .await;
    assert!(matches!(&result, Err(e) if e.msg == "user_not_found"));
}

#[tokio::test]
async fn custom_error_type_backend_failure_degrades_to_loader() {
    // Backend failure is logged and L3 provides the result.
    let cache = MultiLevelCache::<Item, AppError>::new(
        FailingBackend,
        CacheConfig::new(true, 1024, Duration::from_secs(60)),
    );
    let result: Result<Item, AppError> = cache
        .get_or_load("item:1", Duration::from_secs(3600), || async {
            Ok(Item::new(1))
        })
        .await;
    assert_eq!(result.unwrap(), Item::new(1));
}

#[tokio::test]
async fn cache_custom_variant_distinguishes_business_error() {
    let cache = make_cache(1024, true);
    let result: Result<Item, CacheError> = cache
        .get_or_load("item:1", Duration::from_secs(3600), || async {
            Err(CacheError::custom(std::io::Error::other("user_not_found")))
        })
        .await;
    assert!(matches!(result, Err(CacheError::Custom(_))));
}

#[tokio::test]
async fn concurrent_loads_do_not_corrupt_state() {
    let cache = Arc::new(make_cache(1024, true));
    let mut handles = Vec::new();
    for _ in 0..8 {
        let cache = cache.clone();
        handles.push(tokio::spawn(async move {
            let v = cache
                .get_or_load("item:1", Duration::from_secs(3600), || async {
                    Ok(Item::new(7))
                })
                .await
                .unwrap();
            assert_eq!(v, Item::new(7));
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
}
