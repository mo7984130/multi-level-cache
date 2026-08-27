//! Benchmark for `MultiLevelCache::get_or_load_batch`.
//!
//! Covers four paths (DB scenarios always carry a fixed DB delay; hit scenarios never touch the DB):
//! - `all_l1_hits`: full L1 hits (hot path, no DB)
//! - `l1_miss_l2_hit`: capacity 0 forces L1 misses, L2 hits (no DB)
//! - `disabled_with_db_io`: cache disabled, passes through to a fixed-delay loader (DB)
//! - `full_miss_with_db_io`: full misses, fixed-delay loader with L1/L2 backfill (DB)
//!
//! The L2 backend is the built-in `InMemoryBackend` (pure memory, zero network IO), removing real Redis
//! round-trip noise from measurements; the DB loader carries a fixed delay for stable, reproducible results.
//!
//! Run: `cargo bench -p multi-level-cache --bench get_or_load_batch`

use std::sync::atomic::Ordering;
use std::time::Duration;

use multi_level_cache::{CacheConfig, InMemoryBackend, MultiLevelCache};
use serde::{Deserialize, Serialize};

const KEY_COUNT: u64 = 100;
const L2_TTL: Duration = Duration::from_secs(3600);
/// Simulated duration of one database query (aligned with a real DB round trip).
const DB_IO_DELAY: Duration = Duration::from_millis(5);

#[derive(Clone, Serialize, Deserialize)]
struct MockRow {
    id: u64,
}

impl MockRow {
    fn new(id: u64) -> Self {
        Self { id }
    }
}

fn key_of(id: &u64) -> String {
    format!("key:{id}")
}

fn id_of(row: &MockRow) -> u64 {
    row.id
}

fn keys(count: u64) -> Vec<u64> {
    (0..count).collect()
}

fn make_cache(capacity: u64, enabled: bool) -> MultiLevelCache<MockRow> {
    MultiLevelCache::new(
        InMemoryBackend::new(),
        CacheConfig::new(enabled, capacity, Duration::from_secs(3600)),
    )
}

async fn loader(ids: Vec<u64>) -> Result<Vec<MockRow>, multi_level_cache::CacheError> {
    tokio::time::sleep(DB_IO_DELAY).await;
    Ok(ids.into_iter().map(MockRow::new).collect())
}

async fn prefill(cache: &MultiLevelCache<MockRow>, ks: &[u64]) {
    for id in ks {
        cache
            .put(&key_of(id), MockRow::new(*id), L2_TTL)
            .await
            .unwrap();
    }
}

fn bench_all_l1_hits(c: &mut criterion::Criterion) {
    let cache = make_cache(1024 * 1024, true);
    let ks = keys(KEY_COUNT);
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(prefill(&cache, &ks));

    c.bench_function("get_or_load_batch/all_l1_hits", |b| {
        b.to_async(&rt).iter(|| async {
            let result = cache
                .get_or_load_batch(&ks, key_of, L2_TTL, loader, id_of)
                .await
                .unwrap();
            criterion::black_box(&result);
        });
    });
}

fn bench_l1_miss_l2_hit(c: &mut criterion::Criterion) {
    // Capacity 0 forces L1 to never hit; only L2 is prefilled.
    let cache = make_cache(0, true);
    let ks = keys(KEY_COUNT);
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(prefill(&cache, &ks));

    c.bench_function("get_or_load_batch/l1_miss_l2_hit", |b| {
        b.to_async(&rt).iter(|| async {
            let result = cache
                .get_or_load_batch(&ks, key_of, L2_TTL, loader, id_of)
                .await
                .unwrap();
            criterion::black_box(&result);
        });
    });
}

fn bench_disabled_with_db_io(c: &mut criterion::Criterion) {
    let cache = make_cache(0, false);
    let ks = keys(KEY_COUNT);
    let rt = tokio::runtime::Runtime::new().unwrap();

    c.bench_function("get_or_load_batch/disabled_with_db_io", |b| {
        b.to_async(&rt).iter(|| async {
            let result = cache
                .get_or_load_batch(&ks, key_of, L2_TTL, loader, id_of)
                .await
                .unwrap();
            criterion::black_box(&result);
        });
    });
}

fn bench_full_miss_with_db_io(c: &mut criterion::Criterion) {
    // Capacity 0 forces L1 misses; each iteration uses fresh keys so L2 always misses (avoiding second hits after backfill).
    let cache = make_cache(0, true);
    let counter = std::sync::atomic::AtomicU64::new(0);
    let counter = &counter;
    let rt = tokio::runtime::Runtime::new().unwrap();

    c.bench_function("get_or_load_batch/full_miss_with_db_io", |b| {
        b.to_async(&rt).iter(|| async {
            let start = counter.fetch_add(KEY_COUNT, Ordering::Relaxed);
            let ks: Vec<u64> = (start..start + KEY_COUNT).collect();
            let result = cache
                .get_or_load_batch(&ks, key_of, L2_TTL, loader, id_of)
                .await
                .unwrap();
            criterion::black_box(&result);
        });
    });
}

fn main() {
    let mut c = criterion::Criterion::default().configure_from_args();
    bench_all_l1_hits(&mut c);
    bench_l1_miss_l2_hit(&mut c);
    bench_disabled_with_db_io(&mut c);
    bench_full_miss_with_db_io(&mut c);
    criterion::Criterion::default().final_summary();
}
