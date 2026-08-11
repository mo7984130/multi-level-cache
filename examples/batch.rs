//! Batch reads: parameter dedup + result alignment.
//!
//! Depends on no feature (including `metrics`); builds and runs with the default configuration:
//! `cargo run -p multi-level-cache --example batch`

use std::time::Duration;

use multi_level_cache::{CacheConfig, InMemoryBackend, MultiLevelCache};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
struct Photo {
    id: u64,
    url: String,
}

fn key_of(id: &u64) -> String {
    format!("photo:{id}")
}

fn id_of(photo: &Photo) -> u64 {
    photo.id
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cache = MultiLevelCache::<Photo>::new(
        InMemoryBackend::new(),
        CacheConfig::new(true, 10_000, Duration::from_secs(60)),
    );

    // The parameter list contains duplicate ids; the loader is called only once.
    let ids = [1u64, 2, 1, 3];
    let result = cache
        .get_or_load_batch(
            &ids,
            key_of,
            Duration::from_secs(3600),
            |miss_ids| async move {
                println!("loader received missed params: {miss_ids:?}");
                Ok(miss_ids
                    .into_iter()
                    .map(|id| Photo {
                        id,
                        url: format!("https://cdn.example/{id}.jpg"),
                    })
                    .collect())
            },
            id_of,
        )
        .await?;

    for (id, photo) in ids.iter().zip(&result) {
        println!("id={id} -> {photo:?}");
    }
    Ok(())
}
