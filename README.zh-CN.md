# multi-level-cache

[![Crates.io](https://img.shields.io/crates/v/multi-level-cache.svg)](https://crates.io/crates/multi-level-cache)
[![docs.rs](https://docs.rs/multi-level-cache/badge.svg)](https://docs.rs/multi-level-cache)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](#license)

通用异步多级缓存库(L1 进程内 + L2 分布式后端 + L3 数据源)。

**语言: [English](README.md) · [中文](README.zh-CN.md)**

```text
Request → L1 (进程内 Moka, 存对象, Arc 共享) → L2 (分布式后端) → L3 (调用方 loader)
            <1ms                              2-5ms              10-50ms
```

## 特性

- **多级读路径**:L1 命中零拷贝返回;L2 未命中回填 L1;L3 loader 加载后逐级回填
- **批量管线**:`get_or_load_batch` 先逐个命中 L1,再 `MGET` 批量查询 L2,剩余项一次 loader 加载回填
- **参数去重**:相同 key 的多个参数只加载一次,结果广播到所有对应索引
- **故障降级**:L2 批量读取失败自动降级为全量加载,回填写失败不影响读取结果
- **可插拔后端**:内置纯内存后端(开发/测试/零依赖起步),可选 Redis(deadpool 或 redis-rs 直连)
- **整体开关**:`CacheConfig::enabled` 可一键穿透 loader,便于压测对比
- **可选指标**:`metrics` feature 采集 `cache:{name}:{layer}:{op}` 系列指标

## 安装

```toml
[dependencies]
multi-level-cache = "0.1"
tokio = { version = "1", features = ["full"] }
```

## 快速上手

```rust,no_run
use std::time::Duration;
use multi_level_cache::{CacheConfig, InMemoryBackend, MultiLevelCache};

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct User { id: u64, name: String }

let cache = MultiLevelCache::<User>::new(
    InMemoryBackend::new(),
    CacheConfig::new(true, 10_000, Duration::from_secs(60)),
);

// 未命中时调用 loader,并逐级回填
let user = cache
    .get_or_load("user:1", Duration::from_secs(3600), || async {
        Ok(User { id: 1, name: "alice".into() })
    })
    .await?;
```

## Redis 后端

两种可选后端,按依赖取舍任选其一。两者均直接为 Redis 原生类型实现 `Backend`,无需任何包装:

```toml
# deadpool-redis 连接池(与项目现有连接池共享)
multi-level-cache = { version = "0.1", features = ["redis-deadpool"] }

# redis-rs 直连(内置 ConnectionManager,自动重连)
multi-level-cache = { version = "0.1", features = ["redis-connection"] }
```

直接把 `deadpool_redis::Pool` 传入 `new`:

```rust,no_run
use multi_level_cache::{CacheConfig, MultiLevelCache};

let pool = deadpool_redis::Config::from_url("redis://127.0.0.1:6379")
    .create_pool(Some(deadpool_redis::Runtime::Tokio1))?;

let cache = MultiLevelCache::<User>::new(
    pool,
    CacheConfig::new(true, 10_000, Duration::from_secs(60)),
);
```

或直接传入 `redis::aio::ConnectionManager`:

```rust,no_run
use multi_level_cache::{CacheConfig, MultiLevelCache};

let client = redis::Client::open("redis://127.0.0.1:6379")?;
let conn = redis::aio::ConnectionManager::new(client).await?;

let cache = MultiLevelCache::<User>::new(
    conn,
    CacheConfig::new(true, 10_000, Duration::from_secs(60)),
);
```

## 批量读取

```rust,no_run
let ids = [1u64, 2, 1, 3]; // 重复 id 只加载一次
let result: Vec<Option<User>> = cache
    .get_or_load_batch(
        &ids,
        |id| format!("user:{id}"),                 // key 映射
        Duration::from_secs(3600),
        |miss_ids| async { Ok(fetch_users(&miss_ids).await) }, // 批量 loader
        |user| user.id,                             // 结果反查 key
    )
    .await?;
```

## 写入与删除

- **更新**:`put` 直接覆盖 L1+L2,避免「先删后写」带来的穿透窗口,写后立即读可拿到新值
- **删除**:`invalidate` / `invalidate_batch` 同时清理 L1+L2

```rust,no_run
cache.put("user:1", user, Duration::from_secs(3600)).await?;
cache.invalidate("user:1").await?;
```

## 配置

| 字段 | 说明 |
|------|------|
| `enabled` | 是否启用缓存,`false` 时读写全部穿透 loader(压测对比用) |
| `local_capacity` | L1 最大条目数 |
| `local_ttl` | L1 TTL,短 TTL 保证多实例最终一致 |

## 错误处理

缓存操作返回调用方指定的错误类型 `E`(默认为
[`CacheError`](https://docs.rs/multi-level-cache/latest/multi_level_cache/enum.CacheError.html)):

L1/L2 读取和回填写失败会记录日志并降级到 L3 loader；`get_or_load` /
`get_or_load_batch` 仅在 loader 失败时返回错误。缓存写入和失效操作仍会返回后端错误，
以便调用方处理写入失败。

- `Backend` — L2 后端错误(Redis 连接/命令失败)
- `Serialization` — 值序列化/反序列化失败
- `Loader` — 数据源加载失败(上游/基础设施故障)
- `Custom` — 调用方自定义/业务错误(如"记录不存在"),可与内部错误区分

底层错误作为错误来源(source)保留,可通过 `Error::source()` 获取完整错误链。使用
`CacheError::backend(e)` / `CacheError::loader(e)` / `CacheError::custom(e)` 构造;Redis/Pool
错误可通过 `?` 自动转换。

### 自定义错误类型

`MultiLevelCache<T, E>` 对错误类型 `E` 泛型化(默认 `CacheError`)。loader 闭包返回
`Result<T, E>`(调用方自有错误类型),库内部错误([`CacheError`])通过 `E: From<CacheError>`
自动转换。调用方因此可以用自己的错误类型原生表达业务/内部错误:

```rust,no_run
use multi_level_cache::{CacheConfig, CacheError, MultiLevelCache};

// 应用自有错误类型
#[derive(Debug)]
enum AppError { NotFound(String), Internal(String) }
impl std::fmt::Display for AppError { /* ... */ }
impl std::error::Error for AppError {}

// 库内部错误映射为可匹配的类别
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

// loader 直接用应用错误,零转换
let user = cache
    .get_or_load("user:1", Duration::from_secs(3600), || async {
        let row = db_query(1).await?;                       // → AppError(内部)
        row.ok_or(AppError::NotFound("user:1".into()))?     // → AppError(业务)
    })
    .await?;
```

## 自定义后端

实现 [`Backend`](https://docs.rs/multi-level-cache/latest/multi_level_cache/trait.Backend.html) trait(字符串级读写 + TTL),即可接入任意存储:

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

## 指标

启用 `metrics` feature 后,每个缓存实例按 `cache:{name}:{layer}:{op}` 采集。实例名由
`MultiLevelCache::new_with_name` 指定(默认 `multi-level-cache`);未启用 `metrics` 时
实例名被忽略,使用无 name 参数的 `MultiLevelCache::new` 即可:

```rust,no_run
let cache = MultiLevelCache::<User>::new_with_name(
    "user_info",                                   // 指标前缀 cache:user_info:*
    InMemoryBackend::new(),
    CacheConfig::new(true, 10_000, Duration::from_secs(60)),
);
```

采集的指标:

- `l1:hits` / `l1:misses`、`l2:hits` / `l2:misses`
- `db:loads`
- `l1:get:duration_seconds`、`l2:get:duration_seconds`、`db:load:duration_seconds`
- `l1:entries`

## Features

| Feature | 说明 |
|---------|------|
| `redis-deadpool` | 为 `deadpool_redis::Pool` 实现 `Backend` |
| `redis-connection` | 为 `redis::aio::ConnectionManager` 实现 `Backend` |
| `metrics` | 指标埋点 |

## 运行测试与基准

```bash
cargo test -p multi-level-cache
cargo bench -p multi-level-cache
```

## 示例

```bash
cargo run -p multi-level-cache --example basic
cargo run -p multi-level-cache --example batch
cargo run -p multi-level-cache --features redis-connection --example database_caching  # 需本地 Redis
```

## License

双许可证选择:MIT 或 Apache-2.0。
