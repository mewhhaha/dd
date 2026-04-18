use common::{DeployBinding, DeployConfig, WorkerInvocation};
use runtime::{
    RuntimeConfig, RuntimeService, RuntimeServiceConfig, RuntimeStorageConfig, WorkerDebugDump,
};
use serde::Deserialize;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, AtomicU8, AtomicUsize, Ordering},
    Arc,
};
use std::time::{Duration, Instant};
use tokio::time::{sleep, timeout, Instant as TokioInstant};
use uuid::Uuid;

#[path = "bench_memory_storage/debug.rs"]
mod debug;
#[path = "bench_memory_storage/env.rs"]
mod env;
#[path = "bench_memory_storage/profile.rs"]
mod profile;
#[path = "bench_memory_storage/runner.rs"]
mod runner;
#[path = "bench_memory_storage/support.rs"]
mod support;
#[path = "bench_memory_storage/watchdog.rs"]
mod watchdog;
#[path = "bench_memory_storage/workers.rs"]
mod workers;

use self::debug::*;
use self::env::*;
use self::profile::*;
use self::runner::*;
use self::support::*;
use self::watchdog::*;
use self::workers::*;

#[derive(Clone, Copy)]
struct Scenario {
    requests: usize,
    concurrency: usize,
    path: &'static str,
    key_space: usize,
}

#[derive(Debug)]
struct ScenarioResult {
    requests: usize,
    concurrency: usize,
    total_duration: Duration,
    throughput_rps: f64,
    mean_ms: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[allow(dead_code)]
struct MemoryProfileMetric {
    calls: u64,
    total_us: u64,
    total_items: u64,
    max_us: u64,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct MemoryProfileSnapshot {
    enabled: bool,
    js_read_only_total: MemoryProfileMetric,
    js_freshness_check: MemoryProfileMetric,
    js_hydrate_full: MemoryProfileMetric,
    js_hydrate_keys: MemoryProfileMetric,
    js_txn_commit: MemoryProfileMetric,
    js_txn_blind_commit: MemoryProfileMetric,
    js_txn_validate: MemoryProfileMetric,
    js_cache_hit: MemoryProfileMetric,
    js_cache_miss: MemoryProfileMetric,
    js_cache_stale: MemoryProfileMetric,
    op_read: MemoryProfileMetric,
    op_snapshot: MemoryProfileMetric,
    op_version_if_newer: MemoryProfileMetric,
    op_validate_reads: MemoryProfileMetric,
    op_apply_batch: MemoryProfileMetric,
    op_apply_blind_batch: MemoryProfileMetric,
    store_direct_enqueue: MemoryProfileMetric,
    store_direct_await: MemoryProfileMetric,
    store_direct_queue_load: MemoryProfileMetric,
    store_direct_queue_flush: MemoryProfileMetric,
    store_direct_queue_delete: MemoryProfileMetric,
    store_direct_waiter_complete: MemoryProfileMetric,
    store_read: MemoryProfileMetric,
    store_snapshot: MemoryProfileMetric,
    store_snapshot_keys: MemoryProfileMetric,
    store_version_if_newer: MemoryProfileMetric,
    store_apply_batch: MemoryProfileMetric,
    store_apply_batch_validate: MemoryProfileMetric,
    store_apply_batch_write: MemoryProfileMetric,
    store_apply_blind_batch: MemoryProfileMetric,
    store_apply_blind_batch_write: MemoryProfileMetric,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct MemoryProfileEnvelope {
    ok: bool,
    snapshot: Option<MemoryProfileSnapshot>,
    error: String,
}

#[derive(Clone, Copy, Debug)]
struct BenchOptions {
    request_timeout: Duration,
    verify_timeout: Duration,
    watchdog_interval: Duration,
    watchdog_silence_timeout: Duration,
    wide_key_space: usize,
}

#[derive(Clone, Copy, Debug)]
struct BenchTimings {
    deploy: Duration,
    seed: Duration,
    timed: Duration,
    verify: Duration,
    profile: Duration,
    shutdown: Duration,
}

#[tokio::main]
async fn main() -> Result<(), String> {
    let profile_enabled = env_flag("DD_BENCH_PROFILE_MEMORY");
    let options = bench_options_from_env();
    let runtime = RuntimeConfig {
        min_isolates: env_usize("DD_BENCH_MIN_ISOLATES", 1),
        max_isolates: env_usize("DD_BENCH_MAX_ISOLATES", 1),
        max_inflight_per_isolate: env_usize("DD_BENCH_MAX_INFLIGHT", 1),
        idle_ttl: env_duration_ms("DD_BENCH_IDLE_TTL_MS", 30_000),
        scale_tick: env_duration_ms("DD_BENCH_SCALE_TICK_MS", 1_000),
        queue_warn_thresholds: vec![10, 100, 1000],
        memory_profile_enabled: profile_enabled,
        ..RuntimeConfig::default()
    };
    let service = start_service("memory-storage", runtime)
        .await
        .map_err(|error| error.to_string())?;

    println!("# keyed memory benchmark");
    println!(
        "# compares hosted keyed-memory paths under configurable isolate and inflight settings."
    );
    println!(
        "# runtime min_isolates={} max_isolates={} max_inflight_per_isolate={}",
        env_usize("DD_BENCH_MIN_ISOLATES", 1),
        env_usize("DD_BENCH_MAX_ISOLATES", 1),
        env_usize("DD_BENCH_MAX_INFLIGHT", 1),
    );
    let mode = env_mode();

    if mode.as_deref().is_none() || mode.as_deref() == Some("async-storage") {
        run_and_print(
            &service,
            "memory-read-async-storage",
            MEMORY_READ_ASYNC_STORAGE_WORKER_SOURCE,
            true,
            "/read",
            1,
            None,
            false,
            &options,
        )
        .await?;
    }
    if mode.as_deref().is_none() || mode.as_deref() == Some("async-memory") {
        run_and_print(
            &service,
            "memory-read-async-memory",
            MEMORY_READ_ASYNC_MEMORY_WORKER_SOURCE,
            false,
            "/read",
            1,
            None,
            false,
            &options,
        )
        .await?;
    }
    if mode.as_deref().is_none() || mode.as_deref() == Some("sync-memory") {
        run_and_print(
            &service,
            "memory-read-sync-memory",
            MEMORY_READ_SYNC_MEMORY_WORKER_SOURCE,
            false,
            "/read",
            1,
            None,
            false,
            &options,
        )
        .await?;
    }
    if mode.as_deref().is_none() || mode.as_deref() == Some("direct-read-memory") {
        run_and_print(
            &service,
            "memory-direct-read-memory",
            MEMORY_DIRECT_READ_WORKER_SOURCE,
            true,
            "/read",
            1,
            None,
            profile_enabled,
            &options,
        )
        .await?;
    }
    if mode.as_deref().is_none() || mode.as_deref() == Some("direct-write-memory") {
        run_and_print(
            &service,
            "memory-direct-write-memory",
            MEMORY_DIRECT_WRITE_WORKER_SOURCE,
            true,
            "/write",
            1,
            Some("/get-strong"),
            false,
            &options,
        )
        .await?;
    }
    if mode.as_deref().is_none() || mode.as_deref() == Some("direct-write-memory-multikey") {
        run_and_print(
            &service,
            "memory-direct-write-memory-multikey",
            MEMORY_DIRECT_WRITE_WORKER_SOURCE,
            false,
            "/write",
            env_usize("DD_BENCH_KEY_SPACE", 256),
            Some("/sum-read"),
            false,
            &options,
        )
        .await?;
    }
    if mode.as_deref().is_none() || mode.as_deref() == Some("direct-read-memory-multikey") {
        run_and_print(
            &service,
            "memory-direct-read-memory-multikey",
            MEMORY_DIRECT_READ_WORKER_SOURCE,
            false,
            "/read",
            env_usize("DD_BENCH_KEY_SPACE", 256),
            None,
            profile_enabled,
            &options,
        )
        .await?;
    }
    if mode.as_deref().is_none() || mode.as_deref() == Some("direct-read-memory-wide") {
        run_and_print(
            &service,
            "memory-direct-read-memory-wide",
            MEMORY_DIRECT_READ_WORKER_SOURCE,
            true,
            "/read",
            options.wide_key_space,
            None,
            true,
            &options,
        )
        .await?;
    }
    if mode.as_deref().is_none() || mode.as_deref() == Some("atomic-read-memory") {
        run_and_print(
            &service,
            "memory-atomic-read-memory",
            MEMORY_ATOMIC_READ_MEMORY_WORKER_SOURCE,
            true,
            "/read",
            1,
            None,
            false,
            &options,
        )
        .await?;
    }
    if mode.as_deref().is_none() || mode.as_deref() == Some("atomic-read-memory-allow-concurrency")
    {
        run_and_print(
            &service,
            "memory-atomic-read-memory-allow-concurrency",
            MEMORY_ATOMIC_READ_ALLOW_CONCURRENCY_MEMORY_WORKER_SOURCE,
            true,
            "/read",
            1,
            None,
            false,
            &options,
        )
        .await?;
    }
    if mode.as_deref().is_none() || mode.as_deref() == Some("atomic-read-memory-multikey") {
        run_and_print(
            &service,
            "memory-atomic-read-memory-multikey",
            MEMORY_ATOMIC_READ_MEMORY_WORKER_SOURCE,
            false,
            "/read",
            env_usize("DD_BENCH_KEY_SPACE", 256),
            None,
            false,
            &options,
        )
        .await?;
    }
    if mode.as_deref().is_none() || mode.as_deref() == Some("stm-inc") {
        run_and_print(
            &service,
            "memory-stm-inc",
            MEMORY_STM_INCREMENT_WORKER_SOURCE,
            true,
            "/inc",
            1,
            Some("/get"),
            profile_enabled,
            &options,
        )
        .await?;
    }
    if mode.as_deref().is_none() || mode.as_deref() == Some("stm-read") {
        run_and_print(
            &service,
            "memory-stm-read",
            MEMORY_STM_READ_WRITE_WORKER_SOURCE,
            true,
            "/read",
            1,
            None,
            profile_enabled,
            &options,
        )
        .await?;
    }
    if mode.as_deref().is_none() || mode.as_deref() == Some("stm-read-multikey") {
        run_and_print(
            &service,
            "memory-stm-read-multikey",
            MEMORY_STM_READ_WRITE_WORKER_SOURCE,
            true,
            "/read",
            env_usize("DD_BENCH_KEY_SPACE", 256),
            None,
            profile_enabled,
            &options,
        )
        .await?;
    }
    if mode.as_deref().is_none() || mode.as_deref() == Some("stm-read-allow-concurrency") {
        run_and_print(
            &service,
            "memory-stm-read-allow-concurrency",
            MEMORY_STM_READ_WRITE_ALLOW_CONCURRENCY_WORKER_SOURCE,
            true,
            "/read",
            1,
            None,
            profile_enabled,
            &options,
        )
        .await?;
    }
    if mode.as_deref().is_none() || mode.as_deref() == Some("stm-read-multikey-allow-concurrency") {
        run_and_print(
            &service,
            "memory-stm-read-multikey-allow-concurrency",
            MEMORY_STM_READ_WRITE_ALLOW_CONCURRENCY_WORKER_SOURCE,
            true,
            "/read",
            env_usize("DD_BENCH_KEY_SPACE", 256),
            None,
            profile_enabled,
            &options,
        )
        .await?;
    }
    if mode.as_deref().is_none() || mode.as_deref() == Some("stm-write") {
        run_and_print(
            &service,
            "memory-stm-write",
            MEMORY_STM_READ_WRITE_WORKER_SOURCE,
            true,
            "/write",
            1,
            Some("/get"),
            profile_enabled,
            &options,
        )
        .await?;
    }
    if mode.as_deref().is_none() || mode.as_deref() == Some("stm-write-throughput") {
        run_and_print(
            &service,
            "memory-stm-write-throughput",
            MEMORY_STM_READ_WRITE_WORKER_SOURCE,
            true,
            "/write",
            1,
            Some("/get"),
            profile_enabled,
            &options,
        )
        .await?;
    }
    if mode.as_deref().is_none() || mode.as_deref() == Some("stm-write-multikey") {
        run_and_print(
            &service,
            "memory-stm-write-multikey",
            MEMORY_STM_READ_WRITE_WORKER_SOURCE,
            true,
            "/write",
            env_usize("DD_BENCH_KEY_SPACE", 256),
            Some("/sum"),
            profile_enabled,
            &options,
        )
        .await?;
    }
    if mode.as_deref().is_none() || mode.as_deref() == Some("stm-blind-write") {
        run_and_print(
            &service,
            "memory-stm-blind-write",
            MEMORY_STM_BLIND_WRITE_WORKER_SOURCE,
            true,
            "/write",
            1,
            Some("/get-blind"),
            profile_enabled,
            &options,
        )
        .await?;
    }
    if mode.as_deref().is_none() || mode.as_deref() == Some("stm-blind-write-multikey") {
        run_and_print(
            &service,
            "memory-stm-blind-write-multikey",
            MEMORY_STM_BLIND_WRITE_WORKER_SOURCE,
            true,
            "/write",
            env_usize("DD_BENCH_KEY_SPACE", 256),
            Some("/sum-blind"),
            profile_enabled,
            &options,
        )
        .await?;
    }
    if mode.as_deref().is_none() || mode.as_deref() == Some("atomic-put-inc") {
        run_and_print(
            &service,
            "memory-atomic-put-inc",
            MEMORY_ATOMIC_PUT_INCREMENT_WORKER_SOURCE,
            true,
            "/inc",
            1,
            Some("/get"),
            false,
            &options,
        )
        .await?;
    }

    if env_flag("DD_BENCH_EXIT_IMMEDIATELY") {
        std::process::exit(0);
    }

    Ok(())
}
