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

#[path = "bench_support/cli.rs"]
mod bench_cli;

use bench_cli::{bench_arg_action, BenchArgAction};

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
    js_hydrate_full: MemoryProfileMetric,
    js_hydrate_keys: MemoryProfileMetric,
    js_txn_commit: MemoryProfileMetric,
    js_cache_hit: MemoryProfileMetric,
    js_cache_miss: MemoryProfileMetric,
    js_cache_stale: MemoryProfileMetric,
    op_read: MemoryProfileMetric,
    op_snapshot: MemoryProfileMetric,
    op_version_if_newer: MemoryProfileMetric,
    op_apply_batch: MemoryProfileMetric,
    store_read: MemoryProfileMetric,
    store_snapshot: MemoryProfileMetric,
    store_snapshot_keys: MemoryProfileMetric,
    store_version_if_newer: MemoryProfileMetric,
    store_apply_batch: MemoryProfileMetric,
    store_apply_batch_validate: MemoryProfileMetric,
    store_apply_batch_write: MemoryProfileMetric,
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

#[derive(Clone, Copy)]
enum KeySpaceConfig {
    One,
    Env,
    Wide,
}

impl KeySpaceConfig {
    fn resolve(self, options: &BenchOptions) -> usize {
        match self {
            Self::One => 1,
            Self::Env => env_usize("DD_BENCH_KEY_SPACE", 256),
            Self::Wide => options.wide_key_space,
        }
    }
}

#[derive(Clone, Copy)]
enum ProfileConfig {
    Never,
    Enabled,
    Always,
}

impl ProfileConfig {
    fn resolve(self, profile_enabled: bool) -> bool {
        match self {
            Self::Never => false,
            Self::Enabled => profile_enabled,
            Self::Always => true,
        }
    }
}

#[derive(Clone, Copy)]
struct BenchCase {
    mode: &'static str,
    label: &'static str,
    source: &'static str,
    seed: bool,
    path: &'static str,
    key_space: KeySpaceConfig,
    verify_path: Option<&'static str>,
    profile: ProfileConfig,
}

const BENCH_CASES: &[BenchCase] = &[
    BenchCase {
        mode: "async-storage",
        label: "memory-read-async-storage",
        source: MEMORY_READ_ASYNC_STORAGE_WORKER_SOURCE,
        seed: true,
        path: "/read",
        key_space: KeySpaceConfig::One,
        verify_path: None,
        profile: ProfileConfig::Never,
    },
    BenchCase {
        mode: "async-memory",
        label: "memory-read-async-memory",
        source: MEMORY_READ_ASYNC_MEMORY_WORKER_SOURCE,
        seed: false,
        path: "/read",
        key_space: KeySpaceConfig::One,
        verify_path: None,
        profile: ProfileConfig::Never,
    },
    BenchCase {
        mode: "sync-memory",
        label: "memory-read-sync-memory",
        source: MEMORY_READ_SYNC_MEMORY_WORKER_SOURCE,
        seed: false,
        path: "/read",
        key_space: KeySpaceConfig::One,
        verify_path: None,
        profile: ProfileConfig::Never,
    },
    BenchCase {
        mode: "direct-read-memory",
        label: "memory-direct-read-memory",
        source: MEMORY_DIRECT_READ_WORKER_SOURCE,
        seed: true,
        path: "/read",
        key_space: KeySpaceConfig::One,
        verify_path: None,
        profile: ProfileConfig::Enabled,
    },
    BenchCase {
        mode: "direct-write-memory",
        label: "memory-direct-write-memory",
        source: MEMORY_DIRECT_WRITE_WORKER_SOURCE,
        seed: true,
        path: "/write",
        key_space: KeySpaceConfig::One,
        verify_path: Some("/get-strong"),
        profile: ProfileConfig::Never,
    },
    BenchCase {
        mode: "direct-write-memory-multikey",
        label: "memory-direct-write-memory-multikey",
        source: MEMORY_DIRECT_WRITE_WORKER_SOURCE,
        seed: false,
        path: "/write",
        key_space: KeySpaceConfig::Env,
        verify_path: Some("/sum-read"),
        profile: ProfileConfig::Never,
    },
    BenchCase {
        mode: "direct-read-memory-multikey",
        label: "memory-direct-read-memory-multikey",
        source: MEMORY_DIRECT_READ_WORKER_SOURCE,
        seed: false,
        path: "/read",
        key_space: KeySpaceConfig::Env,
        verify_path: None,
        profile: ProfileConfig::Enabled,
    },
    BenchCase {
        mode: "direct-read-memory-wide",
        label: "memory-direct-read-memory-wide",
        source: MEMORY_DIRECT_READ_WORKER_SOURCE,
        seed: true,
        path: "/read",
        key_space: KeySpaceConfig::Wide,
        verify_path: None,
        profile: ProfileConfig::Always,
    },
    BenchCase {
        mode: "atomic-read-memory",
        label: "memory-atomic-read-memory",
        source: MEMORY_ATOMIC_READ_MEMORY_WORKER_SOURCE,
        seed: true,
        path: "/read",
        key_space: KeySpaceConfig::One,
        verify_path: None,
        profile: ProfileConfig::Never,
    },
    BenchCase {
        mode: "atomic-read-memory-multikey",
        label: "memory-atomic-read-memory-multikey",
        source: MEMORY_ATOMIC_READ_MEMORY_WORKER_SOURCE,
        seed: false,
        path: "/read",
        key_space: KeySpaceConfig::Env,
        verify_path: None,
        profile: ProfileConfig::Never,
    },
    BenchCase {
        mode: "actor-inc",
        label: "memory-actor-inc",
        source: MEMORY_ACTOR_INCREMENT_WORKER_SOURCE,
        seed: true,
        path: "/inc",
        key_space: KeySpaceConfig::One,
        verify_path: Some("/get"),
        profile: ProfileConfig::Enabled,
    },
    BenchCase {
        mode: "actor-read",
        label: "memory-actor-read",
        source: MEMORY_ACTOR_READ_WRITE_WORKER_SOURCE,
        seed: true,
        path: "/read",
        key_space: KeySpaceConfig::One,
        verify_path: None,
        profile: ProfileConfig::Enabled,
    },
    BenchCase {
        mode: "actor-read-multikey",
        label: "memory-actor-read-multikey",
        source: MEMORY_ACTOR_READ_WRITE_WORKER_SOURCE,
        seed: true,
        path: "/read",
        key_space: KeySpaceConfig::Env,
        verify_path: None,
        profile: ProfileConfig::Enabled,
    },
    BenchCase {
        mode: "actor-write",
        label: "memory-actor-write",
        source: MEMORY_ACTOR_READ_WRITE_WORKER_SOURCE,
        seed: true,
        path: "/write",
        key_space: KeySpaceConfig::One,
        verify_path: Some("/get"),
        profile: ProfileConfig::Enabled,
    },
    BenchCase {
        mode: "actor-write-throughput",
        label: "memory-actor-write-throughput",
        source: MEMORY_ACTOR_READ_WRITE_WORKER_SOURCE,
        seed: true,
        path: "/write",
        key_space: KeySpaceConfig::One,
        verify_path: Some("/get"),
        profile: ProfileConfig::Enabled,
    },
    BenchCase {
        mode: "actor-write-multikey",
        label: "memory-actor-write-multikey",
        source: MEMORY_ACTOR_READ_WRITE_WORKER_SOURCE,
        seed: true,
        path: "/write",
        key_space: KeySpaceConfig::Env,
        verify_path: Some("/sum"),
        profile: ProfileConfig::Enabled,
    },
    BenchCase {
        mode: "atomic-put-inc",
        label: "memory-atomic-put-inc",
        source: MEMORY_ATOMIC_PUT_INCREMENT_WORKER_SOURCE,
        seed: true,
        path: "/inc",
        key_space: KeySpaceConfig::One,
        verify_path: Some("/get"),
        profile: ProfileConfig::Never,
    },
];

#[tokio::main]
async fn main() -> Result<(), String> {
    match bench_arg_action(std::env::args().skip(1))? {
        BenchArgAction::Help => {
            print_help();
            return Ok(());
        }
        BenchArgAction::Run => {}
    }

    let mode = env_mode_checked()?;
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

    for bench_case in BENCH_CASES {
        if !should_run_case(mode.as_deref(), bench_case.mode) {
            continue;
        }
        run_and_print(
            &service,
            bench_case.label,
            bench_case.source,
            bench_case.seed,
            bench_case.path,
            bench_case.key_space.resolve(&options),
            bench_case.verify_path,
            bench_case.profile.resolve(profile_enabled),
            &options,
        )
        .await?;
    }

    if env_flag("DD_BENCH_EXIT_IMMEDIATELY") {
        std::process::exit(0);
    }

    Ok(())
}

fn env_mode_checked() -> Result<Option<String>, String> {
    let Some(mode) = env_mode() else {
        return Ok(None);
    };
    if BENCH_CASES.iter().any(|bench_case| bench_case.mode == mode) {
        Ok(Some(mode))
    } else {
        Err(format!(
            "unknown DD_BENCH_MODE `{mode}`; expected one of: {}",
            bench_modes().collect::<Vec<_>>().join(", ")
        ))
    }
}

fn should_run_case(selected_mode: Option<&str>, case_mode: &str) -> bool {
    match selected_mode {
        Some(selected_mode) => selected_mode == case_mode,
        None => true,
    }
}

fn bench_modes() -> impl Iterator<Item = &'static str> {
    BENCH_CASES.iter().map(|bench_case| bench_case.mode)
}

fn print_help() {
    println!("keyed memory benchmark");
    println!();
    println!("Usage:");
    println!("  cargo run -p runtime --bin bench_memory_storage --release");
    println!("  DD_BENCH_MODE=direct-read-memory cargo run -p runtime --bin bench_memory_storage --release");
    println!();
    println!("This benchmark is configured with environment variables, not CLI flags.");
    println!();
    println!("Core env:");
    println!("  DD_BENCH_MODE                  run one scenario; omit to run all");
    println!("  DD_BENCH_REQUESTS              requests per scenario (default 1000)");
    println!("  DD_BENCH_CONCURRENCY           concurrent requests (default 1)");
    println!("  DD_BENCH_MIN_ISOLATES          runtime min isolates (default 1)");
    println!("  DD_BENCH_MAX_ISOLATES          runtime max isolates (default 1)");
    println!("  DD_BENCH_MAX_INFLIGHT          max inflight per isolate (default 1)");
    println!("  DD_BENCH_KEY_SPACE             multikey scenario key space (default 256)");
    println!("  DD_BENCH_PROFILE_MEMORY        enable memory profile output");
    println!();
    println!("Modes:");
    for mode in bench_modes() {
        println!("  {mode}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bench_cases_have_unique_modes_and_labels() {
        assert!(!BENCH_CASES.is_empty());
        for (idx, bench_case) in BENCH_CASES.iter().enumerate() {
            assert!(!bench_case.mode.trim().is_empty());
            assert!(!bench_case.label.trim().is_empty());
            assert_eq!(
                BENCH_CASES
                    .iter()
                    .filter(|candidate| candidate.mode == bench_case.mode)
                    .count(),
                1,
                "duplicate mode at index {}: {}",
                idx,
                bench_case.mode
            );
            assert_eq!(
                BENCH_CASES
                    .iter()
                    .filter(|candidate| candidate.label == bench_case.label)
                    .count(),
                1,
                "duplicate label at index {}: {}",
                idx,
                bench_case.label
            );
        }
    }

    #[test]
    fn should_run_case_matches_selected_mode_or_all() {
        assert!(should_run_case(None, "direct-read-memory"));
        assert!(should_run_case(
            Some("direct-read-memory"),
            "direct-read-memory"
        ));
        assert!(!should_run_case(
            Some("direct-write-memory"),
            "direct-read-memory"
        ));
    }
}
