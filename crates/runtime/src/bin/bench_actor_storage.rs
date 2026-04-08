use common::{DeployBinding, DeployConfig, WorkerInvocation};
use runtime::{RuntimeConfig, RuntimeService, RuntimeServiceConfig, RuntimeStorageConfig};
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
struct ActorProfileMetric {
    calls: u64,
    total_us: u64,
    total_items: u64,
    max_us: u64,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct ActorProfileSnapshot {
    enabled: bool,
    js_read_only_total: ActorProfileMetric,
    js_freshness_check: ActorProfileMetric,
    js_hydrate_full: ActorProfileMetric,
    js_hydrate_keys: ActorProfileMetric,
    js_txn_commit: ActorProfileMetric,
    js_txn_blind_commit: ActorProfileMetric,
    js_txn_validate: ActorProfileMetric,
    js_cache_hit: ActorProfileMetric,
    js_cache_miss: ActorProfileMetric,
    js_cache_stale: ActorProfileMetric,
    op_read: ActorProfileMetric,
    op_snapshot: ActorProfileMetric,
    op_version_if_newer: ActorProfileMetric,
    op_validate_reads: ActorProfileMetric,
    op_apply_batch: ActorProfileMetric,
    op_apply_blind_batch: ActorProfileMetric,
    store_read: ActorProfileMetric,
    store_snapshot: ActorProfileMetric,
    store_snapshot_keys: ActorProfileMetric,
    store_version_if_newer: ActorProfileMetric,
    store_apply_batch: ActorProfileMetric,
    store_apply_batch_validate: ActorProfileMetric,
    store_apply_batch_write: ActorProfileMetric,
    store_apply_blind_batch: ActorProfileMetric,
    store_apply_blind_batch_write: ActorProfileMetric,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct ActorProfileEnvelope {
    ok: bool,
    snapshot: Option<ActorProfileSnapshot>,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
enum BenchPhase {
    Deploy = 0,
    Seed = 1,
    Invoke = 2,
    Verify = 3,
    Profile = 4,
    Shutdown = 5,
    Done = 6,
    Failed = 7,
}

impl BenchPhase {
    fn as_str(self) -> &'static str {
        match self {
            Self::Deploy => "deploy",
            Self::Seed => "seed",
            Self::Invoke => "invoke",
            Self::Verify => "verify",
            Self::Profile => "profile",
            Self::Shutdown => "shutdown",
            Self::Done => "done",
            Self::Failed => "failed",
        }
    }

    fn from_u8(value: u8) -> Self {
        match value {
            0 => Self::Deploy,
            1 => Self::Seed,
            2 => Self::Invoke,
            3 => Self::Verify,
            4 => Self::Profile,
            5 => Self::Shutdown,
            6 => Self::Done,
            7 => Self::Failed,
            _ => Self::Failed,
        }
    }
}

struct BenchWatchdogState {
    phase: AtomicU8,
    completed_requests: AtomicUsize,
    last_completed_request: AtomicUsize,
    last_progress_ms: AtomicU64,
    stop: AtomicBool,
}

impl BenchWatchdogState {
    fn new() -> Self {
        Self {
            phase: AtomicU8::new(BenchPhase::Deploy as u8),
            completed_requests: AtomicUsize::new(0),
            last_completed_request: AtomicUsize::new(0),
            last_progress_ms: AtomicU64::new(0),
            stop: AtomicBool::new(false),
        }
    }
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn env_flag(name: &str) -> bool {
    std::env::var(name)
        .ok()
        .map(|value| matches!(value.trim(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(false)
}

#[derive(Clone, Copy)]
enum ActorKeyMode {
    Pool,
    Unique,
    SameShard,
    CrossShard,
}

impl ActorKeyMode {
    fn from_env() -> Self {
        match std::env::var("DD_BENCH_ACTOR_KEY_MODE")
            .ok()
            .unwrap_or_else(|| "pool".to_string())
            .to_lowercase()
            .as_str()
        {
            "pool" => Self::Pool,
            "unique" => Self::Unique,
            "same-shard" => Self::SameShard,
            "cross-shard" => Self::CrossShard,
            _ => Self::Pool,
        }
    }
}

fn env_mode() -> Option<String> {
    std::env::var("DD_BENCH_MODE")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

const ACTOR_NAMESPACE_SHARDS: usize = 16;

const ACTOR_READ_ASYNC_STORAGE_WORKER_SOURCE: &str = r#"
export function seed(state) {
  state.set("payload", "1");
  return true;
}

export function read(state) {
  return String(state.get("payload") ?? "0");
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const id = env.BENCH_ACTOR.idFromName(url.searchParams.get("key") ?? "hot");
    const actor = env.BENCH_ACTOR.get(id);
    if (url.pathname === "/__profile") {
      return new Response(JSON.stringify(Deno.core.ops.op_actor_profile_take?.() ?? null), {
        headers: [["content-type", "application/json"]],
      });
    }
    if (url.pathname === "/__profile_reset") {
      Deno.core.ops.op_actor_profile_reset?.();
      return new Response("ok");
    }
    if (url.pathname === "/seed") {
      await actor.atomic(seed);
      return new Response("ok");
    }
    const value = await actor.atomic(read);
    return new Response(String(value));
  },
};
"#;

const ACTOR_READ_ASYNC_MEMORY_WORKER_SOURCE: &str = r#"
export function read(_state) { return "1"; }

export default {
  async fetch(_request, env) {
    const url = new URL(_request.url);
    const id = env.BENCH_ACTOR.idFromName(url.searchParams.get("key") ?? "hot");
    const actor = env.BENCH_ACTOR.get(id);
    const value = await actor.atomic(read);
    return new Response(String(value));
  },
};
"#;

const ACTOR_READ_SYNC_MEMORY_WORKER_SOURCE: &str = r#"
export function read(_state) { return "1"; }

export default {
  async fetch(_request, env) {
    const url = new URL(_request.url);
    const id = env.BENCH_ACTOR.idFromName(url.searchParams.get("key") ?? "hot");
    const actor = env.BENCH_ACTOR.get(id);
    return new Response(String(await actor.atomic(read)));
  },
};
"#;

const ACTOR_DIRECT_READ_WORKER_SOURCE: &str = r#"
export function seed(state) {
  state.set("payload", "1");
  return true;
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const id = env.BENCH_ACTOR.idFromName(url.searchParams.get("key") ?? "hot");
    const actor = env.BENCH_ACTOR.get(id);
    if (url.pathname === "/__profile") {
      return new Response(JSON.stringify(Deno.core.ops.op_actor_profile_take?.() ?? null), {
        headers: [["content-type", "application/json"]],
      });
    }
    if (url.pathname === "/__profile_reset") {
      Deno.core.ops.op_actor_profile_reset?.();
      return new Response("ok");
    }
    if (url.pathname === "/seed") {
      await actor.atomic(seed);
      return new Response("ok");
    }
    return new Response(String(await actor.read("payload") ?? "0"));
  },
};
"#;

const ACTOR_DIRECT_WRITE_WORKER_SOURCE: &str = r#"
export function readStrong(state) {
  return String(state.get("payload") ?? "0");
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const id = env.BENCH_ACTOR.idFromName(url.searchParams.get("key") ?? "hot");
    const actor = env.BENCH_ACTOR.get(id);
    if (url.pathname === "/seed") {
      await actor.write("payload", "0");
      return new Response("ok");
    }
    if (url.pathname === "/read") {
      return new Response(String(await actor.read("payload") ?? "0"));
    }
    if (url.pathname === "/get-strong") {
      return new Response(String(await actor.atomic(readStrong)));
    }
    if (url.pathname === "/write") {
      await actor.write("payload", "1");
      return new Response("ok");
    }
    if (url.pathname === "/delete") {
      await actor.delete("payload");
      return new Response("ok");
    }
    return new Response("not found", { status: 404 });
  },
};
"#;

const ACTOR_ATOMIC_READ_MEMORY_WORKER_SOURCE: &str = r#"
export function seed(state) {
  state.set("payload", "1");
  return true;
}

export function read(state) {
  return String(state.get("payload") ?? "1");
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const id = env.BENCH_ACTOR.idFromName(url.searchParams.get("key") ?? "hot");
    const actor = env.BENCH_ACTOR.get(id);
    if (url.pathname === "/__profile") {
      return new Response(JSON.stringify(Deno.core.ops.op_actor_profile_take?.() ?? null), {
        headers: [["content-type", "application/json"]],
      });
    }
    if (url.pathname === "/__profile_reset") {
      Deno.core.ops.op_actor_profile_reset?.();
      return new Response("ok");
    }
    if (url.pathname === "/seed") {
      await actor.atomic(seed);
      return new Response("ok");
    }
    return new Response(String(await actor.atomic(read)));
  },
};
"#;

const ACTOR_ATOMIC_READ_ALLOW_CONCURRENCY_MEMORY_WORKER_SOURCE: &str = r#"
export function seed(state) {
  state.set("payload", "1");
  return true;
}

export function read(state) {
  return String(state.get("payload", { allowConcurrency: true }) ?? "1");
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const id = env.BENCH_ACTOR.idFromName(url.searchParams.get("key") ?? "hot");
    const actor = env.BENCH_ACTOR.get(id);
    if (url.pathname === "/__profile") {
      return new Response(JSON.stringify(Deno.core.ops.op_actor_profile_take?.() ?? null), {
        headers: [["content-type", "application/json"]],
      });
    }
    if (url.pathname === "/__profile_reset") {
      Deno.core.ops.op_actor_profile_reset?.();
      return new Response("ok");
    }
    if (url.pathname === "/seed") {
      await actor.atomic(seed);
      return new Response("ok");
    }
    return new Response(String(await actor.atomic(read)));
  },
};
"#;

const ACTOR_STM_INCREMENT_WORKER_SOURCE: &str = r#"
export function seed(state) {
  state.set("count", "0");
  return true;
}

export function increment(state) {
  const current = Number(state.get("count") ?? 0);
  const next = current + 1;
  state.set("count", String(next));
  return next;
}

export function readCount(state) {
  return String(state.get("count") ?? "0");
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const id = env.BENCH_ACTOR.idFromName(url.searchParams.get("key") ?? "hot");
    const actor = env.BENCH_ACTOR.get(id);
    if (url.pathname === "/__profile") {
      return new Response(JSON.stringify(Deno.core.ops.op_actor_profile_take?.() ?? null), {
        headers: [["content-type", "application/json"]],
      });
    }
    if (url.pathname === "/__profile_reset") {
      Deno.core.ops.op_actor_profile_reset?.();
      return new Response("ok");
    }
    if (url.pathname === "/seed") {
      await actor.atomic(seed);
      return new Response("ok");
    }
    if (url.pathname === "/get") {
      return new Response(String(await actor.atomic(readCount)));
    }
    const value = await actor.atomic(increment);
    return new Response(String(value));
  },
};
"#;

const ACTOR_STM_READ_WRITE_WORKER_SOURCE: &str = r#"
export function seed(state) {
  state.set("count", "0");
  return true;
}

export function readCount(state) {
  return String(state.get("count") ?? "0");
}

export function increment(state) {
  const current = Number(state.get("count") ?? 0);
  const next = current + 1;
  state.set("count", String(next));
  return next;
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const id = env.BENCH_ACTOR.idFromName(url.searchParams.get("key") ?? "hot");
    const actor = env.BENCH_ACTOR.get(id);
    if (url.pathname === "/__profile") {
      return new Response(JSON.stringify(Deno.core.ops.op_actor_profile_take?.() ?? null), {
        headers: [["content-type", "application/json"]],
      });
    }
    if (url.pathname === "/__profile_reset") {
      Deno.core.ops.op_actor_profile_reset?.();
      return new Response("ok");
    }
    if (url.pathname === "/seed") {
      await actor.atomic(seed);
      return new Response("ok");
    }
    if (url.pathname === "/get") {
      return new Response(String(await actor.atomic(readCount)));
    }
    if (url.pathname === "/sum") {
      const keys = Math.max(1, Number(url.searchParams.get("keys") ?? "1") || 1);
      let total = 0;
      for (let i = 0; i < keys; i++) {
        const sumId = env.BENCH_ACTOR.idFromName(keys === 1 ? "hot" : `bench-${i}`);
        const sumActor = env.BENCH_ACTOR.get(sumId);
        total += Number(await sumActor.atomic(readCount));
      }
      return new Response(String(total));
    }
    if (url.pathname === "/read") {
      return new Response(String(await actor.atomic(readCount)));
    }
    if (url.pathname === "/write") {
      return new Response(String(await actor.atomic(increment)));
    }
    return new Response("not found", { status: 404 });
  },
};
"#;

const ACTOR_STM_READ_WRITE_ALLOW_CONCURRENCY_WORKER_SOURCE: &str = r#"
export function seed(state) {
  state.set("count", "0");
  return true;
}

export function readCount(state) {
  return String(state.get("count", { allowConcurrency: true }) ?? "0");
}

export function increment(state) {
  const current = Number(state.get("count") ?? 0);
  const next = current + 1;
  state.set("count", String(next));
  return next;
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const id = env.BENCH_ACTOR.idFromName(url.searchParams.get("key") ?? "hot");
    const actor = env.BENCH_ACTOR.get(id);
    if (url.pathname === "/__profile") {
      return new Response(JSON.stringify(Deno.core.ops.op_actor_profile_take?.() ?? null), {
        headers: [["content-type", "application/json"]],
      });
    }
    if (url.pathname === "/__profile_reset") {
      Deno.core.ops.op_actor_profile_reset?.();
      return new Response("ok");
    }
    if (url.pathname === "/seed") {
      await actor.atomic(seed);
      return new Response("ok");
    }
    if (url.pathname === "/get") {
      return new Response(String(await actor.atomic(readCount)));
    }
    if (url.pathname === "/sum") {
      const keys = Math.max(1, Number(url.searchParams.get("keys") ?? "1") || 1);
      let total = 0;
      for (let i = 0; i < keys; i++) {
        const sumId = env.BENCH_ACTOR.idFromName(keys === 1 ? "hot" : `bench-${i}`);
        const sumActor = env.BENCH_ACTOR.get(sumId);
        total += Number(await sumActor.atomic(readCount));
      }
      return new Response(String(total));
    }
    if (url.pathname === "/read") {
      return new Response(String(await actor.atomic(readCount)));
    }
    if (url.pathname === "/write") {
      return new Response(String(await actor.atomic(increment)));
    }
    return new Response("not found", { status: 404 });
  },
};
"#;

const ACTOR_STM_BLIND_WRITE_WORKER_SOURCE: &str = r#"
export function seed(state) {
  state.set("count", "0");
  return true;
}

export function readCount(state) {
  return String(state.get("count") ?? "0");
}

export function blindWrite(state) {
  state.set("count", "1");
  return "1";
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const id = env.BENCH_ACTOR.idFromName(url.searchParams.get("key") ?? "hot");
    const actor = env.BENCH_ACTOR.get(id);
    if (url.pathname === "/__profile") {
      return new Response(JSON.stringify(Deno.core.ops.op_actor_profile_take?.() ?? null), {
        headers: [["content-type", "application/json"]],
      });
    }
    if (url.pathname === "/__profile_reset") {
      Deno.core.ops.op_actor_profile_reset?.();
      return new Response("ok");
    }
    if (url.pathname === "/seed") {
      await actor.atomic(seed);
      return new Response("ok");
    }
    if (url.pathname === "/get") {
      return new Response(String(await actor.atomic(readCount)));
    }
    if (url.pathname === "/write") {
      return new Response(String(await actor.atomic(blindWrite)));
    }
    return new Response("not found", { status: 404 });
  },
};
"#;

const ACTOR_ATOMIC_PUT_INCREMENT_WORKER_SOURCE: &str = r#"
export function seed(state) {
  state.set("count", "0");
  return true;
}

export function increment(state) {
  let next = 0;
  state.put("count", (previous) => {
    next = Number(previous ?? "0") + 1;
    return String(next);
  });
  return next;
}

export function readCount(state) {
  return String(state.get("count") ?? "0");
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const id = env.BENCH_ACTOR.idFromName("hot");
    const actor = env.BENCH_ACTOR.get(id);
    if (url.pathname === "/seed") {
      await actor.atomic(seed);
      return new Response("ok");
    }
    if (url.pathname === "/get") {
      return new Response(String(await actor.atomic(readCount)));
    }
    return new Response(String(await actor.atomic(increment)));
  },
};
"#;

fn env_duration_ms(name: &str, default: u64) -> Duration {
    Duration::from_millis(
        std::env::var(name)
            .ok()
            .and_then(|value| value.trim().parse::<u64>().ok())
            .unwrap_or(default),
    )
}

fn bench_options_from_env() -> BenchOptions {
    BenchOptions {
        request_timeout: env_duration_ms("DD_BENCH_REQUEST_TIMEOUT_MS", 10_000),
        verify_timeout: env_duration_ms("DD_BENCH_VERIFY_TIMEOUT_MS", 5_000),
        watchdog_interval: env_duration_ms("DD_BENCH_WATCHDOG_INTERVAL_MS", 1_000),
        watchdog_silence_timeout: env_duration_ms("DD_BENCH_WATCHDOG_SILENCE_MS", 2_000),
        wide_key_space: env_usize("DD_BENCH_WIDE_KEY_SPACE", 256),
    }
}

fn set_watchdog_phase(state: &BenchWatchdogState, started_at: Instant, phase: BenchPhase) {
    state.phase.store(phase as u8, Ordering::Relaxed);
    state
        .last_progress_ms
        .store(started_at.elapsed().as_millis() as u64, Ordering::Relaxed);
}

fn record_watchdog_completion(state: &BenchWatchdogState, started_at: Instant, request_idx: usize) {
    state.completed_requests.fetch_add(1, Ordering::Relaxed);
    state
        .last_completed_request
        .store(request_idx, Ordering::Relaxed);
    state
        .last_progress_ms
        .store(started_at.elapsed().as_millis() as u64, Ordering::Relaxed);
}

fn spawn_watchdog(
    label: String,
    total_requests: usize,
    started_at: Instant,
    options: BenchOptions,
    state: Arc<BenchWatchdogState>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            sleep(options.watchdog_interval).await;
            if state.stop.load(Ordering::Relaxed) {
                break;
            }
            let phase = BenchPhase::from_u8(state.phase.load(Ordering::Relaxed));
            if matches!(phase, BenchPhase::Done | BenchPhase::Failed) {
                break;
            }
            let elapsed_ms = started_at.elapsed().as_millis() as u64;
            let last_progress_ms = state.last_progress_ms.load(Ordering::Relaxed);
            let stalled_for_ms = elapsed_ms.saturating_sub(last_progress_ms);
            let completed = state.completed_requests.load(Ordering::Relaxed);
            let last_completed_request = state.last_completed_request.load(Ordering::Relaxed);
            let health =
                if Duration::from_millis(stalled_for_ms) >= options.watchdog_silence_timeout {
                    "stalled"
                } else {
                    "progressing"
                };
            println!(
                "bench-status label={} phase={} health={} completed={}/{} last_completed_request={} elapsed_ms={} stalled_for_ms={}",
                label,
                phase.as_str(),
                health,
                completed,
                total_requests,
                last_completed_request,
                elapsed_ms,
                stalled_for_ms,
            );
        }
    })
}

#[tokio::main]
async fn main() -> Result<(), String> {
    let profile_enabled = env_flag("DD_BENCH_PROFILE_ACTOR");
    let options = bench_options_from_env();
    let runtime = RuntimeConfig {
        min_isolates: env_usize("DD_BENCH_MIN_ISOLATES", 1),
        max_isolates: env_usize("DD_BENCH_MAX_ISOLATES", 1),
        max_inflight_per_isolate: env_usize("DD_BENCH_MAX_INFLIGHT", 1),
        idle_ttl: env_duration_ms("DD_BENCH_IDLE_TTL_MS", 30_000),
        scale_tick: env_duration_ms("DD_BENCH_SCALE_TICK_MS", 1_000),
        queue_warn_thresholds: vec![10, 100, 1000],
        actor_profile_enabled: profile_enabled,
        ..RuntimeConfig::default()
    };
    let service = start_service("actor-storage", runtime)
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
            "actor-read-async-storage",
            ACTOR_READ_ASYNC_STORAGE_WORKER_SOURCE,
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
            "actor-read-async-memory",
            ACTOR_READ_ASYNC_MEMORY_WORKER_SOURCE,
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
            "actor-read-sync-memory",
            ACTOR_READ_SYNC_MEMORY_WORKER_SOURCE,
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
            "actor-direct-read-memory",
            ACTOR_DIRECT_READ_WORKER_SOURCE,
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
            "actor-direct-write-memory",
            ACTOR_DIRECT_WRITE_WORKER_SOURCE,
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
            "actor-direct-write-memory-multikey",
            ACTOR_DIRECT_WRITE_WORKER_SOURCE,
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
            "actor-direct-read-memory-multikey",
            ACTOR_DIRECT_READ_WORKER_SOURCE,
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
            "actor-direct-read-memory-wide",
            ACTOR_DIRECT_READ_WORKER_SOURCE,
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
            ACTOR_ATOMIC_READ_MEMORY_WORKER_SOURCE,
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
            ACTOR_ATOMIC_READ_ALLOW_CONCURRENCY_MEMORY_WORKER_SOURCE,
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
            ACTOR_ATOMIC_READ_MEMORY_WORKER_SOURCE,
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
            "actor-stm-inc",
            ACTOR_STM_INCREMENT_WORKER_SOURCE,
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
            "actor-stm-read",
            ACTOR_STM_READ_WRITE_WORKER_SOURCE,
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
            "actor-stm-read-multikey",
            ACTOR_STM_READ_WRITE_WORKER_SOURCE,
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
            "actor-stm-read-allow-concurrency",
            ACTOR_STM_READ_WRITE_ALLOW_CONCURRENCY_WORKER_SOURCE,
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
            "actor-stm-read-multikey-allow-concurrency",
            ACTOR_STM_READ_WRITE_ALLOW_CONCURRENCY_WORKER_SOURCE,
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
            "actor-stm-write",
            ACTOR_STM_READ_WRITE_WORKER_SOURCE,
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
            "actor-stm-write-throughput",
            ACTOR_STM_READ_WRITE_WORKER_SOURCE,
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
            "actor-stm-write-multikey",
            ACTOR_STM_READ_WRITE_WORKER_SOURCE,
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
            "actor-stm-blind-write",
            ACTOR_STM_BLIND_WRITE_WORKER_SOURCE,
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
            "actor-stm-blind-write-multikey",
            ACTOR_STM_BLIND_WRITE_WORKER_SOURCE,
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
            "actor-atomic-put-inc",
            ACTOR_ATOMIC_PUT_INCREMENT_WORKER_SOURCE,
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

async fn run_and_print(
    service: &RuntimeService,
    label: &str,
    source: &str,
    seed: bool,
    path: &'static str,
    key_space: usize,
    verify_path: Option<&'static str>,
    profile_enabled: bool,
    options: &BenchOptions,
) -> Result<(), String> {
    let requests = env_usize("DD_BENCH_REQUESTS", 1_000);
    let concurrency = env_usize("DD_BENCH_CONCURRENCY", 1);
    let worker_name = format!("{label}-{}", Uuid::new_v4());
    let started_at = Instant::now();
    let watchdog_state = Arc::new(BenchWatchdogState::new());
    let watchdog_task = spawn_watchdog(
        label.to_string(),
        requests,
        started_at,
        *options,
        Arc::clone(&watchdog_state),
    );
    let mut timings = BenchTimings {
        deploy: Duration::ZERO,
        seed: Duration::ZERO,
        timed: Duration::ZERO,
        verify: Duration::ZERO,
        profile: Duration::ZERO,
        shutdown: Duration::ZERO,
    };

    let deploy_started = Instant::now();
    let deploy_result = service
        .deploy_with_config(
            worker_name.clone(),
            source.to_string(),
            DeployConfig {
                public: false,
                bindings: vec![DeployBinding::Actor {
                    binding: "BENCH_ACTOR".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await;
    timings.deploy = deploy_started.elapsed();
    if let Err(error) = deploy_result {
        set_watchdog_phase(&watchdog_state, started_at, BenchPhase::Failed);
        watchdog_state.stop.store(true, Ordering::Relaxed);
        watchdog_task.abort();
        println!(
            "bench-final label={} outcome=error phase={} message={}",
            label,
            BenchPhase::Deploy.as_str(),
            error
        );
        return Err(error.to_string());
    }

    if seed {
        set_watchdog_phase(&watchdog_state, started_at, BenchPhase::Seed);
        let seed_started = Instant::now();
        let seed_result = seed_benchmark_state(
            service,
            &worker_name,
            requests,
            key_space,
            options.request_timeout,
        )
        .await;
        timings.seed = seed_started.elapsed();
        if let Err(error) = seed_result {
            set_watchdog_phase(&watchdog_state, started_at, BenchPhase::Failed);
            watchdog_state.stop.store(true, Ordering::Relaxed);
            watchdog_task.abort();
            if profile_enabled {
                print_profile_on_failure(service, &worker_name, label).await;
            }
            println!(
                "bench-final label={} outcome=error phase={} message={}",
                label,
                BenchPhase::Seed.as_str(),
                error
            );
            return Err(error.to_string());
        }
    }
    if profile_enabled {
        set_watchdog_phase(&watchdog_state, started_at, BenchPhase::Profile);
        let profile_reset_started = Instant::now();
        let profile_reset_result = service
            .invoke(worker_name.clone(), invocation("/__profile_reset", 0, 1))
            .await;
        timings.profile += profile_reset_started.elapsed();
        if let Err(error) = profile_reset_result {
            set_watchdog_phase(&watchdog_state, started_at, BenchPhase::Failed);
            watchdog_state.stop.store(true, Ordering::Relaxed);
            watchdog_task.abort();
            if profile_enabled {
                print_profile_on_failure(service, &worker_name, label).await;
            }
            println!(
                "bench-final label={} outcome=error phase={} message={}",
                label,
                BenchPhase::Profile.as_str(),
                error
            );
            return Err(error.to_string());
        }
    }

    set_watchdog_phase(&watchdog_state, started_at, BenchPhase::Invoke);
    let result = run_scenario(
        service,
        &worker_name,
        Scenario {
            requests,
            concurrency,
            path,
            key_space,
        },
        options,
        Arc::clone(&watchdog_state),
        started_at,
    )
    .await;
    let result = match result {
        Ok(result) => {
            timings.timed = result.total_duration;
            result
        }
        Err(error) => {
            set_watchdog_phase(&watchdog_state, started_at, BenchPhase::Failed);
            watchdog_state.stop.store(true, Ordering::Relaxed);
            watchdog_task.abort();
            if profile_enabled {
                print_profile_on_failure(service, &worker_name, label).await;
            }
            println!(
                "bench-final label={} outcome=error phase={} message={}",
                label,
                BenchPhase::Invoke.as_str(),
                error
            );
            return Err(error.to_string());
        }
    };
    if let Some(verify_path) = verify_path {
        set_watchdog_phase(&watchdog_state, started_at, BenchPhase::Verify);
        let expected =
            if verify_path == "/sum" || verify_path == "/sum-read" || verify_path == "/sum-blind" {
                distinct_actor_keys(requests, key_space).len().to_string()
            } else if verify_path == "/read" || verify_path == "/get-strong" {
                "1".to_string()
            } else if verify_path == "/get-blind" {
                "1".to_string()
            } else {
                requests.to_string()
            };
        let verify_started = Instant::now();
        let observed = timeout(
            options.verify_timeout,
            verify_expected_value(
                service,
                &worker_name,
                requests,
                key_space,
                verify_path,
                &expected,
                options.request_timeout,
            ),
        )
        .await;
        timings.verify = verify_started.elapsed();
        let observed = match observed {
            Ok(Ok(value)) => value,
            Ok(Err(error)) => {
                set_watchdog_phase(&watchdog_state, started_at, BenchPhase::Failed);
                watchdog_state.stop.store(true, Ordering::Relaxed);
                watchdog_task.abort();
                if profile_enabled {
                    print_profile_on_failure(service, &worker_name, label).await;
                }
                println!(
                    "bench-final label={} outcome=error phase={} message={}",
                    label,
                    BenchPhase::Verify.as_str(),
                    error
                );
                return Err(error);
            }
            Err(_) => {
                let message = format!(
                    "{label} verification timed out after {}ms on {}",
                    options.verify_timeout.as_millis(),
                    verify_path
                );
                set_watchdog_phase(&watchdog_state, started_at, BenchPhase::Failed);
                watchdog_state.stop.store(true, Ordering::Relaxed);
                watchdog_task.abort();
                if profile_enabled {
                    print_profile_on_failure(service, &worker_name, label).await;
                }
                println!(
                    "bench-final label={} outcome=error phase={} message={}",
                    label,
                    BenchPhase::Verify.as_str(),
                    message
                );
                return Err(message);
            }
        };
        if observed.trim() != expected {
            let message = format!(
                "{label} verification failed: expected final count {}, got {}",
                expected, observed
            );
            set_watchdog_phase(&watchdog_state, started_at, BenchPhase::Failed);
            watchdog_state.stop.store(true, Ordering::Relaxed);
            watchdog_task.abort();
            if profile_enabled {
                print_profile_on_failure(service, &worker_name, label).await;
            }
            println!(
                "bench-final label={} outcome=error phase={} message={}",
                label,
                BenchPhase::Verify.as_str(),
                message
            );
            return Err(message);
        }
    }
    println!(
        "{:<24} requests={} concurrency={} total={:.2}ms throughput={:.0} req/s mean={:.2}ms p50={:.2}ms p95={:.2}ms p99={:.2}ms",
        label,
        result.requests,
        result.concurrency,
        result.total_duration.as_secs_f64() * 1000.0,
        result.throughput_rps,
        result.mean_ms,
        result.p50_ms,
        result.p95_ms,
        result.p99_ms
    );
    if profile_enabled {
        set_watchdog_phase(&watchdog_state, started_at, BenchPhase::Profile);
        let profile_started = Instant::now();
        let profile = match take_profile(service, &worker_name).await {
            Ok(profile) => profile,
            Err(error) => {
                set_watchdog_phase(&watchdog_state, started_at, BenchPhase::Failed);
                watchdog_state.stop.store(true, Ordering::Relaxed);
                watchdog_task.abort();
                if profile_enabled {
                    print_profile_on_failure(service, &worker_name, label).await;
                }
                println!(
                    "bench-final label={} outcome=error phase={} message={}",
                    label,
                    BenchPhase::Profile.as_str(),
                    error
                );
                return Err(error);
            }
        };
        timings.profile += profile_started.elapsed();
        print_profile(&profile);
    }
    set_watchdog_phase(&watchdog_state, started_at, BenchPhase::Shutdown);
    timings.shutdown = Duration::ZERO;
    set_watchdog_phase(&watchdog_state, started_at, BenchPhase::Done);
    watchdog_state.stop.store(true, Ordering::Relaxed);
    watchdog_task.abort();
    println!(
        "bench-phases label={} deploy_ms={:.2} seed_ms={:.2} timed_ms={:.2} verify_ms={:.2} profile_ms={:.2} shutdown_ms={:.2} total_ms={:.2}",
        label,
        timings.deploy.as_secs_f64() * 1000.0,
        timings.seed.as_secs_f64() * 1000.0,
        timings.timed.as_secs_f64() * 1000.0,
        timings.verify.as_secs_f64() * 1000.0,
        timings.profile.as_secs_f64() * 1000.0,
        timings.shutdown.as_secs_f64() * 1000.0,
        started_at.elapsed().as_secs_f64() * 1000.0,
    );
    println!(
        "bench-final label={} outcome=success phase={} completed_requests={} total_requests={}",
        label,
        BenchPhase::Done.as_str(),
        watchdog_state.completed_requests.load(Ordering::Relaxed),
        requests,
    );
    Ok(())
}

async fn print_profile_on_failure(service: &RuntimeService, worker_name: &str, label: &str) {
    match timeout(Duration::from_secs(2), take_profile(service, worker_name)).await {
        Err(_) => {
            println!(
                "bench-profile label={} outcome=error message=profile collection timed out",
                label
            );
        }
        Ok(Err(error)) => {
            println!(
                "bench-profile label={} outcome=error message={}",
                label, error
            );
        }
        Ok(Ok(profile)) => {
            println!("bench-profile label={} outcome=error", label);
            print_profile(&profile);
        }
    }
}

async fn verify_expected_value(
    service: &RuntimeService,
    worker_name: &str,
    requests: usize,
    key_space: usize,
    verify_path: &str,
    expected: &str,
    request_timeout: Duration,
) -> Result<String, String> {
    let deadline = TokioInstant::now() + Duration::from_secs(2);
    loop {
        let observed =
            if verify_path == "/sum" || verify_path == "/sum-read" || verify_path == "/sum-blind" {
                verify_distinct_actor_sum(
                    service,
                    worker_name,
                    requests,
                    key_space,
                    verify_path,
                    request_timeout,
                )
                .await?
            } else {
                let verify = timeout(
                    request_timeout,
                    service.invoke(
                        worker_name.to_string(),
                        invocation(
                            if verify_path == "/get-blind" {
                                "/get"
                            } else {
                                verify_path
                            },
                            requests + 1,
                            1,
                        ),
                    ),
                )
                .await
                .map_err(|_| {
                    format!(
                        "verification invoke timed out after {}ms on {}",
                        request_timeout.as_millis(),
                        verify_path
                    )
                })?
                .map_err(|error| error.to_string())?;
                String::from_utf8(verify.body).map_err(|error| error.to_string())?
            };
        if observed.trim() == expected {
            return Ok(observed);
        }
        if TokioInstant::now() >= deadline {
            return Ok(observed);
        }
        sleep(Duration::from_millis(10)).await;
    }
}

async fn seed_benchmark_state(
    service: &RuntimeService,
    worker_name: &str,
    requests: usize,
    key_space: usize,
    request_timeout: Duration,
) -> Result<(), String> {
    if key_space <= 1 {
        timeout(
            request_timeout,
            service.invoke(worker_name.to_string(), invocation("/seed", 0, 1)),
        )
        .await
        .map_err(|_| {
            format!(
                "seed invoke timed out after {}ms on /seed",
                request_timeout.as_millis()
            )
        })?
        .map_err(|error| error.to_string())?;
        return Ok(());
    }

    for (offset, actor_key) in distinct_actor_keys(requests, key_space)
        .into_iter()
        .enumerate()
    {
        timeout(
            request_timeout,
            service.invoke(
                worker_name.to_string(),
                WorkerInvocation {
                    method: "GET".to_string(),
                    url: format!("http://worker/seed?key={actor_key}"),
                    headers: Vec::new(),
                    body: Vec::new(),
                    request_id: format!("bench-seed-{offset}"),
                },
            ),
        )
        .await
        .map_err(|_| {
            format!(
                "seed invoke timed out after {}ms on /seed key={}",
                request_timeout.as_millis(),
                actor_key
            )
        })?
        .map_err(|error| error.to_string())?;
    }
    Ok(())
}

async fn verify_distinct_actor_sum(
    service: &RuntimeService,
    worker_name: &str,
    requests: usize,
    key_space: usize,
    path: &str,
    request_timeout: Duration,
) -> Result<String, String> {
    let read_path = if path == "/sum-read" {
        "/get-strong"
    } else {
        "/get"
    };
    let mut total = 0usize;
    for (offset, actor_key) in distinct_actor_keys(requests, key_space)
        .into_iter()
        .enumerate()
    {
        let verify = timeout(
            request_timeout,
            service.invoke(
                worker_name.to_string(),
                WorkerInvocation {
                    method: "GET".to_string(),
                    url: format!("http://worker{read_path}?key={actor_key}"),
                    headers: Vec::new(),
                    body: Vec::new(),
                    request_id: format!("bench-verify-{offset}"),
                },
            ),
        )
        .await
        .map_err(|_| {
            format!(
                "verification sum invoke timed out after {}ms on {} key={}",
                request_timeout.as_millis(),
                read_path,
                actor_key
            )
        })?
        .map_err(|error| error.to_string())?;
        total += String::from_utf8(verify.body)
            .map_err(|error| error.to_string())?
            .trim()
            .parse::<usize>()
            .map_err(|error| error.to_string())?;
    }
    Ok(total.to_string())
}

async fn take_profile(
    service: &RuntimeService,
    worker_name: &str,
) -> Result<ActorProfileSnapshot, String> {
    let output = service
        .invoke(worker_name.to_string(), invocation("/__profile", 0, 1))
        .await
        .map_err(|error| error.to_string())?;
    let body = String::from_utf8(output.body).map_err(|error| error.to_string())?;
    let profile: ActorProfileEnvelope =
        serde_json::from_str(&body).map_err(|error| error.to_string())?;
    if !profile.ok {
        return Err(if profile.error.is_empty() {
            "actor profile collection failed".to_string()
        } else {
            profile.error
        });
    }
    Ok(profile.snapshot.unwrap_or_default())
}

fn print_profile(profile: &ActorProfileSnapshot) {
    if !profile.enabled {
        return;
    }
    println!(
        "profile-actor js_read={:.2}ms js_commit={:.2}ms js_blind_commit={:.2}ms js_validate={:.2}ms freshness={:.2}ms hydrate_full={:.2}ms hydrate_keys={:.2}ms cache hit={} miss={} stale={}",
        metric_mean_ms(&profile.js_read_only_total),
        metric_mean_ms(&profile.js_txn_commit),
        metric_mean_ms(&profile.js_txn_blind_commit),
        metric_mean_ms(&profile.js_txn_validate),
        metric_mean_ms(&profile.js_freshness_check),
        metric_mean_ms(&profile.js_hydrate_full),
        metric_mean_ms(&profile.js_hydrate_keys),
        profile.js_cache_hit.calls,
        profile.js_cache_miss.calls,
        profile.js_cache_stale.calls,
    );
    println!(
        "profile-actor-op read={:.2}ms snapshot={:.2}ms version={:.2}ms validate={:.2}ms apply={:.2}ms blind_apply={:.2}ms store_read={:.2}ms store_snapshot={:.2}ms store_keys={:.2}ms store_version={:.2}ms store_apply={:.2}ms store_validate={:.2}ms store_write={:.2}ms store_blind_apply={:.2}ms store_blind_write={:.2}ms",
        metric_mean_ms(&profile.op_read),
        metric_mean_ms(&profile.op_snapshot),
        metric_mean_ms(&profile.op_version_if_newer),
        metric_mean_ms(&profile.op_validate_reads),
        metric_mean_ms(&profile.op_apply_batch),
        metric_mean_ms(&profile.op_apply_blind_batch),
        metric_mean_ms(&profile.store_read),
        metric_mean_ms(&profile.store_snapshot),
        metric_mean_ms(&profile.store_snapshot_keys),
        metric_mean_ms(&profile.store_version_if_newer),
        metric_mean_ms(&profile.store_apply_batch),
        metric_mean_ms(&profile.store_apply_batch_validate),
        metric_mean_ms(&profile.store_apply_batch_write),
        metric_mean_ms(&profile.store_apply_blind_batch),
        metric_mean_ms(&profile.store_apply_blind_batch_write),
    );
}

fn metric_mean_ms(metric: &ActorProfileMetric) -> f64 {
    if metric.calls == 0 {
        0.0
    } else {
        metric.total_us as f64 / metric.calls as f64 / 1000.0
    }
}

async fn run_scenario(
    service: &RuntimeService,
    worker_name: &str,
    scenario: Scenario,
    options: &BenchOptions,
    watchdog_state: Arc<BenchWatchdogState>,
    started_at: Instant,
) -> common::Result<ScenarioResult> {
    let next = Arc::new(AtomicUsize::new(0));
    let latencies = Arc::new(tokio::sync::Mutex::new(Vec::with_capacity(
        scenario.requests,
    )));
    let scenario_started_at = Instant::now();
    let mut tasks = Vec::with_capacity(scenario.concurrency);

    for _ in 0..scenario.concurrency {
        let service = service.clone();
        let worker_name = worker_name.to_string();
        let next = Arc::clone(&next);
        let latencies = Arc::clone(&latencies);
        let path = scenario.path.to_string();
        let watchdog_state = Arc::clone(&watchdog_state);
        let request_timeout = options.request_timeout;
        tasks.push(tokio::spawn(async move {
            loop {
                let idx = next.fetch_add(1, Ordering::Relaxed);
                if idx >= scenario.requests {
                    break;
                }
                let invoke_started = Instant::now();
                let output = timeout(
                    request_timeout,
                    service.invoke(
                        worker_name.clone(),
                        invocation(&path, idx + 1, scenario.key_space),
                    ),
                )
                .await
                .map_err(|_| {
                    common::PlatformError::runtime(format!(
                        "benchmark invoke timed out after {}ms phase=invoke path={} request={}",
                        request_timeout.as_millis(),
                        path,
                        idx + 1
                    ))
                })??;
                if output.status != 200 {
                    return Err(common::PlatformError::runtime(format!(
                        "benchmark invoke failed with status {} on {}",
                        output.status, path
                    )));
                }
                latencies.lock().await.push(invoke_started.elapsed());
                record_watchdog_completion(&watchdog_state, started_at, idx + 1);
            }
            Ok::<(), common::PlatformError>(())
        }));
    }

    for task in tasks {
        task.await
            .map_err(|error| common::PlatformError::internal(error.to_string()))??;
    }

    let total_duration = scenario_started_at.elapsed();
    let mut latencies = Arc::try_unwrap(latencies)
        .map_err(|_| common::PlatformError::internal("latency collection still shared"))?
        .into_inner();
    latencies.sort_unstable();

    let throughput_rps = scenario.requests as f64 / total_duration.as_secs_f64();
    let mean_ms = if latencies.is_empty() {
        0.0
    } else {
        latencies
            .iter()
            .map(|duration| duration.as_secs_f64() * 1000.0)
            .sum::<f64>()
            / latencies.len() as f64
    };

    Ok(ScenarioResult {
        requests: scenario.requests,
        concurrency: scenario.concurrency,
        total_duration,
        throughput_rps,
        mean_ms,
        p50_ms: percentile_ms(&latencies, 0.50),
        p95_ms: percentile_ms(&latencies, 0.95),
        p99_ms: percentile_ms(&latencies, 0.99),
    })
}

fn percentile_ms(latencies: &[Duration], quantile: f64) -> f64 {
    if latencies.is_empty() {
        return 0.0;
    }
    let q = quantile.clamp(0.0, 1.0);
    let index = ((latencies.len() - 1) as f64 * q).round() as usize;
    latencies[index].as_secs_f64() * 1000.0
}

fn invocation(path: &str, idx: usize, key_space: usize) -> WorkerInvocation {
    let actor_key_mode = ActorKeyMode::from_env();
    let actor_key = actor_key(idx, key_space, actor_key_mode, ACTOR_NAMESPACE_SHARDS);

    let url = if key_space > 1 {
        let separator = if path.contains('?') { '&' } else { '?' };
        format!("http://worker{path}{separator}key={actor_key}")
    } else {
        format!("http://worker{path}")
    };
    WorkerInvocation {
        method: "GET".to_string(),
        url,
        headers: Vec::new(),
        body: Vec::new(),
        request_id: format!("bench-actor-{idx}"),
    }
}

fn actor_shard(actor_key: &str, namespace_shards: usize) -> usize {
    if namespace_shards <= 1 {
        return 0;
    }
    let mut hasher = DefaultHasher::new();
    actor_key.hash(&mut hasher);
    (hasher.finish() as usize) % namespace_shards
}

fn actor_key(idx: usize, key_space: usize, mode: ActorKeyMode, namespace_shards: usize) -> String {
    if key_space == 1 {
        return "hot".to_string();
    }

    let actor_slot = idx % key_space;
    match mode {
        ActorKeyMode::Pool => format!("bench-{actor_slot}"),
        ActorKeyMode::Unique => format!("bench-{idx}"),
        ActorKeyMode::SameShard => {
            let shard = actor_shard("bench-direct-same-shard-anchor", namespace_shards);
            actor_key_for_shard_occurrence(
                shard,
                actor_slot,
                namespace_shards,
                "bench-direct-sameshard",
            )
        }
        ActorKeyMode::CrossShard => {
            let target_shard = actor_slot % namespace_shards;
            let occurrence = actor_slot / namespace_shards;
            actor_key_for_shard_occurrence(
                target_shard,
                occurrence,
                namespace_shards,
                "bench-direct-cross-shard",
            )
        }
    }
}

fn distinct_actor_keys(requests: usize, key_space: usize) -> Vec<String> {
    let actor_key_mode = ActorKeyMode::from_env();
    let mut keys = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for idx in 1..=requests {
        let key = actor_key(idx, key_space, actor_key_mode, ACTOR_NAMESPACE_SHARDS);
        if seen.insert(key.clone()) {
            keys.push(key);
        }
    }
    keys
}

fn actor_key_for_shard_occurrence(
    target_shard: usize,
    occurrence: usize,
    namespace_shards: usize,
    prefix: &str,
) -> String {
    if namespace_shards <= 1 {
        return format!("{prefix}-{occurrence}");
    }
    let mut sequence = 0;
    let mut seen = 0;
    loop {
        let candidate = format!("{prefix}-{sequence}");
        if actor_shard(&candidate, namespace_shards) == target_shard {
            if seen == occurrence {
                return candidate;
            }
            seen += 1;
        }
        sequence += 1;
    }
}

async fn start_service(tag: &str, runtime: RuntimeConfig) -> common::Result<RuntimeService> {
    let paths = bench_paths(tag);
    tokio::fs::create_dir_all(&paths.store_dir)
        .await
        .map_err(|error| common::PlatformError::internal(error.to_string()))?;
    RuntimeService::start_with_service_config(runtime_service_config(
        runtime,
        &paths.db_path,
        &paths.store_dir,
    ))
    .await
}

struct BenchPaths {
    db_path: PathBuf,
    store_dir: PathBuf,
}

fn bench_paths(tag: &str) -> BenchPaths {
    let root = PathBuf::from(format!("/tmp/dd-bench-{tag}-{}", Uuid::new_v4()));
    BenchPaths {
        db_path: root.join("dd-kv.db"),
        store_dir: root.join("store"),
    }
}

fn runtime_service_config(
    runtime: RuntimeConfig,
    db_path: &Path,
    store_dir: &Path,
) -> RuntimeServiceConfig {
    RuntimeServiceConfig {
        runtime,
        storage: RuntimeStorageConfig {
            store_dir: store_dir.to_path_buf(),
            database_url: format!("file:{}", db_path.display()),
            actor_namespace_shards: 16,
            actor_db_cache_max_open: 4096,
            actor_db_idle_ttl: Duration::from_secs(60),
            worker_store_enabled: true,
            blob_store: runtime::BlobStoreConfig::local(store_dir.join("blobs")),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SLOW_WORKER_SOURCE: &str = r#"
export default {
  async fetch() {
    await new Promise((resolve) => setTimeout(resolve, 200));
    return new Response("ok");
  },
};
"#;

    #[tokio::test]
    async fn run_scenario_timeout_surfaces_phase_and_request() {
        let service = start_service(
            "bench-watchdog-timeout",
            RuntimeConfig {
                min_isolates: 1,
                max_isolates: 1,
                max_inflight_per_isolate: 1,
                idle_ttl: Duration::from_secs(5),
                scale_tick: Duration::from_millis(50),
                queue_warn_thresholds: vec![10],
                ..RuntimeConfig::default()
            },
        )
        .await
        .expect("service should start");

        let worker_name = format!("bench-slow-{}", Uuid::new_v4());
        service
            .deploy_with_config(
                worker_name.clone(),
                SLOW_WORKER_SOURCE.to_string(),
                DeployConfig::default(),
            )
            .await
            .expect("deploy should succeed");

        let options = BenchOptions {
            request_timeout: Duration::from_millis(25),
            verify_timeout: Duration::from_millis(25),
            watchdog_interval: Duration::from_millis(5),
            watchdog_silence_timeout: Duration::from_millis(10),
            wide_key_space: 16,
        };
        let watchdog_state = Arc::new(BenchWatchdogState::new());
        let started_at = Instant::now();
        set_watchdog_phase(&watchdog_state, started_at, BenchPhase::Invoke);
        let error = run_scenario(
            &service,
            &worker_name,
            Scenario {
                requests: 1,
                concurrency: 1,
                path: "/",
                key_space: 1,
            },
            &options,
            watchdog_state,
            started_at,
        )
        .await
        .expect_err("scenario should time out")
        .to_string();

        assert!(error.contains("timed out"));
        assert!(error.contains("phase=invoke"));
        assert!(error.contains("request=1"));
    }
}
