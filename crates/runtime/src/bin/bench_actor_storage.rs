use common::{DeployBinding, DeployConfig, WorkerInvocation};
use runtime::{RuntimeConfig, RuntimeService, RuntimeServiceConfig, RuntimeStorageConfig};
use serde::Deserialize;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::{Duration, Instant};
use uuid::Uuid;

#[derive(Clone, Copy)]
struct Scenario {
    requests: usize,
    concurrency: usize,
    path: &'static str,
    key_space: usize,
}

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
    js_cache_hit: ActorProfileMetric,
    js_cache_miss: ActorProfileMetric,
    js_cache_stale: ActorProfileMetric,
    op_snapshot: ActorProfileMetric,
    op_version_if_newer: ActorProfileMetric,
    op_validate_reads: ActorProfileMetric,
    store_snapshot: ActorProfileMetric,
    store_snapshot_keys: ActorProfileMetric,
    store_version_if_newer: ActorProfileMetric,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct ActorProfileEnvelope {
    ok: bool,
    snapshot: Option<ActorProfileSnapshot>,
    error: String,
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

fn env_mode() -> Option<String> {
    std::env::var("DD_BENCH_MODE")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

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

#[tokio::main]
async fn main() -> Result<(), String> {
    let profile_enabled = env_flag("DD_BENCH_PROFILE_ACTOR");
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
) -> Result<(), String> {
    let requests = env_usize("DD_BENCH_REQUESTS", 1_000);
    let concurrency = env_usize("DD_BENCH_CONCURRENCY", 1);
    let worker_name = format!("{label}-{}", Uuid::new_v4());
    service
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
        .await
        .map_err(|error| error.to_string())?;

    if seed {
        service
            .invoke(worker_name.clone(), invocation("/seed", 0, 1))
            .await
            .map_err(|error| error.to_string())?;
    }
    if profile_enabled && label.starts_with("actor-stm") {
        service
            .invoke(worker_name.clone(), invocation("/__profile_reset", 0, 1))
            .await
            .map_err(|error| error.to_string())?;
    }

    let result = run_scenario(
        service,
        &worker_name,
        Scenario {
            requests,
            concurrency,
            path,
            key_space,
        },
    )
    .await
    .map_err(|error| error.to_string())?;
    if let Some(verify_path) = verify_path {
        let verify_invocation = if verify_path == "/sum" {
            invocation(&format!("/sum?keys={}", key_space.max(1)), requests + 1, 1)
        } else {
            invocation(verify_path, requests + 1, 1)
        };
        let verify = service
            .invoke(worker_name.clone(), verify_invocation)
            .await
            .map_err(|error| error.to_string())?;
        let observed = String::from_utf8(verify.body).map_err(|error| error.to_string())?;
        if observed.trim() != requests.to_string() {
            return Err(format!(
                "{label} verification failed: expected final count {}, got {}",
                requests, observed
            ));
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
    if profile_enabled && label.starts_with("actor-stm") {
        let profile = take_profile(service, &worker_name).await?;
        print_profile(&profile);
    }
    Ok(())
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
        "profile-actor js_read={:.2}ms freshness={:.2}ms hydrate_full={:.2}ms hydrate_keys={:.2}ms cache hit={} miss={} stale={}",
        metric_mean_ms(&profile.js_read_only_total),
        metric_mean_ms(&profile.js_freshness_check),
        metric_mean_ms(&profile.js_hydrate_full),
        metric_mean_ms(&profile.js_hydrate_keys),
        profile.js_cache_hit.calls,
        profile.js_cache_miss.calls,
        profile.js_cache_stale.calls,
    );
    println!(
        "profile-actor-op snapshot={:.2}ms version={:.2}ms validate={:.2}ms store_snapshot={:.2}ms store_keys={:.2}ms store_version={:.2}ms",
        metric_mean_ms(&profile.op_snapshot),
        metric_mean_ms(&profile.op_version_if_newer),
        metric_mean_ms(&profile.op_validate_reads),
        metric_mean_ms(&profile.store_snapshot),
        metric_mean_ms(&profile.store_snapshot_keys),
        metric_mean_ms(&profile.store_version_if_newer),
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
) -> common::Result<ScenarioResult> {
    let next = Arc::new(AtomicUsize::new(0));
    let latencies = Arc::new(tokio::sync::Mutex::new(Vec::with_capacity(
        scenario.requests,
    )));
    let started_at = Instant::now();
    let mut tasks = Vec::with_capacity(scenario.concurrency);

    for _ in 0..scenario.concurrency {
        let service = service.clone();
        let worker_name = worker_name.to_string();
        let next = Arc::clone(&next);
        let latencies = Arc::clone(&latencies);
        let path = scenario.path.to_string();
        tasks.push(tokio::spawn(async move {
            loop {
                let idx = next.fetch_add(1, Ordering::Relaxed);
                if idx >= scenario.requests {
                    break;
                }
                let invoke_started = Instant::now();
                let output = service
                    .invoke(
                        worker_name.clone(),
                        invocation(&path, idx + 1, scenario.key_space),
                    )
                    .await?;
                if output.status != 200 {
                    return Err(common::PlatformError::runtime(format!(
                        "benchmark invoke failed with status {} on {}",
                        output.status, path
                    )));
                }
                latencies.lock().await.push(invoke_started.elapsed());
            }
            Ok::<(), common::PlatformError>(())
        }));
    }

    for task in tasks {
        task.await
            .map_err(|error| common::PlatformError::internal(error.to_string()))??;
    }

    let total_duration = started_at.elapsed();
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
    let url = if key_space > 1 {
        let separator = if path.contains('?') { '&' } else { '?' };
        let actor_idx = idx % key_space;
        format!("http://worker{path}{separator}key=bench-{actor_idx}")
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
            actor_db_cache_max_open: 4096,
            actor_db_idle_ttl: Duration::from_secs(60),
            worker_store_enabled: true,
            blob_store: runtime::BlobStoreConfig::local(store_dir.join("blobs")),
        },
    }
}
