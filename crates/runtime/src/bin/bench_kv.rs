use common::{DeployBinding, DeployConfig, WorkerInvocation};
use runtime::{
    BlobStoreConfig, RuntimeConfig, RuntimeService, RuntimeServiceConfig, RuntimeStorageConfig,
};
use serde::Deserialize;
use std::collections::HashMap;
use std::env;
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use uuid::Uuid;

#[derive(Clone, Copy)]
struct Scenario {
    label: &'static str,
    requests: usize,
    concurrency: usize,
    path: &'static str,
    worker_source: &'static str,
    use_kv_binding: bool,
}

struct ScenarioResult {
    throughput_rps: f64,
    mean_ms: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    total_ms: f64,
}

struct BenchConfig {
    name: &'static str,
    runtime: RuntimeConfig,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct KvProfileMetric {
    calls: u64,
    total_us: u64,
    total_items: u64,
    max_us: u64,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct KvProfileSnapshot {
    enabled: bool,
    js_request_total: KvProfileMetric,
    js_batch_flush: KvProfileMetric,
    op_get: KvProfileMetric,
    op_get_many_utf8: KvProfileMetric,
    op_get_value: KvProfileMetric,
    store_get_utf8: KvProfileMetric,
    store_get_utf8_many: KvProfileMetric,
    store_get_value: KvProfileMetric,
    write_enqueue: KvProfileMetric,
    write_superseded: KvProfileMetric,
    write_rejected: KvProfileMetric,
    write_flush: KvProfileMetric,
    write_retry: KvProfileMetric,
    write_queue_wait: KvProfileMetric,
    js_cache_hit: KvProfileMetric,
    js_cache_miss: KvProfileMetric,
    js_cache_stale: KvProfileMetric,
    js_cache_fill: KvProfileMetric,
    js_cache_invalidate: KvProfileMetric,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct KvProfileEnvelope {
    ok: bool,
    snapshot: Option<KvProfileSnapshot>,
    error: String,
}

const FETCH_BASELINE_WORKER_SOURCE: &str = r#"
export default {
  fetch() {
    return new Response("ok");
  },
};
"#;

const FETCH_URL_BASELINE_WORKER_SOURCE: &str = r#"
export default {
  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === "/noop") {
      return new Response("ok");
    }
    return new Response("not found", { status: 404 });
  },
};
"#;

const KV_WORKER_SOURCE: &str = r#"
export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/__profile") {
      return new Response(JSON.stringify(Deno.core.ops.op_kv_profile_take?.() ?? null), {
        headers: [["content-type", "application/json"]],
      });
    }
    if (url.pathname === "/__profile_reset") {
      Deno.core.ops.op_kv_profile_reset?.();
      return new Response("ok");
    }
    const requestStart = performance.now();
    const finish = () => {
      Deno.core.ops.op_kv_profile_record_js?.(
        "js_request_total",
        Math.max(0, Math.round((performance.now() - requestStart) * 1000)),
        1,
      );
    };
    try {
    if (url.pathname === "/seed") {
      await env.MY_KV.put("hot", "1");
      for (let i = 0; i < 10; i++) {
        await env.MY_KV.put(`hot-${i}`, String(i));
      }
      return new Response("ok");
    }
    if (url.pathname === "/noop") {
      return new Response("ok");
    }
    if (url.pathname === "/read") {
      return new Response(String((await env.MY_KV.get("hot")) ?? "0"));
    }
    if (url.pathname === "/read10") {
      let current = "0";
      for (let i = 0; i < 10; i++) {
        current = String((await env.MY_KV.get("hot")) ?? "0");
      }
      return new Response(current);
    }
    if (url.pathname === "/readqueue10") {
      const tasks = [];
      for (let i = 0; i < 10; i++) {
        tasks.push(env.MY_KV.get("hot"));
      }
      const values = await Promise.all(tasks);
      return new Response(String(values.at(-1) ?? "0"));
    }
    if (url.pathname === "/readqueue10distinct") {
      const tasks = [];
      for (let i = 0; i < 10; i++) {
        tasks.push(env.MY_KV.get(`hot-${i}`));
      }
      const values = await Promise.all(tasks);
      return new Response(String(values.at(-1) ?? "0"));
    }
    if (url.pathname === "/write") {
      await env.MY_KV.put("hot", "1");
      return new Response("ok");
    }
    if (url.pathname === "/write10") {
      for (let i = 0; i < 10; i++) {
        await env.MY_KV.put("hot", String(i));
      }
      return new Response("ok");
    }
    if (url.pathname === "/writequeue10") {
      const tasks = [];
      for (let i = 0; i < 10; i++) {
        tasks.push(env.MY_KV.put("hot", String(i)));
      }
      await Promise.all(tasks);
      return new Response("ok");
    }
    if (url.pathname === "/readwrite") {
      const current = Number((await env.MY_KV.get("hot")) ?? "0") || 0;
      const next = current + 1;
      await env.MY_KV.put("hot", String(next));
      return new Response(String(next));
    }
    return new Response("not found", { status: 404 });
    } finally {
      finish();
    }
  },
};
"#;

#[tokio::main]
async fn main() -> Result<(), String> {
    let requests = env_usize("DD_BENCH_REQUESTS", 200);
    let concurrency = env_usize("DD_BENCH_CONCURRENCY", 32);
    let max_inflight = env_usize("DD_BENCH_MAX_INFLIGHT", 4);
    let single_max_inflight = env_usize("DD_BENCH_SINGLE_MAX_INFLIGHT", 1);
    let autoscaling_isolates = env_usize("DD_BENCH_AUTOSCALING_ISOLATES", 8);
    let write_requests = env_usize("DD_BENCH_WRITE_REQUESTS", requests.min(5_000));
    let write_concurrency = env_usize("DD_BENCH_WRITE_CONCURRENCY", concurrency.min(256));
    let profile_enabled = env_bool("DD_BENCH_PROFILE_KV", false);
    let autoscaling_name =
        Box::leak(format!("autoscaling-{autoscaling_isolates}").into_boxed_str());

    let configs = [
        BenchConfig {
            name: "single-isolate",
            runtime: RuntimeConfig {
                min_isolates: 1,
                max_isolates: 1,
                max_inflight_per_isolate: single_max_inflight,
                idle_ttl: Duration::from_secs(60),
                scale_tick: Duration::from_secs(1),
                queue_warn_thresholds: vec![10, 100, 1000],
                kv_profile_enabled: profile_enabled,
                ..RuntimeConfig::default()
            },
        },
        BenchConfig {
            name: autoscaling_name,
            runtime: RuntimeConfig {
                min_isolates: 0,
                max_isolates: autoscaling_isolates,
                max_inflight_per_isolate: max_inflight,
                idle_ttl: Duration::from_secs(30),
                scale_tick: Duration::from_secs(1),
                queue_warn_thresholds: vec![10, 100, 1000],
                kv_profile_enabled: profile_enabled,
                ..RuntimeConfig::default()
            },
        },
    ];

    let scenarios = [
        Scenario {
            label: "fetch-noop",
            requests,
            concurrency,
            path: "/",
            worker_source: FETCH_BASELINE_WORKER_SOURCE,
            use_kv_binding: false,
        },
        Scenario {
            label: "fetch-url-noop",
            requests,
            concurrency,
            path: "/noop",
            worker_source: FETCH_URL_BASELINE_WORKER_SOURCE,
            use_kv_binding: false,
        },
        Scenario {
            label: "kv-noop",
            requests,
            concurrency,
            path: "/noop",
            worker_source: KV_WORKER_SOURCE,
            use_kv_binding: true,
        },
        Scenario {
            label: "kv-read",
            requests,
            concurrency,
            path: "/read",
            worker_source: KV_WORKER_SOURCE,
            use_kv_binding: true,
        },
        Scenario {
            label: "kv-read10",
            requests,
            concurrency,
            path: "/read10",
            worker_source: KV_WORKER_SOURCE,
            use_kv_binding: true,
        },
        Scenario {
            label: "kv-readqueue10",
            requests,
            concurrency,
            path: "/readqueue10",
            worker_source: KV_WORKER_SOURCE,
            use_kv_binding: true,
        },
        Scenario {
            label: "kv-readqueue10distinct",
            requests,
            concurrency,
            path: "/readqueue10distinct",
            worker_source: KV_WORKER_SOURCE,
            use_kv_binding: true,
        },
        Scenario {
            label: "kv-write",
            requests: write_requests,
            concurrency: write_concurrency,
            path: "/write",
            worker_source: KV_WORKER_SOURCE,
            use_kv_binding: true,
        },
        Scenario {
            label: "kv-write10",
            requests: write_requests,
            concurrency: write_concurrency,
            path: "/write10",
            worker_source: KV_WORKER_SOURCE,
            use_kv_binding: true,
        },
        Scenario {
            label: "kv-writequeue10",
            requests: write_requests,
            concurrency: write_concurrency,
            path: "/writequeue10",
            worker_source: KV_WORKER_SOURCE,
            use_kv_binding: true,
        },
        Scenario {
            label: "kv-readwrite",
            requests: write_requests,
            concurrency: write_concurrency,
            path: "/readwrite",
            worker_source: KV_WORKER_SOURCE,
            use_kv_binding: true,
        },
    ];

    if let Some(config_name) = env_name("DD_BENCH_INTERNAL_CONFIG") {
        let config = configs
            .iter()
            .find(|config| config.name == config_name)
            .ok_or_else(|| format!("unknown kv config: {config_name}"))?;
        if let Some(scenario_name) = env_name("DD_BENCH_INTERNAL_SCENARIO") {
            let scenario = scenarios
                .iter()
                .find(|scenario| scenario.label == scenario_name)
                .ok_or_else(|| format!("unknown kv scenario: {scenario_name}"))?;
            run_config_scenario(config, scenario).await?;
        } else {
            run_config(config, &scenarios).await?;
        }
        return Ok(());
    }

    println!("# kv benchmark");
    println!("# runtime service only, without external HTTP overhead.");
    println!(
        "# config: requests={} concurrency={} max_inflight={}",
        requests, concurrency, max_inflight
    );
    println!(
        "# write-config: requests={} concurrency={}",
        write_requests, write_concurrency
    );
    for config in &configs {
        run_config(config, &scenarios).await?;
    }

    Ok(())
}

async fn run_config(config: &BenchConfig, scenarios: &[Scenario]) -> Result<(), String> {
    println!("== {} ==", config.name);
    let mut results = HashMap::new();
    for scenario in scenarios {
        let result = run_config_scenario(config, scenario).await?;
        results.insert(scenario.label, result);
    }
    if let (Some(sequential), Some(queued)) =
        (results.get("kv-read10"), results.get("kv-readqueue10"))
    {
        println!(
            "batching gain  kv-readqueue10/kv-read10 = {:.2}x",
            queued.throughput_rps / sequential.throughput_rps
        );
    }
    println!();
    Ok(())
}

async fn run_config_scenario(
    config: &BenchConfig,
    scenario: &Scenario,
) -> Result<ScenarioResult, String> {
    let label = config.name;
    let runtime = config.runtime.clone();
    let root = PathBuf::from(format!("/tmp/dd-bench-kv-{label}-{}", Uuid::new_v4()));
    let store_dir = root.join("store");
    tokio::fs::create_dir_all(&store_dir)
        .await
        .map_err(|error| error.to_string())?;
    let service = RuntimeService::start_with_service_config(RuntimeServiceConfig {
        runtime,
        storage: RuntimeStorageConfig {
            store_dir: store_dir.clone(),
            database_url: format!("file:{}", root.join("dd-kv.db").display()),
            actor_db_cache_max_open: 4096,
            actor_db_idle_ttl: Duration::from_secs(60),
            worker_store_enabled: true,
            blob_store: BlobStoreConfig::local(store_dir.join("blobs")),
        },
    })
    .await
    .map_err(|error| error.to_string())?;

    let worker_name = format!("bench-kv-{}", Uuid::new_v4());
    service
        .deploy_with_config(
            worker_name.clone(),
            scenario.worker_source.to_string(),
            DeployConfig {
                public: false,
                bindings: if scenario.use_kv_binding {
                    vec![DeployBinding::Kv {
                        binding: "MY_KV".to_string(),
                    }]
                } else {
                    Vec::new()
                },
                ..DeployConfig::default()
            },
        )
        .await
        .map_err(|error| error.to_string())?;
    if scenario.use_kv_binding {
        service
            .invoke(worker_name.clone(), invocation("/seed", 0))
            .await
            .map_err(|error| error.to_string())?;
        if config.runtime.kv_profile_enabled {
            service
                .invoke(worker_name.clone(), invocation("/__profile_reset", 0))
                .await
                .map_err(|error| error.to_string())?;
        }
    }

    let result = run_scenario(&service, &worker_name, *scenario)
        .await
        .map_err(|error| error.to_string())?;
    println!(
        "{:<12} requests={} concurrency={} total={:.2}ms throughput={:.0} req/s mean={:.2}ms p50={:.2}ms p95={:.2}ms p99={:.2}ms",
        scenario.label,
        scenario.requests,
        scenario.concurrency,
        result.total_ms,
        result.throughput_rps,
        result.mean_ms,
        result.p50_ms,
        result.p95_ms,
        result.p99_ms,
    );
    if scenario.use_kv_binding && config.runtime.kv_profile_enabled {
        let profile = take_profile(&service, &worker_name).await?;
        print_profile(&profile);
    }
    Ok(result)
}

async fn take_profile(
    service: &RuntimeService,
    worker_name: &str,
) -> Result<KvProfileSnapshot, String> {
    let response = service
        .invoke(worker_name.to_string(), invocation("/__profile", 0))
        .await
        .map_err(|error| error.to_string())?;
    let body = String::from_utf8(response.body).map_err(|error| error.to_string())?;
    let profile: KvProfileEnvelope =
        serde_json::from_str(&body).map_err(|error| error.to_string())?;
    if !profile.ok {
        return Err(if profile.error.is_empty() {
            "kv profile collection failed".to_string()
        } else {
            profile.error
        });
    }
    Ok(profile.snapshot.unwrap_or_default())
}

fn print_profile(profile: &KvProfileSnapshot) {
    if !profile.enabled {
        return;
    }
    println!(
        "profile       js_request={:.2}ms js_batch={:.2}ms (keys {:.1}) op_get={:.2}ms op_many={:.2}ms op_value={:.2}ms store_get={:.2}ms store_many={:.2}ms store_value={:.2}ms max_batch={:.2}ms",
        metric_mean_ms(&profile.js_request_total),
        metric_mean_ms(&profile.js_batch_flush),
        metric_mean_items(&profile.js_batch_flush),
        metric_mean_ms(&profile.op_get),
        metric_mean_ms(&profile.op_get_many_utf8),
        metric_mean_ms(&profile.op_get_value),
        metric_mean_ms(&profile.store_get_utf8),
        metric_mean_ms(&profile.store_get_utf8_many),
        metric_mean_ms(&profile.store_get_value),
        profile.js_batch_flush.max_us as f64 / 1000.0,
    );
    println!(
        "profile-write enqueue={:.2}ms superseded={:.1} rejected={:.1} flush={:.2}ms retries={:.1} wait={:.2}ms batch={:.1}",
        metric_mean_ms(&profile.write_enqueue),
        metric_mean_items(&profile.write_superseded),
        metric_mean_items(&profile.write_rejected),
        metric_mean_ms(&profile.write_flush),
        metric_mean_items(&profile.write_retry),
        metric_mean_ms(&profile.write_queue_wait),
        metric_mean_items(&profile.write_flush),
    );
    println!(
        "profile-cache hit={} miss={} stale={} fill={} invalidate={} hit_ratio={:.1}%",
        profile.js_cache_hit.calls,
        profile.js_cache_miss.calls,
        profile.js_cache_stale.calls,
        profile.js_cache_fill.calls,
        profile.js_cache_invalidate.calls,
        cache_hit_ratio(profile),
    );
}

fn env_name(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn env_bool(name: &str, default: bool) -> bool {
    env::var(name)
        .ok()
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(default)
}

fn metric_mean_ms(metric: &KvProfileMetric) -> f64 {
    if metric.calls == 0 {
        return 0.0;
    }
    (metric.total_us as f64 / metric.calls as f64) / 1000.0
}

fn metric_mean_items(metric: &KvProfileMetric) -> f64 {
    if metric.calls == 0 {
        return 0.0;
    }
    metric.total_items as f64 / metric.calls as f64
}

fn cache_hit_ratio(profile: &KvProfileSnapshot) -> f64 {
    let hits = profile.js_cache_hit.calls as f64;
    let misses = profile.js_cache_miss.calls as f64;
    let stale = profile.js_cache_stale.calls as f64;
    let total = hits + misses + stale;
    if total == 0.0 {
        return 0.0;
    }
    (hits / total) * 100.0
}

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

async fn run_scenario(
    service: &RuntimeService,
    worker_name: &str,
    scenario: Scenario,
) -> common::Result<ScenarioResult> {
    let next = Arc::new(AtomicUsize::new(0));
    let latencies = Arc::new(Mutex::new(Vec::with_capacity(scenario.requests)));
    let started_at = Instant::now();
    let mut tasks = Vec::with_capacity(scenario.concurrency);

    for _ in 0..scenario.concurrency {
        let service = service.clone();
        let worker_name = worker_name.to_string();
        let next = Arc::clone(&next);
        let latencies = Arc::clone(&latencies);
        tasks.push(tokio::spawn(async move {
            loop {
                let idx = next.fetch_add(1, Ordering::Relaxed);
                if idx >= scenario.requests {
                    break;
                }
                let invoke_started = Instant::now();
                service
                    .invoke(worker_name.clone(), invocation(scenario.path, idx))
                    .await?;
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
    let mut latencies = latencies.lock().await.clone();
    latencies.sort_unstable();
    let millis = latencies
        .iter()
        .map(|duration| duration.as_secs_f64() * 1000.0)
        .collect::<Vec<_>>();
    Ok(ScenarioResult {
        throughput_rps: scenario.requests as f64 / total_duration.as_secs_f64(),
        mean_ms: millis.iter().sum::<f64>() / millis.len().max(1) as f64,
        p50_ms: percentile(&millis, 0.50),
        p95_ms: percentile(&millis, 0.95),
        p99_ms: percentile(&millis, 0.99),
        total_ms: total_duration.as_secs_f64() * 1000.0,
    })
}

fn percentile(values: &[f64], p: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let idx = ((values.len() - 1) as f64 * p).round() as usize;
    values[idx.min(values.len() - 1)]
}

fn invocation(path: &str, idx: usize) -> WorkerInvocation {
    WorkerInvocation {
        method: "GET".to_string(),
        url: format!("http://worker{path}"),
        headers: Vec::new(),
        body: Vec::new(),
        request_id: format!("bench-kv-{idx}"),
    }
}
