use common::{DeployBinding, DeployConfig, PlatformError, WorkerInvocation};
use runtime::{RuntimeConfig, RuntimeService, RuntimeServiceConfig, RuntimeStorageConfig};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use uuid::Uuid;

#[derive(Clone)]
struct BenchConfig {
    name: &'static str,
    runtime: RuntimeConfig,
}

#[derive(Clone, Copy)]
struct Scenario {
    name: &'static str,
    worker_source: &'static str,
    path: &'static str,
    use_kv_binding: bool,
    use_actor_binding: bool,
    seed_path: Option<&'static str>,
}

struct ScenarioResult {
    requests: usize,
    concurrency: usize,
    throughput_rps: f64,
    mean_ms: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn env_optional_usize(name: &str) -> Option<usize> {
    std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
}

fn env_v8_flags() -> Vec<String> {
    std::env::var("DD_BENCH_V8_FLAGS")
        .unwrap_or_default()
        .split_whitespace()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn env_config_name() -> Option<String> {
    std::env::var("DD_BENCH_INTERNAL_CONFIG")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn env_scenario_name() -> Option<String> {
    std::env::var("DD_BENCH_INTERNAL_SCENARIO")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn env_custom_config(max_inflight: usize, v8_flags: &[String]) -> Option<BenchConfig> {
    let min_isolates = env_optional_usize("DD_BENCH_MIN_ISOLATES");
    let max_isolates = env_optional_usize("DD_BENCH_MAX_ISOLATES");
    let Some(max_isolates) = max_isolates.or(min_isolates) else {
        return None;
    };
    let min_isolates = min_isolates.unwrap_or(max_isolates).min(max_isolates);
    let name = std::env::var("DD_BENCH_CUSTOM_NAME")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| format!("custom-{}-{}", min_isolates, max_isolates));
    let leaked_name: &'static str = Box::leak(name.into_boxed_str());
    Some(BenchConfig {
        name: leaked_name,
        runtime: RuntimeConfig {
            min_isolates,
            max_isolates,
            max_inflight_per_isolate: max_inflight,
            idle_ttl: Duration::from_secs(30),
            scale_tick: Duration::from_secs(1),
            v8_flags: v8_flags.to_vec(),
            ..RuntimeConfig::default()
        },
    })
}

#[tokio::main]
async fn main() -> Result<(), String> {
    let requests = env_usize("DD_BENCH_REQUESTS", 4_000);
    let concurrency = env_usize("DD_BENCH_CONCURRENCY", 128);
    let max_inflight = env_usize("DD_BENCH_MAX_INFLIGHT", 16);
    let autoscaling_isolates = env_usize("DD_BENCH_AUTOSCALING_ISOLATES", 8);
    let prewarmed_isolates = env_usize("DD_BENCH_PREWARMED_ISOLATES", autoscaling_isolates);
    let v8_flags = env_v8_flags();
    let autoscaling_name =
        Box::leak(format!("autoscaling-{autoscaling_isolates}").into_boxed_str());
    let prewarmed_name = Box::leak(format!("prewarmed-{prewarmed_isolates}").into_boxed_str());

    let mut configs = vec![
        BenchConfig {
            name: "single-isolate",
            runtime: RuntimeConfig {
                min_isolates: 1,
                max_isolates: 1,
                max_inflight_per_isolate: max_inflight,
                idle_ttl: Duration::from_secs(60),
                scale_tick: Duration::from_secs(1),
                v8_flags: v8_flags.clone(),
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
                v8_flags: v8_flags.clone(),
                ..RuntimeConfig::default()
            },
        },
        BenchConfig {
            name: prewarmed_name,
            runtime: RuntimeConfig {
                min_isolates: prewarmed_isolates,
                max_isolates: prewarmed_isolates,
                max_inflight_per_isolate: max_inflight,
                idle_ttl: Duration::from_secs(30),
                scale_tick: Duration::from_secs(1),
                v8_flags: v8_flags.clone(),
                ..RuntimeConfig::default()
            },
        },
    ];
    if let Some(custom) = env_custom_config(max_inflight, &v8_flags) {
        configs.push(custom);
    }

    let scenarios = [
        Scenario {
            name: "instant-text",
            worker_source: r#"
export default {
  fetch() {
    return new Response("ok");
  },
};
"#,
            path: "/",
            use_kv_binding: false,
            use_actor_binding: false,
            seed_path: None,
        },
        Scenario {
            name: "instant-json",
            worker_source: r#"
export default {
  fetch() {
    return Response.json({ ok: true, now: 1 });
  },
};
"#,
            path: "/",
            use_kv_binding: false,
            use_actor_binding: false,
            seed_path: None,
        },
        Scenario {
            name: "instant-html",
            worker_source: r#"
export default {
  fetch() {
    return new Response("<!doctype html><html><body>ok</body></html>", {
      headers: [["content-type", "text/html"]],
    });
  },
};
"#,
            path: "/",
            use_kv_binding: false,
            use_actor_binding: false,
            seed_path: None,
        },
        Scenario {
            name: "instant-text-actor-bound",
            worker_source: r#"
export default {
  fetch() {
    return new Response("ok");
  },
};
"#,
            path: "/",
            use_kv_binding: false,
            use_actor_binding: true,
            seed_path: None,
        },
        Scenario {
            name: "instant-text-kv-bound",
            worker_source: r#"
export default {
  fetch() {
    return new Response("ok");
  },
};
"#,
            path: "/",
            use_kv_binding: true,
            use_actor_binding: false,
            seed_path: None,
        },
        Scenario {
            name: "instant-text-kv-read",
            worker_source: r#"
export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/seed") {
      await env.MY_KV.put("hot", "ok");
      return new Response("seeded");
    }
    return new Response(String((await env.MY_KV.get("hot")) ?? "missing"));
  },
};
"#,
            path: "/",
            use_kv_binding: true,
            use_actor_binding: false,
            seed_path: Some("/seed"),
        },
        Scenario {
            name: "instant-text-kv-write",
            worker_source: r#"
export default {
  async fetch(request, env) {
    const key = request.headers.get("x-bench-request") ?? "0";
    await env.MY_KV.put("hot:" + key, "ok");
    return new Response("ok");
  },
};
"#,
            path: "/",
            use_kv_binding: true,
            use_actor_binding: false,
            seed_path: None,
        },
        Scenario {
            name: "instant-text-kv-write-unawaited",
            worker_source: r#"
export default {
  fetch(request, env) {
    const key = request.headers.get("x-bench-request") ?? "0";
    env.MY_KV.put("hot:" + key, "ok");
    return new Response("ok");
  },
};
"#,
            path: "/",
            use_kv_binding: true,
            use_actor_binding: false,
            seed_path: None,
        },
    ];

    if let Some(config_name) = env_config_name() {
        let config = configs
            .iter()
            .find(|config| config.name == config_name)
            .ok_or_else(|| format!("unknown fast fetch config: {config_name}"))?;
        if let Some(scenario_name) = env_scenario_name() {
            let scenario = scenarios
                .iter()
                .find(|scenario| scenario.name == scenario_name)
                .ok_or_else(|| format!("unknown fast fetch scenario: {scenario_name}"))?;
            run_config_scenario(config, scenario, requests, concurrency).await?;
        } else {
            run_config(config, &scenarios, requests, concurrency).await?;
        }
        return Ok(());
    }

    println!("# dd fast fetch benchmark");
    println!(
        "# note: this is the minimal worker fetch path through RuntimeService (no external HTTP network overhead)."
    );
    println!(
        "# config: requests={} concurrency={} max_inflight={} v8_flags={:?}",
        requests, concurrency, max_inflight, v8_flags
    );
    println!();

    let current_exe = std::env::current_exe().map_err(|error| error.to_string())?;
    for config in configs {
        let status = Command::new(&current_exe)
            .env("DD_BENCH_INTERNAL_CONFIG", config.name)
            .env("DD_BENCH_REQUESTS", requests.to_string())
            .env("DD_BENCH_CONCURRENCY", concurrency.to_string())
            .env("DD_BENCH_MAX_INFLIGHT", max_inflight.to_string())
            .env("DD_BENCH_V8_FLAGS", v8_flags.join(" "))
            .stdin(Stdio::null())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .status()
            .map_err(|error| error.to_string())?;
        if !status.success() {
            return Err(format!("fast fetch bench child failed for {}", config.name));
        }
    }

    Ok(())
}

async fn run_config(
    config: &BenchConfig,
    scenarios: &[Scenario],
    requests: usize,
    concurrency: usize,
) -> Result<(), String> {
    println!("== fetch-fast: {} ==", config.name);
    for scenario in scenarios {
        run_config_scenario_child(config, scenario, requests, concurrency)?;
    }
    println!();
    Ok(())
}

fn run_config_scenario_child(
    config: &BenchConfig,
    scenario: &Scenario,
    requests: usize,
    concurrency: usize,
) -> Result<(), String> {
    let current_exe = std::env::current_exe().map_err(|error| error.to_string())?;
    let status = Command::new(&current_exe)
        .env("DD_BENCH_INTERNAL_CONFIG", config.name)
        .env("DD_BENCH_INTERNAL_SCENARIO", scenario.name)
        .env("DD_BENCH_REQUESTS", requests.to_string())
        .env("DD_BENCH_CONCURRENCY", concurrency.to_string())
        .env(
            "DD_BENCH_MAX_INFLIGHT",
            config.runtime.max_inflight_per_isolate.to_string(),
        )
        .env("DD_BENCH_V8_FLAGS", config.runtime.v8_flags.join(" "))
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .map_err(|error| error.to_string())?;
    if !status.success() {
        return Err(format!(
            "fast fetch bench child failed for {} / {}",
            config.name, scenario.name
        ));
    }
    Ok(())
}

async fn run_config_scenario(
    config: &BenchConfig,
    scenario: &Scenario,
    requests: usize,
    concurrency: usize,
) -> Result<(), String> {
    let service = start_service(
        &format!("{}-{}", config.name, scenario.name),
        config.runtime.clone(),
    )
    .await
    .map_err(|error| error.to_string())?;
    let worker_name = format!("{}-{}", config.name, scenario.name);
    service
        .deploy_with_config(
            worker_name.clone(),
            scenario.worker_source.to_string(),
            DeployConfig {
                bindings: if scenario.use_kv_binding {
                    vec![DeployBinding::Kv {
                        binding: "MY_KV".to_string(),
                    }]
                } else if scenario.use_actor_binding {
                    vec![DeployBinding::Actor {
                        binding: "BENCH_ACTOR".to_string(),
                    }]
                } else {
                    Vec::new()
                },
                ..DeployConfig::default()
            },
        )
        .await
        .map_err(|error| error.to_string())?;
    if let Some(seed_path) = scenario.seed_path {
        service
            .invoke(worker_name.clone(), invocation(seed_path, usize::MAX))
            .await
            .map_err(|error| error.to_string())?;
    }
    let result = run_scenario(&service, &worker_name, *scenario, requests, concurrency)
        .await
        .map_err(|error| error.to_string())?;
    println!("{}", format_result(scenario.name, result));
    Ok(())
}

async fn start_service(tag: &str, runtime: RuntimeConfig) -> common::Result<RuntimeService> {
    let paths = bench_paths(tag);
    tokio::fs::create_dir_all(&paths.store_dir)
        .await
        .map_err(|error| PlatformError::internal(error.to_string()))?;
    RuntimeService::start_with_service_config(RuntimeServiceConfig {
        runtime,
        storage: RuntimeStorageConfig {
            store_dir: paths.store_dir.clone(),
            database_url: format!("file:{}", paths.db_path.display()),
            actor_namespace_shards: 16,
            actor_db_cache_max_open: 4096,
            actor_db_idle_ttl: Duration::from_secs(60),
            worker_store_enabled: true,
            blob_store: runtime::BlobStoreConfig::local(paths.store_dir.join("blobs")),
        },
    })
    .await
}

struct BenchPaths {
    db_path: PathBuf,
    store_dir: PathBuf,
}

fn bench_paths(tag: &str) -> BenchPaths {
    let root = PathBuf::from(format!("/tmp/dd-fast-fetch-{tag}-{}", Uuid::new_v4()));
    BenchPaths {
        db_path: root.join("dd-kv.db"),
        store_dir: root.join("store"),
    }
}

async fn run_scenario(
    service: &RuntimeService,
    worker_name: &str,
    scenario: Scenario,
    requests: usize,
    concurrency: usize,
) -> common::Result<ScenarioResult> {
    let next = Arc::new(AtomicUsize::new(0));
    let latencies = Arc::new(Mutex::new(Vec::with_capacity(requests)));
    let started_at = Instant::now();
    let mut tasks = Vec::with_capacity(concurrency);

    for _ in 0..concurrency {
        let service = service.clone();
        let worker_name = worker_name.to_string();
        let next = Arc::clone(&next);
        let latencies = Arc::clone(&latencies);
        tasks.push(tokio::spawn(async move {
            loop {
                let idx = next.fetch_add(1, Ordering::Relaxed);
                if idx >= requests {
                    break;
                }
                let invoke_started = Instant::now();
                service
                    .invoke(worker_name.clone(), invocation(scenario.path, idx))
                    .await?;
                latencies.lock().await.push(invoke_started.elapsed());
            }

            Ok::<(), PlatformError>(())
        }));
    }

    for task in tasks {
        task.await
            .map_err(|error| PlatformError::internal(error.to_string()))??;
    }

    let total_duration = started_at.elapsed();
    let latencies = take_sorted_latencies(latencies).await?;
    Ok(to_scenario_result(
        requests,
        concurrency,
        total_duration,
        &latencies,
    ))
}

fn invocation(path: &str, idx: usize) -> WorkerInvocation {
    WorkerInvocation {
        method: "GET".to_string(),
        url: format!("https://bench.example{path}"),
        headers: vec![
            ("accept".to_string(), "*/*".to_string()),
            ("x-bench-request".to_string(), idx.to_string()),
        ],
        body: Vec::new(),
        request_id: format!("bench-{idx}"),
    }
}

async fn take_sorted_latencies(
    latencies: Arc<Mutex<Vec<Duration>>>,
) -> common::Result<Vec<Duration>> {
    let mut latencies = latencies.lock().await.clone();
    latencies.sort_unstable();
    Ok(latencies)
}

fn to_scenario_result(
    requests: usize,
    concurrency: usize,
    total_duration: Duration,
    latencies: &[Duration],
) -> ScenarioResult {
    let throughput_rps = requests as f64 / total_duration.as_secs_f64();
    let mean_ms = latencies
        .iter()
        .map(|value| value.as_secs_f64() * 1_000.0)
        .sum::<f64>()
        / requests as f64;
    let p50_ms = percentile_ms(latencies, 0.50);
    let p95_ms = percentile_ms(latencies, 0.95);
    let p99_ms = percentile_ms(latencies, 0.99);
    ScenarioResult {
        requests,
        concurrency,
        throughput_rps,
        mean_ms,
        p50_ms,
        p95_ms,
        p99_ms,
    }
}

fn percentile_ms(values: &[Duration], percentile: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let idx = ((values.len() - 1) as f64 * percentile).round() as usize;
    values[idx].as_secs_f64() * 1_000.0
}

fn format_result(name: &str, result: ScenarioResult) -> String {
    format!(
        "{:<16} requests={} concurrency={} throughput={:.0} req/s mean={:.2}ms p50={:.2}ms p95={:.2}ms p99={:.2}ms",
        name,
        result.requests,
        result.concurrency,
        result.throughput_rps,
        result.mean_ms,
        result.p50_ms,
        result.p95_ms,
        result.p99_ms,
    )
}
