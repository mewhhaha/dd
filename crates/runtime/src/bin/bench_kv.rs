use common::{DeployBinding, DeployConfig, WorkerInvocation};
use runtime::{
    BlobStoreConfig, RuntimeConfig, RuntimeService, RuntimeServiceConfig, RuntimeStorageConfig,
};
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
}

struct ScenarioResult {
    throughput_rps: f64,
    mean_ms: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    total_ms: f64,
}

const KV_WORKER_SOURCE: &str = r#"
export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/seed") {
      await env.MY_KV.set("hot", "1");
      return new Response("ok");
    }
    if (url.pathname === "/read") {
      return new Response(String((await env.MY_KV.get("hot")) ?? "0"));
    }
    if (url.pathname === "/write") {
      await env.MY_KV.set("hot", "1");
      return new Response("ok");
    }
    if (url.pathname === "/readwrite") {
      const current = Number((await env.MY_KV.get("hot")) ?? "0") || 0;
      const next = current + 1;
      await env.MY_KV.set("hot", String(next));
      return new Response(String(next));
    }
    return new Response("not found", { status: 404 });
  },
};
"#;

#[tokio::main]
async fn main() -> Result<(), String> {
    let configs = [
        (
            "single-isolate",
            RuntimeConfig {
                min_isolates: 1,
                max_isolates: 1,
                max_inflight_per_isolate: 1,
                idle_ttl: Duration::from_secs(60),
                scale_tick: Duration::from_secs(1),
                queue_warn_thresholds: vec![10, 100, 1000],
                ..RuntimeConfig::default()
            },
        ),
        (
            "autoscaling-8",
            RuntimeConfig {
                min_isolates: 0,
                max_isolates: 8,
                max_inflight_per_isolate: 4,
                idle_ttl: Duration::from_secs(30),
                scale_tick: Duration::from_secs(1),
                queue_warn_thresholds: vec![10, 100, 1000],
                ..RuntimeConfig::default()
            },
        ),
    ];

    let scenarios = [
        Scenario {
            label: "kv-read",
            requests: 200,
            concurrency: 32,
            path: "/read",
        },
        Scenario {
            label: "kv-write",
            requests: 200,
            concurrency: 32,
            path: "/write",
        },
        Scenario {
            label: "kv-readwrite",
            requests: 200,
            concurrency: 32,
            path: "/readwrite",
        },
    ];

    println!("# kv benchmark");
    println!("# runtime service only, without external HTTP overhead.");
    for (label, runtime) in configs {
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
                actor_shards_per_namespace: 64,
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
                KV_WORKER_SOURCE.to_string(),
                DeployConfig {
                    public: false,
                    bindings: vec![DeployBinding::Kv {
                        binding: "MY_KV".to_string(),
                    }],
                    ..DeployConfig::default()
                },
            )
            .await
            .map_err(|error| error.to_string())?;
        service
            .invoke(worker_name.clone(), invocation("/seed", 0))
            .await
            .map_err(|error| error.to_string())?;

        println!("== {} ==", label);
        for scenario in scenarios {
            let result = run_scenario(&service, &worker_name, scenario)
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
        }
        println!();
    }

    Ok(())
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
