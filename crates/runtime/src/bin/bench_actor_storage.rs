use common::{DeployBinding, DeployConfig, WorkerInvocation};
use runtime::{RuntimeConfig, RuntimeService, RuntimeServiceConfig, RuntimeStorageConfig};
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

const ACTOR_READ_ASYNC_STORAGE_WORKER_SOURCE: &str = r#"
export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const id = env.BENCH_ACTOR.idFromName("hot");
    const actor = env.BENCH_ACTOR.get(id);
    if (url.pathname === "/seed") {
      await actor.seed();
      return new Response("ok");
    }
    const value = await actor.read();
    return new Response(String(value));
  },
};

export class BenchActor {
  constructor(state) {
    this.state = state;
  }

  async seed() {
    await this.state.storage.put("payload", "1");
    return true;
  }

  async read() {
    const current = await this.state.storage.get("payload");
    return current ? String(current.value) : "0";
  }
}
"#;

const ACTOR_READ_ASYNC_MEMORY_WORKER_SOURCE: &str = r#"
export default {
  async fetch(_request, env) {
    const id = env.BENCH_ACTOR.idFromName("hot");
    const actor = env.BENCH_ACTOR.get(id);
    const value = await actor.read();
    return new Response(String(value));
  },
};

export class BenchActor {
  constructor(state) {
    this.state = state;
    this.cached = "1";
  }

  async read() {
    return this.cached;
  }
}
"#;

const ACTOR_READ_SYNC_MEMORY_WORKER_SOURCE: &str = r#"
export default {
  async fetch(_request, env) {
    const id = env.BENCH_ACTOR.idFromName("hot");
    const actor = env.BENCH_ACTOR.get(id);
    return new Response(String(actor.read()));
  },
};

export class BenchActor {
  constructor(state) {
    this.state = state;
    this.cached = "1";
  }

  read() {
    return this.cached;
  }
}
"#;

#[tokio::main]
async fn main() -> Result<(), String> {
    let runtime = RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 1,
        idle_ttl: Duration::from_secs(30),
        scale_tick: Duration::from_secs(1),
        queue_warn_thresholds: vec![10, 100, 1000],
        ..RuntimeConfig::default()
    };
    let service = start_service("actor-storage", runtime)
        .await
        .map_err(|error| error.to_string())?;

    println!("# actor storage benchmark (single-isolate)");
    println!("# compares async host storage read vs async/sync in-memory reads in actor methods.");

    run_and_print(
        &service,
        "actor-read-async-storage",
        ACTOR_READ_ASYNC_STORAGE_WORKER_SOURCE,
        true,
    )
    .await?;
    run_and_print(
        &service,
        "actor-read-async-memory",
        ACTOR_READ_ASYNC_MEMORY_WORKER_SOURCE,
        false,
    )
    .await?;
    run_and_print(
        &service,
        "actor-read-sync-memory",
        ACTOR_READ_SYNC_MEMORY_WORKER_SOURCE,
        false,
    )
    .await?;

    Ok(())
}

async fn run_and_print(
    service: &RuntimeService,
    label: &str,
    source: &str,
    seed: bool,
) -> Result<(), String> {
    let worker_name = format!("{label}-{}", Uuid::new_v4());
    service
        .deploy_with_config(
            worker_name.clone(),
            source.to_string(),
            DeployConfig {
                public: false,
                bindings: vec![DeployBinding::Actor {
                    binding: "BENCH_ACTOR".to_string(),
                    class: "BenchActor".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .map_err(|error| error.to_string())?;

    if seed {
        service
            .invoke(worker_name.clone(), invocation("/seed", 0))
            .await
            .map_err(|error| error.to_string())?;
    }

    let result = run_scenario(
        service,
        &worker_name,
        Scenario {
            requests: 1000,
            concurrency: 1,
            path: "/read",
        },
    )
    .await
    .map_err(|error| error.to_string())?;
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
    Ok(())
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
                service
                    .invoke(worker_name.clone(), invocation(&path, idx + 1))
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

fn invocation(path: &str, idx: usize) -> WorkerInvocation {
    WorkerInvocation {
        method: "GET".to_string(),
        url: format!("http://worker{path}"),
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
            actor_shards_per_namespace: 64,
            worker_store_enabled: true,
            blob_store: runtime::BlobStoreConfig::local(store_dir.join("blobs")),
        },
    }
}
