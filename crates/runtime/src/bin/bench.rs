use common::{DeployBinding, DeployConfig, WorkerInvocation};
use runtime::{RuntimeConfig, RuntimeService, RuntimeServiceConfig, RuntimeStorageConfig};
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::{Duration, Instant};
use tokio::time::sleep;
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
    requests: usize,
    concurrency: usize,
    paths: &'static [&'static str],
}

struct ScenarioResult {
    total_requests: usize,
    concurrency: usize,
    total_duration: Duration,
    throughput_rps: f64,
    mean_ms: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
}

struct ColdStartResult {
    rounds: usize,
    deploy: Distribution,
    first_invoke: Distribution,
}

struct RestoreStartResult {
    rounds: usize,
    startup: Distribution,
    first_invoke: Distribution,
}

struct ScaleUpResult {
    requests: usize,
    concurrency: usize,
    target_isolates: usize,
    time_to_target: Option<Duration>,
    burst: ScenarioResult,
}

struct WebSocketBenchResult {
    sessions: usize,
    messages_per_session: usize,
    open: Distribution,
    roundtrip: ScenarioResult,
}

struct Distribution {
    mean_ms: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    max_ms: f64,
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn section_enabled(name: &str) -> bool {
    let raw = std::env::var("DD_BENCH_ONLY").unwrap_or_default();
    if raw.trim().is_empty() {
        return true;
    }
    raw.split(',')
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .any(|value| value.eq_ignore_ascii_case(name))
}

#[tokio::main]
async fn main() -> Result<(), String> {
    let configs = [
        BenchConfig {
            name: "single-isolate",
            runtime: RuntimeConfig {
                min_isolates: 1,
                max_isolates: 1,
                max_inflight_per_isolate: 1,
                idle_ttl: Duration::from_secs(60),
                scale_tick: Duration::from_secs(1),
                queue_warn_thresholds: vec![10, 100, 1000],
                ..RuntimeConfig::default()
            },
        },
        BenchConfig {
            name: "autoscaling-8",
            runtime: RuntimeConfig {
                min_isolates: 0,
                max_isolates: 8,
                max_inflight_per_isolate: 4,
                idle_ttl: Duration::from_secs(30),
                scale_tick: Duration::from_secs(1),
                queue_warn_thresholds: vec![10, 100, 1000],
                ..RuntimeConfig::default()
            },
        },
    ];

    let scenarios = [
        Scenario {
            name: "instant-response",
            worker_source: r#"
export default {
  async fetch() {
    return new Response("ok");
  },
};
"#,
            requests: 500,
            concurrency: 64,
            paths: &["/"],
        },
        Scenario {
            name: "cpu-5ms",
            worker_source: r#"
export default {
  async fetch() {
    let acc = 0;
    for (let i = 0; i < 2_000_000; i += 1) {
      acc ^= i;
    }
    if (acc === -1) {
      return new Response("never");
    }
    return new Response("ok");
  },
};
"#,
            requests: 400,
            concurrency: 64,
            paths: &["/"],
        },
        Scenario {
            name: "host-sleep-5ms",
            worker_source: r#"
export default {
  async fetch() {
    await Deno.core.ops.op_sleep(5);
    return new Response("ok");
  },
};
"#,
            requests: 400,
            concurrency: 64,
            paths: &["/"],
        },
        Scenario {
            name: "page-load-mix",
            worker_source: r#"
export default {
  async fetch(request) {
    const { pathname } = new URL(request.url);
    if (pathname === "/") {
      let acc = 0;
      for (let i = 0; i < 500_000; i += 1) {
        acc ^= i;
      }
      if (acc === -1) {
        return new Response("never");
      }
      return new Response("<html>ok</html>", {
        headers: [["content-type", "text/html"]],
      });
    }

    if (pathname.startsWith("/assets/")) {
      return new Response("asset", {
        headers: [
          ["content-type", "text/plain"],
          ["cache-control", "public, max-age=300"],
        ],
      });
    }

    if (pathname.startsWith("/api/")) {
      await Deno.core.ops.op_sleep(8);
      return new Response("{\"ok\":true}", {
        headers: [["content-type", "application/json"]],
      });
    }

    return new Response("not found", { status: 404 });
  },
};
"#,
            requests: 700,
            concurrency: 96,
            paths: &[
                "/",
                "/assets/app.css",
                "/assets/app.js",
                "/assets/logo.svg",
                "/api/nav",
                "/api/feed",
                "/api/notifications",
            ],
        },
    ];

    println!("# dd runtime benchmark");
    println!(
        "# note: this benchmark measures runtime service behavior directly (no API/HTTP network overhead)."
    );
    println!("# note: output includes mean, p50, p95, p99 for each benchmark.");
    println!();

    for config in configs {
        if section_enabled("steady-state") {
            println!("== steady-state: {} ==", config.name);
            let service = start_service(config.name, config.runtime.clone())
                .await
                .map_err(|error| error.to_string())?;
            for scenario in scenarios {
                let worker_name = format!("{}-{}", config.name, scenario.name);
                service
                    .deploy(worker_name.clone(), scenario.worker_source.to_string())
                    .await
                    .map_err(|error| error.to_string())?;
                let result = run_scenario(&service, &worker_name, scenario)
                    .await
                    .map_err(|error| error.to_string())?;
                println!("{}", format_scenario_result(scenario.name, result));
            }
            println!();
        }

        if section_enabled("websocket") {
            println!("== websocket: {} ==", config.name);
            let websocket_tag = format!("{}-websocket", config.name);
            let websocket_service = start_service(&websocket_tag, config.runtime.clone())
                .await
                .map_err(|error| error.to_string())?;
            let websocket = run_websocket_bench(&websocket_service)
                .await
                .map_err(|error| error.to_string())?;
            println!("{}", format_websocket_open_result(&websocket));
            println!("{}", format_websocket_roundtrip_result(&websocket));
            println!();
        }

        if config.name == "autoscaling-8" && section_enabled("lifecycle") {
            println!("== lifecycle: {} ==", config.name);

            let cold_service = start_service("cold-start", config.runtime.clone())
                .await
                .map_err(|error| error.to_string())?;
            let cold = run_cold_start(&cold_service, 40)
                .await
                .map_err(|error| error.to_string())?;
            println!("{}", format_cold_start_result(cold));

            let hot_service = start_service("hot-start", config.runtime.clone())
                .await
                .map_err(|error| error.to_string())?;
            let hot = run_hot_start(&hot_service, 500)
                .await
                .map_err(|error| error.to_string())?;
            println!("{}", format_distribution_result("hot-start", 500, hot));

            let restore = run_restore_from_disk(&config.runtime, 30)
                .await
                .map_err(|error| error.to_string())?;
            println!("{}", format_restore_start_result(restore));

            let scale_service = start_service("scale-up", config.runtime.clone())
                .await
                .map_err(|error| error.to_string())?;
            let scale = run_scale_up(&scale_service, &config.runtime, 1000, 192)
                .await
                .map_err(|error| error.to_string())?;
            println!("{}", format_scale_up_result(scale));
            println!();

            if section_enabled("kv-writes") {
                println!("== kv-writes: {} ==", config.name);
                let kv_service = start_service("kv-writes", config.runtime.clone())
                    .await
                    .map_err(|error| error.to_string())?;
                let kv_worker_name = format!("kv-writes-{}", Uuid::new_v4());
                kv_service
                    .deploy_with_config(
                        kv_worker_name.clone(),
                        KV_WRITE_WORKER_SOURCE.to_string(),
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

                let kv_sync = run_scenario(
                    &kv_service,
                    &kv_worker_name,
                    Scenario {
                        name: "kv-write-sync",
                        worker_source: KV_WRITE_WORKER_SOURCE,
                        requests: 1000,
                        concurrency: 1,
                        paths: &["/write"],
                    },
                )
                .await
                .map_err(|error| error.to_string())?;
                println!("{}", format_scenario_result("kv-write-sync", kv_sync));

                let kv_concurrent = run_scenario(
                    &kv_service,
                    &kv_worker_name,
                    Scenario {
                        name: "kv-write-concurrent",
                        worker_source: KV_WRITE_WORKER_SOURCE,
                        requests: 1000,
                        concurrency: 96,
                        paths: &["/write"],
                    },
                )
                .await
                .map_err(|error| error.to_string())?;
                println!(
                    "{}",
                    format_scenario_result("kv-write-concurrent", kv_concurrent)
                );
                println!();
            }
        }
    }

    Ok(())
}

const KV_WRITE_WORKER_SOURCE: &str = r#"
export default {
  async fetch(request, env) {
    await env.MY_KV.put("hot", "1");
    return new Response("ok");
  },
};
"#;

const WEBSOCKET_ECHO_WORKER_SOURCE: &str = r#"
export function openSocket(state, payload) {
  const { response } = state.accept(payload.request);
  return response;
}

export async function onSocketMessage(stub, event) {
  if (typeof event.data === "string") {
    await stub.sockets.send(event.handle, `echo:${event.data}`);
    return;
  }

  await stub.sockets.send(event.handle, event.data, "binary");
}

export default {
  async fetch(request, env) {
    const actor = env.BENCH_ACTOR.get(env.BENCH_ACTOR.idFromName("global"));
    return await actor.atomic(openSocket, { request });
  },

  async wake(event, _env) {
    if (event.type === "socketmessage") {
      await onSocketMessage(event.stub, event);
    }
  },
};
"#;

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

async fn start_service_with_paths(
    runtime: RuntimeConfig,
    db_path: &Path,
    store_dir: &Path,
) -> common::Result<RuntimeService> {
    tokio::fs::create_dir_all(store_dir)
        .await
        .map_err(|error| common::PlatformError::internal(error.to_string()))?;
    RuntimeService::start_with_service_config(runtime_service_config(runtime, db_path, store_dir))
        .await
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

struct BenchPaths {
    root: PathBuf,
    db_path: PathBuf,
    store_dir: PathBuf,
}

fn bench_paths(tag: &str) -> BenchPaths {
    let root = PathBuf::from(format!("/tmp/dd-bench-{tag}-{}", Uuid::new_v4()));
    BenchPaths {
        root: root.clone(),
        db_path: root.join("dd-kv.db"),
        store_dir: root.join("store"),
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
        tasks.push(tokio::spawn(async move {
            loop {
                let idx = next.fetch_add(1, Ordering::Relaxed);
                if idx >= scenario.requests {
                    break;
                }
                let path = scenario.paths[idx % scenario.paths.len()];
                let invoke_started = Instant::now();
                service
                    .invoke(worker_name.clone(), invocation(path, idx))
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
    let latencies = take_sorted_latencies(latencies)?;
    Ok(to_scenario_result(
        scenario.requests,
        scenario.concurrency,
        total_duration,
        &latencies,
    ))
}

async fn run_cold_start(
    service: &RuntimeService,
    rounds: usize,
) -> common::Result<ColdStartResult> {
    let source = r#"
export default {
  async fetch() {
    return new Response("ok");
  },
};
"#;
    let mut deploy = Vec::with_capacity(rounds);
    let mut first_invoke = Vec::with_capacity(rounds);

    for idx in 0..rounds {
        let worker_name = format!("cold-{idx}-{}", Uuid::new_v4());
        let deploy_started = Instant::now();
        service
            .deploy(worker_name.clone(), source.to_string())
            .await?;
        deploy.push(deploy_started.elapsed());

        let invoke_started = Instant::now();
        service.invoke(worker_name, invocation("/", idx)).await?;
        first_invoke.push(invoke_started.elapsed());
    }

    Ok(ColdStartResult {
        rounds,
        deploy: summarize_distribution(&deploy),
        first_invoke: summarize_distribution(&first_invoke),
    })
}

async fn run_hot_start(service: &RuntimeService, invokes: usize) -> common::Result<Distribution> {
    let worker_name = format!("hot-{}", Uuid::new_v4());
    service
        .deploy(
            worker_name.clone(),
            r#"
export default {
  async fetch() {
    return new Response("ok");
  },
};
"#
            .to_string(),
        )
        .await?;

    service
        .invoke(worker_name.clone(), invocation("/", 0))
        .await?;

    let mut samples = Vec::with_capacity(invokes);
    for idx in 0..invokes {
        let started = Instant::now();
        service
            .invoke(worker_name.clone(), invocation("/", idx + 1))
            .await?;
        samples.push(started.elapsed());
    }

    Ok(summarize_distribution(&samples))
}

async fn run_restore_from_disk(
    runtime: &RuntimeConfig,
    rounds: usize,
) -> common::Result<RestoreStartResult> {
    let source = r#"
export default {
  async fetch() {
    return new Response("ok");
  },
};
"#;
    let mut startup = Vec::with_capacity(rounds);
    let mut first_invoke = Vec::with_capacity(rounds);

    for idx in 0..rounds {
        let paths = bench_paths("restore");
        let worker_name = format!("restore-{idx}-{}", Uuid::new_v4());

        let seed =
            start_service_with_paths(runtime.clone(), &paths.db_path, &paths.store_dir).await?;
        seed.deploy(worker_name.clone(), source.to_string()).await?;
        drop(seed);

        let startup_started = Instant::now();
        let restored =
            start_service_with_paths(runtime.clone(), &paths.db_path, &paths.store_dir).await?;
        startup.push(startup_started.elapsed());

        let invoke_started = Instant::now();
        restored.invoke(worker_name, invocation("/", idx)).await?;
        first_invoke.push(invoke_started.elapsed());
        drop(restored);

        let _ = tokio::fs::remove_dir_all(paths.root).await;
    }

    Ok(RestoreStartResult {
        rounds,
        startup: summarize_distribution(&startup),
        first_invoke: summarize_distribution(&first_invoke),
    })
}

async fn run_scale_up(
    service: &RuntimeService,
    runtime: &RuntimeConfig,
    requests: usize,
    concurrency: usize,
) -> common::Result<ScaleUpResult> {
    let scenario = Scenario {
        name: "scale-up-burst",
        worker_source: r#"
export default {
  async fetch() {
    await Deno.core.ops.op_sleep(15);
    return new Response("ok");
  },
};
"#,
        requests,
        concurrency,
        paths: &["/"],
    };
    let worker_name = format!("scale-up-{}", Uuid::new_v4());
    service
        .deploy(worker_name.clone(), scenario.worker_source.to_string())
        .await?;

    let started = Instant::now();
    let service_for_burst = service.clone();
    let worker_for_burst = worker_name.clone();
    let burst_task =
        tokio::spawn(
            async move { run_scenario(&service_for_burst, &worker_for_burst, scenario).await },
        );

    let target_isolates = runtime.max_isolates;
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut time_to_target = None;
    while Instant::now() < deadline {
        if let Some(stats) = service.stats(worker_name.clone()).await {
            if stats.isolates_total >= target_isolates {
                time_to_target = Some(started.elapsed());
                break;
            }
        }
        sleep(Duration::from_millis(2)).await;
    }

    let burst = burst_task
        .await
        .map_err(|error| common::PlatformError::internal(error.to_string()))??;

    Ok(ScaleUpResult {
        requests,
        concurrency,
        target_isolates,
        time_to_target,
        burst,
    })
}

async fn run_websocket_bench(service: &RuntimeService) -> common::Result<WebSocketBenchResult> {
    let sessions = env_usize("DD_BENCH_WS_SESSIONS", 24);
    let messages_per_session = env_usize("DD_BENCH_WS_MESSAGES_PER_SESSION", 24);
    let worker_name = format!("websocket-{}", Uuid::new_v4());
    service
        .deploy_with_config(
            worker_name.clone(),
            WEBSOCKET_ECHO_WORKER_SOURCE.to_string(),
            DeployConfig {
                public: false,
                bindings: vec![DeployBinding::Actor {
                    binding: "BENCH_ACTOR".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await?;

    let (open, session_ids) = open_websocket_sessions(service, &worker_name, sessions).await?;
    let roundtrip =
        run_websocket_roundtrip(service, &worker_name, &session_ids, messages_per_session).await?;

    close_websocket_sessions(service, &worker_name, &session_ids).await?;

    Ok(WebSocketBenchResult {
        sessions,
        messages_per_session,
        open,
        roundtrip,
    })
}

async fn with_timeout<T>(
    label: &str,
    future: impl std::future::Future<Output = common::Result<T>>,
) -> common::Result<T> {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .map_err(|_| common::PlatformError::runtime(format!("{label} timed out")))?
}

async fn open_websocket_sessions(
    service: &RuntimeService,
    worker_name: &str,
    sessions: usize,
) -> common::Result<(Distribution, Vec<String>)> {
    let concurrency = sessions.min(16).max(1);
    let next = Arc::new(AtomicUsize::new(0));
    let latencies = Arc::new(tokio::sync::Mutex::new(Vec::with_capacity(sessions)));
    let session_ids = Arc::new(tokio::sync::Mutex::new(Vec::with_capacity(sessions)));
    let mut tasks = Vec::with_capacity(concurrency);

    for _ in 0..concurrency {
        let service = service.clone();
        let worker_name = worker_name.to_string();
        let next = Arc::clone(&next);
        let latencies = Arc::clone(&latencies);
        let session_ids = Arc::clone(&session_ids);
        tasks.push(tokio::spawn(async move {
            loop {
                let idx = next.fetch_add(1, Ordering::Relaxed);
                if idx >= sessions {
                    break;
                }
                let started = Instant::now();
                let opened = with_timeout(
                    "websocket open",
                    service.open_websocket(
                        worker_name.clone(),
                        websocket_invocation("/ws", &format!("bench-ws-open-{idx}")),
                        None,
                    ),
                )
                .await?;
                if opened.output.status != 101 {
                    return Err(common::PlatformError::runtime(format!(
                        "websocket benchmark open failed with status {}",
                        opened.output.status
                    )));
                }
                latencies.lock().await.push(started.elapsed());
                session_ids.lock().await.push(opened.session_id);
            }
            Ok::<(), common::PlatformError>(())
        }));
    }

    for task in tasks {
        task.await
            .map_err(|error| common::PlatformError::internal(error.to_string()))??;
    }

    let latencies = take_sorted_latencies(latencies)?;
    let session_ids = Arc::try_unwrap(session_ids)
        .map_err(|_| common::PlatformError::internal("websocket sessions still shared"))?
        .into_inner();
    Ok((summarize_distribution(&latencies), session_ids))
}

async fn run_websocket_roundtrip(
    service: &RuntimeService,
    worker_name: &str,
    session_ids: &[String],
    messages_per_session: usize,
) -> common::Result<ScenarioResult> {
    let total_requests = session_ids.len() * messages_per_session;
    let latencies = Arc::new(tokio::sync::Mutex::new(Vec::with_capacity(total_requests)));
    let started_at = Instant::now();
    let mut tasks = Vec::with_capacity(session_ids.len());

    for (session_index, session_id) in session_ids.iter().cloned().enumerate() {
        let service = service.clone();
        let worker_name = worker_name.to_string();
        let latencies = Arc::clone(&latencies);
        tasks.push(tokio::spawn(async move {
            for message_idx in 0..messages_per_session {
                let payload = format!("hello-{session_index}-{message_idx}");
                let started = Instant::now();
                let sent = with_timeout(
                    "websocket send",
                    service.websocket_send_frame(
                        worker_name.clone(),
                        session_id.clone(),
                        payload.as_bytes().to_vec(),
                        false,
                    ),
                )
                .await?;
                if sent.status != 204 {
                    return Err(common::PlatformError::runtime(format!(
                        "websocket benchmark send failed with status {}",
                        sent.status
                    )));
                }
                let body = String::from_utf8(sent.body)
                    .map_err(|error| common::PlatformError::internal(error.to_string()))?;
                let expected = format!("echo:{payload}");
                if body != expected {
                    return Err(common::PlatformError::runtime(format!(
                        "websocket benchmark expected {}, got {}",
                        expected, body
                    )));
                }
                latencies.lock().await.push(started.elapsed());
            }

            Ok::<(), common::PlatformError>(())
        }));
    }

    for task in tasks {
        task.await
            .map_err(|error| common::PlatformError::internal(error.to_string()))??;
    }

    let total_duration = started_at.elapsed();
    let latencies = take_sorted_latencies(latencies)?;
    Ok(to_scenario_result(
        total_requests,
        session_ids.len(),
        total_duration,
        &latencies,
    ))
}

async fn close_websocket_sessions(
    service: &RuntimeService,
    worker_name: &str,
    session_ids: &[String],
) -> common::Result<()> {
    for session_id in session_ids {
        with_timeout(
            "websocket close",
            service.websocket_close(
                worker_name.to_string(),
                session_id.clone(),
                1000,
                "bench-done".to_string(),
            ),
        )
        .await?;
    }
    Ok(())
}

fn invocation(path: &str, idx: usize) -> WorkerInvocation {
    WorkerInvocation {
        method: "GET".to_string(),
        url: format!("http://worker{path}"),
        headers: Vec::new(),
        body: Vec::new(),
        request_id: format!("bench-{idx}"),
    }
}

fn websocket_invocation(path: &str, request_id: &str) -> WorkerInvocation {
    WorkerInvocation {
        method: "GET".to_string(),
        url: format!("http://worker{path}"),
        headers: vec![
            ("connection".to_string(), "Upgrade".to_string()),
            ("upgrade".to_string(), "websocket".to_string()),
            ("sec-websocket-version".to_string(), "13".to_string()),
            (
                "sec-websocket-key".to_string(),
                "dGhlIHNhbXBsZSBub25jZQ==".to_string(),
            ),
        ],
        body: Vec::new(),
        request_id: request_id.to_string(),
    }
}

fn take_sorted_latencies(
    latencies: Arc<tokio::sync::Mutex<Vec<Duration>>>,
) -> common::Result<Vec<Duration>> {
    let mut latencies = Arc::try_unwrap(latencies)
        .map_err(|_| {
            common::PlatformError::internal("benchmark latency collection is still shared")
        })?
        .into_inner();
    latencies.sort_unstable();
    Ok(latencies)
}

fn to_scenario_result(
    total_requests: usize,
    concurrency: usize,
    total_duration: Duration,
    latencies: &[Duration],
) -> ScenarioResult {
    let throughput_rps = total_requests as f64 / total_duration.as_secs_f64();
    let mean_ms = if latencies.is_empty() {
        0.0
    } else {
        latencies
            .iter()
            .map(|duration| duration.as_secs_f64() * 1000.0)
            .sum::<f64>()
            / latencies.len() as f64
    };
    ScenarioResult {
        total_requests,
        concurrency,
        total_duration,
        throughput_rps,
        mean_ms,
        p50_ms: percentile_ms(latencies, 0.50),
        p95_ms: percentile_ms(latencies, 0.95),
        p99_ms: percentile_ms(latencies, 0.99),
    }
}

fn summarize_distribution(samples: &[Duration]) -> Distribution {
    let mut samples = samples.to_vec();
    samples.sort_unstable();
    let mean_ms = if samples.is_empty() {
        0.0
    } else {
        samples
            .iter()
            .map(|duration| duration.as_secs_f64() * 1000.0)
            .sum::<f64>()
            / samples.len() as f64
    };

    Distribution {
        mean_ms,
        p50_ms: percentile_ms(&samples, 0.50),
        p95_ms: percentile_ms(&samples, 0.95),
        p99_ms: percentile_ms(&samples, 0.99),
        max_ms: samples
            .last()
            .map(|duration| duration.as_secs_f64() * 1000.0)
            .unwrap_or(0.0),
    }
}

fn percentile_ms(latencies: &[Duration], percentile: f64) -> f64 {
    if latencies.is_empty() {
        return 0.0;
    }
    let idx = ((latencies.len().saturating_sub(1)) as f64 * percentile).round() as usize;
    latencies[idx].as_secs_f64() * 1000.0
}

fn format_scenario_result(name: &str, result: ScenarioResult) -> String {
    let mut out = String::new();
    let _ = write!(
        out,
        "{:<18} requests={} concurrency={} total={:.2}ms throughput={:.0} req/s mean={:.2}ms p50={:.2}ms p95={:.2}ms p99={:.2}ms",
        name,
        result.total_requests,
        result.concurrency,
        result.total_duration.as_secs_f64() * 1000.0,
        result.throughput_rps,
        result.mean_ms,
        result.p50_ms,
        result.p95_ms,
        result.p99_ms
    );
    out
}

fn format_distribution_result(name: &str, samples: usize, distribution: Distribution) -> String {
    let mut out = String::new();
    let _ = write!(
        out,
        "{:<18} samples={} mean={:.2}ms p50={:.2}ms p95={:.2}ms p99={:.2}ms max={:.2}ms",
        name,
        samples,
        distribution.mean_ms,
        distribution.p50_ms,
        distribution.p95_ms,
        distribution.p99_ms,
        distribution.max_ms
    );
    out
}

fn format_cold_start_result(result: ColdStartResult) -> String {
    let mut out = String::new();
    let _ = write!(
        out,
        "cold-start         rounds={} deploy(mean/p95/p99)={:.2}/{:.2}/{:.2}ms first-invoke(mean/p95/p99)={:.2}/{:.2}/{:.2}ms",
        result.rounds,
        result.deploy.mean_ms,
        result.deploy.p95_ms,
        result.deploy.p99_ms,
        result.first_invoke.mean_ms,
        result.first_invoke.p95_ms,
        result.first_invoke.p99_ms,
    );
    out
}

fn format_scale_up_result(result: ScaleUpResult) -> String {
    let mut out = String::new();
    let scale_up_ms = result
        .time_to_target
        .map(|duration| format!("{:.2}", duration.as_secs_f64() * 1000.0))
        .unwrap_or_else(|| "timeout".to_string());
    let _ = write!(
        out,
        "scale-up           requests={} concurrency={} target_isolates={} time_to_target={}ms burst_p95={:.2}ms burst_p99={:.2}ms burst_throughput={:.0} req/s",
        result.requests,
        result.concurrency,
        result.target_isolates,
        scale_up_ms,
        result.burst.p95_ms,
        result.burst.p99_ms,
        result.burst.throughput_rps,
    );
    out
}

fn format_restore_start_result(result: RestoreStartResult) -> String {
    let mut out = String::new();
    let _ = write!(
        out,
        "restore-from-disk  rounds={} startup(mean/p95/p99)={:.2}/{:.2}/{:.2}ms first-invoke(mean/p95/p99)={:.2}/{:.2}/{:.2}ms",
        result.rounds,
        result.startup.mean_ms,
        result.startup.p95_ms,
        result.startup.p99_ms,
        result.first_invoke.mean_ms,
        result.first_invoke.p95_ms,
        result.first_invoke.p99_ms,
    );
    out
}

fn format_websocket_open_result(result: &WebSocketBenchResult) -> String {
    let mut out = String::new();
    let _ = write!(
        out,
        "{:<18} sessions={} mean={:.2}ms p50={:.2}ms p95={:.2}ms p99={:.2}ms max={:.2}ms",
        "ws-open",
        result.sessions,
        result.open.mean_ms,
        result.open.p50_ms,
        result.open.p95_ms,
        result.open.p99_ms,
        result.open.max_ms,
    );
    out
}

fn format_websocket_roundtrip_result(result: &WebSocketBenchResult) -> String {
    format_scenario_result(
        &format!("ws-send-echo x{}", result.messages_per_session),
        ScenarioResult {
            total_requests: result.roundtrip.total_requests,
            concurrency: result.roundtrip.concurrency,
            total_duration: result.roundtrip.total_duration,
            throughput_rps: result.roundtrip.throughput_rps,
            mean_ms: result.roundtrip.mean_ms,
            p50_ms: result.roundtrip.p50_ms,
            p95_ms: result.roundtrip.p95_ms,
            p99_ms: result.roundtrip.p99_ms,
        },
    )
}
