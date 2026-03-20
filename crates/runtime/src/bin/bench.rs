use common::WorkerInvocation;
use runtime::{RuntimeConfig, RuntimeService};
use std::fmt::Write as _;
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

struct ScaleUpResult {
    requests: usize,
    concurrency: usize,
    target_isolates: usize,
    time_to_target: Option<Duration>,
    burst: ScenarioResult,
}

struct Distribution {
    mean_ms: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    max_ms: f64,
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

    println!("# grugd runtime benchmark");
    println!(
        "# note: this benchmark measures runtime service behavior directly (no API/HTTP network overhead)."
    );
    println!("# note: output includes mean, p50, p95, p99 for each benchmark.");
    println!();

    for config in configs {
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

        if config.name == "autoscaling-8" {
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

            let scale_service = start_service("scale-up", config.runtime.clone())
                .await
                .map_err(|error| error.to_string())?;
            let scale = run_scale_up(&scale_service, &config.runtime, 1000, 192)
                .await
                .map_err(|error| error.to_string())?;
            println!("{}", format_scale_up_result(scale));
            println!();
        }
    }

    Ok(())
}

async fn start_service(tag: &str, runtime: RuntimeConfig) -> common::Result<RuntimeService> {
    let db_path = format!("/tmp/grugd-bench-{}-{}.db", tag, Uuid::new_v4());
    std::env::set_var("TURSO_DATABASE_URL", format!("file:{db_path}"));
    RuntimeService::start_with_config(runtime).await
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

fn invocation(path: &str, idx: usize) -> WorkerInvocation {
    WorkerInvocation {
        method: "GET".to_string(),
        url: format!("http://worker{path}"),
        headers: Vec::new(),
        body: Vec::new(),
        request_id: format!("bench-{idx}"),
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
