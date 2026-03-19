use common::WorkerInvocation;
use runtime::{RuntimeConfig, RuntimeService};
use std::fmt::Write as _;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::{Duration, Instant};
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
        },
        Scenario {
            name: "microtask-1000",
            worker_source: r#"
export default {
  async fetch() {
    for (let i = 0; i < 1000; i += 1) {
      await Promise.resolve(i);
    }
    return new Response("ok");
  },
};
"#,
            requests: 300,
            concurrency: 64,
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
        },
    ];

    println!("# grugd runtime benchmark");
    println!("# note: this measures the current isolate model directly in the runtime service.");
    println!("# note: host-sleep-5ms uses the runtime's host async sleep op to simulate promise-idle work.");
    println!();

    for config in configs {
        println!("== {} ==", config.name);
        let db_path = format!("/tmp/grugd-bench-{}-{}.db", config.name, Uuid::new_v4());
        std::env::set_var("TURSO_DATABASE_URL", format!("file:{db_path}"));
        let service = RuntimeService::start_with_config(config.runtime)
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
            println!("{}", format_result(scenario.name, result));
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

                let invoke_started = Instant::now();
                let request = WorkerInvocation {
                    method: "GET".to_string(),
                    url: "http://worker/".to_string(),
                    headers: Vec::new(),
                    body: Vec::new(),
                    request_id: format!("bench-{idx}"),
                };

                service.invoke(worker_name.clone(), request).await?;
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
        .map_err(|_| {
            common::PlatformError::internal("benchmark latency collection is still shared")
        })?
        .into_inner();
    latencies.sort_unstable();

    let throughput_rps = scenario.requests as f64 / total_duration.as_secs_f64();
    let mean_ms = latencies
        .iter()
        .map(|duration| duration.as_secs_f64() * 1000.0)
        .sum::<f64>()
        / latencies.len() as f64;

    Ok(ScenarioResult {
        total_requests: scenario.requests,
        concurrency: scenario.concurrency,
        total_duration,
        throughput_rps,
        mean_ms,
        p50_ms: percentile_ms(&latencies, 0.50),
        p95_ms: percentile_ms(&latencies, 0.95),
        p99_ms: percentile_ms(&latencies, 0.99),
    })
}

fn percentile_ms(latencies: &[Duration], percentile: f64) -> f64 {
    let idx = ((latencies.len().saturating_sub(1)) as f64 * percentile).round() as usize;
    latencies[idx].as_secs_f64() * 1000.0
}

fn format_result(name: &str, result: ScenarioResult) -> String {
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
