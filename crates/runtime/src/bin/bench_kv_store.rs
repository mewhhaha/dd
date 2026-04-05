use runtime::KvStore;
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
}

struct ScenarioResult {
    throughput_rps: f64,
    mean_ms: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    total_ms: f64,
}

#[tokio::main]
async fn main() -> Result<(), String> {
    let requests = env_usize("DD_BENCH_REQUESTS", 20_000);
    let concurrency = env_usize("DD_BENCH_CONCURRENCY", 128);
    let scenario_name = env_name("DD_BENCH_INTERNAL_SCENARIO");
    let scenarios = [
        Scenario {
            label: "get-utf8",
            requests,
            concurrency,
        },
        Scenario {
            label: "set-utf8",
            requests,
            concurrency,
        },
    ];

    if let Some(scenario_name) = scenario_name {
        let scenario = scenarios
            .iter()
            .find(|scenario| scenario.label == scenario_name)
            .ok_or_else(|| format!("unknown kv-store scenario: {scenario_name}"))?;
        run_scenario(*scenario).await?;
        return Ok(());
    }

    println!("# kv-store benchmark");
    println!("# direct KvStore path without worker/runtime fetch overhead.");
    println!(
        "# config: requests={} concurrency={}",
        requests, concurrency
    );
    for scenario in scenarios {
        run_scenario(scenario).await?;
    }
    Ok(())
}

async fn run_scenario(scenario: Scenario) -> Result<(), String> {
    let root = PathBuf::from(format!("/tmp/dd-bench-kv-store-{}", Uuid::new_v4()));
    let db_url = format!("file:{}", root.join("dd-kv.db").display());
    let store = KvStore::from_database_url(&db_url)
        .await
        .map_err(|error| error.to_string())?;
    store
        .set("worker-a", "MY_KV", "hot", "1")
        .await
        .map_err(|error| error.to_string())?;

    let next = Arc::new(AtomicUsize::new(0));
    let latencies = Arc::new(Mutex::new(Vec::with_capacity(scenario.requests)));
    let started_at = Instant::now();
    let mut tasks = Vec::with_capacity(scenario.concurrency);

    for _ in 0..scenario.concurrency {
        let store = store.clone();
        let next = Arc::clone(&next);
        let latencies = Arc::clone(&latencies);
        tasks.push(tokio::spawn(async move {
            loop {
                let idx = next.fetch_add(1, Ordering::Relaxed);
                if idx >= scenario.requests {
                    break;
                }
                let invoke_started = Instant::now();
                match scenario.label {
                    "get-utf8" => {
                        let _ = store.get_utf8("worker-a", "MY_KV", "hot").await?;
                    }
                    "set-utf8" => {
                        store.set("worker-a", "MY_KV", "hot", "1").await?;
                    }
                    _ => {}
                }
                latencies.lock().await.push(invoke_started.elapsed());
            }

            Ok::<(), common::PlatformError>(())
        }));
    }

    for task in tasks {
        task.await
            .map_err(|error| error.to_string())?
            .map_err(|error| error.to_string())?;
    }

    let total_duration = started_at.elapsed();
    let result = to_result(
        scenario,
        total_duration,
        &take_sorted_latencies(latencies).await,
    );
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
    Ok(())
}

async fn take_sorted_latencies(latencies: Arc<Mutex<Vec<Duration>>>) -> Vec<Duration> {
    let mut latencies = latencies.lock().await.clone();
    latencies.sort_unstable();
    latencies
}

fn to_result(
    scenario: Scenario,
    total_duration: Duration,
    latencies: &[Duration],
) -> ScenarioResult {
    let throughput_rps = scenario.requests as f64 / total_duration.as_secs_f64();
    let mean_ms = latencies
        .iter()
        .map(|duration| duration.as_secs_f64() * 1_000.0)
        .sum::<f64>()
        / latencies.len().max(1) as f64;
    ScenarioResult {
        throughput_rps,
        mean_ms,
        p50_ms: percentile_ms(latencies, 0.50),
        p95_ms: percentile_ms(latencies, 0.95),
        p99_ms: percentile_ms(latencies, 0.99),
        total_ms: total_duration.as_secs_f64() * 1_000.0,
    }
}

fn percentile_ms(values: &[Duration], percentile: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let idx = ((values.len() - 1) as f64 * percentile).round() as usize;
    values[idx.min(values.len() - 1)].as_secs_f64() * 1_000.0
}

fn env_name(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}
