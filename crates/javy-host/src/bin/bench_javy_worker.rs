//! Measures Javy's pooled-instance request path with the same metrics emitted by
//! the existing Perry Wasm and V8 runtime benchmarks.
//!
//! ```bash
//! cargo run -p javy_host --bin bench_javy_worker --release
//! DD_BENCH_REQUESTS=5000 DD_BENCH_CONCURRENCY=8 \
//!   cargo run -p javy_host --bin bench_javy_worker --release
//! ```

use clap::Parser;
use common::WorkerInvocation;
use javy_host::{InvokeOptions, JavyWorker, WorkerOptions};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

#[derive(Parser)]
#[command(about = "Benchmark the Javy Wasm worker engine")]
struct Args {
    /// Worker Wasm to benchmark; defaults to the instant-response fixture
    #[arg(long)]
    worker: Option<std::path::PathBuf>,
    #[arg(long, env = "DD_BENCH_REQUESTS", default_value = "2000")]
    requests: usize,
    #[arg(long, env = "DD_BENCH_CONCURRENCY", default_value = "8")]
    concurrency: usize,
    #[arg(long, env = "DD_BENCH_COMPILE_ROUNDS", default_value = "20")]
    compile_rounds: usize,
    #[arg(long, env = "DD_BENCH_WARMUP_REQUESTS", default_value = "25")]
    warmup_requests: usize,
    /// Substring required in every response body
    #[arg(long, default_value = "ok")]
    expected_body: String,
    /// Ephemeral KV binding exposed to the benchmark worker
    #[arg(long)]
    kv: Vec<String>,
    /// Transactional memory binding exposed to the benchmark worker
    #[arg(long)]
    memory: Vec<String>,
    /// Maximum number of warm Wasmtime instances
    #[arg(long, default_value_t = WorkerOptions::default().pool_size)]
    pool_size: usize,
    /// Successful requests served before a warm instance is replaced; use 1 for fresh instances
    #[arg(
        long,
        default_value_t = WorkerOptions::default().max_requests_per_instance
    )]
    max_requests_per_instance: usize,
}

struct ScenarioResult {
    total_duration: Duration,
    latencies: Vec<Duration>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let worker_path = args.worker.clone().unwrap_or_else(|| {
        std::path::PathBuf::from(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/fixtures/instant_worker.wasm"
        ))
    });
    let worker_bytes = std::fs::read(&worker_path)
        .map_err(|error| format!("cannot read {}: {error}", worker_path.display()))?;

    println!(
        "bench_javy_worker worker={} worker_bytes={} requests={} concurrency={} pool_size={} \
         max_requests_per_instance={}",
        worker_path.display(),
        worker_bytes.len(),
        args.requests,
        args.concurrency,
        args.pool_size,
        args.max_requests_per_instance
    );

    let compile = measure_rounds(args.compile_rounds, || {
        JavyWorker::new(
            &worker_bytes,
            WorkerOptions {
                kv_bindings: args.kv.clone(),
                memory_bindings: args.memory.clone(),
                pool_size: args.pool_size,
                max_requests_per_instance: args.max_requests_per_instance,
                ..WorkerOptions::default()
            },
        )
        .expect("worker should compile");
    });
    print_distribution("module-compile", args.compile_rounds, &compile);

    let worker = Arc::new(JavyWorker::new(
        &worker_bytes,
        WorkerOptions {
            kv_bindings: args.kv,
            memory_bindings: args.memory,
            pool_size: args.pool_size,
            max_requests_per_instance: args.max_requests_per_instance,
            ..WorkerOptions::default()
        },
    )?);
    for sequence in 0..args.warmup_requests {
        assert_output(
            worker.invoke(invocation(sequence), InvokeOptions::default())?,
            &args.expected_body,
        )?;
    }

    let expected_body: Arc<str> = args.expected_body.into();
    let sequential = run_scenario(&worker, Arc::clone(&expected_body), args.requests, 1)?;
    print_scenario("sequential-invoke", &sequential);

    let parallel = run_scenario(&worker, expected_body, args.requests, args.concurrency)?;
    print_scenario(&format!("parallel-invoke-{}", args.concurrency), &parallel);

    Ok(())
}

fn invocation(sequence: usize) -> WorkerInvocation {
    WorkerInvocation {
        method: "GET".to_string(),
        url: "http://bench.local/".to_string(),
        headers: vec![("user-agent".to_string(), "dd-bench/1".to_string())],
        body: Vec::new(),
        request_id: sequence.to_string(),
    }
}

fn run_scenario(
    worker: &Arc<JavyWorker>,
    expected_body: Arc<str>,
    requests: usize,
    concurrency: usize,
) -> Result<ScenarioResult, String> {
    let next = Arc::new(AtomicUsize::new(0));
    let started = Instant::now();
    let mut threads = Vec::with_capacity(concurrency);
    for _ in 0..concurrency {
        let worker = Arc::clone(worker);
        let expected_body = Arc::clone(&expected_body);
        let next = Arc::clone(&next);
        threads.push(std::thread::spawn(move || {
            let mut latencies = Vec::new();
            loop {
                let sequence = next.fetch_add(1, Ordering::Relaxed);
                if sequence >= requests {
                    return Ok::<_, String>(latencies);
                }
                let request_started = Instant::now();
                let output = worker
                    .invoke(invocation(sequence), InvokeOptions::default())
                    .map_err(|error| format!("invoke {sequence} failed: {error}"))?;
                assert_output(output, &expected_body)?;
                latencies.push(request_started.elapsed());
            }
        }));
    }

    let mut latencies = Vec::with_capacity(requests);
    for thread in threads {
        latencies.extend(thread.join().map_err(|_| "benchmark thread panicked")??);
    }
    latencies.sort_unstable();
    Ok(ScenarioResult {
        total_duration: started.elapsed(),
        latencies,
    })
}

fn assert_output(output: common::WorkerOutput, expected_body: &str) -> Result<(), String> {
    if output.status != 200 {
        return Err(format!("unexpected status {}", output.status));
    }
    let body = String::from_utf8_lossy(&output.body);
    if !body.contains(expected_body) {
        return Err(format!(
            "response body did not contain {expected_body:?}: {body:?}"
        ));
    }
    Ok(())
}

fn measure_rounds(rounds: usize, mut operation: impl FnMut()) -> Vec<Duration> {
    let mut samples = Vec::with_capacity(rounds);
    for _ in 0..rounds {
        let started = Instant::now();
        operation();
        samples.push(started.elapsed());
    }
    samples.sort_unstable();
    samples
}

fn print_scenario(name: &str, result: &ScenarioResult) {
    let requests = result.latencies.len();
    let throughput = requests as f64 / result.total_duration.as_secs_f64();
    println!(
        "scenario {name}: requests={requests} duration={:.2}s throughput={throughput:.0} rps \
         mean={:.3}ms p50={:.3}ms p95={:.3}ms p99={:.3}ms",
        result.total_duration.as_secs_f64(),
        mean_ms(&result.latencies),
        percentile_ms(&result.latencies, 50.0),
        percentile_ms(&result.latencies, 95.0),
        percentile_ms(&result.latencies, 99.0),
    );
}

fn print_distribution(name: &str, rounds: usize, sorted: &[Duration]) {
    println!(
        "scenario {name}: rounds={rounds} mean={:.3}ms p50={:.3}ms max={:.3}ms",
        mean_ms(sorted),
        percentile_ms(sorted, 50.0),
        sorted
            .last()
            .map_or(0.0, |duration| duration.as_secs_f64() * 1000.0),
    );
}

fn mean_ms(samples: &[Duration]) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    samples
        .iter()
        .map(|duration| duration.as_secs_f64())
        .sum::<f64>()
        / samples.len() as f64
        * 1000.0
}

fn percentile_ms(sorted: &[Duration], percentile: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let rank = ((percentile / 100.0) * (sorted.len() as f64 - 1.0)).round() as usize;
    sorted[rank.min(sorted.len() - 1)].as_secs_f64() * 1000.0
}
