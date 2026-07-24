//! Benchmark for the Perry wasm worker engine, reporting the same metrics as
//! `cargo run -p runtime --bin bench` (throughput plus mean/p50/p95/p99
//! latency) so the two runtimes can be compared side by side.
//!
//! Methodology note for that comparison: this engine instantiates a fresh
//! wasm instance and re-runs `_start` on every request, while the V8 runtime
//! reuses warm isolates. Cold work is therefore part of every wasm request.
//!
//! ```bash
//! cargo run -p wasm_host --bin bench_wasm_worker --release
//! DD_BENCH_REQUESTS=10000 DD_BENCH_CONCURRENCY=16 \
//!   cargo run -p wasm_host --bin bench_wasm_worker --release -- --worker my.wasm
//! ```

use clap::Parser;
use common::WorkerInvocation;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use wasm_host::{InvokeOptions, WorkerModule};

#[derive(Parser)]
#[command(about = "Benchmark the Perry wasm worker engine")]
struct Args {
    /// Worker .wasm to benchmark; defaults to the hello fixture
    #[arg(long)]
    worker: Option<std::path::PathBuf>,
    #[arg(long, env = "DD_BENCH_REQUESTS", default_value = "2000")]
    requests: usize,
    #[arg(long, env = "DD_BENCH_CONCURRENCY", default_value = "8")]
    concurrency: usize,
    #[arg(long, env = "DD_BENCH_COMPILE_ROUNDS", default_value = "20")]
    compile_rounds: usize,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let worker_path = args.worker.clone().unwrap_or_else(|| {
        std::path::PathBuf::from(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/fixtures/hello_worker.wasm"
        ))
    });
    let bytes = std::fs::read(&worker_path)
        .map_err(|error| format!("cannot read {}: {error}", worker_path.display()))?;

    println!(
        "bench_wasm_worker worker={} requests={} concurrency={}",
        worker_path.display(),
        args.requests,
        args.concurrency
    );

    let compile = measure_rounds(args.compile_rounds, || {
        WorkerModule::from_bytes(&bytes).expect("worker should compile");
    });
    print_distribution("module-compile", args.compile_rounds, &compile);

    let module = Arc::new(WorkerModule::from_bytes(&bytes)?);

    // Warmup lets wasmtime finish any lazy per-module work before measuring.
    for _ in 0..50 {
        module.invoke(invocation(0), InvokeOptions::default())?;
    }

    let sequential = run_scenario(&module, args.requests, 1)?;
    print_scenario("sequential-invoke", &sequential);

    let parallel = run_scenario(&module, args.requests, args.concurrency)?;
    print_scenario(&format!("parallel-invoke-{}", args.concurrency), &parallel);

    Ok(())
}

fn invocation(sequence: usize) -> WorkerInvocation {
    WorkerInvocation {
        method: "GET".to_string(),
        url: format!("http://bench.local/user-{sequence}"),
        headers: vec![("user-agent".to_string(), "dd-bench/1".to_string())],
        body: Vec::new(),
        request_id: sequence.to_string(),
    }
}

struct ScenarioResult {
    total_duration: Duration,
    latencies: Vec<Duration>,
}

fn run_scenario(
    module: &Arc<WorkerModule>,
    requests: usize,
    concurrency: usize,
) -> Result<ScenarioResult, String> {
    let next = Arc::new(AtomicUsize::new(0));
    let started = Instant::now();
    let mut workers = Vec::with_capacity(concurrency);
    for _ in 0..concurrency {
        let module = Arc::clone(module);
        let next = Arc::clone(&next);
        workers.push(std::thread::spawn(move || {
            let mut latencies = Vec::new();
            loop {
                let sequence = next.fetch_add(1, Ordering::Relaxed);
                if sequence >= requests {
                    return Ok::<_, String>(latencies);
                }
                let begin = Instant::now();
                let output = module
                    .invoke(invocation(sequence), InvokeOptions::default())
                    .map_err(|error| format!("invoke failed: {error}"))?;
                if output.status != 200 {
                    return Err(format!("unexpected status {}", output.status));
                }
                latencies.push(begin.elapsed());
            }
        }));
    }

    let mut latencies = Vec::with_capacity(requests);
    for worker in workers {
        latencies.extend(worker.join().map_err(|_| "bench thread panicked")??);
    }
    latencies.sort();
    Ok(ScenarioResult {
        total_duration: started.elapsed(),
        latencies,
    })
}

fn measure_rounds(rounds: usize, mut op: impl FnMut()) -> Vec<Duration> {
    let mut samples = Vec::with_capacity(rounds);
    for _ in 0..rounds {
        let begin = Instant::now();
        op();
        samples.push(begin.elapsed());
    }
    samples.sort();
    samples
}

fn print_scenario(name: &str, result: &ScenarioResult) {
    let total = result.latencies.len();
    let throughput = total as f64 / result.total_duration.as_secs_f64();
    println!(
        "scenario {name}: requests={total} duration={:.2}s throughput={throughput:.0} rps \
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
        sorted.last().map_or(0.0, |d| d.as_secs_f64() * 1000.0),
    );
}

fn mean_ms(samples: &[Duration]) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    samples.iter().map(|d| d.as_secs_f64()).sum::<f64>() / samples.len() as f64 * 1000.0
}

fn percentile_ms(sorted: &[Duration], percentile: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let rank = ((percentile / 100.0) * (sorted.len() as f64 - 1.0)).round() as usize;
    sorted[rank.min(sorted.len() - 1)].as_secs_f64() * 1000.0
}
