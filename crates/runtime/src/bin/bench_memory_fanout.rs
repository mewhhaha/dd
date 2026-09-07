use common::{DeployBinding, DeployConfig, WorkerInvocation};
use runtime::{RuntimeConfig, RuntimeService, RuntimeServiceConfig, RuntimeStorageConfig};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::path::Path;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::time::{Duration, Instant};
use tokio::task::JoinSet;
use uuid::Uuid;

const WORKER: &str = "memory-fanout";
const WORKER_SOURCE: &str = include_str!("bench_memory_fanout/worker.js");
const ENV_NAMES: &[&str] = &[
    "DD_FANOUT_WIDTH",
    "DD_FANOUT_MODE",
    "DD_FANOUT_POPULATION",
    "DD_FANOUT_PAYLOAD_BYTES",
    "DD_FANOUT_KEYS_PER_ENTITY",
    "DD_FANOUT_PAYLOAD_KIND",
    "DD_FANOUT_CONCURRENCY",
    "DD_FANOUT_DURATION_MS",
    "DD_FANOUT_WARMUP_MS",
];

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "lowercase")]
enum Mode {
    Read,
    Write,
    Mixed,
}

#[derive(Clone, Serialize)]
struct Config {
    width: usize,
    mode: Mode,
    population: usize,
    payload_bytes: usize,
    keys_per_entity: usize,
    payload_kind: String,
    concurrency: usize,
    duration_ms: usize,
    warmup_ms: usize,
    available_cpus: usize,
}

impl Config {
    fn from_env() -> Result<Self, String> {
        for (name, _) in std::env::vars_os() {
            let name = name.to_string_lossy();
            if name.starts_with("DD_FANOUT_") && !ENV_NAMES.contains(&name.as_ref()) {
                return Err(format!(
                    "unsupported environment variable {name}; expected {ENV_NAMES:?}"
                ));
            }
        }
        let available_cpus = std::thread::available_parallelism()
            .map(usize::from)
            .map_err(|error| format!("cannot determine available CPUs: {error}"))?;
        let mode = match std::env::var("DD_FANOUT_MODE") {
            Ok(value) => match value.as_str() {
                "read" => Mode::Read,
                "write" => Mode::Write,
                "mixed" => Mode::Mixed,
                _ => {
                    return Err(format!(
                        "DD_FANOUT_MODE={value:?}; expected read, write, or mixed"
                    ));
                }
            },
            Err(std::env::VarError::NotPresent) => Mode::Read,
            Err(error) => return Err(format!("cannot read DD_FANOUT_MODE: {error}")),
        };
        let config = Self {
            width: env_usize("DD_FANOUT_WIDTH", 1, 1)?,
            mode,
            population: env_usize("DD_FANOUT_POPULATION", 1024, 1)?,
            payload_bytes: env_usize("DD_FANOUT_PAYLOAD_BYTES", 128, 0)?,
            keys_per_entity: env_usize("DD_FANOUT_KEYS_PER_ENTITY", 1, 1)?,
            payload_kind: std::env::var("DD_FANOUT_PAYLOAD_KIND")
                .unwrap_or_else(|_| "repeated".to_string()),
            concurrency: env_usize("DD_FANOUT_CONCURRENCY", 4 * available_cpus, 1)?,
            duration_ms: env_usize("DD_FANOUT_DURATION_MS", 8000, 1)?,
            warmup_ms: env_usize("DD_FANOUT_WARMUP_MS", 2000, 0)?,
            available_cpus,
        };
        if config.keys_per_entity > 256
            || !["repeated", "varied"].contains(&config.payload_kind.as_str())
        {
            return Err("expected 1..=256 keys per entity and repeated/varied payload kind".into());
        }
        if ![1, 4, 16].contains(&config.width) {
            return Err(format!(
                "DD_FANOUT_WIDTH={}; expected 1, 4, or 16",
                config.width
            ));
        }
        if config.population < config.width {
            return Err(format!(
                "DD_FANOUT_POPULATION={} is smaller than DD_FANOUT_WIDTH={}; each request needs distinct entities",
                config.population, config.width
            ));
        }
        Ok(config)
    }
}

fn env_usize(name: &str, default: usize, minimum: usize) -> Result<usize, String> {
    let value = match std::env::var(name) {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => return Ok(default),
        Err(error) => return Err(format!("cannot read {name}: {error}")),
    };
    let parsed = value
        .parse::<usize>()
        .map_err(|error| format!("{name}={value:?}; expected an integer >= {minimum}: {error}"))?;
    if parsed < minimum {
        return Err(format!(
            "{name}={value:?}; expected an integer >= {minimum}"
        ));
    }
    Ok(parsed)
}

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "lowercase")]
enum Operation {
    Seed,
    Read,
    Write,
    Verify,
}

#[derive(Serialize)]
struct RequestPlan {
    operation: Operation,
    sequence: u64,
    entities: Vec<usize>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Reply {
    sequence: u64,
    results: Vec<EntityCount>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EntityCount {
    entity: usize,
    count: u64,
    payload_valid: bool,
}

async fn invoke(service: &RuntimeService, plan: &RequestPlan) -> Result<Vec<EntityCount>, String> {
    let output = service
        .invoke(
            WORKER.to_string(),
            WorkerInvocation {
                method: "POST".to_string(),
                url: "http://memory-fanout/".to_string(),
                headers: vec![("content-type".to_string(), "application/json".to_string())],
                body: serde_json::to_vec(plan).map_err(|error| error.to_string())?,
                request_id: format!("fanout-{}", plan.sequence),
            },
        )
        .await
        .map_err(|error| format!("request {} invoke failed: {error}", plan.sequence))?;
    if output.status != 200 {
        return Err(format!(
            "request {} returned status {}: {}",
            plan.sequence,
            output.status,
            String::from_utf8_lossy(&output.body)
        ));
    }
    let reply: Reply = serde_json::from_slice(&output.body).map_err(|error| {
        format!(
            "request {} returned invalid JSON: {error}; body={}",
            plan.sequence,
            String::from_utf8_lossy(&output.body)
        )
    })?;
    if reply.sequence != plan.sequence || reply.results.len() != plan.entities.len() {
        return Err(format!(
            "request {} expected {} results; reply sequence={} results={}",
            plan.sequence,
            plan.entities.len(),
            reply.sequence,
            reply.results.len()
        ));
    }
    for (expected_entity, result) in plan.entities.iter().zip(&reply.results) {
        if result.entity != *expected_entity
            || !result.payload_valid
            || result.count > 9_007_199_254_740_991
        {
            return Err(format!(
                "request {} expected entity {expected_entity} with intact payload and safe count; got entity={} count={} payload_valid={}",
                plan.sequence, result.entity, result.count, result.payload_valid
            ));
        }
        if matches!(plan.operation, Operation::Seed) && result.count != 0
            || matches!(plan.operation, Operation::Write) && result.count == 0
        {
            return Err(format!(
                "request {} entity {expected_entity} returned count {}",
                plan.sequence, result.count
            ));
        }
    }
    Ok(reply.results)
}

#[derive(Default)]
struct LaneSamples {
    latencies_ms: Vec<f64>,
    writes: Vec<(usize, u64)>,
    highest_counts: Vec<u64>,
    write_requests: usize,
}

struct PhaseSamples {
    lanes: Vec<LaneSamples>,
    elapsed_seconds: f64,
    requested_seconds: f64,
}

impl PhaseSamples {
    fn summary(&self, width: usize) -> Value {
        let mut latencies = self
            .lanes
            .iter()
            .flat_map(|lane| lane.latencies_ms.iter().copied())
            .collect::<Vec<_>>();
        latencies.sort_unstable_by(f64::total_cmp);
        let requests = latencies.len();
        let write_requests = self
            .lanes
            .iter()
            .map(|lane| lane.write_requests)
            .sum::<usize>();
        let percentile = |percent: usize| {
            (!latencies.is_empty())
                .then(|| latencies[(requests * percent).div_ceil(100).saturating_sub(1)])
        };
        json!({
            "requests": requests,
            "transactions": requests * width,
            "write_requests": write_requests,
            "write_request_fraction": (requests > 0).then(|| write_requests as f64 / requests as f64),
            "writes": self.lanes.iter().map(|lane| lane.writes.len()).sum::<usize>(),
            "elapsed_seconds": self.elapsed_seconds,
            "requested_seconds": self.requested_seconds,
            "drain_seconds": (self.elapsed_seconds - self.requested_seconds).max(0.0),
            "requests_per_second": requests as f64 / self.elapsed_seconds,
            "transactions_per_second": (requests * width) as f64 / self.elapsed_seconds,
            "p50_ms": percentile(50),
            "p95_ms": percentile(95),
            "p99_ms": percentile(99),
        })
    }
}

async fn run_phase(
    service: &RuntimeService,
    config: &Config,
    sequence: &Arc<AtomicU64>,
    duration_ms: usize,
) -> Result<PhaseSamples, String> {
    let started = Instant::now();
    let duration = Duration::from_millis(duration_ms as u64);
    let deadline = started
        .checked_add(duration)
        .ok_or_else(|| format!("phase duration {duration_ms}ms exceeds the clock range"))?;
    let stopped = Arc::new(AtomicBool::new(false));
    let mut tasks = JoinSet::new();
    for _ in 0..config.concurrency {
        let service = service.clone();
        let config = config.clone();
        let sequence = Arc::clone(sequence);
        let stopped = Arc::clone(&stopped);
        tasks.spawn(async move {
            let mut samples = LaneSamples {
                highest_counts: vec![0; config.population],
                ..LaneSamples::default()
            };
            let blocks_per_entity_sweep = config.population.div_ceil(10 * config.width) as u64;
            while Instant::now() < deadline && !stopped.load(Ordering::Relaxed) {
                let request_started = Instant::now();
                let sequence = sequence.fetch_add(1, Ordering::Relaxed);
                let write_slot = (sequence / 10 / blocks_per_entity_sweep) % 10;
                let writes = matches!(config.mode, Mode::Write)
                    || matches!(config.mode, Mode::Mixed) && sequence % 10 == write_slot;
                let plan = RequestPlan {
                    operation: if writes {
                        Operation::Write
                    } else {
                        Operation::Read
                    },
                    sequence,
                    entities: (0..config.width)
                        .map(|offset| {
                            ((sequence as u128 * config.width as u128 + offset as u128)
                                % config.population as u128) as usize
                        })
                        .collect(),
                };
                let results = match invoke(&service, &plan).await {
                    Ok(results) => results,
                    Err(error) => {
                        stopped.store(true, Ordering::Relaxed);
                        return Err(error);
                    }
                };
                for result in results {
                    let previous = samples.highest_counts[result.entity];
                    if result.count < previous || writes && result.count == previous {
                        stopped.store(true, Ordering::Relaxed);
                        return Err(format!(
                            "request {sequence} entity {} count {} did not advance from {previous}",
                            result.entity, result.count
                        ));
                    }
                    samples.highest_counts[result.entity] = result.count;
                    if writes {
                        samples.writes.push((result.entity, result.count));
                    }
                }
                samples.write_requests += usize::from(writes);
                samples
                    .latencies_ms
                    .push(request_started.elapsed().as_secs_f64() * 1000.0);
            }
            Ok(samples)
        });
    }
    let mut lanes = Vec::with_capacity(config.concurrency);
    let mut errors = Vec::new();
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(Ok(samples)) => lanes.push(samples),
            Ok(Err(error)) => errors.push(error),
            Err(error) => {
                stopped.store(true, Ordering::Relaxed);
                errors.push(format!("benchmark caller failed: {error}"));
            }
        }
    }
    if !errors.is_empty() {
        return Err(errors.join("; "));
    }
    if duration_ms > 0 && lanes.iter().all(|lane| lane.latencies_ms.is_empty()) {
        return Err(format!("phase of {duration_ms}ms completed no requests"));
    }
    Ok(PhaseSamples {
        lanes,
        elapsed_seconds: started.elapsed().as_secs_f64(),
        requested_seconds: duration.as_secs_f64(),
    })
}

async fn verify(service: &RuntimeService, counts: &[u64]) -> Result<(), String> {
    for start in (0..counts.len()).step_by(16) {
        let plan = RequestPlan {
            operation: Operation::Verify,
            sequence: start as u64,
            entities: (start..(start + 16).min(counts.len())).collect(),
        };
        for result in invoke(service, &plan).await? {
            if result.count != counts[result.entity] {
                return Err(format!(
                    "entity {} expected count {}, got {}",
                    result.entity, counts[result.entity], result.count
                ));
            }
        }
    }
    Ok(())
}

async fn run(config: &Config, store_dir: &Path) -> Result<Value, String> {
    let service_config = RuntimeServiceConfig {
        runtime: RuntimeConfig {
            max_global_isolates: config.available_cpus,
            max_isolates: config.available_cpus,
            ..RuntimeConfig::default()
        },
        storage: RuntimeStorageConfig {
            store_dir: store_dir.to_path_buf(),
            worker_store_enabled: true,
            ..RuntimeStorageConfig::default()
        },
    };
    let mut timings = serde_json::Map::new();
    let effective_config = json!({
        "runtime": format!("{:?}", service_config.runtime),
        "storage": format!("{:?}", service_config.storage),
    });
    let started = Instant::now();
    eprintln!("fanout-phase=startup");
    let service = RuntimeService::start_with_service_config(service_config.clone())
        .await
        .map_err(|error| format!("startup failed: {error}"))?;
    timings.insert(
        "startup_seconds".to_string(),
        json!(started.elapsed().as_secs_f64()),
    );
    let measured = async {
        let started = Instant::now();
        eprintln!("fanout-phase=deployment");
        service
            .deploy_with_config(
                WORKER.to_string(),
                format!(
                    "const payloadBytes = {}; const keysPerEntity = {}; const payloadKind = {};\n{WORKER_SOURCE}",
                    config.payload_bytes, config.keys_per_entity,
                    serde_json::to_string(&config.payload_kind).map_err(|error| error.to_string())?
                ),
                DeployConfig {
                    bindings: vec![DeployBinding::Memory {
                        binding: "MEMORY".to_string(),
                    }],
                    ..DeployConfig::default()
                },
            )
            .await
            .map_err(|error| format!("deployment failed: {error}"))?;
        timings.insert(
            "deployment_seconds".to_string(),
            json!(started.elapsed().as_secs_f64()),
        );
        let started = Instant::now();
        eprintln!("fanout-phase=seed");
        for start in (0..config.population).step_by(16) {
            invoke(
                &service,
                &RequestPlan {
                    operation: Operation::Seed,
                    sequence: start as u64,
                    entities: (start..(start + 16).min(config.population)).collect(),
                },
            )
            .await
            .map_err(|error| format!("seed failed: {error}"))?;
        }
        timings.insert(
            "seed_seconds".to_string(),
            json!(started.elapsed().as_secs_f64()),
        );
        let admin_after_seed = service.admin_snapshot().await;
        let sequence = Arc::new(AtomicU64::new(0));
        eprintln!("fanout-phase=warmup");
        let warmup = run_phase(&service, config, &sequence, config.warmup_ms)
            .await
            .map_err(|error| format!("warmup failed: {error}"))?;
        timings.insert("warmup_seconds".to_string(), json!(warmup.elapsed_seconds));
        let admin_before = service.admin_snapshot().await;
        let timed_sequence_start = sequence.load(Ordering::Relaxed);
        eprintln!("fanout-phase=timed");
        let timed = run_phase(&service, config, &sequence, config.duration_ms)
            .await
            .map_err(|error| format!("timed phase failed: {error}"))?;
        timings.insert("timed_seconds".to_string(), json!(timed.elapsed_seconds));
        eprintln!("fanout-phase=verification");
        let admin_after = service.admin_snapshot().await;
        let started = Instant::now();
        let mut writes = vec![Vec::<u64>::new(); config.population];
        for phase in [&warmup, &timed] {
            for lane in &phase.lanes {
                for (entity, count) in &lane.writes {
                    writes[*entity].push(*count);
                }
            }
        }
        let counts = writes
            .iter_mut()
            .enumerate()
            .map(|(entity, counts)| {
                counts.sort_unstable();
                for (index, count) in counts.iter().enumerate() {
                    if *count != index as u64 + 1 {
                        return Err(format!(
                            "entity {entity} write replies expected count {}, got {count}",
                            index + 1
                        ));
                    }
                }
                Ok(counts.len() as u64)
            })
            .collect::<Result<Vec<_>, String>>()?;
        for phase in [&warmup, &timed] {
            for lane in &phase.lanes {
                for (entity, count) in lane.highest_counts.iter().enumerate() {
                    if *count > counts[entity] {
                        return Err(format!(
                            "entity {entity} reply count {count} exceeds {} completed writes",
                            counts[entity]
                        ));
                    }
                }
            }
        }
        verify(&service, &counts)
            .await
            .map_err(|error| format!("state verification failed: {error}"))?;
        timings.insert(
            "verification_seconds".to_string(),
            json!(started.elapsed().as_secs_f64()),
        );
        Ok::<_, String>((
            json!({
                "warmup": warmup.summary(config.width),
                "timed": timed.summary(config.width),
                "timed_sequence_start": timed_sequence_start,
                "sequence_end": sequence.load(Ordering::Relaxed),
                "admin_after_seed": admin_after_seed,
                "admin_before_timed": admin_before,
                "admin_after_timed": admin_after,
            }),
            counts,
        ))
    }
    .await;
    let started = Instant::now();
    eprintln!("fanout-phase=shutdown");
    let shutdown = service.shutdown().await;
    drop(service);
    timings.insert(
        "shutdown_seconds".to_string(),
        json!(started.elapsed().as_secs_f64()),
    );
    let (mut measured, counts) = match (measured, shutdown) {
        (Ok(result), Ok(())) => result,
        (Err(error), Ok(())) => return Err(error),
        (Ok(_), Err(error)) => return Err(format!("shutdown failed: {error}")),
        (Err(error), Err(shutdown)) => return Err(format!("{error}; shutdown failed: {shutdown}")),
    };
    let started = Instant::now();
    eprintln!("fanout-phase=reopen");
    let reopened = RuntimeService::start_with_service_config(service_config)
        .await
        .map_err(|error| format!("reopen failed: {error}"))?;
    timings.insert(
        "reopen_seconds".to_string(),
        json!(started.elapsed().as_secs_f64()),
    );
    let started = Instant::now();
    eprintln!("fanout-phase=reopen-verification");
    let verified = verify(&reopened, &counts).await;
    timings.insert(
        "reopen_verification_seconds".to_string(),
        json!(started.elapsed().as_secs_f64()),
    );
    let started = Instant::now();
    eprintln!("fanout-phase=reopen-shutdown");
    let shutdown = reopened.shutdown().await;
    drop(reopened);
    timings.insert(
        "reopen_shutdown_seconds".to_string(),
        json!(started.elapsed().as_secs_f64()),
    );
    match (verified, shutdown) {
        (Ok(()), Ok(())) => {}
        (Err(error), Ok(())) => return Err(format!("reopened state verification failed: {error}")),
        (Ok(()), Err(error)) => return Err(format!("reopened shutdown failed: {error}")),
        (Err(error), Err(shutdown)) => {
            return Err(format!(
                "reopened state verification failed: {error}; shutdown failed: {shutdown}"
            ));
        }
    }
    measured["timings"] = Value::Object(timings);
    measured["effective_config"] = effective_config;
    measured["verification"] = json!({ "entities": counts.len(), "completed_writes": counts.iter().sum::<u64>(), "before_shutdown": true, "after_reopen": true });
    Ok(measured)
}

#[tokio::main]
async fn main() -> Result<(), String> {
    let arguments = std::env::args().skip(1).collect::<Vec<_>>();
    if arguments == ["--help"] || arguments == ["-h"] {
        println!(
            "In-process RuntimeService Promise.all memory benchmark. Configure with:\nDD_FANOUT_WIDTH=1|4|16 (default 1)\nDD_FANOUT_MODE=read|write|mixed (default read; mixed writes every tenth request)\nDD_FANOUT_POPULATION=1024 DD_FANOUT_PAYLOAD_BYTES=128\nDD_FANOUT_CONCURRENCY=4*available_CPUs\nDD_FANOUT_DURATION_MS=8000 DD_FANOUT_WARMUP_MS=2000\nA fresh store is retained under TMPDIR. Every response and all final/reopened entity counts are verified."
        );
        return Ok(());
    }
    if !arguments.is_empty() {
        return Err(format!(
            "unsupported arguments {arguments:?}; use --help or DD_FANOUT_* environment variables"
        ));
    }
    let config = Config::from_env()?;
    let store_dir = std::env::temp_dir().join(format!("dd-memory-fanout-{}", Uuid::new_v4()));
    tokio::fs::create_dir(&store_dir)
        .await
        .map_err(|error| format!("cannot create fresh store {}: {error}", store_dir.display()))?;
    eprintln!("memory fanout store: {}", store_dir.display());
    let measured = run(&config, &store_dir).await;
    let mut result = json!({
        "schema_version": 1,
        "scope": "in_process_runtime_service_with_response_validation",
        "config": config,
        "worker": WORKER,
        "binding": "MEMORY",
        "store_dir": store_dir,
        "regular_isolate_cap": config.available_cpus,
        "min_isolates": 0,
        "max_inflight_per_isolate": 4,
        "other_runtime_and_storage_settings": "defaults; outbox enabled",
        "entity_assignment": "(sequence * width + offset) % population; sequence spans warmup and timed phases",
        "mixed_write_selection": "sequence % 10 == (sequence / 10 / ceil(population / (10 * width))) % 10, using integer division; one write per full ten-request block, rotating its slot after each approximate entity sweep",
    });
    match measured {
        Ok(measured) => {
            result["ok"] = json!(true);
            result["request_throughput_rps"] = measured["timed"]["requests_per_second"].clone();
            result["transaction_throughput_rps"] =
                measured["timed"]["transactions_per_second"].clone();
            result["p99_ms"] = measured["timed"]["p99_ms"].clone();
            result["write_request_fraction"] = measured["timed"]["write_request_fraction"].clone();
            result["measurements"] = measured;
            println!("{result}");
            Ok(())
        }
        Err(error) => {
            result["ok"] = json!(false);
            result["error"] = json!(error);
            println!("{result}");
            Err(error)
        }
    }
}
