use super::*;

pub(crate) async fn run_and_print(
    service: &RuntimeService,
    label: &str,
    source: &str,
    seed: bool,
    path: &'static str,
    key_space: usize,
    verify_path: Option<&'static str>,
    profile_enabled: bool,
    options: &BenchOptions,
) -> Result<(), String> {
    let requests = env_usize("DD_BENCH_REQUESTS", 1_000);
    let concurrency = env_usize("DD_BENCH_CONCURRENCY", 1);
    let worker_name = format!("{label}-{}", Uuid::new_v4());
    let started_at = Instant::now();
    let watchdog_state = Arc::new(BenchWatchdogState::new());
    let watchdog_task = spawn_watchdog(
        label.to_string(),
        requests,
        started_at,
        *options,
        Arc::clone(&watchdog_state),
    );
    let mut timings = BenchTimings {
        deploy: Duration::ZERO,
        seed: Duration::ZERO,
        timed: Duration::ZERO,
        verify: Duration::ZERO,
        profile: Duration::ZERO,
        shutdown: Duration::ZERO,
    };

    let deploy_started = Instant::now();
    let deploy_result = service
        .deploy_with_config(
            worker_name.clone(),
            source.to_string(),
            DeployConfig {
                public: false,
                bindings: vec![DeployBinding::Memory {
                    binding: "BENCH_MEMORY".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await;
    timings.deploy = deploy_started.elapsed();
    if let Err(error) = deploy_result {
        set_watchdog_phase(&watchdog_state, started_at, BenchPhase::Failed);
        watchdog_state.stop.store(true, Ordering::Relaxed);
        watchdog_task.abort();
        println!(
            "bench-final label={} outcome=error phase={} message={}",
            label,
            BenchPhase::Deploy.as_str(),
            error
        );
        return Err(error.to_string());
    }

    if seed {
        set_watchdog_phase(&watchdog_state, started_at, BenchPhase::Seed);
        let seed_started = Instant::now();
        let seed_result = seed_benchmark_state(
            service,
            &worker_name,
            requests,
            key_space,
            options.request_timeout,
        )
        .await;
        timings.seed = seed_started.elapsed();
        if let Err(error) = seed_result {
            set_watchdog_phase(&watchdog_state, started_at, BenchPhase::Failed);
            watchdog_state.stop.store(true, Ordering::Relaxed);
            watchdog_task.abort();
            if profile_enabled {
                print_profile_on_failure(service, &worker_name, label).await;
            }
            print_debug_dump_on_failure(service, &worker_name, label).await;
            println!(
                "bench-final label={} outcome=error phase={} message={}",
                label,
                BenchPhase::Seed.as_str(),
                error
            );
            return Err(error.to_string());
        }
    }
    if profile_enabled {
        set_watchdog_phase(&watchdog_state, started_at, BenchPhase::Profile);
        let profile_reset_started = Instant::now();
        let profile_reset_result = service
            .invoke(worker_name.clone(), invocation("/__profile_reset", 0, 1))
            .await;
        timings.profile += profile_reset_started.elapsed();
        if let Err(error) = profile_reset_result {
            set_watchdog_phase(&watchdog_state, started_at, BenchPhase::Failed);
            watchdog_state.stop.store(true, Ordering::Relaxed);
            watchdog_task.abort();
            if profile_enabled {
                print_profile_on_failure(service, &worker_name, label).await;
            }
            print_debug_dump_on_failure(service, &worker_name, label).await;
            println!(
                "bench-final label={} outcome=error phase={} message={}",
                label,
                BenchPhase::Profile.as_str(),
                error
            );
            return Err(error.to_string());
        }
    }

    set_watchdog_phase(&watchdog_state, started_at, BenchPhase::Invoke);
    let result = run_scenario(
        service,
        &worker_name,
        Scenario {
            requests,
            concurrency,
            path,
            key_space,
        },
        options,
        Arc::clone(&watchdog_state),
        started_at,
    )
    .await;
    let result = match result {
        Ok(result) => {
            timings.timed = result.total_duration;
            result
        }
        Err(error) => {
            set_watchdog_phase(&watchdog_state, started_at, BenchPhase::Failed);
            watchdog_state.stop.store(true, Ordering::Relaxed);
            watchdog_task.abort();
            if profile_enabled {
                print_profile_on_failure(service, &worker_name, label).await;
            }
            print_debug_dump_on_failure(service, &worker_name, label).await;
            println!(
                "bench-final label={} outcome=error phase={} message={}",
                label,
                BenchPhase::Invoke.as_str(),
                error
            );
            return Err(error.to_string());
        }
    };
    if let Some(verify_path) = verify_path {
        set_watchdog_phase(&watchdog_state, started_at, BenchPhase::Verify);
        let expected =
            if verify_path == "/sum" || verify_path == "/sum-read" || verify_path == "/sum-blind" {
                distinct_memory_keys(requests, key_space).len().to_string()
            } else if verify_path == "/read" || verify_path == "/get-strong" {
                "1".to_string()
            } else if verify_path == "/get-blind" {
                "1".to_string()
            } else {
                requests.to_string()
            };
        let verify_started = Instant::now();
        let observed = timeout(
            options.verify_timeout,
            verify_expected_value(
                service,
                &worker_name,
                requests,
                key_space,
                verify_path,
                &expected,
                options.request_timeout,
            ),
        )
        .await;
        timings.verify = verify_started.elapsed();
        let observed = match observed {
            Ok(Ok(value)) => value,
            Ok(Err(error)) => {
                set_watchdog_phase(&watchdog_state, started_at, BenchPhase::Failed);
                watchdog_state.stop.store(true, Ordering::Relaxed);
                watchdog_task.abort();
                if profile_enabled {
                    print_profile_on_failure(service, &worker_name, label).await;
                }
                print_debug_dump_on_failure(service, &worker_name, label).await;
                println!(
                    "bench-final label={} outcome=error phase={} message={}",
                    label,
                    BenchPhase::Verify.as_str(),
                    error
                );
                return Err(error);
            }
            Err(_) => {
                let message = format!(
                    "{label} verification timed out after {}ms on {}",
                    options.verify_timeout.as_millis(),
                    verify_path
                );
                set_watchdog_phase(&watchdog_state, started_at, BenchPhase::Failed);
                watchdog_state.stop.store(true, Ordering::Relaxed);
                watchdog_task.abort();
                if profile_enabled {
                    print_profile_on_failure(service, &worker_name, label).await;
                }
                print_debug_dump_on_failure(service, &worker_name, label).await;
                println!(
                    "bench-final label={} outcome=error phase={} message={}",
                    label,
                    BenchPhase::Verify.as_str(),
                    message
                );
                return Err(message);
            }
        };
        if observed.trim() != expected {
            let message = format!(
                "{label} verification failed: expected final count {}, got {}",
                expected, observed
            );
            set_watchdog_phase(&watchdog_state, started_at, BenchPhase::Failed);
            watchdog_state.stop.store(true, Ordering::Relaxed);
            watchdog_task.abort();
            if profile_enabled {
                print_profile_on_failure(service, &worker_name, label).await;
            }
            print_debug_dump_on_failure(service, &worker_name, label).await;
            println!(
                "bench-final label={} outcome=error phase={} message={}",
                label,
                BenchPhase::Verify.as_str(),
                message
            );
            return Err(message);
        }
    }
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
    if profile_enabled {
        set_watchdog_phase(&watchdog_state, started_at, BenchPhase::Profile);
        let profile_started = Instant::now();
        let profile = match take_profile(service, &worker_name).await {
            Ok(profile) => profile,
            Err(error) => {
                set_watchdog_phase(&watchdog_state, started_at, BenchPhase::Failed);
                watchdog_state.stop.store(true, Ordering::Relaxed);
                watchdog_task.abort();
                if profile_enabled {
                    print_profile_on_failure(service, &worker_name, label).await;
                }
                print_debug_dump_on_failure(service, &worker_name, label).await;
                println!(
                    "bench-final label={} outcome=error phase={} message={}",
                    label,
                    BenchPhase::Profile.as_str(),
                    error
                );
                return Err(error);
            }
        };
        timings.profile += profile_started.elapsed();
        print_profile(&profile);
    }
    set_watchdog_phase(&watchdog_state, started_at, BenchPhase::Shutdown);
    timings.shutdown = Duration::ZERO;
    set_watchdog_phase(&watchdog_state, started_at, BenchPhase::Done);
    watchdog_state.stop.store(true, Ordering::Relaxed);
    watchdog_task.abort();
    println!(
        "bench-phases label={} deploy_ms={:.2} seed_ms={:.2} timed_ms={:.2} verify_ms={:.2} profile_ms={:.2} shutdown_ms={:.2} total_ms={:.2}",
        label,
        timings.deploy.as_secs_f64() * 1000.0,
        timings.seed.as_secs_f64() * 1000.0,
        timings.timed.as_secs_f64() * 1000.0,
        timings.verify.as_secs_f64() * 1000.0,
        timings.profile.as_secs_f64() * 1000.0,
        timings.shutdown.as_secs_f64() * 1000.0,
        started_at.elapsed().as_secs_f64() * 1000.0,
    );
    println!(
        "bench-final label={} outcome=success phase={} completed_requests={} total_requests={}",
        label,
        BenchPhase::Done.as_str(),
        watchdog_state.completed_requests.load(Ordering::Relaxed),
        requests,
    );
    Ok(())
}

async fn verify_expected_value(
    service: &RuntimeService,
    worker_name: &str,
    requests: usize,
    key_space: usize,
    verify_path: &str,
    expected: &str,
    request_timeout: Duration,
) -> Result<String, String> {
    let deadline = TokioInstant::now() + Duration::from_secs(2);
    loop {
        let observed =
            if verify_path == "/sum" || verify_path == "/sum-read" || verify_path == "/sum-blind" {
                verify_distinct_memory_sum(
                    service,
                    worker_name,
                    requests,
                    key_space,
                    verify_path,
                    request_timeout,
                )
                .await?
            } else {
                let verify = timeout(
                    request_timeout,
                    service.invoke(
                        worker_name.to_string(),
                        invocation(
                            if verify_path == "/get-blind" {
                                "/get"
                            } else {
                                verify_path
                            },
                            requests + 1,
                            1,
                        ),
                    ),
                )
                .await
                .map_err(|_| {
                    format!(
                        "verification invoke timed out after {}ms on {}",
                        request_timeout.as_millis(),
                        verify_path
                    )
                })?
                .map_err(|error| error.to_string())?;
                String::from_utf8(verify.body).map_err(|error| error.to_string())?
            };
        if observed.trim() == expected {
            return Ok(observed);
        }
        if TokioInstant::now() >= deadline {
            return Ok(observed);
        }
        sleep(Duration::from_millis(10)).await;
    }
}

async fn seed_benchmark_state(
    service: &RuntimeService,
    worker_name: &str,
    requests: usize,
    key_space: usize,
    request_timeout: Duration,
) -> Result<(), String> {
    if key_space <= 1 {
        timeout(
            request_timeout,
            service.invoke(worker_name.to_string(), invocation("/seed", 0, 1)),
        )
        .await
        .map_err(|_| {
            format!(
                "seed invoke timed out after {}ms on /seed",
                request_timeout.as_millis()
            )
        })?
        .map_err(|error| error.to_string())?;
        return Ok(());
    }

    for (offset, memory_key) in distinct_memory_keys(requests, key_space)
        .into_iter()
        .enumerate()
    {
        timeout(
            request_timeout,
            service.invoke(
                worker_name.to_string(),
                WorkerInvocation {
                    method: "GET".to_string(),
                    url: format!("http://worker/seed?key={memory_key}"),
                    headers: Vec::new(),
                    body: Vec::new(),
                    request_id: format!("bench-seed-{offset}"),
                },
            ),
        )
        .await
        .map_err(|_| {
            format!(
                "seed invoke timed out after {}ms on /seed key={}",
                request_timeout.as_millis(),
                memory_key
            )
        })?
        .map_err(|error| error.to_string())?;
    }
    Ok(())
}

async fn verify_distinct_memory_sum(
    service: &RuntimeService,
    worker_name: &str,
    requests: usize,
    key_space: usize,
    path: &str,
    request_timeout: Duration,
) -> Result<String, String> {
    let read_path = if path == "/sum-read" {
        "/get-strong"
    } else {
        "/get"
    };
    let mut total = 0usize;
    for (offset, memory_key) in distinct_memory_keys(requests, key_space)
        .into_iter()
        .enumerate()
    {
        let verify = timeout(
            request_timeout,
            service.invoke(
                worker_name.to_string(),
                WorkerInvocation {
                    method: "GET".to_string(),
                    url: format!("http://worker{read_path}?key={memory_key}"),
                    headers: Vec::new(),
                    body: Vec::new(),
                    request_id: format!("bench-verify-{offset}"),
                },
            ),
        )
        .await
        .map_err(|_| {
            format!(
                "verification sum invoke timed out after {}ms on {} key={}",
                request_timeout.as_millis(),
                read_path,
                memory_key
            )
        })?
        .map_err(|error| error.to_string())?;
        total += String::from_utf8(verify.body)
            .map_err(|error| error.to_string())?
            .trim()
            .parse::<usize>()
            .map_err(|error| error.to_string())?;
    }
    Ok(total.to_string())
}

pub(crate) async fn run_scenario(
    service: &RuntimeService,
    worker_name: &str,
    scenario: Scenario,
    options: &BenchOptions,
    watchdog_state: Arc<BenchWatchdogState>,
    started_at: Instant,
) -> common::Result<ScenarioResult> {
    let next = Arc::new(AtomicUsize::new(0));
    let latencies = Arc::new(tokio::sync::Mutex::new(Vec::with_capacity(
        scenario.requests,
    )));
    let scenario_started_at = Instant::now();
    let mut tasks = Vec::with_capacity(scenario.concurrency);

    for _ in 0..scenario.concurrency {
        let service = service.clone();
        let worker_name = worker_name.to_string();
        let next = Arc::clone(&next);
        let latencies = Arc::clone(&latencies);
        let path = scenario.path.to_string();
        let watchdog_state = Arc::clone(&watchdog_state);
        let request_timeout = options.request_timeout;
        tasks.push(tokio::spawn(async move {
            loop {
                let idx = next.fetch_add(1, Ordering::Relaxed);
                if idx >= scenario.requests {
                    break;
                }
                let invoke_started = Instant::now();
                let output = timeout(
                    request_timeout,
                    service.invoke(
                        worker_name.clone(),
                        invocation(&path, idx + 1, scenario.key_space),
                    ),
                )
                .await
                .map_err(|_| {
                    common::PlatformError::runtime(format!(
                        "benchmark invoke timed out after {}ms phase=invoke path={} request={}",
                        request_timeout.as_millis(),
                        path,
                        idx + 1
                    ))
                })??;
                if output.status != 200 {
                    return Err(common::PlatformError::runtime(format!(
                        "benchmark invoke failed with status {} on {}",
                        output.status, path
                    )));
                }
                latencies.lock().await.push(invoke_started.elapsed());
                record_watchdog_completion(&watchdog_state, started_at, idx + 1);
            }
            Ok::<(), common::PlatformError>(())
        }));
    }

    for task in tasks {
        task.await
            .map_err(|error| common::PlatformError::internal(error.to_string()))??;
    }

    let total_duration = scenario_started_at.elapsed();
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

pub(crate) fn invocation(path: &str, idx: usize, key_space: usize) -> WorkerInvocation {
    let memory_key_mode = MemoryKeyMode::from_env();
    let memory_key = memory_key(idx, key_space, memory_key_mode, MEMORY_NAMESPACE_SHARDS);

    let url = if key_space > 1 {
        let separator = if path.contains('?') { '&' } else { '?' };
        format!("http://worker{path}{separator}key={memory_key}")
    } else {
        format!("http://worker{path}")
    };
    WorkerInvocation {
        method: "GET".to_string(),
        url,
        headers: Vec::new(),
        body: Vec::new(),
        request_id: format!("bench-memory-{idx}"),
    }
}

fn memory_shard(memory_key: &str, namespace_shards: usize) -> usize {
    if namespace_shards <= 1 {
        return 0;
    }
    let mut hasher = DefaultHasher::new();
    memory_key.hash(&mut hasher);
    (hasher.finish() as usize) % namespace_shards
}

fn memory_key(
    idx: usize,
    key_space: usize,
    mode: MemoryKeyMode,
    namespace_shards: usize,
) -> String {
    if key_space == 1 {
        return "hot".to_string();
    }

    let memory_slot = idx % key_space;
    match mode {
        MemoryKeyMode::Pool => format!("bench-{memory_slot}"),
        MemoryKeyMode::Unique => format!("bench-{idx}"),
        MemoryKeyMode::SameShard => {
            let shard = memory_shard("bench-direct-same-shard-anchor", namespace_shards);
            memory_key_for_shard_occurrence(
                shard,
                memory_slot,
                namespace_shards,
                "bench-direct-sameshard",
            )
        }
        MemoryKeyMode::CrossShard => {
            let target_shard = memory_slot % namespace_shards;
            let occurrence = memory_slot / namespace_shards;
            memory_key_for_shard_occurrence(
                target_shard,
                occurrence,
                namespace_shards,
                "bench-direct-cross-shard",
            )
        }
    }
}

fn distinct_memory_keys(requests: usize, key_space: usize) -> Vec<String> {
    let memory_key_mode = MemoryKeyMode::from_env();
    let mut keys = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for idx in 1..=requests {
        let key = memory_key(idx, key_space, memory_key_mode, MEMORY_NAMESPACE_SHARDS);
        if seen.insert(key.clone()) {
            keys.push(key);
        }
    }
    keys
}

fn memory_key_for_shard_occurrence(
    target_shard: usize,
    occurrence: usize,
    namespace_shards: usize,
    prefix: &str,
) -> String {
    if namespace_shards <= 1 {
        return format!("{prefix}-{occurrence}");
    }
    let mut sequence = 0;
    let mut seen = 0;
    loop {
        let candidate = format!("{prefix}-{sequence}");
        if memory_shard(&candidate, namespace_shards) == target_shard {
            if seen == occurrence {
                return candidate;
            }
            seen += 1;
        }
        sequence += 1;
    }
}
