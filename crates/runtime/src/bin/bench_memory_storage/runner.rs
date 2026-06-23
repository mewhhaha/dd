use super::*;

pub(crate) struct BenchRun<'a> {
    pub(crate) service: &'a RuntimeService,
    pub(crate) label: &'a str,
    pub(crate) source: &'a str,
    pub(crate) seed: bool,
    pub(crate) path: &'static str,
    pub(crate) key_space: usize,
    pub(crate) verify_path: Option<&'static str>,
    pub(crate) profile_enabled: bool,
    pub(crate) options: &'a BenchOptions,
}

pub(crate) struct StorageBenchRun<'a> {
    pub(crate) label: &'a str,
    pub(crate) key_space: usize,
    pub(crate) options: &'a BenchOptions,
}

pub(crate) async fn run_and_print(run: BenchRun<'_>) -> Result<(), String> {
    let BenchRun {
        service,
        label,
        source,
        seed,
        path,
        key_space,
        verify_path,
        profile_enabled,
        options,
    } = run;
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
        let expected = if verify_path == "/sum" || verify_path == "/sum-read" {
            MemoryKeySet::from_env(key_space)
                .distinct_for_requests(requests)
                .len()
                .to_string()
        } else if verify_path == "/read" || verify_path == "/get-strong" {
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

pub(crate) async fn run_storage_write_and_print(run: StorageBenchRun<'_>) -> Result<(), String> {
    let StorageBenchRun {
        label,
        key_space,
        options,
    } = run;
    let requests = env_usize("DD_BENCH_REQUESTS", 1_000);
    let concurrency = env_usize("DD_BENCH_CONCURRENCY", 1);
    let paths = bench_paths("memory-storage-only");
    let store = MemoryStore::new(
        paths.store_dir.join("memory"),
        env_memory_namespace_shards(),
        4096,
        Duration::from_secs(60),
    )
    .await
    .map_err(|error| error.to_string())?;
    let started_at = Instant::now();
    let watchdog_state = Arc::new(BenchWatchdogState::new());
    let watchdog_task = spawn_watchdog(
        label.to_string(),
        requests,
        started_at,
        *options,
        Arc::clone(&watchdog_state),
    );
    let memory_keys = Arc::new(MemoryKeySet::from_env(key_space));
    set_watchdog_phase(&watchdog_state, started_at, BenchPhase::Invoke);
    let result = run_storage_write_scenario(
        store.clone(),
        requests,
        concurrency,
        Arc::clone(&memory_keys),
        options,
        Arc::clone(&watchdog_state),
        started_at,
    )
    .await
    .map_err(|error| {
        set_watchdog_phase(&watchdog_state, started_at, BenchPhase::Failed);
        error.to_string()
    })?;

    set_watchdog_phase(&watchdog_state, started_at, BenchPhase::Verify);
    let expected = memory_keys.distinct_for_requests(requests).len();
    let observed = verify_storage_distinct_memory_sum(
        &store,
        requests,
        Arc::clone(&memory_keys),
        options.request_timeout,
    )
    .await?;
    if observed != expected {
        set_watchdog_phase(&watchdog_state, started_at, BenchPhase::Failed);
        watchdog_state.stop.store(true, Ordering::Relaxed);
        watchdog_task.abort();
        println!(
            "bench-final label={} outcome=error phase={} message=storage verification failed: expected {}, got {}",
            label,
            BenchPhase::Verify.as_str(),
            expected,
            observed,
        );
        return Err(format!(
            "{label} storage verification failed: expected {expected}, got {observed}"
        ));
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
    set_watchdog_phase(&watchdog_state, started_at, BenchPhase::Done);
    watchdog_state.stop.store(true, Ordering::Relaxed);
    watchdog_task.abort();
    println!(
        "bench-phases label={} deploy_ms=0.00 seed_ms=0.00 timed_ms={:.2} verify_ms=0.00 profile_ms=0.00 shutdown_ms=0.00 total_ms={:.2}",
        label,
        result.total_duration.as_secs_f64() * 1000.0,
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

async fn run_storage_write_scenario(
    store: MemoryStore,
    requests: usize,
    concurrency: usize,
    memory_keys: Arc<MemoryKeySet>,
    options: &BenchOptions,
    watchdog_state: Arc<BenchWatchdogState>,
    started_at: Instant,
) -> common::Result<ScenarioResult> {
    let next = Arc::new(AtomicUsize::new(0));
    let latencies = Arc::new(tokio::sync::Mutex::new(Vec::with_capacity(requests)));
    let scenario_started_at = Instant::now();
    let mut tasks = Vec::with_capacity(concurrency);

    for _ in 0..concurrency {
        let store = store.clone();
        let next = Arc::clone(&next);
        let latencies = Arc::clone(&latencies);
        let memory_keys = Arc::clone(&memory_keys);
        let watchdog_state = Arc::clone(&watchdog_state);
        let request_timeout = options.request_timeout;
        tasks.push(tokio::spawn(async move {
            loop {
                let idx = next.fetch_add(1, Ordering::Relaxed);
                if idx >= requests {
                    break;
                }
                let memory_key = memory_keys.key(idx + 1);
                let invoke_started = Instant::now();
                timeout(
                    request_timeout,
                    store.apply_batch(
                        "BENCH_MEMORY",
                        &memory_key,
                        &[MemoryBatchMutation {
                            key: "payload".to_string(),
                            value: b"1".to_vec(),
                            encoding: "utf8".to_string(),
                            deleted: false,
                        }],
                        None,
                        &[],
                        None,
                    ),
                )
                .await
                .map_err(|_| {
                    common::PlatformError::runtime(format!(
                        "storage benchmark write timed out after {}ms request={}",
                        request_timeout.as_millis(),
                        idx + 1
                    ))
                })?
                .map_err(|error| common::PlatformError::runtime(error.to_string()))?;
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

    let throughput_rps = requests as f64 / total_duration.as_secs_f64();
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
        requests,
        concurrency,
        total_duration,
        throughput_rps,
        mean_ms,
        p50_ms: percentile_ms(&latencies, 0.50),
        p95_ms: percentile_ms(&latencies, 0.95),
        p99_ms: percentile_ms(&latencies, 0.99),
    })
}

async fn verify_storage_distinct_memory_sum(
    store: &MemoryStore,
    requests: usize,
    memory_keys: Arc<MemoryKeySet>,
    request_timeout: Duration,
) -> Result<usize, String> {
    let mut total = 0usize;
    for memory_key in memory_keys.distinct_for_requests(requests) {
        let point = timeout(
            request_timeout,
            store.point_read("BENCH_MEMORY", &memory_key, "payload"),
        )
        .await
        .map_err(|_| {
            format!(
                "storage verification timed out after {}ms key={}",
                request_timeout.as_millis(),
                memory_key
            )
        })?
        .map_err(|error| error.to_string())?;
        let value = point
            .record
            .and_then(|entry| String::from_utf8(entry.value).ok())
            .unwrap_or_default();
        total = total.saturating_add(value.trim().parse::<usize>().unwrap_or(0));
    }
    Ok(total)
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
        let observed = if verify_path == "/sum" || verify_path == "/sum-read" {
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
                    invocation(verify_path, requests + 1, 1),
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

    let memory_keys = MemoryKeySet::from_env(key_space);
    for (offset, memory_key) in memory_keys
        .distinct_for_requests(requests)
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
    let memory_keys = MemoryKeySet::from_env(key_space);
    let mut total = 0usize;
    for (offset, memory_key) in memory_keys
        .distinct_for_requests(requests)
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
    let memory_keys = Arc::new(MemoryKeySet::from_env(scenario.key_space));

    for _ in 0..scenario.concurrency {
        let service = service.clone();
        let worker_name = worker_name.to_string();
        let next = Arc::clone(&next);
        let latencies = Arc::clone(&latencies);
        let path = scenario.path.to_string();
        let watchdog_state = Arc::clone(&watchdog_state);
        let request_timeout = options.request_timeout;
        let memory_keys = Arc::clone(&memory_keys);
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
                        invocation_with_memory_keys(&path, idx + 1, &memory_keys),
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
    let memory_keys = MemoryKeySet::from_env(key_space);
    invocation_with_memory_keys(path, idx, &memory_keys)
}

fn invocation_with_memory_keys(
    path: &str,
    idx: usize,
    memory_keys: &MemoryKeySet,
) -> WorkerInvocation {
    let memory_key = memory_keys.key(idx);
    let url = if memory_keys.is_multi_key() {
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

struct MemoryKeySet {
    key_space: usize,
    mode: MemoryKeyMode,
    keys: Vec<String>,
}

impl MemoryKeySet {
    fn from_env(key_space: usize) -> Self {
        let mode = MemoryKeyMode::from_env();
        let namespace_shards = env_memory_namespace_shards();
        let keys = if key_space <= 1 || mode == MemoryKeyMode::Unique {
            Vec::new()
        } else {
            build_memory_keys(key_space, mode, namespace_shards)
        };
        Self {
            key_space,
            mode,
            keys,
        }
    }

    fn is_multi_key(&self) -> bool {
        self.key_space > 1
    }

    fn key(&self, idx: usize) -> String {
        if self.key_space <= 1 {
            return "hot".to_string();
        }
        if self.mode == MemoryKeyMode::Unique {
            return format!("bench-{idx}");
        }
        self.keys[idx % self.key_space].clone()
    }

    fn distinct_for_requests(&self, requests: usize) -> Vec<String> {
        let mut keys = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for idx in 1..=requests {
            let key = self.key(idx);
            if seen.insert(key.clone()) {
                keys.push(key);
            }
        }
        keys
    }
}

fn memory_shard(memory_key: &str, namespace_shards: usize) -> usize {
    stable_memory_shard_index(memory_key, namespace_shards)
}

fn build_memory_keys(
    key_space: usize,
    mode: MemoryKeyMode,
    namespace_shards: usize,
) -> Vec<String> {
    match mode {
        MemoryKeyMode::Pool => (0..key_space)
            .map(|memory_slot| format!("bench-{memory_slot}"))
            .collect(),
        MemoryKeyMode::Unique => Vec::new(),
        MemoryKeyMode::SameShard => {
            let shard = memory_shard("bench-direct-same-shard-anchor", namespace_shards);
            memory_keys_for_single_shard(
                key_space,
                shard,
                namespace_shards,
                "bench-direct-sameshard",
            )
        }
        MemoryKeyMode::CrossShard => {
            memory_keys_across_shards(key_space, namespace_shards, "bench-direct-cross-shard")
        }
        MemoryKeyMode::SkewedHotspot => memory_keys_skewed_hotspot(key_space, namespace_shards),
    }
}

fn memory_keys_for_single_shard(
    key_space: usize,
    target_shard: usize,
    namespace_shards: usize,
    prefix: &str,
) -> Vec<String> {
    if namespace_shards <= 1 {
        return (0..key_space)
            .map(|occurrence| format!("{prefix}-{occurrence}"))
            .collect();
    }
    let mut keys = Vec::with_capacity(key_space);
    let mut sequence = 0usize;
    while keys.len() < key_space {
        let candidate = format!("{prefix}-{sequence}");
        if memory_shard(&candidate, namespace_shards) == target_shard {
            keys.push(candidate);
        }
        sequence += 1;
    }
    keys
}

fn memory_keys_across_shards(
    key_space: usize,
    namespace_shards: usize,
    prefix: &str,
) -> Vec<String> {
    if namespace_shards <= 1 {
        return (0..key_space)
            .map(|occurrence| format!("{prefix}-{occurrence}"))
            .collect();
    }

    let mut keys = vec![String::new(); key_space];
    let mut seen_by_shard = vec![0usize; namespace_shards];
    let mut filled = 0usize;
    let mut sequence = 0usize;
    while filled < key_space {
        let candidate = format!("{prefix}-{sequence}");
        let shard = memory_shard(&candidate, namespace_shards);
        let occurrence = seen_by_shard[shard];
        seen_by_shard[shard] += 1;
        let memory_slot = shard + occurrence * namespace_shards;
        if memory_slot < key_space {
            keys[memory_slot] = candidate;
            filled += 1;
        }
        sequence += 1;
    }
    keys
}

fn memory_keys_skewed_hotspot(key_space: usize, namespace_shards: usize) -> Vec<String> {
    let hot_key_space = ((key_space.saturating_mul(4)) / 5).clamp(1, key_space);
    let cold_key_space = key_space.saturating_sub(hot_key_space);
    let hot_shard = memory_shard("bench-skewed-hotspot-anchor", namespace_shards);
    let mut keys = memory_keys_for_single_shard(
        hot_key_space,
        hot_shard,
        namespace_shards,
        "bench-skewed-hot",
    );
    keys.extend(memory_keys_across_shards(
        cold_key_space,
        namespace_shards,
        "bench-skewed-cold",
    ));
    keys
}
