use super::*;

#[tokio::test]
#[serial]
async fn memory_same_key_atomic_commands_are_serialized() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 3,
        max_inflight_per_isolate: 4,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy_with_config(
            "memory".to_string(),
            memory_worker(),
            DeployConfig {
                egress_allow_hosts: Vec::new(),
                public: false,
                cache: Default::default(),
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "MY_MEMORY".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    let warmup = service
        .invoke(
            "memory".to_string(),
            test_invocation_with_path("/run?key=user-1&spin=2000", "memory-run-warmup"),
        )
        .await
        .expect("warmup invoke should succeed");
    assert_eq!(warmup.status, 200);

    let single_started = Instant::now();
    let measured = service
        .invoke(
            "memory".to_string(),
            test_invocation_with_path("/run?key=user-1&spin=2000", "memory-run-measured"),
        )
        .await
        .expect("measured invoke should succeed");
    assert_eq!(measured.status, 200);
    let minimum_serial_elapsed = single_started.elapsed().saturating_mul(3);

    let started = Instant::now();
    let mut tasks = Vec::new();
    for idx in 0..8 {
        let svc = service.clone();
        tasks.push(tokio::spawn(async move {
            svc.invoke(
                "memory".to_string(),
                test_invocation_with_path(
                    "/run?key=user-1&spin=2000",
                    &format!("memory-run-{idx}"),
                ),
            )
            .await
        }));
    }
    for task in tasks {
        let output = task.await.expect("join").expect("invoke should succeed");
        assert_eq!(output.status, 200);
    }
    let elapsed = started.elapsed();
    assert!(
        elapsed >= minimum_serial_elapsed,
        "expected same-key memory atomic commands to serialize, elapsed={elapsed:?}, single={minimum_serial_elapsed:?}"
    );
}

#[tokio::test]
#[serial]
async fn memory_storage_increment_preserves_all_updates_under_concurrency() {
    let service = test_service(RuntimeConfig {
        min_isolates: 2,
        max_isolates: 3,
        max_inflight_per_isolate: 1,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy_with_config(
            "memory".to_string(),
            memory_worker(),
            DeployConfig {
                egress_allow_hosts: Vec::new(),
                public: false,
                cache: Default::default(),
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "MY_MEMORY".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    service
        .invoke(
            "memory".to_string(),
            test_invocation_with_path("/seed?key=user-3", "seed"),
        )
        .await
        .expect("seed should succeed");

    let mut tasks = Vec::new();
    let increments = 8usize;
    for idx in 0..increments {
        let svc = service.clone();
        tasks.push(tokio::spawn(async move {
            svc.invoke(
                "memory".to_string(),
                test_invocation_with_path("/inc-cas?key=user-3", &format!("cas-{idx}")),
            )
            .await
        }));
    }

    for task in tasks {
        let output = task.await.expect("join").expect("invoke should succeed");
        assert_eq!(output.status, 200);
    }

    let current = service
        .invoke(
            "memory".to_string(),
            test_invocation_with_path("/get?key=user-3", "get-after-inc"),
        )
        .await
        .expect("get should succeed");
    assert_eq!(
        String::from_utf8(current.body).expect("utf8"),
        increments.to_string()
    );
}

#[tokio::test]
#[serial]
async fn memory_callbacks_progress_when_all_caller_isolates_are_busy() {
    let service = test_service(RuntimeConfig {
        min_isolates: 0,
        max_global_isolates: 2,
        max_isolates: 8,
        max_inflight_per_isolate: 4,
        request_wall_timeout: Duration::from_secs(5),
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy_with_config(
            "memory".to_string(),
            memory_worker(),
            DeployConfig {
                egress_allow_hosts: Vec::new(),
                public: false,
                cache: Default::default(),
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "MY_MEMORY".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    let mut tasks = Vec::new();
    for idx in 0..8 {
        let svc = service.clone();
        tasks.push(tokio::spawn(async move {
            svc.invoke(
                "memory".to_string(),
                test_invocation_with_path(
                    &format!("/inc-cas?key=callback-{idx}"),
                    &format!("callback-{idx}"),
                ),
            )
            .await
        }));
    }

    timeout(Duration::from_secs(10), async {
        for task in tasks {
            let output = task.await.expect("join").expect("invoke should succeed");
            assert_eq!(output.status, 200);
        }
    })
    .await
    .expect("memory callback invokes should not wait on saturated caller isolates");

    let stats = service
        .stats("memory".to_string())
        .await
        .expect("memory worker stats should exist");
    assert!(stats.global_isolates_total <= 4);
    assert_eq!(stats.global_internal_rescue_isolates, 0);

    for idx in 0..8 {
        let output = service
            .invoke(
                "memory".to_string(),
                test_invocation_with_path(
                    &format!("/get?key=callback-{idx}"),
                    &format!("callback-get-{idx}"),
                ),
            )
            .await
            .expect("get should succeed");
        assert_eq!(String::from_utf8(output.body).expect("utf8"), "1");
    }
}

#[tokio::test]
#[serial]
async fn memory_atomic_callback_executes_once_for_cold_read() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 4,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy_with_config(
            "memory".to_string(),
            memory_worker(),
            DeployConfig {
                egress_allow_hosts: Vec::new(),
                public: false,
                cache: Default::default(),
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "MY_MEMORY".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    let output = service
        .invoke(
            "memory".to_string(),
            test_invocation_with_path(
                "/single-execution-read?key=user-cold-single",
                "single-execution-read",
            ),
        )
        .await
        .expect("invoke should succeed");
    assert_eq!(output.status, 200);
    assert_eq!(String::from_utf8(output.body).expect("utf8"), "missing:1");
}

#[tokio::test]
#[serial]
async fn memory_atomic_rejects_unsupported_storage_options() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 4,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy_with_config(
            "memory".to_string(),
            memory_worker(),
            DeployConfig {
                egress_allow_hosts: Vec::new(),
                public: false,
                cache: Default::default(),
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "MY_MEMORY".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    for operation in ["get", "set", "delete"] {
        let output = service
            .invoke(
                "memory".to_string(),
                test_invocation_with_path(
                    &format!(
                        "/atomic-unsupported-option?key=user-option-{operation}&operation={operation}"
                    ),
                    &format!("atomic-unsupported-option-{operation}"),
                ),
            )
            .await
            .expect("invoke should succeed");
        assert_eq!(output.status, 418);
        let body = String::from_utf8(output.body).expect("body should be utf8");
        assert!(
            body.contains(&format!(
                "memory atomic {operation} options are unsupported"
            )),
            "body was {body}"
        );
    }
}

#[tokio::test]
#[serial]
async fn memory_atomic_staged_writes_do_not_expose_committed_versions() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 4,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy_with_config(
            "memory".to_string(),
            memory_worker(),
            DeployConfig {
                egress_allow_hosts: Vec::new(),
                public: false,
                cache: Default::default(),
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "MY_MEMORY".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    let output = service
        .invoke(
            "memory".to_string(),
            test_invocation_with_path(
                "/multi-write-versions?key=user-staged-versions",
                "multi-write-versions",
            ),
        )
        .await
        .expect("invoke should succeed");
    assert_eq!(output.status, 200);
    assert_eq!(
        String::from_utf8(output.body).expect("body should be utf8"),
        "alpha:1:undefined,beta:2:undefined"
    );
}

#[tokio::test]
#[serial]
async fn memory_atomic_idempotency_key_replays_committed_result() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 2,
        max_inflight_per_isolate: 4,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy_with_config(
            "memory".to_string(),
            memory_worker(),
            DeployConfig {
                egress_allow_hosts: Vec::new(),
                public: false,
                cache: Default::default(),
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "MY_MEMORY".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    let first = service
        .invoke(
            "memory".to_string(),
            test_invocation_with_path(
                "/idempotent-inc?key=user-idempotent&command=cmd-1&amount=1",
                "idempotent-first",
            ),
        )
        .await
        .expect("first invoke should succeed");
    let second = service
        .invoke(
            "memory".to_string(),
            test_invocation_with_path(
                "/idempotent-inc?key=user-idempotent&command=cmd-1&amount=10",
                "idempotent-second",
            ),
        )
        .await
        .expect("second invoke should succeed");
    assert_eq!(first.status, 200);
    assert_eq!(second.status, 200);
    assert_eq!(first.body, second.body);

    let payload: Value = crate::json::from_string(
        String::from_utf8(second.body).expect("idempotent body should be utf8"),
    )
    .expect("idempotent response should parse");
    assert_eq!(payload["next"].as_i64(), Some(1));
    assert_eq!(payload["attempts"].as_i64(), Some(1));

    let current = service
        .invoke(
            "memory".to_string(),
            test_invocation_with_path("/get?key=user-idempotent", "idempotent-current"),
        )
        .await
        .expect("get should succeed");
    assert_eq!(String::from_utf8(current.body).expect("utf8"), "1");
}

#[tokio::test]
#[serial]
async fn memory_atomic_idempotency_key_replays_after_runtime_restart() {
    let root = PathBuf::from(format!("/tmp/dd-memory-idempotent-{}", Uuid::new_v4()));
    let config = RuntimeConfig {
        min_isolates: 1,
        max_isolates: 2,
        max_inflight_per_isolate: 4,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    };

    let service = test_service_with_paths(config.clone(), root.clone(), true).await;
    service
        .deploy_with_config(
            "memory".to_string(),
            memory_worker(),
            DeployConfig {
                egress_allow_hosts: Vec::new(),
                public: false,
                cache: Default::default(),
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "MY_MEMORY".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    let first = service
        .invoke(
            "memory".to_string(),
            test_invocation_with_path(
                "/idempotent-inc?key=user-idempotent-restart&command=cmd-restart&amount=1",
                "idempotent-restart-first",
            ),
        )
        .await
        .expect("first invoke should succeed");
    assert_eq!(first.status, 200);
    service.shutdown().await.expect("service should shut down");
    drop(service);

    let restored = test_service_with_paths(config, root.clone(), true).await;
    let replay = restored
        .invoke(
            "memory".to_string(),
            test_invocation_with_path(
                "/idempotent-inc?key=user-idempotent-restart&command=cmd-restart&amount=10",
                "idempotent-restart-replay",
            ),
        )
        .await
        .expect("replay invoke should succeed");
    assert_eq!(replay.status, 200);
    assert_eq!(first.body, replay.body);

    let payload: Value = crate::json::from_string(
        String::from_utf8(replay.body).expect("idempotent restart body should be utf8"),
    )
    .expect("idempotent restart response should parse");
    assert_eq!(payload["next"].as_i64(), Some(1));
    assert_eq!(payload["attempts"].as_i64(), Some(1));

    let current = restored
        .invoke(
            "memory".to_string(),
            test_invocation_with_path(
                "/get?key=user-idempotent-restart",
                "idempotent-restart-current",
            ),
        )
        .await
        .expect("get should succeed");
    assert_eq!(String::from_utf8(current.body).expect("utf8"), "1");
    restored
        .shutdown()
        .await
        .expect("restored should shut down");
    let _ = tokio::fs::remove_dir_all(root).await;
}

#[tokio::test]
#[serial]
async fn memory_atomic_concurrent_duplicate_idempotency_key_executes_once() {
    let service = test_service(RuntimeConfig {
        min_isolates: 2,
        max_isolates: 3,
        max_inflight_per_isolate: 4,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy_with_config(
            "memory".to_string(),
            memory_worker(),
            DeployConfig {
                egress_allow_hosts: Vec::new(),
                public: false,
                cache: Default::default(),
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "MY_MEMORY".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    let mut tasks = Vec::new();
    for idx in 0..12 {
        let svc = service.clone();
        tasks.push(tokio::spawn(async move {
            svc.invoke(
                "memory".to_string(),
                test_invocation_with_path(
                    &format!(
                        "/idempotent-inc?key=user-idempotent-race&command=cmd-race&amount={}",
                        idx + 1
                    ),
                    &format!("idempotent-race-{idx}"),
                ),
            )
            .await
        }));
    }

    let mut bodies = Vec::new();
    for task in tasks {
        let output = task.await.expect("join").expect("invoke should succeed");
        assert_eq!(output.status, 200);
        bodies.push(output.body);
    }
    let first = bodies.first().expect("at least one response").clone();
    assert!(
        bodies.iter().all(|body| body == &first),
        "all duplicate command responses should replay the first committed result"
    );

    let payload: Value = crate::json::from_string(
        String::from_utf8(first).expect("idempotent race body should be utf8"),
    )
    .expect("idempotent race response should parse");
    let committed_next = payload["next"]
        .as_i64()
        .expect("committed next value should be numeric");
    assert!((1..=12).contains(&committed_next));
    assert_eq!(payload["attempts"].as_i64(), Some(1));

    let current = service
        .invoke(
            "memory".to_string(),
            test_invocation_with_path("/get?key=user-idempotent-race", "idempotent-race-current"),
        )
        .await
        .expect("get should succeed");
    assert_eq!(
        String::from_utf8(current.body).expect("utf8"),
        committed_next.to_string()
    );
}

#[tokio::test]
#[serial]
async fn memory_atomic_idempotency_key_replays_read_only_result() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 2,
        max_inflight_per_isolate: 4,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy_with_config(
            "memory".to_string(),
            memory_worker(),
            DeployConfig {
                egress_allow_hosts: Vec::new(),
                public: false,
                cache: Default::default(),
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "MY_MEMORY".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    let first = service
        .invoke(
            "memory".to_string(),
            test_invocation_with_path(
                "/idempotent-read?key=user-idempotent-read&command=read-1",
                "idempotent-read-first",
            ),
        )
        .await
        .expect("first read invoke should succeed");

    service
        .invoke(
            "memory".to_string(),
            test_invocation_with_path(
                "/idempotent-inc?key=user-idempotent-read&command=write-1&amount=5",
                "idempotent-read-mutator",
            ),
        )
        .await
        .expect("mutating invoke should succeed");

    let second = service
        .invoke(
            "memory".to_string(),
            test_invocation_with_path(
                "/idempotent-read?key=user-idempotent-read&command=read-1",
                "idempotent-read-second",
            ),
        )
        .await
        .expect("second read invoke should succeed");
    assert_eq!(first.status, 200);
    assert_eq!(second.status, 200);
    assert_eq!(first.body, second.body);

    let payload: Value = crate::json::from_string(
        String::from_utf8(second.body).expect("idempotent read body should be utf8"),
    )
    .expect("idempotent read response should parse");
    assert_eq!(payload["current"].as_i64(), Some(0));
    assert_eq!(payload["attempts"].as_i64(), Some(1));
}

#[tokio::test]
#[serial]
async fn memory_atomic_can_emit_durable_effect_records() {
    let root = PathBuf::from(format!("/tmp/dd-memory-outbox-{}", Uuid::new_v4()));
    let service = test_service_with_paths(
        RuntimeConfig {
            min_isolates: 1,
            max_isolates: 2,
            max_inflight_per_isolate: 4,
            idle_ttl: Duration::from_secs(5),
            scale_tick: Duration::from_millis(50),
            queue_warn_thresholds: vec![10],
            ..RuntimeConfig::default()
        },
        root.clone(),
        false,
    )
    .await;

    service
        .deploy_with_config(
            "memory".to_string(),
            memory_worker(),
            DeployConfig {
                egress_allow_hosts: Vec::new(),
                public: false,
                cache: Default::default(),
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "MY_MEMORY".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    let output = service
        .invoke(
            "memory".to_string(),
            test_invocation_with_path("/emit-effect?key=user-effect", "emit-effect"),
        )
        .await
        .expect("emit invoke should succeed");
    assert_eq!(output.status, 200);
    let payload: Value =
        crate::json::from_string(String::from_utf8(output.body).expect("emit body should be utf8"))
            .expect("emit response should parse");
    assert_eq!(payload["next"].as_i64(), Some(1));

    let current = service
        .invoke(
            "memory".to_string(),
            test_invocation_with_path("/get?key=user-effect", "emit-current"),
        )
        .await
        .expect("get should succeed");
    assert_eq!(String::from_utf8(current.body).expect("utf8"), "1");

    let memory_store = service.memory_store.clone();
    let outbox = timeout(Duration::from_secs(2), async {
        loop {
            let outbox = memory_store
                .outbox_records("6:memoryMY_MEMORY", "user-effect")
                .await
                .expect("outbox records should load");
            if outbox.len() == 2 && outbox.iter().all(|record| record.status == "delivered") {
                break outbox;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("outbox effects should drain after response completion");
    assert_eq!(outbox.len(), 2);
    assert!(outbox.iter().all(|record| record.status == "delivered"));
    assert!(
        outbox
            .iter()
            .all(|record| record.kind.starts_with("audit."))
    );
    let _ = tokio::fs::remove_dir_all(root).await;
}

#[tokio::test]
#[serial]
async fn memory_profile_reports_callback_commit_and_outbox_delivery() {
    let root = PathBuf::from(format!("/tmp/dd-memory-profile-atomic-{}", Uuid::new_v4()));
    let service = test_service_with_paths(
        RuntimeConfig {
            min_isolates: 1,
            max_isolates: 2,
            max_inflight_per_isolate: 4,
            idle_ttl: Duration::from_secs(5),
            scale_tick: Duration::from_millis(50),
            queue_warn_thresholds: vec![10],
            memory_profile_enabled: true,
            ..RuntimeConfig::default()
        },
        root.clone(),
        false,
    )
    .await;

    service
        .deploy_with_config(
            "memory".to_string(),
            memory_worker(),
            DeployConfig {
                egress_allow_hosts: Vec::new(),
                public: false,
                cache: Default::default(),
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "MY_MEMORY".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    service
        .invoke(
            "memory".to_string(),
            test_invocation_with_path("/__profile_reset", "atomic-profile-reset"),
        )
        .await
        .expect("profile reset should succeed");

    let output = service
        .invoke(
            "memory".to_string(),
            test_invocation_with_path("/emit-effect?key=profile-effect", "atomic-profile-effect"),
        )
        .await
        .expect("emit invoke should succeed");
    assert_eq!(output.status, 200);

    let memory_store = service.memory_store.clone();
    timeout(Duration::from_secs(2), async {
        loop {
            let outbox = memory_store
                .outbox_records("6:memoryMY_MEMORY", "profile-effect")
                .await
                .expect("outbox records should load");
            if outbox.len() == 2 && outbox.iter().all(|record| record.status == "delivered") {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("profile outbox effects should drain");

    let profile = service
        .invoke(
            "memory".to_string(),
            test_invocation_with_path("/__profile", "atomic-profile"),
        )
        .await
        .expect("profile should succeed");
    let profile: Value = crate::json::from_string(
        String::from_utf8(profile.body).expect("profile body should be utf8"),
    )
    .expect("profile should parse");
    let snapshot = &profile["snapshot"];
    for metric in [
        "js_txn_begin",
        "js_txn_commit",
        "op_apply_batch",
        "runtime_outbox_drain",
    ] {
        assert!(
            snapshot[metric]["calls"].as_u64().unwrap_or_default() >= 1,
            "expected {metric} to record at least one call: {snapshot:?}"
        );
    }
    assert!(
        snapshot["runtime_outbox_drain"]["total_items"]
            .as_u64()
            .unwrap_or_default()
            >= 2
    );

    let _ = tokio::fs::remove_dir_all(root).await;
}

#[tokio::test]
#[serial]
async fn memory_outbox_pending_effects_drain_after_service_start() {
    let root = PathBuf::from(format!("/tmp/dd-memory-outbox-startup-{}", Uuid::new_v4()));
    let state = storage::state::StateStore::open(root.join("state"))
        .await
        .expect("state opens");
    let seed_store = crate::memory::MemoryStore::from_state(state);
    let effects = (0..70)
        .map(|index| crate::memory::MemoryOutboxEffectWrite {
            kind: "audit.startup".to_string(),
            payload: format!(r#"{{"phase":"startup","index":{index}}}"#).into_bytes(),
        })
        .collect::<Vec<_>>();
    seed_store
        .apply_batch(
            "6:memoryMY_MEMORY",
            "startup-effect",
            storage::memory::MemoryCommit {
                outbox_effects: effects.clone(),
                ..Default::default()
            },
        )
        .await
        .expect("outbox seed should commit");
    let seeded = seed_store
        .outbox_records("6:memoryMY_MEMORY", "startup-effect")
        .await
        .expect("seeded outbox should load");
    assert_eq!(seeded.len(), effects.len());
    assert!(seeded.iter().all(|record| record.status == "pending"));
    drop(seed_store);

    let service = test_service_with_paths(
        RuntimeConfig {
            min_isolates: 0,
            max_isolates: 1,
            max_inflight_per_isolate: 1,
            idle_ttl: Duration::from_secs(5),
            scale_tick: Duration::from_secs(60),
            queue_warn_thresholds: vec![10],
            ..RuntimeConfig::default()
        },
        root.clone(),
        false,
    )
    .await;

    let check_store = service.memory_store.clone();
    timeout(Duration::from_secs(2), async {
        loop {
            let records = check_store
                .outbox_records("6:memoryMY_MEMORY", "startup-effect")
                .await
                .expect("outbox should load");
            if records.iter().all(|record| record.status == "delivered") {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("pending outbox should drain after service start");

    service.shutdown().await.expect("service should shutdown");
    let _ = tokio::fs::remove_dir_all(root).await;
}

#[tokio::test]
#[serial]
async fn memory_storage_different_keys_preserve_all_updates_under_concurrency() {
    let service = test_service(RuntimeConfig {
        min_isolates: 2,
        max_isolates: 8,
        max_inflight_per_isolate: 4,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy_with_config(
            "memory".to_string(),
            memory_worker(),
            DeployConfig {
                egress_allow_hosts: Vec::new(),
                public: false,
                cache: Default::default(),
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "MY_MEMORY".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    let keys = (0..8usize)
        .map(|index| format!("user-wide-{index}"))
        .collect::<Vec<_>>();
    for key in &keys {
        service
            .invoke(
                "memory".to_string(),
                test_invocation_with_path(&format!("/seed?key={key}"), &format!("seed-{key}")),
            )
            .await
            .expect("seed should succeed");
    }

    let mut tasks = Vec::new();
    for key in &keys {
        let svc = service.clone();
        let key = key.clone();
        tasks.push(tokio::spawn(async move {
            svc.invoke(
                "memory".to_string(),
                test_invocation_with_path(&format!("/inc-cas?key={key}"), &format!("inc-{key}")),
            )
            .await
        }));
    }
    for task in tasks {
        let output = task.await.expect("join").expect("invoke should succeed");
        assert_eq!(output.status, 200);
    }

    let mut total = 0usize;
    for key in &keys {
        let current = service
            .invoke(
                "memory".to_string(),
                test_invocation_with_path(&format!("/get?key={key}"), &format!("get-{key}")),
            )
            .await
            .expect("get should succeed");
        let value = String::from_utf8(current.body)
            .expect("utf8")
            .parse::<usize>()
            .expect("count should parse");
        assert_eq!(value, 1);
        total += value;
    }
    assert_eq!(total, keys.len());
}

#[tokio::test]
#[serial]
async fn memory_storage_structured_value_roundtrip_works() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 2,
        max_inflight_per_isolate: 4,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy_with_config(
            "memory".to_string(),
            memory_worker(),
            DeployConfig {
                egress_allow_hosts: Vec::new(),
                public: false,
                cache: Default::default(),
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "MY_MEMORY".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    let roundtrip = service
        .invoke(
            "memory".to_string(),
            test_invocation_with_path("/value-roundtrip?key=user-4", "value-roundtrip"),
        )
        .await
        .expect("roundtrip invoke should succeed");
    assert_eq!(roundtrip.status, 200);
    assert_eq!(String::from_utf8(roundtrip.body).expect("utf8"), "ok");

    let guard = service
        .invoke(
            "memory".to_string(),
            test_invocation_with_path("/value-string-get-guard?key=user-5", "value-guard"),
        )
        .await
        .expect("guard invoke should succeed");
    assert_eq!(guard.status, 200);
    assert_eq!(String::from_utf8(guard.body).expect("utf8"), "ok");

    let visibility = service
        .invoke(
            "memory".to_string(),
            test_invocation_with_path("/local-visibility?key=user-6", "value-visibility"),
        )
        .await
        .expect("visibility invoke should succeed");
    assert_eq!(visibility.status, 200);
    assert_eq!(String::from_utf8(visibility.body).expect("utf8"), "ok");
}

#[tokio::test]
#[serial]
async fn memory_committed_writes_are_visible_to_later_transactions() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 4,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy_with_config(
            "memory".to_string(),
            memory_worker(),
            DeployConfig {
                egress_allow_hosts: Vec::new(),
                public: false,
                cache: Default::default(),
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "MY_MEMORY".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    let set = service
        .invoke(
            "memory".to_string(),
            test_invocation_with_path("/put?key=user-transaction-1", "transaction-set"),
        )
        .await
        .expect("transaction put should succeed");
    assert_eq!(set.status, 200);

    let after_set = service
        .invoke(
            "memory".to_string(),
            test_invocation_with_path("/get?key=user-transaction-1", "transaction-get-set"),
        )
        .await
        .expect("transaction get after set should succeed");
    assert_eq!(after_set.status, 200);
    assert_eq!(String::from_utf8(after_set.body).expect("utf8"), "5");

    let delete = service
        .invoke(
            "memory".to_string(),
            test_invocation_with_path("/delete?key=user-transaction-1", "transaction-delete"),
        )
        .await
        .expect("transaction delete should succeed");
    assert_eq!(delete.status, 200);

    let after_delete = service
        .invoke(
            "memory".to_string(),
            test_invocation_with_path("/get?key=user-transaction-1", "transaction-get-delete"),
        )
        .await
        .expect("transaction get after delete should succeed");
    assert_eq!(after_delete.status, 200);
    assert_eq!(String::from_utf8(after_delete.body).expect("utf8"), "0");
}

#[tokio::test]
#[serial]
async fn memory_writes_preserve_updates_to_distinct_entities() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 8,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy_with_config(
            "memory".to_string(),
            memory_worker(),
            DeployConfig {
                egress_allow_hosts: Vec::new(),
                public: false,
                cache: Default::default(),
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "MY_MEMORY".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    let keys = (0..5)
        .map(|idx| format!("user-transaction-distinct-{idx}"))
        .collect::<Vec<_>>();
    for key in &keys {
        let set = service
            .invoke(
                "memory".to_string(),
                test_invocation_with_path(
                    &format!("/put?key={key}"),
                    &format!("transaction-set-{key}"),
                ),
            )
            .await
            .expect("transaction put should succeed");
        assert_eq!(set.status, 200);
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    let mut observed_total;
    loop {
        observed_total = 0;
        for key in &keys {
            let output = service
                .invoke(
                    "memory".to_string(),
                    test_invocation_with_path(
                        &format!("/get?key={key}"),
                        &format!("transaction-get-{key}"),
                    ),
                )
                .await
                .expect("transaction get should succeed");
            assert_eq!(output.status, 200);
            observed_total += String::from_utf8(output.body)
                .expect("utf8")
                .parse::<usize>()
                .expect("transaction value should parse");
        }
        if observed_total == keys.len() * 5 {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert_eq!(observed_total, keys.len() * 5);
}

#[tokio::test]
#[serial]
async fn memory_coordinated_set_write_uses_owner_validated_batch_path() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 4,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        memory_profile_enabled: true,
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy_with_config(
            "memory".to_string(),
            memory_worker(),
            DeployConfig {
                egress_allow_hosts: Vec::new(),
                public: false,
                cache: Default::default(),
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "MY_MEMORY".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    service
        .invoke(
            "memory".to_string(),
            test_invocation_with_path("/__profile_reset", "memory-set-profile-reset"),
        )
        .await
        .expect("profile reset should succeed");

    let write = service
        .invoke(
            "memory".to_string(),
            test_invocation_with_path(
                "/atomic-set-write?key=user-set-memory&value=7",
                "memory-set-write",
            ),
        )
        .await
        .expect("memory set write should succeed");
    assert_eq!(write.status, 200);
    assert_eq!(String::from_utf8(write.body).expect("utf8"), "7");

    let output = service
        .invoke(
            "memory".to_string(),
            test_invocation_with_path("/__profile", "memory-set-profile"),
        )
        .await
        .expect("profile should succeed");
    let profile: Value = crate::json::from_string(
        String::from_utf8(output.body).expect("profile body should be utf8"),
    )
    .expect("profile should parse");
    assert!(
        profile["snapshot"]["op_apply_batch"]["calls"]
            .as_u64()
            .unwrap_or(0)
            >= 1
    );
    assert!(profile["snapshot"].get("op_apply_blind_batch").is_none());
    assert!(
        profile["snapshot"]["js_txn_commit"]["calls"]
            .as_u64()
            .unwrap_or(0)
            >= 1
    );
}

#[tokio::test]
#[serial]
async fn memory_coordinated_read_write_uses_owner_validated_batch_path() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 4,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        memory_profile_enabled: true,
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy_with_config(
            "memory".to_string(),
            memory_worker(),
            DeployConfig {
                egress_allow_hosts: Vec::new(),
                public: false,
                cache: Default::default(),
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "MY_MEMORY".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    service
        .invoke(
            "memory".to_string(),
            test_invocation_with_path("/seed?key=user-mixed-memory", "memory-mixed-seed"),
        )
        .await
        .expect("seed should succeed");

    service
        .invoke(
            "memory".to_string(),
            test_invocation_with_path("/__profile_reset", "memory-mixed-profile-reset"),
        )
        .await
        .expect("profile reset should succeed");

    let write = service
        .invoke(
            "memory".to_string(),
            test_invocation_with_path(
                "/atomic-read-write?key=user-mixed-memory&value=9",
                "memory-mixed-write",
            ),
        )
        .await
        .expect("memory read/write should succeed");
    assert_eq!(write.status, 200);
    assert_eq!(String::from_utf8(write.body).expect("utf8"), "0->9");

    let output = service
        .invoke(
            "memory".to_string(),
            test_invocation_with_path("/__profile", "memory-mixed-profile"),
        )
        .await
        .expect("profile should succeed");
    let profile: Value = crate::json::from_string(
        String::from_utf8(output.body).expect("profile body should be utf8"),
    )
    .expect("profile should parse");
    assert!(
        profile["snapshot"]["op_apply_batch"]["calls"]
            .as_u64()
            .unwrap_or(0)
            >= 1
    );
    assert!(profile["snapshot"].get("op_apply_blind_batch").is_none());
    assert!(
        profile["snapshot"]["js_txn_commit"]["calls"]
            .as_u64()
            .unwrap_or(0)
            >= 1
    );
}

#[tokio::test]
#[serial]
async fn memory_owner_epoch_survives_runtime_restart() {
    let root = PathBuf::from(format!("/tmp/dd-memory-owner-epoch-{}", Uuid::new_v4()));
    let config = RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 4,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    };

    let service = test_service_with_paths(config.clone(), root.clone(), true).await;
    service
        .deploy_with_config(
            "memory".to_string(),
            memory_worker(),
            DeployConfig {
                egress_allow_hosts: Vec::new(),
                public: false,
                cache: Default::default(),
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "MY_MEMORY".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    let first = service
        .invoke(
            "memory".to_string(),
            test_invocation_with_path(
                "/atomic-set-write?key=user-owner-restart&value=7",
                "memory-owner-first",
            ),
        )
        .await
        .expect("first memory write should succeed");
    assert_eq!(first.status, 200);
    assert_eq!(String::from_utf8(first.body).expect("utf8"), "7");
    service.shutdown().await.expect("service should shut down");
    drop(service);

    let restored = test_service_with_paths(config, root.clone(), true).await;
    let second = restored
        .invoke(
            "memory".to_string(),
            test_invocation_with_path(
                "/atomic-set-write?key=user-owner-restart&value=8",
                "memory-owner-second",
            ),
        )
        .await
        .expect("second memory write after restart should succeed");
    assert_eq!(second.status, 200);
    assert_eq!(String::from_utf8(second.body).expect("utf8"), "8");

    let current = restored
        .invoke(
            "memory".to_string(),
            test_invocation_with_path("/get?key=user-owner-restart", "memory-owner-current"),
        )
        .await
        .expect("current value should load");
    assert_eq!(String::from_utf8(current.body).expect("utf8"), "8");
    restored
        .shutdown()
        .await
        .expect("restored should shut down");
    let _ = tokio::fs::remove_dir_all(root).await;
}

#[tokio::test]
#[serial]
async fn memory_read_only_atomic_uses_memory_snapshot_without_commit() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 4,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        memory_profile_enabled: true,
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy_with_config(
            "memory".to_string(),
            memory_worker(),
            DeployConfig {
                egress_allow_hosts: Vec::new(),
                public: false,
                cache: Default::default(),
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "MY_MEMORY".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    service
        .invoke(
            "memory".to_string(),
            test_invocation_with_path("/__profile_reset", "memory-read-only-profile-reset"),
        )
        .await
        .expect("profile reset should succeed");

    let read = service
        .invoke(
            "memory".to_string(),
            test_invocation_with_path("/get?key=user-read-only-fast", "memory-read-only"),
        )
        .await
        .expect("atomic read should succeed");
    assert_eq!(read.status, 200);
    assert_eq!(String::from_utf8(read.body).expect("utf8"), "0");

    let output = service
        .invoke(
            "memory".to_string(),
            test_invocation_with_path("/__profile", "memory-read-only-profile"),
        )
        .await
        .expect("profile should succeed");
    let profile: Value = crate::json::from_string(
        String::from_utf8(output.body).expect("profile body should be utf8"),
    )
    .expect("profile should parse");
    assert!(
        profile["snapshot"]["op_snapshot"]["calls"]
            .as_u64()
            .unwrap_or(0)
            >= 1
    );
    assert_eq!(
        profile["snapshot"]["op_apply_batch"]["calls"]
            .as_u64()
            .unwrap_or(0),
        0
    );
    assert_eq!(
        profile["snapshot"]["js_txn_commit"]["calls"]
            .as_u64()
            .unwrap_or(0),
        0
    );
    assert!(profile["snapshot"].get("op_validate_reads").is_none());
    assert!(profile["snapshot"].get("js_txn_validate").is_none());
}

#[tokio::test]
#[serial]
async fn memory_multiple_atomic_reads_in_one_request_complete() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 4,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy_with_config(
            "memory-multi-read".to_string(),
            memory_multi_atomic_read_worker(),
            DeployConfig {
                egress_allow_hosts: Vec::new(),
                public: false,
                cache: Default::default(),
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "MY_MEMORY".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    let seed = service
        .invoke(
            "memory-multi-read".to_string(),
            test_invocation_with_path("/seed", "multi-read-seed"),
        )
        .await
        .expect("seed should succeed");
    assert_eq!(seed.status, 200);

    let output = tokio::time::timeout(
        Duration::from_secs(2),
        service.invoke(
            "memory-multi-read".to_string(),
            test_invocation_with_path("/sum?keys=2", "multi-read-sum"),
        ),
    )
    .await
    .expect("sum should not hang")
    .expect("sum invoke should succeed");
    assert_eq!(output.status, 200);
    assert_eq!(String::from_utf8(output.body).expect("utf8"), "1");
}

#[tokio::test]
#[serial]
async fn memory_multikey_transactions_complete_after_warmup() {
    let service = test_service(RuntimeConfig {
        min_isolates: 4,
        max_isolates: 4,
        max_inflight_per_isolate: 8,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy_with_config(
            "memory-multi-key".to_string(),
            memory_multi_key_storage_worker(),
            DeployConfig {
                egress_allow_hosts: Vec::new(),
                public: false,
                cache: Default::default(),
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "MY_MEMORY".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    service
        .invoke(
            "memory-multi-key".to_string(),
            test_invocation_with_path("/seed-all?keys=8", "multi-key-coordinated-seed"),
        )
        .await
        .expect("seed should succeed");

    let warmed = service
        .invoke(
            "memory-multi-key".to_string(),
            test_invocation_with_path("/atomic-sum?keys=8", "multi-key-coordinated-warm"),
        )
        .await
        .expect("warm memory sum should succeed");
    assert_eq!(String::from_utf8(warmed.body).expect("utf8"), "8");

    let mut tasks = Vec::new();
    for idx in 0..4 {
        let service = service.clone();
        tasks.push(tokio::spawn(async move {
            timeout(
                Duration::from_secs(2),
                service.invoke(
                    "memory-multi-key".to_string(),
                    test_invocation_with_path(
                        "/atomic-sum?keys=8",
                        &format!("multi-key-coordinated-{idx}"),
                    ),
                ),
            )
            .await
        }));
    }
    for task in tasks {
        let output = task
            .await
            .expect("join")
            .expect("memory sum should not hang")
            .expect("memory sum invoke should succeed");
        assert_eq!(output.status, 200);
        assert_eq!(String::from_utf8(output.body).expect("utf8"), "8");
    }
}

#[tokio::test]
#[serial]
async fn memory_benchmark_worker_returns_correct_total() {
    let service = test_service(RuntimeConfig {
        min_isolates: 2,
        max_isolates: 2,
        max_inflight_per_isolate: 8,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy_with_config(
            "memory-multi-key".to_string(),
            memory_multi_key_storage_worker(),
            DeployConfig {
                egress_allow_hosts: Vec::new(),
                public: false,
                cache: Default::default(),
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "MY_MEMORY".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    service
        .invoke(
            "memory-multi-key".to_string(),
            test_invocation_with_path("/seed-all?keys=1", "multi-key-write-seed"),
        )
        .await
        .expect("seed should succeed");

    for idx in 0..8 {
        let output = timeout(
            Duration::from_secs(2),
            service.invoke(
                "memory-multi-key".to_string(),
                test_invocation_with_path("/inc?key=bench-0", &format!("multi-key-inc-{idx}")),
            ),
        )
        .await
        .expect("increment should not hang")
        .expect("increment invoke should succeed");
        assert_eq!(output.status, 200);
    }

    let total = service
        .invoke(
            "memory-multi-key".to_string(),
            test_invocation_with_path("/get?key=bench-0", "multi-key-write-total"),
        )
        .await
        .expect("total should succeed");
    assert_eq!(total.status, 200);
    assert_eq!(String::from_utf8(total.body).expect("utf8"), "9");
}

#[tokio::test]
#[serial]
async fn memory_atomic_writes_complete_past_repeated_worker_threshold() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 1,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy_with_config(
            "memory-atomic-write-threshold".to_string(),
            memory_multi_key_storage_worker(),
            DeployConfig {
                egress_allow_hosts: Vec::new(),
                public: false,
                cache: Default::default(),
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "MY_MEMORY".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    service
        .invoke(
            "memory-atomic-write-threshold".to_string(),
            test_invocation_with_path("/seed-all?keys=1", "atomic-write-threshold-seed"),
        )
        .await
        .expect("seed should succeed");

    for idx in 0..64 {
        let result = timeout(
            Duration::from_secs(10),
            service.invoke(
                "memory-atomic-write-threshold".to_string(),
                test_invocation_with_path(
                    "/inc?key=bench-0",
                    &format!("atomic-write-threshold-{idx}"),
                ),
            ),
        )
        .await;
        let output = match result {
            Ok(Ok(output)) => output,
            Ok(Err(error)) => panic!("atomic write {idx} failed: {error}"),
            Err(_) => {
                let dump = service
                    .debug_dump("memory-atomic-write-threshold".to_string())
                    .await;
                panic!("atomic write {idx} should not hang; debug dump: {dump:?}");
            }
        };
        assert_eq!(output.status, 200);
    }

    let total = timeout(
        Duration::from_secs(10),
        service.invoke(
            "memory-atomic-write-threshold".to_string(),
            test_invocation_with_path("/get?key=bench-0", "atomic-write-threshold-total"),
        ),
    )
    .await
    .expect("atomic write total should not hang")
    .expect("atomic write total should succeed");
    assert_eq!(total.status, 200);
    assert_eq!(String::from_utf8(total.body).expect("utf8"), "65");
}

#[tokio::test]
#[serial]
async fn memory_transaction_reads_snapshot_after_isolate_restart() {
    let service = test_service(RuntimeConfig {
        min_isolates: 0,
        max_isolates: 1,
        max_inflight_per_isolate: 4,
        idle_ttl: Duration::from_millis(200),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy_with_config(
            "memory-snapshot".to_string(),
            memory_snapshot_worker(),
            DeployConfig {
                egress_allow_hosts: Vec::new(),
                public: false,
                cache: Default::default(),
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "MY_MEMORY".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    let seeded = service
        .invoke(
            "memory-snapshot".to_string(),
            test_invocation_with_path("/seed", "snapshot-seed"),
        )
        .await
        .expect("seed invoke should succeed");
    assert_eq!(seeded.status, 200);

    let warm_snapshot = service
        .invoke(
            "memory-snapshot".to_string(),
            test_invocation_with_path("/get", "snapshot-warm"),
        )
        .await
        .expect("warm snapshot value should succeed");
    assert_eq!(String::from_utf8(warm_snapshot.body).expect("utf8"), "7");

    timeout(Duration::from_secs(3), async {
        loop {
            let stats = service
                .stats("memory-snapshot".to_string())
                .await
                .expect("stats");
            if stats.isolates_total == 0 {
                break;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("memory pool should scale down to zero");

    let cold_snapshot = service
        .invoke(
            "memory-snapshot".to_string(),
            test_invocation_with_path("/get", "snapshot-cold"),
        )
        .await
        .expect("cold snapshot value should succeed");
    assert_eq!(String::from_utf8(cold_snapshot.body).expect("utf8"), "7");
}

#[tokio::test]
#[serial]
async fn hosted_memory_factories_share_state_and_module_globals() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 2,
        max_inflight_per_isolate: 4,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy_with_config(
            "hosted-memory".to_string(),
            hosted_memory_worker(),
            DeployConfig {
                egress_allow_hosts: Vec::new(),
                public: false,
                cache: Default::default(),
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "MY_MEMORY".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    let alpha = service
        .invoke(
            "hosted-memory".to_string(),
            test_invocation_with_path("/alpha/inc?key=user-1", "alpha-inc"),
        )
        .await
        .expect("alpha invoke should succeed");
    assert_eq!(String::from_utf8(alpha.body).expect("utf8"), "1");

    let beta = service
        .invoke(
            "hosted-memory".to_string(),
            test_invocation_with_path("/beta/read?key=user-1", "beta-read"),
        )
        .await
        .expect("beta invoke should succeed");
    assert_eq!(String::from_utf8(beta.body).expect("utf8"), "1");

    let worker_global = service
        .invoke(
            "hosted-memory".to_string(),
            test_invocation_with_path("/worker/global/inc?key=user-1", "worker-global-inc"),
        )
        .await
        .expect("worker global increment should succeed");
    assert_eq!(String::from_utf8(worker_global.body).expect("utf8"), "1");

    let memory_global = service
        .invoke(
            "hosted-memory".to_string(),
            test_invocation_with_path("/memory/global/read?key=user-1", "memory-global-read"),
        )
        .await
        .expect("memory global read should succeed");
    assert_eq!(String::from_utf8(memory_global.body).expect("utf8"), "1");

    let memory_global_inc = service
        .invoke(
            "hosted-memory".to_string(),
            test_invocation_with_path("/memory/global/inc?key=user-1", "memory-global-inc"),
        )
        .await
        .expect("memory global increment should succeed");
    assert_eq!(
        String::from_utf8(memory_global_inc.body).expect("utf8"),
        "2"
    );
}

#[tokio::test]
#[serial]
async fn hosted_memory_allows_inline_closures() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 2,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy_with_config(
            "hosted-memory".to_string(),
            hosted_memory_worker(),
            DeployConfig {
                egress_allow_hosts: Vec::new(),
                public: false,
                cache: Default::default(),
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "MY_MEMORY".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    let output = service
        .invoke(
            "hosted-memory".to_string(),
            test_invocation_with_path("/inline?key=user-2", "inline-closure"),
        )
        .await
        .expect("inline closure invoke should succeed");
    assert_eq!(String::from_utf8(output.body).expect("utf8"), "ok-inline");
}

#[tokio::test]
#[serial]
async fn hosted_memory_coordinated_single_read_is_point_in_time_only() {
    let service = test_service(RuntimeConfig {
        min_isolates: 2,
        max_isolates: 3,
        max_inflight_per_isolate: 1,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy_with_config(
            "hosted-memory".to_string(),
            hosted_memory_worker(),
            DeployConfig {
                egress_allow_hosts: Vec::new(),
                public: false,
                cache: Default::default(),
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "MY_MEMORY".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    service
        .invoke(
            "hosted-memory".to_string(),
            test_invocation_with_path("/atomic/seed?key=user-memory-once", "atomic-seed-once"),
        )
        .await
        .expect("seed should succeed");

    let read_task = {
        let service = service.clone();
        tokio::spawn(async move {
            service
                .invoke(
                    "hosted-memory".to_string(),
                    test_invocation_with_path(
                        "/atomic/read-once?key=user-memory-once",
                        "atomic-read-once",
                    ),
                )
                .await
        })
    };

    sleep(Duration::from_millis(10)).await;

    service
        .invoke(
            "hosted-memory".to_string(),
            test_invocation_with_path(
                "/atomic/write-a?key=user-memory-once&value=1",
                "atomic-write-once",
            ),
        )
        .await
        .expect("write should succeed");

    let read_once = read_task
        .await
        .expect("join")
        .expect("invoke should succeed");
    assert_eq!(String::from_utf8(read_once.body).expect("utf8"), "0");
}

#[tokio::test]
#[serial]
async fn hosted_memory_coordinated_read_command_does_not_replay_when_prior_read_goes_stale() {
    let service = test_service(RuntimeConfig {
        min_isolates: 2,
        max_isolates: 3,
        max_inflight_per_isolate: 1,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy_with_config(
            "hosted-memory".to_string(),
            hosted_memory_worker(),
            DeployConfig {
                egress_allow_hosts: Vec::new(),
                public: false,
                cache: Default::default(),
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "MY_MEMORY".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    service
        .invoke(
            "hosted-memory".to_string(),
            test_invocation_with_path("/atomic/seed?key=user-memory-pair", "atomic-seed-pair"),
        )
        .await
        .expect("seed should succeed");

    let read_task = {
        let service = service.clone();
        tokio::spawn(async move {
            service
                .invoke(
                    "hosted-memory".to_string(),
                    test_invocation_with_path(
                        "/atomic/read-pair?key=user-memory-pair",
                        "atomic-read-pair",
                    ),
                )
                .await
        })
    };

    sleep(Duration::from_millis(10)).await;

    let writer_thread = {
        let service = service.clone();
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build writer runtime");
            runtime.block_on(async move {
                service
                    .invoke(
                        "hosted-memory".to_string(),
                        test_invocation_with_path(
                            "/atomic/write-a?key=user-memory-pair&value=1",
                            "atomic-write-pair",
                        ),
                    )
                    .await
                    .expect("write should succeed");
            });
        })
    };

    let pair = read_task
        .await
        .expect("join")
        .expect("invoke should succeed");
    writer_thread.join().expect("writer join");
    assert_eq!(String::from_utf8(pair.body).expect("utf8"), "0:0");
}

#[tokio::test]
#[serial]
async fn hosted_memory_coordinated_snapshot_read_executes_once() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 3,
        max_inflight_per_isolate: 8,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy_with_config(
            "hosted-memory".to_string(),
            hosted_memory_worker(),
            DeployConfig {
                egress_allow_hosts: Vec::new(),
                public: false,
                cache: Default::default(),
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "MY_MEMORY".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    service
        .invoke(
            "hosted-memory".to_string(),
            test_invocation_with_path("/atomic/seed?key=user-memory-allow", "atomic-seed-allow"),
        )
        .await
        .expect("seed should succeed");

    let read_task = {
        let service = service.clone();
        tokio::spawn(async move {
            service
                .invoke(
                    "hosted-memory".to_string(),
                    test_invocation_with_path(
                        "/atomic/read-pair-snapshot?key=user-memory-allow",
                        "atomic-read-allow",
                    ),
                )
                .await
        })
    };

    sleep(Duration::from_millis(10)).await;

    service
        .invoke(
            "hosted-memory".to_string(),
            test_invocation_with_path(
                "/atomic/write-a?key=user-memory-allow&value=1",
                "atomic-write-allow",
            ),
        )
        .await
        .expect("write should succeed");

    let pair = read_task
        .await
        .expect("join")
        .expect("invoke should succeed");
    assert_eq!(String::from_utf8(pair.body).expect("utf8"), "0:0");
}

#[tokio::test]
#[serial]
async fn hosted_memory_missing_value_default_is_lazy_until_written() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 2,
        max_inflight_per_isolate: 4,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy_with_config(
            "hosted-memory".to_string(),
            hosted_memory_worker(),
            DeployConfig {
                egress_allow_hosts: Vec::new(),
                public: false,
                cache: Default::default(),
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "MY_MEMORY".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    let default_read = service
        .invoke(
            "hosted-memory".to_string(),
            test_invocation_with_path(
                "/atomic/default/read?key=user-1",
                "hosted-memory-missing-value-default-read",
            ),
        )
        .await
        .expect("default read should succeed");
    assert_eq!(String::from_utf8(default_read.body).expect("utf8"), "7");

    let raw_before_write = service
        .invoke(
            "hosted-memory".to_string(),
            test_invocation_with_path(
                "/atomic/default/raw?key=user-1",
                "hosted-memory-missing-value-default-raw-before-write",
            ),
        )
        .await
        .expect("raw read before write should succeed");
    assert_eq!(
        String::from_utf8(raw_before_write.body).expect("utf8"),
        "missing"
    );

    let write = service
        .invoke(
            "hosted-memory".to_string(),
            test_invocation_with_path(
                "/atomic/default/write?key=user-1",
                "hosted-memory-missing-value-default-write",
            ),
        )
        .await
        .expect("write should succeed");
    assert_eq!(String::from_utf8(write.body).expect("utf8"), "8");

    let raw_after_write = service
        .invoke(
            "hosted-memory".to_string(),
            test_invocation_with_path(
                "/atomic/default/raw?key=user-1",
                "hosted-memory-missing-value-default-raw-after-write",
            ),
        )
        .await
        .expect("raw read after write should succeed");
    assert_eq!(String::from_utf8(raw_after_write.body).expect("utf8"), "8");
}

#[tokio::test]
#[serial]
async fn memory_bindings_are_private_to_workers_across_redeployment() {
    let service = test_service(RuntimeConfig::default()).await;
    let source = r#"
export default {
  async fetch(request, env) {
    const memory = env.STATE.get("same-entity");
    const next = await memory.atomic((tx) => {
      const next = Number(tx.get("count") ?? 0) + 1;
      tx.put("count", next);
      return next;
    });
    return Response.json(next);
  }
};
"#;
    let config = DeployConfig {
        bindings: vec![DeployBinding::Memory {
            binding: "STATE".to_string(),
        }],
        ..DeployConfig::default()
    };
    for worker in ["alpha", "beta"] {
        service
            .deploy_with_config(worker.to_string(), source.to_string(), config.clone())
            .await
            .expect("worker deploys");
    }
    for (worker, expected) in [("alpha", 1), ("alpha", 2), ("beta", 1)] {
        let response = service
            .invoke(worker.to_string(), test_invocation())
            .await
            .expect("increment commits");
        assert_eq!(
            response.body,
            expected.to_string().as_bytes(),
            "worker {worker} has its own counter"
        );
    }
    service
        .deploy_with_config("alpha".to_string(), source.to_string(), config)
        .await
        .expect("worker redeploys");
    let response = service
        .invoke("alpha".to_string(), test_invocation())
        .await
        .expect("increment commits after redeploy");
    assert_eq!(response.body, b"3");
}
