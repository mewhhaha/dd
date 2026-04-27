use super::*;

#[tokio::test]
#[serial]
async fn memory_same_key_allows_overlap_by_default() {
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
                public: false,
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "MY_MEMORY".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    let started = Instant::now();
    let mut tasks = Vec::new();
    for idx in 0..8 {
        let svc = service.clone();
        tasks.push(tokio::spawn(async move {
            svc.invoke(
                "memory".to_string(),
                test_invocation_with_path("/run?key=user-1", &format!("memory-run-{idx}")),
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
        elapsed < Duration::from_millis(650),
        "expected overlap for same memory key by default, elapsed={elapsed:?}"
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
                public: false,
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
                public: false,
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
                public: false,
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
async fn memory_direct_write_visibility_roundtrip_works() {
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
                public: false,
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
            test_invocation_with_path("/direct-set?key=user-direct-1", "direct-set"),
        )
        .await
        .expect("direct set should succeed");
    assert_eq!(set.status, 200);

    let after_set = service
        .invoke(
            "memory".to_string(),
            test_invocation_with_path("/direct-get?key=user-direct-1", "direct-get-set"),
        )
        .await
        .expect("direct get after set should succeed");
    assert_eq!(after_set.status, 200);
    assert_eq!(String::from_utf8(after_set.body).expect("utf8"), "5");

    let delete = service
        .invoke(
            "memory".to_string(),
            test_invocation_with_path("/direct-delete?key=user-direct-1", "direct-delete"),
        )
        .await
        .expect("direct delete should succeed");
    assert_eq!(delete.status, 200);

    let after_delete = service
        .invoke(
            "memory".to_string(),
            test_invocation_with_path("/direct-get?key=user-direct-1", "direct-get-delete"),
        )
        .await
        .expect("direct get after delete should succeed");
    assert_eq!(after_delete.status, 200);
    assert_eq!(String::from_utf8(after_delete.body).expect("utf8"), "0");
}

#[tokio::test]
#[serial]
async fn memory_direct_writes_preserve_distinct_memory_updates() {
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
                public: false,
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "MY_MEMORY".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    let keys = (0..5)
        .map(|idx| format!("user-direct-distinct-{idx}"))
        .collect::<Vec<_>>();
    for key in &keys {
        let set = service
            .invoke(
                "memory".to_string(),
                test_invocation_with_path(
                    &format!("/direct-set?key={key}"),
                    &format!("direct-set-{key}"),
                ),
            )
            .await
            .expect("direct set should succeed");
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
                        &format!("/direct-get?key={key}"),
                        &format!("direct-get-{key}"),
                    ),
                )
                .await
                .expect("direct get should succeed");
            assert_eq!(output.status, 200);
            observed_total += String::from_utf8(output.body)
                .expect("utf8")
                .parse::<usize>()
                .expect("direct value should parse");
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
async fn memory_blind_stm_write_uses_blind_apply_path() {
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
                public: false,
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
            test_invocation_with_path("/__profile_reset", "memory-blind-profile-reset"),
        )
        .await
        .expect("profile reset should succeed");

    let write = service
        .invoke(
            "memory".to_string(),
            test_invocation_with_path(
                "/stm-blind-write?key=user-blind-stm&value=7",
                "memory-blind-write",
            ),
        )
        .await
        .expect("blind stm write should succeed");
    assert_eq!(write.status, 200);
    assert_eq!(String::from_utf8(write.body).expect("utf8"), "7");

    let output = service
        .invoke(
            "memory".to_string(),
            test_invocation_with_path("/__profile", "memory-blind-profile"),
        )
        .await
        .expect("profile should succeed");
    let profile: Value = crate::json::from_string(
        String::from_utf8(output.body).expect("profile body should be utf8"),
    )
    .expect("profile should parse");
    assert!(
        profile["snapshot"]["op_apply_blind_batch"]["calls"]
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
    assert!(
        profile["snapshot"]["js_txn_blind_commit"]["calls"]
            .as_u64()
            .unwrap_or(0)
            >= 1
    );
}

#[tokio::test]
#[serial]
async fn memory_mixed_stm_write_stays_on_full_apply_path() {
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
                public: false,
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
            test_invocation_with_path("/seed?key=user-mixed-stm", "memory-mixed-seed"),
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
                "/stm-read-write?key=user-mixed-stm&value=9",
                "memory-mixed-write",
            ),
        )
        .await
        .expect("mixed stm write should succeed");
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
    assert_eq!(
        profile["snapshot"]["op_apply_blind_batch"]["calls"]
            .as_u64()
            .unwrap_or(0),
        0
    );
    assert!(
        profile["snapshot"]["js_txn_commit"]["calls"]
            .as_u64()
            .unwrap_or(0)
            >= 1
    );
}

#[tokio::test]
#[serial]
async fn memory_direct_read_uses_point_read_lane() {
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
                public: false,
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
            test_invocation_with_path("/__profile_reset", "memory-direct-profile-reset"),
        )
        .await
        .expect("profile reset should succeed");

    let read = service
        .invoke(
            "memory".to_string(),
            test_invocation_with_path(
                "/direct-get?key=user-direct-fast-read",
                "memory-direct-read",
            ),
        )
        .await
        .expect("direct read should succeed");
    assert_eq!(read.status, 200);
    assert_eq!(String::from_utf8(read.body).expect("utf8"), "0");

    let output = service
        .invoke(
            "memory".to_string(),
            test_invocation_with_path("/__profile", "memory-direct-profile"),
        )
        .await
        .expect("profile should succeed");
    let profile: Value = crate::json::from_string(
        String::from_utf8(output.body).expect("profile body should be utf8"),
    )
    .expect("profile should parse");
    assert!(
        profile["snapshot"]["op_read"]["calls"]
            .as_u64()
            .unwrap_or(0)
            >= 1
    );
    assert_eq!(
        profile["snapshot"]["op_snapshot"]["calls"]
            .as_u64()
            .unwrap_or(0),
        0
    );
    assert_eq!(
        profile["snapshot"]["op_version_if_newer"]["calls"]
            .as_u64()
            .unwrap_or(0),
        0
    );
}

#[tokio::test]
#[serial]
async fn memory_read_only_atomic_avoids_stm_commit_path() {
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
                public: false,
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
    assert_eq!(
        profile["snapshot"]["op_snapshot"]["calls"]
            .as_u64()
            .unwrap_or(0),
        0
    );
    assert_eq!(
        profile["snapshot"]["op_validate_reads"]["calls"]
            .as_u64()
            .unwrap_or(0),
        0
    );
    assert_eq!(
        profile["snapshot"]["op_apply_batch"]["calls"]
            .as_u64()
            .unwrap_or(0),
        0
    );
    assert_eq!(
        profile["snapshot"]["js_txn_validate"]["calls"]
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
                public: false,
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
async fn memory_multikey_direct_reads_complete_after_warmup() {
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
                public: false,
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
            test_invocation_with_path("/seed-all?keys=8", "multi-key-direct-seed"),
        )
        .await
        .expect("seed should succeed");

    let warmed = service
        .invoke(
            "memory-multi-key".to_string(),
            test_invocation_with_path("/direct-sum?keys=8", "multi-key-direct-warm"),
        )
        .await
        .expect("warm direct sum should succeed");
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
                        "/direct-sum?keys=8",
                        &format!("multi-key-direct-{idx}"),
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
            .expect("direct sum should not hang")
            .expect("direct sum invoke should succeed");
        assert_eq!(output.status, 200);
        assert_eq!(String::from_utf8(output.body).expect("utf8"), "8");
    }
}

#[tokio::test]
#[serial]
async fn memory_multikey_stm_reads_complete_after_warmup() {
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
                public: false,
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
            test_invocation_with_path("/seed-all?keys=8", "multi-key-stm-seed"),
        )
        .await
        .expect("seed should succeed");

    let warmed = service
        .invoke(
            "memory-multi-key".to_string(),
            test_invocation_with_path("/stm-sum?keys=8", "multi-key-stm-warm"),
        )
        .await
        .expect("warm stm sum should succeed");
    assert_eq!(String::from_utf8(warmed.body).expect("utf8"), "8");

    let mut tasks = Vec::new();
    for idx in 0..4 {
        let service = service.clone();
        tasks.push(tokio::spawn(async move {
            timeout(
                Duration::from_secs(2),
                service.invoke(
                    "memory-multi-key".to_string(),
                    test_invocation_with_path("/stm-sum?keys=8", &format!("multi-key-stm-{idx}")),
                ),
            )
            .await
        }));
    }
    for task in tasks {
        let output = task
            .await
            .expect("join")
            .expect("stm sum should not hang")
            .expect("stm sum invoke should succeed");
        assert_eq!(output.status, 200);
        assert_eq!(String::from_utf8(output.body).expect("utf8"), "8");
    }
}

#[tokio::test]
#[serial]
async fn memory_stm_benchmark_worker_returns_correct_total() {
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
                public: false,
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
async fn memory_direct_writes_complete_past_repeated_worker_threshold() {
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
            "memory-direct-write-threshold".to_string(),
            memory_multi_key_storage_worker(),
            DeployConfig {
                public: false,
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "MY_MEMORY".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    for idx in 0..64 {
        let path = format!("/direct-write?key=bench-direct&value={}", idx + 1);
        let result = timeout(
            Duration::from_secs(10),
            service.invoke(
                "memory-direct-write-threshold".to_string(),
                test_invocation_with_path(&path, &format!("direct-write-threshold-{idx}")),
            ),
        )
        .await;
        let output = match result {
            Ok(Ok(output)) => output,
            Ok(Err(error)) => panic!("direct write {idx} failed: {error}"),
            Err(_) => {
                let dump = service
                    .debug_dump("memory-direct-write-threshold".to_string())
                    .await;
                panic!("direct write {idx} should not hang; debug dump: {dump:?}");
            }
        };
        assert_eq!(output.status, 200);
    }

    let total = timeout(
        Duration::from_secs(10),
        service.invoke(
            "memory-direct-write-threshold".to_string(),
            test_invocation_with_path("/get?key=bench-direct", "direct-write-threshold-total"),
        ),
    )
    .await
    .expect("direct write total should not hang")
    .expect("direct write total should succeed");
    assert_eq!(total.status, 200);
    assert_eq!(String::from_utf8(total.body).expect("utf8"), "64");
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
                public: false,
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
async fn memory_constructor_reads_hydrated_storage_snapshot_synchronously() {
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
            "memory-ctor".to_string(),
            memory_constructor_storage_worker(),
            DeployConfig {
                public: false,
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
            "memory-ctor".to_string(),
            test_invocation_with_path("/seed", "ctor-seed"),
        )
        .await
        .expect("seed invoke should succeed");
    assert_eq!(seeded.status, 200);

    let warm_ctor = service
        .invoke(
            "memory-ctor".to_string(),
            test_invocation_with_path("/constructor-value", "ctor-warm"),
        )
        .await
        .expect("warm constructor value should succeed");
    assert_eq!(String::from_utf8(warm_ctor.body).expect("utf8"), "7");

    let warm_direct = service
        .invoke(
            "memory-ctor".to_string(),
            test_invocation_with_path("/direct-value", "ctor-direct-warm"),
        )
        .await
        .expect("warm direct value should succeed");
    assert_eq!(String::from_utf8(warm_direct.body).expect("utf8"), "7");

    timeout(Duration::from_secs(3), async {
        loop {
            let stats = service
                .stats("memory-ctor".to_string())
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

    let cold_ctor = service
        .invoke(
            "memory-ctor".to_string(),
            test_invocation_with_path("/constructor-value", "ctor-cold"),
        )
        .await
        .expect("cold constructor value should succeed");
    assert_eq!(String::from_utf8(cold_ctor.body).expect("utf8"), "7");

    let cold_direct = service
        .invoke(
            "memory-ctor".to_string(),
            test_invocation_with_path("/direct-value", "ctor-direct-cold"),
        )
        .await
        .expect("cold direct value should succeed");
    assert_eq!(String::from_utf8(cold_direct.body).expect("utf8"), "7");

    let current = service
        .invoke(
            "memory-ctor".to_string(),
            test_invocation_with_path("/current-value", "ctor-current"),
        )
        .await
        .expect("current value should succeed");
    assert_eq!(String::from_utf8(current.body).expect("utf8"), "7");
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
                public: false,
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
                public: false,
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
async fn hosted_memory_stm_single_read_is_point_in_time_only() {
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
                public: false,
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
            test_invocation_with_path("/stm/seed?key=user-stm-once", "stm-seed-once"),
        )
        .await
        .expect("seed should succeed");

    let read_task = {
        let service = service.clone();
        tokio::spawn(async move {
            service
                .invoke(
                    "hosted-memory".to_string(),
                    test_invocation_with_path("/stm/read-once?key=user-stm-once", "stm-read-once"),
                )
                .await
        })
    };

    sleep(Duration::from_millis(10)).await;

    service
        .invoke(
            "hosted-memory".to_string(),
            test_invocation_with_path("/stm/write-a?key=user-stm-once&value=1", "stm-write-once"),
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
async fn hosted_memory_stm_retries_when_prior_read_goes_stale() {
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
                public: false,
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
            test_invocation_with_path("/stm/seed?key=user-stm-pair", "stm-seed-pair"),
        )
        .await
        .expect("seed should succeed");

    let read_task = {
        let service = service.clone();
        tokio::spawn(async move {
            service
                .invoke(
                    "hosted-memory".to_string(),
                    test_invocation_with_path("/stm/read-pair?key=user-stm-pair", "stm-read-pair"),
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
                            "/stm/write-a?key=user-stm-pair&value=1",
                            "stm-write-pair",
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
    assert_eq!(String::from_utf8(pair.body).expect("utf8"), "1:0");
}

#[tokio::test]
#[serial]
async fn hosted_memory_stm_snapshot_read_skips_retry_for_that_read() {
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
                public: false,
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
            test_invocation_with_path("/stm/seed?key=user-stm-allow", "stm-seed-allow"),
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
                        "/stm/read-pair-snapshot?key=user-stm-allow",
                        "stm-read-allow",
                    ),
                )
                .await
        })
    };

    sleep(Duration::from_millis(10)).await;

    service
        .invoke(
            "hosted-memory".to_string(),
            test_invocation_with_path("/stm/write-a?key=user-stm-allow&value=1", "stm-write-allow"),
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
async fn hosted_memory_tvar_default_is_lazy_until_written() {
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
                public: false,
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
                "/stm/tvar-default/read?key=user-1",
                "hosted-memory-tvar-default-read",
            ),
        )
        .await
        .expect("default read should succeed");
    assert_eq!(String::from_utf8(default_read.body).expect("utf8"), "7");

    let raw_before_write = service
        .invoke(
            "hosted-memory".to_string(),
            test_invocation_with_path(
                "/stm/tvar-default/raw?key=user-1",
                "hosted-memory-tvar-default-raw-before-write",
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
                "/stm/tvar-default/write?key=user-1",
                "hosted-memory-tvar-default-write",
            ),
        )
        .await
        .expect("write should succeed");
    assert_eq!(String::from_utf8(write.body).expect("utf8"), "8");

    let raw_after_write = service
        .invoke(
            "hosted-memory".to_string(),
            test_invocation_with_path(
                "/stm/tvar-default/raw?key=user-1",
                "hosted-memory-tvar-default-raw-after-write",
            ),
        )
        .await
        .expect("raw read after write should succeed");
    assert_eq!(String::from_utf8(raw_after_write.body).expect("utf8"), "8");
}
