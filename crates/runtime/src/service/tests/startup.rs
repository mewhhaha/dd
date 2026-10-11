//! Starting a runtime service: the bootstrap snapshot, configuration checks,
//! isolate startup, and restoring active deployments.
use super::*;

#[tokio::test]
#[serial]
async fn service_starts_from_the_bootstrap_snapshot() {
    let _ = test_service(test_pool_config(0, 1, 1)).await;
}

#[tokio::test]
async fn service_rejects_empty_snapshot_cache_budgets() {
    let storage = RuntimeStorageConfig {
        memory_snapshot_cache_max_entries: 0,
        ..RuntimeStorageConfig::default()
    };
    let result = RuntimeService::start(RuntimeServiceConfig {
        runtime: RuntimeConfig::default(),
        storage,
    })
    .await;
    let error = match result {
        Ok(service) => {
            let _ = service.shutdown().await;
            panic!("undersized snapshot budget should be rejected");
        }
        Err(error) => error,
    };

    assert!(
        error
            .to_string()
            .contains("memory snapshot cache limits must be greater than zero")
    );
}

#[tokio::test]
#[serial]
async fn isolate_starts_with_configured_heap_limit() {
    let service = test_service(RuntimeConfig {
        min_isolates: 0,
        max_isolates_per_worker: 1,
        max_inflight_per_isolate: 1,
        max_isolate_heap_bytes: 64 * 1024 * 1024,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;
    let worker = "heap-limited".to_string();
    service
        .deploy(
            worker.clone(),
            r#"
export default {
  fetch() {
    const values = Array.from({ length: 1024 }, (_, idx) => idx);
    return new Response(String(values.length));
  },
};
"#
            .to_string(),
        )
        .await
        .expect("deploy should succeed");

    let output = timeout(
        Duration::from_secs(2),
        service.invoke(worker, test_invocation()),
    )
    .await
    .expect("invoke should not hang")
    .expect("request should succeed");
    assert_eq!(output.status, 200);
    assert_eq!(
        String::from_utf8(output.body).expect("body should be utf8"),
        "1024"
    );
}

#[tokio::test]
#[serial]
async fn isolate_startup_does_not_block_manager_commands() {
    let service = test_service(RuntimeConfig {
        min_isolates: 0,
        max_isolates_per_worker: 4,
        max_inflight_per_isolate: 1,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(20),
        queue_warn_thresholds: vec![1],
        ..RuntimeConfig::default()
    })
    .await;
    let worker = "slow-startup".to_string();
    service
        .deploy(
            worker.clone(),
            r#"
await __dd_internals.core.ops.op_time_sleep(800);

export default {
  async fetch() {
    return new Response("ready");
  },
};
"#
            .to_string(),
        )
        .await
        .expect("deploy should succeed");

    let invoke_service = service.clone();
    let invoke_worker = worker.clone();
    let invoke = tokio::spawn(async move {
        invoke_service
            .invoke(
                invoke_worker,
                test_invocation_with_path("/", "slow-startup-invoke"),
            )
            .await
    });

    timeout(Duration::from_secs(1), async {
        loop {
            let stats = service.stats(worker.clone()).await.expect("stats");
            if stats.queued == 1 && stats.isolates_total == 1 {
                break;
            }
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("request should wait while the isolate starts");

    let stats_started = Instant::now();
    let stats = timeout(Duration::from_millis(250), service.stats(worker.clone()))
        .await
        .expect("stats should not wait for isolate startup")
        .expect("stats");
    assert_eq!(stats.queued, 1);
    assert_eq!(stats.inflight_total, 0);
    assert!(
        stats_started.elapsed() < Duration::from_millis(250),
        "stats was delayed by isolate startup: {:?}",
        stats_started.elapsed(),
    );

    let output = timeout(Duration::from_secs(2), invoke)
        .await
        .expect("invoke join should not hang")
        .expect("invoke task should complete")
        .expect("invoke should succeed");
    assert_eq!(String::from_utf8(output.body).expect("utf8"), "ready");
    let stats = service.stats(worker).await.expect("stats");
    assert_eq!(stats.spawn_count, 1);
}

#[tokio::test]
#[serial]
async fn concurrent_runtime_services_isolate_module_graphs_and_refcounts() {
    let first = test_service(RuntimeConfig::default()).await;
    let second = test_service(RuntimeConfig::default()).await;
    let modules = HashMap::from([(
        "worker.js".to_string(),
        "export default { fetch() { return new Response('isolated'); } };".to_string(),
    )]);

    let (first_graph_id, first_entrypoint) = first
        ._module_registry
        .register_module_graph("worker.js", modules.clone())
        .expect("first service graph should register");
    assert!(
        second
            ._module_registry
            .source(&first_graph_id, &first_entrypoint)
            .is_none(),
        "a graph registered in one live service must not be visible to another"
    );

    let (second_graph_id, second_entrypoint) = second
        ._module_registry
        .register_module_graph("worker.js", modules)
        .expect("second service graph should register");
    assert_eq!(
        first_graph_id, second_graph_id,
        "graph ids remain content-addressed"
    );
    assert_eq!(first._module_registry.ref_count(&first_graph_id), Some(1));
    assert_eq!(second._module_registry.ref_count(&second_graph_id), Some(1));

    first._module_registry.release(&first_graph_id);
    assert!(
        first
            ._module_registry
            .source(&first_graph_id, &first_entrypoint)
            .is_none()
    );
    assert!(
        second
            ._module_registry
            .source(&second_graph_id, &second_entrypoint)
            .is_some(),
        "releasing one service's graph must not change another service's refcount"
    );

    first
        .shutdown()
        .await
        .expect("first service should shut down");
    second
        .shutdown()
        .await
        .expect("second service should shut down");
}

#[tokio::test]
#[serial_test::serial]
async fn active_restore_failure_fails_startup_and_records_diagnostic() -> common::Result<()> {
    let store = TestStoreDir::new("dd-restore-failure");
    let root = store.path().to_path_buf();
    drop(storage::state::StateStore::open(root.join("state")).await?);
    let control = storage::control::ControlStore::open(&root).await?;
    let mut invalid = storage::control::ControlDeployment {
        worker: "broken".to_string(),
        deployment_id: Uuid::new_v4().to_string(),
        source: String::new(),
        config: DeployConfig::default(),
        assets: Vec::new(),
        server_modules: Vec::new(),
        asset_headers: None,
        created_at_ms: 1,
        expires_at_ms: None,
        active: true,
    };
    invalid.source = "export default { this is not valid JavaScript".to_string();
    control.insert_deployment(&invalid).await?;
    drop(control);

    let service = crate::RuntimeService::start(crate::RuntimeServiceConfig {
        runtime: crate::RuntimeConfig::default(),
        storage: crate::RuntimeStorageConfig {
            store_dir: root.clone(),
            worker_store_enabled: true,
            ..crate::RuntimeStorageConfig::default()
        },
    })
    .await;
    let error = match service {
        Ok(service) => {
            let _ = service.shutdown().await;
            panic!("invalid active deployment should fail startup")
        }
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("failed to restore worker broken")
    );

    let control = storage::control::ControlStore::open(&root).await?;
    let failures = control.restore_failures().await?;
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0].worker, "broken");
    Ok(())
}
