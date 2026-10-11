use super::*;

const DROP_CHILD: &str = "service::tests::shutdown::outbox_drop_child";

#[tokio::test]
#[serial]
async fn automatic_shutdown_exits_with_pending_outbox_deliveries_and_retries_after_restart() {
    let store = TestStoreDir::new("dd-outbox-drop");
    let root = store.path().to_path_buf();
    let output = std::fs::File::create(root.join("shutdown.log")).expect("child log opens");
    let mut child = std::process::Command::new(std::env::current_exe().expect("test executable"))
        .args(["--ignored", "--exact", DROP_CHILD, "--nocapture"])
        .env("DD_OUTBOX_DROP_ROOT", &root)
        .stdout(output.try_clone().expect("child log duplicates"))
        .stderr(output)
        .spawn()
        .expect("outbox drop subprocess starts");
    let status = timeout(Duration::from_secs(20), async {
        loop {
            if let Some(status) = child.try_wait().expect("child status loads") {
                break status;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    let status = match status {
        Ok(status) => status,
        Err(_) => {
            child.kill().expect("stalled shutdown subprocess stops");
            child.wait().expect("stalled shutdown subprocess joins");
            panic!(
                "automatic shutdown stalled; subprocess log: {}",
                std::fs::read_to_string(root.join("shutdown.log")).unwrap_or_default()
            );
        }
    };
    assert!(
        status.success(),
        "outbox drop subprocess failed: {}",
        std::fs::read_to_string(root.join("shutdown.log")).unwrap_or_default()
    );

    let service = test_service_with_store(
        RuntimeConfig {
            scale_tick: Duration::from_millis(10),
            ..RuntimeConfig::default()
        },
        store.clone(),
        false,
    )
    .await;
    timeout(Duration::from_secs(35), async {
        loop {
            let mut delivered = 0;
            for key in 0..64 {
                let records = service
                    .memory_store
                    .outbox_records("8:shutdownMEMORY", &key.to_string())
                    .await
                    .expect("committed effects reopen");
                assert_eq!(
                    records.len(),
                    32,
                    "committed effects for key {key} survive shutdown"
                );
                delivered += records
                    .iter()
                    .filter(|record| record.status == "delivered")
                    .count();
            }
            if delivered == 2048 {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("retained outbox effects deliver after their shutdown leases expire");
    drop(service);
}

#[tokio::test]
#[ignore = "subprocess fixture for automatic runtime shutdown"]
async fn outbox_drop_child() {
    let root = PathBuf::from(std::env::var_os("DD_OUTBOX_DROP_ROOT").expect("subprocess root"));
    let service = RuntimeService::start(RuntimeServiceConfig {
        runtime: RuntimeConfig {
            scale_tick: Duration::from_millis(1),
            ..RuntimeConfig::default()
        },
        storage: RuntimeStorageConfig {
            store_dir: root,
            memory_outbox_max_concurrent_shards: 32,
            worker_store_enabled: false,
            ..RuntimeStorageConfig::default()
        },
    })
    .await
    .expect("runtime starts");
    service
        .deploy_with_config(
            "shutdown".into(),
            r#"
export default { async fetch(request, env) {
  await Promise.all(Array.from({ length: 64 }, (_, key) =>
    env.MEMORY.get(String(key)).atomic(tx => {
      tx.put('committed', true);
      for (let index = 0; index < 32; index++) tx.emit('audit.shutdown', { key, index });
    })
  ));
  return new Response('committed');
} };
"#
            .into(),
            DeployConfig {
                bindings: vec![DeployBinding::Memory {
                    binding: "MEMORY".into(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("outbox worker deploys");
    let output = service
        .invoke("shutdown".into(), test_invocation())
        .await
        .expect("effects commit");
    assert_eq!(output.body, b"committed");
    println!("committed 2048 effects; dropping the final runtime handle");
    drop(service);
}

#[tokio::test]
#[serial]
async fn shutdown_is_idempotent_across_clones_and_waits_for_runtime_thread_exit() {
    let service = test_service(RuntimeConfig {
        min_isolates: 0,
        ..RuntimeConfig::default()
    })
    .await;

    let shutdowns = (0..8).map(|_| {
        let service = service.clone();
        async move { service.shutdown().await }
    });
    let results = timeout(
        Duration::from_secs(5),
        futures_util::future::join_all(shutdowns),
    )
    .await
    .expect("concurrent shutdown calls should complete");
    assert!(results.into_iter().all(|result| result.is_ok()));
    assert!(
        service.shutdown.is_complete(),
        "shutdown must not return before the shared runtime thread join completes"
    );

    timeout(Duration::from_millis(100), service.shutdown())
        .await
        .expect("repeated shutdown should use the stored result")
        .expect("repeated shutdown should remain successful");
}

#[tokio::test]
#[serial]
async fn last_service_handle_drop_shuts_down_and_joins_runtime_thread() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates_per_worker: 1,
        ..RuntimeConfig::default()
    })
    .await;
    let store_path = service.store_path().to_path_buf();
    let worker = "automatic-shutdown".to_string();
    service
        .deploy(worker.clone(), counter_worker())
        .await
        .expect("worker should deploy");
    service
        .invoke(worker.clone(), test_invocation())
        .await
        .expect("worker invocation should start the configured isolate");
    wait_for_isolate_total(&service, &worker, 1).await;

    let shutdown = Arc::clone(&service.shutdown);
    let remaining_handle = service.clone();

    drop(service);
    assert!(
        store_path.exists(),
        "remaining service clones retain their store"
    );
    assert!(
        !shutdown.is_complete(),
        "dropping one clone must not shut down the runtime"
    );

    drop(remaining_handle);
    assert!(
        !store_path.exists(),
        "last fixture drop removes the closed store"
    );
    assert!(
        shutdown.is_complete(),
        "last service handle drop must not return before runtime teardown completes"
    );
    shutdown
        .wait()
        .await
        .expect("automatic runtime shutdown should succeed");
}
