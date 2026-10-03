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
    let service = RuntimeService::start_with_service_config(RuntimeServiceConfig {
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
