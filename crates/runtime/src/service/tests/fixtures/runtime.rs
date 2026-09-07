use super::*;

pub(crate) async fn test_service(config: RuntimeConfig) -> RuntimeService {
    let store_dir = format!("/tmp/dd-store-{}", Uuid::new_v4());
    RuntimeService::start_with_service_config(RuntimeServiceConfig {
        runtime: config,
        storage: RuntimeStorageConfig {
            store_dir: PathBuf::from(&store_dir),
            memory_outbox_max_concurrent_shards: 8,
            memory_snapshot_cache_max_entries: 4096,
            memory_snapshot_cache_max_bytes: 64 * 1024 * 1024,
            worker_store_enabled: false,
        },
    })
    .await
    .expect("service should start")
}

pub(crate) async fn test_service_with_paths(
    config: RuntimeConfig,
    store_dir: PathBuf,
    worker_store_enabled: bool,
) -> RuntimeService {
    RuntimeService::start_with_service_config(RuntimeServiceConfig {
        runtime: config,
        storage: RuntimeStorageConfig {
            store_dir: store_dir.clone(),
            memory_outbox_max_concurrent_shards: 8,
            memory_snapshot_cache_max_entries: 4096,
            memory_snapshot_cache_max_bytes: 64 * 1024 * 1024,
            worker_store_enabled,
        },
    })
    .await
    .expect("service should start")
}

pub(crate) async fn invoke_with_timeout_and_dump(
    service: &RuntimeService,
    worker_name: &str,
    invocation: WorkerInvocation,
    stage: &str,
) -> WorkerOutput {
    match timeout(
        Duration::from_secs(5),
        service.invoke(worker_name.to_string(), invocation),
    )
    .await
    {
        Ok(Ok(output)) => output,
        Ok(Err(error)) => panic!("{stage} failed: {error}"),
        Err(_) => {
            let dump = service.debug_dump(worker_name.to_string()).await;
            panic!("{stage} timed out; debug dump: {dump:?}");
        }
    }
}

pub(crate) async fn wait_for_isolate_total(
    service: &RuntimeService,
    worker_name: &str,
    expected: usize,
) {
    timeout(Duration::from_secs(5), async {
        loop {
            let stats = service
                .stats(worker_name.to_string())
                .await
                .expect("worker stats should exist");
            if stats.isolates_total == expected {
                break;
            }
            sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("worker isolate count should converge");
}

pub(crate) fn test_assets() -> Vec<DeployAsset> {
    vec![
        DeployAsset {
            path: "/a.js".to_string(),
            content_base64: "YXNzZXQtYm9keQ==".to_string(),
        },
        DeployAsset {
            path: "/nested/b.css".to_string(),
            content_base64: "Ym9keXt9".to_string(),
        },
    ]
}

pub(crate) fn asset_worker() -> String {
    r#"
export default {
  async fetch() {
    return new Response("worker-fallback", {
      headers: [["content-type", "text/plain; charset=utf-8"]],
    });
  },
};
"#
    .to_string()
}

pub(crate) fn asset_headers_file() -> String {
    r#"
/a.js
  Cache-Control: public, max-age=60
  X-Exact: yes
https://:sub.example.com/a.js
  X-Host: :sub
/nested/*
  X-Splat: :splat
        "#
    .to_string()
}
