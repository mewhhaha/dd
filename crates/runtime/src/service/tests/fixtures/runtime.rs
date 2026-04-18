use super::*;

pub(crate) async fn test_service(config: RuntimeConfig) -> RuntimeService {
    let db_path = format!("/tmp/dd-test-{}.db", Uuid::new_v4());
    let store_dir = format!("/tmp/dd-store-{}", Uuid::new_v4());
    RuntimeService::start_with_service_config(RuntimeServiceConfig {
        runtime: config,
        storage: RuntimeStorageConfig {
            store_dir: PathBuf::from(&store_dir),
            database_url: format!("file:{db_path}"),
            memory_namespace_shards: 16,
            memory_db_cache_max_open: 4096,
            memory_db_idle_ttl: Duration::from_secs(60),
            worker_store_enabled: false,
            blob_store: BlobStoreConfig::local(PathBuf::from(&store_dir).join("blobs")),
        },
    })
    .await
    .expect("service should start")
}

pub(crate) async fn test_service_with_paths(
    config: RuntimeConfig,
    store_dir: PathBuf,
    database_url: String,
    worker_store_enabled: bool,
) -> RuntimeService {
    RuntimeService::start_with_service_config(RuntimeServiceConfig {
        runtime: config,
        storage: RuntimeStorageConfig {
            store_dir: store_dir.clone(),
            database_url,
            memory_namespace_shards: 16,
            memory_db_cache_max_open: 4096,
            memory_db_idle_ttl: Duration::from_secs(60),
            worker_store_enabled,
            blob_store: BlobStoreConfig::local(store_dir.join("blobs")),
        },
    })
    .await
    .expect("service should start")
}

pub(crate) fn dynamic_single_isolate_config() -> RuntimeConfig {
    RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 1,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    }
}

pub(crate) fn dynamic_autoscaling_config() -> RuntimeConfig {
    RuntimeConfig {
        min_isolates: 0,
        max_isolates: 2,
        max_inflight_per_isolate: 4,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    }
}

pub(crate) fn dynamic_bench_autoscaling_config() -> RuntimeConfig {
    RuntimeConfig {
        min_isolates: 0,
        max_isolates: 8,
        max_inflight_per_isolate: 4,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    }
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
            let dynamic_dump = service.dynamic_debug_dump().await;
            panic!("{stage} timed out; debug dump: {dump:?}; dynamic dump: {dynamic_dump:?}");
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
