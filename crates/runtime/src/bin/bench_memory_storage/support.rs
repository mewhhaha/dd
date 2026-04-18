use super::*;

pub(super) async fn start_service(tag: &str, runtime: RuntimeConfig) -> common::Result<RuntimeService> {
    let paths = bench_paths(tag);
    tokio::fs::create_dir_all(&paths.store_dir)
        .await
        .map_err(|error| common::PlatformError::internal(error.to_string()))?;
    RuntimeService::start_with_service_config(runtime_service_config(
        runtime,
        &paths.db_path,
        &paths.store_dir,
    ))
    .await
}

pub(super) struct BenchPaths {
    db_path: PathBuf,
    store_dir: PathBuf,
}

pub(super) fn bench_paths(tag: &str) -> BenchPaths {
    let root = PathBuf::from(format!("/tmp/dd-bench-{tag}-{}", Uuid::new_v4()));
    BenchPaths {
        db_path: root.join("dd-kv.db"),
        store_dir: root.join("store"),
    }
}

pub(super) fn runtime_service_config(
    runtime: RuntimeConfig,
    db_path: &Path,
    store_dir: &Path,
) -> RuntimeServiceConfig {
    RuntimeServiceConfig {
        runtime,
        storage: RuntimeStorageConfig {
            store_dir: store_dir.to_path_buf(),
            database_url: format!("file:{}", db_path.display()),
            memory_namespace_shards: 16,
            memory_db_cache_max_open: 4096,
            memory_db_idle_ttl: Duration::from_secs(60),
            worker_store_enabled: true,
            blob_store: runtime::BlobStoreConfig::local(store_dir.join("blobs")),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SLOW_WORKER_SOURCE: &str = r#"
export default {
  async fetch() {
    await new Promise((resolve) => setTimeout(resolve, 200));
    return new Response("ok");
  },
};
"#;

    #[tokio::test]
    async fn run_scenario_timeout_surfaces_phase_and_request() {
        let service = start_service(
            "bench-watchdog-timeout",
            RuntimeConfig {
                min_isolates: 1,
                max_isolates: 1,
                max_inflight_per_isolate: 1,
                idle_ttl: Duration::from_secs(5),
                scale_tick: Duration::from_millis(50),
                queue_warn_thresholds: vec![10],
                ..RuntimeConfig::default()
            },
        )
        .await
        .expect("service should start");

        let worker_name = format!("bench-slow-{}", Uuid::new_v4());
        service
            .deploy_with_config(
                worker_name.clone(),
                SLOW_WORKER_SOURCE.to_string(),
                DeployConfig::default(),
            )
            .await
            .expect("deploy should succeed");

        let options = BenchOptions {
            request_timeout: Duration::from_millis(25),
            verify_timeout: Duration::from_millis(25),
            watchdog_interval: Duration::from_millis(5),
            watchdog_silence_timeout: Duration::from_millis(10),
            wide_key_space: 16,
        };
        let watchdog_state = Arc::new(BenchWatchdogState::new());
        let started_at = Instant::now();
        set_watchdog_phase(&watchdog_state, started_at, BenchPhase::Invoke);
        let error = run_scenario(
            &service,
            &worker_name,
            Scenario {
                requests: 1,
                concurrency: 1,
                path: "/",
                key_space: 1,
            },
            &options,
            watchdog_state,
            started_at,
        )
        .await
        .expect_err("scenario should time out")
        .to_string();

        assert!(error.contains("timed out"));
        assert!(error.contains("phase=invoke"));
        assert!(error.contains("request=1"));
    }
}
