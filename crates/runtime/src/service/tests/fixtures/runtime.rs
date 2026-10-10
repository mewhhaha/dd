use super::*;

#[derive(Clone)]
pub(crate) struct TestStoreDir(Arc<TestStorePath>);

struct TestStorePath(PathBuf);

impl TestStoreDir {
    pub(crate) fn new(prefix: &str) -> Self {
        let path = std::env::temp_dir().join(format!("{prefix}-{}", Uuid::new_v4()));
        std::fs::create_dir(&path).expect("test store directory should be created");
        Self(Arc::new(TestStorePath(path)))
    }

    pub(crate) fn path(&self) -> &std::path::Path {
        &self.0.0
    }
}

impl Drop for TestStorePath {
    fn drop(&mut self) {
        if let Err(error) = std::fs::remove_dir_all(&self.0) {
            eprintln!("failed to remove test store {}: {error}", self.0.display());
        }
    }
}

#[derive(Clone)]
pub(crate) struct TestRuntime {
    // Drop the service first: its lifetime owner joins the runtime thread and
    // releases shard writers before the last directory owner removes files.
    service: RuntimeService,
    store: TestStoreDir,
}

impl std::ops::Deref for TestRuntime {
    type Target = RuntimeService;

    fn deref(&self) -> &Self::Target {
        &self.service
    }
}

impl TestRuntime {
    pub(crate) fn store_path(&self) -> &std::path::Path {
        self.store.path()
    }
}

pub(crate) async fn test_service(config: RuntimeConfig) -> TestRuntime {
    test_service_with_store(config, TestStoreDir::new("dd-store"), false).await
}

pub(crate) async fn test_service_with_store(
    config: RuntimeConfig,
    store: TestStoreDir,
    worker_store_enabled: bool,
) -> TestRuntime {
    // Test workers reach behind the worker API through __dd_internals.
    let service = RuntimeService::start_with_service_config(RuntimeServiceConfig {
        runtime: RuntimeConfig {
            expose_internals: true,
            ..config
        },
        storage: RuntimeStorageConfig {
            store_dir: store.path().to_path_buf(),
            worker_store_enabled,
            ..RuntimeStorageConfig::default()
        },
    })
    .await
    .expect("service should start");
    TestRuntime { service, store }
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
