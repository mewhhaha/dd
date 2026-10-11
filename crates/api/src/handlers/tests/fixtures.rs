//! Runtime-backed app state and assets shared by handler tests.
use crate::deploy_tokens::DeployTokenStore;
use crate::state::AppState;
use common::DeployAsset;
use runtime::{RuntimeService, RuntimeServiceConfig, RuntimeStorageConfig};
use std::path::PathBuf;
use uuid::Uuid;

pub(super) struct TestState {
    pub(super) state: AppState,
    store_dir: PathBuf,
}

impl TestState {
    pub(super) async fn new(public_base_domain: &str) -> Self {
        let store_dir = PathBuf::from(format!("./target/test-store-api-{}", Uuid::new_v4()));
        let storage = RuntimeStorageConfig {
            store_dir: store_dir.clone(),
            worker_store_enabled: true,
            ..RuntimeStorageConfig::default()
        };
        let runtime = RuntimeService::start(RuntimeServiceConfig {
            runtime: Default::default(),
            storage,
        })
        .await
        .expect("runtime");
        let deploy_tokens = DeployTokenStore::from_control_store(runtime.control_store());
        let state = AppState::new(
            runtime,
            deploy_tokens,
            1024 * 1024,
            public_base_domain.to_string(),
            Some("test-private-token".to_string()),
        );
        Self { state, store_dir }
    }

    pub(super) fn app(&self) -> AppState {
        self.state.clone()
    }

    pub(super) async fn shutdown(self) {
        self.state
            .runtime
            .shutdown()
            .await
            .expect("runtime shutdown");
        let _ = tokio::fs::remove_dir_all(self.store_dir).await;
    }
}

pub(super) fn test_assets() -> Vec<DeployAsset> {
    vec![DeployAsset {
        path: "/a.js".to_string(),
        content_base64: "YXNzZXQtYm9keQ==".to_string(),
    }]
}
