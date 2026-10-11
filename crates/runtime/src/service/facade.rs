//! Public runtime service and its implementation modules.

mod admin;
mod config;
mod deploy;
mod invoke;
mod lifecycle;
mod types;

use super::*;
use lifecycle::RuntimeServiceLifetime;
pub(super) use lifecycle::RuntimeShutdownState;

pub use config::*;
pub use types::*;

pub(super) const RUNTIME_FAST_COMMAND_CHANNEL_CAPACITY: usize = 4096;
const MAX_ACCEPTED_DEPLOYMENTS: usize = 64;

enum DeploymentPersistence {
    Insert,
    Activate,
    Restore,
}

struct DeployWithConfigRequest {
    worker_name: String,
    source: String,
    config: DeployConfig,
    assets: Vec<DeployAsset>,
    server_modules: Vec<DeployServerModule>,
    asset_headers: Option<String>,
    deployment_id: Option<String>,
    persistence: DeploymentPersistence,
    temporary: bool,
    expires_at_ms: Option<i64>,
    enforce_temporary_transition: bool,
}

fn default_global_isolate_budget() -> usize {
    std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1)
        .max(2)
}

fn default_memory_outbox_parallelism(shard_count: usize) -> usize {
    let cpus = std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1)
        .max(1);
    shard_count.max(1).min(cpus).clamp(1, 8)
}

fn prepare_worker_deployment(
    worker_name: String,
    source: String,
    config: DeployConfig,
    assets: Vec<DeployAsset>,
    server_modules: Vec<DeployServerModule>,
    asset_headers: Option<String>,
) -> Result<PreparedWorkerDeployment> {
    let worker_name = worker_name.trim().to_string();
    if worker_name.is_empty() {
        return Err(PlatformError::bad_request("Worker name must not be empty"));
    }
    let bindings = extract_bindings(&config)?;
    let compiled_assets = Arc::new(compile_asset_bundle(&assets, asset_headers.as_deref())?);
    Ok(PreparedWorkerDeployment {
        worker_name,
        source,
        config,
        assets,
        server_modules,
        asset_headers,
        compiled_assets,
        bindings,
    })
}

#[derive(Clone)]
pub struct RuntimeService {
    sender: RuntimeCommandSender,
    console: crate::ops::WorkerConsoleSink,
    inspector: Option<InspectorRegistry>,
    cancel_sender: RuntimeCancellationSender,
    fast_sender: RuntimeCommandSender,
    asset_catalog: AssetCatalog,
    state_store: Arc<::storage::state::StateStore>,
    pub(super) memory_store: MemoryStore,
    cache_store: CacheStore,
    request_body_budget: Arc<tokio::sync::Semaphore>,
    request_body_chunk_bytes: usize,
    control_store: ControlStore,
    deployment_validator: Arc<super::deployment::DeploymentValidator>,
    deployment_admission: Arc<tokio::sync::Semaphore>,
    deployment_order: Arc<tokio::sync::Mutex<()>>,
    deployment_stopping: tokio::sync::watch::Sender<bool>,
    temporary_worker_ttl: Duration,
    pub(super) _module_registry: crate::module_registry::ModuleRegistry,
    storage: RuntimeStorageConfig,
    pub(super) shutdown: Arc<RuntimeShutdownState>,
    _lifetime: Arc<RuntimeServiceLifetime>,
}

fn ensure_rustls_crypto_provider() {
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    });
}
