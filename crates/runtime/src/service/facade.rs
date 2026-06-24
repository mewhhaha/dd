use super::*;
use tracing::{info, warn};

const RUNTIME_FAST_COMMAND_CHANNEL_CAPACITY: usize = 4096;

struct DeployWithConfigRequest {
    worker_name: String,
    source: String,
    config: DeployConfig,
    assets: Vec<DeployAsset>,
    server_modules: Vec<DeployServerModule>,
    asset_headers: Option<String>,
    persist: bool,
    temporary: bool,
    expires_at_ms: Option<i64>,
    enforce_temporary_transition: bool,
}

#[derive(Clone, Debug)]
pub struct RuntimeConfig {
    pub min_isolates: usize,
    pub max_isolates: usize,
    pub max_inflight_per_isolate: usize,
    pub max_queued_requests_per_worker: usize,
    pub reserved_internal_queued_requests_per_worker: usize,
    pub max_global_queued_requests: usize,
    pub max_global_queued_bytes: usize,
    pub max_queue_wait: Duration,
    pub request_wall_timeout: Duration,
    pub max_request_body_bytes: usize,
    pub max_response_body_bytes: usize,
    pub max_isolate_heap_bytes: usize,
    pub isolate_startup_timeout: Duration,
    pub idle_ttl: Duration,
    pub scale_tick: Duration,
    pub queue_warn_thresholds: Vec<usize>,
    pub cache_max_entries: usize,
    pub cache_max_bytes: usize,
    pub cache_default_ttl: Duration,
    pub kv_read_cache_max_entries: usize,
    pub kv_read_cache_max_bytes: usize,
    pub kv_read_cache_hit_ttl: Duration,
    pub kv_read_cache_miss_ttl: Duration,
    pub v8_flags: Vec<String>,
    pub debug_code_generation: bool,
    pub kv_profile_enabled: bool,
    pub memory_profile_enabled: bool,
    pub temporary_worker_ttl: Duration,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            min_isolates: 0,
            max_isolates: 8,
            max_inflight_per_isolate: 4,
            max_queued_requests_per_worker: 1024,
            reserved_internal_queued_requests_per_worker: 64,
            max_global_queued_requests: 16 * 1024,
            max_global_queued_bytes: 64 * 1024 * 1024,
            max_queue_wait: Duration::from_secs(30),
            request_wall_timeout: Duration::from_secs(30),
            max_request_body_bytes: 64 * 1024 * 1024,
            max_response_body_bytes: 64 * 1024 * 1024,
            max_isolate_heap_bytes: 128 * 1024 * 1024,
            isolate_startup_timeout: Duration::from_secs(5),
            idle_ttl: Duration::from_secs(30),
            scale_tick: Duration::from_secs(1),
            queue_warn_thresholds: vec![10, 100, 1000],
            cache_max_entries: 2048,
            cache_max_bytes: 64 * 1024 * 1024,
            cache_default_ttl: Duration::from_secs(60),
            kv_read_cache_max_entries: 16_384,
            kv_read_cache_max_bytes: 16 * 1024 * 1024,
            kv_read_cache_hit_ttl: Duration::from_secs(300),
            kv_read_cache_miss_ttl: Duration::from_secs(30),
            v8_flags: Vec::new(),
            debug_code_generation: false,
            kv_profile_enabled: false,
            memory_profile_enabled: false,
            temporary_worker_ttl: Duration::from_secs(60 * 60),
        }
    }
}

#[derive(Clone, Debug)]
pub struct RuntimeStorageConfig {
    pub store_dir: PathBuf,
    pub database_url: String,
    pub memory_namespace_shards: usize,
    pub memory_db_cache_max_open: usize,
    pub memory_db_idle_ttl: Duration,
    pub worker_store_enabled: bool,
    pub blob_store: BlobStoreConfig,
}

impl Default for RuntimeStorageConfig {
    fn default() -> Self {
        let store_dir = PathBuf::from("./store");
        let database_url = format!("file:{}/dd-kv.db", store_dir.display());
        let blob_root = store_dir.join("blobs");
        Self {
            store_dir,
            database_url,
            memory_namespace_shards: 16,
            memory_db_cache_max_open: 4096,
            memory_db_idle_ttl: Duration::from_secs(60),
            worker_store_enabled: !cfg!(test),
            blob_store: BlobStoreConfig::local(blob_root),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct RuntimeServiceConfig {
    pub runtime: RuntimeConfig,
    pub storage: RuntimeStorageConfig,
}

#[derive(Debug, Clone, Default)]
pub struct WorkerStats {
    pub generation: u64,
    pub public: bool,
    pub temporary: bool,
    pub expires_at_ms: Option<i64>,
    pub queued: usize,
    pub busy: usize,
    pub inflight_total: usize,
    pub wait_until_total: usize,
    pub isolates_total: usize,
    pub spawn_count: u64,
    pub reuse_count: u64,
    pub scale_down_count: u64,
    pub targeted_nested_lane_queued: usize,
    pub targeted_lane_queued: usize,
    pub memory_lane_queued: usize,
    pub general_lane_queued: usize,
    pub memory_active_shards: usize,
    pub memory_max_shard_depth: usize,
    pub memory_median_shard_depth: usize,
    pub memory_owner_queues: usize,
    pub memory_blocked_owner_queues: usize,
    pub active_memory_leases: usize,
    pub oldest_queue_ms: u64,
    pub queued_bytes: usize,
    pub max_queued_requests_per_worker: usize,
    pub max_global_queued_bytes: usize,
    pub memory_affinity_entries: usize,
    pub stale_memory_affinity_entries: usize,
    pub pending_memory_outbox_shards: usize,
    pub memory_affinity_hit_count: u64,
    pub memory_affinity_miss_no_mapping_count: u64,
    pub memory_affinity_miss_stale_count: u64,
    pub memory_affinity_miss_saturated_count: u64,
    pub memory_least_loaded_fallback_count: u64,
    pub memory_atomic_overflow_dispatch_count: u64,
    pub memory_candidate_rejected_owner_lease_count: u64,
    pub memory_candidate_rejected_isolate_state_count: u64,
    pub memory_candidate_heads_inspected_count: u64,
    pub memory_dispatch_no_ready_candidate_count: u64,
    pub runtime_ready_work_budget_exhausted_count: u64,
    pub runtime_max_ready_work_batch_size: usize,
    pub memory_outbox_claim_batch_count: u64,
    pub memory_outbox_claim_row_count: u64,
    pub memory_outbox_saturated_batch_count: u64,
    pub memory_outbox_delivery_success_count: u64,
    pub memory_outbox_delivery_retry_count: u64,
    pub memory_outbox_terminal_drop_count: u64,
    pub memory_outbox_ack_failure_count: u64,
    pub memory_outbox_channel_full_count: u64,
    pub memory_outbox_reschedule_count: u64,
}

#[derive(Debug, Clone, Default)]
pub struct WorkerDebugDump {
    pub generation: u64,
    pub queued: usize,
    pub isolates: Vec<WorkerDebugIsolate>,
    pub queued_requests: Vec<WorkerDebugRequest>,
    pub memory_scheduler: MemorySchedulerDebug,
    pub memory_outbox: MemoryOutboxDebug,
}

#[derive(Debug, Clone, Default)]
pub struct MemorySchedulerDebug {
    pub queued: usize,
    pub active_leases: usize,
    pub active_shards: usize,
    pub max_shard_depth: usize,
    pub median_shard_depth: usize,
    pub owner_queues: usize,
    pub blocked_owner_queues: usize,
    pub affinity_entries: usize,
    pub stale_affinity_entries: usize,
    pub oldest_queue_ms: u64,
    pub queued_bytes: usize,
    pub max_queued_requests_per_worker: usize,
    pub max_global_queued_bytes: usize,
    pub top_shards: Vec<MemoryShardDebug>,
    pub affinity_hit_count: u64,
    pub affinity_miss_no_mapping_count: u64,
    pub affinity_miss_stale_count: u64,
    pub affinity_miss_saturated_count: u64,
    pub least_loaded_fallback_count: u64,
    pub atomic_overflow_dispatch_count: u64,
    pub candidate_rejected_owner_lease_count: u64,
    pub candidate_rejected_isolate_state_count: u64,
    pub candidate_heads_inspected_count: u64,
    pub dispatch_no_ready_candidate_count: u64,
    pub runtime_ready_work_budget_exhausted_count: u64,
    pub runtime_max_ready_work_batch_size: usize,
}

#[derive(Debug, Clone, Default)]
pub struct MemoryShardDebug {
    pub shard_index: usize,
    pub queued: usize,
    pub ready_owners: usize,
    pub blocked_owners: usize,
    pub affinity_isolate_id: Option<u64>,
    pub affinity_stale: bool,
}

#[derive(Debug, Clone, Default)]
pub struct MemoryOutboxDebug {
    pub pending_scheduled_shards: usize,
    pub claim_batch_count: u64,
    pub claim_row_count: u64,
    pub saturated_batch_count: u64,
    pub delivery_success_count: u64,
    pub delivery_retry_count: u64,
    pub terminal_drop_count: u64,
    pub ack_failure_count: u64,
    pub channel_full_count: u64,
    pub reschedule_count: u64,
}

#[derive(Debug, Clone, Default)]
pub struct WorkerDebugIsolate {
    pub id: u64,
    pub inflight_count: usize,
    pub pending_wait_until: usize,
    pub active_websocket_sessions: usize,
    pub active_transport_sessions: usize,
    pub pending_requests: Vec<WorkerDebugRequest>,
}

#[derive(Debug, Clone, Default)]
pub struct WorkerDebugRequest {
    pub runtime_request_id: String,
    pub user_request_id: String,
    pub method: String,
    pub url: String,
    pub memory_key: Option<String>,
    pub target_isolate_id: Option<u64>,
    pub internal_origin: bool,
    pub reply_kind: String,
    pub host_rpc_target_id: Option<String>,
    pub host_rpc_method: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct DynamicHandleDebug {
    pub handle: String,
    pub id: String,
    pub owner_worker: String,
    pub owner_generation: u64,
    pub binding: String,
    pub worker_name: String,
    pub timeout_ms: u64,
    pub policy_tier: String,
    pub egress_deny_count: u64,
    pub rpc_deny_count: u64,
    pub quota_kill_count: u64,
    pub upgrade_deny_count: u64,
    pub outbound_requests: u64,
    pub inflight: usize,
    pub max_concurrency: usize,
}

#[derive(Debug, Clone, Default)]
pub struct HostRpcProviderDebug {
    pub provider_id: String,
    pub owner_worker: String,
    pub owner_generation: u64,
    pub owner_isolate_id: u64,
    pub target_id: String,
    pub methods: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct DynamicRuntimeDebugDump {
    pub handles: Vec<DynamicHandleDebug>,
    pub providers: Vec<HostRpcProviderDebug>,
}

#[derive(Debug)]
pub struct WorkerStreamOutput {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: WorkerStreamBody,
}

pub struct WorkerStreamBody {
    receiver: mpsc::Receiver<Result<Bytes>>,
    cancel_guard: Option<InvokeCancelGuard>,
}

impl std::fmt::Debug for WorkerStreamBody {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WorkerStreamBody")
            .field("has_cancel_guard", &self.cancel_guard.is_some())
            .finish_non_exhaustive()
    }
}

impl WorkerStreamBody {
    pub(super) fn new(receiver: mpsc::Receiver<Result<Bytes>>) -> Self {
        Self {
            receiver,
            cancel_guard: None,
        }
    }

    pub(super) fn attach_cancel_guard(&mut self, cancel_guard: InvokeCancelGuard) {
        self.cancel_guard = Some(cancel_guard);
    }

    pub async fn recv(&mut self) -> Option<Result<Bytes>> {
        let next = self.receiver.recv().await;
        if next.is_none() || matches!(next, Some(Err(_))) {
            self.disarm_cancel_guard();
        }
        next
    }

    fn disarm_cancel_guard(&mut self) {
        if let Some(cancel_guard) = self.cancel_guard.as_mut() {
            cancel_guard.disarm();
        }
        self.cancel_guard = None;
    }
}

#[derive(Debug)]
pub struct WebSocketOpen {
    pub session_id: String,
    pub worker_name: String,
    pub output: WorkerOutput,
}

#[derive(Debug)]
pub struct TransportOpen {
    pub session_id: String,
    pub worker_name: String,
    pub output: WorkerOutput,
}

#[derive(Debug, Clone)]
pub struct DynamicDeployResult {
    pub worker: String,
    pub deployment_id: String,
    pub env_placeholders: HashMap<String, String>,
}

pub struct PublicRouteAssetResolution {
    pub public_worker: bool,
    pub generation: Option<u64>,
    pub asset: Option<AssetResponse>,
}

pub type InvokeRequestBodyReceiver = mpsc::Receiver<std::result::Result<Bytes, String>>;

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
    sender: mpsc::Sender<RuntimeCommand>,
    cancel_sender: mpsc::Sender<RuntimeCommand>,
    asset_catalog: AssetCatalog,
    cache_store: CacheStore,
    storage: RuntimeStorageConfig,
}
impl RuntimeService {
    pub async fn start() -> Result<Self> {
        Self::start_with_service_config(RuntimeServiceConfig::default()).await
    }

    pub async fn start_with_config(config: RuntimeConfig) -> Result<Self> {
        Self::start_with_service_config(RuntimeServiceConfig {
            runtime: config,
            storage: RuntimeStorageConfig::default(),
        })
        .await
    }

    pub async fn start_with_service_config(config: RuntimeServiceConfig) -> Result<Self> {
        ensure_rustls_crypto_provider();
        let RuntimeServiceConfig { runtime, storage } = config;
        validate_runtime_config(&runtime)?;
        ensure_v8_flags(&runtime.v8_flags)?;
        if storage.memory_db_cache_max_open == 0 {
            return Err(PlatformError::internal(
                "memory_db_cache_max_open must be greater than 0",
            ));
        }
        if storage.memory_namespace_shards == 0 {
            return Err(PlatformError::internal(
                "memory_namespace_shards must be greater than 0",
            ));
        }
        if storage.memory_db_idle_ttl.is_zero() {
            return Err(PlatformError::internal(
                "memory_db_idle_ttl must be greater than 0",
            ));
        }
        tokio::fs::create_dir_all(&storage.store_dir)
            .await
            .map_err(|error| {
                PlatformError::internal(format!(
                    "failed to create store directory {}: {error}",
                    storage.store_dir.display()
                ))
            })?;

        let bootstrap_snapshot = build_bootstrap_snapshot().await?;
        let kv_store = KvStore::from_database_url(&storage.database_url).await?;
        kv_store.set_profile_enabled(runtime.kv_profile_enabled);
        let memory_store = MemoryStore::new(
            storage.store_dir.join("memory"),
            storage.memory_namespace_shards,
            storage.memory_db_cache_max_open,
            storage.memory_db_idle_ttl,
        )
        .await?;
        memory_store.set_profile_enabled(runtime.memory_profile_enabled);
        let blob_store = BlobStore::from_config(storage.blob_store.clone()).await?;
        let cache_store = CacheStore::from_config(
            CacheConfig {
                max_entries: runtime.cache_max_entries,
                max_bytes: runtime.cache_max_bytes,
                default_ttl: runtime.cache_default_ttl,
                ..CacheConfig::default()
            },
            &storage.database_url,
            blob_store,
        )
        .await?;
        let (sender, receiver) = mpsc::channel(256);
        let (cancel_sender, cancel_receiver) = mpsc::channel(RUNTIME_FAST_COMMAND_CHANNEL_CAPACITY);
        let asset_catalog = AssetCatalog::default();
        spawn_runtime_thread(RuntimeThreadStart {
            receiver,
            cancel_receiver,
            runtime_fast_sender: cancel_sender.clone(),
            asset_catalog: asset_catalog.clone(),
            bootstrap_snapshot,
            kv_store,
            memory_store,
            cache_store: cache_store.clone(),
            config: runtime,
            storage: storage.clone(),
        })?;
        let service = Self {
            sender,
            cancel_sender,
            asset_catalog,
            cache_store,
            storage,
        };
        service.restore_workers_from_store().await?;
        Ok(service)
    }

    fn worker_store_dir(&self) -> PathBuf {
        self.storage.store_dir.join("workers")
    }

    async fn restore_workers_from_store(&self) -> Result<()> {
        if !self.storage.worker_store_enabled {
            return Ok(());
        }

        let workers_dir = self.worker_store_dir();
        let mut read_dir = match tokio::fs::read_dir(&workers_dir).await {
            Ok(read_dir) => read_dir,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => {
                return Err(PlatformError::internal(format!(
                    "failed to read worker store {}: {error}",
                    workers_dir.display()
                )))
            }
        };

        let mut latest_by_worker: HashMap<String, (StoredWorkerDeployment, PathBuf)> =
            HashMap::new();
        while let Some(entry) = read_dir.next_entry().await.map_err(|error| {
            PlatformError::internal(format!(
                "failed to read worker store entry in {}: {error}",
                workers_dir.display()
            ))
        })? {
            let path = entry.path();
            if path.extension().and_then(|value| value.to_str()) != Some("json") {
                continue;
            }

            let body = match tokio::fs::read_to_string(&path).await {
                Ok(body) => body,
                Err(error) => {
                    warn!(path = %path.display(), error = %error, "skipping unreadable worker store file");
                    continue;
                }
            };
            let stored: StoredWorkerDeployment = match crate::json::from_string(body) {
                Ok(stored) => stored,
                Err(error) => {
                    warn!(path = %path.display(), error = %error, "skipping invalid worker store file");
                    continue;
                }
            };
            match latest_by_worker.get(&stored.name) {
                Some((current, current_path)) => {
                    let replace = stored.updated_at_ms > current.updated_at_ms
                        || (stored.updated_at_ms == current.updated_at_ms
                            && path.file_name() > current_path.file_name());
                    if replace {
                        latest_by_worker.insert(stored.name.clone(), (stored, path));
                    }
                }
                None => {
                    latest_by_worker.insert(stored.name.clone(), (stored, path));
                }
            }
        }

        let now_ms = epoch_ms_i64()?;
        let mut restored = 0usize;
        for (_worker_name, (stored, path)) in latest_by_worker {
            if stored
                .expires_at_ms
                .is_some_and(|expires_at_ms| expires_at_ms <= now_ms)
            {
                info!(
                    worker = %stored.name,
                    path = %path.display(),
                    "skipping expired temporary worker from local store"
                );
                if let Err(error) = delete_worker_deployment(&self.storage, &stored.name).await {
                    warn!(
                        worker = %stored.name,
                        error = %error,
                        "failed to remove expired worker deployment from local store"
                    );
                }
                continue;
            }

            match self
                .deploy_with_config_internal(DeployWithConfigRequest {
                    worker_name: stored.name.clone(),
                    source: stored.source,
                    config: stored.config,
                    assets: stored.assets,
                    server_modules: stored.server_modules,
                    asset_headers: stored.asset_headers,
                    persist: false,
                    temporary: stored.expires_at_ms.is_some(),
                    expires_at_ms: stored.expires_at_ms,
                    enforce_temporary_transition: false,
                })
                .await
            {
                Ok(deployment_id) => {
                    restored += 1;
                    info!(
                        worker = %stored.name,
                        deployment_id = %deployment_id,
                        "restored worker from local store"
                    );
                }
                Err(error) => {
                    warn!(
                        worker = %stored.name,
                        path = %path.display(),
                        error = %error,
                        "failed to restore worker from local store"
                    );
                }
            }
        }

        if restored > 0 {
            info!(restored, "restored workers from local store");
        }
        Ok(())
    }

    pub async fn deploy(&self, worker_name: String, source: String) -> Result<String> {
        self.deploy_with_bundle_config(
            worker_name,
            source,
            DeployConfig::default(),
            Vec::new(),
            None,
        )
        .await
    }

    pub async fn deploy_with_config(
        &self,
        worker_name: String,
        source: String,
        config: DeployConfig,
    ) -> Result<String> {
        self.deploy_with_bundle_config(worker_name, source, config, Vec::new(), None)
            .await
    }

    pub async fn deploy_with_bundle_config(
        &self,
        worker_name: String,
        source: String,
        config: DeployConfig,
        assets: Vec<DeployAsset>,
        asset_headers: Option<String>,
    ) -> Result<String> {
        self.deploy_with_bundle_config_lifecycle(
            worker_name,
            source,
            config,
            assets,
            asset_headers,
            false,
        )
        .await
    }

    pub async fn deploy_temporary_with_bundle_config(
        &self,
        worker_name: String,
        source: String,
        config: DeployConfig,
        assets: Vec<DeployAsset>,
        asset_headers: Option<String>,
    ) -> Result<String> {
        self.deploy_with_bundle_config_lifecycle(
            worker_name,
            source,
            config,
            assets,
            asset_headers,
            true,
        )
        .await
    }

    pub async fn deploy_with_bundle_config_lifecycle(
        &self,
        worker_name: String,
        source: String,
        config: DeployConfig,
        assets: Vec<DeployAsset>,
        asset_headers: Option<String>,
        temporary: bool,
    ) -> Result<String> {
        self.deploy_with_bundle_config_lifecycle_and_server_modules(
            worker_name,
            source,
            config,
            assets,
            Vec::new(),
            asset_headers,
            temporary,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn deploy_with_bundle_config_lifecycle_and_server_modules(
        &self,
        worker_name: String,
        source: String,
        config: DeployConfig,
        assets: Vec<DeployAsset>,
        server_modules: Vec<DeployServerModule>,
        asset_headers: Option<String>,
        temporary: bool,
    ) -> Result<String> {
        self.deploy_with_config_internal(DeployWithConfigRequest {
            worker_name,
            source,
            config,
            assets,
            server_modules,
            asset_headers,
            persist: true,
            temporary,
            expires_at_ms: None,
            enforce_temporary_transition: true,
        })
        .await
    }

    async fn deploy_with_config_internal(
        &self,
        request: DeployWithConfigRequest,
    ) -> Result<String> {
        let DeployWithConfigRequest {
            worker_name,
            source,
            config,
            assets,
            server_modules,
            asset_headers,
            persist,
            temporary,
            expires_at_ms,
            enforce_temporary_transition,
        } = request;
        let prepared = prepare_worker_deployment(
            worker_name,
            source,
            config,
            assets,
            server_modules,
            asset_headers,
        )?;
        let (reply_tx, reply_rx) = oneshot::channel();
        self.sender
            .send(RuntimeCommand::Deploy {
                prepared,
                persist,
                temporary,
                expires_at_ms,
                enforce_temporary_transition,
                reply: reply_tx,
            })
            .await
            .map_err(|_| PlatformError::internal("runtime thread is not available"))?;

        reply_rx
            .await
            .map_err(|_| PlatformError::internal("runtime deploy channel closed"))?
    }

    pub async fn deploy_dynamic(
        &self,
        source: String,
        env: HashMap<String, String>,
        egress_allow_hosts: Vec<String>,
    ) -> Result<DynamicDeployResult> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.sender
            .send(RuntimeCommand::DeployDynamic {
                source,
                env,
                egress_allow_hosts,
                reply: reply_tx,
            })
            .await
            .map_err(|_| PlatformError::internal("runtime thread is not available"))?;

        reply_rx
            .await
            .map_err(|_| PlatformError::internal("runtime dynamic deploy channel closed"))?
    }

    pub async fn invoke(
        &self,
        worker_name: String,
        request: WorkerInvocation,
    ) -> Result<WorkerOutput> {
        self.invoke_with_request_body(worker_name, request, None)
            .await
    }

    pub async fn invoke_with_request_body(
        &self,
        worker_name: String,
        request: WorkerInvocation,
        request_body: Option<InvokeRequestBodyReceiver>,
    ) -> Result<WorkerOutput> {
        let runtime_request_id = next_runtime_token("req");
        let invoke_span = if tracing::enabled!(Level::INFO) {
            let span = tracing::info_span!(
                "runtime.invoke",
                worker.name = %worker_name,
                runtime.request_id = %runtime_request_id,
                request.id = %request.request_id
            );
            set_span_parent_from_traceparent(&span, traceparent_from_headers(&request.headers));
            Some(span)
        } else {
            None
        };
        let _invoke_guard = invoke_span.as_ref().map(|span| span.enter());
        let (reply_tx, reply_rx) = oneshot::channel();
        self.sender
            .send(RuntimeCommand::Invoke {
                worker_name: worker_name.clone(),
                runtime_request_id: runtime_request_id.clone(),
                request,
                request_body,
                reply: reply_tx,
            })
            .await
            .map_err(|_| PlatformError::internal("runtime thread is not available"))?;

        let mut cancel_guard =
            InvokeCancelGuard::new(self.cancel_sender.clone(), worker_name, runtime_request_id);
        let reply = reply_rx.await;
        cancel_guard.disarm();
        reply.map_err(|_| PlatformError::internal("runtime invoke channel closed"))?
    }

    pub async fn invoke_stream(
        &self,
        worker_name: String,
        request: WorkerInvocation,
    ) -> Result<WorkerStreamOutput> {
        self.invoke_stream_with_request_body(worker_name, request, None)
            .await
    }

    pub async fn invoke_stream_with_request_body(
        &self,
        worker_name: String,
        request: WorkerInvocation,
        request_body: Option<InvokeRequestBodyReceiver>,
    ) -> Result<WorkerStreamOutput> {
        let runtime_request_id = next_runtime_token("req");
        let stream_span = if tracing::enabled!(Level::INFO) {
            let span = tracing::info_span!(
                "runtime.invoke_stream",
                worker.name = %worker_name,
                runtime.request_id = %runtime_request_id,
                request.id = %request.request_id
            );
            set_span_parent_from_traceparent(&span, traceparent_from_headers(&request.headers));
            Some(span)
        } else {
            None
        };
        let _stream_guard = stream_span.as_ref().map(|span| span.enter());
        let (ready_tx, ready_rx) = oneshot::channel();
        let (reply_tx, _reply_rx) = oneshot::channel();
        self.sender
            .send(RuntimeCommand::InvokeStream {
                worker_name: worker_name.clone(),
                runtime_request_id: runtime_request_id.clone(),
                request,
                request_body,
                ready: ready_tx,
                reply: reply_tx,
            })
            .await
            .map_err(|_| PlatformError::internal("runtime thread is not available"))?;
        let mut cancel_guard =
            InvokeCancelGuard::new(self.cancel_sender.clone(), worker_name, runtime_request_id);

        let ready = ready_rx
            .await
            .map_err(|_| PlatformError::internal("runtime stream channel closed"))?;
        match ready {
            Ok(mut output) => {
                output.body.attach_cancel_guard(cancel_guard);
                Ok(output)
            }
            Err(error) => {
                cancel_guard.disarm();
                Err(error)
            }
        }
    }

    pub async fn open_websocket(
        &self,
        worker_name: String,
        request: WorkerInvocation,
        request_body: Option<InvokeRequestBodyReceiver>,
    ) -> Result<WebSocketOpen> {
        let runtime_request_id = next_runtime_token("ws");
        let session_id = Uuid::new_v4().to_string();
        let (reply_tx, reply_rx) = oneshot::channel();
        self.sender
            .send(RuntimeCommand::OpenWebsocket {
                worker_name: worker_name.clone(),
                runtime_request_id: runtime_request_id.clone(),
                request,
                request_body,
                session_id: session_id.clone(),
                reply: reply_tx,
            })
            .await
            .map_err(|_| PlatformError::internal("runtime thread is not available"))?;

        let mut cancel_guard =
            InvokeCancelGuard::new(self.cancel_sender.clone(), worker_name, runtime_request_id);
        let reply = reply_rx.await;
        cancel_guard.disarm();
        let mut opened = reply
            .map_err(|_| PlatformError::internal("runtime open websocket channel closed"))??;
        opened.session_id = session_id;
        Ok(opened)
    }

    pub async fn open_transport(
        &self,
        worker_name: String,
        request: WorkerInvocation,
        stream_sender: mpsc::Sender<Vec<u8>>,
        datagram_sender: mpsc::Sender<Vec<u8>>,
    ) -> Result<TransportOpen> {
        let session_id = Uuid::new_v4().to_string();
        let (reply_tx, reply_rx) = oneshot::channel();
        self.sender
            .send(RuntimeCommand::OpenTransport {
                worker_name,
                request,
                session_id: session_id.clone(),
                stream_sender,
                datagram_sender,
                reply: reply_tx,
            })
            .await
            .map_err(|_| PlatformError::internal("runtime thread is not available"))?;

        let mut opened = reply_rx
            .await
            .map_err(|_| PlatformError::internal("runtime open transport channel closed"))??;
        opened.session_id = session_id;
        Ok(opened)
    }

    pub async fn websocket_send_frame(
        &self,
        worker_name: String,
        session_id: String,
        frame: Vec<u8>,
        is_binary: bool,
    ) -> Result<WorkerOutput> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.sender
            .send(RuntimeCommand::SendWebsocketFrame {
                worker_name,
                session_id,
                frame,
                is_binary,
                reply: reply_tx,
            })
            .await
            .map_err(|_| PlatformError::internal("runtime thread is not available"))?;

        reply_rx.await.unwrap_or_else(|_| {
            Err(PlatformError::internal(
                "runtime websocket send channel closed",
            ))
        })
    }

    pub async fn websocket_wait_frame(
        &self,
        worker_name: String,
        session_id: String,
    ) -> Result<()> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.sender
            .send(RuntimeCommand::WaitWebsocketFrame {
                worker_name,
                session_id,
                reply: reply_tx,
            })
            .await
            .map_err(|_| PlatformError::internal("runtime thread is not available"))?;

        reply_rx.await.unwrap_or_else(|_| {
            Err(PlatformError::internal(
                "runtime websocket wait channel closed",
            ))
        })
    }

    pub async fn websocket_drain_frame(
        &self,
        worker_name: String,
        session_id: String,
    ) -> Result<Option<WorkerOutput>> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.sender
            .send(RuntimeCommand::DrainWebsocketFrame {
                worker_name,
                session_id,
                reply: reply_tx,
            })
            .await
            .map_err(|_| PlatformError::internal("runtime thread is not available"))?;

        reply_rx.await.unwrap_or_else(|_| {
            Err(PlatformError::internal(
                "runtime websocket drain channel closed",
            ))
        })
    }

    pub async fn websocket_close(
        &self,
        worker_name: String,
        session_id: String,
        close_code: u16,
        close_reason: String,
    ) -> Result<()> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.sender
            .send(RuntimeCommand::CloseWebsocket {
                worker_name,
                session_id,
                close_code,
                close_reason,
                reply: reply_tx,
            })
            .await
            .map_err(|_| PlatformError::internal("runtime thread is not available"))?;

        reply_rx.await.unwrap_or_else(|_| {
            Err(PlatformError::internal(
                "runtime websocket close channel closed",
            ))
        })
    }

    pub async fn transport_push_stream(
        &self,
        worker_name: String,
        session_id: String,
        chunk: Vec<u8>,
        done: bool,
    ) -> Result<()> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.sender
            .send(RuntimeCommand::PushTransportStream {
                worker_name,
                session_id,
                chunk,
                done,
                reply: reply_tx,
            })
            .await
            .map_err(|_| PlatformError::internal("runtime thread is not available"))?;

        reply_rx.await.unwrap_or_else(|_| {
            Err(PlatformError::internal(
                "runtime transport stream push channel closed",
            ))
        })
    }

    pub async fn transport_push_datagram(
        &self,
        worker_name: String,
        session_id: String,
        datagram: Vec<u8>,
    ) -> Result<()> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.sender
            .send(RuntimeCommand::PushTransportDatagram {
                worker_name,
                session_id,
                datagram,
                reply: reply_tx,
            })
            .await
            .map_err(|_| PlatformError::internal("runtime thread is not available"))?;

        reply_rx.await.unwrap_or_else(|_| {
            Err(PlatformError::internal(
                "runtime transport datagram push channel closed",
            ))
        })
    }

    pub async fn transport_close(
        &self,
        worker_name: String,
        session_id: String,
        close_code: u16,
        close_reason: String,
    ) -> Result<()> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.sender
            .send(RuntimeCommand::CloseTransport {
                worker_name,
                session_id,
                close_code,
                close_reason,
                reply: reply_tx,
            })
            .await
            .map_err(|_| PlatformError::internal("runtime thread is not available"))?;

        reply_rx.await.unwrap_or_else(|_| {
            Err(PlatformError::internal(
                "runtime transport close channel closed",
            ))
        })
    }

    pub async fn stats(&self, worker_name: String) -> Option<WorkerStats> {
        let (reply_tx, reply_rx) = oneshot::channel();
        if self
            .sender
            .send(RuntimeCommand::Stats {
                worker_name,
                reply: reply_tx,
            })
            .await
            .is_err()
        {
            return None;
        }
        reply_rx.await.ok().flatten()
    }

    pub fn worker_is_public(&self, worker_name: &str) -> bool {
        self.asset_catalog
            .get(worker_name)
            .is_some_and(|entry| entry.worker_name == worker_name && entry.public)
    }

    pub fn resolve_asset(
        &self,
        worker_name: &str,
        method: &str,
        host: Option<&str>,
        path: &str,
        headers: &[(String, String)],
    ) -> Result<Option<AssetResponse>> {
        self.resolve_asset_from_catalog(worker_name, method, host, path, headers, false)
    }

    pub fn resolve_public_asset(
        &self,
        worker_name: &str,
        method: &str,
        host: Option<&str>,
        path: &str,
        headers: &[(String, String)],
    ) -> Result<Option<AssetResponse>> {
        self.resolve_asset_from_catalog(worker_name, method, host, path, headers, true)
    }

    pub fn resolve_public_route_asset(
        &self,
        worker_name: &str,
        method: &str,
        host: Option<&str>,
        path: &str,
        headers: &[(String, String)],
    ) -> Result<PublicRouteAssetResolution> {
        let Some(entry) = self.asset_catalog.get(worker_name) else {
            return Ok(PublicRouteAssetResolution {
                public_worker: false,
                generation: None,
                asset: None,
            });
        };
        if entry.worker_name != worker_name {
            return Ok(PublicRouteAssetResolution {
                public_worker: false,
                generation: None,
                asset: None,
            });
        }
        if !entry.public {
            return Ok(PublicRouteAssetResolution {
                public_worker: false,
                generation: Some(entry.generation),
                asset: None,
            });
        }
        Ok(PublicRouteAssetResolution {
            public_worker: true,
            generation: Some(entry.generation),
            asset: resolve_asset(
                &entry.assets,
                AssetRequest {
                    method,
                    host,
                    path,
                    headers,
                },
            ),
        })
    }

    fn resolve_asset_from_catalog(
        &self,
        worker_name: &str,
        method: &str,
        host: Option<&str>,
        path: &str,
        headers: &[(String, String)],
        public_only: bool,
    ) -> Result<Option<AssetResponse>> {
        let Some(entry) = self.asset_catalog.get(worker_name) else {
            return Ok(None);
        };
        if entry.worker_name != worker_name {
            return Ok(None);
        }
        if public_only && !entry.public {
            return Ok(None);
        }
        Ok(resolve_asset(
            &entry.assets,
            AssetRequest {
                method,
                host,
                path,
                headers,
            },
        ))
    }

    pub async fn debug_dump(&self, worker_name: String) -> Option<WorkerDebugDump> {
        let (reply_tx, reply_rx) = oneshot::channel();
        if self
            .sender
            .send(RuntimeCommand::DebugDump {
                worker_name,
                reply: reply_tx,
            })
            .await
            .is_err()
        {
            return None;
        }
        reply_rx.await.ok().flatten()
    }

    pub async fn dynamic_debug_dump(&self) -> DynamicRuntimeDebugDump {
        let (reply_tx, reply_rx) = oneshot::channel();
        if self
            .sender
            .send(RuntimeCommand::DynamicDebugDump { reply: reply_tx })
            .await
            .is_err()
        {
            return DynamicRuntimeDebugDump::default();
        }
        reply_rx.await.unwrap_or_default()
    }

    pub async fn shutdown(&self) -> Result<()> {
        let (reply_tx, reply_rx) = oneshot::channel();
        if self
            .sender
            .send(RuntimeCommand::Shutdown { reply: reply_tx })
            .await
            .is_err()
        {
            return Ok(());
        }
        reply_rx
            .await
            .map_err(|_| PlatformError::internal("runtime shutdown channel closed"))?;
        Ok(())
    }

    #[cfg(test)]
    pub async fn force_fail_isolate_for_test(
        &self,
        worker_name: String,
        generation: u64,
        isolate_id: u64,
    ) -> bool {
        let (reply_tx, reply_rx) = oneshot::channel();
        if self
            .sender
            .send(RuntimeCommand::ForceFailIsolate {
                worker_name,
                generation,
                isolate_id,
                reply: reply_tx,
            })
            .await
            .is_err()
        {
            return false;
        }
        reply_rx.await.unwrap_or(false)
    }

    pub async fn cache_match(&self, request: CacheRequest) -> Result<CacheLookup> {
        let span = tracing::info_span!(
            "runtime.cache.match",
            cache.name = %request.cache_name,
            http.method = %request.method,
            http.url = %request.url
        );
        let _guard = span.enter();
        self.cache_store.get(&request).await
    }

    pub async fn cache_put(&self, request: CacheRequest, response: CacheResponse) -> Result<bool> {
        let span = tracing::info_span!(
            "runtime.cache.put",
            cache.name = %request.cache_name,
            http.method = %request.method,
            http.url = %request.url,
            response.status = response.status as u64,
            response.body_size = response.body.len() as u64
        );
        let _guard = span.enter();
        self.cache_store.put(&request, response).await
    }
}

fn ensure_rustls_crypto_provider() {
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    });
}
