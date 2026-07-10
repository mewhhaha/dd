use super::*;
use futures_util::future::join_all;
use serde::Serialize;
use tracing::info;

const RUNTIME_FAST_COMMAND_CHANNEL_CAPACITY: usize = 4096;

struct DeployWithConfigRequest {
    worker_name: String,
    source: String,
    config: DeployConfig,
    assets: Vec<DeployAsset>,
    server_modules: Vec<DeployServerModule>,
    asset_headers: Option<String>,
    deployment_id: Option<String>,
    persist: bool,
    temporary: bool,
    expires_at_ms: Option<i64>,
    enforce_temporary_transition: bool,
}

#[derive(Clone, Debug)]
pub struct RuntimeConfig {
    pub min_isolates: usize,
    pub max_global_isolates: usize,
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
            max_global_isolates: default_global_isolate_budget(),
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

fn default_global_isolate_budget() -> usize {
    std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1)
        .max(2)
}

#[derive(Clone, Debug)]
pub struct RuntimeStorageConfig {
    pub store_dir: PathBuf,
    pub database_url: String,
    pub memory_namespace_shards: usize,
    pub memory_outbox_max_concurrent_shards: usize,
    pub memory_db_cache_max_open: usize,
    pub memory_db_read_connections_per_database: usize,
    pub memory_db_max_total_connections: usize,
    pub memory_db_idle_ttl: Duration,
    pub worker_store_enabled: bool,
}

impl Default for RuntimeStorageConfig {
    fn default() -> Self {
        let store_dir = PathBuf::from("./store");
        let database_url = format!("file:{}/dd-kv.db", store_dir.display());
        Self {
            store_dir,
            database_url,
            memory_namespace_shards: 16,
            memory_outbox_max_concurrent_shards: default_memory_outbox_parallelism(16),
            memory_db_cache_max_open: 512,
            memory_db_read_connections_per_database: 2,
            memory_db_max_total_connections: 1024,
            memory_db_idle_ttl: Duration::from_secs(60),
            worker_store_enabled: !cfg!(test),
        }
    }
}

fn default_memory_outbox_parallelism(namespace_shards: usize) -> usize {
    let cpus = std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1)
        .max(1);
    namespace_shards.max(1).min(cpus).clamp(1, 8)
}

#[derive(Clone, Debug, Default)]
pub struct RuntimeServiceConfig {
    pub runtime: RuntimeConfig,
    pub storage: RuntimeStorageConfig,
}

#[derive(Debug, Clone, Default, Serialize)]
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
    pub global_isolate_budget: usize,
    pub global_isolates_total: usize,
    pub global_isolates_starting: usize,
    pub global_isolate_slots_available: usize,
    pub scale_up_waiting_pools: usize,
    pub scale_up_budget_denied_count: u64,
    pub memory_outbox_claim_batch_count: u64,
    pub memory_outbox_claim_row_count: u64,
    pub memory_outbox_saturated_batch_count: u64,
    pub memory_outbox_delivery_success_count: u64,
    pub memory_outbox_delivery_retry_count: u64,
    pub memory_outbox_terminal_drop_count: u64,
    pub memory_outbox_ack_failure_count: u64,
    pub memory_outbox_channel_full_count: u64,
    pub memory_outbox_reschedule_count: u64,
    pub memory_outbox_worker_pending_shards: usize,
    pub memory_outbox_worker_in_flight_shards: usize,
    pub memory_outbox_worker_parallelism_limit: usize,
    pub memory_outbox_worker_parallelism_peak: usize,
    pub memory_outbox_duplicate_schedule_coalesced_count: u64,
    pub memory_outbox_task_failure_count: u64,
    pub memory_outbox_shard_requeue_count: u64,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct RuntimeRestoreFailure {
    pub worker: Option<String>,
    pub source: String,
    pub error: String,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct RuntimeWorkerStatus {
    pub name: String,
    /// Backlog-based outbox lag proxy. A value of zero means the coordinator
    /// and outbox workers have no scheduled or in-flight shards.
    pub outbox_lag_shards: usize,
    #[serde(flatten)]
    pub stats: WorkerStats,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct RuntimeAdminSnapshot {
    pub active_deployments: usize,
    pub workers: Vec<RuntimeWorkerStatus>,
    pub restore_failures: Vec<RuntimeRestoreFailure>,
    pub readiness: RuntimeReadiness,
    pub storage_retry_count: u64,
    pub cache_flush_failure_count: u64,
    pub cache_pending_recency_touches: usize,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct RuntimeReadiness {
    pub ready: bool,
    pub runtime_ready: bool,
    pub migrations_ready: bool,
    pub storage_ready: bool,
    pub worker_restoration_ready: bool,
    pub restore_failure_count: usize,
    pub failed_components: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct RuntimeCheckpointResult {
    pub kv: bool,
    pub cache: bool,
    pub memory_databases: usize,
    pub control: bool,
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
    pub global_isolate_budget: usize,
    pub global_isolates_total: usize,
    pub global_isolates_starting: usize,
    pub global_isolate_slots_available: usize,
    pub scale_up_waiting_pools: usize,
    pub scale_up_budget_denied_count: u64,
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
    pub worker_pending_shards: usize,
    pub worker_in_flight_shards: usize,
    pub worker_parallelism_limit: usize,
    pub worker_parallelism_peak: usize,
    pub duplicate_schedule_coalesced_count: u64,
    pub task_failure_count: u64,
    pub shard_requeue_count: u64,
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

pub(super) struct RuntimeShutdownState {
    started: AtomicBool,
    thread: StdMutex<Option<thread::JoinHandle<()>>>,
    result: StdMutex<Option<Result<()>>>,
    completed: Notify,
}

impl RuntimeShutdownState {
    fn new(thread: thread::JoinHandle<()>) -> Self {
        Self {
            started: AtomicBool::new(false),
            thread: StdMutex::new(Some(thread)),
            result: StdMutex::new(None),
            completed: Notify::new(),
        }
    }

    fn start(self: &Arc<Self>, sender: mpsc::Sender<RuntimeCommand>) {
        if self
            .started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }

        let state = Arc::clone(self);
        tokio::spawn(async move {
            let (reply_tx, reply_rx) = oneshot::channel();
            let command_result = if sender
                .send(RuntimeCommand::Shutdown { reply: reply_tx })
                .await
                .is_err()
            {
                // The runtime may already have stopped. The thread join below
                // determines whether that was a clean exit or a panic.
                Ok(())
            } else {
                match reply_rx.await {
                    Ok(result) => result,
                    Err(_) => Err(PlatformError::internal("runtime shutdown channel closed")),
                }
            };

            let runtime_thread = match state.thread.lock() {
                Ok(mut thread) => thread.take(),
                Err(_) => {
                    state.finish(Err(PlatformError::internal(
                        "runtime thread join state is poisoned",
                    )));
                    return;
                }
            };
            let join_result = match runtime_thread {
                Some(runtime_thread) => {
                    match tokio::task::spawn_blocking(move || runtime_thread.join()).await {
                        Ok(Ok(())) => Ok(()),
                        Ok(Err(_)) => Err(PlatformError::internal(
                            "runtime thread panicked during shutdown",
                        )),
                        Err(error) => Err(PlatformError::internal(format!(
                            "runtime thread join task failed: {error}"
                        ))),
                    }
                }
                None => Err(PlatformError::internal(
                    "runtime thread join handle is unavailable",
                )),
            };
            state.finish(command_result.and(join_result));
        });
    }

    fn finish(&self, result: Result<()>) {
        match self.result.lock() {
            Ok(mut stored) => *stored = Some(result),
            Err(mut poisoned) => **poisoned.get_mut() = Some(result),
        }
        self.completed.notify_waiters();
    }

    async fn wait(&self) -> Result<()> {
        loop {
            let notified = self.completed.notified();
            let result = match self.result.lock() {
                Ok(result) => result.clone(),
                Err(poisoned) => poisoned.into_inner().clone(),
            };
            if let Some(result) = result {
                return result;
            }
            notified.await;
        }
    }

    #[cfg(test)]
    pub(super) fn is_complete(&self) -> bool {
        match self.result.lock() {
            Ok(result) => result.is_some(),
            Err(poisoned) => poisoned.into_inner().is_some(),
        }
    }
}

#[derive(Clone)]
pub struct RuntimeService {
    sender: mpsc::Sender<RuntimeCommand>,
    cancel_sender: mpsc::Sender<RuntimeCommand>,
    asset_catalog: AssetCatalog,
    kv_store: KvStore,
    memory_store: MemoryStore,
    cache_store: CacheStore,
    control_store: ControlStore,
    pub(super) _dynamic_modules: crate::dynamic_modules::DynamicModuleRegistry,
    storage: RuntimeStorageConfig,
    pub(super) shutdown: Arc<RuntimeShutdownState>,
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
        if storage.memory_outbox_max_concurrent_shards == 0 {
            return Err(PlatformError::internal(
                "memory_outbox_max_concurrent_shards must be greater than 0",
            ));
        }
        if storage.memory_db_read_connections_per_database == 0 {
            return Err(PlatformError::internal(
                "memory_db_read_connections_per_database must be greater than 0",
            ));
        }
        if storage.memory_db_max_total_connections == 0 {
            return Err(PlatformError::internal(
                "memory_db_max_total_connections must be greater than 0",
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

        let control_store = ControlStore::open(&storage.store_dir).await?;
        if storage.worker_store_enabled {
            control_store
                .import_legacy_workers(&storage.store_dir.join("workers"))
                .await?;
        }

        let bootstrap_snapshot = build_bootstrap_snapshot().await?;
        // KV and cache intentionally share one Turso database owner. Each
        // subsystem still configures its own connections for its durability
        // class (FULL for KV, NORMAL for rebuildable cache data).
        let storage_database = KvStore::open_database(&storage.database_url).await?;
        let kv_store = KvStore::from_database(Arc::clone(&storage_database)).await?;
        kv_store.set_profile_enabled(runtime.kv_profile_enabled);
        let memory_store = MemoryStore::new_with_connection_limits(
            storage.store_dir.join("memory"),
            storage.memory_namespace_shards,
            storage.memory_db_cache_max_open,
            storage.memory_db_idle_ttl,
            storage.memory_db_read_connections_per_database,
            storage.memory_db_max_total_connections,
        )
        .await?;
        memory_store.set_profile_enabled(runtime.memory_profile_enabled);
        let blob_store = BlobStore::for_legacy_root(storage.store_dir.join("blobs")).await?;
        let cache_store = CacheStore::from_database(
            CacheConfig {
                max_entries: runtime.cache_max_entries,
                max_bytes: runtime.cache_max_bytes,
                default_ttl: runtime.cache_default_ttl,
                ..CacheConfig::default()
            },
            storage_database,
            blob_store,
        )
        .await?;
        let (sender, receiver) = mpsc::channel(256);
        let (cancel_sender, cancel_receiver) = mpsc::channel(RUNTIME_FAST_COMMAND_CHANNEL_CAPACITY);
        let asset_catalog = AssetCatalog::default();
        let dynamic_modules = crate::dynamic_modules::DynamicModuleRegistry::default();
        let runtime_thread = spawn_runtime_thread(RuntimeThreadStart {
            receiver,
            cancel_receiver,
            runtime_fast_sender: cancel_sender.clone(),
            asset_catalog: asset_catalog.clone(),
            bootstrap_snapshot,
            kv_store: kv_store.clone(),
            memory_store: memory_store.clone(),
            cache_store: cache_store.clone(),
            config: runtime,
            storage: storage.clone(),
            control_store: control_store.clone(),
            dynamic_modules: dynamic_modules.clone(),
        })?;
        let shutdown = Arc::new(RuntimeShutdownState::new(runtime_thread));
        let service = Self {
            sender,
            cancel_sender,
            asset_catalog,
            kv_store,
            memory_store,
            cache_store,
            control_store,
            _dynamic_modules: dynamic_modules,
            storage,
            shutdown,
        };
        if let Err(error) = service.restore_workers_from_store().await {
            let _ = service.shutdown().await;
            return Err(error);
        }
        Ok(service)
    }

    async fn restore_workers_from_store(&self) -> Result<()> {
        if !self.storage.worker_store_enabled {
            return Ok(());
        }
        let now_ms = epoch_ms_i64()?;
        let mut restored = 0usize;
        for stored in self.control_store.active_deployments().await? {
            if stored
                .expires_at_ms
                .is_some_and(|expires_at_ms| expires_at_ms <= now_ms)
            {
                info!(
                    worker = %stored.worker,
                    deployment_id = %stored.deployment_id,
                    "deactivating expired temporary worker from control store"
                );
                self.control_store.deactivate_worker(&stored.worker).await?;
                continue;
            }
            let worker = stored.worker.clone();
            let deployment_id = stored.deployment_id.clone();
            let restore_result = self
                .deploy_with_config_internal(DeployWithConfigRequest {
                    worker_name: stored.worker,
                    source: stored.source,
                    config: stored.config,
                    assets: stored.assets,
                    server_modules: stored.server_modules,
                    asset_headers: stored.asset_headers,
                    deployment_id: Some(deployment_id.clone()),
                    persist: false,
                    temporary: stored.expires_at_ms.is_some(),
                    expires_at_ms: stored.expires_at_ms,
                    enforce_temporary_transition: false,
                })
                .await;
            let diagnostic = restore_result.as_ref().map(|_| ()).map_err(Clone::clone);
            self.control_store
                .record_restore_result(&worker, Some(&deployment_id), &diagnostic)
                .await?;
            match restore_result {
                Ok(restored_id) => {
                    restored += 1;
                    info!(
                        worker = %worker,
                        deployment_id = %restored_id,
                        "restored worker from control store"
                    );
                }
                Err(error) => {
                    return Err(PlatformError::internal(format!(
                        "failed to restore worker {worker} deployment {deployment_id}: {error}"
                    )));
                }
            }
        }

        if restored > 0 {
            info!(restored, "restored workers from control store");
        }
        Ok(())
    }

    pub fn control_store(&self) -> ControlStore {
        self.control_store.clone()
    }

    pub async fn deployments(
        &self,
        worker: Option<&str>,
    ) -> Result<Vec<common::DeploymentSummary>> {
        Ok(self
            .control_store
            .list_deployments(worker)
            .await?
            .iter()
            .map(ControlDeployment::summary)
            .collect())
    }

    pub async fn deployment(&self, deployment_id: &str) -> Result<common::DeploymentDetails> {
        Ok(self
            .control_store
            .get_deployment(deployment_id)
            .await?
            .details())
    }

    pub async fn undeploy(&self, worker_name: String) -> Result<()> {
        if !self.control_store.deactivate_worker(&worker_name).await? {
            return Err(PlatformError::not_found("worker not found"));
        }
        let (reply_tx, reply_rx) = oneshot::channel();
        self.sender
            .send(RuntimeCommand::Undeploy {
                worker_name,
                reply: reply_tx,
            })
            .await
            .map_err(|_| PlatformError::internal("runtime thread is not available"))?;
        reply_rx
            .await
            .map_err(|_| PlatformError::internal("runtime undeploy channel closed"))?
    }

    pub async fn rollback(&self, worker_name: String, deployment_id: String) -> Result<String> {
        let stored = self.control_store.get_deployment(&deployment_id).await?;
        if stored.worker != worker_name {
            return Err(PlatformError::bad_request(
                "deployment does not belong to requested worker",
            ));
        }
        let now_ms = epoch_ms_i64()?;
        if stored
            .expires_at_ms
            .is_some_and(|expires_at_ms| expires_at_ms <= now_ms)
        {
            return Err(PlatformError::conflict(
                "expired temporary deployment cannot be rolled back",
            ));
        }
        let restored_id = self
            .deploy_with_config_internal(DeployWithConfigRequest {
                worker_name: stored.worker.clone(),
                source: stored.source,
                config: stored.config,
                assets: stored.assets,
                server_modules: stored.server_modules,
                asset_headers: stored.asset_headers,
                deployment_id: Some(stored.deployment_id.clone()),
                persist: false,
                temporary: stored.expires_at_ms.is_some(),
                expires_at_ms: stored.expires_at_ms,
                enforce_temporary_transition: false,
            })
            .await?;
        self.control_store
            .activate_deployment(&worker_name, &restored_id)
            .await?;
        Ok(restored_id)
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
            deployment_id: None,
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
            deployment_id,
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
                deployment_id,
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
        let invoke_span = if tracing::enabled!(Level::DEBUG) {
            let span = tracing::debug_span!(
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
        async move {
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
        .instrument(invoke_span.unwrap_or_else(tracing::Span::none))
        .await
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
        let stream_span = if tracing::enabled!(Level::DEBUG) {
            let span = tracing::debug_span!(
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
        async move {
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
        .instrument(stream_span.unwrap_or_else(tracing::Span::none))
        .await
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

    pub async fn admin_snapshot(&self) -> RuntimeAdminSnapshot {
        let worker_names = self.asset_catalog.worker_names();
        let active_deployments = worker_names.len();
        let workers = self.worker_statuses(worker_names).await;
        let restore_failures = match self.control_store.restore_failures().await {
            Ok(failures) => failures
                .into_iter()
                .map(|failure| RuntimeRestoreFailure {
                    worker: Some(failure.worker),
                    source: failure
                        .deployment_id
                        .map(|id| format!("control.db deployment {id}"))
                        .unwrap_or_else(|| "control.db".to_string()),
                    error: failure.error,
                })
                .collect(),
            Err(error) => vec![RuntimeRestoreFailure {
                worker: None,
                source: "control.db".to_string(),
                error: error.to_string(),
            }],
        };
        let readiness = self.readiness().await;
        RuntimeAdminSnapshot {
            active_deployments,
            workers,
            restore_failures,
            readiness,
            storage_retry_count: crate::turso_util::storage_retry_count(),
            cache_flush_failure_count: self.cache_store.flush_failure_count(),
            cache_pending_recency_touches: self.cache_store.pending_touch_count(),
        }
    }

    async fn worker_statuses(&self, worker_names: Vec<String>) -> Vec<RuntimeWorkerStatus> {
        join_all(worker_names.into_iter().map(|name| async move {
            self.stats(name.clone())
                .await
                .map(|stats| RuntimeWorkerStatus {
                    name,
                    outbox_lag_shards: stats
                        .pending_memory_outbox_shards
                        .max(stats.memory_outbox_worker_pending_shards)
                        .saturating_add(stats.memory_outbox_worker_in_flight_shards),
                    stats,
                })
        }))
        .await
        .into_iter()
        .flatten()
        .collect()
    }

    pub async fn is_quiescent(&self) -> bool {
        let worker_names = self.asset_catalog.worker_names();
        let expected = worker_names.len();
        let workers = self.worker_statuses(worker_names).await;
        workers.len() == expected
            && workers.iter().all(|worker| {
                let stats = &worker.stats;
                stats.queued == 0
                    && stats.busy == 0
                    && stats.inflight_total == 0
                    && stats.wait_until_total == 0
                    && stats.pending_memory_outbox_shards == 0
                    && stats.memory_outbox_worker_pending_shards == 0
                    && stats.memory_outbox_worker_in_flight_shards == 0
            })
    }

    pub async fn wait_for_quiescence(&self, timeout: Duration) -> bool {
        if self.is_quiescent().await {
            return true;
        }
        if timeout.is_zero() {
            return false;
        }
        let wait = async {
            loop {
                if self.is_quiescent().await {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        };
        tokio::time::timeout(timeout, wait).await.is_ok()
    }

    pub async fn readiness(&self) -> RuntimeReadiness {
        let (control, kv, cache, memory, restore_failures) = tokio::join!(
            self.control_store.health_check(),
            self.kv_store.health_check(),
            self.cache_store.health_check(),
            self.memory_store.health_check(),
            self.control_store.restore_failures(),
        );
        let mut failed_components = Vec::new();
        if control.is_err() {
            failed_components.push("control".to_string());
        }
        if kv.is_err() {
            failed_components.push("kv".to_string());
        }
        if cache.is_err() {
            failed_components.push("cache".to_string());
        }
        if memory.is_err() {
            failed_components.push("memory".to_string());
        }
        if restore_failures.is_err() {
            failed_components.push("restoration_diagnostics".to_string());
        }
        let runtime_ready = !self.sender.is_closed();
        if !runtime_ready {
            failed_components.push("runtime".to_string());
        }
        let restore_failure_count = restore_failures
            .as_ref()
            .map_or(0, |failures| failures.len());
        let storage_ready = control.is_ok() && kv.is_ok() && cache.is_ok() && memory.is_ok();
        // Each component health check also verifies its current migration
        // version, so migration readiness is intentionally conservative when
        // any store cannot be inspected.
        let migrations_ready = storage_ready;
        let worker_restoration_ready = restore_failures.is_ok() && restore_failure_count == 0;
        RuntimeReadiness {
            ready: runtime_ready && migrations_ready && storage_ready && worker_restoration_ready,
            runtime_ready,
            migrations_ready,
            storage_ready,
            worker_restoration_ready,
            restore_failure_count,
            failed_components,
        }
    }

    pub async fn checkpoint(&self) -> Result<RuntimeCheckpointResult> {
        self.cache_store.flush_pending_touches().await?;
        self.control_store.checkpoint().await?;
        self.kv_store.checkpoint().await?;
        self.cache_store.checkpoint().await?;
        let memory_databases = self.memory_store.checkpoint_all_databases().await?;
        Ok(RuntimeCheckpointResult {
            kv: true,
            cache: true,
            memory_databases,
            control: true,
        })
    }

    pub fn worker_is_public(&self, worker_name: &str) -> bool {
        self.asset_catalog
            .get(worker_name)
            .is_some_and(|entry| entry.worker_name == worker_name && entry.public)
    }

    pub fn worker_cache_enabled(&self, worker_name: &str) -> bool {
        self.asset_catalog
            .get(worker_name)
            .is_some_and(|entry| entry.worker_name == worker_name && entry.cache_enabled)
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
        // Use the fast control lane so shutdown is not queued behind a full
        // request/deploy channel once the bounded drain deadline has elapsed.
        self.shutdown.start(self.cancel_sender.clone());
        self.shutdown.wait().await
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
        let span = tracing::debug_span!(
            "runtime.cache.match",
            cache.name = %request.cache_name,
            http.method = %request.method,
            http.url = %request.url
        );
        self.cache_store.get(&request).instrument(span).await
    }

    pub async fn cache_put(&self, request: CacheRequest, response: CacheResponse) -> Result<bool> {
        let span = tracing::debug_span!(
            "runtime.cache.put",
            cache.name = %request.cache_name,
            http.method = %request.method,
            http.url = %request.url,
            response.status = response.status as u64,
            response.body_size = response.body.len() as u64
        );
        self.cache_store
            .put(&request, response)
            .instrument(span)
            .await
    }
}

fn ensure_rustls_crypto_provider() {
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    });
}
