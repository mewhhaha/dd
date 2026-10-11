use super::*;
use serde::Serialize;
mod pending_queue;

pub(super) use pending_queue::*;
pub(super) struct WorkerManager {
    pub(super) config: RuntimeConfig,
    pub(super) control_store: ControlStore,
    pub(super) module_registry: crate::module_registry::ModuleRegistry,
    pub(super) bootstrap_snapshot: &'static [u8],
    pub(super) runtime_fast_sender: RuntimeCommandSender,
    pub(super) kv_store: KvStore,
    pub(super) memory_store: MemoryStore,
    pub(super) memory_outbox_drain_sender: MemoryOutboxDrainSender,
    pub(super) cache_store: CacheStore,
    pub(super) console: crate::ops::WorkerConsoleSink,
    pub(super) inspector: Option<InspectorRegistry>,
    pub(super) asset_catalog: AssetCatalog,
    pub(super) workers: HashMap<String, WorkerEntry>,
    pub(super) queue_counters: RuntimeQueueCounters,
    pub(super) next_queue_expiry_at: Option<Instant>,
    pub(super) pre_canceled: HashMap<String, HashMap<String, Instant>>,
    pub(super) stream_registrations: HashMap<String, StreamRegistration>,
    pub(super) response_byte_budget: Arc<tokio::sync::Semaphore>,
    pub(super) admission: Arc<RuntimeAdmission>,
    pub(super) revalidation_keys: HashSet<String>,
    pub(super) revalidation_requests: HashMap<String, String>,
    pub(super) websocket_sessions: HashMap<String, WorkerWebSocketSession>,
    pub(super) websocket_handle_index: HashMap<String, String>,
    pub(super) websocket_open_handles: HashMap<String, HashSet<String>>,
    pub(super) open_handle_registry: crate::ops::MemoryOpenHandleRegistry,
    pub(super) websocket_outbound_frames: HashMap<String, WebSocketOutboundQueue>,
    pub(super) websocket_close_signals: HashMap<String, SocketCloseEvent>,
    pub(super) websocket_frame_waiters: HashMap<String, Vec<oneshot::Sender<Result<()>>>>,
    pub(super) websocket_pending_frame_replies: HashMap<String, Vec<WebSocketFrameReply>>,
    pub(super) websocket_open_waiters: HashMap<String, oneshot::Sender<Result<WebSocketOpen>>>,
    pub(super) runtime_batch_depth: usize,
    pub(super) pending_dispatches: HashSet<(String, u64)>,
    pub(super) pending_cleanup_workers: HashSet<String>,
    pub(super) pending_memory_outbox_shards: HashSet<usize>,
    pub(super) scale_up_requests: VecDeque<(String, u64)>,
    pub(super) scale_up_request_members: HashSet<(String, u64)>,
    pub(super) internal_rescue_isolate_slots: HashSet<IsolateSlotKey>,
    pub(super) exiting_isolate_slots: HashMap<IsolateSlotKey, IsolateStartup>,
    pub(super) isolate_thread_tracker: IsolateThreadTracker,
    pub(super) stats: RuntimeManagerStats,
    pub(super) next_generation: u64,
    pub(super) next_isolate_id: u64,
    pub(super) next_memory_entity_epoch: u64,
}

#[derive(Clone, Default)]
pub(super) struct IsolateThreadTracker {
    state: Arc<IsolateThreadTrackerState>,
}

#[derive(Default)]
struct IsolateThreadTrackerState {
    active: AtomicUsize,
    empty: Notify,
}

pub(super) struct IsolateThreadGuard {
    tracker: IsolateThreadTracker,
}

impl IsolateThreadTracker {
    pub(super) fn register(&self) -> IsolateThreadGuard {
        self.state.active.fetch_add(1, Ordering::AcqRel);
        IsolateThreadGuard {
            tracker: self.clone(),
        }
    }

    pub(super) async fn wait_for_empty(&self) {
        loop {
            let notified = self.state.empty.notified();
            if self.state.active.load(Ordering::Acquire) == 0 {
                return;
            }
            notified.await;
        }
    }
}

impl Drop for IsolateThreadGuard {
    fn drop(&mut self) {
        if self.tracker.state.active.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.tracker.state.empty.notify_waiters();
        }
    }
}

#[derive(Clone)]
pub(super) struct WorkerManagerInit {
    pub(super) admission: Arc<RuntimeAdmission>,
    pub(super) bootstrap_snapshot: &'static [u8],
    pub(super) console: crate::ops::WorkerConsoleSink,
    pub(super) inspector: Option<InspectorRegistry>,
    pub(super) kv_store: KvStore,
    pub(super) memory_store: MemoryStore,
    pub(super) memory_outbox_drain_sender: MemoryOutboxDrainSender,
    pub(super) cache_store: CacheStore,
    pub(super) config: RuntimeConfig,
    pub(super) control_store: ControlStore,
    pub(super) module_registry: crate::module_registry::ModuleRegistry,
    pub(super) runtime_fast_sender: RuntimeCommandSender,
    pub(super) asset_catalog: AssetCatalog,
}

pub(crate) struct PreparedWorkerDeployment {
    pub(super) worker_name: String,
    pub(super) source: String,
    pub(super) config: DeployConfig,
    pub(super) assets: Vec<DeployAsset>,
    pub(super) server_modules: Vec<DeployServerModule>,
    pub(super) asset_headers: Option<String>,
    pub(super) compiled_assets: Arc<AssetBundle>,
    pub(super) bindings: DeployBindings,
}

#[derive(Clone, Copy, Default)]
pub(super) struct RuntimeQueueCounters {
    pub(super) requests: usize,
    pub(super) bytes: usize,
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct RuntimeManagerStats {
    pub(super) ready_work_budget_exhausted_count: u64,
    pub(super) max_ready_work_batch_size: usize,
    pub(super) memory_outbox_claim_batch_count: u64,
    pub(super) memory_outbox_claim_row_count: u64,
    pub(super) memory_outbox_saturated_batch_count: u64,
    pub(super) memory_outbox_delivery_success_count: u64,
    pub(super) memory_outbox_delivery_retry_count: u64,
    pub(super) memory_outbox_terminal_drop_count: u64,
    pub(super) memory_outbox_ack_failure_count: u64,
    pub(super) memory_outbox_channel_full_count: u64,
    pub(super) memory_outbox_reschedule_count: u64,
    pub(super) memory_outbox_worker_pending_shards: usize,
    pub(super) memory_outbox_worker_in_flight_shards: usize,
    pub(super) memory_outbox_worker_parallelism_limit: usize,
    pub(super) memory_outbox_worker_parallelism_peak: usize,
    pub(super) memory_outbox_duplicate_schedule_coalesced_count: u64,
    pub(super) memory_outbox_task_failure_count: u64,
    pub(super) memory_outbox_shard_requeue_count: u64,
    pub(super) scale_up_budget_denied_count: u64,
}

pub(super) struct WorkerWebSocketSession {
    pub(super) namespace: String,
    pub(super) worker_name: String,
    pub(super) generation: u64,
    pub(super) owner_isolate_id: u64,
    pub(super) binding: String,
    pub(super) key: String,
    pub(super) handle: String,
}

#[derive(Clone)]
pub(super) struct SocketCloseEvent {
    pub(super) code: u16,
    pub(super) reason: String,
}

pub(super) struct WebSocketOutboundFrame {
    pub(super) is_binary: bool,
    pub(super) payload: Vec<u8>,
    pub(super) lease: WebSocketFrameLease,
}

pub(super) struct WebSocketOutboundQueue {
    pub(super) frames: VecDeque<WebSocketOutboundFrame>,
    pub(super) byte_budget: Arc<tokio::sync::Semaphore>,
}

impl WebSocketOutboundQueue {
    pub(super) fn new(byte_limit: usize) -> Self {
        Self {
            frames: VecDeque::new(),
            byte_budget: Arc::new(tokio::sync::Semaphore::new(byte_limit)),
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.frames.is_empty()
    }

    pub(super) fn pop_front(&mut self) -> Option<WebSocketOutboundFrame> {
        self.frames.pop_front()
    }
}

pub(super) enum InvocationReply {
    Worker(oneshot::Sender<Result<WorkerOutput>>),
    WebSocket(oneshot::Sender<Result<WebSocketFrameOutput>>),
}

impl From<oneshot::Sender<Result<WorkerOutput>>> for InvocationReply {
    fn from(reply: oneshot::Sender<Result<WorkerOutput>>) -> Self {
        Self::Worker(reply)
    }
}

impl From<oneshot::Sender<Result<WebSocketFrameOutput>>> for InvocationReply {
    fn from(reply: oneshot::Sender<Result<WebSocketFrameOutput>>) -> Self {
        Self::WebSocket(reply)
    }
}

impl InvocationReply {
    pub(super) fn send(self, result: Result<WorkerOutput>) -> std::result::Result<(), ()> {
        match self {
            Self::Worker(reply) => reply.send(result).map_err(|_| ()),
            Self::WebSocket(reply) => reply
                .send(result.map(WebSocketFrameOutput::from))
                .map_err(|_| ()),
        }
    }

    pub(super) fn send_websocket(
        self,
        result: Result<WebSocketFrameOutput>,
    ) -> std::result::Result<(), ()> {
        match self {
            Self::WebSocket(reply) => reply.send(result).map_err(|_| ()),
            Self::Worker(_) => unreachable!("WebSocket frames require a frame reply channel"),
        }
    }
}

pub(super) struct WebSocketFrameRequest {
    pub(super) worker_name: String,
    pub(super) session_id: String,
    pub(super) frame: Vec<u8>,
    pub(super) is_binary: bool,
    pub(super) queue_admission: Option<QueueAdmission>,
    pub(super) reply: oneshot::Sender<Result<WebSocketFrameOutput>>,
}

pub(super) struct WebSocketFrameReply {
    pub(super) output: WebSocketFrameOutput,
    pub(super) reply: InvocationReply,
}

pub(super) struct StreamRegistration {
    pub(super) worker_name: String,
    pub(super) completion_token: Option<String>,
    pub(super) ready: Option<oneshot::Sender<Result<WorkerStreamOutput>>>,
    pub(super) body_sender: mpsc::Sender<Bytes>,
    pub(super) body_receiver: Option<(mpsc::Receiver<Bytes>, oneshot::Receiver<Result<()>>)>,
    pub(super) completion: Option<oneshot::Sender<Result<()>>>,
    pub(super) pending_send: Option<tokio::task::JoinHandle<()>>,
    pub(super) started: bool,
    pub(super) bytes_sent: usize,
    pub(super) max_bytes: usize,
}

impl Drop for StreamRegistration {
    fn drop(&mut self) {
        if let Some(task) = self.pending_send.take() {
            task.abort();
        }
    }
}

pub(super) struct WorkerEntry {
    pub(super) current_generation: u64,
    pub(super) pools: HashMap<u64, WorkerPool>,
}

#[derive(Clone, Debug)]
pub(super) struct InternalTraceDestination {
    pub(super) worker: String,
    pub(super) path: String,
}

pub(super) struct WorkerPool {
    pub(super) worker_name: String,
    pub(super) generation: u64,
    pub(super) deployment_id: String,
    pub(super) internal_trace: Option<InternalTraceDestination>,
    pub(super) is_public: bool,
    pub(super) expires_at_ms: Option<i64>,
    pub(super) retired_at: Option<Instant>,
    pub(super) snapshot: &'static [u8],
    pub(super) snapshot_preloaded: bool,
    pub(super) source: crate::ops::WorkerSource,
    pub(super) memory_bindings: Vec<String>,
    pub(super) deployment_config: Arc<crate::ops::WorkerDeploymentPayload>,
    pub(super) request_context: RequestExecutionContext,
    pub(super) strict_request_isolation: bool,
    pub(super) memory_entity_leases: HashMap<String, MemoryEntityLease>,
    pub(super) memory_shard_affinity: HashMap<usize, u64>,
    pub(super) queue: PendingInvokeQueue,
    pub(super) isolates: Vec<IsolateHandle>,
    pub(super) isolate_indices: HashMap<u64, usize>,
    pub(super) stats: PoolStats,
    pub(super) queue_warn_level: usize,
}

impl WorkerPool {
    pub(super) fn push_isolate(&mut self, isolate: IsolateHandle) {
        let isolate_id = isolate.id;
        self.isolate_indices.insert(isolate_id, self.isolates.len());
        self.isolates.push(isolate);
    }

    pub(super) fn isolate_idx(&self, isolate_id: u64) -> Option<usize> {
        self.isolate_indices.get(&isolate_id).copied()
    }

    pub(super) fn swap_remove_isolate(&mut self, isolate_idx: usize) -> Option<IsolateHandle> {
        if isolate_idx >= self.isolates.len() {
            return None;
        }
        let removed = self.isolates.swap_remove(isolate_idx);
        self.isolate_indices.remove(&removed.id);
        if isolate_idx < self.isolates.len() {
            self.isolate_indices
                .insert(self.isolates[isolate_idx].id, isolate_idx);
        }
        Some(removed)
    }

    pub(super) fn clear_isolate_indices(&mut self) {
        self.isolate_indices.clear();
    }

    pub(super) fn acquire_memory_entity_lease(
        &mut self,
        owner_key: String,
        owner_isolate_id: u64,
        epoch: u64,
    ) -> MemoryEntityLease {
        let lease = MemoryEntityLease {
            owner_key: owner_key.clone(),
            epoch: epoch.max(1),
            owner_isolate_id,
        };
        self.memory_entity_leases.insert(owner_key, lease.clone());
        lease
    }

    pub(super) fn release_memory_entity_lease(&mut self, lease: &MemoryEntityLease) -> bool {
        let should_release = self
            .memory_entity_leases
            .get(&lease.owner_key)
            .map(|current| current.matches(lease))
            .unwrap_or(false);
        if should_release {
            self.memory_entity_leases.remove(&lease.owner_key);
            self.queue.mark_memory_owner_ready(&lease.owner_key);
        }
        should_release
    }
}

fn duration_millis_u64(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

#[derive(Default)]
pub(super) struct PoolStats {
    pub(super) spawn_count: u64,
    pub(super) reuse_count: u64,
    pub(super) scale_down_count: u64,
    pub(super) memory_affinity_hit_count: u64,
    pub(super) memory_affinity_miss_no_mapping_count: u64,
    pub(super) memory_affinity_miss_stale_count: u64,
    pub(super) memory_affinity_miss_saturated_count: u64,
    pub(super) memory_least_loaded_fallback_count: u64,
    pub(super) memory_atomic_overflow_dispatch_count: u64,
    pub(super) memory_candidate_rejected_owner_lease_count: u64,
    pub(super) memory_candidate_rejected_isolate_state_count: u64,
    pub(super) memory_candidate_heads_inspected_count: u64,
    pub(super) memory_dispatch_no_ready_candidate_count: u64,
}

impl PoolStats {
    pub(super) fn record_dispatch_attempt(&mut self, stats: DispatchAttemptStats) {
        self.memory_affinity_hit_count = self
            .memory_affinity_hit_count
            .saturating_add(stats.memory_affinity_hit_count);
        self.memory_affinity_miss_no_mapping_count = self
            .memory_affinity_miss_no_mapping_count
            .saturating_add(stats.memory_affinity_miss_no_mapping_count);
        self.memory_affinity_miss_stale_count = self
            .memory_affinity_miss_stale_count
            .saturating_add(stats.memory_affinity_miss_stale_count);
        self.memory_affinity_miss_saturated_count = self
            .memory_affinity_miss_saturated_count
            .saturating_add(stats.memory_affinity_miss_saturated_count);
        self.memory_least_loaded_fallback_count = self
            .memory_least_loaded_fallback_count
            .saturating_add(stats.memory_least_loaded_fallback_count);
        self.memory_atomic_overflow_dispatch_count = self
            .memory_atomic_overflow_dispatch_count
            .saturating_add(stats.memory_atomic_overflow_dispatch_count);
        self.memory_candidate_rejected_owner_lease_count = self
            .memory_candidate_rejected_owner_lease_count
            .saturating_add(stats.memory_candidate_rejected_owner_lease_count);
        self.memory_candidate_rejected_isolate_state_count = self
            .memory_candidate_rejected_isolate_state_count
            .saturating_add(stats.memory_candidate_rejected_isolate_state_count);
        self.memory_candidate_heads_inspected_count = self
            .memory_candidate_heads_inspected_count
            .saturating_add(stats.memory_candidate_heads_inspected_count);
        self.memory_dispatch_no_ready_candidate_count = self
            .memory_dispatch_no_ready_candidate_count
            .saturating_add(stats.memory_dispatch_no_ready_candidate_count);
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct DispatchAttemptStats {
    pub(super) memory_affinity_hit_count: u64,
    pub(super) memory_affinity_miss_no_mapping_count: u64,
    pub(super) memory_affinity_miss_stale_count: u64,
    pub(super) memory_affinity_miss_saturated_count: u64,
    pub(super) memory_least_loaded_fallback_count: u64,
    pub(super) memory_atomic_overflow_dispatch_count: u64,
    pub(super) memory_candidate_rejected_owner_lease_count: u64,
    pub(super) memory_candidate_rejected_isolate_state_count: u64,
    pub(super) memory_candidate_heads_inspected_count: u64,
    pub(super) memory_dispatch_no_ready_candidate_count: u64,
}

pub(super) struct PendingInvoke {
    pub(super) queue_admission: Option<QueueAdmission>,
    pub(super) runtime_request_id: String,
    pub(super) request: WorkerInvocation,
    pub(super) request_body: Option<InvokeRequestBodyReceiver>,
    pub(super) memory_route: Option<MemoryRoute>,
    pub(super) memory_call: Option<MemoryExecutionCall>,
    pub(super) target_isolate_id: Option<u64>,
    pub(super) reply_kind: PendingReplyKind,
    pub(super) internal_origin: bool,
    pub(super) reply: InvocationReply,
    pub(super) enqueued_at: Instant,
    pub(super) queued_bytes: usize,
}

pub(crate) struct EnqueueInvokeRequest {
    pub(super) queue_admission: Option<QueueAdmission>,
    pub(super) worker_name: String,
    pub(super) runtime_request_id: String,
    pub(super) request: WorkerInvocation,
    pub(super) request_body: Option<InvokeRequestBodyReceiver>,
    pub(super) memory_route: Option<MemoryRoute>,
    pub(super) memory_call: Option<MemoryExecutionCall>,
    pub(super) target_isolate_id: Option<u64>,
    pub(super) target_generation: Option<u64>,
    pub(super) internal_origin: bool,
    pub(super) reply: InvocationReply,
    pub(super) reply_kind: PendingReplyKind,
}

#[derive(Clone, Default)]
pub(super) enum PendingReplyKind {
    #[default]
    Normal,
    Stream,
    WebsocketOpen {
        session_id: String,
    },
    WebsocketFrame {
        session_id: String,
    },
}

impl PendingReplyKind {
    pub(super) fn label(&self) -> &'static str {
        match self {
            Self::Normal => "normal",
            Self::Stream => "stream",
            Self::WebsocketOpen { .. } => "websocket-open",
            Self::WebsocketFrame { .. } => "websocket-frame",
        }
    }
}

pub(super) struct PendingReply {
    pub(super) completion_token: String,
    pub(super) canceled: bool,
    pub(super) memory_key: Option<String>,
    pub(super) active_memory_lease: Option<MemoryEntityLease>,
    pub(super) memory_outbox_shard: Option<usize>,
    pub(super) internal_origin: bool,
    pub(super) reply: InvocationReply,
    pub(super) completion_meta: Option<PendingReplyMeta>,
    pub(super) kind: PendingReplyKind,
    pub(super) dispatched_at: Instant,
}

#[derive(Default)]
pub(super) struct RemovedIsolate {
    pub(super) removed: bool,
    pub(super) replies: Vec<(String, InvocationReply)>,
}

#[derive(Clone, Copy)]
pub(super) enum IsolateSlotReservation {
    Regular,
    InternalRescue,
}

#[derive(Clone, Copy)]
pub(super) enum IsolateSpawnPolicy {
    WithinGlobalBudget,
    AllowInternalRescue,
    InternalRescueOnly,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct IsolateSlotKey {
    pub(super) worker_name: String,
    pub(super) generation: u64,
    pub(super) isolate_id: u64,
}

impl IsolateSlotKey {
    pub(super) fn new(worker_name: &str, generation: u64, isolate_id: u64) -> Self {
        Self {
            worker_name: worker_name.to_string(),
            generation,
            isolate_id,
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct MemoryEntityLease {
    pub(super) owner_key: String,
    pub(super) epoch: u64,
    pub(super) owner_isolate_id: u64,
}

impl MemoryEntityLease {
    fn matches(&self, other: &Self) -> bool {
        self.owner_key == other.owner_key
            && self.epoch == other.epoch
            && self.owner_isolate_id == other.owner_isolate_id
    }
}

pub(super) struct PendingReplyMeta {
    pub(super) method: String,
    pub(super) url: String,
    pub(super) traceparent: Option<String>,
    pub(super) user_request_id: String,
}

pub(super) struct TraceResultMeta {
    pub(super) status: Option<u16>,
    pub(super) error: Option<String>,
}

pub(super) struct FinishRequest {
    pub(super) worker_name: String,
    pub(super) generation: u64,
    pub(super) isolate_id: u64,
    pub(super) request_id: String,
    pub(super) completion_token: String,
    pub(super) finished_at: Instant,
    pub(super) wait_until_count: usize,
    pub(super) result: Result<WorkerOutput>,
}

pub(super) struct TraceForwardRequest {
    pub(super) worker_name: String,
    pub(super) generation: u64,
    pub(super) request_method: String,
    pub(super) request_url: String,
    pub(super) runtime_request_id: String,
    pub(super) user_request_id: String,
    pub(super) result: TraceResultMeta,
    pub(super) execution_ms: u64,
    pub(super) wait_until_count: usize,
    pub(super) internal_origin: bool,
    pub(super) trace_destination: Option<InternalTraceDestination>,
}

pub(super) struct BuildExecuteCommand {
    pub(super) runtime_request_id: String,
    pub(super) completion_token: String,
    pub(super) request: WorkerInvocation,
    pub(super) request_body: Option<InvokeRequestBodyReceiver>,
    pub(super) stream_response: bool,
    pub(super) memory_call: Option<MemoryExecutionCall>,
    pub(super) memory_route: Option<MemoryRoute>,
    pub(super) dispatched_at: Instant,
    pub(super) profile_memory_atomic: bool,
}

pub(super) struct ServiceBindingFetchStart {
    pub(super) reply_inbox: crate::ops::RequestControlInbox,
    pub(super) queue_admission: Option<QueueAdmission>,
    pub(super) owner_worker: String,
    pub(super) owner_generation: u64,
    pub(super) binding: String,
    pub(super) target_worker: String,
    pub(super) request: WorkerInvocation,
    pub(super) reply_id: String,
    pub(super) pending_replies: crate::ops::PendingReplies,
    pub(super) cancellation: crate::ops::PendingReplyCancellation,
}

pub(super) struct RuntimeThreadStart {
    pub(super) admission: Arc<RuntimeAdmission>,
    pub(super) routes: WorkerRoutes,
    pub(super) receiver: mpsc::Receiver<RuntimeCommand>,
    pub(super) cancel_receiver: mpsc::Receiver<RuntimeCommand>,
    pub(super) cancellation_receiver: mpsc::UnboundedReceiver<RuntimeCommand>,
    pub(super) runtime_fast_sender: RuntimeCommandSender,
    pub(super) asset_catalog: AssetCatalog,
    pub(super) bootstrap_snapshot: &'static [u8],
    pub(super) console: crate::ops::WorkerConsoleSink,
    pub(super) inspector: Option<InspectorRegistry>,
    pub(super) kv_store: KvStore,
    pub(super) memory_store: MemoryStore,
    pub(super) cache_store: CacheStore,
    pub(super) config: RuntimeConfig,
    pub(super) storage: RuntimeStorageConfig,
    pub(super) control_store: ControlStore,
    pub(super) module_registry: crate::module_registry::ModuleRegistry,
}

pub(super) struct IsolateThreadStart {
    pub(super) admission: IsolateAdmission,
    pub(super) snapshot: &'static [u8],
    pub(super) snapshot_preloaded: bool,
    pub(super) source: crate::ops::WorkerSource,
    pub(super) module_registry: crate::module_registry::ModuleRegistry,
    pub(super) deployment_config: Arc<crate::ops::WorkerDeploymentPayload>,
    pub(super) policy: crate::engine::IsolatePolicy,
    pub(super) console: crate::ops::WorkerConsoleSink,
    pub(super) inspector: Option<InspectorRegistry>,
    pub(super) kv_store: KvStore,
    pub(super) memory_store: MemoryStore,
    pub(super) cache_store: CacheStore,
    pub(super) open_handle_registry: crate::ops::MemoryOpenHandleRegistry,
    pub(super) execution_limits: crate::ops::RuntimeExecutionLimits,
    pub(super) runtime_fast_sender: RuntimeCommandSender,
    pub(super) worker_name: String,
    pub(super) generation: u64,
    pub(super) isolate_id: u64,
    pub(super) event_tx: RuntimeEventSender,
    pub(super) thread_tracker: IsolateThreadTracker,
}

pub(super) struct WebSocketSessionRegistration {
    pub(super) worker_name: String,
    pub(super) generation: u64,
    pub(super) isolate_id: u64,
    pub(super) session_id: String,
    pub(super) binding: String,
    pub(super) key: String,
    pub(super) handle: String,
}

#[derive(Debug, Clone)]
pub(super) struct MemoryRoute {
    pub(super) binding: String,
    pub(super) key: String,
    pub(super) owner_epoch: i64,
    pub(super) owner_key: String,
    pub(super) shard_index: Option<usize>,
}

impl MemoryRoute {
    pub(super) fn new(binding: String, key: String) -> Self {
        let owner_key = memory_owner_key(&binding, &key);
        Self {
            binding,
            key,
            owner_epoch: 0,
            owner_key,
            shard_index: None,
        }
    }

    pub(super) fn with_owner_epoch(mut self, owner_epoch: i64) -> Self {
        self.owner_epoch = owner_epoch;
        self
    }
}

pub(super) struct DispatchCandidate {
    pub(super) queue_key: PendingQueueKey,
    pub(super) isolate_idx: usize,
}

pub(super) enum DispatchSelection {
    Dispatch(DispatchCandidate),
    DropStaleTarget { queue_key: PendingQueueKey },
}

pub(super) struct IsolateHandle {
    pub(super) shutdown: tokio::sync::watch::Sender<()>,
    pub(super) slot_starting: Arc<AtomicBool>,
    pub(super) id: u64,
    pub(super) sender: mpsc::Sender<IsolateCommand>,
    pub(super) v8_handle: Arc<StdMutex<Option<dd_v8::RuntimeHandle>>>,
    pub(super) request_control_inbox: crate::ops::RequestControlInbox,
    pub(super) startup: IsolateStartup,
    pub(super) internal_rescue: bool,
    pub(super) inflight_count: usize,
    pub(super) active_websocket_sessions: usize,
    pub(super) served_requests: u64,
    pub(super) last_used_at: Instant,
    pub(super) pending_replies: HashMap<String, PendingReply>,
    pub(super) pending_wait_until: HashMap<String, PendingWaitUntil>,
}

pub(super) struct PendingWaitUntil {
    pub(super) completion_token: String,
    pub(super) completed_at: Instant,
}

impl IsolateHandle {
    pub(super) fn request_shutdown(&self) {
        self.shutdown.send_replace(());
        if let Some(handle) = self
            .v8_handle
            .lock()
            .expect("v8 handle mutex poisoned")
            .as_ref()
        {
            handle.terminate_execution();
        }
    }
}

#[derive(Clone, Copy)]
pub(super) enum IsolateStartup {
    Starting { started_at: Instant },
    Ready,
    Retiring,
}

impl IsolateStartup {
    pub(super) fn is_ready(&self) -> bool {
        matches!(self, Self::Ready)
    }

    pub(super) fn is_starting(&self) -> bool {
        matches!(self, Self::Starting { .. })
    }

    pub(super) fn timed_out(&self, now: Instant, timeout: Duration) -> bool {
        match self {
            Self::Starting { started_at } => now.duration_since(*started_at) >= timeout,
            Self::Ready | Self::Retiring => false,
        }
    }
}
#[derive(Serialize)]
pub(super) struct TraceEventPayload {
    pub(super) ts_ms: i64,
    pub(super) worker: String,
    pub(super) generation: u64,
    pub(super) request_id: String,
    pub(super) runtime_request_id: String,
    pub(super) method: String,
    pub(super) url: String,
    pub(super) status: Option<u16>,
    pub(super) ok: bool,
    pub(super) error: Option<String>,
    pub(super) execution_ms: u64,
    pub(super) wait_until_count: usize,
}

#[cfg(test)]
mod tests;
