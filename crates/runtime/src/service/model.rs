use super::*;
use serde::{Deserialize, Serialize};
pub(super) struct WorkerManager {
    pub(super) config: RuntimeConfig,
    pub(super) storage: RuntimeStorageConfig,
    pub(super) bootstrap_snapshot: &'static [u8],
    pub(super) runtime_fast_sender: mpsc::UnboundedSender<RuntimeCommand>,
    pub(super) kv_store: KvStore,
    pub(super) memory_store: MemoryStore,
    pub(super) cache_store: CacheStore,
    pub(super) workers: HashMap<String, WorkerEntry>,
    pub(super) pre_canceled: HashMap<String, HashSet<String>>,
    pub(super) stream_registrations: HashMap<String, StreamRegistration>,
    pub(super) revalidation_keys: HashSet<String>,
    pub(super) revalidation_requests: HashMap<String, String>,
    pub(super) websocket_sessions: HashMap<String, WorkerWebSocketSession>,
    pub(super) websocket_handle_index: HashMap<String, String>,
    pub(super) websocket_open_handles: HashMap<String, HashSet<String>>,
    pub(super) open_handle_registry: crate::ops::MemoryOpenHandleRegistry,
    pub(super) websocket_pending_closes: HashMap<String, HashMap<String, Vec<SocketCloseEvent>>>,
    pub(super) websocket_outbound_frames: HashMap<String, VecDeque<WebSocketOutboundFrame>>,
    pub(super) websocket_close_signals: HashMap<String, SocketCloseEvent>,
    pub(super) websocket_frame_waiters: HashMap<String, Vec<oneshot::Sender<Result<()>>>>,
    pub(super) websocket_open_waiters: HashMap<String, oneshot::Sender<Result<WebSocketOpen>>>,
    pub(super) transport_sessions: HashMap<String, WorkerTransportSession>,
    pub(super) transport_handle_index: HashMap<String, String>,
    pub(super) transport_open_handles: HashMap<String, HashSet<String>>,
    pub(super) transport_pending_closes: HashMap<String, HashMap<String, Vec<TransportCloseEvent>>>,
    pub(super) transport_open_channels: HashMap<String, TransportOpenChannels>,
    pub(super) transport_open_waiters: HashMap<String, oneshot::Sender<Result<TransportOpen>>>,
    pub(super) dynamic_worker_handles: HashMap<String, DynamicWorkerHandle>,
    pub(super) dynamic_worker_ids: HashMap<DynamicWorkerIdKey, HashMap<String, String>>,
    pub(super) host_rpc_providers: HashMap<String, HostRpcProvider>,
    pub(super) dynamic_profile: crate::ops::DynamicProfile,
    pub(super) validated_worker_sources: HashSet<[u8; 32]>,
    pub(super) dynamic_worker_snapshots: HashMap<[u8; 32], &'static [u8]>,
    pub(super) dynamic_worker_snapshot_failures: HashSet<[u8; 32]>,
    pub(super) runtime_batch_depth: usize,
    pub(super) pending_dispatches: HashSet<(String, u64)>,
    pub(super) pending_cleanup_workers: HashSet<String>,
    pub(super) next_generation: u64,
    pub(super) next_isolate_id: u64,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct DynamicWorkerIdKey {
    pub(super) owner_worker: String,
    pub(super) owner_generation: u64,
    pub(super) binding: String,
}

#[derive(Clone)]
pub(super) struct DynamicWorkerHandle {
    pub(super) id: String,
    pub(super) owner_worker: String,
    pub(super) owner_generation: u64,
    pub(super) binding: String,
    pub(super) worker_name: String,
    pub(super) worker_generation: u64,
    pub(super) timeout: u64,
    pub(super) policy: ValidatedDynamicWorkerPolicy,
    pub(super) host_rpc_provider_ids: Vec<String>,
    pub(super) preferred_isolate_id: Option<u64>,
    pub(super) quota_state: Arc<DynamicQuotaState>,
}

#[derive(Default)]
pub(crate) struct DynamicQuotaState {
    pub(crate) inflight: AtomicUsize,
    pub(crate) outbound_requests: AtomicU64,
    pub(crate) total_request_bytes: AtomicU64,
    pub(crate) total_response_bytes: AtomicU64,
    pub(crate) egress_deny_count: AtomicU64,
    pub(crate) rpc_deny_count: AtomicU64,
    pub(crate) quota_kill_count: AtomicU64,
    pub(crate) upgrade_deny_count: AtomicU64,
}

#[derive(Clone)]
pub(super) struct HostRpcProvider {
    pub(super) owner_worker: String,
    pub(super) owner_generation: u64,
    pub(super) owner_isolate_id: u64,
    pub(super) target_id: String,
    pub(super) methods: HashSet<String>,
}

pub(super) enum TargetedHostRpcReply {
    Dynamic {
        reply_id: String,
        pending_replies: crate::ops::DynamicPendingReplies,
    },
    Test {
        reply_id: String,
        replies: crate::ops::TestAsyncReplies,
        success_value: String,
    },
}

#[derive(Debug, Clone)]
pub(super) struct DynamicTimeoutDiagnostic {
    pub(super) stage: &'static str,
    pub(super) owner_worker: String,
    pub(super) owner_generation: u64,
    pub(super) binding: String,
    pub(super) handle: String,
    pub(super) target_worker: String,
    pub(super) target_isolate_id: Option<u64>,
    pub(super) target_generation: Option<u64>,
    pub(super) provider_id: Option<String>,
    pub(super) provider_owner_isolate_id: Option<u64>,
    pub(super) provider_target_id: Option<String>,
    pub(super) timeout_ms: u64,
}

#[derive(Clone)]
pub(super) struct WorkerWebSocketSession {
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

#[derive(Clone)]
pub(super) struct WebSocketOutboundFrame {
    pub(super) is_binary: bool,
    pub(super) payload: Vec<u8>,
}

#[derive(Clone)]
pub(super) struct WorkerTransportSession {
    pub(super) worker_name: String,
    pub(super) generation: u64,
    pub(super) owner_isolate_id: u64,
    pub(super) binding: String,
    pub(super) key: String,
    pub(super) handle: String,
    pub(super) stream_sender: mpsc::UnboundedSender<Vec<u8>>,
    pub(super) datagram_sender: mpsc::UnboundedSender<Vec<u8>>,
    pub(super) inbound_streams: VecDeque<Vec<u8>>,
    pub(super) inbound_stream_closed: bool,
    pub(super) inbound_datagrams: VecDeque<Vec<u8>>,
}

pub(super) struct TransportOpenChannels {
    pub(super) stream_sender: mpsc::UnboundedSender<Vec<u8>>,
    pub(super) datagram_sender: mpsc::UnboundedSender<Vec<u8>>,
}

#[derive(Clone)]
pub(super) struct TransportCloseEvent {
    pub(super) code: u16,
    pub(super) reason: String,
}

pub(super) struct StreamRegistration {
    pub(super) worker_name: String,
    pub(super) completion_token: Option<String>,
    pub(super) ready: Option<oneshot::Sender<Result<WorkerStreamOutput>>>,
    pub(super) body_sender: mpsc::UnboundedSender<Result<Vec<u8>>>,
    pub(super) body_receiver: Option<mpsc::UnboundedReceiver<Result<Vec<u8>>>>,
    pub(super) started: bool,
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
    pub(super) worker_name_json: Arc<str>,
    pub(super) generation: u64,
    pub(super) deployment_id: String,
    pub(super) internal_trace: Option<InternalTraceDestination>,
    pub(super) is_public: bool,
    pub(super) snapshot: &'static [u8],
    pub(super) snapshot_preloaded: bool,
    pub(super) source: Arc<str>,
    pub(super) kv_bindings_json: Arc<str>,
    pub(super) kv_read_cache_config_json: Arc<str>,
    pub(super) memory_bindings: Vec<String>,
    pub(super) memory_bindings_json: Arc<str>,
    pub(super) dynamic_bindings: Vec<String>,
    pub(super) dynamic_bindings_json: Arc<str>,
    pub(super) dynamic_rpc_bindings: Vec<DynamicRpcBinding>,
    pub(super) dynamic_rpc_bindings_json: Arc<str>,
    pub(super) dynamic_env_json: Arc<str>,
    pub(super) secret_replacements: Vec<(String, String)>,
    pub(super) egress_allow_hosts: Vec<String>,
    pub(super) dynamic_child_policy: Option<ValidatedDynamicWorkerPolicy>,
    pub(super) dynamic_quota_state: Option<Arc<DynamicQuotaState>>,
    pub(super) assets: AssetBundle,
    pub(super) strict_request_isolation: bool,
    pub(super) queue: VecDeque<PendingInvoke>,
    pub(super) isolates: Vec<IsolateHandle>,
    pub(super) memory_owners: HashMap<String, u64>,
    pub(super) memory_inflight: HashMap<String, usize>,
    pub(super) stats: PoolStats,
    pub(super) queue_warn_level: usize,
}

#[derive(Clone)]
pub(super) struct DynamicRpcBinding {
    pub(super) binding: String,
    pub(super) provider_id: String,
}

#[derive(Default)]
pub(super) struct PoolStats {
    pub(super) spawn_count: u64,
    pub(super) reuse_count: u64,
    pub(super) scale_down_count: u64,
}

pub(super) struct PendingInvoke {
    pub(super) runtime_request_id: String,
    pub(super) request: WorkerInvocation,
    pub(super) request_body: Option<InvokeRequestBodyReceiver>,
    pub(super) memory_route: Option<MemoryRoute>,
    pub(super) memory_call: Option<MemoryExecutionCall>,
    pub(super) host_rpc_call: Option<HostRpcExecutionCall>,
    pub(super) target_isolate_id: Option<u64>,
    pub(super) reply_kind: PendingReplyKind,
    pub(super) internal_origin: bool,
    pub(super) reply: oneshot::Sender<Result<WorkerOutput>>,
    pub(super) enqueued_at: Instant,
}

#[derive(Clone)]
pub(super) enum PendingReplyKind {
    Normal,
    DynamicInvoke { handle: String },
    DynamicFetch { handle: String },
    WebsocketOpen { session_id: String },
    WebsocketFrame { session_id: String },
    TransportOpen { session_id: String },
}

impl Default for PendingReplyKind {
    fn default() -> Self {
        Self::Normal
    }
}

impl PendingReplyKind {
    pub(super) fn label(&self) -> &'static str {
        match self {
            Self::Normal => "normal",
            Self::DynamicInvoke { .. } => "dynamic-invoke",
            Self::DynamicFetch { .. } => "dynamic-fetch",
            Self::WebsocketOpen { .. } => "websocket-open",
            Self::WebsocketFrame { .. } => "websocket-frame",
            Self::TransportOpen { .. } => "transport-open",
        }
    }
}

pub(super) struct PendingReply {
    pub(super) completion_token: String,
    pub(super) canceled: bool,
    pub(super) memory_key: Option<String>,
    pub(super) internal_origin: bool,
    pub(super) reply: oneshot::Sender<Result<WorkerOutput>>,
    pub(super) completion_meta: Option<PendingReplyMeta>,
    pub(super) kind: PendingReplyKind,
    pub(super) dispatched_at: Instant,
}

pub(super) struct PendingReplyMeta {
    pub(super) method: String,
    pub(super) url: String,
    pub(super) traceparent: Option<String>,
    pub(super) user_request_id: String,
}

pub(super) enum DirectDynamicFetchDispatch {
    Dispatched,
    Fallback {
        reply: oneshot::Sender<Result<WorkerOutput>>,
        clear_preferred: bool,
    },
}

#[derive(Debug, Clone)]
pub(super) struct MemoryRoute {
    pub(super) binding: String,
    pub(super) key: String,
}

impl MemoryRoute {
    pub(super) fn owner_key(&self) -> String {
        format!("{}\u{001f}{}", self.binding, self.key)
    }
}

pub(super) struct DispatchCandidate {
    pub(super) queue_idx: usize,
    pub(super) isolate_idx: usize,
    pub(super) memory_key: Option<String>,
    pub(super) assign_owner: bool,
}

pub(super) enum DispatchSelection {
    Dispatch(DispatchCandidate),
    DropStaleTarget { queue_idx: usize },
}

pub(super) struct IsolateHandle {
    pub(super) id: u64,
    pub(super) sender: std_mpsc::Sender<IsolateCommand>,
    pub(super) dynamic_control_inbox: crate::ops::DynamicControlInbox,
    pub(super) inflight_count: usize,
    pub(super) active_websocket_sessions: usize,
    pub(super) active_transport_sessions: usize,
    pub(super) served_requests: u64,
    pub(super) last_used_at: Instant,
    pub(super) pending_replies: HashMap<String, PendingReply>,
    pub(super) pending_wait_until: HashMap<String, String>,
}
#[derive(Deserialize)]
pub(super) struct CompletionPayload {
    pub(super) request_id: String,
    pub(super) completion_token: String,
    #[serde(default)]
    pub(super) wait_until_count: usize,
    pub(super) ok: bool,
    pub(super) result: Option<WorkerOutput>,
    pub(super) error: Option<String>,
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

#[derive(Deserialize)]
pub(super) struct WaitUntilPayload {
    pub(super) request_id: String,
    pub(super) completion_token: String,
}

#[derive(Deserialize)]
pub(super) struct ResponseStartPayload {
    pub(super) request_id: String,
    pub(super) completion_token: String,
    pub(super) status: u16,
    pub(super) headers: Vec<(String, String)>,
}

#[derive(Deserialize)]
pub(super) struct ResponseChunkPayload {
    pub(super) request_id: String,
    pub(super) completion_token: String,
    pub(super) chunk: Vec<u8>,
}

#[derive(Deserialize)]
pub(super) struct CacheRevalidatePayload {
    pub(super) cache_name: String,
    pub(super) method: String,
    pub(super) url: String,
    #[serde(default)]
    pub(super) headers: Vec<(String, String)>,
}

#[derive(Serialize, Deserialize)]
pub(super) struct StoredWorkerDeployment {
    pub(super) name: String,
    pub(super) source: String,
    pub(super) config: DeployConfig,
    #[serde(default)]
    pub(super) assets: Vec<DeployAsset>,
    #[serde(default)]
    pub(super) asset_headers: Option<String>,
    pub(super) deployment_id: String,
    pub(super) updated_at_ms: i64,
}

#[derive(Serialize)]
pub(super) struct KvReadCacheConfigPayload {
    pub(super) max_entries: usize,
    pub(super) max_bytes: usize,
    pub(super) hit_ttl_ms: u64,
    pub(super) miss_ttl_ms: u64,
}
