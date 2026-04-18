mod config;
mod dynamic;
mod sessions;

use crate::actor::ActorStore;
use crate::actor_rpc::{
    decode_actor_invoke_request, encode_actor_invoke_response, ActorInvokeCall, ActorInvokeResponse,
};
use crate::blob::{BlobStore, BlobStoreConfig};
use crate::cache::{CacheConfig, CacheLookup, CacheRequest, CacheResponse, CacheStore};
use crate::engine::{
    abort_worker_request, build_bootstrap_snapshot, build_worker_snapshot, dispatch_worker_request,
    ensure_v8_flags, load_worker, new_runtime_from_snapshot, pump_event_loop_once,
    validate_loaded_worker_runtime, validate_worker, ExecuteActorCall, ExecuteHostRpcCall,
};
use crate::kv::KvStore;
use crate::ops::{
    cancel_request_body_stream, clear_request_body_stream, clear_request_secret_context,
    register_actor_request_scope, register_request_body_stream, register_request_secret_context,
    ActorInvokeEvent, IsolateEventPayload, IsolateEventSender, RequestBodyStreams,
};
use crate::static_assets::{
    compile_asset_bundle, resolve_asset, AssetBundle, AssetRequest, AssetResponse,
};
use common::{DeployAsset, DeployConfig, PlatformError, Result, WorkerInvocation, WorkerOutput};
use opentelemetry::global;
use opentelemetry::propagation::Extractor;
use opentelemetry::trace::TraceContextExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet, VecDeque};
use std::mem;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc as std_mpsc;
use std::sync::{Arc, Once};
use std::task::{Wake, Waker};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::runtime::Builder;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::{mpsc, oneshot};
use tracing::{info, warn, Level};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use uuid::Uuid;

use self::config::{build_dynamic_worker_config, extract_bindings, validate_runtime_config};

const INTERNAL_HEADER: &str = "x-dd-internal";
const INTERNAL_REASON_HEADER: &str = "x-dd-internal-reason";
const TRACE_SOURCE_WORKER_HEADER: &str = "x-dd-trace-source-worker";
const TRACE_SOURCE_GENERATION_HEADER: &str = "x-dd-trace-source-generation";
const INTERNAL_WS_ACCEPT_HEADER: &str = "x-dd-ws-accept";
const INTERNAL_WS_SESSION_HEADER: &str = "x-dd-ws-session";
const INTERNAL_WS_HANDLE_HEADER: &str = "x-dd-ws-handle";
const INTERNAL_WS_BINDING_HEADER: &str = "x-dd-ws-memory-binding";
const INTERNAL_WS_KEY_HEADER: &str = "x-dd-ws-memory-key";
const INTERNAL_WS_BINARY_HEADER: &str = "x-dd-ws-binary";
const INTERNAL_WS_CLOSE_CODE_HEADER: &str = "x-dd-ws-close-code";
const INTERNAL_WS_CLOSE_REASON_HEADER: &str = "x-dd-ws-close-reason";
const INTERNAL_TRANSPORT_ACCEPT_HEADER: &str = "x-dd-transport-accept";
const INTERNAL_TRANSPORT_SESSION_HEADER: &str = "x-dd-transport-session";
const INTERNAL_TRANSPORT_HANDLE_HEADER: &str = "x-dd-transport-handle";
const INTERNAL_TRANSPORT_BINDING_HEADER: &str = "x-dd-transport-memory-binding";
const INTERNAL_TRANSPORT_KEY_HEADER: &str = "x-dd-transport-memory-key";
const INTERNAL_TRANSPORT_CLOSE_CODE_HEADER: &str = "x-dd-transport-close-code";
const INTERNAL_TRANSPORT_CLOSE_REASON_HEADER: &str = "x-dd-transport-close-reason";
const CONTENT_TYPE_HEADER: &str = "content-type";
const JSON_CONTENT_TYPE: &str = "application/json";
const ACTOR_ATOMIC_METHOD: &str = "__dd_atomic";

static NEXT_RUNTIME_TOKEN: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug)]
pub struct RuntimeConfig {
    pub min_isolates: usize,
    pub max_isolates: usize,
    pub max_inflight_per_isolate: usize,
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
    pub kv_profile_enabled: bool,
    pub actor_profile_enabled: bool,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            min_isolates: 0,
            max_isolates: 8,
            max_inflight_per_isolate: 4,
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
            kv_profile_enabled: false,
            actor_profile_enabled: false,
        }
    }
}

#[derive(Clone, Debug)]
pub struct RuntimeStorageConfig {
    pub store_dir: PathBuf,
    pub database_url: String,
    pub actor_namespace_shards: usize,
    pub actor_db_cache_max_open: usize,
    pub actor_db_idle_ttl: Duration,
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
            actor_namespace_shards: 16,
            actor_db_cache_max_open: 4096,
            actor_db_idle_ttl: Duration::from_secs(60),
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
    pub queued: usize,
    pub busy: usize,
    pub inflight_total: usize,
    pub wait_until_total: usize,
    pub isolates_total: usize,
    pub spawn_count: u64,
    pub reuse_count: u64,
    pub scale_down_count: u64,
}

#[derive(Debug, Clone, Default)]
pub struct WorkerDebugDump {
    pub generation: u64,
    pub queued: usize,
    pub actor_owners: Vec<(String, u64)>,
    pub actor_inflight: Vec<(String, usize)>,
    pub isolates: Vec<WorkerDebugIsolate>,
    pub queued_requests: Vec<WorkerDebugRequest>,
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
    pub actor_key: Option<String>,
    pub target_isolate_id: Option<u64>,
    pub internal_origin: bool,
    pub reply_kind: String,
    pub host_rpc_target_id: Option<String>,
    pub host_rpc_method: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct DynamicHandleDebug {
    pub handle: String,
    pub owner_worker: String,
    pub owner_generation: u64,
    pub binding: String,
    pub worker_name: String,
    pub timeout_ms: u64,
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
    pub snapshot_cache_entries: usize,
    pub snapshot_cache_failures: usize,
}

#[derive(Debug)]
pub struct WorkerStreamOutput {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: mpsc::UnboundedReceiver<Result<Vec<u8>>>,
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

pub type InvokeRequestBodyReceiver = mpsc::Receiver<std::result::Result<Vec<u8>, String>>;

#[derive(Clone)]
pub struct RuntimeService {
    sender: mpsc::Sender<RuntimeCommand>,
    cancel_sender: mpsc::UnboundedSender<RuntimeCommand>,
    cache_store: CacheStore,
    storage: RuntimeStorageConfig,
}

#[derive(Clone)]
pub(crate) struct RuntimeFastCommandSender(pub mpsc::UnboundedSender<RuntimeCommand>);

pub(crate) enum RuntimeCommand {
    Deploy {
        worker_name: String,
        source: String,
        config: DeployConfig,
        assets: Vec<DeployAsset>,
        asset_headers: Option<String>,
        persist: bool,
        reply: oneshot::Sender<Result<String>>,
    },
    DeployDynamic {
        source: String,
        env: HashMap<String, String>,
        egress_allow_hosts: Vec<String>,
        reply: oneshot::Sender<Result<DynamicDeployResult>>,
    },
    Invoke {
        worker_name: String,
        runtime_request_id: String,
        request: WorkerInvocation,
        request_body: Option<InvokeRequestBodyReceiver>,
        reply: oneshot::Sender<Result<WorkerOutput>>,
    },
    DynamicWorkerFetchStart {
        owner_worker: String,
        owner_generation: u64,
        binding: String,
        handle: String,
        request: WorkerInvocation,
        reply_id: String,
        pending_replies: crate::ops::DynamicPendingReplies,
    },
    RegisterStream {
        worker_name: String,
        runtime_request_id: String,
        ready: oneshot::Sender<Result<WorkerStreamOutput>>,
    },
    Cancel {
        worker_name: String,
        runtime_request_id: String,
    },
    Stats {
        worker_name: String,
        reply: oneshot::Sender<Option<WorkerStats>>,
    },
    ResolveAsset {
        worker_name: String,
        method: String,
        host: Option<String>,
        path: String,
        headers: Vec<(String, String)>,
        reply: oneshot::Sender<Result<Option<AssetResponse>>>,
    },
    DebugDump {
        worker_name: String,
        reply: oneshot::Sender<Option<WorkerDebugDump>>,
    },
    DynamicDebugDump {
        reply: oneshot::Sender<DynamicRuntimeDebugDump>,
    },
    Shutdown {
        reply: oneshot::Sender<()>,
    },
    #[cfg(test)]
    ForceFailIsolate {
        worker_name: String,
        generation: u64,
        isolate_id: u64,
        reply: oneshot::Sender<bool>,
    },
    OpenWebsocket {
        worker_name: String,
        request: WorkerInvocation,
        request_body: Option<InvokeRequestBodyReceiver>,
        session_id: String,
        reply: oneshot::Sender<Result<WebSocketOpen>>,
    },
    SendWebsocketFrame {
        worker_name: String,
        session_id: String,
        frame: Vec<u8>,
        is_binary: bool,
        reply: oneshot::Sender<Result<WorkerOutput>>,
    },
    WaitWebsocketFrame {
        worker_name: String,
        session_id: String,
        reply: oneshot::Sender<Result<()>>,
    },
    DrainWebsocketFrame {
        worker_name: String,
        session_id: String,
        reply: oneshot::Sender<Result<Option<WorkerOutput>>>,
    },
    CloseWebsocket {
        worker_name: String,
        session_id: String,
        close_code: u16,
        close_reason: String,
        reply: oneshot::Sender<Result<()>>,
    },
    OpenTransport {
        worker_name: String,
        request: WorkerInvocation,
        session_id: String,
        stream_sender: mpsc::UnboundedSender<Vec<u8>>,
        datagram_sender: mpsc::UnboundedSender<Vec<u8>>,
        reply: oneshot::Sender<Result<TransportOpen>>,
    },
    PushTransportStream {
        worker_name: String,
        session_id: String,
        chunk: Vec<u8>,
        done: bool,
        reply: oneshot::Sender<Result<()>>,
    },
    PushTransportDatagram {
        worker_name: String,
        session_id: String,
        datagram: Vec<u8>,
        reply: oneshot::Sender<Result<()>>,
    },
    CloseTransport {
        worker_name: String,
        session_id: String,
        close_code: u16,
        close_reason: String,
        reply: oneshot::Sender<Result<()>>,
    },
}

struct WorkerManager {
    config: RuntimeConfig,
    storage: RuntimeStorageConfig,
    bootstrap_snapshot: &'static [u8],
    runtime_fast_sender: mpsc::UnboundedSender<RuntimeCommand>,
    kv_store: KvStore,
    actor_store: ActorStore,
    cache_store: CacheStore,
    workers: HashMap<String, WorkerEntry>,
    pre_canceled: HashMap<String, HashSet<String>>,
    stream_registrations: HashMap<String, StreamRegistration>,
    revalidation_keys: HashSet<String>,
    revalidation_requests: HashMap<String, String>,
    websocket_sessions: HashMap<String, WorkerWebSocketSession>,
    websocket_handle_index: HashMap<String, String>,
    websocket_open_handles: HashMap<String, HashSet<String>>,
    open_handle_registry: crate::ops::ActorOpenHandleRegistry,
    websocket_pending_closes: HashMap<String, HashMap<String, Vec<SocketCloseEvent>>>,
    websocket_outbound_frames: HashMap<String, VecDeque<WebSocketOutboundFrame>>,
    websocket_close_signals: HashMap<String, SocketCloseEvent>,
    websocket_frame_waiters: HashMap<String, Vec<oneshot::Sender<Result<()>>>>,
    websocket_open_waiters: HashMap<String, oneshot::Sender<Result<WebSocketOpen>>>,
    transport_sessions: HashMap<String, WorkerTransportSession>,
    transport_handle_index: HashMap<String, String>,
    transport_open_handles: HashMap<String, HashSet<String>>,
    transport_pending_closes: HashMap<String, HashMap<String, Vec<TransportCloseEvent>>>,
    transport_open_channels: HashMap<String, TransportOpenChannels>,
    transport_open_waiters: HashMap<String, oneshot::Sender<Result<TransportOpen>>>,
    dynamic_worker_handles: HashMap<String, DynamicWorkerHandle>,
    dynamic_worker_ids: HashMap<DynamicWorkerIdKey, HashMap<String, String>>,
    host_rpc_providers: HashMap<String, HostRpcProvider>,
    dynamic_profile: crate::ops::DynamicProfile,
    validated_worker_sources: HashSet<[u8; 32]>,
    dynamic_worker_snapshots: HashMap<[u8; 32], &'static [u8]>,
    dynamic_worker_snapshot_failures: HashSet<[u8; 32]>,
    runtime_batch_depth: usize,
    pending_dispatches: HashSet<(String, u64)>,
    pending_cleanup_workers: HashSet<String>,
    next_generation: u64,
    next_isolate_id: u64,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct DynamicWorkerIdKey {
    owner_worker: String,
    owner_generation: u64,
    binding: String,
}

#[derive(Clone)]
struct DynamicWorkerHandle {
    owner_worker: String,
    owner_generation: u64,
    binding: String,
    worker_name: String,
    worker_generation: u64,
    timeout: u64,
    preferred_isolate_id: Option<u64>,
}

#[derive(Clone)]
struct HostRpcProvider {
    owner_worker: String,
    owner_generation: u64,
    owner_isolate_id: u64,
    target_id: String,
    methods: HashSet<String>,
}

enum TargetedHostRpcReply {
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
struct DynamicTimeoutDiagnostic {
    stage: &'static str,
    owner_worker: String,
    owner_generation: u64,
    binding: String,
    handle: String,
    target_worker: String,
    target_isolate_id: Option<u64>,
    target_generation: Option<u64>,
    provider_id: Option<String>,
    provider_owner_isolate_id: Option<u64>,
    provider_target_id: Option<String>,
    timeout_ms: u64,
}

#[derive(Clone)]
struct WorkerWebSocketSession {
    worker_name: String,
    generation: u64,
    owner_isolate_id: u64,
    binding: String,
    key: String,
    handle: String,
}

#[derive(Clone)]
struct SocketCloseEvent {
    code: u16,
    reason: String,
}

#[derive(Clone)]
struct WebSocketOutboundFrame {
    is_binary: bool,
    payload: Vec<u8>,
}

#[derive(Clone)]
struct WorkerTransportSession {
    worker_name: String,
    generation: u64,
    owner_isolate_id: u64,
    binding: String,
    key: String,
    handle: String,
    stream_sender: mpsc::UnboundedSender<Vec<u8>>,
    datagram_sender: mpsc::UnboundedSender<Vec<u8>>,
    inbound_streams: VecDeque<Vec<u8>>,
    inbound_stream_closed: bool,
    inbound_datagrams: VecDeque<Vec<u8>>,
}

struct TransportOpenChannels {
    stream_sender: mpsc::UnboundedSender<Vec<u8>>,
    datagram_sender: mpsc::UnboundedSender<Vec<u8>>,
}

#[derive(Clone)]
struct TransportCloseEvent {
    code: u16,
    reason: String,
}

struct StreamRegistration {
    worker_name: String,
    completion_token: Option<String>,
    ready: Option<oneshot::Sender<Result<WorkerStreamOutput>>>,
    body_sender: mpsc::UnboundedSender<Result<Vec<u8>>>,
    body_receiver: Option<mpsc::UnboundedReceiver<Result<Vec<u8>>>>,
    started: bool,
}

struct WorkerEntry {
    current_generation: u64,
    pools: HashMap<u64, WorkerPool>,
}

#[derive(Clone, Debug)]
struct InternalTraceDestination {
    worker: String,
    path: String,
}

struct WorkerPool {
    worker_name: String,
    worker_name_json: Arc<str>,
    generation: u64,
    deployment_id: String,
    internal_trace: Option<InternalTraceDestination>,
    is_public: bool,
    snapshot: &'static [u8],
    snapshot_preloaded: bool,
    source: Arc<str>,
    kv_bindings_json: Arc<str>,
    kv_read_cache_config_json: Arc<str>,
    actor_bindings: Vec<String>,
    actor_bindings_json: Arc<str>,
    dynamic_bindings: Vec<String>,
    dynamic_bindings_json: Arc<str>,
    dynamic_rpc_bindings: Vec<DynamicRpcBinding>,
    dynamic_rpc_bindings_json: Arc<str>,
    dynamic_env_json: Arc<str>,
    secret_replacements: Vec<(String, String)>,
    egress_allow_hosts: Vec<String>,
    assets: AssetBundle,
    strict_request_isolation: bool,
    queue: VecDeque<PendingInvoke>,
    isolates: Vec<IsolateHandle>,
    actor_owners: HashMap<String, u64>,
    actor_inflight: HashMap<String, usize>,
    stats: PoolStats,
    queue_warn_level: usize,
}

#[derive(Clone)]
struct DynamicRpcBinding {
    binding: String,
    provider_id: String,
}

#[derive(Default)]
struct PoolStats {
    spawn_count: u64,
    reuse_count: u64,
    scale_down_count: u64,
}

struct PendingInvoke {
    runtime_request_id: String,
    request: WorkerInvocation,
    request_body: Option<InvokeRequestBodyReceiver>,
    actor_route: Option<ActorRoute>,
    actor_call: Option<ActorExecutionCall>,
    host_rpc_call: Option<HostRpcExecutionCall>,
    target_isolate_id: Option<u64>,
    reply_kind: PendingReplyKind,
    internal_origin: bool,
    reply: oneshot::Sender<Result<WorkerOutput>>,
    enqueued_at: Instant,
}

#[derive(Clone)]
enum PendingReplyKind {
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
    fn label(&self) -> &'static str {
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

struct PendingReply {
    completion_token: String,
    canceled: bool,
    actor_key: Option<String>,
    internal_origin: bool,
    reply: oneshot::Sender<Result<WorkerOutput>>,
    completion_meta: Option<PendingReplyMeta>,
    kind: PendingReplyKind,
    dispatched_at: Instant,
}

struct PendingReplyMeta {
    method: String,
    url: String,
    traceparent: Option<String>,
    user_request_id: String,
}

enum DirectDynamicFetchDispatch {
    Dispatched,
    Fallback {
        reply: oneshot::Sender<Result<WorkerOutput>>,
        clear_preferred: bool,
    },
}

#[derive(Debug, Clone)]
struct ActorRoute {
    binding: String,
    key: String,
}

impl ActorRoute {
    fn owner_key(&self) -> String {
        format!("{}\u{001f}{}", self.binding, self.key)
    }
}

struct DispatchCandidate {
    queue_idx: usize,
    isolate_idx: usize,
    actor_key: Option<String>,
    assign_owner: bool,
}

enum DispatchSelection {
    Dispatch(DispatchCandidate),
    DropStaleTarget { queue_idx: usize },
}

struct IsolateHandle {
    id: u64,
    sender: std_mpsc::Sender<IsolateCommand>,
    dynamic_control_inbox: crate::ops::DynamicControlInbox,
    inflight_count: usize,
    active_websocket_sessions: usize,
    active_transport_sessions: usize,
    served_requests: u64,
    last_used_at: Instant,
    pending_replies: HashMap<String, PendingReply>,
    pending_wait_until: HashMap<String, String>,
}

enum IsolateCommand {
    Execute {
        runtime_request_id: String,
        completion_token: String,
        worker_name_json: Arc<str>,
        kv_bindings_json: Arc<str>,
        kv_read_cache_config_json: Arc<str>,
        actor_bindings_json: Arc<str>,
        dynamic_bindings_json: Arc<str>,
        dynamic_rpc_bindings_json: Arc<str>,
        dynamic_env_json: Arc<str>,
        dynamic_bindings: Vec<String>,
        dynamic_rpc_bindings: Vec<String>,
        secret_replacements: Vec<(String, String)>,
        egress_allow_hosts: Vec<String>,
        request: WorkerInvocation,
        request_body: Option<InvokeRequestBodyReceiver>,
        stream_response: bool,
        actor_call: Option<ActorExecutionCall>,
        host_rpc_call: Option<HostRpcExecutionCall>,
        actor_route: Option<ActorRoute>,
    },
    Abort {
        runtime_request_id: String,
    },
    DrainDynamicControl,
    PollEventLoop,
    Shutdown,
}

struct IsolateEventLoopWaker {
    sender: std_mpsc::Sender<IsolateCommand>,
}

impl Wake for IsolateEventLoopWaker {
    fn wake(self: Arc<Self>) {
        let _ = self.sender.send(IsolateCommand::PollEventLoop);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        let _ = self.sender.send(IsolateCommand::PollEventLoop);
    }
}

#[derive(Clone)]
enum ActorExecutionCall {
    Method {
        binding: String,
        key: String,
        name: String,
        args: Vec<u8>,
    },
    Message {
        binding: String,
        key: String,
        handle: String,
        is_text: bool,
        data: Vec<u8>,
        socket_handles: Vec<String>,
        transport_handles: Vec<String>,
    },
    Close {
        binding: String,
        key: String,
        handle: String,
        code: u16,
        reason: String,
        socket_handles: Vec<String>,
        transport_handles: Vec<String>,
    },
    TransportDatagram {
        binding: String,
        key: String,
        handle: String,
        data: Vec<u8>,
        socket_handles: Vec<String>,
        transport_handles: Vec<String>,
    },
    TransportStream {
        binding: String,
        key: String,
        handle: String,
        data: Vec<u8>,
        socket_handles: Vec<String>,
        transport_handles: Vec<String>,
    },
    TransportClose {
        binding: String,
        key: String,
        handle: String,
        code: u16,
        reason: String,
        socket_handles: Vec<String>,
        transport_handles: Vec<String>,
    },
}

#[derive(Clone)]
struct HostRpcExecutionCall {
    target_id: String,
    method: String,
    args: Vec<u8>,
}

#[derive(Clone)]
struct InvokeCancelGuard {
    cancel_sender: mpsc::UnboundedSender<RuntimeCommand>,
    worker_name: String,
    runtime_request_id: String,
    armed: bool,
}

impl InvokeCancelGuard {
    fn new(
        cancel_sender: mpsc::UnboundedSender<RuntimeCommand>,
        worker_name: String,
        runtime_request_id: String,
    ) -> Self {
        Self {
            cancel_sender,
            worker_name,
            runtime_request_id,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for InvokeCancelGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }

        let _ = self.cancel_sender.send(RuntimeCommand::Cancel {
            worker_name: self.worker_name.clone(),
            runtime_request_id: self.runtime_request_id.clone(),
        });
    }
}

enum RuntimeEvent {
    RequestFinished {
        worker_name: String,
        generation: u64,
        isolate_id: u64,
        request_id: String,
        completion_token: String,
        wait_until_count: usize,
        result: Result<WorkerOutput>,
    },
    WaitUntilFinished {
        worker_name: String,
        generation: u64,
        isolate_id: u64,
        request_id: String,
        completion_token: String,
    },
    ResponseStart {
        worker_name: String,
        request_id: String,
        completion_token: String,
        status: u16,
        headers: Vec<(String, String)>,
    },
    ResponseChunk {
        worker_name: String,
        request_id: String,
        completion_token: String,
        chunk: Vec<u8>,
    },
    CacheRevalidate {
        worker_name: String,
        generation: u64,
        payload: String,
    },
    ActorInvoke(ActorInvokeEvent),
    ActorSocketSend(crate::ops::ActorSocketSendEvent),
    ActorSocketClose(crate::ops::ActorSocketCloseEvent),
    ActorSocketConsumeClose {
        worker_name: String,
        generation: u64,
        payload: crate::ops::ActorSocketConsumeCloseEvent,
    },
    ActorTransportSendStream(crate::ops::ActorTransportSendStreamEvent),
    ActorTransportSendDatagram(crate::ops::ActorTransportSendDatagramEvent),
    ActorTransportRecvStream(crate::ops::ActorTransportRecvStreamEvent),
    ActorTransportRecvDatagram(crate::ops::ActorTransportRecvDatagramEvent),
    ActorTransportClose(crate::ops::ActorTransportCloseEvent),
    ActorTransportConsumeClose {
        worker_name: String,
        generation: u64,
        payload: crate::ops::ActorTransportConsumeCloseEvent,
    },
    DynamicWorkerCreate(crate::ops::DynamicWorkerCreateEvent),
    DynamicWorkerLookup(crate::ops::DynamicWorkerLookupEvent),
    DynamicWorkerList(crate::ops::DynamicWorkerListEvent),
    DynamicWorkerDelete(crate::ops::DynamicWorkerDeleteEvent),
    DynamicWorkerInvoke(crate::ops::DynamicWorkerInvokeEvent),
    DynamicHostRpcInvoke(crate::ops::DynamicHostRpcInvokeEvent),
    DynamicReplyReady(crate::ops::DynamicPendingReplyDelivery),
    DynamicFetchReplyReady(crate::ops::DynamicPendingReplyDelivery),
    TestAsyncReply(crate::ops::TestAsyncReplyEvent),
    TestNestedTargetedInvoke(crate::ops::TestNestedTargetedInvokeEvent),
    TestAsyncReplyComplete {
        reply_id: String,
        replies: crate::ops::TestAsyncReplies,
        result: Result<String>,
    },
    DynamicTimeoutDiagnostic(DynamicTimeoutDiagnostic),
    IsolateFailed {
        worker_name: String,
        generation: u64,
        isolate_id: u64,
        error: PlatformError,
    },
}

#[derive(Deserialize)]
struct CompletionPayload {
    request_id: String,
    completion_token: String,
    #[serde(default)]
    wait_until_count: usize,
    ok: bool,
    result: Option<WorkerOutput>,
    error: Option<String>,
}

#[derive(Serialize)]
struct TraceEventPayload {
    ts_ms: i64,
    worker: String,
    generation: u64,
    request_id: String,
    runtime_request_id: String,
    method: String,
    url: String,
    status: Option<u16>,
    ok: bool,
    error: Option<String>,
    execution_ms: u64,
    wait_until_count: usize,
}

#[derive(Deserialize)]
struct WaitUntilPayload {
    request_id: String,
    completion_token: String,
}

#[derive(Deserialize)]
struct ResponseStartPayload {
    request_id: String,
    completion_token: String,
    status: u16,
    headers: Vec<(String, String)>,
}

#[derive(Deserialize)]
struct ResponseChunkPayload {
    request_id: String,
    completion_token: String,
    chunk: Vec<u8>,
}

#[derive(Deserialize)]
struct CacheRevalidatePayload {
    cache_name: String,
    method: String,
    url: String,
    #[serde(default)]
    headers: Vec<(String, String)>,
}

#[derive(Serialize, Deserialize)]
struct StoredWorkerDeployment {
    name: String,
    source: String,
    config: DeployConfig,
    #[serde(default)]
    assets: Vec<DeployAsset>,
    #[serde(default)]
    asset_headers: Option<String>,
    deployment_id: String,
    updated_at_ms: i64,
}

#[derive(Serialize)]
struct KvReadCacheConfigPayload {
    max_entries: usize,
    max_bytes: usize,
    hit_ttl_ms: u64,
    miss_ttl_ms: u64,
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
        if storage.actor_db_cache_max_open == 0 {
            return Err(PlatformError::internal(
                "actor_db_cache_max_open must be greater than 0",
            ));
        }
        if storage.actor_namespace_shards == 0 {
            return Err(PlatformError::internal(
                "actor_namespace_shards must be greater than 0",
            ));
        }
        if storage.actor_db_idle_ttl.is_zero() {
            return Err(PlatformError::internal(
                "actor_db_idle_ttl must be greater than 0",
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
        let actor_store = ActorStore::new(
            storage.store_dir.join("actors"),
            storage.actor_namespace_shards,
            storage.actor_db_cache_max_open,
            storage.actor_db_idle_ttl,
        )
        .await?;
        actor_store.set_profile_enabled(runtime.actor_profile_enabled);
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
        let (cancel_sender, cancel_receiver) = mpsc::unbounded_channel();
        spawn_runtime_thread(
            receiver,
            cancel_receiver,
            cancel_sender.clone(),
            bootstrap_snapshot,
            kv_store,
            actor_store,
            cache_store.clone(),
            runtime,
            storage.clone(),
        )?;
        let service = Self {
            sender,
            cancel_sender,
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

        let mut restored = 0usize;
        for (_worker_name, (stored, path)) in latest_by_worker {
            match self
                .deploy_with_config_internal(
                    stored.name.clone(),
                    stored.source,
                    stored.config,
                    stored.assets,
                    stored.asset_headers,
                    false,
                )
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
        self.deploy_with_config_internal(worker_name, source, config, assets, asset_headers, true)
            .await
    }

    async fn deploy_with_config_internal(
        &self,
        worker_name: String,
        source: String,
        config: DeployConfig,
        assets: Vec<DeployAsset>,
        asset_headers: Option<String>,
        persist: bool,
    ) -> Result<String> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.sender
            .send(RuntimeCommand::Deploy {
                worker_name,
                source,
                config,
                assets,
                asset_headers,
                persist,
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
            set_span_parent_from_traceparent(
                &span,
                traceparent_from_headers(&request.headers).as_deref(),
            );
            Some(span)
        } else {
            None
        };
        let _invoke_guard = invoke_span.as_ref().map(|span| span.enter());
        let mut cancel_guard = InvokeCancelGuard::new(
            self.cancel_sender.clone(),
            worker_name.clone(),
            runtime_request_id.clone(),
        );
        let (reply_tx, reply_rx) = oneshot::channel();
        self.sender
            .send(RuntimeCommand::Invoke {
                worker_name,
                runtime_request_id,
                request,
                request_body,
                reply: reply_tx,
            })
            .await
            .map_err(|_| PlatformError::internal("runtime thread is not available"))?;

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
            set_span_parent_from_traceparent(
                &span,
                traceparent_from_headers(&request.headers).as_deref(),
            );
            Some(span)
        } else {
            None
        };
        let _stream_guard = stream_span.as_ref().map(|span| span.enter());
        let mut cancel_guard = InvokeCancelGuard::new(
            self.cancel_sender.clone(),
            worker_name.clone(),
            runtime_request_id.clone(),
        );
        let (ready_tx, ready_rx) = oneshot::channel();
        self.sender
            .send(RuntimeCommand::RegisterStream {
                worker_name: worker_name.clone(),
                runtime_request_id: runtime_request_id.clone(),
                ready: ready_tx,
            })
            .await
            .map_err(|_| PlatformError::internal("runtime thread is not available"))?;

        let (reply_tx, reply_rx) = oneshot::channel();
        self.sender
            .send(RuntimeCommand::Invoke {
                worker_name,
                runtime_request_id,
                request,
                request_body,
                reply: reply_tx,
            })
            .await
            .map_err(|_| PlatformError::internal("runtime thread is not available"))?;
        tokio::spawn(async move {
            let _ = reply_rx.await;
        });

        let ready = ready_rx
            .await
            .map_err(|_| PlatformError::internal("runtime stream channel closed"))?;
        cancel_guard.disarm();
        ready
    }

    pub async fn open_websocket(
        &self,
        worker_name: String,
        request: WorkerInvocation,
        request_body: Option<InvokeRequestBodyReceiver>,
    ) -> Result<WebSocketOpen> {
        let session_id = Uuid::new_v4().to_string();
        let (reply_tx, reply_rx) = oneshot::channel();
        self.sender
            .send(RuntimeCommand::OpenWebsocket {
                worker_name,
                request,
                request_body,
                session_id: session_id.clone(),
                reply: reply_tx,
            })
            .await
            .map_err(|_| PlatformError::internal("runtime thread is not available"))?;

        let mut opened = reply_rx
            .await
            .map_err(|_| PlatformError::internal("runtime open websocket channel closed"))??;
        opened.session_id = session_id;
        Ok(opened)
    }

    pub async fn open_transport(
        &self,
        worker_name: String,
        request: WorkerInvocation,
        stream_sender: mpsc::UnboundedSender<Vec<u8>>,
        datagram_sender: mpsc::UnboundedSender<Vec<u8>>,
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

    pub async fn resolve_asset(
        &self,
        worker_name: String,
        method: String,
        host: Option<String>,
        path: String,
        headers: Vec<(String, String)>,
    ) -> Result<Option<AssetResponse>> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.sender
            .send(RuntimeCommand::ResolveAsset {
                worker_name,
                method,
                host,
                path,
                headers,
                reply: reply_tx,
            })
            .await
            .map_err(|_| PlatformError::internal("runtime thread is not available"))?;

        reply_rx
            .await
            .map_err(|_| PlatformError::internal("runtime asset channel closed"))?
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

impl WorkerManager {
    fn new(
        bootstrap_snapshot: &'static [u8],
        kv_store: KvStore,
        actor_store: ActorStore,
        cache_store: CacheStore,
        config: RuntimeConfig,
        storage: RuntimeStorageConfig,
        runtime_fast_sender: mpsc::UnboundedSender<RuntimeCommand>,
    ) -> Self {
        Self {
            config,
            storage,
            bootstrap_snapshot,
            runtime_fast_sender,
            kv_store,
            actor_store,
            cache_store,
            workers: HashMap::new(),
            pre_canceled: HashMap::new(),
            stream_registrations: HashMap::new(),
            revalidation_keys: HashSet::new(),
            revalidation_requests: HashMap::new(),
            websocket_sessions: HashMap::new(),
            websocket_handle_index: HashMap::new(),
            websocket_open_handles: HashMap::new(),
            open_handle_registry: crate::ops::ActorOpenHandleRegistry::default(),
            websocket_pending_closes: HashMap::new(),
            websocket_outbound_frames: HashMap::new(),
            websocket_close_signals: HashMap::new(),
            websocket_frame_waiters: HashMap::new(),
            websocket_open_waiters: HashMap::new(),
            transport_sessions: HashMap::new(),
            transport_handle_index: HashMap::new(),
            transport_open_handles: HashMap::new(),
            transport_pending_closes: HashMap::new(),
            transport_open_channels: HashMap::new(),
            transport_open_waiters: HashMap::new(),
            dynamic_worker_handles: HashMap::new(),
            dynamic_worker_ids: HashMap::new(),
            host_rpc_providers: HashMap::new(),
            dynamic_profile: crate::ops::DynamicProfile::default(),
            validated_worker_sources: HashSet::new(),
            dynamic_worker_snapshots: HashMap::new(),
            dynamic_worker_snapshot_failures: HashSet::new(),
            runtime_batch_depth: 0,
            pending_dispatches: HashSet::new(),
            pending_cleanup_workers: HashSet::new(),
            next_generation: 1,
            next_isolate_id: 1,
        }
    }

    fn begin_runtime_batch(&mut self) {
        self.runtime_batch_depth += 1;
    }

    fn finish_runtime_batch(&mut self, event_tx: &mpsc::UnboundedSender<RuntimeEvent>) {
        debug_assert!(self.runtime_batch_depth > 0);
        if self.runtime_batch_depth == 0 {
            return;
        }
        self.runtime_batch_depth -= 1;
        if self.runtime_batch_depth > 0 {
            return;
        }

        loop {
            let pending_dispatches = mem::take(&mut self.pending_dispatches);
            let pending_cleanup_workers = mem::take(&mut self.pending_cleanup_workers);
            if pending_dispatches.is_empty() && pending_cleanup_workers.is_empty() {
                break;
            }

            for (worker_name, generation) in pending_dispatches {
                self.dispatch_pool(&worker_name, generation, event_tx);
            }
            for worker_name in pending_cleanup_workers {
                self.cleanup_drained_generations_for(&worker_name);
            }
        }
    }

    async fn handle_command(
        &mut self,
        command: RuntimeCommand,
        event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    ) -> bool {
        match command {
            RuntimeCommand::Deploy {
                worker_name,
                source,
                config,
                assets,
                asset_headers,
                persist,
                reply,
            } => {
                let result = self
                    .deploy(worker_name, source, config, assets, asset_headers, persist)
                    .await;
                let _ = reply.send(result);
                true
            }
            RuntimeCommand::DeployDynamic {
                source,
                env,
                egress_allow_hosts,
                reply,
            } => {
                let result = self
                    .deploy_dynamic(source, env, egress_allow_hosts, Vec::new())
                    .await;
                let _ = reply.send(result);
                true
            }
            RuntimeCommand::Invoke {
                worker_name,
                runtime_request_id,
                request,
                request_body,
                reply,
            } => {
                let _ = self.enqueue_invoke(
                    worker_name,
                    runtime_request_id,
                    request,
                    request_body,
                    None,
                    None,
                    None,
                    None,
                    None,
                    false,
                    reply,
                    PendingReplyKind::Normal,
                    event_tx,
                );
                true
            }
            RuntimeCommand::DynamicWorkerFetchStart {
                owner_worker,
                owner_generation,
                binding,
                handle,
                request,
                reply_id,
                pending_replies,
            } => {
                self.start_dynamic_worker_fetch(
                    owner_worker,
                    owner_generation,
                    binding,
                    handle,
                    request,
                    reply_id,
                    pending_replies,
                    event_tx,
                );
                true
            }
            RuntimeCommand::OpenWebsocket {
                worker_name,
                mut request,
                request_body,
                session_id,
                reply,
            } => {
                if !self.workers.contains_key(worker_name.trim()) {
                    let _ = reply.send(Err(PlatformError::not_found("Worker not found")));
                    return true;
                }
                let (inner_tx, _inner_rx) = oneshot::channel();
                append_or_update_header(
                    &mut request.headers,
                    INTERNAL_WS_SESSION_HEADER,
                    &session_id,
                );
                self.websocket_open_waiters
                    .insert(session_id.clone(), reply);

                let runtime_request_id = Uuid::new_v4().to_string();
                let _ = self.enqueue_invoke(
                    worker_name,
                    runtime_request_id,
                    request,
                    request_body,
                    None,
                    None,
                    None,
                    None,
                    None,
                    false,
                    inner_tx,
                    PendingReplyKind::WebsocketOpen { session_id },
                    event_tx,
                );
                true
            }
            RuntimeCommand::SendWebsocketFrame {
                worker_name,
                session_id,
                frame,
                is_binary,
                reply,
            } => {
                self.enqueue_websocket_frame(
                    &worker_name,
                    &session_id,
                    frame,
                    is_binary,
                    reply,
                    event_tx,
                );
                true
            }
            RuntimeCommand::WaitWebsocketFrame {
                worker_name,
                session_id,
                reply,
            } => {
                self.wait_websocket_frame(&worker_name, &session_id, reply);
                true
            }
            RuntimeCommand::DrainWebsocketFrame {
                worker_name,
                session_id,
                reply,
            } => {
                let result = self.drain_websocket_frame(&worker_name, &session_id);
                let _ = reply.send(result);
                true
            }
            RuntimeCommand::CloseWebsocket {
                worker_name,
                session_id,
                close_code,
                close_reason,
                reply,
            } => {
                let result = self.close_websocket(
                    &worker_name,
                    &session_id,
                    close_code,
                    close_reason,
                    event_tx,
                );
                let _ = reply.send(result);
                true
            }
            RuntimeCommand::OpenTransport {
                worker_name,
                mut request,
                session_id,
                stream_sender,
                datagram_sender,
                reply,
            } => {
                if !self.workers.contains_key(worker_name.trim()) {
                    let _ = reply.send(Err(PlatformError::not_found("Worker not found")));
                    return true;
                }
                let (inner_tx, _inner_rx) = oneshot::channel();
                append_or_update_header(
                    &mut request.headers,
                    INTERNAL_TRANSPORT_SESSION_HEADER,
                    &session_id,
                );
                self.transport_open_waiters
                    .insert(session_id.clone(), reply);
                self.transport_open_channels.insert(
                    session_id.clone(),
                    TransportOpenChannels {
                        stream_sender,
                        datagram_sender,
                    },
                );

                let runtime_request_id = Uuid::new_v4().to_string();
                let _ = self.enqueue_invoke(
                    worker_name,
                    runtime_request_id,
                    request,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    false,
                    inner_tx,
                    PendingReplyKind::TransportOpen { session_id },
                    event_tx,
                );
                true
            }
            RuntimeCommand::PushTransportStream {
                worker_name,
                session_id,
                chunk,
                done,
                reply,
            } => {
                let result =
                    self.push_transport_stream(&worker_name, &session_id, chunk, done, event_tx);
                let _ = reply.send(result);
                true
            }
            RuntimeCommand::PushTransportDatagram {
                worker_name,
                session_id,
                datagram,
                reply,
            } => {
                let result =
                    self.push_transport_datagram(&worker_name, &session_id, datagram, event_tx);
                let _ = reply.send(result);
                true
            }
            RuntimeCommand::CloseTransport {
                worker_name,
                session_id,
                close_code,
                close_reason,
                reply,
            } => {
                let result = self.close_transport(
                    &worker_name,
                    &session_id,
                    close_code,
                    close_reason,
                    event_tx,
                );
                let _ = reply.send(result);
                true
            }
            RuntimeCommand::RegisterStream {
                worker_name,
                runtime_request_id,
                ready,
            } => {
                self.register_stream(worker_name, runtime_request_id, ready);
                true
            }
            RuntimeCommand::Cancel {
                worker_name,
                runtime_request_id,
            } => {
                self.cancel_invoke(worker_name, runtime_request_id, event_tx);
                true
            }
            RuntimeCommand::Stats { worker_name, reply } => {
                let _ = reply.send(self.worker_stats(&worker_name));
                true
            }
            RuntimeCommand::ResolveAsset {
                worker_name,
                method,
                host,
                path,
                headers,
                reply,
            } => {
                let _ = reply.send(self.resolve_asset(
                    &worker_name,
                    &method,
                    host.as_deref(),
                    &path,
                    &headers,
                ));
                true
            }
            RuntimeCommand::DebugDump { worker_name, reply } => {
                let _ = reply.send(self.worker_debug_dump(&worker_name));
                true
            }
            RuntimeCommand::DynamicDebugDump { reply } => {
                let _ = reply.send(self.dynamic_debug_dump());
                true
            }
            RuntimeCommand::Shutdown { reply } => {
                self.shutdown_all();
                let _ = reply.send(());
                false
            }
            #[cfg(test)]
            RuntimeCommand::ForceFailIsolate {
                worker_name,
                generation,
                isolate_id,
                reply,
            } => {
                let exists = self
                    .workers
                    .get(&worker_name)
                    .and_then(|entry| entry.pools.get(&generation))
                    .map(|pool| pool.isolates.iter().any(|isolate| isolate.id == isolate_id))
                    .unwrap_or(false);
                if exists {
                    self.fail_isolate(
                        &worker_name,
                        generation,
                        isolate_id,
                        PlatformError::internal("isolate removed for test"),
                    );
                }
                let _ = reply.send(exists);
                true
            }
        }
    }

    async fn handle_event(
        &mut self,
        event: RuntimeEvent,
        event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    ) {
        match event {
            RuntimeEvent::RequestFinished {
                worker_name,
                generation,
                isolate_id,
                request_id,
                completion_token,
                wait_until_count,
                result,
            } => {
                self.finish_request(
                    &worker_name,
                    generation,
                    isolate_id,
                    &request_id,
                    &completion_token,
                    wait_until_count,
                    result,
                    event_tx,
                );
                self.dispatch_pool(&worker_name, generation, event_tx);
                self.cleanup_drained_generations_for(&worker_name);
            }
            RuntimeEvent::WaitUntilFinished {
                worker_name,
                generation,
                isolate_id,
                request_id,
                completion_token,
            } => {
                self.finish_wait_until(
                    &worker_name,
                    generation,
                    isolate_id,
                    &request_id,
                    &completion_token,
                );
                self.cleanup_drained_generations_for(&worker_name);
            }
            RuntimeEvent::ResponseStart {
                worker_name,
                request_id,
                completion_token,
                status,
                headers,
            } => {
                self.handle_response_start(
                    &worker_name,
                    &request_id,
                    &completion_token,
                    status,
                    headers,
                );
            }
            RuntimeEvent::ResponseChunk {
                worker_name,
                request_id,
                completion_token,
                chunk,
            } => {
                self.handle_response_chunk(&worker_name, &request_id, &completion_token, chunk);
            }
            RuntimeEvent::CacheRevalidate {
                worker_name,
                generation,
                payload,
            } => {
                self.schedule_cache_revalidate(&worker_name, generation, payload, event_tx);
            }
            RuntimeEvent::ActorInvoke(payload) => {
                self.enqueue_actor_invoke(payload, event_tx);
            }
            RuntimeEvent::ActorSocketSend(payload) => {
                self.handle_actor_socket_send(payload, event_tx);
            }
            RuntimeEvent::ActorSocketClose(payload) => {
                self.handle_actor_socket_close(payload, event_tx);
            }
            RuntimeEvent::ActorSocketConsumeClose {
                worker_name: _worker_name,
                generation: _generation,
                payload,
            } => {
                self.handle_actor_socket_consume_close(payload, event_tx);
            }
            RuntimeEvent::ActorTransportSendStream(payload) => {
                self.handle_actor_transport_send_stream(payload, event_tx);
            }
            RuntimeEvent::ActorTransportSendDatagram(payload) => {
                self.handle_actor_transport_send_datagram(payload, event_tx);
            }
            RuntimeEvent::ActorTransportRecvStream(payload) => {
                self.handle_actor_transport_recv_stream(payload, event_tx);
            }
            RuntimeEvent::ActorTransportRecvDatagram(payload) => {
                self.handle_actor_transport_recv_datagram(payload, event_tx);
            }
            RuntimeEvent::ActorTransportClose(payload) => {
                self.handle_actor_transport_close(payload, event_tx);
            }
            RuntimeEvent::ActorTransportConsumeClose {
                worker_name: _worker_name,
                generation: _generation,
                payload,
            } => {
                self.handle_actor_transport_consume_close(payload, event_tx);
            }
            RuntimeEvent::DynamicWorkerCreate(payload) => {
                self.handle_dynamic_worker_create(payload).await;
            }
            RuntimeEvent::DynamicWorkerLookup(payload) => {
                self.handle_dynamic_worker_lookup(payload);
            }
            RuntimeEvent::DynamicWorkerList(payload) => {
                self.handle_dynamic_worker_list(payload);
            }
            RuntimeEvent::DynamicWorkerDelete(payload) => {
                self.handle_dynamic_worker_delete(payload);
            }
            RuntimeEvent::DynamicWorkerInvoke(payload) => {
                self.handle_dynamic_worker_invoke(payload, event_tx);
            }
            RuntimeEvent::DynamicHostRpcInvoke(payload) => {
                self.handle_dynamic_host_rpc_invoke(payload, event_tx);
            }
            RuntimeEvent::DynamicReplyReady(delivery) => {
                self.enqueue_isolate_reply(
                    &delivery.owner.worker_name,
                    delivery.owner.generation,
                    delivery.owner.isolate_id,
                    delivery.payload,
                );
            }
            RuntimeEvent::DynamicFetchReplyReady(delivery) => {
                self.enqueue_isolate_reply(
                    &delivery.owner.worker_name,
                    delivery.owner.generation,
                    delivery.owner.isolate_id,
                    delivery.payload,
                );
            }
            RuntimeEvent::TestAsyncReply(payload) => {
                self.handle_test_async_reply(payload, event_tx);
            }
            RuntimeEvent::TestNestedTargetedInvoke(payload) => {
                self.handle_test_nested_targeted_invoke(payload, event_tx);
            }
            RuntimeEvent::TestAsyncReplyComplete {
                reply_id,
                replies,
                result,
            } => {
                self.complete_test_async_reply(reply_id, replies, result);
            }
            RuntimeEvent::DynamicTimeoutDiagnostic(payload) => {
                self.log_dynamic_timeout_diagnostic(payload);
            }
            RuntimeEvent::IsolateFailed {
                worker_name,
                generation,
                isolate_id,
                error,
            } => {
                self.fail_isolate(&worker_name, generation, isolate_id, error);
                self.dispatch_pool(&worker_name, generation, event_tx);
                self.cleanup_drained_generations_for(&worker_name);
            }
        }
    }

    fn enqueue_websocket_frame(
        &mut self,
        worker_name: &str,
        session_id: &str,
        frame: Vec<u8>,
        is_binary: bool,
        reply: oneshot::Sender<Result<WorkerOutput>>,
        event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    ) {
        let Some(session) = self.websocket_sessions.get(session_id).cloned() else {
            let _ = reply.send(Err(PlatformError::not_found("websocket session not found")));
            return;
        };
        if session.worker_name != worker_name {
            let _ = reply.send(Err(PlatformError::bad_request(
                "websocket session worker mismatch",
            )));
            return;
        }

        let runtime_request_id = Uuid::new_v4().to_string();
        let route = ActorRoute {
            binding: session.binding.clone(),
            key: session.key.clone(),
        };
        let socket_handles =
            self.websocket_handles_snapshot(&session.binding, &session.key, Some(&session.handle));
        let transport_handles =
            self.transport_handles_snapshot(&session.binding, &session.key, None);
        let actor_call = ActorExecutionCall::Message {
            binding: session.binding.clone(),
            key: session.key.clone(),
            handle: session.handle.clone(),
            is_text: !is_binary,
            data: frame,
            socket_handles,
            transport_handles,
        };
        let invoke = WorkerInvocation {
            method: "WS-MESSAGE".to_string(),
            url: format!("http://actor/__dd_socket/{session_id}"),
            headers: Vec::new(),
            body: Vec::new(),
            request_id: format!("ws-message-{runtime_request_id}"),
        };
        self.enqueue_invoke(
            session.worker_name,
            runtime_request_id,
            invoke,
            None,
            Some(route),
            Some(actor_call),
            None,
            None,
            Some(session.generation),
            true,
            reply,
            PendingReplyKind::WebsocketFrame {
                session_id: session_id.to_string(),
            },
            event_tx,
        );
    }

    fn close_websocket(
        &mut self,
        worker_name: &str,
        session_id: &str,
        close_code: u16,
        close_reason: String,
        event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    ) -> Result<()> {
        let Some(existing) = self.websocket_sessions.get(session_id) else {
            return Err(PlatformError::not_found("websocket session not found"));
        };
        if existing.worker_name != worker_name {
            return Err(PlatformError::bad_request(
                "websocket session worker mismatch",
            ));
        }

        let session = self
            .unregister_websocket_session(session_id)
            .ok_or_else(|| PlatformError::not_found("websocket session not found"))?;
        self.queue_websocket_close_replay(&session, close_code, close_reason.clone());

        let runtime_request_id = Uuid::new_v4().to_string();
        let route = ActorRoute {
            binding: session.binding.clone(),
            key: session.key.clone(),
        };
        let socket_handles =
            self.websocket_handles_snapshot(&session.binding, &session.key, Some(&session.handle));
        let transport_handles =
            self.transport_handles_snapshot(&session.binding, &session.key, None);
        let actor_call = ActorExecutionCall::Close {
            binding: session.binding.clone(),
            key: session.key.clone(),
            handle: session.handle.clone(),
            code: close_code,
            reason: close_reason,
            socket_handles,
            transport_handles,
        };
        let invoke = WorkerInvocation {
            method: "WS-CLOSE".to_string(),
            url: format!("http://actor/__dd_socket_close/{session_id}"),
            headers: Vec::new(),
            body: Vec::new(),
            request_id: format!("ws-close-{runtime_request_id}"),
        };
        let (reply, receiver) = oneshot::channel();
        self.enqueue_invoke(
            session.worker_name,
            runtime_request_id,
            invoke,
            None,
            Some(route),
            Some(actor_call),
            None,
            None,
            Some(session.generation),
            true,
            reply,
            PendingReplyKind::Normal,
            event_tx,
        );
        let session_id = session_id.to_string();
        tokio::spawn(async move {
            match receiver.await {
                Ok(Err(error)) => {
                    warn!(session_id, error = %error, "websocket close wake dispatch failed");
                }
                Ok(Ok(_)) | Err(_) => {}
            }
        });
        Ok(())
    }

    fn wait_websocket_frame(
        &mut self,
        worker_name: &str,
        session_id: &str,
        reply: oneshot::Sender<Result<()>>,
    ) {
        let Some(session) = self.websocket_sessions.get(session_id) else {
            let _ = reply.send(Err(PlatformError::not_found("websocket session not found")));
            return;
        };
        if session.worker_name != worker_name {
            let _ = reply.send(Err(PlatformError::bad_request(
                "websocket session worker mismatch",
            )));
            return;
        }
        let has_frame = self
            .websocket_outbound_frames
            .get(session_id)
            .map(|queue| !queue.is_empty())
            .unwrap_or(false);
        let has_close = self.websocket_close_signals.contains_key(session_id);
        if has_frame || has_close {
            let _ = reply.send(Ok(()));
            return;
        }
        self.websocket_frame_waiters
            .entry(session_id.to_string())
            .or_default()
            .push(reply);
    }

    fn notify_websocket_frame_waiters(&mut self, session_id: &str) {
        if let Some(waiters) = self.websocket_frame_waiters.remove(session_id) {
            for waiter in waiters {
                let _ = waiter.send(Ok(()));
            }
        }
    }

    fn fail_websocket_frame_waiters(&mut self, session_id: &str, error: PlatformError) {
        if let Some(waiters) = self.websocket_frame_waiters.remove(session_id) {
            for waiter in waiters {
                let _ = waiter.send(Err(error.clone()));
            }
        }
    }

    fn drain_websocket_frame(
        &mut self,
        worker_name: &str,
        session_id: &str,
    ) -> Result<Option<WorkerOutput>> {
        let Some(session) = self.websocket_sessions.get(session_id) else {
            return Err(PlatformError::not_found("websocket session not found"));
        };
        if session.worker_name != worker_name {
            return Err(PlatformError::bad_request(
                "websocket session worker mismatch",
            ));
        }

        let mut output = WorkerOutput {
            status: 204,
            headers: Vec::new(),
            body: Vec::new(),
        };
        let mut has_output = false;

        if let Some(frame) = self
            .websocket_outbound_frames
            .get_mut(session_id)
            .and_then(|queue| queue.pop_front())
        {
            has_output = true;
            output.body = frame.payload;
            if frame.is_binary {
                append_or_update_header(&mut output.headers, INTERNAL_WS_BINARY_HEADER, "1");
            }
        }

        if let Some(close) = self.websocket_close_signals.remove(session_id) {
            has_output = true;
            append_or_update_header(
                &mut output.headers,
                INTERNAL_WS_CLOSE_CODE_HEADER,
                close.code.to_string().as_str(),
            );
            append_or_update_header(
                &mut output.headers,
                INTERNAL_WS_CLOSE_REASON_HEADER,
                &close.reason,
            );
        }

        if has_output {
            Ok(Some(output))
        } else {
            Ok(None)
        }
    }

    fn complete_websocket_frame(
        &mut self,
        session_id: String,
        reply: Option<oneshot::Sender<Result<WorkerOutput>>>,
        result: Result<WorkerOutput>,
    ) {
        let Some(reply) = reply else {
            return;
        };
        match result {
            Ok(mut output) => {
                output.headers = strip_websocket_frame_internal_headers(&output.headers);
                if let Some(frame) = self
                    .websocket_outbound_frames
                    .get_mut(&session_id)
                    .and_then(|queue| queue.pop_front())
                {
                    output.body = frame.payload;
                    if frame.is_binary {
                        append_or_update_header(
                            &mut output.headers,
                            INTERNAL_WS_BINARY_HEADER,
                            "1",
                        );
                    } else {
                        output.headers.retain(|(name, _)| {
                            !name.eq_ignore_ascii_case(INTERNAL_WS_BINARY_HEADER)
                        });
                    }
                }
                if let Some(close) = self.websocket_close_signals.remove(&session_id) {
                    append_or_update_header(
                        &mut output.headers,
                        INTERNAL_WS_CLOSE_CODE_HEADER,
                        close.code.to_string().as_str(),
                    );
                    append_or_update_header(
                        &mut output.headers,
                        INTERNAL_WS_CLOSE_REASON_HEADER,
                        &close.reason,
                    );
                }
                let _ = reply.send(Ok(output));
            }
            Err(error) => {
                let _ = reply.send(Err(error));
            }
        }
    }

    async fn deploy(
        &mut self,
        worker_name: String,
        source: String,
        config: DeployConfig,
        assets: Vec<DeployAsset>,
        asset_headers: Option<String>,
        persist: bool,
    ) -> Result<String> {
        let worker_name = worker_name.trim().to_string();
        if worker_name.is_empty() {
            return Err(PlatformError::bad_request("Worker name must not be empty"));
        }
        let bindings = extract_bindings(&config)?;
        let compiled_assets = compile_asset_bundle(&assets, asset_headers.as_deref())?;
        self.validate_worker_cached(&source).await?;
        let has_dynamic_bindings = !bindings.dynamic.is_empty();
        let (snapshot, snapshot_preloaded) = if has_dynamic_bindings {
            (self.bootstrap_snapshot, false)
        } else {
            let worker_snapshot = build_worker_snapshot(self.bootstrap_snapshot, &source).await?;
            validate_loaded_worker_runtime(worker_snapshot)?;
            (worker_snapshot, true)
        };
        let generation = self.next_generation;
        self.next_generation += 1;
        let deployment_id = Uuid::new_v4().to_string();
        let worker_name_json = Arc::<str>::from(
            crate::json::to_string(&worker_name)
                .map_err(|error| PlatformError::internal(error.to_string()))?,
        );
        let kv_bindings_json = Arc::<str>::from(
            crate::json::to_string(&bindings.kv)
                .map_err(|error| PlatformError::internal(error.to_string()))?,
        );
        let actor_bindings_json = Arc::<str>::from(
            crate::json::to_string(&bindings.actor)
                .map_err(|error| PlatformError::internal(error.to_string()))?,
        );
        let dynamic_bindings_json = Arc::<str>::from(
            crate::json::to_string(&bindings.dynamic)
                .map_err(|error| PlatformError::internal(error.to_string()))?,
        );
        let dynamic_rpc_bindings_json = Arc::<str>::from(
            crate::json::to_string(&Vec::<String>::new())
                .map_err(|error| PlatformError::internal(error.to_string()))?,
        );
        let dynamic_env_json = Arc::<str>::from(
            crate::json::to_string(&Vec::<(String, String)>::new())
                .map_err(|error| PlatformError::internal(error.to_string()))?,
        );
        let kv_read_cache_config_json = Arc::<str>::from(
            crate::json::to_string(&KvReadCacheConfigPayload {
                max_entries: self.config.kv_read_cache_max_entries,
                max_bytes: self.config.kv_read_cache_max_bytes,
                hit_ttl_ms: self.config.kv_read_cache_hit_ttl.as_millis() as u64,
                miss_ttl_ms: self.config.kv_read_cache_miss_ttl.as_millis() as u64,
            })
            .map_err(|error| PlatformError::internal(error.to_string()))?,
        );
        if persist {
            persist_worker_deployment(
                &self.storage,
                &worker_name,
                &source,
                &config,
                &assets,
                asset_headers.as_deref(),
                &deployment_id,
            )
            .await?;
        }
        let pool =
            WorkerPool {
                worker_name: worker_name.clone(),
                worker_name_json,
                generation,
                deployment_id: deployment_id.clone(),
                internal_trace: config.internal.trace.as_ref().map(|trace| {
                    InternalTraceDestination {
                        worker: trace.worker.trim().to_string(),
                        path: normalize_trace_path(&trace.path),
                    }
                }),
                is_public: config.public,
                snapshot,
                snapshot_preloaded,
                source: Arc::<str>::from(source.clone()),
                kv_bindings_json,
                kv_read_cache_config_json,
                actor_bindings: bindings.actor,
                actor_bindings_json,
                dynamic_bindings: bindings.dynamic,
                dynamic_bindings_json,
                dynamic_rpc_bindings: Vec::new(),
                dynamic_rpc_bindings_json,
                dynamic_env_json,
                secret_replacements: Vec::new(),
                egress_allow_hosts: Vec::new(),
                assets: compiled_assets,
                strict_request_isolation: false,
                queue: VecDeque::new(),
                isolates: Vec::new(),
                actor_owners: HashMap::new(),
                actor_inflight: HashMap::new(),
                stats: PoolStats::default(),
                queue_warn_level: 0,
            };

        let entry = self
            .workers
            .entry(worker_name.clone())
            .or_insert_with(|| WorkerEntry {
                current_generation: generation,
                pools: HashMap::new(),
            });
        entry.current_generation = generation;
        entry.pools.insert(generation, pool);
        self.cleanup_drained_generations_for(&worker_name);
        info!(worker = %worker_name, generation, deployment_id = %deployment_id, "deployed worker");
        Ok(deployment_id)
    }

    async fn deploy_dynamic(
        &mut self,
        source: String,
        env: HashMap<String, String>,
        egress_allow_hosts: Vec<String>,
        dynamic_rpc_bindings: Vec<DynamicRpcBinding>,
    ) -> Result<DynamicDeployResult> {
        self.deploy_dynamic_internal(source, env, egress_allow_hosts, dynamic_rpc_bindings, true)
            .await
    }

    async fn deploy_dynamic_internal(
        &mut self,
        source: String,
        env: HashMap<String, String>,
        egress_allow_hosts: Vec<String>,
        dynamic_rpc_bindings: Vec<DynamicRpcBinding>,
        validate_source: bool,
    ) -> Result<DynamicDeployResult> {
        let worker_name = format!("dyn-{}", Uuid::new_v4().simple());
        let dynamic_config =
            build_dynamic_worker_config(env, egress_allow_hosts, dynamic_rpc_bindings)?;
        if validate_source {
            self.validate_worker_cached(&source).await?;
        }
        let generation = self.next_generation;
        self.next_generation += 1;
        let deployment_id = Uuid::new_v4().to_string();
        let dynamic_binding_names = dynamic_config
            .dynamic_rpc_bindings
            .iter()
            .map(|binding| binding.binding.clone())
            .collect::<Vec<_>>();
        let (snapshot, snapshot_preloaded) =
            match self.dynamic_worker_snapshot_cached(&source).await {
                Some(snapshot) => (snapshot, true),
                None => (self.bootstrap_snapshot, false),
            };
        let kv_read_cache_config_json = Arc::<str>::from(
            crate::json::to_string(&KvReadCacheConfigPayload {
                max_entries: self.config.kv_read_cache_max_entries,
                max_bytes: self.config.kv_read_cache_max_bytes,
                hit_ttl_ms: self.config.kv_read_cache_hit_ttl.as_millis() as u64,
                miss_ttl_ms: self.config.kv_read_cache_miss_ttl.as_millis() as u64,
            })
            .map_err(|error| PlatformError::internal(error.to_string()))?,
        );

        let pool = WorkerPool {
            worker_name: worker_name.clone(),
            worker_name_json: Arc::<str>::from(
                crate::json::to_string(&worker_name)
                    .map_err(|error| PlatformError::internal(error.to_string()))?,
            ),
            generation,
            deployment_id: deployment_id.clone(),
            internal_trace: None,
            is_public: false,
            snapshot,
            snapshot_preloaded,
            source: Arc::<str>::from(source),
            kv_bindings_json: Arc::<str>::from(
                crate::json::to_string(&Vec::<String>::new())
                    .map_err(|error| PlatformError::internal(error.to_string()))?,
            ),
            kv_read_cache_config_json,
            actor_bindings: Vec::new(),
            actor_bindings_json: Arc::<str>::from(
                crate::json::to_string(&Vec::<String>::new())
                    .map_err(|error| PlatformError::internal(error.to_string()))?,
            ),
            dynamic_bindings: Vec::new(),
            dynamic_bindings_json: Arc::<str>::from(
                crate::json::to_string(&Vec::<String>::new())
                    .map_err(|error| PlatformError::internal(error.to_string()))?,
            ),
            dynamic_rpc_bindings: dynamic_config.dynamic_rpc_bindings.clone(),
            dynamic_rpc_bindings_json: Arc::<str>::from(
                crate::json::to_string(&dynamic_binding_names)
                    .map_err(|error| PlatformError::internal(error.to_string()))?,
            ),
            dynamic_env_json: Arc::<str>::from(
                crate::json::to_string(&dynamic_config.dynamic_env)
                    .map_err(|error| PlatformError::internal(error.to_string()))?,
            ),
            secret_replacements: dynamic_config.secret_replacements.clone(),
            egress_allow_hosts: dynamic_config.egress_allow_hosts.clone(),
            assets: AssetBundle::default(),
            strict_request_isolation: false,
            queue: VecDeque::new(),
            isolates: Vec::new(),
            actor_owners: HashMap::new(),
            actor_inflight: HashMap::new(),
            stats: PoolStats::default(),
            queue_warn_level: 0,
        };

        let entry = self
            .workers
            .entry(worker_name.clone())
            .or_insert_with(|| WorkerEntry {
                current_generation: generation,
                pools: HashMap::new(),
            });
        entry.current_generation = generation;
        entry.pools.insert(generation, pool);
        self.cleanup_drained_generations_for(&worker_name);
        info!(
            worker = %worker_name,
            generation,
            deployment_id = %deployment_id,
            "deployed dynamic worker"
        );

        Ok(DynamicDeployResult {
            worker: worker_name,
            deployment_id,
            env_placeholders: dynamic_config.env_placeholders,
        })
    }

    fn register_stream(
        &mut self,
        worker_name: String,
        runtime_request_id: String,
        ready: oneshot::Sender<Result<WorkerStreamOutput>>,
    ) {
        let (body_sender, body_receiver) = mpsc::unbounded_channel();
        self.stream_registrations.insert(
            runtime_request_id,
            StreamRegistration {
                worker_name,
                completion_token: None,
                ready: Some(ready),
                body_sender,
                body_receiver: Some(body_receiver),
                started: false,
            },
        );
    }

    fn enqueue_invoke(
        &mut self,
        worker_name: String,
        runtime_request_id: String,
        request: WorkerInvocation,
        request_body: Option<InvokeRequestBodyReceiver>,
        actor_route: Option<ActorRoute>,
        actor_call: Option<ActorExecutionCall>,
        host_rpc_call: Option<HostRpcExecutionCall>,
        target_isolate_id: Option<u64>,
        target_generation: Option<u64>,
        internal_origin: bool,
        reply: oneshot::Sender<Result<WorkerOutput>>,
        reply_kind: PendingReplyKind,
        event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    ) {
        let worker_name = worker_name.trim().to_string();
        if self
            .pre_canceled
            .get_mut(&worker_name)
            .map(|request_ids| request_ids.remove(&runtime_request_id))
            .unwrap_or(false)
        {
            let _ = reply.send(Err(PlatformError::runtime("request was aborted")));
            self.fail_stream_registration(
                &worker_name,
                &runtime_request_id,
                PlatformError::runtime("request was aborted"),
            );
            return;
        }
        let warn_thresholds = self.config.queue_warn_thresholds.clone();
        let Some(entry) = self.workers.get(&worker_name) else {
            let error = PlatformError::not_found("Worker not found");
            let _ = reply.send(Err(error.clone()));
            self.fail_stream_registration(&worker_name, &runtime_request_id, error);
            return;
        };
        let generation = target_generation.unwrap_or(entry.current_generation);
        if !entry.pools.contains_key(&generation) {
            let error = PlatformError::not_found("Worker generation not found");
            let _ = reply.send(Err(error.clone()));
            self.fail_stream_registration(&worker_name, &runtime_request_id, error);
            return;
        }

        if let Some(pool) = self.get_pool_mut(&worker_name, generation) {
            if let Some(route) = &actor_route {
                if !pool
                    .actor_bindings
                    .iter()
                    .any(|binding| binding == &route.binding)
                {
                    let error = PlatformError::bad_request(format!(
                        "unknown memory binding for worker {}: {}",
                        worker_name, route.binding
                    ));
                    let _ = reply.send(Err(error.clone()));
                    self.fail_stream_registration(&worker_name, &runtime_request_id, error);
                    return;
                }
            }
            pool.queue.push_back(PendingInvoke {
                runtime_request_id,
                request,
                request_body,
                actor_route,
                actor_call,
                host_rpc_call,
                target_isolate_id,
                internal_origin,
                reply,
                reply_kind,
                enqueued_at: Instant::now(),
            });
            pool.update_queue_warning(&warn_thresholds);
        } else {
            let error = PlatformError::not_found("Worker not found");
            let _ = reply.send(Err(error.clone()));
            self.fail_stream_registration(&worker_name, &runtime_request_id, error);
            return;
        }

        self.dispatch_pool(&worker_name, generation, event_tx);
    }

    fn try_dispatch_direct_dynamic_fetch(
        &mut self,
        worker_name: &str,
        generation: u64,
        target_isolate_id: u64,
        runtime_request_id: String,
        request: WorkerInvocation,
        reply: oneshot::Sender<Result<WorkerOutput>>,
        handle: String,
    ) -> DirectDynamicFetchDispatch {
        let config_max_inflight = self.config.max_inflight_per_isolate;
        let dispatch_result = {
            let Some(pool) = self.get_pool_mut(worker_name, generation) else {
                return DirectDynamicFetchDispatch::Fallback {
                    reply,
                    clear_preferred: true,
                };
            };
            let max_inflight = if pool.strict_request_isolation {
                1
            } else {
                config_max_inflight
            };
            let Some(isolate_idx) = pool
                .isolates
                .iter()
                .position(|isolate| isolate.id == target_isolate_id)
            else {
                return DirectDynamicFetchDispatch::Fallback {
                    reply,
                    clear_preferred: true,
                };
            };
            let isolate_busy = pool.isolates[isolate_idx].inflight_count >= max_inflight
                || (pool.strict_request_isolation
                    && !pool.isolates[isolate_idx].pending_wait_until.is_empty());
            if isolate_busy {
                return DirectDynamicFetchDispatch::Fallback {
                    reply,
                    clear_preferred: false,
                };
            }

            let worker_name_json = Arc::clone(&pool.worker_name_json);
            let kv_bindings_json = Arc::clone(&pool.kv_bindings_json);
            let kv_read_cache_config_json = Arc::clone(&pool.kv_read_cache_config_json);
            let actor_bindings_json = Arc::clone(&pool.actor_bindings_json);
            let dynamic_bindings_json = Arc::clone(&pool.dynamic_bindings_json);
            let dynamic_rpc_bindings_json = Arc::clone(&pool.dynamic_rpc_bindings_json);
            let dynamic_env_json = Arc::clone(&pool.dynamic_env_json);
            let dynamic_bindings = pool.dynamic_bindings.clone();
            let dynamic_rpc_bindings = pool
                .dynamic_rpc_bindings
                .iter()
                .map(|binding| binding.binding.clone())
                .collect::<Vec<_>>();
            let secret_replacements = pool.secret_replacements.clone();
            let egress_allow_hosts = pool.egress_allow_hosts.clone();
            let counted_reuse = pool.isolates[isolate_idx].served_requests > 0;
            if counted_reuse {
                pool.stats.reuse_count += 1;
            }

            let completion_token = next_runtime_token("done");
            let completion_meta = Some(PendingReplyMeta {
                method: request.method.clone(),
                url: request.url.clone(),
                traceparent: None,
                user_request_id: request.request_id.clone(),
            });
            let command = IsolateCommand::Execute {
                runtime_request_id: runtime_request_id.clone(),
                completion_token: completion_token.clone(),
                worker_name_json,
                kv_bindings_json,
                kv_read_cache_config_json,
                actor_bindings_json,
                dynamic_bindings_json,
                dynamic_rpc_bindings_json,
                dynamic_env_json,
                dynamic_bindings,
                dynamic_rpc_bindings,
                secret_replacements,
                egress_allow_hosts,
                request,
                request_body: None,
                stream_response: false,
                actor_call: None,
                host_rpc_call: None,
                actor_route: None,
            };

            let isolate = &mut pool.isolates[isolate_idx];
            isolate.served_requests += 1;
            isolate.inflight_count += 1;
            isolate.pending_replies.insert(
                runtime_request_id.clone(),
                PendingReply {
                    completion_token,
                    canceled: false,
                    actor_key: None,
                    internal_origin: true,
                    reply,
                    completion_meta,
                    kind: PendingReplyKind::DynamicFetch { handle },
                    dispatched_at: Instant::now(),
                },
            );

            if isolate.sender.send(command).is_ok() {
                return DirectDynamicFetchDispatch::Dispatched;
            }

            let pending_reply = isolate
                .pending_replies
                .remove(&runtime_request_id)
                .expect("direct dynamic fetch pending reply should exist");
            isolate.inflight_count = isolate.inflight_count.saturating_sub(1);
            isolate.served_requests = isolate.served_requests.saturating_sub(1);
            let restored_reply = pending_reply.reply;
            if counted_reuse {
                pool.stats.reuse_count = pool.stats.reuse_count.saturating_sub(1);
            }
            (isolate_idx, restored_reply)
        };
        let (isolate_idx, restored_reply) = dispatch_result;
        let failed = self.remove_isolate(worker_name, generation, isolate_idx);
        for (request_id, reply) in failed {
            if request_id != runtime_request_id {
                let _ = reply.send(Err(PlatformError::internal("isolate is unavailable")));
            }
        }
        DirectDynamicFetchDispatch::Fallback {
            reply: restored_reply,
            clear_preferred: true,
        }
    }

    fn enqueue_actor_invoke(
        &mut self,
        payload: ActorInvokeEvent,
        event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    ) {
        let decoded = match decode_actor_invoke_request(&payload.request_frame) {
            Ok(decoded) => decoded,
            Err(error) => {
                let _ = payload.reply.send(Err(error));
                return;
            }
        };
        let (request, actor_call, prefer_caller_isolate) = match decoded.call {
            ActorInvokeCall::Method {
                name,
                args,
                request_id,
            } => (
                WorkerInvocation {
                    method: "ACTOR-RPC".to_string(),
                    url: format!("http://actor/__dd_rpc/{}", name),
                    headers: Vec::new(),
                    body: args.clone(),
                    request_id,
                },
                ActorExecutionCall::Method {
                    binding: decoded.binding.clone(),
                    key: decoded.key.clone(),
                    name: name.clone(),
                    args,
                },
                name == ACTOR_ATOMIC_METHOD && payload.prefer_caller_isolate,
            ),
            ActorInvokeCall::Fetch(_) => {
                let _ = payload.reply.send(Err(PlatformError::bad_request(
                    "memory fetch invoke is no longer supported; use fetch + wake + stub.atomic",
                )));
                return;
            }
        };
        let runtime_request_id = Uuid::new_v4().to_string();
        let route = ActorRoute {
            binding: decoded.binding.trim().to_string(),
            key: decoded.key.trim().to_string(),
        };
        if route.binding.is_empty() || route.key.is_empty() {
            let _ = payload.reply.send(Err(PlatformError::bad_request(
                "memory binding/key must not be empty",
            )));
            return;
        }
        let mut target_generation = None;
        let mut target_isolate_id = None;
        if payload.caller_worker_name == decoded.worker_name {
            if let Some(pool) = self
                .workers
                .get(&decoded.worker_name)
                .and_then(|entry| entry.pools.get(&payload.caller_generation))
            {
                target_generation = Some(payload.caller_generation);
                if prefer_caller_isolate
                    && pool
                        .isolates
                        .iter()
                        .any(|isolate| isolate.id == payload.caller_isolate_id)
                {
                    let owner_key = route.owner_key();
                    match pool.actor_owners.get(&owner_key).copied() {
                        Some(owner_id) if owner_id == payload.caller_isolate_id => {
                            target_isolate_id = Some(payload.caller_isolate_id);
                        }
                        None => {
                            target_isolate_id = Some(payload.caller_isolate_id);
                        }
                        _ => {}
                    }
                }
            }
        }
        let (reply_tx, reply_rx) = oneshot::channel();
        self.enqueue_invoke(
            decoded.worker_name,
            runtime_request_id,
            request,
            None,
            Some(route),
            Some(actor_call),
            None,
            target_isolate_id,
            target_generation,
            false,
            reply_tx,
            PendingReplyKind::Normal,
            event_tx,
        );
        tokio::spawn(async move {
            let result = match reply_rx.await {
                Ok(Ok(output)) => encode_actor_invoke_response(&ActorInvokeResponse::Method {
                    value: output.body,
                }),
                Ok(Err(error)) => {
                    encode_actor_invoke_response(&ActorInvokeResponse::Error(error.to_string()))
                }
                Err(_) => encode_actor_invoke_response(&ActorInvokeResponse::Error(
                    "memory invoke response channel closed".to_string(),
                )),
            };
            match result {
                Ok(frame) => {
                    let _ = payload.reply.send(Ok(frame));
                }
                Err(error) => {
                    let _ = payload.reply.send(Err(error));
                }
            }
        });
    }

    fn cancel_invoke(
        &mut self,
        worker_name: String,
        runtime_request_id: String,
        event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    ) {
        let worker_name = worker_name.trim().to_string();
        if worker_name.is_empty() {
            return;
        }

        let mut touched_generations = Vec::new();
        let mut abort_commands = Vec::new();
        let mut matched = false;
        let mut cleared_request_ids = Vec::new();
        let mut websocket_waiters_to_abort = Vec::new();

        if let Some(entry) = self.workers.get_mut(&worker_name) {
            for (generation, pool) in &mut entry.pools {
                let mut generation_touched = false;

                if let Some(idx) = pool
                    .queue
                    .iter()
                    .position(|pending| pending.runtime_request_id == runtime_request_id)
                {
                    if let Some(pending) = pool.queue.remove(idx) {
                        if let PendingReplyKind::WebsocketOpen { session_id } = pending.reply_kind {
                            websocket_waiters_to_abort.push(session_id);
                        }
                        cleared_request_ids.push(pending.runtime_request_id.clone());
                        let _ = pending
                            .reply
                            .send(Err(PlatformError::runtime("request was aborted")));
                        generation_touched = true;
                        matched = true;
                    }
                }

                for isolate in &mut pool.isolates {
                    if let Some(pending_reply) =
                        isolate.pending_replies.get_mut(&runtime_request_id)
                    {
                        if let PendingReplyKind::WebsocketOpen { session_id } = &pending_reply.kind
                        {
                            websocket_waiters_to_abort.push(session_id.clone());
                        }
                        pending_reply.canceled = true;
                        abort_commands.push((*generation, isolate.id, isolate.sender.clone()));
                        generation_touched = true;
                        matched = true;
                    }
                }

                if generation_touched {
                    pool.log_stats("cancel");
                    touched_generations.push(*generation);
                }
            }
        }

        for request_id in cleared_request_ids {
            self.clear_revalidation_for_request(&request_id);
        }

        websocket_waiters_to_abort.sort();
        websocket_waiters_to_abort.dedup();
        for session_id in websocket_waiters_to_abort {
            if let Some(waiter) = self.websocket_open_waiters.remove(&session_id) {
                let _ = waiter.send(Err(PlatformError::runtime("request was aborted")));
            }
        }

        for (generation, isolate_id, sender) in abort_commands {
            if sender
                .send(IsolateCommand::Abort {
                    runtime_request_id: runtime_request_id.clone(),
                })
                .is_err()
            {
                let failed = self.remove_isolate_by_id(&worker_name, generation, isolate_id);
                for (request_id, reply) in failed {
                    self.clear_revalidation_for_request(&request_id);
                    let _ = reply.send(Err(PlatformError::internal("isolate is unavailable")));
                }
            }
        }

        touched_generations.sort_unstable();
        touched_generations.dedup();
        for generation in touched_generations {
            self.dispatch_pool(&worker_name, generation, event_tx);
        }
        if !matched {
            self.pre_canceled
                .entry(worker_name.clone())
                .or_default()
                .insert(runtime_request_id.clone());
            self.fail_stream_registration(
                &worker_name,
                &runtime_request_id,
                PlatformError::runtime("request was aborted"),
            );
        }
        self.cleanup_drained_generations_for(&worker_name);
    }

    fn dispatch_pool(
        &mut self,
        worker_name: &str,
        generation: u64,
        event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    ) {
        if self.runtime_batch_depth > 0 {
            self.pending_dispatches
                .insert((worker_name.to_string(), generation));
            return;
        }
        let max_inflight_per_isolate = self.config.max_inflight_per_isolate;
        let max_isolates = self.config.max_isolates;
        loop {
            let (selection, spawn_needed) = {
                let Some(pool) = self.get_pool_mut(worker_name, generation) else {
                    return;
                };
                if pool.queue.is_empty() {
                    pool.log_stats("dispatch");
                    return;
                }
                let max_inflight = if pool.strict_request_isolation {
                    1
                } else {
                    max_inflight_per_isolate
                };
                let has_capacity = pool.isolates.iter().any(|isolate| {
                    isolate.inflight_count < max_inflight
                        && (!pool.strict_request_isolation || isolate.pending_wait_until.is_empty())
                });
                let selection =
                    select_dispatch_candidate(pool, max_inflight, pool.strict_request_isolation);
                let spawn_needed =
                    selection.is_none() && !has_capacity && pool.isolates.len() < max_isolates;
                (selection, spawn_needed)
            };

            if spawn_needed {
                if let Err(error) = self.spawn_isolate(worker_name, generation, event_tx.clone()) {
                    if let Some(pool) = self.get_pool_mut(worker_name, generation) {
                        if let Some(pending) = pool.queue.pop_front() {
                            let _ = pending.reply.send(Err(error));
                        }
                    }
                    return;
                }
                continue;
            }

            let Some(selection) = selection else {
                return;
            };
            let candidate = match selection {
                DispatchSelection::Dispatch(candidate) => candidate,
                DispatchSelection::DropStaleTarget { queue_idx } => {
                    if let Some(pool) = self.get_pool_mut(worker_name, generation) {
                        if let Some(stale) = pool.queue.remove(queue_idx) {
                            let _ = stale
                                .reply
                                .send(Err(PlatformError::runtime("target isolate is unavailable")));
                        }
                    }
                    continue;
                }
            };
            let isolate_idx = candidate.isolate_idx;
            let Some(pending_invoke) = self
                .get_pool_mut(worker_name, generation)
                .and_then(|pool| pool.queue.remove(candidate.queue_idx))
            else {
                return;
            };

            let runtime_request_id = pending_invoke.runtime_request_id.clone();
            let internal_origin = pending_invoke.internal_origin;
            let info_tracing_enabled = tracing::enabled!(Level::INFO);
            let needs_completion_meta = info_tracing_enabled
                || self
                    .get_pool_mut(worker_name, generation)
                    .and_then(|pool| pool.internal_trace.as_ref())
                    .is_some();
            let traceparent = if needs_completion_meta {
                traceparent_from_headers(&pending_invoke.request.headers)
            } else {
                None
            };
            let dispatch_span = if info_tracing_enabled {
                let queue_wait_ms = pending_invoke.enqueued_at.elapsed().as_millis() as u64;
                Some(tracing::info_span!(
                    "runtime.dispatch",
                    worker.name = %worker_name,
                    worker.generation = generation,
                    runtime.request_id = %runtime_request_id,
                    request.id = %pending_invoke.request.request_id,
                    queue.wait_ms = queue_wait_ms
                ))
            } else {
                None
            };
            if let Some(span) = dispatch_span.as_ref() {
                set_span_parent_from_traceparent(span, traceparent.as_deref());
            }
            let _dispatch_guard = dispatch_span.as_ref().map(|span| span.enter());

            let mut pending_reply = Some(pending_invoke.reply);
            let completion_token = next_runtime_token("done");
            if let Some(registration) = self.stream_registrations.get_mut(&runtime_request_id) {
                if registration.worker_name == worker_name {
                    registration.completion_token = Some(completion_token.clone());
                }
            }
            let stream_response = self.stream_registrations.contains_key(&runtime_request_id);
            let mut send_failed = false;
            if let Some(pool) = self.get_pool_mut(worker_name, generation) {
                if isolate_idx >= pool.isolates.len() {
                    continue;
                }

                let worker_name_json = Arc::clone(&pool.worker_name_json);
                let kv_bindings_json = Arc::clone(&pool.kv_bindings_json);
                let kv_read_cache_config_json = Arc::clone(&pool.kv_read_cache_config_json);
                let actor_bindings_json = Arc::clone(&pool.actor_bindings_json);
                let dynamic_bindings_json = Arc::clone(&pool.dynamic_bindings_json);
                let dynamic_rpc_bindings_json = Arc::clone(&pool.dynamic_rpc_bindings_json);
                let dynamic_env_json = Arc::clone(&pool.dynamic_env_json);
                let dynamic_bindings = pool.dynamic_bindings.clone();
                let dynamic_rpc_bindings = pool
                    .dynamic_rpc_bindings
                    .iter()
                    .map(|binding| binding.binding.clone())
                    .collect::<Vec<_>>();
                let secret_replacements = pool.secret_replacements.clone();
                let egress_allow_hosts = pool.egress_allow_hosts.clone();
                let should_count_reuse = pool.isolates[isolate_idx].served_requests > 0;
                if should_count_reuse {
                    pool.stats.reuse_count += 1;
                }
                let pending_kind = pending_invoke.reply_kind.clone();
                if let Some(actor_key) = &candidate.actor_key {
                    if candidate.assign_owner {
                        let owner_id = pool.isolates[isolate_idx].id;
                        pool.actor_owners.insert(actor_key.clone(), owner_id);
                    }
                    let entry = pool.actor_inflight.entry(actor_key.clone()).or_insert(0);
                    *entry += 1;
                }
                let isolate = &mut pool.isolates[isolate_idx];
                isolate.served_requests += 1;
                let completion_meta = Some(PendingReplyMeta {
                    method: pending_invoke.request.method.clone(),
                    url: pending_invoke.request.url.clone(),
                    traceparent: traceparent.clone(),
                    user_request_id: pending_invoke.request.request_id.clone(),
                });
                let command = IsolateCommand::Execute {
                    runtime_request_id: runtime_request_id.clone(),
                    completion_token: completion_token.clone(),
                    worker_name_json,
                    kv_bindings_json,
                    kv_read_cache_config_json,
                    actor_bindings_json,
                    dynamic_bindings_json,
                    dynamic_rpc_bindings_json,
                    dynamic_env_json,
                    dynamic_bindings,
                    dynamic_rpc_bindings,
                    secret_replacements,
                    egress_allow_hosts,
                    request: pending_invoke.request,
                    request_body: pending_invoke.request_body,
                    stream_response,
                    actor_call: pending_invoke.actor_call,
                    host_rpc_call: pending_invoke.host_rpc_call,
                    actor_route: pending_invoke.actor_route,
                };
                isolate.inflight_count += 1;
                isolate.pending_replies.insert(
                    runtime_request_id.clone(),
                    PendingReply {
                        completion_token,
                        canceled: false,
                        actor_key: candidate.actor_key.clone(),
                        internal_origin,
                        reply: pending_reply
                            .take()
                            .expect("pending reply must exist before dispatch"),
                        completion_meta,
                        kind: pending_kind,
                        dispatched_at: Instant::now(),
                    },
                );
                if isolate.sender.send(command).is_err() {
                    isolate.inflight_count = isolate.inflight_count.saturating_sub(1);
                    if let Some(actor_key) = candidate.actor_key.as_ref() {
                        decrement_actor_inflight(&mut pool.actor_inflight, actor_key);
                        if candidate.assign_owner {
                            pool.actor_owners.remove(actor_key);
                        }
                    }
                    pending_reply = isolate
                        .pending_replies
                        .remove(&runtime_request_id)
                        .map(|pending| pending.reply);
                    send_failed = true;
                }
            }

            if send_failed {
                let failed = self.remove_isolate(worker_name, generation, isolate_idx);
                self.clear_revalidation_for_request(&runtime_request_id);
                if let Some(reply) = pending_reply.take() {
                    let _ = reply.send(Err(PlatformError::internal("isolate is unavailable")));
                }
                for (request_id, reply) in failed {
                    self.clear_revalidation_for_request(&request_id);
                    let _ = reply.send(Err(PlatformError::internal("isolate is unavailable")));
                }
                self.fail_stream_registration(
                    worker_name,
                    &runtime_request_id,
                    PlatformError::internal("isolate is unavailable"),
                );
                continue;
            }
        }
    }

    fn spawn_isolate(
        &mut self,
        worker_name: &str,
        generation: u64,
        event_tx: mpsc::UnboundedSender<RuntimeEvent>,
    ) -> Result<()> {
        let (snapshot, snapshot_preloaded, source) = self
            .workers
            .get(worker_name)
            .and_then(|entry| entry.pools.get(&generation))
            .map(|pool| (pool.snapshot, pool.snapshot_preloaded, pool.source.clone()))
            .ok_or_else(|| PlatformError::not_found("Worker not found"))?;
        let isolate_id = self.next_isolate_id;
        self.next_isolate_id += 1;
        let kv_store = self.kv_store.clone();
        let actor_store = self.actor_store.clone();
        let cache_store = self.cache_store.clone();
        let dynamic_profile = self.dynamic_profile.clone();
        let open_handle_registry = self.open_handle_registry.clone();
        let isolate = spawn_isolate_thread(
            snapshot,
            snapshot_preloaded,
            source,
            kv_store,
            actor_store,
            cache_store,
            open_handle_registry,
            dynamic_profile,
            self.runtime_fast_sender.clone(),
            worker_name.to_string(),
            generation,
            isolate_id,
            event_tx,
        )?;
        if let Some(pool) = self.get_pool_mut(worker_name, generation) {
            pool.stats.spawn_count += 1;
            pool.isolates.push(isolate);
            pool.log_stats("spawn");
            Ok(())
        } else {
            Err(PlatformError::internal("worker pool missing"))
        }
    }

    fn finish_request(
        &mut self,
        worker_name: &str,
        generation: u64,
        isolate_id: u64,
        request_id: &str,
        completion_token: &str,
        wait_until_count: usize,
        mut result: Result<WorkerOutput>,
        event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    ) {
        let mut reply = None;
        let mut canceled = false;
        let mut clear_revalidation = false;
        let mut completion_traceparent: Option<String> = None;
        let mut user_request_id = String::new();
        let mut request_method = String::new();
        let mut request_url = String::new();
        let mut execution_ms: Option<u64> = None;
        let mut internal_origin = false;
        let mut pending_kind = PendingReplyKind::Normal;
        let trace_destination = self
            .get_pool_mut(worker_name, generation)
            .and_then(|pool| pool.internal_trace.clone());
        let info_tracing_enabled = tracing::enabled!(Level::INFO);
        let stream_registered = self.stream_registrations.contains_key(request_id);
        if let Some(pool) = self.get_pool_mut(worker_name, generation) {
            if let Some(isolate) = pool
                .isolates
                .iter_mut()
                .find(|isolate| isolate.id == isolate_id)
            {
                let Some(pending) = isolate.pending_replies.get(request_id) else {
                    warn!(
                        worker = %worker_name,
                        generation,
                        isolate_id,
                        request_id,
                        "dropping completion for unknown request id"
                    );
                    return;
                };
                let token_mismatch = pending.completion_token != completion_token;
                if token_mismatch {
                    warn!(
                        worker = %worker_name,
                        generation,
                        isolate_id,
                        request_id,
                        "dropping completion with invalid token"
                    );
                }

                isolate.inflight_count = isolate.inflight_count.saturating_sub(1);
                if isolate.inflight_count == 0 {
                    isolate.last_used_at = Instant::now();
                }
                if let Some(pending) = isolate.pending_replies.remove(request_id) {
                    if let Some(actor_key) = &pending.actor_key {
                        decrement_actor_inflight(&mut pool.actor_inflight, actor_key);
                    }
                    canceled = pending.canceled;
                    internal_origin = pending.internal_origin;
                    if let Some(meta) = pending.completion_meta {
                        request_method = meta.method;
                        request_url = meta.url;
                        completion_traceparent = meta.traceparent;
                        user_request_id = meta.user_request_id;
                    }
                    if token_mismatch {
                        result = Err(PlatformError::internal(format!(
                            "completion token mismatch for runtime request {request_id}"
                        )));
                    } else if wait_until_count > 0 {
                        isolate
                            .pending_wait_until
                            .insert(request_id.to_string(), completion_token.to_string());
                    }
                    clear_revalidation = true;
                    execution_ms = Some(pending.dispatched_at.elapsed().as_millis() as u64);
                    pending_kind = pending.kind;
                    reply = Some(pending.reply);
                }
            }
            pool.log_stats("complete");
        }
        if clear_revalidation {
            self.clear_revalidation_for_request(request_id);
        }
        let complete_span = if info_tracing_enabled {
            let result_status = match &result {
                Ok(output) => output.status as i64,
                Err(_) => -1,
            };
            let result_ok = result.is_ok();
            let span = tracing::info_span!(
                "runtime.complete",
                worker.name = %worker_name,
                worker.generation = generation,
                isolate.id = isolate_id,
                runtime.request_id = %request_id,
                request.id = %user_request_id,
                request.ok = result_ok,
                response.status = result_status,
                request.execution_ms = execution_ms.unwrap_or_default(),
                request.wait_until_count = wait_until_count as u64
            );
            set_span_parent_from_traceparent(&span, completion_traceparent.as_deref());
            Some(span)
        } else {
            None
        };
        let _complete_guard = complete_span.as_ref().map(|span| span.enter());

        let stream_result = if stream_registered {
            Some(result.clone())
        } else {
            None
        };
        let trace_result = if trace_destination.is_some() {
            Some(result.clone())
        } else {
            None
        };

        if !canceled {
            match pending_kind {
                PendingReplyKind::Normal => {
                    if let Some(reply) = reply {
                        let _ = reply.send(result);
                    }
                }
                PendingReplyKind::DynamicInvoke { handle }
                | PendingReplyKind::DynamicFetch { handle } => {
                    if let Some(entry) = self.dynamic_worker_handles.get_mut(&handle) {
                        match &result {
                            Ok(_) => {
                                entry.preferred_isolate_id = Some(isolate_id);
                            }
                            Err(_) if entry.preferred_isolate_id == Some(isolate_id) => {
                                entry.preferred_isolate_id = None;
                            }
                            Err(_) => {}
                        }
                    }
                    if let Some(reply) = reply {
                        let _ = reply.send(result);
                    }
                }
                PendingReplyKind::WebsocketOpen { session_id } => {
                    self.complete_websocket_open(
                        worker_name,
                        generation,
                        isolate_id,
                        session_id,
                        result,
                    );
                }
                PendingReplyKind::WebsocketFrame { session_id } => {
                    self.complete_websocket_frame(session_id, reply, result);
                }
                PendingReplyKind::TransportOpen { session_id } => {
                    self.complete_transport_open(
                        worker_name,
                        generation,
                        isolate_id,
                        session_id,
                        result,
                    );
                }
            }
            if info_tracing_enabled {
                tracing::info!("request completion delivered");
            }
        } else {
            if info_tracing_enabled {
                info!(
                    worker = %worker_name,
                    generation,
                    isolate_id,
                    request_id,
                    "dropped completion for canceled request"
                );
            }
        }
        if let (Some(trace_destination), Some(trace_result)) =
            (trace_destination, trace_result.as_ref())
        {
            self.enqueue_trace_forward(
                worker_name,
                generation,
                &request_method,
                &request_url,
                request_id,
                &user_request_id,
                trace_result,
                execution_ms.unwrap_or_default(),
                wait_until_count,
                internal_origin,
                Some(trace_destination),
                event_tx,
            );
        }
        if let Some(stream_result) = stream_result {
            self.complete_stream_registration(
                worker_name,
                request_id,
                completion_token,
                stream_result,
            );
        }
    }

    fn enqueue_trace_forward(
        &mut self,
        worker_name: &str,
        generation: u64,
        request_method: &str,
        request_url: &str,
        runtime_request_id: &str,
        user_request_id: &str,
        result: &Result<WorkerOutput>,
        execution_ms: u64,
        wait_until_count: usize,
        internal_origin: bool,
        trace_destination: Option<InternalTraceDestination>,
        event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    ) {
        let Some(trace_destination) = trace_destination else {
            return;
        };
        if internal_origin {
            return;
        }

        let (status, error) = match result {
            Ok(output) => (Some(output.status), None),
            Err(error) => (None, Some(error.to_string())),
        };

        let payload = TraceEventPayload {
            ts_ms: match epoch_ms_i64() {
                Ok(ts_ms) => ts_ms,
                Err(error) => {
                    warn!(
                        worker = %worker_name,
                        generation,
                        runtime_request_id,
                        error = %error,
                        "skipping trace forward due invalid clock"
                    );
                    return;
                }
            },
            worker: worker_name.to_string(),
            generation,
            request_id: user_request_id.to_string(),
            runtime_request_id: runtime_request_id.to_string(),
            method: request_method.to_string(),
            url: request_url.to_string(),
            status,
            ok: status.is_some(),
            error,
            execution_ms,
            wait_until_count,
        };

        // Use serde_json for trace forwarding payloads to avoid simd-json's
        // mutable-buffer serialization path on this hot async boundary.
        let body = match serde_json::to_vec(&payload) {
            Ok(body) => body,
            Err(error) => {
                warn!(
                    worker = %worker_name,
                    generation,
                    runtime_request_id,
                    error = %error,
                    "skipping trace forward due payload serialization failure"
                );
                return;
            }
        };
        let mut headers = vec![(
            CONTENT_TYPE_HEADER.to_string(),
            JSON_CONTENT_TYPE.to_string(),
        )];
        append_internal_trace_headers(&mut headers, worker_name, generation);
        let trace_request = WorkerInvocation {
            method: "POST".to_string(),
            url: format!(
                "http://{}{}",
                trace_destination.worker,
                normalize_trace_path(&trace_destination.path)
            ),
            headers,
            body,
            request_id: Uuid::new_v4().to_string(),
        };
        let (reply, reply_rx) = oneshot::channel();
        self.enqueue_invoke(
            trace_destination.worker,
            Uuid::new_v4().to_string(),
            trace_request,
            None,
            None,
            None,
            None,
            None,
            None,
            true,
            reply,
            PendingReplyKind::Normal,
            event_tx,
        );
        let request_id_for_warning = runtime_request_id.to_string();
        tokio::spawn(async move {
            match reply_rx.await {
                Ok(Ok(output)) => {
                    if !matches!(output.status, 200..=299) {
                        warn!(
                            request_id = %request_id_for_warning,
                            status = output.status,
                            "internal trace forward responded non-2xx"
                        );
                    }
                }
                Ok(Err(error)) => {
                    warn!(error = %error, "internal trace forward failed");
                }
                Err(error) => {
                    warn!(error = %error, "internal trace forward receiver dropped");
                }
            }
        });
    }

    fn fail_isolate(
        &mut self,
        worker_name: &str,
        generation: u64,
        isolate_id: u64,
        error: PlatformError,
    ) {
        let failed = self.remove_isolate_by_id(worker_name, generation, isolate_id);
        for (request_id, reply) in failed {
            self.clear_revalidation_for_request(&request_id);
            let _ = reply.send(Err(error.clone()));
        }
        self.fail_all_streams_for_worker(worker_name, error);
    }

    fn finish_wait_until(
        &mut self,
        worker_name: &str,
        generation: u64,
        isolate_id: u64,
        request_id: &str,
        completion_token: &str,
    ) {
        if let Some(pool) = self.get_pool_mut(worker_name, generation) {
            if let Some(isolate) = pool
                .isolates
                .iter_mut()
                .find(|isolate| isolate.id == isolate_id)
            {
                if let Some(token) = isolate.pending_wait_until.get(request_id) {
                    if token == completion_token {
                        isolate.pending_wait_until.remove(request_id);
                        if isolate.inflight_count == 0 && isolate.pending_wait_until.is_empty() {
                            isolate.last_used_at = Instant::now();
                        }
                    }
                }
            }
            pool.log_stats("wait_until_done");
        }
    }

    fn handle_response_start(
        &mut self,
        worker_name: &str,
        request_id: &str,
        completion_token: &str,
        status: u16,
        headers: Vec<(String, String)>,
    ) {
        let Some(registration) = self.stream_registrations.get_mut(request_id) else {
            return;
        };
        if registration.worker_name != worker_name {
            return;
        }
        if registration.completion_token.as_deref() != Some(completion_token) {
            return;
        }
        registration.started = true;
        if let Some(ready) = registration.ready.take() {
            if let Some(body) = registration.body_receiver.take() {
                let _ = ready.send(Ok(WorkerStreamOutput {
                    status,
                    headers,
                    body,
                }));
            } else {
                let _ = ready.send(Err(PlatformError::internal("stream body receiver missing")));
            }
        }
    }

    fn handle_response_chunk(
        &mut self,
        worker_name: &str,
        request_id: &str,
        completion_token: &str,
        chunk: Vec<u8>,
    ) {
        let Some(registration) = self.stream_registrations.get(request_id) else {
            return;
        };
        if registration.worker_name != worker_name {
            return;
        }
        if registration.completion_token.as_deref() != Some(completion_token) {
            return;
        }
        let _ = registration.body_sender.send(Ok(chunk));
    }

    fn schedule_cache_revalidate(
        &mut self,
        worker_name: &str,
        generation: u64,
        payload: String,
        event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    ) {
        let Some(entry) = self.workers.get(worker_name) else {
            return;
        };
        if entry.current_generation != generation {
            return;
        }

        let request = match decode_cache_revalidate_payload(payload) {
            Ok(request) => request,
            Err(error) => {
                warn!(
                    worker = %worker_name,
                    generation,
                    error = %error,
                    "ignoring invalid cache revalidate payload"
                );
                return;
            }
        };

        let method = request.method.trim().to_ascii_uppercase();
        if method != "GET" {
            return;
        }
        let revalidate_span = tracing::info_span!(
            "runtime.cache.revalidate_schedule",
            worker.name = %worker_name,
            worker.generation = generation,
            cache.name = %request.cache_name,
            http.method = %method,
            http.url = %request.url
        );
        set_span_parent_from_traceparent(
            &revalidate_span,
            traceparent_from_headers(&request.headers).as_deref(),
        );
        let _revalidate_guard = revalidate_span.enter();

        let key = cache_revalidation_key(worker_name, generation, &request);
        if !self.revalidation_keys.insert(key.clone()) {
            tracing::info!("skipping duplicate cache revalidation");
            return;
        }

        let runtime_request_id = Uuid::new_v4().to_string();
        let request_id = format!("cache-revalidate-{runtime_request_id}");
        let mut headers = request.headers.clone();
        if !headers
            .iter()
            .any(|(name, _)| name.eq_ignore_ascii_case("x-dd-cache-bypass-stale"))
        {
            headers.push(("x-dd-cache-bypass-stale".to_string(), "1".to_string()));
        }

        let invocation = WorkerInvocation {
            method,
            url: request.url,
            headers,
            body: Vec::new(),
            request_id,
        };
        let (reply, _receiver) = oneshot::channel();
        let warn_thresholds = self.config.queue_warn_thresholds.clone();
        if let Some(pool) = self.get_pool_mut(worker_name, generation) {
            pool.queue.push_back(PendingInvoke {
                runtime_request_id: runtime_request_id.clone(),
                request: invocation,
                request_body: None,
                actor_route: None,
                actor_call: None,
                host_rpc_call: None,
                target_isolate_id: None,
                internal_origin: false,
                reply,
                reply_kind: PendingReplyKind::Normal,
                enqueued_at: Instant::now(),
            });
            pool.update_queue_warning(&warn_thresholds);
            self.revalidation_requests.insert(runtime_request_id, key);
            self.dispatch_pool(worker_name, generation, event_tx);
            tracing::info!("scheduled background cache revalidation");
        } else {
            self.revalidation_keys.remove(&key);
        }
    }

    fn clear_revalidation_for_request(&mut self, request_id: &str) {
        if let Some(key) = self.revalidation_requests.remove(request_id) {
            self.revalidation_keys.remove(&key);
        }
    }

    fn fail_stream_registration(
        &mut self,
        worker_name: &str,
        request_id: &str,
        error: PlatformError,
    ) {
        let Some(mut registration) = self.stream_registrations.remove(request_id) else {
            return;
        };
        if registration.worker_name != worker_name {
            self.stream_registrations
                .insert(request_id.to_string(), registration);
            return;
        }
        if let Some(ready) = registration.ready.take() {
            let _ = ready.send(Err(error.clone()));
            return;
        }
        let _ = registration.body_sender.send(Err(error));
    }

    fn complete_stream_registration(
        &mut self,
        worker_name: &str,
        request_id: &str,
        completion_token: &str,
        result: Result<WorkerOutput>,
    ) {
        let Some(mut registration) = self.stream_registrations.remove(request_id) else {
            return;
        };
        if registration.worker_name != worker_name {
            self.stream_registrations
                .insert(request_id.to_string(), registration);
            return;
        }
        if registration.completion_token.as_deref() != Some(completion_token) {
            self.stream_registrations
                .insert(request_id.to_string(), registration);
            return;
        }

        match result {
            Ok(output) => {
                if !registration.started {
                    if let Some(ready) = registration.ready.take() {
                        if let Some(body) = registration.body_receiver.take() {
                            let _ = ready.send(Ok(WorkerStreamOutput {
                                status: output.status,
                                headers: output.headers.clone(),
                                body,
                            }));
                        } else {
                            let _ = ready
                                .send(Err(PlatformError::internal("stream body receiver missing")));
                        }
                    }
                    if !output.body.is_empty() {
                        let _ = registration.body_sender.send(Ok(output.body));
                    }
                }
            }
            Err(error) => {
                if let Some(ready) = registration.ready.take() {
                    let _ = ready.send(Err(error.clone()));
                } else {
                    let _ = registration.body_sender.send(Err(error));
                }
            }
        }
    }

    fn fail_all_streams_for_worker(&mut self, worker_name: &str, error: PlatformError) {
        let request_ids: Vec<String> = self
            .stream_registrations
            .iter()
            .filter(|(_, registration)| registration.worker_name == worker_name)
            .map(|(request_id, _)| request_id.clone())
            .collect();

        for request_id in request_ids {
            self.fail_stream_registration(worker_name, &request_id, error.clone());
        }
    }

    fn retire_worker_completely(&mut self, worker_name: &str) {
        let mut clear_request_ids = Vec::new();
        self.reap_owned_sessions(worker_name, None, None);
        if let Some(mut entry) = self.workers.remove(worker_name) {
            for (_, pool) in entry.pools.drain() {
                for isolate in pool.isolates {
                    let _ = isolate.sender.send(IsolateCommand::Shutdown);
                    for (request_id, pending) in isolate.pending_replies {
                        clear_request_ids.push(request_id);
                        let _ = pending
                            .reply
                            .send(Err(PlatformError::internal("dynamic worker was deleted")));
                    }
                }
            }
        }
        for request_id in clear_request_ids {
            self.clear_revalidation_for_request(&request_id);
        }
        self.fail_all_streams_for_worker(
            worker_name,
            PlatformError::internal("dynamic worker was deleted"),
        );
        self.dynamic_worker_handles
            .retain(|_, handle| handle.worker_name != worker_name);
        let existing_handles: HashSet<String> =
            self.dynamic_worker_handles.keys().cloned().collect();
        self.dynamic_worker_ids.retain(|_, by_id| {
            by_id.retain(|_, handle| existing_handles.contains(handle));
            !by_id.is_empty()
        });
        self.host_rpc_providers
            .retain(|_, provider| provider.owner_worker != worker_name);
    }

    fn remove_isolate(
        &mut self,
        worker_name: &str,
        generation: u64,
        isolate_idx: usize,
    ) -> Vec<(String, oneshot::Sender<Result<WorkerOutput>>)> {
        let mut websocket_open_session_ids = Vec::new();
        let mut transport_open_session_ids = Vec::new();
        let mut replies = Vec::new();
        let mut removed_isolate_id = None;
        let mut removed = false;
        if let Some(pool) = self.get_pool_mut(worker_name, generation) {
            if isolate_idx < pool.isolates.len() {
                let isolate = pool.isolates.swap_remove(isolate_idx);
                let _ = isolate.sender.send(IsolateCommand::Shutdown);
                removed_isolate_id = Some(isolate.id);
                pool.actor_owners
                    .retain(|_, owner_id| *owner_id != isolate.id);
                replies = Vec::with_capacity(isolate.pending_replies.len());
                for (request_id, pending) in isolate.pending_replies {
                    if let Some(actor_key) = pending.actor_key.as_deref() {
                        decrement_actor_inflight(&mut pool.actor_inflight, actor_key);
                    }
                    match &pending.kind {
                        PendingReplyKind::WebsocketOpen { session_id } => {
                            websocket_open_session_ids.push(session_id.clone());
                        }
                        PendingReplyKind::TransportOpen { session_id } => {
                            transport_open_session_ids.push(session_id.clone());
                        }
                        PendingReplyKind::Normal
                        | PendingReplyKind::DynamicInvoke { .. }
                        | PendingReplyKind::DynamicFetch { .. }
                        | PendingReplyKind::WebsocketFrame { .. } => {}
                    }
                    replies.push((request_id, pending.reply));
                }
                removed = true;
            }
        }
        if let Some(isolate_id) = removed_isolate_id {
            self.reap_owned_sessions(worker_name, Some(generation), Some(isolate_id));
        }
        for session_id in websocket_open_session_ids {
            if let Some(waiter) = self.websocket_open_waiters.remove(&session_id) {
                let _ = waiter.send(Err(PlatformError::internal("isolate is unavailable")));
            }
        }
        for session_id in transport_open_session_ids {
            if let Some(waiter) = self.transport_open_waiters.remove(&session_id) {
                let _ = waiter.send(Err(PlatformError::internal("isolate is unavailable")));
            }
            self.transport_open_channels.remove(&session_id);
        }
        if removed {
            replies
        } else {
            Vec::new()
        }
    }

    fn remove_isolate_by_id(
        &mut self,
        worker_name: &str,
        generation: u64,
        isolate_id: u64,
    ) -> Vec<(String, oneshot::Sender<Result<WorkerOutput>>)> {
        let isolate_idx = self
            .workers
            .get(worker_name)
            .and_then(|entry| entry.pools.get(&generation))
            .and_then(|pool| {
                pool.isolates
                    .iter()
                    .position(|isolate| isolate.id == isolate_id)
            });
        if let Some(idx) = isolate_idx {
            return self.remove_isolate(worker_name, generation, idx);
        }
        Vec::new()
    }

    fn scale_down_idle(&mut self) {
        let now = Instant::now();
        let worker_names: Vec<String> = self.workers.keys().cloned().collect();
        for worker_name in worker_names {
            let generations: Vec<u64> = self
                .workers
                .get(&worker_name)
                .map(|entry| entry.pools.keys().copied().collect())
                .unwrap_or_default();
            for generation in generations {
                self.scale_down_pool(&worker_name, generation, now);
            }
            self.cleanup_drained_generations_for(&worker_name);
        }
    }

    fn scale_down_pool(&mut self, worker_name: &str, generation: u64, now: Instant) {
        let min_isolates = self.config.min_isolates;
        let idle_ttl = self.config.idle_ttl;
        let mut removed = Vec::new();
        if let Some(pool) = self.get_pool_mut(worker_name, generation) {
            loop {
                if pool.isolates.len() <= min_isolates {
                    break;
                }

                let candidate = pool
                    .isolates
                    .iter()
                    .enumerate()
                    .filter(|(_, isolate)| isolate.inflight_count == 0)
                    .filter(|(_, isolate)| isolate.pending_wait_until.is_empty())
                    .filter(|(_, isolate)| isolate.active_websocket_sessions == 0)
                    .filter(|(_, isolate)| isolate.active_transport_sessions == 0)
                    .filter(|(_, isolate)| now.duration_since(isolate.last_used_at) >= idle_ttl)
                    .min_by_key(|(_, isolate)| isolate.last_used_at);
                let Some((idx, _)) = candidate else {
                    break;
                };
                let isolate = pool.isolates.swap_remove(idx);
                pool.stats.scale_down_count += 1;
                pool.actor_owners
                    .retain(|_, owner_id| *owner_id != isolate.id);
                for pending in isolate.pending_replies.values() {
                    if let Some(actor_key) = pending.actor_key.as_deref() {
                        decrement_actor_inflight(&mut pool.actor_inflight, actor_key);
                    }
                }
                removed.push(isolate);
            }

            if !removed.is_empty() {
                pool.log_stats("scale_down");
            }
        }

        for isolate in removed {
            let _ = isolate.sender.send(IsolateCommand::Shutdown);
            for (request_id, pending) in isolate.pending_replies {
                self.clear_revalidation_for_request(&request_id);
                let _ = pending
                    .reply
                    .send(Err(PlatformError::internal("isolate scaled down")));
            }
        }
    }

    fn cleanup_drained_generations_for(&mut self, worker_name: &str) {
        if self.runtime_batch_depth > 0 {
            self.pending_cleanup_workers.insert(worker_name.to_string());
            return;
        }
        let mut clear_request_ids = Vec::new();
        let mut retired_generations = HashSet::new();
        let live_websocket_generations: HashSet<u64> = self
            .websocket_sessions
            .values()
            .filter(|session| session.worker_name == worker_name)
            .map(|session| session.generation)
            .collect();
        let live_transport_generations: HashSet<u64> = self
            .transport_sessions
            .values()
            .filter(|session| session.worker_name == worker_name)
            .map(|session| session.generation)
            .collect();
        let drained = {
            let Some(entry) = self.workers.get(worker_name) else {
                return;
            };
            let current_generation = entry.current_generation;
            entry
                .pools
                .iter()
                .filter(|(generation, pool)| {
                    **generation != current_generation
                        && pool.is_drained()
                        && !live_websocket_generations.contains(generation)
                        && !live_transport_generations.contains(generation)
                })
                .map(|(generation, _)| *generation)
                .collect::<Vec<_>>()
        };

        for generation in drained {
            if let Some(pool) = self
                .workers
                .get_mut(worker_name)
                .and_then(|entry| entry.pools.remove(&generation))
            {
                retired_generations.insert(generation);
                for isolate in pool.isolates {
                    let _ = isolate.sender.send(IsolateCommand::Shutdown);
                    for (request_id, pending) in isolate.pending_replies {
                        clear_request_ids.push(request_id);
                        let _ = pending
                            .reply
                            .send(Err(PlatformError::internal("worker generation retired")));
                    }
                }
                info!(worker = %pool.worker_name, generation, "retired worker generation");
            }
        }
        for request_id in clear_request_ids {
            self.clear_revalidation_for_request(&request_id);
        }
        if retired_generations.is_empty() {
            return;
        }
        self.dynamic_worker_handles.retain(|_, handle| {
            !(handle.owner_worker == worker_name
                && retired_generations.contains(&handle.owner_generation))
        });
        let existing_handles: HashSet<String> =
            self.dynamic_worker_handles.keys().cloned().collect();
        self.dynamic_worker_ids.retain(|key, by_id| {
            if key.owner_worker == worker_name
                && retired_generations.contains(&key.owner_generation)
            {
                return false;
            }
            by_id.retain(|_, handle| existing_handles.contains(handle));
            !by_id.is_empty()
        });
        self.host_rpc_providers.retain(|_, provider| {
            !(provider.owner_worker == worker_name
                && retired_generations.contains(&provider.owner_generation))
        });
    }

    fn worker_stats(&self, worker_name: &str) -> Option<WorkerStats> {
        let entry = self.workers.get(worker_name)?;
        let pool = entry.pools.get(&entry.current_generation)?;
        Some(pool.stats_snapshot())
    }

    fn worker_debug_dump(&self, worker_name: &str) -> Option<WorkerDebugDump> {
        let entry = self.workers.get(worker_name)?;
        let pool = entry.pools.get(&entry.current_generation)?;
        Some(pool.debug_dump())
    }

    fn dynamic_debug_dump(&self) -> DynamicRuntimeDebugDump {
        let mut handles = self
            .dynamic_worker_handles
            .iter()
            .map(|(handle, entry)| DynamicHandleDebug {
                handle: handle.clone(),
                owner_worker: entry.owner_worker.clone(),
                owner_generation: entry.owner_generation,
                binding: entry.binding.clone(),
                worker_name: entry.worker_name.clone(),
                timeout_ms: entry.timeout,
            })
            .collect::<Vec<_>>();
        handles.sort_by(|left, right| left.handle.cmp(&right.handle));

        let mut providers = self
            .host_rpc_providers
            .iter()
            .map(|(provider_id, provider)| {
                let mut methods = provider.methods.iter().cloned().collect::<Vec<_>>();
                methods.sort();
                HostRpcProviderDebug {
                    provider_id: provider_id.clone(),
                    owner_worker: provider.owner_worker.clone(),
                    owner_generation: provider.owner_generation,
                    owner_isolate_id: provider.owner_isolate_id,
                    target_id: provider.target_id.clone(),
                    methods,
                }
            })
            .collect::<Vec<_>>();
        providers.sort_by(|left, right| left.provider_id.cmp(&right.provider_id));

        DynamicRuntimeDebugDump {
            handles,
            providers,
            snapshot_cache_entries: self.dynamic_worker_snapshots.len(),
            snapshot_cache_failures: self.dynamic_worker_snapshot_failures.len(),
        }
    }

    fn log_dynamic_timeout_diagnostic(&self, diagnostic: DynamicTimeoutDiagnostic) {
        let owner_dump = self.worker_debug_dump(&diagnostic.owner_worker);
        let target_dump = self.worker_debug_dump(&diagnostic.target_worker);
        let handle_summary = self
            .dynamic_worker_handles
            .get(&diagnostic.handle)
            .map(|handle| {
                (
                    handle.owner_worker.clone(),
                    handle.owner_generation,
                    handle.binding.clone(),
                    handle.worker_name.clone(),
                    handle.timeout,
                )
            });
        let mut provider_summaries = self
            .host_rpc_providers
            .iter()
            .filter(|(_, provider)| {
                provider.owner_worker == diagnostic.owner_worker
                    && provider.owner_generation == diagnostic.owner_generation
            })
            .map(|(provider_id, provider)| {
                let mut methods = provider.methods.iter().cloned().collect::<Vec<_>>();
                methods.sort();
                (
                    provider_id.clone(),
                    provider.owner_isolate_id,
                    provider.target_id.clone(),
                    methods,
                )
            })
            .collect::<Vec<_>>();
        provider_summaries.sort_by(|left, right| left.0.cmp(&right.0));
        warn!(
            stage = diagnostic.stage,
            owner_worker = %diagnostic.owner_worker,
            owner_generation = diagnostic.owner_generation,
            binding = %diagnostic.binding,
            handle = %diagnostic.handle,
            target_worker = %diagnostic.target_worker,
            target_isolate_id = diagnostic.target_isolate_id,
            target_generation = diagnostic.target_generation,
            provider_id = ?diagnostic.provider_id,
            provider_owner_isolate_id = diagnostic.provider_owner_isolate_id,
            provider_target_id = ?diagnostic.provider_target_id,
            timeout_ms = diagnostic.timeout_ms,
            handle_summary = ?handle_summary,
            host_rpc_providers = ?provider_summaries,
            owner_dump = ?owner_dump,
            target_dump = ?target_dump,
            "dynamic invoke timeout diagnostic"
        );
    }

    fn handle_test_async_reply(
        &mut self,
        payload: crate::ops::TestAsyncReplyEvent,
        event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    ) {
        let result = if payload.ok {
            Ok(payload.value)
        } else {
            Err(PlatformError::runtime(if payload.error.trim().is_empty() {
                "test async reply failed".to_string()
            } else {
                payload.error
            }))
        };
        if payload.delay_ms == 0 {
            self.complete_test_async_reply(payload.reply_id, payload.replies, result);
            return;
        }
        let event_tx = event_tx.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(payload.delay_ms)).await;
            let _ = event_tx.send(RuntimeEvent::TestAsyncReplyComplete {
                reply_id: payload.reply_id,
                replies: payload.replies,
                result,
            });
        });
    }

    fn handle_test_nested_targeted_invoke(
        &mut self,
        payload: crate::ops::TestNestedTargetedInvokeEvent,
        event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    ) {
        let Some(pool) = self.get_pool_mut(&payload.worker_name, payload.generation) else {
            self.complete_test_async_reply(
                payload.reply_id,
                payload.replies,
                Err(PlatformError::runtime(
                    "test nested invoke worker pool is unavailable",
                )),
            );
            return;
        };

        let target_isolate_id = match payload.target_mode.trim() {
            "same" | "" => Some(payload.caller_isolate_id),
            "other" => pool
                .isolates
                .iter()
                .find(|isolate| isolate.id != payload.caller_isolate_id)
                .map(|isolate| isolate.id),
            mode => {
                self.complete_test_async_reply(
                    payload.reply_id,
                    payload.replies,
                    Err(PlatformError::runtime(format!(
                        "unknown test nested target mode: {mode}"
                    ))),
                );
                return;
            }
        };
        let Some(target_isolate_id) = target_isolate_id else {
            self.complete_test_async_reply(
                payload.reply_id,
                payload.replies,
                Err(PlatformError::runtime(
                    "test nested invoke target isolate is unavailable",
                )),
            );
            return;
        };

        let Some(target_isolate_id) = self
            .workers
            .get(&payload.worker_name)
            .and_then(|entry| entry.pools.get(&payload.generation))
            .and_then(|pool| {
                pool.isolates
                    .iter()
                    .find(|isolate| isolate.id == target_isolate_id)
                    .map(|isolate| isolate.id)
            })
        else {
            self.complete_test_async_reply(
                payload.reply_id,
                payload.replies,
                Err(PlatformError::runtime(
                    "test nested invoke target isolate sender is unavailable",
                )),
            );
            return;
        };

        let reply_id = payload.reply_id;
        let replies = payload.replies;
        if let Err(error) = self.start_targeted_host_rpc_invoke(
            payload.worker_name,
            payload.generation,
            target_isolate_id,
            payload.target_id,
            payload.method_name,
            payload.args,
            TargetedHostRpcReply::Test {
                reply_id: reply_id.clone(),
                replies: replies.clone(),
                success_value: format!("ok:{target_isolate_id}"),
            },
            event_tx,
        ) {
            self.complete_test_async_reply(reply_id, replies, Err(error));
        }
    }

    fn complete_test_async_reply(
        &mut self,
        reply_id: String,
        replies: crate::ops::TestAsyncReplies,
        result: Result<String>,
    ) {
        let Some(delivery) = replies.finish(reply_id, result) else {
            return;
        };
        self.enqueue_isolate_reply(
            &delivery.owner.worker_name,
            delivery.owner.generation,
            delivery.owner.isolate_id,
            crate::ops::DynamicPushedReplyPayload::TestAsync(delivery.payload),
        );
    }

    fn finish_dynamic_reply(
        &mut self,
        pending_replies: crate::ops::DynamicPendingReplies,
        reply_id: String,
        payload: crate::ops::DynamicPendingReplyPayload,
    ) {
        let Some(delivery) = pending_replies.finish(reply_id, payload) else {
            return;
        };
        self.enqueue_isolate_reply(
            &delivery.owner.worker_name,
            delivery.owner.generation,
            delivery.owner.isolate_id,
            delivery.payload,
        );
    }

    fn enqueue_isolate_reply(
        &mut self,
        worker_name: &str,
        generation: u64,
        isolate_id: u64,
        payload: crate::ops::DynamicPushedReplyPayload,
    ) {
        let Some((sender, inbox)) = self
            .workers
            .get(worker_name)
            .and_then(|entry| entry.pools.get(&generation))
            .and_then(|pool| {
                pool.isolates
                    .iter()
                    .find(|isolate| isolate.id == isolate_id)
                    .map(|isolate| {
                        (
                            isolate.sender.clone(),
                            isolate.dynamic_control_inbox.clone(),
                        )
                    })
            })
        else {
            return;
        };
        let schedule = inbox.push_reply(payload);
        if schedule {
            let _ = sender.send(IsolateCommand::DrainDynamicControl);
        }
    }

    fn start_targeted_host_rpc_invoke(
        &mut self,
        worker_name: String,
        generation: u64,
        isolate_id: u64,
        target_id: String,
        method_name: String,
        args: Vec<u8>,
        reply: TargetedHostRpcReply,
        event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    ) -> Result<()> {
        let provider_available = self
            .workers
            .get(&worker_name)
            .and_then(|entry| entry.pools.get(&generation))
            .map(|pool| pool.isolates.iter().any(|isolate| isolate.id == isolate_id))
            .unwrap_or(false);
        if !provider_available {
            return Err(PlatformError::runtime(
                "dynamic host rpc provider isolate is unavailable",
            ));
        }
        let runtime_request_id = next_runtime_token("dhrpc");
        let request = WorkerInvocation {
            method: "POST".to_string(),
            url: "http://worker/__dd_internal_host_rpc".to_string(),
            headers: Vec::new(),
            body: Vec::new(),
            request_id: runtime_request_id.clone(),
        };
        let (inner_reply_tx, inner_reply_rx) = oneshot::channel();
        self.enqueue_invoke(
            worker_name,
            runtime_request_id,
            request,
            None,
            None,
            None,
            Some(HostRpcExecutionCall {
                target_id,
                method: method_name,
                args,
            }),
            Some(isolate_id),
            Some(generation),
            true,
            inner_reply_tx,
            PendingReplyKind::Normal,
            event_tx,
        );
        let event_tx = event_tx.clone();
        let profile = self.dynamic_profile.clone();
        tokio::spawn(async move {
            let result = match inner_reply_rx.await {
                Ok(Ok(output)) => Ok(output.body),
                Ok(Err(error)) => Err(error),
                Err(_) => Err(PlatformError::internal(
                    "dynamic host rpc response channel closed",
                )),
            };
            profile.record_provider_task_callback();
            match reply {
                TargetedHostRpcReply::Dynamic {
                    reply_id,
                    pending_replies,
                } => {
                    if let Some(delivery) = pending_replies.finish(
                        reply_id,
                        crate::ops::DynamicPendingReplyPayload::HostRpc(result),
                    ) {
                        let _ = event_tx.send(RuntimeEvent::DynamicReplyReady(delivery));
                    }
                }
                TargetedHostRpcReply::Test {
                    reply_id,
                    replies,
                    success_value,
                } => {
                    let string_result = result.map(|_| success_value);
                    let _ = event_tx.send(RuntimeEvent::TestAsyncReplyComplete {
                        reply_id,
                        replies,
                        result: string_result,
                    });
                }
            }
        });
        Ok(())
    }

    fn resolve_asset(
        &self,
        worker_name: &str,
        method: &str,
        host: Option<&str>,
        path: &str,
        headers: &[(String, String)],
    ) -> Result<Option<AssetResponse>> {
        let Some(entry) = self.workers.get(worker_name) else {
            return Ok(None);
        };
        let Some(pool) = entry.pools.get(&entry.current_generation) else {
            return Ok(None);
        };
        Ok(resolve_asset(
            &pool.assets,
            AssetRequest {
                method,
                host,
                path,
                headers,
            },
        ))
    }

    fn get_pool_mut(&mut self, worker_name: &str, generation: u64) -> Option<&mut WorkerPool> {
        self.workers
            .get_mut(worker_name)
            .and_then(|entry| entry.pools.get_mut(&generation))
    }

    fn shutdown_all(&mut self) {
        let worker_names = self.workers.keys().cloned().collect::<Vec<_>>();
        for worker_name in worker_names {
            self.reap_owned_sessions(&worker_name, None, None);
        }
        let mut clear_request_ids = Vec::new();
        for entry in self.workers.values_mut() {
            for pool in entry.pools.values_mut() {
                for isolate in pool.isolates.drain(..) {
                    let _ = isolate.sender.send(IsolateCommand::Shutdown);
                    for (request_id, pending) in isolate.pending_replies {
                        clear_request_ids.push(request_id);
                        let _ = pending
                            .reply
                            .send(Err(PlatformError::internal("runtime shutting down")));
                    }
                }
            }
        }
        for request_id in clear_request_ids {
            self.clear_revalidation_for_request(&request_id);
        }
        for (_, mut registration) in std::mem::take(&mut self.stream_registrations) {
            let error = PlatformError::internal("runtime shutting down");
            if let Some(ready) = registration.ready.take() {
                let _ = ready.send(Err(error.clone()));
            } else {
                let _ = registration.body_sender.send(Err(error));
            }
        }
        for (_, waiter) in std::mem::take(&mut self.websocket_open_waiters) {
            let _ = waiter.send(Err(PlatformError::internal("runtime shutting down")));
        }
        for (_, waiter) in std::mem::take(&mut self.transport_open_waiters) {
            let _ = waiter.send(Err(PlatformError::internal("runtime shutting down")));
        }
        for (_, waiters) in std::mem::take(&mut self.websocket_frame_waiters) {
            for waiter in waiters {
                let _ = waiter.send(Err(PlatformError::internal("runtime shutting down")));
            }
        }
        self.websocket_sessions.clear();
        self.websocket_handle_index.clear();
        self.websocket_open_handles.clear();
        self.open_handle_registry.clear();
        self.websocket_pending_closes.clear();
        self.websocket_outbound_frames.clear();
        self.websocket_close_signals.clear();
        self.transport_sessions.clear();
        self.transport_handle_index.clear();
        self.transport_open_handles.clear();
        self.transport_pending_closes.clear();
        self.transport_open_channels.clear();
        self.dynamic_worker_handles.clear();
        self.dynamic_worker_ids.clear();
        self.host_rpc_providers.clear();
    }
}

fn select_dispatch_candidate(
    pool: &mut WorkerPool,
    max_inflight: usize,
    require_wait_until_idle: bool,
) -> Option<DispatchSelection> {
    for (queue_idx, pending) in pool.queue.iter().enumerate() {
        let Some(target_isolate_id) = pending.target_isolate_id else {
            continue;
        };
        let targeted_nested_call = pending.host_rpc_call.is_some() || pending.actor_route.is_some();
        if !targeted_nested_call {
            continue;
        }
        if let Some((isolate_idx, isolate)) = pool
            .isolates
            .iter()
            .enumerate()
            .find(|(_, isolate)| isolate.id == target_isolate_id)
        {
            let actor_key = pending.actor_route.as_ref().map(ActorRoute::owner_key);
            if isolate.inflight_count < max_inflight || targeted_nested_call {
                return Some(DispatchSelection::Dispatch(DispatchCandidate {
                    queue_idx,
                    isolate_idx,
                    actor_key,
                    assign_owner: false,
                }));
            }
        } else {
            return Some(DispatchSelection::DropStaleTarget { queue_idx });
        }
    }

    for (queue_idx, pending) in pool.queue.iter().enumerate() {
        if let Some(target_isolate_id) = pending.target_isolate_id {
            if let Some((isolate_idx, isolate)) = pool
                .isolates
                .iter()
                .enumerate()
                .find(|(_, isolate)| isolate.id == target_isolate_id)
            {
                let targeted_nested_call =
                    pending.host_rpc_call.is_some() || pending.actor_route.is_some();
                let actor_key = pending.actor_route.as_ref().map(ActorRoute::owner_key);
                if (targeted_nested_call || isolate.inflight_count < max_inflight)
                    && (targeted_nested_call
                        || !require_wait_until_idle
                        || isolate.pending_wait_until.is_empty())
                {
                    return Some(DispatchSelection::Dispatch(DispatchCandidate {
                        queue_idx,
                        isolate_idx,
                        actor_key,
                        assign_owner: false,
                    }));
                }
            } else {
                return Some(DispatchSelection::DropStaleTarget { queue_idx });
            }
            continue;
        }

        let Some(route) = &pending.actor_route else {
            return least_loaded_isolate_idx(&pool.isolates, max_inflight, require_wait_until_idle)
                .map(|isolate_idx| {
                    DispatchSelection::Dispatch(DispatchCandidate {
                        queue_idx,
                        isolate_idx,
                        actor_key: None,
                        assign_owner: false,
                    })
                });
        };

        let actor_key = route.owner_key();

        if let Some(isolate_idx) =
            least_loaded_isolate_any_idx(&pool.isolates, require_wait_until_idle)
        {
            return Some(DispatchSelection::Dispatch(DispatchCandidate {
                queue_idx,
                isolate_idx,
                actor_key: Some(actor_key),
                assign_owner: false,
            }));
        }
    }
    None
}

fn host_rpc_method_blocked(method: &str) -> bool {
    let method = method.trim();
    method.is_empty()
        || method == "constructor"
        || method == "then"
        || method == "fetch"
        || method.starts_with("__dd_")
}

fn least_loaded_isolate_idx(
    isolates: &[IsolateHandle],
    max_inflight: usize,
    require_wait_until_idle: bool,
) -> Option<usize> {
    isolates
        .iter()
        .enumerate()
        .filter(|(_, isolate)| isolate.inflight_count < max_inflight)
        .filter(|(_, isolate)| !require_wait_until_idle || isolate.pending_wait_until.is_empty())
        .min_by_key(|(_, isolate)| isolate.inflight_count)
        .map(|(idx, _)| idx)
}

fn least_loaded_isolate_any_idx(
    isolates: &[IsolateHandle],
    require_wait_until_idle: bool,
) -> Option<usize> {
    isolates
        .iter()
        .enumerate()
        .filter(|(_, isolate)| !require_wait_until_idle || isolate.pending_wait_until.is_empty())
        .min_by_key(|(_, isolate)| isolate.inflight_count)
        .map(|(idx, _)| idx)
}

fn decrement_actor_inflight(actor_inflight: &mut HashMap<String, usize>, actor_key: &str) {
    let Some(current) = actor_inflight.get_mut(actor_key) else {
        return;
    };
    *current = current.saturating_sub(1);
    if *current == 0 {
        actor_inflight.remove(actor_key);
    }
}

impl WorkerPool {
    fn is_drained(&self) -> bool {
        self.queue.is_empty()
            && self.inflight_total() == 0
            && self.wait_until_total() == 0
            && self.active_websocket_total() == 0
            && self.active_transport_total() == 0
    }

    fn busy_count(&self) -> usize {
        self.isolates
            .iter()
            .filter(|isolate| {
                isolate.inflight_count > 0
                    || !isolate.pending_wait_until.is_empty()
                    || isolate.active_websocket_sessions > 0
                    || isolate.active_transport_sessions > 0
            })
            .count()
    }

    fn inflight_total(&self) -> usize {
        self.isolates
            .iter()
            .map(|isolate| isolate.inflight_count)
            .sum()
    }

    fn wait_until_total(&self) -> usize {
        self.isolates
            .iter()
            .map(|isolate| isolate.pending_wait_until.len())
            .sum()
    }

    fn active_websocket_total(&self) -> usize {
        self.isolates
            .iter()
            .map(|isolate| isolate.active_websocket_sessions)
            .sum()
    }

    fn active_transport_total(&self) -> usize {
        self.isolates
            .iter()
            .map(|isolate| isolate.active_transport_sessions)
            .sum()
    }

    fn update_queue_warning(&mut self, thresholds: &[usize]) {
        let queue_len = self.queue.len();
        let level = thresholds
            .iter()
            .take_while(|threshold| queue_len >= **threshold)
            .count();
        if level > self.queue_warn_level {
            warn!(
                worker = %self.worker_name,
                generation = self.generation,
                queued = queue_len,
                "worker queue depth crossed warning threshold"
            );
        }
        self.queue_warn_level = level;
    }

    fn stats_snapshot(&self) -> WorkerStats {
        WorkerStats {
            generation: self.generation,
            public: self.is_public,
            queued: self.queue.len(),
            busy: self.busy_count(),
            inflight_total: self.inflight_total(),
            wait_until_total: self.wait_until_total(),
            isolates_total: self.isolates.len(),
            spawn_count: self.stats.spawn_count,
            reuse_count: self.stats.reuse_count,
            scale_down_count: self.stats.scale_down_count,
        }
    }

    fn debug_dump(&self) -> WorkerDebugDump {
        let mut actor_owners = self
            .actor_owners
            .iter()
            .map(|(key, isolate_id)| (key.clone(), *isolate_id))
            .collect::<Vec<_>>();
        actor_owners.sort_by(|left, right| left.0.cmp(&right.0));

        let mut actor_inflight = self
            .actor_inflight
            .iter()
            .map(|(key, count)| (key.clone(), *count))
            .collect::<Vec<_>>();
        actor_inflight.sort_by(|left, right| left.0.cmp(&right.0));

        let queued_requests = self
            .queue
            .iter()
            .map(|pending| WorkerDebugRequest {
                runtime_request_id: pending.runtime_request_id.clone(),
                user_request_id: pending.request.request_id.clone(),
                method: pending.request.method.clone(),
                url: pending.request.url.clone(),
                actor_key: pending.actor_route.as_ref().map(ActorRoute::owner_key),
                target_isolate_id: pending.target_isolate_id,
                internal_origin: pending.internal_origin,
                reply_kind: pending.reply_kind.label().to_string(),
                host_rpc_target_id: pending
                    .host_rpc_call
                    .as_ref()
                    .map(|call| call.target_id.clone()),
                host_rpc_method: pending
                    .host_rpc_call
                    .as_ref()
                    .map(|call| call.method.clone()),
            })
            .collect::<Vec<_>>();

        let isolates = self
            .isolates
            .iter()
            .map(|isolate| {
                let mut pending_requests = isolate
                    .pending_replies
                    .iter()
                    .map(|(runtime_request_id, pending)| {
                        let meta = pending.completion_meta.as_ref();
                        WorkerDebugRequest {
                            runtime_request_id: runtime_request_id.clone(),
                            user_request_id: meta
                                .map(|meta| meta.user_request_id.clone())
                                .unwrap_or_default(),
                            method: meta.map(|meta| meta.method.clone()).unwrap_or_default(),
                            url: meta.map(|meta| meta.url.clone()).unwrap_or_default(),
                            actor_key: pending.actor_key.clone(),
                            target_isolate_id: Some(isolate.id),
                            internal_origin: pending.internal_origin,
                            reply_kind: pending.kind.label().to_string(),
                            host_rpc_target_id: None,
                            host_rpc_method: None,
                        }
                    })
                    .collect::<Vec<_>>();
                pending_requests
                    .sort_by(|left, right| left.runtime_request_id.cmp(&right.runtime_request_id));
                WorkerDebugIsolate {
                    id: isolate.id,
                    inflight_count: isolate.inflight_count,
                    pending_wait_until: isolate.pending_wait_until.len(),
                    active_websocket_sessions: isolate.active_websocket_sessions,
                    active_transport_sessions: isolate.active_transport_sessions,
                    pending_requests,
                }
            })
            .collect::<Vec<_>>();

        WorkerDebugDump {
            generation: self.generation,
            queued: self.queue.len(),
            actor_owners,
            actor_inflight,
            isolates,
            queued_requests,
        }
    }

    fn log_stats(&self, event: &str) {
        if !tracing::enabled!(Level::INFO) {
            return;
        }
        let snapshot = self.stats_snapshot();
        info!(
            worker = %self.worker_name,
            generation = snapshot.generation,
            public = snapshot.public,
            deployment_id = %self.deployment_id,
            queued = snapshot.queued,
            busy = snapshot.busy,
            inflight_total = snapshot.inflight_total,
            wait_until_total = snapshot.wait_until_total,
            isolates_total = snapshot.isolates_total,
            spawn_count = snapshot.spawn_count,
            reuse_count = snapshot.reuse_count,
            scale_down_count = snapshot.scale_down_count,
            event,
            "worker pool stats"
        );
    }
}

fn spawn_runtime_thread(
    mut receiver: mpsc::Receiver<RuntimeCommand>,
    mut cancel_receiver: mpsc::UnboundedReceiver<RuntimeCommand>,
    runtime_fast_sender: mpsc::UnboundedSender<RuntimeCommand>,
    bootstrap_snapshot: &'static [u8],
    kv_store: KvStore,
    actor_store: ActorStore,
    cache_store: CacheStore,
    config: RuntimeConfig,
    storage: RuntimeStorageConfig,
) -> Result<()> {
    thread::Builder::new()
        .name("dd-runtime".to_string())
        .spawn(move || {
            let runtime = Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime thread should build");

            runtime.block_on(async move {
                let (event_tx, mut event_rx) = mpsc::unbounded_channel();
                let mut manager = WorkerManager::new(
                    bootstrap_snapshot,
                    kv_store,
                    actor_store,
                    cache_store,
                    config.clone(),
                    storage,
                    runtime_fast_sender,
                );
                let mut ticker = tokio::time::interval(config.scale_tick);
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

                loop {
                    tokio::select! {
                        Some(command) = receiver.recv() => {
                            manager.begin_runtime_batch();
                            let keep_running = manager.handle_command(command, &event_tx).await
                                && drain_ready_runtime_work(
                                &mut manager,
                                &mut receiver,
                                &mut cancel_receiver,
                                &mut event_rx,
                                &event_tx,
                            )
                                .await;
                            manager.finish_runtime_batch(&event_tx);
                            if !keep_running {
                                break;
                            }
                        }
                        Some(command) = cancel_receiver.recv() => {
                            manager.begin_runtime_batch();
                            let keep_running = manager.handle_command(command, &event_tx).await;
                            let keep_running = keep_running
                                && drain_ready_runtime_work(
                                &mut manager,
                                &mut receiver,
                                &mut cancel_receiver,
                                &mut event_rx,
                                &event_tx,
                            )
                                .await;
                            manager.finish_runtime_batch(&event_tx);
                            if !keep_running {
                                break;
                            }
                        }
                        Some(event) = event_rx.recv() => {
                            manager.begin_runtime_batch();
                            manager.handle_event(event, &event_tx).await;
                            let keep_running = drain_ready_runtime_work(
                                &mut manager,
                                &mut receiver,
                                &mut cancel_receiver,
                                &mut event_rx,
                                &event_tx,
                            )
                            .await;
                            manager.finish_runtime_batch(&event_tx);
                            if !keep_running {
                                break;
                            }
                        }
                        _ = ticker.tick() => {
                            manager.scale_down_idle();
                        }
                        else => {
                            break;
                        }
                    }
                }

                manager.shutdown_all();
            });
        })
        .map_err(|error| PlatformError::internal(error.to_string()))?;

    Ok(())
}

async fn drain_ready_runtime_work(
    manager: &mut WorkerManager,
    receiver: &mut mpsc::Receiver<RuntimeCommand>,
    cancel_receiver: &mut mpsc::UnboundedReceiver<RuntimeCommand>,
    event_rx: &mut mpsc::UnboundedReceiver<RuntimeEvent>,
    event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
) -> bool {
    loop {
        let mut made_progress = false;
        let mut keep_running = true;

        match receiver.try_recv() {
            Ok(command) => {
                keep_running = manager.handle_command(command, event_tx).await;
                made_progress = true;
            }
            Err(TryRecvError::Empty | TryRecvError::Disconnected) => {}
        }
        if !keep_running {
            return false;
        }

        match cancel_receiver.try_recv() {
            Ok(command) => {
                keep_running = manager.handle_command(command, event_tx).await;
                made_progress = true;
            }
            Err(TryRecvError::Empty | TryRecvError::Disconnected) => {}
        }
        if !keep_running {
            return false;
        }

        match event_rx.try_recv() {
            Ok(event) => {
                manager.handle_event(event, event_tx).await;
                made_progress = true;
            }
            Err(TryRecvError::Empty | TryRecvError::Disconnected) => {}
        }

        if !made_progress {
            return true;
        }
    }
}

fn handle_isolate_event_payload(
    event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    worker_name: &str,
    generation: u64,
    isolate_id: u64,
    payload: IsolateEventPayload,
) {
    match payload {
        IsolateEventPayload::Completion(payload) => match decode_completion_payload(payload) {
            Ok((request_id, completion_token, wait_until_count, result)) => {
                let _ = event_tx.send(RuntimeEvent::RequestFinished {
                    worker_name: worker_name.to_string(),
                    generation,
                    isolate_id,
                    request_id,
                    completion_token,
                    wait_until_count,
                    result,
                });
            }
            Err(error) => {
                warn!(
                    worker = %worker_name,
                    generation,
                    isolate_id,
                    error = %error,
                    "ignoring invalid completion payload"
                );
            }
        },
        IsolateEventPayload::WaitUntilDone(payload) => match decode_wait_until_payload(payload) {
            Ok((request_id, completion_token)) => {
                let _ = event_tx.send(RuntimeEvent::WaitUntilFinished {
                    worker_name: worker_name.to_string(),
                    generation,
                    isolate_id,
                    request_id,
                    completion_token,
                });
            }
            Err(error) => {
                warn!(
                    worker = %worker_name,
                    generation,
                    isolate_id,
                    error = %error,
                    "ignoring invalid waitUntil payload"
                );
            }
        },
        IsolateEventPayload::ResponseStart(payload) => match decode_response_start_payload(payload)
        {
            Ok((request_id, completion_token, status, headers)) => {
                let _ = event_tx.send(RuntimeEvent::ResponseStart {
                    worker_name: worker_name.to_string(),
                    request_id,
                    completion_token,
                    status,
                    headers,
                });
            }
            Err(error) => {
                warn!(
                    worker = %worker_name,
                    generation,
                    isolate_id,
                    error = %error,
                    "ignoring invalid response start payload"
                );
            }
        },
        IsolateEventPayload::ResponseChunk(payload) => match decode_response_chunk_payload(payload)
        {
            Ok((request_id, completion_token, chunk)) => {
                let _ = event_tx.send(RuntimeEvent::ResponseChunk {
                    worker_name: worker_name.to_string(),
                    request_id,
                    completion_token,
                    chunk,
                });
            }
            Err(error) => {
                warn!(
                    worker = %worker_name,
                    generation,
                    isolate_id,
                    error = %error,
                    "ignoring invalid response chunk payload"
                );
            }
        },
        IsolateEventPayload::CacheRevalidate(payload) => {
            let _ = event_tx.send(RuntimeEvent::CacheRevalidate {
                worker_name: worker_name.to_string(),
                generation,
                payload,
            });
        }
        IsolateEventPayload::ActorInvoke(payload) => {
            let _ = event_tx.send(RuntimeEvent::ActorInvoke(payload));
        }
        IsolateEventPayload::ActorSocketSend(payload) => {
            let _ = event_tx.send(RuntimeEvent::ActorSocketSend(payload));
        }
        IsolateEventPayload::ActorSocketClose(payload) => {
            let _ = event_tx.send(RuntimeEvent::ActorSocketClose(payload));
        }
        IsolateEventPayload::ActorSocketConsumeClose(payload) => {
            let _ = event_tx.send(RuntimeEvent::ActorSocketConsumeClose {
                worker_name: worker_name.to_string(),
                generation,
                payload,
            });
        }
        IsolateEventPayload::ActorTransportSendStream(payload) => {
            let _ = event_tx.send(RuntimeEvent::ActorTransportSendStream(payload));
        }
        IsolateEventPayload::ActorTransportSendDatagram(payload) => {
            let _ = event_tx.send(RuntimeEvent::ActorTransportSendDatagram(payload));
        }
        IsolateEventPayload::ActorTransportRecvStream(payload) => {
            let _ = event_tx.send(RuntimeEvent::ActorTransportRecvStream(payload));
        }
        IsolateEventPayload::ActorTransportRecvDatagram(payload) => {
            let _ = event_tx.send(RuntimeEvent::ActorTransportRecvDatagram(payload));
        }
        IsolateEventPayload::ActorTransportClose(payload) => {
            let _ = event_tx.send(RuntimeEvent::ActorTransportClose(payload));
        }
        IsolateEventPayload::ActorTransportConsumeClose(payload) => {
            let _ = event_tx.send(RuntimeEvent::ActorTransportConsumeClose {
                worker_name: worker_name.to_string(),
                generation,
                payload,
            });
        }
        IsolateEventPayload::DynamicWorkerCreate(payload) => {
            let _ = event_tx.send(RuntimeEvent::DynamicWorkerCreate(payload));
        }
        IsolateEventPayload::DynamicWorkerLookup(payload) => {
            let _ = event_tx.send(RuntimeEvent::DynamicWorkerLookup(payload));
        }
        IsolateEventPayload::DynamicWorkerList(payload) => {
            let _ = event_tx.send(RuntimeEvent::DynamicWorkerList(payload));
        }
        IsolateEventPayload::DynamicWorkerDelete(payload) => {
            let _ = event_tx.send(RuntimeEvent::DynamicWorkerDelete(payload));
        }
        IsolateEventPayload::DynamicWorkerInvoke(payload) => {
            let _ = event_tx.send(RuntimeEvent::DynamicWorkerInvoke(payload));
        }
        IsolateEventPayload::DynamicHostRpcInvoke(payload) => {
            let _ = event_tx.send(RuntimeEvent::DynamicHostRpcInvoke(payload));
        }
        IsolateEventPayload::TestAsyncReply(payload) => {
            let _ = event_tx.send(RuntimeEvent::TestAsyncReply(payload));
        }
        IsolateEventPayload::TestNestedTargetedInvoke(payload) => {
            let _ = event_tx.send(RuntimeEvent::TestNestedTargetedInvoke(payload));
        }
    }
}

fn spawn_isolate_thread(
    snapshot: &'static [u8],
    snapshot_preloaded: bool,
    source: Arc<str>,
    kv_store: KvStore,
    actor_store: ActorStore,
    cache_store: CacheStore,
    open_handle_registry: crate::ops::ActorOpenHandleRegistry,
    dynamic_profile: crate::ops::DynamicProfile,
    runtime_fast_sender: mpsc::UnboundedSender<RuntimeCommand>,
    worker_name: String,
    generation: u64,
    isolate_id: u64,
    event_tx: mpsc::UnboundedSender<RuntimeEvent>,
) -> Result<IsolateHandle> {
    let (command_tx, command_rx) = std_mpsc::channel();
    let (init_tx, init_rx) = std_mpsc::channel::<Result<()>>();
    let dynamic_control_inbox = crate::ops::DynamicControlInbox::default();
    let thread_dynamic_control_inbox = dynamic_control_inbox.clone();
    let event_loop_waker = Waker::from(Arc::new(IsolateEventLoopWaker {
        sender: command_tx.clone(),
    }));
    let thread_name = format!("dd-isolate-{worker_name}-{generation}-{isolate_id}");

    thread::Builder::new()
        .name(thread_name)
        .spawn(move || {
            let runtime = match Builder::new_current_thread().enable_all().build() {
                Ok(runtime) => runtime,
                Err(error) => {
                    let _ = init_tx.send(Err(PlatformError::internal(error.to_string())));
                    return;
                }
            };

            runtime.block_on(async move {
                let mut js_runtime = match new_runtime_from_snapshot(snapshot) {
                    Ok(runtime) => runtime,
                    Err(error) => {
                        let _ = init_tx.send(Err(error));
                        return;
                    }
                };

                let (event_payload_tx, event_payload_rx) =
                    std_mpsc::channel::<IsolateEventPayload>();
                {
                    let op_state = js_runtime.op_state();
                    let mut op_state = op_state.borrow_mut();
                    op_state.put(IsolateEventSender(event_payload_tx));
                    op_state.put(kv_store.clone());
                    op_state.put(actor_store.clone());
                    op_state.put(cache_store.clone());
                    op_state.put(open_handle_registry.clone());
                    op_state.put(RequestBodyStreams::default());
                    op_state.put(crate::ops::ActorRequestScopes::default());
                    op_state.put(crate::ops::RequestSecretContexts::default());
                    op_state.put(crate::ops::DynamicPendingReplies::default());
                    op_state.put(crate::ops::TestAsyncReplies::default());
                    op_state.put(thread_dynamic_control_inbox.clone());
                    op_state.put(RuntimeFastCommandSender(runtime_fast_sender.clone()));
                    op_state.put(dynamic_profile.clone());
                }
                {
                    let event_tx = event_tx.clone();
                    let worker_name = worker_name.clone();
                    thread::Builder::new()
                        .name(format!("dd-isolate-events-{isolate_id}"))
                        .spawn(move || {
                            while let Ok(payload) = event_payload_rx.recv() {
                                handle_isolate_event_payload(
                                    &event_tx,
                                    &worker_name,
                                    generation,
                                    isolate_id,
                                    payload,
                                );
                            }
                        })
                        .expect("isolate event forwarder should spawn");
                }
                if !snapshot_preloaded {
                    if let Err(error) = load_worker(&mut js_runtime, &source).await {
                        let _ = init_tx.send(Err(error));
                        return;
                    }
                }
                let _ = init_tx.send(Ok(()));

                loop {
                    let mut made_progress = false;

                    loop {
                        match command_rx.try_recv() {
                            Ok(command) => {
                                made_progress = true;
                                match handle_isolate_command(
                                    &mut js_runtime,
                                    &event_tx,
                                    &worker_name,
                                    generation,
                                    isolate_id,
                                    command,
                                ) {
                                    Ok(true) => {}
                                    Ok(false) => return,
                                    Err(error) => {
                                        let _ = event_tx.send(RuntimeEvent::IsolateFailed {
                                            worker_name: worker_name.clone(),
                                            generation,
                                            isolate_id,
                                            error,
                                        });
                                        return;
                                    }
                                }
                            }
                            Err(std_mpsc::TryRecvError::Empty) => break,
                            Err(std_mpsc::TryRecvError::Disconnected) => return,
                        }
                    }

                    if let Err(error) = pump_event_loop_once(&mut js_runtime, &event_loop_waker) {
                        let _ = event_tx.send(RuntimeEvent::IsolateFailed {
                            worker_name: worker_name.clone(),
                            generation,
                            isolate_id,
                            error,
                        });
                        break;
                    }

                    if made_progress {
                        continue;
                    }

                    match command_rx.recv_timeout(Duration::from_millis(1)) {
                        Ok(command) => match handle_isolate_command(
                            &mut js_runtime,
                            &event_tx,
                            &worker_name,
                            generation,
                            isolate_id,
                            command,
                        ) {
                            Ok(true) => {}
                            Ok(false) => return,
                            Err(error) => {
                                let _ = event_tx.send(RuntimeEvent::IsolateFailed {
                                    worker_name: worker_name.clone(),
                                    generation,
                                    isolate_id,
                                    error,
                                });
                                return;
                            }
                        },
                        Err(std_mpsc::RecvTimeoutError::Timeout) => {}
                        Err(std_mpsc::RecvTimeoutError::Disconnected) => return,
                    }
                }
            });
        })
        .map_err(|error| PlatformError::internal(error.to_string()))?;

    match init_rx.recv_timeout(Duration::from_secs(5)) {
        Ok(Ok(())) => Ok(IsolateHandle {
            id: isolate_id,
            sender: command_tx,
            dynamic_control_inbox,
            inflight_count: 0,
            active_websocket_sessions: 0,
            active_transport_sessions: 0,
            served_requests: 0,
            last_used_at: Instant::now(),
            pending_replies: HashMap::new(),
            pending_wait_until: HashMap::new(),
        }),
        Ok(Err(error)) => Err(error),
        Err(_) => Err(PlatformError::internal("isolate startup timed out")),
    }
}

fn handle_isolate_command(
    js_runtime: &mut deno_core::JsRuntime,
    event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    worker_name: &str,
    generation: u64,
    isolate_id: u64,
    command: IsolateCommand,
) -> Result<bool> {
    match command {
        IsolateCommand::Execute {
            runtime_request_id,
            completion_token,
            worker_name_json,
            kv_bindings_json,
            kv_read_cache_config_json,
            actor_bindings_json,
            dynamic_bindings_json,
            dynamic_rpc_bindings_json,
            dynamic_env_json,
            dynamic_bindings,
            dynamic_rpc_bindings,
            secret_replacements,
            egress_allow_hosts,
            request,
            request_body,
            stream_response,
            actor_call,
            host_rpc_call,
            actor_route,
        } => {
            let request_id = request.request_id.clone();
            let has_request_body_stream = request_body.is_some();
            if let Some(request_body) = request_body {
                let op_state = js_runtime.op_state();
                let mut op_state = op_state.borrow_mut();
                register_request_body_stream(
                    &mut op_state,
                    runtime_request_id.clone(),
                    request_body,
                );
            }
            if let Some(route) = actor_route.as_ref() {
                let op_state = js_runtime.op_state();
                let mut op_state = op_state.borrow_mut();
                register_actor_request_scope(
                    &mut op_state,
                    runtime_request_id.clone(),
                    route.binding.clone(),
                    route.key.clone(),
                );
            }
            {
                let op_state = js_runtime.op_state();
                let mut op_state = op_state.borrow_mut();
                register_request_secret_context(
                    &mut op_state,
                    runtime_request_id.clone(),
                    worker_name.to_string(),
                    generation,
                    isolate_id,
                    dynamic_bindings.clone(),
                    dynamic_rpc_bindings.clone(),
                    secret_replacements,
                    egress_allow_hosts,
                );
            }
            let execute_span = if tracing::enabled!(Level::INFO) {
                let span = tracing::info_span!(
                    "runtime.isolate.execute",
                    worker.name = %worker_name,
                    worker.generation = generation,
                    isolate.id = isolate_id,
                    runtime.request_id = %runtime_request_id,
                    request.id = %request_id
                );
                set_span_parent_from_traceparent(
                    &span,
                    traceparent_from_headers(&request.headers).as_deref(),
                );
                Some(span)
            } else {
                None
            };
            let _execute_guard = execute_span.as_ref().map(|span| span.enter());
            let started_at = Instant::now();
            let dispatch_actor_call = actor_call.as_ref().map(|call| match call {
                ActorExecutionCall::Method {
                    binding,
                    key,
                    name,
                    args,
                } => ExecuteActorCall::Method {
                    binding: binding.clone(),
                    key: key.clone(),
                    name: name.clone(),
                    args: args.clone(),
                },
                ActorExecutionCall::Message {
                    binding,
                    key,
                    handle,
                    is_text,
                    data,
                    socket_handles,
                    transport_handles,
                } => ExecuteActorCall::Message {
                    binding: binding.clone(),
                    key: key.clone(),
                    handle: handle.clone(),
                    is_text: *is_text,
                    data: data.clone(),
                    socket_handles: socket_handles.clone(),
                    transport_handles: transport_handles.clone(),
                },
                ActorExecutionCall::Close {
                    binding,
                    key,
                    handle,
                    code,
                    reason,
                    socket_handles,
                    transport_handles,
                } => ExecuteActorCall::Close {
                    binding: binding.clone(),
                    key: key.clone(),
                    handle: handle.clone(),
                    code: *code,
                    reason: reason.clone(),
                    socket_handles: socket_handles.clone(),
                    transport_handles: transport_handles.clone(),
                },
                ActorExecutionCall::TransportDatagram {
                    binding,
                    key,
                    handle,
                    data,
                    socket_handles,
                    transport_handles,
                } => ExecuteActorCall::TransportDatagram {
                    binding: binding.clone(),
                    key: key.clone(),
                    handle: handle.clone(),
                    data: data.clone(),
                    socket_handles: socket_handles.clone(),
                    transport_handles: transport_handles.clone(),
                },
                ActorExecutionCall::TransportStream {
                    binding,
                    key,
                    handle,
                    data,
                    socket_handles,
                    transport_handles,
                } => ExecuteActorCall::TransportStream {
                    binding: binding.clone(),
                    key: key.clone(),
                    handle: handle.clone(),
                    data: data.clone(),
                    socket_handles: socket_handles.clone(),
                    transport_handles: transport_handles.clone(),
                },
                ActorExecutionCall::TransportClose {
                    binding,
                    key,
                    handle,
                    code,
                    reason,
                    socket_handles,
                    transport_handles,
                } => ExecuteActorCall::TransportClose {
                    binding: binding.clone(),
                    key: key.clone(),
                    handle: handle.clone(),
                    code: *code,
                    reason: reason.clone(),
                    socket_handles: socket_handles.clone(),
                    transport_handles: transport_handles.clone(),
                },
            });
            let dispatch_host_rpc_call = host_rpc_call.as_ref().map(|call| ExecuteHostRpcCall {
                target_id: call.target_id.clone(),
                method: call.method.clone(),
                args: call.args.clone(),
            });
            if let Err(error) = dispatch_worker_request(
                js_runtime,
                &runtime_request_id,
                &completion_token,
                &worker_name_json,
                &kv_bindings_json,
                &kv_read_cache_config_json,
                &actor_bindings_json,
                &dynamic_bindings_json,
                &dynamic_rpc_bindings_json,
                &dynamic_env_json,
                has_request_body_stream,
                stream_response,
                dispatch_actor_call.as_ref(),
                dispatch_host_rpc_call.as_ref(),
                request,
            ) {
                {
                    let op_state = js_runtime.op_state();
                    let mut op_state = op_state.borrow_mut();
                    clear_request_body_stream(&mut op_state, &runtime_request_id);
                    crate::ops::clear_actor_request_scope(&mut op_state, &runtime_request_id);
                    clear_request_secret_context(&mut op_state, &runtime_request_id);
                }
                tracing::warn!(
                    dispatch_ms = started_at.elapsed().as_millis() as u64,
                    error = %error,
                    "failed to dispatch request into isolate"
                );
                let _ = event_tx.send(RuntimeEvent::RequestFinished {
                    worker_name: worker_name.to_string(),
                    generation,
                    isolate_id,
                    request_id: runtime_request_id,
                    completion_token,
                    wait_until_count: 0,
                    result: Err(error),
                });
            } else {
                tracing::info!(
                    dispatch_ms = started_at.elapsed().as_millis() as u64,
                    "request dispatched into isolate event loop"
                );
            }
            Ok(true)
        }
        IsolateCommand::Abort { runtime_request_id } => {
            {
                let op_state = js_runtime.op_state();
                let mut op_state = op_state.borrow_mut();
                cancel_request_body_stream(&mut op_state, &runtime_request_id);
                clear_request_body_stream(&mut op_state, &runtime_request_id);
                crate::ops::clear_actor_request_scope(&mut op_state, &runtime_request_id);
                clear_request_secret_context(&mut op_state, &runtime_request_id);
            }
            abort_worker_request(js_runtime, &runtime_request_id)?;
            Ok(true)
        }
        IsolateCommand::DrainDynamicControl => {
            js_runtime
                .execute_script(
                    "<dd:dynamic-control-drain>",
                    "(() => {
                        const drain = globalThis.__dd_drain_dynamic_control_queue;
                        if (typeof drain !== \"function\") {
                          throw new Error(\"dynamic control drain helper missing\");
                        }
                        void Promise.resolve(drain()).catch(() => undefined);
                      })()",
                )
                .map_err(|error| PlatformError::internal(error.to_string()))?;
            {
                let op_state = js_runtime.op_state();
                op_state.borrow().waker.wake();
            }
            Ok(true)
        }
        IsolateCommand::PollEventLoop => Ok(true),
        IsolateCommand::Shutdown => Ok(false),
    }
}

async fn persist_worker_deployment(
    storage: &RuntimeStorageConfig,
    worker_name: &str,
    source: &str,
    config: &DeployConfig,
    assets: &[DeployAsset],
    asset_headers: Option<&str>,
    deployment_id: &str,
) -> Result<()> {
    if !storage.worker_store_enabled {
        return Ok(());
    }

    let workers_dir = storage.store_dir.join("workers");
    tokio::fs::create_dir_all(&workers_dir)
        .await
        .map_err(|error| {
            PlatformError::internal(format!(
                "failed to create worker store directory {}: {error}",
                workers_dir.display()
            ))
        })?;
    let final_path = workers_dir.join(format!("{}.json", encoded_worker_name(worker_name)));
    let temp_path = workers_dir.join(format!("{}.tmp", encoded_worker_name(worker_name)));
    let payload = StoredWorkerDeployment {
        name: worker_name.to_string(),
        source: source.to_string(),
        config: config.clone(),
        assets: assets.to_vec(),
        asset_headers: asset_headers.map(str::to_string),
        deployment_id: deployment_id.to_string(),
        updated_at_ms: epoch_ms_i64()?,
    };
    let body = crate::json::to_vec(&payload).map_err(|error| {
        PlatformError::internal(format!("failed to serialize worker deployment: {error}"))
    })?;
    tokio::fs::write(&temp_path, body).await.map_err(|error| {
        PlatformError::internal(format!(
            "failed to write worker store file {}: {error}",
            temp_path.display()
        ))
    })?;
    tokio::fs::rename(&temp_path, &final_path)
        .await
        .map_err(|error| {
            PlatformError::internal(format!(
                "failed to commit worker store file {}: {error}",
                final_path.display()
            ))
        })?;
    Ok(())
}

fn encoded_worker_name(worker_name: &str) -> String {
    let mut out = String::with_capacity(worker_name.len().saturating_mul(2).max(2));
    for byte in worker_name.as_bytes() {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{byte:02x}");
    }
    if out.is_empty() {
        "00".to_string()
    } else {
        out
    }
}

fn epoch_ms_i64() -> Result<i64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| PlatformError::internal(format!("system clock error: {error}")))?;
    Ok(duration.as_millis() as i64)
}

fn decode_completion_payload(
    payload: String,
) -> Result<(String, String, usize, Result<WorkerOutput>)> {
    let completion: CompletionPayload = crate::json::from_string(payload)
        .map_err(|error| PlatformError::runtime(format!("invalid completion payload: {error}")))?;
    if completion.ok {
        let output = completion
            .result
            .ok_or_else(|| PlatformError::runtime("completion is missing result"))?;
        Ok((
            completion.request_id,
            completion.completion_token,
            completion.wait_until_count,
            Ok(output),
        ))
    } else {
        let message = completion
            .error
            .unwrap_or_else(|| "worker execution failed".to_string());
        Ok((
            completion.request_id,
            completion.completion_token,
            completion.wait_until_count,
            Err(PlatformError::runtime(message)),
        ))
    }
}

fn decode_wait_until_payload(payload: String) -> Result<(String, String)> {
    let done: WaitUntilPayload = crate::json::from_string(payload)
        .map_err(|error| PlatformError::runtime(format!("invalid waitUntil payload: {error}")))?;
    Ok((done.request_id, done.completion_token))
}

fn decode_response_start_payload(
    payload: String,
) -> Result<(String, String, u16, Vec<(String, String)>)> {
    let start: ResponseStartPayload = crate::json::from_string(payload).map_err(|error| {
        PlatformError::runtime(format!("invalid response start payload: {error}"))
    })?;
    Ok((
        start.request_id,
        start.completion_token,
        start.status,
        start.headers,
    ))
}

fn decode_response_chunk_payload(payload: String) -> Result<(String, String, Vec<u8>)> {
    let chunk: ResponseChunkPayload = crate::json::from_string(payload).map_err(|error| {
        PlatformError::runtime(format!("invalid response chunk payload: {error}"))
    })?;
    Ok((chunk.request_id, chunk.completion_token, chunk.chunk))
}

fn decode_cache_revalidate_payload(payload: String) -> Result<CacheRevalidatePayload> {
    let request: CacheRevalidatePayload = crate::json::from_string(payload).map_err(|error| {
        PlatformError::runtime(format!("invalid cache revalidate payload: {error}"))
    })?;
    if request.cache_name.trim().is_empty() {
        return Err(PlatformError::runtime(
            "cache revalidate payload is missing cache_name",
        ));
    }
    if request.method.trim().is_empty() {
        return Err(PlatformError::runtime(
            "cache revalidate payload is missing method",
        ));
    }
    if request.url.trim().is_empty() {
        return Err(PlatformError::runtime(
            "cache revalidate payload is missing url",
        ));
    }
    Ok(request)
}

fn cache_revalidation_key(
    worker_name: &str,
    generation: u64,
    payload: &CacheRevalidatePayload,
) -> String {
    let mut headers: Vec<(String, String)> = payload
        .headers
        .iter()
        .map(|(name, value)| (name.to_ascii_lowercase(), value.clone()))
        .collect();
    headers.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
    let header_key = crate::json::to_string(&headers).unwrap_or_default();
    format!(
        "{worker_name}:{generation}:{}:{}:{}:{header_key}",
        payload.cache_name.trim(),
        payload.method.trim().to_ascii_uppercase(),
        payload.url.trim()
    )
}

fn append_internal_trace_headers(
    headers: &mut Vec<(String, String)>,
    worker: &str,
    generation: u64,
) {
    append_or_update_header(headers, INTERNAL_HEADER, "1");
    append_or_update_header(headers, INTERNAL_REASON_HEADER, "trace");
    append_or_update_header(headers, TRACE_SOURCE_WORKER_HEADER, worker);
    append_or_update_header(
        headers,
        TRACE_SOURCE_GENERATION_HEADER,
        generation.to_string().as_str(),
    );
}

fn append_or_update_header(headers: &mut Vec<(String, String)>, key: &str, value: &str) {
    headers.retain(|(name, _)| !name.eq_ignore_ascii_case(key));
    headers.push((key.to_string(), value.to_string()));
}

fn actor_owner_key(binding: &str, key: &str) -> String {
    format!("{binding}\u{001f}{key}")
}

fn actor_handle_key(binding: &str, key: &str, handle: &str) -> String {
    format!("{binding}\u{001f}{key}\u{001f}{handle}")
}

fn internal_header_value(headers: &[(String, String)], key: &str) -> Option<String> {
    headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(key))
        .map(|(_, value)| value.clone())
}

fn parse_websocket_open_metadata(
    output: &WorkerOutput,
    expected_session_id: &str,
) -> Result<(String, String, String)> {
    if output.status != 101 {
        return Err(PlatformError::bad_request(
            "websocket upgrade rejected by worker",
        ));
    }
    let accepted = internal_header_value(&output.headers, INTERNAL_WS_ACCEPT_HEADER)
        .map(|value| value == "1")
        .unwrap_or(false);
    if !accepted {
        return Err(PlatformError::bad_request(
            "worker did not accept websocket request",
        ));
    }
    let handle = internal_header_value(&output.headers, INTERNAL_WS_HANDLE_HEADER)
        .unwrap_or_else(|| expected_session_id.to_string());
    let binding = internal_header_value(&output.headers, INTERNAL_WS_BINDING_HEADER)
        .ok_or_else(|| PlatformError::bad_request("missing websocket memory binding metadata"))?;
    let key = internal_header_value(&output.headers, INTERNAL_WS_KEY_HEADER)
        .ok_or_else(|| PlatformError::bad_request("missing websocket memory key metadata"))?;
    if let Some(session_id) = internal_header_value(&output.headers, INTERNAL_WS_SESSION_HEADER) {
        if session_id != expected_session_id {
            return Err(PlatformError::bad_request(
                "websocket session metadata mismatch",
            ));
        }
    }
    Ok((handle, binding, key))
}

fn strip_websocket_open_internal_headers(headers: &[(String, String)]) -> Vec<(String, String)> {
    headers
        .iter()
        .filter(|(name, _)| !name.eq_ignore_ascii_case(INTERNAL_WS_ACCEPT_HEADER))
        .filter(|(name, _)| !name.eq_ignore_ascii_case(INTERNAL_WS_SESSION_HEADER))
        .filter(|(name, _)| !name.eq_ignore_ascii_case(INTERNAL_WS_HANDLE_HEADER))
        .filter(|(name, _)| !name.eq_ignore_ascii_case(INTERNAL_WS_BINDING_HEADER))
        .filter(|(name, _)| !name.eq_ignore_ascii_case(INTERNAL_WS_KEY_HEADER))
        .cloned()
        .collect()
}

fn strip_websocket_frame_internal_headers(headers: &[(String, String)]) -> Vec<(String, String)> {
    headers
        .iter()
        .filter(|(name, _)| !name.eq_ignore_ascii_case(INTERNAL_WS_ACCEPT_HEADER))
        .filter(|(name, _)| !name.eq_ignore_ascii_case(INTERNAL_WS_SESSION_HEADER))
        .filter(|(name, _)| !name.eq_ignore_ascii_case(INTERNAL_WS_HANDLE_HEADER))
        .filter(|(name, _)| !name.eq_ignore_ascii_case(INTERNAL_WS_BINDING_HEADER))
        .filter(|(name, _)| !name.eq_ignore_ascii_case(INTERNAL_WS_KEY_HEADER))
        .cloned()
        .collect()
}

fn parse_transport_open_metadata(
    output: &WorkerOutput,
    expected_session_id: &str,
) -> Result<(String, String, String)> {
    if output.status != 200 {
        return Err(PlatformError::bad_request(
            "transport connect rejected by worker",
        ));
    }
    let accepted = internal_header_value(&output.headers, INTERNAL_TRANSPORT_ACCEPT_HEADER)
        .map(|value| value == "1")
        .unwrap_or(false);
    if !accepted {
        return Err(PlatformError::bad_request(
            "worker did not accept transport request",
        ));
    }
    let handle = internal_header_value(&output.headers, INTERNAL_TRANSPORT_HANDLE_HEADER)
        .unwrap_or_else(|| expected_session_id.to_string());
    let binding = internal_header_value(&output.headers, INTERNAL_TRANSPORT_BINDING_HEADER)
        .ok_or_else(|| PlatformError::bad_request("missing transport memory binding metadata"))?;
    let key = internal_header_value(&output.headers, INTERNAL_TRANSPORT_KEY_HEADER)
        .ok_or_else(|| PlatformError::bad_request("missing transport memory key metadata"))?;
    if let Some(session_id) =
        internal_header_value(&output.headers, INTERNAL_TRANSPORT_SESSION_HEADER)
    {
        if session_id != expected_session_id {
            return Err(PlatformError::bad_request(
                "transport session metadata mismatch",
            ));
        }
    }
    Ok((handle, binding, key))
}

fn strip_transport_open_internal_headers(headers: &[(String, String)]) -> Vec<(String, String)> {
    headers
        .iter()
        .filter(|(name, _)| !name.eq_ignore_ascii_case(INTERNAL_TRANSPORT_ACCEPT_HEADER))
        .filter(|(name, _)| !name.eq_ignore_ascii_case(INTERNAL_TRANSPORT_SESSION_HEADER))
        .filter(|(name, _)| !name.eq_ignore_ascii_case(INTERNAL_TRANSPORT_HANDLE_HEADER))
        .filter(|(name, _)| !name.eq_ignore_ascii_case(INTERNAL_TRANSPORT_BINDING_HEADER))
        .filter(|(name, _)| !name.eq_ignore_ascii_case(INTERNAL_TRANSPORT_KEY_HEADER))
        .filter(|(name, _)| !name.eq_ignore_ascii_case(INTERNAL_TRANSPORT_CLOSE_CODE_HEADER))
        .filter(|(name, _)| !name.eq_ignore_ascii_case(INTERNAL_TRANSPORT_CLOSE_REASON_HEADER))
        .cloned()
        .collect()
}

fn normalize_trace_path(path: &str) -> String {
    let trimmed = path.trim();
    if trimmed.starts_with('/') {
        trimmed.to_string()
    } else {
        format!("/{trimmed}")
    }
}

fn traceparent_from_headers(headers: &[(String, String)]) -> Option<String> {
    headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("traceparent"))
        .map(|(_, value)| value.clone())
}

fn set_span_parent_from_traceparent(span: &tracing::Span, traceparent: Option<&str>) {
    let Some(traceparent) = traceparent.filter(|value| !value.trim().is_empty()) else {
        return;
    };
    global::get_text_map_propagator(|propagator| {
        let extractor = TraceparentExtractor(traceparent);
        let parent = propagator.extract(&extractor);
        if parent.span().span_context().is_valid() {
            span.set_parent(parent);
        }
    });
}

struct TraceparentExtractor<'a>(&'a str);

impl Extractor for TraceparentExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        if key.eq_ignore_ascii_case("traceparent") {
            Some(self.0)
        } else {
            None
        }
    }

    fn keys(&self) -> Vec<&str> {
        vec!["traceparent"]
    }
}

fn next_runtime_token(prefix: &str) -> String {
    format!(
        "{prefix}-{:x}",
        NEXT_RUNTIME_TOKEN.fetch_add(1, Ordering::Relaxed)
    )
}

#[cfg(test)]
mod tests;
