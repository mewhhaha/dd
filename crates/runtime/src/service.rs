mod config;
mod debug;
mod dispatch;
mod dynamic;
mod lifecycle;
mod protocol;
mod runtime;
mod sessions;
mod storage;

use crate::blob::{BlobStore, BlobStoreConfig};
use crate::cache::{CacheConfig, CacheLookup, CacheRequest, CacheResponse, CacheStore};
use crate::engine::{
    abort_worker_request, build_bootstrap_snapshot, build_worker_snapshot, dispatch_worker_request,
    ensure_v8_flags, load_worker, new_runtime_from_snapshot, pump_event_loop_once,
    validate_loaded_worker_runtime, validate_worker, ExecuteHostRpcCall, ExecuteMemoryCall,
};
use crate::kv::KvStore;
use crate::memory::MemoryStore;
use crate::memory_rpc::{
    decode_memory_invoke_request, encode_memory_invoke_response, MemoryInvokeCall,
    MemoryInvokeResponse,
};
use crate::ops::{
    cancel_request_body_stream, clear_request_body_stream, clear_request_secret_context,
    register_memory_request_scope, register_request_body_stream, register_request_secret_context,
    IsolateEventPayload, IsolateEventSender, MemoryInvokeEvent, RequestBodyStreams,
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
use self::protocol::*;
use self::runtime::*;
use self::storage::{epoch_ms_i64, persist_worker_deployment};

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
const MEMORY_ATOMIC_METHOD: &str = "__dd_atomic";

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
    pub memory_profile_enabled: bool,
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
            memory_profile_enabled: false,
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
    pub memory_owners: Vec<(String, u64)>,
    pub memory_inflight: Vec<(String, usize)>,
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
    memory_store: MemoryStore,
    cache_store: CacheStore,
    workers: HashMap<String, WorkerEntry>,
    pre_canceled: HashMap<String, HashSet<String>>,
    stream_registrations: HashMap<String, StreamRegistration>,
    revalidation_keys: HashSet<String>,
    revalidation_requests: HashMap<String, String>,
    websocket_sessions: HashMap<String, WorkerWebSocketSession>,
    websocket_handle_index: HashMap<String, String>,
    websocket_open_handles: HashMap<String, HashSet<String>>,
    open_handle_registry: crate::ops::MemoryOpenHandleRegistry,
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
    memory_bindings: Vec<String>,
    memory_bindings_json: Arc<str>,
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
    memory_owners: HashMap<String, u64>,
    memory_inflight: HashMap<String, usize>,
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
    memory_route: Option<MemoryRoute>,
    memory_call: Option<MemoryExecutionCall>,
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
    memory_key: Option<String>,
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
struct MemoryRoute {
    binding: String,
    key: String,
}

impl MemoryRoute {
    fn owner_key(&self) -> String {
        format!("{}\u{001f}{}", self.binding, self.key)
    }
}

struct DispatchCandidate {
    queue_idx: usize,
    isolate_idx: usize,
    memory_key: Option<String>,
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
        memory_bindings_json: Arc<str>,
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
        memory_call: Option<MemoryExecutionCall>,
        host_rpc_call: Option<HostRpcExecutionCall>,
        memory_route: Option<MemoryRoute>,
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
enum MemoryExecutionCall {
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
    MemoryInvoke(MemoryInvokeEvent),
    MemorySocketSend(crate::ops::MemorySocketSendEvent),
    MemorySocketClose(crate::ops::MemorySocketCloseEvent),
    MemorySocketConsumeClose {
        worker_name: String,
        generation: u64,
        payload: crate::ops::MemorySocketConsumeCloseEvent,
    },
    MemoryTransportSendStream(crate::ops::MemoryTransportSendStreamEvent),
    MemoryTransportSendDatagram(crate::ops::MemoryTransportSendDatagramEvent),
    MemoryTransportRecvStream(crate::ops::MemoryTransportRecvStreamEvent),
    MemoryTransportRecvDatagram(crate::ops::MemoryTransportRecvDatagramEvent),
    MemoryTransportClose(crate::ops::MemoryTransportCloseEvent),
    MemoryTransportConsumeClose {
        worker_name: String,
        generation: u64,
        payload: crate::ops::MemoryTransportConsumeCloseEvent,
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
        let (cancel_sender, cancel_receiver) = mpsc::unbounded_channel();
        spawn_runtime_thread(
            receiver,
            cancel_receiver,
            cancel_sender.clone(),
            bootstrap_snapshot,
            kv_store,
            memory_store,
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
        memory_store: MemoryStore,
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
            memory_store,
            cache_store,
            workers: HashMap::new(),
            pre_canceled: HashMap::new(),
            stream_registrations: HashMap::new(),
            revalidation_keys: HashSet::new(),
            revalidation_requests: HashMap::new(),
            websocket_sessions: HashMap::new(),
            websocket_handle_index: HashMap::new(),
            websocket_open_handles: HashMap::new(),
            open_handle_registry: crate::ops::MemoryOpenHandleRegistry::default(),
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
            RuntimeEvent::MemoryInvoke(payload) => {
                self.enqueue_memory_invoke(payload, event_tx);
            }
            RuntimeEvent::MemorySocketSend(payload) => {
                self.handle_memory_socket_send(payload, event_tx);
            }
            RuntimeEvent::MemorySocketClose(payload) => {
                self.handle_memory_socket_close(payload, event_tx);
            }
            RuntimeEvent::MemorySocketConsumeClose {
                worker_name: _worker_name,
                generation: _generation,
                payload,
            } => {
                self.handle_memory_socket_consume_close(payload, event_tx);
            }
            RuntimeEvent::MemoryTransportSendStream(payload) => {
                self.handle_memory_transport_send_stream(payload, event_tx);
            }
            RuntimeEvent::MemoryTransportSendDatagram(payload) => {
                self.handle_memory_transport_send_datagram(payload, event_tx);
            }
            RuntimeEvent::MemoryTransportRecvStream(payload) => {
                self.handle_memory_transport_recv_stream(payload, event_tx);
            }
            RuntimeEvent::MemoryTransportRecvDatagram(payload) => {
                self.handle_memory_transport_recv_datagram(payload, event_tx);
            }
            RuntimeEvent::MemoryTransportClose(payload) => {
                self.handle_memory_transport_close(payload, event_tx);
            }
            RuntimeEvent::MemoryTransportConsumeClose {
                worker_name: _worker_name,
                generation: _generation,
                payload,
            } => {
                self.handle_memory_transport_consume_close(payload, event_tx);
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
        let route = MemoryRoute {
            binding: session.binding.clone(),
            key: session.key.clone(),
        };
        let socket_handles =
            self.websocket_handles_snapshot(&session.binding, &session.key, Some(&session.handle));
        let transport_handles =
            self.transport_handles_snapshot(&session.binding, &session.key, None);
        let memory_call = MemoryExecutionCall::Message {
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
            url: format!("http://memory/__dd_socket/{session_id}"),
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
            Some(memory_call),
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
        let route = MemoryRoute {
            binding: session.binding.clone(),
            key: session.key.clone(),
        };
        let socket_handles =
            self.websocket_handles_snapshot(&session.binding, &session.key, Some(&session.handle));
        let transport_handles =
            self.transport_handles_snapshot(&session.binding, &session.key, None);
        let memory_call = MemoryExecutionCall::Close {
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
            url: format!("http://memory/__dd_socket_close/{session_id}"),
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
            Some(memory_call),
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

#[cfg(test)]
mod tests;
