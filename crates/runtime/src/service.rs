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
use common::{DeployBinding, DeployConfig, PlatformError, Result, WorkerInvocation, WorkerOutput};
use opentelemetry::global;
use opentelemetry::propagation::Extractor;
use opentelemetry::trace::TraceContextExt;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::mem;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc as std_mpsc;
use std::sync::{Arc, Once};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::runtime::Builder;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::{mpsc, oneshot};
use tracing::{info, warn, Level};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use uuid::Uuid;

const INTERNAL_HEADER: &str = "x-dd-internal";
const INTERNAL_REASON_HEADER: &str = "x-dd-internal-reason";
const TRACE_SOURCE_WORKER_HEADER: &str = "x-dd-trace-source-worker";
const TRACE_SOURCE_GENERATION_HEADER: &str = "x-dd-trace-source-generation";
const INTERNAL_WS_ACCEPT_HEADER: &str = "x-dd-ws-accept";
const INTERNAL_WS_SESSION_HEADER: &str = "x-dd-ws-session";
const INTERNAL_WS_HANDLE_HEADER: &str = "x-dd-ws-handle";
const INTERNAL_WS_BINDING_HEADER: &str = "x-dd-ws-actor-binding";
const INTERNAL_WS_KEY_HEADER: &str = "x-dd-ws-actor-key";
const INTERNAL_WS_BINARY_HEADER: &str = "x-dd-ws-binary";
const INTERNAL_WS_CLOSE_CODE_HEADER: &str = "x-dd-ws-close-code";
const INTERNAL_WS_CLOSE_REASON_HEADER: &str = "x-dd-ws-close-reason";
const INTERNAL_TRANSPORT_ACCEPT_HEADER: &str = "x-dd-transport-accept";
const INTERNAL_TRANSPORT_SESSION_HEADER: &str = "x-dd-transport-session";
const INTERNAL_TRANSPORT_HANDLE_HEADER: &str = "x-dd-transport-handle";
const INTERNAL_TRANSPORT_BINDING_HEADER: &str = "x-dd-transport-actor-binding";
const INTERNAL_TRANSPORT_KEY_HEADER: &str = "x-dd-transport-actor-key";
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
    pub v8_flags: Vec<String>,
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
            v8_flags: Vec::new(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct RuntimeStorageConfig {
    pub store_dir: PathBuf,
    pub database_url: String,
    pub actor_shards_per_namespace: usize,
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
            actor_shards_per_namespace: 64,
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

enum RuntimeCommand {
    Deploy {
        worker_name: String,
        source: String,
        config: DeployConfig,
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
    timeout: u64,
}

#[derive(Clone)]
struct HostRpcProvider {
    owner_worker: String,
    owner_generation: u64,
    owner_isolate_id: u64,
    target_id: String,
    methods: HashSet<String>,
}

#[derive(Clone)]
struct WorkerWebSocketSession {
    worker_name: String,
    generation: u64,
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
    actor_bindings: Vec<String>,
    actor_bindings_json: Arc<str>,
    dynamic_bindings: Vec<String>,
    dynamic_bindings_json: Arc<str>,
    dynamic_rpc_bindings: Vec<DynamicRpcBinding>,
    dynamic_rpc_bindings_json: Arc<str>,
    dynamic_env_json: Arc<str>,
    secret_replacements: Vec<(String, String)>,
    egress_allow_hosts: Vec<String>,
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
    WebsocketOpen { session_id: String },
    WebsocketFrame { session_id: String },
    TransportOpen { session_id: String },
}

impl Default for PendingReplyKind {
    fn default() -> Self {
        Self::Normal
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
    Shutdown,
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
    },
    Close {
        binding: String,
        key: String,
        handle: String,
        code: u16,
        reason: String,
    },
    TransportDatagram {
        binding: String,
        key: String,
        handle: String,
        data: Vec<u8>,
    },
    TransportStream {
        binding: String,
        key: String,
        handle: String,
        data: Vec<u8>,
    },
    TransportClose {
        binding: String,
        key: String,
        handle: String,
        code: u16,
        reason: String,
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
    ActorSocketList {
        worker_name: String,
        generation: u64,
        payload: crate::ops::ActorSocketListEvent,
    },
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
    ActorTransportList {
        worker_name: String,
        generation: u64,
        payload: crate::ops::ActorTransportListEvent,
    },
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
    deployment_id: String,
    updated_at_ms: i64,
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
        if storage.actor_shards_per_namespace == 0 {
            return Err(PlatformError::internal(
                "actor_shards_per_namespace must be greater than 0",
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
        let actor_store = ActorStore::new(
            storage.store_dir.join("actors"),
            storage.actor_shards_per_namespace,
        )
        .await?;
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
        self.deploy_with_config(worker_name, source, DeployConfig::default())
            .await
    }

    pub async fn deploy_with_config(
        &self,
        worker_name: String,
        source: String,
        config: DeployConfig,
    ) -> Result<String> {
        self.deploy_with_config_internal(worker_name, source, config, true)
            .await
    }

    async fn deploy_with_config_internal(
        &self,
        worker_name: String,
        source: String,
        config: DeployConfig,
        persist: bool,
    ) -> Result<String> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.sender
            .send(RuntimeCommand::Deploy {
                worker_name,
                source,
                config,
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
    ) -> Self {
        Self {
            config,
            storage,
            bootstrap_snapshot,
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
    ) {
        match command {
            RuntimeCommand::Deploy {
                worker_name,
                source,
                config,
                persist,
                reply,
            } => {
                let result = self.deploy(worker_name, source, config, persist).await;
                let _ = reply.send(result);
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
                    return;
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
            }
            RuntimeCommand::WaitWebsocketFrame {
                worker_name,
                session_id,
                reply,
            } => {
                self.wait_websocket_frame(&worker_name, &session_id, reply);
            }
            RuntimeCommand::DrainWebsocketFrame {
                worker_name,
                session_id,
                reply,
            } => {
                let result = self.drain_websocket_frame(&worker_name, &session_id);
                let _ = reply.send(result);
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
                    return;
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
            }
            RuntimeCommand::RegisterStream {
                worker_name,
                runtime_request_id,
                ready,
            } => {
                self.register_stream(worker_name, runtime_request_id, ready);
            }
            RuntimeCommand::Cancel {
                worker_name,
                runtime_request_id,
            } => {
                self.cancel_invoke(worker_name, runtime_request_id, event_tx);
            }
            RuntimeCommand::Stats { worker_name, reply } => {
                let _ = reply.send(self.worker_stats(&worker_name));
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
            RuntimeEvent::ActorSocketList {
                worker_name: _worker_name,
                generation: _generation,
                payload,
            } => {
                self.handle_actor_socket_list(payload, event_tx);
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
            RuntimeEvent::ActorTransportList {
                worker_name: _worker_name,
                generation: _generation,
                payload,
            } => {
                self.handle_actor_transport_list(payload, event_tx);
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
        let actor_call = ActorExecutionCall::Message {
            binding: session.binding.clone(),
            key: session.key.clone(),
            handle: session.handle.clone(),
            is_text: !is_binary,
            data: frame,
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
        let actor_call = ActorExecutionCall::Close {
            binding: session.binding.clone(),
            key: session.key.clone(),
            handle: session.handle.clone(),
            code: close_code,
            reason: close_reason,
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
        tokio::spawn(async move {
            let _ = receiver.await;
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

    fn complete_websocket_open(
        &mut self,
        worker_name: &str,
        generation: u64,
        isolate_id: u64,
        session_id: String,
        result: Result<WorkerOutput>,
    ) {
        let Some(waiter) = self.websocket_open_waiters.remove(&session_id) else {
            warn!(
                worker = %worker_name,
                generation,
                isolate_id,
                session_id,
                "missing websocket open waiter"
            );
            return;
        };
        let output = match result {
            Ok(output) => output,
            Err(error) => {
                let _ = waiter.send(Err(error));
                return;
            }
        };
        let (handle, binding, key) = match parse_websocket_open_metadata(&output, &session_id) {
            Ok(values) => values,
            Err(error) => {
                let _ = waiter.send(Err(error));
                return;
            }
        };
        if let Err(error) = self.register_websocket_session(
            worker_name,
            generation,
            isolate_id,
            &session_id,
            &binding,
            &key,
            &handle,
        ) {
            let _ = waiter.send(Err(error));
            return;
        }
        let mut output = output;
        output.headers = strip_websocket_open_internal_headers(&output.headers);
        let _ = waiter.send(Ok(WebSocketOpen {
            session_id,
            worker_name: worker_name.to_string(),
            output,
        }));
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

    fn register_websocket_session(
        &mut self,
        worker_name: &str,
        generation: u64,
        _isolate_id: u64,
        session_id: &str,
        binding: &str,
        key: &str,
        handle: &str,
    ) -> Result<()> {
        if self.get_pool_mut(worker_name, generation).is_none() {
            return Err(PlatformError::not_found("worker pool missing"));
        }

        if self.websocket_sessions.contains_key(session_id) {
            let _ = self.unregister_websocket_session(session_id);
        }

        let session = WorkerWebSocketSession {
            worker_name: worker_name.to_string(),
            generation,
            binding: binding.to_string(),
            key: key.to_string(),
            handle: handle.to_string(),
        };
        self.websocket_sessions
            .insert(session_id.to_string(), session.clone());
        self.websocket_handle_index.insert(
            actor_handle_key(&session.binding, &session.key, &session.handle),
            session_id.to_string(),
        );
        self.websocket_open_handles
            .entry(actor_owner_key(binding, key))
            .or_default()
            .insert(handle.to_string());
        Ok(())
    }

    fn unregister_websocket_session(&mut self, session_id: &str) -> Option<WorkerWebSocketSession> {
        let session = self.websocket_sessions.remove(session_id)?;
        self.fail_websocket_frame_waiters(
            session_id,
            PlatformError::not_found("websocket session not found"),
        );
        self.websocket_handle_index.remove(&actor_handle_key(
            &session.binding,
            &session.key,
            &session.handle,
        ));
        self.websocket_outbound_frames.remove(session_id);
        self.websocket_close_signals.remove(session_id);

        let owner_key = actor_owner_key(&session.binding, &session.key);
        let remove_owner_key =
            if let Some(handles) = self.websocket_open_handles.get_mut(&owner_key) {
                handles.remove(&session.handle);
                handles.is_empty()
            } else {
                false
            };
        if remove_owner_key {
            self.websocket_open_handles.remove(&owner_key);
        }

        Some(session)
    }

    fn register_transport_session(
        &mut self,
        worker_name: &str,
        generation: u64,
        _isolate_id: u64,
        session_id: &str,
        binding: &str,
        key: &str,
        handle: &str,
        stream_sender: mpsc::UnboundedSender<Vec<u8>>,
        datagram_sender: mpsc::UnboundedSender<Vec<u8>>,
    ) -> Result<()> {
        if self.get_pool_mut(worker_name, generation).is_none() {
            return Err(PlatformError::not_found("worker pool missing"));
        }

        if self.transport_sessions.contains_key(session_id) {
            let _ = self.unregister_transport_session(session_id);
        }

        let session = WorkerTransportSession {
            worker_name: worker_name.to_string(),
            generation,
            binding: binding.to_string(),
            key: key.to_string(),
            handle: handle.to_string(),
            stream_sender,
            datagram_sender,
            inbound_streams: VecDeque::new(),
            inbound_stream_closed: false,
            inbound_datagrams: VecDeque::new(),
        };
        self.transport_sessions
            .insert(session_id.to_string(), session.clone());
        self.transport_handle_index.insert(
            actor_handle_key(&session.binding, &session.key, &session.handle),
            session_id.to_string(),
        );
        self.transport_open_handles
            .entry(actor_owner_key(binding, key))
            .or_default()
            .insert(handle.to_string());
        Ok(())
    }

    fn unregister_transport_session(&mut self, session_id: &str) -> Option<WorkerTransportSession> {
        let session = self.transport_sessions.remove(session_id)?;
        self.transport_handle_index.remove(&actor_handle_key(
            &session.binding,
            &session.key,
            &session.handle,
        ));
        self.transport_open_channels.remove(session_id);
        self.transport_open_waiters.remove(session_id);

        let owner_key = actor_owner_key(&session.binding, &session.key);
        let remove_owner_key =
            if let Some(handles) = self.transport_open_handles.get_mut(&owner_key) {
                handles.remove(&session.handle);
                handles.is_empty()
            } else {
                false
            };
        if remove_owner_key {
            self.transport_open_handles.remove(&owner_key);
        }

        Some(session)
    }

    fn queue_transport_close_replay(
        &mut self,
        session: &WorkerTransportSession,
        close_code: u16,
        close_reason: String,
    ) {
        let owner_key = actor_owner_key(&session.binding, &session.key);
        self.transport_pending_closes
            .entry(owner_key)
            .or_default()
            .entry(session.handle.clone())
            .or_default()
            .push(TransportCloseEvent {
                code: close_code,
                reason: close_reason,
            });
    }

    fn queue_websocket_close_replay(
        &mut self,
        session: &WorkerWebSocketSession,
        close_code: u16,
        close_reason: String,
    ) {
        let owner_key = actor_owner_key(&session.binding, &session.key);
        self.websocket_pending_closes
            .entry(owner_key)
            .or_default()
            .entry(session.handle.clone())
            .or_default()
            .push(SocketCloseEvent {
                code: close_code,
                reason: close_reason,
            });
    }

    fn complete_transport_open(
        &mut self,
        worker_name: &str,
        generation: u64,
        isolate_id: u64,
        session_id: String,
        result: Result<WorkerOutput>,
    ) {
        let Some(waiter) = self.transport_open_waiters.remove(&session_id) else {
            warn!(
                worker = %worker_name,
                generation,
                isolate_id,
                session_id,
                "missing transport open waiter"
            );
            self.transport_open_channels.remove(&session_id);
            return;
        };
        let channels = self.transport_open_channels.remove(&session_id);
        let output = match result {
            Ok(output) => output,
            Err(error) => {
                let _ = waiter.send(Err(error));
                return;
            }
        };
        let (handle, binding, key) = match parse_transport_open_metadata(&output, &session_id) {
            Ok(values) => values,
            Err(error) => {
                let _ = waiter.send(Err(error));
                return;
            }
        };
        let Some(channels) = channels else {
            let _ = waiter.send(Err(PlatformError::internal(
                "missing transport open channels",
            )));
            return;
        };
        if let Err(error) = self.register_transport_session(
            worker_name,
            generation,
            isolate_id,
            &session_id,
            &binding,
            &key,
            &handle,
            channels.stream_sender,
            channels.datagram_sender,
        ) {
            let _ = waiter.send(Err(error));
            return;
        }
        let mut output = output;
        output.headers = strip_transport_open_internal_headers(&output.headers);
        let _ = waiter.send(Ok(TransportOpen {
            session_id,
            worker_name: worker_name.to_string(),
            output,
        }));
    }

    fn handle_actor_socket_send(
        &mut self,
        payload: crate::ops::ActorSocketSendEvent,
        _event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    ) {
        let crate::ops::ActorSocketSendEvent {
            reply,
            handle,
            binding,
            key,
            is_text,
            message,
        } = payload;
        let index_key = actor_handle_key(&binding, &key, &handle);
        let result = match self.websocket_handle_index.get(&index_key).cloned() {
            Some(session_id) => {
                self.websocket_outbound_frames
                    .entry(session_id.clone())
                    .or_default()
                    .push_back(WebSocketOutboundFrame {
                        is_binary: !is_text,
                        payload: message,
                    });
                self.notify_websocket_frame_waiters(&session_id);
                Ok(())
            }
            None => Err(PlatformError::not_found("websocket session not found")),
        };
        let _ = reply.send(result);
    }

    fn handle_actor_socket_close(
        &mut self,
        payload: crate::ops::ActorSocketCloseEvent,
        _event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    ) {
        let crate::ops::ActorSocketCloseEvent {
            reply,
            handle,
            binding,
            key,
            code,
            reason,
        } = payload;
        let index_key = actor_handle_key(&binding, &key, &handle);
        let result = match self.websocket_handle_index.get(&index_key).cloned() {
            Some(session_id) => {
                self.websocket_close_signals
                    .insert(session_id.clone(), SocketCloseEvent { code, reason });
                self.notify_websocket_frame_waiters(&session_id);
                Ok(())
            }
            None => Err(PlatformError::not_found("websocket session not found")),
        };
        let _ = reply.send(result);
    }

    fn handle_actor_socket_list(
        &mut self,
        payload: crate::ops::ActorSocketListEvent,
        _event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    ) {
        let owner_key = actor_owner_key(&payload.binding, &payload.key);
        let mut handles: Vec<String> = self
            .websocket_open_handles
            .get(&owner_key)
            .map(|values| values.iter().cloned().collect())
            .unwrap_or_default();
        handles.sort();
        let _ = payload.reply.send(Ok(handles));
    }

    fn handle_actor_socket_consume_close(
        &mut self,
        payload: crate::ops::ActorSocketConsumeCloseEvent,
        _event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    ) {
        let owner_key = actor_owner_key(&payload.binding, &payload.key);
        let events = self
            .websocket_pending_closes
            .get_mut(&owner_key)
            .and_then(|by_handle| by_handle.remove(&payload.handle))
            .unwrap_or_default();
        let remove_owner_key = self
            .websocket_pending_closes
            .get(&owner_key)
            .map(|by_handle| by_handle.is_empty())
            .unwrap_or(false);
        if remove_owner_key {
            self.websocket_pending_closes.remove(&owner_key);
        }
        let replay: Vec<crate::ops::ActorSocketCloseReplayEvent> = events
            .into_iter()
            .map(|event| crate::ops::ActorSocketCloseReplayEvent {
                code: event.code,
                reason: event.reason,
            })
            .collect();
        let _ = payload.reply.send(Ok(replay));
    }

    fn push_transport_stream(
        &mut self,
        worker_name: &str,
        session_id: &str,
        chunk: Vec<u8>,
        done: bool,
        event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    ) -> Result<()> {
        let _ = done;
        let Some(session) = self.transport_sessions.get(session_id).cloned() else {
            return Err(PlatformError::not_found("transport session not found"));
        };
        if session.worker_name != worker_name {
            return Err(PlatformError::bad_request(
                "transport session worker mismatch",
            ));
        }
        if chunk.is_empty() {
            return Ok(());
        }
        let runtime_request_id = Uuid::new_v4().to_string();
        let route = ActorRoute {
            binding: session.binding.clone(),
            key: session.key.clone(),
        };
        let actor_call = ActorExecutionCall::TransportStream {
            binding: session.binding.clone(),
            key: session.key.clone(),
            handle: session.handle.clone(),
            data: chunk,
        };
        let invoke = WorkerInvocation {
            method: "TRANSPORT-STREAM".to_string(),
            url: format!("http://actor/__dd_transport_stream/{session_id}"),
            headers: Vec::new(),
            body: Vec::new(),
            request_id: format!("transport-stream-{runtime_request_id}"),
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
        tokio::spawn(async move {
            let _ = receiver.await;
        });
        Ok(())
    }

    fn push_transport_datagram(
        &mut self,
        worker_name: &str,
        session_id: &str,
        datagram: Vec<u8>,
        event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    ) -> Result<()> {
        let Some(session) = self.transport_sessions.get(session_id).cloned() else {
            return Err(PlatformError::not_found("transport session not found"));
        };
        if session.worker_name != worker_name {
            return Err(PlatformError::bad_request(
                "transport session worker mismatch",
            ));
        }
        if datagram.is_empty() {
            return Ok(());
        }
        let runtime_request_id = Uuid::new_v4().to_string();
        let route = ActorRoute {
            binding: session.binding.clone(),
            key: session.key.clone(),
        };
        let actor_call = ActorExecutionCall::TransportDatagram {
            binding: session.binding.clone(),
            key: session.key.clone(),
            handle: session.handle.clone(),
            data: datagram,
        };
        let invoke = WorkerInvocation {
            method: "TRANSPORT-DATAGRAM".to_string(),
            url: format!("http://actor/__dd_transport_datagram/{session_id}"),
            headers: Vec::new(),
            body: Vec::new(),
            request_id: format!("transport-datagram-{runtime_request_id}"),
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
        tokio::spawn(async move {
            let _ = receiver.await;
        });
        Ok(())
    }

    fn close_transport(
        &mut self,
        worker_name: &str,
        session_id: &str,
        close_code: u16,
        close_reason: String,
        event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    ) -> Result<()> {
        let Some(existing) = self.transport_sessions.get(session_id) else {
            return Err(PlatformError::not_found("transport session not found"));
        };
        if existing.worker_name != worker_name {
            return Err(PlatformError::bad_request(
                "transport session worker mismatch",
            ));
        }

        let session = self
            .unregister_transport_session(session_id)
            .ok_or_else(|| PlatformError::not_found("transport session not found"))?;
        self.queue_transport_close_replay(&session, close_code, close_reason.clone());

        let runtime_request_id = Uuid::new_v4().to_string();
        let route = ActorRoute {
            binding: session.binding.clone(),
            key: session.key.clone(),
        };
        let actor_call = ActorExecutionCall::TransportClose {
            binding: session.binding.clone(),
            key: session.key.clone(),
            handle: session.handle.clone(),
            code: close_code,
            reason: close_reason,
        };
        let invoke = WorkerInvocation {
            method: "TRANSPORT-CLOSE".to_string(),
            url: format!("http://actor/__dd_transport_close/{session_id}"),
            headers: Vec::new(),
            body: Vec::new(),
            request_id: format!("transport-close-{runtime_request_id}"),
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
        tokio::spawn(async move {
            let _ = receiver.await;
        });
        Ok(())
    }

    fn handle_actor_transport_send_stream(
        &mut self,
        payload: crate::ops::ActorTransportSendStreamEvent,
        _event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    ) {
        let crate::ops::ActorTransportSendStreamEvent {
            reply,
            handle,
            binding,
            key,
            chunk,
        } = payload;
        let index_key = actor_handle_key(&binding, &key, &handle);
        let session_id = self.transport_handle_index.get(&index_key).cloned();
        let result = match session_id.as_deref() {
            Some(session_id) => match self.transport_sessions.get(session_id) {
                Some(session) => session
                    .stream_sender
                    .send(chunk)
                    .map_err(|_| PlatformError::internal("transport stream channel closed")),
                None => Err(PlatformError::not_found("transport session not found")),
            },
            None => Err(PlatformError::not_found("transport session not found")),
        };
        let _ = reply.send(result);
    }

    fn handle_actor_transport_send_datagram(
        &mut self,
        payload: crate::ops::ActorTransportSendDatagramEvent,
        _event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    ) {
        let crate::ops::ActorTransportSendDatagramEvent {
            reply,
            handle,
            binding,
            key,
            datagram,
        } = payload;
        let index_key = actor_handle_key(&binding, &key, &handle);
        let session_id = self.transport_handle_index.get(&index_key).cloned();
        let result = match session_id.as_deref() {
            Some(session_id) => match self.transport_sessions.get(session_id) {
                Some(session) => session
                    .datagram_sender
                    .send(datagram)
                    .map_err(|_| PlatformError::internal("transport datagram channel closed")),
                None => Err(PlatformError::not_found("transport session not found")),
            },
            None => Err(PlatformError::not_found("transport session not found")),
        };
        let _ = reply.send(result);
    }

    fn handle_actor_transport_recv_stream(
        &mut self,
        payload: crate::ops::ActorTransportRecvStreamEvent,
        _event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    ) {
        let crate::ops::ActorTransportRecvStreamEvent {
            reply,
            handle,
            binding,
            key,
        } = payload;
        let index_key = actor_handle_key(&binding, &key, &handle);
        let session_id = self.transport_handle_index.get(&index_key).cloned();
        let result = match session_id.as_deref() {
            Some(session_id) => match self.transport_sessions.get_mut(session_id) {
                Some(session) => {
                    let chunk = session.inbound_streams.pop_front().unwrap_or_default();
                    let done = chunk.is_empty() && session.inbound_stream_closed;
                    Ok(crate::ops::TransportRecvEvent { done, chunk })
                }
                None => Err(PlatformError::not_found("transport session not found")),
            },
            None => Err(PlatformError::not_found("transport session not found")),
        };
        let _ = reply.send(result);
    }

    fn handle_actor_transport_recv_datagram(
        &mut self,
        payload: crate::ops::ActorTransportRecvDatagramEvent,
        _event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    ) {
        let crate::ops::ActorTransportRecvDatagramEvent {
            reply,
            handle,
            binding,
            key,
        } = payload;
        let index_key = actor_handle_key(&binding, &key, &handle);
        let session_id = self.transport_handle_index.get(&index_key).cloned();
        let result = match session_id.as_deref() {
            Some(session_id) => match self.transport_sessions.get_mut(session_id) {
                Some(session) => {
                    let chunk = session.inbound_datagrams.pop_front().unwrap_or_default();
                    Ok(crate::ops::TransportRecvEvent { done: false, chunk })
                }
                None => Err(PlatformError::not_found("transport session not found")),
            },
            None => Err(PlatformError::not_found("transport session not found")),
        };
        let _ = reply.send(result);
    }

    fn handle_actor_transport_close(
        &mut self,
        payload: crate::ops::ActorTransportCloseEvent,
        _event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    ) {
        let crate::ops::ActorTransportCloseEvent {
            reply,
            handle,
            binding,
            key,
            code,
            reason,
        } = payload;
        let index_key = actor_handle_key(&binding, &key, &handle);
        let session_id = self.transport_handle_index.get(&index_key).cloned();
        let result = match session_id.as_deref() {
            Some(session_id) => {
                if let Some(session) = self.unregister_transport_session(session_id) {
                    self.queue_transport_close_replay(&session, code, reason);
                    Ok(())
                } else {
                    Err(PlatformError::not_found("transport session not found"))
                }
            }
            None => Err(PlatformError::not_found("transport session not found")),
        };
        let _ = reply.send(result);
    }

    fn handle_actor_transport_list(
        &mut self,
        payload: crate::ops::ActorTransportListEvent,
        _event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    ) {
        let owner_key = actor_owner_key(&payload.binding, &payload.key);
        let mut handles: Vec<String> = self
            .transport_open_handles
            .get(&owner_key)
            .map(|values| values.iter().cloned().collect())
            .unwrap_or_default();
        handles.sort();
        let _ = payload.reply.send(Ok(handles));
    }

    fn handle_actor_transport_consume_close(
        &mut self,
        payload: crate::ops::ActorTransportConsumeCloseEvent,
        _event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    ) {
        let owner_key = actor_owner_key(&payload.binding, &payload.key);
        let events = self
            .transport_pending_closes
            .get_mut(&owner_key)
            .and_then(|by_handle| by_handle.remove(&payload.handle))
            .unwrap_or_default();
        let remove_owner_key = self
            .transport_pending_closes
            .get(&owner_key)
            .map(|by_handle| by_handle.is_empty())
            .unwrap_or(false);
        if remove_owner_key {
            self.transport_pending_closes.remove(&owner_key);
        }
        let replay: Vec<crate::ops::ActorTransportCloseReplayEvent> = events
            .into_iter()
            .map(|event| crate::ops::ActorTransportCloseReplayEvent {
                code: event.code,
                reason: event.reason,
            })
            .collect();
        let _ = payload.reply.send(Ok(replay));
    }

    async fn handle_dynamic_worker_create(
        &mut self,
        payload: crate::ops::DynamicWorkerCreateEvent,
    ) {
        let crate::ops::DynamicWorkerCreateEvent {
            owner_worker,
            owner_generation,
            owner_isolate_id,
            binding,
            id,
            source,
            env,
            timeout,
            host_rpc_bindings,
            reply,
        } = payload;
        let normalized_id = id.trim().to_string();
        if normalized_id.is_empty() {
            let _ = reply.send(Err(PlatformError::bad_request(
                "dynamic worker id must not be empty",
            )));
            return;
        }
        let timeout = timeout.clamp(1, 60_000);
        let id_key = DynamicWorkerIdKey {
            owner_worker: owner_worker.clone(),
            owner_generation,
            binding: binding.clone(),
        };
        if let Some(handle) = self
            .dynamic_worker_ids
            .get(&id_key)
            .and_then(|by_id| by_id.get(&normalized_id))
            .cloned()
        {
            if let Some(entry) = self.dynamic_worker_handles.get(&handle) {
                let _ = reply.send(Ok(crate::ops::DynamicWorkerCreateReply {
                    handle,
                    worker_name: entry.worker_name.clone(),
                    timeout: entry.timeout,
                }));
                return;
            }
            if let Some(by_id) = self.dynamic_worker_ids.get_mut(&id_key) {
                by_id.remove(&normalized_id);
            }
        }
        let mut dynamic_rpc_bindings = Vec::new();
        let mut created_provider_ids = Vec::new();
        let mut seen_binding_names = HashSet::new();
        for binding_spec in host_rpc_bindings {
            let binding_name = binding_spec.binding.trim().to_string();
            let target_id = binding_spec.target_id.trim().to_string();
            if binding_name.is_empty() {
                let _ = reply.send(Err(PlatformError::bad_request(
                    "dynamic host rpc binding must not be empty",
                )));
                return;
            }
            if target_id.is_empty() {
                let _ = reply.send(Err(PlatformError::bad_request(
                    "dynamic host rpc target_id must not be empty",
                )));
                return;
            }
            if !seen_binding_names.insert(binding_name.clone()) {
                let _ = reply.send(Err(PlatformError::bad_request(format!(
                    "duplicate dynamic host rpc binding: {binding_name}"
                ))));
                return;
            }
            let provider_id = format!("hrpc-{}", Uuid::new_v4().simple());
            let methods = binding_spec
                .methods
                .into_iter()
                .map(|method| method.trim().to_string())
                .filter(|method| !method.is_empty())
                .collect::<HashSet<_>>();
            self.host_rpc_providers.insert(
                provider_id.clone(),
                HostRpcProvider {
                    owner_worker: owner_worker.clone(),
                    owner_generation,
                    owner_isolate_id,
                    target_id,
                    methods,
                },
            );
            created_provider_ids.push(provider_id.clone());
            dynamic_rpc_bindings.push(DynamicRpcBinding {
                binding: binding_name,
                provider_id,
            });
        }
        let result = self
            .deploy_dynamic(source, env, Vec::new(), dynamic_rpc_bindings)
            .await;
        let result = result.map(|deployed| {
            let handle = format!("dynh-{}", Uuid::new_v4().simple());
            let worker_name = deployed.worker;
            self.dynamic_worker_handles.insert(
                handle.clone(),
                DynamicWorkerHandle {
                    owner_worker,
                    owner_generation,
                    binding,
                    worker_name: worker_name.clone(),
                    timeout,
                },
            );
            self.dynamic_worker_ids
                .entry(id_key)
                .or_default()
                .insert(normalized_id, handle.clone());
            crate::ops::DynamicWorkerCreateReply {
                handle,
                worker_name,
                timeout,
            }
        });
        if result.is_err() {
            for provider_id in created_provider_ids {
                self.host_rpc_providers.remove(&provider_id);
            }
        }
        let _ = reply.send(result);
    }

    fn handle_dynamic_worker_lookup(&mut self, payload: crate::ops::DynamicWorkerLookupEvent) {
        let crate::ops::DynamicWorkerLookupEvent {
            owner_worker,
            owner_generation,
            binding,
            id,
            reply,
        } = payload;
        let key = DynamicWorkerIdKey {
            owner_worker,
            owner_generation,
            binding,
        };
        let id = id.trim().to_string();
        if id.is_empty() {
            let _ = reply.send(Err(PlatformError::bad_request(
                "dynamic worker id must not be empty",
            )));
            return;
        }
        let handle = self
            .dynamic_worker_ids
            .get(&key)
            .and_then(|by_id| by_id.get(&id))
            .cloned();
        let Some(handle) = handle else {
            let _ = reply.send(Ok(None));
            return;
        };
        let Some(entry) = self.dynamic_worker_handles.get(&handle).cloned() else {
            if let Some(by_id) = self.dynamic_worker_ids.get_mut(&key) {
                by_id.remove(&id);
            }
            let _ = reply.send(Ok(None));
            return;
        };
        let _ = reply.send(Ok(Some(crate::ops::DynamicWorkerCreateReply {
            handle,
            worker_name: entry.worker_name,
            timeout: entry.timeout,
        })));
    }

    fn handle_dynamic_worker_list(&mut self, payload: crate::ops::DynamicWorkerListEvent) {
        let crate::ops::DynamicWorkerListEvent {
            owner_worker,
            owner_generation,
            binding,
            reply,
        } = payload;
        let key = DynamicWorkerIdKey {
            owner_worker,
            owner_generation,
            binding,
        };
        let mut ids = self
            .dynamic_worker_ids
            .get(&key)
            .map(|by_id| by_id.keys().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        ids.sort();
        let _ = reply.send(Ok(ids));
    }

    fn handle_dynamic_worker_delete(&mut self, payload: crate::ops::DynamicWorkerDeleteEvent) {
        let crate::ops::DynamicWorkerDeleteEvent {
            owner_worker,
            owner_generation,
            binding,
            id,
            reply,
        } = payload;
        let key = DynamicWorkerIdKey {
            owner_worker,
            owner_generation,
            binding,
        };
        let id = id.trim().to_string();
        if id.is_empty() {
            let _ = reply.send(Err(PlatformError::bad_request(
                "dynamic worker id must not be empty",
            )));
            return;
        }
        let handle = self
            .dynamic_worker_ids
            .get_mut(&key)
            .and_then(|by_id| by_id.remove(&id));
        if self
            .dynamic_worker_ids
            .get(&key)
            .map(|by_id| by_id.is_empty())
            .unwrap_or(false)
        {
            self.dynamic_worker_ids.remove(&key);
        }
        let Some(handle) = handle else {
            let _ = reply.send(Ok(false));
            return;
        };
        let Some(entry) = self.dynamic_worker_handles.remove(&handle) else {
            let _ = reply.send(Ok(false));
            return;
        };
        self.retire_worker_completely(&entry.worker_name);
        let _ = reply.send(Ok(true));
    }

    fn handle_dynamic_worker_invoke(
        &mut self,
        payload: crate::ops::DynamicWorkerInvokeEvent,
        event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    ) {
        let crate::ops::DynamicWorkerInvokeEvent {
            owner_worker,
            owner_generation,
            binding,
            handle,
            request,
            reply,
        } = payload;
        let Some(handle_entry) = self.dynamic_worker_handles.get(&handle).cloned() else {
            let _ = reply.send(Err(PlatformError::not_found(
                "dynamic worker handle not found",
            )));
            return;
        };
        if handle_entry.owner_worker != owner_worker {
            let _ = reply.send(Err(PlatformError::bad_request(
                "dynamic worker handle owner mismatch",
            )));
            return;
        }
        if handle_entry.owner_generation != owner_generation {
            let _ = reply.send(Err(PlatformError::bad_request(
                "dynamic worker handle generation mismatch",
            )));
            return;
        }
        if handle_entry.binding != binding {
            let _ = reply.send(Err(PlatformError::bad_request(
                "dynamic worker binding mismatch",
            )));
            return;
        }

        let runtime_request_id = Uuid::new_v4().to_string();
        let (inner_reply_tx, inner_reply_rx) = oneshot::channel();
        let timeout = handle_entry.timeout;
        self.enqueue_invoke(
            handle_entry.worker_name,
            runtime_request_id,
            request,
            None,
            None,
            None,
            None,
            None,
            None,
            true,
            inner_reply_tx,
            PendingReplyKind::Normal,
            event_tx,
        );
        tokio::spawn(async move {
            let result =
                match tokio::time::timeout(Duration::from_millis(timeout), inner_reply_rx).await {
                    Ok(Ok(output)) => output,
                    Ok(Err(_)) => Err(PlatformError::internal(
                        "dynamic worker invoke response channel closed",
                    )),
                    Err(_) => Err(PlatformError::runtime(format!(
                        "dynamic worker invoke timed out after {timeout}ms"
                    ))),
                };
            let _ = reply.send(result);
        });
    }

    fn handle_dynamic_host_rpc_invoke(
        &mut self,
        payload: crate::ops::DynamicHostRpcInvokeEvent,
        event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    ) {
        let crate::ops::DynamicHostRpcInvokeEvent {
            caller_worker,
            caller_generation,
            binding,
            method_name,
            args,
            reply,
        } = payload;
        if host_rpc_method_blocked(&method_name) {
            let _ = reply.send(Err(PlatformError::bad_request(format!(
                "dynamic host rpc method is blocked: {method_name}"
            ))));
            return;
        }

        let provider_id = {
            let Some(pool) = self.get_pool_mut(&caller_worker, caller_generation) else {
                let _ = reply.send(Err(PlatformError::not_found(
                    "dynamic worker pool not found",
                )));
                return;
            };
            pool.dynamic_rpc_bindings
                .iter()
                .find(|entry| entry.binding == binding)
                .map(|entry| entry.provider_id.clone())
        };
        let Some(provider_id) = provider_id else {
            let _ = reply.send(Err(PlatformError::bad_request(format!(
                "dynamic host rpc binding not found: {binding}"
            ))));
            return;
        };
        let Some(provider) = self.host_rpc_providers.get(&provider_id).cloned() else {
            let _ = reply.send(Err(PlatformError::not_found(
                "dynamic host rpc provider not found",
            )));
            return;
        };
        if !provider.methods.contains(&method_name) {
            let _ = reply.send(Err(PlatformError::bad_request(format!(
                "dynamic host rpc method is not allowed: {method_name}"
            ))));
            return;
        }
        let owner_isolate_exists = self
            .get_pool_mut(&provider.owner_worker, provider.owner_generation)
            .map(|pool| {
                pool.isolates
                    .iter()
                    .any(|isolate| isolate.id == provider.owner_isolate_id)
            })
            .unwrap_or(false);
        if !owner_isolate_exists {
            let _ = reply.send(Err(PlatformError::runtime(
                "dynamic host rpc provider isolate is unavailable",
            )));
            return;
        }

        let runtime_request_id = Uuid::new_v4().to_string();
        let request = WorkerInvocation {
            method: "RPC".to_string(),
            url: format!("http://host-rpc/{}", method_name),
            headers: Vec::new(),
            body: Vec::new(),
            request_id: format!("host-rpc-{runtime_request_id}"),
        };
        let (inner_reply_tx, inner_reply_rx) = oneshot::channel();
        self.enqueue_invoke(
            provider.owner_worker.clone(),
            runtime_request_id,
            request,
            None,
            None,
            None,
            Some(HostRpcExecutionCall {
                target_id: provider.target_id.clone(),
                method: method_name,
                args,
            }),
            Some(provider.owner_isolate_id),
            Some(provider.owner_generation),
            true,
            inner_reply_tx,
            PendingReplyKind::Normal,
            event_tx,
        );
        tokio::spawn(async move {
            let result = match inner_reply_rx.await {
                Ok(Ok(output)) => Ok(output.body),
                Ok(Err(error)) => Err(error),
                Err(_) => Err(PlatformError::internal(
                    "dynamic host rpc invoke response channel closed",
                )),
            };
            let _ = reply.send(result);
        });
    }

    async fn deploy(
        &mut self,
        worker_name: String,
        source: String,
        config: DeployConfig,
        persist: bool,
    ) -> Result<String> {
        let worker_name = worker_name.trim().to_string();
        if worker_name.is_empty() {
            return Err(PlatformError::bad_request("Worker name must not be empty"));
        }
        let bindings = extract_bindings(&config)?;
        validate_worker(self.bootstrap_snapshot, &source).await?;
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
        if persist {
            persist_worker_deployment(
                &self.storage,
                &worker_name,
                &source,
                &config,
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
                actor_bindings: bindings.actor,
                actor_bindings_json,
                dynamic_bindings: bindings.dynamic,
                dynamic_bindings_json,
                dynamic_rpc_bindings: Vec::new(),
                dynamic_rpc_bindings_json,
                dynamic_env_json,
                secret_replacements: Vec::new(),
                egress_allow_hosts: Vec::new(),
                strict_request_isolation: has_dynamic_bindings,
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
        let worker_name = format!("dyn-{}", Uuid::new_v4().simple());
        let dynamic_config =
            build_dynamic_worker_config(env, egress_allow_hosts, dynamic_rpc_bindings)?;
        validate_worker(self.bootstrap_snapshot, &source).await?;
        let generation = self.next_generation;
        self.next_generation += 1;
        let deployment_id = Uuid::new_v4().to_string();
        let dynamic_binding_names = dynamic_config
            .dynamic_rpc_bindings
            .iter()
            .map(|binding| binding.binding.clone())
            .collect::<Vec<_>>();

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
            snapshot: self.bootstrap_snapshot,
            snapshot_preloaded: false,
            source: Arc::<str>::from(source),
            kv_bindings_json: Arc::<str>::from(
                crate::json::to_string(&Vec::<String>::new())
                    .map_err(|error| PlatformError::internal(error.to_string()))?,
            ),
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
            strict_request_isolation: true,
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
                        "unknown actor binding for worker {}: {}",
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
                    "actor fetch invoke is no longer supported; use fetch + wake + stub.atomic",
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
                "actor binding/key must not be empty",
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
                    "actor invoke response channel closed".to_string(),
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
                let completion_meta = if needs_completion_meta {
                    Some(PendingReplyMeta {
                        method: pending_invoke.request.method.clone(),
                        url: pending_invoke.request.url.clone(),
                        traceparent: traceparent.clone(),
                        user_request_id: pending_invoke.request.request_id.clone(),
                    })
                } else {
                    None
                };
                let command = IsolateCommand::Execute {
                    runtime_request_id: runtime_request_id.clone(),
                    completion_token: completion_token.clone(),
                    worker_name_json,
                    kv_bindings_json,
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
                if isolate.sender.send(command).is_err() {
                    send_failed = true;
                } else {
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
        let isolate = spawn_isolate_thread(
            snapshot,
            snapshot_preloaded,
            source,
            kv_store,
            actor_store,
            cache_store,
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
        result: Result<WorkerOutput>,
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
                if pending.completion_token != completion_token {
                    warn!(
                        worker = %worker_name,
                        generation,
                        isolate_id,
                        request_id,
                        "dropping completion with invalid token"
                    );
                    return;
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
                    if wait_until_count > 0 {
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
        let mut removed = false;
        if let Some(pool) = self.get_pool_mut(worker_name, generation) {
            if isolate_idx < pool.isolates.len() {
                let isolate = pool.isolates.swap_remove(isolate_idx);
                let _ = isolate.sender.send(IsolateCommand::Shutdown);
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
                        PendingReplyKind::Normal | PendingReplyKind::WebsocketFrame { .. } => {}
                    }
                    replies.push((request_id, pending.reply));
                }
                removed = true;
            }
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
        {
            let Some(entry) = self.workers.get_mut(worker_name) else {
                return;
            };
            let current_generation = entry.current_generation;
            let drained: Vec<u64> = entry
                .pools
                .iter()
                .filter(|(generation, pool)| {
                    **generation != current_generation
                        && pool.is_drained()
                        && !live_websocket_generations.contains(generation)
                        && !live_transport_generations.contains(generation)
                })
                .map(|(generation, _)| *generation)
                .collect();

            for generation in drained {
                if let Some(pool) = entry.pools.remove(&generation) {
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

    fn get_pool_mut(&mut self, worker_name: &str, generation: u64) -> Option<&mut WorkerPool> {
        self.workers
            .get_mut(worker_name)
            .and_then(|entry| entry.pools.get_mut(&generation))
    }

    fn shutdown_all(&mut self) {
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

struct DeployBindings {
    kv: Vec<String>,
    actor: Vec<String>,
    dynamic: Vec<String>,
}

struct DynamicWorkerConfig {
    dynamic_env: Vec<(String, String)>,
    dynamic_rpc_bindings: Vec<DynamicRpcBinding>,
    secret_replacements: Vec<(String, String)>,
    egress_allow_hosts: Vec<String>,
    env_placeholders: HashMap<String, String>,
}

fn extract_bindings(config: &DeployConfig) -> Result<DeployBindings> {
    let mut kv = Vec::new();
    let mut actor = Vec::new();
    let mut dynamic = Vec::new();
    let mut seen = HashSet::new();
    for binding in &config.bindings {
        match binding {
            DeployBinding::Kv { binding } => {
                let name = binding.trim();
                if name.is_empty() {
                    return Err(PlatformError::bad_request("binding name must not be empty"));
                }
                if !seen.insert(name.to_string()) {
                    return Err(PlatformError::bad_request(format!(
                        "duplicate binding name: {name}"
                    )));
                }
                kv.push(name.to_string());
            }
            DeployBinding::Actor { binding } => {
                let name = binding.trim();
                if name.is_empty() {
                    return Err(PlatformError::bad_request("binding name must not be empty"));
                }
                if !seen.insert(name.to_string()) {
                    return Err(PlatformError::bad_request(format!(
                        "duplicate binding name: {name}"
                    )));
                }
                actor.push(name.to_string());
            }
            DeployBinding::Dynamic { binding } => {
                let name = binding.trim();
                if name.is_empty() {
                    return Err(PlatformError::bad_request("binding name must not be empty"));
                }
                if !seen.insert(name.to_string()) {
                    return Err(PlatformError::bad_request(format!(
                        "duplicate binding name: {name}"
                    )));
                }
                dynamic.push(name.to_string());
            }
        }
    }
    Ok(DeployBindings { kv, actor, dynamic })
}

fn build_dynamic_worker_config(
    env: HashMap<String, String>,
    egress_allow_hosts: Vec<String>,
    dynamic_rpc_bindings: Vec<DynamicRpcBinding>,
) -> Result<DynamicWorkerConfig> {
    let mut dynamic_env = Vec::new();
    let mut secret_replacements = Vec::new();
    let mut env_placeholders = HashMap::new();

    for (name, value) in env {
        let key = name.trim().to_string();
        if key.is_empty() {
            return Err(PlatformError::bad_request(
                "dynamic env variable name must not be empty",
            ));
        }
        if !is_valid_env_name(&key) {
            return Err(PlatformError::bad_request(format!(
                "invalid dynamic env variable name: {key}"
            )));
        }
        if env_placeholders.contains_key(&key) {
            return Err(PlatformError::bad_request(format!(
                "duplicate dynamic env variable name: {key}"
            )));
        }

        let placeholder = format!("__DD_SECRET_{}__", Uuid::new_v4().simple());
        dynamic_env.push((key.clone(), placeholder.clone()));
        secret_replacements.push((placeholder.clone(), value));
        env_placeholders.insert(key, placeholder);
    }

    let mut normalized_hosts = Vec::new();
    let mut seen_hosts = HashSet::new();
    for host in egress_allow_hosts {
        let normalized = host.trim().to_ascii_lowercase();
        if normalized.is_empty() {
            continue;
        }
        if !is_valid_egress_host(&normalized) {
            return Err(PlatformError::bad_request(format!(
                "invalid egress allow host: {normalized}"
            )));
        }
        if seen_hosts.insert(normalized.clone()) {
            normalized_hosts.push(normalized);
        }
    }

    Ok(DynamicWorkerConfig {
        dynamic_env,
        dynamic_rpc_bindings,
        secret_replacements,
        egress_allow_hosts: normalized_hosts,
        env_placeholders,
    })
}

fn is_valid_env_name(name: &str) -> bool {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !(first == '_' || first.is_ascii_alphabetic()) {
        return false;
    }
    chars.all(|char| char == '_' || char.is_ascii_alphanumeric())
}

fn is_valid_egress_host(host: &str) -> bool {
    if let Some(rest) = host.strip_prefix("*.") {
        return !rest.is_empty()
            && rest
                .chars()
                .all(|char| char.is_ascii_alphanumeric() || char == '-' || char == '.')
            && rest.contains('.');
    }
    host.chars()
        .all(|char| char.is_ascii_alphanumeric() || char == '-' || char == '.')
        && host.contains('.')
}

fn validate_runtime_config(config: &RuntimeConfig) -> Result<()> {
    if config.max_isolates == 0 {
        return Err(PlatformError::internal(
            "max_isolates must be greater than 0",
        ));
    }
    if config.max_inflight_per_isolate == 0 {
        return Err(PlatformError::internal(
            "max_inflight_per_isolate must be greater than 0",
        ));
    }
    if config.min_isolates > config.max_isolates {
        return Err(PlatformError::internal(
            "min_isolates cannot exceed max_isolates",
        ));
    }
    if config.cache_max_entries == 0 {
        return Err(PlatformError::internal(
            "cache_max_entries must be greater than 0",
        ));
    }
    if config.cache_max_bytes == 0 {
        return Err(PlatformError::internal(
            "cache_max_bytes must be greater than 0",
        ));
    }
    if config.cache_default_ttl.is_zero() {
        return Err(PlatformError::internal(
            "cache_default_ttl must be greater than 0",
        ));
    }
    Ok(())
}

fn spawn_runtime_thread(
    mut receiver: mpsc::Receiver<RuntimeCommand>,
    mut cancel_receiver: mpsc::UnboundedReceiver<RuntimeCommand>,
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
                );
                let mut ticker = tokio::time::interval(config.scale_tick);
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

                loop {
                    tokio::select! {
                        Some(command) = receiver.recv() => {
                            manager.begin_runtime_batch();
                            manager.handle_command(command, &event_tx).await;
                            drain_ready_runtime_work(
                                &mut manager,
                                &mut receiver,
                                &mut cancel_receiver,
                                &mut event_rx,
                                &event_tx,
                            )
                            .await;
                            manager.finish_runtime_batch(&event_tx);
                        }
                        Some(command) = cancel_receiver.recv() => {
                            manager.begin_runtime_batch();
                            manager.handle_command(command, &event_tx).await;
                            drain_ready_runtime_work(
                                &mut manager,
                                &mut receiver,
                                &mut cancel_receiver,
                                &mut event_rx,
                                &event_tx,
                            )
                            .await;
                            manager.finish_runtime_batch(&event_tx);
                        }
                        Some(event) = event_rx.recv() => {
                            manager.begin_runtime_batch();
                            manager.handle_event(event, &event_tx).await;
                            drain_ready_runtime_work(
                                &mut manager,
                                &mut receiver,
                                &mut cancel_receiver,
                                &mut event_rx,
                                &event_tx,
                            )
                            .await;
                            manager.finish_runtime_batch(&event_tx);
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
) {
    loop {
        let mut made_progress = false;

        match receiver.try_recv() {
            Ok(command) => {
                manager.handle_command(command, event_tx).await;
                made_progress = true;
            }
            Err(TryRecvError::Empty | TryRecvError::Disconnected) => {}
        }

        match cancel_receiver.try_recv() {
            Ok(command) => {
                manager.handle_command(command, event_tx).await;
                made_progress = true;
            }
            Err(TryRecvError::Empty | TryRecvError::Disconnected) => {}
        }

        match event_rx.try_recv() {
            Ok(event) => {
                manager.handle_event(event, event_tx).await;
                made_progress = true;
            }
            Err(TryRecvError::Empty | TryRecvError::Disconnected) => {}
        }

        if !made_progress {
            break;
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
        IsolateEventPayload::ResponseStart(payload) => match decode_response_start_payload(payload) {
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
        IsolateEventPayload::ResponseChunk(payload) => match decode_response_chunk_payload(payload) {
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
        IsolateEventPayload::ActorSocketList(payload) => {
            let _ = event_tx.send(RuntimeEvent::ActorSocketList {
                worker_name: worker_name.to_string(),
                generation,
                payload,
            });
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
        IsolateEventPayload::ActorTransportList(payload) => {
            let _ = event_tx.send(RuntimeEvent::ActorTransportList {
                worker_name: worker_name.to_string(),
                generation,
                payload,
            });
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
    }
}

fn spawn_isolate_thread(
    snapshot: &'static [u8],
    snapshot_preloaded: bool,
    source: Arc<str>,
    kv_store: KvStore,
    actor_store: ActorStore,
    cache_store: CacheStore,
    worker_name: String,
    generation: u64,
    isolate_id: u64,
    event_tx: mpsc::UnboundedSender<RuntimeEvent>,
) -> Result<IsolateHandle> {
    let (command_tx, command_rx) = std_mpsc::channel();
    let (init_tx, init_rx) = std_mpsc::channel::<Result<()>>();
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
                    op_state.put(RequestBodyStreams::default());
                    op_state.put(crate::ops::ActorRequestScopes::default());
                    op_state.put(crate::ops::RequestSecretContexts::default());
                }
                if !snapshot_preloaded {
                    if let Err(error) = load_worker(&mut js_runtime, &source).await
                    {
                        let _ = init_tx.send(Err(error));
                        return;
                    }
                }
                let _ = init_tx.send(Ok(()));

                loop {
                    let mut made_progress = false;

                    loop {
                        match event_payload_rx.try_recv() {
                            Ok(payload) => {
                                made_progress = true;
                                handle_isolate_event_payload(
                                    &event_tx,
                                    &worker_name,
                                    generation,
                                    isolate_id,
                                    payload,
                                );
                            }
                            Err(std_mpsc::TryRecvError::Empty) => break,
                            Err(std_mpsc::TryRecvError::Disconnected) => break,
                        }
                    }

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

                    if let Err(error) = pump_event_loop_once(&mut js_runtime) {
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
                } => ExecuteActorCall::Message {
                    binding: binding.clone(),
                    key: key.clone(),
                    handle: handle.clone(),
                    is_text: *is_text,
                    data: data.clone(),
                },
                ActorExecutionCall::Close {
                    binding,
                    key,
                    handle,
                    code,
                    reason,
                } => ExecuteActorCall::Close {
                    binding: binding.clone(),
                    key: key.clone(),
                    handle: handle.clone(),
                    code: *code,
                    reason: reason.clone(),
                },
                ActorExecutionCall::TransportDatagram {
                    binding,
                    key,
                    handle,
                    data,
                } => ExecuteActorCall::TransportDatagram {
                    binding: binding.clone(),
                    key: key.clone(),
                    handle: handle.clone(),
                    data: data.clone(),
                },
                ActorExecutionCall::TransportStream {
                    binding,
                    key,
                    handle,
                    data,
                } => ExecuteActorCall::TransportStream {
                    binding: binding.clone(),
                    key: key.clone(),
                    handle: handle.clone(),
                    data: data.clone(),
                },
                ActorExecutionCall::TransportClose {
                    binding,
                    key,
                    handle,
                    code,
                    reason,
                } => ExecuteActorCall::TransportClose {
                    binding: binding.clone(),
                    key: key.clone(),
                    handle: handle.clone(),
                    code: *code,
                    reason: reason.clone(),
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
        IsolateCommand::Shutdown => Ok(false),
    }
}

async fn persist_worker_deployment(
    storage: &RuntimeStorageConfig,
    worker_name: &str,
    source: &str,
    config: &DeployConfig,
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
        .ok_or_else(|| PlatformError::bad_request("missing websocket actor binding metadata"))?;
    let key = internal_header_value(&output.headers, INTERNAL_WS_KEY_HEADER)
        .ok_or_else(|| PlatformError::bad_request("missing websocket actor key metadata"))?;
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
        .ok_or_else(|| PlatformError::bad_request("missing transport actor binding metadata"))?;
    let key = internal_header_value(&output.headers, INTERNAL_TRANSPORT_KEY_HEADER)
        .ok_or_else(|| PlatformError::bad_request("missing transport actor key metadata"))?;
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
mod tests {
    use super::{
        BlobStoreConfig, RuntimeConfig, RuntimeService, RuntimeServiceConfig, RuntimeStorageConfig,
    };
    use common::{
        DeployBinding, DeployConfig, DeployInternalConfig, DeployTraceDestination, WorkerInvocation,
    };
    use serde::Deserialize;
    use serial_test::serial;
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::time::{Duration, Instant};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::sync::mpsc;
    use tokio::time::{sleep, timeout};
    use uuid::Uuid;

    fn test_invocation() -> WorkerInvocation {
        WorkerInvocation {
            method: "GET".to_string(),
            url: "http://worker/".to_string(),
            headers: Vec::new(),
            body: Vec::new(),
            request_id: "test-request".to_string(),
        }
    }

    fn test_invocation_with_path(path: &str, request_id: &str) -> WorkerInvocation {
        WorkerInvocation {
            method: "GET".to_string(),
            url: format!("http://worker{path}"),
            headers: Vec::new(),
            body: Vec::new(),
            request_id: request_id.to_string(),
        }
    }

    fn test_websocket_invocation(path: &str, request_id: &str) -> WorkerInvocation {
        WorkerInvocation {
            method: "GET".to_string(),
            url: format!("http://worker{path}"),
            headers: vec![
                ("connection".to_string(), "Upgrade".to_string()),
                ("upgrade".to_string(), "websocket".to_string()),
                ("sec-websocket-version".to_string(), "13".to_string()),
                (
                    "sec-websocket-key".to_string(),
                    "dGhlIHNhbXBsZSBub25jZQ==".to_string(),
                ),
            ],
            body: Vec::new(),
            request_id: request_id.to_string(),
        }
    }

    fn test_transport_invocation() -> WorkerInvocation {
        WorkerInvocation {
            method: "CONNECT".to_string(),
            url: "http://worker/session".to_string(),
            headers: vec![(
                "x-dd-transport-protocol".to_string(),
                "webtransport".to_string(),
            )],
            body: Vec::new(),
            request_id: "test-transport-request".to_string(),
        }
    }

    fn counter_worker() -> String {
        r#"
let counter = 0;
export default {
  async fetch() {
    counter += 1;
    return new Response(String(counter));
  },
};
"#
        .to_string()
    }

    fn slow_worker() -> String {
        r#"
export default {
  async fetch() {
    await Deno.core.ops.op_sleep(40);
    return new Response("ok");
  },
};
"#
        .to_string()
    }

    fn dynamic_namespace_worker() -> String {
        r#"
let child = null;
class Api extends RpcTarget {
  constructor() {
    super();
    this.count = 0;
  }
  async bump() {
    this.count += 1;
    return this.count;
  }
}

export default {
  async fetch(request, env) {
    if (!child) {
      child = await env.SANDBOX.get("test:v1", async () => ({
        entrypoint: "worker.js",
        modules: {
          "worker.js": "import { nextCounter } from './lib.js'; export default { async fetch(_request, childEnv) { const hostCount = await childEnv.API.bump(); return new Response(String(nextCounter()) + ':' + String(hostCount)); } };",
          "./lib.js": "let counter = 0; export function nextCounter() { counter += 1; return counter; }",
        },
        env: { SECRET: "ok", API: new Api() },
        timeout: 1_500,
      }));
    }
    const response = await child.fetch("http://worker/");
    return new Response(await response.text());
  },
};
"#
        .to_string()
    }

    fn dynamic_namespace_admin_worker() -> String {
        r#"
let slow = null;

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (url.pathname === "/ensure") {
      await env.SANDBOX.get("admin:v1", async () => ({
        entrypoint: "worker.js",
        modules: {
          "worker.js": "export default { async fetch() { return new Response('ok'); } };",
        },
        timeout: 200,
      }));
      return new Response("ok");
    }

    if (url.pathname === "/ids") {
      const ids = await env.SANDBOX.list();
      return new Response(JSON.stringify(ids.sort()), {
        headers: [["content-type", "application/json"]],
      });
    }

    if (url.pathname === "/delete") {
      const deleted = await env.SANDBOX.delete("admin:v1");
      return new Response(String(Boolean(deleted)));
    }

    if (url.pathname === "/timeout") {
      if (!slow) {
        slow = await env.SANDBOX.get("slow:v1", async () => ({
          entrypoint: "worker.js",
          modules: {
            "worker.js": "export default { async fetch() { await Deno.core.ops.op_sleep(100); return new Response('slow'); } };",
          },
          timeout: 25,
        }));
      }
      try {
        await slow.fetch("http://worker/");
        return new Response("no-timeout", { status: 200 });
      } catch (error) {
        return new Response(String(error || "timeout"), { status: 504 });
      }
    }

    return new Response("not found", { status: 404 });
  },
};
"#
        .to_string()
    }

    fn dynamic_response_json_worker() -> String {
        r#"
export default {
  async fetch(_request, env) {
    const child = await env.SANDBOX.get("json:v1", async () => ({
      entrypoint: "worker.js",
      modules: {
        "worker.js": "export default { async fetch() { return Response.json({ ok: true, source: 'dynamic-child' }); } };",
      },
      timeout: 1_500,
    }));
    const response = await child.fetch("http://worker/");
    return new Response(await response.text(), {
      headers: [["content-type", "application/json; charset=utf-8"]],
    });
  },
};
"#
        .to_string()
    }

    fn dynamic_runtime_surface_worker() -> String {
        r#"
function runtimeSurface() {
  return {
    fetch: typeof fetch === "function",
    request: typeof Request === "function",
    response: typeof Response === "function",
    headers: typeof Headers === "function",
    formData: typeof FormData === "function",
    url: typeof URL === "function",
    urlPattern: typeof URLPattern === "function",
    readableStream: typeof ReadableStream === "function",
    blob: typeof Blob === "function",
    textEncoder: typeof TextEncoder === "function",
    textDecoder: typeof TextDecoder === "function",
    structuredClone: typeof structuredClone === "function",
    cryptoDigest: typeof crypto?.subtle?.digest === "function",
    responseJsonType: Response.json({ ok: true }).headers.get("content-type"),
  };
}

export default {
  async fetch(_request, env) {
    const child = await env.SANDBOX.get("surface:v1", async () => ({
      entrypoint: "worker.js",
      modules: {
        "worker.js": `
function runtimeSurface() {
  return {
    fetch: typeof fetch === "function",
    request: typeof Request === "function",
    response: typeof Response === "function",
    headers: typeof Headers === "function",
    formData: typeof FormData === "function",
    url: typeof URL === "function",
    urlPattern: typeof URLPattern === "function",
    readableStream: typeof ReadableStream === "function",
    blob: typeof Blob === "function",
    textEncoder: typeof TextEncoder === "function",
    textDecoder: typeof TextDecoder === "function",
    structuredClone: typeof structuredClone === "function",
    cryptoDigest: typeof crypto?.subtle?.digest === "function",
    responseJsonType: Response.json({ ok: true }).headers.get("content-type"),
  };
}

export default {
  async fetch() {
    return Response.json(runtimeSurface());
  },
};
        `,
      },
      timeout: 1_500,
    }));
    const childResponse = await child.fetch("http://worker/");
    const childSurface = await childResponse.json();
    const selfSurface = runtimeSurface();
    return Response.json({
      same: JSON.stringify(selfSurface) === JSON.stringify(childSurface),
      self: selfSurface,
      child: childSurface,
    });
  },
};
"#
        .to_string()
    }

    fn transport_echo_worker() -> String {
        r#"
export default {
  async fetch(request, env) {
    return await env.MEDIA.get(env.MEDIA.idFromName("global")).atomic((state) => {
      const { response } = state.accept(request);
      return response;
    });
  },

  async wake(event, env) {
    const _ = env;
    if (event.type !== "transportstream" || !event.stub || !event.handle) {
      return;
    }
    await event.stub.apply([{ type: "transport.stream", handle: event.handle, payload: event.data }]);
  },
};
"#
        .to_string()
    }

    fn transport_shape_worker() -> String {
        r#"
export default {
  async fetch(request, env) {
    const protocol = String(request.headers.get("x-dd-transport-protocol") ?? "").toLowerCase();
    if (request.method !== "CONNECT") {
      return new Response(`bad-method:${request.method}`, { status: 500 });
    }
    if (protocol !== "webtransport") {
      return new Response(`bad-protocol:${protocol}`, { status: 500 });
    }
    return await env.MEDIA.get(env.MEDIA.idFromName("global")).atomic((state) => {
      const { response } = state.accept(request);
      return response;
    });
  },
};
"#
        .to_string()
    }

    fn websocket_storage_worker() -> String {
        r#"
export function openSocket(state, payload) {
  const { response } = state.accept(payload.request);
  return response;
}

export function onSocketMessage(state, event) {
  const text = typeof event.data === "string"
    ? event.data
    : new TextDecoder().decode(event.data);
  const chat = state.tvar("chat", { count: 0, last: null });
  const next = chat.modify((previous) => ({
    count: Number(previous?.count ?? 0) + 1,
    last: text,
  }));
  const socket = new WebSocket(event.handle);
  socket.send(JSON.stringify({
    seen: next.last,
    count: next.count,
  }), "text");
  return next;
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/state") {
      const stored = await env.CHAT.get(env.CHAT.idFromName("global")).atomic((state) => state.tvar("chat", { count: 0, last: null }).read());
      return Response.json(stored ?? { count: 0, last: null });
    }
    return await env.CHAT.get(env.CHAT.idFromName("global")).atomic(openSocket, { request });
  },

  async wake(event, env) {
    const _ = env;
    if (event.type !== "socketmessage" || !event.stub) {
      return;
    }
    await event.stub.atomic(onSocketMessage, event);
  },
};
"#
        .to_string()
    }

    fn dynamic_fetch_probe_worker(url: &str) -> String {
        format!(
            r#"
export default {{
  async fetch(_request, env) {{
    const response = await fetch("{url}?token=" + encodeURIComponent(env.API_TOKEN), {{
      headers: {{
        "authorization": "Bearer " + env.API_TOKEN,
        "x-dd-secret": env.API_TOKEN,
      }},
    }});
    return new Response(await response.text(), {{
      status: response.status,
      headers: response.headers,
    }});
  }},
}};
"#
        )
    }

    fn dynamic_fetch_abort_worker(url: &str) -> String {
        format!(
            r#"
export default {{
  async fetch() {{
    const controller = new AbortController();
    setTimeout(() => controller.abort(new Error("stop")), 25);
    try {{
      await fetch("{url}", {{
        signal: controller.signal,
        headers: {{
          "x-abort-test": "true",
        }},
      }});
      return new Response("unexpected-success", {{ status: 500 }});
    }} catch (error) {{
      return new Response(String(error?.name ?? error));
    }}
  }},
}};
"#
        )
    }

    fn preview_dynamic_worker() -> String {
        r#"
class PreviewControl extends RpcTarget {
  constructor(previewId) {
    super();
    this.previewId = previewId;
    this.hits = 0;
  }

  async metadata() {
    this.hits += 1;
    return {
      previewId: this.previewId,
      hits: this.hits,
    };
  }
}

function previewModules() {
  return {
    "worker.js": `
      export default {
        async fetch(request, env) {
          const url = new URL(request.url);
          const meta = await env.PREVIEW.metadata();
          if (url.pathname === "/" || url.pathname === "/index.html") {
            return new Response(JSON.stringify({
              ok: true,
              preview: meta.previewId,
              hits: meta.hits,
              route: "root",
            }), {
              headers: { "content-type": "application/json; charset=utf-8" },
            });
          }
          if (url.pathname === "/api/health") {
            return new Response(JSON.stringify({
              ok: true,
              preview: meta.previewId,
              hits: meta.hits,
              route: "health",
            }), {
              headers: { "content-type": "application/json; charset=utf-8" },
            });
          }
          return new Response("preview route not found", { status: 404 });
        },
      };
    `,
  };
}

async function ensurePreview(env, previewId) {
  return env.SANDBOX.get(`preview:${previewId}`, async () => ({
    entrypoint: "worker.js",
    modules: previewModules(),
    env: {
      PREVIEW: new PreviewControl(previewId),
    },
    timeout: 3000,
  }));
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (url.pathname === "/") {
      return new Response("preview manager");
    }

    if (!url.pathname.startsWith("/preview/")) {
      return new Response("not found", { status: 404 });
    }

    const rest = url.pathname.slice("/preview/".length);
    const slashIdx = rest.indexOf("/");
    const previewId = slashIdx === -1 ? rest : rest.slice(0, slashIdx);
    const tailPath = slashIdx === -1 ? "/" : rest.slice(slashIdx);
    const preview = await ensurePreview(env, previewId);
    const target = new URL(`http://worker${tailPath}`);
    target.search = url.search;
    return preview.fetch(target.toString(), {
      method: request.method,
      headers: request.headers,
    });
  },
};
"#
        .to_string()
    }

    fn versioned_worker(version: &str, delay_ms: u64) -> String {
        format!(
            r#"
export default {{
  async fetch() {{
    await Deno.core.ops.op_sleep({delay_ms});
    return new Response("{version}");
  }},
}};
"#
        )
    }

    fn io_wait_worker() -> String {
        r#"
export default {
  async fetch() {
    await Deno.core.ops.op_sleep(50);
    return new Response("ok");
  },
};
"#
        .to_string()
    }

    fn frozen_time_worker() -> String {
        r#"
export default {
  async fetch() {
    const now0 = Date.now();
    const perf0 = performance.now();
    let guard = 0;
    for (let i = 0; i < 250000; i++) {
      guard += i;
    }
    const now1 = Date.now();
    const perf1 = performance.now();

    await new Promise((resolve) => setTimeout(resolve, 20));

    const now2 = Date.now();
    const perf2 = performance.now();
    return new Response(JSON.stringify({ now0, now1, now2, perf0, perf1, perf2, guard }), {
      headers: [["content-type", "application/json"]],
    });
  },
};
"#
        .to_string()
    }

    fn crypto_worker() -> String {
        r#"
export default {
  async fetch() {
    const random = new Uint8Array(16);
    crypto.getRandomValues(random);
    const digestBuffer = await crypto.subtle.digest(
      "SHA-256",
      new TextEncoder().encode("dd-runtime"),
    );
    const digest = Array.from(new Uint8Array(digestBuffer));
    return Response.json({
      random_length: random.length,
      random_non_zero: random.some((value) => value !== 0),
      uuid: crypto.randomUUID(),
      digest_length: digest.length,
    });
  },
};
"#
        .to_string()
    }

    fn abort_aware_worker() -> String {
        r#"
let abortCount = 0;

export default {
  async fetch(_request, _env, ctx) {
    if (ctx.requestId === "block") {
      await new Promise((resolve) => {
        const done = () => {
          abortCount += 1;
          resolve();
        };
        if (ctx.signal?.aborted) {
          done();
          return;
        }
        ctx.signal?.addEventListener("abort", done);
      });
      return new Response("aborted");
    }

    return new Response(`abortCount=${abortCount}`);
  },
};
"#
        .to_string()
    }

    fn malicious_completion_worker() -> String {
        r#"
let counter = 0;

export default {
  async fetch(_request, _env, ctx) {
    counter += 1;

    Deno.core.ops.op_emit_completion("{");
    Deno.core.ops.op_emit_completion(
      JSON.stringify({
        request_id: ctx.requestId,
        completion_token: "forged-token",
        ok: true,
        result: { status: 200, headers: [], body: [102, 97, 107, 101] },
      }),
    );

    return new Response(String(counter));
  },
};
"#
        .to_string()
    }

    fn cache_worker(cache_name: &str, label: &str) -> String {
        format!(
            r#"
let count = 0;

export default {{
  async fetch() {{
    const cache = await caches.open("{cache_name}");
    const key = new Request("http://cache/item", {{ method: "GET" }});
    const hit = await cache.match(key);
    if (hit) {{
      return hit;
    }}

    count += 1;
    const response = new Response("{label}:" + String(count), {{
      headers: [["cache-control", "public, max-age=60"]],
    }});
    await cache.put(key, response.clone());
    return response;
  }},
}};
"#
        )
    }

    fn streaming_request_body_worker() -> String {
        r#"
export default {
  async fetch(request) {
    const reader = request.body?.getReader?.();
    if (!reader) {
      return new Response("no-body");
    }
    let output = "";
    while (true) {
      const { value, done } = await reader.read();
      if (done) {
        break;
      }
      for (const byte of value) {
        output += String.fromCharCode(byte);
      }
    }
    return new Response(output);
  },
};
"#
        .to_string()
    }

    fn actor_worker() -> String {
        r#"
globalThis.__dd_actor_runtime = globalThis.__dd_actor_runtime ?? {
  active: new Map(),
  max: new Map(),
};

function busyWait(ms) {
  let guard = 0;
  const steps = Math.max(1, ms * 50000);
  while (guard < steps) {
    guard += 1;
  }
  return guard;
}

function asNumber(input, fallback = 0) {
  const parsed = Number(input);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function queryParam(search, name) {
  const trimmed = String(search || "").replace(/^\?/, "");
  if (!trimmed) {
    return null;
  }
  for (const pair of trimmed.split("&")) {
    if (!pair) {
      continue;
    }
    const [rawKey, rawValue = ""] = pair.split("=");
    if (decodeURIComponent(rawKey) === name) {
      return decodeURIComponent(rawValue);
    }
  }
  return null;
}

export function seedCount(state) {
  state.set("count", "0");
  return true;
}

export function incrementStrict(state) {
  const currentValue = asNumber(state.get("count", { strict: true }), 0);
  busyWait(10);
  state.set("count", String(currentValue + 1));
  return currentValue + 1;
}

export function readCount(state) {
  const current = state.get("count");
  return current ? String(current) : "0";
}

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const key = queryParam(url.search, "key") ?? "default";
    const id = env.MY_ACTOR.idFromName(key);
    const actor = env.MY_ACTOR.get(id);

    if (url.pathname === "/run") {
      await actor.atomic((state) => {
        const slot = String(state.id);
        const runtime = globalThis.__dd_actor_runtime;
        const active = (runtime.active.get(slot) ?? 0) + 1;
        runtime.active.set(slot, active);
        const max = Math.max(runtime.max.get(slot) ?? 0, active);
        runtime.max.set(slot, max);
        busyWait(100);
        runtime.active.set(slot, Math.max(0, (runtime.active.get(slot) ?? 1) - 1));
        return null;
      });
      return new Response("ok");
    }

    if (url.pathname === "/max") {
      return new Response(String(await actor.atomic((state) => {
        const runtime = globalThis.__dd_actor_runtime;
        return runtime.max.get(String(state.id)) ?? 0;
      })));
    }

    if (url.pathname === "/seed") {
      await actor.atomic(seedCount);
      return new Response("ok");
    }

    if (url.pathname === "/value-roundtrip") {
      const ok = await actor.atomic((state) => {
        state.set("profile", {
          name: "alice",
          createdAt: new Date("2026-01-02T03:04:05.000Z"),
          flags: new Set(["a", "b"]),
          scores: new Map([["p95", 21], ["p99", 32]]),
          bytes: new Uint8Array([1, 2, 3, 4]),
        });
        const value = state.get("profile");
        return Boolean(
          value
            && value.name === "alice"
            && value.createdAt instanceof Date
            && value.createdAt.toISOString() === "2026-01-02T03:04:05.000Z"
            && value.flags instanceof Set
            && value.flags.has("a")
            && value.scores instanceof Map
            && value.scores.get("p95") === 21
            && value.bytes instanceof Uint8Array
            && value.bytes.length === 4
            && value.bytes[3] === 4,
        );
      });
      return new Response(ok ? "ok" : "bad", { status: ok ? 200 : 500 });
    }

    if (url.pathname === "/value-string-get-guard") {
      const ok = await actor.atomic((state) => {
        state.set("profile", { nested: { ok: true } });
        const loaded = state.get("profile");
        return Boolean(
          loaded
            && loaded.nested
            && loaded.nested.ok === true,
        );
      });
      return new Response(ok ? "ok" : "bad", { status: ok ? 200 : 500 });
    }

    if (url.pathname === "/local-visibility") {
      const ok = await actor.atomic((state) => {
        state.set("count", "41");
        const loaded = state.get("count");
        const listed = state.list({ prefix: "co" });
        return Boolean(
          loaded === "41"
            && Array.isArray(listed)
            && listed.length === 1
            && listed[0].key === "count"
            && listed[0].value === "41",
        );
      });
      return new Response(ok ? "ok" : "bad", { status: ok ? 200 : 500 });
    }

    if (url.pathname === "/inc-cas") {
      await actor.atomic(incrementStrict);
      return new Response("ok");
    }

    if (url.pathname === "/get") {
      return new Response(String(await actor.atomic(readCount)));
    }

    return new Response("not found", { status: 404 });
  },
};
"#
        .to_string()
    }

    fn actor_constructor_storage_worker() -> String {
        r#"
export default {
  async fetch(request, env) {
    const actor = env.MY_ACTOR.get(env.MY_ACTOR.idFromName("user-ctor"));
    const url = new URL(request.url);

    if (url.pathname === "/seed") {
      await actor.atomic((state) => {
        state.set("count", "7");
        return true;
      });
      return new Response("ok");
    }

    if (url.pathname === "/constructor-value") {
      return new Response(String(await actor.atomic((state) => state.get("count") ?? "missing")));
    }

    if (url.pathname === "/current-value") {
      return new Response(String(await actor.atomic((state) => state.get("count") ?? "missing")));
    }

    return new Response("not found", { status: 404 });
  },
};
"#
        .to_string()
    }

    fn hosted_actor_worker() -> String {
        r#"
globalThis.__hosted_shared_global = globalThis.__hosted_shared_global ?? 0;

function busyWait(ms) {
  let guard = 0;
  const steps = Math.max(1, ms * 5000000);
  while (guard < steps) {
    guard += 1;
  }
  return guard;
}

export function seedStm(state) {
  state.set("a", "0");
  state.set("b", "0");
  return true;
}

export function writeA(state, value) {
  state.set("a", String(value));
  return String(value);
}

export function readOnce(state) {
  const a = String(state.get("a") ?? "missing");
  busyWait(40);
  return a;
}

export function readPairStrict(state) {
  const a = String(state.get("a", { strict: true }) ?? "missing");
  busyWait(40);
  const b = String(state.get("b") ?? "missing");
  return `${a}:${b}`;
}

export function readPairSnapshot(state) {
  const a = String(state.get("a") ?? "missing");
  busyWait(40);
  const b = String(state.get("b") ?? "missing");
  return `${a}:${b}`;
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const id = env.MY_ACTOR.idFromName(url.searchParams.get("key") ?? "default");
    const actor = env.MY_ACTOR.get(id);

    if (url.pathname === "/alpha/inc") {
      return new Response(String(await actor.atomic((state) => {
        const current = Number(state.get("count") ?? 0);
        const next = current + 1;
        state.set("count", String(next));
        return next;
      })));
    }

    if (url.pathname === "/beta/read") {
      return new Response(String(await actor.atomic((state) => {
        return String(state.get("count") ?? "0");
      })));
    }

    if (url.pathname === "/worker/global/inc") {
      globalThis.__hosted_shared_global += 1;
      return new Response(String(globalThis.__hosted_shared_global));
    }

    if (url.pathname === "/actor/global/read") {
      return new Response(String(await actor.atomic((_state) => globalThis.__hosted_shared_global)));
    }

    if (url.pathname === "/actor/global/inc") {
      return new Response(String(await actor.atomic((_state) => {
        globalThis.__hosted_shared_global += 1;
        return globalThis.__hosted_shared_global;
      })));
    }

    if (url.pathname === "/inline") {
      const suffix = "inline";
      return new Response(String(await actor.atomic(() => `ok-${suffix}`)));
    }

    if (url.pathname === "/stm/seed") {
      await actor.atomic(seedStm);
      return new Response("ok");
    }

    if (url.pathname === "/stm/write-a") {
      const value = String(url.searchParams.get("value") ?? "1");
      await actor.atomic(writeA, value);
      return new Response("ok");
    }

    if (url.pathname === "/stm/read-once") {
      return new Response(String(await actor.atomic(readOnce)));
    }

    if (url.pathname === "/stm/read-pair") {
      return new Response(String(await actor.atomic(readPairStrict)));
    }

    if (url.pathname === "/stm/read-pair-snapshot") {
      return new Response(String(await actor.atomic(readPairSnapshot)));
    }

    if (url.pathname === "/stm/tvar-default/read") {
      const count = actor.tvar("count", 7);
      return new Response(String(await actor.atomic(() => count.read())));
    }

    if (url.pathname === "/stm/tvar-default/raw") {
      return new Response(String(await actor.atomic((state) => state.get("count") ?? "missing")));
    }

    if (url.pathname === "/stm/tvar-default/write") {
      const count = actor.tvar("count", 7);
      return new Response(String(await actor.atomic(() => {
        const next = Number(count.read()) + 1;
        count.write(String(next));
        return next;
      })));
    }

    return new Response("not found", { status: 404 });
  },
};
"#
        .to_string()
    }

    fn async_context_worker() -> String {
        r#"
export default {
  async fetch(request) {
    const url = new URL(request.url);
    const ctx = globalThis.__dd_async_context;
    if (!ctx) {
      return new Response("missing", { status: 500 });
    }

    if (url.pathname === "/promise") {
      return await ctx.run({ label: "outer" }, async () => {
        await Promise.resolve();
        return new Response(String(ctx.getStore()?.label ?? "missing"));
      });
    }

    if (url.pathname === "/nested") {
      return await ctx.run({ label: "outer" }, async () => {
        const before = String(ctx.getStore()?.label ?? "missing");
        const inner = await ctx.run({ label: "inner" }, async () => {
          await Promise.resolve();
          return String(ctx.getStore()?.label ?? "missing");
        });
        const after = String(ctx.getStore()?.label ?? "missing");
        return new Response(`${before}:${inner}:${after}`);
      });
    }

    if (url.pathname === "/restore") {
      const before = String(ctx.getStore()?.label ?? "missing");
      await ctx.run({ label: "temp" }, async () => {
        await Promise.resolve();
      });
      const after = String(ctx.getStore()?.label ?? "missing");
      return new Response(`${before}:${after}`);
    }

    return new Response("not found", { status: 404 });
  },
};
"#
        .to_string()
    }

    fn trace_sink_worker() -> String {
        r#"
export default {
  async fetch(request) {
    return new Response("ok");
  },
};
"#
        .to_string()
    }

    fn loop_trace_worker() -> String {
        r#"
let totalCalls = 0;
let traceCalls = 0;

export default {
  async fetch(request) {
    totalCalls += 1;
    const path = new URL(request.url).pathname;
    if (path === "/trace") {
      traceCalls += 1;
      return new Response("ok");
    }
    if (path === "/state") {
      return new Response(
        JSON.stringify({ total_calls: totalCalls, trace_calls: traceCalls }),
        { headers: [["content-type", "application/json"]] }
      );
    }
    return new Response("ok");
  },
};
"#
        .to_string()
    }

    #[derive(Deserialize)]
    struct LoopTraceState {
        total_calls: usize,
        trace_calls: usize,
    }

    #[derive(Deserialize)]
    struct FrozenTimeState {
        now0: i64,
        now1: i64,
        now2: i64,
        perf0: f64,
        perf1: f64,
        perf2: f64,
        guard: i64,
    }

    #[derive(Deserialize)]
    struct CryptoState {
        random_length: usize,
        random_non_zero: bool,
        uuid: String,
        digest_length: usize,
    }

    async fn test_service(config: RuntimeConfig) -> RuntimeService {
        let db_path = format!("/tmp/dd-test-{}.db", Uuid::new_v4());
        let store_dir = format!("/tmp/dd-store-{}", Uuid::new_v4());
        RuntimeService::start_with_service_config(RuntimeServiceConfig {
            runtime: config,
            storage: RuntimeStorageConfig {
                store_dir: PathBuf::from(&store_dir),
                database_url: format!("file:{db_path}"),
                actor_shards_per_namespace: 64,
                worker_store_enabled: false,
                blob_store: BlobStoreConfig::local(PathBuf::from(&store_dir).join("blobs")),
            },
        })
        .await
        .expect("service should start")
    }

    #[tokio::test]
    #[serial]
    async fn service_starts_with_deno_runtime_bootstrap() {
        let _ = test_service(RuntimeConfig {
            min_isolates: 0,
            max_isolates: 1,
            max_inflight_per_isolate: 1,
            idle_ttl: Duration::from_secs(5),
            scale_tick: Duration::from_millis(50),
            queue_warn_thresholds: vec![10],
            ..RuntimeConfig::default()
        })
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn service_can_deploy_simple_worker_with_deno_runtime_bootstrap() {
        let service = test_service(RuntimeConfig {
            min_isolates: 0,
            max_isolates: 1,
            max_inflight_per_isolate: 1,
            idle_ttl: Duration::from_secs(5),
            scale_tick: Duration::from_millis(50),
            queue_warn_thresholds: vec![10],
            ..RuntimeConfig::default()
        })
        .await;

        service
            .deploy(
                "simple-deno-worker".to_string(),
                r#"
                export default {
                  async fetch() {
                    return new Response("ok");
                  }
                };
                "#
                .to_string(),
            )
            .await
            .expect("deploy should succeed");
    }

    #[tokio::test]
    #[serial]
    async fn service_can_invoke_simple_worker_with_deno_runtime_bootstrap() {
        let service = test_service(RuntimeConfig {
            min_isolates: 0,
            max_isolates: 1,
            max_inflight_per_isolate: 1,
            idle_ttl: Duration::from_secs(5),
            scale_tick: Duration::from_millis(50),
            queue_warn_thresholds: vec![10],
            ..RuntimeConfig::default()
        })
        .await;

        service
            .deploy(
                "simple-deno-invoke".to_string(),
                r#"
                export default {
                  async fetch() {
                    return new Response("ok");
                  }
                };
                "#
                .to_string(),
            )
            .await
            .expect("deploy should succeed");

        let output = service
            .invoke("simple-deno-invoke".to_string(), test_invocation())
            .await
            .expect("invoke should succeed");
        assert_eq!(output.status, 200);
        assert_eq!(
            String::from_utf8(output.body).expect("body should be utf8"),
            "ok"
        );
    }

    #[tokio::test]
    #[serial]
    async fn reuse_preserves_state() {
        let service = test_service(RuntimeConfig {
            min_isolates: 0,
            max_isolates: 2,
            max_inflight_per_isolate: 4,
            idle_ttl: Duration::from_secs(5),
            scale_tick: Duration::from_millis(50),
            queue_warn_thresholds: vec![10],
            ..RuntimeConfig::default()
        })
        .await;

        service
            .deploy("counter".to_string(), counter_worker())
            .await
            .expect("deploy should succeed");

        let one = service
            .invoke("counter".to_string(), test_invocation())
            .await
            .expect("first invoke should succeed");
        let two = service
            .invoke("counter".to_string(), test_invocation())
            .await
            .expect("second invoke should succeed");

        assert_eq!(String::from_utf8(one.body).expect("utf8"), "1");
        assert_eq!(String::from_utf8(two.body).expect("utf8"), "2");
    }

    #[tokio::test]
    #[serial]
    async fn spectre_time_mitigation_freezes_time_between_io_boundaries() {
        let service = test_service(RuntimeConfig {
            min_isolates: 0,
            max_isolates: 1,
            max_inflight_per_isolate: 1,
            idle_ttl: Duration::from_secs(5),
            scale_tick: Duration::from_millis(50),
            queue_warn_thresholds: vec![10],
            ..RuntimeConfig::default()
        })
        .await;

        service
            .deploy("frozen-time".to_string(), frozen_time_worker())
            .await
            .expect("deploy should succeed");

        let output = service
            .invoke("frozen-time".to_string(), test_invocation())
            .await
            .expect("invoke should succeed");
        let payload: FrozenTimeState = crate::json::from_string(
            String::from_utf8(output.body).expect("frozen-time body should be utf8"),
        )
        .expect("frozen-time response should parse");

        assert_eq!(
            payload.now0, payload.now1,
            "Date.now should remain frozen during pure compute"
        );
        assert_eq!(
            payload.perf0, payload.perf1,
            "performance.now should remain frozen during pure compute"
        );
        assert!(
            payload.now2 >= payload.now1,
            "Date.now should not move backwards across I/O boundaries"
        );
        assert!(
            payload.perf2 >= payload.perf1,
            "performance.now should not move backwards across I/O boundaries"
        );
        assert!(
            payload.now2 > payload.now1 || payload.perf2 > payload.perf1,
            "expected frozen clocks to advance after an I/O boundary"
        );
        assert!(payload.guard > 0, "worker should run local compute loop");
    }

    #[tokio::test]
    #[serial]
    async fn crypto_globals_work_with_deno_crypto_ops() {
        let service = test_service(RuntimeConfig {
            min_isolates: 0,
            max_isolates: 1,
            max_inflight_per_isolate: 1,
            idle_ttl: Duration::from_secs(5),
            scale_tick: Duration::from_millis(50),
            queue_warn_thresholds: vec![10],
            ..RuntimeConfig::default()
        })
        .await;

        service
            .deploy("crypto-worker".to_string(), crypto_worker())
            .await
            .expect("deploy should succeed");

        let output = service
            .invoke("crypto-worker".to_string(), test_invocation())
            .await
            .expect("invoke should succeed");
        let payload: CryptoState =
            crate::json::from_string(String::from_utf8(output.body).expect("body should be utf8"))
                .expect("response should parse");

        assert_eq!(payload.random_length, 16);
        assert!(
            payload.random_non_zero,
            "random bytes should not be all zero"
        );
        assert_eq!(payload.digest_length, 32);
        assert_eq!(payload.uuid.len(), 36, "uuid should be canonical v4 length");
    }

    #[tokio::test]
    #[serial]
    async fn dynamic_namespace_can_create_and_invoke_dynamic_workers() {
        let service = test_service(RuntimeConfig {
            min_isolates: 0,
            max_isolates: 2,
            max_inflight_per_isolate: 4,
            idle_ttl: Duration::from_secs(5),
            scale_tick: Duration::from_millis(50),
            queue_warn_thresholds: vec![10],
            ..RuntimeConfig::default()
        })
        .await;

        service
            .deploy_with_config(
                "dynamic-parent".to_string(),
                dynamic_namespace_worker(),
                DeployConfig {
                    bindings: vec![DeployBinding::Dynamic {
                        binding: "SANDBOX".to_string(),
                    }],
                    ..DeployConfig::default()
                },
            )
            .await
            .expect("deploy should succeed");

        let one = service
            .invoke("dynamic-parent".to_string(), test_invocation())
            .await
            .expect("first invoke should succeed");
        let two = service
            .invoke("dynamic-parent".to_string(), test_invocation())
            .await
            .expect("second invoke should succeed");

        assert_eq!(String::from_utf8(one.body).expect("utf8"), "1:1");
        assert_eq!(String::from_utf8(two.body).expect("utf8"), "2:2");
    }

    #[tokio::test]
    #[serial]
    async fn dynamic_namespace_supports_list_delete_and_timeout() {
        let service = test_service(RuntimeConfig {
            min_isolates: 0,
            max_isolates: 2,
            max_inflight_per_isolate: 2,
            idle_ttl: Duration::from_secs(5),
            scale_tick: Duration::from_millis(50),
            queue_warn_thresholds: vec![10],
            ..RuntimeConfig::default()
        })
        .await;

        service
            .deploy_with_config(
                "dynamic-admin".to_string(),
                dynamic_namespace_admin_worker(),
                DeployConfig {
                    bindings: vec![DeployBinding::Dynamic {
                        binding: "SANDBOX".to_string(),
                    }],
                    ..DeployConfig::default()
                },
            )
            .await
            .expect("deploy should succeed");

        let ensure = service
            .invoke(
                "dynamic-admin".to_string(),
                test_invocation_with_path("/ensure", "dyn-admin-ensure"),
            )
            .await
            .expect("ensure should succeed");
        assert_eq!(ensure.status, 200);

        let ids = service
            .invoke(
                "dynamic-admin".to_string(),
                test_invocation_with_path("/ids", "dyn-admin-ids-1"),
            )
            .await
            .expect("ids should succeed");
        assert_eq!(String::from_utf8(ids.body).expect("utf8"), "[\"admin:v1\"]");

        let deleted = service
            .invoke(
                "dynamic-admin".to_string(),
                test_invocation_with_path("/delete", "dyn-admin-delete"),
            )
            .await
            .expect("delete should succeed");
        assert_eq!(String::from_utf8(deleted.body).expect("utf8"), "true");

        let ids_after = service
            .invoke(
                "dynamic-admin".to_string(),
                test_invocation_with_path("/ids", "dyn-admin-ids-2"),
            )
            .await
            .expect("ids after delete should succeed");
        assert_eq!(String::from_utf8(ids_after.body).expect("utf8"), "[]");

        let timeout_out = service
            .invoke(
                "dynamic-admin".to_string(),
                test_invocation_with_path("/timeout", "dyn-admin-timeout"),
            )
            .await
            .expect("timeout invoke should succeed");
        assert_eq!(timeout_out.status, 504);
    }

    #[tokio::test]
    #[serial]
    async fn dynamic_namespace_child_can_return_json_response() {
        let service = test_service(RuntimeConfig {
            min_isolates: 0,
            max_isolates: 2,
            max_inflight_per_isolate: 2,
            idle_ttl: Duration::from_secs(5),
            scale_tick: Duration::from_millis(50),
            queue_warn_thresholds: vec![10],
            ..RuntimeConfig::default()
        })
        .await;

        service
            .deploy_with_config(
                "dynamic-parent-json".to_string(),
                r#"
let child = null;

export default {
  async fetch(_request, env) {
    if (!child) {
      child = await env.SANDBOX.get("json-child", async () => ({
        entrypoint: "worker.js",
        modules: {
          "worker.js": "export default { async fetch() { return Response.json({ ok: true }, { status: 201 }); } };",
        },
        timeout: 1500,
      }));
    }
    return child.fetch("http://worker/");
  },
};
"#
                .to_string(),
                DeployConfig {
                    bindings: vec![DeployBinding::Dynamic {
                        binding: "SANDBOX".to_string(),
                    }],
                    ..DeployConfig::default()
                },
            )
            .await
            .expect("deploy should succeed");

        let output = service
            .invoke("dynamic-parent-json".to_string(), test_invocation())
            .await
            .expect("invoke should succeed");

        assert_eq!(output.status, 201);
        assert_eq!(
            String::from_utf8(output.body).expect("utf8"),
            r#"{"ok":true}"#
        );
        assert!(output
            .headers
            .iter()
            .any(|(name, value)| name.eq_ignore_ascii_case("content-type")
                && value == "application/json"));
    }

    #[tokio::test]
    #[serial]
    async fn dynamic_namespace_host_rpc_works_with_single_inflight_parent_isolate() {
        let service = test_service(RuntimeConfig {
            min_isolates: 1,
            max_isolates: 1,
            max_inflight_per_isolate: 1,
            idle_ttl: Duration::from_secs(5),
            scale_tick: Duration::from_millis(50),
            queue_warn_thresholds: vec![10],
            ..RuntimeConfig::default()
        })
        .await;

        service
            .deploy_with_config(
                "dynamic-parent".to_string(),
                dynamic_namespace_worker(),
                DeployConfig {
                    bindings: vec![DeployBinding::Dynamic {
                        binding: "SANDBOX".to_string(),
                    }],
                    ..DeployConfig::default()
                },
            )
            .await
            .expect("deploy should succeed");

        let output = timeout(Duration::from_secs(2), async {
            service
                .invoke("dynamic-parent".to_string(), test_invocation())
                .await
        })
        .await
        .expect("dynamic invoke should not deadlock")
        .expect("dynamic invoke should succeed");

        assert_eq!(String::from_utf8(output.body).expect("utf8"), "1:1");
    }

    #[tokio::test]
    #[serial]
    async fn dynamic_namespace_child_can_use_response_json() {
        let service = test_service(RuntimeConfig {
            min_isolates: 1,
            max_isolates: 2,
            max_inflight_per_isolate: 4,
            idle_ttl: Duration::from_secs(5),
            scale_tick: Duration::from_millis(50),
            queue_warn_thresholds: vec![10],
            ..RuntimeConfig::default()
        })
        .await;

        service
            .deploy_with_config(
                "dynamic-json".to_string(),
                dynamic_response_json_worker(),
                DeployConfig {
                    public: false,
                    internal: DeployInternalConfig { trace: None },
                    bindings: vec![DeployBinding::Dynamic {
                        binding: "SANDBOX".to_string(),
                    }],
                },
            )
            .await
            .expect("deploy should succeed");

        let output = service
            .invoke("dynamic-json".to_string(), test_invocation())
            .await
            .expect("invoke should succeed");
        assert_eq!(output.status, 200);
        assert_eq!(
            String::from_utf8(output.body).expect("utf8"),
            r#"{"ok":true,"source":"dynamic-child"}"#
        );
    }

    #[tokio::test]
    #[serial]
    async fn normal_and_dynamic_workers_expose_same_runtime_surface() {
        let service = test_service(RuntimeConfig {
            min_isolates: 1,
            max_isolates: 2,
            max_inflight_per_isolate: 4,
            idle_ttl: Duration::from_secs(5),
            scale_tick: Duration::from_millis(50),
            queue_warn_thresholds: vec![10],
            ..RuntimeConfig::default()
        })
        .await;

        service
            .deploy_with_config(
                "dynamic-surface".to_string(),
                dynamic_runtime_surface_worker(),
                DeployConfig {
                    public: false,
                    internal: DeployInternalConfig { trace: None },
                    bindings: vec![DeployBinding::Dynamic {
                        binding: "SANDBOX".to_string(),
                    }],
                },
            )
            .await
            .expect("deploy should succeed");

        let output = service
            .invoke("dynamic-surface".to_string(), test_invocation())
            .await
            .expect("invoke should succeed");
        assert_eq!(output.status, 200);
        let body = String::from_utf8(output.body).expect("utf8");
        assert!(body.contains(r#""same":true"#), "body was {body}");
        assert!(body.contains(r#""fetch":true"#), "body was {body}");
        assert!(body.contains(r#""request":true"#), "body was {body}");
        assert!(body.contains(r#""response":true"#), "body was {body}");
        assert!(body.contains(r#""headers":true"#), "body was {body}");
        assert!(body.contains(r#""formData":true"#), "body was {body}");
        assert!(
            body.contains(r#""responseJsonType":"application/json""#),
            "body was {body}"
        );
    }

    #[tokio::test]
    #[serial]
    async fn transport_open_works_with_deno_request_compatibility() {
        let service = test_service(RuntimeConfig {
            min_isolates: 1,
            max_isolates: 2,
            max_inflight_per_isolate: 4,
            idle_ttl: Duration::from_secs(5),
            scale_tick: Duration::from_millis(50),
            queue_warn_thresholds: vec![10],
            ..RuntimeConfig::default()
        })
        .await;

        service
            .deploy_with_config(
                "transport-runtime".to_string(),
                transport_echo_worker(),
                DeployConfig {
                    public: false,
                    internal: DeployInternalConfig { trace: None },
                    bindings: vec![DeployBinding::Actor {
                        binding: "MEDIA".to_string(),
                    }],
                },
            )
            .await
            .expect("deploy should succeed");

        let (stream_tx, mut stream_rx) = mpsc::unbounded_channel();
        let (datagram_tx, _datagram_rx) = mpsc::unbounded_channel();
        let opened = service
            .open_transport(
                "transport-runtime".to_string(),
                test_transport_invocation(),
                stream_tx,
                datagram_tx,
            )
            .await
            .expect("transport open should succeed");

        assert_eq!(opened.output.status, 200);

        service
            .transport_push_stream(
                "transport-runtime".to_string(),
                opened.session_id.clone(),
                b"hello-transport".to_vec(),
                false,
            )
            .await
            .expect("transport push should succeed");

        let echoed = timeout(Duration::from_secs(2), stream_rx.recv())
            .await
            .expect("stream echo should arrive")
            .expect("stream echo channel should stay open");
        assert_eq!(echoed, b"hello-transport");

        service
            .transport_close(
                "transport-runtime".to_string(),
                opened.session_id,
                0,
                "done".to_string(),
            )
            .await
            .expect("transport close should succeed");
    }

    #[tokio::test]
    #[serial]
    async fn transport_open_preserves_connect_shape_for_actor_code() {
        let service = test_service(RuntimeConfig {
            min_isolates: 1,
            max_isolates: 2,
            max_inflight_per_isolate: 4,
            idle_ttl: Duration::from_secs(5),
            scale_tick: Duration::from_millis(50),
            queue_warn_thresholds: vec![10],
            ..RuntimeConfig::default()
        })
        .await;

        service
            .deploy_with_config(
                "transport-shape".to_string(),
                transport_shape_worker(),
                DeployConfig {
                    public: false,
                    internal: DeployInternalConfig { trace: None },
                    bindings: vec![DeployBinding::Actor {
                        binding: "MEDIA".to_string(),
                    }],
                },
            )
            .await
            .expect("deploy should succeed");

        let (stream_tx, _stream_rx) = mpsc::unbounded_channel();
        let (datagram_tx, _datagram_rx) = mpsc::unbounded_channel();
        let opened = service
            .open_transport(
                "transport-shape".to_string(),
                test_transport_invocation(),
                stream_tx,
                datagram_tx,
            )
            .await
            .expect("transport open should succeed");

        assert_eq!(opened.output.status, 200);
    }

    #[tokio::test]
    #[serial]
    async fn websocket_message_handler_can_use_actor_storage_after_handshake() {
        let service = test_service(RuntimeConfig {
            min_isolates: 1,
            max_isolates: 2,
            max_inflight_per_isolate: 4,
            idle_ttl: Duration::from_secs(5),
            scale_tick: Duration::from_millis(50),
            queue_warn_thresholds: vec![10],
            ..RuntimeConfig::default()
        })
        .await;

        service
            .deploy_with_config(
                "ws-storage".to_string(),
                websocket_storage_worker(),
                DeployConfig {
                    public: false,
                    internal: DeployInternalConfig { trace: None },
                    bindings: vec![DeployBinding::Actor {
                        binding: "CHAT".to_string(),
                    }],
                },
            )
            .await
            .expect("deploy should succeed");

        let opened = service
            .open_websocket(
                "ws-storage".to_string(),
                test_websocket_invocation("/ws", "ws-open"),
                None,
            )
            .await
            .expect("websocket open should succeed");

        assert_eq!(opened.output.status, 101);

        let echoed = service
            .websocket_send_frame(
                "ws-storage".to_string(),
                opened.session_id.clone(),
                b"ready".to_vec(),
                false,
            )
            .await
            .expect("websocket message should succeed");
        assert_eq!(echoed.status, 204);
        assert_eq!(
            String::from_utf8(echoed.body).expect("utf8"),
            r#"{"seen":"ready","count":1}"#
        );

        let state = service
            .invoke(
                "ws-storage".to_string(),
                test_invocation_with_path("/state", "ws-state"),
            )
            .await
            .expect("state invoke should succeed");
        assert_eq!(state.status, 200);
        let state_json: serde_json::Value =
            serde_json::from_slice(&state.body).expect("state body should be json");
        assert_eq!(state_json["count"], 1);
        assert_eq!(state_json["last"], "ready");

        service
            .websocket_close(
                "ws-storage".to_string(),
                opened.session_id,
                1000,
                "done".to_string(),
            )
            .await
            .expect("websocket close should succeed");
    }

    #[tokio::test]
    #[serial]
    async fn websocket_storage_uses_current_request_scope_on_warm_actor_instance() {
        let service = test_service(RuntimeConfig {
            min_isolates: 1,
            max_isolates: 2,
            max_inflight_per_isolate: 4,
            idle_ttl: Duration::from_secs(5),
            scale_tick: Duration::from_millis(50),
            queue_warn_thresholds: vec![10],
            ..RuntimeConfig::default()
        })
        .await;

        service
            .deploy_with_config(
                "ws-storage".to_string(),
                websocket_storage_worker(),
                DeployConfig {
                    public: false,
                    internal: DeployInternalConfig { trace: None },
                    bindings: vec![DeployBinding::Actor {
                        binding: "CHAT".to_string(),
                    }],
                },
            )
            .await
            .expect("deploy should succeed");

        let opened = service
            .open_websocket(
                "ws-storage".to_string(),
                test_websocket_invocation("/ws", "ws-open-warm"),
                None,
            )
            .await
            .expect("websocket open should succeed");

        let first = service
            .websocket_send_frame(
                "ws-storage".to_string(),
                opened.session_id.clone(),
                b"first".to_vec(),
                false,
            )
            .await
            .expect("first websocket message should succeed");
        assert_eq!(
            String::from_utf8(first.body).expect("utf8"),
            r#"{"seen":"first","count":1}"#
        );

        let second = service
            .websocket_send_frame(
                "ws-storage".to_string(),
                opened.session_id.clone(),
                b"second".to_vec(),
                false,
            )
            .await
            .expect("second websocket message should succeed");
        assert_eq!(
            String::from_utf8(second.body).expect("utf8"),
            r#"{"seen":"second","count":2}"#
        );

        service
            .websocket_close(
                "ws-storage".to_string(),
                opened.session_id,
                1000,
                "done".to_string(),
            )
            .await
            .expect("websocket close should succeed");
    }

    #[tokio::test]
    #[serial]
    async fn chat_worker_second_join_and_message_do_not_hang() {
        let service = test_service(RuntimeConfig {
            min_isolates: 1,
            max_isolates: 2,
            max_inflight_per_isolate: 4,
            idle_ttl: Duration::from_secs(5),
            scale_tick: Duration::from_millis(50),
            queue_warn_thresholds: vec![10],
            ..RuntimeConfig::default()
        })
        .await;

        service
            .deploy_with_config(
                "chat".to_string(),
                include_str!("../../../examples/chat-worker/src/worker.js").to_string(),
                DeployConfig {
                    public: false,
                    internal: DeployInternalConfig { trace: None },
                    bindings: vec![DeployBinding::Actor {
                        binding: "CHAT_ROOM".to_string(),
                    }],
                },
            )
            .await
            .expect("deploy should succeed");

        let alice = tokio::time::timeout(
            Duration::from_secs(5),
            service.open_websocket(
                "chat".to_string(),
                test_websocket_invocation(
                    "/rooms/test/ws?username=alice&participant=alice",
                    "chat-open-alice",
                ),
                None,
            ),
        )
        .await
        .expect("alice websocket open should not hang")
        .expect("alice websocket open should succeed");
        assert_eq!(alice.output.status, 101);

        let alice_ready = tokio::time::timeout(
            Duration::from_secs(5),
            service.websocket_send_frame(
                "chat".to_string(),
                alice.session_id.clone(),
                br#"{"type":"ready"}"#.to_vec(),
                false,
            ),
        )
        .await
        .expect("alice ready should not hang")
        .expect("alice ready should succeed");
        assert_eq!(alice_ready.status, 204);

        let bob = tokio::time::timeout(
            Duration::from_secs(5),
            service.open_websocket(
                "chat".to_string(),
                test_websocket_invocation(
                    "/rooms/test/ws?username=bob&participant=bob",
                    "chat-open-bob",
                ),
                None,
            ),
        )
        .await
        .expect("bob websocket open should not hang")
        .expect("bob websocket open should succeed");
        assert_eq!(bob.output.status, 101);

        let bob_ready = tokio::time::timeout(
            Duration::from_secs(5),
            service.websocket_send_frame(
                "chat".to_string(),
                bob.session_id.clone(),
                br#"{"type":"ready"}"#.to_vec(),
                false,
            ),
        )
        .await
        .expect("bob ready should not hang")
        .expect("bob ready should succeed");
        assert_eq!(bob_ready.status, 204);
        let bob_ready_body = String::from_utf8(bob_ready.body).expect("utf8");
        assert!(
            bob_ready_body.contains("alice"),
            "bob ready payload should include alice: {bob_ready_body}"
        );

        tokio::time::timeout(
            Duration::from_secs(5),
            service.websocket_wait_frame("chat".to_string(), alice.session_id.clone()),
        )
        .await
        .expect("alice participant update should not hang")
        .expect("alice participant update should succeed");
        let alice_participants = tokio::time::timeout(
            Duration::from_secs(5),
            service.websocket_drain_frame("chat".to_string(), alice.session_id.clone()),
        )
        .await
        .expect("alice participant drain should not hang")
        .expect("alice participant drain should succeed")
        .expect("alice should have a pending participant update");
        let alice_participants_body =
            String::from_utf8(alice_participants.body).expect("participant payload utf8");
        assert!(
            alice_participants_body.contains("bob"),
            "alice participant payload should include bob: {alice_participants_body}"
        );

        let alice_message = tokio::time::timeout(
            Duration::from_secs(5),
            service.websocket_send_frame(
                "chat".to_string(),
                alice.session_id.clone(),
                br#"{"type":"message","text":"hello"}"#.to_vec(),
                false,
            ),
        )
        .await
        .expect("alice message should not hang")
        .expect("alice message should succeed");
        assert_eq!(alice_message.status, 204);
        let alice_message_body = String::from_utf8(alice_message.body).expect("utf8");
        assert!(
            alice_message_body.contains("hello"),
            "alice message payload should include the sent message: {alice_message_body}"
        );

        tokio::time::timeout(
            Duration::from_secs(5),
            service.websocket_wait_frame("chat".to_string(), bob.session_id.clone()),
        )
        .await
        .expect("bob message update should not hang")
        .expect("bob message update should succeed");
        let bob_message = tokio::time::timeout(
            Duration::from_secs(5),
            service.websocket_drain_frame("chat".to_string(), bob.session_id.clone()),
        )
        .await
        .expect("bob message drain should not hang")
        .expect("bob message drain should succeed")
        .expect("bob should have a pending message update");
        let bob_message_body = String::from_utf8(bob_message.body).expect("utf8");
        assert!(
            bob_message_body.contains("hello"),
            "bob message payload should include the sent message: {bob_message_body}"
        );

        service
            .websocket_close(
                "chat".to_string(),
                alice.session_id,
                1000,
                "done".to_string(),
            )
            .await
            .expect("alice websocket close should succeed");
        service
            .websocket_close("chat".to_string(), bob.session_id, 1000, "done".to_string())
            .await
            .expect("bob websocket close should succeed");
    }

    #[tokio::test]
    #[serial]
    async fn chat_worker_refresh_replaces_prior_participant_socket() {
        let service = test_service(RuntimeConfig {
            min_isolates: 1,
            max_isolates: 2,
            max_inflight_per_isolate: 4,
            idle_ttl: Duration::from_secs(5),
            scale_tick: Duration::from_millis(50),
            queue_warn_thresholds: vec![10],
            ..RuntimeConfig::default()
        })
        .await;

        service
            .deploy_with_config(
                "chat".to_string(),
                include_str!("../../../examples/chat-worker/src/worker.js").to_string(),
                DeployConfig {
                    public: false,
                    internal: DeployInternalConfig { trace: None },
                    bindings: vec![DeployBinding::Actor {
                        binding: "CHAT_ROOM".to_string(),
                    }],
                },
            )
            .await
            .expect("deploy should succeed");

        let first = tokio::time::timeout(
            Duration::from_secs(5),
            service.open_websocket(
                "chat".to_string(),
                test_websocket_invocation(
                    "/rooms/test/ws?username=alice&participant=alice",
                    "chat-refresh-open-1",
                ),
                None,
            ),
        )
        .await
        .expect("first websocket open should not hang")
        .expect("first websocket open should succeed");
        assert_eq!(first.output.status, 101);

        let second = tokio::time::timeout(
            Duration::from_secs(5),
            service.open_websocket(
                "chat".to_string(),
                test_websocket_invocation(
                    "/rooms/test/ws?username=alice&participant=alice",
                    "chat-refresh-open-2",
                ),
                None,
            ),
        )
        .await
        .expect("refreshed websocket open should not hang")
        .expect("refreshed websocket open should succeed");
        assert_eq!(second.output.status, 101);

        let state = service
            .invoke(
                "chat".to_string(),
                test_invocation_with_path("/rooms/test/state", "chat-refresh-state"),
            )
            .await
            .expect("state invoke should succeed");
        assert_eq!(state.status, 200);
        let state_json: serde_json::Value =
            serde_json::from_slice(&state.body).expect("state body should be json");
        let participants = state_json["participants"]
            .as_array()
            .expect("participants should be array");
        assert_eq!(participants.len(), 1);
        assert_eq!(participants[0]["id"], "alice");

        let refreshed_ready = tokio::time::timeout(
            Duration::from_secs(5),
            service.websocket_send_frame(
                "chat".to_string(),
                second.session_id.clone(),
                br#"{"type":"ready"}"#.to_vec(),
                false,
            ),
        )
        .await
        .expect("refreshed ready should not hang")
        .expect("refreshed ready should succeed");
        assert_eq!(refreshed_ready.status, 204);

        let refreshed_message = tokio::time::timeout(
            Duration::from_secs(5),
            service.websocket_send_frame(
                "chat".to_string(),
                second.session_id.clone(),
                br#"{"type":"message","text":"after-refresh"}"#.to_vec(),
                false,
            ),
        )
        .await
        .expect("refreshed message should not hang")
        .expect("refreshed message should succeed");
        let refreshed_message_body =
            String::from_utf8(refreshed_message.body).expect("message payload utf8");
        assert!(
            refreshed_message_body.contains("after-refresh"),
            "refreshed message payload should include the sent message: {refreshed_message_body}"
        );

        service
            .websocket_close(
                "chat".to_string(),
                second.session_id,
                1000,
                "done".to_string(),
            )
            .await
            .expect("refreshed websocket close should succeed");
        let _ = service
            .websocket_close(
                "chat".to_string(),
                first.session_id,
                1000,
                "done".to_string(),
            )
            .await;
    }

    #[tokio::test]
    #[serial]
    async fn dynamic_worker_fetch_uses_deno_fetch_with_host_policy_and_secret_replacement() {
        let service = test_service(RuntimeConfig {
            min_isolates: 1,
            max_isolates: 2,
            max_inflight_per_isolate: 4,
            idle_ttl: Duration::from_secs(5),
            scale_tick: Duration::from_millis(50),
            queue_warn_thresholds: vec![10],
            ..RuntimeConfig::default()
        })
        .await;

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let address = listener.local_addr().expect("listener should have addr");
        let (request_tx, request_rx) = tokio::sync::oneshot::channel::<String>();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept should succeed");
            let mut buffer = vec![0_u8; 8192];
            let bytes_read = socket
                .read(&mut buffer)
                .await
                .expect("server read should succeed");
            request_tx
                .send(String::from_utf8_lossy(&buffer[..bytes_read]).to_string())
                .expect("request should be captured");
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\ncontent-length: 2\r\nconnection: close\r\n\r\nok",
                )
                .await
                .expect("server write should succeed");
        });

        let deployed = service
            .deploy_dynamic(
                dynamic_fetch_probe_worker(&format!("http://{address}/fetch-probe")),
                HashMap::from([("API_TOKEN".to_string(), "secret-value".to_string())]),
                vec!["127.0.0.1".to_string()],
            )
            .await
            .expect("dynamic deploy should succeed");

        let output = service
            .invoke(deployed.worker, test_invocation())
            .await
            .expect("dynamic fetch invoke should succeed");
        assert_eq!(output.status, 200);
        assert_eq!(String::from_utf8(output.body).expect("utf8"), "ok");

        let raw_request = request_rx.await.expect("request should arrive");
        assert!(
            raw_request.starts_with("GET /fetch-probe?token=secret-value HTTP/1.1\r\n"),
            "raw request was {raw_request}"
        );
        assert!(
            raw_request.contains("\r\nauthorization: Bearer secret-value\r\n"),
            "raw request was {raw_request}"
        );
        assert!(
            raw_request.contains("\r\nx-dd-secret: secret-value\r\n"),
            "raw request was {raw_request}"
        );
        assert!(
            !raw_request.contains("__DD_SECRET_"),
            "secret placeholders leaked into outbound request: {raw_request}"
        );
    }

    #[tokio::test]
    #[serial]
    async fn dynamic_worker_fetch_rejects_egress_hosts_outside_allowlist() {
        let service = test_service(RuntimeConfig {
            min_isolates: 1,
            max_isolates: 2,
            max_inflight_per_isolate: 4,
            idle_ttl: Duration::from_secs(5),
            scale_tick: Duration::from_millis(50),
            queue_warn_thresholds: vec![10],
            ..RuntimeConfig::default()
        })
        .await;

        let deployed = service
            .deploy_dynamic(
                dynamic_fetch_probe_worker("http://127.0.0.1:9/blocked"),
                HashMap::from([("API_TOKEN".to_string(), "secret-value".to_string())]),
                vec!["example.com".to_string()],
            )
            .await
            .expect("dynamic deploy should succeed");

        let error = service
            .invoke(deployed.worker, test_invocation())
            .await
            .expect_err("dynamic fetch invoke should fail");
        let body = error.to_string();
        assert!(
            body.contains("egress host is not allowed: 127.0.0.1"),
            "body was {body}"
        );
    }

    #[tokio::test]
    #[serial]
    async fn dynamic_worker_fetch_abort_signal_cancels_outbound_request() {
        let service = test_service(RuntimeConfig {
            min_isolates: 1,
            max_isolates: 2,
            max_inflight_per_isolate: 4,
            idle_ttl: Duration::from_secs(5),
            scale_tick: Duration::from_millis(50),
            queue_warn_thresholds: vec![10],
            ..RuntimeConfig::default()
        })
        .await;

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let address = listener.local_addr().expect("listener should have addr");
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept should succeed");
            let mut buffer = vec![0_u8; 4096];
            let _ = socket.read(&mut buffer).await;
            sleep(Duration::from_millis(200)).await;
            let _ = socket.shutdown().await;
        });

        let deployed = service
            .deploy_dynamic(
                dynamic_fetch_abort_worker(&format!("http://{address}/abort-probe")),
                HashMap::new(),
                vec!["127.0.0.1".to_string()],
            )
            .await
            .expect("dynamic deploy should succeed");

        let started_at = Instant::now();
        let output = timeout(
            Duration::from_secs(2),
            service.invoke(deployed.worker, test_invocation()),
        )
        .await
        .expect("invoke should not hang")
        .expect("invoke should succeed");
        assert_eq!(output.status, 200);
        let body = String::from_utf8(output.body).expect("utf8");
        assert!(
            body == "Error"
                || body.contains("Abort")
                || body.to_ascii_lowercase().contains("abort"),
            "body was {body}"
        );
        assert!(
            started_at.elapsed() < Duration::from_millis(500),
            "abort should finish quickly"
        );
    }

    #[tokio::test]
    #[serial]
    async fn preview_dynamic_worker_can_proxy_module_based_children() {
        let service = test_service(RuntimeConfig {
            min_isolates: 0,
            max_isolates: 2,
            max_inflight_per_isolate: 2,
            idle_ttl: Duration::from_secs(5),
            scale_tick: Duration::from_millis(50),
            queue_warn_thresholds: vec![10],
            ..RuntimeConfig::default()
        })
        .await;

        service
            .deploy_with_config(
                "preview-dynamic".to_string(),
                preview_dynamic_worker(),
                DeployConfig {
                    bindings: vec![DeployBinding::Dynamic {
                        binding: "SANDBOX".to_string(),
                    }],
                    ..DeployConfig::default()
                },
            )
            .await
            .expect("deploy should succeed");

        let root = service
            .invoke(
                "preview-dynamic".to_string(),
                test_invocation_with_path("/preview/pr-123", "preview-root"),
            )
            .await
            .expect("preview root should succeed");
        assert_eq!(root.status, 200);
        let root_text = String::from_utf8(root.body).expect("utf8");
        assert!(root_text.contains("\"preview\":\"pr-123\""));
        assert!(root_text.contains("\"route\":\"root\""));

        let health = service
            .invoke(
                "preview-dynamic".to_string(),
                test_invocation_with_path("/preview/pr-123/api/health", "preview-health"),
            )
            .await
            .expect("preview health should succeed");
        assert_eq!(health.status, 200);
        let health_text = String::from_utf8(health.body).expect("utf8");
        assert!(health_text.contains("\"route\":\"health\""));
    }

    #[tokio::test]
    #[serial]
    async fn scales_up_with_backlog() {
        let service = test_service(RuntimeConfig {
            min_isolates: 0,
            max_isolates: 4,
            max_inflight_per_isolate: 4,
            idle_ttl: Duration::from_secs(5),
            scale_tick: Duration::from_millis(50),
            queue_warn_thresholds: vec![10],
            ..RuntimeConfig::default()
        })
        .await;

        service
            .deploy("slow".to_string(), slow_worker())
            .await
            .expect("deploy should succeed");

        let mut tasks = Vec::new();
        for idx in 0..12 {
            let svc = service.clone();
            tasks.push(tokio::spawn(async move {
                let mut req = test_invocation();
                req.request_id = format!("req-{idx}");
                svc.invoke("slow".to_string(), req).await
            }));
        }

        for task in tasks {
            task.await.expect("join").expect("invoke should succeed");
        }

        let stats = service
            .stats("slow".to_string())
            .await
            .expect("stats should exist");
        assert!(stats.spawn_count > 1);
        assert!(stats.isolates_total <= 4);
    }

    #[tokio::test]
    #[serial]
    async fn scales_down_when_idle() {
        let service = test_service(RuntimeConfig {
            min_isolates: 0,
            max_isolates: 3,
            max_inflight_per_isolate: 4,
            idle_ttl: Duration::from_millis(200),
            scale_tick: Duration::from_millis(50),
            queue_warn_thresholds: vec![10],
            ..RuntimeConfig::default()
        })
        .await;

        service
            .deploy("slow".to_string(), slow_worker())
            .await
            .expect("deploy should succeed");

        for idx in 0..6 {
            let mut req = test_invocation();
            req.request_id = format!("req-{idx}");
            service
                .invoke("slow".to_string(), req)
                .await
                .expect("invoke should succeed");
        }

        let before = service
            .stats("slow".to_string())
            .await
            .expect("stats should exist");
        assert!(before.isolates_total > 0);

        timeout(Duration::from_secs(3), async {
            loop {
                let stats = service.stats("slow".to_string()).await.expect("stats");
                if stats.isolates_total == 0 {
                    break;
                }
                sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("isolates should scale down to zero");
    }

    #[tokio::test]
    #[serial]
    async fn invalid_redeploy_keeps_previous_generation() {
        let service = test_service(RuntimeConfig {
            min_isolates: 0,
            max_isolates: 2,
            max_inflight_per_isolate: 4,
            idle_ttl: Duration::from_secs(5),
            scale_tick: Duration::from_millis(50),
            queue_warn_thresholds: vec![10],
            ..RuntimeConfig::default()
        })
        .await;

        service
            .deploy("counter".to_string(), counter_worker())
            .await
            .expect("initial deploy should succeed");

        let one = service
            .invoke("counter".to_string(), test_invocation())
            .await
            .expect("first invoke should succeed");
        assert_eq!(String::from_utf8(one.body).expect("utf8"), "1");

        let bad_redeploy = service
            .deploy("counter".to_string(), "export default {};".to_string())
            .await;
        assert!(bad_redeploy.is_err());

        let two = service
            .invoke("counter".to_string(), test_invocation())
            .await
            .expect("invoke should still use old generation");
        assert_eq!(String::from_utf8(two.body).expect("utf8"), "2");
    }

    #[tokio::test]
    #[serial]
    async fn redeploy_switches_new_traffic_while_old_generation_drains() {
        let service = test_service(RuntimeConfig {
            min_isolates: 0,
            max_isolates: 1,
            max_inflight_per_isolate: 4,
            idle_ttl: Duration::from_secs(5),
            scale_tick: Duration::from_millis(50),
            queue_warn_thresholds: vec![10],
            ..RuntimeConfig::default()
        })
        .await;

        service
            .deploy("worker".to_string(), versioned_worker("v1", 120))
            .await
            .expect("deploy v1 should succeed");

        let svc_one = service.clone();
        let first = tokio::spawn(async move {
            let mut req = test_invocation();
            req.request_id = "first".to_string();
            svc_one.invoke("worker".to_string(), req).await
        });

        sleep(Duration::from_millis(10)).await;

        let svc_two = service.clone();
        let second = tokio::spawn(async move {
            let mut req = test_invocation();
            req.request_id = "second".to_string();
            svc_two.invoke("worker".to_string(), req).await
        });

        sleep(Duration::from_millis(10)).await;
        service
            .deploy("worker".to_string(), versioned_worker("v2", 0))
            .await
            .expect("deploy v2 should succeed");

        let mut third_req = test_invocation();
        third_req.request_id = "third".to_string();
        let third = service
            .invoke("worker".to_string(), third_req)
            .await
            .expect("third invoke should succeed");
        assert_eq!(String::from_utf8(third.body).expect("utf8"), "v2");

        let first_output = first.await.expect("join first").expect("first invoke");
        let second_output = second.await.expect("join second").expect("second invoke");
        assert_eq!(String::from_utf8(first_output.body).expect("utf8"), "v1");
        assert_eq!(String::from_utf8(second_output.body).expect("utf8"), "v1");
    }

    #[tokio::test]
    #[serial]
    async fn single_isolate_allows_multiple_inflight_requests() {
        let service = test_service(RuntimeConfig {
            min_isolates: 1,
            max_isolates: 1,
            max_inflight_per_isolate: 4,
            idle_ttl: Duration::from_secs(5),
            scale_tick: Duration::from_millis(50),
            queue_warn_thresholds: vec![10],
            ..RuntimeConfig::default()
        })
        .await;

        service
            .deploy("io".to_string(), io_wait_worker())
            .await
            .expect("deploy should succeed");

        let started = Instant::now();
        let mut tasks = Vec::new();
        for idx in 0..2 {
            let svc = service.clone();
            tasks.push(tokio::spawn(async move {
                let mut req = test_invocation();
                req.request_id = format!("io-{idx}");
                svc.invoke("io".to_string(), req).await
            }));
        }

        for task in tasks {
            task.await.expect("join").expect("invoke should succeed");
        }
        let elapsed = started.elapsed();

        assert!(
            elapsed < Duration::from_millis(260),
            "expected multiplexed inflight execution, elapsed={elapsed:?}"
        );
    }

    #[tokio::test]
    #[serial]
    async fn dropped_invoke_aborts_inflight_request() {
        let service = test_service(RuntimeConfig {
            min_isolates: 1,
            max_isolates: 1,
            max_inflight_per_isolate: 1,
            idle_ttl: Duration::from_secs(5),
            scale_tick: Duration::from_millis(50),
            queue_warn_thresholds: vec![10],
            ..RuntimeConfig::default()
        })
        .await;

        service
            .deploy("abortable".to_string(), abort_aware_worker())
            .await
            .expect("deploy should succeed");

        let service_for_blocked = service.clone();
        let blocked = tokio::spawn(async move {
            let mut req = test_invocation();
            req.request_id = "block".to_string();
            service_for_blocked
                .invoke("abortable".to_string(), req)
                .await
        });

        timeout(Duration::from_secs(1), async {
            loop {
                let stats = service.stats("abortable".to_string()).await.expect("stats");
                if stats.inflight_total == 1 {
                    break;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("request should become inflight");

        blocked.abort();
        assert!(blocked.await.is_err(), "aborted task should be canceled");

        timeout(Duration::from_secs(2), async {
            loop {
                let stats = service.stats("abortable".to_string()).await.expect("stats");
                if stats.inflight_total == 0 && stats.queued == 0 {
                    break;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("abort should clear inflight slot");

        let mut followup_req = test_invocation();
        followup_req.request_id = "after".to_string();
        let followup = service
            .invoke("abortable".to_string(), followup_req)
            .await
            .expect("followup invoke should succeed");

        assert_eq!(
            String::from_utf8(followup.body).expect("utf8"),
            "abortCount=1"
        );
    }

    #[tokio::test]
    #[serial]
    async fn duplicate_user_request_ids_do_not_collide() {
        let service = test_service(RuntimeConfig {
            min_isolates: 1,
            max_isolates: 1,
            max_inflight_per_isolate: 4,
            idle_ttl: Duration::from_secs(5),
            scale_tick: Duration::from_millis(50),
            queue_warn_thresholds: vec![10],
            ..RuntimeConfig::default()
        })
        .await;

        service
            .deploy("io".to_string(), io_wait_worker())
            .await
            .expect("deploy should succeed");

        let mut tasks = Vec::new();
        for _ in 0..8 {
            let svc = service.clone();
            tasks.push(tokio::spawn(async move {
                let mut req = test_invocation();
                req.request_id = "same-user-request-id".to_string();
                svc.invoke("io".to_string(), req).await
            }));
        }

        for task in tasks {
            let output = task.await.expect("join").expect("invoke should succeed");
            assert_eq!(String::from_utf8(output.body).expect("utf8"), "ok");
        }
    }

    #[tokio::test]
    #[serial]
    async fn forged_and_invalid_completion_payloads_are_ignored() {
        let service = test_service(RuntimeConfig {
            min_isolates: 1,
            max_isolates: 1,
            max_inflight_per_isolate: 2,
            idle_ttl: Duration::from_secs(5),
            scale_tick: Duration::from_millis(50),
            queue_warn_thresholds: vec![10],
            ..RuntimeConfig::default()
        })
        .await;

        service
            .deploy("malicious".to_string(), malicious_completion_worker())
            .await
            .expect("deploy should succeed");

        let first = service
            .invoke("malicious".to_string(), test_invocation())
            .await
            .expect("first invoke should succeed");
        assert_eq!(String::from_utf8(first.body).expect("utf8"), "1");

        let second = service
            .invoke("malicious".to_string(), test_invocation())
            .await
            .expect("second invoke should succeed");
        assert_eq!(String::from_utf8(second.body).expect("utf8"), "2");
    }

    #[tokio::test]
    #[serial]
    async fn invoke_stream_delivers_chunked_response_body() {
        let service = test_service(RuntimeConfig {
            min_isolates: 1,
            max_isolates: 1,
            max_inflight_per_isolate: 4,
            idle_ttl: Duration::from_secs(5),
            scale_tick: Duration::from_millis(50),
            queue_warn_thresholds: vec![10],
            ..RuntimeConfig::default()
        })
        .await;

        service
            .deploy(
                "streaming".to_string(),
                r#"
export default {
  async fetch() {
    return new Response(new ReadableStream({
      start(controller) {
        controller.enqueue("hel");
        controller.enqueue("lo");
        controller.close();
      }
    }), { status: 201, headers: [["x-mode", "stream"]] });
  },
};
"#
                .to_string(),
            )
            .await
            .expect("deploy should succeed");

        let mut output = service
            .invoke_stream("streaming".to_string(), test_invocation())
            .await
            .expect("invoke stream should succeed");
        assert_eq!(output.status, 201);
        assert!(output
            .headers
            .iter()
            .any(|(name, value)| name == "x-mode" && value == "stream"));

        let mut body = Vec::new();
        while let Some(chunk) = output.body.recv().await {
            body.extend(chunk.expect("chunk should be ok"));
        }
        assert_eq!(String::from_utf8(body).expect("utf8"), "hello");
    }

    #[tokio::test]
    #[serial]
    async fn invoke_with_request_body_stream_delivers_chunks_to_worker() {
        let service = test_service(RuntimeConfig {
            min_isolates: 1,
            max_isolates: 1,
            max_inflight_per_isolate: 1,
            idle_ttl: Duration::from_secs(5),
            scale_tick: Duration::from_millis(50),
            queue_warn_thresholds: vec![10],
            ..RuntimeConfig::default()
        })
        .await;

        service
            .deploy(
                "streaming-body".to_string(),
                streaming_request_body_worker(),
            )
            .await
            .expect("deploy should succeed");

        let (tx, rx) = mpsc::channel(4);
        let mut request = test_invocation();
        request.method = "POST".to_string();
        request.request_id = "streaming-body-request".to_string();

        let invoke_task = {
            let service = service.clone();
            tokio::spawn(async move {
                service
                    .invoke_with_request_body("streaming-body".to_string(), request, Some(rx))
                    .await
            })
        };

        tx.send(Ok(b"hel".to_vec()))
            .await
            .expect("first body chunk should send");
        tx.send(Ok(b"lo".to_vec()))
            .await
            .expect("second body chunk should send");
        drop(tx);

        let output = invoke_task
            .await
            .expect("join")
            .expect("invoke should succeed");
        assert_eq!(String::from_utf8(output.body).expect("utf8"), "hello");
    }

    #[tokio::test]
    #[serial]
    async fn actor_same_key_allows_overlap_by_default() {
        let service = test_service(RuntimeConfig {
            min_isolates: 1,
            max_isolates: 3,
            max_inflight_per_isolate: 4,
            idle_ttl: Duration::from_secs(5),
            scale_tick: Duration::from_millis(50),
            queue_warn_thresholds: vec![10],
            ..RuntimeConfig::default()
        })
        .await;

        service
            .deploy_with_config(
                "actor".to_string(),
                actor_worker(),
                DeployConfig {
                    public: false,
                    internal: DeployInternalConfig { trace: None },
                    bindings: vec![DeployBinding::Actor {
                        binding: "MY_ACTOR".to_string(),
                    }],
                },
            )
            .await
            .expect("deploy should succeed");

        let started = Instant::now();
        let mut tasks = Vec::new();
        for idx in 0..8 {
            let svc = service.clone();
            tasks.push(tokio::spawn(async move {
                svc.invoke(
                    "actor".to_string(),
                    test_invocation_with_path("/run?key=user-1", &format!("actor-run-{idx}")),
                )
                .await
            }));
        }
        for task in tasks {
            let output = task.await.expect("join").expect("invoke should succeed");
            assert_eq!(output.status, 200);
        }
        let elapsed = started.elapsed();
        assert!(
            elapsed < Duration::from_millis(650),
            "expected overlap for same actor key by default, elapsed={elapsed:?}"
        );
    }

    #[tokio::test]
    #[serial]
    #[ignore = "hot-key strict-write contention remains a scheduler stress case under the new STM cutover"]
    async fn actor_storage_strict_increment_preserves_all_updates_under_concurrency() {
        let service = test_service(RuntimeConfig {
            min_isolates: 2,
            max_isolates: 3,
            max_inflight_per_isolate: 1,
            idle_ttl: Duration::from_secs(5),
            scale_tick: Duration::from_millis(50),
            queue_warn_thresholds: vec![10],
            ..RuntimeConfig::default()
        })
        .await;

        service
            .deploy_with_config(
                "actor".to_string(),
                actor_worker(),
                DeployConfig {
                    public: false,
                    internal: DeployInternalConfig { trace: None },
                    bindings: vec![DeployBinding::Actor {
                        binding: "MY_ACTOR".to_string(),
                    }],
                },
            )
            .await
            .expect("deploy should succeed");

        service
            .invoke(
                "actor".to_string(),
                test_invocation_with_path("/seed?key=user-3", "seed"),
            )
            .await
            .expect("seed should succeed");

        let mut tasks = Vec::new();
        for idx in 0..8 {
            let svc = service.clone();
            tasks.push(tokio::spawn(async move {
                svc.invoke(
                    "actor".to_string(),
                    test_invocation_with_path("/inc-cas?key=user-3", &format!("cas-{idx}")),
                )
                .await
            }));
        }

        for task in tasks {
            let output = task.await.expect("join").expect("invoke should succeed");
            assert_eq!(output.status, 200);
        }

        let current = service
            .invoke(
                "actor".to_string(),
                test_invocation_with_path("/get?key=user-3", "get-after-inc"),
            )
            .await
            .expect("get should succeed");
        assert_eq!(String::from_utf8(current.body).expect("utf8"), "2");
    }

    #[tokio::test]
    #[serial]
    async fn actor_storage_structured_value_roundtrip_works() {
        let service = test_service(RuntimeConfig {
            min_isolates: 1,
            max_isolates: 2,
            max_inflight_per_isolate: 4,
            idle_ttl: Duration::from_secs(5),
            scale_tick: Duration::from_millis(50),
            queue_warn_thresholds: vec![10],
            ..RuntimeConfig::default()
        })
        .await;

        service
            .deploy_with_config(
                "actor".to_string(),
                actor_worker(),
                DeployConfig {
                    public: false,
                    internal: DeployInternalConfig { trace: None },
                    bindings: vec![DeployBinding::Actor {
                        binding: "MY_ACTOR".to_string(),
                    }],
                },
            )
            .await
            .expect("deploy should succeed");

        let roundtrip = service
            .invoke(
                "actor".to_string(),
                test_invocation_with_path("/value-roundtrip?key=user-4", "value-roundtrip"),
            )
            .await
            .expect("roundtrip invoke should succeed");
        assert_eq!(roundtrip.status, 200);
        assert_eq!(String::from_utf8(roundtrip.body).expect("utf8"), "ok");

        let guard = service
            .invoke(
                "actor".to_string(),
                test_invocation_with_path("/value-string-get-guard?key=user-5", "value-guard"),
            )
            .await
            .expect("guard invoke should succeed");
        assert_eq!(guard.status, 200);
        assert_eq!(String::from_utf8(guard.body).expect("utf8"), "ok");

        let visibility = service
            .invoke(
                "actor".to_string(),
                test_invocation_with_path("/local-visibility?key=user-6", "value-visibility"),
            )
            .await
            .expect("visibility invoke should succeed");
        assert_eq!(visibility.status, 200);
        assert_eq!(String::from_utf8(visibility.body).expect("utf8"), "ok");
    }

    #[tokio::test]
    #[serial]
    async fn actor_constructor_reads_hydrated_storage_snapshot_synchronously() {
        let service = test_service(RuntimeConfig {
            min_isolates: 0,
            max_isolates: 1,
            max_inflight_per_isolate: 4,
            idle_ttl: Duration::from_millis(200),
            scale_tick: Duration::from_millis(50),
            queue_warn_thresholds: vec![10],
            ..RuntimeConfig::default()
        })
        .await;

        service
            .deploy_with_config(
                "actor-ctor".to_string(),
                actor_constructor_storage_worker(),
                DeployConfig {
                    public: false,
                    internal: DeployInternalConfig { trace: None },
                    bindings: vec![DeployBinding::Actor {
                        binding: "MY_ACTOR".to_string(),
                    }],
                },
            )
            .await
            .expect("deploy should succeed");

        let seeded = service
            .invoke(
                "actor-ctor".to_string(),
                test_invocation_with_path("/seed", "ctor-seed"),
            )
            .await
            .expect("seed invoke should succeed");
        assert_eq!(seeded.status, 200);

        let warm_ctor = service
            .invoke(
                "actor-ctor".to_string(),
                test_invocation_with_path("/constructor-value", "ctor-warm"),
            )
            .await
            .expect("warm constructor value should succeed");
        assert_eq!(String::from_utf8(warm_ctor.body).expect("utf8"), "7");

        timeout(Duration::from_secs(3), async {
            loop {
                let stats = service
                    .stats("actor-ctor".to_string())
                    .await
                    .expect("stats");
                if stats.isolates_total == 0 {
                    break;
                }
                sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("actor pool should scale down to zero");

        let cold_ctor = service
            .invoke(
                "actor-ctor".to_string(),
                test_invocation_with_path("/constructor-value", "ctor-cold"),
            )
            .await
            .expect("cold constructor value should succeed");
        assert_eq!(String::from_utf8(cold_ctor.body).expect("utf8"), "7");

        let current = service
            .invoke(
                "actor-ctor".to_string(),
                test_invocation_with_path("/current-value", "ctor-current"),
            )
            .await
            .expect("current value should succeed");
        assert_eq!(String::from_utf8(current.body).expect("utf8"), "7");
    }

    #[tokio::test]
    #[serial]
    async fn hosted_actor_factories_share_state_and_module_globals() {
        let service = test_service(RuntimeConfig {
            min_isolates: 1,
            max_isolates: 2,
            max_inflight_per_isolate: 4,
            idle_ttl: Duration::from_secs(5),
            scale_tick: Duration::from_millis(50),
            queue_warn_thresholds: vec![10],
            ..RuntimeConfig::default()
        })
        .await;

        service
            .deploy_with_config(
                "hosted-actor".to_string(),
                hosted_actor_worker(),
                DeployConfig {
                    public: false,
                    internal: DeployInternalConfig { trace: None },
                    bindings: vec![DeployBinding::Actor {
                        binding: "MY_ACTOR".to_string(),
                    }],
                },
            )
            .await
            .expect("deploy should succeed");

        let alpha = service
            .invoke(
                "hosted-actor".to_string(),
                test_invocation_with_path("/alpha/inc?key=user-1", "alpha-inc"),
            )
            .await
            .expect("alpha invoke should succeed");
        assert_eq!(String::from_utf8(alpha.body).expect("utf8"), "1");

        let beta = service
            .invoke(
                "hosted-actor".to_string(),
                test_invocation_with_path("/beta/read?key=user-1", "beta-read"),
            )
            .await
            .expect("beta invoke should succeed");
        assert_eq!(String::from_utf8(beta.body).expect("utf8"), "1");

        let worker_global = service
            .invoke(
                "hosted-actor".to_string(),
                test_invocation_with_path("/worker/global/inc?key=user-1", "worker-global-inc"),
            )
            .await
            .expect("worker global increment should succeed");
        assert_eq!(String::from_utf8(worker_global.body).expect("utf8"), "1");

        let actor_global = service
            .invoke(
                "hosted-actor".to_string(),
                test_invocation_with_path("/actor/global/read?key=user-1", "actor-global-read"),
            )
            .await
            .expect("actor global read should succeed");
        assert_eq!(String::from_utf8(actor_global.body).expect("utf8"), "1");

        let actor_global_inc = service
            .invoke(
                "hosted-actor".to_string(),
                test_invocation_with_path("/actor/global/inc?key=user-1", "actor-global-inc"),
            )
            .await
            .expect("actor global increment should succeed");
        assert_eq!(String::from_utf8(actor_global_inc.body).expect("utf8"), "2");
    }

    #[tokio::test]
    #[serial]
    async fn hosted_actor_allows_inline_closures() {
        let service = test_service(RuntimeConfig {
            min_isolates: 1,
            max_isolates: 1,
            max_inflight_per_isolate: 2,
            idle_ttl: Duration::from_secs(5),
            scale_tick: Duration::from_millis(50),
            queue_warn_thresholds: vec![10],
            ..RuntimeConfig::default()
        })
        .await;

        service
            .deploy_with_config(
                "hosted-actor".to_string(),
                hosted_actor_worker(),
                DeployConfig {
                    public: false,
                    internal: DeployInternalConfig { trace: None },
                    bindings: vec![DeployBinding::Actor {
                        binding: "MY_ACTOR".to_string(),
                    }],
                },
            )
            .await
            .expect("deploy should succeed");

        let output = service
            .invoke(
                "hosted-actor".to_string(),
                test_invocation_with_path("/inline?key=user-2", "inline-closure"),
            )
            .await
            .expect("inline closure invoke should succeed");
        assert_eq!(String::from_utf8(output.body).expect("utf8"), "ok-inline");
    }

    #[tokio::test]
    #[serial]
    async fn hosted_actor_stm_single_read_is_point_in_time_only() {
        let service = test_service(RuntimeConfig {
            min_isolates: 2,
            max_isolates: 3,
            max_inflight_per_isolate: 1,
            idle_ttl: Duration::from_secs(5),
            scale_tick: Duration::from_millis(50),
            queue_warn_thresholds: vec![10],
            ..RuntimeConfig::default()
        })
        .await;

        service
            .deploy_with_config(
                "hosted-actor".to_string(),
                hosted_actor_worker(),
                DeployConfig {
                    public: false,
                    internal: DeployInternalConfig { trace: None },
                    bindings: vec![DeployBinding::Actor {
                        binding: "MY_ACTOR".to_string(),
                    }],
                },
            )
            .await
            .expect("deploy should succeed");

        service
            .invoke(
                "hosted-actor".to_string(),
                test_invocation_with_path("/stm/seed?key=user-stm-once", "stm-seed-once"),
            )
            .await
            .expect("seed should succeed");

        let read_task = {
            let service = service.clone();
            tokio::spawn(async move {
                service
                    .invoke(
                        "hosted-actor".to_string(),
                        test_invocation_with_path(
                            "/stm/read-once?key=user-stm-once",
                            "stm-read-once",
                        ),
                    )
                    .await
            })
        };

        sleep(Duration::from_millis(10)).await;

        service
            .invoke(
                "hosted-actor".to_string(),
                test_invocation_with_path(
                    "/stm/write-a?key=user-stm-once&value=1",
                    "stm-write-once",
                ),
            )
            .await
            .expect("write should succeed");

        let read_once = read_task
            .await
            .expect("join")
            .expect("invoke should succeed");
        assert_eq!(String::from_utf8(read_once.body).expect("utf8"), "0");
    }

    #[tokio::test]
    #[serial]
    async fn hosted_actor_stm_retries_when_prior_read_goes_stale() {
        let service = test_service(RuntimeConfig {
            min_isolates: 2,
            max_isolates: 3,
            max_inflight_per_isolate: 1,
            idle_ttl: Duration::from_secs(5),
            scale_tick: Duration::from_millis(50),
            queue_warn_thresholds: vec![10],
            ..RuntimeConfig::default()
        })
        .await;

        service
            .deploy_with_config(
                "hosted-actor".to_string(),
                hosted_actor_worker(),
                DeployConfig {
                    public: false,
                    internal: DeployInternalConfig { trace: None },
                    bindings: vec![DeployBinding::Actor {
                        binding: "MY_ACTOR".to_string(),
                    }],
                },
            )
            .await
            .expect("deploy should succeed");

        service
            .invoke(
                "hosted-actor".to_string(),
                test_invocation_with_path("/stm/seed?key=user-stm-pair", "stm-seed-pair"),
            )
            .await
            .expect("seed should succeed");

        let read_task = {
            let service = service.clone();
            tokio::spawn(async move {
                service
                    .invoke(
                        "hosted-actor".to_string(),
                        test_invocation_with_path(
                            "/stm/read-pair?key=user-stm-pair",
                            "stm-read-pair",
                        ),
                    )
                    .await
            })
        };

        let writer_thread = {
            let service = service.clone();
            std::thread::spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("build writer runtime");
                runtime.block_on(async move {
                    for attempt in 0..8 {
                        service
                            .invoke(
                                "hosted-actor".to_string(),
                                test_invocation_with_path(
                                    "/stm/write-a?key=user-stm-pair&value=1",
                                    format!("stm-write-pair-{attempt}").as_str(),
                                ),
                            )
                            .await
                            .expect("write should succeed");
                        sleep(Duration::from_millis(5)).await;
                    }
                });
            })
        };

        let pair = read_task
            .await
            .expect("join")
            .expect("invoke should succeed");
        writer_thread.join().expect("writer join");
        assert_eq!(String::from_utf8(pair.body).expect("utf8"), "1:0");
    }

    #[tokio::test]
    #[serial]
    async fn hosted_actor_stm_snapshot_read_skips_retry_for_that_read() {
        let service = test_service(RuntimeConfig {
            min_isolates: 1,
            max_isolates: 3,
            max_inflight_per_isolate: 8,
            idle_ttl: Duration::from_secs(5),
            scale_tick: Duration::from_millis(50),
            queue_warn_thresholds: vec![10],
            ..RuntimeConfig::default()
        })
        .await;

        service
            .deploy_with_config(
                "hosted-actor".to_string(),
                hosted_actor_worker(),
                DeployConfig {
                    public: false,
                    internal: DeployInternalConfig { trace: None },
                    bindings: vec![DeployBinding::Actor {
                        binding: "MY_ACTOR".to_string(),
                    }],
                },
            )
            .await
            .expect("deploy should succeed");

        service
            .invoke(
                "hosted-actor".to_string(),
                test_invocation_with_path("/stm/seed?key=user-stm-allow", "stm-seed-allow"),
            )
            .await
            .expect("seed should succeed");

        let read_task = {
            let service = service.clone();
            tokio::spawn(async move {
                service
                    .invoke(
                        "hosted-actor".to_string(),
                        test_invocation_with_path(
                            "/stm/read-pair-snapshot?key=user-stm-allow",
                            "stm-read-allow",
                        ),
                    )
                    .await
            })
        };

        sleep(Duration::from_millis(10)).await;

        service
            .invoke(
                "hosted-actor".to_string(),
                test_invocation_with_path(
                    "/stm/write-a?key=user-stm-allow&value=1",
                    "stm-write-allow",
                ),
            )
            .await
            .expect("write should succeed");

        let pair = read_task
            .await
            .expect("join")
            .expect("invoke should succeed");
        assert_eq!(String::from_utf8(pair.body).expect("utf8"), "0:0");
    }

    #[tokio::test]
    #[serial]
    async fn hosted_actor_tvar_default_is_lazy_until_written() {
        let service = test_service(RuntimeConfig {
            min_isolates: 1,
            max_isolates: 2,
            max_inflight_per_isolate: 4,
            idle_ttl: Duration::from_secs(5),
            scale_tick: Duration::from_millis(50),
            queue_warn_thresholds: vec![10],
            ..RuntimeConfig::default()
        })
        .await;

        service
            .deploy_with_config(
                "hosted-actor".to_string(),
                hosted_actor_worker(),
                DeployConfig {
                    public: false,
                    internal: DeployInternalConfig { trace: None },
                    bindings: vec![DeployBinding::Actor {
                        binding: "MY_ACTOR".to_string(),
                    }],
                },
            )
            .await
            .expect("deploy should succeed");

        let default_read = service
            .invoke(
                "hosted-actor".to_string(),
                test_invocation_with_path(
                    "/stm/tvar-default/read?key=user-1",
                    "hosted-actor-tvar-default-read",
                ),
            )
            .await
            .expect("default read should succeed");
        assert_eq!(String::from_utf8(default_read.body).expect("utf8"), "7");

        let raw_before_write = service
            .invoke(
                "hosted-actor".to_string(),
                test_invocation_with_path(
                    "/stm/tvar-default/raw?key=user-1",
                    "hosted-actor-tvar-default-raw-before-write",
                ),
            )
            .await
            .expect("raw read before write should succeed");
        assert_eq!(
            String::from_utf8(raw_before_write.body).expect("utf8"),
            "missing"
        );

        let write = service
            .invoke(
                "hosted-actor".to_string(),
                test_invocation_with_path(
                    "/stm/tvar-default/write?key=user-1",
                    "hosted-actor-tvar-default-write",
                ),
            )
            .await
            .expect("write should succeed");
        assert_eq!(String::from_utf8(write.body).expect("utf8"), "8");

        let raw_after_write = service
            .invoke(
                "hosted-actor".to_string(),
                test_invocation_with_path(
                    "/stm/tvar-default/raw?key=user-1",
                    "hosted-actor-tvar-default-raw-after-write",
                ),
            )
            .await
            .expect("raw read after write should succeed");
        assert_eq!(String::from_utf8(raw_after_write.body).expect("utf8"), "8");
    }

    #[tokio::test]
    #[serial]
    async fn async_context_store_survives_promise_boundaries_and_nested_runs() {
        let service = test_service(RuntimeConfig::default()).await;

        service
            .deploy_with_config(
                "async-context".to_string(),
                async_context_worker(),
                DeployConfig {
                    public: false,
                    internal: DeployInternalConfig { trace: None },
                    bindings: Vec::new(),
                },
            )
            .await
            .expect("deploy should succeed");

        let promise = service
            .invoke(
                "async-context".to_string(),
                test_invocation_with_path("/promise", "async-context-promise"),
            )
            .await
            .expect("promise request should succeed");
        assert_eq!(String::from_utf8(promise.body).expect("utf8"), "outer");

        let nested = service
            .invoke(
                "async-context".to_string(),
                test_invocation_with_path("/nested", "async-context-nested"),
            )
            .await
            .expect("nested request should succeed");
        assert_eq!(
            String::from_utf8(nested.body).expect("utf8"),
            "outer:inner:outer"
        );

        let restore = service
            .invoke(
                "async-context".to_string(),
                test_invocation_with_path("/restore", "async-context-restore"),
            )
            .await
            .expect("restore request should succeed");
        assert_eq!(
            String::from_utf8(restore.body).expect("utf8"),
            "missing:missing"
        );
    }

    #[tokio::test]
    #[serial]
    async fn cache_default_reuses_response() {
        let service = test_service(RuntimeConfig {
            min_isolates: 1,
            max_isolates: 1,
            max_inflight_per_isolate: 1,
            idle_ttl: Duration::from_secs(5),
            scale_tick: Duration::from_millis(50),
            queue_warn_thresholds: vec![10],
            ..RuntimeConfig::default()
        })
        .await;

        service
            .deploy("cache".to_string(), cache_worker("default", "cache"))
            .await
            .expect("deploy should succeed");

        let one = service
            .invoke(
                "cache".to_string(),
                test_invocation_with_path("/", "cache-one"),
            )
            .await
            .expect("first invoke should succeed");
        let two = service
            .invoke(
                "cache".to_string(),
                test_invocation_with_path("/", "cache-two"),
            )
            .await
            .expect("second invoke should succeed");

        assert_eq!(String::from_utf8(one.body).expect("utf8"), "cache:1");
        assert_eq!(String::from_utf8(two.body).expect("utf8"), "cache:1");
    }

    #[tokio::test]
    #[serial]
    async fn named_caches_share_global_capacity_budget() {
        let service = test_service(RuntimeConfig {
            min_isolates: 1,
            max_isolates: 1,
            max_inflight_per_isolate: 1,
            idle_ttl: Duration::from_secs(5),
            scale_tick: Duration::from_millis(50),
            queue_warn_thresholds: vec![10],
            cache_max_entries: 1,
            ..RuntimeConfig::default()
        })
        .await;

        service
            .deploy("worker-a".to_string(), cache_worker("cache-a", "A"))
            .await
            .expect("deploy a should succeed");
        service
            .deploy("worker-b".to_string(), cache_worker("cache-b", "B"))
            .await
            .expect("deploy b should succeed");

        let a1 = service
            .invoke(
                "worker-a".to_string(),
                test_invocation_with_path("/", "a-1"),
            )
            .await
            .expect("a1 should succeed");
        let b1 = service
            .invoke(
                "worker-b".to_string(),
                test_invocation_with_path("/", "b-1"),
            )
            .await
            .expect("b1 should succeed");
        let a2 = service
            .invoke(
                "worker-a".to_string(),
                test_invocation_with_path("/", "a-2"),
            )
            .await
            .expect("a2 should succeed");

        assert_eq!(String::from_utf8(a1.body).expect("utf8"), "A:1");
        assert_eq!(String::from_utf8(b1.body).expect("utf8"), "B:1");
        assert_eq!(String::from_utf8(a2.body).expect("utf8"), "A:2");
    }

    #[tokio::test]
    #[serial]
    async fn internal_trace_includes_markers_and_targets_configured_worker() {
        let service = test_service(RuntimeConfig {
            min_isolates: 1,
            max_isolates: 1,
            max_inflight_per_isolate: 4,
            idle_ttl: Duration::from_secs(5),
            scale_tick: Duration::from_millis(50),
            queue_warn_thresholds: vec![10],
            ..RuntimeConfig::default()
        })
        .await;

        service
            .deploy("trace-sink".to_string(), trace_sink_worker())
            .await
            .expect("deploy trace sink should succeed");
        service
            .deploy_with_config(
                "traced-worker".to_string(),
                r#"
                export default {
                  async fetch() {
                    return new Response("ok");
                  },
                };
                "#
                .to_string(),
                DeployConfig {
                    internal: DeployInternalConfig {
                        trace: Some(DeployTraceDestination {
                            worker: "trace-sink".to_string(),
                            path: "/ingest".to_string(),
                        }),
                    },
                    ..DeployConfig::default()
                },
            )
            .await
            .expect("deploy traced worker should succeed");

        let mut request = test_invocation_with_path("/", "trace-request");
        request
            .headers
            .push(("x-test".to_string(), "value".to_string()));
        service
            .invoke("traced-worker".to_string(), request)
            .await
            .expect("traced invoke should succeed");

        sleep(Duration::from_millis(100)).await;
    }

    #[test]
    fn internal_trace_headers_include_markers() {
        let mut headers = vec![("x-other".to_string(), "value".to_string())];
        super::append_internal_trace_headers(&mut headers, "traced-worker", 42);

        let internal = headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("x-dd-internal"))
            .expect("x-dd-internal header should be present")
            .1
            .as_str();
        let reason = headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("x-dd-internal-reason"))
            .expect("x-dd-internal-reason header should be present")
            .1
            .as_str();
        let source_worker = headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("x-dd-trace-source-worker"))
            .expect("x-dd-trace-source-worker header should be present")
            .1
            .as_str();
        let source_generation = headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("x-dd-trace-source-generation"))
            .expect("x-dd-trace-source-generation header should be present")
            .1
            .as_str();

        assert_eq!(internal, "1");
        assert_eq!(reason, "trace");
        assert_eq!(source_worker, "traced-worker");
        assert_eq!(source_generation, "42");
    }

    #[tokio::test]
    #[serial]
    async fn internal_trace_invocations_do_not_recurse() {
        let service = test_service(RuntimeConfig {
            min_isolates: 1,
            max_isolates: 1,
            max_inflight_per_isolate: 4,
            idle_ttl: Duration::from_secs(5),
            scale_tick: Duration::from_millis(50),
            queue_warn_thresholds: vec![10],
            ..RuntimeConfig::default()
        })
        .await;

        service
            .deploy_with_config(
                "loop-worker".to_string(),
                loop_trace_worker(),
                DeployConfig {
                    internal: DeployInternalConfig {
                        trace: Some(DeployTraceDestination {
                            worker: "loop-worker".to_string(),
                            path: "/trace".to_string(),
                        }),
                    },
                    ..DeployConfig::default()
                },
            )
            .await
            .expect("deploy loop worker should succeed");

        service
            .invoke(
                "loop-worker".to_string(),
                test_invocation_with_path("/", "loop-user"),
            )
            .await
            .expect("loop worker invoke should succeed");

        sleep(Duration::from_millis(100)).await;
        let state = timeout(Duration::from_secs(2), async {
            loop {
                let state_output = service
                    .invoke(
                        "loop-worker".to_string(),
                        test_invocation_with_path("/state", "loop-state"),
                    )
                    .await
                    .expect("loop worker state invoke should succeed");
                let state: LoopTraceState = crate::json::from_string(
                    String::from_utf8(state_output.body).expect("loop state body should be utf8"),
                )
                .expect("loop state should parse as json");
                if state.trace_calls >= 2 {
                    return state;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("loop state query should complete");

        assert_eq!(state.trace_calls, 2);
        assert!(state.total_calls >= 2);
    }

    #[test]
    fn dynamic_worker_config_builds_placeholders() {
        let mut env = HashMap::new();
        env.insert("OPENAI_API_KEY".to_string(), "sk-test-123".to_string());
        let config =
            super::build_dynamic_worker_config(env, vec!["api.openai.com".to_string()], Vec::new())
                .expect("dynamic config should build");

        assert_eq!(config.dynamic_env.len(), 1);
        assert_eq!(config.secret_replacements.len(), 1);
        assert_eq!(
            config.egress_allow_hosts,
            vec!["api.openai.com".to_string()]
        );

        let placeholder = config
            .env_placeholders
            .get("OPENAI_API_KEY")
            .expect("placeholder should be present");
        assert!(placeholder.starts_with("__DD_SECRET_"));
    }

    #[test]
    fn dynamic_worker_config_rejects_invalid_host() {
        let config = super::build_dynamic_worker_config(
            HashMap::new(),
            vec!["http://bad-host".to_string()],
            Vec::new(),
        );
        assert!(config.is_err());
    }

    #[test]
    fn extract_bindings_collects_dynamic_bindings() {
        let bindings = super::extract_bindings(&DeployConfig {
            bindings: vec![
                DeployBinding::Kv {
                    binding: "MY_KV".to_string(),
                },
                DeployBinding::Dynamic {
                    binding: "SANDBOX".to_string(),
                },
            ],
            ..DeployConfig::default()
        })
        .expect("bindings should parse");

        assert_eq!(bindings.kv, vec!["MY_KV".to_string()]);
        assert_eq!(bindings.dynamic, vec!["SANDBOX".to_string()]);
    }

    #[test]
    fn extract_bindings_rejects_duplicate_dynamic_name() {
        let result = super::extract_bindings(&DeployConfig {
            bindings: vec![
                DeployBinding::Dynamic {
                    binding: "SANDBOX".to_string(),
                },
                DeployBinding::Dynamic {
                    binding: "SANDBOX".to_string(),
                },
            ],
            ..DeployConfig::default()
        });
        assert!(result.is_err());
    }
}
