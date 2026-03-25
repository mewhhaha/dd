use crate::actor::ActorStore;
use crate::actor_rpc::{
    decode_actor_invoke_request, encode_actor_invoke_response, ActorInvokeCall, ActorInvokeResponse,
};
use crate::blob::{BlobStore, BlobStoreConfig};
use crate::cache::{CacheConfig, CacheLookup, CacheRequest, CacheResponse, CacheStore};
use crate::engine::{
    abort_worker_request, build_bootstrap_snapshot, build_worker_snapshot, dispatch_worker_request,
    new_runtime_from_snapshot, pump_event_loop_once, validate_worker, ExecuteActorCall,
};
use crate::kv::KvStore;
use crate::ops::{
    cancel_request_body_stream, clear_request_body_stream, register_actor_request_scope,
    register_request_body_stream, ActorInvokeEvent, IsolateEventPayload, IsolateEventSender,
    RequestBodyStreams,
};
use common::{DeployBinding, DeployConfig, PlatformError, Result, WorkerInvocation, WorkerOutput};
use opentelemetry::global;
use opentelemetry::propagation::Extractor;
use opentelemetry::trace::TraceContextExt;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::mpsc as std_mpsc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::runtime::Builder;
use tokio::sync::{mpsc, oneshot};
use tracing::{info, warn};
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
const CONTENT_TYPE_HEADER: &str = "content-type";
const JSON_CONTENT_TYPE: &str = "application/json";

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
    CloseWebsocket {
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
    websocket_open_waiters: HashMap<String, oneshot::Sender<Result<WebSocketOpen>>>,
    next_generation: u64,
    next_isolate_id: u64,
}

#[derive(Clone)]
struct WorkerWebSocketSession {
    worker_name: String,
    generation: u64,
    isolate_id: u64,
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
    generation: u64,
    deployment_id: String,
    internal_trace: Option<InternalTraceDestination>,
    is_public: bool,
    snapshot: &'static [u8],
    kv_bindings: Vec<String>,
    actor_bindings: Vec<(String, String)>,
    queue: VecDeque<PendingInvoke>,
    isolates: Vec<IsolateHandle>,
    actor_owners: HashMap<String, u64>,
    actor_inflight: HashMap<String, usize>,
    stats: PoolStats,
    queue_warn_level: usize,
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
    method: String,
    url: String,
    reply: oneshot::Sender<Result<WorkerOutput>>,
    traceparent: Option<String>,
    user_request_id: String,
    kind: PendingReplyKind,
    dispatched_at: Instant,
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

struct IsolateHandle {
    id: u64,
    sender: mpsc::UnboundedSender<IsolateCommand>,
    inflight_count: usize,
    active_websocket_sessions: usize,
    served_requests: u64,
    last_used_at: Instant,
    pending_replies: HashMap<String, PendingReply>,
    pending_wait_until: HashMap<String, String>,
}

enum IsolateCommand {
    Execute {
        runtime_request_id: String,
        completion_token: String,
        worker_name: String,
        kv_bindings: Vec<String>,
        actor_bindings: Vec<(String, String)>,
        request: WorkerInvocation,
        request_body: Option<InvokeRequestBodyReceiver>,
        actor_call: Option<ActorExecutionCall>,
        actor_route: Option<ActorRoute>,
    },
    Abort {
        runtime_request_id: String,
    },
    Shutdown,
}

#[derive(Clone)]
enum ActorExecutionCall {
    Fetch {
        binding: String,
        key: String,
    },
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
        let RuntimeServiceConfig { runtime, storage } = config;
        validate_runtime_config(&runtime)?;
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

        let mut restored = 0usize;
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
        let runtime_request_id = Uuid::new_v4().to_string();
        let invoke_span = tracing::info_span!(
            "runtime.invoke",
            worker.name = %worker_name,
            runtime.request_id = %runtime_request_id,
            request.id = %request.request_id
        );
        set_span_parent_from_traceparent(
            &invoke_span,
            traceparent_from_headers(&request.headers).as_deref(),
        );
        let _invoke_guard = invoke_span.enter();
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
        let runtime_request_id = Uuid::new_v4().to_string();
        let stream_span = tracing::info_span!(
            "runtime.invoke_stream",
            worker.name = %worker_name,
            runtime.request_id = %runtime_request_id,
            request.id = %request.request_id
        );
        set_span_parent_from_traceparent(
            &stream_span,
            traceparent_from_headers(&request.headers).as_deref(),
        );
        let _stream_guard = stream_span.enter();
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
            websocket_open_waiters: HashMap::new(),
            next_generation: 1,
            next_isolate_id: 1,
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

    fn handle_event(
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
        isolate_id: u64,
        session_id: &str,
        binding: &str,
        key: &str,
        handle: &str,
    ) -> Result<()> {
        let owner_key = actor_owner_key(binding, key);
        let owner_isolate_id = self
            .get_pool_mut(worker_name, generation)
            .and_then(|pool| pool.actor_owners.get(&owner_key).copied())
            .unwrap_or(isolate_id);

        let Some(pool) = self.get_pool_mut(worker_name, generation) else {
            return Err(PlatformError::not_found("worker pool missing"));
        };
        let Some(isolate) = pool
            .isolates
            .iter_mut()
            .find(|candidate| candidate.id == owner_isolate_id)
        else {
            return Err(PlatformError::not_found(
                "websocket owner isolate is not available",
            ));
        };
        isolate.active_websocket_sessions += 1;

        if self.websocket_sessions.contains_key(session_id) {
            let _ = self.unregister_websocket_session(session_id);
        }

        let session = WorkerWebSocketSession {
            worker_name: worker_name.to_string(),
            generation,
            isolate_id: owner_isolate_id,
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
            .entry(owner_key)
            .or_default()
            .insert(handle.to_string());
        Ok(())
    }

    fn unregister_websocket_session(&mut self, session_id: &str) -> Option<WorkerWebSocketSession> {
        let session = self.websocket_sessions.remove(session_id)?;
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

        if let Some(pool) = self.get_pool_mut(&session.worker_name, session.generation) {
            if let Some(isolate) = pool
                .isolates
                .iter_mut()
                .find(|isolate| isolate.id == session.isolate_id)
            {
                isolate.active_websocket_sessions =
                    isolate.active_websocket_sessions.saturating_sub(1);
            }
        }
        Some(session)
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
        let result = match self.websocket_handle_index.get(&index_key) {
            Some(session_id) => {
                self.websocket_outbound_frames
                    .entry(session_id.clone())
                    .or_default()
                    .push_back(WebSocketOutboundFrame {
                        is_binary: !is_text,
                        payload: message,
                    });
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
        let result = match self.websocket_handle_index.get(&index_key) {
            Some(session_id) => {
                self.websocket_close_signals
                    .insert(session_id.clone(), SocketCloseEvent { code, reason });
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
        let actor_classes: Vec<String> = bindings
            .actor
            .iter()
            .map(|(_, class)| class.clone())
            .collect();

        validate_worker(self.bootstrap_snapshot, &source, &actor_classes).await?;
        let worker_snapshot =
            build_worker_snapshot(self.bootstrap_snapshot, &source, &actor_classes).await?;
        let generation = self.next_generation;
        self.next_generation += 1;
        let deployment_id = Uuid::new_v4().to_string();
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
                generation,
                deployment_id: deployment_id.clone(),
                internal_trace: config.internal.trace.as_ref().map(|trace| {
                    InternalTraceDestination {
                        worker: trace.worker.trim().to_string(),
                        path: normalize_trace_path(&trace.path),
                    }
                }),
                is_public: config.public,
                snapshot: worker_snapshot,
                kv_bindings: bindings.kv,
                actor_bindings: bindings.actor,
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
                    .any(|(binding, _)| binding == &route.binding)
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
        let (request, actor_call) = match decoded.call {
            ActorInvokeCall::Fetch(request) => (
                request,
                ActorExecutionCall::Fetch {
                    binding: decoded.binding.clone(),
                    key: decoded.key.clone(),
                },
            ),
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
                    name,
                    args,
                },
            ),
        };
        let is_method_call = matches!(actor_call, ActorExecutionCall::Method { .. });
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
        let (reply_tx, reply_rx) = oneshot::channel();
        self.enqueue_invoke(
            decoded.worker_name,
            runtime_request_id,
            request,
            None,
            Some(route),
            Some(actor_call),
            None,
            false,
            reply_tx,
            PendingReplyKind::Normal,
            event_tx,
        );
        tokio::spawn(async move {
            let result = match reply_rx.await {
                Ok(Ok(output)) => {
                    if is_method_call {
                        encode_actor_invoke_response(&ActorInvokeResponse::Method {
                            value: output.body,
                        })
                    } else {
                        encode_actor_invoke_response(&ActorInvokeResponse::Fetch(output))
                    }
                }
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
        loop {
            let max_inflight = self.config.max_inflight_per_isolate;
            let (candidate, spawn_needed) = {
                let Some(pool) = self.get_pool_mut(worker_name, generation) else {
                    return;
                };
                if pool.queue.is_empty() {
                    pool.log_stats("dispatch");
                    return;
                }
                let has_capacity = pool
                    .isolates
                    .iter()
                    .any(|isolate| isolate.inflight_count < max_inflight);
                (
                    select_dispatch_candidate(pool, max_inflight),
                    !has_capacity && pool.isolates.len() < self.config.max_isolates,
                )
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

            let Some(candidate) = candidate else {
                return;
            };
            let isolate_idx = candidate.isolate_idx;
            let Some(pending_invoke) = self
                .get_pool_mut(worker_name, generation)
                .and_then(|pool| pool.queue.remove(candidate.queue_idx))
            else {
                return;
            };

            let queue_wait_ms = pending_invoke.enqueued_at.elapsed().as_millis() as u64;
            let runtime_request_id = pending_invoke.runtime_request_id.clone();
            let user_request_id = pending_invoke.request.request_id.clone();
            let request_method = pending_invoke.request.method.clone();
            let request_url = pending_invoke.request.url.clone();
            let internal_origin = pending_invoke.internal_origin;
            let traceparent = traceparent_from_headers(&pending_invoke.request.headers);
            let span = tracing::info_span!(
                "runtime.dispatch",
                worker.name = %worker_name,
                worker.generation = generation,
                runtime.request_id = %runtime_request_id,
                request.id = %user_request_id,
                queue.wait_ms = queue_wait_ms
            );
            set_span_parent_from_traceparent(&span, traceparent.as_deref());
            let _dispatch_guard = span.enter();
            tracing::info!("dispatching request to isolate");

            let mut pending_reply = Some(pending_invoke.reply);
            let completion_token = Uuid::new_v4().to_string();
            if let Some(registration) = self.stream_registrations.get_mut(&runtime_request_id) {
                if registration.worker_name == worker_name {
                    registration.completion_token = Some(completion_token.clone());
                }
            }
            let mut send_failed = false;
            if let Some(pool) = self.get_pool_mut(worker_name, generation) {
                if isolate_idx >= pool.isolates.len() {
                    continue;
                }

                let kv_bindings = pool.kv_bindings.clone();
                let actor_bindings = pool.actor_bindings.clone();
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
                let command = IsolateCommand::Execute {
                    runtime_request_id: runtime_request_id.clone(),
                    completion_token: completion_token.clone(),
                    worker_name: worker_name.to_string(),
                    kv_bindings,
                    actor_bindings,
                    request: pending_invoke.request,
                    request_body: pending_invoke.request_body,
                    actor_call: pending_invoke.actor_call,
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
                            method: request_method,
                            url: request_url,
                            reply: pending_reply
                                .take()
                                .expect("pending reply must exist before dispatch"),
                            traceparent: traceparent.clone(),
                            user_request_id: user_request_id.clone(),
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
        let snapshot = self
            .get_pool_mut(worker_name, generation)
            .ok_or_else(|| PlatformError::not_found("Worker not found"))?
            .snapshot;
        let isolate_id = self.next_isolate_id;
        self.next_isolate_id += 1;
        let kv_store = self.kv_store.clone();
        let actor_store = self.actor_store.clone();
        let cache_store = self.cache_store.clone();
        let isolate = spawn_isolate_thread(
            snapshot,
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
        let stream_result = result.clone();
        let trace_result = result.clone();
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
                    request_method = pending.method;
                    request_url = pending.url;
                    if wait_until_count > 0 {
                        isolate
                            .pending_wait_until
                            .insert(request_id.to_string(), completion_token.to_string());
                    }
                    clear_revalidation = true;
                    completion_traceparent = pending.traceparent;
                    user_request_id = pending.user_request_id;
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
        let result_status = match &result {
            Ok(output) => output.status as i64,
            Err(_) => -1,
        };
        let result_ok = result.is_ok();
        let complete_span = tracing::info_span!(
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
        set_span_parent_from_traceparent(&complete_span, completion_traceparent.as_deref());
        let _complete_guard = complete_span.enter();

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
            }
            tracing::info!("request completion delivered");
        } else {
            info!(
                worker = %worker_name,
                generation,
                isolate_id,
                request_id,
                "dropped completion for canceled request"
            );
        }
        self.enqueue_trace_forward(
            worker_name,
            generation,
            &request_method,
            &request_url,
            request_id,
            &user_request_id,
            &trace_result,
            execution_ms.unwrap_or_default(),
            wait_until_count,
            internal_origin,
            trace_destination,
            event_tx,
        );
        self.complete_stream_registration(worker_name, request_id, completion_token, stream_result);
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
        let affected_sessions: Vec<String> = self
            .websocket_sessions
            .iter()
            .filter(|(_, session)| {
                session.worker_name == worker_name
                    && session.generation == generation
                    && session.isolate_id == isolate_id
            })
            .map(|(session_id, _)| session_id.clone())
            .collect();
        let failed = self.remove_isolate_by_id(worker_name, generation, isolate_id);
        for (request_id, reply) in failed {
            self.clear_revalidation_for_request(&request_id);
            let _ = reply.send(Err(error.clone()));
        }
        for session_id in affected_sessions {
            if let Some(session) = self.unregister_websocket_session(&session_id) {
                self.queue_websocket_close_replay(&session, 1006, "isolate failed".to_string());
            }
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

    fn remove_isolate(
        &mut self,
        worker_name: &str,
        generation: u64,
        isolate_idx: usize,
    ) -> Vec<(String, oneshot::Sender<Result<WorkerOutput>>)> {
        let mut websocket_open_session_ids = Vec::new();
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
                    if let PendingReplyKind::WebsocketOpen { session_id } = pending.kind {
                        websocket_open_session_ids.push(session_id);
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
        let mut clear_request_ids = Vec::new();
        {
            let Some(entry) = self.workers.get_mut(worker_name) else {
                return;
            };
            let current_generation = entry.current_generation;
            let drained: Vec<u64> = entry
                .pools
                .iter()
                .filter(|(generation, pool)| {
                    **generation != current_generation && pool.is_drained()
                })
                .map(|(generation, _)| *generation)
                .collect();

            for generation in drained {
                if let Some(pool) = entry.pools.remove(&generation) {
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
        self.websocket_sessions.clear();
        self.websocket_handle_index.clear();
        self.websocket_open_handles.clear();
        self.websocket_pending_closes.clear();
        self.websocket_outbound_frames.clear();
        self.websocket_close_signals.clear();
    }
}

fn select_dispatch_candidate(
    pool: &mut WorkerPool,
    max_inflight: usize,
) -> Option<DispatchCandidate> {
    for (queue_idx, pending) in pool.queue.iter().enumerate() {
        let Some(route) = &pending.actor_route else {
            return least_loaded_isolate_idx(&pool.isolates, max_inflight).map(|isolate_idx| {
                DispatchCandidate {
                    queue_idx,
                    isolate_idx,
                    actor_key: None,
                    assign_owner: false,
                }
            });
        };

        let actor_key = route.owner_key();

        if let Some(owner_id) = pool.actor_owners.get(&actor_key).copied() {
            if let Some((idx, _)) = pool
                .isolates
                .iter()
                .enumerate()
                .find(|(_, isolate)| isolate.id == owner_id)
            {
                return Some(DispatchCandidate {
                    queue_idx,
                    isolate_idx: idx,
                    actor_key: Some(actor_key),
                    assign_owner: false,
                });
            }
            if !pool.isolates.iter().any(|isolate| isolate.id == owner_id) {
                pool.actor_owners.remove(&actor_key);
            } else {
                continue;
            }
        }

        if let Some(isolate_idx) = least_loaded_isolate_any_idx(&pool.isolates) {
            return Some(DispatchCandidate {
                queue_idx,
                isolate_idx,
                actor_key: Some(actor_key),
                assign_owner: true,
            });
        }
    }
    None
}

fn least_loaded_isolate_idx(isolates: &[IsolateHandle], max_inflight: usize) -> Option<usize> {
    isolates
        .iter()
        .enumerate()
        .filter(|(_, isolate)| isolate.inflight_count < max_inflight)
        .min_by_key(|(_, isolate)| isolate.inflight_count)
        .map(|(idx, _)| idx)
}

fn least_loaded_isolate_any_idx(isolates: &[IsolateHandle]) -> Option<usize> {
    isolates
        .iter()
        .enumerate()
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
    }

    fn busy_count(&self) -> usize {
        self.isolates
            .iter()
            .filter(|isolate| {
                isolate.inflight_count > 0
                    || !isolate.pending_wait_until.is_empty()
                    || isolate.active_websocket_sessions > 0
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
    actor: Vec<(String, String)>,
}

fn extract_bindings(config: &DeployConfig) -> Result<DeployBindings> {
    let mut kv = Vec::new();
    let mut actor = Vec::new();
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
            DeployBinding::Actor { binding, class } => {
                let name = binding.trim();
                let class_name = class.trim();
                if name.is_empty() {
                    return Err(PlatformError::bad_request("binding name must not be empty"));
                }
                if class_name.is_empty() {
                    return Err(PlatformError::bad_request("actor class must not be empty"));
                }
                if !seen.insert(name.to_string()) {
                    return Err(PlatformError::bad_request(format!(
                        "duplicate binding name: {name}"
                    )));
                }
                actor.push((name.to_string(), class_name.to_string()));
            }
        }
    }
    Ok(DeployBindings { kv, actor })
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
                            manager.handle_command(command, &event_tx).await;
                        }
                        Some(command) = cancel_receiver.recv() => {
                            manager.handle_command(command, &event_tx).await;
                        }
                        Some(event) = event_rx.recv() => {
                            manager.handle_event(event, &event_tx);
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

fn spawn_isolate_thread(
    snapshot: &'static [u8],
    kv_store: KvStore,
    actor_store: ActorStore,
    cache_store: CacheStore,
    worker_name: String,
    generation: u64,
    isolate_id: u64,
    event_tx: mpsc::UnboundedSender<RuntimeEvent>,
) -> Result<IsolateHandle> {
    let (command_tx, mut command_rx) = mpsc::unbounded_channel();
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

                let (event_payload_tx, mut event_payload_rx) =
                    mpsc::unbounded_channel::<IsolateEventPayload>();
                {
                    let op_state = js_runtime.op_state();
                    let mut op_state = op_state.borrow_mut();
                    op_state.put(IsolateEventSender(event_payload_tx));
                    op_state.put(kv_store.clone());
                    op_state.put(actor_store.clone());
                    op_state.put(cache_store.clone());
                    op_state.put(RequestBodyStreams::default());
                    op_state.put(crate::ops::ActorRequestScopes::default());
                }
                let _ = init_tx.send(Ok(()));

                let mut ticker = tokio::time::interval(Duration::from_millis(1));
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

                loop {
                    tokio::select! {
                        Some(payload) = event_payload_rx.recv() => {
                            match payload {
                                IsolateEventPayload::Completion(payload) => {
                                    match decode_completion_payload(payload) {
                                        Ok((request_id, completion_token, wait_until_count, result)) => {
                                            let _ = event_tx.send(RuntimeEvent::RequestFinished {
                                                worker_name: worker_name.clone(),
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
                                    }
                                }
                                IsolateEventPayload::WaitUntilDone(payload) => {
                                    match decode_wait_until_payload(payload) {
                                        Ok((request_id, completion_token)) => {
                                            let _ = event_tx.send(RuntimeEvent::WaitUntilFinished {
                                                worker_name: worker_name.clone(),
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
                                    }
                                }
                                IsolateEventPayload::ResponseStart(payload) => {
                                    match decode_response_start_payload(payload) {
                                        Ok((request_id, completion_token, status, headers)) => {
                                            let _ = event_tx.send(RuntimeEvent::ResponseStart {
                                                worker_name: worker_name.clone(),
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
                                    }
                                }
                                IsolateEventPayload::ResponseChunk(payload) => {
                                    match decode_response_chunk_payload(payload) {
                                        Ok((request_id, completion_token, chunk)) => {
                                            let _ = event_tx.send(RuntimeEvent::ResponseChunk {
                                                worker_name: worker_name.clone(),
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
                                    }
                                }
                                IsolateEventPayload::CacheRevalidate(payload) => {
                                    let _ = event_tx.send(RuntimeEvent::CacheRevalidate {
                                        worker_name: worker_name.clone(),
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
                                        worker_name: worker_name.clone(),
                                        generation,
                                        payload,
                                    });
                                }
                                IsolateEventPayload::ActorSocketConsumeClose(payload) => {
                                    let _ = event_tx.send(RuntimeEvent::ActorSocketConsumeClose {
                                        worker_name: worker_name.clone(),
                                        generation,
                                        payload,
                                    });
                                }
                            }
                        }
                        Some(command) = command_rx.recv() => {
                            match command {
                                IsolateCommand::Execute {
                                    runtime_request_id,
                                    completion_token,
                                    worker_name: worker_name_for_env,
                                    kv_bindings,
                                    actor_bindings,
                                    request,
                                    request_body,
                                    actor_call,
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
                                    let execute_span = tracing::info_span!(
                                        "runtime.isolate.execute",
                                        worker.name = %worker_name,
                                        worker.generation = generation,
                                        isolate.id = isolate_id,
                                        runtime.request_id = %runtime_request_id,
                                        request.id = %request_id
                                    );
                                    set_span_parent_from_traceparent(
                                        &execute_span,
                                        traceparent_from_headers(&request.headers).as_deref(),
                                    );
                                    let _execute_guard = execute_span.enter();
                                    let started_at = Instant::now();
                                    let dispatch_actor_call = actor_call.as_ref().map(|call| match call {
                                        ActorExecutionCall::Fetch { binding, key } => ExecuteActorCall::Fetch {
                                            binding: binding.clone(),
                                            key: key.clone(),
                                        },
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
                                    });
                                    if let Err(error) = dispatch_worker_request(
                                        &mut js_runtime,
                                        &runtime_request_id,
                                        &completion_token,
                                        &worker_name_for_env,
                                        &kv_bindings,
                                        &actor_bindings,
                                        has_request_body_stream,
                                        dispatch_actor_call.as_ref(),
                                        request,
                                    ) {
                                        tracing::warn!(
                                            dispatch_ms = started_at.elapsed().as_millis() as u64,
                                            error = %error,
                                            "failed to dispatch request into isolate"
                                        );
                                        let _ = event_tx.send(RuntimeEvent::RequestFinished {
                                            worker_name: worker_name.clone(),
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
                                }
                                IsolateCommand::Abort { runtime_request_id } => {
                                    {
                                        let op_state = js_runtime.op_state();
                                        let mut op_state = op_state.borrow_mut();
                                        cancel_request_body_stream(&mut op_state, &runtime_request_id);
                                        clear_request_body_stream(&mut op_state, &runtime_request_id);
                                        crate::ops::clear_actor_request_scope(
                                            &mut op_state,
                                            &runtime_request_id,
                                        );
                                    }
                                    if let Err(error) =
                                        abort_worker_request(&mut js_runtime, &runtime_request_id)
                                    {
                                        let _ = event_tx.send(RuntimeEvent::IsolateFailed {
                                            worker_name: worker_name.clone(),
                                            generation,
                                            isolate_id,
                                            error,
                                        });
                                        break;
                                    }
                                }
                                IsolateCommand::Shutdown => {
                                    break;
                                }
                            }
                        }
                        _ = ticker.tick() => {}
                        else => {
                            break;
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
            served_requests: 0,
            last_used_at: Instant::now(),
            pending_replies: HashMap::new(),
            pending_wait_until: HashMap::new(),
        }),
        Ok(Err(error)) => Err(error),
        Err(_) => Err(PlatformError::internal("isolate startup timed out")),
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
    use std::path::PathBuf;
    use std::time::{Duration, Instant};
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

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const key = queryParam(url.search, "key") ?? "default";
    const id = env.MY_ACTOR.idFromName(key);
    const actor = env.MY_ACTOR.get(id);

    if (url.pathname === "/run") {
      return actor.fetch("/actor/work", { method: "POST" });
    }

    if (url.pathname === "/max") {
      return new Response(String(await actor.maxActive()));
    }

    if (url.pathname === "/seed") {
      await actor.seedCount();
      return new Response("ok");
    }

    if (url.pathname === "/value-roundtrip") {
      const ok = await actor.valueRoundtrip();
      return new Response(ok ? "ok" : "bad", { status: ok ? 200 : 500 });
    }

    if (url.pathname === "/value-string-get-guard") {
      const ok = await actor.valueStringGetGuard();
      return new Response(ok ? "ok" : "bad", { status: ok ? 200 : 500 });
    }

    if (url.pathname === "/inc-cas") {
      const result = await actor.incCas();
      if (result && result.conflict) {
        return new Response("conflict", { status: 409 });
      }
      return new Response("ok");
    }

    if (url.pathname === "/get") {
      return new Response(String(await actor.getCount()));
    }

    return new Response("not found", { status: 404 });
  },
};

export class MyActor {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.active = 0;
    this.max = 0;
  }

  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === "/actor/work") {
      this.active += 1;
      if (this.active > this.max) {
        this.max = this.active;
      }
      await Deno.core.ops.op_sleep(25);
      this.active -= 1;
      return new Response("ok");
    }
    return new Response("not found", { status: 404 });
  }

  async maxActive() {
    return this.max;
  }

  async seedCount() {
    await this.state.storage.put("count", "0");
    return true;
  }

  async incCas() {
    const current = await this.state.storage.get("count");
    const currentValue = current ? asNumber(current.value, 0) : 0;
    const expectedVersion = current ? current.version : -1;
    await Deno.core.ops.op_sleep(10);
    return this.state.storage.put("count", String(currentValue + 1), { expectedVersion });
  }

  async getCount() {
    const current = await this.state.storage.get("count");
    return current ? String(current.value) : "0";
  }

  async valueRoundtrip() {
    const write = await this.state.storage.put("profile", {
      name: "alice",
      createdAt: new Date("2026-01-02T03:04:05.000Z"),
      flags: new Set(["a", "b"]),
      scores: new Map([["p95", 21], ["p99", 32]]),
      bytes: new Uint8Array([1, 2, 3, 4]),
    });
    if (write.conflict) {
      return false;
    }
    const loaded = await this.state.storage.get("profile");
    const value = loaded?.value;
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
  }

  async valueStringGetGuard() {
    await this.state.storage.put("profile", { nested: { ok: true } });
    const loaded = await this.state.storage.get("profile");
    return Boolean(
      loaded
        && loaded.encoding === "v8sc"
        && loaded.value
        && loaded.value.nested
        && loaded.value.nested.ok === true,
    );
  }
}
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
        for idx in 0..8 {
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
                        class: "MyActor".to_string(),
                    }],
                },
            )
            .await
            .expect("deploy should succeed");

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

        let max = service
            .invoke(
                "actor".to_string(),
                test_invocation_with_path("/max?key=user-1", "actor-max"),
            )
            .await
            .expect("max invoke should succeed");
        let parsed = String::from_utf8(max.body)
            .expect("utf8")
            .parse::<u64>()
            .expect("numeric max");
        assert!(parsed > 1, "expected overlap for same actor key by default");
    }

    #[tokio::test]
    #[serial]
    async fn actor_storage_cas_reports_conflicts_under_concurrency() {
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
                "actor".to_string(),
                actor_worker(),
                DeployConfig {
                    public: false,
                    internal: DeployInternalConfig { trace: None },
                    bindings: vec![DeployBinding::Actor {
                        binding: "MY_ACTOR".to_string(),
                        class: "MyActor".to_string(),
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
        for idx in 0..16 {
            let svc = service.clone();
            tasks.push(tokio::spawn(async move {
                svc.invoke(
                    "actor".to_string(),
                    test_invocation_with_path("/inc-cas?key=user-3", &format!("cas-{idx}")),
                )
                .await
            }));
        }

        let mut conflicts = 0usize;
        for task in tasks {
            let output = task.await.expect("join").expect("invoke should succeed");
            if output.status == 409 {
                conflicts += 1;
            }
        }

        assert!(conflicts > 0, "expected at least one CAS conflict");
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
                        class: "MyActor".to_string(),
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
}
