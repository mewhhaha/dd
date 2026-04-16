use crate::actor::{
    ActorBatchMutation, ActorDirectMutation, ActorProfileMetricKind, ActorReadDependency,
    ActorStore,
};
use crate::actor_rpc::{
    decode_actor_invoke_response, encode_actor_invoke_request, ActorInvokeCall, ActorInvokeRequest,
    ActorInvokeResponse,
};
use crate::cache::{CacheLookup, CacheRequest, CacheResponse, CacheStore};
use crate::kv::{
    KvBatchMutation, KvEntry, KvProfileMetricKind, KvProfileSnapshot, KvStore, KvUtf8Lookup,
};
use aes_gcm::aead::{Aead, Payload};
use aes_gcm::{Aes128Gcm, Aes256Gcm, KeyInit, Nonce};
use common::{PlatformError, Result, WorkerInvocation, WorkerOutput};
use deno_core::OpState;
use deno_permissions::{PermissionsContainer, RuntimePermissionDescriptorParser};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha1::Sha1;
use sha2::{Digest, Sha256, Sha384, Sha512};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::OnceLock;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use sys_traits::impls::RealSys;
use tokio::sync::{mpsc, oneshot, Mutex, Notify};

#[derive(Clone)]
pub struct IsolateEventSender(pub std::sync::mpsc::Sender<IsolateEventPayload>);

#[derive(Clone, Default)]
pub struct ActorOpenHandleRegistry {
    socket_handles: Arc<StdMutex<HashMap<String, HashSet<String>>>>,
    transport_handles: Arc<StdMutex<HashMap<String, HashSet<String>>>>,
}

impl ActorOpenHandleRegistry {
    fn owner_key(binding: &str, key: &str) -> String {
        format!("{binding}\u{001f}{key}")
    }

    pub fn list_socket_handles(&self, binding: &str, key: &str) -> Vec<String> {
        let owner_key = Self::owner_key(binding, key);
        let mut handles = self
            .socket_handles
            .lock()
            .expect("socket handle registry mutex poisoned")
            .get(&owner_key)
            .map(|values| values.iter().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        handles.sort();
        handles
    }

    pub fn list_transport_handles(&self, binding: &str, key: &str) -> Vec<String> {
        let owner_key = Self::owner_key(binding, key);
        let mut handles = self
            .transport_handles
            .lock()
            .expect("transport handle registry mutex poisoned")
            .get(&owner_key)
            .map(|values| values.iter().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        handles.sort();
        handles
    }

    pub fn add_socket_handle(&self, binding: &str, key: &str, handle: &str) {
        let owner_key = Self::owner_key(binding, key);
        self.socket_handles
            .lock()
            .expect("socket handle registry mutex poisoned")
            .entry(owner_key)
            .or_default()
            .insert(handle.to_string());
    }

    pub fn remove_socket_handle(&self, binding: &str, key: &str, handle: &str) {
        let owner_key = Self::owner_key(binding, key);
        let mut handles = self
            .socket_handles
            .lock()
            .expect("socket handle registry mutex poisoned");
        let remove_owner = if let Some(values) = handles.get_mut(&owner_key) {
            values.remove(handle);
            values.is_empty()
        } else {
            false
        };
        if remove_owner {
            handles.remove(&owner_key);
        }
    }

    pub fn add_transport_handle(&self, binding: &str, key: &str, handle: &str) {
        let owner_key = Self::owner_key(binding, key);
        self.transport_handles
            .lock()
            .expect("transport handle registry mutex poisoned")
            .entry(owner_key)
            .or_default()
            .insert(handle.to_string());
    }

    pub fn remove_transport_handle(&self, binding: &str, key: &str, handle: &str) {
        let owner_key = Self::owner_key(binding, key);
        let mut handles = self
            .transport_handles
            .lock()
            .expect("transport handle registry mutex poisoned");
        let remove_owner = if let Some(values) = handles.get_mut(&owner_key) {
            values.remove(handle);
            values.is_empty()
        } else {
            false
        };
        if remove_owner {
            handles.remove(&owner_key);
        }
    }

    pub fn clear(&self) {
        self.socket_handles
            .lock()
            .expect("socket handle registry mutex poisoned")
            .clear();
        self.transport_handles
            .lock()
            .expect("transport handle registry mutex poisoned")
            .clear();
    }
}

pub enum IsolateEventPayload {
    Completion(String),
    WaitUntilDone(String),
    ResponseStart(String),
    ResponseChunk(String),
    CacheRevalidate(String),
    ActorInvoke(ActorInvokeEvent),
    ActorSocketSend(ActorSocketSendEvent),
    ActorSocketClose(ActorSocketCloseEvent),
    ActorSocketConsumeClose(ActorSocketConsumeCloseEvent),
    ActorTransportSendStream(ActorTransportSendStreamEvent),
    ActorTransportSendDatagram(ActorTransportSendDatagramEvent),
    ActorTransportRecvStream(ActorTransportRecvStreamEvent),
    ActorTransportRecvDatagram(ActorTransportRecvDatagramEvent),
    ActorTransportClose(ActorTransportCloseEvent),
    ActorTransportConsumeClose(ActorTransportConsumeCloseEvent),
    DynamicWorkerCreate(DynamicWorkerCreateEvent),
    DynamicWorkerLookup(DynamicWorkerLookupEvent),
    DynamicWorkerList(DynamicWorkerListEvent),
    DynamicWorkerDelete(DynamicWorkerDeleteEvent),
    DynamicWorkerInvoke(DynamicWorkerInvokeEvent),
    DynamicHostRpcInvoke(DynamicHostRpcInvokeEvent),
    DynamicHostRpcTaskComplete(DynamicHostRpcTaskCompletePayload),
    TestAsyncReply(TestAsyncReplyEvent),
    TestNestedTargetedInvoke(TestNestedTargetedInvokeEvent),
}

pub type RequestBodyChunk = std::result::Result<Vec<u8>, String>;
pub type RequestBodyReceiver = mpsc::Receiver<RequestBodyChunk>;

pub struct ActorInvokeEvent {
    pub request_frame: Vec<u8>,
    pub caller_worker_name: String,
    pub caller_generation: u64,
    pub caller_isolate_id: u64,
    pub prefer_caller_isolate: bool,
    pub reply: oneshot::Sender<Result<Vec<u8>>>,
}

pub struct ActorSocketSendEvent {
    pub reply: oneshot::Sender<Result<()>>,
    pub handle: String,
    pub binding: String,
    pub key: String,
    pub is_text: bool,
    pub message: Vec<u8>,
}

pub struct ActorSocketCloseEvent {
    pub reply: oneshot::Sender<Result<()>>,
    pub handle: String,
    pub binding: String,
    pub key: String,
    pub code: u16,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ActorSocketCloseReplayEvent {
    pub code: u16,
    pub reason: String,
}

pub struct ActorSocketConsumeCloseEvent {
    pub reply: oneshot::Sender<Result<Vec<ActorSocketCloseReplayEvent>>>,
    pub binding: String,
    pub key: String,
    pub handle: String,
}

pub struct ActorTransportSendStreamEvent {
    pub reply: oneshot::Sender<Result<()>>,
    pub handle: String,
    pub binding: String,
    pub key: String,
    pub chunk: Vec<u8>,
}

pub struct ActorTransportSendDatagramEvent {
    pub reply: oneshot::Sender<Result<()>>,
    pub handle: String,
    pub binding: String,
    pub key: String,
    pub datagram: Vec<u8>,
}

pub struct ActorTransportRecvStreamEvent {
    pub reply: oneshot::Sender<Result<TransportRecvEvent>>,
    pub handle: String,
    pub binding: String,
    pub key: String,
}

pub struct ActorTransportRecvDatagramEvent {
    pub reply: oneshot::Sender<Result<TransportRecvEvent>>,
    pub handle: String,
    pub binding: String,
    pub key: String,
}

pub struct ActorTransportCloseEvent {
    pub reply: oneshot::Sender<Result<()>>,
    pub handle: String,
    pub binding: String,
    pub key: String,
    pub code: u16,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ActorTransportCloseReplayEvent {
    pub code: u16,
    pub reason: String,
}

pub struct ActorTransportConsumeCloseEvent {
    pub reply: oneshot::Sender<Result<Vec<ActorTransportCloseReplayEvent>>>,
    pub binding: String,
    pub key: String,
    pub handle: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct TransportRecvEvent {
    pub done: bool,
    pub chunk: Vec<u8>,
}

pub struct DynamicWorkerCreateEvent {
    pub owner_worker: String,
    pub owner_generation: u64,
    pub owner_isolate_id: u64,
    pub binding: String,
    pub id: String,
    pub source: String,
    pub env: HashMap<String, String>,
    pub timeout: u64,
    pub host_rpc_bindings: Vec<DynamicHostRpcBindingSpec>,
    pub reply_id: String,
    pub pending_replies: DynamicPendingReplies,
}

#[derive(Debug, Clone)]
pub struct DynamicWorkerCreateReply {
    pub handle: String,
    pub worker_name: String,
    pub timeout: u64,
}

pub struct DynamicWorkerLookupEvent {
    pub owner_worker: String,
    pub owner_generation: u64,
    pub owner_isolate_id: u64,
    pub binding: String,
    pub id: String,
    pub reply_id: String,
    pub pending_replies: DynamicPendingReplies,
}

pub struct DynamicWorkerListEvent {
    pub owner_worker: String,
    pub owner_generation: u64,
    pub owner_isolate_id: u64,
    pub binding: String,
    pub reply_id: String,
    pub pending_replies: DynamicPendingReplies,
}

pub struct DynamicWorkerDeleteEvent {
    pub owner_worker: String,
    pub owner_generation: u64,
    pub owner_isolate_id: u64,
    pub binding: String,
    pub id: String,
    pub reply_id: String,
    pub pending_replies: DynamicPendingReplies,
}

pub struct DynamicWorkerInvokeEvent {
    pub owner_worker: String,
    pub owner_generation: u64,
    pub owner_isolate_id: u64,
    pub binding: String,
    pub handle: String,
    pub request: WorkerInvocation,
    pub reply_id: String,
    pub pending_replies: DynamicPendingReplies,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DynamicHostRpcBindingSpec {
    pub binding: String,
    pub target_id: String,
    #[serde(default)]
    pub methods: Vec<String>,
}

pub struct DynamicHostRpcInvokeEvent {
    pub caller_worker: String,
    pub caller_generation: u64,
    pub _caller_isolate_id: u64,
    pub binding: String,
    pub method_name: String,
    pub args: Vec<u8>,
    pub reply_id: String,
    pub pending_replies: DynamicPendingReplies,
}

pub struct TestAsyncReplyEvent {
    pub reply_id: String,
    pub replies: TestAsyncReplies,
    pub delay_ms: u64,
    pub ok: bool,
    pub value: String,
    pub error: String,
}

#[derive(Clone)]
pub struct TestAsyncReplyOwner {
    pub worker_name: String,
    pub generation: u64,
    pub isolate_id: u64,
}

pub struct TestNestedTargetedInvokeEvent {
    pub worker_name: String,
    pub generation: u64,
    pub caller_isolate_id: u64,
    pub target_mode: String,
    pub target_id: String,
    pub method_name: String,
    pub args: Vec<u8>,
    pub reply_id: String,
    pub replies: TestAsyncReplies,
}

#[derive(Clone, Default)]
pub struct DynamicPendingReplies {
    entries: Arc<StdMutex<HashMap<String, DynamicPendingReplyEntry>>>,
    next_id: Arc<AtomicU64>,
}

#[derive(Clone, Default)]
pub struct TestAsyncReplies {
    entries: Arc<StdMutex<HashMap<String, TestAsyncReplyEntry>>>,
    next_id: Arc<AtomicU64>,
}

struct TestAsyncReplyEntry {
    owner: TestAsyncReplyOwner,
}

#[derive(Clone)]
pub struct DynamicPendingReplyOwner {
    pub worker_name: String,
    pub generation: u64,
    pub isolate_id: u64,
}

struct DynamicPendingReplyEntry {
    owner: DynamicPendingReplyOwner,
}

pub enum DynamicPendingReplyPayload {
    Create(Result<DynamicWorkerCreateReply>),
    Lookup(Result<Option<DynamicWorkerCreateReply>>),
    List(Result<Vec<String>>),
    Delete(Result<bool>),
    Invoke(Result<WorkerOutput>),
    HostRpc(Result<Vec<u8>>),
}

pub struct DynamicPendingReplyDelivery {
    pub owner: DynamicPendingReplyOwner,
    pub payload: DynamicPendingReplyResult,
}

#[derive(Serialize)]
pub struct DynamicPendingReplyResult {
    pub reply_id: String,
    ready: bool,
    kind: String,
    ok: bool,
    found: bool,
    deleted: bool,
    handle: String,
    worker: String,
    timeout: u64,
    ids: Vec<String>,
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
    value: Vec<u8>,
    error: String,
}

#[derive(Deserialize)]
pub struct DynamicHostRpcTaskCompletePayload {
    pub task_id: String,
    pub ok: bool,
    #[serde(default)]
    pub value: Vec<u8>,
    #[serde(default)]
    pub error: String,
}

#[derive(Serialize)]
struct DynamicPendingReplyStartResult {
    ok: bool,
    reply_id: String,
    error: String,
}

#[derive(Serialize)]
pub struct TestAsyncReplyResult {
    pub reply_id: String,
    pub ok: bool,
    pub value: String,
    pub error: String,
}

impl Default for DynamicPendingReplyResult {
    fn default() -> Self {
        Self {
            reply_id: String::new(),
            ready: false,
            kind: String::new(),
            ok: false,
            found: false,
            deleted: false,
            handle: String::new(),
            worker: String::new(),
            timeout: 0,
            ids: Vec::new(),
            status: 0,
            headers: Vec::new(),
            body: Vec::new(),
            value: Vec::new(),
            error: String::new(),
        }
    }
}

impl DynamicPendingReplies {
    pub fn allocate(&self, owner: DynamicPendingReplyOwner) -> String {
        let reply_id = format!("dynr-{}", self.next_id.fetch_add(1, Ordering::Relaxed));
        let mut entries = self
            .entries
            .lock()
            .expect("dynamic pending reply lock poisoned");
        entries.insert(reply_id.clone(), DynamicPendingReplyEntry { owner });
        reply_id
    }

    pub fn cancel(&self, reply_id: &str) {
        let mut entries = self
            .entries
            .lock()
            .expect("dynamic pending reply lock poisoned");
        entries.remove(reply_id);
    }

    pub fn finish(
        &self,
        reply_id: String,
        payload: DynamicPendingReplyPayload,
    ) -> Option<DynamicPendingReplyDelivery> {
        let mut entries = self
            .entries
            .lock()
            .expect("dynamic pending reply lock poisoned");
        let entry = entries.remove(&reply_id)?;
        Some(DynamicPendingReplyDelivery {
            owner: entry.owner,
            payload: payload.into_result(reply_id),
        })
    }
}

impl DynamicPendingReplyPayload {
    fn into_result(self, reply_id: String) -> DynamicPendingReplyResult {
        match self {
            Self::Create(result) => match result {
                Ok(created) => DynamicPendingReplyResult {
                    reply_id,
                    ready: true,
                    kind: "create".to_string(),
                    ok: true,
                    handle: created.handle,
                    worker: created.worker_name,
                    timeout: created.timeout,
                    ..DynamicPendingReplyResult::default()
                },
                Err(error) => DynamicPendingReplyResult {
                    reply_id,
                    ready: true,
                    kind: "create".to_string(),
                    ok: false,
                    error: error.to_string(),
                    ..DynamicPendingReplyResult::default()
                },
            },
            Self::Lookup(result) => match result {
                Ok(Some(found)) => DynamicPendingReplyResult {
                    reply_id,
                    ready: true,
                    kind: "lookup".to_string(),
                    ok: true,
                    found: true,
                    handle: found.handle,
                    worker: found.worker_name,
                    timeout: found.timeout,
                    ..DynamicPendingReplyResult::default()
                },
                Ok(None) => DynamicPendingReplyResult {
                    reply_id,
                    ready: true,
                    kind: "lookup".to_string(),
                    ok: true,
                    found: false,
                    ..DynamicPendingReplyResult::default()
                },
                Err(error) => DynamicPendingReplyResult {
                    reply_id,
                    ready: true,
                    kind: "lookup".to_string(),
                    ok: false,
                    error: error.to_string(),
                    ..DynamicPendingReplyResult::default()
                },
            },
            Self::List(result) => match result {
                Ok(ids) => DynamicPendingReplyResult {
                    reply_id,
                    ready: true,
                    kind: "list".to_string(),
                    ok: true,
                    ids,
                    ..DynamicPendingReplyResult::default()
                },
                Err(error) => DynamicPendingReplyResult {
                    reply_id,
                    ready: true,
                    kind: "list".to_string(),
                    ok: false,
                    error: error.to_string(),
                    ..DynamicPendingReplyResult::default()
                },
            },
            Self::Delete(result) => match result {
                Ok(deleted) => DynamicPendingReplyResult {
                    reply_id,
                    ready: true,
                    kind: "delete".to_string(),
                    ok: true,
                    deleted,
                    ..DynamicPendingReplyResult::default()
                },
                Err(error) => DynamicPendingReplyResult {
                    reply_id,
                    ready: true,
                    kind: "delete".to_string(),
                    ok: false,
                    error: error.to_string(),
                    ..DynamicPendingReplyResult::default()
                },
            },
            Self::Invoke(result) => match result {
                Ok(output) => DynamicPendingReplyResult {
                    reply_id,
                    ready: true,
                    kind: "invoke".to_string(),
                    ok: true,
                    status: output.status,
                    headers: output.headers,
                    body: output.body,
                    ..DynamicPendingReplyResult::default()
                },
                Err(error) => DynamicPendingReplyResult {
                    reply_id,
                    ready: true,
                    kind: "invoke".to_string(),
                    ok: false,
                    error: error.to_string(),
                    ..DynamicPendingReplyResult::default()
                },
            },
            Self::HostRpc(result) => match result {
                Ok(value) => DynamicPendingReplyResult {
                    reply_id,
                    ready: true,
                    kind: "host-rpc".to_string(),
                    ok: true,
                    value,
                    ..DynamicPendingReplyResult::default()
                },
                Err(error) => DynamicPendingReplyResult {
                    reply_id,
                    ready: true,
                    kind: "host-rpc".to_string(),
                    ok: false,
                    error: error.to_string(),
                    ..DynamicPendingReplyResult::default()
                },
            },
        }
    }
}

impl TestAsyncReplies {
    pub fn allocate(&self, owner: TestAsyncReplyOwner) -> String {
        let reply_id = format!("tasr-{}", self.next_id.fetch_add(1, Ordering::Relaxed));
        let mut entries = self.entries.lock().expect("test async reply lock poisoned");
        entries.insert(reply_id.clone(), TestAsyncReplyEntry { owner });
        reply_id
    }

    pub fn cancel(&self, reply_id: &str) {
        let mut entries = self.entries.lock().expect("test async reply lock poisoned");
        entries.remove(reply_id);
    }

    pub fn finish(
        &self,
        reply_id: String,
        result: Result<String>,
    ) -> Option<TestAsyncReplyDelivery> {
        let mut entries = self.entries.lock().expect("test async reply lock poisoned");
        let entry = entries.remove(&reply_id)?;
        let (ok, value, error) = match result {
            Ok(value) => (true, value, String::new()),
            Err(error) => (false, String::new(), error.to_string()),
        };
        Some(TestAsyncReplyDelivery {
            owner: entry.owner,
            payload: TestAsyncReplyResult {
                reply_id,
                ok,
                value,
                error,
            },
        })
    }
}

pub struct TestAsyncReplyDelivery {
    pub owner: TestAsyncReplyOwner,
    pub payload: TestAsyncReplyResult,
}

#[derive(Default)]
pub struct ActorRequestScopes {
    scopes: HashMap<String, ActorRequestScope>,
}

#[derive(Clone)]
struct ActorRequestScope {
    namespace: String,
    actor_key: String,
}

#[derive(Default)]
pub struct RequestSecretContexts {
    contexts: HashMap<String, RequestSecretContext>,
}

struct RequestSecretContext {
    worker_name: String,
    generation: u64,
    isolate_id: u64,
    dynamic_bindings: HashSet<String>,
    dynamic_rpc_bindings: HashSet<String>,
    replacements: HashMap<String, String>,
    egress_allow_hosts: Vec<String>,
    canceled: Arc<AtomicBool>,
    canceled_notify: Arc<Notify>,
}

#[derive(Default)]
pub struct RequestBodyStreams {
    streams: HashMap<String, Arc<RequestBodyStream>>,
}

struct RequestBodyStream {
    receiver: Mutex<RequestBodyReceiver>,
    canceled: AtomicBool,
    canceled_notify: Notify,
}

impl RequestBodyStream {
    fn new(receiver: RequestBodyReceiver) -> Self {
        Self {
            receiver: Mutex::new(receiver),
            canceled: AtomicBool::new(false),
            canceled_notify: Notify::new(),
        }
    }

    fn cancel(&self) {
        self.canceled.store(true, Ordering::SeqCst);
        self.canceled_notify.notify_waiters();
    }

    fn is_canceled(&self) -> bool {
        self.canceled.load(Ordering::SeqCst)
    }
}

#[derive(Debug, Serialize)]
struct TimeBoundary {
    now_ms: u64,
    perf_ms: f64,
}

#[derive(Debug, Serialize)]
struct KvListItem {
    key: String,
    value: Vec<u8>,
    encoding: String,
}

#[derive(Debug, Serialize)]
struct KvGetResult {
    ok: bool,
    found: bool,
    wrong_encoding: bool,
    value: String,
    error: String,
}

#[derive(Debug, Deserialize)]
struct KvGetManyPayload {
    worker_name: String,
    binding: String,
    keys: Vec<String>,
}

#[derive(Debug, Serialize)]
struct KvGetManyItem {
    found: bool,
    wrong_encoding: bool,
    value: String,
}

#[derive(Debug, Serialize)]
struct KvGetManyResult {
    ok: bool,
    values: Vec<KvGetManyItem>,
    error: String,
}

#[derive(Debug, Serialize)]
struct KvProfileResult {
    ok: bool,
    snapshot: Option<KvProfileSnapshot>,
    error: String,
}

#[derive(Debug, Deserialize)]
struct KvGetValuePayload {
    worker_name: String,
    binding: String,
    key: String,
}

#[derive(Debug, Serialize)]
struct KvGetValueResult {
    ok: bool,
    found: bool,
    value: Vec<u8>,
    encoding: String,
    error: String,
}

#[derive(Debug, Deserialize)]
struct KvPutValuePayload {
    worker_name: String,
    binding: String,
    key: String,
    encoding: String,
    value: Vec<u8>,
}

#[derive(Debug, Clone, Deserialize)]
struct KvApplyBatchPayload {
    worker_name: String,
    binding: String,
    mutations: Vec<KvApplyBatchMutationPayload>,
}

#[derive(Debug, Clone, Deserialize)]
struct KvApplyBatchMutationPayload {
    key: String,
    encoding: String,
    value: Vec<u8>,
    deleted: bool,
}

#[derive(Debug, Serialize)]
struct KvOpResult {
    ok: bool,
    error: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    version: Option<i64>,
}

#[derive(Debug, Serialize)]
struct KvListResult {
    ok: bool,
    entries: Vec<KvListItem>,
    error: String,
}

#[derive(Debug, Deserialize)]
struct CacheRequestPayload {
    cache_name: String,
    method: String,
    url: String,
    headers: Vec<(String, String)>,
    #[serde(default)]
    bypass_stale: bool,
}

#[derive(Debug, Deserialize)]
struct CachePutPayload {
    cache_name: String,
    method: String,
    url: String,
    request_headers: Vec<(String, String)>,
    response_status: u16,
    response_headers: Vec<(String, String)>,
    response_body: Vec<u8>,
}

#[derive(Debug, Serialize)]
struct CacheMatchResult {
    ok: bool,
    found: bool,
    stale: bool,
    should_revalidate: bool,
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
    error: String,
}

#[derive(Debug, Serialize)]
struct CacheDeleteResult {
    ok: bool,
    deleted: bool,
    error: String,
}

#[derive(Debug, Deserialize)]
struct HttpFetchPayload {
    request_id: String,
    method: String,
    url: String,
    #[serde(default)]
    headers: Vec<(String, String)>,
    #[serde(default)]
    body: Vec<u8>,
}

#[derive(Debug, Serialize)]
struct HttpPrepareResult {
    ok: bool,
    method: String,
    url: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
    error: String,
}

#[derive(Debug, Deserialize)]
struct HttpUrlCheckPayload {
    request_id: String,
    url: String,
}

#[derive(Debug, Serialize)]
struct HttpUrlCheckResult {
    ok: bool,
    url: String,
    error: String,
}

#[derive(Debug, Deserialize)]
struct DynamicWorkerCreatePayload {
    request_id: String,
    binding: String,
    id: String,
    source: String,
    #[serde(default)]
    env: HashMap<String, String>,
    #[serde(default = "default_dynamic_worker_timeout")]
    timeout: u64,
    #[serde(default)]
    host_rpc_bindings: Vec<DynamicHostRpcBindingSpec>,
}

#[derive(Debug, Deserialize)]
struct DynamicWorkerLookupPayload {
    request_id: String,
    binding: String,
    id: String,
}

#[derive(Debug, Deserialize)]
struct DynamicWorkerListPayload {
    request_id: String,
    binding: String,
}

#[derive(Debug, Deserialize)]
struct DynamicWorkerDeletePayload {
    request_id: String,
    binding: String,
    id: String,
}

fn default_dynamic_worker_timeout() -> u64 {
    5_000
}

#[derive(Debug, Deserialize)]
struct DynamicWorkerInvokePayload {
    request_id: String,
    subrequest_id: String,
    binding: String,
    handle: String,
    method: String,
    url: String,
    #[serde(default)]
    headers: Vec<(String, String)>,
    #[serde(default)]
    body: Vec<u8>,
}

#[derive(Debug, Deserialize)]
struct DynamicHostRpcInvokePayload {
    request_id: String,
    binding: String,
    method_name: String,
    #[serde(default)]
    args: Vec<u8>,
}

#[derive(Clone, Default)]
pub struct DynamicProfile {
    warm_isolate_hit: Arc<AtomicU64>,
    fallback_dispatch: Arc<AtomicU64>,
    async_reply_completion: Arc<AtomicU64>,
    local_host_rpc_callback: Arc<AtomicU64>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct DynamicProfileSnapshot {
    pub warm_isolate_hit: u64,
    pub fallback_dispatch: u64,
    pub async_reply_completion: u64,
    pub local_host_rpc_callback: u64,
}

#[derive(Debug, Serialize)]
struct DynamicProfileResult {
    ok: bool,
    snapshot: Option<DynamicProfileSnapshot>,
    error: String,
}

impl DynamicProfile {
    pub fn record_warm_isolate_hit(&self) {
        self.warm_isolate_hit.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_fallback_dispatch(&self) {
        self.fallback_dispatch.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_async_reply_completion(&self) {
        self.async_reply_completion.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_local_host_rpc_callback(&self) {
        self.local_host_rpc_callback.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> DynamicProfileSnapshot {
        DynamicProfileSnapshot {
            warm_isolate_hit: self.warm_isolate_hit.load(Ordering::Relaxed),
            fallback_dispatch: self.fallback_dispatch.load(Ordering::Relaxed),
            async_reply_completion: self.async_reply_completion.load(Ordering::Relaxed),
            local_host_rpc_callback: self.local_host_rpc_callback.load(Ordering::Relaxed),
        }
    }

    pub fn reset(&self) {
        self.warm_isolate_hit.store(0, Ordering::Relaxed);
        self.fallback_dispatch.store(0, Ordering::Relaxed);
        self.async_reply_completion.store(0, Ordering::Relaxed);
        self.local_host_rpc_callback.store(0, Ordering::Relaxed);
    }
}

#[derive(Debug, Serialize)]
struct RequestBodyReadResult {
    ok: bool,
    done: bool,
    chunk: Vec<u8>,
    error: String,
}

#[derive(Debug, Deserialize)]
struct CompletionMeta {
    request_id: String,
    #[serde(default)]
    wait_until_count: usize,
}

#[derive(Debug, Deserialize)]
struct WaitUntilRequestId {
    request_id: String,
}

#[derive(Debug, Deserialize)]
struct ActorInvokeMethodPayload {
    caller_request_id: String,
    worker_name: String,
    binding: String,
    key: String,
    method_name: String,
    #[serde(default)]
    prefer_caller_isolate: bool,
    #[serde(default)]
    args: Vec<u8>,
    request_id: String,
}

#[derive(Debug, Serialize)]
struct ActorInvokeMethodResult {
    ok: bool,
    value: Vec<u8>,
    error: String,
}

#[derive(Debug, Deserialize)]
struct ActorSocketSendPayload {
    request_id: String,
    handle: String,
    #[serde(default)]
    binding: String,
    #[serde(default)]
    key: String,
    message_kind: String,
    message: Vec<u8>,
}

#[derive(Debug, Serialize)]
struct ActorSocketSendResult {
    ok: bool,
    error: String,
}

#[derive(Debug, Deserialize)]
struct ActorSocketClosePayload {
    request_id: String,
    handle: String,
    #[serde(default)]
    binding: String,
    #[serde(default)]
    key: String,
    code: u16,
    reason: String,
}

#[derive(Debug, Deserialize)]
struct ActorSocketListPayload {
    request_id: String,
    #[serde(default)]
    binding: String,
    #[serde(default)]
    key: String,
}

#[derive(Debug, Serialize)]
struct ActorSocketListResult {
    ok: bool,
    handles: Vec<String>,
    error: String,
}

#[derive(Debug, Deserialize)]
struct ActorSocketConsumeClosePayload {
    request_id: String,
    handle: String,
    #[serde(default)]
    binding: String,
    #[serde(default)]
    key: String,
}

#[derive(Debug, Serialize)]
struct ActorSocketReplayClose {
    code: u16,
    reason: String,
}

#[derive(Debug, Serialize)]
struct ActorSocketConsumeCloseResult {
    ok: bool,
    events: Vec<ActorSocketReplayClose>,
    error: String,
}

#[derive(Debug, Serialize)]
struct ActorSocketCloseResult {
    ok: bool,
    error: String,
}

#[derive(Debug, Deserialize)]
struct ActorTransportSendStreamPayload {
    request_id: String,
    handle: String,
    #[serde(default)]
    binding: String,
    #[serde(default)]
    key: String,
    chunk: Vec<u8>,
}

#[derive(Debug, Deserialize)]
struct ActorTransportSendDatagramPayload {
    request_id: String,
    handle: String,
    #[serde(default)]
    binding: String,
    #[serde(default)]
    key: String,
    datagram: Vec<u8>,
}

#[derive(Debug, Deserialize)]
struct ActorTransportRecvPayload {
    request_id: String,
    handle: String,
    #[serde(default)]
    binding: String,
    #[serde(default)]
    key: String,
}

#[derive(Debug, Serialize)]
struct ActorTransportSendResult {
    ok: bool,
    error: String,
}

#[derive(Debug, Serialize)]
struct ActorTransportRecvResult {
    ok: bool,
    done: bool,
    chunk: Vec<u8>,
    error: String,
}

#[derive(Debug, Deserialize)]
struct ActorTransportClosePayload {
    request_id: String,
    handle: String,
    #[serde(default)]
    binding: String,
    #[serde(default)]
    key: String,
    code: u16,
    reason: String,
}

#[derive(Debug, Deserialize)]
struct ActorTransportListPayload {
    request_id: String,
    #[serde(default)]
    binding: String,
    #[serde(default)]
    key: String,
}

#[derive(Debug, Serialize)]
struct ActorTransportListResult {
    ok: bool,
    handles: Vec<String>,
    error: String,
}

#[derive(Debug, Deserialize)]
struct ActorTransportConsumeClosePayload {
    request_id: String,
    handle: String,
    #[serde(default)]
    binding: String,
    #[serde(default)]
    key: String,
}

#[derive(Debug, Serialize)]
struct ActorTransportReplayClose {
    code: u16,
    reason: String,
}

#[derive(Debug, Serialize)]
struct ActorTransportConsumeCloseResult {
    ok: bool,
    events: Vec<ActorTransportReplayClose>,
    error: String,
}

#[derive(Debug, Serialize)]
struct ActorTransportCloseResult {
    ok: bool,
    error: String,
}

#[derive(Debug, Deserialize)]
struct ActorStateSnapshotPayload {
    request_id: String,
    #[serde(default)]
    binding: String,
    #[serde(default)]
    key: String,
    #[serde(default)]
    keys: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct ActorStateGetPayload {
    request_id: String,
    #[serde(default)]
    binding: String,
    #[serde(default)]
    key: String,
    item_key: String,
}

#[derive(Debug, Serialize)]
struct ActorStateGetEntry {
    key: String,
    value: Vec<u8>,
    encoding: String,
    version: i64,
    deleted: bool,
}

#[derive(Debug, Serialize)]
struct ActorStateGetResult {
    ok: bool,
    record: Option<ActorStateGetEntry>,
    max_version: i64,
    error: String,
}

#[derive(Debug, Serialize)]
struct ActorStateSnapshotEntry {
    key: String,
    value: Vec<u8>,
    encoding: String,
    version: i64,
    deleted: bool,
}

#[derive(Debug, Serialize)]
struct ActorStateSnapshotResult {
    ok: bool,
    entries: Vec<ActorStateSnapshotEntry>,
    max_version: i64,
    error: String,
}

#[derive(Debug, Serialize)]
struct ActorProfileResult {
    ok: bool,
    snapshot: Option<crate::actor::ActorProfileSnapshot>,
    error: String,
}

#[derive(Debug, Deserialize)]
struct ActorStateVersionIfNewerPayload {
    request_id: String,
    #[serde(default)]
    binding: String,
    #[serde(default)]
    key: String,
    known_version: i64,
}

#[derive(Debug, Serialize)]
struct ActorStateVersionIfNewerResult {
    ok: bool,
    stale: bool,
    max_version: i64,
    error: String,
}

#[derive(Debug, Deserialize)]
struct ActorStateBatchMutationPayload {
    key: String,
    value: Vec<u8>,
    encoding: String,
    version: i64,
    deleted: bool,
}

#[derive(Debug, Deserialize)]
struct ActorStateReadDependencyPayload {
    key: String,
    version: i64,
}

#[derive(Debug, Deserialize)]
struct ActorStateApplyBatchPayload {
    request_id: String,
    #[serde(default)]
    binding: String,
    #[serde(default)]
    key: String,
    expected_base_version: i64,
    #[serde(default)]
    transactional: bool,
    #[serde(default)]
    reads: Vec<ActorStateReadDependencyPayload>,
    #[serde(default)]
    list_gate_version: i64,
    mutations: Vec<ActorStateBatchMutationPayload>,
}

#[derive(Debug, Deserialize)]
struct ActorStateApplyBlindBatchPayload {
    request_id: String,
    #[serde(default)]
    binding: String,
    #[serde(default)]
    key: String,
    mutations: Vec<ActorStateBatchMutationPayload>,
}

#[derive(Debug, Serialize)]
struct ActorStateApplyBatchResult {
    ok: bool,
    conflict: bool,
    max_version: i64,
    error: String,
}

#[derive(Debug, Deserialize)]
struct ActorStateEnqueueBatchMutationPayload {
    key: String,
    value: Vec<u8>,
    encoding: String,
    deleted: bool,
}

#[derive(Debug, Deserialize)]
struct ActorStateEnqueueBatchPayload {
    request_id: String,
    #[serde(default)]
    binding: String,
    #[serde(default)]
    key: String,
    mutations: Vec<ActorStateEnqueueBatchMutationPayload>,
}

#[derive(Debug, Serialize)]
struct ActorStateEnqueueBatchResult {
    ok: bool,
    submission_id: u64,
    error: String,
}

#[derive(Debug, Deserialize)]
struct ActorStateAwaitSubmissionPayload {
    submission_id: u64,
}

#[derive(Debug, Serialize)]
struct ActorStateAwaitSubmissionResult {
    ok: bool,
    pending: bool,
    max_version: i64,
    error: String,
}

#[derive(Debug, Deserialize)]
struct ActorStateValidateReadsPayload {
    request_id: String,
    #[serde(default)]
    binding: String,
    #[serde(default)]
    key: String,
    #[serde(default)]
    reads: Vec<ActorStateReadDependencyPayload>,
    #[serde(default)]
    list_gate_version: i64,
}

#[derive(Debug, Deserialize)]
struct ActorScopePayload {
    request_id: String,
    binding: String,
    key: String,
}

#[derive(Debug, Deserialize)]
struct ActorScopeClearPayload {
    request_id: String,
}

#[derive(Debug, Serialize)]
struct ActorScopeResult {
    ok: bool,
    error: String,
}

#[derive(Debug, Deserialize)]
struct CryptoDigestPayload {
    algorithm: String,
    data: Vec<u8>,
}

#[derive(Debug, Serialize)]
struct CryptoDigestResult {
    ok: bool,
    digest: Vec<u8>,
    error: String,
}

#[derive(Debug, Deserialize)]
struct CryptoHmacPayload {
    hash: String,
    key: Vec<u8>,
    data: Vec<u8>,
}

#[derive(Debug, Deserialize)]
struct CryptoHmacVerifyPayload {
    hash: String,
    key: Vec<u8>,
    data: Vec<u8>,
    signature: Vec<u8>,
}

#[derive(Debug, Deserialize)]
struct CryptoAesGcmPayload {
    key: Vec<u8>,
    iv: Vec<u8>,
    data: Vec<u8>,
    #[serde(default)]
    additional_data: Vec<u8>,
    #[serde(default = "default_tag_length_bits")]
    tag_length: u8,
}

#[derive(Debug, Serialize)]
struct CryptoBytesResult {
    ok: bool,
    bytes: Vec<u8>,
    error: String,
}

#[derive(Debug, Serialize)]
struct CryptoBoolResult {
    ok: bool,
    value: bool,
    error: String,
}

enum CryptoDigestAlgorithm {
    Sha1,
    Sha256,
    Sha384,
    Sha512,
}

fn default_tag_length_bits() -> u8 {
    128
}

static PROCESS_MONO_START: OnceLock<Instant> = OnceLock::new();

#[deno_core::op2]
async fn op_sleep(millis: u32) {
    tokio::time::sleep(Duration::from_millis(u64::from(millis))).await;
}

#[deno_core::op2]
#[serde]
fn op_time_boundary_now() -> TimeBoundary {
    let now_ms = wall_ms();
    let perf_ms = PROCESS_MONO_START
        .get_or_init(Instant::now)
        .elapsed()
        .as_secs_f64()
        * 1000.0;
    TimeBoundary { now_ms, perf_ms }
}

#[deno_core::op2]
#[serde]
fn op_crypto_digest(#[string] payload: String) -> CryptoDigestResult {
    let payload = match crate::json::from_string::<CryptoDigestPayload>(payload) {
        Ok(value) => value,
        Err(error) => {
            return CryptoDigestResult {
                ok: false,
                digest: Vec::new(),
                error: format!("invalid digest payload: {error}"),
            };
        }
    };

    let algorithm = match parse_crypto_digest_algorithm(&payload.algorithm) {
        Some(value) => value,
        None => {
            return CryptoDigestResult {
                ok: false,
                digest: Vec::new(),
                error: format!("unsupported digest algorithm: {}", payload.algorithm),
            };
        }
    };

    let digest = match algorithm {
        CryptoDigestAlgorithm::Sha1 => {
            let mut hasher = Sha1::new();
            hasher.update(&payload.data);
            hasher.finalize().to_vec()
        }
        CryptoDigestAlgorithm::Sha256 => {
            let mut hasher = Sha256::new();
            hasher.update(&payload.data);
            hasher.finalize().to_vec()
        }
        CryptoDigestAlgorithm::Sha384 => {
            let mut hasher = Sha384::new();
            hasher.update(&payload.data);
            hasher.finalize().to_vec()
        }
        CryptoDigestAlgorithm::Sha512 => {
            let mut hasher = Sha512::new();
            hasher.update(&payload.data);
            hasher.finalize().to_vec()
        }
    };

    CryptoDigestResult {
        ok: true,
        digest,
        error: String::new(),
    }
}

#[deno_core::op2]
#[serde]
fn op_crypto_hmac_sign(#[string] payload: String) -> CryptoBytesResult {
    let payload: CryptoHmacPayload = match crate::json::from_string(payload) {
        Ok(value) => value,
        Err(error) => {
            return CryptoBytesResult {
                ok: false,
                bytes: Vec::new(),
                error: format!("invalid hmac sign payload: {error}"),
            };
        }
    };

    let hash = match parse_crypto_digest_algorithm(&payload.hash) {
        Some(value) => value,
        None => {
            return CryptoBytesResult {
                ok: false,
                bytes: Vec::new(),
                error: format!("unsupported hmac hash algorithm: {}", payload.hash),
            };
        }
    };

    match compute_hmac(hash, &payload.key, &payload.data) {
        Ok(bytes) => CryptoBytesResult {
            ok: true,
            bytes,
            error: String::new(),
        },
        Err(error) => CryptoBytesResult {
            ok: false,
            bytes: Vec::new(),
            error,
        },
    }
}

#[deno_core::op2]
#[serde]
fn op_crypto_hmac_verify(#[string] payload: String) -> CryptoBoolResult {
    let payload: CryptoHmacVerifyPayload = match crate::json::from_string(payload) {
        Ok(value) => value,
        Err(error) => {
            return CryptoBoolResult {
                ok: false,
                value: false,
                error: format!("invalid hmac verify payload: {error}"),
            };
        }
    };

    let hash = match parse_crypto_digest_algorithm(&payload.hash) {
        Some(value) => value,
        None => {
            return CryptoBoolResult {
                ok: false,
                value: false,
                error: format!("unsupported hmac hash algorithm: {}", payload.hash),
            };
        }
    };

    match verify_hmac(hash, &payload.key, &payload.data, &payload.signature) {
        Ok(value) => CryptoBoolResult {
            ok: true,
            value,
            error: String::new(),
        },
        Err(error) => CryptoBoolResult {
            ok: false,
            value: false,
            error,
        },
    }
}

#[deno_core::op2]
#[serde]
fn op_crypto_aes_gcm_encrypt(#[string] payload: String) -> CryptoBytesResult {
    let payload: CryptoAesGcmPayload = match crate::json::from_string(payload) {
        Ok(value) => value,
        Err(error) => {
            return CryptoBytesResult {
                ok: false,
                bytes: Vec::new(),
                error: format!("invalid aes-gcm encrypt payload: {error}"),
            };
        }
    };

    match aes_gcm_encrypt(&payload) {
        Ok(bytes) => CryptoBytesResult {
            ok: true,
            bytes,
            error: String::new(),
        },
        Err(error) => CryptoBytesResult {
            ok: false,
            bytes: Vec::new(),
            error,
        },
    }
}

#[deno_core::op2]
#[serde]
fn op_crypto_aes_gcm_decrypt(#[string] payload: String) -> CryptoBytesResult {
    let payload: CryptoAesGcmPayload = match crate::json::from_string(payload) {
        Ok(value) => value,
        Err(error) => {
            return CryptoBytesResult {
                ok: false,
                bytes: Vec::new(),
                error: format!("invalid aes-gcm decrypt payload: {error}"),
            };
        }
    };

    match aes_gcm_decrypt(&payload) {
        Ok(bytes) => CryptoBytesResult {
            ok: true,
            bytes,
            error: String::new(),
        },
        Err(error) => CryptoBytesResult {
            ok: false,
            bytes: Vec::new(),
            error,
        },
    }
}

#[deno_core::op2]
#[serde]
async fn op_kv_get(
    state: Rc<RefCell<OpState>>,
    #[string] worker_name: String,
    #[string] binding: String,
    #[string] key: String,
) -> KvGetResult {
    let started = Instant::now();
    let store = state.borrow().borrow::<KvStore>().clone();
    let result = match store.get_utf8(&worker_name, &binding, &key).await {
        Ok(Ok(decoded)) => KvGetResult {
            ok: true,
            found: true,
            wrong_encoding: false,
            value: decoded,
            error: String::new(),
        },
        Ok(Err(KvUtf8Lookup::Missing)) => KvGetResult {
            ok: true,
            found: false,
            wrong_encoding: false,
            value: String::new(),
            error: String::new(),
        },
        Ok(Err(KvUtf8Lookup::WrongEncoding)) => KvGetResult {
            ok: false,
            found: true,
            wrong_encoding: true,
            value: String::new(),
            error: String::new(),
        },
        Err(error) => KvGetResult {
            ok: false,
            found: false,
            wrong_encoding: false,
            value: String::new(),
            error: error.to_string(),
        },
    };
    store.record_profile(
        KvProfileMetricKind::OpGet,
        started.elapsed().as_micros() as u64,
        1,
    );
    result
}

#[deno_core::op2]
#[serde]
async fn op_kv_get_many_utf8(
    state: Rc<RefCell<OpState>>,
    #[string] payload: String,
) -> KvGetManyResult {
    let started = Instant::now();
    let payload: KvGetManyPayload = match crate::json::from_string(payload) {
        Ok(value) => value,
        Err(error) => {
            return KvGetManyResult {
                ok: false,
                values: Vec::new(),
                error: format!("invalid kv get many payload: {error}"),
            };
        }
    };
    let store = state.borrow().borrow::<KvStore>().clone();
    let item_count = payload.keys.len() as u64;
    let result = match store
        .get_utf8_many(&payload.worker_name, &payload.binding, &payload.keys)
        .await
    {
        Ok(values) => KvGetManyResult {
            ok: true,
            values: values
                .into_iter()
                .map(|value| match value {
                    Ok(decoded) => KvGetManyItem {
                        found: true,
                        wrong_encoding: false,
                        value: decoded,
                    },
                    Err(KvUtf8Lookup::Missing) => KvGetManyItem {
                        found: false,
                        wrong_encoding: false,
                        value: String::new(),
                    },
                    Err(KvUtf8Lookup::WrongEncoding) => KvGetManyItem {
                        found: true,
                        wrong_encoding: true,
                        value: String::new(),
                    },
                })
                .collect(),
            error: String::new(),
        },
        Err(error) => KvGetManyResult {
            ok: false,
            values: Vec::new(),
            error: error.to_string(),
        },
    };
    store.record_profile(
        KvProfileMetricKind::OpGetManyUtf8,
        started.elapsed().as_micros() as u64,
        item_count,
    );
    result
}

#[deno_core::op2]
#[serde]
async fn op_kv_get_value(
    state: Rc<RefCell<OpState>>,
    #[string] payload: String,
) -> KvGetValueResult {
    let started = Instant::now();
    let payload: KvGetValuePayload = match crate::json::from_string(payload) {
        Ok(value) => value,
        Err(error) => {
            return KvGetValueResult {
                ok: false,
                found: false,
                value: Vec::new(),
                encoding: "utf8".to_string(),
                error: format!("invalid kv get payload: {error}"),
            };
        }
    };
    let store = state.borrow().borrow::<KvStore>().clone();
    let result = match store
        .get(&payload.worker_name, &payload.binding, &payload.key)
        .await
    {
        Ok(Some(value)) => KvGetValueResult {
            ok: true,
            found: true,
            value: value.value,
            encoding: value.encoding,
            error: String::new(),
        },
        Ok(None) => KvGetValueResult {
            ok: true,
            found: false,
            value: Vec::new(),
            encoding: "utf8".to_string(),
            error: String::new(),
        },
        Err(error) => KvGetValueResult {
            ok: false,
            found: false,
            value: Vec::new(),
            encoding: "utf8".to_string(),
            error: error.to_string(),
        },
    };
    store.record_profile(
        KvProfileMetricKind::OpGetValue,
        started.elapsed().as_micros() as u64,
        1,
    );
    result
}

#[deno_core::op2(fast)]
fn op_kv_profile_record_js(
    state: &mut OpState,
    #[string] metric: String,
    duration_us: u32,
    items: u32,
) {
    let kind = match metric.as_str() {
        "js_request_total" => KvProfileMetricKind::JsRequestTotal,
        "js_batch_flush" => KvProfileMetricKind::JsBatchFlush,
        "kv_cache_hit" => KvProfileMetricKind::JsCacheHit,
        "kv_cache_miss" => KvProfileMetricKind::JsCacheMiss,
        "kv_cache_stale" => KvProfileMetricKind::JsCacheStale,
        "kv_cache_fill" => KvProfileMetricKind::JsCacheFill,
        "kv_cache_invalidate" => KvProfileMetricKind::JsCacheInvalidate,
        _ => return,
    };
    let store = state.borrow::<KvStore>().clone();
    store.record_profile(kind, u64::from(duration_us), u64::from(items.max(1)));
}

#[deno_core::op2]
#[serde]
fn op_kv_profile_take(state: &mut OpState) -> KvProfileResult {
    let store = state.borrow::<KvStore>().clone();
    KvProfileResult {
        ok: true,
        snapshot: Some(store.take_profile_snapshot_and_reset()),
        error: String::new(),
    }
}

#[deno_core::op2(fast)]
fn op_kv_profile_reset(state: &mut OpState) {
    let store = state.borrow::<KvStore>().clone();
    store.reset_profile();
}

#[deno_core::op2(fast)]
fn op_kv_take_failed_write_version(state: &mut OpState, #[bigint] version: i64) -> bool {
    let store = state.borrow::<KvStore>().clone();
    store.take_failed_write_version(version)
}

#[deno_core::op2]
#[serde]
async fn op_kv_put(
    state: Rc<RefCell<OpState>>,
    #[string] worker_name: String,
    #[string] binding: String,
    #[string] key: String,
    #[string] value: String,
) -> KvOpResult {
    let store = state.borrow().borrow::<KvStore>().clone();
    match store.put(&worker_name, &binding, &key, &value).await {
        Ok(()) => KvOpResult {
            ok: true,
            error: String::new(),
            version: None,
        },
        Err(error) => KvOpResult {
            ok: false,
            error: error.to_string(),
            version: None,
        },
    }
}

#[deno_core::op2]
#[serde]
async fn op_kv_put_value(state: Rc<RefCell<OpState>>, #[string] payload: String) -> KvOpResult {
    let payload: KvPutValuePayload = match crate::json::from_string(payload) {
        Ok(value) => value,
        Err(error) => {
            return KvOpResult {
                ok: false,
                error: format!("invalid kv put payload: {error}"),
                version: None,
            };
        }
    };
    let store = state.borrow().borrow::<KvStore>().clone();
    match store
        .put_value(
            &payload.worker_name,
            &payload.binding,
            &payload.key,
            &payload.value,
            &payload.encoding,
        )
        .await
    {
        Ok(()) => KvOpResult {
            ok: true,
            error: String::new(),
            version: None,
        },
        Err(error) => KvOpResult {
            ok: false,
            error: error.to_string(),
            version: None,
        },
    }
}

#[deno_core::op2]
#[serde]
async fn op_kv_delete(
    state: Rc<RefCell<OpState>>,
    #[string] worker_name: String,
    #[string] binding: String,
    #[string] key: String,
) -> KvOpResult {
    let store = state.borrow().borrow::<KvStore>().clone();
    match store.delete(&worker_name, &binding, &key).await {
        Ok(()) => KvOpResult {
            ok: true,
            error: String::new(),
            version: None,
        },
        Err(error) => KvOpResult {
            ok: false,
            error: error.to_string(),
            version: None,
        },
    }
}

#[deno_core::op2]
#[serde]
fn op_kv_enqueue_put(
    state: &mut OpState,
    #[string] worker_name: String,
    #[string] binding: String,
    #[string] key: String,
    #[string] value: String,
) -> KvOpResult {
    let store = state.borrow::<KvStore>().clone();
    match store.enqueue_batch_versions(
        &worker_name,
        &binding,
        &[KvBatchMutation {
            key,
            value: value.into_bytes(),
            encoding: "utf8".to_string(),
            deleted: false,
        }],
    ) {
        Ok(versions) => KvOpResult {
            ok: true,
            error: String::new(),
            version: versions.first().copied(),
        },
        Err(error) => KvOpResult {
            ok: false,
            error: error.to_string(),
            version: None,
        },
    }
}

#[deno_core::op2]
#[serde]
fn op_kv_enqueue_put_value(state: &mut OpState, #[string] payload: String) -> KvOpResult {
    let payload: KvPutValuePayload = match crate::json::from_string(payload) {
        Ok(value) => value,
        Err(error) => {
            return KvOpResult {
                ok: false,
                error: format!("invalid kv enqueue payload: {error}"),
                version: None,
            };
        }
    };
    let store = state.borrow::<KvStore>().clone();
    match store.enqueue_batch_versions(
        &payload.worker_name,
        &payload.binding,
        &[KvBatchMutation {
            key: payload.key,
            value: payload.value,
            encoding: payload.encoding,
            deleted: false,
        }],
    ) {
        Ok(versions) => KvOpResult {
            ok: true,
            error: String::new(),
            version: versions.first().copied(),
        },
        Err(error) => KvOpResult {
            ok: false,
            error: error.to_string(),
            version: None,
        },
    }
}

#[deno_core::op2]
#[serde]
fn op_kv_enqueue_delete(
    state: &mut OpState,
    #[string] worker_name: String,
    #[string] binding: String,
    #[string] key: String,
) -> KvOpResult {
    let store = state.borrow::<KvStore>().clone();
    match store.enqueue_batch_versions(
        &worker_name,
        &binding,
        &[KvBatchMutation {
            key,
            value: Vec::new(),
            encoding: "utf8".to_string(),
            deleted: true,
        }],
    ) {
        Ok(versions) => KvOpResult {
            ok: true,
            error: String::new(),
            version: versions.first().copied(),
        },
        Err(error) => KvOpResult {
            ok: false,
            error: error.to_string(),
            version: None,
        },
    }
}

#[deno_core::op2]
#[serde]
fn op_kv_apply_batch(state: &mut OpState, #[string] payload: String) -> KvOpResult {
    let payload: KvApplyBatchPayload = match crate::json::from_string(payload) {
        Ok(value) => value,
        Err(error) => {
            return KvOpResult {
                ok: false,
                error: format!("invalid kv apply batch payload: {error}"),
                version: None,
            };
        }
    };
    let store = state.borrow::<KvStore>().clone();
    let mutations = payload
        .mutations
        .into_iter()
        .map(|mutation| KvBatchMutation {
            key: mutation.key,
            value: mutation.value,
            encoding: mutation.encoding,
            deleted: mutation.deleted,
        })
        .collect::<Vec<_>>();
    match store.apply_batch(&payload.worker_name, &payload.binding, &mutations) {
        Ok(()) => KvOpResult {
            ok: true,
            error: String::new(),
            version: None,
        },
        Err(error) => KvOpResult {
            ok: false,
            error: error.to_string(),
            version: None,
        },
    }
}

#[deno_core::op2]
#[serde]
async fn op_kv_list(
    state: Rc<RefCell<OpState>>,
    #[string] worker_name: String,
    #[string] binding: String,
    #[string] prefix: String,
    limit: u32,
) -> KvListResult {
    let store = state.borrow().borrow::<KvStore>().clone();
    let clamped_limit = limit.clamp(1, 1000) as usize;
    match store
        .list(&worker_name, &binding, &prefix, clamped_limit)
        .await
    {
        Ok(values) => KvListResult {
            ok: true,
            entries: values.into_iter().map(to_list_item).collect(),
            error: String::new(),
        },
        Err(error) => KvListResult {
            ok: false,
            entries: Vec::new(),
            error: error.to_string(),
        },
    }
}

#[deno_core::op2]
#[serde]
async fn op_cache_match(
    state: Rc<RefCell<OpState>>,
    #[string] payload: String,
) -> CacheMatchResult {
    let request = match decode_cache_request_payload(payload) {
        Ok(request) => request,
        Err(error) => {
            return CacheMatchResult {
                ok: false,
                found: false,
                stale: false,
                should_revalidate: false,
                status: 0,
                headers: Vec::new(),
                body: Vec::new(),
                error: error.to_string(),
            };
        }
    };

    let store = state.borrow().borrow::<CacheStore>().clone();
    match store.get(&request).await {
        Ok(CacheLookup::Fresh(response)) => CacheMatchResult {
            ok: true,
            found: true,
            stale: false,
            should_revalidate: false,
            status: response.status,
            headers: response.headers,
            body: response.body,
            error: String::new(),
        },
        Ok(CacheLookup::StaleWhileRevalidate(response)) => CacheMatchResult {
            ok: true,
            found: true,
            stale: true,
            should_revalidate: true,
            status: response.status,
            headers: response.headers,
            body: response.body,
            error: String::new(),
        },
        Ok(CacheLookup::StaleIfError(response)) => CacheMatchResult {
            ok: true,
            found: true,
            stale: true,
            should_revalidate: false,
            status: response.status,
            headers: response.headers,
            body: response.body,
            error: String::new(),
        },
        Ok(CacheLookup::Miss) => CacheMatchResult {
            ok: true,
            found: false,
            stale: false,
            should_revalidate: false,
            status: 0,
            headers: Vec::new(),
            body: Vec::new(),
            error: String::new(),
        },
        Err(error) => CacheMatchResult {
            ok: false,
            found: false,
            stale: false,
            should_revalidate: false,
            status: 0,
            headers: Vec::new(),
            body: Vec::new(),
            error: error.to_string(),
        },
    }
}

#[deno_core::op2]
#[serde]
async fn op_cache_put(state: Rc<RefCell<OpState>>, #[string] payload: String) -> KvOpResult {
    let (request, response) = match decode_cache_put_payload(payload) {
        Ok(values) => values,
        Err(error) => {
            return KvOpResult {
                ok: false,
                error: error.to_string(),
                version: None,
            };
        }
    };
    let store = state.borrow().borrow::<CacheStore>().clone();
    match store.put(&request, response).await {
        Ok(_) => KvOpResult {
            ok: true,
            error: String::new(),
            version: None,
        },
        Err(error) => KvOpResult {
            ok: false,
            error: error.to_string(),
            version: None,
        },
    }
}

#[deno_core::op2]
#[serde]
async fn op_cache_delete(
    state: Rc<RefCell<OpState>>,
    #[string] payload: String,
) -> CacheDeleteResult {
    let request = match decode_cache_request_payload(payload) {
        Ok(request) => request,
        Err(error) => {
            return CacheDeleteResult {
                ok: false,
                deleted: false,
                error: error.to_string(),
            };
        }
    };

    let store = state.borrow().borrow::<CacheStore>().clone();
    match store.delete(&request).await {
        Ok(deleted) => CacheDeleteResult {
            ok: true,
            deleted,
            error: String::new(),
        },
        Err(error) => CacheDeleteResult {
            ok: false,
            deleted: false,
            error: error.to_string(),
        },
    }
}

fn prepare_http_fetch_request(
    state: &Rc<RefCell<OpState>>,
    payload: HttpFetchPayload,
) -> std::result::Result<
    (
        reqwest::Method,
        reqwest::Url,
        Vec<(String, String)>,
        Vec<u8>,
        Arc<AtomicBool>,
        Arc<Notify>,
    ),
    String,
> {
    let (replacements, egress_allow_hosts, canceled, canceled_notify) =
        http_fetch_context(state, &payload.request_id)?;
    if canceled.load(Ordering::SeqCst) {
        canceled_notify.notify_waiters();
        return Err("host fetch request canceled".to_string());
    }

    let method_raw = replace_placeholders_text(&payload.method, &replacements);
    let method = reqwest::Method::from_bytes(method_raw.trim().to_ascii_uppercase().as_bytes())
        .map_err(|error| format!("invalid host fetch method: {error}"))?;

    let url = replace_placeholders_text(&payload.url, &replacements);
    let parsed_url =
        reqwest::Url::parse(&url).map_err(|error| format!("invalid host fetch URL: {error}"))?;
    if !is_egress_url_allowed(&parsed_url, &egress_allow_hosts) {
        return Err(format!(
            "egress origin is not allowed: {}",
            parsed_url.origin().ascii_serialization()
        ));
    }

    let headers = payload
        .headers
        .into_iter()
        .filter_map(|(name, value)| {
            let normalized_name = replace_placeholders_text(&name, &replacements);
            let normalized_value = replace_placeholders_text(&value, &replacements);
            let trimmed = normalized_name.trim().to_string();
            if trimmed.eq_ignore_ascii_case("host")
                || trimmed.eq_ignore_ascii_case("content-length")
            {
                return None;
            }
            Some((trimmed, normalized_value))
        })
        .collect::<Vec<_>>();
    let body = replace_placeholders_in_body(payload.body, &replacements);

    Ok((method, parsed_url, headers, body, canceled, canceled_notify))
}

fn check_http_fetch_url(
    state: &Rc<RefCell<OpState>>,
    payload: HttpUrlCheckPayload,
) -> std::result::Result<String, String> {
    let (replacements, egress_allow_hosts, canceled, canceled_notify) =
        http_fetch_context(state, &payload.request_id)?;
    if canceled.load(Ordering::SeqCst) {
        canceled_notify.notify_waiters();
        return Err("host fetch request canceled".to_string());
    }
    let url = replace_placeholders_text(&payload.url, &replacements);
    let parsed_url =
        reqwest::Url::parse(&url).map_err(|error| format!("invalid host fetch URL: {error}"))?;
    if !is_egress_url_allowed(&parsed_url, &egress_allow_hosts) {
        return Err(format!(
            "egress origin is not allowed: {}",
            parsed_url.origin().ascii_serialization()
        ));
    }
    Ok(parsed_url.to_string())
}

fn http_fetch_context(
    state: &Rc<RefCell<OpState>>,
    request_id: &str,
) -> std::result::Result<
    (
        HashMap<String, String>,
        Vec<String>,
        Arc<AtomicBool>,
        Arc<Notify>,
    ),
    String,
> {
    let request_id = request_id.trim();
    if request_id.is_empty() {
        return Err("host fetch request_id must not be empty".to_string());
    }
    let context = {
        let state_ref = state.borrow();
        state_ref
            .borrow::<RequestSecretContexts>()
            .contexts
            .get(request_id)
            .map(|context| {
                (
                    context.replacements.clone(),
                    context.egress_allow_hosts.clone(),
                    context.canceled.clone(),
                    context.canceled_notify.clone(),
                )
            })
    };
    context.ok_or_else(|| "host fetch context is unavailable (request likely canceled)".to_string())
}

#[deno_core::op2]
#[serde]
async fn op_http_prepare(
    state: Rc<RefCell<OpState>>,
    #[string] payload: String,
) -> HttpPrepareResult {
    let payload: HttpFetchPayload = match crate::json::from_string(payload) {
        Ok(value) => value,
        Err(error) => {
            return HttpPrepareResult {
                ok: false,
                method: String::new(),
                url: String::new(),
                headers: Vec::new(),
                body: Vec::new(),
                error: format!("invalid host fetch payload: {error}"),
            };
        }
    };

    match prepare_http_fetch_request(&state, payload) {
        Ok((method, url, headers, body, _, _)) => HttpPrepareResult {
            ok: true,
            method: method.as_str().to_string(),
            url: url.to_string(),
            headers,
            body,
            error: String::new(),
        },
        Err(error) => HttpPrepareResult {
            ok: false,
            method: String::new(),
            url: String::new(),
            headers: Vec::new(),
            body: Vec::new(),
            error,
        },
    }
}

#[deno_core::op2]
#[serde]
fn op_http_check_url(state: Rc<RefCell<OpState>>, #[string] payload: String) -> HttpUrlCheckResult {
    let payload: HttpUrlCheckPayload = match crate::json::from_string(payload) {
        Ok(value) => value,
        Err(error) => {
            return HttpUrlCheckResult {
                ok: false,
                url: String::new(),
                error: format!("invalid host fetch URL check payload: {error}"),
            };
        }
    };

    match check_http_fetch_url(&state, payload) {
        Ok(url) => HttpUrlCheckResult {
            ok: true,
            url,
            error: String::new(),
        },
        Err(error) => HttpUrlCheckResult {
            ok: false,
            url: String::new(),
            error,
        },
    }
}

#[deno_core::op2]
#[serde]
fn op_dynamic_profile_take(state: &mut OpState) -> DynamicProfileResult {
    let profile = state.borrow::<DynamicProfile>().clone();
    DynamicProfileResult {
        ok: true,
        snapshot: Some(profile.snapshot()),
        error: String::new(),
    }
}

#[deno_core::op2(fast)]
fn op_dynamic_profile_reset(state: &mut OpState) {
    let profile = state.borrow::<DynamicProfile>().clone();
    profile.reset();
}

fn dynamic_worker_owner_for_request(
    state: &Rc<RefCell<OpState>>,
    request_id: &str,
    binding: &str,
) -> Result<(String, u64, u64)> {
    let request_id = request_id.trim();
    if request_id.is_empty() {
        return Err(PlatformError::bad_request(
            "dynamic worker request_id must not be empty",
        ));
    }
    let binding = binding.trim();
    if binding.is_empty() {
        return Err(PlatformError::bad_request(
            "dynamic worker binding must not be empty",
        ));
    }
    let (worker_name, generation, isolate_id) = {
        let op_state = state.borrow();
        let contexts = op_state.borrow::<RequestSecretContexts>();
        let context = contexts
            .contexts
            .get(request_id)
            .ok_or_else(|| PlatformError::runtime("dynamic worker request scope is unavailable"))?;
        if !context.dynamic_bindings.contains(binding) {
            return Err(PlatformError::runtime(format!(
                "dynamic worker binding is not allowed: {binding}"
            )));
        }
        (
            context.worker_name.clone(),
            context.generation,
            context.isolate_id,
        )
    };
    Ok((worker_name, generation, isolate_id))
}

fn dynamic_host_rpc_owner_for_request(
    state: &Rc<RefCell<OpState>>,
    request_id: &str,
    binding: &str,
) -> Result<(String, u64, u64)> {
    let request_id = request_id.trim();
    if request_id.is_empty() {
        return Err(PlatformError::bad_request(
            "dynamic host rpc request_id must not be empty",
        ));
    }
    let binding = binding.trim();
    if binding.is_empty() {
        return Err(PlatformError::bad_request(
            "dynamic host rpc binding must not be empty",
        ));
    }
    let (worker_name, generation, isolate_id) = {
        let op_state = state.borrow();
        let contexts = op_state.borrow::<RequestSecretContexts>();
        let context = contexts.contexts.get(request_id).ok_or_else(|| {
            PlatformError::runtime("dynamic host rpc request scope is unavailable")
        })?;
        if !context.dynamic_rpc_bindings.contains(binding) {
            return Err(PlatformError::runtime(format!(
                "dynamic host rpc binding is not allowed: {binding}"
            )));
        }
        (
            context.worker_name.clone(),
            context.generation,
            context.isolate_id,
        )
    };
    Ok((worker_name, generation, isolate_id))
}

fn actor_invoke_owner_for_request(
    state: &Rc<RefCell<OpState>>,
    request_id: &str,
) -> Result<(String, u64, u64)> {
    let request_id = request_id.trim();
    if request_id.is_empty() {
        return Err(PlatformError::bad_request(
            "memory invoke caller_request_id must not be empty",
        ));
    }
    let (worker_name, generation, isolate_id) = {
        let op_state = state.borrow();
        let contexts = op_state.borrow::<RequestSecretContexts>();
        let context = contexts
            .contexts
            .get(request_id)
            .ok_or_else(|| PlatformError::runtime("memory invoke request scope is unavailable"))?;
        (
            context.worker_name.clone(),
            context.generation,
            context.isolate_id,
        )
    };
    Ok((worker_name, generation, isolate_id))
}

fn request_owner_for_request(
    state: &Rc<RefCell<OpState>>,
    request_id: &str,
) -> Result<(String, u64, u64)> {
    let request_id = request_id.trim();
    if request_id.is_empty() {
        return Err(PlatformError::bad_request("request_id must not be empty"));
    }
    let (worker_name, generation, isolate_id) = {
        let op_state = state.borrow();
        let contexts = op_state.borrow::<RequestSecretContexts>();
        let context = contexts
            .contexts
            .get(request_id)
            .ok_or_else(|| PlatformError::runtime("request scope is unavailable"))?;
        (
            context.worker_name.clone(),
            context.generation,
            context.isolate_id,
        )
    };
    Ok((worker_name, generation, isolate_id))
}

#[derive(Deserialize)]
struct TestAsyncReplyStartPayload {
    request_id: String,
    #[serde(default)]
    delay_ms: u64,
    #[serde(default = "test_async_reply_start_ok_default")]
    ok: bool,
    #[serde(default)]
    value: String,
    #[serde(default)]
    error: String,
}

const fn test_async_reply_start_ok_default() -> bool {
    true
}

#[derive(Deserialize)]
struct TestNestedTargetedInvokeStartPayload {
    request_id: String,
    #[serde(default)]
    target_mode: String,
    target_id: String,
    method_name: String,
    #[serde(default)]
    args: Vec<u8>,
}

#[deno_core::op2(fast)]
fn op_dynamic_cancel_reply(state: &mut OpState, #[string] reply_id: String) {
    let pending = state.borrow::<DynamicPendingReplies>().clone();
    pending.cancel(reply_id.trim());
}

#[deno_core::op2]
fn op_dynamic_host_rpc_task_complete(
    state: &mut OpState,
    #[serde] payload: DynamicHostRpcTaskCompletePayload,
) {
    let sender = state.borrow::<IsolateEventSender>().clone();
    let _ = sender
        .0
        .send(IsolateEventPayload::DynamicHostRpcTaskComplete(payload));
    let profile = state.borrow::<DynamicProfile>().clone();
    profile.record_async_reply_completion();
    profile.record_local_host_rpc_callback();
}

#[deno_core::op2]
#[serde]
fn op_test_async_reply_start(
    state: Rc<RefCell<OpState>>,
    #[serde] payload: TestAsyncReplyStartPayload,
) -> DynamicPendingReplyStartResult {
    let request_id = payload.request_id.trim();
    if request_id.is_empty() {
        return DynamicPendingReplyStartResult {
            ok: false,
            reply_id: String::new(),
            error: "test async reply requires request_id".to_string(),
        };
    }
    let (worker_name, generation, isolate_id) = match request_owner_for_request(&state, request_id)
    {
        Ok(value) => value,
        Err(error) => {
            return DynamicPendingReplyStartResult {
                ok: false,
                reply_id: String::new(),
                error: error.to_string(),
            };
        }
    };
    let replies = state.borrow().borrow::<TestAsyncReplies>().clone();
    let reply_id = replies.allocate(TestAsyncReplyOwner {
        worker_name,
        generation,
        isolate_id,
    });
    let sender = state.borrow().borrow::<IsolateEventSender>().clone();
    if sender
        .0
        .send(IsolateEventPayload::TestAsyncReply(TestAsyncReplyEvent {
            reply_id: reply_id.clone(),
            replies,
            delay_ms: payload.delay_ms,
            ok: payload.ok,
            value: payload.value,
            error: payload.error,
        }))
        .is_err()
    {
        return DynamicPendingReplyStartResult {
            ok: false,
            reply_id: String::new(),
            error: "test async reply runtime is unavailable".to_string(),
        };
    }
    DynamicPendingReplyStartResult {
        ok: true,
        reply_id,
        error: String::new(),
    }
}

#[deno_core::op2(fast)]
fn op_test_async_reply_cancel(state: &mut OpState, #[string] reply_id: String) {
    let replies = state.borrow::<TestAsyncReplies>().clone();
    replies.cancel(reply_id.trim());
}

#[deno_core::op2]
#[serde]
fn op_test_nested_targeted_invoke_start(
    state: Rc<RefCell<OpState>>,
    #[serde] payload: TestNestedTargetedInvokeStartPayload,
) -> DynamicPendingReplyStartResult {
    let request_id = payload.request_id.trim();
    if request_id.is_empty() {
        return DynamicPendingReplyStartResult {
            ok: false,
            reply_id: String::new(),
            error: "test nested targeted invoke requires request_id".to_string(),
        };
    }
    let target_id = payload.target_id.trim();
    if target_id.is_empty() {
        return DynamicPendingReplyStartResult {
            ok: false,
            reply_id: String::new(),
            error: "test nested targeted invoke requires target_id".to_string(),
        };
    }
    let method_name = payload.method_name.trim();
    if method_name.is_empty() {
        return DynamicPendingReplyStartResult {
            ok: false,
            reply_id: String::new(),
            error: "test nested targeted invoke requires method_name".to_string(),
        };
    }
    let (worker_name, generation, isolate_id) = match request_owner_for_request(&state, request_id)
    {
        Ok(value) => value,
        Err(error) => {
            return DynamicPendingReplyStartResult {
                ok: false,
                reply_id: String::new(),
                error: error.to_string(),
            };
        }
    };
    let replies = state.borrow().borrow::<TestAsyncReplies>().clone();
    let reply_id = replies.allocate(TestAsyncReplyOwner {
        worker_name: worker_name.clone(),
        generation,
        isolate_id,
    });
    let sender = state.borrow().borrow::<IsolateEventSender>().clone();
    if sender
        .0
        .send(IsolateEventPayload::TestNestedTargetedInvoke(
            TestNestedTargetedInvokeEvent {
                worker_name,
                generation,
                caller_isolate_id: isolate_id,
                target_mode: payload.target_mode,
                target_id: target_id.to_string(),
                method_name: method_name.to_string(),
                args: payload.args,
                reply_id: reply_id.clone(),
                replies,
            },
        ))
        .is_err()
    {
        return DynamicPendingReplyStartResult {
            ok: false,
            reply_id: String::new(),
            error: "test nested targeted invoke runtime is unavailable".to_string(),
        };
    }
    DynamicPendingReplyStartResult {
        ok: true,
        reply_id,
        error: String::new(),
    }
}

#[deno_core::op2]
#[serde]
fn op_dynamic_worker_create(
    state: Rc<RefCell<OpState>>,
    #[serde] payload: DynamicWorkerCreatePayload,
) -> DynamicPendingReplyStartResult {
    let id = payload.id.trim();
    if id.is_empty() {
        return DynamicPendingReplyStartResult {
            ok: false,
            reply_id: String::new(),
            error: "dynamic worker id must not be empty".to_string(),
        };
    }
    if payload.source.trim().is_empty() {
        return DynamicPendingReplyStartResult {
            ok: false,
            reply_id: String::new(),
            error: "dynamic worker source must not be empty".to_string(),
        };
    }
    if payload.timeout == 0 {
        return DynamicPendingReplyStartResult {
            ok: false,
            reply_id: String::new(),
            error: "dynamic worker timeout must be greater than 0".to_string(),
        };
    }
    let (owner_worker, owner_generation, owner_isolate_id) =
        match dynamic_worker_owner_for_request(&state, &payload.request_id, &payload.binding) {
            Ok(value) => value,
            Err(error) => {
                return DynamicPendingReplyStartResult {
                    ok: false,
                    reply_id: String::new(),
                    error: error.to_string(),
                };
            }
        };
    let pending_replies = state.borrow().borrow::<DynamicPendingReplies>().clone();
    let reply_id = pending_replies.allocate(DynamicPendingReplyOwner {
        worker_name: owner_worker.clone(),
        generation: owner_generation,
        isolate_id: owner_isolate_id,
    });
    let sender = state.borrow().borrow::<IsolateEventSender>().clone();
    if sender
        .0
        .send(IsolateEventPayload::DynamicWorkerCreate(
            DynamicWorkerCreateEvent {
                owner_worker,
                owner_generation,
                owner_isolate_id,
                binding: payload.binding,
                id: id.to_string(),
                source: payload.source,
                env: payload.env,
                timeout: payload.timeout,
                host_rpc_bindings: payload.host_rpc_bindings,
                reply_id: reply_id.clone(),
                pending_replies,
            },
        ))
        .is_err()
    {
        return DynamicPendingReplyStartResult {
            ok: false,
            reply_id: String::new(),
            error: "dynamic worker runtime is unavailable".to_string(),
        };
    }
    DynamicPendingReplyStartResult {
        ok: true,
        reply_id,
        error: String::new(),
    }
}

#[deno_core::op2]
#[serde]
fn op_dynamic_worker_lookup(
    state: Rc<RefCell<OpState>>,
    #[serde] payload: DynamicWorkerLookupPayload,
) -> DynamicPendingReplyStartResult {
    let id = payload.id.trim();
    if id.is_empty() {
        return DynamicPendingReplyStartResult {
            ok: false,
            reply_id: String::new(),
            error: "dynamic worker id must not be empty".to_string(),
        };
    }
    let (owner_worker, owner_generation, owner_isolate_id) =
        match dynamic_worker_owner_for_request(&state, &payload.request_id, &payload.binding) {
            Ok(value) => value,
            Err(error) => {
                return DynamicPendingReplyStartResult {
                    ok: false,
                    reply_id: String::new(),
                    error: error.to_string(),
                };
            }
        };
    let pending_replies = state.borrow().borrow::<DynamicPendingReplies>().clone();
    let reply_id = pending_replies.allocate(DynamicPendingReplyOwner {
        worker_name: owner_worker.clone(),
        generation: owner_generation,
        isolate_id: owner_isolate_id,
    });
    let sender = state.borrow().borrow::<IsolateEventSender>().clone();
    if sender
        .0
        .send(IsolateEventPayload::DynamicWorkerLookup(
            DynamicWorkerLookupEvent {
                owner_worker,
                owner_generation,
                owner_isolate_id,
                binding: payload.binding,
                id: id.to_string(),
                reply_id: reply_id.clone(),
                pending_replies,
            },
        ))
        .is_err()
    {
        return DynamicPendingReplyStartResult {
            ok: false,
            reply_id: String::new(),
            error: "dynamic worker runtime is unavailable".to_string(),
        };
    }
    DynamicPendingReplyStartResult {
        ok: true,
        reply_id,
        error: String::new(),
    }
}

#[deno_core::op2]
#[serde]
fn op_dynamic_worker_list(
    state: Rc<RefCell<OpState>>,
    #[serde] payload: DynamicWorkerListPayload,
) -> DynamicPendingReplyStartResult {
    let (owner_worker, owner_generation, owner_isolate_id) =
        match dynamic_worker_owner_for_request(&state, &payload.request_id, &payload.binding) {
            Ok(value) => value,
            Err(error) => {
                return DynamicPendingReplyStartResult {
                    ok: false,
                    reply_id: String::new(),
                    error: error.to_string(),
                };
            }
        };
    let pending_replies = state.borrow().borrow::<DynamicPendingReplies>().clone();
    let reply_id = pending_replies.allocate(DynamicPendingReplyOwner {
        worker_name: owner_worker.clone(),
        generation: owner_generation,
        isolate_id: owner_isolate_id,
    });
    let sender = state.borrow().borrow::<IsolateEventSender>().clone();
    if sender
        .0
        .send(IsolateEventPayload::DynamicWorkerList(
            DynamicWorkerListEvent {
                owner_worker,
                owner_generation,
                owner_isolate_id,
                binding: payload.binding,
                reply_id: reply_id.clone(),
                pending_replies,
            },
        ))
        .is_err()
    {
        return DynamicPendingReplyStartResult {
            ok: false,
            reply_id: String::new(),
            error: "dynamic worker runtime is unavailable".to_string(),
        };
    }
    DynamicPendingReplyStartResult {
        ok: true,
        reply_id,
        error: String::new(),
    }
}

#[deno_core::op2]
#[serde]
fn op_dynamic_worker_delete(
    state: Rc<RefCell<OpState>>,
    #[serde] payload: DynamicWorkerDeletePayload,
) -> DynamicPendingReplyStartResult {
    let id = payload.id.trim();
    if id.is_empty() {
        return DynamicPendingReplyStartResult {
            ok: false,
            reply_id: String::new(),
            error: "dynamic worker id must not be empty".to_string(),
        };
    }
    let (owner_worker, owner_generation, owner_isolate_id) =
        match dynamic_worker_owner_for_request(&state, &payload.request_id, &payload.binding) {
            Ok(value) => value,
            Err(error) => {
                return DynamicPendingReplyStartResult {
                    ok: false,
                    reply_id: String::new(),
                    error: error.to_string(),
                };
            }
        };
    let pending_replies = state.borrow().borrow::<DynamicPendingReplies>().clone();
    let reply_id = pending_replies.allocate(DynamicPendingReplyOwner {
        worker_name: owner_worker.clone(),
        generation: owner_generation,
        isolate_id: owner_isolate_id,
    });
    let sender = state.borrow().borrow::<IsolateEventSender>().clone();
    if sender
        .0
        .send(IsolateEventPayload::DynamicWorkerDelete(
            DynamicWorkerDeleteEvent {
                owner_worker,
                owner_generation,
                owner_isolate_id,
                binding: payload.binding,
                id: id.to_string(),
                reply_id: reply_id.clone(),
                pending_replies,
            },
        ))
        .is_err()
    {
        return DynamicPendingReplyStartResult {
            ok: false,
            reply_id: String::new(),
            error: "dynamic worker runtime is unavailable".to_string(),
        };
    }
    DynamicPendingReplyStartResult {
        ok: true,
        reply_id,
        error: String::new(),
    }
}

#[deno_core::op2]
#[serde]
fn op_dynamic_worker_invoke(
    state: Rc<RefCell<OpState>>,
    #[serde] payload: DynamicWorkerInvokePayload,
) -> DynamicPendingReplyStartResult {
    if payload.subrequest_id.trim().is_empty() {
        return DynamicPendingReplyStartResult {
            ok: false,
            reply_id: String::new(),
            error: "dynamic worker subrequest_id must not be empty".to_string(),
        };
    }
    if payload.handle.trim().is_empty() {
        return DynamicPendingReplyStartResult {
            ok: false,
            reply_id: String::new(),
            error: "dynamic worker handle must not be empty".to_string(),
        };
    }
    let (owner_worker, owner_generation, owner_isolate_id) =
        match dynamic_worker_owner_for_request(&state, &payload.request_id, &payload.binding) {
            Ok(value) => value,
            Err(error) => {
                return DynamicPendingReplyStartResult {
                    ok: false,
                    reply_id: String::new(),
                    error: error.to_string(),
                };
            }
        };

    let request = WorkerInvocation {
        method: payload.method,
        url: payload.url,
        headers: payload.headers,
        body: payload.body,
        request_id: payload.subrequest_id,
    };

    let pending_replies = state.borrow().borrow::<DynamicPendingReplies>().clone();
    let reply_id = pending_replies.allocate(DynamicPendingReplyOwner {
        worker_name: owner_worker.clone(),
        generation: owner_generation,
        isolate_id: owner_isolate_id,
    });
    let sender = state.borrow().borrow::<IsolateEventSender>().clone();
    if sender
        .0
        .send(IsolateEventPayload::DynamicWorkerInvoke(
            DynamicWorkerInvokeEvent {
                owner_worker,
                owner_generation,
                owner_isolate_id,
                binding: payload.binding,
                handle: payload.handle,
                request,
                reply_id: reply_id.clone(),
                pending_replies,
            },
        ))
        .is_err()
    {
        return DynamicPendingReplyStartResult {
            ok: false,
            reply_id: String::new(),
            error: "dynamic worker runtime is unavailable".to_string(),
        };
    }
    DynamicPendingReplyStartResult {
        ok: true,
        reply_id,
        error: String::new(),
    }
}

#[deno_core::op2]
#[serde]
fn op_dynamic_host_rpc_invoke(
    state: Rc<RefCell<OpState>>,
    #[serde] payload: DynamicHostRpcInvokePayload,
) -> DynamicPendingReplyStartResult {
    let method_name = payload.method_name.trim();
    if method_name.is_empty() {
        return DynamicPendingReplyStartResult {
            ok: false,
            reply_id: String::new(),
            error: "dynamic host rpc method_name must not be empty".to_string(),
        };
    }
    let (caller_worker, caller_generation, caller_isolate_id) =
        match dynamic_host_rpc_owner_for_request(&state, &payload.request_id, &payload.binding) {
            Ok(value) => value,
            Err(error) => {
                return DynamicPendingReplyStartResult {
                    ok: false,
                    reply_id: String::new(),
                    error: error.to_string(),
                };
            }
        };
    let pending_replies = state.borrow().borrow::<DynamicPendingReplies>().clone();
    let reply_id = pending_replies.allocate(DynamicPendingReplyOwner {
        worker_name: caller_worker.clone(),
        generation: caller_generation,
        isolate_id: caller_isolate_id,
    });
    let sender = state.borrow().borrow::<IsolateEventSender>().clone();
    if sender
        .0
        .send(IsolateEventPayload::DynamicHostRpcInvoke(
            DynamicHostRpcInvokeEvent {
                caller_worker,
                caller_generation,
                _caller_isolate_id: caller_isolate_id,
                binding: payload.binding,
                method_name: method_name.to_string(),
                args: payload.args,
                reply_id: reply_id.clone(),
                pending_replies,
            },
        ))
        .is_err()
    {
        return DynamicPendingReplyStartResult {
            ok: false,
            reply_id: String::new(),
            error: "dynamic host rpc runtime is unavailable".to_string(),
        };
    }
    DynamicPendingReplyStartResult {
        ok: true,
        reply_id,
        error: String::new(),
    }
}

#[deno_core::op2]
#[serde]
async fn op_request_body_read(
    state: Rc<RefCell<OpState>>,
    #[string] request_id: String,
) -> RequestBodyReadResult {
    let stream = {
        let state_ref = state.borrow();
        state_ref
            .borrow::<RequestBodyStreams>()
            .streams
            .get(&request_id)
            .cloned()
    };
    let Some(stream) = stream else {
        return RequestBodyReadResult {
            ok: true,
            done: true,
            chunk: Vec::new(),
            error: String::new(),
        };
    };

    if stream.is_canceled() {
        return RequestBodyReadResult {
            ok: false,
            done: true,
            chunk: Vec::new(),
            error: "request body stream canceled".to_string(),
        };
    }

    let canceled = stream.canceled_notify.notified();
    tokio::pin!(canceled);
    let mut receiver = stream.receiver.lock().await;
    tokio::select! {
        chunk = receiver.recv() => {
            match chunk {
                Some(Ok(bytes)) => RequestBodyReadResult {
                    ok: true,
                    done: false,
                    chunk: bytes,
                    error: String::new(),
                },
                Some(Err(error)) => {
                    clear_request_body_stream_entry(&state, &request_id);
                    RequestBodyReadResult {
                        ok: false,
                        done: true,
                        chunk: Vec::new(),
                        error,
                    }
                }
                None => {
                    clear_request_body_stream_entry(&state, &request_id);
                    RequestBodyReadResult {
                        ok: true,
                        done: true,
                        chunk: Vec::new(),
                        error: String::new(),
                    }
                }
            }
        }
        _ = &mut canceled => {
            clear_request_body_stream_entry(&state, &request_id);
            RequestBodyReadResult {
                ok: false,
                done: true,
                chunk: Vec::new(),
                error: "request body stream canceled".to_string(),
            }
        }
    }
}

#[deno_core::op2(fast)]
fn op_request_body_cancel(state: &mut OpState, #[string] request_id: String) {
    cancel_request_body_stream(state, &request_id);
}

#[deno_core::op2]
#[serde]
async fn op_actor_invoke_method(
    state: Rc<RefCell<OpState>>,
    #[serde] payload: ActorInvokeMethodPayload,
) -> ActorInvokeMethodResult {
    if payload.caller_request_id.trim().is_empty()
        || payload.worker_name.trim().is_empty()
        || payload.binding.trim().is_empty()
        || payload.key.trim().is_empty()
        || payload.method_name.trim().is_empty()
        || payload.request_id.trim().is_empty()
    {
        return ActorInvokeMethodResult {
            ok: false,
            value: Vec::new(),
            error: "memory method invoke requires caller_request_id, worker_name, binding, key, method_name, request_id"
                .to_string(),
        };
    }
    let (caller_worker_name, caller_generation, caller_isolate_id) =
        match actor_invoke_owner_for_request(&state, &payload.caller_request_id) {
            Ok(value) => value,
            Err(error) => {
                return ActorInvokeMethodResult {
                    ok: false,
                    value: Vec::new(),
                    error: error.to_string(),
                };
            }
        };
    let request_frame = match encode_actor_invoke_request(&ActorInvokeRequest {
        worker_name: payload.worker_name,
        binding: payload.binding,
        key: payload.key,
        call: ActorInvokeCall::Method {
            name: payload.method_name,
            args: payload.args,
            request_id: payload.request_id,
        },
    }) {
        Ok(frame) => frame,
        Err(error) => {
            return ActorInvokeMethodResult {
                ok: false,
                value: Vec::new(),
                error: format!("memory method invoke encode failed: {error}"),
            };
        }
    };

    let (reply_tx, reply_rx) = oneshot::channel();
    let sender = state.borrow().borrow::<IsolateEventSender>().clone();
    if sender
        .0
        .send(IsolateEventPayload::ActorInvoke(ActorInvokeEvent {
            request_frame,
            caller_worker_name,
            caller_generation,
            caller_isolate_id,
            prefer_caller_isolate: payload.prefer_caller_isolate,
            reply: reply_tx,
        }))
        .is_err()
    {
        return ActorInvokeMethodResult {
            ok: false,
            value: Vec::new(),
            error: "memory method runtime is unavailable".to_string(),
        };
    }

    match reply_rx.await {
        Ok(Ok(frame)) => match decode_actor_invoke_response(&frame) {
            Ok(ActorInvokeResponse::Method { value }) => ActorInvokeMethodResult {
                ok: true,
                value,
                error: String::new(),
            },
            Ok(ActorInvokeResponse::Error(error)) => ActorInvokeMethodResult {
                ok: false,
                value: Vec::new(),
                error,
            },
            Ok(ActorInvokeResponse::Fetch(_)) => ActorInvokeMethodResult {
                ok: false,
                value: Vec::new(),
                error: "memory method invoke received fetch response".to_string(),
            },
            Err(error) => ActorInvokeMethodResult {
                ok: false,
                value: Vec::new(),
                error: format!("memory method invoke decode failed: {error}"),
            },
        },
        Ok(Err(error)) => ActorInvokeMethodResult {
            ok: false,
            value: Vec::new(),
            error: error.to_string(),
        },
        Err(_) => ActorInvokeMethodResult {
            ok: false,
            value: Vec::new(),
            error: "memory method invoke response channel closed".to_string(),
        },
    }
}

fn actor_scope_for_request(
    state: &Rc<RefCell<OpState>>,
    request_id: &str,
) -> Result<(String, String)> {
    state
        .borrow()
        .borrow::<ActorRequestScopes>()
        .scopes
        .get(request_id)
        .cloned()
        .map(|scope| (scope.namespace, scope.actor_key))
        .ok_or_else(|| PlatformError::runtime("memory storage scope is unavailable"))
}

fn actor_socket_scope_for_payload(
    state: &Rc<RefCell<OpState>>,
    request_id: &str,
    binding: &str,
    key: &str,
) -> Result<(String, String)> {
    let binding = binding.trim();
    let key = key.trim();
    if !binding.is_empty() && !key.is_empty() {
        return Ok((binding.to_string(), key.to_string()));
    }
    actor_scope_for_request(state, request_id)
}

fn actor_storage_scope_for_payload(
    state: &Rc<RefCell<OpState>>,
    request_id: &str,
    binding: &str,
    key: &str,
) -> Result<(String, String)> {
    let binding = binding.trim();
    let key = key.trim();
    if !binding.is_empty() && !key.is_empty() {
        return Ok((binding.to_string(), key.to_string()));
    }
    actor_scope_for_request(state, request_id)
}

#[deno_core::op2]
#[serde]
async fn op_actor_state_get(
    state: Rc<RefCell<OpState>>,
    #[serde] payload: ActorStateGetPayload,
) -> ActorStateGetResult {
    let started = Instant::now();
    let (namespace, actor_key) = match actor_storage_scope_for_payload(
        &state,
        &payload.request_id,
        &payload.binding,
        &payload.key,
    ) {
        Ok(scope) => scope,
        Err(error) => {
            return ActorStateGetResult {
                ok: false,
                record: None,
                max_version: -1,
                error: error.to_string(),
            };
        }
    };
    let store = state.borrow().borrow::<ActorStore>().clone();
    let store_for_read = store.clone();
    let item_key = payload.item_key;
    let read_result = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| PlatformError::internal(error.to_string()))?;
        runtime.block_on(store_for_read.point_read(&namespace, &actor_key, &item_key))
    })
    .join()
    .unwrap_or_else(|_| Err(PlatformError::internal("actor point read worker panicked")));
    match read_result {
        Ok(point) => {
            store.record_profile(
                ActorProfileMetricKind::OpRead,
                started.elapsed().as_micros() as u64,
                1,
            );
            ActorStateGetResult {
                ok: true,
                record: point.record.map(|entry| ActorStateGetEntry {
                    key: entry.key,
                    value: entry.value,
                    encoding: entry.encoding,
                    version: entry.version,
                    deleted: entry.deleted,
                }),
                max_version: point.max_version,
                error: String::new(),
            }
        }
        Err(error) => ActorStateGetResult {
            ok: false,
            record: None,
            max_version: -1,
            error: error.to_string(),
        },
    }
}

#[deno_core::op2]
#[serde]
async fn op_actor_state_snapshot(
    state: Rc<RefCell<OpState>>,
    #[serde] payload: ActorStateSnapshotPayload,
) -> ActorStateSnapshotResult {
    let started = Instant::now();
    let (namespace, actor_key) = match actor_storage_scope_for_payload(
        &state,
        &payload.request_id,
        &payload.binding,
        &payload.key,
    ) {
        Ok(scope) => scope,
        Err(error) => {
            return ActorStateSnapshotResult {
                ok: false,
                entries: Vec::new(),
                max_version: -1,
                error: error.to_string(),
            };
        }
    };
    let store = state.borrow().borrow::<ActorStore>().clone();
    let keys = payload
        .keys
        .into_iter()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    let profile_items = keys.len().max(1) as u64;
    let store_for_snapshot = store.clone();
    let snapshot_result = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| PlatformError::internal(error.to_string()))?;
        runtime.block_on(async move {
            if keys.is_empty() {
                store_for_snapshot.snapshot(&namespace, &actor_key).await
            } else {
                store_for_snapshot
                    .snapshot_keys(&namespace, &actor_key, &keys)
                    .await
            }
        })
    })
    .join()
    .unwrap_or_else(|_| Err(PlatformError::internal("actor snapshot worker panicked")));
    match snapshot_result {
        Ok(snapshot) => {
            store.record_profile(
                ActorProfileMetricKind::OpSnapshot,
                started.elapsed().as_micros() as u64,
                profile_items,
            );
            ActorStateSnapshotResult {
                ok: true,
                entries: snapshot
                    .entries
                    .into_iter()
                    .map(|entry| ActorStateSnapshotEntry {
                        key: entry.key,
                        value: entry.value,
                        encoding: entry.encoding,
                        version: entry.version,
                        deleted: entry.deleted,
                    })
                    .collect(),
                max_version: snapshot.max_version,
                error: String::new(),
            }
        }
        Err(error) => ActorStateSnapshotResult {
            ok: false,
            entries: Vec::new(),
            max_version: -1,
            error: error.to_string(),
        },
    }
}

#[deno_core::op2]
#[serde]
async fn op_actor_state_version_if_newer(
    state: Rc<RefCell<OpState>>,
    #[serde] payload: ActorStateVersionIfNewerPayload,
) -> ActorStateVersionIfNewerResult {
    let started = Instant::now();
    let (namespace, actor_key) = match actor_storage_scope_for_payload(
        &state,
        &payload.request_id,
        &payload.binding,
        &payload.key,
    ) {
        Ok(scope) => scope,
        Err(error) => {
            return ActorStateVersionIfNewerResult {
                ok: false,
                stale: false,
                max_version: -1,
                error: error.to_string(),
            };
        }
    };
    let store = state.borrow().borrow::<ActorStore>().clone();
    match store
        .version_if_newer(&namespace, &actor_key, payload.known_version)
        .await
    {
        Ok(Some(max_version)) => {
            store.record_profile(
                ActorProfileMetricKind::OpVersionIfNewer,
                started.elapsed().as_micros() as u64,
                1,
            );
            ActorStateVersionIfNewerResult {
                ok: true,
                stale: true,
                max_version,
                error: String::new(),
            }
        }
        Ok(None) => {
            store.record_profile(
                ActorProfileMetricKind::OpVersionIfNewer,
                started.elapsed().as_micros() as u64,
                1,
            );
            ActorStateVersionIfNewerResult {
                ok: true,
                stale: false,
                max_version: payload.known_version,
                error: String::new(),
            }
        }
        Err(error) => ActorStateVersionIfNewerResult {
            ok: false,
            stale: false,
            max_version: -1,
            error: error.to_string(),
        },
    }
}

#[deno_core::op2]
#[serde]
async fn op_actor_state_validate_reads(
    state: Rc<RefCell<OpState>>,
    #[serde] payload: ActorStateValidateReadsPayload,
) -> ActorStateApplyBatchResult {
    let started = Instant::now();
    let (namespace, actor_key) = match actor_storage_scope_for_payload(
        &state,
        &payload.request_id,
        &payload.binding,
        &payload.key,
    ) {
        Ok(scope) => scope,
        Err(error) => {
            return ActorStateApplyBatchResult {
                ok: false,
                conflict: false,
                max_version: -1,
                error: error.to_string(),
            };
        }
    };
    let reads = payload
        .reads
        .into_iter()
        .map(|dependency| ActorReadDependency {
            key: dependency.key,
            version: dependency.version,
        })
        .collect::<Vec<_>>();
    let list_gate_version = if payload.list_gate_version < 0 {
        None
    } else {
        Some(payload.list_gate_version)
    };
    let store = state.borrow().borrow::<ActorStore>().clone();
    match store
        .validate_reads(&namespace, &actor_key, &reads, list_gate_version)
        .await
    {
        Ok(result) => {
            store.record_profile(
                ActorProfileMetricKind::OpValidateReads,
                started.elapsed().as_micros() as u64,
                reads.len().max(1) as u64,
            );
            ActorStateApplyBatchResult {
                ok: true,
                conflict: result.conflict,
                max_version: result.max_version,
                error: String::new(),
            }
        }
        Err(error) => ActorStateApplyBatchResult {
            ok: false,
            conflict: false,
            max_version: -1,
            error: error.to_string(),
        },
    }
}

#[deno_core::op2(fast)]
fn op_actor_profile_record_js(
    state: &mut OpState,
    #[string] metric: String,
    duration_us: u32,
    items: u32,
) {
    let kind = match metric.as_str() {
        "js_read_only_total" => ActorProfileMetricKind::JsReadOnlyTotal,
        "js_freshness_check" => ActorProfileMetricKind::JsFreshnessCheck,
        "js_hydrate_full" => ActorProfileMetricKind::JsHydrateFull,
        "js_hydrate_keys" => ActorProfileMetricKind::JsHydrateKeys,
        "js_txn_commit" => ActorProfileMetricKind::JsTxnCommit,
        "js_txn_blind_commit" => ActorProfileMetricKind::JsTxnBlindCommit,
        "js_txn_validate" => ActorProfileMetricKind::JsTxnValidate,
        "actor_cache_hit" => ActorProfileMetricKind::JsCacheHit,
        "actor_cache_miss" => ActorProfileMetricKind::JsCacheMiss,
        "actor_cache_stale" => ActorProfileMetricKind::JsCacheStale,
        _ => return,
    };
    let store = state.borrow::<ActorStore>().clone();
    store.record_profile(kind, u64::from(duration_us), u64::from(items.max(1)));
}

#[deno_core::op2]
#[serde]
fn op_actor_profile_take(state: &mut OpState) -> ActorProfileResult {
    let store = state.borrow::<ActorStore>().clone();
    ActorProfileResult {
        ok: true,
        snapshot: Some(store.take_profile_snapshot_and_reset()),
        error: String::new(),
    }
}

#[deno_core::op2(fast)]
fn op_actor_profile_reset(state: &mut OpState) {
    let store = state.borrow::<ActorStore>().clone();
    store.reset_profile();
}

#[deno_core::op2]
#[serde]
async fn op_actor_state_apply_batch(
    state: Rc<RefCell<OpState>>,
    #[serde] payload: ActorStateApplyBatchPayload,
) -> ActorStateApplyBatchResult {
    let started = std::time::Instant::now();
    let mutation_count = payload.mutations.len() as u64;
    let read_count = payload.reads.len() as u64;
    let (namespace, actor_key) = match actor_storage_scope_for_payload(
        &state,
        &payload.request_id,
        &payload.binding,
        &payload.key,
    ) {
        Ok(scope) => scope,
        Err(error) => {
            return ActorStateApplyBatchResult {
                ok: false,
                conflict: false,
                max_version: -1,
                error: error.to_string(),
            };
        }
    };
    let mutations = payload
        .mutations
        .into_iter()
        .map(|mutation| ActorBatchMutation {
            key: mutation.key,
            value: mutation.value,
            encoding: mutation.encoding,
            version: mutation.version,
            deleted: mutation.deleted,
        })
        .collect::<Vec<_>>();
    let reads = payload
        .reads
        .into_iter()
        .map(|dependency| ActorReadDependency {
            key: dependency.key,
            version: dependency.version,
        })
        .collect::<Vec<_>>();
    let expected_base_version = if payload.expected_base_version < 0 {
        Some(-1)
    } else {
        Some(payload.expected_base_version)
    };
    let list_gate_version = if payload.list_gate_version < 0 {
        None
    } else {
        Some(payload.list_gate_version)
    };
    let store = state.borrow().borrow::<ActorStore>().clone();
    let store_for_apply = store.clone();
    let apply_result = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| PlatformError::internal(error.to_string()))?;
        runtime.block_on(store_for_apply.apply_batch(
            &namespace,
            &actor_key,
            &reads,
            &mutations,
            expected_base_version,
            list_gate_version,
            payload.transactional,
        ))
    })
    .join()
    .unwrap_or_else(|_| Err(PlatformError::internal("actor apply worker panicked")));
    match apply_result {
        Ok(result) => {
            store.record_profile(
                ActorProfileMetricKind::OpApplyBatch,
                started.elapsed().as_micros() as u64,
                mutation_count + read_count + 1,
            );
            ActorStateApplyBatchResult {
                ok: true,
                conflict: result.conflict,
                max_version: result.max_version,
                error: String::new(),
            }
        }
        Err(error) => ActorStateApplyBatchResult {
            ok: false,
            conflict: false,
            max_version: -1,
            error: error.to_string(),
        },
    }
}

#[deno_core::op2]
#[serde]
async fn op_actor_state_apply_blind_batch(
    state: Rc<RefCell<OpState>>,
    #[serde] payload: ActorStateApplyBlindBatchPayload,
) -> ActorStateApplyBatchResult {
    let started = std::time::Instant::now();
    let mutation_count = payload.mutations.len() as u64;
    let (namespace, actor_key) = match actor_storage_scope_for_payload(
        &state,
        &payload.request_id,
        &payload.binding,
        &payload.key,
    ) {
        Ok(scope) => scope,
        Err(error) => {
            return ActorStateApplyBatchResult {
                ok: false,
                conflict: false,
                max_version: -1,
                error: error.to_string(),
            };
        }
    };
    let mutations = payload
        .mutations
        .into_iter()
        .map(|mutation| ActorBatchMutation {
            key: mutation.key,
            value: mutation.value,
            encoding: mutation.encoding,
            version: mutation.version,
            deleted: mutation.deleted,
        })
        .collect::<Vec<_>>();
    let store = state.borrow().borrow::<ActorStore>().clone();
    let store_for_apply = store.clone();
    let apply_result = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| PlatformError::internal(error.to_string()))?;
        runtime.block_on(store_for_apply.apply_blind_batch(&namespace, &actor_key, &mutations))
    })
    .join()
    .unwrap_or_else(|_| Err(PlatformError::internal("actor blind apply worker panicked")));
    match apply_result {
        Ok(result) => {
            store.record_profile(
                ActorProfileMetricKind::OpApplyBlindBatch,
                started.elapsed().as_micros() as u64,
                mutation_count + 1,
            );
            ActorStateApplyBatchResult {
                ok: true,
                conflict: result.conflict,
                max_version: result.max_version,
                error: String::new(),
            }
        }
        Err(error) => ActorStateApplyBatchResult {
            ok: false,
            conflict: false,
            max_version: -1,
            error: error.to_string(),
        },
    }
}

#[deno_core::op2]
#[serde]
async fn op_actor_state_enqueue_batch(
    state: Rc<RefCell<OpState>>,
    #[serde] payload: ActorStateEnqueueBatchPayload,
) -> ActorStateEnqueueBatchResult {
    let (namespace, actor_key) = match actor_storage_scope_for_payload(
        &state,
        &payload.request_id,
        &payload.binding,
        &payload.key,
    ) {
        Ok(scope) => scope,
        Err(error) => {
            return ActorStateEnqueueBatchResult {
                ok: false,
                submission_id: 0,
                error: error.to_string(),
            };
        }
    };
    let mutations = payload
        .mutations
        .into_iter()
        .map(|mutation| ActorDirectMutation {
            key: mutation.key,
            value: mutation.value,
            encoding: mutation.encoding,
            deleted: mutation.deleted,
        })
        .collect::<Vec<_>>();
    let store = state.borrow().borrow::<ActorStore>().clone();
    let store_for_enqueue = store.clone();
    let enqueue_result = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| PlatformError::internal(error.to_string()))?;
        runtime.block_on(store_for_enqueue.enqueue_direct_batch(&namespace, &actor_key, &mutations))
    })
    .join()
    .unwrap_or_else(|_| {
        Err(PlatformError::internal(
            "actor direct enqueue worker panicked",
        ))
    });
    match enqueue_result {
        Ok(submission_id) => ActorStateEnqueueBatchResult {
            ok: true,
            submission_id,
            error: String::new(),
        },
        Err(error) => ActorStateEnqueueBatchResult {
            ok: false,
            submission_id: 0,
            error: error.to_string(),
        },
    }
}

#[deno_core::op2]
#[serde]
async fn op_actor_state_await_submission(
    state: Rc<RefCell<OpState>>,
    #[serde] payload: ActorStateAwaitSubmissionPayload,
) -> ActorStateAwaitSubmissionResult {
    let store = state.borrow().borrow::<ActorStore>().clone();
    let started = Instant::now();
    loop {
        match store.try_poll_direct_submission(payload.submission_id) {
            Ok(Some(max_version)) => {
                store.record_profile(
                    ActorProfileMetricKind::StoreDirectAwait,
                    started.elapsed().as_micros() as u64,
                    1,
                );
                return ActorStateAwaitSubmissionResult {
                    ok: true,
                    pending: false,
                    max_version,
                    error: String::new(),
                };
            }
            Ok(None) if started.elapsed() < Duration::from_secs(30) => {
                std::thread::sleep(Duration::from_millis(1));
            }
            Ok(None) => {
                return ActorStateAwaitSubmissionResult {
                    ok: false,
                    pending: false,
                    max_version: -1,
                    error: format!(
                        "memory direct write submission {} timed out",
                        payload.submission_id
                    ),
                };
            }
            Err(error) => {
                return ActorStateAwaitSubmissionResult {
                    ok: false,
                    pending: false,
                    max_version: -1,
                    error: error.to_string(),
                };
            }
        }
    }
}

#[deno_core::op2]
#[serde]
async fn op_actor_socket_send(
    state: Rc<RefCell<OpState>>,
    #[serde] payload: ActorSocketSendPayload,
) -> ActorSocketSendResult {
    if payload.request_id.trim().is_empty() {
        return ActorSocketSendResult {
            ok: false,
            error: "memory socket send requires request_id".to_string(),
        };
    }
    if payload.handle.trim().is_empty() {
        return ActorSocketSendResult {
            ok: false,
            error: "memory socket send requires handle".to_string(),
        };
    }
    let normalized_kind = payload.message_kind.as_str();
    let is_text = match normalized_kind {
        "text" => true,
        "binary" => false,
        _ => {
            return ActorSocketSendResult {
                ok: false,
                error: format!("unsupported message kind: {normalized_kind}"),
            };
        }
    };

    let (binding, key) = match actor_socket_scope_for_payload(
        &state,
        &payload.request_id,
        &payload.binding,
        &payload.key,
    ) {
        Ok(value) => value,
        Err(error) => {
            return ActorSocketSendResult {
                ok: false,
                error: error.to_string(),
            }
        }
    };

    let (reply_tx, reply_rx) = oneshot::channel();
    let sender = state.borrow().borrow::<IsolateEventSender>().clone();
    if sender
        .0
        .send(IsolateEventPayload::ActorSocketSend(ActorSocketSendEvent {
            reply: reply_tx,
            handle: payload.handle,
            binding,
            key,
            is_text,
            message: payload.message,
        }))
        .is_err()
    {
        return ActorSocketSendResult {
            ok: false,
            error: "memory socket send runtime is unavailable".to_string(),
        };
    }

    drop(reply_rx);
    ActorSocketSendResult {
        ok: true,
        error: String::new(),
    }
}

#[deno_core::op2]
#[serde]
async fn op_actor_socket_close(
    state: Rc<RefCell<OpState>>,
    #[serde] payload: ActorSocketClosePayload,
) -> ActorSocketCloseResult {
    if payload.request_id.trim().is_empty() {
        return ActorSocketCloseResult {
            ok: false,
            error: "memory socket close requires request_id".to_string(),
        };
    }
    if payload.handle.trim().is_empty() {
        return ActorSocketCloseResult {
            ok: false,
            error: "memory socket close requires handle".to_string(),
        };
    }

    let (binding, key) = match actor_socket_scope_for_payload(
        &state,
        &payload.request_id,
        &payload.binding,
        &payload.key,
    ) {
        Ok(value) => value,
        Err(error) => {
            return ActorSocketCloseResult {
                ok: false,
                error: error.to_string(),
            };
        }
    };

    let (reply_tx, reply_rx) = oneshot::channel();
    let sender = state.borrow().borrow::<IsolateEventSender>().clone();
    if sender
        .0
        .send(IsolateEventPayload::ActorSocketClose(
            ActorSocketCloseEvent {
                reply: reply_tx,
                handle: payload.handle,
                binding,
                key,
                code: payload.code,
                reason: payload.reason,
            },
        ))
        .is_err()
    {
        return ActorSocketCloseResult {
            ok: false,
            error: "memory socket close runtime is unavailable".to_string(),
        };
    }

    drop(reply_rx);
    ActorSocketCloseResult {
        ok: true,
        error: String::new(),
    }
}

#[deno_core::op2]
#[serde]
async fn op_actor_socket_list(
    state: Rc<RefCell<OpState>>,
    #[serde] payload: ActorSocketListPayload,
) -> ActorSocketListResult {
    if payload.request_id.trim().is_empty() {
        return ActorSocketListResult {
            ok: false,
            handles: Vec::new(),
            error: "memory socket list requires request_id".to_string(),
        };
    }

    let (binding, key) = match actor_socket_scope_for_payload(
        &state,
        &payload.request_id,
        &payload.binding,
        &payload.key,
    ) {
        Ok(value) => value,
        Err(error) => {
            return ActorSocketListResult {
                ok: false,
                handles: Vec::new(),
                error: error.to_string(),
            };
        }
    };

    let registry = state.borrow().borrow::<ActorOpenHandleRegistry>().clone();
    ActorSocketListResult {
        ok: true,
        handles: registry.list_socket_handles(&binding, &key),
        error: String::new(),
    }
}

#[deno_core::op2]
#[serde]
async fn op_actor_socket_consume_close(
    state: Rc<RefCell<OpState>>,
    #[serde] payload: ActorSocketConsumeClosePayload,
) -> ActorSocketConsumeCloseResult {
    if payload.request_id.trim().is_empty() {
        return ActorSocketConsumeCloseResult {
            ok: false,
            events: Vec::new(),
            error: "memory socket consumeClose requires request_id".to_string(),
        };
    }
    if payload.handle.trim().is_empty() {
        return ActorSocketConsumeCloseResult {
            ok: false,
            events: Vec::new(),
            error: "memory socket consumeClose requires handle".to_string(),
        };
    }

    let (binding, key) = match actor_socket_scope_for_payload(
        &state,
        &payload.request_id,
        &payload.binding,
        &payload.key,
    ) {
        Ok(value) => value,
        Err(error) => {
            return ActorSocketConsumeCloseResult {
                ok: false,
                events: Vec::new(),
                error: error.to_string(),
            };
        }
    };

    let (reply_tx, reply_rx) = oneshot::channel();
    let sender = state.borrow().borrow::<IsolateEventSender>().clone();
    if sender
        .0
        .send(IsolateEventPayload::ActorSocketConsumeClose(
            ActorSocketConsumeCloseEvent {
                reply: reply_tx,
                binding,
                key,
                handle: payload.handle,
            },
        ))
        .is_err()
    {
        return ActorSocketConsumeCloseResult {
            ok: false,
            events: Vec::new(),
            error: "memory socket consumeClose runtime is unavailable".to_string(),
        };
    }

    match reply_rx.await {
        Ok(Ok(events)) => ActorSocketConsumeCloseResult {
            ok: true,
            events: events
                .into_iter()
                .map(|event| ActorSocketReplayClose {
                    code: event.code,
                    reason: event.reason,
                })
                .collect(),
            error: String::new(),
        },
        Ok(Err(error)) => ActorSocketConsumeCloseResult {
            ok: false,
            events: Vec::new(),
            error: error.to_string(),
        },
        Err(_) => ActorSocketConsumeCloseResult {
            ok: false,
            events: Vec::new(),
            error: "memory socket consumeClose response channel closed".to_string(),
        },
    }
}

fn actor_transport_scope_for_payload(
    state: &Rc<RefCell<OpState>>,
    request_id: &str,
    binding: &str,
    key: &str,
) -> std::result::Result<(String, String), PlatformError> {
    let binding = binding.trim();
    let key = key.trim();
    if !binding.is_empty() && !key.is_empty() {
        return Ok((binding.to_string(), key.to_string()));
    }
    actor_scope_for_request(state, request_id)
}

#[deno_core::op2]
#[serde]
fn op_actor_scope_enter(
    state: &mut OpState,
    #[serde] payload: ActorScopePayload,
) -> ActorScopeResult {
    if payload.request_id.trim().is_empty() {
        return ActorScopeResult {
            ok: false,
            error: "memory scope enter requires request_id".to_string(),
        };
    }
    if payload.binding.trim().is_empty() || payload.key.trim().is_empty() {
        return ActorScopeResult {
            ok: false,
            error: "memory scope enter requires binding and key".to_string(),
        };
    }
    register_actor_request_scope(state, payload.request_id, payload.binding, payload.key);
    ActorScopeResult {
        ok: true,
        error: String::new(),
    }
}

#[deno_core::op2]
#[serde]
fn op_actor_scope_exit(
    state: &mut OpState,
    #[serde] payload: ActorScopeClearPayload,
) -> ActorScopeResult {
    if payload.request_id.trim().is_empty() {
        return ActorScopeResult {
            ok: false,
            error: "memory scope exit requires request_id".to_string(),
        };
    }
    clear_actor_request_scope(state, &payload.request_id);
    ActorScopeResult {
        ok: true,
        error: String::new(),
    }
}

#[deno_core::op2]
#[serde]
async fn op_actor_transport_send_stream(
    state: Rc<RefCell<OpState>>,
    #[serde] payload: ActorTransportSendStreamPayload,
) -> ActorTransportSendResult {
    if payload.request_id.trim().is_empty() {
        return ActorTransportSendResult {
            ok: false,
            error: "memory transport sendStream requires request_id".to_string(),
        };
    }
    if payload.handle.trim().is_empty() {
        return ActorTransportSendResult {
            ok: false,
            error: "memory transport sendStream requires handle".to_string(),
        };
    }

    let (binding, key) = match actor_transport_scope_for_payload(
        &state,
        &payload.request_id,
        &payload.binding,
        &payload.key,
    ) {
        Ok(value) => value,
        Err(error) => {
            return ActorTransportSendResult {
                ok: false,
                error: error.to_string(),
            };
        }
    };

    let (reply_tx, reply_rx) = oneshot::channel();
    let sender = state.borrow().borrow::<IsolateEventSender>().clone();
    if sender
        .0
        .send(IsolateEventPayload::ActorTransportSendStream(
            ActorTransportSendStreamEvent {
                reply: reply_tx,
                handle: payload.handle,
                binding,
                key,
                chunk: payload.chunk,
            },
        ))
        .is_err()
    {
        return ActorTransportSendResult {
            ok: false,
            error: "memory transport sendStream runtime is unavailable".to_string(),
        };
    }

    match reply_rx.await {
        Ok(Ok(())) => ActorTransportSendResult {
            ok: true,
            error: String::new(),
        },
        Ok(Err(error)) => ActorTransportSendResult {
            ok: false,
            error: error.to_string(),
        },
        Err(_) => ActorTransportSendResult {
            ok: false,
            error: "memory transport sendStream response channel closed".to_string(),
        },
    }
}

#[deno_core::op2]
#[serde]
async fn op_actor_transport_send_datagram(
    state: Rc<RefCell<OpState>>,
    #[serde] payload: ActorTransportSendDatagramPayload,
) -> ActorTransportSendResult {
    if payload.request_id.trim().is_empty() {
        return ActorTransportSendResult {
            ok: false,
            error: "memory transport sendDatagram requires request_id".to_string(),
        };
    }
    if payload.handle.trim().is_empty() {
        return ActorTransportSendResult {
            ok: false,
            error: "memory transport sendDatagram requires handle".to_string(),
        };
    }

    let (binding, key) = match actor_transport_scope_for_payload(
        &state,
        &payload.request_id,
        &payload.binding,
        &payload.key,
    ) {
        Ok(value) => value,
        Err(error) => {
            return ActorTransportSendResult {
                ok: false,
                error: error.to_string(),
            };
        }
    };

    let (reply_tx, reply_rx) = oneshot::channel();
    let sender = state.borrow().borrow::<IsolateEventSender>().clone();
    if sender
        .0
        .send(IsolateEventPayload::ActorTransportSendDatagram(
            ActorTransportSendDatagramEvent {
                reply: reply_tx,
                handle: payload.handle,
                binding,
                key,
                datagram: payload.datagram,
            },
        ))
        .is_err()
    {
        return ActorTransportSendResult {
            ok: false,
            error: "memory transport sendDatagram runtime is unavailable".to_string(),
        };
    }

    match reply_rx.await {
        Ok(Ok(())) => ActorTransportSendResult {
            ok: true,
            error: String::new(),
        },
        Ok(Err(error)) => ActorTransportSendResult {
            ok: false,
            error: error.to_string(),
        },
        Err(_) => ActorTransportSendResult {
            ok: false,
            error: "memory transport sendDatagram response channel closed".to_string(),
        },
    }
}

#[deno_core::op2]
#[serde]
async fn op_actor_transport_recv_stream(
    state: Rc<RefCell<OpState>>,
    #[serde] payload: ActorTransportRecvPayload,
) -> ActorTransportRecvResult {
    if payload.request_id.trim().is_empty() {
        return ActorTransportRecvResult {
            ok: false,
            done: true,
            chunk: Vec::new(),
            error: "memory transport recvStream requires request_id".to_string(),
        };
    }
    if payload.handle.trim().is_empty() {
        return ActorTransportRecvResult {
            ok: false,
            done: true,
            chunk: Vec::new(),
            error: "memory transport recvStream requires handle".to_string(),
        };
    }

    let (binding, key) = match actor_transport_scope_for_payload(
        &state,
        &payload.request_id,
        &payload.binding,
        &payload.key,
    ) {
        Ok(value) => value,
        Err(error) => {
            return ActorTransportRecvResult {
                ok: false,
                done: true,
                chunk: Vec::new(),
                error: error.to_string(),
            };
        }
    };

    let (reply_tx, reply_rx) = oneshot::channel();
    let sender = state.borrow().borrow::<IsolateEventSender>().clone();
    if sender
        .0
        .send(IsolateEventPayload::ActorTransportRecvStream(
            ActorTransportRecvStreamEvent {
                reply: reply_tx,
                handle: payload.handle,
                binding,
                key,
            },
        ))
        .is_err()
    {
        return ActorTransportRecvResult {
            ok: false,
            done: true,
            chunk: Vec::new(),
            error: "memory transport recvStream runtime is unavailable".to_string(),
        };
    }

    match reply_rx.await {
        Ok(Ok(event)) => ActorTransportRecvResult {
            ok: true,
            done: event.done,
            chunk: event.chunk,
            error: String::new(),
        },
        Ok(Err(error)) => ActorTransportRecvResult {
            ok: false,
            done: true,
            chunk: Vec::new(),
            error: error.to_string(),
        },
        Err(_) => ActorTransportRecvResult {
            ok: false,
            done: true,
            chunk: Vec::new(),
            error: "memory transport recvStream response channel closed".to_string(),
        },
    }
}

#[deno_core::op2]
#[serde]
async fn op_actor_transport_recv_datagram(
    state: Rc<RefCell<OpState>>,
    #[serde] payload: ActorTransportRecvPayload,
) -> ActorTransportRecvResult {
    if payload.request_id.trim().is_empty() {
        return ActorTransportRecvResult {
            ok: false,
            done: true,
            chunk: Vec::new(),
            error: "memory transport recvDatagram requires request_id".to_string(),
        };
    }
    if payload.handle.trim().is_empty() {
        return ActorTransportRecvResult {
            ok: false,
            done: true,
            chunk: Vec::new(),
            error: "memory transport recvDatagram requires handle".to_string(),
        };
    }

    let (binding, key) = match actor_transport_scope_for_payload(
        &state,
        &payload.request_id,
        &payload.binding,
        &payload.key,
    ) {
        Ok(value) => value,
        Err(error) => {
            return ActorTransportRecvResult {
                ok: false,
                done: true,
                chunk: Vec::new(),
                error: error.to_string(),
            };
        }
    };

    let (reply_tx, reply_rx) = oneshot::channel();
    let sender = state.borrow().borrow::<IsolateEventSender>().clone();
    if sender
        .0
        .send(IsolateEventPayload::ActorTransportRecvDatagram(
            ActorTransportRecvDatagramEvent {
                reply: reply_tx,
                handle: payload.handle,
                binding,
                key,
            },
        ))
        .is_err()
    {
        return ActorTransportRecvResult {
            ok: false,
            done: true,
            chunk: Vec::new(),
            error: "memory transport recvDatagram runtime is unavailable".to_string(),
        };
    }

    match reply_rx.await {
        Ok(Ok(event)) => ActorTransportRecvResult {
            ok: true,
            done: event.done,
            chunk: event.chunk,
            error: String::new(),
        },
        Ok(Err(error)) => ActorTransportRecvResult {
            ok: false,
            done: true,
            chunk: Vec::new(),
            error: error.to_string(),
        },
        Err(_) => ActorTransportRecvResult {
            ok: false,
            done: true,
            chunk: Vec::new(),
            error: "memory transport recvDatagram response channel closed".to_string(),
        },
    }
}

#[deno_core::op2]
#[serde]
async fn op_actor_transport_close(
    state: Rc<RefCell<OpState>>,
    #[serde] payload: ActorTransportClosePayload,
) -> ActorTransportCloseResult {
    if payload.request_id.trim().is_empty() {
        return ActorTransportCloseResult {
            ok: false,
            error: "memory transport close requires request_id".to_string(),
        };
    }
    if payload.handle.trim().is_empty() {
        return ActorTransportCloseResult {
            ok: false,
            error: "memory transport close requires handle".to_string(),
        };
    }

    let (binding, key) = match actor_transport_scope_for_payload(
        &state,
        &payload.request_id,
        &payload.binding,
        &payload.key,
    ) {
        Ok(value) => value,
        Err(error) => {
            return ActorTransportCloseResult {
                ok: false,
                error: error.to_string(),
            };
        }
    };

    let (reply_tx, reply_rx) = oneshot::channel();
    let sender = state.borrow().borrow::<IsolateEventSender>().clone();
    if sender
        .0
        .send(IsolateEventPayload::ActorTransportClose(
            ActorTransportCloseEvent {
                reply: reply_tx,
                handle: payload.handle,
                binding,
                key,
                code: payload.code,
                reason: payload.reason,
            },
        ))
        .is_err()
    {
        return ActorTransportCloseResult {
            ok: false,
            error: "memory transport close runtime is unavailable".to_string(),
        };
    }

    match reply_rx.await {
        Ok(Ok(())) => ActorTransportCloseResult {
            ok: true,
            error: String::new(),
        },
        Ok(Err(error)) => ActorTransportCloseResult {
            ok: false,
            error: error.to_string(),
        },
        Err(_) => ActorTransportCloseResult {
            ok: false,
            error: "memory transport close response channel closed".to_string(),
        },
    }
}

#[deno_core::op2]
#[serde]
async fn op_actor_transport_list(
    state: Rc<RefCell<OpState>>,
    #[serde] payload: ActorTransportListPayload,
) -> ActorTransportListResult {
    if payload.request_id.trim().is_empty() {
        return ActorTransportListResult {
            ok: false,
            handles: Vec::new(),
            error: "memory transport list requires request_id".to_string(),
        };
    }

    let (binding, key) = match actor_transport_scope_for_payload(
        &state,
        &payload.request_id,
        &payload.binding,
        &payload.key,
    ) {
        Ok(value) => value,
        Err(error) => {
            return ActorTransportListResult {
                ok: false,
                handles: Vec::new(),
                error: error.to_string(),
            };
        }
    };

    let registry = state.borrow().borrow::<ActorOpenHandleRegistry>().clone();
    ActorTransportListResult {
        ok: true,
        handles: registry.list_transport_handles(&binding, &key),
        error: String::new(),
    }
}

#[deno_core::op2]
#[serde]
async fn op_actor_transport_consume_close(
    state: Rc<RefCell<OpState>>,
    #[serde] payload: ActorTransportConsumeClosePayload,
) -> ActorTransportConsumeCloseResult {
    if payload.request_id.trim().is_empty() {
        return ActorTransportConsumeCloseResult {
            ok: false,
            events: Vec::new(),
            error: "memory transport consumeClose requires request_id".to_string(),
        };
    }
    if payload.handle.trim().is_empty() {
        return ActorTransportConsumeCloseResult {
            ok: false,
            events: Vec::new(),
            error: "memory transport consumeClose requires handle".to_string(),
        };
    }

    let (binding, key) = match actor_transport_scope_for_payload(
        &state,
        &payload.request_id,
        &payload.binding,
        &payload.key,
    ) {
        Ok(value) => value,
        Err(error) => {
            return ActorTransportConsumeCloseResult {
                ok: false,
                events: Vec::new(),
                error: error.to_string(),
            };
        }
    };

    let (reply_tx, reply_rx) = oneshot::channel();
    let sender = state.borrow().borrow::<IsolateEventSender>().clone();
    if sender
        .0
        .send(IsolateEventPayload::ActorTransportConsumeClose(
            ActorTransportConsumeCloseEvent {
                reply: reply_tx,
                binding,
                key,
                handle: payload.handle,
            },
        ))
        .is_err()
    {
        return ActorTransportConsumeCloseResult {
            ok: false,
            events: Vec::new(),
            error: "memory transport consumeClose runtime is unavailable".to_string(),
        };
    }

    match reply_rx.await {
        Ok(Ok(events)) => ActorTransportConsumeCloseResult {
            ok: true,
            events: events
                .into_iter()
                .map(|event| ActorTransportReplayClose {
                    code: event.code,
                    reason: event.reason,
                })
                .collect(),
            error: String::new(),
        },
        Ok(Err(error)) => ActorTransportConsumeCloseResult {
            ok: false,
            events: Vec::new(),
            error: error.to_string(),
        },
        Err(_) => ActorTransportConsumeCloseResult {
            ok: false,
            events: Vec::new(),
            error: "memory transport consumeClose response channel closed".to_string(),
        },
    }
}

#[deno_core::op2(fast)]
fn op_emit_completion(state: &mut OpState, #[string] payload: String) {
    if let Some(meta) = completion_meta(&payload) {
        let request_id = meta.request_id;
        clear_request_body_stream(state, &request_id);
        if meta.wait_until_count == 0 {
            clear_actor_request_scope(state, &request_id);
            clear_request_secret_context(state, &request_id);
        }
    }
    let sender = state.borrow::<IsolateEventSender>().clone();
    let _ = sender.0.send(IsolateEventPayload::Completion(payload));
}

#[deno_core::op2(fast)]
fn op_emit_wait_until_done(state: &mut OpState, #[string] payload: String) {
    if let Some(request_id) = wait_until_request_id(&payload) {
        clear_actor_request_scope(state, &request_id);
        clear_request_secret_context(state, &request_id);
    }
    let sender = state.borrow::<IsolateEventSender>().clone();
    let _ = sender.0.send(IsolateEventPayload::WaitUntilDone(payload));
}

#[deno_core::op2(fast)]
fn op_emit_response_start(state: &mut OpState, #[string] payload: String) {
    let sender = state.borrow::<IsolateEventSender>().clone();
    let _ = sender.0.send(IsolateEventPayload::ResponseStart(payload));
}

#[deno_core::op2(fast)]
fn op_emit_response_chunk(state: &mut OpState, #[string] payload: String) {
    let sender = state.borrow::<IsolateEventSender>().clone();
    let _ = sender.0.send(IsolateEventPayload::ResponseChunk(payload));
}

#[deno_core::op2(fast)]
fn op_emit_cache_revalidate(state: &mut OpState, #[string] payload: String) {
    let sender = state.borrow::<IsolateEventSender>().clone();
    let _ = sender.0.send(IsolateEventPayload::CacheRevalidate(payload));
}

deno_core::extension!(
    dd_runtime_ops,
    ops = [
        op_sleep,
        op_time_boundary_now,
        op_crypto_digest,
        op_crypto_hmac_sign,
        op_crypto_hmac_verify,
        op_crypto_aes_gcm_encrypt,
        op_crypto_aes_gcm_decrypt,
        op_kv_get,
        op_kv_get_many_utf8,
        op_kv_get_value,
        op_kv_profile_record_js,
        op_kv_profile_take,
        op_kv_profile_reset,
        op_kv_take_failed_write_version,
        op_kv_put,
        op_kv_put_value,
        op_kv_delete,
        op_kv_enqueue_put,
        op_kv_enqueue_put_value,
        op_kv_enqueue_delete,
        op_kv_apply_batch,
        op_kv_list,
        op_cache_match,
        op_cache_put,
        op_cache_delete,
        op_dynamic_profile_take,
        op_dynamic_profile_reset,
        op_http_prepare,
        op_http_check_url,
        op_dynamic_cancel_reply,
        op_dynamic_host_rpc_task_complete,
        op_test_async_reply_start,
        op_test_async_reply_cancel,
        op_test_nested_targeted_invoke_start,
        op_dynamic_worker_create,
        op_dynamic_worker_lookup,
        op_dynamic_worker_list,
        op_dynamic_worker_delete,
        op_dynamic_worker_invoke,
        op_dynamic_host_rpc_invoke,
        op_request_body_read,
        op_request_body_cancel,
        op_actor_invoke_method,
        op_actor_profile_record_js,
        op_actor_profile_take,
        op_actor_profile_reset,
        op_actor_state_get,
        op_actor_state_snapshot,
        op_actor_state_validate_reads,
        op_actor_state_version_if_newer,
        op_actor_state_apply_batch,
        op_actor_state_apply_blind_batch,
        op_actor_state_enqueue_batch,
        op_actor_state_await_submission,
        op_actor_socket_send,
        op_actor_socket_close,
        op_actor_socket_list,
        op_actor_socket_consume_close,
        op_actor_scope_enter,
        op_actor_scope_exit,
        op_actor_transport_send_stream,
        op_actor_transport_send_datagram,
        op_actor_transport_recv_stream,
        op_actor_transport_recv_datagram,
        op_actor_transport_close,
        op_actor_transport_list,
        op_actor_transport_consume_close,
        op_emit_completion,
        op_emit_wait_until_done,
        op_emit_response_start,
        op_emit_response_chunk,
        op_emit_cache_revalidate
    ],
    state = |state| {
        let parser = Arc::new(RuntimePermissionDescriptorParser::new(RealSys));
        state.put(PermissionsContainer::allow_all(parser));
    }
);

pub fn runtime_extension() -> deno_core::Extension {
    dd_runtime_ops::init()
}

fn parse_crypto_digest_algorithm(value: &str) -> Option<CryptoDigestAlgorithm> {
    let normalized = value.trim().to_ascii_uppercase();
    match normalized.as_str() {
        "SHA-1" | "SHA1" => Some(CryptoDigestAlgorithm::Sha1),
        "SHA-256" | "SHA256" => Some(CryptoDigestAlgorithm::Sha256),
        "SHA-384" | "SHA384" => Some(CryptoDigestAlgorithm::Sha384),
        "SHA-512" | "SHA512" => Some(CryptoDigestAlgorithm::Sha512),
        _ => None,
    }
}

fn compute_hmac(
    algorithm: CryptoDigestAlgorithm,
    key: &[u8],
    data: &[u8],
) -> std::result::Result<Vec<u8>, String> {
    match algorithm {
        CryptoDigestAlgorithm::Sha1 => {
            let mut mac = <Hmac<Sha1> as Mac>::new_from_slice(key)
                .map_err(|error| format!("hmac init failed: {error}"))?;
            mac.update(data);
            Ok(mac.finalize().into_bytes().to_vec())
        }
        CryptoDigestAlgorithm::Sha256 => {
            let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(key)
                .map_err(|error| format!("hmac init failed: {error}"))?;
            mac.update(data);
            Ok(mac.finalize().into_bytes().to_vec())
        }
        CryptoDigestAlgorithm::Sha384 => {
            let mut mac = <Hmac<Sha384> as Mac>::new_from_slice(key)
                .map_err(|error| format!("hmac init failed: {error}"))?;
            mac.update(data);
            Ok(mac.finalize().into_bytes().to_vec())
        }
        CryptoDigestAlgorithm::Sha512 => {
            let mut mac = <Hmac<Sha512> as Mac>::new_from_slice(key)
                .map_err(|error| format!("hmac init failed: {error}"))?;
            mac.update(data);
            Ok(mac.finalize().into_bytes().to_vec())
        }
    }
}

fn verify_hmac(
    algorithm: CryptoDigestAlgorithm,
    key: &[u8],
    data: &[u8],
    signature: &[u8],
) -> std::result::Result<bool, String> {
    match algorithm {
        CryptoDigestAlgorithm::Sha1 => {
            let mut mac = <Hmac<Sha1> as Mac>::new_from_slice(key)
                .map_err(|error| format!("hmac init failed: {error}"))?;
            mac.update(data);
            Ok(mac.verify_slice(signature).is_ok())
        }
        CryptoDigestAlgorithm::Sha256 => {
            let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(key)
                .map_err(|error| format!("hmac init failed: {error}"))?;
            mac.update(data);
            Ok(mac.verify_slice(signature).is_ok())
        }
        CryptoDigestAlgorithm::Sha384 => {
            let mut mac = <Hmac<Sha384> as Mac>::new_from_slice(key)
                .map_err(|error| format!("hmac init failed: {error}"))?;
            mac.update(data);
            Ok(mac.verify_slice(signature).is_ok())
        }
        CryptoDigestAlgorithm::Sha512 => {
            let mut mac = <Hmac<Sha512> as Mac>::new_from_slice(key)
                .map_err(|error| format!("hmac init failed: {error}"))?;
            mac.update(data);
            Ok(mac.verify_slice(signature).is_ok())
        }
    }
}

fn aes_gcm_encrypt(payload: &CryptoAesGcmPayload) -> std::result::Result<Vec<u8>, String> {
    if payload.iv.len() != 12 {
        return Err("AES-GCM iv must be exactly 12 bytes in v1".to_string());
    }
    if payload.tag_length != 128 {
        return Err("AES-GCM tagLength must be 128 in v1".to_string());
    }

    let nonce = Nonce::from_slice(&payload.iv);
    match payload.key.len() {
        16 => {
            let cipher = Aes128Gcm::new_from_slice(&payload.key)
                .map_err(|error| format!("AES-128-GCM key init failed: {error}"))?;
            cipher
                .encrypt(
                    nonce,
                    Payload {
                        msg: &payload.data,
                        aad: &payload.additional_data,
                    },
                )
                .map_err(|error| format!("AES-128-GCM encrypt failed: {error}"))
        }
        32 => {
            let cipher = Aes256Gcm::new_from_slice(&payload.key)
                .map_err(|error| format!("AES-256-GCM key init failed: {error}"))?;
            cipher
                .encrypt(
                    nonce,
                    Payload {
                        msg: &payload.data,
                        aad: &payload.additional_data,
                    },
                )
                .map_err(|error| format!("AES-256-GCM encrypt failed: {error}"))
        }
        _ => Err("AES-GCM key length must be 16 or 32 bytes".to_string()),
    }
}

fn aes_gcm_decrypt(payload: &CryptoAesGcmPayload) -> std::result::Result<Vec<u8>, String> {
    if payload.iv.len() != 12 {
        return Err("AES-GCM iv must be exactly 12 bytes in v1".to_string());
    }
    if payload.tag_length != 128 {
        return Err("AES-GCM tagLength must be 128 in v1".to_string());
    }

    let nonce = Nonce::from_slice(&payload.iv);
    match payload.key.len() {
        16 => {
            let cipher = Aes128Gcm::new_from_slice(&payload.key)
                .map_err(|error| format!("AES-128-GCM key init failed: {error}"))?;
            cipher
                .decrypt(
                    nonce,
                    Payload {
                        msg: &payload.data,
                        aad: &payload.additional_data,
                    },
                )
                .map_err(|error| format!("AES-128-GCM decrypt failed: {error}"))
        }
        32 => {
            let cipher = Aes256Gcm::new_from_slice(&payload.key)
                .map_err(|error| format!("AES-256-GCM key init failed: {error}"))?;
            cipher
                .decrypt(
                    nonce,
                    Payload {
                        msg: &payload.data,
                        aad: &payload.additional_data,
                    },
                )
                .map_err(|error| format!("AES-256-GCM decrypt failed: {error}"))
        }
        _ => Err("AES-GCM key length must be 16 or 32 bytes".to_string()),
    }
}

fn replace_placeholders_text(value: &str, replacements: &HashMap<String, String>) -> String {
    if replacements.is_empty() {
        return value.to_string();
    }
    let mut output = value.to_string();
    for (placeholder, secret) in replacements {
        if placeholder.is_empty() {
            continue;
        }
        output = output.replace(placeholder, secret);
    }
    output
}

fn replace_placeholders_in_body(body: Vec<u8>, replacements: &HashMap<String, String>) -> Vec<u8> {
    if replacements.is_empty() || body.is_empty() {
        return body;
    }
    match String::from_utf8(body) {
        Ok(value) => replace_placeholders_text(&value, replacements).into_bytes(),
        Err(error) => error.into_bytes(),
    }
}

fn is_egress_url_allowed(url: &reqwest::Url, allow_hosts: &[String]) -> bool {
    if allow_hosts.is_empty() {
        return false;
    }
    let host = url
        .host_str()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    if host.is_empty() {
        return false;
    }
    let Some(request_port) = url.port_or_known_default() else {
        return false;
    };
    let Some(default_port) = default_port_for_scheme(url.scheme()) else {
        return false;
    };
    allow_hosts.iter().any(|allowed| {
        let Some((allowed_host, allowed_port)) = parse_egress_allow_host(allowed) else {
            return false;
        };
        let port_matches = match allowed_port {
            Some(port) => port == request_port,
            None => request_port == default_port,
        };
        if !port_matches {
            return false;
        }
        if let Some(suffix) = allowed_host.strip_prefix("*.") {
            return host == suffix || host.ends_with(&format!(".{suffix}"));
        }
        host == allowed_host
    })
}

fn parse_egress_allow_host(allowed: &str) -> Option<(String, Option<u16>)> {
    let allowed = allowed.trim().to_ascii_lowercase();
    if allowed.is_empty() {
        return None;
    }
    let (host, port) = match allowed.rsplit_once(':') {
        Some((host, port)) if port.chars().all(|char| char.is_ascii_digit()) => {
            let parsed = port.parse::<u16>().ok().filter(|port| *port > 0)?;
            (host.to_string(), Some(parsed))
        }
        _ => (allowed, None),
    };
    if host.is_empty() {
        return None;
    }
    Some((host, port))
}

fn default_port_for_scheme(scheme: &str) -> Option<u16> {
    match scheme {
        "http" => Some(80),
        "https" => Some(443),
        _ => None,
    }
}

pub fn register_request_body_stream(
    state: &mut OpState,
    request_id: String,
    receiver: RequestBodyReceiver,
) {
    state
        .borrow_mut::<RequestBodyStreams>()
        .streams
        .insert(request_id, Arc::new(RequestBodyStream::new(receiver)));
}

pub fn register_actor_request_scope(
    state: &mut OpState,
    request_id: String,
    namespace: String,
    actor_key: String,
) {
    state.borrow_mut::<ActorRequestScopes>().scopes.insert(
        request_id,
        ActorRequestScope {
            namespace,
            actor_key,
        },
    );
}

pub fn register_request_secret_context(
    state: &mut OpState,
    request_id: String,
    worker_name: String,
    generation: u64,
    isolate_id: u64,
    dynamic_bindings: Vec<String>,
    dynamic_rpc_bindings: Vec<String>,
    replacements: Vec<(String, String)>,
    egress_allow_hosts: Vec<String>,
) {
    let dynamic_bindings: HashSet<String> = dynamic_bindings
        .into_iter()
        .map(|binding| binding.trim().to_string())
        .filter(|binding| !binding.is_empty())
        .collect();
    let dynamic_rpc_bindings: HashSet<String> = dynamic_rpc_bindings
        .into_iter()
        .map(|binding| binding.trim().to_string())
        .filter(|binding| !binding.is_empty())
        .collect();
    let replacements = replacements
        .into_iter()
        .filter_map(|(placeholder, value)| {
            let key = placeholder.trim().to_string();
            if key.is_empty() {
                return None;
            }
            Some((key, value))
        })
        .collect();
    if let Some(previous) = state.borrow_mut::<RequestSecretContexts>().contexts.insert(
        request_id,
        RequestSecretContext {
            worker_name,
            generation,
            isolate_id,
            dynamic_bindings,
            dynamic_rpc_bindings,
            replacements,
            egress_allow_hosts,
            canceled: Arc::new(AtomicBool::new(false)),
            canceled_notify: Arc::new(Notify::new()),
        },
    ) {
        previous.canceled.store(true, Ordering::SeqCst);
        previous.canceled_notify.notify_waiters();
    }
}

pub fn cancel_request_body_stream(state: &mut OpState, request_id: &str) {
    if let Some(stream) = state.borrow::<RequestBodyStreams>().streams.get(request_id) {
        stream.cancel();
    }
}

pub fn clear_request_body_stream(state: &mut OpState, request_id: &str) {
    if let Some(stream) = state
        .borrow_mut::<RequestBodyStreams>()
        .streams
        .remove(request_id)
    {
        stream.cancel();
    }
}

pub fn clear_actor_request_scope(state: &mut OpState, request_id: &str) {
    state
        .borrow_mut::<ActorRequestScopes>()
        .scopes
        .remove(request_id);
}

pub fn clear_request_secret_context(state: &mut OpState, request_id: &str) {
    if let Some(context) = state
        .borrow_mut::<RequestSecretContexts>()
        .contexts
        .remove(request_id)
    {
        context.canceled.store(true, Ordering::SeqCst);
        context.canceled_notify.notify_waiters();
    }
}

fn clear_request_body_stream_entry(state: &Rc<RefCell<OpState>>, request_id: &str) {
    if let Some(stream) = state
        .borrow_mut()
        .borrow_mut::<RequestBodyStreams>()
        .streams
        .remove(request_id)
    {
        stream.cancel();
    }
}

fn completion_meta(payload: &str) -> Option<CompletionMeta> {
    let mut bytes = payload.as_bytes().to_vec();
    simd_json::serde::from_slice::<CompletionMeta>(&mut bytes).ok()
}

fn wait_until_request_id(payload: &str) -> Option<String> {
    let mut bytes = payload.as_bytes().to_vec();
    simd_json::serde::from_slice::<WaitUntilRequestId>(&mut bytes)
        .ok()
        .map(|value| value.request_id)
}

fn wall_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

fn to_list_item(entry: KvEntry) -> KvListItem {
    KvListItem {
        key: entry.key,
        value: entry.value,
        encoding: entry.encoding,
    }
}

fn decode_cache_request_payload(payload: String) -> common::Result<CacheRequest> {
    let payload: CacheRequestPayload = crate::json::from_string(payload).map_err(|error| {
        common::PlatformError::runtime(format!("invalid cache payload: {error}"))
    })?;
    Ok(CacheRequest {
        cache_name: payload.cache_name,
        method: payload.method,
        url: payload.url,
        headers: payload.headers,
        bypass_stale: payload.bypass_stale,
    })
}

fn decode_cache_put_payload(payload: String) -> common::Result<(CacheRequest, CacheResponse)> {
    let payload: CachePutPayload = crate::json::from_string(payload).map_err(|error| {
        common::PlatformError::runtime(format!("invalid cache put payload: {error}"))
    })?;
    Ok((
        CacheRequest {
            cache_name: payload.cache_name,
            method: payload.method,
            url: payload.url,
            headers: payload.request_headers,
            bypass_stale: false,
        },
        CacheResponse {
            status: payload.response_status,
            headers: payload.response_headers,
            body: payload.response_body,
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::{is_egress_url_allowed, parse_egress_allow_host};

    #[test]
    fn parse_egress_allow_host_supports_exact_and_port_rules() {
        assert_eq!(
            parse_egress_allow_host("api.example.com"),
            Some(("api.example.com".to_string(), None))
        );
        assert_eq!(
            parse_egress_allow_host("api.example.com:8443"),
            Some(("api.example.com".to_string(), Some(8443)))
        );
        assert_eq!(
            parse_egress_allow_host("*.example.com:8443"),
            Some(("*.example.com".to_string(), Some(8443)))
        );
    }

    #[test]
    fn egress_url_rules_require_matching_port() {
        let allowed = vec!["api.example.com".to_string()];
        let https_default = reqwest::Url::parse("https://api.example.com/path").expect("url");
        let https_alt = reqwest::Url::parse("https://api.example.com:8443/path").expect("url");
        assert!(is_egress_url_allowed(&https_default, &allowed));
        assert!(!is_egress_url_allowed(&https_alt, &allowed));
    }

    #[test]
    fn egress_url_rules_support_explicit_ports_and_wildcards() {
        let allowed = vec![
            "api.example.com:8443".to_string(),
            "*.internal.example.com".to_string(),
        ];
        let exact = reqwest::Url::parse("https://api.example.com:8443/path").expect("url");
        let wildcard_ok =
            reqwest::Url::parse("https://svc.internal.example.com/path").expect("url");
        let wildcard_bad =
            reqwest::Url::parse("https://svc.internal.example.com:8443/path").expect("url");
        assert!(is_egress_url_allowed(&exact, &allowed));
        assert!(is_egress_url_allowed(&wildcard_ok, &allowed));
        assert!(!is_egress_url_allowed(&wildcard_bad, &allowed));
    }
}
