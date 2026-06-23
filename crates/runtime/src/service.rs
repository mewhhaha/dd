mod config;
mod control;
mod debug;
mod dispatch;
mod dynamic;
mod facade;
mod isolate;
mod lifecycle;
mod model;
mod protocol;
mod runtime;
mod sessions;
mod storage;

use crate::blob::{BlobStore, BlobStoreConfig};
use crate::cache::{CacheConfig, CacheLookup, CacheRequest, CacheResponse, CacheStore};
use crate::engine::{
    abort_worker_request_handle, build_bootstrap_snapshot, cache_runtime_entrypoints,
    dispatch_worker_request, drain_dynamic_control_queue, ensure_v8_flags,
    install_worker_deployment_config, new_runtime_from_snapshot,
    new_runtime_from_snapshot_with_heap_limit, pump_event_loop_once, validate_worker,
};
use crate::kv::KvStore;
use crate::memory::{MemoryOutboxClaim, MemoryStore};
use crate::memory_rpc::{
    decode_memory_invoke_request, encode_memory_invoke_response, MemoryInvokeCall,
    MemoryInvokeResponse,
};
use crate::ops::{
    clear_request_body_stream, clear_request_secret_context, register_memory_request_scope,
    register_request_body_stream, register_request_secret_context, CacheRevalidatePayload,
    IsolateEventPayload, IsolateEventSender, MemoryInvokeEvent, RequestBodyStreams,
    RequestExecutionContext,
};
use crate::static_assets::{
    compile_asset_bundle, resolve_asset, AssetBundle, AssetRequest, AssetResponse,
};
use bytes::Bytes;
use common::{
    DeployAsset, DeployBinding, DeployConfig, ErrorKind, PlatformError, Result, WorkerInvocation,
    WorkerOutput,
};
#[cfg(feature = "otel")]
use opentelemetry::global;
#[cfg(feature = "otel")]
use opentelemetry::propagation::Extractor;
#[cfg(feature = "otel")]
use opentelemetry::trace::TraceContextExt;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::mem;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex, Once};
use std::task::{Wake, Waker};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::runtime::Builder;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::{mpsc, oneshot, Notify};
use tracing::{info, warn, Level};
#[cfg(feature = "otel")]
use tracing_opentelemetry::OpenTelemetrySpanExt;
use uuid::Uuid;

use self::config::{
    build_dynamic_worker_config, extract_bindings, full_dynamic_internal_policy,
    validate_runtime_config, DeployBindings, ValidatedDynamicWorkerPolicy,
    MAX_DYNAMIC_HOST_RPC_ARG_BYTES, MAX_DYNAMIC_HOST_RPC_METHODS, MAX_DYNAMIC_HOST_RPC_REPLY_BYTES,
};
use self::control::RuntimeEvent;
type RuntimeEventReceiver = mpsc::Receiver<RuntimeEvent>;
type RuntimeEventSender = mpsc::Sender<RuntimeEvent>;
pub(crate) use self::control::{RuntimeCommand, RuntimeFastCommandSender};
pub use self::facade::{
    DynamicDeployResult, DynamicHandleDebug, DynamicRuntimeDebugDump, HostRpcProviderDebug,
    InvokeRequestBodyReceiver, PublicRouteAssetResolution, RuntimeConfig, RuntimeService,
    RuntimeServiceConfig, RuntimeStorageConfig, TransportOpen, WebSocketOpen, WorkerDebugDump,
    WorkerDebugIsolate, WorkerDebugRequest, WorkerStats, WorkerStreamBody, WorkerStreamOutput,
};

#[derive(Clone, Default)]
struct AssetCatalog {
    snapshot: Arc<StdMutex<Arc<HashMap<String, Arc<AssetCatalogEntry>>>>>,
}

impl AssetCatalog {
    fn get(&self, worker_name: &str) -> Option<Arc<AssetCatalogEntry>> {
        self.snapshot
            .lock()
            .expect("asset catalog mutex poisoned")
            .get(worker_name)
            .cloned()
    }

    fn insert(&self, worker_name: String, entry: AssetCatalogEntry) {
        let mut snapshot = self.snapshot.lock().expect("asset catalog mutex poisoned");
        let mut next = (**snapshot).clone();
        next.insert(worker_name, Arc::new(entry));
        *snapshot = Arc::new(next);
    }

    fn remove(&self, worker_name: &str) {
        let mut snapshot = self.snapshot.lock().expect("asset catalog mutex poisoned");
        if !snapshot.contains_key(worker_name) {
            return;
        }
        let mut next = (**snapshot).clone();
        next.remove(worker_name);
        *snapshot = Arc::new(next);
    }
}

#[derive(Clone, Debug)]
struct AssetCatalogEntry {
    worker_name: String,
    generation: u64,
    assets: Arc<AssetBundle>,
    public: bool,
}
pub(crate) use self::isolate::*;
pub(crate) use self::model::DynamicQuotaState;
use self::model::*;
use self::protocol::*;
use self::runtime::*;
use self::storage::{delete_worker_deployment, epoch_ms_i64, persist_worker_deployment};

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

#[cfg(test)]
mod tests;
