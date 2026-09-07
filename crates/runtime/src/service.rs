mod bindings;
mod config;
mod control;
mod debug;
mod deployment;
mod dispatch;
mod facade;
mod isolate;
mod lifecycle;
mod model;
mod protocol;
mod router;
mod runtime;
mod sessions;
mod storage;

use crate::cache::{CacheConfig, CacheLookup, CacheRequest, CacheResponse, CacheStore};
use crate::control_store::{ControlDeployment, ControlStore};
use crate::engine::{
    WorkerDispatchRequest, abort_worker_request_handle, build_bootstrap_snapshot,
    cache_runtime_entrypoints, dispatch_worker_request, drain_request_control_queue,
    ensure_v8_flags, install_worker_deployment_config, new_runtime_from_snapshot_with_heap_limit,
    pump_event_loop_once,
};
use crate::kv::KvStore;
use crate::memory::{
    DEFAULT_MEMORY_SNAPSHOT_CACHE_MAX_BYTES, DEFAULT_MEMORY_SNAPSHOT_CACHE_MAX_ENTRIES,
    MemoryOutboxClaim, MemoryOutboxDeliveryAction, MemoryOutboxDeliveryOutcome,
    MemoryProfileMetricKind, MemoryStore,
};

use crate::ops::{
    CacheRevalidatePayload, IsolateEventPayload, IsolateEventSender, RequestBodyStreams,
    RequestExecutionContext, RequestExecutionContextInit, clear_request_body_stream,
    clear_request_secret_context, register_memory_request_scope, register_request_body_stream,
    register_request_secret_context,
};
use crate::static_assets::{
    AssetBundle, AssetRequest, AssetResponse, compile_asset_bundle, resolve_asset,
};
use arc_swap::ArcSwap;
use bytes::Bytes;
use common::{
    DeployAsset, DeployConfig, DeployServerModule, ErrorKind, PlatformError, Result,
    WorkerInvocation, WorkerOutput,
};
use futures_util::FutureExt;
#[cfg(feature = "otel")]
use opentelemetry::global;
#[cfg(feature = "otel")]
use opentelemetry::propagation::Extractor;
#[cfg(feature = "otel")]
use opentelemetry::trace::TraceContextExt;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::mem;
use std::panic::AssertUnwindSafe;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex, Once};
use std::task::{Wake, Waker};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::runtime::Builder;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::{Notify, mpsc, oneshot};
use tokio::task::JoinSet;
use tracing::{Instrument, Level, info, warn};
#[cfg(feature = "otel")]
use tracing_opentelemetry::OpenTelemetrySpanExt;
use uuid::Uuid;

use self::config::{DeployBindings, extract_bindings, validate_runtime_config};
use self::control::RuntimeEvent;
use self::sessions::{
    MemoryOutboxDrainSender, memory_outbox_worker_channel, run_memory_outbox_worker,
};
type RuntimeEventReceiver = mpsc::Receiver<RuntimeEvent>;
type RuntimeEventSender = mpsc::Sender<RuntimeEvent>;
type AssetCatalogSnapshot = Arc<ArcSwap<HashMap<String, Arc<AssetCatalogEntry>>>>;
pub(crate) use self::control::{RuntimeCommand, RuntimeFastCommandSender};
pub use self::facade::{
    InvokeRequestBodyReceiver, MemoryOutboxDebug, MemorySchedulerDebug, MemoryShardDebug,
    PublicRouteAssetResolution, RuntimeAdminSnapshot, RuntimeCheckpointResult, RuntimeConfig,
    RuntimeReadiness, RuntimeRestoreFailure, RuntimeService, RuntimeServiceConfig,
    RuntimeStorageConfig, RuntimeWorkerStatus, WebSocketOpen, WorkerDebugDump, WorkerDebugIsolate,
    WorkerDebugRequest, WorkerStats, WorkerStreamBody, WorkerStreamOutput,
};

#[derive(Clone)]
struct AssetCatalog {
    snapshot: AssetCatalogSnapshot,
}

impl Default for AssetCatalog {
    fn default() -> Self {
        Self {
            snapshot: Arc::new(ArcSwap::from_pointee(HashMap::new())),
        }
    }
}

impl AssetCatalog {
    fn get(&self, worker_name: &str) -> Option<Arc<AssetCatalogEntry>> {
        self.snapshot.load().get(worker_name).cloned()
    }

    fn insert(&self, worker_name: String, entry: AssetCatalogEntry) {
        let entry = Arc::new(entry);
        self.snapshot.rcu(|snapshot| {
            let mut next = (**snapshot).clone();
            next.insert(worker_name.clone(), Arc::clone(&entry));
            next
        });
    }

    fn remove(&self, worker_name: &str) {
        self.snapshot.rcu(|snapshot| {
            if !snapshot.contains_key(worker_name) {
                return Arc::clone(snapshot);
            }
            let mut next = (**snapshot).clone();
            next.remove(worker_name);
            Arc::new(next)
        });
    }

    fn worker_names(&self) -> Vec<String> {
        let mut names = self.snapshot.load().keys().cloned().collect::<Vec<_>>();
        names.sort();
        names
    }
}

#[derive(Clone, Debug)]
struct AssetCatalogEntry {
    worker_name: String,
    generation: u64,
    assets: Arc<AssetBundle>,
    public: bool,
    cache_enabled: bool,
}
pub(crate) use self::isolate::*;
use self::model::*;
use self::protocol::*;
use self::router::*;
use self::runtime::*;
use self::storage::epoch_ms_i64;

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
const CONTENT_TYPE_HEADER: &str = "content-type";
const JSON_CONTENT_TYPE: &str = "application/json";

static NEXT_RUNTIME_TOKEN: AtomicU64 = AtomicU64::new(1);

fn duration_us(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests;
