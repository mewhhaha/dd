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
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet, VecDeque};
use std::mem;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
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

use self::config::{
    build_dynamic_worker_config, extract_bindings, full_dynamic_internal_policy,
    validate_runtime_config, ValidatedDynamicWorkerPolicy, MAX_DYNAMIC_HOST_RPC_ARG_BYTES,
    MAX_DYNAMIC_HOST_RPC_METHODS, MAX_DYNAMIC_HOST_RPC_REPLY_BYTES,
};
use self::control::RuntimeEvent;
pub(crate) use self::control::{RuntimeCommand, RuntimeFastCommandSender};
pub use self::facade::{
    DynamicDeployResult, DynamicHandleDebug, DynamicRuntimeDebugDump, HostRpcProviderDebug,
    InvokeRequestBodyReceiver, RuntimeConfig, RuntimeService, RuntimeServiceConfig,
    RuntimeStorageConfig, TransportOpen, WebSocketOpen, WorkerDebugDump, WorkerDebugIsolate,
    WorkerDebugRequest, WorkerStats, WorkerStreamOutput,
};
use self::isolate::*;
pub(crate) use self::model::DynamicQuotaState;
use self::model::*;
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

#[cfg(test)]
mod tests;
