use crate::cache::{CacheLookup, CacheRequest, CacheResponse, CacheStore};
use crate::kv::{
    KvBatchMutation, KvEntry, KvProfileMetricKind, KvProfileSnapshot, KvStore, KvUtf8Lookup,
};
use crate::memory::{
    MemoryBatchMutation, MemoryDirectMutation, MemoryProfileMetricKind, MemoryReadDependency,
    MemoryStore,
};
use crate::memory_rpc::{
    decode_memory_invoke_response, encode_memory_invoke_request, MemoryInvokeCall,
    MemoryInvokeRequest, MemoryInvokeResponse,
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
use std::collections::{HashMap, HashSet, VecDeque};
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::OnceLock;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use sys_traits::impls::RealSys;
use tokio::sync::{mpsc, oneshot, Mutex, Notify};

#[path = "ops/dynamic.rs"]
mod dynamic_ops;
#[path = "ops/memory.rs"]
mod memory_ops;
#[path = "ops/request.rs"]
mod request_ops;
#[path = "ops/dynamic_types.rs"]
mod dynamic_types;
#[path = "ops/memory_types.rs"]
mod memory_types;
#[path = "ops/request_types.rs"]
mod request_types;


use self::dynamic_ops::*;
use self::memory_ops::*;
use self::request_ops::*;
pub(crate) use self::dynamic_types::*;
pub(crate) use self::memory_types::*;
pub(crate) use self::request_types::*;

#[derive(Clone)]
pub struct IsolateEventSender(pub std::sync::mpsc::Sender<IsolateEventPayload>);

#[derive(Clone, Default)]
pub struct MemoryOpenHandleRegistry {
    socket_handles: Arc<StdMutex<HashMap<String, HashSet<String>>>>,
    transport_handles: Arc<StdMutex<HashMap<String, HashSet<String>>>>,
}

impl MemoryOpenHandleRegistry {
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
    MemoryInvoke(MemoryInvokeEvent),
    MemorySocketSend(MemorySocketSendEvent),
    MemorySocketClose(MemorySocketCloseEvent),
    MemorySocketConsumeClose(MemorySocketConsumeCloseEvent),
    MemoryTransportSendStream(MemoryTransportSendStreamEvent),
    MemoryTransportSendDatagram(MemoryTransportSendDatagramEvent),
    MemoryTransportRecvStream(MemoryTransportRecvStreamEvent),
    MemoryTransportRecvDatagram(MemoryTransportRecvDatagramEvent),
    MemoryTransportClose(MemoryTransportCloseEvent),
    MemoryTransportConsumeClose(MemoryTransportConsumeCloseEvent),
    DynamicWorkerCreate(DynamicWorkerCreateEvent),
    DynamicWorkerLookup(DynamicWorkerLookupEvent),
    DynamicWorkerList(DynamicWorkerListEvent),
    DynamicWorkerDelete(DynamicWorkerDeleteEvent),
    DynamicWorkerInvoke(DynamicWorkerInvokeEvent),
    DynamicHostRpcInvoke(DynamicHostRpcInvokeEvent),
    TestAsyncReply(TestAsyncReplyEvent),
    TestNestedTargetedInvoke(TestNestedTargetedInvokeEvent),
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

pub(crate) fn current_time_boundary() -> TimeBoundary {
    let now_ms = wall_ms();
    let perf_ms = PROCESS_MONO_START
        .get_or_init(Instant::now)
        .elapsed()
        .as_secs_f64()
        * 1000.0;
    TimeBoundary { now_ms, perf_ms }
}

#[deno_core::op2]
async fn op_sleep(millis: u32) {
    tokio::time::sleep(Duration::from_millis(u64::from(millis))).await;
}

#[deno_core::op2]
#[serde]
fn op_time_boundary_now() -> TimeBoundary {
    current_time_boundary()
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

#[deno_core::op2(fast)]
fn op_emit_completion(state: &mut OpState, #[string] payload: String) {
    if let Some(meta) = completion_meta(&payload) {
        let request_id = meta.request_id;
        clear_request_body_stream(state, &request_id);
        if meta.wait_until_count == 0 {
            clear_memory_request_scope(state, &request_id);
            clear_request_secret_context(state, &request_id);
        }
    }
    let sender = state.borrow::<IsolateEventSender>().clone();
    let _ = sender.0.send(IsolateEventPayload::Completion(payload));
}

#[deno_core::op2(fast)]
fn op_emit_wait_until_done(state: &mut OpState, #[string] payload: String) {
    if let Some(request_id) = wait_until_request_id(&payload) {
        clear_memory_request_scope(state, &request_id);
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
        op_dynamic_take_control_items,
        op_test_async_reply_start,
        op_test_async_reply_cancel,
        op_test_nested_targeted_invoke_start,
        op_dynamic_worker_create,
        op_dynamic_worker_lookup,
        op_dynamic_worker_list,
        op_dynamic_worker_delete,
        op_dynamic_worker_invoke,
        op_dynamic_worker_fetch_start,
        op_dynamic_host_rpc_invoke,
        op_request_body_read,
        op_request_body_cancel,
        op_memory_invoke_method,
        op_memory_profile_record_js,
        op_memory_profile_take,
        op_memory_profile_reset,
        op_memory_state_get,
        op_memory_state_snapshot,
        op_memory_state_validate_reads,
        op_memory_state_version_if_newer,
        op_memory_state_apply_batch,
        op_memory_state_apply_blind_batch,
        op_memory_state_enqueue_batch,
        op_memory_state_await_submission,
        op_memory_socket_send,
        op_memory_socket_close,
        op_memory_socket_list,
        op_memory_socket_consume_close,
        op_memory_scope_enter,
        op_memory_scope_exit,
        op_memory_transport_send_stream,
        op_memory_transport_send_datagram,
        op_memory_transport_recv_stream,
        op_memory_transport_recv_datagram,
        op_memory_transport_close,
        op_memory_transport_list,
        op_memory_transport_consume_close,
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

pub fn register_memory_request_scope(
    state: &mut OpState,
    request_id: String,
    namespace: String,
    memory_key: String,
) {
    state.borrow_mut::<MemoryRequestScopes>().scopes.insert(
        request_id,
        MemoryRequestScope {
            namespace,
            memory_key,
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

pub fn clear_memory_request_scope(state: &mut OpState, request_id: &str) {
    state
        .borrow_mut::<MemoryRequestScopes>()
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
    use super::{
        is_egress_url_allowed, parse_egress_allow_host, DynamicControlInbox, DynamicControlItem,
        DynamicPendingReplyResult, DynamicPushedReplyPayload,
    };

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

    #[test]
    fn dynamic_control_inbox_batches_reply_items_under_one_schedule() {
        let inbox = DynamicControlInbox::default();
        assert!(inbox.push_reply(DynamicPushedReplyPayload::Dynamic(
            DynamicPendingReplyResult {
                reply_id: "dynr-1".to_string(),
                ready: true,
                ..DynamicPendingReplyResult::default()
            }
        )));
        assert!(!inbox.push_reply(DynamicPushedReplyPayload::Dynamic(
            DynamicPendingReplyResult {
                reply_id: "dynr-2".to_string(),
                ready: true,
                ..DynamicPendingReplyResult::default()
            }
        )));

        let first = inbox.take_batch();
        assert_eq!(first.len(), 2);
        assert!(matches!(
            &first[0],
            DynamicControlItem::Reply(DynamicPushedReplyPayload::Dynamic(payload))
                if payload.reply_id == "dynr-1"
        ));
        assert!(matches!(
            &first[1],
            DynamicControlItem::Reply(DynamicPushedReplyPayload::Dynamic(payload))
                if payload.reply_id == "dynr-2"
        ));
        assert!(inbox.take_batch().is_empty());
        assert!(inbox.push_reply(DynamicPushedReplyPayload::Dynamic(
            DynamicPendingReplyResult {
                reply_id: "dynr-3".to_string(),
                ready: true,
                ..DynamicPendingReplyResult::default()
            }
        )));
    }

    #[test]
    fn dynamic_control_inbox_reschedules_after_empty_drain() {
        let inbox = DynamicControlInbox::default();
        assert!(inbox.push_reply(DynamicPushedReplyPayload::Dynamic(
            DynamicPendingReplyResult {
                reply_id: "dynr-1".to_string(),
                ready: true,
                ..DynamicPendingReplyResult::default()
            }
        )));

        let first = inbox.take_batch();
        assert_eq!(first.len(), 1);
        assert!(matches!(
            &first[0],
            DynamicControlItem::Reply(DynamicPushedReplyPayload::Dynamic(payload))
                if payload.reply_id == "dynr-1"
        ));

        assert!(
            inbox.push_reply(DynamicPushedReplyPayload::Dynamic(
                DynamicPendingReplyResult {
                    reply_id: "dynr-2".to_string(),
                    ready: true,
                    ..DynamicPendingReplyResult::default()
                }
            )) == false
        );
        let second = inbox.take_batch();
        assert_eq!(second.len(), 1);
        assert!(matches!(
            &second[0],
            DynamicControlItem::Reply(DynamicPushedReplyPayload::Dynamic(payload))
                if payload.reply_id == "dynr-2"
        ));
        assert!(inbox.take_batch().is_empty());
        assert!(inbox.push_reply(DynamicPushedReplyPayload::Dynamic(
            DynamicPendingReplyResult {
                reply_id: "dynr-3".to_string(),
                ready: true,
                ..DynamicPendingReplyResult::default()
            }
        )));
    }
}
