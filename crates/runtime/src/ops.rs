use crate::actor::{ActorStateEntry, ActorStore};
use crate::actor_rpc::{
    decode_actor_invoke_response, encode_actor_invoke_request, ActorInvokeCall, ActorInvokeRequest,
    ActorInvokeResponse,
};
use crate::cache::{CacheLookup, CacheRequest, CacheResponse, CacheStore};
use crate::kv::{KvEntry, KvStore};
use aes_gcm::aead::{Aead, Payload};
use aes_gcm::{Aes128Gcm, Aes192Gcm, Aes256Gcm, KeyInit, Nonce};
use common::{PlatformError, Result};
use deno_core::OpState;
use getrandom::fill as fill_random_bytes;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha1::Sha1;
use sha2::{Digest, Sha256, Sha384, Sha512};
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, oneshot, Mutex, Notify};
use uuid::Uuid;

#[derive(Clone)]
pub struct IsolateEventSender(pub tokio::sync::mpsc::UnboundedSender<IsolateEventPayload>);

pub enum IsolateEventPayload {
    Completion(String),
    WaitUntilDone(String),
    ResponseStart(String),
    ResponseChunk(String),
    CacheRevalidate(String),
    ActorInvoke(ActorInvokeEvent),
    ActorSocketSend(ActorSocketSendEvent),
    ActorSocketClose(ActorSocketCloseEvent),
    ActorSocketList(ActorSocketListEvent),
    ActorSocketConsumeClose(ActorSocketConsumeCloseEvent),
}

pub type RequestBodyChunk = std::result::Result<Vec<u8>, String>;
pub type RequestBodyReceiver = mpsc::Receiver<RequestBodyChunk>;

pub struct ActorInvokeEvent {
    pub request_frame: Vec<u8>,
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

pub struct ActorSocketListEvent {
    pub reply: oneshot::Sender<Result<Vec<String>>>,
    pub binding: String,
    pub key: String,
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
    value: String,
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
struct KvSetValuePayload {
    worker_name: String,
    binding: String,
    key: String,
    encoding: String,
    value: Vec<u8>,
}

#[derive(Debug, Serialize)]
struct KvOpResult {
    ok: bool,
    error: String,
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
struct ActorInvokeFetchPayload {
    worker_name: String,
    binding: String,
    key: String,
    method: String,
    url: String,
    #[serde(default)]
    headers: Vec<(String, String)>,
    #[serde(default)]
    body: Vec<u8>,
    request_id: String,
}

#[derive(Debug, Serialize)]
struct ActorInvokeFetchResult {
    ok: bool,
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
    error: String,
}

#[derive(Debug, Deserialize)]
struct ActorInvokeMethodPayload {
    worker_name: String,
    binding: String,
    key: String,
    method_name: String,
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
    code: u16,
    reason: String,
}

#[derive(Debug, Deserialize)]
struct ActorSocketListPayload {
    request_id: String,
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
struct ActorStateGetValuePayload {
    request_id: String,
    key: String,
}

#[derive(Debug, Serialize)]
struct ActorStateGetValueResult {
    ok: bool,
    found: bool,
    value: Vec<u8>,
    encoding: String,
    version: i64,
    error: String,
}

#[derive(Debug, Deserialize)]
struct ActorStateSetValuePayload {
    request_id: String,
    key: String,
    encoding: String,
    value: Vec<u8>,
    expected_version: i64,
}

#[derive(Debug, Serialize)]
struct ActorStateWriteResult {
    ok: bool,
    conflict: bool,
    version: i64,
    error: String,
}

#[derive(Debug, Serialize)]
struct ActorStateListItem {
    key: String,
    value: String,
    version: i64,
}

#[derive(Debug, Deserialize)]
struct ActorStateListPayload {
    request_id: String,
    #[serde(default)]
    prefix: String,
    limit: u32,
}

#[derive(Debug, Serialize)]
struct ActorStateListResult {
    ok: bool,
    entries: Vec<ActorStateListItem>,
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

#[derive(Debug, Serialize)]
struct CryptoRandomValuesResult {
    ok: bool,
    bytes: Vec<u8>,
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
async fn op_time_boundary_now() -> TimeBoundary {
    let now_ms = wall_ms();
    let perf_ms = PROCESS_MONO_START
        .get_or_init(Instant::now)
        .elapsed()
        .as_secs_f64()
        * 1000.0;
    TimeBoundary { now_ms, perf_ms }
}

#[deno_core::op2]
#[string]
fn op_crypto_random_uuid() -> String {
    Uuid::new_v4().to_string()
}

#[deno_core::op2]
#[serde]
fn op_crypto_get_random_values(len: u32) -> CryptoRandomValuesResult {
    const MAX_BYTES: usize = 1 << 20;
    let len = len as usize;
    if len > MAX_BYTES {
        return CryptoRandomValuesResult {
            ok: false,
            bytes: Vec::new(),
            error: format!("random byte request too large (max {MAX_BYTES})"),
        };
    }

    let mut bytes = vec![0_u8; len];
    match fill_random_bytes(&mut bytes) {
        Ok(()) => CryptoRandomValuesResult {
            ok: true,
            bytes,
            error: String::new(),
        },
        Err(error) => CryptoRandomValuesResult {
            ok: false,
            bytes: Vec::new(),
            error: format!("random bytes failed: {error}"),
        },
    }
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
    let store = state.borrow().borrow::<KvStore>().clone();
    match store.get(&worker_name, &binding, &key).await {
        Ok(Some(value)) => {
            if value.encoding != "utf8" {
                return KvGetResult {
                    ok: false,
                    found: true,
                    value: String::new(),
                    error: "kv value is encoded as v8sc; use env.KV.get() for JS value decoding"
                        .to_string(),
                };
            }
            match String::from_utf8(value.value) {
                Ok(decoded) => KvGetResult {
                    ok: true,
                    found: true,
                    value: decoded,
                    error: String::new(),
                },
                Err(error) => KvGetResult {
                    ok: false,
                    found: true,
                    value: String::new(),
                    error: format!("kv utf8 decode failed: {error}"),
                },
            }
        }
        Ok(None) => KvGetResult {
            ok: true,
            found: false,
            value: String::new(),
            error: String::new(),
        },
        Err(error) => KvGetResult {
            ok: false,
            found: false,
            value: String::new(),
            error: error.to_string(),
        },
    }
}

#[deno_core::op2]
#[serde]
async fn op_kv_get_value(
    state: Rc<RefCell<OpState>>,
    #[string] payload: String,
) -> KvGetValueResult {
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
    match store
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
    }
}

#[deno_core::op2]
#[serde]
async fn op_kv_set(
    state: Rc<RefCell<OpState>>,
    #[string] worker_name: String,
    #[string] binding: String,
    #[string] key: String,
    #[string] value: String,
) -> KvOpResult {
    let store = state.borrow().borrow::<KvStore>().clone();
    match store.set(&worker_name, &binding, &key, &value).await {
        Ok(()) => KvOpResult {
            ok: true,
            error: String::new(),
        },
        Err(error) => KvOpResult {
            ok: false,
            error: error.to_string(),
        },
    }
}

#[deno_core::op2]
#[serde]
async fn op_kv_set_value(state: Rc<RefCell<OpState>>, #[string] payload: String) -> KvOpResult {
    let payload: KvSetValuePayload = match crate::json::from_string(payload) {
        Ok(value) => value,
        Err(error) => {
            return KvOpResult {
                ok: false,
                error: format!("invalid kv set payload: {error}"),
            };
        }
    };
    let store = state.borrow().borrow::<KvStore>().clone();
    match store
        .set_value(
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
        },
        Err(error) => KvOpResult {
            ok: false,
            error: error.to_string(),
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
        },
        Err(error) => KvOpResult {
            ok: false,
            error: error.to_string(),
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
            };
        }
    };
    let store = state.borrow().borrow::<CacheStore>().clone();
    match store.put(&request, response).await {
        Ok(_) => KvOpResult {
            ok: true,
            error: String::new(),
        },
        Err(error) => KvOpResult {
            ok: false,
            error: error.to_string(),
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
async fn op_actor_invoke_fetch(
    state: Rc<RefCell<OpState>>,
    #[serde] payload: ActorInvokeFetchPayload,
) -> ActorInvokeFetchResult {
    if payload.worker_name.trim().is_empty()
        || payload.binding.trim().is_empty()
        || payload.key.trim().is_empty()
        || payload.request_id.trim().is_empty()
    {
        return ActorInvokeFetchResult {
            ok: false,
            status: 500,
            headers: Vec::new(),
            body: Vec::new(),
            error: "actor fetch invoke requires worker_name, binding, key, request_id".to_string(),
        };
    }
    let request_frame = match encode_actor_invoke_request(&ActorInvokeRequest {
        worker_name: payload.worker_name,
        binding: payload.binding,
        key: payload.key,
        call: ActorInvokeCall::Fetch(common::WorkerInvocation {
            method: payload.method,
            url: payload.url,
            headers: payload.headers,
            body: payload.body,
            request_id: payload.request_id,
        }),
    }) {
        Ok(frame) => frame,
        Err(error) => {
            return ActorInvokeFetchResult {
                ok: false,
                status: 500,
                headers: Vec::new(),
                body: Vec::new(),
                error: format!("actor fetch invoke encode failed: {error}"),
            };
        }
    };

    let (reply_tx, reply_rx) = oneshot::channel();
    let sender = state.borrow().borrow::<IsolateEventSender>().clone();
    if sender
        .0
        .send(IsolateEventPayload::ActorInvoke(ActorInvokeEvent {
            request_frame,
            reply: reply_tx,
        }))
        .is_err()
    {
        return ActorInvokeFetchResult {
            ok: false,
            status: 500,
            headers: Vec::new(),
            body: Vec::new(),
            error: "actor fetch runtime is unavailable".to_string(),
        };
    }

    match reply_rx.await {
        Ok(Ok(frame)) => match decode_actor_invoke_response(&frame) {
            Ok(ActorInvokeResponse::Fetch(output)) => ActorInvokeFetchResult {
                ok: true,
                status: output.status,
                headers: output.headers,
                body: output.body,
                error: String::new(),
            },
            Ok(ActorInvokeResponse::Error(error)) => ActorInvokeFetchResult {
                ok: false,
                status: 500,
                headers: Vec::new(),
                body: Vec::new(),
                error,
            },
            Ok(ActorInvokeResponse::Method { .. }) => ActorInvokeFetchResult {
                ok: false,
                status: 500,
                headers: Vec::new(),
                body: Vec::new(),
                error: "actor fetch invoke received method response".to_string(),
            },
            Err(error) => ActorInvokeFetchResult {
                ok: false,
                status: 500,
                headers: Vec::new(),
                body: Vec::new(),
                error: format!("actor fetch invoke decode failed: {error}"),
            },
        },
        Ok(Err(error)) => ActorInvokeFetchResult {
            ok: false,
            status: 500,
            headers: Vec::new(),
            body: Vec::new(),
            error: error.to_string(),
        },
        Err(_) => ActorInvokeFetchResult {
            ok: false,
            status: 500,
            headers: Vec::new(),
            body: Vec::new(),
            error: "actor fetch invoke response channel closed".to_string(),
        },
    }
}

#[deno_core::op2]
#[serde]
async fn op_actor_invoke_method(
    state: Rc<RefCell<OpState>>,
    #[serde] payload: ActorInvokeMethodPayload,
) -> ActorInvokeMethodResult {
    if payload.worker_name.trim().is_empty()
        || payload.binding.trim().is_empty()
        || payload.key.trim().is_empty()
        || payload.method_name.trim().is_empty()
        || payload.request_id.trim().is_empty()
    {
        return ActorInvokeMethodResult {
            ok: false,
            value: Vec::new(),
            error:
                "actor method invoke requires worker_name, binding, key, method_name, request_id"
                    .to_string(),
        };
    }
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
                error: format!("actor method invoke encode failed: {error}"),
            };
        }
    };

    let (reply_tx, reply_rx) = oneshot::channel();
    let sender = state.borrow().borrow::<IsolateEventSender>().clone();
    if sender
        .0
        .send(IsolateEventPayload::ActorInvoke(ActorInvokeEvent {
            request_frame,
            reply: reply_tx,
        }))
        .is_err()
    {
        return ActorInvokeMethodResult {
            ok: false,
            value: Vec::new(),
            error: "actor method runtime is unavailable".to_string(),
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
                error: "actor method invoke received fetch response".to_string(),
            },
            Err(error) => ActorInvokeMethodResult {
                ok: false,
                value: Vec::new(),
                error: format!("actor method invoke decode failed: {error}"),
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
            error: "actor method invoke response channel closed".to_string(),
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
        .ok_or_else(|| PlatformError::runtime("actor storage scope is unavailable"))
}

#[deno_core::op2]
#[serde]
async fn op_actor_state_get_value(
    state: Rc<RefCell<OpState>>,
    #[serde] payload: ActorStateGetValuePayload,
) -> ActorStateGetValueResult {
    let (namespace, actor_key) = match actor_scope_for_request(&state, &payload.request_id) {
        Ok(scope) => scope,
        Err(error) => {
            return ActorStateGetValueResult {
                ok: false,
                found: false,
                value: Vec::new(),
                encoding: "utf8".to_string(),
                version: -1,
                error: error.to_string(),
            };
        }
    };

    let store = state.borrow().borrow::<ActorStore>().clone();
    match store.get(&namespace, &actor_key, &payload.key).await {
        Ok(Some(value)) => ActorStateGetValueResult {
            ok: true,
            found: true,
            value: value.value,
            encoding: value.encoding,
            version: value.version,
            error: String::new(),
        },
        Ok(None) => ActorStateGetValueResult {
            ok: true,
            found: false,
            value: Vec::new(),
            encoding: "utf8".to_string(),
            version: -1,
            error: String::new(),
        },
        Err(error) => ActorStateGetValueResult {
            ok: false,
            found: false,
            value: Vec::new(),
            encoding: "utf8".to_string(),
            version: -1,
            error: error.to_string(),
        },
    }
}

#[deno_core::op2]
#[serde]
async fn op_actor_state_set(
    state: Rc<RefCell<OpState>>,
    #[string] request_id: String,
    #[string] key: String,
    #[string] value: String,
    #[bigint] expected_version: i64,
) -> ActorStateWriteResult {
    let (namespace, actor_key) = match actor_scope_for_request(&state, &request_id) {
        Ok(scope) => scope,
        Err(error) => {
            return ActorStateWriteResult {
                ok: false,
                conflict: false,
                version: -1,
                error: error.to_string(),
            };
        }
    };
    let expected_version = if expected_version < 0 {
        None
    } else {
        Some(expected_version)
    };
    let store = state.borrow().borrow::<ActorStore>().clone();
    match store
        .put(&namespace, &actor_key, &key, &value, expected_version)
        .await
    {
        Ok(result) => ActorStateWriteResult {
            ok: true,
            conflict: result.conflict,
            version: result.version,
            error: String::new(),
        },
        Err(error) => ActorStateWriteResult {
            ok: false,
            conflict: false,
            version: -1,
            error: error.to_string(),
        },
    }
}

#[deno_core::op2]
#[serde]
async fn op_actor_state_set_value(
    state: Rc<RefCell<OpState>>,
    #[serde] payload: ActorStateSetValuePayload,
) -> ActorStateWriteResult {
    let (namespace, actor_key) = match actor_scope_for_request(&state, &payload.request_id) {
        Ok(scope) => scope,
        Err(error) => {
            return ActorStateWriteResult {
                ok: false,
                conflict: false,
                version: -1,
                error: error.to_string(),
            };
        }
    };
    let expected_version = if payload.expected_version < 0 {
        None
    } else {
        Some(payload.expected_version)
    };
    let store = state.borrow().borrow::<ActorStore>().clone();
    match store
        .put_value(
            &namespace,
            &actor_key,
            &payload.key,
            &payload.value,
            &payload.encoding,
            expected_version,
        )
        .await
    {
        Ok(result) => ActorStateWriteResult {
            ok: true,
            conflict: result.conflict,
            version: result.version,
            error: String::new(),
        },
        Err(error) => ActorStateWriteResult {
            ok: false,
            conflict: false,
            version: -1,
            error: error.to_string(),
        },
    }
}

#[deno_core::op2]
#[serde]
async fn op_actor_state_delete(
    state: Rc<RefCell<OpState>>,
    #[string] request_id: String,
    #[string] key: String,
    #[bigint] expected_version: i64,
) -> ActorStateWriteResult {
    let (namespace, actor_key) = match actor_scope_for_request(&state, &request_id) {
        Ok(scope) => scope,
        Err(error) => {
            return ActorStateWriteResult {
                ok: false,
                conflict: false,
                version: -1,
                error: error.to_string(),
            };
        }
    };
    let expected_version = if expected_version < 0 {
        None
    } else {
        Some(expected_version)
    };
    let store = state.borrow().borrow::<ActorStore>().clone();
    match store
        .delete(&namespace, &actor_key, &key, expected_version)
        .await
    {
        Ok(result) => ActorStateWriteResult {
            ok: true,
            conflict: result.conflict,
            version: result.version,
            error: String::new(),
        },
        Err(error) => ActorStateWriteResult {
            ok: false,
            conflict: false,
            version: -1,
            error: error.to_string(),
        },
    }
}

#[deno_core::op2]
#[serde]
async fn op_actor_state_list(
    state: Rc<RefCell<OpState>>,
    #[serde] payload: ActorStateListPayload,
) -> ActorStateListResult {
    let (namespace, actor_key) = match actor_scope_for_request(&state, &payload.request_id) {
        Ok(scope) => scope,
        Err(error) => {
            return ActorStateListResult {
                ok: false,
                entries: Vec::new(),
                error: error.to_string(),
            };
        }
    };
    let store = state.borrow().borrow::<ActorStore>().clone();
    let clamped_limit = payload.limit.clamp(1, 1000) as usize;
    match store
        .list(&namespace, &actor_key, &payload.prefix, clamped_limit)
        .await
    {
        Ok(entries) => ActorStateListResult {
            ok: true,
            entries: entries
                .into_iter()
                .map(|entry: ActorStateEntry| ActorStateListItem {
                    key: entry.key,
                    value: entry.value,
                    version: entry.version,
                })
                .collect(),
            error: String::new(),
        },
        Err(error) => ActorStateListResult {
            ok: false,
            entries: Vec::new(),
            error: error.to_string(),
        },
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
            error: "actor socket send requires request_id".to_string(),
        };
    }
    if payload.handle.trim().is_empty() {
        return ActorSocketSendResult {
            ok: false,
            error: "actor socket send requires handle".to_string(),
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

    let (binding, key) = match actor_scope_for_request(&state, &payload.request_id) {
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
            error: "actor socket send runtime is unavailable".to_string(),
        };
    }

    match reply_rx.await {
        Ok(Ok(())) => ActorSocketSendResult {
            ok: true,
            error: String::new(),
        },
        Ok(Err(error)) => ActorSocketSendResult {
            ok: false,
            error: error.to_string(),
        },
        Err(_) => ActorSocketSendResult {
            ok: false,
            error: "actor socket send response channel closed".to_string(),
        },
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
            error: "actor socket close requires request_id".to_string(),
        };
    }
    if payload.handle.trim().is_empty() {
        return ActorSocketCloseResult {
            ok: false,
            error: "actor socket close requires handle".to_string(),
        };
    }

    let (binding, key) = match actor_scope_for_request(&state, &payload.request_id) {
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
            error: "actor socket close runtime is unavailable".to_string(),
        };
    }

    match reply_rx.await {
        Ok(Ok(())) => ActorSocketCloseResult {
            ok: true,
            error: String::new(),
        },
        Ok(Err(error)) => ActorSocketCloseResult {
            ok: false,
            error: error.to_string(),
        },
        Err(_) => ActorSocketCloseResult {
            ok: false,
            error: "actor socket close response channel closed".to_string(),
        },
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
            error: "actor socket list requires request_id".to_string(),
        };
    }

    let (binding, key) = match actor_scope_for_request(&state, &payload.request_id) {
        Ok(value) => value,
        Err(error) => {
            return ActorSocketListResult {
                ok: false,
                handles: Vec::new(),
                error: error.to_string(),
            };
        }
    };

    let (reply_tx, reply_rx) = oneshot::channel();
    let sender = state.borrow().borrow::<IsolateEventSender>().clone();
    if sender
        .0
        .send(IsolateEventPayload::ActorSocketList(ActorSocketListEvent {
            reply: reply_tx,
            binding,
            key,
        }))
        .is_err()
    {
        return ActorSocketListResult {
            ok: false,
            handles: Vec::new(),
            error: "actor socket list runtime is unavailable".to_string(),
        };
    }

    match reply_rx.await {
        Ok(Ok(handles)) => ActorSocketListResult {
            ok: true,
            handles,
            error: String::new(),
        },
        Ok(Err(error)) => ActorSocketListResult {
            ok: false,
            handles: Vec::new(),
            error: error.to_string(),
        },
        Err(_) => ActorSocketListResult {
            ok: false,
            handles: Vec::new(),
            error: "actor socket list response channel closed".to_string(),
        },
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
            error: "actor socket consumeClose requires request_id".to_string(),
        };
    }
    if payload.handle.trim().is_empty() {
        return ActorSocketConsumeCloseResult {
            ok: false,
            events: Vec::new(),
            error: "actor socket consumeClose requires handle".to_string(),
        };
    }

    let (binding, key) = match actor_scope_for_request(&state, &payload.request_id) {
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
            error: "actor socket consumeClose runtime is unavailable".to_string(),
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
            error: "actor socket consumeClose response channel closed".to_string(),
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
        }
    }
    let sender = state.borrow::<IsolateEventSender>().clone();
    let _ = sender.0.send(IsolateEventPayload::Completion(payload));
}

#[deno_core::op2(fast)]
fn op_emit_wait_until_done(state: &mut OpState, #[string] payload: String) {
    if let Some(request_id) = wait_until_request_id(&payload) {
        clear_actor_request_scope(state, &request_id);
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
        op_crypto_random_uuid,
        op_crypto_get_random_values,
        op_crypto_digest,
        op_crypto_hmac_sign,
        op_crypto_hmac_verify,
        op_crypto_aes_gcm_encrypt,
        op_crypto_aes_gcm_decrypt,
        op_kv_get,
        op_kv_get_value,
        op_kv_set,
        op_kv_set_value,
        op_kv_delete,
        op_kv_list,
        op_cache_match,
        op_cache_put,
        op_cache_delete,
        op_request_body_read,
        op_request_body_cancel,
        op_actor_invoke_fetch,
        op_actor_invoke_method,
        op_actor_state_get_value,
        op_actor_state_set,
        op_actor_state_set_value,
        op_actor_state_delete,
        op_actor_state_list,
        op_actor_socket_send,
        op_actor_socket_close,
        op_actor_socket_list,
        op_actor_socket_consume_close,
        op_emit_completion,
        op_emit_wait_until_done,
        op_emit_response_start,
        op_emit_response_chunk,
        op_emit_cache_revalidate
    ]
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
            let mut mac = Hmac::<Sha1>::new_from_slice(key)
                .map_err(|error| format!("hmac init failed: {error}"))?;
            mac.update(data);
            Ok(mac.finalize().into_bytes().to_vec())
        }
        CryptoDigestAlgorithm::Sha256 => {
            let mut mac = Hmac::<Sha256>::new_from_slice(key)
                .map_err(|error| format!("hmac init failed: {error}"))?;
            mac.update(data);
            Ok(mac.finalize().into_bytes().to_vec())
        }
        CryptoDigestAlgorithm::Sha384 => {
            let mut mac = Hmac::<Sha384>::new_from_slice(key)
                .map_err(|error| format!("hmac init failed: {error}"))?;
            mac.update(data);
            Ok(mac.finalize().into_bytes().to_vec())
        }
        CryptoDigestAlgorithm::Sha512 => {
            let mut mac = Hmac::<Sha512>::new_from_slice(key)
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
            let mut mac = Hmac::<Sha1>::new_from_slice(key)
                .map_err(|error| format!("hmac init failed: {error}"))?;
            mac.update(data);
            Ok(mac.verify_slice(signature).is_ok())
        }
        CryptoDigestAlgorithm::Sha256 => {
            let mut mac = Hmac::<Sha256>::new_from_slice(key)
                .map_err(|error| format!("hmac init failed: {error}"))?;
            mac.update(data);
            Ok(mac.verify_slice(signature).is_ok())
        }
        CryptoDigestAlgorithm::Sha384 => {
            let mut mac = Hmac::<Sha384>::new_from_slice(key)
                .map_err(|error| format!("hmac init failed: {error}"))?;
            mac.update(data);
            Ok(mac.verify_slice(signature).is_ok())
        }
        CryptoDigestAlgorithm::Sha512 => {
            let mut mac = Hmac::<Sha512>::new_from_slice(key)
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
        24 => {
            let cipher = Aes192Gcm::new_from_slice(&payload.key)
                .map_err(|error| format!("AES-192-GCM key init failed: {error}"))?;
            cipher
                .encrypt(
                    nonce,
                    Payload {
                        msg: &payload.data,
                        aad: &payload.additional_data,
                    },
                )
                .map_err(|error| format!("AES-192-GCM encrypt failed: {error}"))
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
        _ => Err("AES-GCM key length must be 16, 24, or 32 bytes".to_string()),
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
        24 => {
            let cipher = Aes192Gcm::new_from_slice(&payload.key)
                .map_err(|error| format!("AES-192-GCM key init failed: {error}"))?;
            cipher
                .decrypt(
                    nonce,
                    Payload {
                        msg: &payload.data,
                        aad: &payload.additional_data,
                    },
                )
                .map_err(|error| format!("AES-192-GCM decrypt failed: {error}"))
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
        _ => Err("AES-GCM key length must be 16, 24, or 32 bytes".to_string()),
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
