use crate::actor::{ActorStateEntry, ActorStore};
use crate::actor_rpc::{
    decode_actor_invoke_response, encode_actor_invoke_request, ActorInvokeRequest,
};
use crate::cache::{CacheLookup, CacheRequest, CacheResponse, CacheStore};
use crate::kv::{KvEntry, KvStore};
use common::Result;
use deno_core::OpState;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, oneshot, Mutex, Notify};

#[derive(Clone)]
pub struct IsolateEventSender(pub tokio::sync::mpsc::UnboundedSender<IsolateEventPayload>);

pub enum IsolateEventPayload {
    Completion(String),
    WaitUntilDone(String),
    ResponseStart(String),
    ResponseChunk(String),
    CacheRevalidate(String),
    ActorInvoke(ActorInvokeEvent),
}

pub type RequestBodyChunk = std::result::Result<Vec<u8>, String>;
pub type RequestBodyReceiver = mpsc::Receiver<RequestBodyChunk>;

pub struct ActorInvokeEvent {
    pub request_frame: Vec<u8>,
    pub reply: oneshot::Sender<Result<Vec<u8>>>,
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
struct CompletionRequestId {
    request_id: String,
}

#[derive(Debug, Deserialize)]
struct ActorInvokePayload {
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
struct ActorInvokeResult {
    ok: bool,
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
    error: String,
}

#[derive(Debug, Serialize)]
struct ActorStateGetResult {
    ok: bool,
    found: bool,
    value: String,
    version: i64,
    error: String,
}

#[derive(Debug, Deserialize)]
struct ActorStateGetValuePayload {
    namespace: String,
    actor_key: String,
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
    namespace: String,
    actor_key: String,
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

#[derive(Debug, Serialize)]
struct ActorStateListResult {
    ok: bool,
    entries: Vec<ActorStateListItem>,
    error: String,
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
async fn op_actor_invoke(
    state: Rc<RefCell<OpState>>,
    #[string] payload: String,
) -> ActorInvokeResult {
    let payload = match crate::json::from_string::<ActorInvokePayload>(payload) {
        Ok(payload) => payload,
        Err(error) => {
            return ActorInvokeResult {
                ok: false,
                status: 500,
                headers: Vec::new(),
                body: Vec::new(),
                error: format!("invalid actor invoke payload: {error}"),
            };
        }
    };
    if payload.worker_name.trim().is_empty() {
        return ActorInvokeResult {
            ok: false,
            status: 500,
            headers: Vec::new(),
            body: Vec::new(),
            error: "actor invoke requires worker_name".to_string(),
        };
    }
    if payload.binding.trim().is_empty() {
        return ActorInvokeResult {
            ok: false,
            status: 500,
            headers: Vec::new(),
            body: Vec::new(),
            error: "actor invoke requires binding".to_string(),
        };
    }
    if payload.key.trim().is_empty() {
        return ActorInvokeResult {
            ok: false,
            status: 500,
            headers: Vec::new(),
            body: Vec::new(),
            error: "actor invoke requires key".to_string(),
        };
    }
    if payload.request_id.trim().is_empty() {
        return ActorInvokeResult {
            ok: false,
            status: 500,
            headers: Vec::new(),
            body: Vec::new(),
            error: "actor invoke requires request_id".to_string(),
        };
    }

    let request_frame = match encode_actor_invoke_request(&ActorInvokeRequest {
        worker_name: payload.worker_name,
        binding: payload.binding,
        key: payload.key,
        request: common::WorkerInvocation {
            method: payload.method,
            url: payload.url,
            headers: payload.headers,
            body: payload.body,
            request_id: payload.request_id,
        },
    }) {
        Ok(frame) => frame,
        Err(error) => {
            return ActorInvokeResult {
                ok: false,
                status: 500,
                headers: Vec::new(),
                body: Vec::new(),
                error: format!("actor invoke encode failed: {error}"),
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
        return ActorInvokeResult {
            ok: false,
            status: 500,
            headers: Vec::new(),
            body: Vec::new(),
            error: "actor runtime is unavailable".to_string(),
        };
    }

    match reply_rx.await {
        Ok(Ok(frame)) => match decode_actor_invoke_response(&frame) {
            Ok(output) => ActorInvokeResult {
                ok: true,
                status: output.status,
                headers: output.headers,
                body: output.body,
                error: String::new(),
            },
            Err(error) => ActorInvokeResult {
                ok: false,
                status: 500,
                headers: Vec::new(),
                body: Vec::new(),
                error: format!("actor invoke decode failed: {error}"),
            },
        },
        Ok(Err(error)) => ActorInvokeResult {
            ok: false,
            status: 500,
            headers: Vec::new(),
            body: Vec::new(),
            error: error.to_string(),
        },
        Err(_) => ActorInvokeResult {
            ok: false,
            status: 500,
            headers: Vec::new(),
            body: Vec::new(),
            error: "actor invoke response channel closed".to_string(),
        },
    }
}

#[deno_core::op2]
#[serde]
async fn op_actor_state_get(
    state: Rc<RefCell<OpState>>,
    #[string] namespace: String,
    #[string] actor_key: String,
    #[string] key: String,
) -> ActorStateGetResult {
    let store = state.borrow().borrow::<ActorStore>().clone();
    match store.get(&namespace, &actor_key, &key).await {
        Ok(Some(value)) => {
            if value.encoding != "utf8" {
                return ActorStateGetResult {
                    ok: false,
                    found: true,
                    value: String::new(),
                    version: value.version,
                    error: "actor storage value is encoded as v8sc; use actor.storage.getValue()"
                        .to_string(),
                };
            }
            match String::from_utf8(value.value) {
                Ok(decoded) => ActorStateGetResult {
                    ok: true,
                    found: true,
                    value: decoded,
                    version: value.version,
                    error: String::new(),
                },
                Err(error) => ActorStateGetResult {
                    ok: false,
                    found: true,
                    value: String::new(),
                    version: value.version,
                    error: format!("actor storage utf8 decode failed: {error}"),
                },
            }
        }
        Ok(None) => ActorStateGetResult {
            ok: true,
            found: false,
            value: String::new(),
            version: -1,
            error: String::new(),
        },
        Err(error) => ActorStateGetResult {
            ok: false,
            found: false,
            value: String::new(),
            version: -1,
            error: error.to_string(),
        },
    }
}

#[deno_core::op2]
#[serde]
async fn op_actor_state_get_value(
    state: Rc<RefCell<OpState>>,
    #[string] payload: String,
) -> ActorStateGetValueResult {
    let payload: ActorStateGetValuePayload = match crate::json::from_string(payload) {
        Ok(value) => value,
        Err(error) => {
            return ActorStateGetValueResult {
                ok: false,
                found: false,
                value: Vec::new(),
                encoding: "utf8".to_string(),
                version: -1,
                error: format!("invalid actor state get value payload: {error}"),
            };
        }
    };

    let store = state.borrow().borrow::<ActorStore>().clone();
    match store
        .get(&payload.namespace, &payload.actor_key, &payload.key)
        .await
    {
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
    #[string] namespace: String,
    #[string] actor_key: String,
    #[string] key: String,
    #[string] value: String,
    #[bigint] expected_version: i64,
) -> ActorStateWriteResult {
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
    #[string] payload: String,
) -> ActorStateWriteResult {
    let payload: ActorStateSetValuePayload = match crate::json::from_string(payload) {
        Ok(value) => value,
        Err(error) => {
            return ActorStateWriteResult {
                ok: false,
                conflict: false,
                version: -1,
                error: format!("invalid actor state set value payload: {error}"),
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
            &payload.namespace,
            &payload.actor_key,
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
    #[string] namespace: String,
    #[string] actor_key: String,
    #[string] key: String,
    #[bigint] expected_version: i64,
) -> ActorStateWriteResult {
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
    #[string] namespace: String,
    #[string] actor_key: String,
    #[string] prefix: String,
    limit: u32,
) -> ActorStateListResult {
    let store = state.borrow().borrow::<ActorStore>().clone();
    let clamped_limit = limit.clamp(1, 1000) as usize;
    match store
        .list(&namespace, &actor_key, &prefix, clamped_limit)
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

#[deno_core::op2(fast)]
fn op_emit_completion(state: &mut OpState, #[string] payload: String) {
    if let Some(request_id) = completion_request_id(&payload) {
        clear_request_body_stream(state, &request_id);
    }
    let sender = state.borrow::<IsolateEventSender>().clone();
    let _ = sender.0.send(IsolateEventPayload::Completion(payload));
}

#[deno_core::op2(fast)]
fn op_emit_wait_until_done(state: &mut OpState, #[string] payload: String) {
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
        op_actor_invoke,
        op_actor_state_get,
        op_actor_state_get_value,
        op_actor_state_set,
        op_actor_state_set_value,
        op_actor_state_delete,
        op_actor_state_list,
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

fn completion_request_id(payload: &str) -> Option<String> {
    let mut bytes = payload.as_bytes().to_vec();
    simd_json::serde::from_slice::<CompletionRequestId>(&mut bytes)
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
