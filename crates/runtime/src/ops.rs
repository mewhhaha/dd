use crate::cache::{CacheLookup, CacheRequest, CacheResponse, CacheStore};
use crate::kv::{KvEntry, KvStore};
use deno_core::OpState;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::OnceLock;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Clone)]
pub struct IsolateEventSender(pub tokio::sync::mpsc::UnboundedSender<IsolateEventPayload>);

#[derive(Debug)]
pub enum IsolateEventPayload {
    Completion(String),
    WaitUntilDone(String),
    ResponseStart(String),
    ResponseChunk(String),
    CacheRevalidate(String),
}

#[derive(Debug, Serialize)]
struct TimeBoundary {
    now_ms: u64,
    perf_ms: f64,
}

#[derive(Debug, Serialize)]
struct KvListItem {
    key: String,
    value: String,
}

#[derive(Debug, Serialize)]
struct KvGetResult {
    ok: bool,
    found: bool,
    value: String,
    error: String,
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
        Ok(Some(value)) => KvGetResult {
            ok: true,
            found: true,
            value,
            error: String::new(),
        },
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

#[deno_core::op2(fast)]
fn op_emit_completion(state: &mut OpState, #[string] payload: String) {
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
    grugd_runtime_ops,
    ops = [
        op_sleep,
        op_time_boundary_now,
        op_kv_get,
        op_kv_set,
        op_kv_delete,
        op_kv_list,
        op_cache_match,
        op_cache_put,
        op_cache_delete,
        op_emit_completion,
        op_emit_wait_until_done,
        op_emit_response_start,
        op_emit_response_chunk,
        op_emit_cache_revalidate
    ]
);

pub fn runtime_extension() -> deno_core::Extension {
    grugd_runtime_ops::init()
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
    }
}

fn decode_cache_request_payload(payload: String) -> common::Result<CacheRequest> {
    let payload: CacheRequestPayload = crate::json::from_str(&payload).map_err(|error| {
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
    let payload: CachePutPayload = crate::json::from_str(&payload).map_err(|error| {
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
