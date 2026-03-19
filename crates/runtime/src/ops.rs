use crate::kv::{KvEntry, KvStore};
use deno_core::OpState;
use serde::Serialize;
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

deno_core::extension!(
    grugd_runtime_ops,
    ops = [
        op_sleep,
        op_time_boundary_now,
        op_kv_get,
        op_kv_set,
        op_kv_delete,
        op_kv_list,
        op_emit_completion,
        op_emit_wait_until_done,
        op_emit_response_start,
        op_emit_response_chunk
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
