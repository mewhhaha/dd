//! Async host operations surfaced to the guest as promises: `fetch(url)` and
//! `dd_sleep(ms)`. The HTTP/sleep work runs on the io runtime; a completion
//! message resolves the promise from the engine's event loop, so `.then`
//! chains and promise-returning fetch handlers work even though Perry cannot
//! compile `new Promise(executor)`.

use super::{HostResult, arg, fail, string_arg};
use crate::heap::{HostValue, ObjectData, PromiseState};
use crate::nanbox::{JsValue, TAG_NULL, TAG_UNDEFINED, decode, encode, encode_number};
use crate::state::{HostCompletion, HostOpOutcome, HostState};
use std::time::Duration;
use wasmtime::Caller;

const FETCH_BODY_LIMIT: usize = 16 * 1024 * 1024;
const SLEEP_LIMIT_MS: f64 = 60_000.0;

/// Allocate a pending promise and register one in-flight host operation.
fn pending_host_promise(state: &mut HostState) -> (u32, std::sync::mpsc::Sender<HostCompletion>) {
    let promise = state.heap.alloc(HostValue::Promise {
        state: PromiseState::Pending,
        reactions: Vec::new(),
    });
    state.host_ops.pending += 1;
    (promise, state.host_ops.sender.clone())
}

/// `dd_sleep(ms)`: a promise that resolves with `undefined` after `ms`.
pub(super) fn sleep(caller: &mut Caller<'_, HostState>, args: &[u64]) -> HostResult {
    let ms = caller.data().heap.to_number(arg(args, 0));
    if !ms.is_finite() || !(0.0..=SLEEP_LIMIT_MS).contains(&ms) {
        return Err(fail(
            "dd_sleep",
            format!("delay must be between 0 and {SLEEP_LIMIT_MS} ms, got {ms}"),
        ));
    }
    let (promise, sender) = pending_host_promise(caller.data_mut());
    crate::host_api::io_spawn(async move {
        tokio::time::sleep(Duration::from_millis(ms as u64)).await;
        let _ = sender.send(HostCompletion {
            promise,
            outcome: Ok(HostOpOutcome::Sleep),
        });
    });
    Ok(encode(JsValue::Handle(promise)))
}

/// `fetch_url(url)` / `fetch_with_options(url, method, body, headers)`:
/// resolves to a Response object once the request completes.
pub(super) fn fetch_async(
    caller: &mut Caller<'_, HostState>,
    name: &str,
    args: &[u64],
) -> HostResult {
    let state = caller.data();
    let url = string_arg(state, arg(args, 0));
    let method = match decode(arg(args, 1)) {
        JsValue::Undefined | JsValue::Null => "GET".to_string(),
        _ => string_arg(state, arg(args, 1)),
    };
    let body = match decode(arg(args, 2)) {
        JsValue::Undefined | JsValue::Null => Vec::new(),
        _ => string_arg(state, arg(args, 2)).into_bytes(),
    };
    let headers: Vec<(String, String)> = match decode(arg(args, 3)) {
        JsValue::Handle(id) => match state.heap.handle(id) {
            Some(HostValue::Object(options)) => options
                .props
                .iter()
                .filter(|(key, _)| key != "__class__")
                .map(|(key, value)| (key.clone(), state.heap.display(*value)))
                .collect(),
            _ => Vec::new(),
        },
        _ => Vec::new(),
    };
    if url.is_empty() {
        return Err(fail(name, "url must be a non-empty string"));
    }

    let client = caller.data().context.http.clone();
    let (promise, sender) = pending_host_promise(caller.data_mut());
    crate::host_api::io_spawn(async move {
        let outcome = run_fetch(client, method, url, headers, body).await;
        let _ = sender.send(HostCompletion { promise, outcome });
    });
    Ok(encode(JsValue::Handle(promise)))
}

async fn run_fetch(
    client: reqwest::Client,
    method: String,
    url: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
) -> Result<HostOpOutcome, String> {
    let method = reqwest::Method::from_bytes(method.as_bytes())
        .map_err(|e| format!("invalid method {method:?}: {e}"))?;
    let mut request = client.request(method, &url);
    for (key, value) in headers {
        request = request.header(key, value);
    }
    if !body.is_empty() {
        request = request.body(body);
    }
    let response = request
        .send()
        .await
        .map_err(|e| format!("fetch {url:?} failed: {e}"))?;
    let status = response.status().as_u16();
    let final_url = response.url().to_string();
    let headers: Vec<(String, String)> = response
        .headers()
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
        .collect();
    let bytes = response
        .bytes()
        .await
        .map_err(|e| format!("fetch {url:?} body read failed: {e}"))?;
    if bytes.len() > FETCH_BODY_LIMIT {
        return Err(format!(
            "fetch {url:?} response body of {} bytes exceeds the 16 MiB cap",
            bytes.len()
        ));
    }
    Ok(HostOpOutcome::Fetch {
        status,
        url: final_url,
        headers,
        body: bytes.to_vec(),
    })
}

/// Materialize a completed host operation into a heap value the promise
/// resolves with. Fetch results become Response objects (class "Response")
/// whose fields the `response_*` bridge functions read.
pub(crate) fn outcome_to_heap(state: &mut HostState, outcome: HostOpOutcome) -> u64 {
    match outcome {
        HostOpOutcome::Sleep => TAG_UNDEFINED,
        HostOpOutcome::Fetch {
            status,
            url,
            headers,
            body,
        } => {
            let headers_props: Vec<(String, u64)> = headers
                .into_iter()
                .map(|(key, value)| {
                    let bits = state.heap.intern_bits(value);
                    (key.to_ascii_lowercase(), bits)
                })
                .collect();
            let headers_bits = state.heap.alloc_bits(HostValue::Object(ObjectData {
                class: None,
                props: headers_props,
            }));
            let url_bits = state.heap.intern_bits(url);
            let body_bits = state.heap.alloc_bits(HostValue::Buffer(body));
            state.heap.alloc_bits(HostValue::Object(ObjectData {
                class: Some("Response".to_string()),
                props: vec![
                    ("status".to_string(), encode_number(f64::from(status))),
                    ("url".to_string(), url_bits),
                    ("headers".to_string(), headers_bits),
                    ("__body".to_string(), body_bits),
                ],
            }))
        }
    }
}

fn response_field(state: &HostState, bits: u64, field: &str) -> Option<u64> {
    let JsValue::Handle(id) = decode(bits) else {
        return None;
    };
    match state.heap.handle(id) {
        Some(HostValue::Object(object)) => object.get(field),
        _ => None,
    }
}

fn response_body_text(state: &HostState, bits: u64) -> Option<String> {
    let body_bits = response_field(state, bits, "__body")?;
    let JsValue::Handle(id) = decode(body_bits) else {
        return None;
    };
    match state.heap.handle(id) {
        Some(HostValue::Buffer(bytes)) => Some(String::from_utf8_lossy(bytes).into_owned()),
        _ => None,
    }
}

fn resolved_promise(state: &mut HostState, value: u64) -> u64 {
    state.heap.alloc_bits(HostValue::Promise {
        state: PromiseState::Resolved(value),
        reactions: Vec::new(),
    })
}

/// Bare method calls on a Response object (`r.text()` where `r` is typed
/// `any`): map onto the `response_*` family. Returns `None` for methods that
/// are not part of the Response surface.
pub(super) fn response_method(
    caller: &mut Caller<'_, HostState>,
    method: &str,
    receiver: u64,
    args: &[u64],
) -> Option<HostResult> {
    let bridge_name = match method {
        "text" => "response_text",
        "json" => "response_json",
        _ => return None,
    };
    let mut full = vec![receiver];
    full.extend_from_slice(args);
    Some(response_op(caller, bridge_name, &full))
}

/// The `response_*` bridge family. `text()`/`json()` return already-resolved
/// promises, matching the reference glue.
pub(super) fn response_op(
    caller: &mut Caller<'_, HostState>,
    name: &str,
    args: &[u64],
) -> HostResult {
    let handle = arg(args, 0);
    match name {
        "response_status" => {
            Ok(response_field(caller.data(), handle, "status").unwrap_or(TAG_UNDEFINED))
        }
        "response_url" => Ok(response_field(caller.data(), handle, "url").unwrap_or(TAG_UNDEFINED)),
        "response_ok" => {
            let status = response_field(caller.data(), handle, "status")
                .map(|bits| caller.data().heap.to_number(bits))
                .unwrap_or(0.0);
            Ok(crate::nanbox::encode_bool((200.0..300.0).contains(&status)))
        }
        "response_headers_get" => {
            let wanted = string_arg(caller.data(), arg(args, 1)).to_ascii_lowercase();
            let Some(headers_bits) = response_field(caller.data(), handle, "headers") else {
                return Ok(TAG_NULL);
            };
            let JsValue::Handle(id) = decode(headers_bits) else {
                return Ok(TAG_NULL);
            };
            match caller.data().heap.handle(id) {
                Some(HostValue::Object(object)) => Ok(object.get(&wanted).unwrap_or(TAG_NULL)),
                _ => Ok(TAG_NULL),
            }
        }
        "response_text" => {
            let text = response_body_text(caller.data(), handle)
                .ok_or_else(|| fail(name, "receiver is not a fetch Response"))?;
            let bits = caller.data_mut().heap.intern_bits(text);
            Ok(resolved_promise(caller.data_mut(), bits))
        }
        "response_json" => {
            let text = response_body_text(caller.data(), handle)
                .ok_or_else(|| fail(name, "receiver is not a fetch Response"))?;
            match serde_json::from_str::<serde_json::Value>(&text) {
                Ok(value) => {
                    let bits = super::values::json_to_heap(caller.data_mut(), &value);
                    Ok(resolved_promise(caller.data_mut(), bits))
                }
                Err(error) => Err(fail(name, format!("response body is not JSON: {error}"))),
            }
        }
        other => Err(fail(other, "unsupported response operation")),
    }
}
