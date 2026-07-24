//! dd's `ffi` host surface for Perry wasm workers.
//!
//! Every function here is reachable from worker TypeScript through a plain
//! `declare function` (which Perry compiles to an `ffi.*` import). All calls
//! are synchronous from the guest's point of view; storage and network work
//! runs on a dedicated tokio runtime the invoke thread blocks on.

use crate::bridge::{call_closure, json_stringify_bits};
use crate::heap::{HostValue, ObjectData};
use crate::nanbox::{JsValue, TAG_NULL, TAG_UNDEFINED, decode, encode_number};
use crate::state::{ActiveAtomic, HostState};
use common::WorkerInvocation;
use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};
use storage::cache::{CacheLookup, CacheRequest, CacheResponse};
use storage::kv::KvUtf8Lookup;
use storage::memory::MemoryBatchMutation;
use wasmtime::Caller;

type HostResult = Result<u64, wasmtime::Error>;
/// (status, headers, body) of a decoded guest response.
type HttpPayload = (u16, Vec<(String, String)>, Vec<u8>);
/// (method, url, headers, body) for an outbound fetch.
type OutboundRequest = (String, String, Vec<(String, String)>, Vec<u8>);

const SERVICE_DEPTH_LIMIT: u32 = 8;
const FETCH_BODY_LIMIT: usize = 16 * 1024 * 1024;
const ATOMIC_LOCK_TIMEOUT: Duration = Duration::from_secs(5);
const MEMORY_ENCODING_UTF8: &str = "utf8";

pub const FFI_FUNCTIONS: &[&str] = &[
    "dd_register",
    "dd_header",
    "dd_json",
    "dd_kv_get",
    "dd_kv_set",
    "dd_kv_delete",
    "dd_kv_list",
    "dd_cache_match",
    "dd_cache_put",
    "dd_cache_delete",
    "dd_memory_atomic",
    "dd_tvar_read",
    "dd_tvar_write",
    "dd_fetch",
    "dd_service_fetch",
    "dd_ws_register",
    "dd_ws_send",
    "dd_ws_close",
];

fn fail(name: &str, detail: impl std::fmt::Display) -> wasmtime::Error {
    wasmtime::Error::msg(format!("{name}: {detail}"))
}

/// Dedicated runtime for storage and outbound HTTP. Invoke threads are plain
/// blocking threads, so `block_on` here never runs inside another runtime.
fn io_runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .thread_name("dd-wasm-io")
            .build()
            .expect("io runtime construction cannot fail")
    })
}

pub fn block_on<F: Future>(future: F) -> F::Output {
    io_runtime().block_on(future)
}

fn arg(args: &[u64], index: usize) -> u64 {
    args.get(index).copied().unwrap_or(TAG_UNDEFINED)
}

fn string_arg(state: &HostState, args: &[u64], index: usize) -> String {
    state.heap.display(arg(args, index))
}

/// Serialize per-(namespace, key) execution of `dd_memory_atomic`, the
/// single-writer guarantee the memory model is built on.
type AtomicLockTable = Mutex<HashMap<(String, String), Arc<Mutex<()>>>>;

fn atomic_key_lock(namespace: &str, memory_key: &str) -> Arc<Mutex<()>> {
    static LOCKS: OnceLock<AtomicLockTable> = OnceLock::new();
    let mut table = LOCKS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .expect("atomic lock table is never poisoned");
    Arc::clone(
        table
            .entry((namespace.to_string(), memory_key.to_string()))
            .or_default(),
    )
}

pub fn dispatch_ffi(caller: &mut Caller<'_, HostState>, name: &str, args: &[u64]) -> HostResult {
    match name {
        "dd_register" => {
            caller.data_mut().registered_handler = Some(arg(args, 0));
            Ok(TAG_UNDEFINED)
        }
        "dd_header" => {
            let wanted = string_arg(caller.data(), args, 0).to_ascii_lowercase();
            let found = caller.data().current_request.as_ref().and_then(|request| {
                request
                    .headers
                    .iter()
                    .find(|(key, _)| key.to_ascii_lowercase() == wanted)
                    .map(|(_, value)| value.clone())
            });
            match found {
                Some(value) => Ok(caller.data_mut().heap.intern_bits(value)),
                None => Ok(TAG_NULL),
            }
        }
        "dd_json" => match json_stringify_bits(caller.data(), arg(args, 0))? {
            Some(text) => Ok(caller.data_mut().heap.intern_bits(text)),
            None => Ok(TAG_UNDEFINED),
        },

        // ===== KV =====
        "dd_kv_get" => {
            let (kv, worker) = kv_store(caller, name)?;
            let binding = string_arg(caller.data(), args, 0);
            let key = string_arg(caller.data(), args, 1);
            let lookup =
                block_on(kv.get_utf8(&worker, &binding, &key)).map_err(|e| fail(name, e))?;
            match lookup {
                Ok(value) => Ok(caller.data_mut().heap.intern_bits(value)),
                Err(KvUtf8Lookup::Missing) => Ok(TAG_NULL),
                Err(KvUtf8Lookup::WrongEncoding) => Err(fail(
                    name,
                    format!(
                        "key {key:?} in binding {binding:?} holds a non-utf8 value \
                         (written by the V8 runtime?)"
                    ),
                )),
            }
        }
        "dd_kv_set" => {
            let (kv, worker) = kv_store(caller, name)?;
            let binding = string_arg(caller.data(), args, 0);
            let key = string_arg(caller.data(), args, 1);
            let value = string_arg(caller.data(), args, 2);
            block_on(kv.put(&worker, &binding, &key, &value)).map_err(|e| fail(name, e))?;
            Ok(TAG_UNDEFINED)
        }
        "dd_kv_delete" => {
            let (kv, worker) = kv_store(caller, name)?;
            let binding = string_arg(caller.data(), args, 0);
            let key = string_arg(caller.data(), args, 1);
            block_on(kv.delete(&worker, &binding, &key)).map_err(|e| fail(name, e))?;
            Ok(TAG_UNDEFINED)
        }
        "dd_kv_list" => {
            let (kv, worker) = kv_store(caller, name)?;
            let binding = string_arg(caller.data(), args, 0);
            let prefix = string_arg(caller.data(), args, 1);
            let entries =
                block_on(kv.list(&worker, &binding, &prefix, 1000)).map_err(|e| fail(name, e))?;
            let keys: Vec<u64> = entries
                .into_iter()
                .map(|entry| caller.data_mut().heap.intern_bits(entry.key))
                .collect();
            Ok(caller.data_mut().heap.alloc_bits(HostValue::Array(keys)))
        }

        // ===== Cache =====
        "dd_cache_match" => {
            let cache = cache_store(caller, name)?;
            let request = cache_request(caller, args);
            let lookup = block_on(cache.get(&request)).map_err(|e| fail(name, e))?;
            match lookup {
                CacheLookup::Fresh(response)
                | CacheLookup::StaleWhileRevalidate(response)
                | CacheLookup::StaleIfError(response) => Ok(response_object(
                    caller.data_mut(),
                    response.status,
                    response.headers,
                    response.body.to_vec(),
                )),
                CacheLookup::Miss => Ok(TAG_NULL),
            }
        }
        "dd_cache_put" => {
            let cache = cache_store(caller, name)?;
            let request = cache_request(caller, args);
            let response = decode_guest_response(caller.data(), arg(args, 1))
                .map_err(|detail| fail(name, detail))?;
            block_on(cache.put(
                &request,
                CacheResponse {
                    status: response.0,
                    headers: response.1,
                    body: response.2.into(),
                },
            ))
            .map_err(|e| fail(name, e))?;
            Ok(TAG_UNDEFINED)
        }
        "dd_cache_delete" => {
            let cache = cache_store(caller, name)?;
            let request = cache_request(caller, args);
            let deleted = block_on(cache.delete(&request)).map_err(|e| fail(name, e))?;
            Ok(crate::nanbox::encode_bool(deleted))
        }

        // ===== Memory namespaces =====
        "dd_memory_atomic" => memory_atomic(caller, args),
        "dd_tvar_read" => {
            let tvar = string_arg(caller.data(), args, 0);
            let Some(active) = caller.data().active_atomic.as_ref() else {
                return Err(fail(name, "only valid inside dd_memory_atomic"));
            };
            match active.tvars.get(&tvar).cloned() {
                Some(value) => Ok(crate::bridge::json_value_to_heap(caller.data_mut(), &value)),
                None => Ok(TAG_UNDEFINED),
            }
        }
        "dd_tvar_write" => {
            let tvar = string_arg(caller.data(), args, 0);
            let value = crate::bridge::heap_to_json_value(caller.data(), arg(args, 1))?
                .unwrap_or(serde_json::Value::Null);
            let Some(active) = caller.data_mut().active_atomic.as_mut() else {
                return Err(fail(name, "only valid inside dd_memory_atomic"));
            };
            active.tvars.insert(tvar.clone(), value);
            active.dirty.insert(tvar);
            Ok(TAG_UNDEFINED)
        }

        // ===== Outbound fetch =====
        "dd_fetch" => {
            let (method, url, headers, body) = fetch_args(caller.data(), args)?;
            let client = caller.data().context.http.clone();
            let (status, response_headers, response_body) = block_on(async move {
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
                Ok::<_, String>((status, headers, bytes.to_vec()))
            })
            .map_err(|detail| fail(name, detail))?;
            Ok(response_object(
                caller.data_mut(),
                status,
                response_headers,
                response_body,
            ))
        }

        // ===== Websockets =====
        "dd_ws_register" => {
            caller.data_mut().ws_handlers = Some(arg(args, 0));
            Ok(TAG_UNDEFINED)
        }
        "dd_ws_send" => {
            let connection = caller.data().heap.to_number(arg(args, 0)) as u64;
            let text = string_arg(caller.data(), args, 1);
            let delivered = caller
                .data()
                .context
                .ws_connections
                .send(connection, crate::ws::WsOutbound::Text(text));
            Ok(crate::nanbox::encode_bool(delivered))
        }
        "dd_ws_close" => {
            let connection = caller.data().heap.to_number(arg(args, 0)) as u64;
            caller
                .data()
                .context
                .ws_connections
                .send(connection, crate::ws::WsOutbound::Close);
            Ok(TAG_UNDEFINED)
        }

        // ===== Service bindings =====
        "dd_service_fetch" => {
            let depth = caller.data().service_depth;
            if depth >= SERVICE_DEPTH_LIMIT {
                return Err(fail(
                    name,
                    format!("service call chain exceeds depth {SERVICE_DEPTH_LIMIT}"),
                ));
            }
            let binding = string_arg(caller.data(), args, 0);
            // Bindings map to worker names; an unmapped binding falls back to
            // being a worker name itself (single-server ergonomics).
            let target_name = caller
                .data()
                .context
                .service_bindings
                .get(&binding)
                .cloned()
                .unwrap_or_else(|| binding.clone());
            let target = {
                let workers = caller
                    .data()
                    .context
                    .workers
                    .read()
                    .expect("worker registry is never poisoned");
                workers.get(&target_name).cloned()
            };
            let Some(target) = target else {
                return Err(fail(
                    name,
                    format!("service binding {binding:?} points at unknown worker {target_name:?}"),
                ));
            };
            let invocation = WorkerInvocation {
                method: string_arg(caller.data(), args, 1),
                url: string_arg(caller.data(), args, 2),
                headers: Vec::new(),
                body: string_arg(caller.data(), args, 3).into_bytes(),
                request_id: format!("service-{depth}"),
            };
            let output = target
                .invoke_at_depth(
                    invocation,
                    crate::engine::InvokeOptions::default(),
                    depth + 1,
                )
                .map_err(|e| fail(name, e))?;
            Ok(response_object(
                caller.data_mut(),
                output.status,
                output.headers,
                output.body,
            ))
        }

        other => Err(fail(
            other,
            format!(
                "not a dd host function; available: {}",
                FFI_FUNCTIONS.join(", ")
            ),
        )),
    }
}

fn kv_store(
    caller: &Caller<'_, HostState>,
    name: &str,
) -> Result<(storage::kv::KvStore, String), wasmtime::Error> {
    let context = &caller.data().context;
    match &context.stores {
        Some(stores) => Ok((stores.kv.clone(), context.worker_name.clone())),
        None => Err(fail(
            name,
            "no store attached to this worker (start the server with --store-dir)",
        )),
    }
}

fn cache_store(
    caller: &Caller<'_, HostState>,
    name: &str,
) -> Result<storage::cache::CacheStore, wasmtime::Error> {
    match &caller.data().context.stores {
        Some(stores) => Ok(stores.cache.clone()),
        None => Err(fail(
            name,
            "no store attached to this worker (start the server with --store-dir)",
        )),
    }
}

/// Cache keys are URLs; the cache namespace is scoped per worker the same way
/// the V8 runtime isolates cache namespaces.
fn cache_request(caller: &Caller<'_, HostState>, args: &[u64]) -> CacheRequest {
    let state = caller.data();
    CacheRequest {
        cache_name: format!("{}:default", state.context.worker_name),
        method: "GET".to_string(),
        url: string_arg(state, args, 0),
        headers: Vec::new(),
        bypass_stale: false,
    }
}

/// Read a guest response object `{status?, headers?, body?}`.
fn decode_guest_response(state: &HostState, bits: u64) -> Result<HttpPayload, String> {
    let JsValue::Handle(id) = decode(bits) else {
        return Err(format!(
            "expected a response object, got {:?}",
            decode(bits)
        ));
    };
    let Some(HostValue::Object(object)) = state.heap.handle(id) else {
        return Err("expected a response object handle".to_string());
    };
    let status = object
        .get("status")
        .map(|bits| state.heap.to_number(bits))
        .filter(|n| n.is_finite() && *n >= 100.0 && *n <= 599.0)
        .map(|n| n as u16)
        .unwrap_or(200);
    let headers = match object.get("headers").map(decode) {
        Some(JsValue::Handle(headers_id)) => match state.heap.handle(headers_id) {
            Some(HostValue::Object(header_object)) => header_object
                .props
                .iter()
                .filter(|(key, _)| key != "__class__")
                .map(|(key, value)| (key.clone(), state.heap.display(*value)))
                .collect(),
            _ => Vec::new(),
        },
        _ => Vec::new(),
    };
    let body = object
        .get("body")
        .map(|bits| match decode(bits) {
            JsValue::Undefined | JsValue::Null => Vec::new(),
            _ => state.heap.display(bits).into_bytes(),
        })
        .unwrap_or_default();
    Ok((status, headers, body))
}

/// Build the `{status, headers, body}` object handed back to the guest. The
/// guest must access it dynamically (`any`), not through a typed shape —
/// Perry's shape-field lowering breaks on host-created objects.
fn response_object(
    state: &mut HostState,
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
) -> u64 {
    let headers_props: Vec<(String, u64)> = headers
        .into_iter()
        .map(|(key, value)| {
            let bits = state.heap.intern_bits(value);
            (key, bits)
        })
        .collect();
    let headers_bits = state.heap.alloc_bits(HostValue::Object(ObjectData {
        class: None,
        props: headers_props,
    }));
    let body_bits = state
        .heap
        .intern_bits(String::from_utf8_lossy(&body).into_owned());
    state.heap.alloc_bits(HostValue::Object(ObjectData {
        class: None,
        props: vec![
            ("status".to_string(), encode_number(f64::from(status))),
            ("headers".to_string(), headers_bits),
            ("body".to_string(), body_bits),
        ],
    }))
}

fn fetch_args(state: &HostState, args: &[u64]) -> Result<OutboundRequest, wasmtime::Error> {
    // dd_fetch(url) or dd_fetch(url, {method?, headers?, body?})
    let url = string_arg(state, args, 0);
    if url.is_empty() {
        return Err(fail("dd_fetch", "url must be a non-empty string"));
    }
    let mut method = "GET".to_string();
    let mut headers = Vec::new();
    let mut body = Vec::new();
    if let JsValue::Handle(id) = decode(arg(args, 1))
        && let Some(HostValue::Object(options)) = state.heap.handle(id)
    {
        if let Some(bits) = options.get("method") {
            method = state.heap.display(bits);
        }
        if let Some(bits) = options.get("body") {
            body = state.heap.display(bits).into_bytes();
        }
        if let Some(JsValue::Handle(headers_id)) = options.get("headers").map(decode)
            && let Some(HostValue::Object(header_object)) = state.heap.handle(headers_id)
        {
            headers = header_object
                .props
                .iter()
                .filter(|(key, _)| key != "__class__")
                .map(|(key, value)| (key.clone(), state.heap.display(*value)))
                .collect();
        }
    }
    Ok((method, url, headers, body))
}

/// `dd_memory_atomic(binding, key, command)`: run `command` under the key's
/// lock with its persisted tvars loaded; on success, commit dirty tvars in
/// one batch. State plus result commit atomically per key, mirroring the
/// memory-namespace model of the V8 runtime (without the outbox effects).
fn memory_atomic(caller: &mut Caller<'_, HostState>, args: &[u64]) -> HostResult {
    let name = "dd_memory_atomic";
    if caller.data().active_atomic.is_some() {
        return Err(fail(name, "atomic commands cannot nest"));
    }
    let stores = match &caller.data().context.stores {
        Some(stores) => Arc::clone(stores),
        None => {
            return Err(fail(
                name,
                "no store attached to this worker (start the server with --store-dir)",
            ));
        }
    };
    let binding = string_arg(caller.data(), args, 0);
    let memory_key = string_arg(caller.data(), args, 1);
    let command = arg(args, 2);
    let namespace = format!("{}.{}", caller.data().context.worker_name, binding);

    let lock = atomic_key_lock(&namespace, &memory_key);
    let deadline = Instant::now() + ATOMIC_LOCK_TIMEOUT;
    let guard = loop {
        match lock.try_lock() {
            Ok(guard) => break guard,
            Err(std::sync::TryLockError::WouldBlock) => {
                if Instant::now() > deadline {
                    return Err(fail(
                        name,
                        format!("key {memory_key:?} is locked by another atomic command"),
                    ));
                }
                std::thread::sleep(Duration::from_millis(1));
            }
            Err(std::sync::TryLockError::Poisoned(_)) => {
                return Err(fail(name, "atomic key lock poisoned"));
            }
        }
    };

    let snapshot =
        block_on(stores.memory.snapshot(&namespace, &memory_key)).map_err(|e| fail(name, e))?;
    let mut tvars = HashMap::new();
    for entry in snapshot.entries {
        if entry.deleted {
            continue;
        }
        if entry.encoding != MEMORY_ENCODING_UTF8 {
            return Err(fail(
                name,
                format!(
                    "tvar {:?} uses encoding {:?} (written by the V8 runtime?); \
                     the wasm host only reads utf8 JSON",
                    entry.key, entry.encoding
                ),
            ));
        }
        let value = serde_json::from_slice(&entry.value).map_err(|e| {
            fail(
                name,
                format!("tvar {:?} holds invalid JSON: {e}", entry.key),
            )
        })?;
        tvars.insert(entry.key, value);
    }

    caller.data_mut().active_atomic = Some(ActiveAtomic {
        tvars,
        dirty: Default::default(),
    });

    let outcome = call_closure(&mut *caller, command, &[]);

    let active = caller
        .data_mut()
        .active_atomic
        .take()
        .expect("active_atomic set above and only taken here");
    let result = outcome?;
    if caller.data().pending_exception.is_some() {
        // The command threw: nothing commits, the exception propagates to the
        // caller's try/catch handling.
        return Ok(result);
    }

    let mutations: Vec<MemoryBatchMutation> = active
        .dirty
        .iter()
        .map(|tvar| {
            let value = active
                .tvars
                .get(tvar)
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            Ok(MemoryBatchMutation {
                key: tvar.clone(),
                value: serde_json::to_vec(&value)
                    .map_err(|e| fail(name, format!("tvar {tvar:?} serialization: {e}")))?,
                encoding: MEMORY_ENCODING_UTF8.to_string(),
                deleted: false,
            })
        })
        .collect::<Result<_, wasmtime::Error>>()?;
    if !mutations.is_empty() {
        block_on(
            stores
                .memory
                .apply_batch(&namespace, &memory_key, &mutations, None, &[], None),
        )
        .map_err(|e| fail(name, e))?;
    }
    drop(guard);
    Ok(result)
}
