//! Perry wasm worker engine: instantiation, dd's `ffi` surface, the event
//! loop, and request dispatch.
//!
//! Execution model: one fresh instance per request. `_start` runs the
//! worker's top level (string interning, class registration, `dd_register`),
//! then the registered fetch closure is called with `(method, url, body)`.
//! The worker is stateless across requests by construction.

use crate::bridge::{call_closure, dispatch, json_stringify_bits, resolve_promise};
use crate::heap::{HostValue, PromiseState};
use crate::nanbox::{JsValue, TAG_NULL, TAG_UNDEFINED, decode, encode, encode_number};
use crate::state::{CurrentRequest, HostState};
use common::{PlatformError, Result, WorkerInvocation, WorkerOutput};
use std::sync::OnceLock;
use std::time::{Duration, Instant};
use wasmtime::{Caller, Config, Engine, Extern, FuncType, Linker, Module, Store, Val, ValType};

const EPOCH_TICK: Duration = Duration::from_millis(50);

/// Engine shared by all worker modules; a single ticker thread drives epoch
/// interruption so runaway guest code hits its deadline.
fn shared_engine() -> &'static Engine {
    static ENGINE: OnceLock<Engine> = OnceLock::new();
    ENGINE.get_or_init(|| {
        let mut config = Config::new();
        config.epoch_interruption(true);
        let engine = Engine::new(&config).expect("wasmtime engine construction cannot fail");
        let ticker = engine.clone();
        std::thread::Builder::new()
            .name("perry-wasm-epoch".to_string())
            .spawn(move || {
                loop {
                    std::thread::sleep(EPOCH_TICK);
                    ticker.increment_epoch();
                }
            })
            .expect("epoch ticker thread failed to spawn");
        engine
    })
}

#[derive(Clone, Copy)]
pub struct InvokeOptions {
    pub timeout: Duration,
}

impl Default for InvokeOptions {
    fn default() -> Self {
        InvokeOptions {
            timeout: Duration::from_secs(5),
        }
    }
}

/// A compiled Perry worker. Compilation happens once; each `invoke` runs in a
/// fresh instance.
pub struct WorkerModule {
    module: Module,
}

impl WorkerModule {
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let module = Module::new(shared_engine(), bytes)
            .map_err(|error| PlatformError::bad_request(format!("invalid wasm module: {error}")))?;

        for import in module.imports() {
            match import.module() {
                "rt" if import.name().starts_with("__async_") => {
                    return Err(PlatformError::bad_request(format!(
                        "worker uses async function {:?}: Perry compiles async bodies to JavaScript, \
                         which the dd wasm host cannot run — use explicit .then() chains instead",
                        import.name().trim_start_matches("__async_"),
                    )));
                }
                "rt" | "ffi" => {}
                other => {
                    return Err(PlatformError::bad_request(format!(
                        "worker imports from unsupported module {other:?} \
                         (expected only \"rt\" and \"ffi\")"
                    )));
                }
            }
        }
        Ok(WorkerModule { module })
    }

    pub fn invoke(
        &self,
        invocation: WorkerInvocation,
        options: InvokeOptions,
    ) -> Result<WorkerOutput> {
        let deadline = Instant::now() + options.timeout;
        let mut store = Store::new(shared_engine(), HostState::default());
        let ticks = options.timeout.as_millis() / EPOCH_TICK.as_millis() + 2;
        store.set_epoch_deadline(ticks as u64);

        let linker = build_linker(&self.module)?;
        let instance = linker
            .instantiate(&mut store, &self.module)
            .map_err(runtime_error)?;

        store.data_mut().table = instance.get_table(&mut store, "__indirect_function_table");
        store.data_mut().memory = instance.get_memory(&mut store, "memory");
        if store.data().table.is_none() {
            return Err(PlatformError::bad_request(
                "worker does not export __indirect_function_table",
            ));
        }

        let start = instance
            .get_func(&mut store, "_start")
            .ok_or_else(|| PlatformError::bad_request("worker does not export _start"))?;
        start
            .call(&mut store, &[], &mut [])
            .map_err(runtime_error)?;
        drain_event_loop(&mut store, deadline, None)?;

        let handler = store.data().registered_handler.ok_or_else(|| {
            PlatformError::bad_request(
                "worker never called dd_register(fetchHandler) during startup",
            )
        })?;

        store.data_mut().current_request = Some(CurrentRequest {
            headers: invocation.headers.clone(),
        });
        let args = {
            let state = store.data_mut();
            [
                state.heap.intern_bits(invocation.method),
                state.heap.intern_bits(invocation.url),
                state
                    .heap
                    .intern_bits(String::from_utf8_lossy(&invocation.body).into_owned()),
            ]
        };
        let result = call_closure(&mut store, handler, &args).map_err(runtime_error)?;

        if let Some(exception) = store.data().pending_exception {
            let message = store.data().heap.display(exception);
            return Err(PlatformError::runtime(format!(
                "worker fetch handler threw: {message}"
            )));
        }

        let result = settle(&mut store, result, deadline)?;
        decode_response(store.data(), result)
    }
}

/// Wait for a promise result to resolve, pumping timers and microtasks.
fn settle(store: &mut Store<HostState>, bits: u64, deadline: Instant) -> Result<u64> {
    let JsValue::Handle(id) = decode(bits) else {
        return Ok(bits);
    };
    if !matches!(
        store.data().heap.handle(id),
        Some(HostValue::Promise { .. })
    ) {
        return Ok(bits);
    }
    let resolved = move |state: &HostState| {
        matches!(
            state.heap.handle(id),
            Some(HostValue::Promise {
                state: PromiseState::Resolved(_),
                ..
            })
        )
    };
    drain_event_loop(store, deadline, Some(&resolved))?;
    match store.data().heap.handle(id) {
        Some(HostValue::Promise {
            state: PromiseState::Resolved(value),
            ..
        }) => {
            let value = *value;
            settle(store, value, deadline)
        }
        _ => Err(PlatformError::runtime(
            "worker response promise never resolved (timers exhausted before resolution)",
        )),
    }
}

/// Run microtasks and timers. With `waiting_for = Some(done)`, sleep for
/// future timers and fail if `done` cannot become true before the deadline
/// (used while a response promise is pending). With `None`, only run work
/// that is already due and return — a worker that schedules a long timer at
/// startup must not delay its synchronous responses.
fn drain_event_loop(
    store: &mut Store<HostState>,
    deadline: Instant,
    waiting_for: Option<&dyn Fn(&HostState) -> bool>,
) -> Result<()> {
    loop {
        while let Some(task) = {
            let state = store.data_mut();
            state.microtasks.pop_front()
        } {
            let outcome = if task.callback_bits == TAG_UNDEFINED {
                task.value_bits
            } else {
                call_closure(&mut *store, task.callback_bits, &[task.value_bits])
                    .map_err(runtime_error)?
            };
            chain_into(store.data_mut(), task.downstream, outcome);
            if Instant::now() > deadline {
                return Err(PlatformError::runtime("worker exceeded its time budget"));
            }
        }

        if let Some(done) = waiting_for
            && done(store.data())
        {
            return Ok(());
        }

        let next = store
            .data()
            .timers
            .iter()
            .min_by_key(|timer| timer.due)
            .map(|timer| (timer.id, timer.due));
        let Some((timer_id, due)) = next else {
            return Ok(());
        };
        let now = Instant::now();
        if waiting_for.is_none() && due > now {
            return Ok(());
        }
        if due > deadline {
            return Err(PlatformError::runtime(
                "worker response is waiting on a timer due after the request time budget",
            ));
        }
        if due > now {
            std::thread::sleep(due - now);
        }

        let fired = {
            let state = store.data_mut();
            let Some(position) = state.timers.iter().position(|timer| timer.id == timer_id) else {
                continue;
            };
            match state.timers[position].every {
                Some(every) => {
                    state.timers[position].due = Instant::now() + every;
                    state.timers[position].callback_bits
                }
                None => state.timers.remove(position).callback_bits,
            }
        };
        call_closure(&mut *store, fired, &[]).map_err(runtime_error)?;
        if Instant::now() > deadline {
            return Err(PlatformError::runtime("worker exceeded its time budget"));
        }
    }
}

/// Resolve `downstream` with a callback's outcome, chaining through promises.
fn chain_into(state: &mut HostState, downstream: u32, outcome: u64) {
    if let JsValue::Handle(id) = decode(outcome) {
        match state.heap.handle(id) {
            Some(HostValue::Promise {
                state: PromiseState::Resolved(value),
                ..
            }) => {
                let value = *value;
                resolve_promise(state, encode(JsValue::Handle(downstream)), value);
                return;
            }
            Some(HostValue::Promise { .. }) => {
                if let Some(HostValue::Promise { reactions, .. }) = state.heap.handle_mut(id) {
                    reactions.push(crate::heap::PromiseReaction {
                        callback_bits: TAG_UNDEFINED,
                        downstream,
                    });
                }
                return;
            }
            _ => {}
        }
    }
    resolve_promise(state, encode(JsValue::Handle(downstream)), outcome);
}

fn decode_response(state: &HostState, bits: u64) -> Result<WorkerOutput> {
    match decode(bits) {
        JsValue::Str(id) => Ok(WorkerOutput {
            status: 200,
            headers: vec![(
                "content-type".to_string(),
                "text/plain; charset=utf-8".to_string(),
            )],
            body: state.heap.string(id).unwrap_or("").as_bytes().to_vec(),
        }),
        JsValue::Handle(id) => {
            let Some(HostValue::Object(object)) = state.heap.handle(id) else {
                return Err(PlatformError::runtime(format!(
                    "worker fetch handler returned a non-response handle: {:?}",
                    state.heap.handle(id)
                )));
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
            let body = match object.get("body").map(decode) {
                None | Some(JsValue::Undefined) | Some(JsValue::Null) => Vec::new(),
                Some(JsValue::Str(body_id)) => {
                    state.heap.string(body_id).unwrap_or("").as_bytes().to_vec()
                }
                Some(JsValue::Handle(body_id)) => match state.heap.handle(body_id) {
                    Some(HostValue::Buffer(bytes)) => bytes.clone(),
                    _ => state.heap.display(object.get("body").unwrap()).into_bytes(),
                },
                Some(_) => state.heap.display(object.get("body").unwrap()).into_bytes(),
            };
            Ok(WorkerOutput {
                status,
                headers,
                body,
            })
        }
        other => Err(PlatformError::runtime(format!(
            "worker fetch handler returned {other:?} instead of a response object or string"
        ))),
    }
}

/// Bind every import the module declares. Real logic lives in the name-keyed
/// bridge dispatcher; individually-declared `rt.*` imports adapt into it so
/// both entry paths (direct call and `mem_call`) share one implementation.
fn build_linker(module: &Module) -> Result<Linker<HostState>> {
    let engine = shared_engine();
    let mut linker: Linker<HostState> = Linker::new(engine);

    for import in module.imports() {
        let Some(func_type) = import.ty().func().cloned() else {
            return Err(PlatformError::bad_request(format!(
                "worker declares a non-function import {}.{}",
                import.module(),
                import.name()
            )));
        };
        let name = import.name().to_string();
        let bound = match (import.module(), name.as_str()) {
            ("rt", "string_new") => bind_string_new(&mut linker, func_type),
            ("rt", "mem_call") => bind_mem_call(&mut linker, func_type, false),
            ("rt", "mem_call_i32") => bind_mem_call(&mut linker, func_type, true),
            ("rt", _) => bind_rt_adapter(&mut linker, func_type, name.clone()),
            ("ffi", _) => bind_ffi(&mut linker, func_type, name.clone()),
            _ => unreachable!("import modules validated in from_bytes"),
        };
        bound.map_err(|error| {
            PlatformError::runtime(format!("failed to bind import rt/{name}: {error}"))
        })?;
    }
    Ok(linker)
}

/// wasmtime errors are anyhow-style chains; `{:#}` keeps every cause on one
/// line so host bridge failures aren't hidden behind "error while executing".
fn runtime_error(error: wasmtime::Error) -> PlatformError {
    PlatformError::runtime(format!("{error:#}"))
}

fn guest_memory(
    caller: &mut Caller<'_, HostState>,
    who: &str,
) -> std::result::Result<wasmtime::Memory, wasmtime::Error> {
    if let Some(memory) = caller.data().memory {
        return Ok(memory);
    }
    match caller.get_export("memory") {
        Some(Extern::Memory(memory)) => Ok(memory),
        _ => Err(wasmtime::Error::msg(format!(
            "{who}: guest memory unavailable"
        ))),
    }
}

/// `rt.string_new(offset, len)`: intern a UTF-8 literal from guest memory.
/// Ids are implicit — assignment order must match the guest's interning order.
fn bind_string_new(
    linker: &mut Linker<HostState>,
    func_type: FuncType,
) -> std::result::Result<(), wasmtime::Error> {
    linker.func_new(
        "rt",
        "string_new",
        func_type,
        |mut caller, params, _results| {
            let (offset, len) = match (params[0].i32(), params[1].i32()) {
                (Some(offset), Some(len)) => (offset as u32 as usize, len as u32 as usize),
                _ => return Err(wasmtime::Error::msg("string_new: malformed parameters")),
            };
            let memory = guest_memory(&mut caller, "string_new")?;
            let mut bytes = vec![0u8; len];
            memory
                .read(&caller, offset, &mut bytes)
                .map_err(|e| wasmtime::Error::msg(format!("string_new: bad range: {e}")))?;
            let text = String::from_utf8_lossy(&bytes).into_owned();
            tracing::trace!(target: "perry_bridge", "string_new {text:?}");
            caller.data_mut().heap.intern_string(text);
            Ok(())
        },
    )?;
    Ok(())
}

/// `rt.mem_call(name_id, arg_count, base)`: the generic bridge bus. Arguments
/// sit in guest memory as raw NaN-box bits; the result is written back to
/// `base` (f64 variant) or returned directly (i32 variant).
fn bind_mem_call(
    linker: &mut Linker<HostState>,
    func_type: FuncType,
    returns_i32: bool,
) -> std::result::Result<(), wasmtime::Error> {
    let import_name = if returns_i32 {
        "mem_call_i32"
    } else {
        "mem_call"
    };
    linker.func_new(
        "rt",
        import_name,
        func_type,
        move |mut caller, params, results| {
            let name_id = params[0].f64().unwrap_or(0.0) as u32;
            let arg_count = params[1].f64().unwrap_or(0.0) as usize;
            let base = params[2].i32().unwrap_or(0) as u32 as usize;

            let name = caller
                .data()
                .heap
                .string(name_id)
                .map(str::to_string)
                .ok_or_else(|| {
                    wasmtime::Error::msg(format!("mem_call: unknown bridge name id {name_id}"))
                })?;
            let memory = guest_memory(&mut caller, "mem_call")?;

            let mut raw = vec![0u8; arg_count * 8];
            memory.read(&caller, base, &mut raw).map_err(|e| {
                wasmtime::Error::msg(format!("mem_call {name}: bad arg range: {e}"))
            })?;
            let args: Vec<u64> = raw
                .chunks_exact(8)
                .map(|chunk| u64::from_le_bytes(chunk.try_into().expect("chunk is 8 bytes")))
                .collect();

            let result = dispatch(&mut caller, &name, &args)?;

            if returns_i32 {
                let value = match decode(result) {
                    JsValue::Bool(b) => i32::from(b),
                    JsValue::Number(n) => n as i32,
                    _ => 0,
                };
                results[0] = Val::I32(value);
            } else {
                memory
                    .write(&mut caller, base, &result.to_le_bytes())
                    .map_err(|e| {
                        wasmtime::Error::msg(format!("mem_call {name}: result writeback: {e}"))
                    })?;
                results[0] = Val::F64(0);
            }
            Ok(())
        },
    )?;
    Ok(())
}

#[derive(Clone, Copy)]
enum ResultKind {
    None,
    I32,
    I64,
    F64,
}

fn result_kind(func_type: &FuncType) -> ResultKind {
    match func_type.results().next() {
        None => ResultKind::None,
        Some(ValType::I32) => ResultKind::I32,
        Some(ValType::F64) => ResultKind::F64,
        _ => ResultKind::I64,
    }
}

fn params_to_bits(params: &[Val]) -> Vec<u64> {
    params
        .iter()
        .map(|param| match param {
            Val::I64(bits) => *bits as u64,
            Val::I32(n) => encode_number(f64::from(*n)),
            Val::F64(bits) => encode_number(f64::from_bits(*bits)),
            _ => TAG_UNDEFINED,
        })
        .collect()
}

fn write_result(
    caller: &mut Caller<'_, HostState>,
    kind: ResultKind,
    outcome: u64,
    results: &mut [Val],
) {
    match kind {
        ResultKind::None => {}
        ResultKind::I32 => {
            results[0] = Val::I32(match decode(outcome) {
                JsValue::Bool(b) => i32::from(b),
                JsValue::Number(n) => n as i32,
                _ => 0,
            });
        }
        ResultKind::F64 => {
            results[0] = Val::F64(caller.data().heap.to_number(outcome).to_bits());
        }
        ResultKind::I64 => {
            results[0] = Val::I64(outcome as i64);
        }
    }
}

/// Adapter for individually-declared `rt.*` imports: convert wasm params to
/// NaN-box bits, run the shared dispatcher, convert back per the declared
/// result type.
fn bind_rt_adapter(
    linker: &mut Linker<HostState>,
    func_type: FuncType,
    name: String,
) -> std::result::Result<(), wasmtime::Error> {
    let kind = result_kind(&func_type);
    let import_name = name.clone();
    linker.func_new(
        "rt",
        &import_name,
        func_type,
        move |mut caller, params, results| {
            let args = params_to_bits(params);
            let outcome = dispatch(&mut caller, &name, &args)?;
            write_result(&mut caller, kind, outcome, results);
            Ok(())
        },
    )?;
    Ok(())
}

/// dd's own host surface, reachable from TS via `declare function`:
///  - `dd_register(handler)` — install the fetch handler
///  - `dd_header(name)` — current request header (or null)
///  - `dd_json(value)` — host-side JSON.stringify
fn bind_ffi(
    linker: &mut Linker<HostState>,
    func_type: FuncType,
    name: String,
) -> std::result::Result<(), wasmtime::Error> {
    let kind = result_kind(&func_type);
    let import_name = name.clone();
    linker.func_new(
        "ffi",
        &import_name,
        func_type,
        move |mut caller, params, results| {
            let args = params_to_bits(params);
            let first = args.first().copied().unwrap_or(TAG_UNDEFINED);
            let outcome = match name.as_str() {
                "dd_register" => {
                    caller.data_mut().registered_handler = Some(first);
                    TAG_UNDEFINED
                }
                "dd_header" => {
                    let wanted = caller.data().heap.display(first).to_ascii_lowercase();
                    let found = caller.data().current_request.as_ref().and_then(|request| {
                        request
                            .headers
                            .iter()
                            .find(|(key, _)| key.to_ascii_lowercase() == wanted)
                            .map(|(_, value)| value.clone())
                    });
                    match found {
                        Some(value) => caller.data_mut().heap.intern_bits(value),
                        None => TAG_NULL,
                    }
                }
                "dd_json" => match json_stringify_bits(caller.data(), first)? {
                    Some(text) => caller.data_mut().heap.intern_bits(text),
                    None => TAG_UNDEFINED,
                },
                other => {
                    return Err(wasmtime::Error::msg(format!(
                        "worker calls undeclared host function ffi.{other} — \
                     the dd wasm host provides dd_register, dd_header, and dd_json"
                    )));
                }
            };
            write_result(&mut caller, kind, outcome, results);
            Ok(())
        },
    )?;
    Ok(())
}
