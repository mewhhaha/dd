use crate::assets::{
    BOOTSTRAP_JS, BOOTSTRAP_SPECIFIER, EXECUTE_WORKER_JS, EXECUTE_WORKER_SPECIFIER,
    WORKER_SPECIFIER,
};
use crate::module_registry::{
    ModuleRegistry, RuntimeModuleKind, normalize_module_path, resolve_module_path,
};
use crate::ops::{
    WorkerDeploymentPayload, WorkerRequestPayload, WorkerSource, clear_request_invocation,
    clear_worker_deployment_config, register_request_invocation, register_worker_deployment_config,
};
use crate::service::MemoryExecutionCall;
use base64::Engine;
use common::{PlatformError, Result, WorkerInvocation};
use dd_v8::{
    JsRuntime, ModuleCode, ModuleId, ModuleLoader, ModuleSource, ModuleType, OpDecl,
    RuntimeOptions, v8,
};
use std::borrow::Cow;
use std::future::poll_fn;
use std::mem;
use std::rc::Rc;
use std::sync::OnceLock;
use std::task::{Context, Poll, Waker};
use url::Url;

const PRIMORDIALS_JS: &str = include_str!("../js/core/00_primordials.js");
const CORE_JS: &str = include_str!("../js/core/core.js");
const WEB_INIT_JS: &str = include_str!("../js/web/init.js");
const NODE_ASYNC_HOOKS_SOURCE: &str =
    include_str!("../../../packages/dd-vite/src/shims/node_async_hooks.js");

static BOOTSTRAP_SNAPSHOT: OnceLock<Result<Box<[u8]>>> = OnceLock::new();

/// Every op, in the order the bootstrap snapshot and the runtimes restored
/// from it both create them.
fn runtime_ops() -> Vec<OpDecl> {
    let mut ops = dd_v8::builtins::ops();
    ops.extend(crate::web::ops());
    ops.extend(crate::ops::runtime_ops());
    ops
}

/// Builds (once per process) the snapshot every isolate starts from: the
/// web layer, dd's worker globals and its request machinery, evaluated and
/// captured.
///
/// Every runtime script runs as the body of a function, so nothing it
/// declares is global. core.js returns the bootstrap object (the ops, the
/// web layer's shared state, and dd's own under `dd`); each later script
/// receives it as its `__bootstrap` parameter, and the snapshot keeps it as
/// the runtime's internals, where only the host reaches it.
pub async fn build_bootstrap_snapshot() -> Result<&'static [u8]> {
    BOOTSTRAP_SNAPSHOT
        .get_or_init(|| {
            let mut runtime = JsRuntime::new_for_snapshot(RuntimeOptions {
                ops: runtime_ops(),
                ..Default::default()
            })
            .map_err(runtime_error)?;
            runtime
                .execute_script("ext:core/00_primordials.js", PRIMORDIALS_JS)
                .map_err(runtime_error)?;
            let bootstrap = runtime
                .execute_with_ops("ext:core/core.js", CORE_JS)
                .map_err(runtime_error)?;
            for (name, source) in [
                ("ext:dd/web/init.js", WEB_INIT_JS),
                (BOOTSTRAP_SPECIFIER, BOOTSTRAP_JS),
                (EXECUTE_WORKER_SPECIFIER, EXECUTE_WORKER_JS),
            ] {
                runtime
                    .execute_function(
                        name,
                        source,
                        &["__bootstrap"],
                        std::slice::from_ref(&bootstrap),
                    )
                    .map_err(runtime_error)?;
            }
            runtime.set_internals(bootstrap);
            runtime.snapshot().map_err(runtime_error)
        })
        .as_deref()
        .map_err(Clone::clone)
}

/// What a worker's isolate may do beyond the defaults.
#[derive(Clone, Copy, Debug, Default)]
pub struct IsolatePolicy {
    /// Allow `eval` and `new Function`.
    pub allow_code_generation: bool,
    /// Expose `globalThis.__dd_raw_host_fetch`, a fetch outside any request
    /// and egress rule. For the local development runtime only.
    pub unscoped_fetch: bool,
    /// Expose `globalThis.__dd_internals`, the bootstrap object with every
    /// op. For the runtime's own tests and benchmarks only.
    pub expose_internals: bool,
}

pub fn ensure_v8_flags(flags: &[String]) -> Result<()> {
    dd_v8::set_flags(flags).map_err(PlatformError::internal)
}

#[cfg(test)]
pub async fn validate_worker(
    bootstrap_snapshot: &'static [u8],
    source: &str,
    allow_code_generation: bool,
) -> Result<()> {
    let policy = IsolatePolicy {
        allow_code_generation,
        ..IsolatePolicy::default()
    };
    let mut runtime = new_runtime(bootstrap_snapshot, policy, 0, ModuleRegistry::default())?;
    load_worker(&mut runtime, source).await
}

#[cfg(test)]
pub fn new_runtime_from_snapshot(
    startup_snapshot: &'static [u8],
    allow_code_generation: bool,
    module_registry: ModuleRegistry,
) -> Result<JsRuntime> {
    let policy = IsolatePolicy {
        allow_code_generation,
        ..IsolatePolicy::default()
    };
    new_runtime(startup_snapshot, policy, 0, module_registry)
}

pub fn new_runtime_from_snapshot_with_heap_limit(
    startup_snapshot: &'static [u8],
    policy: IsolatePolicy,
    max_heap_bytes: usize,
    module_registry: ModuleRegistry,
) -> Result<JsRuntime> {
    new_runtime(startup_snapshot, policy, max_heap_bytes, module_registry)
}

pub async fn load_worker(runtime: &mut JsRuntime, source: &str) -> Result<()> {
    let module_id = evaluate_module(runtime, WORKER_SPECIFIER, source).await?;
    let namespace = runtime.module_namespace(module_id).map_err(runtime_error)?;
    let namespace = {
        dd_v8::scope!(scope, runtime);
        let namespace = v8::Local::new(scope, namespace);
        v8::Global::new(scope, v8::Local::<v8::Value>::from(namespace))
    };
    let install = cached_entrypoint(runtime, |entrypoints| {
        Rc::clone(&entrypoints.install_worker)
    });
    runtime
        .call_function(&install, &[namespace])
        .map_err(runtime_error)?;
    Ok(())
}

pub async fn load_worker_source(runtime: &mut JsRuntime, source: &WorkerSource) -> Result<()> {
    let module_registry = runtime
        .op_state()
        .borrow()
        .borrow::<ModuleRegistry>()
        .clone();
    let source = worker_source_text(source, &module_registry)?;
    load_worker(runtime, source.as_ref()).await
}

/// The functions on the runtime's internals (`__bootstrap.dd`) the host calls.
struct RuntimeEntrypoints {
    install_worker: Rc<v8::Global<v8::Function>>,
    install_worker_deployment_handle: Rc<v8::Global<v8::Function>>,
    execute_worker_handle: Rc<v8::Global<v8::Function>>,
    abort_worker_request_handle: Rc<v8::Global<v8::Function>>,
    drain_request_control_queue: Rc<v8::Global<v8::Function>>,
}

pub fn install_worker_deployment_config(
    runtime: &mut JsRuntime,
    payload: WorkerDeploymentPayload,
) -> Result<()> {
    let deployment_handle = {
        let op_state = runtime.op_state();
        let mut op_state = op_state.borrow_mut();
        register_worker_deployment_config(&mut op_state, payload)
    };

    match call_cached_u32_function(
        runtime,
        "installWorkerDeploymentHandle",
        deployment_handle,
        |entrypoints| Rc::clone(&entrypoints.install_worker_deployment_handle),
    ) {
        Ok(()) => Ok(()),
        Err(error) => {
            let op_state = runtime.op_state();
            let mut op_state = op_state.borrow_mut();
            clear_worker_deployment_config(&mut op_state, deployment_handle);
            Err(error)
        }
    }
}

fn cache_runtime_entrypoints(runtime: &mut JsRuntime) -> Result<()> {
    let internals = runtime
        .internals()
        .ok_or_else(|| PlatformError::runtime("the bootstrap snapshot has no runtime internals"))?;
    let entrypoints = {
        dd_v8::scope!(scope, runtime);
        let internals = v8::Local::new(scope, internals);
        let dd = v8::Local::<v8::Object>::try_from(internals)
            .ok()
            .and_then(|internals| {
                let key = v8::String::new(scope, "dd")?;
                internals.get(scope, key.into())
            })
            .and_then(|dd| v8::Local::<v8::Object>::try_from(dd).ok())
            .ok_or_else(|| PlatformError::runtime("the runtime internals have no dd object"))?;
        RuntimeEntrypoints {
            install_worker: internal_function(scope, dd, "installWorker")?,
            install_worker_deployment_handle: internal_function(
                scope,
                dd,
                "installWorkerDeploymentHandle",
            )?,
            execute_worker_handle: internal_function(scope, dd, "executeWorkerHandle")?,
            abort_worker_request_handle: internal_function(scope, dd, "abortWorkerRequestHandle")?,
            drain_request_control_queue: internal_function(
                scope,
                dd,
                "drainRequestControlQueueHandle",
            )?,
        }
    };
    let op_state = runtime.op_state();
    op_state.borrow_mut().put(entrypoints);
    Ok(())
}

pub struct WorkerDispatchRequest<'a> {
    pub request_id: &'a str,
    pub request_context_handle: u32,
    pub completion_handle: u32,
    pub memory_request_scope_handle: u32,
    pub request_body_stream_handle: u32,
    pub stream_response: bool,
    pub memory_call: Option<&'a MemoryExecutionCall>,
    pub request: WorkerInvocation,
}

pub fn dispatch_worker_request(
    runtime: &mut JsRuntime,
    dispatch: WorkerDispatchRequest<'_>,
) -> Result<()> {
    let WorkerDispatchRequest {
        request_id,
        request_context_handle,
        completion_handle,
        memory_request_scope_handle,
        request_body_stream_handle,
        stream_response,
        memory_call,
        mut request,
    } = dispatch;
    let request_handle = {
        let op_state = runtime.op_state();
        let mut op_state = op_state.borrow_mut();
        let request_headers_handle = op_state
            .borrow_mut::<crate::ops::HttpPreparedHeaders>()
            .insert(mem::take(&mut request.headers));
        let request_body_handle = op_state
            .borrow_mut::<crate::ops::HttpPreparedBodies>()
            .insert(mem::take(&mut request.body));
        let payload = WorkerRequestPayload {
            request_id: request_id.to_string(),
            request_context_handle,
            completion_handle,
            memory_request_scope_handle,
            memory_call: memory_call.cloned(),
            request_body_stream_handle,
            request_headers_handle,
            request_body_handle,
            stream_response,
            max_response_body_bytes: op_state
                .borrow::<crate::ops::RuntimeExecutionLimits>()
                .max_response_body_bytes,
            max_request_body_bytes: op_state
                .borrow::<crate::ops::RuntimeExecutionLimits>()
                .max_request_body_bytes,
            method: mem::take(&mut request.method),
            url: mem::take(&mut request.url),
            input_request_id: mem::take(&mut request.request_id),
        };
        register_request_invocation(&mut op_state, payload)
    };

    match call_cached_u32_function(
        runtime,
        "executeWorkerHandle",
        request_handle,
        |entrypoints| Rc::clone(&entrypoints.execute_worker_handle),
    ) {
        Ok(()) => Ok(()),
        Err(error) => {
            let op_state = runtime.op_state();
            let mut op_state = op_state.borrow_mut();
            clear_request_invocation(&mut op_state, request_handle);
            Err(error)
        }
    }
}

pub fn abort_worker_request_handle(
    runtime: &mut JsRuntime,
    request_context_handle: u32,
) -> Result<()> {
    call_cached_u32_function(
        runtime,
        "abortWorkerRequestHandle",
        request_context_handle,
        |entrypoints| Rc::clone(&entrypoints.abort_worker_request_handle),
    )
}

pub fn drain_request_control_queue(runtime: &mut JsRuntime) -> Result<()> {
    call_cached_noarg_function(runtime, "drainRequestControlQueueHandle", |entrypoints| {
        Rc::clone(&entrypoints.drain_request_control_queue)
    })
}

pub fn pump_event_loop_once(runtime: &mut JsRuntime, waker: &Waker) -> Result<()> {
    let mut cx = Context::from_waker(waker);
    match runtime.poll_event_loop(&mut cx) {
        Poll::Ready(Ok(())) | Poll::Pending => Ok(()),
        Poll::Ready(Err(error)) => Err(runtime_error(error)),
    }
}

fn new_runtime(
    startup_snapshot: &'static [u8],
    policy: IsolatePolicy,
    max_heap_bytes: usize,
    module_registry: ModuleRegistry,
) -> Result<JsRuntime> {
    let mut runtime = JsRuntime::new(RuntimeOptions {
        ops: runtime_ops(),
        module_loader: Some(Rc::new(RuntimeModuleLoader {
            module_registry: module_registry.clone(),
        })),
        startup_snapshot: Some(startup_snapshot),
        max_heap_bytes,
    })
    .map_err(runtime_error)?;
    if max_heap_bytes > 0 {
        let isolate = runtime.v8_isolate().thread_safe_handle();
        runtime.set_near_heap_limit_callback(move |current_limit, _| {
            isolate.terminate_execution();
            // V8 needs headroom to unwind after termination instead of aborting the process.
            current_limit.saturating_add(16 * 1024 * 1024)
        });
    }
    runtime.op_state().borrow_mut().put(module_registry);
    set_code_generation_from_strings(&mut runtime, policy.allow_code_generation);
    cache_runtime_entrypoints(&mut runtime)?;
    if policy.unscoped_fetch {
        runtime
            .op_state()
            .borrow_mut()
            .put(crate::ops::UnscopedFetch);
        expose_global(
            &mut runtime,
            "<dd:unscoped-fetch>",
            "__dd_raw_host_fetch",
            "__bootstrap.dd.unscopedFetch",
        )?;
    }
    if policy.expose_internals {
        expose_global(
            &mut runtime,
            "<dd:internals>",
            "__dd_internals",
            "__bootstrap",
        )?;
    }
    Ok(runtime)
}

/// Defines `globalThis[name]` as `value`, an expression over the runtime's
/// internals (`__bootstrap`).
fn expose_global(runtime: &mut JsRuntime, script: &str, name: &str, value: &str) -> Result<()> {
    let internals = runtime
        .internals()
        .ok_or_else(|| PlatformError::runtime("the bootstrap snapshot has no runtime internals"))?;
    let source = format!(
        "Object.defineProperty(globalThis, {name:?}, {{ value: {value}, configurable: true, writable: true }});"
    );
    runtime
        .execute_function(script, &source, &["__bootstrap"], &[internals])
        .map_err(runtime_error)?;
    Ok(())
}

fn set_code_generation_from_strings(runtime: &mut JsRuntime, allow: bool) {
    dd_v8::scope!(scope, runtime);
    scope
        .get_current_context()
        .set_allow_generation_from_strings(allow);
}

fn call_cached_u32_function(
    runtime: &mut JsRuntime,
    name: &str,
    arg: u32,
    select: impl FnOnce(&RuntimeEntrypoints) -> Rc<v8::Global<v8::Function>>,
) -> Result<()> {
    let function = cached_entrypoint(runtime, select);
    dd_v8::scope!(scope, runtime);
    let function = v8::Local::new(scope, function.as_ref());
    let arg = v8::Integer::new_from_unsigned(scope, arg).into();
    v8::tc_scope!(let scope, scope);
    let receiver = v8::undefined(scope).into();
    function
        .call(scope, receiver, &[arg])
        .ok_or_else(|| PlatformError::runtime(format!("runtime entrypoint {name} threw")))?;
    Ok(())
}

fn call_cached_noarg_function(
    runtime: &mut JsRuntime,
    name: &str,
    select: impl FnOnce(&RuntimeEntrypoints) -> Rc<v8::Global<v8::Function>>,
) -> Result<()> {
    let function = cached_entrypoint(runtime, select);
    dd_v8::scope!(scope, runtime);
    let function = v8::Local::new(scope, function.as_ref());
    v8::tc_scope!(let scope, scope);
    let receiver = v8::undefined(scope).into();
    function
        .call(scope, receiver, &[])
        .ok_or_else(|| PlatformError::runtime(format!("runtime entrypoint {name} threw")))?;
    Ok(())
}

fn cached_entrypoint(
    runtime: &mut JsRuntime,
    select: impl FnOnce(&RuntimeEntrypoints) -> Rc<v8::Global<v8::Function>>,
) -> Rc<v8::Global<v8::Function>> {
    let op_state = runtime.op_state();
    let op_state = op_state.borrow();
    select(op_state.borrow::<RuntimeEntrypoints>())
}

fn internal_function<'scope>(
    scope: &mut v8::PinScope<'scope, '_>,
    dd: v8::Local<'scope, v8::Object>,
    name: &str,
) -> Result<Rc<v8::Global<v8::Function>>> {
    let key = v8::String::new(scope, name)
        .ok_or_else(|| PlatformError::runtime("failed to allocate V8 function name"))?;
    let function = dd
        .get(scope, key.into())
        .and_then(|value| v8::Local::<v8::Function>::try_from(value).ok())
        .ok_or_else(|| {
            PlatformError::runtime(format!("runtime entrypoint {name} is not a function"))
        })?;
    Ok(Rc::new(v8::Global::new(scope, function)))
}

#[derive(Default)]
struct RuntimeModuleLoader {
    module_registry: ModuleRegistry,
}

impl ModuleLoader for RuntimeModuleLoader {
    fn resolve(&self, specifier: &str, referrer: &str) -> std::result::Result<String, String> {
        if referrer.starts_with("dd-module:")
            && !specifier.starts_with("//")
            && !has_url_scheme(specifier)
        {
            return resolve_dd_module_import(specifier, referrer).map(String::from);
        }
        resolve_import(specifier, referrer).map(String::from)
    }

    fn load(
        &self,
        name: &str,
        requested_type: ModuleType,
    ) -> std::result::Result<ModuleSource, String> {
        let specifier = Url::parse(name).map_err(|error| error.to_string())?;
        match specifier.scheme() {
            "dd-module" => load_dd_module(&self.module_registry, &specifier, requested_type),
            "node" if specifier.as_str() == "node:async_hooks" => Ok(ModuleSource::new(
                ModuleType::JavaScript,
                ModuleCode::String(NODE_ASYNC_HOOKS_SOURCE.to_string()),
            )),
            _ => Err(format!(
                "runtime module loader does not support: {specifier}"
            )),
        }
    }
}

/// Resolves an import the way a browser does: an absolute URL as is, a path
/// starting with `/`, `./` or `../` against the referrer, anything else
/// (a bare specifier) is an error.
fn resolve_import(specifier: &str, referrer: &str) -> std::result::Result<Url, String> {
    match Url::parse(specifier) {
        Ok(url) => Ok(url),
        Err(url::ParseError::RelativeUrlWithoutBase) => {
            if !(specifier.starts_with('/')
                || specifier.starts_with("./")
                || specifier.starts_with("../"))
            {
                let from = if referrer.is_empty() {
                    String::new()
                } else {
                    format!(" from \"{referrer}\"")
                };
                return Err(format!(
                    "Relative import path \"{specifier}\" not prefixed with / or ./ or ../{from}"
                ));
            }
            let base = Url::parse(referrer).map_err(|error| {
                format!("invalid referrer {referrer:?} for import {specifier:?}: {error}")
            })?;
            base.join(specifier)
                .map_err(|error| format!("invalid import {specifier:?}: {error}"))
        }
        Err(error) => Err(format!("invalid import {specifier:?}: {error}")),
    }
}

fn resolve_dd_module_import(specifier: &str, referrer: &str) -> std::result::Result<Url, String> {
    let referrer = Url::parse(referrer).map_err(|error| error.to_string())?;
    let (graph_id, referrer_path) = dd_module_parts(&referrer)?;
    let module_path = resolve_module_path(&referrer_path, specifier)?;
    let encoded_path = module_path
        .split('/')
        .map(percent_encode_path_segment)
        .collect::<Vec<_>>()
        .join("/");
    Url::parse(&format!("dd-module://graph/{graph_id}/{encoded_path}"))
        .map_err(|error| error.to_string())
}

fn load_dd_module(
    module_registry: &ModuleRegistry,
    specifier: &Url,
    requested_type: ModuleType,
) -> std::result::Result<ModuleSource, String> {
    let (graph_id, module_path) = dd_module_parts(specifier)?;
    let module = module_registry
        .module(&graph_id, &module_path)
        .ok_or_else(|| format!("module graph {graph_id} does not contain module: {module_path}"))?;
    let module_type = module_type(module.kind);
    if requested_type != module_type {
        return Err(format!(
            "requested module type {requested_type} does not match {module_type} module: {module_path}"
        ));
    }
    let code = match module.kind {
        RuntimeModuleKind::Wasm => ModuleCode::String(compiled_wasm_module_source(&module.code)),
        RuntimeModuleKind::JavaScript
        | RuntimeModuleKind::Json
        | RuntimeModuleKind::Text
        | RuntimeModuleKind::Bytes => ModuleCode::Bytes(module.code),
    };
    Ok(ModuleSource::new(module_type, code))
}

fn module_type(kind: RuntimeModuleKind) -> ModuleType {
    match kind {
        RuntimeModuleKind::JavaScript | RuntimeModuleKind::Wasm => ModuleType::JavaScript,
        RuntimeModuleKind::Json => ModuleType::Json,
        RuntimeModuleKind::Text => ModuleType::Text,
        RuntimeModuleKind::Bytes => ModuleType::Bytes,
    }
}

fn compiled_wasm_module_source(bytes: &[u8]) -> String {
    let encoded = base64::engine::general_purpose::STANDARD.encode(bytes);
    format!(
        r#"
const encoded = {encoded:?};
const binary = atob(encoded);
const bytes = new Uint8Array(binary.length);
for (let index = 0; index < binary.length; index += 1) {{
  bytes[index] = binary.charCodeAt(index);
}}
export default new WebAssembly.Module(bytes);
"#
    )
}

fn dd_module_parts(specifier: &Url) -> std::result::Result<(String, String), String> {
    if specifier.scheme() != "dd-module" {
        return Err(format!("expected dd-module module URL, got {specifier}"));
    }
    if specifier.host_str() != Some("graph") {
        return Err(format!(
            "expected dd-module://graph module URL, got {specifier}"
        ));
    }
    let path = specifier.path().trim_start_matches('/');
    let (graph_id, module_path) = path.split_once('/').ok_or_else(|| {
        format!("dd-module module URL is missing graph id or module path: {specifier}")
    })?;
    let graph_id = graph_id.trim();
    if graph_id.is_empty()
        || graph_id.len() > 128
        || !graph_id.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(format!(
            "invalid dd-module graph id in module URL: {specifier}"
        ));
    }
    let bytes = percent_decode(module_path)?;
    let path = String::from_utf8(bytes)
        .map_err(|error| format!("invalid UTF-8 in dd-module module path: {error}"))?;
    normalize_module_path(&path).map(|path| (graph_id.to_string(), path))
}

fn has_url_scheme(value: &str) -> bool {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !first.is_ascii_alphabetic() {
        return false;
    }
    for ch in chars {
        if ch == ':' {
            return true;
        }
        if !(ch.is_ascii_alphanumeric() || ch == '+' || ch == '-' || ch == '.') {
            return false;
        }
    }
    false
}

fn percent_encode_path_segment(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.as_bytes() {
        match *byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(*byte as char)
            }
            other => {
                use std::fmt::Write as _;
                let _ = write!(&mut out, "%{other:02X}");
            }
        }
    }
    out
}

fn worker_source_text<'a>(
    source: &'a WorkerSource,
    module_registry: &ModuleRegistry,
) -> Result<Cow<'a, str>> {
    match source {
        WorkerSource::Inline(source) => Ok(Cow::Borrowed(source.as_ref())),
        WorkerSource::Module {
            graph_id,
            entrypoint,
        } => {
            let specifier = module_entrypoint_specifier(module_registry, graph_id, entrypoint)?;
            Ok(Cow::Owned(format!(
                "export {{ default }} from {specifier:?};\n"
            )))
        }
    }
}

fn module_entrypoint_specifier(
    module_registry: &ModuleRegistry,
    graph_id: &str,
    entrypoint: &str,
) -> Result<String> {
    let graph_id = graph_id.trim();
    if graph_id.is_empty()
        || graph_id.len() > 128
        || !graph_id.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(PlatformError::bad_request("module graph id is invalid"));
    }
    let entrypoint = normalize_module_path(entrypoint).map_err(|error| {
        PlatformError::bad_request(format!("invalid module entrypoint: {error}"))
    })?;
    if module_registry.source(graph_id, &entrypoint).is_none() {
        return Err(PlatformError::bad_request(format!(
            "module graph {graph_id} does not contain entrypoint: {entrypoint}"
        )));
    }
    let encoded_path = entrypoint
        .split('/')
        .map(percent_encode_path_segment)
        .collect::<Vec<_>>()
        .join("/");
    Ok(format!("dd-module://graph/{graph_id}/{encoded_path}"))
}

fn percent_decode(value: &str) -> std::result::Result<Vec<u8>, String> {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut idx = 0;
    while idx < bytes.len() {
        match bytes[idx] {
            b'%' => {
                if idx + 2 >= bytes.len() {
                    return Err("truncated percent-escape in module URL".to_string());
                }
                let hi = decode_hex_digit(bytes[idx + 1])?;
                let lo = decode_hex_digit(bytes[idx + 2])?;
                out.push((hi << 4) | lo);
                idx += 3;
            }
            b => {
                out.push(b);
                idx += 1;
            }
        }
    }
    Ok(out)
}

fn decode_hex_digit(value: u8) -> std::result::Result<u8, String> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        b'A'..=b'F' => Ok(value - b'A' + 10),
        _ => Err(format!(
            "invalid hex digit in module URL escape: {}",
            value as char
        )),
    }
}

/// Loads and evaluates a module, then runs one turn of the event loop so an
/// error its evaluation left behind (an unhandled rejection) fails the load.
async fn evaluate_module(
    runtime: &mut JsRuntime,
    specifier: &str,
    source: &str,
) -> Result<ModuleId> {
    let module_id = runtime
        .load_side_module(specifier, Some(source.to_string()))
        .map_err(runtime_error)?;
    runtime
        .evaluate_module(module_id)
        .await
        .map_err(runtime_error)?;
    poll_fn(|cx| match runtime.poll_event_loop(cx) {
        Poll::Ready(result) => Poll::Ready(result),
        Poll::Pending => Poll::Ready(Ok(())),
    })
    .await
    .map_err(runtime_error)?;
    Ok(module_id)
}

fn runtime_error(error: impl std::fmt::Display) -> PlatformError {
    PlatformError::runtime(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    fn simple_worker_source() -> &'static str {
        r#"
        export default {
          async fetch(_request) {
            return new Response("ok");
          }
        };
        "#
    }

    /// Runs `source` as a function body with the runtime's internals as
    /// `__bootstrap`, as the runtime's own scripts see them.
    fn execute_internal(runtime: &mut JsRuntime, source: &str) -> v8::Global<v8::Value> {
        let internals = runtime.internals().expect("runtime internals");
        runtime
            .execute_function("<dd:test>", source, &["__bootstrap"], &[internals])
            .expect("internal script should run")
    }

    fn string(runtime: &mut JsRuntime, value: v8::Global<v8::Value>) -> String {
        dd_v8::scope!(scope, runtime);
        let value = v8::Local::new(scope, value);
        value
            .to_string(scope)
            .expect("value should stringify")
            .to_rust_string_lossy(scope)
    }

    #[tokio::test]
    #[serial]
    async fn bootstrap_snapshot_builds() {
        let _ = build_bootstrap_snapshot()
            .await
            .expect("bootstrap snapshot should build");
    }

    #[tokio::test]
    #[serial]
    async fn runtime_starts_from_bootstrap_snapshot() {
        let snapshot = build_bootstrap_snapshot()
            .await
            .expect("bootstrap snapshot should build");
        let _ = new_runtime_from_snapshot(snapshot, false, ModuleRegistry::default())
            .expect("runtime should start from snapshot");
    }

    #[test]
    #[serial]
    fn fetch_classes_work_from_bootstrap_snapshot() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime should build");
        let snapshot = runtime
            .block_on(build_bootstrap_snapshot())
            .expect("bootstrap snapshot should build");
        let mut js_runtime = new_runtime_from_snapshot(snapshot, false, ModuleRegistry::default())
            .expect("runtime should start from snapshot");

        js_runtime
            .execute_script(
                "<dd:test>",
                r#"
                globalThis.__dd_test_request = new Request("http://example.com/test", {
                  method: "POST",
                  body: "hi",
                });
                globalThis.__dd_test_response = Response.json({ ok: true });
                globalThis.__dd_test_headers = new Headers([["x-dd", "ok"]]);
                globalThis.__dd_test_form_data = new FormData();
                globalThis.__dd_test_form_data.append("name", "value");
                "#,
            )
            .expect("fetch classes should construct");
        let ctor_match = execute_internal(
            &mut js_runtime,
            r#"
            const { web } = __bootstrap.dd;
            return JSON.stringify({
              fetch: typeof globalThis.fetch === "function" && globalThis.fetch === web.fetch,
              request: globalThis.Request === web.Request,
              headers: globalThis.Headers === web.Headers,
              response: globalThis.Response === web.Response,
              formData: globalThis.FormData === web.FormData,
            });
            "#,
        );
        assert_eq!(
            string(&mut js_runtime, ctor_match),
            r#"{"fetch":true,"request":true,"headers":true,"response":true,"formData":true}"#
        );

        let mut eval = |source: &str| {
            let value = js_runtime
                .execute_script("<dd:test>", source)
                .expect("script should execute");
            let value = runtime
                .block_on(js_runtime.resolve(value))
                .expect("value should resolve");
            string(&mut js_runtime, value)
        };
        assert_eq!(
            eval("globalThis.__dd_test_request.url"),
            "http://example.com/test"
        );
        assert_eq!(
            eval("globalThis.__dd_test_response.text()"),
            r#"{"ok":true}"#
        );
        assert_eq!(
            eval("globalThis.__dd_test_response.headers.get('content-type')"),
            "application/json"
        );
        assert_eq!(eval("globalThis.__dd_test_headers.get('x-dd')"), "ok");
        assert_eq!(eval("globalThis.__dd_test_form_data.get('name')"), "value");
    }

    #[test]
    #[serial]
    fn teed_and_piped_streams_deliver_every_chunk() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime should build");
        let snapshot = runtime
            .block_on(build_bootstrap_snapshot())
            .expect("bootstrap snapshot should build");
        let mut js_runtime = new_runtime_from_snapshot(snapshot, false, ModuleRegistry::default())
            .expect("runtime should start from bootstrap snapshot");
        // Tee and pipe chunk steps run through the web layer's queueMicrotask.
        let result = js_runtime
            .execute_script(
                "<dd:test>",
                r#"
                (async () => {
                  const bytes = new ReadableStream({
                    type: "bytes",
                    start(controller) {
                      controller.enqueue(new TextEncoder().encode("hello"));
                      controller.close();
                    },
                  });
                  const [left, right] = bytes.tee();
                  const teed = await Promise.all([new Response(left).text(), new Response(right).text()]);
                  const [first, second] = new Response("plain").body.tee();
                  const plain = await Promise.all([new Response(first).text(), new Response(second).text()]);
                  const piped = await new Response(
                    new Response("piped").body.pipeThrough(new TransformStream()),
                  ).text();
                  return [...teed, ...plain, piped].join(",");
                })()
                "#,
            )
            .expect("stream script should run");
        let result = runtime
            .block_on(js_runtime.resolve(result))
            .expect("streams should deliver");
        assert_eq!(
            string(&mut js_runtime, result),
            "hello,hello,plain,plain,piped"
        );
    }

    #[test]
    #[serial]
    fn worker_code_reaches_no_runtime_internals() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime should build");
        let snapshot = runtime
            .block_on(build_bootstrap_snapshot())
            .expect("bootstrap snapshot should build");
        let mut js_runtime = new_runtime_from_snapshot(snapshot, false, ModuleRegistry::default())
            .expect("runtime should start from bootstrap snapshot");
        let source = r#"
            const probes = {
              Deno: typeof Deno,
              __bootstrap: typeof __bootstrap,
              core: typeof core,
              ops: typeof ops,
              dd: typeof dd,
              primordials: typeof primordials,
              internals: typeof internals,
              runtimeOp: typeof runtimeOp,
              define: typeof define,
            };
            const leaked = Object.keys(probes).filter((name) => probes[name] !== "undefined");
            const ddGlobals = Object.getOwnPropertyNames(globalThis)
              .filter((name) => name.startsWith("__"));
            const asyncContext = Object.keys(globalThis.__dd_async_context).sort();
            export default {
              fetch() {
                return Response.json({ leaked, ddGlobals, asyncContext });
              },
            };
        "#;
        runtime
            .block_on(load_worker(&mut js_runtime, source))
            .expect("worker should load");
        let body = execute_internal(
            &mut js_runtime,
            "return Promise.resolve(__bootstrap.dd.worker.fetch(new Request('http://worker/'))).then((response) => response.text())",
        );
        let body = runtime
            .block_on(js_runtime.resolve(body))
            .expect("worker fetch should resolve");
        assert_eq!(
            string(&mut js_runtime, body),
            r#"{"leaked":[],"ddGlobals":["__dd_async_context"],"asyncContext":["disableAsyncLocalStore","enterWithAsyncLocalStore","getAsyncLocalStore","runWithAsyncLocalStore"]}"#
        );
    }

    #[test]
    #[serial]
    fn direct_worker_fetch_works_after_loading_worker_into_runtime() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime should build");
        let snapshot = runtime.block_on(async {
            let bootstrap = build_bootstrap_snapshot()
                .await
                .expect("bootstrap snapshot should build");
            validate_worker(bootstrap, simple_worker_source(), false)
                .await
                .expect("worker should validate");
            bootstrap
        });
        let mut js_runtime = new_runtime_from_snapshot(snapshot, false, ModuleRegistry::default())
            .expect("runtime should start from bootstrap snapshot");
        runtime
            .block_on(load_worker(&mut js_runtime, simple_worker_source()))
            .expect("worker should load into runtime");

        let response_promise = execute_internal(
            &mut js_runtime,
            r#"
            return __bootstrap.dd.worker
              .fetch(new Request("http://worker/"), {}, { waitUntil() {} })
              .then((response) => String(response.status));
            "#,
        );
        let response_value = runtime
            .block_on(js_runtime.resolve(response_promise))
            .expect("response promise should resolve");
        assert_eq!(string(&mut js_runtime, response_value), "200");
    }

    #[test]
    #[serial]
    fn response_constructor_works_after_loading_worker_into_runtime() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime should build");
        let snapshot = runtime.block_on(async {
            let bootstrap = build_bootstrap_snapshot()
                .await
                .expect("bootstrap snapshot should build");
            validate_worker(bootstrap, simple_worker_source(), false)
                .await
                .expect("worker should validate");
            bootstrap
        });
        let mut js_runtime = new_runtime_from_snapshot(snapshot, false, ModuleRegistry::default())
            .expect("runtime should start from bootstrap snapshot");
        runtime
            .block_on(load_worker(&mut js_runtime, simple_worker_source()))
            .expect("worker should load into runtime");

        let response_value = js_runtime
            .execute_script("<dd:test>", r#"String(new Response("ok").status)"#)
            .expect("response constructor should execute");
        assert_eq!(string(&mut js_runtime, response_value), "200");
    }

    #[test]
    #[serial]
    fn web_crypto_covers_common_algorithms() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime should build");
        let snapshot = runtime
            .block_on(build_bootstrap_snapshot())
            .expect("bootstrap snapshot should build");
        let mut js_runtime = new_runtime_from_snapshot(snapshot, false, ModuleRegistry::default())
            .expect("runtime should start from snapshot");
        let result = js_runtime
            .execute_script(
                "<dd:test>",
                r#"
                (async () => {
                  const subtle = crypto.subtle;
                  const data = new TextEncoder().encode("dd");
                  const results = [];

                  const ed = await subtle.generateKey({ name: "Ed25519" }, true, ["sign", "verify"]);
                  const edSignature = await subtle.sign("Ed25519", ed.privateKey, data);
                  results.push(await subtle.verify("Ed25519", ed.publicKey, edSignature, data));
                  const edSpki = await subtle.exportKey("spki", ed.publicKey);
                  const edPublic = await subtle.importKey("spki", edSpki, "Ed25519", true, ["verify"]);
                  results.push(await subtle.verify("Ed25519", edPublic, edSignature, data));
                  const edPkcs8 = await subtle.exportKey("pkcs8", ed.privateKey);
                  await subtle.importKey("pkcs8", edPkcs8, "Ed25519", true, ["sign"]);
                  const edJwk = await subtle.exportKey("jwk", ed.privateKey);
                  results.push(edJwk.crv === "Ed25519" && typeof edJwk.x === "string");

                  const alice = await subtle.generateKey({ name: "X25519" }, true, ["deriveBits"]);
                  const bob = await subtle.generateKey({ name: "X25519" }, true, ["deriveBits"]);
                  const aliceShared = new Uint8Array(await subtle.deriveBits({ name: "X25519", public: bob.publicKey }, alice.privateKey, 256));
                  const bobShared = new Uint8Array(await subtle.deriveBits({ name: "X25519", public: alice.publicKey }, bob.privateKey, 256));
                  results.push(aliceShared.length === 32 && aliceShared.every((byte, index) => byte === bobShared[index]));
                  const xSpki = await subtle.exportKey("spki", alice.publicKey);
                  await subtle.importKey("spki", xSpki, { name: "X25519" }, true, []);

                  const rsa = await subtle.generateKey(
                    { name: "RSASSA-PKCS1-v1_5", modulusLength: 1024, publicExponent: new Uint8Array([1, 0, 1]), hash: "SHA-256" },
                    true,
                    ["sign", "verify"],
                  );
                  const rsaSignature = await subtle.sign("RSASSA-PKCS1-v1_5", rsa.privateKey, data);
                  results.push(await subtle.verify("RSASSA-PKCS1-v1_5", rsa.publicKey, rsaSignature, data));

                  const password = await subtle.importKey("raw", data, "PBKDF2", false, ["deriveBits"]);
                  const pbkdf2 = await subtle.deriveBits({ name: "PBKDF2", hash: "SHA-256", salt: data, iterations: 1000 }, password, 256);
                  results.push(pbkdf2.byteLength === 32);
                  const ikm = await subtle.importKey("raw", data, "HKDF", false, ["deriveBits"]);
                  const hkdf = await subtle.deriveBits({ name: "HKDF", hash: "SHA-256", salt: data, info: data }, ikm, 128);
                  results.push(hkdf.byteLength === 16);

                  const kek = await subtle.generateKey({ name: "AES-KW", length: 128 }, true, ["wrapKey", "unwrapKey"]);
                  const aes = await subtle.generateKey({ name: "AES-GCM", length: 256 }, true, ["encrypt", "decrypt"]);
                  const wrapped = await subtle.wrapKey("raw", aes, kek, "AES-KW");
                  const unwrapped = await subtle.unwrapKey("raw", wrapped, kek, "AES-KW", "AES-GCM", true, ["encrypt"]);
                  results.push(unwrapped.algorithm.length === 256);

                  const jwk = await subtle.exportKey("jwk", aes);
                  const reimported = await subtle.importKey("jwk", jwk, "AES-GCM", true, ["decrypt"]);
                  const iv = new Uint8Array(12);
                  const ciphertext = await subtle.encrypt({ name: "AES-GCM", iv }, aes, data);
                  results.push(new TextDecoder().decode(await subtle.decrypt({ name: "AES-GCM", iv }, reimported, ciphertext)) === "dd");

                  const other = await subtle.generateKey({ name: "AES-GCM", length: 256 }, false, ["decrypt"]);
                  try {
                    await subtle.decrypt({ name: "AES-GCM", iv }, other, ciphertext);
                    results.push("decrypt with the wrong key succeeded");
                  } catch (error) {
                    results.push(error instanceof DOMException && error.name === "OperationError");
                  }

                  return results.join(",");
                })()
                "#,
            )
            .expect("crypto script should run");
        let result = runtime
            .block_on(js_runtime.resolve(result))
            .expect("crypto script should resolve");
        assert_eq!(
            string(&mut js_runtime, result),
            "true,true,true,true,true,true,true,true,true,true"
        );
    }

    #[test]
    #[serial]
    fn webassembly_streaming_compiles_responses() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime should build");
        let snapshot = runtime
            .block_on(build_bootstrap_snapshot())
            .expect("bootstrap snapshot should build");
        let mut js_runtime = new_runtime_from_snapshot(snapshot, false, ModuleRegistry::default())
            .expect("runtime should start from snapshot");
        // (module (func (export "add") (param i32 i32) (result i32) local.get 0 local.get 1 i32.add))
        let result = js_runtime
            .execute_script(
                "<dd:test>",
                r#"
                (async () => {
                  const bytes = new Uint8Array([0,97,115,109,1,0,0,0,1,7,1,96,2,127,127,1,127,3,2,1,0,7,7,1,3,97,100,100,0,0,10,9,1,7,0,32,0,32,1,106,11]);
                  const wasm = (body) => new Response(body, { headers: { "content-type": "application/wasm" } });
                  const { instance } = await WebAssembly.instantiateStreaming(wasm(bytes));
                  const module = await WebAssembly.compileStreaming(Promise.resolve(wasm(bytes)));
                  let rejected = "no";
                  try {
                    await WebAssembly.compileStreaming(new Response(bytes));
                  } catch (error) {
                    rejected = error.message;
                  }
                  return [instance.exports.add(19, 23), module instanceof WebAssembly.Module, rejected].join("|");
                })()
                "#,
            )
            .expect("wasm script should run");
        let result = runtime
            .block_on(js_runtime.resolve(result))
            .expect("wasm script should resolve");
        assert_eq!(
            string(&mut js_runtime, result),
            "42|true|Invalid WebAssembly content type"
        );
    }

    #[test]
    #[serial]
    fn unscoped_fetch_is_a_development_runtime_capability() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime should build");
        let _entered = runtime.enter();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("listener");
        let address = listener.local_addr().expect("address");
        let server = std::thread::spawn(move || {
            use std::io::{Read, Write};
            let (mut socket, _) = listener.accept().expect("accept");
            let mut request = [0u8; 4096];
            let _ = socket.read(&mut request);
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\ncontent-length: 7\r\nconnection: close\r\n\r\nmodules",
                )
                .expect("respond");
        });
        let snapshot = runtime
            .block_on(build_bootstrap_snapshot())
            .expect("bootstrap snapshot should build");
        let source = format!(
            r#"
            const body = await globalThis.__dd_raw_host_fetch("http://{address}/module").then((response) => response.text());
            export default {{ fetch() {{ return new Response(body); }} }};
            "#
        );

        let dev = IsolatePolicy {
            unscoped_fetch: true,
            ..IsolatePolicy::default()
        };
        let mut js_runtime =
            new_runtime_from_snapshot_with_heap_limit(snapshot, dev, 0, ModuleRegistry::default())
                .expect("dev runtime should start");
        runtime
            .block_on(load_worker(&mut js_runtime, &source))
            .expect("module evaluation should fetch from the dev server");
        let body = execute_internal(
            &mut js_runtime,
            "return Promise.resolve(__bootstrap.dd.worker.fetch(new Request('http://worker/'))).then((response) => response.text())",
        );
        let body = runtime
            .block_on(js_runtime.resolve(body))
            .expect("worker fetch should resolve");
        assert_eq!(string(&mut js_runtime, body), "modules");
        server.join().expect("server thread");
        drop(js_runtime);

        let mut js_runtime = new_runtime_from_snapshot(snapshot, false, ModuleRegistry::default())
            .expect("runtime should start");
        let refused = execute_internal(
            &mut js_runtime,
            r#"
            return typeof globalThis.__dd_raw_host_fetch === "undefined"
              ? __bootstrap.dd.unscopedFetch("http://127.0.0.1:9/").then(() => "fetched", (error) => error.message)
              : "exposed";
            "#,
        );
        let refused = runtime
            .block_on(js_runtime.resolve(refused))
            .expect("probe should resolve");
        assert_eq!(
            string(&mut js_runtime, refused),
            "fetch outside a request is only available in the development runtime"
        );
    }

    #[test]
    fn bare_imports_need_a_relative_prefix() {
        assert_eq!(
            resolve_import("./b.js", "file:///dd/a.js")
                .unwrap()
                .as_str(),
            "file:///dd/b.js"
        );
        assert_eq!(
            resolve_import("https://example.com/x.js", "file:///dd/a.js")
                .unwrap()
                .as_str(),
            "https://example.com/x.js"
        );
        assert_eq!(
            resolve_import("lodash", "file:///dd/a.js").unwrap_err(),
            "Relative import path \"lodash\" not prefixed with / or ./ or ../ from \"file:///dd/a.js\""
        );
    }
}
