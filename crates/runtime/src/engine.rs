use crate::assets::{
    install_worker_js, BOOTSTRAP_JS, BOOTSTRAP_SPECIFIER, INSTALL_SPECIFIER, WORKER_SPECIFIER,
};
use crate::dynamic_modules::{
    dynamic_module, dynamic_module_source, normalize_dynamic_module_path,
    resolve_dynamic_module_path, RuntimeModuleKind,
};
use crate::ops::{
    clear_request_invocation, clear_worker_deployment_config, register_request_invocation,
    register_worker_deployment_config, runtime_extension, WorkerDeploymentPayload,
    WorkerRequestPayload, WorkerSource,
};
use crate::service::{HostRpcExecutionCall, MemoryExecutionCall};
use base64::Engine;
use common::{PlatformError, Result, WorkerInvocation};
use deno_core::{
    resolve_import, v8, v8_set_flags, Extension, JsRuntime, JsRuntimeForSnapshot, ModuleCodeBytes,
    ModuleLoadResponse, ModuleLoader, ModuleSource, ModuleSourceCode, ModuleSpecifier, ModuleType,
    PollEventLoopOptions, RequestedModuleType, ResolutionKind, RuntimeOptions,
};
use deno_crypto::deno_crypto as deno_crypto_ext;
use deno_error::JsErrorBox;
use deno_fetch::Options as DenoFetchOptions;
use deno_web::{BlobStore, InMemoryBroadcastChannel};
use std::borrow::Cow;
use std::mem;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::OnceLock;
use std::task::{Context, Poll, Waker};

include!(concat!(env!("OUT_DIR"), "/dd_deno_js_extension.rs"));

static CONFIGURED_V8_FLAGS: OnceLock<Vec<String>> = OnceLock::new();

pub async fn build_bootstrap_snapshot() -> Result<&'static [u8]> {
    let mut runtime = JsRuntimeForSnapshot::new(RuntimeOptions {
        extensions: runtime_extensions(),
        module_loader: Some(Rc::new(RuntimeModuleLoader)),
        ..Default::default()
    });
    runtime
        .execute_script(BOOTSTRAP_SPECIFIER, BOOTSTRAP_JS)
        .map_err(runtime_error)?;
    let snapshot = runtime.snapshot();
    Ok(Box::leak(snapshot))
}

pub fn ensure_v8_flags(flags: &[String]) -> Result<()> {
    let normalized = flags
        .iter()
        .map(|flag| flag.trim())
        .filter(|flag| !flag.is_empty())
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();

    if let Some(existing) = CONFIGURED_V8_FLAGS.get() {
        if existing != &normalized {
            return Err(PlatformError::internal(format!(
                "v8 flags were already initialized as {:?}; cannot reinitialize with {:?}",
                existing, normalized
            )));
        }
        return Ok(());
    }

    let mut argv = Vec::with_capacity(normalized.len() + 1);
    argv.push("dd-runtime".to_string());
    argv.extend(normalized.iter().cloned());
    let leftovers = v8_set_flags(argv);
    if leftovers.len() > 1 {
        return Err(PlatformError::internal(format!(
            "unsupported v8 flags: {:?}",
            &leftovers[1..]
        )));
    }

    let _ = CONFIGURED_V8_FLAGS.set(normalized);
    Ok(())
}

pub async fn validate_worker(
    bootstrap_snapshot: &'static [u8],
    source: &str,
    allow_code_generation: bool,
) -> Result<()> {
    let mut runtime = new_runtime(bootstrap_snapshot, allow_code_generation, 0)?;
    load_worker(&mut runtime, source).await
}

#[cfg_attr(not(test), allow(dead_code))]
pub async fn build_worker_snapshot(
    _bootstrap_snapshot: &'static [u8],
    source: &str,
    allow_code_generation: bool,
) -> Result<Box<[u8]>> {
    let mut runtime = JsRuntimeForSnapshot::new(RuntimeOptions {
        extensions: runtime_extensions(),
        module_loader: Some(Rc::new(RuntimeModuleLoader)),
        ..Default::default()
    });
    runtime
        .execute_script(BOOTSTRAP_SPECIFIER, BOOTSTRAP_JS)
        .map_err(runtime_error)?;
    set_snapshot_code_generation_from_strings(&mut runtime, allow_code_generation);
    evaluate_snapshot_module(&mut runtime, WORKER_SPECIFIER, source, false).await?;
    let install_code = install_worker_js();
    evaluate_snapshot_module(&mut runtime, INSTALL_SPECIFIER, &install_code, true).await?;
    Ok(runtime.snapshot())
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn validate_loaded_worker_runtime(
    startup_snapshot: &'static [u8],
    allow_code_generation: bool,
) -> Result<()> {
    let mut runtime = new_runtime(startup_snapshot, allow_code_generation, 0)?;
    runtime
        .execute_script(
            "<dd:worker-snapshot-validate>",
            r#"
            (() => {
              if (!globalThis.__dd_worker || typeof globalThis.__dd_worker.fetch !== "function") {
                throw new Error("worker install did not expose fetch");
              }
              const request = new Request("http://worker/");
              const response = Response.json({ ok: true });
              const result = {
                url: request.url,
                status: response.status,
                contentType: response.headers.get("content-type"),
              };
              globalThis.__dd_worker_snapshot_validation = JSON.stringify(result);
            })();
            "#,
        )
        .map_err(runtime_error)?;
    Ok(())
}

pub fn new_runtime_from_snapshot(
    startup_snapshot: &'static [u8],
    allow_code_generation: bool,
) -> Result<JsRuntime> {
    new_runtime(startup_snapshot, allow_code_generation, 0)
}

pub fn new_runtime_from_snapshot_with_heap_limit(
    startup_snapshot: &'static [u8],
    allow_code_generation: bool,
    max_heap_bytes: usize,
) -> Result<JsRuntime> {
    new_runtime(startup_snapshot, allow_code_generation, max_heap_bytes)
}

pub async fn load_worker(runtime: &mut JsRuntime, source: &str) -> Result<()> {
    evaluate_module(runtime, WORKER_SPECIFIER, source, false).await?;
    let install_code = install_worker_js();
    evaluate_module(runtime, INSTALL_SPECIFIER, &install_code, true).await
}

pub async fn load_worker_source(runtime: &mut JsRuntime, source: &WorkerSource) -> Result<()> {
    let source = worker_source_text(source)?;
    load_worker(runtime, source.as_ref()).await
}

struct RuntimeEntrypoints {
    install_worker_deployment_handle: Rc<v8::Global<v8::Function>>,
    execute_worker_handle: Rc<v8::Global<v8::Function>>,
    abort_worker_request_handle: Rc<v8::Global<v8::Function>>,
    drain_dynamic_control_queue: Rc<v8::Global<v8::Function>>,
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
        "__dd_install_worker_deployment_handle",
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

pub fn cache_runtime_entrypoints(runtime: &mut JsRuntime) -> Result<()> {
    let entrypoints = {
        let context = runtime.main_context();
        deno_core::scope!(scope, runtime);
        let context = v8::Local::new(scope, context);
        let global = context.global(scope);
        let install_worker_deployment_handle =
            global_function(scope, global, "__dd_install_worker_deployment_handle")?;
        let execute_worker_handle = global_function(scope, global, "__dd_execute_worker_handle")?;
        let abort_worker_request_handle =
            global_function(scope, global, "__dd_abort_worker_request_handle")?;
        let drain_dynamic_control_queue =
            global_function(scope, global, "__dd_drain_dynamic_control_queue_handle")?;
        RuntimeEntrypoints {
            install_worker_deployment_handle: Rc::new(v8::Global::new(
                scope,
                install_worker_deployment_handle,
            )),
            execute_worker_handle: Rc::new(v8::Global::new(scope, execute_worker_handle)),
            abort_worker_request_handle: Rc::new(v8::Global::new(
                scope,
                abort_worker_request_handle,
            )),
            drain_dynamic_control_queue: Rc::new(v8::Global::new(
                scope,
                drain_dynamic_control_queue,
            )),
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
    pub host_rpc_call: Option<&'a HostRpcExecutionCall>,
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
        host_rpc_call,
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
            host_rpc_call: host_rpc_call.cloned(),
            request_body_stream_handle,
            request_headers_handle,
            request_body_handle,
            stream_response,
            method: mem::take(&mut request.method),
            url: mem::take(&mut request.url),
            input_request_id: mem::take(&mut request.request_id),
        };
        register_request_invocation(&mut op_state, payload)
    };

    match call_cached_u32_function(
        runtime,
        "__dd_execute_worker_handle",
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
        "__dd_abort_worker_request_handle",
        request_context_handle,
        |entrypoints| Rc::clone(&entrypoints.abort_worker_request_handle),
    )
}

pub fn drain_dynamic_control_queue(runtime: &mut JsRuntime) -> Result<()> {
    call_cached_noarg_function(
        runtime,
        "__dd_drain_dynamic_control_queue_handle",
        |entrypoints| Rc::clone(&entrypoints.drain_dynamic_control_queue),
    )
}

pub fn pump_event_loop_once(runtime: &mut JsRuntime, waker: &Waker) -> Result<()> {
    let mut cx = Context::from_waker(waker);
    match runtime.poll_event_loop(&mut cx, PollEventLoopOptions::default()) {
        Poll::Ready(Ok(())) | Poll::Pending => Ok(()),
        Poll::Ready(Err(error)) => Err(runtime_error(error)),
    }
}

fn new_runtime(
    startup_snapshot: &'static [u8],
    allow_code_generation: bool,
    max_heap_bytes: usize,
) -> Result<JsRuntime> {
    let create_params =
        (max_heap_bytes > 0).then(|| v8::CreateParams::default().heap_limits(0, max_heap_bytes));
    let mut runtime = JsRuntime::try_new(RuntimeOptions {
        extensions: runtime_extensions(),
        module_loader: Some(Rc::new(RuntimeModuleLoader)),
        startup_snapshot: Some(startup_snapshot),
        create_params,
        ..Default::default()
    })
    .map_err(runtime_error)?;
    set_code_generation_from_strings(&mut runtime, allow_code_generation);
    Ok(runtime)
}

fn set_code_generation_from_strings(runtime: &mut JsRuntime, allow: bool) {
    let context = runtime.main_context();
    deno_core::scope!(scope, runtime);
    let context = v8::Local::new(scope, context);
    context.set_allow_generation_from_strings(allow);
}

fn set_snapshot_code_generation_from_strings(runtime: &mut JsRuntimeForSnapshot, allow: bool) {
    let context = runtime.main_context();
    deno_core::scope!(scope, runtime);
    let context = v8::Local::new(scope, context);
    context.set_allow_generation_from_strings(allow);
}

fn call_cached_u32_function(
    runtime: &mut JsRuntime,
    name: &str,
    arg: u32,
    select: impl FnOnce(&RuntimeEntrypoints) -> Rc<v8::Global<v8::Function>>,
) -> Result<()> {
    let function = cached_entrypoint(runtime, select);
    let context = runtime.main_context();
    deno_core::scope!(scope, runtime);
    let context = v8::Local::new(scope, context);
    let global = context.global(scope);
    let function = v8::Local::new(scope, function.as_ref());
    let arg = v8::Integer::new_from_unsigned(scope, arg).into();
    function
        .call(scope, global.into(), &[arg])
        .ok_or_else(|| PlatformError::runtime(format!("global runtime entrypoint {name} threw")))?;
    Ok(())
}

fn call_cached_noarg_function(
    runtime: &mut JsRuntime,
    name: &str,
    select: impl FnOnce(&RuntimeEntrypoints) -> Rc<v8::Global<v8::Function>>,
) -> Result<()> {
    let function = cached_entrypoint(runtime, select);
    let context = runtime.main_context();
    deno_core::scope!(scope, runtime);
    let context = v8::Local::new(scope, context);
    let global = context.global(scope);
    let function = v8::Local::new(scope, function.as_ref());
    function
        .call(scope, global.into(), &[])
        .ok_or_else(|| PlatformError::runtime(format!("global runtime entrypoint {name} threw")))?;
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

fn global_function<'scope>(
    scope: &mut v8::PinScope<'scope, '_>,
    global: v8::Local<'scope, v8::Object>,
    name: &str,
) -> Result<v8::Local<'scope, v8::Function>> {
    let name_value = v8::String::new(scope, name)
        .ok_or_else(|| PlatformError::runtime("failed to allocate V8 function name"))?;
    let value = global.get(scope, name_value.into()).ok_or_else(|| {
        PlatformError::runtime(format!("global runtime entrypoint {name} is unavailable"))
    })?;
    let function = v8::Local::<v8::Function>::try_from(value).map_err(|_| {
        PlatformError::runtime(format!(
            "global runtime entrypoint {name} is not a function"
        ))
    })?;
    Ok(function)
}

fn runtime_extensions() -> Vec<Extension> {
    vec![
        without_esm(deno_webidl::deno_webidl::init()),
        without_esm(deno_web::deno_web::init(
            Arc::new(BlobStore::default()),
            None,
            InMemoryBroadcastChannel::default(),
        )),
        without_esm(deno_fetch::deno_fetch::init(DenoFetchOptions::default())),
        without_esm(deno_crypto_ext::init(None)),
        dd_deno_js::init(),
        runtime_extension(),
    ]
}

fn without_esm(extension: Extension) -> Extension {
    Extension {
        js_files: Cow::Borrowed(&[]),
        lazy_loaded_esm_files: Cow::Borrowed(&[]),
        esm_files: Cow::Borrowed(&[]),
        esm_entry_point: None,
        ..extension
    }
}

struct RuntimeModuleLoader;

impl ModuleLoader for RuntimeModuleLoader {
    fn resolve(
        &self,
        specifier: &str,
        referrer: &str,
        _kind: ResolutionKind,
    ) -> std::result::Result<ModuleSpecifier, JsErrorBox> {
        if referrer.starts_with("dd-dynamic:")
            && !specifier.starts_with("//")
            && !has_url_scheme(specifier)
        {
            return resolve_dd_dynamic_import(specifier, referrer);
        }
        resolve_import(specifier, referrer).map_err(JsErrorBox::from_err)
    }

    fn load(
        &self,
        module_specifier: &ModuleSpecifier,
        _maybe_referrer: Option<&deno_core::ModuleLoadReferrer>,
        options: deno_core::ModuleLoadOptions,
    ) -> ModuleLoadResponse {
        ModuleLoadResponse::Sync(match module_specifier.scheme() {
            "dd-dynamic" => load_dd_dynamic_module(module_specifier, options.requested_module_type),
            _ => Err(JsErrorBox::generic(format!(
                "dynamic module loader only supports dd-dynamic: URLs, got {module_specifier}"
            ))),
        })
    }
}

fn resolve_dd_dynamic_import(
    specifier: &str,
    referrer: &str,
) -> std::result::Result<ModuleSpecifier, JsErrorBox> {
    let referrer = ModuleSpecifier::parse(referrer).map_err(JsErrorBox::from_err)?;
    let (graph_id, referrer_path) = dd_dynamic_module_parts(&referrer)?;
    let module_path =
        resolve_dynamic_module_path(&referrer_path, specifier).map_err(JsErrorBox::generic)?;
    let encoded_path = module_path
        .split('/')
        .map(percent_encode_path_segment)
        .collect::<Vec<_>>()
        .join("/");
    ModuleSpecifier::parse(&format!("dd-dynamic://graph/{graph_id}/{encoded_path}"))
        .map_err(JsErrorBox::from_err)
}

fn load_dd_dynamic_module(
    module_specifier: &ModuleSpecifier,
    requested_module_type: RequestedModuleType,
) -> std::result::Result<ModuleSource, JsErrorBox> {
    let (graph_id, module_path) = dd_dynamic_module_parts(module_specifier)?;
    let module = dynamic_module(&graph_id, &module_path).ok_or_else(|| {
        JsErrorBox::generic(format!(
            "dynamic module graph {graph_id} does not contain module: {module_path}"
        ))
    })?;
    let module_type = deno_module_type(module.kind);
    if requested_module_type != module_type.clone() {
        return Err(JsErrorBox::generic(format!(
            "requested module type {requested_module_type} does not match {module_type} module: {module_path}"
        )));
    }
    let code = match module.kind {
        RuntimeModuleKind::JavaScript => ModuleSourceCode::String(
            String::from_utf8(module.code.as_ref().to_vec())
                .map_err(|error| {
                    JsErrorBox::generic(format!(
                        "dynamic JavaScript module is not valid UTF-8: {module_path}: {error}"
                    ))
                })?
                .into(),
        ),
        RuntimeModuleKind::Wasm => {
            ModuleSourceCode::String(compiled_wasm_module_source(module.code.as_ref()))
        }
        RuntimeModuleKind::Json | RuntimeModuleKind::Text => ModuleSourceCode::String(
            String::from_utf8(module.code.as_ref().to_vec())
                .map_err(|error| {
                    JsErrorBox::generic(format!(
                        "dynamic text module is not valid UTF-8: {module_path}: {error}"
                    ))
                })?
                .into(),
        ),
        RuntimeModuleKind::Bytes => ModuleSourceCode::Bytes(ModuleCodeBytes::Arc(module.code)),
    };
    Ok(ModuleSource::new(module_type, code, module_specifier, None))
}

fn deno_module_type(kind: RuntimeModuleKind) -> ModuleType {
    match kind {
        RuntimeModuleKind::JavaScript => ModuleType::JavaScript,
        RuntimeModuleKind::Wasm => ModuleType::JavaScript,
        RuntimeModuleKind::Json => ModuleType::Json,
        RuntimeModuleKind::Text => ModuleType::Text,
        RuntimeModuleKind::Bytes => ModuleType::Bytes,
    }
}

fn compiled_wasm_module_source(bytes: &[u8]) -> deno_core::ModuleCodeString {
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
    .into()
}

fn dd_dynamic_module_parts(
    module_specifier: &ModuleSpecifier,
) -> std::result::Result<(String, String), JsErrorBox> {
    if module_specifier.scheme() != "dd-dynamic" {
        return Err(JsErrorBox::generic(format!(
            "expected dd-dynamic module URL, got {module_specifier}"
        )));
    }
    if module_specifier.host_str() != Some("graph") {
        return Err(JsErrorBox::generic(format!(
            "expected dd-dynamic://graph module URL, got {module_specifier}"
        )));
    }
    let path = module_specifier.path().trim_start_matches('/');
    let (graph_id, module_path) = path.split_once('/').ok_or_else(|| {
        JsErrorBox::generic(format!(
            "dd-dynamic module URL is missing graph id or module path: {module_specifier}"
        ))
    })?;
    let graph_id = graph_id.trim();
    if graph_id.is_empty()
        || graph_id.len() > 128
        || !graph_id.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(JsErrorBox::generic(format!(
            "invalid dd-dynamic graph id in module URL: {module_specifier}"
        )));
    }
    let bytes = percent_decode(module_path).map_err(JsErrorBox::generic)?;
    let path = String::from_utf8(bytes).map_err(|error| {
        JsErrorBox::generic(format!("invalid UTF-8 in dd-dynamic module path: {error}"))
    })?;
    normalize_dynamic_module_path(&path)
        .map(|path| (graph_id.to_string(), path))
        .map_err(JsErrorBox::generic)
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

fn worker_source_text(source: &WorkerSource) -> Result<Cow<'_, str>> {
    match source {
        WorkerSource::Inline(source) => Ok(Cow::Borrowed(source.as_ref())),
        WorkerSource::DynamicModule {
            graph_id,
            entrypoint,
        } => {
            let specifier = dynamic_module_entrypoint_specifier(graph_id, entrypoint)?;
            Ok(Cow::Owned(format!(
                "export {{ default }} from {specifier:?};\n"
            )))
        }
    }
}

fn dynamic_module_entrypoint_specifier(graph_id: &str, entrypoint: &str) -> Result<String> {
    let graph_id = graph_id.trim();
    if graph_id.is_empty()
        || graph_id.len() > 128
        || !graph_id.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(PlatformError::bad_request(
            "dynamic module graph id is invalid",
        ));
    }
    let entrypoint = normalize_dynamic_module_path(entrypoint).map_err(|error| {
        PlatformError::bad_request(format!("invalid dynamic module entrypoint: {error}"))
    })?;
    if dynamic_module_source(graph_id, &entrypoint).is_none() {
        return Err(PlatformError::bad_request(format!(
            "dynamic module graph {graph_id} does not contain entrypoint: {entrypoint}"
        )));
    }
    let encoded_path = entrypoint
        .split('/')
        .map(percent_encode_path_segment)
        .collect::<Vec<_>>()
        .join("/");
    Ok(format!("dd-dynamic://graph/{graph_id}/{encoded_path}"))
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

async fn evaluate_module(
    runtime: &mut JsRuntime,
    specifier: &str,
    source: &str,
    is_main: bool,
) -> Result<()> {
    let specifier = ModuleSpecifier::parse(specifier)
        .map_err(|error| PlatformError::runtime(error.to_string()))?;
    let module_id = if is_main {
        runtime
            .load_main_es_module_from_code(&specifier, source.to_string())
            .await
            .map_err(runtime_error)?
    } else {
        runtime
            .load_side_es_module_from_code(&specifier, source.to_string())
            .await
            .map_err(runtime_error)?
    };

    let evaluation = runtime.mod_evaluate(module_id);
    runtime
        .run_event_loop(PollEventLoopOptions::default())
        .await
        .map_err(runtime_error)?;
    evaluation.await.map_err(runtime_error)?;
    Ok(())
}

#[cfg_attr(not(test), allow(dead_code))]
async fn evaluate_snapshot_module(
    runtime: &mut JsRuntimeForSnapshot,
    specifier: &str,
    source: &str,
    is_main: bool,
) -> Result<()> {
    let specifier = ModuleSpecifier::parse(specifier)
        .map_err(|error| PlatformError::runtime(error.to_string()))?;
    let module_id = if is_main {
        runtime
            .load_main_es_module_from_code(&specifier, source.to_string())
            .await
            .map_err(runtime_error)?
    } else {
        runtime
            .load_side_es_module_from_code(&specifier, source.to_string())
            .await
            .map_err(runtime_error)?
    };

    let evaluation = runtime.mod_evaluate(module_id);
    runtime
        .run_event_loop(PollEventLoopOptions::default())
        .await
        .map_err(runtime_error)?;
    evaluation.await.map_err(runtime_error)?;
    Ok(())
}

fn runtime_error(error: impl std::fmt::Display) -> PlatformError {
    PlatformError::runtime(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    async fn resolve_with_event_loop(
        js_runtime: &mut JsRuntime,
        value: deno_core::v8::Global<deno_core::v8::Value>,
    ) -> std::result::Result<deno_core::v8::Global<deno_core::v8::Value>, deno_core::error::CoreError>
    {
        let promise = {
            deno_core::scope!(scope, js_runtime);
            JsRuntime::scoped_resolve(scope, value)
        };
        js_runtime
            .with_event_loop_promise(promise, PollEventLoopOptions::default())
            .await
    }

    fn simple_worker_source() -> &'static str {
        r#"
        export default {
          async fetch(_request) {
            return new Response("ok");
          }
        };
        "#
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
        let _ =
            new_runtime_from_snapshot(snapshot, false).expect("runtime should start from snapshot");
    }

    #[tokio::test]
    #[serial]
    async fn worker_snapshot_builds_from_bootstrap_snapshot() {
        let snapshot = build_bootstrap_snapshot()
            .await
            .expect("bootstrap snapshot should build");
        validate_worker(snapshot, simple_worker_source(), false)
            .await
            .expect("worker should validate");
        let worker_snapshot = build_worker_snapshot(snapshot, simple_worker_source(), false)
            .await
            .expect("worker snapshot should build");
        let worker_snapshot = Box::leak(worker_snapshot);
        validate_loaded_worker_runtime(worker_snapshot, false)
            .expect("worker snapshot should validate");
    }

    #[test]
    #[serial]
    fn deno_fetch_classes_work_from_bootstrap_snapshot() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime should build");
        let snapshot = runtime
            .block_on(build_bootstrap_snapshot())
            .expect("bootstrap snapshot should build");
        let mut js_runtime =
            new_runtime_from_snapshot(snapshot, false).expect("runtime should start from snapshot");

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
                globalThis.__dd_test_ctor_match = JSON.stringify({
                  fetch: typeof globalThis.fetch === "function"
                    && globalThis.fetch === globalThis.__dd_deno_runtime.fetch,
                  request: globalThis.Request === globalThis.__dd_deno_runtime.Request,
                  headers: globalThis.Headers === globalThis.__dd_deno_runtime.Headers,
                  response: globalThis.Response === globalThis.__dd_deno_runtime.Response,
                  formData: globalThis.FormData === globalThis.__dd_deno_runtime.FormData,
                });
                "#,
            )
            .expect("fetch classes should construct");

        let request_url = js_runtime
            .execute_script("<dd:test>", "globalThis.__dd_test_request.url")
            .expect("request url should execute");
        let response_text_promise = js_runtime
            .execute_script("<dd:test>", "globalThis.__dd_test_response.text()")
            .expect("response.text should execute");
        let response_content_type = js_runtime
            .execute_script(
                "<dd:test>",
                "globalThis.__dd_test_response.headers.get('content-type')",
            )
            .expect("response content-type should execute");
        let header_value = js_runtime
            .execute_script("<dd:test>", "globalThis.__dd_test_headers.get('x-dd')")
            .expect("headers get should execute");
        let form_value = js_runtime
            .execute_script("<dd:test>", "globalThis.__dd_test_form_data.get('name')")
            .expect("formdata get should execute");
        let ctor_match = js_runtime
            .execute_script("<dd:test>", "globalThis.__dd_test_ctor_match")
            .expect("constructor match should execute");
        runtime
            .block_on(async {
                js_runtime
                    .run_event_loop(PollEventLoopOptions::default())
                    .await
            })
            .expect("event loop should run");
        let request_url = runtime
            .block_on(resolve_with_event_loop(&mut js_runtime, request_url))
            .expect("request url should resolve");
        let response_text = runtime
            .block_on(resolve_with_event_loop(
                &mut js_runtime,
                response_text_promise,
            ))
            .expect("response text should resolve");
        let response_content_type = runtime
            .block_on(resolve_with_event_loop(
                &mut js_runtime,
                response_content_type,
            ))
            .expect("response content-type should resolve");
        let header_value = runtime
            .block_on(resolve_with_event_loop(&mut js_runtime, header_value))
            .expect("header value should resolve");
        let form_value = runtime
            .block_on(resolve_with_event_loop(&mut js_runtime, form_value))
            .expect("form value should resolve");
        let ctor_match = runtime
            .block_on(resolve_with_event_loop(&mut js_runtime, ctor_match))
            .expect("constructor match should resolve");
        {
            deno_core::scope!(scope, js_runtime);
            let request_url = request_url
                .open(scope)
                .to_string(scope)
                .expect("request url should stringify")
                .to_rust_string_lossy(scope);
            let response_text = response_text
                .open(scope)
                .to_string(scope)
                .expect("response text should stringify")
                .to_rust_string_lossy(scope);
            let response_content_type = response_content_type
                .open(scope)
                .to_string(scope)
                .expect("content type should stringify")
                .to_rust_string_lossy(scope);
            let header_value = header_value
                .open(scope)
                .to_string(scope)
                .expect("header value should stringify")
                .to_rust_string_lossy(scope);
            let form_value = form_value
                .open(scope)
                .to_string(scope)
                .expect("form value should stringify")
                .to_rust_string_lossy(scope);
            let ctor_match = ctor_match
                .open(scope)
                .to_string(scope)
                .expect("constructor match should stringify")
                .to_rust_string_lossy(scope);
            assert_eq!(request_url, "http://example.com/test");
            assert_eq!(response_text, r#"{"ok":true}"#);
            assert_eq!(response_content_type, "application/json");
            assert_eq!(header_value, "ok");
            assert_eq!(form_value, "value");
            assert_eq!(
                ctor_match,
                r#"{"fetch":true,"request":true,"headers":true,"response":true,"formData":true}"#
            );
        }
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
        let mut js_runtime = new_runtime_from_snapshot(snapshot, false)
            .expect("runtime should start from bootstrap snapshot");
        runtime
            .block_on(load_worker(&mut js_runtime, simple_worker_source()))
            .expect("worker should load into runtime");

        let response_promise = js_runtime
            .execute_script(
                "<dd:test>",
                r#"
                globalThis.__dd_test_direct_fetch = globalThis.__dd_worker.fetch(
                  new Request("http://worker/"),
                  {},
                  { waitUntil() {} },
                );
                globalThis.__dd_test_direct_fetch.then((response) => String(response.status));
                "#,
            )
            .expect("direct worker fetch should execute");
        runtime
            .block_on(async {
                js_runtime
                    .run_event_loop(PollEventLoopOptions::default())
                    .await
            })
            .expect("event loop should run");
        let response_value = runtime
            .block_on(resolve_with_event_loop(&mut js_runtime, response_promise))
            .expect("response promise should resolve");
        deno_core::scope!(scope, js_runtime);
        let response_json = response_value
            .open(scope)
            .to_string(scope)
            .expect("response should stringify")
            .to_rust_string_lossy(scope);
        assert_eq!(response_json, "200");
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
        let mut js_runtime = new_runtime_from_snapshot(snapshot, false)
            .expect("runtime should start from bootstrap snapshot");
        runtime
            .block_on(load_worker(&mut js_runtime, simple_worker_source()))
            .expect("worker should load into runtime");

        let response_value = js_runtime
            .execute_script(
                "<dd:test>",
                r#"
                String(new Response("ok").status)
                "#,
            )
            .expect("response constructor should execute");
        deno_core::scope!(scope, js_runtime);
        let response_value = response_value
            .open(scope)
            .to_string(scope)
            .expect("response should stringify")
            .to_rust_string_lossy(scope);
        assert_eq!(response_value, "200");
    }
}
