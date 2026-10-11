//! Isolate construction and worker execution.

mod loader;

use loader::*;

use crate::RuntimeConfig;
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
                ("ext:web/init.js", WEB_INIT_JS),
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
    /// The most one op argument may copy out of JavaScript (see
    /// [`op_argument_budget`]); 0 keeps dd_v8's default.
    pub max_op_argument_bytes: usize,
}

impl IsolatePolicy {
    /// The policy a deployed worker's isolates run under with `config`.
    pub fn for_config(config: &RuntimeConfig) -> Self {
        Self {
            allow_code_generation: config.debug_code_generation,
            unscoped_fetch: config.dev_unscoped_fetch,
            expose_internals: config.expose_internals,
            max_op_argument_bytes: op_argument_budget(
                config.max_request_body_bytes,
                config.max_response_body_bytes,
            ),
        }
    }
}

/// What one op argument may copy into Rust: room for the largest request or
/// response body the runtime accepts, and never less than dd_v8's default.
fn op_argument_budget(max_request_body_bytes: usize, max_response_body_bytes: usize) -> usize {
    dd_v8::serde_v8::DEFAULT_MAX_OP_ARGUMENT_BYTES
        .max(max_request_body_bytes)
        .max(max_response_body_bytes)
        .saturating_add(1024 * 1024)
}

pub fn ensure_v8_flags(flags: &[String]) -> Result<()> {
    dd_v8::set_flags(flags).map_err(PlatformError::internal)
}

#[cfg(test)]
async fn validate_worker(
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
fn new_runtime_from_snapshot(
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

async fn load_worker(runtime: &mut JsRuntime, source: &str) -> Result<()> {
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
    set_time: Rc<v8::Global<v8::Function>>,
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
            set_time: internal_function(scope, dd, "setTime")?,
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
        max_op_argument_bytes: policy.max_op_argument_bytes,
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
    sync_isolate_clock(&mut runtime)?;
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

/// Sets the worker's frozen clock to now. The snapshot keeps the clock as it
/// read while the snapshot was built, so without this `Date.now()` and
/// `performance.now()` would report that moment until the first timer or
/// I/O boundary.
fn sync_isolate_clock(runtime: &mut JsRuntime) -> Result<()> {
    let boundary = crate::ops::current_time_boundary();
    let set_time = cached_entrypoint(runtime, |entrypoints| Rc::clone(&entrypoints.set_time));
    dd_v8::scope!(scope, runtime);
    let set_time = v8::Local::new(scope, set_time.as_ref());
    let now = v8::Number::new(scope, boundary.now_ms as f64).into();
    let perf = v8::Number::new(scope, boundary.perf_ms).into();
    v8::tc_scope!(let scope, scope);
    let receiver = v8::undefined(scope).into();
    set_time
        .call(scope, receiver, &[now, perf])
        .ok_or_else(|| PlatformError::runtime("runtime entrypoint setTime threw"))?;
    Ok(())
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
mod tests;
