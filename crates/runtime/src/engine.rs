use crate::assets::{
    abort_worker_js, execute_worker_js, install_worker_js, BOOTSTRAP_JS, BOOTSTRAP_SPECIFIER,
    INSTALL_SPECIFIER, WORKER_SPECIFIER,
};
use crate::ops::runtime_extension;
use common::{PlatformError, Result, WorkerInvocation};
use deno_core::{
    JsRuntime, JsRuntimeForSnapshot, ModuleSpecifier, PollEventLoopOptions, RuntimeOptions,
};
use std::ptr;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

pub async fn build_bootstrap_snapshot() -> Result<&'static [u8]> {
    let mut runtime = JsRuntimeForSnapshot::new(RuntimeOptions {
        extensions: vec![runtime_extension()],
        ..Default::default()
    });
    evaluate_snapshot_module(&mut runtime, BOOTSTRAP_SPECIFIER, BOOTSTRAP_JS, false).await?;
    let snapshot = runtime.snapshot();
    Ok(Box::leak(snapshot))
}

pub async fn validate_worker(bootstrap_snapshot: &'static [u8], source: &str) -> Result<()> {
    let mut runtime = new_runtime(bootstrap_snapshot)?;
    evaluate_module(&mut runtime, WORKER_SPECIFIER, source, false).await?;
    let install_code = install_worker_js();
    evaluate_module(&mut runtime, INSTALL_SPECIFIER, &install_code, true).await
}

pub async fn build_worker_snapshot(
    bootstrap_snapshot: &'static [u8],
    source: &str,
) -> Result<&'static [u8]> {
    let mut runtime = JsRuntimeForSnapshot::new(RuntimeOptions {
        extensions: vec![runtime_extension()],
        startup_snapshot: Some(bootstrap_snapshot),
        ..Default::default()
    });
    evaluate_snapshot_module(&mut runtime, WORKER_SPECIFIER, source, false).await?;
    let install_code = install_worker_js();
    evaluate_snapshot_module(&mut runtime, INSTALL_SPECIFIER, &install_code, true).await?;
    let snapshot = runtime.snapshot();
    Ok(Box::leak(snapshot))
}

pub fn new_runtime_from_snapshot(startup_snapshot: &'static [u8]) -> Result<JsRuntime> {
    new_runtime(startup_snapshot)
}

pub fn dispatch_worker_request(
    runtime: &mut JsRuntime,
    request_id: &str,
    completion_token: &str,
    worker_name: &str,
    kv_bindings: &[String],
    request: WorkerInvocation,
) -> Result<()> {
    let request_json = serde_json::to_string(&request)
        .map_err(|error| PlatformError::internal(error.to_string()))?;
    let worker_name_json = serde_json::to_string(worker_name)
        .map_err(|error| PlatformError::internal(error.to_string()))?;
    let kv_bindings_json = serde_json::to_string(kv_bindings)
        .map_err(|error| PlatformError::internal(error.to_string()))?;
    let request_id_json = serde_json::to_string(request_id)
        .map_err(|error| PlatformError::internal(error.to_string()))?;
    let completion_token_json = serde_json::to_string(completion_token)
        .map_err(|error| PlatformError::internal(error.to_string()))?;
    let entry_code = execute_worker_js(
        &worker_name_json,
        &kv_bindings_json,
        &request_id_json,
        &completion_token_json,
        &request_json,
    );
    runtime
        .execute_script("<grugd:invoke>", entry_code)
        .map_err(runtime_error)?;
    Ok(())
}

pub fn abort_worker_request(runtime: &mut JsRuntime, request_id: &str) -> Result<()> {
    let request_id_json = serde_json::to_string(request_id)
        .map_err(|error| PlatformError::internal(error.to_string()))?;
    let entry_code = abort_worker_js(&request_id_json);
    runtime
        .execute_script("<grugd:abort>", entry_code)
        .map_err(runtime_error)?;
    Ok(())
}

pub fn pump_event_loop_once(runtime: &mut JsRuntime) -> Result<()> {
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    match runtime.poll_event_loop(&mut cx, PollEventLoopOptions::default()) {
        Poll::Ready(Ok(())) | Poll::Pending => Ok(()),
        Poll::Ready(Err(error)) => Err(runtime_error(error)),
    }
}

fn new_runtime(startup_snapshot: &'static [u8]) -> Result<JsRuntime> {
    JsRuntime::try_new(RuntimeOptions {
        extensions: vec![runtime_extension()],
        startup_snapshot: Some(startup_snapshot),
        ..Default::default()
    })
    .map_err(runtime_error)
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

fn noop_waker() -> Waker {
    unsafe { Waker::from_raw(RawWaker::new(ptr::null(), &NOOP_WAKER_VTABLE)) }
}

const NOOP_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(
    noop_waker_clone,
    noop_waker_wake,
    noop_waker_wake_by_ref,
    noop_waker_drop,
);

unsafe fn noop_waker_clone(_: *const ()) -> RawWaker {
    RawWaker::new(ptr::null(), &NOOP_WAKER_VTABLE)
}

unsafe fn noop_waker_wake(_: *const ()) {}

unsafe fn noop_waker_wake_by_ref(_: *const ()) {}

unsafe fn noop_waker_drop(_: *const ()) {}
