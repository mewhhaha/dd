use crate::assets::{
    abort_worker_js, execute_worker_js, install_worker_js, BOOTSTRAP_JS, BOOTSTRAP_SPECIFIER,
    INSTALL_SPECIFIER, WORKER_SPECIFIER,
};
use crate::ops::runtime_extension;
use common::{PlatformError, Result, WorkerInvocation};
use deno_core::{
    resolve_import, Extension, JsRuntime, JsRuntimeForSnapshot, ModuleLoadResponse, ModuleLoader,
    ModuleSource, ModuleSourceCode, ModuleSpecifier, ModuleType, PollEventLoopOptions,
    RequestedModuleType, ResolutionKind, RuntimeOptions,
};
use deno_crypto::deno_crypto as deno_crypto_ext;
use deno_error::JsErrorBox;
use deno_fetch::Options as DenoFetchOptions;
use deno_web::{BlobStore, InMemoryBroadcastChannel};
use serde::Serialize;
use std::borrow::Cow;
use std::ptr;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

include!(concat!(env!("OUT_DIR"), "/dd_deno_js_extension.rs"));

#[derive(Clone, Serialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum ExecuteActorCall {
    Method {
        binding: String,
        key: String,
        name: String,
        args: Vec<u8>,
    },
    Message {
        binding: String,
        key: String,
        handle: String,
        is_text: bool,
        data: Vec<u8>,
    },
    Close {
        binding: String,
        key: String,
        handle: String,
        code: u16,
        reason: String,
    },
    #[serde(rename = "transport_datagram")]
    TransportDatagram {
        binding: String,
        key: String,
        handle: String,
        data: Vec<u8>,
    },
    #[serde(rename = "transport_stream")]
    TransportStream {
        binding: String,
        key: String,
        handle: String,
        data: Vec<u8>,
    },
    #[serde(rename = "transport_close")]
    TransportClose {
        binding: String,
        key: String,
        handle: String,
        code: u16,
        reason: String,
    },
}

#[derive(Clone, Serialize)]
pub struct ExecuteHostRpcCall {
    pub target_id: String,
    pub method: String,
    pub args: Vec<u8>,
}

pub async fn build_bootstrap_snapshot() -> Result<&'static [u8]> {
    let mut runtime = JsRuntimeForSnapshot::new(RuntimeOptions {
        extensions: runtime_extensions(),
        module_loader: Some(Rc::new(DataUrlModuleLoader)),
        ..Default::default()
    });
    runtime
        .execute_script(BOOTSTRAP_SPECIFIER, BOOTSTRAP_JS)
        .map_err(runtime_error)?;
    let snapshot = runtime.snapshot();
    Ok(Box::leak(snapshot))
}

pub async fn validate_worker(bootstrap_snapshot: &'static [u8], source: &str) -> Result<()> {
    let mut runtime = new_runtime(bootstrap_snapshot)?;
    load_worker(&mut runtime, source).await
}

#[cfg_attr(not(test), allow(dead_code))]
pub async fn build_worker_snapshot(
    _bootstrap_snapshot: &'static [u8],
    source: &str,
) -> Result<&'static [u8]> {
    let mut runtime = JsRuntimeForSnapshot::new(RuntimeOptions {
        extensions: runtime_extensions(),
        module_loader: Some(Rc::new(DataUrlModuleLoader)),
        ..Default::default()
    });
    runtime
        .execute_script(BOOTSTRAP_SPECIFIER, BOOTSTRAP_JS)
        .map_err(runtime_error)?;
    evaluate_snapshot_module(&mut runtime, WORKER_SPECIFIER, source, false).await?;
    let install_code = install_worker_js();
    evaluate_snapshot_module(&mut runtime, INSTALL_SPECIFIER, &install_code, true).await?;
    let snapshot = runtime.snapshot();
    Ok(Box::leak(snapshot))
}

pub fn validate_loaded_worker_runtime(startup_snapshot: &'static [u8]) -> Result<()> {
    let mut runtime = new_runtime(startup_snapshot)?;
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

pub fn new_runtime_from_snapshot(startup_snapshot: &'static [u8]) -> Result<JsRuntime> {
    new_runtime(startup_snapshot)
}

pub async fn load_worker(runtime: &mut JsRuntime, source: &str) -> Result<()> {
    evaluate_module(runtime, WORKER_SPECIFIER, source, false).await?;
    let install_code = install_worker_js();
    evaluate_module(runtime, INSTALL_SPECIFIER, &install_code, true).await
}

pub fn dispatch_worker_request(
    runtime: &mut JsRuntime,
    request_id: &str,
    completion_token: &str,
    worker_name: &str,
    kv_bindings: &[String],
    actor_bindings: &[String],
    dynamic_bindings: &[String],
    dynamic_rpc_bindings: &[String],
    dynamic_env: &[(String, String)],
    has_request_body_stream: bool,
    actor_call: Option<&ExecuteActorCall>,
    host_rpc_call: Option<&ExecuteHostRpcCall>,
    request: WorkerInvocation,
) -> Result<()> {
    let request_json = crate::json::to_string(&request)
        .map_err(|error| PlatformError::internal(error.to_string()))?;
    let worker_name_json = crate::json::to_string(worker_name)
        .map_err(|error| PlatformError::internal(error.to_string()))?;
    let kv_bindings_json = crate::json::to_string(kv_bindings)
        .map_err(|error| PlatformError::internal(error.to_string()))?;
    let actor_bindings_json = crate::json::to_string(actor_bindings)
        .map_err(|error| PlatformError::internal(error.to_string()))?;
    let dynamic_bindings_json = crate::json::to_string(dynamic_bindings)
        .map_err(|error| PlatformError::internal(error.to_string()))?;
    let dynamic_rpc_bindings_json = crate::json::to_string(dynamic_rpc_bindings)
        .map_err(|error| PlatformError::internal(error.to_string()))?;
    let dynamic_env_json = crate::json::to_string(dynamic_env)
        .map_err(|error| PlatformError::internal(error.to_string()))?;
    let actor_call_json = crate::json::to_string(&actor_call)
        .map_err(|error| PlatformError::internal(error.to_string()))?;
    let host_rpc_call_json = crate::json::to_string(&host_rpc_call)
        .map_err(|error| PlatformError::internal(error.to_string()))?;
    let request_id_json = crate::json::to_string(request_id)
        .map_err(|error| PlatformError::internal(error.to_string()))?;
    let completion_token_json = crate::json::to_string(completion_token)
        .map_err(|error| PlatformError::internal(error.to_string()))?;
    let entry_code = execute_worker_js(
        &worker_name_json,
        &kv_bindings_json,
        &actor_bindings_json,
        &dynamic_bindings_json,
        &dynamic_rpc_bindings_json,
        &dynamic_env_json,
        &actor_call_json,
        &host_rpc_call_json,
        &request_id_json,
        &completion_token_json,
        has_request_body_stream,
        &request_json,
    );
    runtime
        .execute_script("<dd:invoke>", entry_code)
        .map_err(runtime_error)?;
    Ok(())
}

pub fn abort_worker_request(runtime: &mut JsRuntime, request_id: &str) -> Result<()> {
    let request_id_json = crate::json::to_string(request_id)
        .map_err(|error| PlatformError::internal(error.to_string()))?;
    let entry_code = abort_worker_js(&request_id_json);
    runtime
        .execute_script("<dd:abort>", entry_code)
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
        extensions: runtime_extensions(),
        module_loader: Some(Rc::new(DataUrlModuleLoader)),
        startup_snapshot: Some(startup_snapshot),
        ..Default::default()
    })
    .map_err(runtime_error)
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

struct DataUrlModuleLoader;

impl ModuleLoader for DataUrlModuleLoader {
    fn resolve(
        &self,
        specifier: &str,
        referrer: &str,
        _kind: ResolutionKind,
    ) -> std::result::Result<ModuleSpecifier, JsErrorBox> {
        resolve_import(specifier, referrer).map_err(JsErrorBox::from_err)
    }

    fn load(
        &self,
        module_specifier: &ModuleSpecifier,
        _maybe_referrer: Option<&deno_core::ModuleLoadReferrer>,
        options: deno_core::ModuleLoadOptions,
    ) -> ModuleLoadResponse {
        if options.requested_module_type != RequestedModuleType::None {
            return ModuleLoadResponse::Sync(Err(JsErrorBox::generic(format!(
                "unsupported requested module type for dynamic data URL: {}",
                options.requested_module_type
            ))));
        }

        ModuleLoadResponse::Sync(load_data_url_module(module_specifier))
    }
}

fn load_data_url_module(
    module_specifier: &ModuleSpecifier,
) -> std::result::Result<ModuleSource, JsErrorBox> {
    let source = decode_data_url_module_source(module_specifier)?;
    Ok(ModuleSource::new(
        ModuleType::JavaScript,
        ModuleSourceCode::String(source.into()),
        module_specifier,
        None,
    ))
}

fn decode_data_url_module_source(
    module_specifier: &ModuleSpecifier,
) -> std::result::Result<String, JsErrorBox> {
    if module_specifier.scheme() != "data" {
        return Err(JsErrorBox::generic(format!(
            "dynamic module loader only supports data: URLs, got {module_specifier}"
        )));
    }

    let raw = module_specifier.as_str();
    let Some(rest) = raw.strip_prefix("data:") else {
        return Err(JsErrorBox::generic(format!(
            "invalid data URL: {module_specifier}"
        )));
    };
    let Some((meta, body)) = rest.split_once(',') else {
        return Err(JsErrorBox::generic(format!(
            "invalid data URL payload: {module_specifier}"
        )));
    };
    if meta
        .split(';')
        .any(|part| part.eq_ignore_ascii_case("base64"))
    {
        return Err(JsErrorBox::generic(
            "base64 data URLs are not supported in dynamic modules",
        ));
    }

    let bytes = percent_decode(body).map_err(JsErrorBox::generic)?;
    String::from_utf8(bytes)
        .map_err(|error| JsErrorBox::generic(format!("invalid UTF-8 in data URL module: {error}")))
}

fn percent_decode(value: &str) -> std::result::Result<Vec<u8>, String> {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut idx = 0;
    while idx < bytes.len() {
        match bytes[idx] {
            b'%' => {
                if idx + 2 >= bytes.len() {
                    return Err("truncated percent-escape in data URL".to_string());
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
            "invalid hex digit in data URL escape: {}",
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
        let _ = new_runtime_from_snapshot(snapshot).expect("runtime should start from snapshot");
    }

    #[tokio::test]
    #[serial]
    async fn worker_snapshot_builds_from_bootstrap_snapshot() {
        let snapshot = build_bootstrap_snapshot()
            .await
            .expect("bootstrap snapshot should build");
        validate_worker(snapshot, simple_worker_source())
            .await
            .expect("worker should validate");
        let worker_snapshot = build_worker_snapshot(snapshot, simple_worker_source())
            .await
            .expect("worker snapshot should build");
        validate_loaded_worker_runtime(worker_snapshot).expect("worker snapshot should validate");
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
            new_runtime_from_snapshot(snapshot).expect("runtime should start from snapshot");

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
            validate_worker(bootstrap, simple_worker_source())
                .await
                .expect("worker should validate");
            bootstrap
        });
        let mut js_runtime = new_runtime_from_snapshot(snapshot)
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
            validate_worker(bootstrap, simple_worker_source())
                .await
                .expect("worker should validate");
            bootstrap
        });
        let mut js_runtime = new_runtime_from_snapshot(snapshot)
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
