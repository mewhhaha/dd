use super::{RuntimeConfig, RuntimeService, RuntimeServiceConfig, RuntimeStorageConfig};
use base64::Engine;
use bytes::Bytes;
use common::{
    DeployAsset, DeployBinding, DeployConfig, DeployInternalConfig, DeployServerModule,
    DeployServerModuleKind, DeployTraceDestination, ErrorKind, WorkerInvocation, WorkerOutput,
};
use serde::Deserialize;
use serde_json::Value;
use serial_test::serial;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout};
use uuid::Uuid;

#[path = "tests/dynamic.rs"]
mod dynamic;
#[path = "tests/fixtures.rs"]
mod fixtures;
#[path = "tests/memory.rs"]
mod memory;
#[path = "tests/sessions.rs"]
mod sessions;

use self::fixtures::*;

#[tokio::test]
#[serial]
async fn service_starts_with_deno_runtime_bootstrap() {
    let _ = test_service(RuntimeConfig {
        min_isolates: 0,
        max_isolates: 1,
        max_inflight_per_isolate: 1,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;
}

#[tokio::test]
#[serial]
async fn shutdown_is_idempotent_across_clones_and_waits_for_runtime_thread_exit() {
    let service = test_service(RuntimeConfig {
        min_isolates: 0,
        ..RuntimeConfig::default()
    })
    .await;

    let shutdowns = (0..8).map(|_| {
        let service = service.clone();
        async move { service.shutdown().await }
    });
    let results = timeout(
        Duration::from_secs(5),
        futures_util::future::join_all(shutdowns),
    )
    .await
    .expect("concurrent shutdown calls should complete");
    assert!(results.into_iter().all(|result| result.is_ok()));
    assert!(
        service.shutdown.is_complete(),
        "shutdown must not return before the shared runtime thread join completes"
    );

    timeout(Duration::from_millis(100), service.shutdown())
        .await
        .expect("repeated shutdown should use the stored result")
        .expect("repeated shutdown should remain successful");
}

#[tokio::test]
#[serial]
async fn last_service_handle_drop_shuts_down_and_joins_runtime_thread() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        ..RuntimeConfig::default()
    })
    .await;
    let worker = "automatic-shutdown".to_string();
    service
        .deploy(worker.clone(), counter_worker())
        .await
        .expect("worker should deploy");
    service
        .invoke(worker.clone(), test_invocation())
        .await
        .expect("worker invocation should start the configured isolate");
    wait_for_isolate_total(&service, &worker, 1).await;

    let shutdown = Arc::clone(&service.shutdown);
    let remaining_handle = service.clone();

    drop(service);
    assert!(
        !shutdown.is_complete(),
        "dropping one clone must not shut down the runtime"
    );

    drop(remaining_handle);
    assert!(
        shutdown.is_complete(),
        "last service handle drop must not return before runtime teardown completes"
    );
    shutdown
        .wait()
        .await
        .expect("automatic runtime shutdown should succeed");
}

#[tokio::test]
#[serial]
async fn isolate_starts_with_configured_heap_limit() {
    let service = test_service(RuntimeConfig {
        min_isolates: 0,
        max_isolates: 1,
        max_inflight_per_isolate: 1,
        max_isolate_heap_bytes: 64 * 1024 * 1024,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;
    let worker = "heap-limited".to_string();
    service
        .deploy(
            worker.clone(),
            r#"
export default {
  fetch() {
    const values = Array.from({ length: 1024 }, (_, idx) => idx);
    return new Response(String(values.length));
  },
};
"#
            .to_string(),
        )
        .await
        .expect("deploy should succeed");

    let output = timeout(
        Duration::from_secs(2),
        service.invoke(worker, test_invocation()),
    )
    .await
    .expect("invoke should not hang")
    .expect("request should succeed");
    assert_eq!(output.status, 200);
    assert_eq!(
        String::from_utf8(output.body).expect("body should be utf8"),
        "1024"
    );
}

#[tokio::test]
#[serial]
async fn deployed_worker_imports_server_module_assets() {
    let service = test_service(RuntimeConfig {
        min_isolates: 0,
        max_isolates: 1,
        max_inflight_per_isolate: 1,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;
    let worker = "server-modules".to_string();
    let encode = |bytes: &[u8]| base64::engine::general_purpose::STANDARD.encode(bytes);

    service
        .deploy_with_bundle_config_lifecycle_and_server_modules(
            worker.clone(),
            r#"
import config from "./config.json" with { type: "json" };
import message from "./message.txt" with { type: "text" };
import payload from "./payload.bin" with { type: "bytes" };
import wasmModule from "./empty.wasm";

function byteValues(value) {
  if (value instanceof Uint8Array) {
    return Array.from(value);
  }
  if (value instanceof ArrayBuffer) {
    return Array.from(new Uint8Array(value));
  }
  return Array.from(new Uint8Array(value.buffer));
}

export default {
  async fetch() {
    const dynamic = await import("./dynamic.json", { with: { type: "json" } });
    return Response.json({
      config,
      message,
      payload: byteValues(payload),
      dynamic: dynamic.default,
      wasm: wasmModule instanceof WebAssembly.Module,
    });
  },
};
"#
            .to_string(),
            DeployConfig::default(),
            Vec::new(),
            vec![
                DeployServerModule {
                    path: "config.json".to_string(),
                    kind: DeployServerModuleKind::Json,
                    content_base64: encode(br#"{"answer":42}"#),
                },
                DeployServerModule {
                    path: "message.txt".to_string(),
                    kind: DeployServerModuleKind::Text,
                    content_base64: encode(b"hello"),
                },
                DeployServerModule {
                    path: "payload.bin".to_string(),
                    kind: DeployServerModuleKind::Data,
                    content_base64: encode(&[1, 2, 3, 4]),
                },
                DeployServerModule {
                    path: "dynamic.json".to_string(),
                    kind: DeployServerModuleKind::Json,
                    content_base64: encode(br#"{"loaded":true}"#),
                },
                DeployServerModule {
                    path: "empty.wasm".to_string(),
                    kind: DeployServerModuleKind::CompiledWasm,
                    content_base64: encode(&[0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00]),
                },
            ],
            None,
            false,
        )
        .await
        .expect("deploy should succeed");

    let output = timeout(
        Duration::from_secs(2),
        service.invoke(worker, test_invocation()),
    )
    .await
    .expect("invoke should not hang")
    .expect("request should succeed");
    assert_eq!(output.status, 200);
    let body: Value = serde_json::from_slice(&output.body).expect("body should be json");
    assert_eq!(body["config"]["answer"], 42);
    assert_eq!(body["message"], "hello");
    assert_eq!(body["payload"], serde_json::json!([1, 2, 3, 4]));
    assert_eq!(body["dynamic"]["loaded"], true);
    assert_eq!(body["wasm"], true);
}

#[tokio::test]
#[serial]
async fn isolate_startup_does_not_block_manager_commands() {
    let service = test_service(RuntimeConfig {
        min_isolates: 0,
        max_isolates: 4,
        max_inflight_per_isolate: 1,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(20),
        queue_warn_thresholds: vec![1],
        ..RuntimeConfig::default()
    })
    .await;
    let worker = "slow-startup".to_string();
    service
        .deploy(
            worker.clone(),
            r#"
await Deno.core.ops.op_sleep(800);

export default {
  async fetch() {
    return new Response("ready");
  },
};
"#
            .to_string(),
        )
        .await
        .expect("deploy should succeed");

    let invoke_service = service.clone();
    let invoke_worker = worker.clone();
    let invoke = tokio::spawn(async move {
        invoke_service
            .invoke(
                invoke_worker,
                test_invocation_with_path("/", "slow-startup-invoke"),
            )
            .await
    });

    timeout(Duration::from_secs(1), async {
        loop {
            let stats = service.stats(worker.clone()).await.expect("stats");
            if stats.queued == 1 && stats.isolates_total == 1 {
                break;
            }
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("request should wait while the isolate starts");

    let stats_started = Instant::now();
    let stats = timeout(Duration::from_millis(250), service.stats(worker.clone()))
        .await
        .expect("stats should not wait for isolate startup")
        .expect("stats");
    assert_eq!(stats.queued, 1);
    assert_eq!(stats.inflight_total, 0);
    assert!(
        stats_started.elapsed() < Duration::from_millis(250),
        "stats was delayed by isolate startup: {:?}",
        stats_started.elapsed(),
    );

    let output = timeout(Duration::from_secs(2), invoke)
        .await
        .expect("invoke join should not hang")
        .expect("invoke task should complete")
        .expect("invoke should succeed");
    assert_eq!(String::from_utf8(output.body).expect("utf8"), "ready");
    let stats = service.stats(worker).await.expect("stats");
    assert_eq!(stats.spawn_count, 1);
}

#[tokio::test]
#[serial]
async fn production_runtime_rejects_string_code_generation() {
    let service = test_service(RuntimeConfig {
        min_isolates: 0,
        max_isolates: 1,
        max_inflight_per_isolate: 1,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;
    service
        .deploy(
            "production-codegen".to_string(),
            r#"
export default {
  async fetch() {
    return new Response(String(eval("20 + 1")));
  },
};
"#
            .to_string(),
        )
        .await
        .expect("deploy should succeed");

    let error = service
        .invoke(
            "production-codegen".to_string(),
            test_invocation_with_path("/", "production-codegen"),
        )
        .await
        .expect_err("production invoke should reject eval");
    assert_eq!(error.kind(), ErrorKind::Runtime);
    assert!(
        error.to_string().contains("Code generation from strings"),
        "{error}"
    );
}

#[tokio::test]
#[serial]
async fn debug_runtime_allows_string_code_generation() {
    let service = test_service(RuntimeConfig {
        min_isolates: 0,
        max_isolates: 1,
        max_inflight_per_isolate: 1,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        debug_code_generation: true,
        ..RuntimeConfig::default()
    })
    .await;
    service
        .deploy(
            "debug-codegen".to_string(),
            r#"
export default {
  async fetch() {
    const evalValue = eval("20 + 1");
    const functionValue = new Function("value", "return value + 1")(evalValue);
    return new Response(String(functionValue));
  },
};
"#
            .to_string(),
        )
        .await
        .expect("deploy should succeed");

    let output = invoke_with_timeout_and_dump(
        &service,
        "debug-codegen",
        test_invocation_with_path("/", "debug-codegen"),
        "debug code generation",
    )
    .await;
    assert_eq!(String::from_utf8(output.body).expect("utf8"), "22");
}

#[tokio::test]
#[serial]
async fn host_fetch_is_single_permanent_request_scoped_wrapper() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 1,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy(
            "fetch-wrapper".to_string(),
            r#"
let firstFetch;
let firstInstaller;
let firstRequestIdGetter;
let firstRequestContextHandleGetter;
let firstSyncBoundary;
let firstCacheBypassGetter;

export default {
  async fetch() {
    const current = globalThis.fetch;
    const installer = globalThis.__dd_install_host_fetch;
    const requestIdGetter = globalThis.__dd_get_runtime_request_id;
    const requestContextHandleGetter = globalThis.__dd_get_runtime_request_context_handle;
    const syncBoundary = globalThis.__dd_sync_time_boundary;
    const cacheBypassGetter = globalThis.__dd_get_cache_bypass_stale;
    const first = firstFetch === undefined;
    if (first) {
      firstFetch = current;
      firstInstaller = installer;
      firstRequestIdGetter = requestIdGetter;
      firstRequestContextHandleGetter = requestContextHandleGetter;
      firstSyncBoundary = syncBoundary;
      firstCacheBypassGetter = cacheBypassGetter;
    }
    return Response.json({
      first,
      sameAsFirst: current === firstFetch,
      installerAvailable: typeof installer === "function",
      installerSameAsFirst: installer === firstInstaller,
      requestIdGetterSameAsFirst: requestIdGetter === firstRequestIdGetter,
      requestContextHandleGetterSameAsFirst: requestContextHandleGetter === firstRequestContextHandleGetter,
      syncBoundarySameAsFirst: syncBoundary === firstSyncBoundary,
      cacheBypassGetterSameAsFirst: cacheBypassGetter === firstCacheBypassGetter,
      cacheBypassGetterAvailable: typeof cacheBypassGetter === "function",
      cacheBypassValue: Boolean(cacheBypassGetter?.()),
      rawAvailable: typeof globalThis.__dd_raw_host_fetch === "function",
      rawInstalled: current === globalThis.__dd_raw_host_fetch,
      wrapperInstalled: current.__dd_host_fetch === true,
    });
  },
};
"#
            .to_string(),
        )
        .await
        .expect("deploy should succeed");

    let first = service
        .invoke(
            "fetch-wrapper".to_string(),
            test_invocation_with_path("/", "fetch-wrapper-first"),
        )
        .await
        .expect("first invoke should succeed");
    let second = service
        .invoke(
            "fetch-wrapper".to_string(),
            test_invocation_with_path("/", "fetch-wrapper-second"),
        )
        .await
        .expect("second invoke should succeed");

    let first: Value = serde_json::from_slice(&first.body).expect("first body should be json");
    let second: Value = serde_json::from_slice(&second.body).expect("second body should be json");

    assert_eq!(first["first"], true);
    assert_eq!(first["sameAsFirst"], true);
    assert_eq!(first["installerAvailable"], true);
    assert_eq!(first["installerSameAsFirst"], true);
    assert_eq!(first["requestIdGetterSameAsFirst"], true);
    assert_eq!(first["requestContextHandleGetterSameAsFirst"], true);
    assert_eq!(first["syncBoundarySameAsFirst"], true);
    assert_eq!(first["cacheBypassGetterSameAsFirst"], true);
    assert_eq!(first["cacheBypassGetterAvailable"], true);
    assert_eq!(first["cacheBypassValue"], false);
    assert_eq!(first["rawAvailable"], true);
    assert_eq!(first["rawInstalled"], false);
    assert_eq!(first["wrapperInstalled"], true);
    assert_eq!(second["first"], false);
    assert_eq!(second["sameAsFirst"], true);
    assert_eq!(second["installerAvailable"], true);
    assert_eq!(second["installerSameAsFirst"], true);
    assert_eq!(second["requestIdGetterSameAsFirst"], true);
    assert_eq!(second["requestContextHandleGetterSameAsFirst"], true);
    assert_eq!(second["syncBoundarySameAsFirst"], true);
    assert_eq!(second["cacheBypassGetterSameAsFirst"], true);
    assert_eq!(second["cacheBypassGetterAvailable"], true);
    assert_eq!(second["cacheBypassValue"], false);
    assert_eq!(second["rawAvailable"], true);
    assert_eq!(second["rawInstalled"], false);
    assert_eq!(second["wrapperInstalled"], true);
}

#[tokio::test]
#[serial]
async fn overlapping_requests_keep_independent_request_context_handles() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 2,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy(
            "overlap-context".to_string(),
            r#"
export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const label = url.searchParams.get("label");
    const before = {
      id: globalThis.__dd_get_runtime_request_id?.(),
      handle: globalThis.__dd_get_runtime_request_context_handle?.(),
    };
    await ctx.sleep(Number(url.searchParams.get("delay") ?? "0"));
    const after = {
      id: globalThis.__dd_get_runtime_request_id?.(),
      handle: globalThis.__dd_get_runtime_request_context_handle?.(),
    };
    return Response.json({ label, before, after });
  },
};
"#
            .to_string(),
        )
        .await
        .expect("deploy should succeed");

    let slow = {
        let service = service.clone();
        tokio::spawn(async move {
            service
                .invoke(
                    "overlap-context".to_string(),
                    test_invocation_with_path("/?label=slow&delay=50", "overlap-slow"),
                )
                .await
        })
    };
    let fast = {
        let service = service.clone();
        tokio::spawn(async move {
            service
                .invoke(
                    "overlap-context".to_string(),
                    test_invocation_with_path("/?label=fast&delay=1", "overlap-fast"),
                )
                .await
        })
    };

    let slow = slow
        .await
        .expect("slow task should join")
        .expect("slow request should succeed");
    let fast = fast
        .await
        .expect("fast task should join")
        .expect("fast request should succeed");
    let slow: Value = serde_json::from_slice(&slow.body).expect("slow response should be json");
    let fast: Value = serde_json::from_slice(&fast.body).expect("fast response should be json");

    assert_eq!(slow["label"], "slow");
    assert_eq!(fast["label"], "fast");
    assert_eq!(slow["before"]["id"], slow["after"]["id"]);
    assert_eq!(fast["before"]["id"], fast["after"]["id"]);
    assert_eq!(slow["before"]["handle"], slow["after"]["handle"]);
    assert_eq!(fast["before"]["handle"], fast["after"]["handle"]);
    assert_ne!(slow["before"]["id"], fast["before"]["id"]);
    assert_ne!(slow["before"]["handle"], fast["before"]["handle"]);
}

#[tokio::test]
#[serial]
async fn worker_queue_rejects_when_per_worker_limit_is_full() {
    let service = test_service(RuntimeConfig {
        min_isolates: 0,
        max_isolates: 1,
        max_inflight_per_isolate: 1,
        max_queued_requests_per_worker: 1,
        max_global_queued_requests: 16,
        max_global_queued_bytes: 1024 * 1024,
        max_queue_wait: Duration::from_secs(5),
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(20),
        queue_warn_thresholds: vec![1],
        ..RuntimeConfig::default()
    })
    .await;
    let worker = "queue-limit".to_string();
    service
        .deploy(
            worker.clone(),
            r#"
export default {
  async fetch(request) {
    await Deno.core.ops.op_sleep(200);
    return new Response(new URL(request.url).pathname);
  },
};
"#
            .to_string(),
        )
        .await
        .expect("deploy should succeed");

    let first_service = service.clone();
    let first_worker = worker.clone();
    let first = tokio::spawn(async move {
        first_service
            .invoke(first_worker, test_invocation_with_path("/one", "queue-one"))
            .await
    });

    timeout(Duration::from_secs(2), async {
        loop {
            let stats = service.stats(worker.clone()).await.expect("stats");
            if stats.inflight_total == 1 {
                break;
            }
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("first request should dispatch");

    let second_service = service.clone();
    let second_worker = worker.clone();
    let second = tokio::spawn(async move {
        second_service
            .invoke(
                second_worker,
                test_invocation_with_path("/two", "queue-two"),
            )
            .await
    });

    timeout(Duration::from_secs(2), async {
        loop {
            let stats = service.stats(worker.clone()).await.expect("stats");
            if stats.queued == 1 && stats.inflight_total == 1 {
                break;
            }
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("second request should occupy the queue");

    let error = service
        .invoke(
            worker.clone(),
            test_invocation_with_path("/three", "queue-three"),
        )
        .await
        .expect_err("third request should be rejected");
    assert_eq!(error.kind(), ErrorKind::Overloaded);
    assert!(error.to_string().contains("worker queue is full"));

    timeout(Duration::from_secs(3), first)
        .await
        .expect("first join")
        .expect("first request should complete")
        .expect("first request should succeed");
    timeout(Duration::from_secs(3), second)
        .await
        .expect("second join")
        .expect("second request should complete")
        .expect("second request should succeed");
}

#[tokio::test]
#[serial]
async fn worker_queue_rejects_when_global_limit_is_full_across_workers() {
    let service = test_service(RuntimeConfig {
        min_isolates: 0,
        max_isolates: 1,
        max_inflight_per_isolate: 1,
        max_queued_requests_per_worker: 8,
        max_global_queued_requests: 1,
        max_global_queued_bytes: 1024 * 1024,
        max_queue_wait: Duration::from_secs(5),
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(20),
        queue_warn_thresholds: vec![1],
        ..RuntimeConfig::default()
    })
    .await;
    let worker_a = "global-queue-a".to_string();
    let worker_b = "global-queue-b".to_string();
    let source = r#"
export default {
  async fetch(request) {
    await Deno.core.ops.op_sleep(300);
    return new Response(new URL(request.url).pathname);
  },
};
"#
    .to_string();
    service
        .deploy(worker_a.clone(), source.clone())
        .await
        .expect("worker a deploy should succeed");
    service
        .deploy(worker_b.clone(), source)
        .await
        .expect("worker b deploy should succeed");

    let first_a_service = service.clone();
    let first_a_worker = worker_a.clone();
    let first_a = tokio::spawn(async move {
        first_a_service
            .invoke(
                first_a_worker,
                test_invocation_with_path("/one", "global-a-one"),
            )
            .await
    });
    timeout(Duration::from_secs(2), async {
        loop {
            let stats = service
                .stats(worker_a.clone())
                .await
                .expect("worker a stats");
            if stats.inflight_total == 1 {
                break;
            }
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("worker a first request should dispatch");

    let first_b_service = service.clone();
    let first_b_worker = worker_b.clone();
    let first_b = tokio::spawn(async move {
        first_b_service
            .invoke(
                first_b_worker,
                test_invocation_with_path("/one", "global-b-one"),
            )
            .await
    });
    timeout(Duration::from_secs(2), async {
        loop {
            let stats = service
                .stats(worker_b.clone())
                .await
                .expect("worker b stats");
            if stats.inflight_total == 1 {
                break;
            }
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("worker b first request should dispatch");

    let queued_a_service = service.clone();
    let queued_a_worker = worker_a.clone();
    let queued_a = tokio::spawn(async move {
        queued_a_service
            .invoke(
                queued_a_worker,
                test_invocation_with_path("/two", "global-a-two"),
            )
            .await
    });
    timeout(Duration::from_secs(2), async {
        loop {
            let stats = service
                .stats(worker_a.clone())
                .await
                .expect("worker a stats");
            if stats.queued == 1 && stats.inflight_total == 1 {
                break;
            }
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("worker a second request should occupy the only global queue slot");

    let error = service
        .invoke(
            worker_b.clone(),
            test_invocation_with_path("/two", "global-b-two"),
        )
        .await
        .expect_err("worker b queued request should hit the global queue limit");
    assert_eq!(error.kind(), ErrorKind::Overloaded);
    assert!(error.to_string().contains("runtime queue is full"));

    timeout(Duration::from_secs(3), first_a)
        .await
        .expect("worker a first join")
        .expect("worker a first request should complete")
        .expect("worker a first request should succeed");
    timeout(Duration::from_secs(3), queued_a)
        .await
        .expect("worker a queued join")
        .expect("worker a queued request should complete")
        .expect("worker a queued request should succeed");
    timeout(Duration::from_secs(3), first_b)
        .await
        .expect("worker b first join")
        .expect("worker b first request should complete")
        .expect("worker b first request should succeed");
}

#[tokio::test]
#[serial]
async fn worker_queue_expires_requests_after_queue_wait_limit() {
    let service = test_service(RuntimeConfig {
        min_isolates: 0,
        max_isolates: 1,
        max_inflight_per_isolate: 1,
        max_queued_requests_per_worker: 8,
        max_global_queued_requests: 16,
        max_global_queued_bytes: 1024 * 1024,
        max_queue_wait: Duration::from_millis(50),
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(10),
        queue_warn_thresholds: vec![1],
        ..RuntimeConfig::default()
    })
    .await;
    let worker = "queue-timeout".to_string();
    service
        .deploy(
            worker.clone(),
            r#"
export default {
  async fetch(request) {
    await Deno.core.ops.op_sleep(200);
    return new Response(new URL(request.url).pathname);
  },
};
"#
            .to_string(),
        )
        .await
        .expect("deploy should succeed");

    let first_service = service.clone();
    let first_worker = worker.clone();
    let first = tokio::spawn(async move {
        first_service
            .invoke(
                first_worker,
                test_invocation_with_path("/one", "queue-wait-one"),
            )
            .await
    });

    timeout(Duration::from_secs(2), async {
        loop {
            let stats = service.stats(worker.clone()).await.expect("stats");
            if stats.inflight_total == 1 {
                break;
            }
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("first request should dispatch");

    let queued_service = service.clone();
    let queued_worker = worker.clone();
    let queued = tokio::spawn(async move {
        queued_service
            .invoke(
                queued_worker,
                test_invocation_with_path("/queued", "queue-wait-two"),
            )
            .await
    });

    let error = timeout(Duration::from_secs(2), queued)
        .await
        .expect("queued request should complete")
        .expect("queued join")
        .expect_err("queued request should expire");
    assert_eq!(error.kind(), ErrorKind::Overloaded);
    assert!(error.to_string().contains("queue wait limit"));

    timeout(Duration::from_secs(3), first)
        .await
        .expect("first join")
        .expect("first request should complete")
        .expect("first request should succeed");
}

#[tokio::test]
#[serial]
async fn worker_request_wall_timeout_retires_cooperative_isolate() {
    let service = test_service(RuntimeConfig {
        min_isolates: 0,
        max_isolates: 1,
        max_inflight_per_isolate: 1,
        request_wall_timeout: Duration::from_millis(50),
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(10),
        queue_warn_thresholds: vec![1],
        ..RuntimeConfig::default()
    })
    .await;
    let worker = "cooperative-wall-timeout".to_string();
    service
        .deploy(
            worker.clone(),
            r#"
export default {
  async fetch() {
    await Deno.core.ops.op_sleep(500);
    return new Response("late");
  },
};
"#
            .to_string(),
        )
        .await
        .expect("deploy should succeed");

    let error = timeout(
        Duration::from_secs(2),
        service.invoke(worker.clone(), test_invocation()),
    )
    .await
    .expect("invoke should not hang")
    .expect_err("request should exceed wall timeout");
    assert!(
        error.to_string().contains("wall-time limit"),
        "unexpected error: {error}"
    );

    wait_for_isolate_total(&service, &worker, 0).await;
}

#[tokio::test]
#[serial]
async fn worker_request_wall_timeout_interrupts_cpu_bound_isolate() {
    let service = test_service(RuntimeConfig {
        min_isolates: 0,
        max_isolates: 1,
        max_inflight_per_isolate: 1,
        request_wall_timeout: Duration::from_millis(50),
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(10),
        queue_warn_thresholds: vec![1],
        ..RuntimeConfig::default()
    })
    .await;
    let worker = "cpu-wall-timeout".to_string();
    service
        .deploy(
            worker.clone(),
            r#"
export default {
  fetch() {
    while (true) {}
  },
};
"#
            .to_string(),
        )
        .await
        .expect("deploy should succeed");

    let started_at = Instant::now();
    let error = timeout(
        Duration::from_secs(2),
        service.invoke(worker.clone(), test_invocation()),
    )
    .await
    .expect("invoke should not hang")
    .expect_err("request should exceed wall timeout");
    assert!(
        error.to_string().contains("wall-time limit"),
        "unexpected error: {error}"
    );
    assert!(
        started_at.elapsed() < Duration::from_secs(1),
        "cpu-bound request took too long to interrupt"
    );

    wait_for_isolate_total(&service, &worker, 0).await;
}

#[tokio::test]
#[serial]
async fn buffered_request_body_limit_rejects_before_dispatch() {
    let service = test_service(RuntimeConfig {
        max_request_body_bytes: 3,
        ..RuntimeConfig::default()
    })
    .await;
    service
        .deploy(
            "request-limit".to_string(),
            r#"
export default {
  async fetch(request) {
    return new Response(await request.text());
  },
};
"#
            .to_string(),
        )
        .await
        .expect("deploy should succeed");

    let mut request = test_invocation();
    request.method = "POST".to_string();
    request.body = b"four".to_vec();
    let error = service
        .invoke("request-limit".to_string(), request)
        .await
        .expect_err("oversized request should fail");
    assert!(
        error.to_string().contains("max_request_body_bytes"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
#[serial]
async fn streamed_request_body_limit_fails_during_read() {
    let service = test_service(RuntimeConfig {
        max_request_body_bytes: 3,
        ..RuntimeConfig::default()
    })
    .await;
    service
        .deploy(
            "streamed-request-limit".to_string(),
            r#"
export default {
  async fetch(request) {
    return new Response(await request.text());
  },
};
"#
            .to_string(),
        )
        .await
        .expect("deploy should succeed");

    let (tx, rx) = mpsc::channel(4);
    tx.send(Ok(Bytes::from_static(b"ab")))
        .await
        .expect("send first chunk");
    tx.send(Ok(Bytes::from_static(b"cd")))
        .await
        .expect("send second chunk");
    drop(tx);

    let mut request = test_invocation();
    request.method = "POST".to_string();
    let error = service
        .invoke_with_request_body("streamed-request-limit".to_string(), request, Some(rx))
        .await
        .expect_err("oversized streamed request should fail");
    assert!(
        error.to_string().contains("max_request_body_bytes"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
#[serial]
async fn buffered_response_body_limit_rejects_completion() {
    let service = test_service(RuntimeConfig {
        max_response_body_bytes: 4,
        ..RuntimeConfig::default()
    })
    .await;
    service
        .deploy(
            "response-limit".to_string(),
            r#"
export default {
  fetch() {
    return new Response("hello");
  },
};
"#
            .to_string(),
        )
        .await
        .expect("deploy should succeed");

    let error = service
        .invoke("response-limit".to_string(), test_invocation())
        .await
        .expect_err("oversized response should fail");
    assert!(
        error.to_string().contains("max_response_body_bytes"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
#[serial]
async fn streamed_response_body_limit_fails_stream_and_retires_isolate() {
    let service = test_service(RuntimeConfig {
        min_isolates: 0,
        max_isolates: 1,
        max_inflight_per_isolate: 1,
        max_response_body_bytes: 5,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(10),
        queue_warn_thresholds: vec![1],
        ..RuntimeConfig::default()
    })
    .await;
    service
        .deploy(
            "streamed-response-limit".to_string(),
            r#"
export default {
  fetch(request) {
    if (new URL(request.url).pathname === "/ok") {
      return new Response("ok");
    }
    return new Response(new ReadableStream({
      start(controller) {
        controller.enqueue("abc");
        controller.enqueue("def");
        controller.close();
      }
    }));
  },
};
"#
            .to_string(),
        )
        .await
        .expect("deploy should succeed");

    let mut output = service
        .invoke_stream(
            "streamed-response-limit".to_string(),
            test_invocation_with_path("/stream", "stream-limit"),
        )
        .await
        .expect("stream should start");
    let first = timeout(Duration::from_secs(2), output.body.recv())
        .await
        .expect("first chunk should arrive")
        .expect("body should still be open")
        .expect("first chunk should be ok");
    assert_eq!(String::from_utf8(first.to_vec()).expect("utf8"), "abc");

    let error = timeout(Duration::from_secs(2), output.body.recv())
        .await
        .expect("limit error should arrive")
        .expect("body should report limit error")
        .expect_err("second chunk should exceed response limit");
    assert!(
        error.to_string().contains("max_response_body_bytes"),
        "unexpected error: {error}"
    );

    let followup = timeout(
        Duration::from_secs(3),
        service.invoke(
            "streamed-response-limit".to_string(),
            test_invocation_with_path("/ok", "stream-limit-followup"),
        ),
    )
    .await
    .expect("stream limit should retire the isolate and allow a followup")
    .expect("followup invoke should succeed");
    assert_eq!(String::from_utf8(followup.body).expect("utf8"), "ok");
}

#[tokio::test]
#[serial]
async fn deployed_assets_resolve_with_headers_and_head_support() {
    let service = test_service(RuntimeConfig::default()).await;
    service
        .deploy_with_bundle_config(
            "assets".to_string(),
            asset_worker(),
            DeployConfig::default(),
            test_assets(),
            Some(asset_headers_file()),
        )
        .await
        .expect("deploy should succeed");

    let private_public_lookup = service
        .resolve_public_asset("assets", "GET", Some("foo.example.com"), "/a.js", &[])
        .expect("public asset lookup should succeed");
    assert!(private_public_lookup.is_none());

    let asset = service
        .resolve_asset("assets", "GET", Some("foo.example.com:443"), "/a.js", &[])
        .expect("asset lookup should succeed")
        .expect("asset should exist");
    assert_eq!(asset.status, 200);
    assert_eq!(asset.body.as_ref(), b"asset-body");
    assert!(asset.headers.iter().any(|(name, value)| {
        name.eq_ignore_ascii_case("cache-control") && value == "public, max-age=60"
    }));
    assert!(
        asset
            .headers
            .iter()
            .any(|(name, value)| name.eq_ignore_ascii_case("x-host") && value == "foo")
    );

    let etag = asset
        .headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("etag"))
        .map(|(_, value)| value.clone())
        .expect("etag should be present");

    let head = service
        .resolve_asset(
            "assets",
            "HEAD",
            Some("foo.example.com"),
            "/nested/b.css",
            &[],
        )
        .expect("head lookup should succeed")
        .expect("asset should exist");
    assert_eq!(head.status, 200);
    assert!(head.body.is_empty());
    assert!(
        head.headers
            .iter()
            .any(|(name, value)| name.eq_ignore_ascii_case("x-splat") && value == "b.css")
    );

    let not_modified = service
        .resolve_asset(
            "assets",
            "GET",
            Some("foo.example.com"),
            "/a.js",
            &[("if-none-match".to_string(), etag)],
        )
        .expect("etag lookup should succeed")
        .expect("asset should exist");
    assert_eq!(not_modified.status, 304);
    assert!(not_modified.body.is_empty());
}

#[tokio::test]
#[serial]
async fn private_worker_can_call_private_service_binding() {
    let service = test_service(RuntimeConfig {
        min_isolates: 0,
        max_isolates: 2,
        max_inflight_per_isolate: 2,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy_with_config(
            "private-auth".to_string(),
            r#"
export default {
  async fetch(request) {
    const url = new URL(request.url);
    const body = await request.text();
    return new Response(
      JSON.stringify({
        method: request.method,
        path: url.pathname,
        service: request.headers.get("x-service"),
        body,
      }),
      { status: 201, headers: { "x-target-worker": "private-auth" } },
    );
  },
};
"#
            .to_string(),
            DeployConfig {
                public: false,
                cache: Default::default(),
                ..DeployConfig::default()
            },
        )
        .await
        .expect("target deploy should succeed");

    service
        .deploy_with_config(
            "private-app".to_string(),
            r#"
export default {
  async fetch(_request, env) {
    const response = await env.AUTH.fetch("/session", {
      method: "POST",
      headers: { "x-service": "app" },
      body: "hello",
    });
    return new Response(
      `${response.status}|${response.headers.get("x-target-worker")}|${await response.text()}`,
    );
  },
};
"#
            .to_string(),
            DeployConfig {
                public: false,
                cache: Default::default(),
                bindings: vec![DeployBinding::Service {
                    binding: "AUTH".to_string(),
                    service: "private-auth".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("caller deploy should succeed");

    assert!(!service.worker_is_public("private-auth"));
    let public_lookup = service
        .resolve_public_asset("private-auth", "GET", Some("auth.example.com"), "/", &[])
        .expect("public lookup should not fail");
    assert!(public_lookup.is_none());

    let output = invoke_with_timeout_and_dump(
        &service,
        "private-app",
        test_invocation_with_path("/", "service-binding-private"),
        "service binding invoke",
    )
    .await;
    assert_eq!(output.status, 200);
    let body = String::from_utf8(output.body).expect("utf8");
    assert!(body.starts_with("201|private-auth|"), "body was {body}");
    assert!(body.contains("\"method\":\"POST\""), "body was {body}");
    assert!(body.contains("\"path\":\"/session\""), "body was {body}");
    assert!(body.contains("\"service\":\"app\""), "body was {body}");
    assert!(body.contains("\"body\":\"hello\""), "body was {body}");
}

#[tokio::test]
#[serial]
async fn deployed_assets_restore_from_worker_store() {
    let root = PathBuf::from(format!("/tmp/dd-assets-{}", Uuid::new_v4()));
    let db_path = root.join("dd-test.db");
    let database_url = format!("file:{}", db_path.display());

    let service = test_service_with_paths(
        RuntimeConfig::default(),
        root.clone(),
        database_url.clone(),
        true,
    )
    .await;
    service
        .deploy_with_bundle_config(
            "assets".to_string(),
            asset_worker(),
            DeployConfig::default(),
            test_assets(),
            Some(asset_headers_file()),
        )
        .await
        .expect("deploy should succeed");
    drop(service);

    let restored =
        test_service_with_paths(RuntimeConfig::default(), root.clone(), database_url, true).await;
    let asset = restored
        .resolve_asset("assets", "GET", Some("foo.example.com"), "/a.js", &[])
        .expect("asset lookup should succeed")
        .expect("asset should exist after restore");
    assert_eq!(asset.body.as_ref(), b"asset-body");

    let _ = tokio::fs::remove_dir_all(root).await;
}

#[tokio::test]
#[serial]
async fn temporary_worker_redeploy_refreshes_and_normal_deploy_makes_permanent() {
    let service = test_service(RuntimeConfig {
        scale_tick: Duration::from_millis(20),
        temporary_worker_ttl: Duration::from_secs(60),
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy_temporary_with_bundle_config(
            "preview".to_string(),
            versioned_worker("temp-one", 0),
            DeployConfig::default(),
            Vec::new(),
            None,
        )
        .await
        .expect("temporary deploy should succeed");
    let first = service
        .stats("preview".to_string())
        .await
        .expect("temporary worker stats");
    assert!(first.temporary);
    let first_expires_at = first.expires_at_ms.expect("temporary expiration");

    sleep(Duration::from_millis(5)).await;
    service
        .deploy_temporary_with_bundle_config(
            "preview".to_string(),
            versioned_worker("temp-two", 0),
            DeployConfig::default(),
            Vec::new(),
            None,
        )
        .await
        .expect("temporary redeploy should refresh expiration");
    let refreshed = service
        .stats("preview".to_string())
        .await
        .expect("refreshed temporary worker stats");
    assert!(refreshed.expires_at_ms.expect("refreshed expiration") > first_expires_at);

    service
        .deploy_with_bundle_config(
            "preview".to_string(),
            versioned_worker("permanent", 0),
            DeployConfig::default(),
            Vec::new(),
            None,
        )
        .await
        .expect("normal deploy should make worker permanent");
    let permanent = service
        .stats("preview".to_string())
        .await
        .expect("permanent worker stats");
    assert!(!permanent.temporary);
    assert_eq!(permanent.expires_at_ms, None);

    let error = service
        .deploy_temporary_with_bundle_config(
            "preview".to_string(),
            versioned_worker("should-not-replace", 0),
            DeployConfig::default(),
            Vec::new(),
            None,
        )
        .await
        .expect_err("temporary deploy over permanent worker should fail");
    assert_eq!(error.kind(), ErrorKind::Conflict);

    let output = service
        .invoke(
            "preview".to_string(),
            test_invocation_with_path("/", "temporary-permanent-check"),
        )
        .await
        .expect("permanent worker should remain deployed");
    assert_eq!(String::from_utf8(output.body).expect("utf8"), "permanent");
}

#[tokio::test]
#[serial]
async fn temporary_worker_expires_and_is_not_restored_from_store() {
    let root = PathBuf::from(format!("/tmp/dd-temp-workers-{}", Uuid::new_v4()));
    let db_path = root.join("dd-test.db");
    let database_url = format!("file:{}", db_path.display());
    let config = RuntimeConfig {
        scale_tick: Duration::from_secs(1),
        temporary_worker_ttl: Duration::from_millis(250),
        ..RuntimeConfig::default()
    };

    let service =
        test_service_with_paths(config.clone(), root.clone(), database_url.clone(), true).await;
    service
        .deploy_temporary_with_bundle_config(
            "preview".to_string(),
            versioned_worker("temp", 0),
            DeployConfig::default(),
            Vec::new(),
            None,
        )
        .await
        .expect("temporary deploy should succeed");
    assert!(service.stats("preview".to_string()).await.is_some());
    service.shutdown().await.expect("service should shut down");
    drop(service);
    tokio::time::sleep(Duration::from_millis(300)).await;

    let restored = test_service_with_paths(config, root.clone(), database_url, true).await;
    assert!(restored.stats("preview".to_string()).await.is_none());

    let _ = tokio::fs::remove_dir_all(root).await;
}

#[tokio::test]
#[serial]
async fn temporary_worker_expires_while_runtime_is_running() {
    let service = test_service(RuntimeConfig {
        scale_tick: Duration::from_millis(10),
        temporary_worker_ttl: Duration::from_millis(30),
        ..RuntimeConfig::default()
    })
    .await;
    service
        .deploy_temporary_with_bundle_config(
            "preview-live".to_string(),
            versioned_worker("temp", 0),
            DeployConfig::default(),
            test_assets(),
            None,
        )
        .await
        .expect("temporary deploy should succeed");
    assert!(
        service
            .resolve_asset("preview-live", "GET", None, "/a.js", &[],)
            .expect("asset lookup before expiry should succeed")
            .is_some()
    );

    timeout(Duration::from_secs(2), async {
        loop {
            if service.stats("preview-live".to_string()).await.is_none() {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("temporary worker should expire");
    assert!(
        service
            .resolve_asset("preview-live", "GET", None, "/a.js", &[],)
            .expect("asset lookup after expiry should succeed")
            .is_none()
    );
}

#[tokio::test]
#[serial]
async fn invalid_asset_headers_fail_deploy() {
    let service = test_service(RuntimeConfig::default()).await;
    let error = service
        .deploy_with_bundle_config(
            "assets".to_string(),
            asset_worker(),
            DeployConfig::default(),
            test_assets(),
            Some("/a.js\n  BadHeader".to_string()),
        )
        .await
        .expect_err("deploy should fail");
    assert!(
        error
            .to_string()
            .contains("must be `Name: value` or `! Name`")
    );
}

#[tokio::test]
#[serial]
async fn service_can_deploy_simple_worker_with_deno_runtime_bootstrap() {
    let service = test_service(RuntimeConfig {
        min_isolates: 0,
        max_isolates: 1,
        max_inflight_per_isolate: 1,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy(
            "simple-deno-worker".to_string(),
            r#"
                export default {
                  async fetch() {
                    return new Response("ok");
                  }
                };
                "#
            .to_string(),
        )
        .await
        .expect("deploy should succeed");
}

#[tokio::test]
#[serial]
async fn service_can_invoke_simple_worker_with_deno_runtime_bootstrap() {
    let service = test_service(RuntimeConfig {
        min_isolates: 0,
        max_isolates: 1,
        max_inflight_per_isolate: 1,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy(
            "simple-deno-invoke".to_string(),
            r#"
                export default {
                  async fetch() {
                    return new Response("ok");
                  }
                };
                "#
            .to_string(),
        )
        .await
        .expect("deploy should succeed");

    let output = service
        .invoke("simple-deno-invoke".to_string(), test_invocation())
        .await
        .expect("invoke should succeed");
    assert_eq!(output.status, 200);
    assert_eq!(
        String::from_utf8(output.body).expect("body should be utf8"),
        "ok"
    );
}

#[tokio::test]
#[serial]
async fn reuse_preserves_state() {
    let service = test_service(RuntimeConfig {
        min_isolates: 0,
        max_isolates: 2,
        max_inflight_per_isolate: 4,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy("counter".to_string(), counter_worker())
        .await
        .expect("deploy should succeed");

    let one = service
        .invoke("counter".to_string(), test_invocation())
        .await
        .expect("first invoke should succeed");
    let two = service
        .invoke("counter".to_string(), test_invocation())
        .await
        .expect("second invoke should succeed");

    assert_eq!(String::from_utf8(one.body).expect("utf8"), "1");
    assert_eq!(String::from_utf8(two.body).expect("utf8"), "2");
}

#[tokio::test]
#[serial]
async fn spectre_time_mitigation_freezes_time_between_io_boundaries() {
    let service = test_service(RuntimeConfig {
        min_isolates: 0,
        max_isolates: 1,
        max_inflight_per_isolate: 1,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy("frozen-time".to_string(), frozen_time_worker())
        .await
        .expect("deploy should succeed");

    let output = service
        .invoke("frozen-time".to_string(), test_invocation())
        .await
        .expect("invoke should succeed");
    let payload: FrozenTimeState = crate::json::from_string(
        String::from_utf8(output.body).expect("frozen-time body should be utf8"),
    )
    .expect("frozen-time response should parse");

    assert_eq!(
        payload.now0, payload.now1,
        "Date.now should remain frozen during pure compute"
    );
    assert_eq!(
        payload.perf0, payload.perf1,
        "performance.now should remain frozen during pure compute"
    );
    assert!(
        payload.now2 >= payload.now1,
        "Date.now should not move backwards across I/O boundaries"
    );
    assert!(
        payload.perf2 >= payload.perf1,
        "performance.now should not move backwards across I/O boundaries"
    );
    assert!(
        payload.now2 > payload.now1 || payload.perf2 > payload.perf1,
        "expected frozen clocks to advance after an I/O boundary"
    );
    assert!(payload.guard > 0, "worker should run local compute loop");
}

#[tokio::test]
#[serial]
async fn crypto_globals_work_with_deno_crypto_ops() {
    let service = test_service(RuntimeConfig {
        min_isolates: 0,
        max_isolates: 1,
        max_inflight_per_isolate: 1,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy("crypto-worker".to_string(), crypto_worker())
        .await
        .expect("deploy should succeed");

    let output = service
        .invoke("crypto-worker".to_string(), test_invocation())
        .await
        .expect("invoke should succeed");
    let payload: CryptoState =
        crate::json::from_string(String::from_utf8(output.body).expect("body should be utf8"))
            .expect("response should parse");

    assert_eq!(payload.random_length, 16);
    assert!(
        payload.random_non_zero,
        "random bytes should not be all zero"
    );
    assert_eq!(payload.digest_length, 32);
    assert_eq!(
        payload.digest_hex,
        "fdc8751e9cb507759ed6fb3f14b311bb5427acb288ebc5c70e4e06f5c8471d04"
    );
    assert_eq!(payload.hmac_signature_length, 32);
    assert!(payload.hmac_verified, "HMAC signature should verify");
    assert!(
        payload.aes_ciphertext_length > "secret-data".len(),
        "AES-GCM ciphertext should include authentication tag"
    );
    assert_eq!(payload.aes_roundtrip, "secret-data");
    assert!(
        payload.asymmetric_signature_length > 0,
        "asymmetric signature should be non-empty"
    );
    assert!(
        payload.asymmetric_verified,
        "asymmetric signature should verify"
    );
    assert_eq!(payload.uuid.len(), 36, "uuid should be canonical v4 length");
}

#[tokio::test]
#[serial]
async fn lazy_kv_get_batching_matches_sequential_reads_and_preserves_duplicates() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 8,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    let worker_name = "kv-batching-equality".to_string();
    service
        .deploy_with_config(
            worker_name.clone(),
            kv_batching_worker(&worker_name),
            DeployConfig {
                bindings: vec![DeployBinding::Kv {
                    binding: "MY_KV".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");
    service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/seed", "kv-seed-request"),
        )
        .await
        .expect("seed should succeed");

    let sequential = service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/sequential", "kv-sequential-request"),
        )
        .await
        .expect("sequential read should succeed");
    let queued = service
        .invoke(
            worker_name,
            test_invocation_with_path("/queued", "kv-queued-request"),
        )
        .await
        .expect("queued read should succeed");

    let sequential_values: Vec<Value> = crate::json::from_string(
        String::from_utf8(sequential.body).expect("sequential body should be utf8"),
    )
    .expect("sequential response should parse");
    let queued_values: Vec<Value> = crate::json::from_string(
        String::from_utf8(queued.body).expect("queued body should be utf8"),
    )
    .expect("queued response should parse");

    assert_eq!(queued_values, sequential_values);
    assert_eq!(queued_values.len(), 10);
    assert!(queued_values.iter().all(|value| value == "1"));
}

#[tokio::test]
#[serial]
async fn lazy_kv_get_batching_decodes_mixed_values_and_rejects_whole_batch_on_failure() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 8,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    let worker_name = "kv-batching-mixed".to_string();
    service
        .deploy_with_config(
            worker_name.clone(),
            kv_batching_worker(&worker_name),
            DeployConfig {
                bindings: vec![DeployBinding::Kv {
                    binding: "MY_KV".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");
    service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/seed", "kv-mixed-seed-request"),
        )
        .await
        .expect("seed should succeed");

    let mixed = service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/mixed", "kv-mixed-request"),
        )
        .await
        .expect("mixed read should succeed");
    let mixed_values: Vec<Value> =
        crate::json::from_string(String::from_utf8(mixed.body).expect("mixed body should be utf8"))
            .expect("mixed response should parse");
    assert_eq!(
        mixed_values,
        vec![
            Value::String("plain".to_string()),
            crate::json::from_string::<Value>(r#"{"ok":true,"n":7}"#.to_string())
                .expect("object value should parse"),
            Value::Null,
            Value::String("plain".to_string()),
            crate::json::from_string::<Value>(r#"{"ok":true,"n":7}"#.to_string())
                .expect("object value should parse"),
        ]
    );

    let listed = service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/list-object", "kv-list-object-request"),
        )
        .await
        .expect("list object read should succeed");
    let listed_values: Vec<Value> =
        crate::json::from_string(String::from_utf8(listed.body).expect("list body should be utf8"))
            .expect("list response should parse");
    assert_eq!(listed_values.len(), 1);
    assert_eq!(
        listed_values[0]["key"],
        Value::String("obj-list".to_string())
    );
    assert_eq!(
        listed_values[0]["encoding"],
        Value::String("v8sc".to_string())
    );
    assert_eq!(listed_values[0]["value"]["ok"], Value::Bool(true));
    assert_eq!(listed_values[0]["value"]["n"], Value::from(17));

    let rejected = service
        .invoke(
            worker_name,
            test_invocation_with_path("/reject", "kv-reject-request"),
        )
        .await
        .expect("reject route should return response");
    assert_eq!(rejected.status, 500);
    let body = String::from_utf8(rejected.body).expect("reject body should be utf8");
    assert!(
        body.contains("deserialize failed"),
        "expected queued batch rejection body, got: {body}"
    );
}

#[tokio::test]
#[serial]
async fn lazy_kv_get_batching_is_scoped_to_each_request() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 16,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    let worker_name = "kv-batching-scope".to_string();
    service
        .deploy_with_config(
            worker_name.clone(),
            kv_batching_worker(&worker_name),
            DeployConfig {
                bindings: vec![DeployBinding::Kv {
                    binding: "MY_KV".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");
    service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/seed", "kv-scope-seed-request"),
        )
        .await
        .expect("seed should succeed");

    let left_request = test_invocation_with_path("/scoped?key=left", "kv-scope-left-request");
    let right_request = test_invocation_with_path("/scoped?key=right", "kv-scope-right-request");
    let (left, right) = tokio::join!(
        service.invoke(worker_name.clone(), left_request),
        service.invoke(worker_name, right_request)
    );
    let left_values: Vec<Value> = crate::json::from_string(
        String::from_utf8(left.expect("left invoke should succeed").body)
            .expect("left body should be utf8"),
    )
    .expect("left response should parse");
    let right_values: Vec<Value> = crate::json::from_string(
        String::from_utf8(right.expect("right invoke should succeed").body)
            .expect("right body should be utf8"),
    )
    .expect("right response should parse");

    assert_eq!(left_values.len(), 10);
    assert_eq!(right_values.len(), 10);
    assert!(left_values.iter().all(|value| value == "L"));
    assert!(right_values.iter().all(|value| value == "R"));
}

#[tokio::test]
#[serial]
async fn shared_env_is_reused_safely_across_requests() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 8,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    let worker_name = "shared-env-reuse".to_string();
    service
        .deploy_with_config(
            worker_name.clone(),
            reusable_env_worker(),
            DeployConfig {
                bindings: vec![DeployBinding::Kv {
                    binding: "MY_KV".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");

    let first = service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/", "shared-env-first-request"),
        )
        .await
        .expect("first invoke should succeed");
    let second = service
        .invoke(
            worker_name,
            test_invocation_with_path("/", "shared-env-second-request"),
        )
        .await
        .expect("second invoke should succeed");

    let first_payload: Value =
        crate::json::from_string(String::from_utf8(first.body).expect("first body should be utf8"))
            .expect("first response should parse");
    let second_payload: Value = crate::json::from_string(
        String::from_utf8(second.body).expect("second body should be utf8"),
    )
    .expect("second response should parse");

    assert_eq!(first_payload["sameEnv"], Value::Bool(false));
    assert_eq!(first_payload["sameKv"], Value::Bool(false));
    assert_eq!(second_payload["sameEnv"], Value::Bool(true));
    assert_eq!(second_payload["sameKv"], Value::Bool(true));

    for payload in [&first_payload, &second_payload] {
        assert_eq!(payload["envExtensible"], Value::Bool(false));
        assert_eq!(payload["kvExtensible"], Value::Bool(false));
        assert_eq!(payload["envMutationResult"], Value::Bool(false));
        assert_eq!(payload["kvMutationResult"], Value::Bool(false));
        assert_eq!(payload["envHasTemp"], Value::Bool(false));
        assert_eq!(payload["kvHasTemp"], Value::Bool(false));
    }
}

#[tokio::test]
#[serial]
async fn kv_write_batching_preserves_last_write_wins() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 8,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    let worker_name = "kv-write-batching".to_string();
    service
        .deploy_with_config(
            worker_name.clone(),
            kv_write_worker(),
            DeployConfig {
                bindings: vec![DeployBinding::Kv {
                    binding: "MY_KV".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");
    service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/seed", "kv-write-batch-seed-request"),
        )
        .await
        .expect("seed should succeed");

    let output = service
        .invoke(
            worker_name,
            test_invocation_with_path("/write-batch", "kv-write-batch-request"),
        )
        .await
        .expect("write batch should succeed");
    assert_eq!(
        String::from_utf8(output.body).expect("body should be utf8"),
        "4"
    );
}

#[tokio::test]
#[serial]
async fn kv_write_overlay_makes_same_request_reads_predictable() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 8,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    let worker_name = "kv-write-overlay".to_string();
    service
        .deploy_with_config(
            worker_name.clone(),
            kv_write_worker(),
            DeployConfig {
                bindings: vec![DeployBinding::Kv {
                    binding: "MY_KV".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");
    service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/seed", "kv-write-overlay-seed-request"),
        )
        .await
        .expect("seed should succeed");

    let output = service
        .invoke(
            worker_name,
            test_invocation_with_path("/write-overlay", "kv-write-overlay-request"),
        )
        .await
        .expect("write overlay should succeed");
    assert_eq!(
        String::from_utf8(output.body).expect("body should be utf8"),
        "9"
    );
}

#[tokio::test]
#[serial]
async fn kv_queued_durability_returns_explicit_version_ack() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 8,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    let worker_name = "kv-queued-durability".to_string();
    service
        .deploy_with_config(
            worker_name.clone(),
            kv_write_worker(),
            DeployConfig {
                bindings: vec![DeployBinding::Kv {
                    binding: "MY_KV".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");
    service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/seed", "kv-queued-durability-seed"),
        )
        .await
        .expect("seed should succeed");

    let put = service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/put-queued-version-read", "kv-queued-durability-put"),
        )
        .await
        .expect("queued put should succeed");
    let put_body: Value =
        serde_json::from_slice(&put.body).expect("queued put response should be json");
    assert_eq!(put_body["queued"], Value::Bool(true));
    assert_eq!(put_body["durability"], Value::String("queued".to_string()));
    assert!(put_body["version"].as_i64().expect("queued put version") > 0);
    assert_eq!(put_body["value"], Value::String("13".to_string()));

    let delete = service
        .invoke(
            worker_name,
            test_invocation_with_path("/delete-queued-version-read", "kv-queued-durability-delete"),
        )
        .await
        .expect("queued delete should succeed");
    let delete_body: Value =
        serde_json::from_slice(&delete.body).expect("queued delete response should be json");
    assert_eq!(delete_body["queued"], Value::Bool(true));
    assert_eq!(
        delete_body["durability"],
        Value::String("queued".to_string())
    );
    assert!(
        delete_body["version"]
            .as_i64()
            .expect("queued delete version")
            > 0
    );
    assert_eq!(delete_body["value"], Value::Null);
}

#[tokio::test]
#[serial]
async fn kv_committed_durability_awaits_canonical_write() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 8,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    let worker_name = "kv-committed-durability".to_string();
    service
        .deploy_with_config(
            worker_name.clone(),
            kv_write_worker(),
            DeployConfig {
                bindings: vec![DeployBinding::Kv {
                    binding: "MY_KV".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");
    service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/seed", "kv-committed-durability-seed"),
        )
        .await
        .expect("seed should succeed");

    let put = service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/put-committed-read", "kv-committed-durability-put"),
        )
        .await
        .expect("committed put should succeed");
    let put_body = String::from_utf8(put.body).expect("put body should be utf8");
    let (put_version, put_value) = put_body
        .split_once(':')
        .expect("committed put should return version and value");
    assert!(put_version.parse::<i64>().expect("put version") > 0);
    assert_eq!(put_value, "12");

    let put_object = service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path(
                "/put-committed-object-read",
                "kv-committed-durability-put-object",
            ),
        )
        .await
        .expect("committed object put should succeed");
    let object_body: Value =
        serde_json::from_slice(&put_object.body).expect("object response should be json");
    assert!(object_body["version"].as_i64().expect("object version") > 0);
    assert_eq!(object_body["value"]["ok"], Value::Bool(true));
    assert_eq!(object_body["value"]["n"], Value::from(12));

    let delete = service
        .invoke(
            worker_name,
            test_invocation_with_path("/delete-committed-read", "kv-committed-durability-delete"),
        )
        .await
        .expect("committed delete should succeed");
    let delete_body = String::from_utf8(delete.body).expect("delete body should be utf8");
    let (delete_version, delete_value) = delete_body
        .split_once(':')
        .expect("committed delete should return version and value");
    assert!(delete_version.parse::<i64>().expect("delete version") > 0);
    assert_eq!(delete_value, "missing");
}

#[tokio::test]
#[serial]
async fn kv_read_cache_hits_across_requests_in_same_isolate() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 1,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        kv_profile_enabled: true,
        ..RuntimeConfig::default()
    })
    .await;

    let worker_name = "kv-read-cache-hit".to_string();
    service
        .deploy_with_config(
            worker_name.clone(),
            kv_write_worker(),
            DeployConfig {
                bindings: vec![DeployBinding::Kv {
                    binding: "MY_KV".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");
    service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/seed", "kv-read-cache-hit-seed"),
        )
        .await
        .expect("seed should succeed");
    service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/__profile_reset", "kv-read-cache-hit-profile-reset"),
        )
        .await
        .expect("profile reset should succeed");

    let first = service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/read", "kv-read-cache-hit-read-1"),
        )
        .await
        .expect("first read should succeed");
    let second = service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/read", "kv-read-cache-hit-read-2"),
        )
        .await
        .expect("second read should succeed");
    let profile = decode_kv_profile(
        service
            .invoke(
                worker_name,
                test_invocation_with_path("/__profile", "kv-read-cache-hit-profile"),
            )
            .await
            .expect("profile should succeed"),
    );

    assert_eq!(String::from_utf8(first.body).expect("utf8"), "1");
    assert_eq!(String::from_utf8(second.body).expect("utf8"), "1");
    assert_eq!(profile.op_get.calls, 0);
    assert!(profile.js_cache_hit.calls >= 2);
}

#[tokio::test]
#[serial]
async fn kv_read_cache_caches_missing_keys_across_requests() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 1,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        kv_profile_enabled: true,
        ..RuntimeConfig::default()
    })
    .await;

    let worker_name = "kv-read-cache-miss".to_string();
    service
        .deploy_with_config(
            worker_name.clone(),
            kv_write_worker(),
            DeployConfig {
                bindings: vec![DeployBinding::Kv {
                    binding: "MY_KV".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");
    service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/__profile_reset", "kv-read-cache-miss-profile-reset"),
        )
        .await
        .expect("profile reset should succeed");

    let first = service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/read-missing", "kv-read-cache-miss-read-1"),
        )
        .await
        .expect("first missing read should succeed");
    let second = service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/read-missing", "kv-read-cache-miss-read-2"),
        )
        .await
        .expect("second missing read should succeed");
    let profile = decode_kv_profile(
        service
            .invoke(
                worker_name,
                test_invocation_with_path("/__profile", "kv-read-cache-miss-profile"),
            )
            .await
            .expect("profile should succeed"),
    );

    assert_eq!(String::from_utf8(first.body).expect("utf8"), "missing");
    assert_eq!(String::from_utf8(second.body).expect("utf8"), "missing");
    assert_eq!(profile.op_get.calls, 1);
    assert!(profile.js_cache_miss.calls >= 1);
    assert!(profile.js_cache_hit.calls >= 1);
}

#[tokio::test]
#[serial]
async fn kv_read_cache_expires_and_refills_after_ttl() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 1,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        kv_profile_enabled: true,
        kv_read_cache_hit_ttl: Duration::from_millis(20),
        kv_read_cache_miss_ttl: Duration::from_millis(20),
        ..RuntimeConfig::default()
    })
    .await;

    let worker_name = "kv-read-cache-expiry".to_string();
    service
        .deploy_with_config(
            worker_name.clone(),
            kv_write_worker(),
            DeployConfig {
                bindings: vec![DeployBinding::Kv {
                    binding: "MY_KV".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");
    service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/__profile_reset", "kv-read-cache-expiry-profile-reset"),
        )
        .await
        .expect("profile reset should succeed");

    let first = service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/read-missing", "kv-read-cache-expiry-read-1"),
        )
        .await
        .expect("first read should succeed");
    tokio::time::sleep(Duration::from_millis(40)).await;
    let second = service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/read-missing", "kv-read-cache-expiry-read-2"),
        )
        .await
        .expect("second read should succeed");
    let profile = decode_kv_profile(
        service
            .invoke(
                worker_name,
                test_invocation_with_path("/__profile", "kv-read-cache-expiry-profile"),
            )
            .await
            .expect("profile should succeed"),
    );

    assert_eq!(String::from_utf8(first.body).expect("utf8"), "missing");
    assert_eq!(String::from_utf8(second.body).expect("utf8"), "missing");
    assert!(profile.op_get.calls >= 2);
    assert!(profile.js_cache_stale.calls >= 1);
}

#[tokio::test]
#[serial]
async fn kv_local_cache_updates_immediately_after_put_and_delete() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 1,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    let worker_name = "kv-local-cache-updates".to_string();
    service
        .deploy_with_config(
            worker_name.clone(),
            kv_write_worker(),
            DeployConfig {
                bindings: vec![DeployBinding::Kv {
                    binding: "MY_KV".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");
    service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/seed", "kv-local-cache-updates-seed"),
        )
        .await
        .expect("seed should succeed");

    service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/write-fire-and-forget", "kv-local-cache-updates-put"),
        )
        .await
        .expect("put should enqueue");
    let read_after_put = service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/read", "kv-local-cache-updates-read-after-put"),
        )
        .await
        .expect("read after put should succeed");

    service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/delete-fire-and-forget", "kv-local-cache-updates-delete"),
        )
        .await
        .expect("delete should enqueue");
    let read_after_delete = service
        .invoke(
            worker_name,
            test_invocation_with_path("/read", "kv-local-cache-updates-read-after-delete"),
        )
        .await
        .expect("read after delete should succeed");

    assert_eq!(String::from_utf8(read_after_put.body).expect("utf8"), "7");
    assert_eq!(
        String::from_utf8(read_after_delete.body).expect("utf8"),
        "missing"
    );
}

#[tokio::test]
#[serial]
async fn kv_unawaited_write_flushes_after_response() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 8,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    let worker_name = "kv-write-fire-and-forget".to_string();
    service
        .deploy_with_config(
            worker_name.clone(),
            kv_write_worker(),
            DeployConfig {
                bindings: vec![DeployBinding::Kv {
                    binding: "MY_KV".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");
    service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/seed", "kv-write-fire-seed-request"),
        )
        .await
        .expect("seed should succeed");

    service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/write-fire-and-forget", "kv-write-fire-request"),
        )
        .await
        .expect("fire-and-forget request should succeed");

    let mut observed = None;
    for _ in 0..20 {
        let output = service
            .invoke(
                worker_name.clone(),
                test_invocation_with_path("/read", "kv-write-fire-read-request"),
            )
            .await
            .expect("read request should succeed");
        let body = String::from_utf8(output.body).expect("read body should be utf8");
        if body == "7" {
            observed = Some(body);
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert_eq!(observed.as_deref(), Some("7"));
}

#[tokio::test]
#[serial]
async fn kv_wait_until_write_flushes_after_response() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 8,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    let worker_name = "kv-write-wait-until".to_string();
    service
        .deploy_with_config(
            worker_name.clone(),
            kv_write_worker(),
            DeployConfig {
                bindings: vec![DeployBinding::Kv {
                    binding: "MY_KV".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");
    service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/seed", "kv-write-wait-until-seed-request"),
        )
        .await
        .expect("seed should succeed");

    service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/write-wait-until", "kv-write-wait-until-request"),
        )
        .await
        .expect("wait-until request should succeed");

    let mut observed = None;
    for _ in 0..20 {
        let output = service
            .invoke(
                worker_name.clone(),
                test_invocation_with_path("/read", "kv-write-wait-until-read-request"),
            )
            .await
            .expect("read request should succeed");
        let body = String::from_utf8(output.body).expect("read body should be utf8");
        if body == "8" {
            observed = Some(body);
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let status = service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path(
                "/write-wait-until-result",
                "kv-write-wait-until-status-request",
            ),
        )
        .await
        .expect("status request should succeed");
    let status_body = String::from_utf8(status.body).expect("status body should be utf8");

    assert_eq!(status_body, "ok");
    assert_eq!(observed.as_deref(), Some("8"));
}

#[tokio::test]
#[serial]
async fn kv_wait_until_read_runs_after_response() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 8,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    let worker_name = "kv-read-wait-until".to_string();
    service
        .deploy_with_config(
            worker_name.clone(),
            kv_wait_until_read_worker(),
            DeployConfig {
                bindings: vec![DeployBinding::Kv {
                    binding: "MY_KV".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");
    service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/seed", "kv-read-wait-until-seed-request"),
        )
        .await
        .expect("seed should succeed");

    service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/read-wait-until", "kv-read-wait-until-request"),
        )
        .await
        .expect("wait-until read request should succeed");

    let mut observed = None;
    for _ in 0..20 {
        let output = service
            .invoke(
                worker_name.clone(),
                test_invocation_with_path(
                    "/read-wait-until-result",
                    "kv-read-wait-until-result-request",
                ),
            )
            .await
            .expect("result request should succeed");
        let body = String::from_utf8(output.body).expect("result body should be utf8");
        if body == "1" {
            observed = Some(body);
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert_eq!(observed.as_deref(), Some("1"));
}

#[tokio::test]
#[serial]
async fn wait_until_background_work_runs_after_response() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 8,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    let worker_name = "wait-until-basic".to_string();
    service
        .deploy(worker_name.clone(), wait_until_worker())
        .await
        .expect("deploy should succeed");

    service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/trigger", "wait-until-trigger-request"),
        )
        .await
        .expect("trigger request should succeed");

    let mut observed = None;
    for _ in 0..20 {
        let output = service
            .invoke(
                worker_name.clone(),
                test_invocation_with_path("/read", "wait-until-read-request"),
            )
            .await
            .expect("read request should succeed");
        let body = String::from_utf8(output.body).expect("read body should be utf8");
        if body == "done" {
            observed = Some(body);
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert_eq!(observed.as_deref(), Some("done"));
}

#[tokio::test]
#[serial]
async fn dynamic_worker_fetch_uses_deno_fetch_with_host_policy_and_secret_replacement() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 2,
        max_inflight_per_isolate: 4,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let address = listener.local_addr().expect("listener should have addr");
    let (request_tx, request_rx) = tokio::sync::oneshot::channel::<String>();
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept should succeed");
        let mut buffer = vec![0_u8; 8192];
        let bytes_read = socket
            .read(&mut buffer)
            .await
            .expect("server read should succeed");
        request_tx
            .send(String::from_utf8_lossy(&buffer[..bytes_read]).to_string())
            .expect("request should be captured");
        socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\ncontent-length: 2\r\nconnection: close\r\n\r\nok",
                )
                .await
                .expect("server write should succeed");
    });

    let deployed = service
        .deploy_dynamic(
            dynamic_fetch_probe_worker(&format!("http://{address}/fetch-probe")),
            HashMap::from([("API_TOKEN".to_string(), "secret-value".to_string())]),
            vec![format!("private:{address}")],
        )
        .await
        .expect("dynamic deploy should succeed");

    let output = service
        .invoke(deployed.worker, test_invocation())
        .await
        .expect("dynamic fetch invoke should succeed");
    assert_eq!(output.status, 200);
    assert_eq!(String::from_utf8(output.body).expect("utf8"), "ok");

    let raw_request = request_rx.await.expect("request should arrive");
    assert!(
        raw_request.starts_with("GET /fetch-probe?token=secret-value HTTP/1.1\r\n"),
        "raw request was {raw_request}"
    );
    assert!(
        raw_request.contains("\r\nauthorization: Bearer secret-value\r\n"),
        "raw request was {raw_request}"
    );
    assert!(
        raw_request.contains("\r\nx-dd-secret: secret-value\r\n"),
        "raw request was {raw_request}"
    );
    assert!(
        !raw_request.contains("__DD_SECRET_"),
        "secret placeholders leaked into outbound request: {raw_request}"
    );
}

#[tokio::test]
#[serial]
async fn dynamic_worker_fetch_revalidates_redirect_and_strips_cross_origin_credentials() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 2,
        max_inflight_per_isolate: 4,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    let destination = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("destination listener should bind");
    let destination_address = destination.local_addr().expect("destination address");
    let (request_tx, request_rx) = tokio::sync::oneshot::channel::<String>();
    tokio::spawn(async move {
        let (mut socket, _) = destination.accept().await.expect("destination accept");
        let mut buffer = vec![0_u8; 8192];
        let bytes_read = socket.read(&mut buffer).await.expect("destination read");
        request_tx
            .send(String::from_utf8_lossy(&buffer[..bytes_read]).to_string())
            .expect("request should be captured");
        socket
            .write_all(
                b"HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\ncontent-length: 2\r\nconnection: close\r\n\r\nok",
            )
            .await
            .expect("destination write");
    });

    let redirect = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("redirect listener should bind");
    let redirect_address = redirect.local_addr().expect("redirect address");
    tokio::spawn(async move {
        let (mut socket, _) = redirect.accept().await.expect("redirect accept");
        let mut buffer = vec![0_u8; 4096];
        let _ = socket.read(&mut buffer).await.expect("redirect read");
        let response = format!(
            "HTTP/1.1 302 Found\r\nlocation: http://{destination_address}/final\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
        );
        socket
            .write_all(response.as_bytes())
            .await
            .expect("redirect write");
    });

    let deployed = service
        .deploy_dynamic(
            dynamic_fetch_probe_worker(&format!("http://{redirect_address}/start")),
            HashMap::from([("API_TOKEN".to_string(), "redirect-secret".to_string())]),
            vec![
                format!("private:{redirect_address}"),
                format!("private:{destination_address}"),
            ],
        )
        .await
        .expect("dynamic deploy should succeed");

    let output = service
        .invoke(deployed.worker, test_invocation())
        .await
        .expect("redirected fetch should succeed");
    assert_eq!(output.status, 200);
    assert_eq!(String::from_utf8(output.body).expect("utf8"), "ok");

    let redirected_request = request_rx.await.expect("redirected request should arrive");
    assert!(redirected_request.starts_with("GET /final HTTP/1.1\r\n"));
    assert!(
        !redirected_request
            .to_ascii_lowercase()
            .contains("\r\nauthorization:"),
        "cross-origin authorization leaked: {redirected_request}"
    );
}

#[tokio::test]
#[serial]
async fn dynamic_worker_fetch_rejects_egress_hosts_outside_allowlist() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 2,
        max_inflight_per_isolate: 4,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    let deployed = service
        .deploy_dynamic(
            dynamic_fetch_probe_worker("http://127.0.0.1:9/blocked"),
            HashMap::from([("API_TOKEN".to_string(), "secret-value".to_string())]),
            vec!["example.com".to_string()],
        )
        .await
        .expect("dynamic deploy should succeed");

    let error = service
        .invoke(deployed.worker, test_invocation())
        .await
        .expect_err("dynamic fetch invoke should fail");
    let body = error.to_string();
    assert!(
        body.contains("egress origin is not allowed: http://127.0.0.1:9"),
        "body was {body}"
    );
}

#[tokio::test]
#[serial]
async fn dynamic_worker_fetch_abort_signal_cancels_outbound_request() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 2,
        max_inflight_per_isolate: 4,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let address = listener.local_addr().expect("listener should have addr");
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept should succeed");
        let mut buffer = vec![0_u8; 4096];
        let _ = socket.read(&mut buffer).await;
        sleep(Duration::from_millis(200)).await;
        let _ = socket.shutdown().await;
    });

    let deployed = service
        .deploy_dynamic(
            dynamic_fetch_abort_worker(&format!("http://{address}/abort-probe")),
            HashMap::new(),
            vec![address.to_string()],
        )
        .await
        .expect("dynamic deploy should succeed");

    let started_at = Instant::now();
    let output = timeout(
        Duration::from_secs(2),
        service.invoke(deployed.worker, test_invocation()),
    )
    .await
    .expect("invoke should not hang")
    .expect("invoke should succeed");
    assert_eq!(output.status, 200);
    let body = String::from_utf8(output.body).expect("utf8");
    assert!(
        body == "Error" || body.contains("Abort") || body.to_ascii_lowercase().contains("abort"),
        "body was {body}"
    );
    assert!(
        started_at.elapsed() < Duration::from_millis(500),
        "abort should finish quickly"
    );
}

#[tokio::test]
#[serial]
async fn preview_dynamic_worker_can_proxy_module_based_children() {
    let service = test_service(RuntimeConfig {
        min_isolates: 0,
        max_isolates: 2,
        max_inflight_per_isolate: 2,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy_with_config(
            "preview-dynamic".to_string(),
            preview_dynamic_worker(),
            DeployConfig {
                bindings: vec![DeployBinding::Dynamic {
                    binding: "SANDBOX".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");

    let root = service
        .invoke(
            "preview-dynamic".to_string(),
            test_invocation_with_path("/preview/pr-123", "preview-root"),
        )
        .await
        .expect("preview root should succeed");
    assert_eq!(root.status, 200);
    let root_text = String::from_utf8(root.body).expect("utf8");
    assert!(root_text.contains("\"preview\":\"pr-123\""));
    assert!(root_text.contains("\"route\":\"root\""));

    let health = service
        .invoke(
            "preview-dynamic".to_string(),
            test_invocation_with_path("/preview/pr-123/api/health", "preview-health"),
        )
        .await
        .expect("preview health should succeed");
    assert_eq!(health.status, 200);
    let health_text = String::from_utf8(health.body).expect("utf8");
    assert!(health_text.contains("\"route\":\"health\""));
}

#[tokio::test]
#[serial]
async fn scales_up_with_backlog() {
    let service = test_service(RuntimeConfig {
        min_isolates: 0,
        max_global_isolates: 4,
        max_isolates: 4,
        max_inflight_per_isolate: 4,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy("slow".to_string(), slow_worker())
        .await
        .expect("deploy should succeed");

    let mut tasks = Vec::new();
    for idx in 0..12 {
        let svc = service.clone();
        tasks.push(tokio::spawn(async move {
            let mut req = test_invocation();
            req.request_id = format!("req-{idx}");
            svc.invoke("slow".to_string(), req).await
        }));
    }

    timeout(Duration::from_secs(10), async {
        for task in tasks {
            task.await.expect("join").expect("invoke should succeed");
        }
    })
    .await
    .expect("single worker requests should finish");

    let stats = service
        .stats("slow".to_string())
        .await
        .expect("stats should exist");
    assert!(stats.spawn_count > 1);
    assert!(stats.isolates_total <= 4);
    assert_eq!(stats.global_isolate_budget, 4);
    assert!(stats.global_isolates_total <= 4);
}

#[tokio::test]
#[serial]
async fn single_worker_can_grow_to_global_isolate_budget() {
    let service = test_service(RuntimeConfig {
        min_isolates: 0,
        max_global_isolates: 2,
        max_isolates: 8,
        max_inflight_per_isolate: 4,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(20),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy("slow".to_string(), slow_worker())
        .await
        .expect("deploy should succeed");

    let mut tasks = Vec::new();
    for idx in 0..12 {
        let svc = service.clone();
        tasks.push(tokio::spawn(async move {
            let mut req = test_invocation();
            req.request_id = format!("budget-req-{idx}");
            svc.invoke("slow".to_string(), req).await
        }));
    }

    timeout(Duration::from_secs(10), async {
        for task in tasks {
            task.await.expect("join").expect("invoke should succeed");
        }
    })
    .await
    .expect("single worker requests should finish");

    let stats = service
        .stats("slow".to_string())
        .await
        .expect("stats should exist");
    assert_eq!(stats.global_isolate_budget, 2);
    assert!(stats.spawn_count > 1);
    assert!(stats.isolates_total <= 2);
    assert!(stats.global_isolates_total <= 2);
    assert_eq!(
        stats.global_isolate_slots_available,
        2usize.saturating_sub(stats.global_isolates_total)
    );
}

#[tokio::test]
#[serial]
async fn active_workers_share_small_global_isolate_budget() {
    let service = test_service(RuntimeConfig {
        min_isolates: 0,
        max_global_isolates: 2,
        max_isolates: 4,
        max_inflight_per_isolate: 4,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(20),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy("slow-a".to_string(), slow_worker())
        .await
        .expect("deploy a should succeed");
    service
        .deploy("slow-b".to_string(), slow_worker())
        .await
        .expect("deploy b should succeed");

    let mut tasks = Vec::new();
    for worker in ["slow-a", "slow-b"] {
        for idx in 0..4 {
            let svc = service.clone();
            let worker = worker.to_string();
            tasks.push(tokio::spawn(async move {
                let mut req = test_invocation();
                req.request_id = format!("{worker}-req-{idx}");
                svc.invoke(worker, req).await
            }));
        }
    }

    for task in tasks {
        task.await.expect("join").expect("invoke should succeed");
    }

    let a = service.stats("slow-a".to_string()).await.expect("stats a");
    let b = service.stats("slow-b".to_string()).await.expect("stats b");
    assert!(a.spawn_count >= 1);
    assert!(b.spawn_count >= 1);
    assert!(a.isolates_total + b.isolates_total <= 2);
    assert_eq!(a.global_isolates_total, b.global_isolates_total);
    assert!(a.global_isolates_total <= 2);
}

#[test]
fn runtime_default_global_isolate_budget_has_two_slot_floor() {
    assert!(RuntimeConfig::default().max_global_isolates >= 2);
}

#[tokio::test]
#[serial]
async fn cold_routes_retire_lru_idle_isolates_without_waiting_for_idle_ttl() {
    let service = test_service(RuntimeConfig {
        min_isolates: 0,
        max_global_isolates: 2,
        max_isolates: 1,
        max_inflight_per_isolate: 1,
        idle_ttl: Duration::from_secs(60 * 60),
        scale_tick: Duration::from_millis(20),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    let workers = ["cold-a", "cold-b", "cold-c", "cold-d"];
    for worker in workers {
        service
            .deploy(worker.to_string(), counter_worker())
            .await
            .expect("deploy should succeed");
    }

    for (index, worker) in workers.into_iter().enumerate() {
        let mut request = test_invocation();
        request.request_id = format!("cold-route-{index}");
        let output = timeout(
            Duration::from_secs(2),
            service.invoke(worker.to_string(), request),
        )
        .await
        .expect("cold route must not wait for the one-hour idle TTL")
        .expect("cold route should succeed");
        assert_eq!(output.status, 200);
        sleep(Duration::from_millis(20)).await;
    }

    let first = service
        .stats("cold-a".to_string())
        .await
        .expect("first worker stats should exist");
    let second = service
        .stats("cold-b".to_string())
        .await
        .expect("second worker stats should exist");
    let last = service
        .stats("cold-d".to_string())
        .await
        .expect("last worker stats should exist");
    assert_eq!(first.isolates_total, 0, "oldest idle isolate should retire");
    assert_eq!(
        second.isolates_total, 0,
        "next-oldest idle isolate should retire"
    );
    assert_eq!(last.global_isolate_budget, 2);
    assert!(last.global_isolates_total <= 2);
}

#[tokio::test]
#[serial]
async fn budget_pressure_never_retires_an_isolate_with_an_active_request() {
    let service = test_service(RuntimeConfig {
        min_isolates: 0,
        max_global_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 1,
        idle_ttl: Duration::from_secs(60 * 60),
        scale_tick: Duration::from_millis(20),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;
    service
        .deploy("active-a".to_string(), slow_worker())
        .await
        .expect("first deploy should succeed");
    service
        .deploy("active-b".to_string(), counter_worker())
        .await
        .expect("second deploy should succeed");

    let first_service = service.clone();
    let first = tokio::spawn(async move {
        first_service
            .invoke("active-a".to_string(), test_invocation())
            .await
    });
    sleep(Duration::from_millis(10)).await;
    let second = timeout(
        Duration::from_secs(2),
        service.invoke("active-b".to_string(), test_invocation()),
    );

    let (first, second) = tokio::join!(first, second);
    assert_eq!(
        String::from_utf8(
            first
                .expect("first task should join")
                .expect("active request must not be evicted")
                .body,
        )
        .expect("utf8"),
        "ok"
    );
    assert!(
        second
            .expect("second route should receive the slot after completion")
            .is_ok()
    );
}

#[tokio::test]
#[serial]
async fn budget_pressure_waits_for_wait_until_before_retiring_an_isolate() {
    let service = test_service(RuntimeConfig {
        min_isolates: 0,
        max_global_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 1,
        idle_ttl: Duration::from_secs(60 * 60),
        scale_tick: Duration::from_millis(20),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;
    service
        .deploy(
            "wait-until-a".to_string(),
            r#"
export default {
  async fetch(_request, _env, ctx) {
    ctx.waitUntil(Deno.core.ops.op_sleep(150));
    return new Response("queued");
  },
};
"#
            .to_string(),
        )
        .await
        .expect("waitUntil worker should deploy");
    service
        .deploy("wait-until-b".to_string(), counter_worker())
        .await
        .expect("second worker should deploy");

    let first = service
        .invoke("wait-until-a".to_string(), test_invocation())
        .await
        .expect("first response should succeed");
    assert_eq!(String::from_utf8(first.body).expect("utf8"), "queued");

    let started = Instant::now();
    timeout(
        Duration::from_secs(2),
        service.invoke("wait-until-b".to_string(), test_invocation()),
    )
    .await
    .expect("second worker should receive the slot after waitUntil")
    .expect("second request should succeed");
    assert!(
        started.elapsed() >= Duration::from_millis(100),
        "waitUntil isolate was retired before its background work completed"
    );
}

#[tokio::test]
#[serial]
async fn scales_down_when_idle() {
    let service = test_service(RuntimeConfig {
        min_isolates: 0,
        max_global_isolates: 3,
        max_isolates: 3,
        max_inflight_per_isolate: 4,
        idle_ttl: Duration::from_millis(200),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy("slow".to_string(), slow_worker())
        .await
        .expect("deploy should succeed");

    for idx in 0..6 {
        let mut req = test_invocation();
        req.request_id = format!("req-{idx}");
        service
            .invoke("slow".to_string(), req)
            .await
            .expect("invoke should succeed");
    }

    let before = service
        .stats("slow".to_string())
        .await
        .expect("stats should exist");
    assert!(before.isolates_total > 0);

    timeout(Duration::from_secs(3), async {
        loop {
            let stats = service.stats("slow".to_string()).await.expect("stats");
            if stats.isolates_total == 0 && stats.global_isolates_total == 0 {
                assert_eq!(stats.global_isolates_total, 0);
                assert_eq!(stats.global_isolate_slots_available, 3);
                break;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("isolates should scale down to zero");
}

#[tokio::test]
#[serial]
async fn invalid_redeploy_keeps_previous_generation() {
    let service = test_service(RuntimeConfig {
        min_isolates: 0,
        max_isolates: 2,
        max_inflight_per_isolate: 4,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy("counter".to_string(), counter_worker())
        .await
        .expect("initial deploy should succeed");

    let one = service
        .invoke("counter".to_string(), test_invocation())
        .await
        .expect("first invoke should succeed");
    assert_eq!(String::from_utf8(one.body).expect("utf8"), "1");

    let bad_redeploy = service
        .deploy("counter".to_string(), "export default {};".to_string())
        .await;
    assert!(bad_redeploy.is_err());

    let two = service
        .invoke("counter".to_string(), test_invocation())
        .await
        .expect("invoke should still use old generation");
    assert_eq!(String::from_utf8(two.body).expect("utf8"), "2");
}

#[tokio::test]
#[serial]
async fn redeploy_switches_new_traffic_while_old_generation_drains() {
    let service = test_service(RuntimeConfig {
        min_isolates: 0,
        max_isolates: 1,
        max_inflight_per_isolate: 4,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy("worker".to_string(), versioned_worker("v1", 120))
        .await
        .expect("deploy v1 should succeed");

    let svc_one = service.clone();
    let first = tokio::spawn(async move {
        let mut req = test_invocation();
        req.request_id = "first".to_string();
        svc_one.invoke("worker".to_string(), req).await
    });

    sleep(Duration::from_millis(10)).await;

    let svc_two = service.clone();
    let second = tokio::spawn(async move {
        let mut req = test_invocation();
        req.request_id = "second".to_string();
        svc_two.invoke("worker".to_string(), req).await
    });

    sleep(Duration::from_millis(10)).await;
    service
        .deploy("worker".to_string(), versioned_worker("v2", 0))
        .await
        .expect("deploy v2 should succeed");

    let mut third_req = test_invocation();
    third_req.request_id = "third".to_string();
    let third = service
        .invoke("worker".to_string(), third_req)
        .await
        .expect("third invoke should succeed");
    assert_eq!(String::from_utf8(third.body).expect("utf8"), "v2");

    let first_output = first.await.expect("join first").expect("first invoke");
    let second_output = second.await.expect("join second").expect("second invoke");
    assert_eq!(String::from_utf8(first_output.body).expect("utf8"), "v1");
    assert_eq!(String::from_utf8(second_output.body).expect("utf8"), "v1");
}

#[tokio::test]
#[serial]
async fn single_isolate_allows_multiple_inflight_requests() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 4,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy("io".to_string(), io_wait_worker())
        .await
        .expect("deploy should succeed");

    let started = Instant::now();
    let mut tasks = Vec::new();
    for idx in 0..2 {
        let svc = service.clone();
        tasks.push(tokio::spawn(async move {
            let mut req = test_invocation();
            req.request_id = format!("io-{idx}");
            svc.invoke("io".to_string(), req).await
        }));
    }

    for task in tasks {
        task.await.expect("join").expect("invoke should succeed");
    }
    let elapsed = started.elapsed();

    assert!(
        elapsed < Duration::from_millis(260),
        "expected multiplexed inflight execution, elapsed={elapsed:?}"
    );
}

#[tokio::test]
#[serial]
async fn dropped_invoke_aborts_inflight_request() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 1,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy("abortable".to_string(), abort_aware_worker())
        .await
        .expect("deploy should succeed");

    let service_for_blocked = service.clone();
    let blocked = tokio::spawn(async move {
        let mut req = test_invocation();
        req.request_id = "block".to_string();
        service_for_blocked
            .invoke("abortable".to_string(), req)
            .await
    });

    timeout(Duration::from_secs(1), async {
        loop {
            let stats = service.stats("abortable".to_string()).await.expect("stats");
            if stats.inflight_total == 1 {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("request should become inflight");

    blocked.abort();
    assert!(blocked.await.is_err(), "aborted task should be canceled");

    timeout(Duration::from_secs(2), async {
        loop {
            let stats = service.stats("abortable".to_string()).await.expect("stats");
            if stats.inflight_total == 0 && stats.queued == 0 {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("abort should clear inflight slot");

    let mut followup_req = test_invocation();
    followup_req.request_id = "after".to_string();
    let followup = service
        .invoke("abortable".to_string(), followup_req)
        .await
        .expect("followup invoke should succeed");

    assert_eq!(
        String::from_utf8(followup.body).expect("utf8"),
        "abortCount=1"
    );
}

#[tokio::test]
#[serial]
async fn duplicate_user_request_ids_do_not_collide() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 4,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy("io".to_string(), io_wait_worker())
        .await
        .expect("deploy should succeed");

    let mut tasks = Vec::new();
    for _ in 0..8 {
        let svc = service.clone();
        tasks.push(tokio::spawn(async move {
            let mut req = test_invocation();
            req.request_id = "same-user-request-id".to_string();
            svc.invoke("io".to_string(), req).await
        }));
    }

    for task in tasks {
        let output = task.await.expect("join").expect("invoke should succeed");
        assert_eq!(String::from_utf8(output.body).expect("utf8"), "ok");
    }
}

#[tokio::test]
#[serial]
async fn forged_and_invalid_completion_payloads_are_ignored() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 2,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy("malicious".to_string(), malicious_completion_worker())
        .await
        .expect("deploy should succeed");

    let first = service
        .invoke("malicious".to_string(), test_invocation())
        .await
        .expect("first invoke should succeed");
    assert_eq!(String::from_utf8(first.body).expect("utf8"), "1");

    let second = service
        .invoke("malicious".to_string(), test_invocation())
        .await
        .expect("second invoke should succeed");
    assert_eq!(String::from_utf8(second.body).expect("utf8"), "2");
}

#[tokio::test]
#[serial]
async fn invoke_stream_delivers_chunked_response_body() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 4,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy(
            "streaming".to_string(),
            r#"
export default {
  async fetch() {
    return new Response(new ReadableStream({
      start(controller) {
        controller.enqueue("hel");
        controller.enqueue("lo");
        controller.close();
      }
    }), { status: 201, headers: [["x-mode", "stream"]] });
  },
};
"#
            .to_string(),
        )
        .await
        .expect("deploy should succeed");

    let mut output = service
        .invoke_stream("streaming".to_string(), test_invocation())
        .await
        .expect("invoke stream should succeed");
    assert_eq!(output.status, 201);
    assert!(
        output
            .headers
            .iter()
            .any(|(name, value)| name == "x-mode" && value == "stream")
    );

    let mut body = Vec::new();
    while let Some(chunk) = output.body.recv().await {
        body.extend_from_slice(&chunk.expect("chunk should be ok"));
    }
    assert_eq!(String::from_utf8(body).expect("utf8"), "hello");
}

#[tokio::test]
#[serial]
async fn invoke_stream_delivers_binary_response_chunks() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 4,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy(
            "binary-streaming".to_string(),
            r#"
export default {
  async fetch() {
    return new Response(new ReadableStream({
      start(controller) {
        controller.enqueue(new Uint8Array([0, 255, 1]));
        controller.enqueue(new Uint8Array([2, 128, 3]));
        controller.close();
      }
    }), { status: 206, headers: [["content-type", "application/octet-stream"]] });
  },
};
"#
            .to_string(),
        )
        .await
        .expect("deploy should succeed");

    let mut output = service
        .invoke_stream("binary-streaming".to_string(), test_invocation())
        .await
        .expect("invoke stream should succeed");
    assert_eq!(output.status, 206);

    let mut body = Vec::new();
    while let Some(chunk) = output.body.recv().await {
        body.extend_from_slice(&chunk.expect("chunk should be ok"));
    }
    assert_eq!(body, vec![0, 255, 1, 2, 128, 3]);
}

#[tokio::test]
#[serial]
async fn invoke_stream_body_drop_cancels_running_request() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 1,
        max_queue_wait: Duration::from_secs(5),
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(20),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy(
            "drop-stream".to_string(),
            r#"
export default {
  async fetch(request) {
    const path = new URL(request.url).pathname;
    if (path === "/stream") {
      return new Response(new ReadableStream({
        start(controller) {
          controller.enqueue("first");
        }
      }));
    }
    return new Response("ok");
  },
};
"#
            .to_string(),
        )
        .await
        .expect("deploy should succeed");

    let mut output = service
        .invoke_stream(
            "drop-stream".to_string(),
            test_invocation_with_path("/stream", "drop-stream-start"),
        )
        .await
        .expect("stream should start");
    let first = timeout(Duration::from_secs(2), output.body.recv())
        .await
        .expect("first chunk should arrive")
        .expect("body should still be open")
        .expect("first chunk should be ok");
    assert_eq!(String::from_utf8(first.to_vec()).expect("utf8"), "first");

    drop(output);

    let followup = timeout(
        Duration::from_secs(3),
        service.invoke(
            "drop-stream".to_string(),
            test_invocation_with_path("/ok", "drop-stream-followup"),
        ),
    )
    .await
    .expect("body drop should cancel the stream and free the isolate")
    .expect("followup invoke should succeed");
    assert_eq!(String::from_utf8(followup.body).expect("utf8"), "ok");
}

#[tokio::test]
#[serial]
async fn invoke_stream_propagates_response_body_error() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 1,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy(
            "error-stream".to_string(),
            r#"
export default {
  async fetch() {
    let count = 0;
    return new Response(new ReadableStream({
      pull(controller) {
        if (count === 0) {
          count++;
          controller.enqueue("first");
          return;
        }
        controller.error(new Error("stream failed"));
      }
    }));
  },
};
"#
            .to_string(),
        )
        .await
        .expect("deploy should succeed");

    let mut output = service
        .invoke_stream("error-stream".to_string(), test_invocation())
        .await
        .expect("stream should start");
    let first = timeout(Duration::from_secs(2), output.body.recv())
        .await
        .expect("first chunk should arrive")
        .expect("body should still be open")
        .expect("first chunk should be ok");
    assert_eq!(String::from_utf8(first.to_vec()).expect("utf8"), "first");

    let error = timeout(Duration::from_secs(2), output.body.recv())
        .await
        .expect("stream error should arrive")
        .expect("body should deliver an error")
        .expect_err("second body item should be an error");
    assert!(
        error.to_string().contains("stream failed"),
        "stream error should include original message: {error}"
    );
}

#[tokio::test]
#[serial]
async fn invoke_with_request_body_stream_delivers_chunks_to_worker() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 1,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy(
            "streaming-body".to_string(),
            streaming_request_body_worker(),
        )
        .await
        .expect("deploy should succeed");

    let (tx, rx) = mpsc::channel(4);
    let mut request = test_invocation();
    request.method = "POST".to_string();
    request.request_id = "streaming-body-request".to_string();

    let invoke_task = {
        let service = service.clone();
        tokio::spawn(async move {
            service
                .invoke_with_request_body("streaming-body".to_string(), request, Some(rx))
                .await
        })
    };

    tx.send(Ok(Bytes::from_static(b"hel")))
        .await
        .expect("first body chunk should send");
    tx.send(Ok(Bytes::from_static(b"lo")))
        .await
        .expect("second body chunk should send");
    drop(tx);

    let output = invoke_task
        .await
        .expect("join")
        .expect("invoke should succeed");
    assert_eq!(String::from_utf8(output.body).expect("utf8"), "hello");
}

#[tokio::test]
#[serial]
async fn async_context_store_survives_promise_boundaries_and_nested_runs() {
    let service = test_service(RuntimeConfig::default()).await;

    service
        .deploy_with_config(
            "async-context".to_string(),
            async_context_worker(),
            DeployConfig {
                public: false,
                cache: Default::default(),
                internal: DeployInternalConfig { trace: None },
                bindings: Vec::new(),
            },
        )
        .await
        .expect("deploy should succeed");

    let promise = service
        .invoke(
            "async-context".to_string(),
            test_invocation_with_path("/promise", "async-context-promise"),
        )
        .await
        .expect("promise request should succeed");
    assert_eq!(String::from_utf8(promise.body).expect("utf8"), "outer");

    let nested = service
        .invoke(
            "async-context".to_string(),
            test_invocation_with_path("/nested", "async-context-nested"),
        )
        .await
        .expect("nested request should succeed");
    assert_eq!(
        String::from_utf8(nested.body).expect("utf8"),
        "outer:inner:outer"
    );

    let restore = service
        .invoke(
            "async-context".to_string(),
            test_invocation_with_path("/restore", "async-context-restore"),
        )
        .await
        .expect("restore request should succeed");
    assert_eq!(
        String::from_utf8(restore.body).expect("utf8"),
        "missing:missing"
    );
}

#[tokio::test]
#[serial]
async fn cache_default_reuses_response() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 1,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy("cache".to_string(), cache_worker("default", "cache"))
        .await
        .expect("deploy should succeed");

    let one = service
        .invoke(
            "cache".to_string(),
            test_invocation_with_path("/", "cache-one"),
        )
        .await
        .expect("first invoke should succeed");
    let two = service
        .invoke(
            "cache".to_string(),
            test_invocation_with_path("/", "cache-two"),
        )
        .await
        .expect("second invoke should succeed");

    assert_eq!(String::from_utf8(one.body).expect("utf8"), "cache:1");
    assert_eq!(String::from_utf8(two.body).expect("utf8"), "cache:1");
}

#[tokio::test]
#[serial]
async fn cache_names_are_isolated_by_worker() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 1,
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy("cache-owner-a".to_string(), cache_worker("shared", "A"))
        .await
        .expect("deploy a should succeed");
    service
        .deploy("cache-owner-b".to_string(), cache_worker("shared", "B"))
        .await
        .expect("deploy b should succeed");

    let a = service
        .invoke(
            "cache-owner-a".to_string(),
            test_invocation_with_path("/", "cache-owner-a-1"),
        )
        .await
        .expect("worker a should succeed");
    let b = service
        .invoke(
            "cache-owner-b".to_string(),
            test_invocation_with_path("/", "cache-owner-b-1"),
        )
        .await
        .expect("worker b should succeed");

    assert_eq!(String::from_utf8(a.body).expect("utf8"), "A:1");
    assert_eq!(String::from_utf8(b.body).expect("utf8"), "B:1");
}

#[tokio::test]
#[serial]
async fn named_caches_share_global_capacity_budget() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 1,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        cache_max_entries: 1,
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy("worker-a".to_string(), cache_worker("cache-a", "A"))
        .await
        .expect("deploy a should succeed");
    service
        .deploy("worker-b".to_string(), cache_worker("cache-b", "B"))
        .await
        .expect("deploy b should succeed");

    let a1 = service
        .invoke(
            "worker-a".to_string(),
            test_invocation_with_path("/", "a-1"),
        )
        .await
        .expect("a1 should succeed");
    let b1 = service
        .invoke(
            "worker-b".to_string(),
            test_invocation_with_path("/", "b-1"),
        )
        .await
        .expect("b1 should succeed");
    let a2 = service
        .invoke(
            "worker-a".to_string(),
            test_invocation_with_path("/", "a-2"),
        )
        .await
        .expect("a2 should succeed");

    assert_eq!(String::from_utf8(a1.body).expect("utf8"), "A:1");
    assert_eq!(String::from_utf8(b1.body).expect("utf8"), "B:1");
    assert_eq!(String::from_utf8(a2.body).expect("utf8"), "A:2");
}

#[tokio::test]
#[serial]
async fn internal_trace_includes_markers_and_targets_configured_worker() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 4,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy("trace-sink".to_string(), trace_sink_worker())
        .await
        .expect("deploy trace sink should succeed");
    service
        .deploy_with_config(
            "traced-worker".to_string(),
            r#"
                export default {
                  async fetch() {
                    return new Response("ok");
                  },
                };
                "#
            .to_string(),
            DeployConfig {
                internal: DeployInternalConfig {
                    trace: Some(DeployTraceDestination {
                        worker: "trace-sink".to_string(),
                        path: "/ingest".to_string(),
                    }),
                },
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy traced worker should succeed");

    let mut request = test_invocation_with_path("/", "trace-request");
    request
        .headers
        .push(("x-test".to_string(), "value".to_string()));
    service
        .invoke("traced-worker".to_string(), request)
        .await
        .expect("traced invoke should succeed");

    sleep(Duration::from_millis(100)).await;
}

#[test]
fn internal_trace_headers_include_markers() {
    let mut headers = vec![("x-other".to_string(), "value".to_string())];
    super::append_internal_trace_headers(&mut headers, "traced-worker", 42);

    let internal = headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("x-dd-internal"))
        .expect("x-dd-internal header should be present")
        .1
        .as_str();
    let reason = headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("x-dd-internal-reason"))
        .expect("x-dd-internal-reason header should be present")
        .1
        .as_str();
    let source_worker = headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("x-dd-trace-source-worker"))
        .expect("x-dd-trace-source-worker header should be present")
        .1
        .as_str();
    let source_generation = headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("x-dd-trace-source-generation"))
        .expect("x-dd-trace-source-generation header should be present")
        .1
        .as_str();

    assert_eq!(internal, "1");
    assert_eq!(reason, "trace");
    assert_eq!(source_worker, "traced-worker");
    assert_eq!(source_generation, "42");
}

#[tokio::test]
#[serial]
async fn internal_trace_invocations_do_not_recurse() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 4,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy_with_config(
            "loop-worker".to_string(),
            loop_trace_worker(),
            DeployConfig {
                internal: DeployInternalConfig {
                    trace: Some(DeployTraceDestination {
                        worker: "loop-worker".to_string(),
                        path: "/trace".to_string(),
                    }),
                },
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy loop worker should succeed");

    service
        .invoke(
            "loop-worker".to_string(),
            test_invocation_with_path("/", "loop-user"),
        )
        .await
        .expect("loop worker invoke should succeed");

    sleep(Duration::from_millis(100)).await;
    let state = timeout(Duration::from_secs(2), async {
        loop {
            let state_output = service
                .invoke(
                    "loop-worker".to_string(),
                    test_invocation_with_path("/state", "loop-state"),
                )
                .await
                .expect("loop worker state invoke should succeed");
            let state: LoopTraceState = crate::json::from_string(
                String::from_utf8(state_output.body).expect("loop state body should be utf8"),
            )
            .expect("loop state should parse as json");
            if state.trace_calls >= 2 {
                return state;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("loop state query should complete");

    assert_eq!(state.trace_calls, 2);
    assert!(state.total_calls >= 2);
}

#[tokio::test]
#[serial]
async fn concurrent_runtime_services_isolate_dynamic_module_graphs_and_refcounts() {
    let first = test_service(RuntimeConfig::default()).await;
    let second = test_service(RuntimeConfig::default()).await;
    let modules = HashMap::from([(
        "worker.js".to_string(),
        "export default { fetch() { return new Response('isolated'); } };".to_string(),
    )]);

    let (first_graph_id, first_entrypoint) = first
        ._dynamic_modules
        .register_dynamic_module_graph("worker.js", modules.clone())
        .expect("first service graph should register");
    assert!(
        second
            ._dynamic_modules
            .source(&first_graph_id, &first_entrypoint)
            .is_none(),
        "a graph registered in one live service must not be visible to another"
    );

    let (second_graph_id, second_entrypoint) = second
        ._dynamic_modules
        .register_dynamic_module_graph("worker.js", modules)
        .expect("second service graph should register");
    assert_eq!(
        first_graph_id, second_graph_id,
        "graph ids remain content-addressed"
    );
    assert_eq!(first._dynamic_modules.ref_count(&first_graph_id), Some(1));
    assert_eq!(second._dynamic_modules.ref_count(&second_graph_id), Some(1));

    first._dynamic_modules.release(&first_graph_id);
    assert!(
        first
            ._dynamic_modules
            .source(&first_graph_id, &first_entrypoint)
            .is_none()
    );
    assert!(
        second
            ._dynamic_modules
            .source(&second_graph_id, &second_entrypoint)
            .is_some(),
        "releasing one service's graph must not change another service's refcount"
    );

    first
        .shutdown()
        .await
        .expect("first service should shut down");
    second
        .shutdown()
        .await
        .expect("second service should shut down");
}
#[test]
fn asset_catalog_copy_on_write_preserves_concurrent_updates_and_redeploy_snapshots() {
    let catalog = super::AssetCatalog::default();
    let assets = Arc::new(crate::static_assets::AssetBundle::default());

    std::thread::scope(|scope| {
        for generation in 1..=32 {
            let catalog = catalog.clone();
            let assets = Arc::clone(&assets);
            scope.spawn(move || {
                let worker_name = format!("worker-{generation}");
                catalog.insert(
                    worker_name.clone(),
                    super::AssetCatalogEntry {
                        worker_name,
                        generation,
                        assets,
                        public: true,
                        cache_enabled: false,
                    },
                );
            });
        }
    });

    for generation in 1..=32 {
        let worker_name = format!("worker-{generation}");
        let entry = catalog
            .get(&worker_name)
            .expect("concurrent catalog update should not be lost");
        assert_eq!(entry.worker_name, worker_name);
        assert_eq!(entry.generation, generation);
    }

    let original = catalog.get("worker-1").expect("original snapshot");
    catalog.insert(
        "worker-1".to_string(),
        super::AssetCatalogEntry {
            worker_name: "worker-1".to_string(),
            generation: 100,
            assets,
            public: false,
            cache_enabled: true,
        },
    );
    let redeployed = catalog.get("worker-1").expect("redeployed snapshot");
    assert_eq!(original.generation, 1);
    assert_eq!(redeployed.generation, 100);
    assert!(original.public);
    assert!(!redeployed.public);
}

#[test]
fn dynamic_worker_config_builds_placeholders() {
    let mut env = HashMap::new();
    env.insert("OPENAI_API_KEY".to_string(), "sk-test-123".to_string());
    let config = super::build_dynamic_worker_config(
        env,
        Vec::new(),
        crate::ops::DynamicWorkerPolicy {
            egress_allow_hosts: vec!["api.openai.com".to_string()],
            ..Default::default()
        },
        Vec::new(),
    )
    .expect("dynamic config should build");

    assert_eq!(config.dynamic_env.len(), 1);
    assert_eq!(config.secret_replacements.len(), 1);
    assert_eq!(
        config.egress_allow_hosts,
        vec!["api.openai.com".to_string()]
    );

    let placeholder = config
        .env_placeholders
        .get("OPENAI_API_KEY")
        .expect("placeholder should be present");
    assert!(placeholder.starts_with("__DD_SECRET_"));
}

#[test]
fn dynamic_worker_config_rejects_invalid_host() {
    let config = super::build_dynamic_worker_config(
        HashMap::new(),
        Vec::new(),
        crate::ops::DynamicWorkerPolicy {
            egress_allow_hosts: vec!["http://bad-host".to_string()],
            ..Default::default()
        },
        Vec::new(),
    );
    assert!(config.is_err());
}

#[test]
fn dynamic_worker_config_requires_state_policy_for_bindings() {
    let config = super::build_dynamic_worker_config(
        HashMap::new(),
        vec![DeployBinding::Kv {
            binding: "AUTH_DB".to_string(),
        }],
        crate::ops::DynamicWorkerPolicy::default(),
        Vec::new(),
    );
    assert!(config.is_err());
}

#[test]
fn dynamic_worker_config_accepts_state_bindings() {
    let config = super::build_dynamic_worker_config(
        HashMap::new(),
        vec![
            DeployBinding::Kv {
                binding: "AUTH_DB".to_string(),
            },
            DeployBinding::Memory {
                binding: "AUTH_STATE".to_string(),
            },
        ],
        crate::ops::DynamicWorkerPolicy {
            allow_state_bindings: true,
            ..Default::default()
        },
        Vec::new(),
    )
    .expect("dynamic config should accept state bindings when the policy allows them");
    assert_eq!(config.bindings.kv, vec!["AUTH_DB".to_string()]);
    assert_eq!(config.bindings.memory, vec!["AUTH_STATE".to_string()]);
}

#[test]
fn dynamic_worker_config_accepts_host_port_and_wildcard_rules() {
    let config = super::build_dynamic_worker_config(
        HashMap::new(),
        Vec::new(),
        crate::ops::DynamicWorkerPolicy {
            egress_allow_hosts: vec![
                "api.example.com:8443".to_string(),
                "*.example.com".to_string(),
                "*.example.com:9443".to_string(),
            ],
            ..Default::default()
        },
        Vec::new(),
    )
    .expect("dynamic config should accept host+port rules");

    assert_eq!(
        config.egress_allow_hosts,
        vec![
            "api.example.com:8443".to_string(),
            "*.example.com".to_string(),
            "*.example.com:9443".to_string(),
        ]
    );
}

#[test]
fn extract_bindings_collects_dynamic_bindings() {
    let bindings = super::extract_bindings(&DeployConfig {
        bindings: vec![
            DeployBinding::Kv {
                binding: "MY_KV".to_string(),
            },
            DeployBinding::Dynamic {
                binding: "SANDBOX".to_string(),
            },
            DeployBinding::Service {
                binding: "AUTH".to_string(),
                service: "auth-worker".to_string(),
            },
        ],
        ..DeployConfig::default()
    })
    .expect("bindings should parse");

    assert_eq!(bindings.kv, vec!["MY_KV".to_string()]);
    assert_eq!(bindings.dynamic, vec!["SANDBOX".to_string()]);
    assert_eq!(bindings.service.len(), 1);
    assert_eq!(bindings.service[0].binding, "AUTH");
    assert_eq!(bindings.service[0].service, "auth-worker");
}

#[test]
fn extract_bindings_rejects_duplicate_dynamic_name() {
    let result = super::extract_bindings(&DeployConfig {
        bindings: vec![
            DeployBinding::Dynamic {
                binding: "SANDBOX".to_string(),
            },
            DeployBinding::Dynamic {
                binding: "SANDBOX".to_string(),
            },
        ],
        ..DeployConfig::default()
    });
    assert!(result.is_err());
}
