use super::{
    BlobStoreConfig, RuntimeConfig, RuntimeService, RuntimeServiceConfig, RuntimeStorageConfig,
};
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

    let worker_runtime_source =
        include_str!(concat!(env!("OUT_DIR"), "/execute_worker.generated.js"));
    assert!(worker_runtime_source.contains("__dd_install_host_fetch"));
    assert!(worker_runtime_source.contains("value: function installHostFetch()"));
    assert!(worker_runtime_source
        .contains("const installHostFetch = globalThis.__dd_install_host_fetch"));
    assert!(!worker_runtime_source.contains("const installHostFetch = () =>"));
    assert!(worker_runtime_source.contains("__dd_get_cache_bypass_stale"));
    assert!(worker_runtime_source.contains("cacheBypassStale,"));
    assert!(!worker_runtime_source.contains("globalThis.__dd_get_runtime_request_id ="));
    assert!(!worker_runtime_source.contains("globalThis.__dd_get_runtime_request_context_handle ="));
    assert!(!worker_runtime_source.contains("globalThis.__dd_sync_time_boundary ="));
    assert!(!worker_runtime_source.contains("globalThis.__dd_cache_bypass_stale ="));
    let bootstrap_source = include_str!("../../js/bootstrap.js");
    assert!(bootstrap_source.contains("function activeCacheBypassStale()"));
    assert!(bootstrap_source.contains("activeCacheBypassStale(),"));
    assert!(!bootstrap_source.contains("__dd_cache_bypass_stale"));
    assert_eq!(
        worker_runtime_source
            .matches("globalThis.fetch = scopedFetch")
            .count(),
        1
    );
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
    assert!(asset
        .headers
        .iter()
        .any(|(name, value)| name.eq_ignore_ascii_case("x-host") && value == "foo"));

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
    assert!(head
        .headers
        .iter()
        .any(|(name, value)| name.eq_ignore_ascii_case("x-splat") && value == "b.css"));

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
        temporary_worker_ttl: Duration::from_secs(60),
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

    let worker_store = root.join("workers");
    let mut entries = tokio::fs::read_dir(&worker_store)
        .await
        .expect("worker store should exist");
    let entry = entries
        .next_entry()
        .await
        .expect("read worker store")
        .expect("stored worker deployment should exist");
    let path = entry.path();
    let body = tokio::fs::read(&path)
        .await
        .expect("read stored deployment");
    let mut stored: Value = serde_json::from_slice(&body).expect("stored deployment json");
    stored["expires_at_ms"] = Value::from(0);
    tokio::fs::write(
        &path,
        serde_json::to_vec(&stored).expect("updated stored deployment json"),
    )
    .await
    .expect("write expired stored deployment");

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
    assert!(service
        .resolve_asset("preview-live", "GET", None, "/a.js", &[],)
        .expect("asset lookup before expiry should succeed")
        .is_some());

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
    assert!(service
        .resolve_asset("preview-live", "GET", None, "/a.js", &[],)
        .expect("asset lookup after expiry should succeed")
        .is_none());
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
    assert!(error
        .to_string()
        .contains("must be `Name: value` or `! Name`"));
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
            vec![address.to_string()],
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
    assert!(output
        .headers
        .iter()
        .any(|(name, value)| name == "x-mode" && value == "stream"));

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

#[test]
fn dynamic_host_rpc_fake_wake_path_is_removed_from_runtime_sources() {
    let worker_runtime_source =
        include_str!(concat!(env!("OUT_DIR"), "/execute_worker.generated.js"));

    assert!(!worker_runtime_source.contains("__dd_drain_dynamic_host_rpc_queue"));
    assert!(!worker_runtime_source.contains("__dd_run_dynamic_host_rpc_tasks"));
    assert!(!worker_runtime_source.contains("__dd_dynamic_colocated_runtime_cache"));
    assert!(!worker_runtime_source.contains("tryColocatedDynamicFetch"));
    assert!(!worker_runtime_source.contains("__dd_force_remote_fetch"));
    assert!(!worker_runtime_source.contains("runDynamicHostRpcTask"));
    assert!(!worker_runtime_source.contains("host-rpc-task"));
    assert!(!worker_runtime_source.contains("op_dynamic_host_rpc_task_complete"));
    assert!(!worker_runtime_source.contains("op_dynamic_take_reply"));
    assert!(!worker_runtime_source.contains("op_dynamic_take_pushed_replies"));
    assert!(!worker_runtime_source.contains("op_dynamic_take_host_rpc_tasks"));
    assert!(worker_runtime_source.contains("__dd_drain_dynamic_control_queue"));
    assert!(worker_runtime_source.contains("__dd_source_unit:execute_worker/core.js"));
    assert!(worker_runtime_source.contains("__dd_source_unit:execute_worker/fetch_cache.js"));
    assert!(worker_runtime_source.contains("__dd_source_unit:execute_worker/sockets_transport.js"));
    assert!(worker_runtime_source.contains("__dd_source_unit:execute_worker/memory.js"));
    assert!(worker_runtime_source.contains("__dd_source_unit:execute_worker/dynamic.js"));
}

#[test]
fn dynamic_module_workers_do_not_generate_js_import_wrappers() {
    let worker_runtime_source =
        include_str!(concat!(env!("OUT_DIR"), "/execute_worker.generated.js"));
    let dynamic_modules_source = include_str!("../dynamic_modules.rs");
    let dynamic_ops_source = include_str!("../ops/dynamic.rs");

    assert!(!worker_runtime_source.contains("buildSourceFromModules"));
    assert!(!worker_runtime_source.contains("normalizeModulePath"));
    assert!(!worker_runtime_source.contains("dynamicSourceCacheKey"));
    assert!(!worker_runtime_source.contains("dynamic worker missing entrypoint module"));
    assert!(!worker_runtime_source.contains("__dd_dynamic_graph"));
    assert!(!worker_runtime_source.contains("await import(\"dd-dynamic://graph/\""));
    assert!(worker_runtime_source.contains("op_dynamic_module_graph_register"));
    assert!(worker_runtime_source.contains("op_dynamic_module_graph_release"));
    assert!(worker_runtime_source.contains("onRemove: (source) =>"));
    assert!(worker_runtime_source.contains("result.entrypoint"));
    assert!(worker_runtime_source.contains("module_graph_id"));
    assert!(worker_runtime_source.contains("module_entrypoint"));
    assert!(dynamic_ops_source.contains("#[string] entrypoint: String"));
    assert!(dynamic_modules_source.contains("register_dynamic_module_graph("));
    assert!(dynamic_modules_source.contains("retain_dynamic_module_graph("));
    assert!(dynamic_modules_source.contains("release_dynamic_module_graph("));
    assert!(dynamic_modules_source.contains("ref_count: usize"));
    assert!(dynamic_modules_source.contains("normalize_dynamic_module_path(entrypoint)"));
    assert!(dynamic_modules_source.contains("dynamic module graph missing entrypoint module"));
    assert!(dynamic_modules_source.contains("duplicate normalized path"));
    assert!(dynamic_modules_source.contains("dynamic module URL imports are unsupported"));
}

#[test]
fn worker_invocation_uses_typed_request_handle_runtime_entrypoint() {
    let engine_source = include_str!("../engine.rs");
    let ops_source = include_str!("../ops.rs");
    let worker_runtime_source =
        include_str!(concat!(env!("OUT_DIR"), "/execute_worker.generated.js"));
    let api_invocation_source = include_str!("../../../api/src/handlers/invocation.rs");
    let request_types_source = include_str!("../ops/request_types.rs");
    let request_ops_source = include_str!("../ops/request.rs");
    let storage_http_source = include_str!("../ops/storage_http.rs");

    assert!(!engine_source.contains("<dd:invoke>"));
    assert!(!engine_source.contains("execute_script(\"<dd:invoke>\""));
    assert!(engine_source.contains("__dd_execute_worker_handle"));
    assert!(engine_source.contains("__dd_abort_worker_request_handle"));
    assert!(engine_source.contains("pub fn abort_worker_request_handle("));
    assert!(engine_source.contains("call_cached_u32_function("));
    assert!(!engine_source.contains("call_cached_string_function"));
    assert!(!engine_source.contains("__dd_abort_worker_request\","));
    assert!(worker_runtime_source.contains("__dd_execute_worker_handle = (requestHandle)"));
    assert!(
        worker_runtime_source.contains("__dd_abort_worker_request_handle = (requestContextHandle)")
    );
    assert!(worker_runtime_source.contains("__dd_inflight_requests_by_context_handle"));
    assert!(!worker_runtime_source.contains("__dd_abort_worker_request = (requestId)"));
    assert!(worker_runtime_source.contains("op_http_take_prepared_headers"));
    assert!(worker_runtime_source.contains("op_http_take_prepared_body(requestBodyHandle)"));
    assert!(worker_runtime_source.contains("op_http_take_prepared_body\", bodyHandle"));
    assert!(engine_source.contains("request_headers_handle"));
    assert!(engine_source.contains("request_body_handle"));
    assert!(engine_source.contains("HttpPreparedHeaders"));
    assert!(engine_source.contains("HttpPreparedBodies"));
    assert!(api_invocation_source.contains("tx.send(Ok(chunk))"));
    assert!(!api_invocation_source.contains("tx.send(Ok(chunk.to_vec()))"));
    assert!(request_types_source
        .contains("pub type RequestBodyChunk = std::result::Result<Bytes, String>"));
    assert!(request_types_source.contains("bodies: HashMap<u32, Bytes>"));
    assert!(request_ops_source.contains(".insert(bytes)"));
    assert!(storage_http_source.contains("#[buffer] body: JsBuffer"));
    assert!(storage_http_source.contains("Bytes::copy_from_slice(body.as_ref())"));
    assert!(storage_http_source.contains("pub(crate) struct KvGetValueResult"));
    assert!(storage_http_source.contains("pub(crate) struct KvListItem"));
    assert!(storage_http_source.contains("value_handle: u32"));
    assert!(storage_http_source.contains(".insert(Bytes::from(value.value))"));
    assert!(storage_http_source.contains("to_list_item(entry, bodies)"));
    assert!(worker_runtime_source.contains("result.value_handle"));
    assert!(worker_runtime_source.contains("entry?.value_handle"));
    assert!(!storage_http_source.contains("#[buffer(copy)] body: Vec<u8>"));
    assert!(!storage_http_source.contains("value: Vec<u8>,\n    encoding: String,\n}\n\n#[derive(Debug, Serialize)]\npub(crate) struct KvGetValueResult"));
    assert!(!storage_http_source.contains(
        "pub(crate) struct KvGetValueResult {\n    ok: bool,\n    found: bool,\n    value: Vec<u8>"
    ));
    assert!(!worker_runtime_source.contains("new Uint8Array(Array.isArray(rawValue)"));
    assert!(!worker_runtime_source.contains("entry?.value, \"kv list\""));
    assert!(request_types_source.contains("take_descriptor"));
    assert!(!ops_source.contains("std::mem::take(&mut payload.request.headers)"));
    assert!(!worker_runtime_source.contains("op_take_request_invocation_body"));
    assert!(!worker_runtime_source.contains("op_take_request_body_chunk"));
    assert!(!worker_runtime_source.contains("descriptor.headers"));
    assert!(!worker_runtime_source.contains("payload?.request ??"));
    assert!(!worker_runtime_source.contains("request: {\n      method: String(descriptor.method"));
    assert!(worker_runtime_source.contains("const input = {"));
    assert!(worker_runtime_source.contains("input_request_id: String(descriptor.input_request_id"));
    assert!(request_types_source.contains("request_headers_handle"));
    assert!(request_types_source.contains("request_body_handle"));
    assert!(request_types_source.contains("input_request_id: String"));
    assert!(!request_types_source.contains("request: WorkerInvocation"));
    assert!(!request_types_source.contains("payload.request."));
    assert!(engine_source.contains("method: mem::take(&mut request.method)"));
    assert!(engine_source.contains("url: mem::take(&mut request.url)"));
    assert!(engine_source.contains("input_request_id: mem::take(&mut request.request_id)"));
    assert!(request_types_source.contains("body_handle: u32"));
    assert!(!request_types_source.contains("pub(crate) headers: Vec<(String, String)>,"));
    assert!(!request_types_source.contains("fn take_body("));
    assert!(!request_types_source.contains("RequestBodyChunkHandles"));
}

#[test]
fn memory_rpc_nested_request_ids_are_host_generated() {
    let memory_ops_source = include_str!("../ops/memory.rs");
    let dynamic_ops_source = include_str!("../ops/dynamic.rs");
    let dynamic_types_source = include_str!("../ops/dynamic_types.rs");
    let worker_runtime_source =
        include_str!(concat!(env!("OUT_DIR"), "/execute_worker.generated.js"));
    let pending_reply_start = dynamic_types_source
        .find("pub struct DynamicPendingReplyResult")
        .expect("dynamic pending reply result should exist");
    let pending_reply_end = dynamic_types_source[pending_reply_start..]
        .find("pub struct DynamicFetchReplyResult")
        .map(|offset| pending_reply_start + offset)
        .expect("dynamic fetch reply result should follow pending reply result");
    let pending_reply_source = &dynamic_types_source[pending_reply_start..pending_reply_end];

    assert!(memory_ops_source.contains("MEMORY_INVOKE_REQUEST_SEQ"));
    assert!(memory_ops_source.contains(":memory-run:"));
    assert!(worker_runtime_source.contains("op_memory_invoke_method"));
    assert!(memory_ops_source.contains("#[buffer] args: JsBuffer"));
    assert!(!memory_ops_source.contains("#[buffer(copy)] args: Vec<u8>"));
    assert!(!worker_runtime_source.contains(":memory-run:"));
    assert!(dynamic_ops_source.contains("DYNAMIC_FETCH_REQUEST_SEQ"));
    assert!(dynamic_ops_source.contains(":dynamic:"));
    assert!(worker_runtime_source.contains("op_dynamic_worker_fetch_start"));
    assert_eq!(
        dynamic_ops_source
            .matches("#[buffer] args: JsBuffer")
            .count(),
        2
    );
    assert!(!dynamic_ops_source.contains("#[buffer(copy)] args: Vec<u8>"));
    assert!(!worker_runtime_source.contains(":dynamic:"));
    assert!(dynamic_ops_source.contains("headers_handle: u32"));
    assert!(dynamic_ops_source.contains("body_handle: u32"));
    assert!(!dynamic_ops_source.contains("#[serde] headers: Vec<(String, String)>"));
    assert!(!dynamic_ops_source.contains("#[buffer(copy)] body: Vec<u8>"));
    assert!(!pending_reply_source.contains("status: u16"));
    assert!(!pending_reply_source.contains("Vec<(String, String)>"));
    assert!(!pending_reply_source.contains("body: Vec<u8>"));
    assert!(pending_reply_source.contains("value_handle: u32"));
    assert!(pending_reply_source.contains("pending_value: Option<Vec<u8>>"));
    assert!(!pending_reply_source.contains("value: Vec<u8>"));
    assert!(!dynamic_types_source.contains("pub headers: Vec<(String, String)>"));
    assert!(!dynamic_types_source.contains("pub body: Vec<u8>"));
    assert!(dynamic_types_source.contains("pub pending_output: Option<WorkerOutput>"));
    assert!(dynamic_ops_source.contains("reply.headers_handle"));
    assert!(dynamic_ops_source.contains("reply.body_handle"));
    assert!(dynamic_ops_source.contains("reply.value_handle"));
    assert!(dynamic_types_source.contains("pub headers_handle: u32"));
    assert!(dynamic_types_source.contains("pub body_handle: u32"));
    assert!(dynamic_ops_source.contains("pending_output.take()"));
    assert!(dynamic_ops_source.contains("pending_value.take()"));
    assert!(worker_runtime_source.contains("result.headers_handle"));
    assert!(worker_runtime_source.contains("result.body_handle"));
    assert!(worker_runtime_source.contains("result.value_handle"));
    assert!(worker_runtime_source.contains("op_http_take_prepared_body\", valueHandle"));
    assert!(worker_runtime_source.contains("const discardDynamicReplyHandles = (payload) =>"));
    assert!(worker_runtime_source.contains("payload?.value_handle"));
    assert!(worker_runtime_source.contains("discardDynamicReplyHandles(payload)"));
    assert!(worker_runtime_source.contains("discardDynamicReplyHandles(readyPayload)"));
    assert!(!worker_runtime_source.contains("toArrayBytes(result.body)"));
    assert!(!worker_runtime_source.contains("toArrayBytes(result.value)"));
    assert!(!worker_runtime_source.contains("Array.isArray(result.headers) ? result.headers : []"));
}

#[test]
fn response_streaming_uses_binary_chunk_ops_not_json_number_arrays() {
    let worker_runtime_source =
        include_str!(concat!(env!("OUT_DIR"), "/execute_worker.generated.js"));
    let storage_http_source = include_str!("../ops/storage_http.rs");
    let ops_source = include_str!("../ops.rs");
    let facade_source = include_str!("facade.rs");
    let lifecycle_source = include_str!("lifecycle.rs");
    let complete_start = lifecycle_source
        .find("pub(crate) async fn complete_stream_registration")
        .expect("complete_stream_registration should be async");
    let complete_end = lifecycle_source[complete_start..]
        .find("pub(crate) fn fail_all_streams_for_worker")
        .map(|offset| complete_start + offset)
        .expect("fail_all_streams_for_worker should follow complete_stream_registration");
    let complete_source = &lifecycle_source[complete_start..complete_end];

    assert!(worker_runtime_source.contains("op_emit_response_chunk"));
    assert!(storage_http_source.contains("#[buffer] chunk: JsBuffer"));
    assert!(storage_http_source.contains("Bytes::copy_from_slice(chunk.as_ref())"));
    assert!(ops_source.contains("chunk: Bytes"));
    assert!(facade_source.contains("mpsc::Receiver<Result<Bytes>>"));
    assert!(complete_source.contains(".body_sender.send("));
    assert!(!complete_source.contains(".body_sender.try_send("));
    assert!(!worker_runtime_source.contains("Array.from(bytes)"));
    assert!(!worker_runtime_source.contains("bodyBytes.push"));
    assert!(!worker_runtime_source.contains("JSON.stringify({ request_id"));
}

#[test]
fn memory_realtime_ops_use_borrowed_buffer_inputs() {
    let memory_ops_source = include_str!("../ops/memory.rs");
    let memory_types_source = include_str!("../ops/memory_types.rs");
    let ops_source = include_str!("../ops.rs");
    let control_source = include_str!("control.rs");
    let runtime_source = include_str!("runtime.rs");
    let sessions_source = include_str!("sessions.rs");

    assert!(memory_ops_source.contains("#[buffer] message: JsBuffer"));
    assert!(memory_ops_source.contains("#[buffer] chunk: JsBuffer"));
    assert!(memory_ops_source.contains("#[buffer] datagram: JsBuffer"));
    assert!(!memory_ops_source.contains("#[buffer(copy)] message: Vec<u8>"));
    assert!(!memory_ops_source.contains("#[buffer(copy)] chunk: Vec<u8>"));
    assert!(!memory_ops_source.contains("#[buffer(copy)] datagram: Vec<u8>"));
    assert!(!ops_source.contains("op_memory_transport_recv_stream"));
    assert!(!ops_source.contains("op_memory_transport_recv_datagram"));
    assert!(!memory_ops_source.contains("op_memory_transport_recv_stream"));
    assert!(!memory_ops_source.contains("op_memory_transport_recv_datagram"));
    assert!(!memory_types_source.contains("MemoryTransportRecvResult"));
    assert!(!memory_types_source.contains("MemoryTransportRecvStreamEvent"));
    assert!(!memory_types_source.contains("MemoryTransportRecvDatagramEvent"));
    assert!(!memory_types_source.contains("TransportRecvEvent"));
    assert!(!control_source.contains("MemoryTransportRecvStream"));
    assert!(!control_source.contains("MemoryTransportRecvDatagram"));
    assert!(!runtime_source.contains("MemoryTransportRecvStream"));
    assert!(!runtime_source.contains("MemoryTransportRecvDatagram"));
    assert!(!sessions_source.contains("handle_memory_transport_recv_stream"));
    assert!(!sessions_source.contains("handle_memory_transport_recv_datagram"));
    assert!(!sessions_source.contains("inbound_streams"));
    assert!(!sessions_source.contains("inbound_stream_closed"));
    assert!(!sessions_source.contains("inbound_datagrams"));
}

#[test]
fn response_headers_are_prepared_handles_for_lifecycle_ops() {
    let worker_runtime_source =
        include_str!(concat!(env!("OUT_DIR"), "/execute_worker.generated.js"));
    let request_types_source = include_str!("../ops/request_types.rs");
    let storage_http_source = include_str!("../ops/storage_http.rs");
    let completion_start = storage_http_source
        .find("pub(crate) fn op_emit_completion_ok")
        .expect("op_emit_completion_ok should exist");
    let completion_end = storage_http_source[completion_start..]
        .find("pub(crate) fn op_emit_completion_error")
        .map(|offset| completion_start + offset)
        .expect("op_emit_completion_error should follow op_emit_completion_ok");
    let completion_source = &storage_http_source[completion_start..completion_end];

    assert!(worker_runtime_source.contains("op_http_store_prepared_headers"));
    assert!(worker_runtime_source
        .contains("headersHandle: streamResponse ? 0 : storeResponseHeaders(headers)"));
    assert!(worker_runtime_source
        .contains("Math.max(0, Math.trunc(Number(result.headersHandle ?? 0) || 0))"));
    assert!(worker_runtime_source.contains("bodyHandle: 0"));
    assert!(worker_runtime_source.contains("result.bodyHandle = callOp("));
    assert!(worker_runtime_source
        .contains("Math.max(0, Math.trunc(Number(result.bodyHandle ?? 0) || 0))"));
    assert!(!worker_runtime_source.contains("result.body = concatByteChunks"));
    assert!(!worker_runtime_source.contains("result.body == null"));
    assert!(request_types_source.contains("pub struct HttpPreparedHeaders"));
    assert!(completion_source.contains("headers_handle: u32"));
    assert!(completion_source.contains("body_handle: u32"));
    assert!(completion_source.contains("borrow_mut::<HttpPreparedHeaders>()"));
    assert!(completion_source.contains("borrow_mut::<HttpPreparedBodies>()"));
    assert!(!completion_source.contains("#[buffer(copy)] body: Vec<u8>"));
}

#[test]
fn host_fetch_prepare_uses_prepared_header_and_body_handles() {
    let worker_runtime_source =
        include_str!(concat!(env!("OUT_DIR"), "/execute_worker.generated.js"));
    let storage_http_source = include_str!("../ops/storage_http.rs");
    let prepare_start = storage_http_source
        .find("pub(crate) fn op_http_prepare")
        .expect("op_http_prepare should exist");
    let prepare_end = storage_http_source[prepare_start..]
        .find("pub(crate) fn op_http_take_prepared_body")
        .map(|offset| prepare_start + offset)
        .expect("op_http_take_prepared_body should follow op_http_prepare");
    let prepare_source = &storage_http_source[prepare_start..prepare_end];

    assert!(worker_runtime_source.contains("normalizedHeadersHandle"));
    assert!(worker_runtime_source.contains("normalizedBodyHandle"));
    assert!(worker_runtime_source.contains("prepared.headers_handle"));
    assert!(prepare_source.contains("headers_handle: u32"));
    assert!(prepare_source.contains("body_handle: u32"));
    assert!(!prepare_source.contains("#[serde] headers: Vec<(String, String)>"));
    assert!(!prepare_source.contains("#[buffer(copy)] body: Vec<u8>"));
}

#[test]
fn cache_ops_use_prepared_header_and_body_handles() {
    let bootstrap_source = include_str!("../../js/bootstrap.js");
    let storage_http_source = include_str!("../ops/storage_http.rs");
    let op_source = |name: &str, next: &str| {
        let start = storage_http_source
            .find(name)
            .unwrap_or_else(|| panic!("{name} should exist"));
        let end = storage_http_source[start..]
            .find(next)
            .map(|offset| start + offset)
            .unwrap_or(storage_http_source.len());
        &storage_http_source[start..end]
    };
    let match_source = op_source(
        "pub(crate) async fn op_cache_match",
        "pub(crate) async fn op_cache_put",
    );
    let put_source = op_source(
        "pub(crate) async fn op_cache_put",
        "pub(crate) async fn op_cache_delete",
    );
    let delete_source = op_source(
        "pub(crate) async fn op_cache_delete",
        "fn ensure_cache_allowed",
    );
    let revalidate_source = op_source(
        "pub(crate) fn op_emit_cache_revalidate",
        "pub(crate) fn replace_placeholders_text",
    );

    assert!(bootstrap_source.contains("op_http_store_prepared_headers"));
    assert!(bootstrap_source.contains("result.body_handle"));
    assert!(bootstrap_source.contains("result.headers_handle"));
    assert!(!bootstrap_source.contains("new Uint8Array(result.body ?? [])"));
    assert!(!bootstrap_source.contains("Array.isArray(result.headers) ? result.headers : []"));
    assert!(match_source.contains("headers_handle: u32"));
    assert!(match_source.contains("body_handle"));
    assert!(!match_source.contains("#[serde] headers: Vec<(String, String)>"));
    assert!(put_source.contains("request_headers_handle: u32"));
    assert!(put_source.contains("response_headers_handle: u32"));
    assert!(!put_source.contains("#[serde] request_headers: Vec<(String, String)>"));
    assert!(!put_source.contains("#[serde] response_headers: Vec<(String, String)>"));
    assert!(delete_source.contains("headers_handle: u32"));
    assert!(!delete_source.contains("#[serde] headers: Vec<(String, String)>"));
    assert!(revalidate_source.contains("headers_handle: u32"));
    assert!(!revalidate_source.contains("#[serde] headers: Vec<(String, String)>"));
}

#[test]
fn kv_committed_writes_use_writer_reply_lane() {
    let kv_source = include_str!("../kv.rs");
    let storage_http_source = include_str!("../ops/storage_http.rs");
    let source_range = |start_marker: &str, end_marker: &str| {
        let start = kv_source
            .find(start_marker)
            .unwrap_or_else(|| panic!("{start_marker} should exist"));
        let end = kv_source[start..]
            .find(end_marker)
            .map(|offset| start + offset)
            .unwrap_or(kv_source.len());
        &kv_source[start..end]
    };
    let put_source = source_range("pub async fn put(", "pub async fn put_value(");
    let put_value_source = source_range("pub async fn put_value(", "pub async fn delete(");
    let delete_source = source_range("pub async fn delete(", "async fn commit_single_version(");

    assert!(kv_source.contains("committed: VecDeque<KvCommittedBatch>"));
    assert!(kv_source.contains("committed_mutations: usize"));
    assert!(kv_source.contains("committed_bytes: usize"));
    assert!(kv_source.contains("committed_backlog_accepts"));
    assert!(kv_source.contains("state.push_committed"));
    assert!(kv_source.contains("state.pop_committed"));
    assert!(kv_source.contains("kv committed write queue overloaded: enqueue rejected"));
    assert!(kv_source.contains("struct KvCommittedBatch"));
    assert!(kv_source.contains("async fn commit_batch("));
    assert!(kv_source.contains("KvWriterWork::Committed"));
    assert!(put_source.contains("commit_single_version"));
    assert!(put_value_source.contains("commit_single_version"));
    assert!(delete_source.contains("commit_single_version"));
    assert!(!put_source.contains("conn.execute("));
    assert!(!put_value_source.contains("conn.execute("));
    assert!(!delete_source.contains("conn.execute("));
    assert!(!kv_source.contains("KV_VERSION_RETRIES"));
    assert!(!kv_source.contains("execute_with_retry"));
    assert!(storage_http_source.contains("pub(crate) async fn op_kv_put_value_bytes"));
    assert!(storage_http_source.contains("pub(crate) fn op_kv_enqueue_put_value_bytes"));
    assert!(storage_http_source.contains("#[buffer] value: JsBuffer"));
    assert!(!storage_http_source.contains("#[buffer(copy)] value: Vec<u8>"));
}

#[test]
fn wait_until_lifecycle_count_is_owned_by_request_context_handle() {
    let worker_runtime_source =
        include_str!(concat!(env!("OUT_DIR"), "/execute_worker.generated.js"));
    let request_ops_source = include_str!("../ops/request.rs");
    let request_types_source = include_str!("../ops/request_types.rs");
    let storage_http_source = include_str!("../ops/storage_http.rs");

    assert!(worker_runtime_source.contains("op_request_wait_until_register"));
    assert!(!worker_runtime_source.contains("waitUntilDoneSent"));
    assert!(request_ops_source.contains("op_request_wait_until_register"));
    assert!(request_types_source.contains("increment_wait_until"));
    assert!(request_types_source.contains("mark_wait_until_done"));
    assert!(storage_http_source.contains("context.wait_until_count"));
    assert!(!storage_http_source.contains("wait_until_count: u32"));
}

#[test]
fn isolate_runtime_loop_uses_thread_local_event_queue_without_poll_sleep() {
    let runtime_source = include_str!("runtime.rs");
    let model_source = include_str!("model.rs");
    let lifecycle_source = include_str!("lifecycle.rs");
    let ops_source = include_str!("../ops.rs");

    assert!(runtime_source.contains("pending_events: &Rc<RefCell<VecDeque<RuntimeEvent>>>,"));
    assert!(runtime_source.contains("Rc::new(RefCell::new(VecDeque::<RuntimeEvent>::new()))"));
    assert!(runtime_source.contains("event_loop_notify.notified()"));
    assert!(runtime_source.contains("pump_event_loop_once"));
    assert!(model_source.contains("Starting { started_at: Instant }"));
    assert!(model_source.contains("Ready"));
    assert!(model_source.contains("Retiring"));
    assert!(lifecycle_source.contains("isolate.startup = IsolateStartup::Ready"));
    assert!(lifecycle_source.contains("isolate.startup = IsolateStartup::Retiring"));
    assert!(runtime_source.contains("RuntimeEvent::IsolateExited"));
    assert!(lifecycle_source.contains("track_exiting_isolate_slot"));
    assert!(ops_source.contains("pub struct IsolateEventSender(pub Rc<dyn Fn"));
    assert!(!runtime_source.contains("Arc<StdMutex<VecDeque<RuntimeEvent>>>"));
    assert!(!runtime_source.contains("isolate event queue mutex poisoned"));
    assert!(!runtime_source.contains("Duration::from_millis(1)"));
    assert!(!runtime_source.contains("recv_timeout"));
}

#[test]
fn vite_dev_websocket_bridge_waits_for_runtime_frames_without_interval_polling() {
    let vite_source = include_str!("../../../../packages/dd-vite/src/vite.js");
    let runtime_client_source = include_str!("../../../../packages/dd-vite/src/runtime.js");
    let dev_runtime_source = include_str!("../bin/dd_dev_runtime.rs");

    assert!(vite_source.contains("runRuntimeFrameLoop"));
    assert!(vite_source.contains("drainRuntimeFrames"));
    assert!(vite_source.contains("waitWebSocketFrame"));
    assert!(!vite_source.contains("pollRuntimeFrames"));
    assert!(!vite_source.contains("pollTimer"));
    assert!(!vite_source.contains("setInterval(() => {\n      void this.pollRuntimeFrames();"));
    assert!(runtime_client_source.contains("op: \"wait_websocket_frame\""));
    assert!(runtime_client_source.contains("}, { timeoutMs: 0 })"));
    assert!(dev_runtime_source.contains("WaitWebsocketFrame"));
    assert!(dev_runtime_source.contains("websocket_wait_frame"));
    assert!(dev_runtime_source.contains("mpsc::channel::<ResponseEnvelope<CommandResult>>"));
    assert!(dev_runtime_source
        .contains("matches!(&request.command, DevCommand::WaitWebsocketFrame { .. })"));
    assert!(dev_runtime_source.contains("tokio::spawn(async move"));
}

#[test]
fn scheduler_queue_uses_stable_keys_and_drains_stale_targets_by_index() {
    let model_source = include_str!("model.rs");
    let lifecycle_source = include_str!("lifecycle.rs");
    let dispatch_source = include_str!("dispatch.rs");
    let remove_isolate_start = lifecycle_source
        .find("pub(crate) fn remove_isolate(")
        .expect("remove_isolate should exist");
    let remove_isolate_end = lifecycle_source[remove_isolate_start..]
        .find("pub(crate) fn remove_isolate_by_id(")
        .map(|offset| remove_isolate_start + offset)
        .expect("remove_isolate_by_id should follow remove_isolate");
    let remove_isolate_source = &lifecycle_source[remove_isolate_start..remove_isolate_end];
    let expire_queue_start = dispatch_source
        .find("pub(crate) fn expire_queued_requests(")
        .expect("expire_queued_requests should exist");
    let expire_queue_end = dispatch_source[expire_queue_start..]
        .find("pub(crate) fn expire_inflight_requests(")
        .map(|offset| expire_queue_start + offset)
        .expect("expire_inflight_requests should follow expire_queued_requests");
    let expire_queue_source = &dispatch_source[expire_queue_start..expire_queue_end];

    assert!(model_source.contains("by_runtime_request_id: HashMap<String, PendingQueueKey>"));
    assert!(model_source.contains("by_target_isolate_id: HashMap<u64, HashSet<PendingQueueKey>>"));
    assert!(model_source.contains("by_memory_owner_key: HashMap<String, HashSet<PendingQueueKey>>"));
    assert!(model_source.contains("by_enqueued_at: BTreeMap<Instant, HashSet<PendingQueueKey>>"));
    assert!(model_source.contains("memory_shard_affinity: HashMap<usize, u64>"));
    assert!(model_source.contains("memory_shards: BTreeMap<usize, MemoryShardQueue>"));
    assert!(model_source.contains("struct MemoryOwnerQueue"));
    assert!(model_source.contains("ready: VecDeque<String>"));
    assert!(model_source.contains("ready_membership: HashSet<String>"));
    assert!(model_source.contains("blocked: HashSet<String>"));
    assert!(model_source.contains("memory_next_shard_cursor: Option<usize>"));
    assert!(model_source.contains("memory_shard_index: Option<usize>"));
    assert!(model_source.contains("memory_owner_key: Option<String>"));
    assert!(model_source.contains("pub(super) fn find_fair_map<T>("));
    assert!(model_source.contains("fn find_memory_round_robin_head_map<T>("));
    assert!(model_source.contains("fn find_round_robin_owner_head_map<T>("));
    assert!(model_source.contains("pub(super) fn mark_memory_owner_blocked("));
    assert!(model_source.contains("pub(super) fn mark_memory_owner_ready("));
    assert!(model_source.contains("targeted: BTreeMap<u64, PendingInvoke>"));
    assert!(!model_source.contains("memory: BTreeMap<u64, PendingInvoke>"));
    assert!(model_source.contains("pub(super) fn drain_target_isolate_id("));
    assert!(model_source.contains("pub(super) fn drain_expired("));
    assert!(model_source.contains("pub(super) fn next_expiry_at("));
    assert!(!model_source.contains("VecDeque<QueuedPendingInvoke>"));
    assert!(!model_source.contains("lane.remove(index)"));
    assert!(dispatch_source.contains("remove_by_runtime_request_id(&runtime_request_id)"));
    assert!(dispatch_source.contains("RuntimeAtomicQueueWait"));
    assert!(dispatch_source.contains("memory_shard_affinity"));
    assert!(runtime_source_contains_fair_memory_dispatch());
    assert!(remove_isolate_source.contains("pool.queue.drain_target_isolate_id(isolate.id)"));
    assert!(remove_isolate_source.contains("pool.memory_shard_affinity"));
    assert!(remove_isolate_source
        .contains("self.account_dequeued_many(stale_targeted_count, stale_targeted_bytes)"));
    assert!(
        remove_isolate_source.contains("PlatformError::runtime(\"target isolate is unavailable\")")
    );
    assert!(expire_queue_source.contains("pool.queue.drain_expired(now, max_queue_wait)"));
    assert!(expire_queue_source.contains("pool.queue.next_expiry_at(max_queue_wait)"));
    assert!(!expire_queue_source.contains("duration_since(pending.enqueued_at)"));
    assert!(!expire_queue_source.contains("for pending in pool.queue.iter()"));
}

fn runtime_source_contains_fair_memory_dispatch() -> bool {
    let runtime_source = include_str!("runtime.rs");
    runtime_source.contains("pool.queue.find_fair_map(")
        && runtime_source.contains("memory_shard_affinity_outcome(")
}

#[test]
fn memory_outbox_scheduled_drains_are_bounded_and_requeued() {
    let sessions_source = include_str!("sessions.rs");
    let control_source = include_str!("control.rs");
    let runtime_source = include_str!("runtime.rs");
    let memory_source = include_str!("../memory.rs");

    assert!(sessions_source.contains("MEMORY_OUTBOX_SCHEDULED_DRAIN_BATCHES: usize = 1"));
    assert!(sessions_source.contains("MEMORY_OUTBOX_WORKER_CHANNEL_CAPACITY"));
    assert!(sessions_source.contains("run_memory_outbox_worker"));
    assert!(sessions_source.contains("claim_due_outbox_records_for_shard_index"));
    assert!(sessions_source.contains("RuntimeEvent::MemoryOutboxDelivery"));
    assert!(sessions_source.contains("apply_outbox_delivery_outcomes(&outcomes).await"));
    assert!(sessions_source.contains("retry_pending_memory_outbox_drains"));
    assert!(control_source.contains("MemoryOutboxDelivery"));
    assert!(!control_source.contains("drain_scheduled_memory_outbox_shard"));
    assert!(runtime_source.contains("tokio::spawn(run_memory_outbox_worker"));
    assert!(runtime_source.contains("schedule_all_memory_outbox_shards(&event_tx)"));
    assert!(memory_source.contains("pub async fn apply_outbox_delivery_outcomes"));
    assert!(!sessions_source.contains("MEMORY_OUTBOX_MAX_DRAIN_BATCHES"));
}

#[test]
fn memory_direct_writes_use_direct_apply_fast_path() {
    let memory_source = include_str!("../../js/execute_worker/memory.js");
    let fetch_cache_source = include_str!("../../js/execute_worker/fetch_cache.js");
    let worker_runtime_source =
        include_str!(concat!(env!("OUT_DIR"), "/execute_worker.generated.js"));
    let memory_ops_source = include_str!("../ops/memory.rs");
    let memory_types_source = include_str!("../ops/memory_types.rs");
    let memory_store_source = include_str!("../memory.rs");
    let add_mutation_start = worker_runtime_source
        .find("const addMemoryBatchMutation = (txn, mutation) =>")
        .expect("addMemoryBatchMutation should exist");
    let add_mutation_end = worker_runtime_source[add_mutation_start..]
        .find("const finishMemoryTxn")
        .map(|offset| add_mutation_start + offset)
        .expect("finishMemoryTxn should follow addMemoryBatchMutation");
    let add_mutation_source = &worker_runtime_source[add_mutation_start..add_mutation_end];
    let batch_mutation_start = memory_store_source
        .find("pub struct MemoryBatchMutation")
        .expect("MemoryBatchMutation should exist");
    let batch_mutation_end = memory_store_source[batch_mutation_start..]
        .find("pub struct MemoryCommandResultWrite")
        .map(|offset| batch_mutation_start + offset)
        .expect("MemoryCommandResultWrite should follow MemoryBatchMutation");
    let batch_mutation_source = &memory_store_source[batch_mutation_start..batch_mutation_end];
    let memory_state_entry_start = memory_types_source
        .find("pub(crate) struct MemoryStateGetEntry")
        .expect("MemoryStateGetEntry should exist");
    let memory_state_entry_end = memory_types_source[memory_state_entry_start..]
        .find("pub(crate) struct MemoryProfileResult")
        .map(|offset| memory_state_entry_start + offset)
        .expect("MemoryProfileResult should follow memory state entries");
    let memory_state_entry_source =
        &memory_types_source[memory_state_entry_start..memory_state_entry_end];
    let memory_command_begin_start = memory_types_source
        .find("pub(crate) struct MemoryCommandBeginResult")
        .expect("MemoryCommandBeginResult should exist");
    let memory_command_begin_end = memory_types_source[memory_command_begin_start..]
        .find("pub(crate) struct MemorySocketSendResult")
        .map(|offset| memory_command_begin_start + offset)
        .unwrap_or(memory_types_source.len());
    let memory_command_begin_source =
        &memory_types_source[memory_command_begin_start..memory_command_begin_end];
    let memory_invoke_method_start = memory_types_source
        .find("pub(crate) struct MemoryInvokeMethodResult")
        .expect("MemoryInvokeMethodResult should exist");
    let memory_invoke_method_end = memory_types_source[memory_invoke_method_start..]
        .find("pub(crate) struct MemorySocketSendResult")
        .map(|offset| memory_invoke_method_start + offset)
        .expect("MemorySocketSendResult should follow MemoryInvokeMethodResult");
    let memory_invoke_method_source =
        &memory_types_source[memory_invoke_method_start..memory_invoke_method_end];

    assert!(memory_source.contains("async write(key, value, options)"));
    assert!(memory_source.contains("const preferCallerIsolate = !descriptor.export_name"));
    assert!(
        !memory_source.contains("methodName === MEMORY_ATOMIC_METHOD || !descriptor.export_name")
    );
    assert!(memory_source.contains("rejectMemoryDirectOptions(\"set\", options)"));
    assert!(memory_source.contains("await applyDirectMemoryMutations(entry, runtimeRequestId"));
    assert!(memory_source.contains("const encoded = encodeMemoryStorageValue(value)"));
    assert!(memory_source.contains("async delete(key, options)"));
    assert!(memory_source.contains("rejectMemoryDirectOptions(\"delete\", options)"));
    assert!(memory_source.contains("deleted: true"));
    assert!(memory_source.contains("async writeMany(entries)"));
    assert!(memory_source.contains("normalizedEntries.map((item) =>"));
    assert!(!memory_source.contains("ensureMemoryStorageCommitted(entry, runtimeRequestId)"));
    assert!(!memory_source.contains("createMemoryStorageBinding(entry, runtimeRequestId);"));
    assert!(worker_runtime_source.contains("op_memory_direct_apply"));
    assert!(worker_runtime_source.contains("const applyDirectMemoryMutations = async"));
    assert!(worker_runtime_source.contains("op_memory_batch_apply"));
    assert!(!worker_runtime_source.contains("op_memory_batch_next_version"));
    assert!(!memory_ops_source.contains("op_memory_batch_next_version"));
    assert!(!memory_types_source.contains("MemoryBatchNextVersionResult"));
    assert!(!worker_runtime_source.contains("memoryTxnNextVersion"));
    assert!(memory_ops_source.contains("op_memory_direct_apply"));
    assert!(memory_ops_source.contains("MemoryDirectMutationInput"));
    assert!(memory_ops_source.contains("MemoryBatchMutationResult"));
    assert!(memory_ops_source.contains("#[buffer] value: JsBuffer"));
    assert!(memory_ops_source.contains("op_memory_bytes_take"));
    assert!(memory_ops_source.contains("memory_output_bytes_insert"));
    assert!(memory_ops_source.contains("value_handle: memory_output_bytes_insert"));
    assert!(memory_ops_source.contains("Bytes::copy_from_slice(value.as_ref())"));
    assert!(!memory_ops_source.contains("#[buffer(copy)] value: Vec<u8>"));
    assert!(memory_state_entry_source.contains("value_handle: u32"));
    assert!(!memory_state_entry_source.contains("value: Vec<u8>"));
    assert!(memory_command_begin_source.contains("value_handle: u32"));
    assert!(!memory_command_begin_source.contains("value: Vec<u8>"));
    assert!(memory_invoke_method_source.contains("value_handle: u32"));
    assert!(!memory_invoke_method_source.contains("value: Vec<u8>"));
    assert!(memory_ops_source.contains("Ok(MemoryInvokeResponse::Method { value })"));
    assert!(
        memory_ops_source.contains("caller_request_context_handle,\n                    value,")
    );
    assert!(memory_source.contains("return decodeRpcResult(takeMemoryBytes(result))"));
    assert!(!memory_source.contains("decodeRpcResult(toArrayBytes(result.value))"));
    assert!(fetch_cache_source.contains("const takeMemoryBytes = (record) =>"));
    assert!(fetch_cache_source.contains("op_memory_bytes_take"));
    assert!(fetch_cache_source.contains("activeRequestContextHandle(), handle"));
    assert!(fetch_cache_source.contains("value: takeMemoryBytes(record)"));
    assert!(fetch_cache_source.contains("value: takeMemoryBytes(entryValue)"));
    assert!(
        fetch_cache_source.contains("value: result.hit === true ? takeMemoryBytes(result) : null")
    );
    assert!(!fetch_cache_source.contains("toArrayBytes(entryValue?.value"));
    assert!(
        !fetch_cache_source.contains("result.value == null ? null : toArrayBytes(result.value)")
    );
    assert!(memory_types_source.contains("value: Bytes"));
    assert!(!add_mutation_source.contains("committedVersion"));
    assert!(!memory_ops_source.contains("committed_version: f64"));
    assert!(!memory_types_source.contains("next_version: Option<i64>"));
    assert!(!batch_mutation_source.contains("version: i64"));
    assert!(worker_runtime_source.contains("memory batch mutation did not return a staged record"));
    assert!(memory_ops_source.contains("close_memory_command_for_committed_batch"));
    assert!(memory_ops_source.contains("memory_batch_commit_owner_epoch"));
    assert!(memory_ops_source.contains("memory batch commit requires memory owner epoch"));
    assert!(memory_ops_source.contains("Some(owner_epoch)"));
    assert!(memory_ops_source.contains(".get(batch.command_handle)"));
    assert!(!memory_ops_source.contains(".remove(batch.command_handle)\n        .ok_or_else"));
    assert!(worker_runtime_source.contains("memory transaction commit failed"));
    assert!(worker_runtime_source.contains("finishMemoryTxn"));
    assert!(worker_runtime_source.contains("memory storage writes require stub.atomic(...)"));
    assert!(!worker_runtime_source.contains("memory storage batch commit failed"));
    assert!(!worker_runtime_source.contains("queueMemoryMutation"));
    assert!(!worker_runtime_source.contains("ensureMemoryStorageCommitted"));
    assert!(!worker_runtime_source.contains("pendingBatches"));
    assert!(!worker_runtime_source.contains("flushRunning"));
    assert!(!worker_runtime_source.contains("flushTail"));
    assert!(!worker_runtime_source.contains("commitMemoryTxn"));
    assert!(!worker_runtime_source.contains("requires_commit"));
    assert!(!worker_runtime_source.contains("batchSnapshot"));
    assert!(!worker_runtime_source.contains("op_memory_batch_snapshot"));
    assert!(!memory_ops_source.contains("op_memory_batch_snapshot"));
    assert!(!memory_types_source.contains("MemoryBatchSnapshotResult"));
    assert!(worker_runtime_source.contains("op_memory_batch_list_overlay"));
    assert!(memory_ops_source.contains("op_memory_batch_list_overlay"));
    assert!(!worker_runtime_source.contains("op_memory_batch_enqueue"));
    assert!(!worker_runtime_source.contains("op_memory_state_await_submission"));
    assert!(!worker_runtime_source.contains("op_memory_batch_configure"));
    assert!(!worker_runtime_source.contains("configureMemoryBatch"));
    assert!(!worker_runtime_source.contains("blind_apply_allowed"));
    assert!(!worker_runtime_source.contains("blind_applied"));
    assert!(!worker_runtime_source.contains("js_txn_blind_commit"));
    assert!(!memory_ops_source.contains("apply_blind_batch"));
    assert!(!memory_ops_source.contains("memory_batch_blind_apply_allowed"));
    assert!(!memory_ops_source.contains("js_txn_blind_commit"));
    assert!(!worker_runtime_source.contains("pendingSubmissionPromises"));
    assert!(!worker_runtime_source.contains("result.conflict"));
    assert!(!worker_runtime_source.contains("expectedVersion"));
    assert!(!worker_runtime_source.contains("optimisticWriteVersion"));
    assert!(!worker_runtime_source.contains("conflict: true"));
    assert!(!worker_runtime_source.contains("conflict: false"));
    assert!(!memory_types_source.contains("pub(crate) conflict: bool"));
    assert!(!memory_types_source.contains("requires_commit"));
    assert!(!memory_types_source.contains("blind_apply_allowed"));
    assert!(!memory_types_source.contains("blind_applied"));
    assert!(!memory_ops_source.contains("MemoryBatchKind"));
    assert!(!worker_runtime_source.contains("defer(callback)"));
    assert!(!worker_runtime_source.contains("txn.deferred"));
    assert!(!worker_runtime_source.contains("memory transaction conflicted"));
}

#[test]
fn static_asset_lookup_uses_catalog_without_runtime_manager_command() {
    let control_source = include_str!("control.rs");
    let facade_source = include_str!("facade.rs");
    let lifecycle_source = include_str!("lifecycle.rs");

    assert!(!control_source.contains("ResolveAsset"));
    assert!(facade_source.contains("pub fn resolve_asset("));
    assert!(facade_source.contains("pub fn resolve_public_asset("));
    assert!(facade_source.contains("pub fn resolve_public_route_asset("));
    assert!(facade_source.contains("pub struct PublicRouteAssetResolution"));
    assert!(facade_source.contains("fn resolve_asset_from_catalog("));
    assert!(facade_source.contains("self.asset_catalog.get(worker_name)"));
    assert!(control_source.contains("prepared: PreparedWorkerDeployment"));
    assert!(facade_source.contains("fn prepare_worker_deployment("));
    assert!(lifecycle_source.contains("AssetCatalogEntry {"));
    assert!(lifecycle_source.contains("worker_name: worker_name.clone()"));
    assert!(lifecycle_source.contains("generation,"));
    assert!(facade_source.contains("entry.worker_name != worker_name"));
    assert!(lifecycle_source.contains(
        "self.asset_catalog\n            .insert(worker_name.clone(), asset_catalog_entry)"
    ));
    assert!(lifecycle_source.contains("self.asset_catalog.remove(worker_name)"));
    assert!(!lifecycle_source.contains("compile_asset_bundle("));
    assert!(!lifecycle_source.contains("extract_bindings(&config)"));
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
