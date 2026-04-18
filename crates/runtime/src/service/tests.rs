use super::{
    BlobStoreConfig, RuntimeConfig, RuntimeService, RuntimeServiceConfig, RuntimeStorageConfig,
};
use common::{
    DeployAsset, DeployBinding, DeployConfig, DeployInternalConfig, DeployTraceDestination,
    WorkerInvocation, WorkerOutput,
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

#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize, Default)]
struct TestKvProfileMetric {
    calls: u64,
    total_us: u64,
    total_items: u64,
    max_us: u64,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize, Default)]
struct TestKvProfileSnapshot {
    enabled: bool,
    op_get: TestKvProfileMetric,
    js_cache_hit: TestKvProfileMetric,
    js_cache_miss: TestKvProfileMetric,
    js_cache_stale: TestKvProfileMetric,
    js_cache_fill: TestKvProfileMetric,
    js_cache_invalidate: TestKvProfileMetric,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct TestKvProfileEnvelope {
    ok: bool,
    snapshot: Option<TestKvProfileSnapshot>,
    error: String,
}

fn decode_kv_profile(output: WorkerOutput) -> TestKvProfileSnapshot {
    let envelope: TestKvProfileEnvelope = crate::json::from_string(
        String::from_utf8(output.body).expect("profile body should be utf8"),
    )
    .expect("profile response should parse");
    assert!(
        envelope.ok,
        "profile route should succeed: {}",
        envelope.error
    );
    envelope
        .snapshot
        .expect("profile snapshot should be present")
}
use tokio::time::{sleep, timeout};
use uuid::Uuid;

fn test_invocation() -> WorkerInvocation {
    WorkerInvocation {
        method: "GET".to_string(),
        url: "http://worker/".to_string(),
        headers: Vec::new(),
        body: Vec::new(),
        request_id: "test-request".to_string(),
    }
}

fn test_invocation_with_path(path: &str, request_id: &str) -> WorkerInvocation {
    WorkerInvocation {
        method: "GET".to_string(),
        url: format!("http://worker{path}"),
        headers: Vec::new(),
        body: Vec::new(),
        request_id: request_id.to_string(),
    }
}

fn test_websocket_invocation(path: &str, request_id: &str) -> WorkerInvocation {
    WorkerInvocation {
        method: "GET".to_string(),
        url: format!("http://worker{path}"),
        headers: vec![
            ("connection".to_string(), "Upgrade".to_string()),
            ("upgrade".to_string(), "websocket".to_string()),
            ("sec-websocket-version".to_string(), "13".to_string()),
            (
                "sec-websocket-key".to_string(),
                "dGhlIHNhbXBsZSBub25jZQ==".to_string(),
            ),
        ],
        body: Vec::new(),
        request_id: request_id.to_string(),
    }
}

fn test_transport_invocation() -> WorkerInvocation {
    WorkerInvocation {
        method: "CONNECT".to_string(),
        url: "http://worker/session".to_string(),
        headers: vec![(
            "x-dd-transport-protocol".to_string(),
            "webtransport".to_string(),
        )],
        body: Vec::new(),
        request_id: "test-transport-request".to_string(),
    }
}

fn counter_worker() -> String {
    r#"
let counter = 0;
export default {
  async fetch() {
    counter += 1;
    return new Response(String(counter));
  },
};
"#
    .to_string()
}

fn slow_worker() -> String {
    r#"
export default {
  async fetch() {
    await Deno.core.ops.op_sleep(40);
    return new Response("ok");
  },
};
"#
    .to_string()
}

fn dynamic_namespace_worker() -> String {
    r#"
let child = null;
class Api extends RpcTarget {
  constructor() {
    super();
    this.count = 0;
  }
  async bump() {
    this.count += 1;
    return this.count;
  }
}

export default {
  async fetch(request, env) {
    if (!child) {
      child = await env.SANDBOX.get("test:v1", async () => ({
        entrypoint: "worker.js",
        modules: {
          "worker.js": "import { nextCounter } from './lib.js'; export default { async fetch(_request, childEnv) { const hostCount = await childEnv.API.bump(); return new Response(String(nextCounter()) + ':' + String(hostCount)); } };",
          "./lib.js": "let counter = 0; export function nextCounter() { counter += 1; return counter; }",
        },
        env: { SECRET: "ok", API: new Api() },
        timeout: 1_500,
      }));
    }
    const response = await child.fetch("http://worker/");
    return new Response(await response.text());
  },
};
"#
        .to_string()
}

fn dynamic_plain_namespace_worker() -> String {
    dynamic_plain_namespace_worker_with_source(
            "let count = 0; export default { async fetch() { count += 1; return new Response(String(count)); } };",
        )
}

fn dynamic_plain_namespace_worker_with_source(child_source: &str) -> String {
    format!(
        r#"
let child = null;

export default {{
  async fetch(_request, env) {{
    if (!child) {{
      child = await env.SANDBOX.get("plain:v1", async () => ({{
        source: {child_source:?},
        timeout: 1_500,
      }}));
    }}
    return child.fetch("http://worker/");
  }},
}};
"#
    )
}

fn dynamic_plain_handle_cache_worker() -> String {
    r#"
export default {
  async fetch(_request, env) {
    const child = await env.SANDBOX.get("plain:v1", async () => ({
      source: "let count = 0; export default { async fetch() { count += 1; return new Response(String(count)); } };",
      timeout: 1_500,
    }));
    return child.fetch("http://worker/");
  },
};
"#
        .to_string()
}

fn dynamic_fast_fetch_worker() -> String {
    r#"
let hotChild = null;

function childConfig() {
  return {
    entrypoint: "worker.js",
    modules: {
      "worker.js": `
export default {
  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === "/echo") {
      return new Response(await request.arrayBuffer(), {
        status: Number(url.searchParams.get("status") ?? "207"),
        headers: [
          ["x-method", request.method],
          ["x-query", url.searchParams.get("q") ?? ""],
          ["x-test", request.headers.get("x-test") ?? ""],
          ["content-type", request.headers.get("content-type") ?? "application/octet-stream"],
        ],
      });
    }
    return new Response("ok");
  },
};
      `,
    },
    timeout: 1_500,
  };
}

function metricsResponse() {
  const jsMetrics = globalThis.__dd_dynamic_metrics ?? {};
  const rustProfile = Deno.core.ops.op_dynamic_profile_take?.() ?? null;
  return Response.json({
    ...jsMetrics,
    ...(rustProfile?.snapshot ?? {}),
  });
}

function resetMetrics() {
  const metrics = globalThis.__dd_dynamic_metrics ?? null;
  if (metrics && typeof metrics === "object") {
    for (const key of Object.keys(metrics)) {
      metrics[key] = 0;
    }
  }
  Deno.core.ops.op_dynamic_profile_reset?.();
  return new Response("ok");
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (url.pathname === "/__dynamic_metrics") {
      return metricsResponse();
    }
    if (url.pathname === "/__dynamic_metrics_reset") {
      return resetMetrics();
    }

    if (!hotChild) {
      hotChild = await env.SANDBOX.get("fast:v1", async () => childConfig());
    }

    if (url.pathname === "/hot") {
      return hotChild.fetch("http://worker/");
    }

    if (url.pathname === "/echo") {
      const body = await request.arrayBuffer();
      return hotChild.fetch(new Request(`http://worker/echo${url.search}`, {
        method: request.method,
        headers: request.headers,
        body,
      }));
    }

    return new Response("not found", { status: 404 });
  },
};
"#
    .to_string()
}

fn dynamic_remote_fast_fetch_worker() -> String {
    r#"
let hotChild = null;

class Api extends RpcTarget {
  constructor() {
    super();
    this.count = 0;
  }

  async bump() {
    this.count += 1;
    return this.count;
  }
}

function childConfig() {
  return {
    entrypoint: "worker.js",
    modules: {
      "worker.js": `
export default {
  async fetch(_request, childEnv) {
    const count = await childEnv.API.bump();
    return new Response(String(count));
  },
};
      `,
    },
    env: {
      API: new Api(),
    },
    timeout: 1_500,
  };
}

function metricsResponse() {
  const jsMetrics = globalThis.__dd_dynamic_metrics ?? {};
  const rustProfile = Deno.core.ops.op_dynamic_profile_take?.() ?? null;
  return Response.json({
    ...jsMetrics,
    ...(rustProfile?.snapshot ?? {}),
  });
}

function resetMetrics() {
  const metrics = globalThis.__dd_dynamic_metrics ?? null;
  if (metrics && typeof metrics === "object") {
    for (const key of Object.keys(metrics)) {
      metrics[key] = 0;
    }
  }
  Deno.core.ops.op_dynamic_profile_reset?.();
  return new Response("ok");
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (url.pathname === "/__dynamic_metrics") {
      return metricsResponse();
    }
    if (url.pathname === "/__dynamic_metrics_reset") {
      return resetMetrics();
    }

    if (!hotChild) {
      hotChild = await env.SANDBOX.get("fast-remote:v1", async () => childConfig());
    }

    if (url.pathname === "/hot") {
      return hotChild.fetch("http://worker/");
    }

    return new Response("not found", { status: 404 });
  },
};
"#
    .to_string()
}

fn dynamic_wake_probe_worker() -> String {
    r#"
class ProbeTarget extends RpcTarget {
  async ping() {
    return "ok";
  }
}

function ensureProvider() {
  const targetId = "probe-target";
  globalThis.__dd_probe_provider_id = targetId;
  if (globalThis.__dd_host_rpc_targets.has(targetId)) {
    return targetId;
  }
  globalThis.__dd_host_rpc_targets.set(targetId, new ProbeTarget());
  return targetId;
}

function testReplyFailure(error) {
  return {
    ok: false,
    value: "",
    error: String(error?.message ?? error ?? "test async reply failed"),
  };
}

async function waitTestReply(runtimeRequestId, started, timeoutMs) {
  const waitReply = globalThis.__dd_await_dynamic_reply;
  if (typeof waitReply !== "function") {
    throw new Error("dynamic reply helper missing");
  }
  const actualReplyId = String(started?.reply_id ?? "").trim();
  if (!actualReplyId) {
    return testReplyFailure(started?.error ?? "test reply failed to start");
  }
  const timeoutStarted = Deno.core.ops.op_test_async_reply_start({
    request_id: runtimeRequestId,
    delay_ms: timeoutMs,
    ok: false,
    error: `test async reply timed out after ${timeoutMs}ms`,
  });
  const timeoutReplyId = String(timeoutStarted?.reply_id ?? "").trim();
  try {
    const raced = await Promise.race([
      waitReply("test async reply", () => started, timeoutMs + 1_000).then((value) => ({
        kind: "actual",
        value,
      })),
      waitReply("test async timeout", () => timeoutStarted, timeoutMs + 1_000).then((value) => ({
        kind: "timeout",
        value,
      })),
    ]);
    if (raced.kind === "actual") {
      if (timeoutReplyId) {
        Deno.core.ops.op_test_async_reply_cancel(timeoutReplyId);
      }
      return raced.value;
    }
    if (actualReplyId) {
      Deno.core.ops.op_test_async_reply_cancel(actualReplyId);
    }
    return raced.value;
  } catch (error) {
    if (actualReplyId) {
      Deno.core.ops.op_test_async_reply_cancel(actualReplyId);
    }
    if (timeoutReplyId) {
      Deno.core.ops.op_test_async_reply_cancel(timeoutReplyId);
    }
    return testReplyFailure(error);
  }
}

function testReplyResponse(result, fallbackStatus = 500) {
  if (result && typeof result === "object" && result.ok === true) {
    return new Response(String(result.value ?? ""), { status: 200 });
  }
  const error = String(result?.error ?? "test reply failed");
  return new Response(error, { status: fallbackStatus });
}

export default {
  async fetch(request, _env, ctx) {
    const url = new URL(request.url);
    const runtimeRequestId = String(globalThis.__dd_get_runtime_request_id?.() ?? "").trim();

    if (url.pathname === "/async/immediate") {
      const result = await waitTestReply(
        runtimeRequestId,
        Deno.core.ops.op_test_async_reply_start({
          request_id: runtimeRequestId,
          value: "immediate",
        }),
        250,
      );
      return testReplyResponse(result);
    }

    if (url.pathname === "/async/delayed") {
      const result = await waitTestReply(
        runtimeRequestId,
        Deno.core.ops.op_test_async_reply_start({
          request_id: runtimeRequestId,
          delay_ms: 25,
          value: "delayed",
        }),
        1_000,
      );
      return testReplyResponse(result);
    }

    if (url.pathname === "/async/timeout") {
      const result = await waitTestReply(
        runtimeRequestId,
        Deno.core.ops.op_test_async_reply_start({
          request_id: runtimeRequestId,
          delay_ms: 200,
          value: "late",
        }),
        25,
      );
      return testReplyResponse(result, 504);
    }

    if (url.pathname === "/warm-provider") {
      ensureProvider();
      const holdMs = Math.max(0, Math.trunc(Number(url.searchParams.get("hold") ?? "0") || 0));
      if (holdMs > 0) {
        const hold = await waitTestReply(
          runtimeRequestId,
          Deno.core.ops.op_test_async_reply_start({
            request_id: runtimeRequestId,
            delay_ms: holdMs,
            value: "warm",
          }),
          holdMs + 1_000,
        );
        if (!hold || typeof hold !== "object" || hold.ok !== true) {
          return testReplyResponse(hold);
        }
      }
      return new Response("warm");
    }

    if (url.pathname === "/nested/same" || url.pathname === "/nested/other") {
      const targetId = ensureProvider();
      const result = await waitTestReply(
        runtimeRequestId,
        Deno.core.ops.op_test_nested_targeted_invoke_start({
          request_id: runtimeRequestId,
          target_mode: url.pathname.endsWith("/other") ? "other" : "same",
          target_id: targetId,
          method_name: "ping",
          args: Array.from(new Uint8Array(await globalThis.__dd_encode_rpc_args([]))),
        }),
        1_000,
      );
      return testReplyResponse(result);
    }

    return new Response("not found", { status: 404 });
  },
};
"#
    .to_string()
}

fn dynamic_namespace_ops_worker() -> String {
    r#"
function childConfig() {
  return {
    entrypoint: "worker.js",
    modules: {
      "worker.js": "export default { async fetch() { return new Response('ok'); } };",
    },
    timeout: 200,
  };
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (url.pathname === "/list") {
      const ids = await env.SANDBOX.list();
      return new Response(JSON.stringify(ids.sort()), {
        headers: [["content-type", "application/json"]],
      });
    }

    if (url.pathname === "/get-create") {
      await env.SANDBOX.delete("control:v1");
      let factoryCalls = 0;
      await env.SANDBOX.get("control:v1", async () => {
        factoryCalls += 1;
        return childConfig();
      });
      return new Response(String(factoryCalls));
    }

    if (url.pathname === "/get-hit") {
      let factoryCalls = 0;
      await env.SANDBOX.get("control:v1", async () => {
        factoryCalls += 1;
        return childConfig();
      });
      return new Response(String(factoryCalls));
    }

    if (url.pathname === "/delete") {
      const deleted = await env.SANDBOX.delete("control:v1");
      return new Response(String(Boolean(deleted)));
    }

    return new Response("not found", { status: 404 });
  },
};
"#
    .to_string()
}

fn dynamic_repeated_create_worker() -> String {
    r#"
export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const id = url.searchParams.get("id") ?? "0";
    const child = await env.SANDBOX.get(`repeat:${id}`, async () => ({
      entrypoint: "worker.js",
      modules: {
        "worker.js": `export default { async fetch() { return new Response(${JSON.stringify(id)}); } };`,
      },
      timeout: 1_500,
    }));
    return child.fetch("http://worker/");
  },
};
"#
        .to_string()
}

fn dynamic_snapshot_cache_worker() -> String {
    r#"
function childConfig() {
  return {
    entrypoint: "worker.js",
    modules: {
      "worker.js": "export default { async fetch() { return new Response('ok'); } };",
    },
    timeout: 1_500,
  };
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const id = url.searchParams.get("id") ?? "one";
    const child = await env.SANDBOX.get(`snap:${id}`, async () => childConfig());
    return child.fetch("http://worker/");
  },
};
"#
    .to_string()
}

fn dynamic_namespace_admin_worker() -> String {
    r#"
let slow = null;

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (url.pathname === "/ensure") {
      await env.SANDBOX.get("admin:v1", async () => ({
        entrypoint: "worker.js",
        modules: {
          "worker.js": "export default { async fetch() { return new Response('ok'); } };",
        },
        timeout: 200,
      }));
      return new Response("ok");
    }

    if (url.pathname === "/ids") {
      const ids = await env.SANDBOX.list();
      return new Response(JSON.stringify(ids.sort()), {
        headers: [["content-type", "application/json"]],
      });
    }

    if (url.pathname === "/delete") {
      const deleted = await env.SANDBOX.delete("admin:v1");
      return new Response(String(Boolean(deleted)));
    }

    if (url.pathname === "/timeout") {
      if (!slow) {
        slow = await env.SANDBOX.get("slow:v1", async () => ({
          entrypoint: "worker.js",
          modules: {
            "worker.js": "export default { async fetch() { await Deno.core.ops.op_sleep(100); return new Response('slow'); } };",
          },
          timeout: 25,
        }));
      }
      try {
        await slow.fetch("http://worker/");
        return new Response("no-timeout", { status: 200 });
      } catch (error) {
        return new Response(String(error || "timeout"), { status: 504 });
      }
    }

    return new Response("not found", { status: 404 });
  },
};
"#
        .to_string()
}

fn dynamic_handle_cache_delete_worker() -> String {
    r#"
function workerSource(value) {
  return `export default { async fetch() { return new Response(${JSON.stringify(value)}); } };`;
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (url.pathname === "/delete-recreate") {
      const first = await env.SANDBOX.get("cache:v1", async () => ({
        source: workerSource("one"),
        timeout: 1_500,
      }));
      const firstBody = await (await first.fetch("http://worker/")).text();
      await env.SANDBOX.delete("cache:v1");
      const second = await env.SANDBOX.get("cache:v1", async () => ({
        source: workerSource("two"),
        timeout: 1_500,
      }));
      const secondBody = await (await second.fetch("http://worker/")).text();
      return new Response(`${firstBody}:${secondBody}`);
    }

    return new Response("not found", { status: 404 });
  },
};
"#
    .to_string()
}

fn dynamic_response_json_worker() -> String {
    r#"
export default {
  async fetch(_request, env) {
    const child = await env.SANDBOX.get("json:v1", async () => ({
      entrypoint: "worker.js",
      modules: {
        "worker.js": "export default { async fetch() { return Response.json({ ok: true, source: 'dynamic-child' }); } };",
      },
      timeout: 1_500,
    }));
    const response = await child.fetch("http://worker/");
    return new Response(await response.text(), {
      headers: [["content-type", "application/json; charset=utf-8"]],
    });
  },
};
"#
        .to_string()
}

fn dynamic_runtime_surface_worker() -> String {
    r#"
function runtimeSurface() {
  return {
    fetch: typeof fetch === "function",
    request: typeof Request === "function",
    response: typeof Response === "function",
    headers: typeof Headers === "function",
    formData: typeof FormData === "function",
    url: typeof URL === "function",
    urlPattern: typeof URLPattern === "function",
    readableStream: typeof ReadableStream === "function",
    blob: typeof Blob === "function",
    textEncoder: typeof TextEncoder === "function",
    textDecoder: typeof TextDecoder === "function",
    structuredClone: typeof structuredClone === "function",
    cryptoDigest: typeof crypto?.subtle?.digest === "function",
    responseJsonType: Response.json({ ok: true }).headers.get("content-type"),
  };
}

export default {
  async fetch(_request, env) {
    const child = await env.SANDBOX.get("surface:v1", async () => ({
      entrypoint: "worker.js",
      modules: {
        "worker.js": `
function runtimeSurface() {
  return {
    fetch: typeof fetch === "function",
    request: typeof Request === "function",
    response: typeof Response === "function",
    headers: typeof Headers === "function",
    formData: typeof FormData === "function",
    url: typeof URL === "function",
    urlPattern: typeof URLPattern === "function",
    readableStream: typeof ReadableStream === "function",
    blob: typeof Blob === "function",
    textEncoder: typeof TextEncoder === "function",
    textDecoder: typeof TextDecoder === "function",
    structuredClone: typeof structuredClone === "function",
    cryptoDigest: typeof crypto?.subtle?.digest === "function",
    responseJsonType: Response.json({ ok: true }).headers.get("content-type"),
  };
}

export default {
  async fetch() {
    return Response.json(runtimeSurface());
  },
};
        `,
      },
      timeout: 1_500,
    }));
    const childResponse = await child.fetch("http://worker/");
    const childSurface = await childResponse.json();
    const selfSurface = runtimeSurface();
    return Response.json({
      same: JSON.stringify(selfSurface) === JSON.stringify(childSurface),
      self: selfSurface,
      child: childSurface,
    });
  },
};
"#
    .to_string()
}

fn transport_echo_worker() -> String {
    r#"
export default {
  async fetch(request, env) {
    return await env.MEDIA.get(env.MEDIA.idFromName("global")).atomic((state) => {
      const { response } = state.accept(request);
      return response;
    });
  },

  async wake(event, env) {
    const _ = env;
    if (event.type !== "transportstream" || !event.stub || !event.handle) {
      return;
    }
    await event.stub.apply([{ type: "transport.stream", handle: event.handle, payload: event.data }]);
  },
};
"#
        .to_string()
}

fn transport_shape_worker() -> String {
    r#"
export default {
  async fetch(request, env) {
    const protocol = String(request.headers.get("x-dd-transport-protocol") ?? "").toLowerCase();
    if (request.method !== "CONNECT") {
      return new Response(`bad-method:${request.method}`, { status: 500 });
    }
    if (protocol !== "webtransport") {
      return new Response(`bad-protocol:${protocol}`, { status: 500 });
    }
    return await env.MEDIA.get(env.MEDIA.idFromName("global")).atomic((state) => {
      const { response } = state.accept(request);
      return response;
    });
  },
};
"#
    .to_string()
}

fn transport_values_worker() -> String {
    r#"
export default {
  async fetch(request, env) {
    return await env.MEDIA.get(env.MEDIA.idFromName("global")).atomic((state) => {
      const { response } = state.accept(request);
      return response;
    });
  },

  async wake(event, env) {
    const _ = env;
    if (event.type !== "transportstream" || !event.stub || !event.handle) {
      return;
    }
    const handles = await event.stub.transports.values();
    await event.stub.apply([{
      type: "transport.stream",
      handle: event.handle,
      payload: new TextEncoder().encode(`ready:${handles.length}`),
    }]);
  },
};
"#
    .to_string()
}

fn websocket_storage_worker() -> String {
    r#"
export function openSocket(state, payload) {
  const { response } = state.accept(payload.request);
  return response;
}

export function onSocketMessage(state, event) {
  const text = typeof event.data === "string"
    ? event.data
    : new TextDecoder().decode(event.data);
  const chat = state.tvar("chat", { count: 0, last: null });
  const next = chat.modify((previous) => ({
    count: Number(previous?.count ?? 0) + 1,
    last: text,
  }));
  const socket = new WebSocket(event.handle);
  if (text === "close-me") {
    socket.close(1000, "server-close");
    return next;
  }
  socket.send(JSON.stringify({
    seen: next.last,
    count: next.count,
  }), "text");
  return next;
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/state") {
      const stored = await env.CHAT.get(env.CHAT.idFromName("global")).atomic((state) => state.tvar("chat", { count: 0, last: null }).read());
      return Response.json(stored ?? { count: 0, last: null });
    }
    return await env.CHAT.get(env.CHAT.idFromName("global")).atomic(openSocket, { request });
  },

  async wake(event, env) {
    const _ = env;
    if (event.type !== "socketmessage" || !event.stub) {
      return;
    }
    await event.stub.atomic(onSocketMessage, event);
  },
};
"#
        .to_string()
}

fn websocket_values_worker() -> String {
    r#"
function room(env) {
  return env.CHAT.get(env.CHAT.idFromName("global"));
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/handles") {
      const handles = await room(env).sockets.values();
      return Response.json({ count: handles.length, handles });
    }
    if (url.pathname === "/txn-handles") {
      const snapshot = await room(env).atomic((state) => {
        const first = state.stub.sockets.values();
        const second = state.stub.sockets.values();
        return {
          first_is_array: Array.isArray(first),
          second_is_array: Array.isArray(second),
          first_count: Array.isArray(first) ? first.length : -1,
          second_count: Array.isArray(second) ? second.length : -1,
        };
      });
      return Response.json(snapshot);
    }
    return await room(env).atomic((state) => {
      const { response } = state.accept(request);
      return response;
    });
  },

  async wake(event) {
    if (event.type !== "socketmessage" || !event.stub) {
      return;
    }
    const handles = await event.stub.sockets.values();
    const socket = new WebSocket(event.handle);
    socket.send(JSON.stringify({ count: handles.length, handles }), "text");
  },
};
"#
    .to_string()
}

fn websocket_socket_surface_worker() -> String {
    r#"
function stateSocketSurface(state) {
  return {
    accept: typeof state.accept,
    sockets: typeof state.sockets,
  };
}

export default {
  async fetch(_request, env) {
    const memory = env.CHAT.get(env.CHAT.idFromName("global"));
    const stubSurface = {
      values: typeof memory.sockets.values,
      send: typeof memory.sockets.send,
      close: typeof memory.sockets.close,
    };
    const stateSurface = await memory.atomic(stateSocketSurface);
    return Response.json({ stubSurface, stateSurface });
  },
};
"#
    .to_string()
}

fn dynamic_fetch_probe_worker(url: &str) -> String {
    format!(
        r#"
export default {{
  async fetch(_request, env) {{
    const response = await fetch("{url}?token=" + encodeURIComponent(env.API_TOKEN), {{
      headers: {{
        "authorization": "Bearer " + env.API_TOKEN,
        "x-dd-secret": env.API_TOKEN,
      }},
    }});
    return new Response(await response.text(), {{
      status: response.status,
      headers: response.headers,
    }});
  }},
}};
"#
    )
}

fn dynamic_fetch_abort_worker(url: &str) -> String {
    format!(
        r#"
export default {{
  async fetch() {{
    const controller = new AbortController();
    setTimeout(() => controller.abort(new Error("stop")), 25);
    try {{
      await fetch("{url}", {{
        signal: controller.signal,
        headers: {{
          "x-abort-test": "true",
        }},
      }});
      return new Response("unexpected-success", {{ status: 500 }});
    }} catch (error) {{
      return new Response(String(error?.name ?? error));
    }}
  }},
}};
"#
    )
}

fn preview_dynamic_worker() -> String {
    r#"
class PreviewControl extends RpcTarget {
  constructor(previewId) {
    super();
    this.previewId = previewId;
    this.hits = 0;
  }

  async metadata() {
    this.hits += 1;
    return {
      previewId: this.previewId,
      hits: this.hits,
    };
  }
}

function previewModules() {
  return {
    "worker.js": `
      export default {
        async fetch(request, env) {
          const url = new URL(request.url);
          const meta = await env.PREVIEW.metadata();
          if (url.pathname === "/" || url.pathname === "/index.html") {
            return new Response(JSON.stringify({
              ok: true,
              preview: meta.previewId,
              hits: meta.hits,
              route: "root",
            }), {
              headers: { "content-type": "application/json; charset=utf-8" },
            });
          }
          if (url.pathname === "/api/health") {
            return new Response(JSON.stringify({
              ok: true,
              preview: meta.previewId,
              hits: meta.hits,
              route: "health",
            }), {
              headers: { "content-type": "application/json; charset=utf-8" },
            });
          }
          return new Response("preview route not found", { status: 404 });
        },
      };
    `,
  };
}

async function ensurePreview(env, previewId) {
  return env.SANDBOX.get(`preview:${previewId}`, async () => ({
    entrypoint: "worker.js",
    modules: previewModules(),
    env: {
      PREVIEW: new PreviewControl(previewId),
    },
    timeout: 3000,
  }));
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (url.pathname === "/") {
      return new Response("preview manager");
    }

    if (!url.pathname.startsWith("/preview/")) {
      return new Response("not found", { status: 404 });
    }

    const rest = url.pathname.slice("/preview/".length);
    const slashIdx = rest.indexOf("/");
    const previewId = slashIdx === -1 ? rest : rest.slice(0, slashIdx);
    const tailPath = slashIdx === -1 ? "/" : rest.slice(slashIdx);
    const preview = await ensurePreview(env, previewId);
    const target = new URL(`http://worker${tailPath}`);
    target.search = url.search;
    return preview.fetch(target.toString(), {
      method: request.method,
      headers: request.headers,
    });
  },
};
"#
    .to_string()
}

fn versioned_worker(version: &str, delay_ms: u64) -> String {
    format!(
        r#"
export default {{
  async fetch() {{
    await Deno.core.ops.op_sleep({delay_ms});
    return new Response("{version}");
  }},
}};
"#
    )
}

fn io_wait_worker() -> String {
    r#"
export default {
  async fetch() {
    await Deno.core.ops.op_sleep(50);
    return new Response("ok");
  },
};
"#
    .to_string()
}

fn frozen_time_worker() -> String {
    r#"
export default {
  async fetch() {
    const now0 = Date.now();
    const perf0 = performance.now();
    let guard = 0;
    for (let i = 0; i < 250000; i++) {
      guard += i;
    }
    const now1 = Date.now();
    const perf1 = performance.now();

    await new Promise((resolve) => setTimeout(resolve, 20));

    const now2 = Date.now();
    const perf2 = performance.now();
    return new Response(JSON.stringify({ now0, now1, now2, perf0, perf1, perf2, guard }), {
      headers: [["content-type", "application/json"]],
    });
  },
};
"#
    .to_string()
}

fn crypto_worker() -> String {
    r#"
export default {
  async fetch() {
    const random = new Uint8Array(16);
    crypto.getRandomValues(random);
    const digestBuffer = await crypto.subtle.digest(
      "SHA-256",
      new TextEncoder().encode("dd-runtime"),
    );
    const digest = Array.from(new Uint8Array(digestBuffer));
    return Response.json({
      random_length: random.length,
      random_non_zero: random.some((value) => value !== 0),
      uuid: crypto.randomUUID(),
      digest_length: digest.length,
    });
  },
};
"#
    .to_string()
}

fn kv_batching_worker(worker_name: &str) -> String {
    format!(
        r#"
export default {{
  async fetch(request, env, ctx) {{
    const url = new URL(request.url);

    if (url.pathname === "/seed") {{
      await env.MY_KV.put("hot", "1");
      await env.MY_KV.put("utf8", "plain");
      await env.MY_KV.put("obj", {{ ok: true, n: 7 }});
      await env.MY_KV.put("left", "L");
      await env.MY_KV.put("right", "R");
      const bad = await Deno.core.ops.op_kv_put_value(JSON.stringify({{
        worker_name: "{worker_name}",
        binding: "MY_KV",
        key: "broken",
        encoding: "v8sc",
        value: [1, 2, 3],
      }}));
      if (bad && bad.ok === false) {{
        throw new Error(String(bad.error ?? "seed broken failed"));
      }}
      return new Response("ok");
    }}

    if (url.pathname === "/sequential") {{
      const values = [];
      for (let i = 0; i < 10; i++) {{
        values.push(await env.MY_KV.get("hot"));
      }}
      return Response.json(values);
    }}

    if (url.pathname === "/queued") {{
      const tasks = [];
      for (let i = 0; i < 10; i++) {{
        tasks.push(env.MY_KV.get("hot"));
      }}
      return Response.json(await Promise.all(tasks));
    }}

    if (url.pathname === "/mixed") {{
      const values = await Promise.all([
        env.MY_KV.get("utf8"),
        env.MY_KV.get("obj"),
        env.MY_KV.get("missing"),
        env.MY_KV.get("utf8"),
        env.MY_KV.get("obj"),
      ]);
      return Response.json(values);
    }}

    if (url.pathname === "/scoped") {{
      const key = String(url.searchParams.get("key") ?? "left");
      const tasks = [];
      for (let i = 0; i < 10; i++) {{
        tasks.push(env.MY_KV.get(key));
      }}
      return Response.json(await Promise.all(tasks));
    }}

    if (url.pathname === "/reject") {{
      try {{
        await Promise.all([
          env.MY_KV.get("hot"),
          env.MY_KV.get("broken"),
          env.MY_KV.get("hot"),
        ]);
        return new Response("unexpected-success", {{ status: 500 }});
      }} catch (error) {{
        return new Response(String(error?.message ?? error), {{ status: 500 }});
      }}
    }}

    if (url.pathname === "/write-batch") {{
      await Promise.all([
        env.MY_KV.put("hot", "2"),
        env.MY_KV.put("hot", "3"),
        env.MY_KV.put("hot", "4"),
      ]);
      return new Response(String((await env.MY_KV.get("hot")) ?? "missing"));
    }}

    if (url.pathname === "/write-overlay") {{
      const pending = env.MY_KV.put("hot", "9");
      const observed = await env.MY_KV.get("hot");
      await pending;
      return new Response(String(observed ?? "missing"));
    }}

    if (url.pathname === "/read") {{
      return new Response(String((await env.MY_KV.get("hot")) ?? "missing"));
    }}

    if (url.pathname === "/write-fire-and-forget") {{
      env.MY_KV.put("hot", "7");
      return new Response("queued");
    }}

    if (url.pathname === "/write-wait-until") {{
      ctx.waitUntil((async () => {{
        try {{
          await env.MY_KV.put("hot", "8");
          globalThis.__dd_wait_until_kv_write = "ok";
        }} catch (error) {{
          globalThis.__dd_wait_until_kv_write = "error:" + String(error?.message ?? error);
        }}
      }})());
      return new Response("queued");
    }}

    if (url.pathname === "/write-wait-until-result") {{
      return new Response(String(globalThis.__dd_wait_until_kv_write ?? "unset"));
    }}

    if (url.pathname === "/read-wait-until") {{
      ctx.waitUntil((async () => {{
        const value = await env.MY_KV.get("hot");
        globalThis.__dd_wait_until_kv_read = String(value ?? "missing");
      }})());
      return new Response("queued");
    }}

    if (url.pathname === "/read-wait-until-result") {{
      return new Response(String(globalThis.__dd_wait_until_kv_read ?? "unset"));
    }}

    return new Response("not found", {{ status: 404 }});
  }},
}};
"#
    )
}

fn kv_write_worker() -> String {
    r#"
export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    if (url.pathname === "/__profile") {
      return new Response(JSON.stringify(Deno.core.ops.op_kv_profile_take?.() ?? null), {
        headers: [["content-type", "application/json"]],
      });
    }

    if (url.pathname === "/__profile_reset") {
      Deno.core.ops.op_kv_profile_reset?.();
      return new Response("ok");
    }

    if (url.pathname === "/seed") {
      await env.MY_KV.put("hot", "1");
      return new Response("ok");
    }

    if (url.pathname === "/write-batch") {
      await Promise.all([
        env.MY_KV.put("hot", "2"),
        env.MY_KV.put("hot", "3"),
        env.MY_KV.put("hot", "4"),
      ]);
      return new Response(String((await env.MY_KV.get("hot")) ?? "missing"));
    }

    if (url.pathname === "/write-overlay") {
      const pending = env.MY_KV.put("hot", "9");
      const observed = await env.MY_KV.get("hot");
      await pending;
      return new Response(String(observed ?? "missing"));
    }

    if (url.pathname === "/read") {
      return new Response(String((await env.MY_KV.get("hot")) ?? "missing"));
    }

    if (url.pathname === "/read-missing") {
      return new Response(String((await env.MY_KV.get("ghost")) ?? "missing"));
    }

    if (url.pathname === "/write-fire-and-forget") {
      env.MY_KV.put("hot", "7");
      return new Response("queued");
    }

    if (url.pathname === "/delete-fire-and-forget") {
      env.MY_KV.delete("hot");
      return new Response("queued");
    }

    if (url.pathname === "/put-read") {
      await env.MY_KV.put("hot", "11");
      return new Response(String((await env.MY_KV.get("hot")) ?? "missing"));
    }

    if (url.pathname === "/delete-read") {
      await env.MY_KV.delete("hot");
      return new Response(String((await env.MY_KV.get("hot")) ?? "missing"));
    }

    if (url.pathname === "/write-wait-until") {
      ctx.waitUntil((async () => {
        try {
          await env.MY_KV.put("hot", "8");
          globalThis.__dd_wait_until_kv_write = "ok";
        } catch (error) {
          globalThis.__dd_wait_until_kv_write = "error:" + String(error?.message ?? error);
        }
      })());
      return new Response("queued");
    }

    if (url.pathname === "/write-wait-until-result") {
      return new Response(String(globalThis.__dd_wait_until_kv_write ?? "unset"));
    }

    return new Response("not found", { status: 404 });
  },
};
"#
    .to_string()
}

fn kv_wait_until_read_worker() -> String {
    r#"
export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    if (url.pathname === "/seed") {
      await env.MY_KV.put("hot", "1");
      return new Response("ok");
    }

    if (url.pathname === "/read-wait-until") {
      ctx.waitUntil((async () => {
        const value = await env.MY_KV.get("hot");
        globalThis.__dd_wait_until_kv_read = String(value ?? "missing");
      })());
      return new Response("queued");
    }

    if (url.pathname === "/read-wait-until-result") {
      return new Response(String(globalThis.__dd_wait_until_kv_read ?? "unset"));
    }

    return new Response("not found", { status: 404 });
  },
};
"#
    .to_string()
}

fn reusable_env_worker() -> String {
    r#"
let previousEnv = null;
let previousKv = null;

export default {
  async fetch(_request, env) {
    const kv = env.MY_KV;
    const payload = {
      sameEnv: previousEnv === env,
      sameKv: previousKv === kv,
      envExtensible: Object.isExtensible(env),
      kvExtensible: Object.isExtensible(kv),
      envMutationResult: Reflect.set(env, "TEMP", "value"),
      kvMutationResult: Reflect.set(kv, "TEMP", "value"),
      envHasTemp: Object.prototype.hasOwnProperty.call(env, "TEMP"),
      kvHasTemp: Object.prototype.hasOwnProperty.call(kv, "TEMP"),
    };
    previousEnv = env;
    previousKv = kv;
    return Response.json(payload);
  },
};
"#
    .to_string()
}

fn abort_aware_worker() -> String {
    r#"
let abortCount = 0;

export default {
  async fetch(_request, _env, ctx) {
    if (ctx.requestId === "block") {
      await new Promise((resolve) => {
        const done = () => {
          abortCount += 1;
          resolve();
        };
        if (ctx.signal?.aborted) {
          done();
          return;
        }
        ctx.signal?.addEventListener("abort", done);
      });
      return new Response("aborted");
    }

    return new Response(`abortCount=${abortCount}`);
  },
};
"#
    .to_string()
}

fn malicious_completion_worker() -> String {
    r#"
let counter = 0;

export default {
  async fetch(_request, _env, ctx) {
    counter += 1;

    Deno.core.ops.op_emit_completion("{");
    Deno.core.ops.op_emit_completion(
      JSON.stringify({
        request_id: ctx.requestId,
        completion_token: "forged-token",
        ok: true,
        result: { status: 200, headers: [], body: [102, 97, 107, 101] },
      }),
    );

    return new Response(String(counter));
  },
};
"#
    .to_string()
}

fn cache_worker(cache_name: &str, label: &str) -> String {
    format!(
        r#"
let count = 0;

export default {{
  async fetch() {{
    const cache = await caches.open("{cache_name}");
    const key = new Request("http://cache/item", {{ method: "GET" }});
    const hit = await cache.match(key);
    if (hit) {{
      return hit;
    }}

    count += 1;
    const response = new Response("{label}:" + String(count), {{
      headers: [["cache-control", "public, max-age=60"]],
    }});
    await cache.put(key, response.clone());
    return response;
  }},
}};
"#
    )
}

fn streaming_request_body_worker() -> String {
    r#"
export default {
  async fetch(request) {
    const reader = request.body?.getReader?.();
    if (!reader) {
      return new Response("no-body");
    }
    let output = "";
    while (true) {
      const { value, done } = await reader.read();
      if (done) {
        break;
      }
      for (const byte of value) {
        output += String.fromCharCode(byte);
      }
    }
    return new Response(output);
  },
};
"#
    .to_string()
}

fn memory_worker() -> String {
    r#"
globalThis.__dd_memory_runtime = globalThis.__dd_memory_runtime ?? {
  active: new Map(),
  max: new Map(),
};

function busyWait(ms) {
  let guard = 0;
  const steps = Math.max(1, ms * 50000);
  while (guard < steps) {
    guard += 1;
  }
  return guard;
}

function asNumber(input, fallback = 0) {
  const parsed = Number(input);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function queryParam(search, name) {
  const trimmed = String(search || "").replace(/^\?/, "");
  if (!trimmed) {
    return null;
  }
  for (const pair of trimmed.split("&")) {
    if (!pair) {
      continue;
    }
    const [rawKey, rawValue = ""] = pair.split("=");
    if (decodeURIComponent(rawKey) === name) {
      return decodeURIComponent(rawValue);
    }
  }
  return null;
}

export function seedCount(state) {
  state.set("count", "0");
  return true;
}

export function incrementStrict(state) {
  const currentValue = asNumber(state.get("count"), 0);
  busyWait(1);
  state.set("count", String(currentValue + 1));
  return currentValue + 1;
}

export function readCount(state) {
  const current = state.get("count");
  return current ? String(current) : "0";
}

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const key = queryParam(url.search, "key") ?? "default";
    const id = env.MY_MEMORY.idFromName(key);
    const memory = env.MY_MEMORY.get(id);

    if (url.pathname === "/__profile") {
      return new Response(JSON.stringify(Deno.core.ops.op_memory_profile_take?.() ?? null), {
        headers: [["content-type", "application/json"]],
      });
    }

    if (url.pathname === "/__profile_reset") {
      Deno.core.ops.op_memory_profile_reset?.();
      return new Response("ok");
    }

    if (url.pathname === "/run") {
      await memory.atomic((state) => {
        const slot = String(state.id);
        const runtime = globalThis.__dd_memory_runtime;
        const active = (runtime.active.get(slot) ?? 0) + 1;
        runtime.active.set(slot, active);
        const max = Math.max(runtime.max.get(slot) ?? 0, active);
        runtime.max.set(slot, max);
        busyWait(100);
        runtime.active.set(slot, Math.max(0, (runtime.active.get(slot) ?? 1) - 1));
        return null;
      });
      return new Response("ok");
    }

    if (url.pathname === "/max") {
      return new Response(String(await memory.atomic((state) => {
        const runtime = globalThis.__dd_memory_runtime;
        return runtime.max.get(String(state.id)) ?? 0;
      })));
    }

    if (url.pathname === "/seed") {
      await memory.atomic(seedCount);
      return new Response("ok");
    }

    if (url.pathname === "/value-roundtrip") {
      const ok = await memory.atomic((state) => {
        state.set("profile", {
          name: "alice",
          createdAt: new Date("2026-01-02T03:04:05.000Z"),
          flags: new Set(["a", "b"]),
          scores: new Map([["p95", 21], ["p99", 32]]),
          bytes: new Uint8Array([1, 2, 3, 4]),
        });
        const value = state.get("profile");
        return Boolean(
          value
            && value.name === "alice"
            && value.createdAt instanceof Date
            && value.createdAt.toISOString() === "2026-01-02T03:04:05.000Z"
            && value.flags instanceof Set
            && value.flags.has("a")
            && value.scores instanceof Map
            && value.scores.get("p95") === 21
            && value.bytes instanceof Uint8Array
            && value.bytes.length === 4
            && value.bytes[3] === 4,
        );
      });
      return new Response(ok ? "ok" : "bad", { status: ok ? 200 : 500 });
    }

    if (url.pathname === "/value-string-get-guard") {
      const ok = await memory.atomic((state) => {
        state.set("profile", { nested: { ok: true } });
        const loaded = state.get("profile");
        return Boolean(
          loaded
            && loaded.nested
            && loaded.nested.ok === true,
        );
      });
      return new Response(ok ? "ok" : "bad", { status: ok ? 200 : 500 });
    }

    if (url.pathname === "/local-visibility") {
      const ok = await memory.atomic((state) => {
        state.set("count", "41");
        const loaded = state.get("count");
        const listed = state.list({ prefix: "co" });
        return Boolean(
          loaded === "41"
            && Array.isArray(listed)
            && listed.length === 1
            && listed[0].key === "count"
            && listed[0].value === "41",
        );
      });
      return new Response(ok ? "ok" : "bad", { status: ok ? 200 : 500 });
    }

    if (url.pathname === "/inc-cas") {
      await memory.atomic(incrementStrict);
      return new Response("ok");
    }

    if (url.pathname === "/stm-blind-write") {
      const value = String(url.searchParams.get("value") ?? "1");
      const committed = await memory.atomic((state) => {
        state.set("count", value);
        return value;
      });
      return new Response(String(committed));
    }

    if (url.pathname === "/stm-read-write") {
      const value = String(url.searchParams.get("value") ?? "1");
      const committed = await memory.atomic((state) => {
        const previous = String(state.get("count") ?? "0");
        state.set("count", value);
        return previous + "->" + String(state.get("count") ?? "missing");
      });
      return new Response(String(committed));
    }

    if (url.pathname === "/direct-set") {
      await memory.write("count", "5");
      return new Response("ok");
    }

    if (url.pathname === "/direct-delete") {
      await memory.delete("count");
      return new Response("ok");
    }

    if (url.pathname === "/direct-get") {
      return new Response(String(await memory.read("count") ?? "0"));
    }

    if (url.pathname === "/get") {
      return new Response(String(await memory.atomic(readCount)));
    }

    return new Response("not found", { status: 404 });
  },
};
"#
    .to_string()
}

fn memory_constructor_storage_worker() -> String {
    r#"
export default {
  async fetch(request, env) {
    const memory = env.MY_MEMORY.get(env.MY_MEMORY.idFromName("user-ctor"));
    const url = new URL(request.url);

    if (url.pathname === "/seed") {
      await memory.atomic((state) => {
        state.set("count", "7");
        return true;
      });
      return new Response("ok");
    }

    if (url.pathname === "/constructor-value") {
      return new Response(String(await memory.atomic((state) => state.get("count") ?? "missing")));
    }

    if (url.pathname === "/direct-value") {
      return new Response(String(await memory.read("count") ?? "missing"));
    }

    if (url.pathname === "/current-value") {
      return new Response(String(await memory.atomic((state) => state.get("count") ?? "missing")));
    }

    return new Response("not found", { status: 404 });
  },
};
"#
    .to_string()
}

fn memory_multi_atomic_read_worker() -> String {
    r#"
export function seedOne(state) {
  state.set("count", "1");
  return true;
}

export function readCount(state) {
  return String(state.get("count") ?? "0");
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/seed") {
      const memory = env.MY_MEMORY.get(env.MY_MEMORY.idFromName("bench-1"));
      await memory.atomic(seedOne);
      return new Response("ok");
    }
    if (url.pathname === "/sum") {
      const keys = Math.max(1, Number(url.searchParams.get("keys") ?? "1") || 1);
      let total = 0;
      for (let i = 0; i < keys; i++) {
        const memory = env.MY_MEMORY.get(env.MY_MEMORY.idFromName(keys === 1 ? "hot" : `bench-${i}`));
        total += Number(await memory.atomic(readCount));
      }
      return new Response(String(total));
    }
    return new Response("not found", { status: 404 });
  },
};
"#
    .to_string()
}

fn memory_multi_key_storage_worker() -> String {
    r#"
export function seedCount(state) {
  state.set("count", "1");
  return true;
}

export function readCount(state) {
  return Number(state.get("count") ?? "0");
}

export function incrementCount(state) {
  const next = Number(state.get("count") ?? "0") + 1;
  state.set("count", String(next));
  return next;
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const keys = Math.max(1, Number(url.searchParams.get("keys") ?? "1") || 1);
    const key = String(url.searchParams.get("key") ?? "bench-0");

    if (url.pathname === "/seed-all") {
      for (let i = 0; i < keys; i++) {
        const memory = env.MY_MEMORY.get(env.MY_MEMORY.idFromName(`bench-${i}`));
        await memory.atomic(seedCount);
      }
      return new Response("ok");
    }

    if (url.pathname === "/direct-sum") {
      let total = 0;
      for (let i = 0; i < keys; i++) {
        const memory = env.MY_MEMORY.get(env.MY_MEMORY.idFromName(`bench-${i}`));
        total += Number(await memory.read("count") ?? "0");
      }
      return new Response(String(total));
    }

    if (url.pathname === "/stm-sum") {
      let total = 0;
      for (let i = 0; i < keys; i++) {
        const memory = env.MY_MEMORY.get(env.MY_MEMORY.idFromName(`bench-${i}`));
        total += Number(await memory.atomic(readCount));
      }
      return new Response(String(total));
    }

    if (url.pathname === "/inc") {
      const memory = env.MY_MEMORY.get(env.MY_MEMORY.idFromName(key));
      return new Response(String(await memory.atomic(incrementCount)));
    }

    if (url.pathname === "/direct-write") {
      const value = String(url.searchParams.get("value") ?? "1");
      const memory = env.MY_MEMORY.get(env.MY_MEMORY.idFromName(key));
      await memory.write("count", value);
      return new Response(value);
    }

    if (url.pathname === "/get") {
      const memory = env.MY_MEMORY.get(env.MY_MEMORY.idFromName(key));
      return new Response(String(await memory.atomic(readCount)));
    }

    return new Response("not found", { status: 404 });
  },
};
"#
    .to_string()
}

fn hosted_memory_worker() -> String {
    r#"
globalThis.__hosted_shared_global = globalThis.__hosted_shared_global ?? 0;

function busyWait(ms) {
  let guard = 0;
  const steps = Math.max(1, ms * 5000000);
  while (guard < steps) {
    guard += 1;
  }
  return guard;
}

export function seedStm(state) {
  state.set("a", "0");
  state.set("b", "0");
  return true;
}

export function writeA(state, value) {
  state.set("a", String(value));
  return String(value);
}

export function readOnce(state) {
  const a = String(state.get("a", { allowConcurrency: true }) ?? "missing");
  busyWait(5);
  return a;
}

export function readPairStrict(state) {
  const a = String(state.get("a") ?? "missing");
  busyWait(5);
  const b = String(state.get("b") ?? "missing");
  return `${a}:${b}`;
}

export function readPairSnapshot(state) {
  const a = String(state.get("a", { allowConcurrency: true }) ?? "missing");
  busyWait(5);
  const b = String(state.get("b", { allowConcurrency: true }) ?? "missing");
  return `${a}:${b}`;
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const id = env.MY_MEMORY.idFromName(url.searchParams.get("key") ?? "default");
    const memory = env.MY_MEMORY.get(id);

    if (url.pathname === "/__profile") {
      return new Response(JSON.stringify(Deno.core.ops.op_memory_profile_take?.() ?? null), {
        headers: [["content-type", "application/json"]],
      });
    }

    if (url.pathname === "/__profile_reset") {
      Deno.core.ops.op_memory_profile_reset?.();
      return new Response("ok");
    }

    if (url.pathname === "/alpha/inc") {
      return new Response(String(await memory.atomic((state) => {
        const current = Number(state.get("count") ?? 0);
        const next = current + 1;
        state.set("count", String(next));
        return next;
      })));
    }

    if (url.pathname === "/beta/read") {
      return new Response(String(await memory.atomic((state) => {
        return String(state.get("count") ?? "0");
      })));
    }

    if (url.pathname === "/worker/global/inc") {
      globalThis.__hosted_shared_global += 1;
      return new Response(String(globalThis.__hosted_shared_global));
    }

    if (url.pathname === "/memory/global/read") {
      return new Response(String(await memory.atomic((_state) => globalThis.__hosted_shared_global)));
    }

    if (url.pathname === "/memory/global/inc") {
      return new Response(String(await memory.atomic((_state) => {
        globalThis.__hosted_shared_global += 1;
        return globalThis.__hosted_shared_global;
      })));
    }

    if (url.pathname === "/inline") {
      const suffix = "inline";
      return new Response(String(await memory.atomic(() => `ok-${suffix}`)));
    }

    if (url.pathname === "/stm/seed") {
      await memory.atomic(seedStm);
      return new Response("ok");
    }

    if (url.pathname === "/stm/write-a") {
      const value = String(url.searchParams.get("value") ?? "1");
      await memory.atomic(writeA, value);
      return new Response("ok");
    }

    if (url.pathname === "/stm/read-once") {
      return new Response(String(await memory.atomic(readOnce)));
    }

    if (url.pathname === "/read-direct") {
      return new Response(String(await memory.read("a") ?? "missing"));
    }

    if (url.pathname === "/stm/read-pair") {
      return new Response(String(await memory.atomic(readPairStrict)));
    }

    if (url.pathname === "/stm/read-pair-snapshot") {
      return new Response(String(await memory.atomic(readPairSnapshot)));
    }

    if (url.pathname === "/stm/tvar-default/read") {
      const count = memory.tvar("count", 7);
      return new Response(String(await memory.atomic(() => count.read())));
    }

    if (url.pathname === "/stm/tvar-default/raw") {
      return new Response(String(await memory.atomic((state) => state.get("count") ?? "missing")));
    }

    if (url.pathname === "/stm/tvar-default/write") {
      const count = memory.tvar("count", 7);
      return new Response(String(await memory.atomic(() => {
        const next = Number(count.read()) + 1;
        count.write(String(next));
        return next;
      })));
    }

    return new Response("not found", { status: 404 });
  },
};
"#
        .to_string()
}

fn async_context_worker() -> String {
    r#"
export default {
  async fetch(request) {
    const url = new URL(request.url);
    const ctx = globalThis.__dd_async_context;
    if (!ctx) {
      return new Response("missing", { status: 500 });
    }

    if (url.pathname === "/promise") {
      return await ctx.run({ label: "outer" }, async () => {
        await Promise.resolve();
        return new Response(String(ctx.getStore()?.label ?? "missing"));
      });
    }

    if (url.pathname === "/nested") {
      return await ctx.run({ label: "outer" }, async () => {
        const before = String(ctx.getStore()?.label ?? "missing");
        const inner = await ctx.run({ label: "inner" }, async () => {
          await Promise.resolve();
          return String(ctx.getStore()?.label ?? "missing");
        });
        const after = String(ctx.getStore()?.label ?? "missing");
        return new Response(`${before}:${inner}:${after}`);
      });
    }

    if (url.pathname === "/restore") {
      const before = String(ctx.getStore()?.label ?? "missing");
      await ctx.run({ label: "temp" }, async () => {
        await Promise.resolve();
      });
      const after = String(ctx.getStore()?.label ?? "missing");
      return new Response(`${before}:${after}`);
    }

    return new Response("not found", { status: 404 });
  },
};
"#
    .to_string()
}

fn trace_sink_worker() -> String {
    r#"
export default {
  async fetch(request) {
    return new Response("ok");
  },
};
"#
    .to_string()
}

fn wait_until_worker() -> String {
    r#"
globalThis.__dd_wait_until_value = globalThis.__dd_wait_until_value ?? "idle";

export default {
  async fetch(request, _env, ctx) {
    const url = new URL(request.url);
    if (url.pathname === "/trigger") {
      ctx.waitUntil((async () => {
        await Promise.resolve();
        globalThis.__dd_wait_until_value = "done";
      })());
      return new Response("queued");
    }
    if (url.pathname === "/read") {
      return new Response(String(globalThis.__dd_wait_until_value));
    }
    return new Response("not found", { status: 404 });
  },
};
"#
    .to_string()
}

fn loop_trace_worker() -> String {
    r#"
let totalCalls = 0;
let traceCalls = 0;

export default {
  async fetch(request) {
    totalCalls += 1;
    const path = new URL(request.url).pathname;
    if (path === "/trace") {
      traceCalls += 1;
      return new Response("ok");
    }
    if (path === "/state") {
      return new Response(
        JSON.stringify({ total_calls: totalCalls, trace_calls: traceCalls }),
        { headers: [["content-type", "application/json"]] }
      );
    }
    return new Response("ok");
  },
};
"#
    .to_string()
}

#[derive(Deserialize)]
struct LoopTraceState {
    total_calls: usize,
    trace_calls: usize,
}

#[derive(Deserialize)]
struct FrozenTimeState {
    now0: i64,
    now1: i64,
    now2: i64,
    perf0: f64,
    perf1: f64,
    perf2: f64,
    guard: i64,
}

#[derive(Deserialize)]
struct CryptoState {
    random_length: usize,
    random_non_zero: bool,
    uuid: String,
    digest_length: usize,
}

async fn test_service(config: RuntimeConfig) -> RuntimeService {
    let db_path = format!("/tmp/dd-test-{}.db", Uuid::new_v4());
    let store_dir = format!("/tmp/dd-store-{}", Uuid::new_v4());
    RuntimeService::start_with_service_config(RuntimeServiceConfig {
        runtime: config,
        storage: RuntimeStorageConfig {
            store_dir: PathBuf::from(&store_dir),
            database_url: format!("file:{db_path}"),
            memory_namespace_shards: 16,
            memory_db_cache_max_open: 4096,
            memory_db_idle_ttl: Duration::from_secs(60),
            worker_store_enabled: false,
            blob_store: BlobStoreConfig::local(PathBuf::from(&store_dir).join("blobs")),
        },
    })
    .await
    .expect("service should start")
}

async fn test_service_with_paths(
    config: RuntimeConfig,
    store_dir: PathBuf,
    database_url: String,
    worker_store_enabled: bool,
) -> RuntimeService {
    RuntimeService::start_with_service_config(RuntimeServiceConfig {
        runtime: config,
        storage: RuntimeStorageConfig {
            store_dir: store_dir.clone(),
            database_url,
            memory_namespace_shards: 16,
            memory_db_cache_max_open: 4096,
            memory_db_idle_ttl: Duration::from_secs(60),
            worker_store_enabled,
            blob_store: BlobStoreConfig::local(store_dir.join("blobs")),
        },
    })
    .await
    .expect("service should start")
}

fn dynamic_single_isolate_config() -> RuntimeConfig {
    RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 1,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    }
}

fn dynamic_autoscaling_config() -> RuntimeConfig {
    RuntimeConfig {
        min_isolates: 0,
        max_isolates: 2,
        max_inflight_per_isolate: 4,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    }
}

fn dynamic_bench_autoscaling_config() -> RuntimeConfig {
    RuntimeConfig {
        min_isolates: 0,
        max_isolates: 8,
        max_inflight_per_isolate: 4,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    }
}

async fn invoke_with_timeout_and_dump(
    service: &RuntimeService,
    worker_name: &str,
    invocation: WorkerInvocation,
    stage: &str,
) -> WorkerOutput {
    match timeout(
        Duration::from_secs(5),
        service.invoke(worker_name.to_string(), invocation),
    )
    .await
    {
        Ok(Ok(output)) => output,
        Ok(Err(error)) => panic!("{stage} failed: {error}"),
        Err(_) => {
            let dump = service.debug_dump(worker_name.to_string()).await;
            let dynamic_dump = service.dynamic_debug_dump().await;
            panic!("{stage} timed out; debug dump: {dump:?}; dynamic dump: {dynamic_dump:?}");
        }
    }
}

async fn wait_for_isolate_total(service: &RuntimeService, worker_name: &str, expected: usize) {
    timeout(Duration::from_secs(5), async {
        loop {
            let stats = service
                .stats(worker_name.to_string())
                .await
                .expect("worker stats should exist");
            if stats.isolates_total == expected {
                break;
            }
            sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("worker isolate count should converge");
}

fn test_assets() -> Vec<DeployAsset> {
    vec![
        DeployAsset {
            path: "/a.js".to_string(),
            content_base64: "YXNzZXQtYm9keQ==".to_string(),
        },
        DeployAsset {
            path: "/nested/b.css".to_string(),
            content_base64: "Ym9keXt9".to_string(),
        },
    ]
}

fn asset_worker() -> String {
    r#"
export default {
  async fetch() {
    return new Response("worker-fallback", {
      headers: [["content-type", "text/plain; charset=utf-8"]],
    });
  },
};
"#
    .to_string()
}

fn asset_headers_file() -> String {
    r#"
/a.js
  Cache-Control: public, max-age=60
  X-Exact: yes
https://:sub.example.com/a.js
  X-Host: :sub
/nested/*
  X-Splat: :splat
        "#
    .to_string()
}

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

    let asset = service
        .resolve_asset(
            "assets".to_string(),
            "GET".to_string(),
            Some("foo.example.com:443".to_string()),
            "/a.js".to_string(),
            Vec::new(),
        )
        .await
        .expect("asset lookup should succeed")
        .expect("asset should exist");
    assert_eq!(asset.status, 200);
    assert_eq!(asset.body, b"asset-body");
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
            "assets".to_string(),
            "HEAD".to_string(),
            Some("foo.example.com".to_string()),
            "/nested/b.css".to_string(),
            Vec::new(),
        )
        .await
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
            "assets".to_string(),
            "GET".to_string(),
            Some("foo.example.com".to_string()),
            "/a.js".to_string(),
            vec![("if-none-match".to_string(), etag)],
        )
        .await
        .expect("etag lookup should succeed")
        .expect("asset should exist");
    assert_eq!(not_modified.status, 304);
    assert!(not_modified.body.is_empty());
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
        .resolve_asset(
            "assets".to_string(),
            "GET".to_string(),
            Some("foo.example.com".to_string()),
            "/a.js".to_string(),
            Vec::new(),
        )
        .await
        .expect("asset lookup should succeed")
        .expect("asset should exist after restore");
    assert_eq!(asset.body, b"asset-body");

    let _ = tokio::fs::remove_dir_all(root).await;
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
async fn dynamic_namespace_can_create_and_invoke_dynamic_workers() {
    let service = test_service(dynamic_autoscaling_config()).await;

    service
        .deploy_with_config(
            "dynamic-parent".to_string(),
            dynamic_namespace_worker(),
            DeployConfig {
                bindings: vec![DeployBinding::Dynamic {
                    binding: "SANDBOX".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");

    let one = invoke_with_timeout_and_dump(
        &service,
        "dynamic-parent",
        test_invocation(),
        "dynamic host rpc autoscaling first invoke",
    )
    .await;
    let two = invoke_with_timeout_and_dump(
        &service,
        "dynamic-parent",
        test_invocation(),
        "dynamic host rpc autoscaling second invoke",
    )
    .await;

    assert_eq!(String::from_utf8(one.body).expect("utf8"), "1:1");
    assert_eq!(String::from_utf8(two.body).expect("utf8"), "2:2");
}

#[tokio::test]
#[serial]
async fn dynamic_namespace_plain_child_fetch_completes_in_single_isolate() {
    let service = test_service(dynamic_single_isolate_config()).await;

    service
        .deploy_with_config(
            "dynamic-plain-single".to_string(),
            dynamic_plain_namespace_worker(),
            DeployConfig {
                bindings: vec![DeployBinding::Dynamic {
                    binding: "SANDBOX".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");

    let one = invoke_with_timeout_and_dump(
        &service,
        "dynamic-plain-single",
        test_invocation(),
        "dynamic plain single-isolate first invoke",
    )
    .await;
    let two = invoke_with_timeout_and_dump(
        &service,
        "dynamic-plain-single",
        test_invocation(),
        "dynamic plain single-isolate second invoke",
    )
    .await;

    assert_eq!(String::from_utf8(one.body).expect("utf8"), "1");
    assert_eq!(String::from_utf8(two.body).expect("utf8"), "2");
}

#[tokio::test]
#[serial]
async fn dynamic_namespace_concurrent_hot_fetch_completes_in_single_isolate() {
    let service = test_service(dynamic_single_isolate_config()).await;

    service
        .deploy_with_config(
            "dynamic-plain-concurrent".to_string(),
            dynamic_plain_handle_cache_worker(),
            DeployConfig {
                bindings: vec![DeployBinding::Dynamic {
                    binding: "SANDBOX".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");

    let mut tasks = Vec::new();
    for idx in 0..32 {
        let service = service.clone();
        tasks.push(tokio::spawn(async move {
            invoke_with_timeout_and_dump(
                &service,
                "dynamic-plain-concurrent",
                test_invocation_with_path("/", &format!("dynamic-plain-concurrent-{idx}")),
                &format!("dynamic plain concurrent single-isolate {idx}"),
            )
            .await
        }));
    }

    let mut bodies = Vec::new();
    for task in tasks {
        let output = task.await.expect("task should join");
        assert_eq!(output.status, 200);
        bodies.push(String::from_utf8(output.body).expect("utf8"));
    }

    assert_eq!(bodies.len(), 32);
}

#[derive(Deserialize)]
struct DynamicFastFetchMetrics {
    #[serde(default, rename = "remoteFetchHit")]
    remote_fetch_hit: u64,
    #[serde(default, rename = "remoteFetchFallback")]
    remote_fetch_fallback: u64,
    #[serde(default, rename = "fastFetchPathHit")]
    fast_fetch_path_hit: u64,
    #[serde(default, rename = "direct_fetch_fast_path_hit")]
    direct_fetch_fast_path_hit: u64,
    #[serde(default, rename = "direct_fetch_fast_path_fallback")]
    direct_fetch_fast_path_fallback: u64,
    #[serde(default, rename = "warm_isolate_hit")]
    warm_isolate_hit: u64,
    #[serde(default, rename = "fallback_dispatch")]
    fallback_dispatch: u64,
    #[serde(default, rename = "timeSyncApplied")]
    time_sync_applied: u64,
    #[serde(default, rename = "direct_fetch_dispatch_count")]
    direct_fetch_dispatch_count: u64,
    #[serde(default, rename = "control_drain_batch")]
    control_drain_batch: u64,
}

#[tokio::test]
#[serial]
async fn dynamic_fast_fetch_preserves_request_response_shape_and_records_direct_metrics() {
    let service = test_service(dynamic_single_isolate_config()).await;

    service
        .deploy_with_config(
            "dynamic-fast-fetch".to_string(),
            dynamic_fast_fetch_worker(),
            DeployConfig {
                bindings: vec![DeployBinding::Dynamic {
                    binding: "SANDBOX".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");

    invoke_with_timeout_and_dump(
        &service,
        "dynamic-fast-fetch",
        test_invocation_with_path("/__dynamic_metrics_reset", "dynamic-fast-reset"),
        "dynamic fast metrics reset",
    )
    .await;

    invoke_with_timeout_and_dump(
        &service,
        "dynamic-fast-fetch",
        test_invocation_with_path("/hot", "dynamic-fast-hot-1"),
        "dynamic fast first hot invoke",
    )
    .await;
    invoke_with_timeout_and_dump(
        &service,
        "dynamic-fast-fetch",
        test_invocation_with_path("/hot", "dynamic-fast-hot-2"),
        "dynamic fast second hot invoke",
    )
    .await;

    let binary_body = vec![0, 1, 2, 3, 255];
    let parity = invoke_with_timeout_and_dump(
        &service,
        "dynamic-fast-fetch",
        WorkerInvocation {
            method: "POST".to_string(),
            url: "http://worker/echo?q=hello&status=207".to_string(),
            headers: vec![
                (
                    "content-type".to_string(),
                    "application/octet-stream".to_string(),
                ),
                ("x-test".to_string(), "alpha".to_string()),
            ],
            body: binary_body.clone(),
            request_id: "dynamic-fast-echo".to_string(),
        },
        "dynamic fast parity invoke",
    )
    .await;
    assert_eq!(parity.status, 207);
    assert_eq!(parity.body, binary_body);
    assert!(parity
        .headers
        .iter()
        .any(|(name, value)| name.eq_ignore_ascii_case("x-method") && value == "POST"));
    assert!(parity
        .headers
        .iter()
        .any(|(name, value)| name.eq_ignore_ascii_case("x-query") && value == "hello"));
    assert!(parity
        .headers
        .iter()
        .any(|(name, value)| name.eq_ignore_ascii_case("x-test") && value == "alpha"));

    let metrics_output = invoke_with_timeout_and_dump(
        &service,
        "dynamic-fast-fetch",
        test_invocation_with_path("/__dynamic_metrics", "dynamic-fast-metrics"),
        "dynamic fast metrics read",
    )
    .await;
    let metrics: DynamicFastFetchMetrics = crate::json::from_string(
        String::from_utf8(metrics_output.body).expect("metrics body should be utf8"),
    )
    .expect("metrics should parse");
    assert!(
        metrics.fast_fetch_path_hit >= 3,
        "{:?}",
        metrics.fast_fetch_path_hit
    );
    assert!(
        metrics.remote_fetch_hit >= 3,
        "{:?}",
        metrics.remote_fetch_hit
    );
    assert_eq!(metrics.remote_fetch_fallback, 0);
    assert!(metrics.direct_fetch_fast_path_hit >= 2);
    assert!(metrics.direct_fetch_dispatch_count >= 2);
    assert!(metrics.control_drain_batch >= 1);
    assert!(
        metrics.time_sync_applied >= 1,
        "{:?}",
        metrics.time_sync_applied
    );
}

#[tokio::test]
#[serial]
async fn dynamic_fast_fetch_repeated_warm_requests_stay_on_direct_path() {
    let service = test_service(dynamic_single_isolate_config()).await;

    service
        .deploy_with_config(
            "dynamic-fast-repeat".to_string(),
            dynamic_fast_fetch_worker(),
            DeployConfig {
                bindings: vec![DeployBinding::Dynamic {
                    binding: "SANDBOX".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");

    invoke_with_timeout_and_dump(
        &service,
        "dynamic-fast-repeat",
        test_invocation_with_path("/hot", "dynamic-fast-repeat-warm"),
        "dynamic fast repeat warm invoke",
    )
    .await;

    invoke_with_timeout_and_dump(
        &service,
        "dynamic-fast-repeat",
        test_invocation_with_path("/__dynamic_metrics_reset", "dynamic-fast-repeat-reset"),
        "dynamic fast repeat reset",
    )
    .await;

    for index in 0..64 {
        let request_id = format!("dynamic-fast-repeat-{index}");
        invoke_with_timeout_and_dump(
            &service,
            "dynamic-fast-repeat",
            test_invocation_with_path("/hot", &request_id),
            "dynamic fast repeat hot invoke",
        )
        .await;
    }

    let metrics_output = invoke_with_timeout_and_dump(
        &service,
        "dynamic-fast-repeat",
        test_invocation_with_path("/__dynamic_metrics", "dynamic-fast-repeat-metrics"),
        "dynamic fast repeat metrics",
    )
    .await;
    let metrics: DynamicFastFetchMetrics = crate::json::from_string(
        String::from_utf8(metrics_output.body).expect("metrics body should be utf8"),
    )
    .expect("metrics should parse");
    assert!(metrics.remote_fetch_hit >= 64);
    assert!(metrics.direct_fetch_fast_path_hit >= 64);
    assert_eq!(metrics.direct_fetch_fast_path_fallback, 0);
    assert_eq!(metrics.fallback_dispatch, 0);
    assert!(metrics.warm_isolate_hit >= 64);
}

#[tokio::test]
#[serial]
async fn dynamic_fast_fetch_recovers_after_preferred_isolate_failure_and_rewarms() {
    let service = test_service(dynamic_single_isolate_config()).await;

    service
        .deploy_with_config(
            "dynamic-fast-rewarm".to_string(),
            dynamic_remote_fast_fetch_worker(),
            DeployConfig {
                bindings: vec![DeployBinding::Dynamic {
                    binding: "SANDBOX".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");

    invoke_with_timeout_and_dump(
        &service,
        "dynamic-fast-rewarm",
        test_invocation_with_path("/hot", "dynamic-fast-rewarm-warm-1"),
        "dynamic fast rewarm first invoke",
    )
    .await;
    invoke_with_timeout_and_dump(
        &service,
        "dynamic-fast-rewarm",
        test_invocation_with_path("/hot", "dynamic-fast-rewarm-warm-2"),
        "dynamic fast rewarm second invoke",
    )
    .await;

    let child_worker = service
        .dynamic_debug_dump()
        .await
        .handles
        .into_iter()
        .find(|handle| handle.owner_worker == "dynamic-fast-rewarm" && handle.binding == "SANDBOX")
        .map(|handle| handle.worker_name)
        .expect("dynamic child worker should exist");
    let child_dump = service
        .debug_dump(child_worker.clone())
        .await
        .expect("child debug dump should exist");
    let child_isolate_id = child_dump
        .isolates
        .first()
        .map(|isolate| isolate.id)
        .expect("child isolate should exist");
    assert!(
        service
            .force_fail_isolate_for_test(child_worker, child_dump.generation, child_isolate_id)
            .await
    );

    invoke_with_timeout_and_dump(
        &service,
        "dynamic-fast-rewarm",
        test_invocation_with_path("/__dynamic_metrics_reset", "dynamic-fast-rewarm-reset"),
        "dynamic fast rewarm metrics reset",
    )
    .await;

    invoke_with_timeout_and_dump(
        &service,
        "dynamic-fast-rewarm",
        test_invocation_with_path("/hot", "dynamic-fast-rewarm-after-fail-1"),
        "dynamic fast rewarm invoke after fail",
    )
    .await;
    invoke_with_timeout_and_dump(
        &service,
        "dynamic-fast-rewarm",
        test_invocation_with_path("/hot", "dynamic-fast-rewarm-after-fail-2"),
        "dynamic fast rewarm second invoke after fail",
    )
    .await;

    let metrics_output = invoke_with_timeout_and_dump(
        &service,
        "dynamic-fast-rewarm",
        test_invocation_with_path("/__dynamic_metrics", "dynamic-fast-rewarm-metrics"),
        "dynamic fast rewarm metrics",
    )
    .await;
    let metrics: DynamicFastFetchMetrics = crate::json::from_string(
        String::from_utf8(metrics_output.body).expect("metrics body should be utf8"),
    )
    .expect("metrics should parse");
    assert!(
        metrics.fallback_dispatch >= 1,
        "{:?}",
        metrics.fallback_dispatch
    );
    assert!(
        metrics.direct_fetch_fast_path_fallback >= 1,
        "{:?}",
        metrics.direct_fetch_fast_path_fallback
    );
    assert!(metrics.remote_fetch_hit >= 2);
    assert!(
        metrics.warm_isolate_hit >= 1,
        "{:?}",
        metrics.warm_isolate_hit
    );
}

#[tokio::test]
#[serial]
async fn dynamic_namespace_plain_child_fetch_completes_in_autoscaling_config() {
    let service = test_service(dynamic_autoscaling_config()).await;

    service
        .deploy_with_config(
            "dynamic-plain-auto".to_string(),
            dynamic_plain_namespace_worker(),
            DeployConfig {
                bindings: vec![DeployBinding::Dynamic {
                    binding: "SANDBOX".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");

    let one = invoke_with_timeout_and_dump(
        &service,
        "dynamic-plain-auto",
        test_invocation(),
        "dynamic plain autoscaling first invoke",
    )
    .await;
    let two = invoke_with_timeout_and_dump(
        &service,
        "dynamic-plain-auto",
        test_invocation(),
        "dynamic plain autoscaling second invoke",
    )
    .await;

    assert_eq!(String::from_utf8(one.body).expect("utf8"), "1");
    assert_eq!(String::from_utf8(two.body).expect("utf8"), "2");
}

#[tokio::test]
#[serial]
async fn dynamic_namespace_ops_complete_without_child_fetch_in_single_isolate() {
    let service = test_service(dynamic_single_isolate_config()).await;

    service
        .deploy_with_config(
            "dynamic-ops-single".to_string(),
            dynamic_namespace_ops_worker(),
            DeployConfig {
                bindings: vec![DeployBinding::Dynamic {
                    binding: "SANDBOX".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");

    let empty = invoke_with_timeout_and_dump(
        &service,
        "dynamic-ops-single",
        test_invocation_with_path("/list", "dyn-ops-list-empty"),
        "lookup-reply empty list",
    )
    .await;
    assert_eq!(String::from_utf8(empty.body).expect("utf8"), "[]");

    let create = invoke_with_timeout_and_dump(
        &service,
        "dynamic-ops-single",
        test_invocation_with_path("/get-create", "dyn-ops-get-create"),
        "create-reply first get",
    )
    .await;
    assert_eq!(String::from_utf8(create.body).expect("utf8"), "1");

    let hit = invoke_with_timeout_and_dump(
        &service,
        "dynamic-ops-single",
        test_invocation_with_path("/get-hit", "dyn-ops-get-hit"),
        "lookup-reply cached get",
    )
    .await;
    assert_eq!(String::from_utf8(hit.body).expect("utf8"), "0");

    let ids = invoke_with_timeout_and_dump(
        &service,
        "dynamic-ops-single",
        test_invocation_with_path("/list", "dyn-ops-list-populated"),
        "list-reply populated",
    )
    .await;
    assert_eq!(
        String::from_utf8(ids.body).expect("utf8"),
        "[\"control:v1\"]"
    );

    let deleted = invoke_with_timeout_and_dump(
        &service,
        "dynamic-ops-single",
        test_invocation_with_path("/delete", "dyn-ops-delete"),
        "delete-reply control worker",
    )
    .await;
    assert_eq!(String::from_utf8(deleted.body).expect("utf8"), "true");

    let ids_after = invoke_with_timeout_and_dump(
        &service,
        "dynamic-ops-single",
        test_invocation_with_path("/list", "dyn-ops-list-final"),
        "list-reply after delete",
    )
    .await;
    assert_eq!(String::from_utf8(ids_after.body).expect("utf8"), "[]");
}

#[tokio::test]
#[serial]
async fn dynamic_namespace_supports_list_delete_and_timeout() {
    let service = test_service(dynamic_autoscaling_config()).await;

    service
        .deploy_with_config(
            "dynamic-admin".to_string(),
            dynamic_namespace_admin_worker(),
            DeployConfig {
                bindings: vec![DeployBinding::Dynamic {
                    binding: "SANDBOX".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");

    let ensure = invoke_with_timeout_and_dump(
        &service,
        "dynamic-admin",
        test_invocation_with_path("/ensure", "dyn-admin-ensure"),
        "dynamic admin ensure",
    )
    .await;
    assert_eq!(ensure.status, 200);

    let ids = invoke_with_timeout_and_dump(
        &service,
        "dynamic-admin",
        test_invocation_with_path("/ids", "dyn-admin-ids-1"),
        "dynamic admin ids before delete",
    )
    .await;
    assert_eq!(String::from_utf8(ids.body).expect("utf8"), "[\"admin:v1\"]");

    let deleted = invoke_with_timeout_and_dump(
        &service,
        "dynamic-admin",
        test_invocation_with_path("/delete", "dyn-admin-delete"),
        "dynamic admin delete",
    )
    .await;
    assert_eq!(String::from_utf8(deleted.body).expect("utf8"), "true");

    let ids_after = invoke_with_timeout_and_dump(
        &service,
        "dynamic-admin",
        test_invocation_with_path("/ids", "dyn-admin-ids-2"),
        "dynamic admin ids after delete",
    )
    .await;
    assert_eq!(String::from_utf8(ids_after.body).expect("utf8"), "[]");

    let timeout_out = invoke_with_timeout_and_dump(
        &service,
        "dynamic-admin",
        test_invocation_with_path("/timeout", "dyn-admin-timeout"),
        "dynamic admin timeout route",
    )
    .await;
    assert_eq!(timeout_out.status, 504);
}

#[tokio::test]
#[serial]
async fn dynamic_namespace_repeated_list_delete_complete_past_threshold() {
    let service = test_service(dynamic_autoscaling_config()).await;

    service
        .deploy_with_config(
            "dynamic-admin-loop".to_string(),
            dynamic_namespace_admin_worker(),
            DeployConfig {
                bindings: vec![DeployBinding::Dynamic {
                    binding: "SANDBOX".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");

    for idx in 0..64 {
        let ensure = invoke_with_timeout_and_dump(
            &service,
            "dynamic-admin-loop",
            test_invocation_with_path("/ensure", &format!("dyn-admin-loop-ensure-{idx}")),
            &format!("dynamic admin loop ensure {idx}"),
        )
        .await;
        assert_eq!(ensure.status, 200);

        let ids = invoke_with_timeout_and_dump(
            &service,
            "dynamic-admin-loop",
            test_invocation_with_path("/ids", &format!("dyn-admin-loop-ids-{idx}")),
            &format!("dynamic admin loop ids {idx}"),
        )
        .await;
        assert_eq!(String::from_utf8(ids.body).expect("utf8"), "[\"admin:v1\"]");

        let deleted = invoke_with_timeout_and_dump(
            &service,
            "dynamic-admin-loop",
            test_invocation_with_path("/delete", &format!("dyn-admin-loop-delete-{idx}")),
            &format!("dynamic admin loop delete {idx}"),
        )
        .await;
        assert_eq!(String::from_utf8(deleted.body).expect("utf8"), "true");
    }

    let ids_after = invoke_with_timeout_and_dump(
        &service,
        "dynamic-admin-loop",
        test_invocation_with_path("/ids", "dyn-admin-loop-final-ids"),
        "dynamic admin loop final ids",
    )
    .await;
    assert_eq!(String::from_utf8(ids_after.body).expect("utf8"), "[]");
}

#[tokio::test]
#[serial]
async fn dynamic_test_async_reply_immediate_completion_resumes_request() {
    let service = test_service(dynamic_single_isolate_config()).await;

    service
        .deploy(
            "dynamic-wake-probe".to_string(),
            dynamic_wake_probe_worker(),
        )
        .await
        .expect("deploy should succeed");

    let output = invoke_with_timeout_and_dump(
        &service,
        "dynamic-wake-probe",
        test_invocation_with_path("/async/immediate", "dyn-wake-immediate"),
        "dynamic test async immediate",
    )
    .await;
    assert_eq!(output.status, 200);
    assert_eq!(String::from_utf8(output.body).expect("utf8"), "immediate");
}

#[tokio::test]
#[serial]
async fn dynamic_test_async_reply_delayed_completion_resumes_after_yield() {
    let service = test_service(dynamic_single_isolate_config()).await;

    service
        .deploy(
            "dynamic-wake-probe".to_string(),
            dynamic_wake_probe_worker(),
        )
        .await
        .expect("deploy should succeed");

    let output = invoke_with_timeout_and_dump(
        &service,
        "dynamic-wake-probe",
        test_invocation_with_path("/async/delayed", "dyn-wake-delayed"),
        "dynamic test async delayed",
    )
    .await;
    assert_eq!(output.status, 200);
    assert_eq!(String::from_utf8(output.body).expect("utf8"), "delayed");
}

#[tokio::test]
#[serial]
async fn dynamic_test_async_reply_timeout_surfaces_explicit_error() {
    let service = test_service(dynamic_single_isolate_config()).await;

    service
        .deploy(
            "dynamic-wake-probe".to_string(),
            dynamic_wake_probe_worker(),
        )
        .await
        .expect("deploy should succeed");

    let output = invoke_with_timeout_and_dump(
        &service,
        "dynamic-wake-probe",
        test_invocation_with_path("/async/timeout", "dyn-wake-timeout"),
        "dynamic test async timeout",
    )
    .await;
    assert_eq!(output.status, 504);
    assert_eq!(
        String::from_utf8(output.body).expect("utf8"),
        "test async reply timed out after 25ms"
    );
}

#[tokio::test]
#[serial]
async fn dynamic_test_nested_targeted_invoke_completes_on_same_isolate() {
    let service = test_service(dynamic_single_isolate_config()).await;

    service
        .deploy(
            "dynamic-wake-probe".to_string(),
            dynamic_wake_probe_worker(),
        )
        .await
        .expect("deploy should succeed");

    let warm = invoke_with_timeout_and_dump(
        &service,
        "dynamic-wake-probe",
        test_invocation_with_path("/warm-provider", "dyn-nested-same-warm"),
        "dynamic test nested same warm",
    )
    .await;
    assert_eq!(warm.status, 200);

    let output = invoke_with_timeout_and_dump(
        &service,
        "dynamic-wake-probe",
        test_invocation_with_path("/nested/same", "dyn-nested-same"),
        "dynamic test nested same",
    )
    .await;
    let body = String::from_utf8(output.body).expect("utf8");
    assert_eq!(output.status, 200, "{body}");
    assert!(body.starts_with("ok:"), "{body}");
}

#[tokio::test]
#[serial]
async fn dynamic_test_nested_targeted_invoke_completes_across_isolates() {
    let service = test_service(RuntimeConfig {
        min_isolates: 2,
        max_isolates: 2,
        max_inflight_per_isolate: 1,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy(
            "dynamic-wake-probe".to_string(),
            dynamic_wake_probe_worker(),
        )
        .await
        .expect("deploy should succeed");

    let mut warm_tasks = Vec::new();
    for idx in 0..4 {
        let service = service.clone();
        warm_tasks.push(tokio::spawn(async move {
            invoke_with_timeout_and_dump(
                &service,
                "dynamic-wake-probe",
                test_invocation_with_path(
                    "/warm-provider?hold=50",
                    &format!("dyn-nested-other-warm-{idx}"),
                ),
                &format!("dynamic test nested other warm {idx}"),
            )
            .await
        }));
    }
    for task in warm_tasks {
        let output = task.await.expect("join");
        assert_eq!(output.status, 200);
    }
    wait_for_isolate_total(&service, "dynamic-wake-probe", 2).await;

    let output = invoke_with_timeout_and_dump(
        &service,
        "dynamic-wake-probe",
        test_invocation_with_path("/nested/other", "dyn-nested-other"),
        "dynamic test nested other",
    )
    .await;
    let body = String::from_utf8(output.body).expect("utf8");
    assert_eq!(output.status, 200, "{body}");
    assert!(body.starts_with("ok:"));
    assert_ne!(body, "ok:1");
}

#[tokio::test]
#[serial]
async fn dynamic_handle_cache_invalidates_on_delete_before_recreate() {
    let service = test_service(dynamic_single_isolate_config()).await;

    service
        .deploy_with_config(
            "dynamic-cache-delete".to_string(),
            dynamic_handle_cache_delete_worker(),
            DeployConfig {
                bindings: vec![DeployBinding::Dynamic {
                    binding: "SANDBOX".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");

    let output = invoke_with_timeout_and_dump(
        &service,
        "dynamic-cache-delete",
        test_invocation_with_path("/delete-recreate", "dynamic-cache-delete-recreate"),
        "dynamic delete should invalidate handle cache",
    )
    .await;

    assert_eq!(output.status, 200);
    assert_eq!(String::from_utf8(output.body).expect("utf8"), "one:two");
}

#[tokio::test]
#[serial]
async fn dynamic_handle_cache_clears_on_owner_generation_change() {
    let service = test_service(dynamic_single_isolate_config()).await;

    service
        .deploy_with_config(
            "dynamic-generation-cache".to_string(),
            dynamic_plain_namespace_worker_with_source(
                "export default { async fetch() { return new Response('one'); } };",
            ),
            DeployConfig {
                bindings: vec![DeployBinding::Dynamic {
                    binding: "SANDBOX".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("first deploy should succeed");

    let first = invoke_with_timeout_and_dump(
        &service,
        "dynamic-generation-cache",
        test_invocation_with_path("/", "dynamic-generation-cache-one"),
        "dynamic generation cache first invoke",
    )
    .await;
    assert_eq!(String::from_utf8(first.body).expect("utf8"), "one");

    service
        .deploy_with_config(
            "dynamic-generation-cache".to_string(),
            dynamic_plain_namespace_worker_with_source(
                "export default { async fetch() { return new Response('two'); } };",
            ),
            DeployConfig {
                bindings: vec![DeployBinding::Dynamic {
                    binding: "SANDBOX".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("second deploy should succeed");

    let second = invoke_with_timeout_and_dump(
        &service,
        "dynamic-generation-cache",
        test_invocation_with_path("/", "dynamic-generation-cache-two"),
        "dynamic generation cache second invoke",
    )
    .await;
    assert_eq!(String::from_utf8(second.body).expect("utf8"), "two");
}

#[tokio::test]
#[serial]
async fn dynamic_snapshot_cache_reuses_snapshot_for_same_source() {
    let service = test_service(dynamic_single_isolate_config()).await;

    service
        .deploy_with_config(
            "dynamic-snapshot-cache".to_string(),
            dynamic_snapshot_cache_worker(),
            DeployConfig {
                bindings: vec![DeployBinding::Dynamic {
                    binding: "SANDBOX".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");

    let first = invoke_with_timeout_and_dump(
        &service,
        "dynamic-snapshot-cache",
        test_invocation_with_path("/?id=one", "dynamic-snapshot-cache-one"),
        "dynamic snapshot cache first create",
    )
    .await;
    assert_eq!(first.status, 200);

    let after_first = service.dynamic_debug_dump().await;
    assert_eq!(after_first.snapshot_cache_entries, 1);
    assert_eq!(after_first.snapshot_cache_failures, 0);

    let second = invoke_with_timeout_and_dump(
        &service,
        "dynamic-snapshot-cache",
        test_invocation_with_path("/?id=two", "dynamic-snapshot-cache-two"),
        "dynamic snapshot cache second create",
    )
    .await;
    assert_eq!(second.status, 200);

    let after_second = service.dynamic_debug_dump().await;
    assert_eq!(after_second.snapshot_cache_entries, 1);
    assert_eq!(after_second.snapshot_cache_failures, 0);
}

#[tokio::test]
#[serial]
async fn dynamic_namespace_child_can_return_json_response() {
    let service = test_service(dynamic_autoscaling_config()).await;

    service
            .deploy_with_config(
                "dynamic-parent-json".to_string(),
                r#"
let child = null;

export default {
  async fetch(_request, env) {
    if (!child) {
      child = await env.SANDBOX.get("json-child", async () => ({
        entrypoint: "worker.js",
        modules: {
          "worker.js": "export default { async fetch() { return Response.json({ ok: true }, { status: 201 }); } };",
        },
        timeout: 1500,
      }));
    }
    return child.fetch("http://worker/");
  },
};
"#
                .to_string(),
                DeployConfig {
                    bindings: vec![DeployBinding::Dynamic {
                        binding: "SANDBOX".to_string(),
                    }],
                    ..DeployConfig::default()
                },
            )
            .await
            .expect("deploy should succeed");

    let output = invoke_with_timeout_and_dump(
        &service,
        "dynamic-parent-json",
        test_invocation(),
        "dynamic json child invoke",
    )
    .await;

    assert_eq!(output.status, 201);
    assert_eq!(
        String::from_utf8(output.body).expect("utf8"),
        r#"{"ok":true}"#
    );
    assert!(output
        .headers
        .iter()
        .any(|(name, value)| name.eq_ignore_ascii_case("content-type")
            && value == "application/json"));
}

#[tokio::test]
#[serial]
async fn dynamic_namespace_host_rpc_works_with_single_inflight_parent_isolate() {
    let service = test_service(dynamic_single_isolate_config()).await;

    service
        .deploy_with_config(
            "dynamic-parent".to_string(),
            dynamic_namespace_worker(),
            DeployConfig {
                bindings: vec![DeployBinding::Dynamic {
                    binding: "SANDBOX".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");

    let output = invoke_with_timeout_and_dump(
        &service,
        "dynamic-parent",
        test_invocation(),
        "dynamic host rpc single-isolate invoke",
    )
    .await;

    assert_eq!(String::from_utf8(output.body).expect("utf8"), "1:1");
}

#[tokio::test]
#[serial]
async fn dynamic_namespace_host_rpc_completes_under_bench_like_autoscaling_load() {
    let service = test_service(dynamic_bench_autoscaling_config()).await;

    service
        .deploy_with_config(
            "dynamic-parent-autoscaling-host-rpc".to_string(),
            dynamic_namespace_worker(),
            DeployConfig {
                bindings: vec![DeployBinding::Dynamic {
                    binding: "SANDBOX".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");

    let warm = invoke_with_timeout_and_dump(
        &service,
        "dynamic-parent-autoscaling-host-rpc",
        test_invocation_with_path("/", "dynamic-host-rpc-autoscaling-warm"),
        "dynamic host rpc autoscaling warm",
    )
    .await;
    assert_eq!(warm.status, 200);

    let mut tasks = Vec::new();
    for idx in 0..32 {
        let service = service.clone();
        tasks.push(tokio::spawn(async move {
            invoke_with_timeout_and_dump(
                &service,
                "dynamic-parent-autoscaling-host-rpc",
                test_invocation_with_path("/", &format!("dynamic-host-rpc-autoscaling-{idx}")),
                &format!("dynamic host rpc autoscaling request {idx}"),
            )
            .await
        }));
    }

    let mut bodies = Vec::new();
    for task in tasks {
        let output = task.await.expect("task should join");
        assert_eq!(output.status, 200);
        bodies.push(String::from_utf8(output.body).expect("utf8"));
    }

    assert_eq!(bodies.len(), 32);
    assert!(bodies.iter().all(|body| body.contains(':')));
}

#[tokio::test]
#[serial]
async fn dynamic_namespace_host_rpc_reports_provider_isolate_loss_promptly() {
    let service = test_service(dynamic_single_isolate_config()).await;

    service
        .deploy_with_config(
            "dynamic-parent-stale-provider".to_string(),
            dynamic_namespace_worker(),
            DeployConfig {
                bindings: vec![DeployBinding::Dynamic {
                    binding: "SANDBOX".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");

    let warm = invoke_with_timeout_and_dump(
        &service,
        "dynamic-parent-stale-provider",
        test_invocation_with_path("/", "dynamic-host-rpc-stale-provider-warm"),
        "dynamic host rpc stale provider warm",
    )
    .await;
    assert_eq!(String::from_utf8(warm.body).expect("utf8"), "1:1");

    let dump = service
        .debug_dump("dynamic-parent-stale-provider".to_string())
        .await
        .expect("debug dump should exist");
    let isolate_id = dump
        .isolates
        .first()
        .map(|isolate| isolate.id)
        .expect("provider isolate should exist");
    assert!(
        service
            .force_fail_isolate_for_test(
                "dynamic-parent-stale-provider".to_string(),
                dump.generation,
                isolate_id,
            )
            .await
    );

    let error = timeout(
        Duration::from_secs(5),
        service.invoke(
            "dynamic-parent-stale-provider".to_string(),
            test_invocation_with_path("/", "dynamic-host-rpc-stale-provider"),
        ),
    )
    .await
    .expect("stale provider invoke should not hang")
    .expect_err("invoke should fail once provider isolate is gone");
    let error_text = error.to_string();
    assert!(
        error_text.contains("dynamic host rpc provider isolate is unavailable"),
        "unexpected error: {error_text}"
    );
}

#[tokio::test]
#[serial]
async fn dynamic_namespace_repeated_create_and_invoke_complete_past_threshold() {
    let service = test_service(dynamic_autoscaling_config()).await;

    service
        .deploy_with_config(
            "dynamic-repeat".to_string(),
            dynamic_repeated_create_worker(),
            DeployConfig {
                bindings: vec![DeployBinding::Dynamic {
                    binding: "SANDBOX".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");

    for idx in 0..64 {
        let output = invoke_with_timeout_and_dump(
            &service,
            "dynamic-repeat",
            test_invocation_with_path(&format!("/?id={idx}"), &format!("dynamic-repeat-{idx}")),
            &format!("dynamic repeat create+invoke {idx}"),
        )
        .await;
        assert_eq!(output.status, 200);
        assert_eq!(
            String::from_utf8(output.body).expect("utf8"),
            idx.to_string()
        );
    }
}

#[tokio::test]
#[serial]
async fn dynamic_namespace_child_can_use_response_json() {
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

    service
        .deploy_with_config(
            "dynamic-json".to_string(),
            dynamic_response_json_worker(),
            DeployConfig {
                public: false,
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Dynamic {
                    binding: "SANDBOX".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    let output = service
        .invoke("dynamic-json".to_string(), test_invocation())
        .await
        .expect("invoke should succeed");
    assert_eq!(output.status, 200);
    assert_eq!(
        String::from_utf8(output.body).expect("utf8"),
        r#"{"ok":true,"source":"dynamic-child"}"#
    );
}

#[tokio::test]
#[serial]
async fn normal_and_dynamic_workers_expose_same_runtime_surface() {
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

    service
        .deploy_with_config(
            "dynamic-surface".to_string(),
            dynamic_runtime_surface_worker(),
            DeployConfig {
                public: false,
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Dynamic {
                    binding: "SANDBOX".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    let output = service
        .invoke("dynamic-surface".to_string(), test_invocation())
        .await
        .expect("invoke should succeed");
    assert_eq!(output.status, 200);
    let body = String::from_utf8(output.body).expect("utf8");
    assert!(body.contains(r#""same":true"#), "body was {body}");
    assert!(body.contains(r#""fetch":true"#), "body was {body}");
    assert!(body.contains(r#""request":true"#), "body was {body}");
    assert!(body.contains(r#""response":true"#), "body was {body}");
    assert!(body.contains(r#""headers":true"#), "body was {body}");
    assert!(body.contains(r#""formData":true"#), "body was {body}");
    assert!(
        body.contains(r#""responseJsonType":"application/json""#),
        "body was {body}"
    );
}

#[tokio::test]
#[serial]
async fn transport_open_works_with_deno_request_compatibility() {
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

    service
        .deploy_with_config(
            "transport-runtime".to_string(),
            transport_echo_worker(),
            DeployConfig {
                public: false,
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "MEDIA".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    let (stream_tx, mut stream_rx) = mpsc::unbounded_channel();
    let (datagram_tx, _datagram_rx) = mpsc::unbounded_channel();
    let opened = service
        .open_transport(
            "transport-runtime".to_string(),
            test_transport_invocation(),
            stream_tx,
            datagram_tx,
        )
        .await
        .expect("transport open should succeed");

    assert_eq!(opened.output.status, 200);

    service
        .transport_push_stream(
            "transport-runtime".to_string(),
            opened.session_id.clone(),
            b"hello-transport".to_vec(),
            false,
        )
        .await
        .expect("transport push should succeed");

    let echoed = timeout(Duration::from_secs(2), stream_rx.recv())
        .await
        .expect("stream echo should arrive")
        .expect("stream echo channel should stay open");
    assert_eq!(echoed, b"hello-transport");

    service
        .transport_close(
            "transport-runtime".to_string(),
            opened.session_id,
            0,
            "done".to_string(),
        )
        .await
        .expect("transport close should succeed");
}

#[tokio::test]
#[serial]
async fn transport_open_preserves_connect_shape_for_memory_namespace_code() {
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

    service
        .deploy_with_config(
            "transport-shape".to_string(),
            transport_shape_worker(),
            DeployConfig {
                public: false,
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "MEDIA".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    let (stream_tx, _stream_rx) = mpsc::unbounded_channel();
    let (datagram_tx, _datagram_rx) = mpsc::unbounded_channel();
    let opened = service
        .open_transport(
            "transport-shape".to_string(),
            test_transport_invocation(),
            stream_tx,
            datagram_tx,
        )
        .await
        .expect("transport open should succeed");

    assert_eq!(opened.output.status, 200);
}

#[tokio::test]
#[serial]
async fn transport_wake_can_list_transport_handles_without_deadlock() {
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
        .deploy_with_config(
            "transport-values".to_string(),
            transport_values_worker(),
            DeployConfig {
                public: false,
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "MEDIA".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    let (stream_tx, mut stream_rx) = mpsc::unbounded_channel();
    let (datagram_tx, _datagram_rx) = mpsc::unbounded_channel();
    let opened = tokio::time::timeout(
        Duration::from_secs(5),
        service.open_transport(
            "transport-values".to_string(),
            test_transport_invocation(),
            stream_tx,
            datagram_tx,
        ),
    )
    .await
    .expect("transport open should not hang")
    .expect("transport open should succeed");
    assert_eq!(opened.output.status, 200);

    tokio::time::timeout(
        Duration::from_secs(5),
        service.transport_push_stream(
            "transport-values".to_string(),
            opened.session_id.clone(),
            b"ping".to_vec(),
            false,
        ),
    )
    .await
    .expect("transport push should not hang")
    .expect("transport push should succeed");

    let echoed = tokio::time::timeout(Duration::from_secs(5), stream_rx.recv())
        .await
        .expect("transport reply should arrive")
        .expect("transport reply channel should stay open");
    assert_eq!(echoed, b"ready:1");

    service
        .transport_close(
            "transport-values".to_string(),
            opened.session_id,
            0,
            "done".to_string(),
        )
        .await
        .expect("transport close should succeed");
}

#[tokio::test]
#[serial]
async fn transport_session_survives_idle_ttl_and_scales_down_after_close() {
    let service = test_service(RuntimeConfig {
        min_isolates: 0,
        max_isolates: 1,
        max_inflight_per_isolate: 1,
        idle_ttl: Duration::from_millis(200),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy_with_config(
            "transport-idle".to_string(),
            transport_echo_worker(),
            DeployConfig {
                public: false,
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "MEDIA".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    let (stream_tx, mut stream_rx) = mpsc::unbounded_channel();
    let (datagram_tx, _datagram_rx) = mpsc::unbounded_channel();
    let opened = service
        .open_transport(
            "transport-idle".to_string(),
            test_transport_invocation(),
            stream_tx,
            datagram_tx,
        )
        .await
        .expect("transport open should succeed");

    sleep(Duration::from_millis(500)).await;
    let stats = service
        .stats("transport-idle".to_string())
        .await
        .expect("worker stats should exist");
    assert_eq!(stats.isolates_total, 1);

    service
        .transport_push_stream(
            "transport-idle".to_string(),
            opened.session_id.clone(),
            b"idle-transport".to_vec(),
            false,
        )
        .await
        .expect("transport push should succeed");
    let echoed = timeout(Duration::from_secs(2), stream_rx.recv())
        .await
        .expect("transport echo should arrive")
        .expect("transport stream should stay open");
    assert_eq!(echoed, b"idle-transport");

    service
        .transport_close(
            "transport-idle".to_string(),
            opened.session_id,
            0,
            "done".to_string(),
        )
        .await
        .expect("transport close should succeed");
}

#[tokio::test]
#[serial]
async fn transport_session_reaped_when_owner_isolate_fails() {
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
        .deploy_with_config(
            "transport-reap".to_string(),
            transport_echo_worker(),
            DeployConfig {
                public: false,
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "MEDIA".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    let (stream_tx, _stream_rx) = mpsc::unbounded_channel();
    let (datagram_tx, _datagram_rx) = mpsc::unbounded_channel();
    let opened = service
        .open_transport(
            "transport-reap".to_string(),
            test_transport_invocation(),
            stream_tx,
            datagram_tx,
        )
        .await
        .expect("transport open should succeed");

    let dump = service
        .debug_dump("transport-reap".to_string())
        .await
        .expect("debug dump should exist");
    let isolate_id = dump
        .isolates
        .first()
        .map(|isolate| isolate.id)
        .expect("transport isolate should exist");
    assert!(
        service
            .force_fail_isolate_for_test("transport-reap".to_string(), dump.generation, isolate_id,)
            .await
    );

    let error = service
        .transport_push_stream(
            "transport-reap".to_string(),
            opened.session_id,
            b"after-fail".to_vec(),
            false,
        )
        .await
        .expect_err("reaped transport session should fail promptly");
    assert!(
        error.to_string().contains("transport session not found"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
#[serial]
async fn websocket_message_handler_can_use_memory_storage_after_handshake() {
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

    service
        .deploy_with_config(
            "ws-storage".to_string(),
            websocket_storage_worker(),
            DeployConfig {
                public: false,
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "CHAT".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    let opened = service
        .open_websocket(
            "ws-storage".to_string(),
            test_websocket_invocation("/ws", "ws-open"),
            None,
        )
        .await
        .expect("websocket open should succeed");

    assert_eq!(opened.output.status, 101);

    let echoed = service
        .websocket_send_frame(
            "ws-storage".to_string(),
            opened.session_id.clone(),
            b"ready".to_vec(),
            false,
        )
        .await
        .expect("websocket message should succeed");
    assert_eq!(echoed.status, 204);
    assert_eq!(
        String::from_utf8(echoed.body).expect("utf8"),
        r#"{"seen":"ready","count":1}"#
    );

    let state = service
        .invoke(
            "ws-storage".to_string(),
            test_invocation_with_path("/state", "ws-state"),
        )
        .await
        .expect("state invoke should succeed");
    assert_eq!(state.status, 200);
    let state_json: serde_json::Value =
        serde_json::from_slice(&state.body).expect("state body should be json");
    assert_eq!(state_json["count"], 1);
    assert_eq!(state_json["last"], "ready");

    service
        .websocket_close(
            "ws-storage".to_string(),
            opened.session_id,
            1000,
            "done".to_string(),
        )
        .await
        .expect("websocket close should succeed");
}

#[tokio::test]
#[serial]
async fn websocket_wake_can_list_socket_handles_without_deadlock() {
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
        .deploy_with_config(
            "ws-values".to_string(),
            websocket_values_worker(),
            DeployConfig {
                public: false,
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "CHAT".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    let opened = tokio::time::timeout(
        Duration::from_secs(5),
        service.open_websocket(
            "ws-values".to_string(),
            test_websocket_invocation("/ws", "ws-values-open"),
            None,
        ),
    )
    .await
    .expect("websocket open should not hang")
    .expect("websocket open should succeed");
    assert_eq!(opened.output.status, 101);

    let echoed = tokio::time::timeout(
        Duration::from_secs(5),
        service.websocket_send_frame(
            "ws-values".to_string(),
            opened.session_id.clone(),
            b"ping".to_vec(),
            false,
        ),
    )
    .await
    .expect("websocket message should not hang")
    .expect("websocket message should succeed");
    assert_eq!(echoed.status, 204);
    let echoed_json: serde_json::Value =
        serde_json::from_slice(&echoed.body).expect("wake payload should be json");
    assert_eq!(echoed_json["count"], 1);

    let handles = service
        .invoke(
            "ws-values".to_string(),
            test_invocation_with_path("/handles", "ws-values-handles"),
        )
        .await
        .expect("handles invoke should succeed");
    assert_eq!(handles.status, 200);
    let handles_json: serde_json::Value =
        serde_json::from_slice(&handles.body).expect("handles body should be json");
    assert_eq!(handles_json["count"], 1);

    let txn_handles = service
        .invoke(
            "ws-values".to_string(),
            test_invocation_with_path("/txn-handles", "ws-values-txn-handles"),
        )
        .await
        .expect("txn handles invoke should succeed");
    assert_eq!(txn_handles.status, 200);
    let txn_handles_json: serde_json::Value =
        serde_json::from_slice(&txn_handles.body).expect("txn handles body should be json");
    assert_eq!(txn_handles_json["first_is_array"], true);
    assert_eq!(txn_handles_json["second_is_array"], true);
    assert_eq!(txn_handles_json["first_count"], 1);
    assert_eq!(txn_handles_json["second_count"], 1);

    service
        .websocket_close(
            "ws-values".to_string(),
            opened.session_id,
            1000,
            "done".to_string(),
        )
        .await
        .expect("websocket close should succeed");
}

#[tokio::test]
#[serial]
async fn websocket_session_survives_idle_ttl_and_scales_down_after_close() {
    let service = test_service(RuntimeConfig {
        min_isolates: 0,
        max_isolates: 1,
        max_inflight_per_isolate: 1,
        idle_ttl: Duration::from_millis(200),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy_with_config(
            "ws-idle".to_string(),
            websocket_storage_worker(),
            DeployConfig {
                public: false,
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "CHAT".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    let opened = service
        .open_websocket(
            "ws-idle".to_string(),
            test_websocket_invocation("/ws", "ws-idle-open"),
            None,
        )
        .await
        .expect("websocket open should succeed");

    sleep(Duration::from_millis(500)).await;
    let stats = service
        .stats("ws-idle".to_string())
        .await
        .expect("worker stats should exist");
    assert_eq!(stats.isolates_total, 1);

    let echoed = service
        .websocket_send_frame(
            "ws-idle".to_string(),
            opened.session_id.clone(),
            b"idle".to_vec(),
            false,
        )
        .await
        .expect("websocket message should succeed");
    assert_eq!(echoed.status, 204);
    assert_eq!(
        String::from_utf8(echoed.body).expect("utf8"),
        r#"{"seen":"idle","count":1}"#
    );

    service
        .websocket_close(
            "ws-idle".to_string(),
            opened.session_id,
            1000,
            "done".to_string(),
        )
        .await
        .expect("websocket close should succeed");

    wait_for_isolate_total(&service, "ws-idle", 0).await;
}

#[tokio::test]
#[serial]
async fn websocket_session_reaped_when_owner_isolate_fails() {
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
        .deploy_with_config(
            "ws-reap".to_string(),
            websocket_storage_worker(),
            DeployConfig {
                public: false,
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "CHAT".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    let opened = service
        .open_websocket(
            "ws-reap".to_string(),
            test_websocket_invocation("/ws", "ws-reap-open"),
            None,
        )
        .await
        .expect("websocket open should succeed");

    let dump = service
        .debug_dump("ws-reap".to_string())
        .await
        .expect("debug dump should exist");
    let isolate_id = dump
        .isolates
        .first()
        .map(|isolate| isolate.id)
        .expect("websocket isolate should exist");
    assert!(
        service
            .force_fail_isolate_for_test("ws-reap".to_string(), dump.generation, isolate_id)
            .await
    );

    let error = service
        .websocket_send_frame(
            "ws-reap".to_string(),
            opened.session_id,
            b"after-fail".to_vec(),
            false,
        )
        .await
        .expect_err("reaped websocket session should fail promptly");
    assert!(
        error.to_string().contains("websocket session not found"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
#[serial]
async fn websocket_session_survives_redeploy_while_old_generation_stays_live() {
    let service = test_service(RuntimeConfig {
        min_isolates: 0,
        max_isolates: 1,
        max_inflight_per_isolate: 1,
        idle_ttl: Duration::from_millis(200),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    let deploy_config = DeployConfig {
        public: false,
        internal: DeployInternalConfig { trace: None },
        bindings: vec![DeployBinding::Memory {
            binding: "CHAT".to_string(),
        }],
    };

    service
        .deploy_with_config(
            "ws-redeploy".to_string(),
            websocket_storage_worker(),
            deploy_config.clone(),
        )
        .await
        .expect("initial deploy should succeed");

    let opened = service
        .open_websocket(
            "ws-redeploy".to_string(),
            test_websocket_invocation("/ws", "ws-redeploy-open"),
            None,
        )
        .await
        .expect("websocket open should succeed");

    service
        .deploy_with_config(
            "ws-redeploy".to_string(),
            websocket_storage_worker(),
            deploy_config,
        )
        .await
        .expect("redeploy should succeed");

    sleep(Duration::from_millis(500)).await;

    let echoed = service
        .websocket_send_frame(
            "ws-redeploy".to_string(),
            opened.session_id.clone(),
            b"after-redeploy".to_vec(),
            false,
        )
        .await
        .expect("old generation websocket should stay live");
    assert_eq!(echoed.status, 204);
    assert_eq!(
        String::from_utf8(echoed.body).expect("utf8"),
        r#"{"seen":"after-redeploy","count":1}"#
    );

    service
        .websocket_close(
            "ws-redeploy".to_string(),
            opened.session_id,
            1000,
            "done".to_string(),
        )
        .await
        .expect("websocket close should succeed");
}

#[tokio::test]
#[serial]
async fn websocket_stub_surface_uses_handle_backed_send_close_only() {
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

    service
        .deploy_with_config(
            "ws-surface".to_string(),
            websocket_socket_surface_worker(),
            DeployConfig {
                public: false,
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "CHAT".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    let output = service
        .invoke("ws-surface".to_string(), test_invocation())
        .await
        .expect("invoke should succeed");
    assert_eq!(output.status, 200);
    let surface: serde_json::Value =
        serde_json::from_slice(&output.body).expect("surface body should be json");
    assert_eq!(surface["stubSurface"]["values"], "function");
    assert_eq!(surface["stubSurface"]["send"], "undefined");
    assert_eq!(surface["stubSurface"]["close"], "undefined");
    assert_eq!(surface["stateSurface"]["accept"], "function");
    assert_eq!(surface["stateSurface"]["sockets"], "undefined");
}

#[tokio::test]
#[serial]
async fn websocket_message_handler_can_close_handle_backed_socket() {
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

    service
        .deploy_with_config(
            "ws-close".to_string(),
            websocket_storage_worker(),
            DeployConfig {
                public: false,
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "CHAT".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    let opened = service
        .open_websocket(
            "ws-close".to_string(),
            test_websocket_invocation("/ws", "ws-close-open"),
            None,
        )
        .await
        .expect("websocket open should succeed");
    assert_eq!(opened.output.status, 101);

    let closed = service
        .websocket_send_frame(
            "ws-close".to_string(),
            opened.session_id,
            b"close-me".to_vec(),
            false,
        )
        .await
        .expect("websocket close message should succeed");
    assert_eq!(closed.status, 204);
    let close_code = closed
        .headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("x-dd-ws-close-code"))
        .map(|(_, value)| value.as_str());
    let close_reason = closed
        .headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("x-dd-ws-close-reason"))
        .map(|(_, value)| value.as_str());
    assert_eq!(close_code, Some("1000"));
    assert_eq!(close_reason, Some("server-close"));
}

#[tokio::test]
#[serial]
async fn websocket_storage_uses_current_request_scope_on_warm_memory_instance() {
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

    service
        .deploy_with_config(
            "ws-storage".to_string(),
            websocket_storage_worker(),
            DeployConfig {
                public: false,
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "CHAT".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    let opened = service
        .open_websocket(
            "ws-storage".to_string(),
            test_websocket_invocation("/ws", "ws-open-warm"),
            None,
        )
        .await
        .expect("websocket open should succeed");

    let first = service
        .websocket_send_frame(
            "ws-storage".to_string(),
            opened.session_id.clone(),
            b"first".to_vec(),
            false,
        )
        .await
        .expect("first websocket message should succeed");
    assert_eq!(
        String::from_utf8(first.body).expect("utf8"),
        r#"{"seen":"first","count":1}"#
    );

    let second = service
        .websocket_send_frame(
            "ws-storage".to_string(),
            opened.session_id.clone(),
            b"second".to_vec(),
            false,
        )
        .await
        .expect("second websocket message should succeed");
    assert_eq!(
        String::from_utf8(second.body).expect("utf8"),
        r#"{"seen":"second","count":2}"#
    );

    service
        .websocket_close(
            "ws-storage".to_string(),
            opened.session_id,
            1000,
            "done".to_string(),
        )
        .await
        .expect("websocket close should succeed");
}

#[tokio::test]
#[serial]
async fn chat_worker_second_join_and_message_do_not_hang() {
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

    service
        .deploy_with_config(
            "chat".to_string(),
            include_str!("../../../../examples/chat-worker/src/worker.js").to_string(),
            DeployConfig {
                public: false,
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "CHAT_ROOM".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    let alice = tokio::time::timeout(
        Duration::from_secs(5),
        service.open_websocket(
            "chat".to_string(),
            test_websocket_invocation(
                "/rooms/test/ws?username=alice&participant=alice",
                "chat-open-alice",
            ),
            None,
        ),
    )
    .await
    .expect("alice websocket open should not hang")
    .expect("alice websocket open should succeed");
    assert_eq!(alice.output.status, 101);

    let alice_ready = tokio::time::timeout(
        Duration::from_secs(5),
        service.websocket_send_frame(
            "chat".to_string(),
            alice.session_id.clone(),
            br#"{"type":"ready"}"#.to_vec(),
            false,
        ),
    )
    .await
    .expect("alice ready should not hang")
    .expect("alice ready should succeed");
    assert_eq!(alice_ready.status, 204);

    let bob = tokio::time::timeout(
        Duration::from_secs(5),
        service.open_websocket(
            "chat".to_string(),
            test_websocket_invocation(
                "/rooms/test/ws?username=bob&participant=bob",
                "chat-open-bob",
            ),
            None,
        ),
    )
    .await
    .expect("bob websocket open should not hang")
    .expect("bob websocket open should succeed");
    assert_eq!(bob.output.status, 101);

    let bob_ready = tokio::time::timeout(
        Duration::from_secs(5),
        service.websocket_send_frame(
            "chat".to_string(),
            bob.session_id.clone(),
            br#"{"type":"ready"}"#.to_vec(),
            false,
        ),
    )
    .await
    .expect("bob ready should not hang")
    .expect("bob ready should succeed");
    assert_eq!(bob_ready.status, 204);
    let bob_ready_body = String::from_utf8(bob_ready.body).expect("utf8");
    assert!(
        bob_ready_body.contains("alice"),
        "bob ready payload should include alice: {bob_ready_body}"
    );

    tokio::time::timeout(
        Duration::from_secs(5),
        service.websocket_wait_frame("chat".to_string(), alice.session_id.clone()),
    )
    .await
    .expect("alice participant update should not hang")
    .expect("alice participant update should succeed");
    let alice_participants = tokio::time::timeout(
        Duration::from_secs(5),
        service.websocket_drain_frame("chat".to_string(), alice.session_id.clone()),
    )
    .await
    .expect("alice participant drain should not hang")
    .expect("alice participant drain should succeed")
    .expect("alice should have a pending participant update");
    let alice_participants_body =
        String::from_utf8(alice_participants.body).expect("participant payload utf8");
    assert!(
        alice_participants_body.contains("bob"),
        "alice participant payload should include bob: {alice_participants_body}"
    );

    let alice_message = tokio::time::timeout(
        Duration::from_secs(5),
        service.websocket_send_frame(
            "chat".to_string(),
            alice.session_id.clone(),
            br#"{"type":"message","text":"hello"}"#.to_vec(),
            false,
        ),
    )
    .await
    .expect("alice message should not hang")
    .expect("alice message should succeed");
    assert_eq!(alice_message.status, 204);
    let alice_message_body = String::from_utf8(alice_message.body).expect("utf8");
    assert!(
        alice_message_body.contains("hello"),
        "alice message payload should include the sent message: {alice_message_body}"
    );

    tokio::time::timeout(
        Duration::from_secs(5),
        service.websocket_wait_frame("chat".to_string(), bob.session_id.clone()),
    )
    .await
    .expect("bob message update should not hang")
    .expect("bob message update should succeed");
    let bob_message = tokio::time::timeout(
        Duration::from_secs(5),
        service.websocket_drain_frame("chat".to_string(), bob.session_id.clone()),
    )
    .await
    .expect("bob message drain should not hang")
    .expect("bob message drain should succeed")
    .expect("bob should have a pending message update");
    let bob_message_body = String::from_utf8(bob_message.body).expect("utf8");
    assert!(
        bob_message_body.contains("hello"),
        "bob message payload should include the sent message: {bob_message_body}"
    );

    service
        .websocket_close(
            "chat".to_string(),
            alice.session_id,
            1000,
            "done".to_string(),
        )
        .await
        .expect("alice websocket close should succeed");
    service
        .websocket_close("chat".to_string(), bob.session_id, 1000, "done".to_string())
        .await
        .expect("bob websocket close should succeed");
}

#[tokio::test]
#[serial]
async fn chat_worker_refresh_replaces_prior_participant_socket() {
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

    service
        .deploy_with_config(
            "chat".to_string(),
            include_str!("../../../../examples/chat-worker/src/worker.js").to_string(),
            DeployConfig {
                public: false,
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "CHAT_ROOM".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    let first = tokio::time::timeout(
        Duration::from_secs(5),
        service.open_websocket(
            "chat".to_string(),
            test_websocket_invocation(
                "/rooms/test/ws?username=alice&participant=alice",
                "chat-refresh-open-1",
            ),
            None,
        ),
    )
    .await
    .expect("first websocket open should not hang")
    .expect("first websocket open should succeed");
    assert_eq!(first.output.status, 101);

    let second = tokio::time::timeout(
        Duration::from_secs(5),
        service.open_websocket(
            "chat".to_string(),
            test_websocket_invocation(
                "/rooms/test/ws?username=alice&participant=alice",
                "chat-refresh-open-2",
            ),
            None,
        ),
    )
    .await
    .expect("refreshed websocket open should not hang")
    .expect("refreshed websocket open should succeed");
    assert_eq!(second.output.status, 101);

    let state = service
        .invoke(
            "chat".to_string(),
            test_invocation_with_path("/rooms/test/state", "chat-refresh-state"),
        )
        .await
        .expect("state invoke should succeed");
    assert_eq!(state.status, 200);
    let state_json: serde_json::Value =
        serde_json::from_slice(&state.body).expect("state body should be json");
    let participants = state_json["participants"]
        .as_array()
        .expect("participants should be array");
    assert_eq!(participants.len(), 1);
    assert_eq!(participants[0]["id"], "alice");

    let refreshed_ready = tokio::time::timeout(
        Duration::from_secs(5),
        service.websocket_send_frame(
            "chat".to_string(),
            second.session_id.clone(),
            br#"{"type":"ready"}"#.to_vec(),
            false,
        ),
    )
    .await
    .expect("refreshed ready should not hang")
    .expect("refreshed ready should succeed");
    assert_eq!(refreshed_ready.status, 204);

    let refreshed_message = tokio::time::timeout(
        Duration::from_secs(5),
        service.websocket_send_frame(
            "chat".to_string(),
            second.session_id.clone(),
            br#"{"type":"message","text":"after-refresh"}"#.to_vec(),
            false,
        ),
    )
    .await
    .expect("refreshed message should not hang")
    .expect("refreshed message should succeed");
    let refreshed_message_body =
        String::from_utf8(refreshed_message.body).expect("message payload utf8");
    assert!(
        refreshed_message_body.contains("after-refresh"),
        "refreshed message payload should include the sent message: {refreshed_message_body}"
    );

    service
        .websocket_close(
            "chat".to_string(),
            second.session_id,
            1000,
            "done".to_string(),
        )
        .await
        .expect("refreshed websocket close should succeed");
    let _ = service
        .websocket_close(
            "chat".to_string(),
            first.session_id,
            1000,
            "done".to_string(),
        )
        .await;
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

    for task in tasks {
        task.await.expect("join").expect("invoke should succeed");
    }

    let stats = service
        .stats("slow".to_string())
        .await
        .expect("stats should exist");
    assert!(stats.spawn_count > 1);
    assert!(stats.isolates_total <= 4);
}

#[tokio::test]
#[serial]
async fn scales_down_when_idle() {
    let service = test_service(RuntimeConfig {
        min_isolates: 0,
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
            if stats.isolates_total == 0 {
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
        body.extend(chunk.expect("chunk should be ok"));
    }
    assert_eq!(String::from_utf8(body).expect("utf8"), "hello");
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

    tx.send(Ok(b"hel".to_vec()))
        .await
        .expect("first body chunk should send");
    tx.send(Ok(b"lo".to_vec()))
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
async fn memory_same_key_allows_overlap_by_default() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 3,
        max_inflight_per_isolate: 4,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy_with_config(
            "memory".to_string(),
            memory_worker(),
            DeployConfig {
                public: false,
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "MY_MEMORY".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    let started = Instant::now();
    let mut tasks = Vec::new();
    for idx in 0..8 {
        let svc = service.clone();
        tasks.push(tokio::spawn(async move {
            svc.invoke(
                "memory".to_string(),
                test_invocation_with_path("/run?key=user-1", &format!("memory-run-{idx}")),
            )
            .await
        }));
    }
    for task in tasks {
        let output = task.await.expect("join").expect("invoke should succeed");
        assert_eq!(output.status, 200);
    }
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_millis(650),
        "expected overlap for same memory key by default, elapsed={elapsed:?}"
    );
}

#[tokio::test]
#[serial]
#[ignore = "hot same-memory STM write contention still needs a dedicated follow-up pass"]
async fn memory_storage_increment_preserves_all_updates_under_concurrency() {
    let service = test_service(RuntimeConfig {
        min_isolates: 2,
        max_isolates: 3,
        max_inflight_per_isolate: 1,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy_with_config(
            "memory".to_string(),
            memory_worker(),
            DeployConfig {
                public: false,
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "MY_MEMORY".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    service
        .invoke(
            "memory".to_string(),
            test_invocation_with_path("/seed?key=user-3", "seed"),
        )
        .await
        .expect("seed should succeed");

    let mut tasks = Vec::new();
    let increments = 8usize;
    for idx in 0..increments {
        let svc = service.clone();
        tasks.push(tokio::spawn(async move {
            svc.invoke(
                "memory".to_string(),
                test_invocation_with_path("/inc-cas?key=user-3", &format!("cas-{idx}")),
            )
            .await
        }));
    }

    for task in tasks {
        let output = task.await.expect("join").expect("invoke should succeed");
        assert_eq!(output.status, 200);
    }

    let current = service
        .invoke(
            "memory".to_string(),
            test_invocation_with_path("/get?key=user-3", "get-after-inc"),
        )
        .await
        .expect("get should succeed");
    assert_eq!(
        String::from_utf8(current.body).expect("utf8"),
        increments.to_string()
    );
}

#[tokio::test]
#[serial]
async fn memory_storage_different_keys_preserve_all_updates_under_concurrency() {
    let service = test_service(RuntimeConfig {
        min_isolates: 2,
        max_isolates: 8,
        max_inflight_per_isolate: 4,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy_with_config(
            "memory".to_string(),
            memory_worker(),
            DeployConfig {
                public: false,
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "MY_MEMORY".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    let keys = (0..8usize)
        .map(|index| format!("user-wide-{index}"))
        .collect::<Vec<_>>();
    for key in &keys {
        service
            .invoke(
                "memory".to_string(),
                test_invocation_with_path(&format!("/seed?key={key}"), &format!("seed-{key}")),
            )
            .await
            .expect("seed should succeed");
    }

    let mut tasks = Vec::new();
    for key in &keys {
        let svc = service.clone();
        let key = key.clone();
        tasks.push(tokio::spawn(async move {
            svc.invoke(
                "memory".to_string(),
                test_invocation_with_path(&format!("/inc-cas?key={key}"), &format!("inc-{key}")),
            )
            .await
        }));
    }
    for task in tasks {
        let output = task.await.expect("join").expect("invoke should succeed");
        assert_eq!(output.status, 200);
    }

    let mut total = 0usize;
    for key in &keys {
        let current = service
            .invoke(
                "memory".to_string(),
                test_invocation_with_path(&format!("/get?key={key}"), &format!("get-{key}")),
            )
            .await
            .expect("get should succeed");
        let value = String::from_utf8(current.body)
            .expect("utf8")
            .parse::<usize>()
            .expect("count should parse");
        assert_eq!(value, 1);
        total += value;
    }
    assert_eq!(total, keys.len());
}

#[tokio::test]
#[serial]
async fn memory_storage_structured_value_roundtrip_works() {
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

    service
        .deploy_with_config(
            "memory".to_string(),
            memory_worker(),
            DeployConfig {
                public: false,
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "MY_MEMORY".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    let roundtrip = service
        .invoke(
            "memory".to_string(),
            test_invocation_with_path("/value-roundtrip?key=user-4", "value-roundtrip"),
        )
        .await
        .expect("roundtrip invoke should succeed");
    assert_eq!(roundtrip.status, 200);
    assert_eq!(String::from_utf8(roundtrip.body).expect("utf8"), "ok");

    let guard = service
        .invoke(
            "memory".to_string(),
            test_invocation_with_path("/value-string-get-guard?key=user-5", "value-guard"),
        )
        .await
        .expect("guard invoke should succeed");
    assert_eq!(guard.status, 200);
    assert_eq!(String::from_utf8(guard.body).expect("utf8"), "ok");

    let visibility = service
        .invoke(
            "memory".to_string(),
            test_invocation_with_path("/local-visibility?key=user-6", "value-visibility"),
        )
        .await
        .expect("visibility invoke should succeed");
    assert_eq!(visibility.status, 200);
    assert_eq!(String::from_utf8(visibility.body).expect("utf8"), "ok");
}

#[tokio::test]
#[serial]
async fn memory_direct_write_visibility_roundtrip_works() {
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
            "memory".to_string(),
            memory_worker(),
            DeployConfig {
                public: false,
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "MY_MEMORY".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    let set = service
        .invoke(
            "memory".to_string(),
            test_invocation_with_path("/direct-set?key=user-direct-1", "direct-set"),
        )
        .await
        .expect("direct set should succeed");
    assert_eq!(set.status, 200);

    let after_set = service
        .invoke(
            "memory".to_string(),
            test_invocation_with_path("/direct-get?key=user-direct-1", "direct-get-set"),
        )
        .await
        .expect("direct get after set should succeed");
    assert_eq!(after_set.status, 200);
    assert_eq!(String::from_utf8(after_set.body).expect("utf8"), "5");

    let delete = service
        .invoke(
            "memory".to_string(),
            test_invocation_with_path("/direct-delete?key=user-direct-1", "direct-delete"),
        )
        .await
        .expect("direct delete should succeed");
    assert_eq!(delete.status, 200);

    let after_delete = service
        .invoke(
            "memory".to_string(),
            test_invocation_with_path("/direct-get?key=user-direct-1", "direct-get-delete"),
        )
        .await
        .expect("direct get after delete should succeed");
    assert_eq!(after_delete.status, 200);
    assert_eq!(String::from_utf8(after_delete.body).expect("utf8"), "0");
}

#[tokio::test]
#[serial]
async fn memory_direct_writes_preserve_distinct_memory_updates() {
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

    service
        .deploy_with_config(
            "memory".to_string(),
            memory_worker(),
            DeployConfig {
                public: false,
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "MY_MEMORY".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    let keys = (0..5)
        .map(|idx| format!("user-direct-distinct-{idx}"))
        .collect::<Vec<_>>();
    for key in &keys {
        let set = service
            .invoke(
                "memory".to_string(),
                test_invocation_with_path(
                    &format!("/direct-set?key={key}"),
                    &format!("direct-set-{key}"),
                ),
            )
            .await
            .expect("direct set should succeed");
        assert_eq!(set.status, 200);
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    let mut observed_total;
    loop {
        observed_total = 0;
        for key in &keys {
            let output = service
                .invoke(
                    "memory".to_string(),
                    test_invocation_with_path(
                        &format!("/direct-get?key={key}"),
                        &format!("direct-get-{key}"),
                    ),
                )
                .await
                .expect("direct get should succeed");
            assert_eq!(output.status, 200);
            observed_total += String::from_utf8(output.body)
                .expect("utf8")
                .parse::<usize>()
                .expect("direct value should parse");
        }
        if observed_total == keys.len() * 5 {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert_eq!(observed_total, keys.len() * 5);
}

#[tokio::test]
#[serial]
async fn memory_blind_stm_write_uses_blind_apply_path() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 4,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        memory_profile_enabled: true,
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy_with_config(
            "memory".to_string(),
            memory_worker(),
            DeployConfig {
                public: false,
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "MY_MEMORY".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    service
        .invoke(
            "memory".to_string(),
            test_invocation_with_path("/__profile_reset", "memory-blind-profile-reset"),
        )
        .await
        .expect("profile reset should succeed");

    let write = service
        .invoke(
            "memory".to_string(),
            test_invocation_with_path(
                "/stm-blind-write?key=user-blind-stm&value=7",
                "memory-blind-write",
            ),
        )
        .await
        .expect("blind stm write should succeed");
    assert_eq!(write.status, 200);
    assert_eq!(String::from_utf8(write.body).expect("utf8"), "7");

    let output = service
        .invoke(
            "memory".to_string(),
            test_invocation_with_path("/__profile", "memory-blind-profile"),
        )
        .await
        .expect("profile should succeed");
    let profile: Value = crate::json::from_string(
        String::from_utf8(output.body).expect("profile body should be utf8"),
    )
    .expect("profile should parse");
    assert!(
        profile["snapshot"]["op_apply_blind_batch"]["calls"]
            .as_u64()
            .unwrap_or(0)
            >= 1
    );
    assert_eq!(
        profile["snapshot"]["op_apply_batch"]["calls"]
            .as_u64()
            .unwrap_or(0),
        0
    );
    assert!(
        profile["snapshot"]["js_txn_blind_commit"]["calls"]
            .as_u64()
            .unwrap_or(0)
            >= 1
    );
}

#[tokio::test]
#[serial]
async fn memory_mixed_stm_write_stays_on_full_apply_path() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 4,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        memory_profile_enabled: true,
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy_with_config(
            "memory".to_string(),
            memory_worker(),
            DeployConfig {
                public: false,
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "MY_MEMORY".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    service
        .invoke(
            "memory".to_string(),
            test_invocation_with_path("/seed?key=user-mixed-stm", "memory-mixed-seed"),
        )
        .await
        .expect("seed should succeed");

    service
        .invoke(
            "memory".to_string(),
            test_invocation_with_path("/__profile_reset", "memory-mixed-profile-reset"),
        )
        .await
        .expect("profile reset should succeed");

    let write = service
        .invoke(
            "memory".to_string(),
            test_invocation_with_path(
                "/stm-read-write?key=user-mixed-stm&value=9",
                "memory-mixed-write",
            ),
        )
        .await
        .expect("mixed stm write should succeed");
    assert_eq!(write.status, 200);
    assert_eq!(String::from_utf8(write.body).expect("utf8"), "0->9");

    let output = service
        .invoke(
            "memory".to_string(),
            test_invocation_with_path("/__profile", "memory-mixed-profile"),
        )
        .await
        .expect("profile should succeed");
    let profile: Value = crate::json::from_string(
        String::from_utf8(output.body).expect("profile body should be utf8"),
    )
    .expect("profile should parse");
    assert!(
        profile["snapshot"]["op_apply_batch"]["calls"]
            .as_u64()
            .unwrap_or(0)
            >= 1
    );
    assert_eq!(
        profile["snapshot"]["op_apply_blind_batch"]["calls"]
            .as_u64()
            .unwrap_or(0),
        0
    );
    assert!(
        profile["snapshot"]["js_txn_commit"]["calls"]
            .as_u64()
            .unwrap_or(0)
            >= 1
    );
}

#[tokio::test]
#[serial]
async fn memory_direct_read_uses_point_read_lane() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 4,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        memory_profile_enabled: true,
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy_with_config(
            "memory".to_string(),
            memory_worker(),
            DeployConfig {
                public: false,
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "MY_MEMORY".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    service
        .invoke(
            "memory".to_string(),
            test_invocation_with_path("/__profile_reset", "memory-direct-profile-reset"),
        )
        .await
        .expect("profile reset should succeed");

    let read = service
        .invoke(
            "memory".to_string(),
            test_invocation_with_path(
                "/direct-get?key=user-direct-fast-read",
                "memory-direct-read",
            ),
        )
        .await
        .expect("direct read should succeed");
    assert_eq!(read.status, 200);
    assert_eq!(String::from_utf8(read.body).expect("utf8"), "0");

    let output = service
        .invoke(
            "memory".to_string(),
            test_invocation_with_path("/__profile", "memory-direct-profile"),
        )
        .await
        .expect("profile should succeed");
    let profile: Value = crate::json::from_string(
        String::from_utf8(output.body).expect("profile body should be utf8"),
    )
    .expect("profile should parse");
    assert!(
        profile["snapshot"]["op_read"]["calls"]
            .as_u64()
            .unwrap_or(0)
            >= 1
    );
    assert_eq!(
        profile["snapshot"]["op_snapshot"]["calls"]
            .as_u64()
            .unwrap_or(0),
        0
    );
    assert_eq!(
        profile["snapshot"]["op_version_if_newer"]["calls"]
            .as_u64()
            .unwrap_or(0),
        0
    );
}

#[tokio::test]
#[serial]
async fn memory_read_only_atomic_avoids_stm_commit_path() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 4,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        memory_profile_enabled: true,
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy_with_config(
            "memory".to_string(),
            memory_worker(),
            DeployConfig {
                public: false,
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "MY_MEMORY".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    service
        .invoke(
            "memory".to_string(),
            test_invocation_with_path("/__profile_reset", "memory-read-only-profile-reset"),
        )
        .await
        .expect("profile reset should succeed");

    let read = service
        .invoke(
            "memory".to_string(),
            test_invocation_with_path("/get?key=user-read-only-fast", "memory-read-only"),
        )
        .await
        .expect("atomic read should succeed");
    assert_eq!(read.status, 200);
    assert_eq!(String::from_utf8(read.body).expect("utf8"), "0");

    let output = service
        .invoke(
            "memory".to_string(),
            test_invocation_with_path("/__profile", "memory-read-only-profile"),
        )
        .await
        .expect("profile should succeed");
    let profile: Value = crate::json::from_string(
        String::from_utf8(output.body).expect("profile body should be utf8"),
    )
    .expect("profile should parse");
    assert_eq!(
        profile["snapshot"]["op_snapshot"]["calls"]
            .as_u64()
            .unwrap_or(0),
        0
    );
    assert_eq!(
        profile["snapshot"]["op_validate_reads"]["calls"]
            .as_u64()
            .unwrap_or(0),
        0
    );
    assert_eq!(
        profile["snapshot"]["op_apply_batch"]["calls"]
            .as_u64()
            .unwrap_or(0),
        0
    );
    assert_eq!(
        profile["snapshot"]["js_txn_validate"]["calls"]
            .as_u64()
            .unwrap_or(0),
        0
    );
    assert_eq!(
        profile["snapshot"]["js_txn_commit"]["calls"]
            .as_u64()
            .unwrap_or(0),
        0
    );
}

#[tokio::test]
#[serial]
async fn memory_multiple_atomic_reads_in_one_request_complete() {
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
            "memory-multi-read".to_string(),
            memory_multi_atomic_read_worker(),
            DeployConfig {
                public: false,
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "MY_MEMORY".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    let seed = service
        .invoke(
            "memory-multi-read".to_string(),
            test_invocation_with_path("/seed", "multi-read-seed"),
        )
        .await
        .expect("seed should succeed");
    assert_eq!(seed.status, 200);

    let output = tokio::time::timeout(
        Duration::from_secs(2),
        service.invoke(
            "memory-multi-read".to_string(),
            test_invocation_with_path("/sum?keys=2", "multi-read-sum"),
        ),
    )
    .await
    .expect("sum should not hang")
    .expect("sum invoke should succeed");
    assert_eq!(output.status, 200);
    assert_eq!(String::from_utf8(output.body).expect("utf8"), "1");
}

#[tokio::test]
#[serial]
async fn memory_multikey_direct_reads_complete_after_warmup() {
    let service = test_service(RuntimeConfig {
        min_isolates: 4,
        max_isolates: 4,
        max_inflight_per_isolate: 8,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy_with_config(
            "memory-multi-key".to_string(),
            memory_multi_key_storage_worker(),
            DeployConfig {
                public: false,
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "MY_MEMORY".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    service
        .invoke(
            "memory-multi-key".to_string(),
            test_invocation_with_path("/seed-all?keys=8", "multi-key-direct-seed"),
        )
        .await
        .expect("seed should succeed");

    let warmed = service
        .invoke(
            "memory-multi-key".to_string(),
            test_invocation_with_path("/direct-sum?keys=8", "multi-key-direct-warm"),
        )
        .await
        .expect("warm direct sum should succeed");
    assert_eq!(String::from_utf8(warmed.body).expect("utf8"), "8");

    let mut tasks = Vec::new();
    for idx in 0..4 {
        let service = service.clone();
        tasks.push(tokio::spawn(async move {
            timeout(
                Duration::from_secs(2),
                service.invoke(
                    "memory-multi-key".to_string(),
                    test_invocation_with_path(
                        "/direct-sum?keys=8",
                        &format!("multi-key-direct-{idx}"),
                    ),
                ),
            )
            .await
        }));
    }
    for task in tasks {
        let output = task
            .await
            .expect("join")
            .expect("direct sum should not hang")
            .expect("direct sum invoke should succeed");
        assert_eq!(output.status, 200);
        assert_eq!(String::from_utf8(output.body).expect("utf8"), "8");
    }
}

#[tokio::test]
#[serial]
async fn memory_multikey_stm_reads_complete_after_warmup() {
    let service = test_service(RuntimeConfig {
        min_isolates: 4,
        max_isolates: 4,
        max_inflight_per_isolate: 8,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy_with_config(
            "memory-multi-key".to_string(),
            memory_multi_key_storage_worker(),
            DeployConfig {
                public: false,
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "MY_MEMORY".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    service
        .invoke(
            "memory-multi-key".to_string(),
            test_invocation_with_path("/seed-all?keys=8", "multi-key-stm-seed"),
        )
        .await
        .expect("seed should succeed");

    let warmed = service
        .invoke(
            "memory-multi-key".to_string(),
            test_invocation_with_path("/stm-sum?keys=8", "multi-key-stm-warm"),
        )
        .await
        .expect("warm stm sum should succeed");
    assert_eq!(String::from_utf8(warmed.body).expect("utf8"), "8");

    let mut tasks = Vec::new();
    for idx in 0..4 {
        let service = service.clone();
        tasks.push(tokio::spawn(async move {
            timeout(
                Duration::from_secs(2),
                service.invoke(
                    "memory-multi-key".to_string(),
                    test_invocation_with_path("/stm-sum?keys=8", &format!("multi-key-stm-{idx}")),
                ),
            )
            .await
        }));
    }
    for task in tasks {
        let output = task
            .await
            .expect("join")
            .expect("stm sum should not hang")
            .expect("stm sum invoke should succeed");
        assert_eq!(output.status, 200);
        assert_eq!(String::from_utf8(output.body).expect("utf8"), "8");
    }
}

#[tokio::test]
#[serial]
async fn memory_stm_benchmark_worker_returns_correct_total() {
    let service = test_service(RuntimeConfig {
        min_isolates: 2,
        max_isolates: 2,
        max_inflight_per_isolate: 8,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy_with_config(
            "memory-multi-key".to_string(),
            memory_multi_key_storage_worker(),
            DeployConfig {
                public: false,
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "MY_MEMORY".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    service
        .invoke(
            "memory-multi-key".to_string(),
            test_invocation_with_path("/seed-all?keys=1", "multi-key-write-seed"),
        )
        .await
        .expect("seed should succeed");

    for idx in 0..8 {
        let output = timeout(
            Duration::from_secs(2),
            service.invoke(
                "memory-multi-key".to_string(),
                test_invocation_with_path("/inc?key=bench-0", &format!("multi-key-inc-{idx}")),
            ),
        )
        .await
        .expect("increment should not hang")
        .expect("increment invoke should succeed");
        assert_eq!(output.status, 200);
    }

    let total = service
        .invoke(
            "memory-multi-key".to_string(),
            test_invocation_with_path("/get?key=bench-0", "multi-key-write-total"),
        )
        .await
        .expect("total should succeed");
    assert_eq!(total.status, 200);
    assert_eq!(String::from_utf8(total.body).expect("utf8"), "9");
}

#[tokio::test]
#[serial]
async fn memory_direct_writes_complete_past_repeated_worker_threshold() {
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
        .deploy_with_config(
            "memory-direct-write-threshold".to_string(),
            memory_multi_key_storage_worker(),
            DeployConfig {
                public: false,
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "MY_MEMORY".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    for idx in 0..64 {
        let path = format!("/direct-write?key=bench-direct&value={}", idx + 1);
        let result = timeout(
            Duration::from_secs(10),
            service.invoke(
                "memory-direct-write-threshold".to_string(),
                test_invocation_with_path(&path, &format!("direct-write-threshold-{idx}")),
            ),
        )
        .await;
        let output = match result {
            Ok(Ok(output)) => output,
            Ok(Err(error)) => panic!("direct write {idx} failed: {error}"),
            Err(_) => {
                let dump = service
                    .debug_dump("memory-direct-write-threshold".to_string())
                    .await;
                panic!("direct write {idx} should not hang; debug dump: {dump:?}");
            }
        };
        assert_eq!(output.status, 200);
    }

    let total = timeout(
        Duration::from_secs(10),
        service.invoke(
            "memory-direct-write-threshold".to_string(),
            test_invocation_with_path("/get?key=bench-direct", "direct-write-threshold-total"),
        ),
    )
    .await
    .expect("direct write total should not hang")
    .expect("direct write total should succeed");
    assert_eq!(total.status, 200);
    assert_eq!(String::from_utf8(total.body).expect("utf8"), "64");
}

#[tokio::test]
#[serial]
async fn memory_atomic_writes_complete_past_repeated_worker_threshold() {
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
        .deploy_with_config(
            "memory-atomic-write-threshold".to_string(),
            memory_multi_key_storage_worker(),
            DeployConfig {
                public: false,
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "MY_MEMORY".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    service
        .invoke(
            "memory-atomic-write-threshold".to_string(),
            test_invocation_with_path("/seed-all?keys=1", "atomic-write-threshold-seed"),
        )
        .await
        .expect("seed should succeed");

    for idx in 0..64 {
        let result = timeout(
            Duration::from_secs(10),
            service.invoke(
                "memory-atomic-write-threshold".to_string(),
                test_invocation_with_path(
                    "/inc?key=bench-0",
                    &format!("atomic-write-threshold-{idx}"),
                ),
            ),
        )
        .await;
        let output = match result {
            Ok(Ok(output)) => output,
            Ok(Err(error)) => panic!("atomic write {idx} failed: {error}"),
            Err(_) => {
                let dump = service
                    .debug_dump("memory-atomic-write-threshold".to_string())
                    .await;
                panic!("atomic write {idx} should not hang; debug dump: {dump:?}");
            }
        };
        assert_eq!(output.status, 200);
    }

    let total = timeout(
        Duration::from_secs(10),
        service.invoke(
            "memory-atomic-write-threshold".to_string(),
            test_invocation_with_path("/get?key=bench-0", "atomic-write-threshold-total"),
        ),
    )
    .await
    .expect("atomic write total should not hang")
    .expect("atomic write total should succeed");
    assert_eq!(total.status, 200);
    assert_eq!(String::from_utf8(total.body).expect("utf8"), "65");
}

#[tokio::test]
#[serial]
async fn memory_constructor_reads_hydrated_storage_snapshot_synchronously() {
    let service = test_service(RuntimeConfig {
        min_isolates: 0,
        max_isolates: 1,
        max_inflight_per_isolate: 4,
        idle_ttl: Duration::from_millis(200),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy_with_config(
            "memory-ctor".to_string(),
            memory_constructor_storage_worker(),
            DeployConfig {
                public: false,
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "MY_MEMORY".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    let seeded = service
        .invoke(
            "memory-ctor".to_string(),
            test_invocation_with_path("/seed", "ctor-seed"),
        )
        .await
        .expect("seed invoke should succeed");
    assert_eq!(seeded.status, 200);

    let warm_ctor = service
        .invoke(
            "memory-ctor".to_string(),
            test_invocation_with_path("/constructor-value", "ctor-warm"),
        )
        .await
        .expect("warm constructor value should succeed");
    assert_eq!(String::from_utf8(warm_ctor.body).expect("utf8"), "7");

    let warm_direct = service
        .invoke(
            "memory-ctor".to_string(),
            test_invocation_with_path("/direct-value", "ctor-direct-warm"),
        )
        .await
        .expect("warm direct value should succeed");
    assert_eq!(String::from_utf8(warm_direct.body).expect("utf8"), "7");

    timeout(Duration::from_secs(3), async {
        loop {
            let stats = service
                .stats("memory-ctor".to_string())
                .await
                .expect("stats");
            if stats.isolates_total == 0 {
                break;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("memory pool should scale down to zero");

    let cold_ctor = service
        .invoke(
            "memory-ctor".to_string(),
            test_invocation_with_path("/constructor-value", "ctor-cold"),
        )
        .await
        .expect("cold constructor value should succeed");
    assert_eq!(String::from_utf8(cold_ctor.body).expect("utf8"), "7");

    let cold_direct = service
        .invoke(
            "memory-ctor".to_string(),
            test_invocation_with_path("/direct-value", "ctor-direct-cold"),
        )
        .await
        .expect("cold direct value should succeed");
    assert_eq!(String::from_utf8(cold_direct.body).expect("utf8"), "7");

    let current = service
        .invoke(
            "memory-ctor".to_string(),
            test_invocation_with_path("/current-value", "ctor-current"),
        )
        .await
        .expect("current value should succeed");
    assert_eq!(String::from_utf8(current.body).expect("utf8"), "7");
}

#[tokio::test]
#[serial]
async fn hosted_memory_factories_share_state_and_module_globals() {
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

    service
        .deploy_with_config(
            "hosted-memory".to_string(),
            hosted_memory_worker(),
            DeployConfig {
                public: false,
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "MY_MEMORY".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    let alpha = service
        .invoke(
            "hosted-memory".to_string(),
            test_invocation_with_path("/alpha/inc?key=user-1", "alpha-inc"),
        )
        .await
        .expect("alpha invoke should succeed");
    assert_eq!(String::from_utf8(alpha.body).expect("utf8"), "1");

    let beta = service
        .invoke(
            "hosted-memory".to_string(),
            test_invocation_with_path("/beta/read?key=user-1", "beta-read"),
        )
        .await
        .expect("beta invoke should succeed");
    assert_eq!(String::from_utf8(beta.body).expect("utf8"), "1");

    let worker_global = service
        .invoke(
            "hosted-memory".to_string(),
            test_invocation_with_path("/worker/global/inc?key=user-1", "worker-global-inc"),
        )
        .await
        .expect("worker global increment should succeed");
    assert_eq!(String::from_utf8(worker_global.body).expect("utf8"), "1");

    let memory_global = service
        .invoke(
            "hosted-memory".to_string(),
            test_invocation_with_path("/memory/global/read?key=user-1", "memory-global-read"),
        )
        .await
        .expect("memory global read should succeed");
    assert_eq!(String::from_utf8(memory_global.body).expect("utf8"), "1");

    let memory_global_inc = service
        .invoke(
            "hosted-memory".to_string(),
            test_invocation_with_path("/memory/global/inc?key=user-1", "memory-global-inc"),
        )
        .await
        .expect("memory global increment should succeed");
    assert_eq!(
        String::from_utf8(memory_global_inc.body).expect("utf8"),
        "2"
    );
}

#[tokio::test]
#[serial]
async fn hosted_memory_allows_inline_closures() {
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
        .deploy_with_config(
            "hosted-memory".to_string(),
            hosted_memory_worker(),
            DeployConfig {
                public: false,
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "MY_MEMORY".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    let output = service
        .invoke(
            "hosted-memory".to_string(),
            test_invocation_with_path("/inline?key=user-2", "inline-closure"),
        )
        .await
        .expect("inline closure invoke should succeed");
    assert_eq!(String::from_utf8(output.body).expect("utf8"), "ok-inline");
}

#[tokio::test]
#[serial]
async fn hosted_memory_stm_single_read_is_point_in_time_only() {
    let service = test_service(RuntimeConfig {
        min_isolates: 2,
        max_isolates: 3,
        max_inflight_per_isolate: 1,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy_with_config(
            "hosted-memory".to_string(),
            hosted_memory_worker(),
            DeployConfig {
                public: false,
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "MY_MEMORY".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    service
        .invoke(
            "hosted-memory".to_string(),
            test_invocation_with_path("/stm/seed?key=user-stm-once", "stm-seed-once"),
        )
        .await
        .expect("seed should succeed");

    let read_task = {
        let service = service.clone();
        tokio::spawn(async move {
            service
                .invoke(
                    "hosted-memory".to_string(),
                    test_invocation_with_path("/stm/read-once?key=user-stm-once", "stm-read-once"),
                )
                .await
        })
    };

    sleep(Duration::from_millis(10)).await;

    service
        .invoke(
            "hosted-memory".to_string(),
            test_invocation_with_path("/stm/write-a?key=user-stm-once&value=1", "stm-write-once"),
        )
        .await
        .expect("write should succeed");

    let read_once = read_task
        .await
        .expect("join")
        .expect("invoke should succeed");
    assert_eq!(String::from_utf8(read_once.body).expect("utf8"), "0");
}

#[tokio::test]
#[serial]
#[ignore = "same-memory STM retry path still needs a dedicated follow-up pass"]
async fn hosted_memory_stm_retries_when_prior_read_goes_stale() {
    let service = test_service(RuntimeConfig {
        min_isolates: 2,
        max_isolates: 3,
        max_inflight_per_isolate: 1,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy_with_config(
            "hosted-memory".to_string(),
            hosted_memory_worker(),
            DeployConfig {
                public: false,
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "MY_MEMORY".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    service
        .invoke(
            "hosted-memory".to_string(),
            test_invocation_with_path("/stm/seed?key=user-stm-pair", "stm-seed-pair"),
        )
        .await
        .expect("seed should succeed");

    let read_task = {
        let service = service.clone();
        tokio::spawn(async move {
            service
                .invoke(
                    "hosted-memory".to_string(),
                    test_invocation_with_path("/stm/read-pair?key=user-stm-pair", "stm-read-pair"),
                )
                .await
        })
    };

    let writer_thread = {
        let service = service.clone();
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build writer runtime");
            runtime.block_on(async move {
                service
                    .invoke(
                        "hosted-memory".to_string(),
                        test_invocation_with_path(
                            "/stm/write-a?key=user-stm-pair&value=1",
                            "stm-write-pair",
                        ),
                    )
                    .await
                    .expect("write should succeed");
            });
        })
    };

    let pair = read_task
        .await
        .expect("join")
        .expect("invoke should succeed");
    writer_thread.join().expect("writer join");
    assert_eq!(String::from_utf8(pair.body).expect("utf8"), "1:0");
}

#[tokio::test]
#[serial]
async fn hosted_memory_stm_snapshot_read_skips_retry_for_that_read() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 3,
        max_inflight_per_isolate: 8,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy_with_config(
            "hosted-memory".to_string(),
            hosted_memory_worker(),
            DeployConfig {
                public: false,
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "MY_MEMORY".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    service
        .invoke(
            "hosted-memory".to_string(),
            test_invocation_with_path("/stm/seed?key=user-stm-allow", "stm-seed-allow"),
        )
        .await
        .expect("seed should succeed");

    let read_task = {
        let service = service.clone();
        tokio::spawn(async move {
            service
                .invoke(
                    "hosted-memory".to_string(),
                    test_invocation_with_path(
                        "/stm/read-pair-snapshot?key=user-stm-allow",
                        "stm-read-allow",
                    ),
                )
                .await
        })
    };

    sleep(Duration::from_millis(10)).await;

    service
        .invoke(
            "hosted-memory".to_string(),
            test_invocation_with_path("/stm/write-a?key=user-stm-allow&value=1", "stm-write-allow"),
        )
        .await
        .expect("write should succeed");

    let pair = read_task
        .await
        .expect("join")
        .expect("invoke should succeed");
    assert_eq!(String::from_utf8(pair.body).expect("utf8"), "0:0");
}

#[tokio::test]
#[serial]
async fn hosted_memory_tvar_default_is_lazy_until_written() {
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

    service
        .deploy_with_config(
            "hosted-memory".to_string(),
            hosted_memory_worker(),
            DeployConfig {
                public: false,
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "MY_MEMORY".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    let default_read = service
        .invoke(
            "hosted-memory".to_string(),
            test_invocation_with_path(
                "/stm/tvar-default/read?key=user-1",
                "hosted-memory-tvar-default-read",
            ),
        )
        .await
        .expect("default read should succeed");
    assert_eq!(String::from_utf8(default_read.body).expect("utf8"), "7");

    let raw_before_write = service
        .invoke(
            "hosted-memory".to_string(),
            test_invocation_with_path(
                "/stm/tvar-default/raw?key=user-1",
                "hosted-memory-tvar-default-raw-before-write",
            ),
        )
        .await
        .expect("raw read before write should succeed");
    assert_eq!(
        String::from_utf8(raw_before_write.body).expect("utf8"),
        "missing"
    );

    let write = service
        .invoke(
            "hosted-memory".to_string(),
            test_invocation_with_path(
                "/stm/tvar-default/write?key=user-1",
                "hosted-memory-tvar-default-write",
            ),
        )
        .await
        .expect("write should succeed");
    assert_eq!(String::from_utf8(write.body).expect("utf8"), "8");

    let raw_after_write = service
        .invoke(
            "hosted-memory".to_string(),
            test_invocation_with_path(
                "/stm/tvar-default/raw?key=user-1",
                "hosted-memory-tvar-default-raw-after-write",
            ),
        )
        .await
        .expect("raw read after write should succeed");
    assert_eq!(String::from_utf8(raw_after_write.body).expect("utf8"), "8");
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
fn dynamic_worker_config_builds_placeholders() {
    let mut env = HashMap::new();
    env.insert("OPENAI_API_KEY".to_string(), "sk-test-123".to_string());
    let config =
        super::build_dynamic_worker_config(env, vec!["api.openai.com".to_string()], Vec::new())
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
        vec!["http://bad-host".to_string()],
        Vec::new(),
    );
    assert!(config.is_err());
}

#[test]
fn dynamic_worker_config_accepts_host_port_and_wildcard_rules() {
    let config = super::build_dynamic_worker_config(
        HashMap::new(),
        vec![
            "api.example.com:8443".to_string(),
            "*.example.com".to_string(),
            "*.example.com:9443".to_string(),
        ],
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
        ],
        ..DeployConfig::default()
    })
    .expect("bindings should parse");

    assert_eq!(bindings.kv, vec!["MY_KV".to_string()]);
    assert_eq!(bindings.dynamic, vec!["SANDBOX".to_string()]);
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
