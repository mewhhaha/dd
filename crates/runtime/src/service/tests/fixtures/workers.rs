pub(crate) fn counter_worker() -> String {
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

pub(crate) fn slow_worker() -> String {
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

pub(crate) fn dynamic_namespace_worker() -> String {
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

pub(crate) fn dynamic_plain_namespace_worker() -> String {
    dynamic_plain_namespace_worker_with_source(
            "let count = 0; export default { async fetch() { count += 1; return new Response(String(count)); } };",
        )
}

pub(crate) fn dynamic_plain_namespace_worker_with_source(child_source: &str) -> String {
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

pub(crate) fn dynamic_plain_handle_cache_worker() -> String {
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

pub(crate) fn dynamic_fast_fetch_worker() -> String {
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

pub(crate) fn dynamic_remote_fast_fetch_worker() -> String {
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

pub(crate) fn dynamic_wake_probe_worker() -> String {
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

pub(crate) fn dynamic_namespace_ops_worker() -> String {
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

pub(crate) fn dynamic_repeated_create_worker() -> String {
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

pub(crate) fn dynamic_snapshot_cache_worker() -> String {
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

pub(crate) fn dynamic_namespace_admin_worker() -> String {
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

pub(crate) fn dynamic_handle_cache_delete_worker() -> String {
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

pub(crate) fn dynamic_response_json_worker() -> String {
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

pub(crate) fn dynamic_runtime_surface_worker() -> String {
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

pub(crate) fn transport_echo_worker() -> String {
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

pub(crate) fn transport_shape_worker() -> String {
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

pub(crate) fn transport_values_worker() -> String {
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

pub(crate) fn websocket_storage_worker() -> String {
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

pub(crate) fn websocket_values_worker() -> String {
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

pub(crate) fn websocket_socket_surface_worker() -> String {
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

pub(crate) fn dynamic_fetch_probe_worker(url: &str) -> String {
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

pub(crate) fn dynamic_fetch_abort_worker(url: &str) -> String {
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

pub(crate) fn preview_dynamic_worker() -> String {
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

pub(crate) fn versioned_worker(version: &str, delay_ms: u64) -> String {
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

pub(crate) fn io_wait_worker() -> String {
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

pub(crate) fn frozen_time_worker() -> String {
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

pub(crate) fn crypto_worker() -> String {
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

pub(crate) fn kv_batching_worker(worker_name: &str) -> String {
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

pub(crate) fn kv_write_worker() -> String {
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

pub(crate) fn kv_wait_until_read_worker() -> String {
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

pub(crate) fn reusable_env_worker() -> String {
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

pub(crate) fn abort_aware_worker() -> String {
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

pub(crate) fn malicious_completion_worker() -> String {
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

pub(crate) fn cache_worker(cache_name: &str, label: &str) -> String {
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

pub(crate) fn streaming_request_body_worker() -> String {
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

pub(crate) fn memory_worker() -> String {
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

pub(crate) fn memory_constructor_storage_worker() -> String {
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

pub(crate) fn memory_multi_atomic_read_worker() -> String {
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

pub(crate) fn memory_multi_key_storage_worker() -> String {
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

pub(crate) fn hosted_memory_worker() -> String {
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

pub(crate) fn async_context_worker() -> String {
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

pub(crate) fn trace_sink_worker() -> String {
    r#"
export default {
  async fetch(request) {
    return new Response("ok");
  },
};
"#
    .to_string()
}

pub(crate) fn wait_until_worker() -> String {
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

pub(crate) fn loop_trace_worker() -> String {
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
