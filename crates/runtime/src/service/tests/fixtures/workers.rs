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

pub(crate) fn async_reply_probe_worker() -> String {
    r#"
function testReplyFailure(error) {
  return {
    ok: false,
    value: "",
    error: String(error?.message ?? error ?? "test async reply failed"),
  };
}

function startTestAsyncReply(runtimeRequestId, options = {}) {
  return Deno.core.ops.op_test_async_reply_start(
    Math.max(0, Math.trunc(Number(globalThis.__dd_get_runtime_request_context_handle?.() ?? 0) || 0)),
    Math.max(0, Math.trunc(Number(options.delay_ms ?? options.delayMs ?? 0) || 0)),
    options.ok == null ? true : Boolean(options.ok),
    String(options.value ?? ""),
    String(options.error ?? ""),
  );
}

async function waitTestReply(runtimeRequestId, started, timeoutMs) {
  const waitReply = globalThis.__dd_await_request_reply;
  if (typeof waitReply !== "function") {
    throw new Error("request reply helper missing");
  }
  const actualReplyId = String(started?.reply_id ?? "").trim();
  if (!actualReplyId) {
    return testReplyFailure(started?.error ?? "test reply failed to start");
  }
  const timeoutStarted = startTestAsyncReply(runtimeRequestId, {
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
        startTestAsyncReply(runtimeRequestId, {
          value: "immediate",
        }),
        250,
      );
      return testReplyResponse(result);
    }

    if (url.pathname === "/async/delayed") {
      const result = await waitTestReply(
        runtimeRequestId,
        startTestAsyncReply(runtimeRequestId, {
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
        startTestAsyncReply(runtimeRequestId, {
          delay_ms: 200,
          value: "late",
        }),
        25,
      );
      return testReplyResponse(result, 504);
    }

    return new Response("not found", { status: 404 });
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
  const previous = state.get("chat");
  const next = { count: Number(previous?.count ?? 0) + 1, last: text };
  state.put("chat", next);
  if (text === "close-me") {
    state.sockets.close(event.handle, 1000, "server-close");
    return next;
  }
  state.sockets.send(event.handle, JSON.stringify({ seen: next.last, count: next.count }));
  return next;
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/state") {
      const stored = await env.CHAT.get(env.CHAT.idFromName("global")).atomic((state) => (state.get("chat") ?? { count: 0, last: null }));
      return Response.json(stored ?? { count: 0, last: null });
    }
    return await env.CHAT.get(env.CHAT.idFromName("global")).atomic((tx) => openSocket(tx, { request }));
  },

  async wake(event, env) {
    const _ = env;
    if (event.type !== "socketmessage" || !event.stub) {
      return;
    }
    await event.stub.atomic((tx) => onSocketMessage(tx, event));
  },
};
"#
        .to_string()
}

pub(crate) fn websocket_transaction_broadcast_worker() -> String {
    r#"
export function openSocket(state, payload) {
  const handles = state.get("handles") ?? [];
  const { handle, response } = state.accept(payload.request);
  state.put("handles", [...handles.filter((value) => value !== handle), handle]);
  return response;
}

export default {
  async fetch(request, env) {
    const room = env.CHAT.get(env.CHAT.idFromName("global"));
    const url = new URL(request.url);
    if (url.pathname === "/ws") {
      return await room.atomic((tx) => openSocket(tx, { request }));
    }
    if (url.pathname === "/broadcast") {
      const count = await room.atomic((state) => {
        const handles = state.get("handles") ?? [];
        for (const handle of handles) state.sockets.send(handle, "broadcast-ready");
        return handles.length;
      });
      return new Response(`sent:${count}`);
    }
    return new Response("not found", { status: 404 });
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
        const first = state.sockets.values();
        const second = state.sockets.values();
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
    await event.stub.atomic((tx) => {
      const handles = tx.sockets.values();
      tx.sockets.send(event.handle, JSON.stringify({ count: handles.length, handles }));
    });
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
    const encoder = new TextEncoder();
    const decoder = new TextDecoder();
    const bytes = (buffer) => Array.from(new Uint8Array(buffer));
    const hex = (buffer) => bytes(buffer).map((value) => value.toString(16).padStart(2, "0")).join("");

    const random = new Uint8Array(16);
    crypto.getRandomValues(random);
    const digestBuffer = await crypto.subtle.digest(
      "SHA-256",
      encoder.encode("dd-runtime"),
    );
    const hmacKey = await crypto.subtle.importKey(
      "raw",
      encoder.encode("secret-key"),
      { name: "HMAC", hash: "SHA-256" },
      false,
      ["sign", "verify"],
    );
    const hmacSignature = await crypto.subtle.sign(
      "HMAC",
      hmacKey,
      encoder.encode("signed-payload"),
    );
    const hmacVerified = await crypto.subtle.verify(
      "HMAC",
      hmacKey,
      hmacSignature,
      encoder.encode("signed-payload"),
    );

    const aesKey = await crypto.subtle.importKey(
      "raw",
      new Uint8Array([1, 35, 69, 103, 137, 171, 205, 239, 16, 50, 84, 118, 152, 186, 220, 254]),
      "AES-GCM",
      false,
      ["encrypt", "decrypt"],
    );
    const iv = new Uint8Array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]);
    const additionalData = encoder.encode("dd-aad");
    const aesCiphertext = await crypto.subtle.encrypt(
      { name: "AES-GCM", iv, additionalData, tagLength: 128 },
      aesKey,
      encoder.encode("secret-data"),
    );
    const aesPlaintext = await crypto.subtle.decrypt(
      { name: "AES-GCM", iv, additionalData, tagLength: 128 },
      aesKey,
      aesCiphertext,
    );

    const asymmetricKey = await crypto.subtle.generateKey(
      { name: "ECDSA", namedCurve: "P-256" },
      false,
      ["sign", "verify"],
    );
    const asymmetricPayload = encoder.encode("asymmetric-payload");
    const asymmetricSignature = await crypto.subtle.sign(
      { name: "ECDSA", hash: "SHA-256" },
      asymmetricKey.privateKey,
      asymmetricPayload,
    );
    const asymmetricVerified = await crypto.subtle.verify(
      { name: "ECDSA", hash: "SHA-256" },
      asymmetricKey.publicKey,
      asymmetricSignature,
      asymmetricPayload,
    );

    return Response.json({
      random_length: random.length,
      random_non_zero: random.some((value) => value !== 0),
      uuid: crypto.randomUUID(),
      digest_length: bytes(digestBuffer).length,
      digest_hex: hex(digestBuffer),
      hmac_signature_length: bytes(hmacSignature).length,
      hmac_verified: hmacVerified,
      aes_ciphertext_length: bytes(aesCiphertext).length,
      aes_roundtrip: decoder.decode(aesPlaintext),
      asymmetric_signature_length: bytes(asymmetricSignature).length,
      asymmetric_verified: asymmetricVerified,
    });
  },
};
"#
    .to_string()
}

pub(crate) fn kv_batching_worker() -> String {
    r#"
export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    if (url.pathname === "/seed") {
      await env.MY_KV.put("hot", "1");
      await env.MY_KV.put("utf8", "plain");
      await env.MY_KV.put("obj", { ok: true, n: 7 });
      await env.MY_KV.put("left", "L");
      await env.MY_KV.put("right", "R");
      const bad = await Deno.core.ops.op_kv_put_value_bytes(
        "MY_KV",
        "broken",
        "v8sc",
        new Uint8Array([1, 2, 3]),
      );
      if (bad && bad.ok === false) {
        throw new Error(String(bad.error ?? "seed broken failed"));
      }
      return new Response("ok");
    }

    if (url.pathname === "/sequential") {
      const values = [];
      for (let i = 0; i < 10; i++) {
        values.push(await env.MY_KV.get("hot"));
      }
      return Response.json(values);
    }

    if (url.pathname === "/queued") {
      const tasks = [];
      for (let i = 0; i < 10; i++) {
        tasks.push(env.MY_KV.get("hot"));
      }
      return Response.json(await Promise.all(tasks));
    }

    if (url.pathname === "/mixed") {
      const values = await Promise.all([
        env.MY_KV.get("utf8"),
        env.MY_KV.get("obj"),
        env.MY_KV.get("missing"),
        env.MY_KV.get("utf8"),
        env.MY_KV.get("obj"),
      ]);
      return Response.json(values);
    }

    if (url.pathname === "/list-object") {
      await env.MY_KV.put("obj-list", { ok: true, n: 17 });
      return Response.json(await env.MY_KV.list({ prefix: "obj-list", limit: 10 }));
    }

    if (url.pathname === "/scoped") {
      const key = String(url.searchParams.get("key") ?? "left");
      const tasks = [];
      for (let i = 0; i < 10; i++) {
        tasks.push(env.MY_KV.get(key));
      }
      return Response.json(await Promise.all(tasks));
    }

    if (url.pathname === "/reject") {
      try {
        await Promise.all([
          env.MY_KV.get("hot"),
          env.MY_KV.get("broken"),
          env.MY_KV.get("hot"),
        ]);
        return new Response("unexpected-success", { status: 500 });
      } catch (error) {
        return new Response(String(error?.message ?? error), { status: 500 });
      }
    }

    if (url.pathname === "/write-batch") {
      await Promise.all([
        env.MY_KV.put("hot", "2"),
        env.MY_KV.put("hot", "3"),
        env.MY_KV.put("hot", "4"),
      ]);
      return new Response(String((await env.MY_KV.get("hot")) ?? "missing"));
    }

    if (url.pathname === "/read") {
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

    if (url.pathname === "/read") {
      return new Response(String((await env.MY_KV.get("hot")) ?? "missing"));
    }

    if (url.pathname === "/read-missing") {
      return new Response(String((await env.MY_KV.get("ghost")) ?? "missing"));
    }

    if (url.pathname === "/put-read") {
      await env.MY_KV.put("hot", "11");
      return new Response(String((await env.MY_KV.get("hot")) ?? "missing"));
    }

    if (url.pathname === "/put-committed-read") {
      await env.MY_KV.put("hot", "12");
      return new Response(String((await env.MY_KV.get("hot")) ?? "missing"));
    }

    if (url.pathname === "/put-committed-object-read") {
      await env.MY_KV.put("obj2", { ok: true, n: 12 });
      return new Response(JSON.stringify({
        value: await env.MY_KV.get("obj2"),
      }), { headers: [["content-type", "application/json"]] });
    }

    if (url.pathname === "/delete-read") {
      await env.MY_KV.delete("hot");
      return new Response(String((await env.MY_KV.get("hot")) ?? "missing"));
    }

    if (url.pathname === "/delete-committed-read") {
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

    const bodyHandle = Deno.core.ops.op_http_store_prepared_body(
      new Uint8Array([102, 97, 107, 101]),
    );
    Deno.core.ops.op_emit_completion_ok(
      0,
      200,
      0,
      bodyHandle,
    );
    Deno.core.ops.op_emit_completion_ok(
      0,
      200,
      0,
      bodyHandle,
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
  attempts: new Map(),
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
  state.put("count", "0");
  return true;
}

export function incrementStrict(state) {
  const currentValue = asNumber(state.get("count"), 0);
  busyWait(1);
  state.put("count", String(currentValue + 1));
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
      const spin = asNumber(queryParam(url.search, "spin") ?? "100", 100);
      await memory.atomic((state) => {
        const slot = String(state.id);
        const runtime = globalThis.__dd_memory_runtime;
        const active = (runtime.active.get(slot) ?? 0) + 1;
        runtime.active.set(slot, active);
        const max = Math.max(runtime.max.get(slot) ?? 0, active);
        runtime.max.set(slot, max);
        busyWait(spin);
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

    if (url.pathname === "/single-execution-read") {
      const result = await memory.atomic((state) => {
        const slot = String(state.id);
        const runtime = globalThis.__dd_memory_runtime;
        runtime.attempts.set(slot, (runtime.attempts.get(slot) ?? 0) + 1);
        return state.get("cold") ?? "missing";
      });
      const attempts = globalThis.__dd_memory_runtime.attempts.get(String(id)) ?? 0;
      return new Response(`${result}:${attempts}`);
    }

    if (url.pathname === "/atomic-unsupported-option") {
      const operation = queryParam(url.search, "operation") ?? "get";
      try {
        await memory.atomic((state) => {
          if (operation === "set") {
            state.put("count", "1", { expectedVersion: 0 });
            return null;
          }
          if (operation === "delete") {
            state.delete("count", { expectedVersion: 0 });
            return null;
          }
          return state.get("count", { mode: "legacy" });
        });
        return new Response("not rejected", { status: 500 });
      } catch (error) {
        return new Response(String(error?.message ?? error), { status: 418 });
      }
    }


    if (url.pathname === "/idempotent-inc") {
      const idempotencyKey = queryParam(url.search, "command") ?? "";
      const amount = asNumber(queryParam(url.search, "amount") ?? "1", 1);
      const result = await memory.atomic((tx) => ((state, delta) => {
        const slot = String(state.id);
        const runtime = globalThis.__dd_memory_runtime;
        const attempts = (runtime.attempts.get(slot) ?? 0) + 1;
        runtime.attempts.set(slot, attempts);
        const current = asNumber(state.get("count"), 0);
        const next = current + delta;
        state.put("count", String(next));
        return { next, attempts };
      })(tx, amount), { idempotencyKey });
      return Response.json(result);
    }

    if (url.pathname === "/idempotent-read") {
      const idempotencyKey = queryParam(url.search, "command") ?? "";
      const result = await memory.atomic((state) => {
        const slot = String(state.id);
        const runtime = globalThis.__dd_memory_runtime;
        const attempts = (runtime.attempts.get(slot) ?? 0) + 1;
        runtime.attempts.set(slot, attempts);
        return { current: asNumber(state.get("count"), 0), attempts };
      }, { idempotencyKey });
      return Response.json(result);
    }

    if (url.pathname === "/emit-effect") {
      const result = await memory.atomic((state) => {
        const current = asNumber(state.get("count"), 0);
        const next = current + 1;
        state.put("count", String(next));
        state.emit("audit.increment", { key, next });
        state.emit("audit.second", { key, next });
        return { next };
      });
      return Response.json(result);
    }

    if (url.pathname === "/seed") {
      await memory.atomic(seedCount);
      return new Response("ok");
    }

    if (url.pathname === "/value-roundtrip") {
      const ok = await memory.atomic((state) => {
        state.put("profile", {
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
        state.put("profile", { nested: { ok: true } });
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
        state.put("count", "41");
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

    if (url.pathname === "/multi-write-versions") {
      const result = await memory.atomic((state) => {
        state.put("alpha", "1");
        state.put("beta", "2");
        const entries = state.list({ prefix: "" })
          .filter((entry) => entry.key === "alpha" || entry.key === "beta");
        return entries.map((entry) => `${entry.key}:${entry.value}:${entry.version}`).join(",");
      });
      return new Response(String(result));
    }

    if (url.pathname === "/inc-cas") {
      await memory.atomic(incrementStrict);
      return new Response("ok");
    }

    if (url.pathname === "/atomic-set-write") {
      const value = String(url.searchParams.get("value") ?? "1");
      const committed = await memory.atomic((state) => {
        state.put("count", value);
        return value;
      });
      return new Response(String(committed));
    }

    if (url.pathname === "/atomic-read-write") {
      const value = String(url.searchParams.get("value") ?? "1");
      const committed = await memory.atomic((state) => {
        const previous = String(state.get("count") ?? "0");
        state.put("count", value);
        return previous + "->" + String(state.get("count") ?? "missing");
      });
      return new Response(String(committed));
    }

    if (url.pathname === "/put") {
      await memory.atomic((tx) => tx.put("count", "5"));
      return new Response("ok");
    }

    if (url.pathname === "/delete") {
      await memory.atomic((tx) => tx.delete("count"));
      return new Response("ok");
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

pub(crate) fn memory_snapshot_worker() -> String {
    r#"
export default {
  async fetch(request, env) {
    const memory = env.MY_MEMORY.get(env.MY_MEMORY.idFromName("user-snapshot"));
    const url = new URL(request.url);

    if (url.pathname === "/seed") {
      await memory.atomic((state) => {
        state.put("count", "7");
        return true;
      });
      return new Response("ok");
    }

    if (url.pathname === "/get") {
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
  state.put("count", "1");
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
  state.put("count", "1");
  return true;
}

export function readCount(state) {
  return Number(state.get("count") ?? "0");
}

export function incrementCount(state) {
  const next = Number(state.get("count") ?? "0") + 1;
  state.put("count", String(next));
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


    if (url.pathname === "/atomic-sum") {
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
  state.put("a", "0");
  state.put("b", "0");
  return true;
}

export function writeA(state, value) {
  state.put("a", String(value));
  return String(value);
}

export function readOnce(state) {
  const a = String(state.get("a") ?? "missing");
  busyWait(5);
  return a;
}

export function readPairStrict(state) {
  const a = String(state.get("a") ?? "missing");
  busyWait(50);
  const b = String(state.get("b") ?? "missing");
  return `${a}:${b}`;
}

export function readPairSnapshot(state) {
  const a = String(state.get("a") ?? "missing");
  busyWait(5);
  const b = String(state.get("b") ?? "missing");
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
        state.put("count", String(next));
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

    if (url.pathname === "/atomic/seed") {
      await memory.atomic(seedStm);
      return new Response("ok");
    }

    if (url.pathname === "/atomic/write-a") {
      const value = String(url.searchParams.get("value") ?? "1");
      await memory.atomic((tx) => writeA(tx, value));
      return new Response("ok");
    }

    if (url.pathname === "/atomic/read-once") {
      return new Response(String(await memory.atomic(readOnce)));
    }


    if (url.pathname === "/atomic/read-pair") {
      return new Response(String(await memory.atomic(readPairStrict)));
    }

    if (url.pathname === "/atomic/read-pair-snapshot") {
      return new Response(String(await memory.atomic(readPairSnapshot)));
    }

    if (url.pathname === "/atomic/default/read") {
      return new Response(String(await memory.atomic((tx) => tx.get("count") ?? 7)));
    }

    if (url.pathname === "/atomic/default/raw") {
      return new Response(String(await memory.atomic((state) => state.get("count") ?? "missing")));
    }

    if (url.pathname === "/atomic/default/write") {
      return new Response(String(await memory.atomic((tx) => {
        const next = Number(tx.get("count") ?? 7) + 1;
        tx.put("count", String(next));
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
