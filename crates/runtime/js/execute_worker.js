(() => {
  const requestId = __REQUEST_ID__;
  const completionToken = __COMPLETION_TOKEN__;
  const workerName = __WORKER_NAME__;
  const kvBindingsConfig = __KV_BINDINGS_JSON__;
  const actorBindingsConfig = __ACTOR_BINDINGS_JSON__;
  const dynamicBindingsConfig = __DYNAMIC_BINDINGS_JSON__;
  const dynamicRpcBindingsConfig = __DYNAMIC_RPC_BINDINGS_JSON__;
  const dynamicEnvConfig = __DYNAMIC_ENV_JSON__;
  const actorCallConfig = __ACTOR_CALL_JSON__;
  const hostRpcCallConfig = __HOST_RPC_CALL_JSON__;
  const hasRequestBodyStream = __HAS_REQUEST_BODY_STREAM__;
  const worker = globalThis.__dd_worker;

  if (worker === undefined) {
    throw new Error("Worker is not installed");
  }

  const inflightRequests = globalThis.__dd_inflight_requests ??= new Map();
  const hostRpcTargets = globalThis.__dd_host_rpc_targets ??= new Map();
  const RpcTarget = globalThis.RpcTarget ?? class RpcTarget {};
  if (globalThis.RpcTarget !== RpcTarget) {
    globalThis.RpcTarget = RpcTarget;
  }
  const input = __REQUEST_JSON__;
  const controller = new AbortController();
  const waitUntilPromises = [];
  let waitUntilDoneSent = false;
  let actorInvokeSeq = 0;
  globalThis.__dd_active_request_id = requestId;

  inflightRequests.set(requestId, controller);

  const callOp = (name, ...args) => {
    const op = Deno?.core?.ops?.[name];
    if (typeof op !== "function") {
      return undefined;
    }
    return op(...args);
  };

  const callOpAny = (names, ...args) => {
    for (const name of names) {
      const result = callOp(name, ...args);
      if (result !== undefined) {
        return result;
      }
    }
    return undefined;
  };

  const activeRequestId = () => {
    const scoped = String(globalThis.__dd_active_request_id ?? requestId).trim();
    if (!scoped) {
      throw new Error("dynamic worker request scope is unavailable");
    }
    return scoped;
  };

  const sleep = (millis) => callOp("op_sleep", Number(millis) || 0);

  const normalizeBoundaryValue = (value) => {
    if (value == null) {
      return null;
    }

    if (typeof value === "number") {
      return { nowMs: value, perfMs: value };
    }

    if (Array.isArray(value)) {
      const [nowMs, perfMs = nowMs] = value;
      return { nowMs, perfMs };
    }

    if (typeof value === "object") {
      const nowMs =
        value.nowMs ?? value.now_ms ?? value.now ?? value.wallMs ?? value.wall_ms;
      const perfMs =
        value.perfMs ?? value.perf_ms ?? value.perf ?? value.monotonicMs ?? value.monotonic_ms ?? nowMs;
      return { nowMs, perfMs };
    }

    return null;
  };

  const syncFrozenTime = async () => {
    const value = await callOp("op_time_boundary_now");
    const boundary = normalizeBoundaryValue(value);
    if (boundary && typeof globalThis.__dd_set_time === "function") {
      globalThis.__dd_set_time(boundary.nowMs, boundary.perfMs);
    }
  };
  globalThis.__dd_sync_time_boundary = syncFrozenTime;
  globalThis.__dd_cache_bypass_stale = Array.isArray(input.headers)
    && input.headers.some(([name, value]) => {
      const key = String(name || "").toLowerCase();
      if (key !== "x-dd-cache-bypass-stale") {
        return false;
      }
      const normalized = String(value || "").toLowerCase();
      return normalized === "1" || normalized === "true" || normalized === "yes";
    });

  const toUtf8Bytes = (value) => {
    if (value == null) {
      return new Uint8Array();
    }
    if (value instanceof Uint8Array) {
      return value;
    }
    if (value instanceof ArrayBuffer) {
      return new Uint8Array(value);
    }
    if (ArrayBuffer.isView(value)) {
      return new Uint8Array(value.buffer.slice(value.byteOffset, value.byteOffset + value.byteLength));
    }
    return new TextEncoder().encode(String(value));
  };

  const createRequestBodyStream = () => {
    let released = false;
    let done = false;

    const read = async () => {
      if (released) {
        throw new TypeError("Reader has been released");
      }
      if (done) {
        return { value: undefined, done: true };
      }

      const payload = await callOp("op_request_body_read", requestId);
      await syncFrozenTime();
      if (!payload || typeof payload !== "object") {
        done = true;
        return { value: undefined, done: true };
      }
      if (payload.ok === false) {
        done = true;
        throw new Error(String(payload.error ?? "request body stream failed"));
      }
      if (payload.done === true) {
        done = true;
        return { value: undefined, done: true };
      }
      return {
        value: new Uint8Array(Array.isArray(payload.chunk) ? payload.chunk : []),
        done: false,
      };
    };

    return {
      getReader() {
        return {
          read,
          releaseLock() {
            released = true;
          },
          async cancel() {
            done = true;
            await callOp("op_request_body_cancel", requestId);
            await syncFrozenTime();
            return undefined;
          },
        };
      },
      [Symbol.asyncIterator]() {
        const reader = this.getReader();
        return {
          next: () => reader.read(),
          return: async () => {
            if (typeof reader.cancel === "function") {
              await reader.cancel();
            }
            if (typeof reader.releaseLock === "function") {
              reader.releaseLock();
            }
            return { done: true, value: undefined };
          },
        };
      },
    };
  };

  const decodeStoredValue = (encoding, rawValue, context) => {
    const bytes = new Uint8Array(Array.isArray(rawValue) ? rawValue : []);
    if (encoding === "utf8") {
      return Deno.core.decode(bytes);
    }
    if (encoding === "v8sc") {
      try {
        return Deno.core.deserialize(bytes, { forStorage: true });
      } catch (error) {
        throw new Error(`${context} deserialize failed: ${String(error?.message ?? error)}`);
      }
    }
    throw new Error(`${context} unsupported encoding: ${encoding}`);
  };

  const createKvBinding = (bindingName) => ({
    async get(key, options = {}) {
      const _ = options;
      const result = await callOp(
        "op_kv_get_value",
        JSON.stringify({
          worker_name: workerName,
          binding: bindingName,
          key: String(key),
        }),
      );
      await syncFrozenTime();
      if (result && typeof result === "object" && result.ok === false) {
        throw new Error(String(result.error ?? "kv get failed"));
      }
      if (result?.found !== true) {
        return null;
      }
      const encoding = String(result.encoding ?? "utf8");
      return decodeStoredValue(encoding, result.value, "kv get");
    },
    async set(key, value, options = {}) {
      const _ = options;
      if (typeof value === "string") {
        const result = await callOp(
          "op_kv_set",
          workerName,
          bindingName,
          String(key),
          value,
        );
        await syncFrozenTime();
        if (result && typeof result === "object" && result.ok === false) {
          throw new Error(String(result.error ?? "kv set failed"));
        }
        return;
      }
      let encoded;
      try {
        encoded = Deno.core.serialize(value, { forStorage: true });
      } catch (error) {
        throw new Error(`kv set serialize failed: ${String(error?.message ?? error)}`);
      }
      const result = await callOp(
        "op_kv_set_value",
        JSON.stringify({
          worker_name: workerName,
          binding: bindingName,
          key: String(key),
          encoding: "v8sc",
          value: Array.from(new Uint8Array(encoded)),
        }),
      );
      await syncFrozenTime();
      if (result && typeof result === "object" && result.ok === false) {
        throw new Error(String(result.error ?? "kv set failed"));
      }
    },
    async delete(key, options = {}) {
      const _ = options;
      const result = await callOp(
        "op_kv_delete",
        workerName,
        bindingName,
        String(key),
      );
      await syncFrozenTime();
      if (result && typeof result === "object" && result.ok === false) {
        throw new Error(String(result.error ?? "kv delete failed"));
      }
    },
    async list(options = {}) {
      const prefix = String(options?.prefix ?? "");
      const limitInput = Number(options?.limit ?? 100);
      const limit = Number.isFinite(limitInput)
        ? Math.max(1, Math.min(1000, Math.trunc(limitInput)))
        : 100;
      const result = await callOp(
        "op_kv_list",
        workerName,
        bindingName,
        prefix,
        limit,
      );
      await syncFrozenTime();
      if (result && typeof result === "object" && result.ok === false) {
        throw new Error(String(result.error ?? "kv list failed"));
      }
      const entries = Array.isArray(result?.entries) ? result.entries : [];
      return entries.map((entry) => {
        const encoding = String(entry?.encoding ?? "utf8");
        return {
          key: String(entry?.key ?? ""),
          value: decodeStoredValue(encoding, entry?.value, "kv list"),
          encoding,
        };
      });
    },
  });

  const toHeaderEntries = (headersInput) => {
    if (!headersInput) {
      return [];
    }
    try {
      return Array.from(new Headers(headersInput).entries());
    } catch {
      return [];
    }
  };

  const isBinaryLike = (value) => (
    value instanceof ArrayBuffer
    || ArrayBuffer.isView(value)
    || value instanceof Uint8Array
  );

  const normalizeHostFetchInput = async (inputValue, initValue = undefined) => {
    let method = "GET";
    let url = "";
    let headers = [];
    let body = new Uint8Array();
    let signal = undefined;

    if (inputValue instanceof Request) {
      method = String(inputValue.method || "GET").toUpperCase();
      url = String(inputValue.url || "");
      headers = Array.from(inputValue.headers.entries());
      body = new Uint8Array(await inputValue.arrayBuffer());
      signal = inputValue.signal;
    } else {
      method = String(initValue?.method ?? "GET").toUpperCase();
      const raw = String(inputValue ?? "");
      url = raw;
      headers = toHeaderEntries(initValue?.headers);
      body = toUtf8Bytes(initValue?.body);
      signal = initValue?.signal;
    }

    if (inputValue instanceof Request && initValue) {
      if (initValue.method != null) {
        method = String(initValue.method).toUpperCase();
      }
      if (initValue.headers != null) {
        headers = toHeaderEntries(initValue.headers);
      }
      if (Object.prototype.hasOwnProperty.call(initValue, "body")) {
        body = toUtf8Bytes(initValue.body);
      }
      if (initValue.signal != null) {
        signal = initValue.signal;
      }
    }

    if (!(url.startsWith("http://") || url.startsWith("https://"))) {
      throw new TypeError("fetch requires an absolute http(s) URL in this runtime");
    }

    return {
      method,
      url,
      headers,
      body,
      signal,
    };
  };

  const composeAbortSignal = (signals) => {
    const filtered = signals.filter((signal) => (
      signal
      && typeof signal === "object"
      && typeof signal.addEventListener === "function"
    ));
    if (filtered.length === 0) {
      return undefined;
    }
    if (filtered.some((signal) => signal.aborted)) {
      const aborted = filtered.find((signal) => signal.aborted);
      const composed = new AbortController();
      composed.abort(aborted?.reason);
      return composed.signal;
    }
    if (filtered.length === 1) {
      return filtered[0];
    }
    const composed = new AbortController();
    const abort = (event) => {
      if (!composed.signal.aborted) {
        composed.abort(event?.target?.reason);
      }
    };
    for (const signal of filtered) {
      signal.addEventListener("abort", abort, { once: true });
    }
    return composed.signal;
  };

  const installHostFetch = () => {
    const previousFetch = globalThis.fetch;
    if (typeof previousFetch !== "function") {
      throw new TypeError("fetch is not available in this runtime");
    }
    const scopedFetch = async (inputValue, initValue = undefined) => {
      const normalized = await normalizeHostFetchInput(inputValue, initValue);
      const prepared = await callOp(
        "op_http_prepare",
        JSON.stringify({
          request_id: requestId,
          method: normalized.method,
          url: normalized.url,
          headers: normalized.headers,
          body: Array.from(normalized.body),
        }),
      );
      await syncFrozenTime();
      if (!prepared || typeof prepared !== "object" || prepared.ok === false) {
        throw new Error(String(prepared?.error ?? "host fetch prepare failed"));
      }
      const signal = composeAbortSignal([controller.signal, normalized.signal]);
      const body = Array.isArray(prepared.body) && prepared.body.length > 0
        ? toArrayBytes(prepared.body)
        : undefined;
      return previousFetch(new Request(String(prepared.url), {
        method: String(prepared.method || "GET"),
        headers: Array.isArray(prepared.headers) ? prepared.headers : [],
        body,
        signal,
      }));
    };
    globalThis.fetch = scopedFetch;
    return () => {
      if (globalThis.fetch === scopedFetch) {
        globalThis.fetch = previousFetch;
      }
    };
  };

  const normalizeSocketMessageForJs = (value) => {
    if (!value || typeof value !== "object" || Array.isArray(value)) {
      return value;
    }
    if (value.__dd_rpc_type === "socket_message") {
      if (String(value.kind || "").toLowerCase() === "binary") {
        return toArrayBytes(value.value ?? value.body ?? value.data);
      }
      return String(value.value ?? value.body ?? "");
    }
    return value;
  };

  const normalizeActorFetchInput = async (inputValue, initValue) => {
    let method = "GET";
    let url = "http://worker/";
    let headers = [];
    let body = new Uint8Array();

    if (inputValue instanceof Request) {
      method = String(inputValue.method || "GET").toUpperCase();
      url = String(inputValue.url || "http://worker/");
      headers = Array.from(inputValue.headers.entries());
      body = new Uint8Array(await inputValue.arrayBuffer());
    } else {
      method = String(initValue?.method ?? "GET").toUpperCase();
      const raw = String(inputValue ?? "/");
      url = raw.startsWith("http://") || raw.startsWith("https://")
        ? raw
        : new URL(raw, "http://worker").toString();
      headers = toHeaderEntries(initValue?.headers);
      body = toUtf8Bytes(initValue?.body);
    }

    if (inputValue instanceof Request && initValue) {
      if (initValue.method != null) {
        method = String(initValue.method).toUpperCase();
      }
      if (initValue.headers != null) {
        headers = toHeaderEntries(initValue.headers);
      }
      if (Object.prototype.hasOwnProperty.call(initValue, "body")) {
        body = toUtf8Bytes(initValue.body);
      }
    }

    return {
      method,
      url,
      headers,
      body,
    };
  };

  const toArrayBytes = (value) => {
    if (value instanceof Uint8Array) {
      return value;
    }
    if (ArrayBuffer.isView(value)) {
      return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
    }
    if (value instanceof ArrayBuffer) {
      return new Uint8Array(value);
    }
    return new Uint8Array(Array.isArray(value) ? value : []);
  };

  const isPlainObject = (value) => {
    if (!value || typeof value !== "object") {
      return false;
    }
    const proto = Object.getPrototypeOf(value);
    return proto === Object.prototype || proto === null;
  };

  const encodeRpcValue = async (value) => {
    if (value instanceof Request) {
      return {
        __dd_rpc_type: "request",
        url: String(value.url || ""),
        method: String(value.method || "GET"),
        headers: Array.from(value.headers.entries()),
        body: Array.from(new Uint8Array(await value.arrayBuffer())),
      };
    }
    if (value instanceof Response) {
      return {
        __dd_rpc_type: "response",
        status: Number(value.status || 200),
        headers: Array.from(value.headers.entries()),
        body: Array.from(new Uint8Array(await value.arrayBuffer())),
      };
    }
    if (Array.isArray(value)) {
      const out = [];
      for (const item of value) {
        out.push(await encodeRpcValue(item));
      }
      return out;
    }
    if (value instanceof Map) {
      const out = new Map();
      for (const [key, item] of value.entries()) {
        out.set(await encodeRpcValue(key), await encodeRpcValue(item));
      }
      return out;
    }
    if (value instanceof Set) {
      const out = new Set();
      for (const item of value.values()) {
        out.add(await encodeRpcValue(item));
      }
      return out;
    }
    if (isPlainObject(value)) {
      const out = {};
      for (const [key, item] of Object.entries(value)) {
        out[key] = await encodeRpcValue(item);
      }
      return out;
    }
    return value;
  };

  const decodeRpcValue = (value) => {
    if (value?.__dd_rpc_type === "socket_message") {
      return normalizeSocketMessageForJs(value);
    }
    if (Array.isArray(value)) {
      return value.map((item) => decodeRpcValue(item));
    }
    if (value instanceof Map) {
      const out = new Map();
      for (const [key, item] of value.entries()) {
        out.set(decodeRpcValue(key), decodeRpcValue(item));
      }
      return out;
    }
    if (value instanceof Set) {
      const out = new Set();
      for (const item of value.values()) {
        out.add(decodeRpcValue(item));
      }
      return out;
    }
    if (!isPlainObject(value)) {
      return value;
    }
    if (value.__dd_rpc_type === "request") {
      return new Request(String(value.url || "http://worker/"), {
        method: String(value.method || "GET"),
        headers: Array.isArray(value.headers) ? value.headers : [],
        body: toArrayBytes(value.body),
      });
    }
    if (value.__dd_rpc_type === "response") {
      return new Response(toArrayBytes(value.body), {
        status: Number(value.status || 200),
        headers: Array.isArray(value.headers) ? value.headers : [],
      });
    }
    const out = {};
    for (const [key, item] of Object.entries(value)) {
      out[key] = decodeRpcValue(item);
    }
    return out;
  };

  const encodeRpcArgs = async (args) => {
    const encoded = [];
    for (const arg of args) {
      encoded.push(await encodeRpcValue(arg));
    }
    return new Uint8Array(Deno.core.serialize(encoded));
  };

  const decodeRpcArgs = (bytes) => {
    const decoded = Deno.core.deserialize(bytes);
    return Array.isArray(decoded) ? decoded.map((value) => decodeRpcValue(value)) : [];
  };

  const encodeRpcResult = async (value) => {
    const encoded = await encodeRpcValue(value);
    return new Uint8Array(Deno.core.serialize(encoded));
  };

  const decodeRpcResult = (bytes) => decodeRpcValue(Deno.core.deserialize(bytes));

  const INTERNAL_WS_ACCEPT_HEADER = "x-dd-ws-accept";
  const INTERNAL_WS_SESSION_HEADER = "x-dd-ws-session";
  const INTERNAL_WS_HANDLE_HEADER = "x-dd-ws-handle";
  const INTERNAL_WS_BINDING_HEADER = "x-dd-ws-actor-binding";
  const INTERNAL_WS_KEY_HEADER = "x-dd-ws-actor-key";
  const INTERNAL_TRANSPORT_ACCEPT_HEADER = "x-dd-transport-accept";
  const INTERNAL_TRANSPORT_SESSION_HEADER = "x-dd-transport-session";
  const INTERNAL_TRANSPORT_HANDLE_HEADER = "x-dd-transport-handle";
  const INTERNAL_TRANSPORT_BINDING_HEADER = "x-dd-transport-actor-binding";
  const INTERNAL_TRANSPORT_KEY_HEADER = "x-dd-transport-actor-key";

  const encodeSocketSendPayload = (value, kind) => {
    if (kind != null) {
      const normalizedKind = String(kind).toLowerCase();
      if (normalizedKind === "text") {
        return {
          kind: "text",
          value: Array.from(toUtf8Bytes(String(value))),
        };
      }
      if (normalizedKind === "binary") {
        return {
          kind: "binary",
          value: Array.from(toArrayBytes(value)),
        };
      }
      throw new Error(`WebSocket(handle).send unsupported kind: ${kind}`);
    }
    if (isBinaryLike(value)) {
      return {
        kind: "binary",
        value: Array.from(toArrayBytes(value)),
      };
    }
    return {
      kind: "text",
      value: Array.from(toUtf8Bytes(String(value ?? ""))),
    };
  };

  const createActorStorageBinding = (runtimeRequestId) => ({
    async get(key) {
      const result = await callOp(
        "op_actor_state_get_value",
        {
          request_id: runtimeRequestId,
          key: String(key),
        },
      );
      await syncFrozenTime();
      if (result && typeof result === "object" && result.ok === false) {
        throw new Error(String(result.error ?? "actor storage get failed"));
      }
      if (result?.found !== true) {
        return null;
      }
      const encoding = String(result.encoding ?? "utf8");
      const bytes = toArrayBytes(result.value);
      if (encoding === "utf8") {
        return {
          value: Deno.core.decode(bytes),
          version: Number(result.version ?? -1),
          encoding,
        };
      }
      if (encoding === "v8sc") {
        return {
          value: Deno.core.deserialize(bytes, { forStorage: true }),
          version: Number(result.version ?? -1),
          encoding,
        };
      }
      throw new Error(`actor storage get unsupported encoding: ${encoding}`);
    },
    async put(key, value, options = {}) {
      const expectedInput = options?.expectedVersion;
      const expectedVersion = Number.isFinite(Number(expectedInput))
        ? Math.trunc(Number(expectedInput))
        : -1;
      if (typeof value === "string") {
        const result = await callOp(
          "op_actor_state_set",
          runtimeRequestId,
          String(key),
          value,
          expectedVersion,
        );
        await syncFrozenTime();
        if (result && typeof result === "object" && result.ok === false) {
          throw new Error(String(result.error ?? "actor storage put failed"));
        }
        return {
          ok: true,
          conflict: result?.conflict === true,
          version: Number(result?.version ?? -1),
        };
      }
      const encoded = new Uint8Array(Deno.core.serialize(value, { forStorage: true }));
      const result = await callOp(
        "op_actor_state_set_value",
        {
          request_id: runtimeRequestId,
          key: String(key),
          encoding: "v8sc",
          value: Array.from(encoded),
          expected_version: expectedVersion,
        },
      );
      await syncFrozenTime();
      if (result && typeof result === "object" && result.ok === false) {
        throw new Error(String(result.error ?? "actor storage put failed"));
      }
      return {
        ok: true,
        conflict: result?.conflict === true,
        version: Number(result?.version ?? -1),
      };
    },
    async delete(key, options = {}) {
      const expectedInput = options?.expectedVersion;
      const expectedVersion = Number.isFinite(Number(expectedInput))
        ? Math.trunc(Number(expectedInput))
        : -1;
      const result = await callOp(
        "op_actor_state_delete",
        runtimeRequestId,
        String(key),
        expectedVersion,
      );
      await syncFrozenTime();
      if (result && typeof result === "object" && result.ok === false) {
        throw new Error(String(result.error ?? "actor storage delete failed"));
      }
      return {
        ok: true,
        conflict: result?.conflict === true,
        version: Number(result?.version ?? -1),
      };
    },
    async list(options = {}) {
      const prefix = String(options?.prefix ?? "");
      const limitInput = Number(options?.limit ?? 100);
      const limit = Number.isFinite(limitInput)
        ? Math.max(1, Math.min(1000, Math.trunc(limitInput)))
        : 100;
      const result = await callOp(
        "op_actor_state_list",
        {
          request_id: runtimeRequestId,
          prefix,
          limit,
        },
      );
      await syncFrozenTime();
      if (result && typeof result === "object" && result.ok === false) {
        throw new Error(String(result.error ?? "actor storage list failed"));
      }
      return Array.isArray(result?.entries) ? result.entries : [];
    },
  });

  const createActorSocketRuntime = (entry, runtimeRequestId, allowSocketAccept) => {
    const socketsByHandle = entry.socketBindings ??= new Map();
    const openHandles = entry.openSocketHandles ??= new Set();
    const currentSocketRequestId = () => activeRequestId() || runtimeRequestId;

    const consumeCloseEvents = async (target) => {
      const result = await callOp("op_actor_socket_consume_close", {
        request_id: currentSocketRequestId(),
        handle: target.__dd_handle,
      });
      await syncFrozenTime();
      if (result && typeof result === "object" && result.ok === false) {
        throw new Error(String(result.error ?? "socket consumeClose failed"));
      }
      const events = Array.isArray(result?.events) ? result.events : [];
      for (const event of events) {
        target.__dd_dispatchClose(
          Number(event?.code ?? 1000),
          String(event?.reason ?? ""),
        );
      }
    };

    const ensureSocket = (handle) => {
      const normalizedHandle = String(handle ?? "").trim();
      if (!normalizedHandle) {
        throw new Error("WebSocket(handle) requires a non-empty handle");
      }
      const existing = socketsByHandle.get(normalizedHandle);
      if (existing) {
        consumeCloseEvents(existing).catch(() => {});
        return existing;
      }

      const listeners = new Map();
      let onmessage = null;
      let onclose = null;
      const closeQueue = [];
      let closed = false;
      const target = {
        __dd_handle: normalizedHandle,
        binaryType: "arraybuffer",
        get readyState() {
          return closed ? 3 : 1;
        },
        addEventListener(type, listener) {
          const key = String(type ?? "");
          if (!listeners.has(key)) {
            listeners.set(key, new Set());
          }
          if (typeof listener === "function") {
            listeners.get(key).add(listener);
          }
          if (key === "close") {
            this.__dd_flushCloseQueue();
          }
        },
        removeEventListener(type, listener) {
          const key = String(type ?? "");
          if (!listeners.has(key)) {
            return;
          }
          listeners.get(key).delete(listener);
        },
        get onmessage() {
          return onmessage;
        },
        set onmessage(listener) {
          onmessage = typeof listener === "function" ? listener : null;
        },
        get onclose() {
          return onclose;
        },
        set onclose(listener) {
          onclose = typeof listener === "function" ? listener : null;
          this.__dd_flushCloseQueue();
        },
        async send(value, kind) {
          const payload = encodeSocketSendPayload(value, kind);
          const result = await callOp("op_actor_socket_send", {
            request_id: currentSocketRequestId(),
            handle: normalizedHandle,
            message_kind: payload.kind,
            message: payload.value,
          });
          await syncFrozenTime();
          if (result && typeof result === "object" && result.ok === false) {
            throw new Error(String(result.error ?? "socket send failed"));
          }
        },
        async close(code, reason) {
          const normalizedCode = Number.isFinite(Number(code))
            ? Math.trunc(Number(code))
            : 1000;
          const result = await callOp("op_actor_socket_close", {
            request_id: currentSocketRequestId(),
            handle: normalizedHandle,
            code: normalizedCode,
            reason: reason == null ? "" : String(reason),
          });
          await syncFrozenTime();
          if (result && typeof result === "object" && result.ok === false) {
            throw new Error(String(result.error ?? "socket close failed"));
          }
        },
        async __dd_dispatch(type, event) {
          const current = listeners.get(type);
          if (current) {
            for (const listener of current) {
              try {
                await listener.call(this, event);
              } catch (error) {
                try {
                  console.warn(
                    "actor websocket listener failed",
                    String((error && (error.stack || error.message)) || error),
                  );
                } catch {
                  // Ignore logging failures.
                }
              }
            }
          }
        },
        async __dd_dispatchMessage(value) {
          const event = { type: "message", data: value, target: this };
          await this.__dd_dispatch("message", event);
          if (typeof onmessage === "function") {
            try {
              await onmessage.call(this, event);
            } catch (error) {
              try {
                console.warn(
                  "actor websocket onmessage failed",
                  String((error && (error.stack || error.message)) || error),
                );
              } catch {
                // Ignore logging failures.
              }
            }
          }
        },
        __dd_dispatchClose(code, reason) {
          const event = {
            type: "close",
            code: Number(code),
            reason: String(reason ?? ""),
            wasClean: true,
            target: this,
          };
          closeQueue.push(event);
          closed = true;
          openHandles.delete(normalizedHandle);
          this.__dd_flushCloseQueue();
        },
        __dd_flushCloseQueue() {
          if (closeQueue.length === 0) {
            return;
          }
          if (!onclose && !(listeners.get("close")?.size > 0)) {
            return;
          }
          while (closeQueue.length > 0) {
            const event = closeQueue.shift();
            this.__dd_dispatch("close", event);
            if (typeof onclose === "function") {
              try {
                onclose.call(this, event);
              } catch {
                // Ignore onclose failures.
              }
            }
          }
        },
      };
      socketsByHandle.set(normalizedHandle, target);
      consumeCloseEvents(target).catch(() => {});
      return target;
    };

    const WebSocketForActor = function WebSocket(handle) {
      return ensureSocket(handle);
    };

    const upgradeAccepted = { used: false };
    const sockets = {
      async accept(request, options = {}) {
        const _ = options;
        if (!allowSocketAccept) {
          throw new Error("state.sockets.accept is only available during actor fetch()");
        }
        if (upgradeAccepted.used) {
          throw new Error("state.sockets.accept can only be called once per request");
        }
        if (!(request instanceof Request)) {
          throw new Error("state.sockets.accept requires a Request");
        }
        const connection = String(request.headers.get("connection") ?? "");
        const upgrade = String(request.headers.get("upgrade") ?? "");
        const hasUpgrade = connection
          .split(",")
          .map((value) => value.trim().toLowerCase())
          .includes("upgrade");
        if (!hasUpgrade || upgrade.toLowerCase() !== "websocket") {
          throw new Error("state.sockets.accept requires a websocket upgrade request");
        }
        const sessionId = String(request.headers.get(INTERNAL_WS_SESSION_HEADER) ?? "").trim();
        if (!sessionId) {
          throw new Error("state.sockets.accept missing runtime websocket session metadata");
        }
        const handle = sessionId;
        const headers = new Headers();
        headers.set(INTERNAL_WS_ACCEPT_HEADER, "1");
        headers.set(INTERNAL_WS_SESSION_HEADER, sessionId);
        headers.set(INTERNAL_WS_HANDLE_HEADER, handle);
        headers.set(INTERNAL_WS_BINDING_HEADER, entry.binding);
        headers.set(INTERNAL_WS_KEY_HEADER, entry.actorKey);
        upgradeAccepted.used = true;
        openHandles.add(handle);
        return {
          handle,
          response: {
            __dd_websocket_accept: true,
            status: 101,
            headers,
            body: null,
          },
        };
      },
      values() {
        return Array.from(openHandles.values());
      },
    };

    return {
      sockets,
      WebSocket: WebSocketForActor,
      ensureSocket,
      async refreshOpenHandles() {
        const result = await callOp("op_actor_socket_list", {
          request_id: currentSocketRequestId(),
        });
        await syncFrozenTime();
        if (result && typeof result === "object" && result.ok === false) {
          throw new Error(String(result.error ?? "socket values failed"));
        }
        openHandles.clear();
        const handles = Array.isArray(result?.handles) ? result.handles : [];
        for (const value of handles) {
          openHandles.add(String(value));
        }
      },
    };
  };

  const createTransportReadableChannel = () => {
    const queue = [];
    let controller = null;
    let closed = false;
    const readable = new ReadableStream({
      start(nextController) {
        controller = nextController;
        while (queue.length > 0) {
          controller.enqueue(queue.shift());
        }
        if (closed) {
          controller.close();
        }
      },
      cancel() {
        closed = true;
      },
    });
    return {
      readable,
      push(value) {
        if (closed) {
          return;
        }
        const bytes = toArrayBytes(value);
        if (controller) {
          controller.enqueue(bytes);
          return;
        }
        queue.push(bytes);
      },
      close() {
        closed = true;
        if (controller) {
          controller.close();
        }
      },
      error(error) {
        closed = true;
        if (controller) {
          controller.error(error);
        }
      },
    };
  };

  const createActorTransportRuntime = (entry, runtimeRequestId, allowTransportAccept) => {
    const transportsByHandle = entry.transportBindings ??= new Map();
    const openHandles = entry.openTransportHandles ??= new Set();
    const currentTransportRequestId = () => activeRequestId() || runtimeRequestId;

    const consumeCloseEvents = async (target) => {
      const result = await callOp("op_actor_transport_consume_close", {
        request_id: currentTransportRequestId(),
        handle: target.__dd_handle,
      });
      await syncFrozenTime();
      if (result && typeof result === "object" && result.ok === false) {
        throw new Error(String(result.error ?? "transport consumeClose failed"));
      }
      const events = Array.isArray(result?.events) ? result.events : [];
      for (const event of events) {
        target.__dd_dispatchClose(
          Number(event?.code ?? 0),
          String(event?.reason ?? ""),
        );
      }
    };

    const ensureTransport = (handle) => {
      const normalizedHandle = String(handle ?? "").trim();
      if (!normalizedHandle) {
        throw new Error("WebTransportSession(handle) requires a non-empty handle");
      }
      const existing = transportsByHandle.get(normalizedHandle);
      if (existing) {
        consumeCloseEvents(existing).catch(() => {});
        return existing;
      }

      const datagramReadable = createTransportReadableChannel();
      const streamReadable = createTransportReadableChannel();
      let closed = false;
      let closedResolved = false;
      let closedResolve = null;
      const closedPromise = new Promise((resolve) => {
        closedResolve = resolve;
      });

      const finishClosed = () => {
        if (closedResolved) {
          return;
        }
        closedResolved = true;
        closed = true;
        openHandles.delete(normalizedHandle);
        datagramReadable.close();
        streamReadable.close();
        if (typeof closedResolve === "function") {
          closedResolve(undefined);
        }
      };

      const sendTransportPayload = async (kind, value) => {
        const bytes = isBinaryLike(value) ? toArrayBytes(value) : toUtf8Bytes(value);
        const payload = Array.from(bytes);
        const result = await callOpAny(
          [
            kind === "datagram"
              ? "op_actor_transport_datagram_send"
              : "op_actor_transport_stream_send",
            "op_actor_transport_send",
          ],
          {
            request_id: currentTransportRequestId(),
            handle: normalizedHandle,
            message_kind: kind,
            message: payload,
          },
        );
        await syncFrozenTime();
        if (result && typeof result === "object" && result.ok === false) {
          throw new Error(String(result.error ?? `transport ${kind} send failed`));
        }
      };

      const target = {
        __dd_handle: normalizedHandle,
        get readyState() {
          return closed ? 3 : 1;
        },
        ready: Promise.resolve(undefined),
        closed: closedPromise,
        datagrams: {
          readable: datagramReadable.readable,
          writable: new WritableStream({
            write(chunk) {
              return sendTransportPayload("datagram", chunk);
            },
            close() {
              return undefined;
            },
            abort() {
              return undefined;
            },
          }),
        },
        stream: {
          readable: streamReadable.readable,
          writable: new WritableStream({
            write(chunk) {
              return sendTransportPayload("stream", chunk);
            },
            close() {
              return undefined;
            },
            abort() {
              return undefined;
            },
          }),
        },
        async close(code, reason) {
          const normalizedCode = Number.isFinite(Number(code))
            ? Math.trunc(Number(code))
            : 0;
          const result = await callOpAny(
            [
              "op_actor_transport_close",
              "op_actor_transport_terminate",
            ],
            {
              request_id: currentTransportRequestId(),
              handle: normalizedHandle,
              code: normalizedCode,
              reason: reason == null ? "" : String(reason),
            },
          );
          await syncFrozenTime();
          if (result && typeof result === "object" && result.ok === false) {
            throw new Error(String(result.error ?? "transport close failed"));
          }
          finishClosed();
        },
        __dd_dispatchDatagram(value) {
          datagramReadable.push(value);
        },
        __dd_dispatchStreamChunk(value) {
          streamReadable.push(value);
        },
        __dd_dispatchClose(code, reason) {
          if (closedResolved) {
            return;
          }
          finishClosed();
          target.__dd_lastClose = {
            code: Number(code),
            reason: String(reason ?? ""),
          };
        },
      };

      transportsByHandle.set(normalizedHandle, target);
      consumeCloseEvents(target).catch(() => {});
      return target;
    };

    const WebTransportSessionForActor = function WebTransportSession(handle) {
      return ensureTransport(handle);
    };

    const transportAccepted = { used: false };
    const transports = {
      async accept(request, options = {}) {
        const _ = options;
        if (!allowTransportAccept) {
          throw new Error("state.transports.accept is only available during actor fetch()");
        }
        if (transportAccepted.used) {
          throw new Error("state.transports.accept can only be called once per request");
        }
        if (!(request instanceof Request)) {
          throw new Error("state.transports.accept requires a Request");
        }
        if (String(request.method || "").toUpperCase() !== "CONNECT") {
          throw new Error("state.transports.accept requires a CONNECT request");
        }
        const protocol = String(
          request.headers.get(":protocol")
          ?? request.headers.get("protocol")
          ?? request.headers.get("x-dd-transport-protocol")
          ?? "",
        ).trim().toLowerCase();
        if (protocol && protocol !== "webtransport") {
          throw new Error("state.transports.accept requires a webtransport request");
        }
        const sessionId = String(
          request.headers.get(INTERNAL_TRANSPORT_SESSION_HEADER)
          ?? request.headers.get(INTERNAL_TRANSPORT_HANDLE_HEADER)
          ?? "",
        ).trim();
        if (!sessionId) {
          throw new Error("state.transports.accept missing runtime transport session metadata");
        }
        const handle = sessionId;
        const headers = new Headers();
        headers.set(INTERNAL_TRANSPORT_ACCEPT_HEADER, "1");
        headers.set(INTERNAL_TRANSPORT_SESSION_HEADER, sessionId);
        headers.set(INTERNAL_TRANSPORT_HANDLE_HEADER, handle);
        headers.set(INTERNAL_TRANSPORT_BINDING_HEADER, entry.binding);
        headers.set(INTERNAL_TRANSPORT_KEY_HEADER, entry.actorKey);
        transportAccepted.used = true;
        openHandles.add(handle);
        return {
          handle,
          response: {
            __dd_transport_accept: true,
            status: 200,
            headers,
            body: null,
          },
        };
      },
      values() {
        return Array.from(openHandles.values());
      },
    };

    return {
      transports,
      WebTransportSession: WebTransportSessionForActor,
      ensureTransport,
      async refreshOpenHandles() {
        const result = await callOpAny([
          "op_actor_transport_list",
          "op_actor_transport_handles",
        ], {
          request_id: currentTransportRequestId(),
        });
        await syncFrozenTime();
        if (result && typeof result === "object" && result.ok === false) {
          throw new Error(String(result.error ?? "transport values failed"));
        }
        openHandles.clear();
        const handles = Array.isArray(result?.handles)
          ? result.handles
          : Array.isArray(result?.values)
            ? result.values
            : [];
        for (const value of handles) {
          openHandles.add(String(value));
        }
      },
    };
  };

  const createActorRuntimeState = (entry, runtimeRequestId, allowSocketAccept, allowTransportAccept) => {
    const socketRuntime = createActorSocketRuntime(entry, runtimeRequestId, allowSocketAccept);
    const transportRuntime = createActorTransportRuntime(entry, runtimeRequestId, allowTransportAccept);
    entry.socketRuntime = socketRuntime;
    entry.transportRuntime = transportRuntime;
    return {
      id: {
        toString() {
          return entry.actorKey;
        },
      },
      storage: createActorStorageBinding(runtimeRequestId),
      sockets: socketRuntime.sockets,
      transports: transportRuntime.transports,
      __dd_socket_runtime: socketRuntime,
      __dd_transport_runtime: transportRuntime,
    };
  };

  const actorInstances = globalThis.__dd_actor_instances ??= new Map();

  const actorMethodNameIsBlocked = (name) => (
    !name
    || name === "constructor"
    || name === "fetch"
    || name === "then"
    || name.startsWith("__dd_")
  );

  const actorMethodNameIsBlockedForProxy = (name) => (
    actorMethodNameIsBlocked(name)
  );

  const hostRpcMethodNameIsBlocked = (name) => (
    !name
    || name === "constructor"
    || name === "fetch"
    || name === "then"
    || name.startsWith("__dd_")
  );

  const isRpcTargetInstance = (value) => (
    value != null
    && typeof value === "object"
    && typeof RpcTarget === "function"
    && value instanceof RpcTarget
  );

  const extractRpcTargetMethods = (target) => {
    const methods = new Set();
    let proto = Object.getPrototypeOf(target);
    while (proto && proto !== Object.prototype) {
      for (const name of Object.getOwnPropertyNames(proto)) {
        if (hostRpcMethodNameIsBlocked(name)) {
          continue;
        }
        const descriptor = Object.getOwnPropertyDescriptor(proto, name);
        if (!descriptor || typeof descriptor.value !== "function") {
          continue;
        }
        methods.add(name);
      }
      if (proto === RpcTarget.prototype) {
        break;
      }
      proto = Object.getPrototypeOf(proto);
    }
    return Array.from(methods.values()).sort();
  };

  const createDynamicHostRpcNamespace = (bindingName) => new Proxy({}, {
    get(_target, prop) {
      if (typeof prop === "symbol") {
        return undefined;
      }
      const methodName = String(prop);
      if (methodName === "then") {
        return undefined;
      }
      if (hostRpcMethodNameIsBlocked(methodName)) {
        throw new Error(`dynamic host rpc method is blocked: ${methodName}`);
      }
      return async (...args) => {
        const argsBytes = await encodeRpcArgs(args);
        const result = await callOp("op_dynamic_host_rpc_invoke", {
          request_id: requestId,
          binding: bindingName,
          method_name: methodName,
          args: Array.from(argsBytes),
        });
        await syncFrozenTime();
        if (!result || typeof result !== "object" || result.ok === false) {
          throw new Error(String(result?.error ?? `dynamic host rpc invoke failed: ${methodName}`));
        }
        return decodeRpcResult(toArrayBytes(result.value));
      };
    },
  });

  const createActorStub = (namespace, actorKey) => {
    const target = {
      async fetch(inputValue, initValue = undefined) {
        const request = await normalizeActorFetchInput(inputValue, initValue);
        actorInvokeSeq += 1;
        const result = await callOp(
          "op_actor_invoke_fetch",
          {
            worker_name: workerName,
            binding: namespace,
            key: actorKey,
            method: request.method,
            url: request.url,
            headers: request.headers,
            body: Array.from(request.body),
            request_id: `${requestId}:actor-fetch:${actorInvokeSeq}`,
          },
        );
        await syncFrozenTime();
        if (!result || typeof result !== "object" || result.ok === false) {
          throw new Error(String(result?.error ?? "actor fetch invoke failed"));
        }
        const status = Number(result.status ?? 200);
        const headers = Array.isArray(result.headers) ? result.headers : [];
        const responseHeaders = new Headers(headers);
        if (status === 101) {
          return {
            __dd_websocket_accept: true,
            status,
            headers: responseHeaders,
            body: null,
          };
        }
        if (
          status === 200
          && String(responseHeaders.get(INTERNAL_TRANSPORT_ACCEPT_HEADER) ?? "") === "1"
        ) {
          return {
            __dd_transport_accept: true,
            status,
            headers: responseHeaders,
            body: null,
          };
        }
        return new Response(toArrayBytes(result.body), {
          status,
          headers,
        });
      },
    };

    return new Proxy(target, {
      get(currentTarget, prop, receiver) {
        if (typeof prop === "symbol") {
          return Reflect.get(currentTarget, prop, receiver);
        }
        const methodName = String(prop);
        if (methodName in currentTarget) {
          return Reflect.get(currentTarget, prop, receiver);
        }
        if (methodName === "then") {
          return undefined;
        }
        if (actorMethodNameIsBlockedForProxy(methodName)) {
          throw new Error(`actor method is blocked: ${methodName}`);
        }
        return async (...args) => {
          actorInvokeSeq += 1;
          const argsBytes = await encodeRpcArgs(args);
          const result = await callOp(
            "op_actor_invoke_method",
            {
              worker_name: workerName,
              binding: namespace,
              key: actorKey,
              method_name: methodName,
              args: Array.from(argsBytes),
              request_id: `${requestId}:actor-method:${actorInvokeSeq}`,
            },
          );
          await syncFrozenTime();
          if (!result || typeof result !== "object" || result.ok === false) {
            throw new Error(String(result?.error ?? `actor method invoke failed: ${methodName}`));
          }
          return decodeRpcResult(toArrayBytes(result.value));
        };
      },
    });
  };

  const actorIdKey = (id) => {
    if (typeof id === "string") {
      return id;
    }
    if (id && typeof id === "object" && typeof id.__dd_actor_key === "string") {
      return id.__dd_actor_key;
    }
    return "";
  };

  const createActorNamespace = (bindingName) => ({
    idFromName(name) {
      const key = String(name ?? "").trim();
      if (!key) {
        throw new Error("actor idFromName requires a non-empty name");
      }
      return {
        __dd_actor_key: key,
        __dd_actor_binding: bindingName,
        toString() {
          return key;
        },
      };
    },
    get(id) {
      const actorKey = actorIdKey(id).trim();
      if (!actorKey) {
        throw new Error("actor namespace get() requires a valid actor id");
      }
      return createActorStub(bindingName, actorKey);
    },
  });

  const splitDynamicEnvInput = (value) => {
    if (value == null) {
      return {
        stringEnv: {},
        hostRpcBindings: [],
      };
    }
    if (typeof value !== "object" || Array.isArray(value)) {
      throw new Error("dynamic worker env must be an object");
    }
    const stringEnv = {};
    const hostRpcBindings = [];
    for (const [key, entry] of Object.entries(value)) {
      const name = String(key ?? "").trim();
      if (!name) {
        throw new Error("dynamic worker env key must not be empty");
      }
      if (isRpcTargetInstance(entry)) {
        const targetIdRaw = callOp("op_crypto_random_uuid");
        const targetId = String(targetIdRaw ?? "").trim();
        if (!targetId) {
          throw new Error("failed to allocate dynamic host rpc target id");
        }
        const methods = extractRpcTargetMethods(entry);
        hostRpcTargets.set(targetId, entry);
        hostRpcBindings.push({
          binding: name,
          target_id: targetId,
          methods,
        });
        continue;
      }
      if (entry != null && typeof entry === "object") {
        throw new Error(`dynamic worker env value for ${name} must be primitive or RpcTarget`);
      }
      stringEnv[name] = String(entry ?? "");
    }
    return {
      stringEnv,
      hostRpcBindings,
    };
  };

  const normalizeDynamicInstanceId = (value) => {
    const normalized = String(value ?? "").trim();
    if (!normalized) {
      throw new Error("dynamic worker id must not be empty");
    }
    return normalized;
  };

  const normalizeDynamicTimeout = (value) => {
    if (value == null) {
      return 5_000;
    }
    const parsed = Number(value);
    if (!Number.isFinite(parsed) || parsed <= 0) {
      throw new Error("dynamic worker timeout must be a positive number");
    }
    const normalized = Math.trunc(parsed);
    if (normalized > 60_000) {
      return 60_000;
    }
    return normalized;
  };

  const normalizeModulePath = (value) => {
    const raw = String(value ?? "").replaceAll("\\", "/").trim();
    if (!raw) {
      throw new Error("dynamic worker module path must not be empty");
    }
    const absolute = raw.startsWith("/");
    const parts = raw.split("/");
    const out = [];
    for (const part of parts) {
      if (!part || part === ".") {
        continue;
      }
      if (part === "..") {
        if (out.length > 0) {
          out.pop();
        }
        continue;
      }
      out.push(part);
    }
    const normalized = out.join("/");
    return absolute ? normalized : normalized;
  };

  const resolveModuleSpecifier = (fromPath, specifier) => {
    const spec = String(specifier ?? "").trim();
    if (!spec) {
      return null;
    }
    if (/^[a-zA-Z][a-zA-Z0-9+.-]*:/.test(spec) || spec.startsWith("//")) {
      return null;
    }
    if (!spec.startsWith("./") && !spec.startsWith("../") && !spec.startsWith("/")) {
      return normalizeModulePath(spec);
    }
    const baseParts = normalizeModulePath(fromPath).split("/");
    if (baseParts.length > 0) {
      baseParts.pop();
    }
    const relative = spec.startsWith("/") ? spec.slice(1) : spec;
    const merged = spec.startsWith("/") ? relative : `${baseParts.join("/")}/${relative}`;
    return normalizeModulePath(merged);
  };

  const replaceModuleSpecifiers = (modulePath, source, mapResolver) => {
    let out = String(source ?? "");
    const fromReplace = /(\bfrom\s*['"])([^'"]+)(['"])/g;
    out = out.replace(fromReplace, (full, prefix, spec, suffix) => {
      const replacement = mapResolver(modulePath, spec);
      if (!replacement) {
        return full;
      }
      return `${prefix}${replacement}${suffix}`;
    });
    const sideEffectImport = /(\bimport\s*['"])([^'"]+)(['"])/g;
    out = out.replace(sideEffectImport, (full, prefix, spec, suffix) => {
      const replacement = mapResolver(modulePath, spec);
      if (!replacement) {
        return full;
      }
      return `${prefix}${replacement}${suffix}`;
    });
    const dynamicImport = /(\bimport\s*\(\s*['"])([^'"]+)(['"]\s*\))/g;
    out = out.replace(dynamicImport, (full, prefix, spec, suffix) => {
      const replacement = mapResolver(modulePath, spec);
      if (!replacement) {
        return full;
      }
      return `${prefix}${replacement}${suffix}`;
    });
    return out;
  };

  const buildSourceFromModules = (entrypointInput, modulesInput) => {
    const entrypoint = normalizeModulePath(entrypointInput || "worker.js");
    if (!isPlainObject(modulesInput)) {
      throw new Error("dynamic worker modules must be an object");
    }

    const modules = new Map();
    for (const [rawPath, rawCode] of Object.entries(modulesInput)) {
      const modulePath = normalizeModulePath(rawPath);
      const source = String(rawCode ?? "");
      if (!source.trim()) {
        throw new Error(`dynamic worker module must not be empty: ${modulePath}`);
      }
      modules.set(modulePath, source);
    }
    if (modules.size === 0) {
      throw new Error("dynamic worker modules must not be empty");
    }
    if (!modules.has(entrypoint)) {
      throw new Error(`dynamic worker missing entrypoint module: ${entrypoint}`);
    }

    const encodedByPath = new Map();
    const urlsByPath = new Map();
    const unresolved = new Set();
    const maxRounds = 16;
    for (let round = 0; round < maxRounds; round += 1) {
      unresolved.clear();
      let changed = false;
      for (const [modulePath, moduleSource] of modules.entries()) {
        const rewritten = replaceModuleSpecifiers(
          modulePath,
          moduleSource,
          (fromPath, specifier) => {
            const resolved = resolveModuleSpecifier(fromPath, specifier);
            if (!resolved) {
              return null;
            }
            if (!modules.has(resolved)) {
              unresolved.add(`${fromPath} -> ${specifier}`);
              return null;
            }
            const url = urlsByPath.get(resolved);
            if (!url) {
              unresolved.add(`${fromPath} -> ${specifier}`);
              return null;
            }
            return url;
          },
        );
        const encoded = `data:text/javascript;charset=utf-8,${encodeURIComponent(rewritten)}`;
        if (encodedByPath.get(modulePath) !== encoded) {
          encodedByPath.set(modulePath, encoded);
          urlsByPath.set(modulePath, encoded);
          changed = true;
        }
      }
      if (!changed && unresolved.size === 0) {
        break;
      }
      if (round === maxRounds - 1) {
        if (unresolved.size > 0) {
          throw new Error(
            `dynamic worker module graph did not resolve after ${maxRounds} rounds; unresolved imports: ${Array.from(unresolved).slice(0, 5).join(", ")}`,
          );
        }
        throw new Error(
          "dynamic worker module graph did not stabilize; avoid circular imports in dynamic modules",
        );
      }
    }

    const entryUrl = urlsByPath.get(entrypoint);
    if (!entryUrl) {
      throw new Error("dynamic worker entrypoint URL could not be built");
    }
    return `export { default } from ${JSON.stringify(entryUrl)};\n`;
  };

  const resolveDynamicWorkerSource = (options) => {
    if (Object.prototype.hasOwnProperty.call(options, "source")) {
      const source = String(options.source ?? "");
      if (!source.trim()) {
        throw new Error("dynamic worker source must not be empty");
      }
      return source;
    }

    const entrypoint = String(options.entrypoint ?? "worker.js").trim();
    if (!entrypoint) {
      throw new Error("dynamic worker entrypoint must not be empty");
    }
    const modules = options.modules;
    if (!isPlainObject(modules)) {
      throw new Error("dynamic worker modules must be an object");
    }
    return buildSourceFromModules(entrypoint, modules);
  };

  const createDynamicWorkerStub = (bindingName, handle, worker, timeout) => ({
    worker,
    async fetch(inputValue, initValue = undefined) {
      const request = await normalizeActorFetchInput(inputValue, initValue);
      const scopedRequestId = activeRequestId();
      actorInvokeSeq += 1;
      let timeoutId = 0;
      const timeoutError = new Promise((_, reject) => {
        timeoutId = setTimeout(
          () => reject(new Error(`dynamic worker invoke timed out after ${timeout}ms`)),
          timeout,
        );
      });
      const result = await Promise.race([
        callOp("op_dynamic_worker_invoke", {
          request_id: scopedRequestId,
          subrequest_id: `${scopedRequestId}:dynamic:${actorInvokeSeq}`,
          binding: bindingName,
          handle,
          method: request.method,
          url: request.url,
          headers: request.headers,
          body: Array.from(request.body),
        }),
        timeoutError,
      ]).finally(() => clearTimeout(timeoutId));
      await syncFrozenTime();
      if (!result || typeof result !== "object" || result.ok === false) {
        throw new Error(String(result?.error ?? "dynamic worker invoke failed"));
      }
      return new Response(toArrayBytes(result.body), {
        status: Number(result.status ?? 200),
        headers: Array.isArray(result.headers) ? result.headers : [],
      });
    },
  });

  const parseDynamicFactoryOptions = async (factory) => {
    if (typeof factory !== "function") {
      throw new Error("dynamic worker get() requires a factory function");
    }
    const options = await factory();
    if (!options || typeof options !== "object" || Array.isArray(options)) {
      throw new Error("dynamic worker factory must return an options object");
    }
    return options;
  };

  /**
   * @typedef {Object} DynamicWorkerConfig
   * @property {string} [source] Full worker source. If omitted, `entrypoint + modules` are used.
   * @property {string} [entrypoint] Entrypoint module path when using `modules`. Defaults to `worker.js`.
   * @property {Record<string, string>} [modules] Module graph map: `modulePath -> source`.
   * @property {Record<string, any>} [env] Env bindings for the dynamic worker.
   * String values stay opaque and are only resolved at host I/O boundaries; `RpcTarget` values become host RPC bindings.
   * @property {number} [timeout] Per-invoke timeout in ms. Default `5000`, max `60000`.
   */

  /**
   * @typedef {Object} DynamicWorkerStub
   * @property {string} worker Generated internal worker name.
   * @property {(input: Request|string, init?: RequestInit) => Promise<Response>} fetch Invoke the dynamic worker over HTTP-style fetch.
   */

  const createDynamicNamespace = (bindingName) => ({
    /**
     * Get or lazily create a dynamic worker by stable id.
     *
     * @param {string} id
     * @param {() => Promise<DynamicWorkerConfig>|DynamicWorkerConfig} factory
     * @returns {Promise<DynamicWorkerStub>}
     */
    async get(id, factory) {
      const instanceId = normalizeDynamicInstanceId(id);
      const scopedRequestId = activeRequestId();
      const lookup = await callOp("op_dynamic_worker_lookup", {
        request_id: scopedRequestId,
        binding: bindingName,
        id: instanceId,
      });
      await syncFrozenTime();
      if (!lookup || typeof lookup !== "object" || lookup.ok === false) {
        throw new Error(String(lookup?.error ?? "dynamic worker lookup failed"));
      }
      if (lookup.found === true) {
        return createDynamicWorkerStub(
          bindingName,
          String(lookup.handle ?? "").trim(),
          String(lookup.worker ?? ""),
          normalizeDynamicTimeout(lookup.timeout),
        );
      }

      const options = await parseDynamicFactoryOptions(factory);
      const source = resolveDynamicWorkerSource(options);
      const envInput = options.env;
      const envConfig = splitDynamicEnvInput(envInput);
      const timeout = normalizeDynamicTimeout(options.timeout);
      const result = await callOp("op_dynamic_worker_create", {
        request_id: scopedRequestId,
        binding: bindingName,
        id: instanceId,
        source,
        env: envConfig.stringEnv,
        host_rpc_bindings: envConfig.hostRpcBindings,
        timeout,
      });
      await syncFrozenTime();
      if (!result || typeof result !== "object" || result.ok === false) {
        throw new Error(String(result?.error ?? "dynamic worker create failed"));
      }
      const handle = String(result.handle ?? "").trim();
      if (!handle) {
        throw new Error("dynamic worker create returned an invalid handle");
      }
      return createDynamicWorkerStub(
        bindingName,
        handle,
        String(result.worker ?? ""),
        normalizeDynamicTimeout(result.timeout ?? timeout),
      );
    },
    /**
     * List active dynamic worker ids for this namespace in the current owner worker generation.
     *
     * @returns {Promise<string[]>}
     */
    async list() {
      const scopedRequestId = activeRequestId();
      const result = await callOp("op_dynamic_worker_list", {
        request_id: scopedRequestId,
        binding: bindingName,
      });
      await syncFrozenTime();
      if (!result || typeof result !== "object" || result.ok === false) {
        throw new Error(String(result?.error ?? "dynamic worker list failed"));
      }
      return Array.isArray(result.ids)
        ? result.ids.map((value) => String(value))
        : [];
    },
    /**
     * Delete a dynamic worker by stable id.
     *
     * @param {string} id
     * @returns {Promise<boolean>} True when an existing worker was deleted.
     */
    async delete(id) {
      const instanceId = normalizeDynamicInstanceId(id);
      const scopedRequestId = activeRequestId();
      const result = await callOp("op_dynamic_worker_delete", {
        request_id: scopedRequestId,
        binding: bindingName,
        id: instanceId,
      });
      await syncFrozenTime();
      if (!result || typeof result !== "object" || result.ok === false) {
        throw new Error(String(result?.error ?? "dynamic worker delete failed"));
      }
      return Boolean(result.deleted);
    },
  });

  const buildEnv = () => {
    const env = {};
    const actorBindingClasses = new Map();
    const kvBindings = Array.isArray(kvBindingsConfig)
      ? kvBindingsConfig
      : kvBindingsConfig && typeof kvBindingsConfig === "object"
        ? Object.entries(kvBindingsConfig).map(([name, binding]) => [name, typeof binding === "string" ? binding : name])
        : [];

    for (const binding of kvBindings) {
      const [envName, bindingName] = Array.isArray(binding)
        ? binding
        : [binding, binding];
      if (typeof envName !== "string" || envName.length === 0) {
        continue;
      }
      Object.defineProperty(env, envName, {
        value: createKvBinding(bindingName),
        enumerable: true,
        configurable: true,
        writable: true,
      });
    }

    const dynamicBindings = Array.isArray(dynamicBindingsConfig)
      ? dynamicBindingsConfig
      : dynamicBindingsConfig && typeof dynamicBindingsConfig === "object"
        ? Object.entries(dynamicBindingsConfig).map(([name, binding]) => [name, typeof binding === "string" ? binding : name])
        : [];
    for (const binding of dynamicBindings) {
      const [envName, bindingName] = Array.isArray(binding)
        ? binding
        : [binding, binding];
      if (typeof envName !== "string" || envName.trim().length === 0) {
        continue;
      }
      Object.defineProperty(env, envName.trim(), {
        value: createDynamicNamespace(String(bindingName ?? envName).trim()),
        enumerable: true,
        configurable: true,
        writable: true,
      });
    }

    const dynamicRpcBindings = Array.isArray(dynamicRpcBindingsConfig)
      ? dynamicRpcBindingsConfig
      : dynamicRpcBindingsConfig && typeof dynamicRpcBindingsConfig === "object"
        ? Object.entries(dynamicRpcBindingsConfig).map(([name, binding]) => [name, typeof binding === "string" ? binding : name])
        : [];
    for (const binding of dynamicRpcBindings) {
      const [envName, bindingName] = Array.isArray(binding)
        ? binding
        : [binding, binding];
      if (typeof envName !== "string" || envName.trim().length === 0) {
        continue;
      }
      Object.defineProperty(env, envName.trim(), {
        value: createDynamicHostRpcNamespace(String(bindingName ?? envName).trim()),
        enumerable: true,
        configurable: true,
        writable: true,
      });
    }

    const dynamicEnv = Array.isArray(dynamicEnvConfig)
      ? dynamicEnvConfig
      : dynamicEnvConfig && typeof dynamicEnvConfig === "object"
        ? Object.entries(dynamicEnvConfig)
        : [];
    for (const entry of dynamicEnv) {
      const [envName, value] = Array.isArray(entry) ? entry : [null, null];
      if (typeof envName !== "string" || envName.trim().length === 0) {
        continue;
      }
      Object.defineProperty(env, envName.trim(), {
        value: String(value ?? ""),
        enumerable: true,
        configurable: true,
        writable: true,
      });
    }

    const actorBindings = Array.isArray(actorBindingsConfig) ? actorBindingsConfig : [];
    for (const entry of actorBindings) {
      let bindingName = "";
      let className = "";
      if (Array.isArray(entry) && entry.length >= 2) {
        bindingName = String(entry[0] ?? "").trim();
        className = String(entry[1] ?? "").trim();
      } else if (entry && typeof entry === "object") {
        bindingName = String(entry.binding ?? "").trim();
        className = String(entry.class ?? entry.class_name ?? "").trim();
      }
      if (!bindingName || !className) {
        continue;
      }
      const envName = bindingName;
      actorBindingClasses.set(bindingName, className);
      Object.defineProperty(env, envName, {
        value: createActorNamespace(bindingName),
        enumerable: true,
        configurable: true,
        writable: true,
      });
    }

    return { env, actorBindingClasses };
  };

  const actorClassRegistry = globalThis.__dd_actor_classes ?? {};

  const invokeActorClass = async (actorCall, request, env, actorBindingClasses) => {
    if (!actorCall || typeof actorCall !== "object") {
      throw new Error("actor invoke config is missing");
    }
    const binding = String(actorCall.binding ?? "").trim();
    const actorKey = String(actorCall.key ?? "").trim();
    if (!binding || !actorKey) {
      throw new Error("actor invoke requires binding and key");
    }
    const className = actorBindingClasses.get(binding);
    if (!className) {
      throw new Error(`actor binding not declared for worker: ${binding}`);
    }
    const actorClass = actorClassRegistry[className];
    if (typeof actorClass !== "function") {
      throw new Error(`actor class export not found: ${className}`);
    }

    const cacheKey = `${binding}\u001f${actorKey}`;
    let entry = actorInstances.get(cacheKey);
    if (!entry) {
      entry = {
        binding,
        actorKey,
        socketBindings: new Map(),
        socketRuntime: null,
        transportBindings: new Map(),
        transportRuntime: null,
      };
      const constructorState = createActorRuntimeState(entry, requestId, false, false);
      await constructorState.__dd_socket_runtime.refreshOpenHandles();
      await constructorState.__dd_transport_runtime.refreshOpenHandles();
      const previousWebSocketForCtor = globalThis.WebSocket;
      const previousWebTransportForCtor = globalThis.WebTransportSession;
      globalThis.WebSocket = constructorState.__dd_socket_runtime.WebSocket;
      globalThis.WebTransportSession = constructorState.__dd_transport_runtime.WebTransportSession;
      try {
        entry = {
          ...entry,
          instance: new actorClass(constructorState, env),
        };
      } finally {
        if (previousWebSocketForCtor === undefined) {
          delete globalThis.WebSocket;
        } else {
          globalThis.WebSocket = previousWebSocketForCtor;
        }
        if (previousWebTransportForCtor === undefined) {
          delete globalThis.WebTransportSession;
        } else {
          globalThis.WebTransportSession = previousWebTransportForCtor;
        }
      }
      actorInstances.set(cacheKey, entry);
    }
    entry.binding = binding;
    entry.actorKey = actorKey;

    const kind = String(actorCall.kind ?? "");
    const scopedState = createActorRuntimeState(entry, requestId, kind === "fetch", kind === "fetch");
    const previousWebSocket = globalThis.WebSocket;
    const previousWebTransport = globalThis.WebTransportSession;
    globalThis.WebSocket = scopedState.__dd_socket_runtime.WebSocket;
    globalThis.WebTransportSession = scopedState.__dd_transport_runtime.WebTransportSession;
    const receiver = new Proxy(entry.instance, {
      get(target, prop, receiverValue) {
        if (prop === "state") {
          return scopedState;
        }
        return Reflect.get(target, prop, target);
      },
      set(target, prop, value, receiverValue) {
        if (prop === "state") {
          return true;
        }
        return Reflect.set(target, prop, value, target);
      },
    });

    try {
      await scopedState.__dd_socket_runtime.refreshOpenHandles();
      await scopedState.__dd_transport_runtime.refreshOpenHandles();
      if (kind === "fetch") {
        if (typeof entry.instance.fetch !== "function") {
          throw new Error(`actor class does not define fetch(): ${className}`);
        }
        return await entry.instance.fetch.call(receiver, request);
      }
      if (kind === "method") {
        const methodName = String(actorCall.name ?? "").trim();
        if (actorMethodNameIsBlocked(methodName)) {
          throw new Error(`actor method is blocked: ${methodName}`);
        }
        const method = entry.instance[methodName];
        if (typeof method !== "function") {
          throw new Error(`actor method not found: ${methodName}`);
        }
        const args = decodeRpcArgs(toArrayBytes(actorCall.args));
        const value = await method.apply(receiver, args);
        const encoded = await encodeRpcResult(value);
        return new Response(encoded, {
          status: 200,
          headers: [["content-type", "application/octet-stream"]],
        });
      }
      if (kind === "message") {
        const handle = String(actorCall.handle ?? "").trim();
        if (!handle) {
          throw new Error("actor message invoke requires socket handle");
        }
        const ws = new globalThis.WebSocket(handle);
        const raw = toArrayBytes(actorCall.data);
        const message = actorCall.is_text === true ? Deno.core.decode(raw) : raw;
        await ws.__dd_dispatchMessage(message);
        return new Response(null, { status: 204 });
      }
      if (kind === "transport_datagram" || kind === "datagram") {
        const handle = String(actorCall.handle ?? "").trim();
        if (!handle) {
          throw new Error("actor transport datagram invoke requires transport handle");
        }
        const session = new globalThis.WebTransportSession(handle);
        await session.__dd_dispatchDatagram(toArrayBytes(actorCall.data));
        return new Response(null, { status: 204 });
      }
      if (kind === "transport_stream" || kind === "stream") {
        const handle = String(actorCall.handle ?? "").trim();
        if (!handle) {
          throw new Error("actor transport stream invoke requires transport handle");
        }
        const session = new globalThis.WebTransportSession(handle);
        await session.__dd_dispatchStreamChunk(toArrayBytes(actorCall.data));
        return new Response(null, { status: 204 });
      }
      if (kind === "transport_close" || kind === "transport") {
        const handle = String(actorCall.handle ?? "").trim();
        if (!handle) {
          throw new Error("actor transport close invoke requires transport handle");
        }
        const session = new globalThis.WebTransportSession(handle);
        session.__dd_dispatchClose(
          Number(actorCall.code ?? 0),
          String(actorCall.reason ?? ""),
        );
        return new Response(null, { status: 204 });
      }
      if (kind === "close") {
        const handle = String(actorCall.handle ?? "").trim();
        if (!handle) {
          throw new Error("actor close invoke requires socket handle");
        }
        new globalThis.WebSocket(handle);
        return new Response(null, { status: 204 });
      }
      throw new Error(`unsupported actor invoke kind: ${kind}`);
    } finally {
      if (previousWebSocket === undefined) {
        delete globalThis.WebSocket;
      } else {
        globalThis.WebSocket = previousWebSocket;
      }
      if (previousWebTransport === undefined) {
        delete globalThis.WebTransportSession;
      } else {
        globalThis.WebTransportSession = previousWebTransport;
      }
    }
  };

  const invokeHostRpcCall = async (hostRpcCall) => {
    if (!hostRpcCall || typeof hostRpcCall !== "object") {
      throw new Error("host rpc call config is missing");
    }
    const targetId = String(hostRpcCall.target_id ?? hostRpcCall.targetId ?? "").trim();
    const methodName = String(hostRpcCall.method ?? "").trim();
    if (!targetId) {
      throw new Error("host rpc target id is missing");
    }
    if (!methodName) {
      throw new Error("host rpc method is missing");
    }
    if (hostRpcMethodNameIsBlocked(methodName)) {
      throw new Error(`host rpc method is blocked: ${methodName}`);
    }
    const target = hostRpcTargets.get(targetId);
    if (!target) {
      throw new Error("host rpc target is unavailable");
    }
    const method = target[methodName];
    if (typeof method !== "function") {
      throw new Error(`host rpc method not found: ${methodName}`);
    }
    const args = decodeRpcArgs(toArrayBytes(hostRpcCall.args));
    const value = await method.apply(target, args);
    const encoded = await encodeRpcResult(value);
    return new Response(encoded, {
      status: 200,
      headers: [["content-type", "application/octet-stream"]],
    });
  };

  const emitWaitUntilDone = async (timedOut) => {
    if (waitUntilDoneSent) {
      return;
    }
    waitUntilDoneSent = true;
    await syncFrozenTime();
    callOp(
      "op_emit_wait_until_done",
      JSON.stringify({
        request_id: requestId,
        completion_token: completionToken,
        wait_until_count: waitUntilPromises.length,
        timed_out: timedOut,
      }),
    );
  };

  const emitResponseStart = async (status, headers) => {
    await syncFrozenTime();
    callOp(
      "op_emit_response_start",
      JSON.stringify({
        request_id: requestId,
        completion_token: completionToken,
        status,
        headers,
      }),
    );
  };

  const emitResponseChunk = async (chunk) => {
    await syncFrozenTime();
    const bytes = toUtf8Bytes(chunk);
    callOp(
      "op_emit_response_chunk",
      JSON.stringify({
        request_id: requestId,
        completion_token: completionToken,
        chunk: Array.from(bytes),
      }),
    );
    return bytes;
  };

  const waitForWaitUntils = async () => {
    if (waitUntilPromises.length === 0) {
      return true;
    }

    let timeoutId = 0;
    const timeout = new Promise((resolve) => {
      timeoutId = setTimeout(() => resolve(false), 30_000);
    });
    const settled = Promise.allSettled(waitUntilPromises).then(() => true);
    try {
      return await Promise.race([settled, timeout]);
    } finally {
      clearTimeout(timeoutId);
    }
  };

  const trackWaitUntil = (promise) => {
    const tracked = Promise.resolve(promise).then(
      async (value) => {
        await syncFrozenTime();
        return { ok: true, value };
      },
      async (error) => {
        await syncFrozenTime();
        try {
          console.warn(
            "waitUntil promise rejected",
            String((error && (error.stack || error.message)) || error),
          );
        } catch {
          // Ignore logging failures in isolate userland.
        }
        return { ok: false, error: String((error && (error.stack || error.message)) || error) };
      },
    );
    waitUntilPromises.push(tracked);
    return tracked;
  };

  (async () => {
    let restoreHostFetch = null;
    try {
      await syncFrozenTime();
      restoreHostFetch = installHostFetch();
      const requestBody = hasRequestBodyStream
        ? createRequestBodyStream()
        : input.body?.length
          ? new Uint8Array(input.body)
          : undefined;
      const request = new Request(input.url, {
        method: input.method,
        headers: input.headers,
        body: requestBody,
        signal: controller.signal,
      });
      const envResult = buildEnv();
      const env = envResult.env;
      const actorBindingClasses = envResult.actorBindingClasses;
      const ctx = {
        requestId: input.request_id,
        signal: controller.signal,
        waitUntil(promise) {
          return trackWaitUntil(promise);
        },
        async sleep(millis) {
          await sleep(millis);
          await syncFrozenTime();
        },
      };

      const response = hostRpcCallConfig
        ? await invokeHostRpcCall(hostRpcCallConfig)
        : actorCallConfig
          ? await invokeActorClass(actorCallConfig, request, env, actorBindingClasses)
          : await worker.fetch(request, env, ctx);
      await syncFrozenTime();

      const isWebSocketAcceptResponse = Boolean(
        response
          && typeof response === "object"
          && response.__dd_websocket_accept === true,
      );
      const isTransportAcceptResponse = Boolean(
        response
          && typeof response === "object"
          && response.__dd_transport_accept === true,
      );
      if (!(response instanceof Response) && !isWebSocketAcceptResponse && !isTransportAcceptResponse) {
        throw new Error("Worker fetch() must return a Response");
      }

      const responseHeaders = (isWebSocketAcceptResponse || isTransportAcceptResponse)
        ? new Headers(response.headers ?? [])
        : response.headers;
      const status = isWebSocketAcceptResponse
        ? Number(response.status ?? 101)
        : isTransportAcceptResponse
          ? Number(response.status ?? 200)
          : response.status;
      const headers = Array.from(responseHeaders.entries());
      await emitResponseStart(status, headers);

      const bodyBytes = [];
      if (!isWebSocketAcceptResponse && response.body) {
        const reader = response.body.getReader();
        while (true) {
          const { done, value } = await reader.read();
          if (done) {
            break;
          }
          const chunk = toUtf8Bytes(value);
          if (chunk.length === 0) {
            continue;
          }
          const emitted = await emitResponseChunk(chunk);
          bodyBytes.push(...emitted);
        }
      }

      return {
        status,
        headers,
        body: bodyBytes,
      };
    } finally {
      if (typeof restoreHostFetch === "function") {
        restoreHostFetch();
      }
      inflightRequests.delete(requestId);
    }
  })()
    .then(async (result) => {
      callOp(
        "op_emit_completion",
        JSON.stringify({
          request_id: requestId,
          completion_token: completionToken,
          ok: true,
          wait_until_count: waitUntilPromises.length,
          result,
        }),
      );

      await emitWaitUntilDone(!(await waitForWaitUntils()));
    })
    .catch(async (error) => {
      const message = String((error && (error.stack || error.message)) || error);
      callOp(
        "op_emit_completion",
        JSON.stringify({
          request_id: requestId,
          completion_token: completionToken,
          ok: false,
          wait_until_count: waitUntilPromises.length,
          error: message,
        }),
      );

      await emitWaitUntilDone(!(await waitForWaitUntils()));
    });
})();
