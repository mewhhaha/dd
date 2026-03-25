(() => {
  const requestId = __REQUEST_ID__;
  const completionToken = __COMPLETION_TOKEN__;
  const workerName = __WORKER_NAME__;
  const kvBindingsConfig = __KV_BINDINGS_JSON__;
  const actorBindingsConfig = __ACTOR_BINDINGS_JSON__;
  const actorCallConfig = __ACTOR_CALL_JSON__;
  const hasRequestBodyStream = __HAS_REQUEST_BODY_STREAM__;
  const worker = globalThis.__dd_worker;

  if (worker === undefined) {
    throw new Error("Worker is not installed");
  }

  const inflightRequests = globalThis.__dd_inflight_requests ??= new Map();
  const input = __REQUEST_JSON__;
  const controller = new AbortController();
  const waitUntilPromises = [];
  let waitUntilDoneSent = false;
  let actorInvokeSeq = 0;

  inflightRequests.set(requestId, controller);

  const callOp = (name, ...args) => {
    const op = Deno?.core?.ops?.[name];
    if (typeof op !== "function") {
      return undefined;
    }
    return op(...args);
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

  const toArrayBytes = (value) => new Uint8Array(Array.isArray(value) ? value : []);

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

  const encodeSocketSendPayload = (value, kind) => {
    if (kind != null) {
      const normalizedKind = String(kind).toLowerCase();
      if (normalizedKind === "text") {
        return {
          kind: "text",
          value: String(value),
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
      value: String(value ?? ""),
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

    const consumeCloseEvents = async (target) => {
      const result = await callOp("op_actor_socket_consume_close", {
        request_id: runtimeRequestId,
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
            request_id: runtimeRequestId,
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
            request_id: runtimeRequestId,
            handle: normalizedHandle,
            code: normalizedCode,
            reason: reason == null ? "" : String(reason),
          });
          await syncFrozenTime();
          if (result && typeof result === "object" && result.ok === false) {
            throw new Error(String(result.error ?? "socket close failed"));
          }
        },
        __dd_dispatch(type, event) {
          const current = listeners.get(type);
          if (current) {
            for (const listener of current) {
              try {
                listener.call(this, event);
              } catch {
                // Ignore listener failures.
              }
            }
          }
        },
        __dd_dispatchMessage(value) {
          const event = { type: "message", data: value, target: this };
          this.__dd_dispatch("message", event);
          if (typeof onmessage === "function") {
            try {
              onmessage.call(this, event);
            } catch {
              // Ignore onmessage failures.
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
          response: new Response(null, {
            status: 101,
            headers: Array.from(headers.entries()),
          }),
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
          request_id: runtimeRequestId,
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

  const createActorRuntimeState = (entry, runtimeRequestId, allowSocketAccept) => {
    const socketRuntime = createActorSocketRuntime(entry, runtimeRequestId, allowSocketAccept);
    entry.socketRuntime = socketRuntime;
    return {
      id: {
        toString() {
          return entry.actorKey;
        },
      },
      storage: createActorStorageBinding(runtimeRequestId),
      sockets: socketRuntime.sockets,
      __dd_socket_runtime: socketRuntime,
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
        return new Response(toArrayBytes(result.body), {
          status: Number(result.status ?? 200),
          headers: Array.isArray(result.headers) ? result.headers : [],
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
      };
      const constructorState = createActorRuntimeState(entry, requestId, false);
      await constructorState.__dd_socket_runtime.refreshOpenHandles();
      const previousWebSocketForCtor = globalThis.WebSocket;
      globalThis.WebSocket = constructorState.__dd_socket_runtime.WebSocket;
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
      }
      actorInstances.set(cacheKey, entry);
    }
    entry.binding = binding;
    entry.actorKey = actorKey;

    const kind = String(actorCall.kind ?? "");
    const scopedState = createActorRuntimeState(entry, requestId, kind === "fetch");
    const previousWebSocket = globalThis.WebSocket;
    globalThis.WebSocket = scopedState.__dd_socket_runtime.WebSocket;
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
        ws.__dd_dispatchMessage(message);
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
    }
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
    try {
      await syncFrozenTime();
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

      const response = actorCallConfig
        ? await invokeActorClass(actorCallConfig, request, env, actorBindingClasses)
        : await worker.fetch(request, env, ctx);
      await syncFrozenTime();

      if (!(response instanceof Response)) {
        throw new Error("Worker fetch() must return a Response");
      }

      const headers = Array.from(response.headers.entries());
      await emitResponseStart(response.status, headers);

      const bodyBytes = [];
      if (response.body && typeof response.body.getReader === "function") {
        const reader = response.body.getReader();
        try {
          while (true) {
            const { value, done } = await reader.read();
            if (done) {
              break;
            }
            const emitted = await emitResponseChunk(value);
            bodyBytes.push(...emitted);
          }
        } finally {
          if (typeof reader.releaseLock === "function") {
            try {
              reader.releaseLock();
            } catch {
              // Ignore release failures.
            }
          }
        }
      } else {
        const emitted = await emitResponseChunk(new Uint8Array(await response.arrayBuffer()));
        bodyBytes.push(...emitted);
      }

      return {
        status: response.status,
        headers,
        body: bodyBytes,
      };
    } finally {
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
