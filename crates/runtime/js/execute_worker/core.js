globalThis.__dd_execute_worker = (payload) => {
  const requestId = String(payload?.request_id ?? "");
  const deploymentConfig = globalThis.__dd_worker_deployment ?? null;
  if (!deploymentConfig || typeof deploymentConfig !== "object") {
    throw new Error("Worker deployment config is not installed");
  }
  const workerName = String(deploymentConfig?.worker_name ?? "");
  const kvBindingsConfig = deploymentConfig?.kv_bindings ?? [];
  const memoryBindingsConfig = deploymentConfig?.memory_bindings ?? [];
  const serviceBindingsConfig = deploymentConfig?.service_bindings ?? [];
  const memoryCallConfig = payload?.memory_call ?? null;
  const requestBodyStreamHandle = Math.max(
    0,
    Math.trunc(Number(payload?.request_body_stream_handle ?? 0) || 0),
  );
  const requestContextHandle = Math.max(
    0,
    Math.trunc(Number(payload?.request_context_handle ?? 0) || 0),
  );
  const completionHandle = Math.max(
    0,
    Math.trunc(Number(payload?.completion_handle ?? 0) || 0),
  );
  const memoryRequestScopeHandle = Math.max(
    0,
    Math.trunc(Number(payload?.memory_request_scope_handle ?? 0) || 0),
  );
  const hasRequestBodyStream = requestBodyStreamHandle > 0;
  const streamResponse = payload?.stream_response === true;
  const maxResponseBodyBytes = Number(payload?.max_response_body_bytes ?? 0);
  const maxRequestBodyBytes = Number(payload?.max_request_body_bytes ?? 0);
  const worker = globalThis.__dd_worker;

  if (worker === undefined) {
    throw new Error("Worker is not installed");
  }

  const inflightRequests = globalThis.__dd_inflight_requests ??= new Map();
  const inflightRequestsByContextHandle =
    globalThis.__dd_inflight_requests_by_context_handle ??= new Map();
  const input = {
    method: String(payload?.method ?? "GET"),
    url: String(payload?.url ?? ""),
    headers: Array.isArray(payload?.headers) ? payload.headers : [],
    body: payload?.body instanceof Uint8Array ? payload.body : new Uint8Array(),
    request_id: String(payload?.input_request_id ?? ""),
  };
  const cacheBypassStale = Array.isArray(input?.headers)
    && input.headers.some(([name, value]) => {
      const key = String(name || "").toLowerCase();
      if (key !== "x-dd-cache-bypass-stale") {
        return false;
      }
      const normalized = String(value || "").toLowerCase();
      return normalized === "1" || normalized === "true" || normalized === "yes";
    });
  const controller = new AbortController();
  const asyncContext = globalThis.__dd_async_context;
  const requestContext = {
    requestId,
    controller,
    waitUntilPromises: [],
    requestContextHandle,
    completionHandle,
    memoryRequestScopeHandle,
    requestBodyStreamHandle,
    memoryEntry: null,
    memoryTxnScope: null,
    socketRuntimeProvider: null,
    cacheBypassStale,
  };

  const currentRequestContext = (required = true) => {
    const current = asyncContext?.getStore?.() ?? null;
    if (!current && required) {
      throw new Error("request scope is unavailable");
    }
    return current;
  };



  const inflightRequest = {
    controller,
    requestContextHandle,
    memoryRequestScopeHandle,
    requestBodyStreamHandle,
  };
  inflightRequests.set(requestId, inflightRequest);
  if (requestContextHandle > 0) {
    inflightRequestsByContextHandle.set(requestContextHandle, inflightRequest);
  }

  const callOp = (name, ...args) => {
    const op = Deno?.core?.ops?.[name];
    if (typeof op !== "function") {
      return undefined;
    }
    return op(...args);
  };

  const recordMemoryProfile = (metric, durationMs, items = 1) => {
    const op = Deno?.core?.ops?.op_memory_profile_record_js;
    if (typeof op !== "function") {
      return;
    }
    op(
      String(metric),
      Math.max(0, Math.round(Number(durationMs ?? 0) * 1000)),
      Math.max(1, Math.trunc(Number(items ?? 1) || 1)),
    );
  };

  const activeRequestId = () => {
    const scoped = String(currentRequestContext().requestId ?? "").trim();
    if (!scoped) {
      throw new Error("worker request scope is unavailable");
    }
    return scoped;
  };
  if (typeof globalThis.__dd_get_runtime_request_id !== "function") {
    Object.defineProperty(globalThis, "__dd_get_runtime_request_id", {
      value: activeRequestId,
      enumerable: false,
      configurable: true,
      writable: true,
    });
  }
  const activeRequestContextHandle = () => {
    const handle = Math.max(
      0,
      Math.trunc(Number(currentRequestContext().requestContextHandle ?? 0) || 0),
    );
    if (handle <= 0) {
      throw new Error("request context handle is unavailable");
    }
    return handle;
  };
  if (typeof globalThis.__dd_get_runtime_request_context_handle !== "function") {
    Object.defineProperty(globalThis, "__dd_get_runtime_request_context_handle", {
      value: activeRequestContextHandle,
      enumerable: false,
      configurable: true,
      writable: true,
    });
  }
  const activeCacheBypassStale = () => Boolean(
    currentRequestContext(false)?.cacheBypassStale,
  );
  if (typeof globalThis.__dd_get_cache_bypass_stale !== "function") {
    Object.defineProperty(globalThis, "__dd_get_cache_bypass_stale", {
      value: activeCacheBypassStale,
      enumerable: false,
      configurable: true,
      writable: true,
    });
  }

  const memoryScopedScopeHandle = (entry) => {
    const current = currentRequestContext(false);
    if (current?.memoryEntry === entry) {
      return Math.max(
        0,
        Math.trunc(Number(current.memoryRequestScopeHandle ?? 0) || 0),
      );
    }
    return 0;
  };

  const withMemoryTxnScope = (scope, callback) => {
    const current = currentRequestContext();
    const previousScope = current.memoryTxnScope;
    current.memoryTxnScope = scope;
    try {
      return callback();
    } finally {
      current.memoryTxnScope = previousScope;
    }
  };

  const currentLocalSocketRuntime = (binding, memoryKey) => {
    const current = currentRequestContext(false);
    if (!current?.memoryEntry) {
      return null;
    }
    if (
      current.memoryEntry.binding !== binding
      || current.memoryEntry.memoryKey !== memoryKey
    ) {
      return null;
    }
    const provider = current.socketRuntimeProvider;
    if (typeof provider !== "function") {
      throw new Error(`memory same-lane socket runtime is unavailable`);
    }
    const runtime = provider();
    if (!runtime || typeof runtime.listOpenHandles !== "function") {
      throw new Error(`memory same-lane socket runtime is unavailable`);
    }
    return runtime;
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
  const syncFrozenTimeNow = () => {
    const value = callOp("op_time_boundary_now");
    const boundary = normalizeBoundaryValue(value);
    if (boundary && typeof globalThis.__dd_set_time === "function") {
      globalThis.__dd_set_time(boundary.nowMs, boundary.perfMs);
    }
  };
  if (typeof globalThis.__dd_sync_time_boundary !== "function") {
    Object.defineProperty(globalThis, "__dd_sync_time_boundary", {
      value: syncFrozenTime,
      enumerable: false,
      configurable: true,
      writable: true,
    });
  }

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

  const toByteChunk = (value) => {
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
    return toUtf8Bytes(value);
  };

  const concatByteChunks = (chunks, totalLength) => {
    if (!Array.isArray(chunks) || chunks.length === 0 || totalLength <= 0) {
      return new Uint8Array();
    }
    if (chunks.length === 1 && chunks[0].byteLength === totalLength) {
      return chunks[0];
    }
    const output = new Uint8Array(totalLength);
    let offset = 0;
    for (const chunk of chunks) {
      output.set(chunk, offset);
      offset += chunk.byteLength;
    }
    return output;
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

      const payload = await callOp("op_request_body_read", requestBodyStreamHandle);
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
      const bodyHandle = Math.max(0, Math.trunc(Number(payload.body_handle ?? 0) || 0));
      return {
        value: callOp("op_http_take_prepared_body", bodyHandle),
        done: false,
      };
    };

    return Object.freeze({
      getReader() {
        return {
          read,
          releaseLock() {
            released = true;
          },
          async cancel() {
            done = true;
            await callOp("op_request_body_cancel", requestBodyStreamHandle);
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
    });
  };

  const decodeStoredValue = (encoding, valueHandle, context) => {
    const handle = Math.max(0, Math.trunc(Number(valueHandle ?? 0) || 0));
    const bytes = callOp("op_http_take_prepared_body", handle);
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

  const createKvBinding = (bindingName) => Object.freeze({
    async get(key) {
      const result = await callOp("op_kv_get_value", bindingName, String(key));
      syncFrozenTimeNow();
      if (!result.ok) throw new Error(`kv get ${key}: ${result.error}`);
      return result.found ? decodeStoredValue(result.encoding, result.value_handle, "kv get") : null;
    },
    async put(key, value) {
      const normalizedKey = String(key);
      const result = typeof value === "string"
        ? await callOp("op_kv_put", bindingName, normalizedKey, value)
        : await callOp("op_kv_put_value_bytes", bindingName, normalizedKey,
          "v8sc", new Uint8Array(Deno.core.serialize(value, { forStorage: true })));
      syncFrozenTimeNow();
      if (!result.ok) throw new Error(`kv put ${normalizedKey}: ${result.error}`);
    },
    async delete(key) {
      const result = await callOp("op_kv_delete", bindingName, String(key));
      syncFrozenTimeNow();
      if (!result.ok) throw new Error(`kv delete ${key}: ${result.error}`);
    },
    async list(options = {}) {
      const prefix = String(options.prefix ?? "");
      const limit = options.limit ?? 100;
      if (!Number.isInteger(limit) || limit < 1 || limit > 1000) {
        throw new Error(`kv list limit must be an integer between 1 and 1000: ${limit}`);
      }
      const result = await callOp("op_kv_list", bindingName, prefix, limit);
      syncFrozenTimeNow();
      if (!result.ok) throw new Error(`kv list ${prefix}: ${result.error}`);
      return result.entries.map((entry) => ({
        key: entry.key,
        value: decodeStoredValue(entry.encoding, entry.value_handle, "kv list"),
      }));
    },
  });

  const isBinaryLike = (value) => (
    value instanceof ArrayBuffer
    || ArrayBuffer.isView(value)
    || value instanceof Uint8Array
  );
