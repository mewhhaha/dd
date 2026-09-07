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
    memoryRequestId: null,
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

  const callOpAny = (names, ...args) => {
    for (const name of names) {
      const result = callOp(name, ...args);
      if (result !== undefined) {
        return result;
      }
    }
    return undefined;
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

  const memoryScopedRequestId = (entry, runtimeRequestId) => {
    const current = currentRequestContext(false);
    if (current?.memoryEntry === entry && current.memoryRequestId) {
      return current.memoryRequestId;
    }
    if (current?.requestId) {
      return String(current.requestId);
    }
    const fallback = String(runtimeRequestId ?? "").trim();
    if (fallback) {
      return fallback;
    }
    return activeRequestId();
  };
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

  const HOST_FETCH_REDIRECT_STATUSES = new Set([301, 302, 303, 307, 308]);
  const HOST_FETCH_MAX_REDIRECTS = 10;

  const normalizeHostFetchInput = async (inputValue, initValue = undefined) => {
    let method = "GET";
    let url = "";
    let headers = [];
    let body = new Uint8Array();
    let signal = undefined;
    let redirect = "follow";

    if (inputValue instanceof Request) {
      method = String(inputValue.method || "GET").toUpperCase();
      url = String(inputValue.url || "");
      headers = Array.from(inputValue.headers.entries());
      body = new Uint8Array(await inputValue.arrayBuffer());
      signal = inputValue.signal;
      redirect = String(inputValue.redirect || "follow");
    } else {
      method = String(initValue?.method ?? "GET").toUpperCase();
      const raw = String(inputValue ?? "");
      url = raw;
      headers = toHeaderEntries(initValue?.headers);
      body = toUtf8Bytes(initValue?.body);
      signal = initValue?.signal;
      redirect = String(initValue?.redirect ?? "follow");
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
      if (initValue.redirect != null) {
        redirect = String(initValue.redirect);
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
      redirect,
    };
  };

  const rewriteMethodForRedirect = (status, method) => {
    if (status === 303 && method !== "HEAD") {
      return "GET";
    }
    if ((status === 301 || status === 302) && method !== "GET" && method !== "HEAD") {
      return "GET";
    }
    return method;
  };

  const stripRedirectBodyHeaders = (headers) => headers.filter(([name]) => {
    const lower = String(name || "").toLowerCase();
    return lower !== "content-type"
      && lower !== "content-length"
      && lower !== "content-encoding"
      && lower !== "content-language"
      && lower !== "content-location"
      && lower !== "transfer-encoding";
  });

  const stripCrossOriginRedirectHeaders = (headers) => headers.filter(([name]) => {
    const lower = String(name || "").toLowerCase();
    return lower !== "authorization"
      && lower !== "proxy-authorization"
      && lower !== "cookie";
  });

  const checkHostFetchUrl = async (requestContextHandle, url) => {
    const checked = await callOp(
      "op_http_check_url",
      requestContextHandle,
      url,
    );
    await syncFrozenTime();
    if (!checked || typeof checked !== "object" || checked.ok === false) {
      throw new Error(String(checked?.error ?? "host fetch URL check failed"));
    }
    return {
      url: String(checked.url || url),
      clientRid: Math.max(0, Math.trunc(Number(checked.client_rid ?? 0) || 0)),
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

  const abortErrorForSignal = (signal) => {
    const reason = signal?.reason;
    if (reason instanceof Error) {
      if (!reason.name) {
        reason.name = "AbortError";
      }
      return reason;
    }
    if (typeof DOMException === "function") {
      return new DOMException(String(reason ?? "Aborted"), "AbortError");
    }
    const error = new Error(String(reason ?? "Aborted"));
    error.name = "AbortError";
    return error;
  };

  const raceAbortSignal = (promise, signal) => {
    if (!signal) {
      return promise;
    }
    if (signal.aborted) {
      return Promise.reject(abortErrorForSignal(signal));
    }
    return new Promise((resolve, reject) => {
      const onAbort = () => {
        cleanup();
        reject(abortErrorForSignal(signal));
      };
      const cleanup = () => {
        signal.removeEventListener("abort", onAbort);
      };
      signal.addEventListener("abort", onAbort, { once: true });
      promise.then(
        (value) => {
          cleanup();
          resolve(value);
        },
        (error) => {
          cleanup();
          reject(error);
        },
      );
    });
  };

  if (typeof globalThis.__dd_install_host_fetch !== "function") {
    Object.defineProperty(globalThis, "__dd_install_host_fetch", {
      value: function installHostFetch() {
        if (typeof globalThis.__dd_host_fetch === "function") {
          return;
        }
        const rawFetch = globalThis.__dd_raw_host_fetch ?? globalThis.fetch;
        if (typeof rawFetch !== "function") {
          throw new TypeError("fetch is not available in this runtime");
        }
        Object.defineProperty(globalThis, "__dd_raw_host_fetch", {
          value: rawFetch,
          enumerable: false,
          configurable: true,
          writable: true,
        });
        const scopedFetch = async (inputValue, initValue = undefined) => {
          const run = async () => {
            const current = currentRequestContext();
            const normalized = await normalizeHostFetchInput(inputValue, initValue);
            const normalizedHeadersHandle = callOp(
              "op_http_store_prepared_headers",
              normalized.headers,
            );
            const normalizedBodyHandle = callOp(
              "op_http_store_prepared_body",
              normalized.body,
            );
            const prepared = await callOp(
              "op_http_prepare",
              current.requestContextHandle,
              normalized.method,
              normalized.url,
              Math.max(0, Math.trunc(Number(normalizedHeadersHandle ?? 0) || 0)),
              Math.max(0, Math.trunc(Number(normalizedBodyHandle ?? 0) || 0)),
            );
            await syncFrozenTime();
            if (!prepared || typeof prepared !== "object" || prepared.ok === false) {
              throw new Error(String(prepared?.error ?? "host fetch prepare failed"));
            }
            const signal = composeAbortSignal([current.controller.signal, normalized.signal]);
            let method = String(prepared.method || "GET");
            let url = String(prepared.url || normalized.url);
            let clientRid = Math.max(
              0,
              Math.trunc(Number(prepared.client_rid ?? 0) || 0),
            );
            let headers = callOp(
              "op_http_take_prepared_headers",
              Math.max(0, Math.trunc(Number(prepared.headers_handle ?? 0) || 0)),
            );
            if (!Array.isArray(headers)) {
              headers = [];
            }
            const preparedBodyHandle = Number(prepared.body_handle ?? 0);
            let body = preparedBodyHandle > 0
              ? callOp("op_http_take_prepared_body", preparedBodyHandle)
              : undefined;
            if (body && body.byteLength === 0) {
              body = undefined;
            }
            const redirectMode = normalized.redirect === "error"
              || normalized.redirect === "manual"
              ? normalized.redirect
              : "follow";
            let redirectsRemaining = HOST_FETCH_MAX_REDIRECTS;
            for (;;) {
              const client = clientRid > 0 ? new RuntimeHttpClient(clientRid) : undefined;
              clientRid = 0;
              let response;
              try {
                response = await raceAbortSignal(
                  rawFetch(new Request(url, {
                    method,
                    headers,
                    body,
                    signal,
                    redirect: "manual",
                    client,
                  })),
                  signal,
                );
              } finally {
                client?.close();
              }
              if (redirectMode === "manual" || !HOST_FETCH_REDIRECT_STATUSES.has(response.status)) {
                return response;
              }
              const location = response.headers.get("location");
              if (!location) {
                return response;
              }
              if (redirectMode === "error") {
                throw new TypeError(`host fetch redirect blocked: ${response.status}`);
              }
              if (redirectsRemaining <= 0) {
                throw new TypeError("host fetch exceeded redirect limit");
              }
              const nextUrl = new URL(location, response.url || url).toString();
              await response.body?.cancel?.();
              const previousOrigin = new URL(url).origin;
              const checked = await checkHostFetchUrl(current.requestContextHandle, nextUrl);
              if (new URL(checked.url).origin !== previousOrigin) {
                headers = stripCrossOriginRedirectHeaders(headers);
              }
              url = checked.url;
              clientRid = checked.clientRid;
              const nextMethod = rewriteMethodForRedirect(response.status, method);
              if (nextMethod !== method) {
                headers = stripRedirectBodyHeaders(headers);
                body = undefined;
              }
              method = nextMethod;
              redirectsRemaining -= 1;
            }
          };
          const current = currentRequestContext(false);
          if (current?.memoryEntry) {
            return gateMemoryOutput(current.memoryEntry, current.memoryRequestId || current.requestId, run);
          }
          return run();
        };
        Object.defineProperty(scopedFetch, "__dd_host_fetch", { value: true });
        Object.defineProperty(globalThis, "__dd_host_fetch", {
          value: scopedFetch,
          enumerable: false,
          configurable: true,
          writable: true,
        });
        globalThis.fetch = scopedFetch;
      },
      enumerable: false,
      configurable: true,
      writable: true,
    });
  }
  const installHostFetch = globalThis.__dd_install_host_fetch;

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

  const normalizeMemoryFetchInput = async (inputValue, initValue) => {
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

  const normalizeServiceFetchBody = (value) => {
    if (value == null) {
      return new Uint8Array();
    }
    if (typeof value === "string") {
      return toUtf8Bytes(value);
    }
    if (value instanceof URLSearchParams) {
      return toUtf8Bytes(value.toString());
    }
    if (value instanceof Uint8Array || value instanceof ArrayBuffer || ArrayBuffer.isView(value)) {
      return toArrayBytes(value);
    }
    return null;
  };
