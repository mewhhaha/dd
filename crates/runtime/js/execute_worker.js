globalThis.__dd_execute_worker = (payload) => {
  const requestId = String(payload?.request_id ?? "");
  const completionToken = String(payload?.completion_token ?? "");
  const workerName = String(payload?.worker_name ?? "");
  const kvBindingsConfig = payload?.kv_bindings ?? [];
  const actorBindingsConfig = payload?.actor_bindings ?? [];
  const dynamicBindingsConfig = payload?.dynamic_bindings ?? [];
  const dynamicRpcBindingsConfig = payload?.dynamic_rpc_bindings ?? [];
  const dynamicEnvConfig = payload?.dynamic_env ?? [];
  const actorCallConfig = payload?.actor_call ?? null;
  const hostRpcCallConfig = payload?.host_rpc_call ?? null;
  const hasRequestBodyStream = payload?.has_request_body_stream === true;
  const streamResponse = payload?.stream_response === true;
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
  const input = payload?.request ?? null;
  const controller = new AbortController();
  const asyncContext = globalThis.__dd_async_context;
  const requestContext = {
    requestId,
    controller,
    waitUntilPromises: [],
    waitUntilDoneSent: false,
    actorInvokeSeq: 0,
    actorEntry: null,
    actorRequestId: null,
    memoryTxnScope: null,
    socketRuntimeProvider: null,
    transportRuntimeProvider: null,
    kvGetBatches: new Map(),
    kvGetResults: new Map(),
    kvWriteOverlay: new Map(),
  };

  const currentRequestContext = (required = true) => {
    const current = asyncContext?.getStore?.() ?? null;
    if (!current && required) {
      throw new Error("request scope is unavailable");
    }
    return current;
  };

  const nextActorInvokeSeq = () => {
    const current = currentRequestContext();
    current.actorInvokeSeq = Number(current.actorInvokeSeq ?? 0) + 1;
    return current.actorInvokeSeq;
  };

  if (!globalThis.__dd_handle_websocket_wrapped) {
    globalThis.__dd_handle_websocket_wrapped = true;
    globalThis.__dd_original_websocket = globalThis.WebSocket;
    globalThis.WebSocket = function WebSocket(handle) {
      const current = currentRequestContext(false);
      if (current?.socketRuntimeProvider) {
        return current.socketRuntimeProvider().WebSocket(handle);
      }
      const originalWebSocket = globalThis.__dd_original_websocket;
      if (typeof originalWebSocket === "function") {
        return new originalWebSocket(handle);
      }
      throw new Error("handle-backed WebSocket is unavailable outside keyed memory scope");
    };
  }

  if (!globalThis.__dd_handle_transport_wrapped) {
    globalThis.__dd_handle_transport_wrapped = true;
    globalThis.__dd_original_webtransport_session = globalThis.WebTransportSession;
    globalThis.WebTransportSession = function WebTransportSession(handle) {
      const current = currentRequestContext(false);
      if (current?.transportRuntimeProvider) {
        return current.transportRuntimeProvider().WebTransportSession(handle);
      }
      const originalWebTransportSession = globalThis.__dd_original_webtransport_session;
      if (typeof originalWebTransportSession === "function") {
        return new originalWebTransportSession(handle);
      }
      throw new Error("handle-backed WebTransportSession is unavailable outside keyed memory scope");
    };
  }

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

  const recordKvProfile = (metric, durationMs, items = 1) => {
    const op = Deno?.core?.ops?.op_kv_profile_record_js;
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
      throw new Error("dynamic worker request scope is unavailable");
    }
    return scoped;
  };

  const actorScopedRequestId = (entry, runtimeRequestId) => {
    const current = currentRequestContext(false);
    if (current?.actorEntry === entry && current.actorRequestId) {
      return current.actorRequestId;
    }
    return activeRequestId() || runtimeRequestId;
  };

  const withActorTxnScope = (scope, callback) => {
    const current = currentRequestContext();
    const previousScope = current.memoryTxnScope;
    current.memoryTxnScope = scope;
    try {
      return callback();
    } finally {
      current.memoryTxnScope = previousScope;
    }
  };

  const actorTxnScopeFor = (binding, actorKey) => {
    const currentActorTxnScope = currentRequestContext(false)?.memoryTxnScope ?? null;
    if (
      currentActorTxnScope
      && currentActorTxnScope.binding === binding
      && currentActorTxnScope.actorKey === actorKey
    ) {
      return currentActorTxnScope;
    }
    return null;
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

    return Object.freeze({
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
    });
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

  const createKvBinding = (bindingName) => {
    const ensureKvWriteOverlay = () => {
      const current = currentRequestContext();
      let overlay = current.kvWriteOverlay.get(bindingName) ?? null;
      if (!overlay) {
        overlay = new Map();
        current.kvWriteOverlay.set(bindingName, overlay);
      }
      return overlay;
    };

    const setKvOverlayValue = (normalizedKey, entry) => {
      const current = currentRequestContext(false);
      if (!current) {
        return;
      }
      ensureKvWriteOverlay().set(normalizedKey, entry);
      current.kvGetResults.get(bindingName)?.delete(normalizedKey);
    };

    const clearKvOverlayValue = (normalizedKey) => {
      const current = currentRequestContext(false);
      current?.kvWriteOverlay?.get(bindingName)?.delete(normalizedKey);
      current?.kvGetResults?.get(bindingName)?.delete(normalizedKey);
    };

    const enqueueKvWrite = (mutation) => {
      const result = mutation.deleted === true
        ? callOp("op_kv_enqueue_delete", workerName, bindingName, mutation.key)
        : mutation.encoding === "utf8"
          ? callOp(
            "op_kv_enqueue_put",
            workerName,
            bindingName,
            mutation.key,
            Deno.core.decode(mutation.value),
          )
          : callOp(
            "op_kv_enqueue_put_value",
            JSON.stringify({
              worker_name: workerName,
              binding: bindingName,
              key: mutation.key,
              encoding: mutation.encoding,
              value: Array.from(mutation.value),
            }),
          );
      syncFrozenTimeNow();
      if (result && typeof result === "object" && result.ok === false) {
        clearKvOverlayValue(mutation.key);
        throw new Error(String(result.error ?? "kv enqueue failed"));
      }
    };

    const loadKvValues = async (normalizedKeys, contextLabel) => {
      if (normalizedKeys.length === 0) {
        return [];
      }
      const uniqueKeys = [];
      const keyIndexes = new Map();
      const normalizedToUnique = normalizedKeys.map((key) => {
        if (!keyIndexes.has(key)) {
          keyIndexes.set(key, uniqueKeys.length);
          uniqueKeys.push(key);
        }
        return keyIndexes.get(key);
      });
      if (uniqueKeys.length === 1) {
        const utf8Result = await callOp(
          "op_kv_get",
          workerName,
          bindingName,
          uniqueKeys[0],
        );
        syncFrozenTimeNow();
        if (utf8Result && typeof utf8Result === "object" && utf8Result.ok === true) {
          const value = utf8Result.found === true ? String(utf8Result.value ?? "") : null;
          return normalizedToUnique.map(() => value);
        }
        if (utf8Result?.wrong_encoding !== true && utf8Result?.error) {
          throw new Error(String(utf8Result.error || `${contextLabel} failed`));
        }
        const result = await callOp(
          "op_kv_get_value",
          JSON.stringify({
            worker_name: workerName,
            binding: bindingName,
            key: uniqueKeys[0],
          }),
        );
        syncFrozenTimeNow();
        if (result && typeof result === "object" && result.ok === false) {
          throw new Error(String(result.error ?? `${contextLabel} failed`));
        }
        const value = result?.found === true
          ? decodeStoredValue(String(result.encoding ?? "utf8"), result.value, contextLabel)
          : null;
        return normalizedToUnique.map(() => value);
      }
      const utf8Result = await callOp(
        "op_kv_get_many_utf8",
        JSON.stringify({
          worker_name: workerName,
          binding: bindingName,
          keys: uniqueKeys,
        }),
      );
      syncFrozenTimeNow();
      if (!utf8Result || typeof utf8Result !== "object" || utf8Result.ok === false) {
        throw new Error(String(utf8Result?.error ?? `${contextLabel} failed`));
      }
      const items = Array.isArray(utf8Result.values) ? utf8Result.values : [];
      const uniqueValues = new Array(uniqueKeys.length);
      const fallbackIndexes = [];
      for (let index = 0; index < uniqueKeys.length; index += 1) {
        const item = items[index];
        if (!item || item.found !== true) {
          uniqueValues[index] = null;
          continue;
        }
        if (item.wrong_encoding === true) {
          fallbackIndexes.push(index);
          continue;
        }
        uniqueValues[index] = String(item.value ?? "");
      }
      if (fallbackIndexes.length === 0) {
        return normalizedToUnique.map((index) => uniqueValues[index]);
      }
      const fallbackValues = await Promise.all(fallbackIndexes.map(async (index) => {
        const result = await callOp(
          "op_kv_get_value",
          JSON.stringify({
            worker_name: workerName,
            binding: bindingName,
            key: uniqueKeys[index],
          }),
        );
        syncFrozenTimeNow();
        if (result && typeof result === "object" && result.ok === false) {
          throw new Error(String(result.error ?? `${contextLabel} failed`));
        }
        if (result?.found !== true) {
          return null;
        }
        return decodeStoredValue(String(result.encoding ?? "utf8"), result.value, contextLabel);
      }));
      for (let index = 0; index < fallbackIndexes.length; index += 1) {
        uniqueValues[fallbackIndexes[index]] = fallbackValues[index];
      }
      return normalizedToUnique.map((index) => uniqueValues[index]);
    };

    const flushPendingGetBatch = async (batch) => {
      const started = performance.now();
      const uniqueKeys = Array.from(batch.requestsByKey.keys());
      try {
        const values = await loadKvValues(uniqueKeys, "kv get");
        for (let index = 0; index < uniqueKeys.length; index += 1) {
          const key = uniqueKeys[index];
          const value = values[index];
          const waiters = batch.requestsByKey.get(key) ?? [];
          for (const waiter of waiters) {
            waiter.resolve(value);
          }
        }
      } finally {
        recordKvProfile("js_batch_flush", performance.now() - started, uniqueKeys.length);
      }
    };

    const queueKvGet = (normalizedKey) => {
      const current = currentRequestContext();
      let cachedByKey = current.kvGetResults.get(bindingName) ?? null;
      if (!cachedByKey) {
        cachedByKey = new Map();
        current.kvGetResults.set(bindingName, cachedByKey);
      }
      const cached = cachedByKey.get(normalizedKey);
      if (cached) {
        return cached;
      }
      let pendingGetBatch = current.kvGetBatches.get(bindingName) ?? null;
      if (!pendingGetBatch) {
        pendingGetBatch = {
          requestsByKey: new Map(),
        };
        current.kvGetBatches.set(bindingName, pendingGetBatch);
        queueMicrotask(() => {
          const batch = current.kvGetBatches.get(bindingName);
          current.kvGetBatches.delete(bindingName);
          if (!batch) {
            return;
          }
          flushPendingGetBatch(batch).catch((error) => {
            const bindingCache = current.kvGetResults.get(bindingName) ?? null;
            for (const [key, waiters] of batch.requestsByKey.entries()) {
              bindingCache?.delete(key);
              for (const waiter of waiters) {
                waiter.reject(error);
              }
            }
          });
        });
      }
      const sharedPromise = new Promise((resolve, reject) => {
        pendingGetBatch.requestsByKey.set(normalizedKey, [{ resolve, reject }]);
      });
      cachedByKey.set(normalizedKey, sharedPromise);
      return sharedPromise;
    };

    return Object.freeze({
      async get(key, options = {}) {
        const _ = options;
        const normalizedKey = String(key);
        const pendingWrite = currentRequestContext(false)?.kvWriteOverlay?.get(bindingName)
          ?.get(normalizedKey);
        if (pendingWrite) {
          if (pendingWrite.deleted === true) {
            return null;
          }
          return pendingWrite.resolvedValue;
        }
        const cachedValue = currentRequestContext(false)?.kvGetResults?.get(bindingName)
          ?.get(normalizedKey);
        if (cachedValue) {
          return await cachedValue;
        }
        return await queueKvGet(normalizedKey);
      },
      put(key, value, options = {}) {
        const _ = options;
        const normalizedKey = String(key);
        let mutation;
        let resolvedValue = value;
        if (typeof value === "string") {
          mutation = {
            key: normalizedKey,
            encoding: "utf8",
            value: toUtf8Bytes(value),
            deleted: false,
          };
        } else {
          let encoded;
          try {
            encoded = Deno.core.serialize(value, { forStorage: true });
          } catch (error) {
            throw new Error(`kv put serialize failed: ${String(error?.message ?? error)}`);
          }
          mutation = {
            key: normalizedKey,
            encoding: "v8sc",
            value: new Uint8Array(encoded),
            deleted: false,
          };
        }
        setKvOverlayValue(normalizedKey, {
          deleted: false,
          resolvedValue,
        });
        enqueueKvWrite(mutation);
      },
      delete(key, options = {}) {
        const _ = options;
        const normalizedKey = String(key);
        setKvOverlayValue(normalizedKey, {
          deleted: true,
          resolvedValue: null,
        });
        enqueueKvWrite({
          key: normalizedKey,
          encoding: "utf8",
          value: new Uint8Array(),
          deleted: true,
        });
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
        syncFrozenTimeNow();
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
  };

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
      const run = async () => {
        const current = currentRequestContext();
        const normalized = await normalizeHostFetchInput(inputValue, initValue);
        const prepared = await callOp(
          "op_http_prepare",
          JSON.stringify({
            request_id: current.requestId,
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
        const signal = composeAbortSignal([current.controller.signal, normalized.signal]);
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
      const current = currentRequestContext(false);
      if (current?.actorEntry) {
        return gateActorOutput(current.actorEntry, current.actorRequestId || current.requestId, run);
      }
      return run();
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
      const method = String(value.method || "GET");
      const bodyBytes = toArrayBytes(value.body);
      const init = {
        method,
        headers: Array.isArray(value.headers) ? value.headers : [],
      };
      if (!(bodyBytes.byteLength === 0 && /^(GET|HEAD)$/i.test(method))) {
        init.body = bodyBytes;
      }
      return new Request(String(value.url || "http://worker/"), init);
    }
    if (value.__dd_rpc_type === "response") {
      const status = Number(value.status || 200);
      const bodyBytes = toArrayBytes(value.body);
      const init = {
        status,
        headers: Array.isArray(value.headers) ? value.headers : [],
      };
      const body = (bodyBytes.byteLength === 0 && [101, 204, 205, 304].includes(status))
        ? null
        : bodyBytes;
      return new Response(body, init);
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
  const INTERNAL_TRANSPORT_METHOD_HEADER = "x-dd-transport-method";

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

  const encodeActorStorageValue = (value) => {
    if (typeof value === "string") {
      return {
        encoding: "utf8",
        value: toUtf8Bytes(value),
      };
    }
    return {
      encoding: "v8sc",
      value: new Uint8Array(Deno.core.serialize(value, { forStorage: true })),
    };
  };

  const decodeActorStorageValue = (record) => {
    if (!record) {
      return null;
    }
    const encoding = String(record.encoding ?? "utf8");
    const bytes = toArrayBytes(record.value ?? []);
    if (encoding === "utf8") {
      return Deno.core.decode(bytes);
    }
    if (encoding === "v8sc") {
      return Deno.core.deserialize(bytes, { forStorage: true });
    }
    throw new Error(`memory storage get unsupported encoding: ${encoding}`);
  };

  const ensureActorStorageState = (entry) => {
    if (!entry.storageState) {
      entry.storageState = {
        hydrated: false,
        hydrating: null,
        mirror: new Map(),
        loadedKeys: new Set(),
        committedVersion: -1,
        nextVersion: 0,
        pendingMutations: [],
        flushRunning: false,
        flushTail: Promise.resolve(),
        failedError: null,
        outputGate: Promise.resolve(),
      };
    }
    return entry.storageState;
  };

  const actorRecordVersion = (record) => (
    record && !record.deleted ? Number(record.version ?? -1) : -1
  );

  const cloneActorRecord = (record) => {
    if (!record) {
      return null;
    }
    return {
      key: String(record.key ?? ""),
      value: toArrayBytes(record.value ?? []),
      encoding: String(record.encoding ?? "utf8"),
      version: Number(record.version ?? -1),
      deleted: record.deleted === true,
    };
  };

  const createActorTxn = (entry) => ({
    entry,
    reads: new Map(),
    writes: new Map(),
    deferred: [],
    listGateVersion: null,
    stagedVersionSeed: null,
    accepted: false,
    committed: false,
    committedVersion: null,
  });

  const actorTxnIsSnapshotOnly = (txn) => !!txn
    && txn.reads.size === 0
    && txn.writes.size === 0
    && txn.deferred.length === 0
    && txn.listGateVersion == null
    && txn.accepted !== true;

  class MemoryHydrationNeeded extends Error {
    constructor(mode, keys = []) {
      super(mode === "full" ? "memory hydration requires full snapshot" : "memory hydration requires keys");
      this.name = "MemoryHydrationNeeded";
      this.__dd_memory_hydration_needed = true;
      this.mode = mode;
      this.keys = Array.isArray(keys)
        ? keys.map((value) => String(value)).filter((value) => value.length > 0)
        : [];
    }
  }

  const mergeActorSnapshotEntries = (storageState, entries, maxVersion, mode, requestedKeys = []) => {
    if (mode === "full") {
      storageState.mirror.clear();
      storageState.loadedKeys.clear();
    }
    const seenKeys = new Set();
    for (const entryValue of Array.isArray(entries) ? entries : []) {
      const key = String(entryValue?.key ?? "");
      if (!key) {
        continue;
      }
      seenKeys.add(key);
      storageState.loadedKeys.add(key);
      storageState.mirror.set(key, {
        key,
        value: toArrayBytes(entryValue?.value ?? []),
        encoding: String(entryValue?.encoding ?? "utf8"),
        version: Number(entryValue?.version ?? -1),
        deleted: entryValue?.deleted === true,
      });
    }
    if (mode !== "full") {
      for (const key of requestedKeys) {
        const normalizedKey = String(key ?? "");
        if (!normalizedKey || storageState.loadedKeys.has(normalizedKey)) {
          continue;
        }
        storageState.loadedKeys.add(normalizedKey);
        if (!seenKeys.has(normalizedKey) && !storageState.mirror.has(normalizedKey)) {
          storageState.mirror.set(normalizedKey, {
            key: normalizedKey,
            value: new Uint8Array(),
            encoding: "utf8",
            version: -1,
            deleted: true,
          });
        }
      }
    }
    const nextCommittedVersion = Number(maxVersion ?? storageState.committedVersion ?? -1);
    storageState.committedVersion = Math.max(
      Number(storageState.committedVersion ?? -1),
      nextCommittedVersion,
    );
    storageState.nextVersion = Math.max(
      Number(storageState.nextVersion ?? 0),
      Number(storageState.committedVersion ?? -1) + 1,
    );
    if (mode === "full") {
      storageState.hydrated = true;
    }
  };

  const actorTxnReadRecord = (txn, key) => {
    const normalizedKey = String(key);
    if (txn?.writes?.has(normalizedKey)) {
      return txn.writes.get(normalizedKey);
    }
    const storageState = ensureActorStorageState(txn.entry);
    return storageState.mirror.get(normalizedKey) ?? null;
  };

  const actorTxnTrackRead = (txn, key, record, strict) => {
    if (!txn || strict !== true) {
      return;
    }
    const normalizedKey = String(key);
    if (txn.reads.has(normalizedKey)) {
      return;
    }
    txn.reads.set(normalizedKey, actorRecordVersion(record));
  };

  const actorTxnNextVersion = (txn) => {
    if (!txn) {
      throw new Error("transaction is required");
    }
    if (!Number.isFinite(Number(txn.stagedVersionSeed))) {
      const storageState = ensureActorStorageState(txn.entry);
      txn.stagedVersionSeed = Number(storageState.committedVersion ?? -1) + 1;
    }
    const version = Number(txn.stagedVersionSeed);
    txn.stagedVersionSeed = version + 1;
    return version;
  };

  const closeActorResourcesOnFailure = async (entry, runtimeRequestId) => {
    const socketHandles = Array.from(entry.openSocketHandles ?? []);
    for (const handle of socketHandles) {
      try {
        const result = await callOp("op_actor_socket_close", {
          request_id: runtimeRequestId,
          handle,
          code: 1011,
          reason: "memory storage flush failed",
        });
        await syncFrozenTime();
        if (result && typeof result === "object" && result.ok === false) {
          console.warn(String(result.error ?? "memory socket close failed"));
        }
      } catch {
        // Ignore follow-up close failures after the actor has already failed.
      }
    }

    const transportHandles = Array.from(entry.openTransportHandles ?? []);
    for (const handle of transportHandles) {
      try {
        const result = await callOpAny(
          [
            "op_actor_transport_close",
            "op_actor_transport_terminate",
          ],
          {
            request_id: runtimeRequestId,
            handle,
            code: 1011,
          reason: "memory storage flush failed",
          },
        );
        await syncFrozenTime();
        if (result && typeof result === "object" && result.ok === false) {
          console.warn(String(result.error ?? "memory transport close failed"));
        }
      } catch {
        // Ignore follow-up close failures after the actor has already failed.
      }
    }
  };

  const failActorEntry = async (entry, runtimeRequestId, error) => {
    const storageState = ensureActorStorageState(entry);
    if (storageState.failedError) {
      throw storageState.failedError;
    }
    const failure = error instanceof Error ? error : new Error(String(error ?? "memory namespace failed"));
    storageState.failedError = failure;
    if (entry.cacheKey) {
      actorStateEntries.delete(entry.cacheKey);
    }
    await closeActorResourcesOnFailure(entry, runtimeRequestId);
    throw failure;
  };

  const flushActorStorage = (entry, runtimeRequestId) => {
    const storageState = ensureActorStorageState(entry);
    if (storageState.flushRunning) {
      return storageState.flushTail;
    }
    storageState.flushRunning = true;
    storageState.flushTail = (async () => {
      while (storageState.pendingMutations.length > 0) {
        const mutations = storageState.pendingMutations.splice(0, storageState.pendingMutations.length);
        const result = await callOp(
          "op_actor_state_apply_batch",
          {
            request_id: runtimeRequestId,
            binding: entry.binding,
            key: entry.actorKey,
            expected_base_version: storageState.committedVersion,
            mutations: mutations.map((mutation) => ({
              key: mutation.key,
              value: Array.from(mutation.value),
              encoding: mutation.encoding,
              version: mutation.version,
              deleted: mutation.deleted,
            })),
          },
        );
        await syncFrozenTime();
        if (!result || typeof result !== "object" || result.ok === false) {
          throw new Error(String(result?.error ?? "memory storage batch flush failed"));
        }
        if (result.conflict === true) {
          throw new Error("memory storage batch flush conflicted");
        }
        storageState.committedVersion = Number(result.max_version ?? storageState.committedVersion);
      }
    })()
      .catch(async (error) => failActorEntry(entry, runtimeRequestId, error))
      .finally(() => {
        storageState.flushRunning = false;
      });
    return storageState.flushTail;
  };

  const queueActorMutation = (entry, runtimeRequestId, mutation) => {
    const storageState = ensureActorStorageState(entry);
    if (storageState.failedError) {
      throw storageState.failedError;
    }
    storageState.pendingMutations.push(mutation);
    flushActorStorage(entry, runtimeRequestId).catch(() => {});
  };

  const waitForActorFlush = async (entry, runtimeRequestId) => {
    const storageState = ensureActorStorageState(entry);
    if (storageState.failedError) {
      throw storageState.failedError;
    }
    if (storageState.flushRunning || storageState.pendingMutations.length > 0) {
      await flushActorStorage(entry, runtimeRequestId);
    }
    if (storageState.failedError) {
      throw storageState.failedError;
    }
  };

  const gateActorOutput = (entry, runtimeRequestId, callback) => {
    const storageState = ensureActorStorageState(entry);
    const run = async () => {
      await waitForActorFlush(entry, runtimeRequestId);
      return await callback();
    };
    const gated = storageState.outputGate.then(run, run);
    storageState.outputGate = gated.then(
      () => undefined,
      () => undefined,
    );
    return gated;
  };

  const ensureActorStorageHydrated = async (entry, runtimeRequestId) => {
    const storageState = ensureActorStorageState(entry);
    if (storageState.hydrated) {
      return storageState;
    }
    if (!storageState.hydrating) {
      storageState.hydrating = (async () => {
        const result = await callOp("op_actor_state_snapshot", {
          request_id: runtimeRequestId,
          binding: entry.binding,
          key: entry.actorKey,
        });
        await syncFrozenTime();
        if (!result || typeof result !== "object" || result.ok === false) {
          throw new Error(String(result?.error ?? "memory storage snapshot failed"));
        }
        mergeActorSnapshotEntries(
          storageState,
          result.entries,
          result.max_version,
          "full",
        );
        storageState.hydrating = null;
        return storageState;
      })().catch(async (error) => {
        storageState.hydrating = null;
        return await failActorEntry(entry, runtimeRequestId, error);
      });
    }
    return await storageState.hydrating;
  };

  const ensureActorStorageKeysHydrated = async (entry, runtimeRequestId, keys, options = {}) => {
    const storageState = ensureActorStorageState(entry);
    const force = options?.force === true;
    if (storageState.hydrated) {
      return storageState;
    }
    const pendingKeys = Array.from(new Set(
      (Array.isArray(keys) ? keys : [])
        .map((value) => String(value ?? ""))
        .filter((value) => value.length > 0)
        .filter((value) => force || !storageState.loadedKeys.has(value)),
    ));
    if (pendingKeys.length === 0) {
      return storageState;
    }
    const result = await callOp("op_actor_state_snapshot", {
      request_id: runtimeRequestId,
      binding: entry.binding,
      key: entry.actorKey,
      keys: pendingKeys,
    });
    await syncFrozenTime();
    if (!result || typeof result !== "object" || result.ok === false) {
      throw new Error(String(result?.error ?? "memory storage point snapshot failed"));
    }
    mergeActorSnapshotEntries(
      storageState,
      result.entries,
      result.max_version,
      "keys",
      pendingKeys,
    );
    return storageState;
  };

  const stageActorTxnWrite = (txn, record) => {
    txn.writes.set(String(record.key ?? ""), cloneActorRecord(record));
  };

  const refreshActorEntrySnapshot = async (entry, runtimeRequestId) => {
    const storageState = ensureActorStorageState(entry);
    storageState.hydrated = false;
    storageState.hydrating = null;
    storageState.loadedKeys.clear();
    await ensureActorStorageHydrated(entry, runtimeRequestId);
  };

  const commitActorTxn = async (txn, runtimeRequestId) => {
    if (!txn) {
      return;
    }
    const storageState = ensureActorStorageState(txn.entry);
    const writes = Array.from(txn.writes.values());
    const reads = Array.from(txn.reads.entries()).map(([key, version]) => ({
      key,
      version: Number(version ?? -1),
    }));
    const result = await callOp("op_actor_state_apply_batch", {
      request_id: runtimeRequestId,
      binding: txn.entry.binding,
      key: txn.entry.actorKey,
      transactional: true,
      expected_base_version: -1,
      reads,
      list_gate_version: txn.listGateVersion == null ? -1 : Number(txn.listGateVersion),
      mutations: writes.map((mutation) => ({
        key: mutation.key,
        value: Array.from(mutation.value),
        encoding: mutation.encoding,
        version: mutation.version,
        deleted: mutation.deleted,
      })),
    });
    await syncFrozenTime();
    if (!result || typeof result !== "object" || result.ok === false) {
      throw new Error(String(result?.error ?? "memory transaction commit failed"));
    }
    if (result.conflict === true) {
      await refreshActorEntrySnapshot(txn.entry, runtimeRequestId);
      return false;
    }
    const committedVersion = Number(result.max_version ?? storageState.committedVersion);
    for (const mutation of writes) {
      storageState.mirror.set(mutation.key, {
        ...cloneActorRecord(mutation),
        version: committedVersion,
      });
    }
    storageState.committedVersion = committedVersion;
    storageState.nextVersion = Math.max(
      Number(storageState.nextVersion ?? 0),
      Number(storageState.committedVersion ?? -1) + 1,
    );
    txn.committed = true;
    txn.committedVersion = committedVersion;
    await gateActorOutput(txn.entry, runtimeRequestId, async () => undefined);
    return true;
  };

  const validateActorTxnReads = async (txn, runtimeRequestId) => {
    if (!txn) {
      return true;
    }
    const storageState = ensureActorStorageState(txn.entry);
    const reads = Array.from(txn.reads.entries()).map(([key, version]) => ({
      key,
      version: Number(version ?? -1),
    }));
    const result = await callOp("op_actor_state_validate_reads", {
      request_id: runtimeRequestId,
      binding: txn.entry.binding,
      key: txn.entry.actorKey,
      reads,
      list_gate_version: txn.listGateVersion == null ? -1 : Number(txn.listGateVersion),
    });
    await syncFrozenTime();
    if (!result || typeof result !== "object" || result.ok === false) {
      throw new Error(String(result?.error ?? "memory transaction validation failed"));
    }
    if (result.conflict === true) {
      await ensureActorStorageKeysHydrated(
        txn.entry,
        runtimeRequestId,
        reads.map((entry) => entry.key),
        { force: true },
      );
      if (txn.listGateVersion != null) {
        await refreshActorEntrySnapshot(txn.entry, runtimeRequestId);
      }
      return false;
    }
    storageState.committedVersion = Math.max(
      Number(storageState.committedVersion ?? -1),
      Number(result.max_version ?? storageState.committedVersion ?? -1),
    );
    storageState.nextVersion = Math.max(
      Number(storageState.nextVersion ?? 0),
      Number(storageState.committedVersion ?? -1) + 1,
    );
    txn.committed = true;
    txn.committedVersion = Number(result.max_version ?? storageState.committedVersion);
    return true;
  };

  const createActorStorageBinding = (entry, runtimeRequestId, txn = null) => {
    const currentStorageRequestId = () => actorScopedRequestId(entry, runtimeRequestId);
    return {
      get(key, options = {}) {
        const storageState = ensureActorStorageState(entry);
        if (storageState.failedError) {
          throw storageState.failedError;
        }
        const normalizedKey = String(key);
        if (txn && !storageState.hydrated && !storageState.loadedKeys.has(normalizedKey)) {
          throw new MemoryHydrationNeeded("keys", [normalizedKey]);
        }
        const record = txn
          ? actorTxnReadRecord(txn, normalizedKey)
          : storageState.mirror.get(normalizedKey);
        actorTxnTrackRead(txn, normalizedKey, record, options?.strict === true);
        if (!record || record.deleted) {
          return null;
        }
        return {
          value: decodeActorStorageValue(record),
          version: Number(record.version ?? -1),
          encoding: String(record.encoding ?? "utf8"),
        };
      },
      put(key, value, options = {}) {
        const storageState = ensureActorStorageState(entry);
        if (storageState.failedError) {
          throw storageState.failedError;
        }
        const normalizedKey = String(key);
        const expectedInput = options?.expectedVersion;
        const expectedVersion = Number.isFinite(Number(expectedInput))
          ? Math.trunc(Number(expectedInput))
          : -1;
        const current = txn
          ? actorTxnReadRecord(txn, normalizedKey)
          : storageState.mirror.get(normalizedKey);
        const currentVersion = current ? Number(current.version ?? -1) : -1;
        if (expectedVersion >= 0 && currentVersion !== expectedVersion) {
          return {
            ok: true,
            conflict: true,
            version: currentVersion,
          };
        }
        const encoded = encodeActorStorageValue(value);
        const version = txn ? actorTxnNextVersion(txn) : storageState.nextVersion++;
        const record = {
          key: normalizedKey,
          value: encoded.value,
          encoding: encoded.encoding,
          version,
          deleted: false,
        };
        if (txn) {
          stageActorTxnWrite(txn, record);
        } else {
          storageState.mirror.set(normalizedKey, record);
          queueActorMutation(entry, currentStorageRequestId(), record);
        }
        return {
          ok: true,
          conflict: false,
          version,
        };
      },
      delete(key, options = {}) {
        const storageState = ensureActorStorageState(entry);
        if (storageState.failedError) {
          throw storageState.failedError;
        }
        const normalizedKey = String(key);
        const expectedInput = options?.expectedVersion;
        const expectedVersion = Number.isFinite(Number(expectedInput))
          ? Math.trunc(Number(expectedInput))
          : -1;
        const current = txn
          ? actorTxnReadRecord(txn, normalizedKey)
          : storageState.mirror.get(normalizedKey);
        const currentVersion = current ? Number(current.version ?? -1) : -1;
        if (expectedVersion >= 0 && currentVersion !== expectedVersion) {
          return {
            ok: true,
            conflict: true,
            version: currentVersion,
          };
        }
        const version = txn ? actorTxnNextVersion(txn) : storageState.nextVersion++;
        const record = {
          key: normalizedKey,
          value: new Uint8Array(),
          encoding: "utf8",
          version,
          deleted: true,
        };
        if (txn) {
          stageActorTxnWrite(txn, record);
        } else {
          storageState.mirror.set(normalizedKey, record);
          queueActorMutation(entry, currentStorageRequestId(), record);
        }
        return {
          ok: true,
          conflict: false,
          version,
        };
      },
      list(options = {}) {
        const storageState = ensureActorStorageState(entry);
        if (storageState.failedError) {
          throw storageState.failedError;
        }
        if (txn && !storageState.hydrated) {
          throw new MemoryHydrationNeeded("full");
        }
        const prefix = String(options?.prefix ?? "");
        const limitInput = Number(options?.limit ?? 100);
        const limit = Number.isFinite(limitInput)
          ? Math.max(1, Math.min(1000, Math.trunc(limitInput)))
          : 100;
        const merged = new Map(storageState.mirror);
        if (txn) {
          for (const [key, record] of txn.writes.entries()) {
            merged.set(key, cloneActorRecord(record));
          }
          if (options?.strict === true) {
            txn.listGateVersion = Number(storageState.committedVersion ?? -1);
          }
        }
        return Array.from(merged.values())
          .filter((record) => !record.deleted)
          .filter((record) => record.encoding === "utf8")
          .filter((record) => record.key.startsWith(prefix))
          .sort((left, right) => left.key.localeCompare(right.key))
          .slice(0, limit)
          .map((record) => ({
            key: record.key,
            value: Deno.core.decode(toArrayBytes(record.value)),
            version: Number(record.version ?? -1),
          }));
      },
    };
  };

  const createActorSocketRuntime = (entry, runtimeRequestId, allowSocketAccept) => {
    const socketsByHandle = entry.socketBindings ??= new Map();
    const openHandles = entry.openSocketHandles ??= new Set();
    const currentSocketRequestId = () => actorScopedRequestId(entry, runtimeRequestId);

    const sendSocketFrame = async (requestIdForOp, normalizedHandle, payload, gated = true) => {
      const perform = async () => {
        const result = await callOp("op_actor_socket_send", {
          request_id: requestIdForOp,
          binding: entry.binding,
          key: entry.actorKey,
          handle: normalizedHandle,
          message_kind: payload.kind,
          message: payload.value,
        });
        await syncFrozenTime();
        if (result && typeof result === "object" && result.ok === false) {
          throw new Error(String(result.error ?? "socket send failed"));
        }
      };
      if (gated) {
        await gateActorOutput(entry, requestIdForOp, perform);
        return;
      }
      await perform();
    };

    const closeSocketFrame = async (requestIdForOp, normalizedHandle, code, reason, gated = true) => {
      const perform = async () => {
        const result = await callOp("op_actor_socket_close", {
          request_id: requestIdForOp,
          binding: entry.binding,
          key: entry.actorKey,
          handle: normalizedHandle,
          code,
          reason,
        });
        await syncFrozenTime();
        if (result && typeof result === "object" && result.ok === false) {
          throw new Error(String(result.error ?? "socket close failed"));
        }
      };
      if (gated) {
        await gateActorOutput(entry, requestIdForOp, perform);
        return;
      }
      await perform();
    };

    const consumeCloseEvents = async (target) => {
      const result = await callOp("op_actor_socket_consume_close", {
        request_id: currentSocketRequestId(),
        binding: entry.binding,
        key: entry.actorKey,
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
        existing.__dd_bindRequestId(currentSocketRequestId());
        consumeCloseEvents(existing).catch(() => {});
        return existing;
      }

      const listeners = new Map();
      let onmessage = null;
      let onclose = null;
      const closeQueue = [];
      let closed = false;
      let boundRequestId = currentSocketRequestId();
      const target = {
        __dd_handle: normalizedHandle,
        __dd_bindRequestId(nextRequestId) {
          const normalizedRequestId = String(nextRequestId ?? "").trim();
          if (normalizedRequestId) {
            boundRequestId = normalizedRequestId;
          }
        },
        __dd_boundRequestId() {
          return boundRequestId;
        },
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
          const scope = actorTxnScopeFor(entry.binding, entry.actorKey);
          const perform = async () => {
            if (this.readyState !== 1) {
              return;
            }
            try {
              await sendSocketFrame(boundRequestId, normalizedHandle, payload, false);
            } catch {
              // Best-effort delivery; drop if the handle closed during flush.
            }
          };
          if (scope) {
            scope.state.defer(perform);
            return;
          }
          await perform();
        },
        async close(code, reason) {
          const normalizedCode = Number.isFinite(Number(code))
            ? Math.trunc(Number(code))
            : 1000;
          const perform = async () => {
            if (this.readyState !== 1) {
              return;
            }
            try {
              await closeSocketFrame(
                boundRequestId,
                normalizedHandle,
                normalizedCode,
                reason == null ? "" : String(reason),
                false,
              );
            } catch {
              // Best-effort close; ignore races with already-closed handles.
            }
          };
          const scope = actorTxnScopeFor(entry.binding, entry.actorKey);
          if (scope) {
            scope.state.defer(perform);
            return;
          }
          await perform();
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
                    "memory websocket listener failed",
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
          this.__dd_bindRequestId(currentSocketRequestId());
          const event = { type: "message", data: value, target: this };
          await this.__dd_dispatch("message", event);
          if (typeof onmessage === "function") {
            try {
              await onmessage.call(this, event);
            } catch (error) {
              try {
                console.warn(
                  "memory websocket onmessage failed",
                  String((error && (error.stack || error.message)) || error),
                );
              } catch {
                // Ignore logging failures.
              }
            }
          }
        },
        __dd_dispatchClose(code, reason) {
          this.__dd_bindRequestId(currentSocketRequestId());
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
      accept(request, options = {}) {
        const _ = options;
        if (!allowSocketAccept) {
          throw new Error("state.sockets.accept is only available during a keyed memory request");
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
      async send(handle, value, kind) {
        return ensureSocket(handle).send(value, kind);
      },
      async close(handle, code, reason) {
        return ensureSocket(handle).close(code, reason);
      },
    };

    return {
      sockets,
      WebSocket: WebSocketForActor,
      ensureSocket,
      async sendEffect(handle, value, kind) {
        const socket = ensureSocket(handle);
        const payload = encodeSocketSendPayload(value, kind);
        await sendSocketFrame(
          socket.__dd_boundRequestId(),
          socket.__dd_handle,
          payload,
          false,
        );
      },
      async closeEffect(handle, code, reason) {
        const socket = ensureSocket(handle);
        const normalizedCode = Number.isFinite(Number(code))
          ? Math.trunc(Number(code))
          : 1000;
        await closeSocketFrame(
          socket.__dd_boundRequestId(),
          socket.__dd_handle,
          normalizedCode,
          reason == null ? "" : String(reason),
          false,
        );
      },
      async refreshOpenHandles() {
        const result = await callOp("op_actor_socket_list", {
          request_id: currentSocketRequestId(),
          binding: entry.binding,
          key: entry.actorKey,
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
    const currentTransportRequestId = () => actorScopedRequestId(entry, runtimeRequestId);

    const consumeCloseEvents = async (target) => {
      const result = await callOp("op_actor_transport_consume_close", {
        request_id: currentTransportRequestId(),
        binding: entry.binding,
        key: entry.actorKey,
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
        existing.__dd_bindRequestId(currentTransportRequestId());
        consumeCloseEvents(existing).catch(() => {});
        return existing;
      }

      const datagramReadable = createTransportReadableChannel();
      const streamReadable = createTransportReadableChannel();
      let boundRequestId = currentTransportRequestId();
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
        const body = kind === "datagram"
          ? {
            request_id: boundRequestId,
            handle: normalizedHandle,
            binding: entry.binding,
            key: entry.actorKey,
            datagram: payload,
          }
          : {
            request_id: boundRequestId,
            handle: normalizedHandle,
            binding: entry.binding,
            key: entry.actorKey,
            chunk: payload,
          };
        await gateActorOutput(entry, currentTransportRequestId(), async () => {
          const result = await callOpAny(
            [
              kind === "datagram"
                ? "op_actor_transport_send_datagram"
                : "op_actor_transport_send_stream",
              "op_actor_transport_send",
            ],
            body,
          );
          await syncFrozenTime();
          if (result && typeof result === "object" && result.ok === false) {
            throw new Error(String(result.error ?? `transport ${kind} send failed`));
          }
        });
      };

      const target = {
        __dd_handle: normalizedHandle,
        __dd_bindRequestId(nextRequestId) {
          const normalizedRequestId = String(nextRequestId ?? "").trim();
          if (normalizedRequestId) {
            boundRequestId = normalizedRequestId;
          }
        },
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
          await gateActorOutput(entry, currentTransportRequestId(), async () => {
            const result = await callOpAny(
              [
                "op_actor_transport_close",
                "op_actor_transport_terminate",
              ],
              {
                request_id: currentTransportRequestId(),
                handle: normalizedHandle,
                binding: entry.binding,
                key: entry.actorKey,
                code: normalizedCode,
                reason: reason == null ? "" : String(reason),
              },
            );
            await syncFrozenTime();
            if (result && typeof result === "object" && result.ok === false) {
              throw new Error(String(result.error ?? "transport close failed"));
            }
          });
          finishClosed();
        },
        __dd_dispatchDatagram(value) {
          target.__dd_bindRequestId(currentTransportRequestId());
          datagramReadable.push(value);
        },
        __dd_dispatchStreamChunk(value) {
          target.__dd_bindRequestId(currentTransportRequestId());
          streamReadable.push(value);
        },
        __dd_dispatchClose(code, reason) {
          target.__dd_bindRequestId(currentTransportRequestId());
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
      accept(request, options = {}) {
        const _ = options;
        if (!allowTransportAccept) {
          throw new Error("state.transports.accept is only available during a keyed memory request");
        }
        if (transportAccepted.used) {
          throw new Error("state.transports.accept can only be called once per request");
        }
        if (!(request instanceof Request)) {
          throw new Error("state.transports.accept requires a Request");
        }
        const requestMethod = String(
          request.headers.get(INTERNAL_TRANSPORT_METHOD_HEADER)
          ?? request.method
          ?? "",
        ).toUpperCase();
        if (requestMethod !== "CONNECT") {
          throw new Error("state.transports.accept requires a CONNECT request");
        }
        const protocol = String(
          request.headers.get("protocol")
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
      session(handle) {
        return ensureTransport(handle);
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
          binding: entry.binding,
          key: entry.actorKey,
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

  const createActorRuntimeState = (
    entry,
    runtimeRequestId,
    allowSocketAccept,
    allowTransportAccept,
    txn = null,
  ) => {
    const storage = createActorStorageBinding(entry, runtimeRequestId, txn);
    let socketRuntime = null;
    let transportRuntime = null;
    const ensureSocketRuntime = () => {
      if (!socketRuntime) {
        socketRuntime = createActorSocketRuntime(
          entry,
          runtimeRequestId,
          allowSocketAccept,
        );
        entry.socketRuntime = socketRuntime;
      }
      return socketRuntime;
    };
    const ensureTransportRuntime = () => {
      if (!transportRuntime) {
        transportRuntime = createActorTransportRuntime(
          entry,
          runtimeRequestId,
          allowTransportAccept,
        );
        entry.transportRuntime = transportRuntime;
      }
      return transportRuntime;
    };
    return {
      id: {
        toString() {
          return entry.actorKey;
        },
      },
      get(key, options) {
        return storage.get(key, options);
      },
      set(key, value, options) {
        return storage.put(key, value, options);
      },
      delete(key, options) {
        return storage.delete(key, options);
      },
      list(options) {
        return storage.list(options);
      },
      storage,
      get sockets() {
        return ensureSocketRuntime().sockets;
      },
      get transports() {
        return ensureTransportRuntime().transports;
      },
      get __dd_socket_runtime() {
        return ensureSocketRuntime();
      },
      get __dd_transport_runtime() {
        return ensureTransportRuntime();
      },
    };
  };

  const createActorAtomicState = (
    entry,
    runtimeRequestId,
    allowSocketAccept,
    allowTransportAccept,
    txn,
  ) => {
    const runtimeState = createActorRuntimeState(
      entry,
      runtimeRequestId,
      allowSocketAccept,
      allowTransportAccept,
      txn,
    );
    let stub = null;
    const ensureStub = () => {
      if (!stub) {
        stub = createActorStub(entry.binding, entry.actorKey);
      }
      return stub;
    };
    const isTransportRequest = (request) => {
      if (!(request instanceof Request)) {
        return false;
      }
      const requestMethod = String(
        request.headers.get(INTERNAL_TRANSPORT_METHOD_HEADER)
        ?? request.method
        ?? "",
      ).toUpperCase();
      return requestMethod === "CONNECT";
    };
    const createTxnVar = (key, defaultValue = null) => {
      const normalizedKey = String(key);
      return {
        key: normalizedKey,
        read(options) {
          const record = runtimeState.storage.get(normalizedKey, options);
          return record ? record.value : defaultValue;
        },
        write(value) {
          runtimeState.storage.put(normalizedKey, value);
          return value;
        },
        modify(updater) {
          if (typeof updater !== "function") {
            throw new Error("state.var(key).modify(updater) requires a function updater");
          }
          const record = runtimeState.storage.get(normalizedKey, { strict: true });
          const previous = record ? record.value : defaultValue;
          const next = updater(previous);
          runtimeState.storage.put(normalizedKey, next);
          return next;
        },
        delete() {
          return runtimeState.storage.delete(normalizedKey);
        },
      };
    };
    return {
      id: runtimeState.id,
      get stub() {
        return ensureStub();
      },
      storage: runtimeState.storage,
      __dd_socket_runtime: runtimeState.__dd_socket_runtime,
      __dd_transport_runtime: runtimeState.__dd_transport_runtime,
      get(key, options) {
        return runtimeState.storage.get(key, options)?.value ?? null;
      },
      set(key, value) {
        runtimeState.storage.put(key, value);
        return value;
      },
      put(key, updater) {
        if (typeof updater !== "function") {
          throw new Error("state.put(key, updater) requires a function updater");
        }
        const previous = runtimeState.storage.get(key, { strict: true })?.value ?? null;
        const next = updater(previous);
        runtimeState.storage.put(key, next);
        return next;
      },
      delete(key) {
        return runtimeState.storage.delete(key);
      },
      list(options) {
        return runtimeState.storage.list(options).map((entryValue) => ({
          key: entryValue.key,
          value: entryValue.value,
          version: entryValue.version,
        }));
      },
      var(key) {
        return createTxnVar(key);
      },
      tvar(key, defaultValue = null) {
        return createTxnVar(key, defaultValue);
      },
      defer(callback) {
        if (!txn) {
          throw new Error("state.defer(callback) requires an active transaction");
        }
        if (typeof callback !== "function") {
          throw new Error("state.defer(callback) requires a function");
        }
        txn.deferred.push(callback);
        return callback;
      },
      accept(request, options = {}) {
        if (txn) {
          txn.accepted = true;
        }
        if (isTransportRequest(request)) {
          const accepted = runtimeState.transports.accept(request, options);
          return {
            handle: accepted.handle,
            response: new Response(accepted.response.body ?? null, {
              status: accepted.response.status ?? 200,
              headers: accepted.response.headers ?? [],
            }),
          };
        }
        const accepted = runtimeState.sockets.accept(request, options);
        return {
          handle: accepted.handle,
          response: new Response(accepted.response.body ?? null, {
            status: accepted.response.status ?? 101,
            headers: accepted.response.headers ?? [],
          }),
        };
      },
    };
  };

  const createActorStateFacade = (stateRef) => {
    const facade = {
      id: {
        toString() {
          return stateRef.current.id.toString();
        },
      },
      storage: {
        get(...args) {
          return stateRef.current.storage.get(...args);
        },
        put(...args) {
          return stateRef.current.storage.put(...args);
        },
        delete(...args) {
          return stateRef.current.storage.delete(...args);
        },
        list(...args) {
          return stateRef.current.storage.list(...args);
        },
      },
      get(...args) {
        return stateRef.current.get(...args);
      },
      set(...args) {
        return stateRef.current.set(...args);
      },
      delete(...args) {
        return stateRef.current.delete(...args);
      },
      list(...args) {
        return stateRef.current.list(...args);
      },
      sockets: {
        accept(...args) {
          return stateRef.current.sockets.accept(...args);
        },
        values(...args) {
          return stateRef.current.sockets.values(...args);
        },
        send(...args) {
          return stateRef.current.sockets.send(...args);
        },
        close(...args) {
          return stateRef.current.sockets.close(...args);
        },
      },
      transports: {
        accept(...args) {
          return stateRef.current.transports.accept(...args);
        },
        values(...args) {
          return stateRef.current.transports.values(...args);
        },
      },
    };
    Object.defineProperty(facade, "__dd_socket_runtime", {
      get() {
        return stateRef.current.__dd_socket_runtime;
      },
    });
    Object.defineProperty(facade, "__dd_transport_runtime", {
      get() {
        return stateRef.current.__dd_transport_runtime;
      },
    });
    return facade;
  };

  const actorStateEntries = globalThis.__dd_actor_state_entries ??= new Map();
  const liveActorExecutables = globalThis.__dd_live_actor_executables ??= new Map();
  let liveActorExecutableSeq = globalThis.__dd_live_actor_executable_seq ?? 0;

  const ACTOR_ATOMIC_METHOD = "__dd_atomic";

  const actorMethodNameIsBlocked = (name) => (
    !name
    || name === "constructor"
    || name === "fetch"
    || name === "then"
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

  const createDynamicHostRpcNamespace = (bindingName) => new Proxy(Object.freeze({}), {
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
          request_id: activeRequestId(),
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
    set() {
      return false;
    },
    defineProperty() {
      return false;
    },
    deleteProperty() {
      return false;
    },
  });

  const actorEntryKey = (binding, actorKey) => `${binding}\u001f${actorKey}`;

  const createActorId = (bindingName, actorKey) => ({
    __dd_actor_key: actorKey,
    __dd_actor_binding: bindingName,
    toString() {
      return actorKey;
    },
  });

  const ensureActorEntry = async (
    binding,
    actorKey,
    hydrationRequestId,
    options = {},
  ) => {
    const cacheKey = actorEntryKey(binding, actorKey);
    let entry = actorStateEntries.get(cacheKey);
    if (!entry) {
      entry = {
        binding,
        actorKey,
        cacheKey,
        socketBindings: new Map(),
        openSocketHandles: new Set(),
        socketRuntime: null,
        transportBindings: new Map(),
        openTransportHandles: new Set(),
        transportRuntime: null,
      };
      actorStateEntries.set(cacheKey, entry);
    }
    entry.binding = binding;
    entry.actorKey = actorKey;
    entry.cacheKey = cacheKey;
    if (options?.hydrate !== false) {
      await ensureActorStorageHydrated(entry, hydrationRequestId);
    }
    return entry;
  };

  const registerLiveActorExecutable = (executable) => {
    liveActorExecutableSeq += 1;
    globalThis.__dd_live_actor_executable_seq = liveActorExecutableSeq;
    const token = `${activeRequestId()}:live-actor:${liveActorExecutableSeq}`;
    liveActorExecutables.set(token, executable);
    return token;
  };

  const extractExecDescriptor = (executable) => {
    if (typeof executable !== "function") {
      throw new Error("stub.atomic(fn, ...args) requires a function");
    }
    const workerModule = globalThis.__dd_worker_module;
    const exportName = String(executable.name ?? "").trim();
    const exported = exportName ? workerModule?.[exportName] : undefined;
    const source = String(executable);
    if (!exportName && !source.trim()) {
      throw new Error("stub.atomic(fn, ...args) requires a non-empty function body");
    }
    const liveToken = registerLiveActorExecutable(executable);
    if (exportName && exported === executable) {
      return { live_token: liveToken, export_name: exportName, source };
    }
    return { live_token: liveToken, source };
  };

  const createActorStubSocketApi = (bindingName, actorKey) => Object.freeze({
    async send(handle, value, kind) {
      const normalizedHandle = String(handle ?? "").trim();
      if (!normalizedHandle) {
        throw new Error("stub.sockets.send requires a non-empty handle");
      }
      const payload = encodeSocketSendPayload(value, kind);
      const result = await callOp("op_actor_socket_send", {
        request_id: activeRequestId(),
        binding: bindingName,
        key: actorKey,
        handle: normalizedHandle,
        message_kind: payload.kind,
        message: payload.value,
      });
      await syncFrozenTime();
      if (result && typeof result === "object" && result.ok === false) {
        throw new Error(String(result.error ?? "socket send failed"));
      }
    },
    async close(handle, code, reason) {
      const normalizedHandle = String(handle ?? "").trim();
      if (!normalizedHandle) {
        throw new Error("stub.sockets.close requires a non-empty handle");
      }
      const normalizedCode = Number.isFinite(Number(code))
        ? Math.trunc(Number(code))
        : 1000;
      const result = await callOp("op_actor_socket_close", {
        request_id: activeRequestId(),
        binding: bindingName,
        key: actorKey,
        handle: normalizedHandle,
        code: normalizedCode,
        reason: reason == null ? "" : String(reason),
      });
      await syncFrozenTime();
      if (result && typeof result === "object" && result.ok === false) {
        throw new Error(String(result.error ?? "socket close failed"));
      }
    },
    async values() {
      const result = await callOp("op_actor_socket_list", {
        request_id: activeRequestId(),
        binding: bindingName,
        key: actorKey,
      });
      await syncFrozenTime();
      if (result && typeof result === "object" && result.ok === false) {
        throw new Error(String(result.error ?? "socket values failed"));
      }
      return Array.isArray(result?.handles)
        ? result.handles.map((value) => String(value))
        : [];
    },
  });

  const createActorStubTransportApi = (bindingName, actorKey) => Object.freeze({
    async values() {
      const runtimeRequestId = activeRequestId();
      const entry = await ensureActorEntry(bindingName, actorKey, runtimeRequestId);
      const state = createActorRuntimeState(entry, runtimeRequestId, false, false);
      await state.__dd_transport_runtime.refreshOpenHandles();
      return state.transports.values();
    },
    async session(handle) {
      const runtimeRequestId = activeRequestId();
      const entry = await ensureActorEntry(bindingName, actorKey, runtimeRequestId);
      const state = createActorRuntimeState(entry, runtimeRequestId, false, false);
      await state.__dd_transport_runtime.refreshOpenHandles();
      return state.transports.session(handle);
    },
  });

  const createActorStubVarApi = (bindingName, actorKey, key, defaultValue = null) => {
    const normalizedKey = String(key);
    const requireScope = (operation) => {
      const scope = actorTxnScopeFor(bindingName, actorKey);
      if (!scope) {
        throw new Error(`stub.var(${JSON.stringify(normalizedKey)}).${operation}() requires stub.atomic(...)`);
      }
      return scope;
    };
    return {
      key: normalizedKey,
      read(options) {
        const scope = actorTxnScopeFor(bindingName, actorKey);
        if (scope) {
          const record = scope.state.storage.get(normalizedKey, options);
          return record ? record.value : defaultValue;
        }
        return (async () => {
          const runtimeRequestId = activeRequestId();
          const entry = await ensureActorEntry(bindingName, actorKey, runtimeRequestId, { hydrate: false });
          await ensureActorStorageKeysHydrated(entry, runtimeRequestId, [normalizedKey]);
          const state = createActorRuntimeState(entry, runtimeRequestId, false, false);
          const record = state.storage.get(normalizedKey, options);
          return record ? record.value : defaultValue;
        })();
      },
      write(value) {
        return requireScope("write").state.set(normalizedKey, value);
      },
      modify(updater) {
        return requireScope("modify").state.put(normalizedKey, updater);
      },
      delete() {
        return requireScope("delete").state.delete(normalizedKey);
      },
    };
  };

  const createActorStub = (namespace, actorKey) => Object.freeze({
    id: createActorId(namespace, actorKey),
    binding: namespace,
    sockets: createActorStubSocketApi(namespace, actorKey),
    transports: createActorStubTransportApi(namespace, actorKey),
    var(key) {
      return createActorStubVarApi(namespace, actorKey, key);
    },
    tvar(key, defaultValue = null) {
      return createActorStubVarApi(namespace, actorKey, key, defaultValue);
    },
    defer(callback) {
      const scope = actorTxnScopeFor(namespace, actorKey);
      if (!scope) {
        throw new Error("stub.defer(callback) requires stub.atomic(...)");
      }
      return scope.state.defer(callback);
    },
    accept(request, options) {
      const scope = actorTxnScopeFor(namespace, actorKey);
      if (!scope) {
        throw new Error("stub.accept(request) requires stub.atomic(...)");
      }
      return scope.state.accept(request, options);
    },
    async atomic(executable, ...args) {
      return await invokeActorExecutable(
        namespace,
        actorKey,
        ACTOR_ATOMIC_METHOD,
        executable,
        args,
      );
    },
    async apply(effects) {
      if (effects == null) {
        return;
      }
      if (!Array.isArray(effects)) {
        throw new Error("stub.apply(effects) requires an array");
      }
      for (const effect of effects) {
        if (!effect || typeof effect !== "object") {
          continue;
        }
        const type = String(effect.type ?? "").trim();
        if (type === "socket.send") {
          await this.sockets.send(
            String(effect.handle ?? ""),
            effect.payload,
            effect.kind == null ? undefined : String(effect.kind),
          );
          continue;
        }
        if (type === "socket.close") {
          await this.sockets.close(
            String(effect.handle ?? ""),
            Number(effect.code ?? 1000),
            String(effect.reason ?? ""),
          );
          continue;
        }
        if (type === "transport.stream") {
          const session = await this.transports.session(String(effect.handle ?? ""));
          const writer = session.stream.writable.getWriter();
          try {
            await writer.write(effect.payload ?? new Uint8Array());
          } finally {
            writer.releaseLock();
          }
          continue;
        }
        if (type === "transport.datagram") {
          const session = await this.transports.session(String(effect.handle ?? ""));
          const writer = session.datagrams.writable.getWriter();
          try {
            await writer.write(effect.payload ?? new Uint8Array());
          } finally {
            writer.releaseLock();
          }
          continue;
        }
        if (type === "transport.close") {
          const session = await this.transports.session(String(effect.handle ?? ""));
          await session.close(Number(effect.code ?? 0), String(effect.reason ?? ""));
          continue;
        }
        throw new Error(`unsupported memory effect type: ${type}`);
      }
    },
  });

  const invokeActorExecutable = async (
    namespace,
    actorKey,
    methodName,
    executable,
    args,
  ) => {
      const scopedRequestId = activeRequestId();
      const localRuntimeRequestId = `${scopedRequestId}:actor-run:${nextActorInvokeSeq()}`;
      if (methodName === ACTOR_ATOMIC_METHOD && typeof executable === "function") {
        const entry = await ensureActorEntry(namespace, actorKey, localRuntimeRequestId, { hydrate: false });
        return await executeActorTransaction(
          entry,
          localRuntimeRequestId,
          executable,
          args,
        );
      }
      const descriptor = extractExecDescriptor(executable);
      const payload = [
        {
          ...descriptor,
        },
        ...args,
      ];
      const argsBytes = await encodeRpcArgs(payload);
      try {
        const preferCallerIsolate = !descriptor.export_name;
        const result = await callOp(
          "op_actor_invoke_method",
          {
            worker_name: workerName,
            binding: namespace,
            key: actorKey,
            method_name: methodName,
            prefer_caller_isolate: preferCallerIsolate,
            args: Array.from(argsBytes),
            caller_request_id: scopedRequestId,
            request_id: localRuntimeRequestId,
          },
        );
        await syncFrozenTime();
        if (!result || typeof result !== "object" || result.ok === false) {
          throw new Error(String(result?.error ?? "memory operation failed"));
        }
        return decodeRpcResult(toArrayBytes(result.value));
      } finally {
        if (descriptor.live_token) {
          liveActorExecutables.delete(String(descriptor.live_token));
        }
      }
    };

  const actorIdKey = (id) => {
    if (typeof id === "string") {
      return id;
    }
    if (
      id
      && typeof id === "object"
      && typeof id.__dd_actor_key === "string"
      && typeof id.__dd_actor_binding === "string"
    ) {
      return id.__dd_actor_key;
    }
    return "";
  };

  const createActorNamespace = (bindingName) => Object.freeze({
    idFromName(name) {
      const key = String(name ?? "").trim();
      if (!key) {
        throw new Error("memory idFromName requires a non-empty name");
      }
      return createActorId(bindingName, key);
    },
    get(id) {
      if (
        id
        && typeof id === "object"
        && "__dd_actor_binding" in id
        && String(id.__dd_actor_binding) !== bindingName
      ) {
        throw new Error(
          `memory id belongs to namespace ${String(id.__dd_actor_binding)}, not ${bindingName}`,
        );
      }
      const actorKey = actorIdKey(id).trim();
      if (!actorKey) {
        throw new Error("memory namespace get() requires a valid memory id");
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
      const invokeSeq = nextActorInvokeSeq();
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
          subrequest_id: `${scopedRequestId}:dynamic:${invokeSeq}`,
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

  const createDynamicNamespace = (bindingName) => Object.freeze({
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

  const getSharedEnv = () => {
    const cache = globalThis.__dd_shared_env_cache ??= new WeakMap();
    const cacheableWorker = worker && (typeof worker === "object" || typeof worker === "function")
      ? worker
      : null;
    const fallbackCache = globalThis.__dd_shared_env_fallback_cache ??= new Map();
    const cached = cacheableWorker
      ? cache.get(cacheableWorker)
      : fallbackCache.get(workerName);
    if (cached) {
      return cached;
    }
    const env = {};
    const defineLazyValue = (target, propertyName, factory) => {
      let initialized = false;
      let cachedValue;
      Object.defineProperty(target, propertyName, {
        enumerable: true,
        configurable: true,
        get() {
          if (!initialized) {
            cachedValue = factory();
            initialized = true;
          }
          return cachedValue;
        },
      });
    };
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
      defineLazyValue(env, envName, () => createKvBinding(bindingName));
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
      defineLazyValue(
        env,
        envName.trim(),
        () => createDynamicNamespace(String(bindingName ?? envName).trim()),
      );
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
      defineLazyValue(
        env,
        envName.trim(),
        () => createDynamicHostRpcNamespace(String(bindingName ?? envName).trim()),
      );
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
      const bindingName = Array.isArray(entry)
        ? String(entry[0] ?? "").trim()
        : entry && typeof entry === "object"
          ? String(entry.binding ?? "").trim()
          : String(entry ?? "").trim();
      if (!bindingName) {
        continue;
      }
      const envName = bindingName;
      defineLazyValue(env, envName, () => createActorNamespace(bindingName));
    }

    Object.freeze(env);
    if (cacheableWorker) {
      cache.set(cacheableWorker, env);
    } else {
      fallbackCache.set(workerName, env);
    }
    return env;
  };

  const decodeExecPayload = (rawArgs) => {
    const values = decodeRpcArgs(toArrayBytes(rawArgs));
    if (!Array.isArray(values) || values.length === 0) {
      throw new Error("memory operation requires an executable payload");
    }
    const [descriptor, ...args] = values;
    const exportName = String(descriptor?.export_name ?? descriptor?.exportName ?? "").trim();
    const liveToken = String(descriptor?.live_token ?? descriptor?.liveToken ?? "").trim();
    const source = String(descriptor?.source ?? "").trim();
    if (!liveToken && !exportName && !source) {
      throw new Error("memory operation executable descriptor must not be empty");
    }
    return {
      descriptor: { liveToken, exportName, source },
      args,
    };
  };

  const instantiateExecutable = (descriptor) => {
    const liveToken = String(descriptor?.liveToken ?? "").trim();
    if (liveToken && liveActorExecutables.has(liveToken)) {
      const executable = liveActorExecutables.get(liveToken);
      liveActorExecutables.delete(liveToken);
      if (typeof executable === "function") {
        return executable;
      }
    }
    const exportName = String(descriptor?.exportName ?? "").trim();
    if (exportName) {
      const workerModule = globalThis.__dd_worker_module;
      const registered = workerModule?.[exportName];
      if (typeof registered === "function") {
        return registered;
      }
    }
    const source = String(descriptor?.source ?? "").trim();
    try {
      const executable = (0, eval)(`(${source})`);
      if (typeof executable !== "function") {
        throw new Error("memory executable did not evaluate to a function");
      }
      return executable;
    } catch (error) {
      throw new Error(`memory executable compile failed: ${String(error?.message ?? error)}`);
    }
  };

  const executeActorTransaction = async (
    entry,
    runtimeRequestId,
    executable,
    args,
  ) => {
    const maxRetries = 256;
    let lastConflict = null;
    for (let attempt = 0; attempt < maxRetries; attempt += 1) {
      for (;;) {
        const txn = createActorTxn(entry);
        const scopedState = createActorAtomicState(
          entry,
          runtimeRequestId,
          true,
          true,
          txn,
        );
        const current = currentRequestContext();
        const previousSocketRuntimeProvider = current.socketRuntimeProvider;
        const previousTransportRuntimeProvider = current.transportRuntimeProvider;
        current.socketRuntimeProvider = () => scopedState.__dd_socket_runtime;
        current.transportRuntimeProvider = () => scopedState.__dd_transport_runtime;
        try {
          const value = withActorTxnScope(
            {
              binding: entry.binding,
              actorKey: entry.actorKey,
              state: scopedState,
              stub: scopedState.stub,
            },
            () => executable(scopedState, ...args),
          );
          if (value instanceof Promise) {
            throw new Error("stub.atomic callback must be synchronous");
          }
          if (actorTxnIsSnapshotOnly(txn)) {
            return value;
          }
          const committed = txn.writes.size === 0 && txn.accepted !== true
            ? await validateActorTxnReads(txn, runtimeRequestId)
            : await commitActorTxn(txn, runtimeRequestId);
          if (!committed) {
            lastConflict = new Error("memory transaction conflicted");
            await new Promise((resolve) => setTimeout(resolve, Math.min(attempt + 1, 8)));
            break;
          }
          for (const deferred of txn.deferred) {
            const deferredResult = deferred();
            if (deferredResult instanceof Promise) {
              await deferredResult;
            }
          }
          return value;
        } catch (error) {
          if (error instanceof MemoryHydrationNeeded && error.__dd_memory_hydration_needed === true) {
            if (error.mode === "full") {
              await ensureActorStorageHydrated(entry, runtimeRequestId);
            } else {
              await ensureActorStorageKeysHydrated(entry, runtimeRequestId, error.keys);
            }
            continue;
          }
          if (txn.committed) {
            throw error;
          }
          if (
            error instanceof Error
            && String(error.message ?? "").includes("conflicted")
          ) {
            lastConflict = error;
            await new Promise((resolve) => setTimeout(resolve, Math.min(attempt + 1, 8)));
            break;
          }
          throw error;
        } finally {
          current.socketRuntimeProvider = previousSocketRuntimeProvider;
          current.transportRuntimeProvider = previousTransportRuntimeProvider;
        }
      }
    }
    throw lastConflict ?? new Error("memory transaction exceeded retry limit");
  };

  const buildWakeEvent = (actorCall, stub) => {
    const kind = String(actorCall.kind ?? "").trim();
    const event = {
      type: kind,
      binding: String(actorCall.binding ?? ""),
      key: String(actorCall.key ?? ""),
    };
    Object.defineProperty(event, "stub", {
      value: stub,
      enumerable: false,
      configurable: true,
      writable: false,
    });
    if ("handle" in actorCall) {
      event.handle = String(actorCall.handle ?? "");
    }
    if (kind === "message") {
      const raw = toArrayBytes(actorCall.data);
      event.data = actorCall.is_text === true ? Deno.core.decode(raw) : raw;
      event.isText = actorCall.is_text === true;
      event.type = "socketmessage";
    } else if (kind === "close") {
      event.code = Number(actorCall.code ?? 1000);
      event.reason = String(actorCall.reason ?? "");
      event.type = "socketclose";
    } else if (kind === "transport_datagram" || kind === "datagram") {
      event.data = toArrayBytes(actorCall.data);
      event.type = "transportdatagram";
    } else if (kind === "transport_stream" || kind === "stream") {
      event.data = toArrayBytes(actorCall.data);
      event.type = "transportstream";
    } else if (kind === "transport_close" || kind === "transport") {
      event.code = Number(actorCall.code ?? 0);
      event.reason = String(actorCall.reason ?? "");
      event.type = "transportclose";
    }
    return event;
  };

  const invokeActorCall = async (actorCall, request, env) => {
    if (!actorCall || typeof actorCall !== "object") {
      throw new Error("memory invoke config is missing");
    }
    const binding = String(actorCall.binding ?? "").trim();
    const actorKey = String(actorCall.key ?? "").trim();
    if (!binding || !actorKey) {
      throw new Error("memory invoke requires binding and key");
    }
    if (!Object.prototype.hasOwnProperty.call(env, binding)) {
      throw new Error(`memory binding not declared for worker: ${binding}`);
    }
    const runtimeRequestId = activeRequestId();
    const entry = await ensureActorEntry(binding, actorKey, runtimeRequestId, { hydrate: false });
    const kind = String(actorCall.kind ?? "");
    const scopedState = createActorRuntimeState(entry, runtimeRequestId, false, false);
    const current = currentRequestContext();
    const previousActorEntry = current.actorEntry;
    const previousActorRequestId = current.actorRequestId;
    const previousSocketRuntimeProvider = current.socketRuntimeProvider;
    const previousTransportRuntimeProvider = current.transportRuntimeProvider;
    current.socketRuntimeProvider = () => scopedState.__dd_socket_runtime;
    current.transportRuntimeProvider = () => scopedState.__dd_transport_runtime;
    current.actorEntry = entry;
    current.actorRequestId = runtimeRequestId;
    try {
      await scopedState.__dd_socket_runtime.refreshOpenHandles();
      await scopedState.__dd_transport_runtime.refreshOpenHandles();
      if (kind === "method") {
        const methodName = String(actorCall.name ?? "").trim();
        if (!methodName) {
          throw new Error("memory method invoke requires a method name");
        }
        if (
          actorMethodNameIsBlocked(methodName)
          && methodName !== ACTOR_ATOMIC_METHOD
        ) {
          throw new Error(`memory method is blocked: ${methodName}`);
        }
        if (methodName !== ACTOR_ATOMIC_METHOD) {
          throw new Error(`unsupported memory method: ${methodName}`);
        }
        const { descriptor, args } = decodeExecPayload(actorCall.args);
        const executable = instantiateExecutable(descriptor);
        const value = await executeActorTransaction(
          entry,
          runtimeRequestId,
          executable,
          args,
        );
        const encoded = await encodeRpcResult(value);
        return new Response(encoded, {
          status: 200,
          headers: [["content-type", "application/octet-stream"]],
        });
      }
      if (
        kind === "message"
        || kind === "close"
        || kind === "transport_datagram"
        || kind === "datagram"
        || kind === "transport_stream"
        || kind === "stream"
        || kind === "transport_close"
        || kind === "transport"
      ) {
        const wakeMethod = worker?.wake;
        if (typeof wakeMethod !== "function") {
          throw new Error("worker does not define wake(event, env)");
        }
        const event = buildWakeEvent(actorCall, createActorStub(binding, actorKey));
        await wakeMethod.call(worker, event, env);
        await gateActorOutput(entry, runtimeRequestId, async () => undefined);
        return new Response(null, { status: 204 });
      }
      throw new Error(`unsupported memory invoke kind: ${kind}`);
    } finally {
      current.actorEntry = previousActorEntry;
      current.actorRequestId = previousActorRequestId;
      current.socketRuntimeProvider = previousSocketRuntimeProvider;
      current.transportRuntimeProvider = previousTransportRuntimeProvider;
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
    if (requestContext.waitUntilDoneSent) {
      return;
    }
    requestContext.waitUntilDoneSent = true;
    await syncFrozenTime();
    callOp(
      "op_emit_wait_until_done",
      JSON.stringify({
        request_id: requestId,
        completion_token: completionToken,
        wait_until_count: requestContext.waitUntilPromises.length,
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
    if (requestContext.waitUntilPromises.length === 0) {
      return true;
    }

    let timeoutId = 0;
    const timeout = new Promise((resolve) => {
      timeoutId = setTimeout(() => resolve(false), 30_000);
    });
    const settled = Promise.allSettled(requestContext.waitUntilPromises).then(() => true);
    try {
      return await Promise.race([settled, timeout]);
    } finally {
      clearTimeout(timeoutId);
    }
  };

  function trackWaitUntil(promise) {
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
    requestContext.waitUntilPromises.push(tracked);
    return tracked;
  }

  const settlePendingKvWriteScheduling = async () => {
    return;
  };

  asyncContext.run(requestContext, () => (async () => {
    let restoreHostFetch = null;
    try {
      await syncFrozenTime();
      restoreHostFetch = installHostFetch();
      const requestBody = hasRequestBodyStream
        ? createRequestBodyStream()
        : input.body?.length
          ? new Uint8Array(input.body)
          : undefined;
      const requestHeaders = new Headers(input.headers);
      let requestMethod = String(input.method || "GET");
      const isTransportConnect = requestMethod.toUpperCase() === "CONNECT"
        && String(
          requestHeaders.get("x-dd-transport-protocol")
          ?? "",
        ).trim().toLowerCase() === "webtransport";
      if (isTransportConnect) {
        requestHeaders.set(INTERNAL_TRANSPORT_METHOD_HEADER, requestMethod);
        requestMethod = "GET";
      }
      const request = new Request(input.url, {
        method: requestMethod,
        headers: requestHeaders,
        body: requestBody,
        signal: requestContext.controller.signal,
      });
      const workerRequest = isTransportConnect
        ? new Proxy(request, {
          get(target, property, receiver) {
            if (property === "method") {
              return "CONNECT";
            }
            return Reflect.get(target, property, receiver);
          },
        })
        : request;
      const env = getSharedEnv();
      const ctx = {
        requestId: input.request_id,
        signal: requestContext.controller.signal,
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
          ? await invokeActorCall(actorCallConfig, workerRequest, env)
          : await worker.fetch(workerRequest, env, ctx);
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
      const bodyBytes = [];
      if (streamResponse) {
        await emitResponseStart(status, headers);
      }
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
          if (streamResponse) {
            const emitted = await emitResponseChunk(chunk);
            bodyBytes.push(...emitted);
          } else {
            bodyBytes.push(...chunk);
          }
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
  })())
    .then(async (result) => {
      await settlePendingKvWriteScheduling();
      callOp(
        "op_emit_completion",
        JSON.stringify({
          request_id: requestId,
          completion_token: completionToken,
          ok: true,
          wait_until_count: requestContext.waitUntilPromises.length,
          result,
        }),
      );

      await emitWaitUntilDone(!(await waitForWaitUntils()));
    })
    .catch(async (error) => {
      await settlePendingKvWriteScheduling();
      const message = String((error && (error.stack || error.message)) || error);
      callOp(
        "op_emit_completion",
        JSON.stringify({
          request_id: requestId,
          completion_token: completionToken,
          ok: false,
          wait_until_count: requestContext.waitUntilPromises.length,
          error: message,
        }),
      );

      await emitWaitUntilDone(!(await waitForWaitUntils()));
    });
};
