globalThis.__dd_execute_worker = (payload) => {
  const requestId = String(payload?.request_id ?? "");
  const completionToken = String(payload?.completion_token ?? "");
  const workerName = String(payload?.worker_name ?? "");
  const kvBindingsConfig = payload?.kv_bindings ?? [];
  const kvReadCacheConfigInput = payload?.kv_read_cache_config ?? null;
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
  const kvReadCacheConfig = (() => {
    const maxEntries = Math.max(
      1,
      Math.trunc(Number(kvReadCacheConfigInput?.max_entries ?? 16384) || 16384),
    );
    const maxBytes = Math.max(
      1024,
      Math.trunc(Number(kvReadCacheConfigInput?.max_bytes ?? 16 * 1024 * 1024) || (16 * 1024 * 1024)),
    );
    const hitTtlMs = Math.max(
      1,
      Math.trunc(Number(kvReadCacheConfigInput?.hit_ttl_ms ?? 300_000) || 300_000),
    );
    const missTtlMs = Math.max(
      1,
      Math.trunc(Number(kvReadCacheConfigInput?.miss_ttl_ms ?? 30_000) || 30_000),
    );
    return Object.freeze({
      maxEntries,
      maxBytes,
      hitTtlMs,
      missTtlMs,
    });
  })();
  const actorReadSnapshotFreshTtlMs = 1_000;
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

  const recordActorProfile = (metric, durationMs, items = 1) => {
    const op = Deno?.core?.ops?.op_actor_profile_record_js;
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
  globalThis.__dd_get_runtime_request_id = activeRequestId;

  const actorScopedRequestId = (entry, runtimeRequestId) => {
    const current = currentRequestContext(false);
    if (current?.actorEntry === entry && current.actorRequestId) {
      return current.actorRequestId;
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

  const currentLocalHandleRuntime = (binding, actorKey, kind) => {
    const current = currentRequestContext(false);
    if (!current?.actorEntry) {
      return null;
    }
    if (
      current.actorEntry.binding !== binding
      || current.actorEntry.actorKey !== actorKey
    ) {
      return null;
    }
    const provider = kind === "socket"
      ? current.socketRuntimeProvider
      : current.transportRuntimeProvider;
    const label = kind === "socket" ? "socket" : "transport";
    if (typeof provider !== "function") {
      throw new Error(`memory same-lane ${label} runtime is unavailable`);
    }
    const runtime = provider();
    if (!runtime || typeof runtime.listOpenHandles !== "function") {
      throw new Error(`memory same-lane ${label} runtime is unavailable`);
    }
    if (typeof runtime.hasOpenHandleSnapshot === "function" && !runtime.hasOpenHandleSnapshot()) {
      throw new Error(`memory same-lane ${label} handles are not initialized`);
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

  const kvCacheNowMs = () => performance.now();

  const estimateKvCachedValueBytes = (value, encoding) => {
    if (value == null) {
      return 8;
    }
    if (encoding === "utf8" && typeof value === "string") {
      return value.length * 2;
    }
    try {
      return JSON.stringify(value).length * 2;
    } catch {
      return 128;
    }
  };

  const createKvBinding = (bindingName) => {
    const persistentReadCache = {
      entries: new Map(),
      totalBytes: 0,
    };

    const deletePersistentCacheEntry = (normalizedKey, metric = "kv_cache_invalidate") => {
      const existing = persistentReadCache.entries.get(normalizedKey);
      if (!existing) {
        return false;
      }
      persistentReadCache.entries.delete(normalizedKey);
      persistentReadCache.totalBytes = Math.max(0, persistentReadCache.totalBytes - existing.sizeBytes);
      recordKvProfile(metric, 0, 1);
      return true;
    };

    const trimPersistentCache = () => {
      while (
        persistentReadCache.entries.size > kvReadCacheConfig.maxEntries
        || persistentReadCache.totalBytes > kvReadCacheConfig.maxBytes
      ) {
        const oldestKey = persistentReadCache.entries.keys().next().value;
        if (oldestKey === undefined) {
          break;
        }
        deletePersistentCacheEntry(oldestKey);
      }
    };

    const setPersistentCacheEntry = (
      normalizedKey,
      value,
      encoding,
      optimisticWriteVersion = null,
    ) => {
      const existing = persistentReadCache.entries.get(normalizedKey);
      if (existing) {
        persistentReadCache.totalBytes = Math.max(0, persistentReadCache.totalBytes - existing.sizeBytes);
        persistentReadCache.entries.delete(normalizedKey);
      }
      const missing = value == null;
      const expiresAtMs = kvCacheNowMs()
        + (missing ? kvReadCacheConfig.missTtlMs : kvReadCacheConfig.hitTtlMs);
      const sizeBytes = normalizedKey.length * 2
        + 64
        + estimateKvCachedValueBytes(value, encoding);
      persistentReadCache.entries.set(normalizedKey, {
        value,
        missing,
        encoding,
        optimisticWriteVersion,
        expiresAtMs,
        sizeBytes,
      });
      persistentReadCache.totalBytes += sizeBytes;
      trimPersistentCache();
      recordKvProfile("kv_cache_fill", 0, 1);
    };

    const getPersistentCacheEntry = (normalizedKey) => {
      const cached = persistentReadCache.entries.get(normalizedKey);
      if (!cached) {
        recordKvProfile("kv_cache_miss", 0, 1);
        return { found: false, value: null };
      }
      if (cached.optimisticWriteVersion != null) {
        const failed = callOp(
          "op_kv_take_failed_write_version",
          BigInt(cached.optimisticWriteVersion),
        );
        if (failed === true) {
          deletePersistentCacheEntry(normalizedKey);
          recordKvProfile("kv_cache_miss", 0, 1);
          return { found: false, value: null };
        }
      }
      if (cached.expiresAtMs <= kvCacheNowMs()) {
        deletePersistentCacheEntry(normalizedKey, "kv_cache_stale");
        recordKvProfile("kv_cache_miss", 0, 1);
        return { found: false, value: null };
      }
      persistentReadCache.entries.delete(normalizedKey);
      persistentReadCache.entries.set(normalizedKey, cached);
      recordKvProfile("kv_cache_hit", 0, 1);
      return { found: true, value: cached.missing ? null : cached.value };
    };

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
      setPersistentCacheEntry(
        mutation.key,
        mutation.deleted === true ? null : mutation.resolvedValue,
        mutation.encoding,
        Number.isFinite(Number(result?.version)) ? Number(result.version) : null,
      );
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
          setPersistentCacheEntry(
            key,
            value,
            value == null ? "missing" : (typeof value === "string" ? "utf8" : "v8sc"),
            null,
          );
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
        const persistentCached = getPersistentCacheEntry(normalizedKey);
        if (persistentCached.found) {
          return persistentCached.value;
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
            resolvedValue,
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
            resolvedValue,
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
          resolvedValue: null,
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

  const checkHostFetchUrl = async (requestId, url) => {
    const checked = await callOp(
      "op_http_check_url",
      JSON.stringify({
        request_id: requestId,
        url,
      }),
    );
    await syncFrozenTime();
    if (!checked || typeof checked !== "object" || checked.ok === false) {
      throw new Error(String(checked?.error ?? "host fetch URL check failed"));
    }
    return String(checked.url || url);
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
        let method = String(prepared.method || "GET");
        let url = String(prepared.url || normalized.url);
        let headers = Array.isArray(prepared.headers) ? prepared.headers : [];
        let body = Array.isArray(prepared.body) && prepared.body.length > 0
          ? toArrayBytes(prepared.body)
          : undefined;
        const redirectMode = normalized.redirect === "error"
          || normalized.redirect === "manual"
          ? normalized.redirect
          : "follow";
        let redirectsRemaining = HOST_FETCH_MAX_REDIRECTS;
        for (;;) {
          const response = await raceAbortSignal(
            previousFetch(new Request(url, {
              method,
              headers,
              body,
              signal,
              redirect: "manual",
            })),
            signal,
          );
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
          url = await checkHostFetchUrl(current.requestId, nextUrl);
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

  const normalizeDynamicFastBody = (value) => {
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
