  const DYNAMIC_HANDLE_CACHE_MAX_ENTRIES = 1_024;
  const DYNAMIC_HANDLE_CACHE_MAX_BYTES = 512 * 1024;
  const DYNAMIC_SOURCE_CACHE_MAX_ENTRIES = 64;
  const DYNAMIC_SOURCE_CACHE_MAX_BYTES = 8 * 1024 * 1024;
  const DYNAMIC_CANCELED_REPLY_MAX_ENTRIES = 4_096;
  const DYNAMIC_CANCELED_REPLY_TTL_MS = 5 * 60 * 1000;

  const incrementDynamicMetric = (metrics, name, count = 1) => {
    if (!metrics || typeof metrics !== "object") {
      return;
    }
    metrics[name] = Number(metrics[name] ?? 0) + Number(count ?? 0);
  };

  const estimateDynamicStringBytes = (value) => String(value ?? "").length * 2;

  const publishDynamicCacheMetrics = (metrics, prefix, entries, bytes) => {
    if (!metrics || typeof metrics !== "object") {
      return;
    }
    metrics[`${prefix}Entries`] = entries;
    metrics[`${prefix}Bytes`] = bytes;
  };

  const createDynamicLruCache = ({
    maxEntries,
    maxBytes,
    metrics,
    metricPrefix,
    estimateEntryBytes,
    onRemove,
  }) => {
    const entries = new Map();
    let totalBytes = 0;

    const publish = () => publishDynamicCacheMetrics(
      metrics,
      metricPrefix,
      entries.size,
      totalBytes,
    );
    const remove = (key) => {
      const entry = entries.get(key);
      if (!entry) {
        return false;
      }
      entries.delete(key);
      totalBytes = Math.max(0, totalBytes - entry.bytes);
      try {
        onRemove?.(entry.value, key);
      } catch {
      }
      return true;
    };
    const evictOldest = () => {
      const oldest = entries.keys().next();
      if (oldest.done) {
        return false;
      }
      remove(oldest.value);
      incrementDynamicMetric(metrics, `${metricPrefix}Evictions`);
      return true;
    };

    const cache = {
      get(key) {
        const entry = entries.get(key);
        if (!entry) {
          return undefined;
        }
        entries.delete(key);
        entries.set(key, entry);
        publish();
        return entry.value;
      },
      set(key, value) {
        remove(key);
        const entryBytes = Math.max(
          0,
          Number(estimateEntryBytes?.(key, value) ?? 0),
        );
        if (
          maxEntries <= 0
          || maxBytes <= 0
          || entryBytes > maxBytes
        ) {
          incrementDynamicMetric(metrics, `${metricPrefix}Evictions`);
          publish();
          return cache;
        }
        entries.set(key, { value, bytes: entryBytes });
        totalBytes += entryBytes;
        while (
          (entries.size > maxEntries || totalBytes > maxBytes)
          && evictOldest()
        ) {}
        publish();
        return cache;
      },
      delete(key) {
        const removed = remove(key);
        if (removed) {
          publish();
        }
        return removed;
      },
      clear() {
        for (const entry of entries.values()) {
          try {
            onRemove?.(entry.value, "");
          } catch {
          }
        }
        entries.clear();
        totalBytes = 0;
        publish();
      },
      get size() {
        return entries.size;
      },
      get bytes() {
        return totalBytes;
      },
    };
    publish();
    return cache;
  };

  const getDynamicCacheState = () => {
    const shared = getSharedEnv();
    return {
      handleCache: shared.__dd_dynamic_handle_cache,
      sourceCache: shared.__dd_dynamic_source_cache,
      metrics: shared.__dd_dynamic_metrics,
    };
  };

  const recordDynamicMetric = (name, count = 1) => {
    const metrics = getDynamicCacheState().metrics;
    incrementDynamicMetric(metrics, name, count);
  };

  const recordDynamicStageMetric = (prefix, durationMs) => {
    const metrics = getDynamicCacheState().metrics;
    metrics[`${prefix}Count`] = Number(metrics[`${prefix}Count`] ?? 0) + 1;
    metrics[`${prefix}TotalMs`] = Number(metrics[`${prefix}TotalMs`] ?? 0) + Number(durationMs ?? 0);
  };

  const dynamicHandleCacheKey = (bindingName, instanceId) => (
    `${workerName}\u001f${bindingName}\u001f${instanceId}`
  );

  const getDynamicHandleCacheEntry = (bindingName, instanceId) => (
    getDynamicCacheState().handleCache.get(dynamicHandleCacheKey(bindingName, instanceId)) ?? null
  );

  const setDynamicHandleCacheEntry = (bindingName, instanceId, entry) => {
    const cacheKey = dynamicHandleCacheKey(bindingName, instanceId);
    getDynamicCacheState().handleCache.set(
      cacheKey,
      Object.freeze({
        handle: String(entry.handle ?? ""),
        worker: String(entry.worker ?? ""),
        timeout: normalizeDynamicTimeout(entry.timeout),
      }),
    );
  };

  const deleteDynamicHandleCacheEntry = (bindingName, instanceId) => {
    const cacheKey = dynamicHandleCacheKey(bindingName, instanceId);
    getDynamicCacheState().handleCache.delete(cacheKey);
  };

  const dynamicHandleErrorIsStale = (result) => {
    const error = String(result?.error ?? "");
    return error.includes("dynamic worker handle not found")
      || error.includes("dynamic worker handle owner mismatch")
      || error.includes("dynamic worker handle generation mismatch")
      || error.includes("dynamic worker binding mismatch");
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

  const normalizeDynamicPolicy = (options) => {
    const parseBool = (value, fallback = false) => value == null ? fallback : Boolean(value);
    const parsePositiveInt = (label, value, fallback, max) => {
      if (value == null) {
        return fallback;
      }
      const parsed = Number(value);
      if (!Number.isFinite(parsed) || parsed <= 0) {
        throw new Error(`dynamic worker ${label} must be a positive number`);
      }
      const normalized = Math.trunc(parsed);
      return Math.min(normalized, max);
    };
    const normalizeHosts = (value) => {
      if (value == null) {
        return [];
      }
      if (!Array.isArray(value)) {
        throw new Error("dynamic worker egress_allow_hosts must be an array");
      }
      const seen = new Set();
      const out = [];
      for (const entry of value) {
        const host = String(entry ?? "").trim().toLowerCase();
        if (!host || seen.has(host)) {
          continue;
        }
        seen.add(host);
        out.push(host);
      }
      return out;
    };
    return Object.freeze({
      egress_allow_hosts: normalizeHosts(options.egress_allow_hosts),
      allow_host_rpc: parseBool(options.allow_host_rpc, false),
      allow_websocket: parseBool(options.allow_websocket, false),
      allow_transport: parseBool(options.allow_transport, false),
      allow_state_bindings: parseBool(options.allow_state_bindings, false),
      max_request_bytes: parsePositiveInt("max_request_bytes", options.max_request_bytes, 1_048_576, 64 * 1024 * 1024),
      max_response_bytes: parsePositiveInt("max_response_bytes", options.max_response_bytes, 2_097_152, 64 * 1024 * 1024),
      max_outbound_requests: parsePositiveInt("max_outbound_requests", options.max_outbound_requests, 16, 10_000),
      max_concurrency: parsePositiveInt("max_concurrency", options.max_concurrency, 32, 1_024),
    });
  };

  const normalizeDynamicBindings = (value) => {
    if (value == null) {
      return [];
    }
    if (!Array.isArray(value)) {
      throw new Error("dynamic worker bindings must be an array");
    }
    const seen = new Set();
    return value.map((entry) => {
      const type = String(entry?.type ?? "").trim().toLowerCase();
      const binding = String(entry?.binding ?? "").trim();
      if (!["kv", "memory", "dynamic"].includes(type)) {
        throw new Error(`dynamic worker binding type is invalid: ${type}`);
      }
      if (!binding) {
        throw new Error("dynamic worker binding name must not be empty");
      }
      if (seen.has(binding)) {
        throw new Error(`duplicate dynamic worker binding: ${binding}`);
      }
      seen.add(binding);
      return { type, binding };
    });
  };

  const buildModuleWorkerSource = (entrypointInput, modulesInput) => {
    const entrypoint = String(entrypointInput || "worker.js").trim();
    if (!entrypoint) {
      throw new Error("dynamic worker entrypoint must not be empty");
    }
    if (!isPlainObject(modulesInput)) {
      throw new Error("dynamic worker modules must be an object");
    }

    const modules = {};
    for (const [rawPath, rawCode] of Object.entries(modulesInput)) {
      modules[String(rawPath ?? "")] = String(rawCode ?? "");
    }

    const result = Deno.core.ops.op_dynamic_module_graph_register(
      entrypoint,
      modules,
    );
    if (!result || result.ok === false) {
      throw new Error(String(result?.error ?? "dynamic module graph registration failed"));
    }
    return Object.freeze({
      module_graph_id: String(result.graph_id ?? ""),
      module_entrypoint: String(result.entrypoint ?? ""),
    });
  };

  const estimateDynamicWorkerSourceBytes = (source) => {
    if (!source || typeof source !== "object") {
      return 0;
    }
    if (typeof source.source === "string") {
      return estimateDynamicStringBytes(source.source);
    }
    return estimateDynamicStringBytes(source.module_graph_id)
      + estimateDynamicStringBytes(source.module_entrypoint)
      + 32;
  };

  const resolveDynamicWorkerSource = (options) => {
    if (Object.prototype.hasOwnProperty.call(options, "source")) {
      const source = String(options.source ?? "");
      if (!source.trim()) {
        throw new Error("dynamic worker source must not be empty");
      }
      return Object.freeze({ source });
    }

    const entrypoint = String(options.entrypoint ?? "worker.js").trim();
    if (!entrypoint) {
      throw new Error("dynamic worker entrypoint must not be empty");
    }
    const modules = options.modules;
    if (!isPlainObject(modules)) {
      throw new Error("dynamic worker modules must be an object");
    }
    const built = buildModuleWorkerSource(entrypoint, modules);
    const { sourceCache } = getDynamicCacheState();
    const cacheKey = `${built.module_graph_id}\u001f${built.module_entrypoint}`;
    const cached = sourceCache.get(cacheKey);
    if (cached && typeof cached === "object") {
      recordDynamicMetric("sourceCacheHit");
      return cached;
    }
    recordDynamicMetric("sourceCacheMiss");
    sourceCache.set(cacheKey, built);
    return built;
  };

  const applyDynamicReplyBoundary = (result) => {
    if (!result || typeof result !== "object" || result.boundary_changed !== true) {
      recordDynamicMetric("timeSyncSkipped");
      return;
    }
    const started = performance.now();
    const boundary = normalizeBoundaryValue({
      nowMs: result.boundary_now_ms,
      perfMs: result.boundary_perf_ms,
    });
    if (boundary && typeof globalThis.__dd_set_time === "function") {
      globalThis.__dd_set_time(boundary.nowMs, boundary.perfMs);
      recordDynamicMetric("timeSyncApplied");
    } else {
      recordDynamicMetric("timeSyncSkipped");
    }
    recordDynamicStageMetric("timeSync", performance.now() - started);
  };

  const invokeRemoteDynamicWorkerFetch = async (
    bindingName,
    entry,
    request,
    requestContextHandle,
    timeout,
    cacheKey,
  ) => {
    const invokeLabel = `dynamic worker invoke (${bindingName}/${entry.handle})`;
    const invokeBase = Object.freeze({
      binding: bindingName,
      handle: entry.handle,
    });
    const startStarted = performance.now();
    recordDynamicMetric("remoteFetchHit");
    const headersHandle = storeResponseHeaders(request.headers);
    const bodyHandle = Math.max(
      0,
      Math.trunc(Number(callOp(
        "op_http_store_prepared_body",
        request.body ?? new Uint8Array(),
      ) ?? 0) || 0),
    );
    const result = await awaitDynamicReply(
      invokeLabel,
      () => callOp(
        "op_dynamic_worker_fetch_start",
        requestContextHandle,
        invokeBase.binding,
        invokeBase.handle,
        request.method,
        request.url,
        headersHandle,
        bodyHandle,
      ),
      timeout,
      { syncTime: false },
    ).finally(() => {
      recordDynamicStageMetric("dynamicStartOp", performance.now() - startStarted);
    });
    if (!result || typeof result !== "object" || result.ok === false) {
      if (cacheKey && (result?.stale_handle === true || dynamicHandleErrorIsStale(result))) {
        getDynamicCacheState().handleCache.delete(cacheKey);
      }
      applyDynamicReplyBoundary(result);
      throw new Error(formatDynamicFailure("dynamic worker invoke failed", result));
    }
    applyDynamicReplyBoundary(result);
    const materializeStarted = performance.now();
    const replyHeadersHandle = Math.max(0, Math.trunc(Number(result.headers_handle ?? 0) || 0));
    const replyBodyHandle = Math.max(0, Math.trunc(Number(result.body_handle ?? 0) || 0));
    const response = new Response(callOp("op_http_take_prepared_body", replyBodyHandle), {
      status: Number(result.status ?? 200),
      headers: callOp("op_http_take_prepared_headers", replyHeadersHandle),
    });
    recordDynamicStageMetric("replyMaterialize", performance.now() - materializeStarted);
    return response;
  };

  const createDynamicWorkerStub = (bindingName, entry, cacheKey = "") => {
    let activeEntry = entry;
    return {
      worker: activeEntry.worker,
      async fetch(inputValue, initValue = undefined) {
        const normalizeStarted = performance.now();
        const request = await normalizeDynamicFetchInput(inputValue, initValue);
        recordDynamicStageMetric("jsRequestNormalize", performance.now() - normalizeStarted);

        const scopedRequestContextHandle = activeRequestContextHandle();
        recordDynamicMetric("fastFetchPathHit");
        return invokeRemoteDynamicWorkerFetch(
          bindingName,
          activeEntry,
          request,
          scopedRequestContextHandle,
          activeEntry.timeout,
          cacheKey,
        );
      },
    };
  };

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

  const withDynamicStageTimeout = async (label, promise, timeoutMs = 5_000) => {
    let timeoutId = 0;
    const timeoutError = new Promise((_, reject) => {
      timeoutId = setTimeout(
        () => reject(new Error(`${label} timed out after ${timeoutMs}ms`)),
        timeoutMs,
      );
    });
    return Promise.race([promise, timeoutError]).finally(() => clearTimeout(timeoutId));
  };

  const dynamicReplyWaiters = () => (globalThis.__dd_dynamic_reply_waiters ??= new Map());
  const dynamicReplyReady = () => (globalThis.__dd_dynamic_reply_ready ??= new Map());
  const dynamicReplyCanceled = () => (globalThis.__dd_dynamic_reply_canceled ??= new Map());
  const dynamicReplyNow = () => (
    globalThis.performance && typeof globalThis.performance.now === "function"
      ? globalThis.performance.now()
      : Date.now()
  );

  const publishCanceledReplyMetrics = (canceled) => {
    const metrics = getDynamicCacheState().metrics;
    if (metrics && typeof metrics === "object") {
      metrics.canceledReplyEntries = canceled.size;
    }
  };

  const sweepDynamicReplyCanceled = (canceled, now = dynamicReplyNow()) => {
    let evictions = 0;
    for (const [replyId, expiresAt] of canceled.entries()) {
      if (expiresAt > now && canceled.size <= DYNAMIC_CANCELED_REPLY_MAX_ENTRIES) {
        break;
      }
      canceled.delete(replyId);
      evictions += 1;
    }
    while (canceled.size > DYNAMIC_CANCELED_REPLY_MAX_ENTRIES) {
      const oldest = canceled.keys().next();
      if (oldest.done) {
        break;
      }
      canceled.delete(oldest.value);
      evictions += 1;
    }
    if (evictions > 0) {
      recordDynamicMetric("canceledReplyEvictions", evictions);
    }
    publishCanceledReplyMetrics(canceled);
  };

  const discardDynamicReplyHandles = (payload) => {
    const headersHandle = Math.max(0, Math.trunc(Number(payload?.headers_handle ?? 0) || 0));
    if (headersHandle > 0) {
      callOp("op_http_take_prepared_headers", headersHandle);
    }
    const bodyHandle = Math.max(0, Math.trunc(Number(payload?.body_handle ?? 0) || 0));
    if (bodyHandle > 0) {
      callOp("op_http_take_prepared_body", bodyHandle);
    }
    const valueHandle = Math.max(0, Math.trunc(Number(payload?.value_handle ?? 0) || 0));
    if (valueHandle > 0) {
      callOp("op_http_take_prepared_body", valueHandle);
    }
  };

  const deliverDynamicReply = (payload) => {
    const replyId = String(payload?.reply_id ?? "").trim();
    if (!replyId) {
      return;
    }
    const canceled = dynamicReplyCanceled();
    const canceledUntil = canceled.get(replyId);
    if (canceledUntil !== undefined) {
      canceled.delete(replyId);
      discardDynamicReplyHandles(payload);
      recordDynamicMetric("lateCanceledReply");
      publishCanceledReplyMetrics(canceled);
      return;
    }
    sweepDynamicReplyCanceled(canceled);
    const waiters = dynamicReplyWaiters();
    const ready = dynamicReplyReady();
    const waiter = waiters.get(replyId);
    if (waiter) {
      waiters.delete(replyId);
      waiter.resolve(payload);
      return;
    }
    ready.set(replyId, payload);
  };

  const waitForDynamicReply = (replyId) => {
    const ready = dynamicReplyReady();
    if (ready.has(replyId)) {
      const payload = ready.get(replyId);
      ready.delete(replyId);
      return Promise.resolve(payload);
    }
    return new Promise((resolve, reject) => {
      dynamicReplyWaiters().set(replyId, { resolve, reject });
    });
  };

  const cancelDynamicReply = (replyId, cause) => {
    if (!replyId) {
      return;
    }
    const canceled = dynamicReplyCanceled();
    const now = dynamicReplyNow();
    sweepDynamicReplyCanceled(canceled, now);
    canceled.set(replyId, now + DYNAMIC_CANCELED_REPLY_TTL_MS);
    sweepDynamicReplyCanceled(canceled, now);
    const ready = dynamicReplyReady();
    const readyPayload = ready.get(replyId);
    if (readyPayload) {
      discardDynamicReplyHandles(readyPayload);
    }
    ready.delete(replyId);
    const waiters = dynamicReplyWaiters();
    const waiter = waiters.get(replyId);
    if (waiter) {
      waiters.delete(replyId);
      waiter.reject(cause);
    }
    callOp("op_dynamic_cancel_reply", replyId);
  };

  const awaitDynamicReply = async (label, startOp, timeoutMs = 5_000, options = undefined) => {
    const started = startOp();
    if (!started || typeof started !== "object" || started.ok === false) {
      throw new Error(String(started?.error ?? `${label} failed to start`));
    }
    const replyId = String(started.reply_id ?? "").trim();
    if (!replyId) {
      throw new Error(`${label} missing reply id`);
    }
    let timeoutId = null;
    try {
      const reply = waitForDynamicReply(replyId);
      const timeoutError = new Promise((_, reject) => {
        timeoutId = setTimeout(
          () => reject(new Error(`${label} timed out after ${timeoutMs}ms`)),
          timeoutMs,
        );
      });
      const result = await Promise.race([reply, timeoutError]);
      if (options?.syncTime !== false) {
        await syncFrozenTime();
      }
      return result;
    } catch (error) {
      cancelDynamicReply(replyId, error);
      throw error;
    } finally {
      clearTimeout(timeoutId);
    }
  };

  const formatDynamicFailure = (fallback, result) => {
    if (result && typeof result === "object") {
      if (typeof result.error === "string" && result.error) {
        return result.error;
      }
      try {
        return JSON.stringify(result);
      } catch {
        return fallback;
      }
    }
    return String(result ?? fallback);
  };

  const drainDynamicControlQueue = async () => {
    for (;;) {
      const batch = callOp("op_dynamic_take_control_items");
      if (!Array.isArray(batch) || batch.length === 0) {
        return;
      }
      const started = performance.now();
      for (const item of batch) {
        if (item?.kind === "reply") {
          deliverDynamicReply(item.payload);
        }
      }
      recordDynamicStageMetric("dynamicControlDrain", performance.now() - started);
    }
  };

  globalThis.__dd_drain_dynamic_control_queue = drainDynamicControlQueue;
  globalThis.__dd_await_dynamic_reply = awaitDynamicReply;
  globalThis.__dd_encode_rpc_args = encodeRpcArgs;

  /**
   * @typedef {Object} DynamicWorkerConfig
   * @property {string} [source] Full worker source. If omitted, `entrypoint + modules` are used.
   * @property {string} [entrypoint] Entrypoint module path when using `modules`. Defaults to `worker.js`.
   * @property {Record<string, string>} [modules] Module graph map: `modulePath -> source`.
   * @property {{type:string,binding:string}[]} [bindings] Runtime bindings for the dynamic worker.
   * @property {Record<string, any>} [env] Env bindings for the dynamic worker.
   * String values stay opaque and are only resolved at host I/O boundaries; `RpcTarget` values become host RPC bindings.
   * @property {number} [timeout] Per-invoke timeout in ms. Default `5000`, max `60000`.
   * @property {string[]} [egress_allow_hosts] Exact host / host:port / wildcard origins allowed for outbound fetch. Default deny.
   * @property {boolean} [allow_host_rpc] Allow `RpcTarget` env bindings. Default `false`.
   * @property {boolean} [allow_websocket] Allow websocket upgrade responses. Default `false`.
   * @property {boolean} [allow_transport] Allow transport upgrade responses. Default `false`.
   * @property {boolean} [allow_state_bindings] Allow stateful runtime bindings like cache. Default `false`.
   * @property {number} [max_request_bytes] Maximum inbound request bytes per child invoke.
   * @property {number} [max_response_bytes] Maximum outbound response bytes per child invoke.
   * @property {number} [max_outbound_requests] Maximum outbound host fetch count across child lifetime.
   * @property {number} [max_concurrency] Maximum concurrent inflight invokes for child.
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
      const cacheKey = dynamicHandleCacheKey(bindingName, instanceId);
      const cached = getDynamicHandleCacheEntry(bindingName, instanceId);
      if (cached) {
        recordDynamicMetric("handleCacheHit");
        return createDynamicWorkerStub(bindingName, cached, cacheKey);
      }
      recordDynamicMetric("handleCacheMiss");
      const scopedRequestContextHandle = activeRequestContextHandle();
      const lookup = await awaitDynamicReply(
        `dynamic worker lookup (${bindingName}/${instanceId})`,
        () => callOp("op_dynamic_worker_lookup", scopedRequestContextHandle, bindingName, instanceId),
      );
      if (!lookup || typeof lookup !== "object" || lookup.ok === false) {
        throw new Error(formatDynamicFailure("dynamic worker lookup failed", lookup));
      }
      if (lookup.found === true) {
        setDynamicHandleCacheEntry(bindingName, instanceId, lookup);
        const created = getDynamicHandleCacheEntry(bindingName, instanceId) ?? Object.freeze({
          handle: String(lookup.handle ?? "").trim(),
          worker: String(lookup.worker ?? ""),
          timeout: normalizeDynamicTimeout(lookup.timeout),
        });
        return createDynamicWorkerStub(bindingName, created, cacheKey);
      }

      const options = await parseDynamicFactoryOptions(factory);
      const workerSource = resolveDynamicWorkerSource(options);
      const policy = normalizeDynamicPolicy(options);
      const bindings = normalizeDynamicBindings(options.bindings);
      const envInput = options.env;
      const envConfig = splitDynamicEnvInput(envInput, policy);
      const timeout = normalizeDynamicTimeout(options.timeout);
      const result = await awaitDynamicReply(
        `dynamic worker create (${bindingName}/${instanceId})`,
        () => callOp(
          "op_dynamic_worker_create",
          scopedRequestContextHandle,
          bindingName,
          instanceId,
          String(workerSource.source ?? ""),
          String(workerSource.module_graph_id ?? ""),
          String(workerSource.module_entrypoint ?? ""),
          bindings,
          envConfig.stringEnv,
          timeout,
          policy,
          envConfig.hostRpcBindings,
        ),
      );
      if (!result || typeof result !== "object" || result.ok === false) {
        throw new Error(formatDynamicFailure("dynamic worker create failed", result));
      }
      const handle = String(result.handle ?? "").trim();
      if (!handle) {
        throw new Error("dynamic worker create returned an invalid handle");
      }
      setDynamicHandleCacheEntry(bindingName, instanceId, result);
      const created = getDynamicHandleCacheEntry(bindingName, instanceId) ?? Object.freeze({
        handle,
        worker: String(result.worker ?? ""),
        timeout: normalizeDynamicTimeout(result.timeout ?? timeout),
      });
      return createDynamicWorkerStub(bindingName, created, cacheKey);
    },
    /**
     * List active dynamic worker ids for this namespace in the current owner worker generation.
     *
     * @returns {Promise<string[]>}
     */
    async list() {
      const scopedRequestContextHandle = activeRequestContextHandle();
      const result = await awaitDynamicReply(
        `dynamic worker list (${bindingName})`,
        () => callOp("op_dynamic_worker_list", scopedRequestContextHandle, bindingName),
      );
      if (!result || typeof result !== "object" || result.ok === false) {
        throw new Error(formatDynamicFailure("dynamic worker list failed", result));
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
      const scopedRequestContextHandle = activeRequestContextHandle();
      const result = await awaitDynamicReply(
        `dynamic worker delete (${bindingName}/${instanceId})`,
        () => callOp("op_dynamic_worker_delete", scopedRequestContextHandle, bindingName, instanceId),
      );
      if (!result || typeof result !== "object" || result.ok === false) {
        throw new Error(formatDynamicFailure("dynamic worker delete failed", result));
      }
      if (result.deleted === true) {
        deleteDynamicHandleCacheEntry(bindingName, instanceId);
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
      globalThis.__dd_dynamic_metrics = cached.__dd_dynamic_metrics ?? null;
      return cached;
    }
    const env = {};
    const dynamicMetrics = {
      handleCacheHit: 0,
      handleCacheMiss: 0,
      handleCacheEntries: 0,
      handleCacheBytes: 0,
      handleCacheEvictions: 0,
      sourceCacheHit: 0,
      sourceCacheMiss: 0,
      sourceCacheEntries: 0,
      sourceCacheBytes: 0,
      sourceCacheEvictions: 0,
      canceledReplyEntries: 0,
      canceledReplyEvictions: 0,
      lateCanceledReply: 0,
      remoteFetchHit: 0,
      remoteFetchFallback: 0,
      normalizeFastPathHit: 0,
      normalizeSlowPathHit: 0,
    };
    Object.defineProperty(env, "__dd_dynamic_metrics", {
      value: dynamicMetrics,
      enumerable: false,
      configurable: false,
      writable: false,
    });
    Object.defineProperty(env, "__dd_dynamic_handle_cache", {
      value: createDynamicLruCache({
        maxEntries: DYNAMIC_HANDLE_CACHE_MAX_ENTRIES,
        maxBytes: DYNAMIC_HANDLE_CACHE_MAX_BYTES,
        metrics: dynamicMetrics,
        metricPrefix: "handleCache",
        estimateEntryBytes: (key, entry) => (
          estimateDynamicStringBytes(key)
          + estimateDynamicStringBytes(entry?.handle)
          + estimateDynamicStringBytes(entry?.worker)
          + 32
        ),
      }),
      enumerable: false,
      configurable: false,
      writable: false,
    });
    Object.defineProperty(env, "__dd_dynamic_source_cache", {
      value: createDynamicLruCache({
        maxEntries: DYNAMIC_SOURCE_CACHE_MAX_ENTRIES,
        maxBytes: DYNAMIC_SOURCE_CACHE_MAX_BYTES,
        metrics: dynamicMetrics,
        metricPrefix: "sourceCache",
        estimateEntryBytes: (key, source) => (
          estimateDynamicStringBytes(key)
          + estimateDynamicWorkerSourceBytes(source)
        ),
        onRemove: (source) => {
          const graphId = String(source?.module_graph_id ?? "").trim();
          if (graphId) {
            Deno.core.ops.op_dynamic_module_graph_release(graphId);
          }
        },
      }),
      enumerable: false,
      configurable: false,
      writable: false,
    });
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
    for (const [envName, bindingName] of kvBindingsConfig) {
      if (!envName) {
        continue;
      }
      defineLazyValue(env, envName, () => createKvBinding(bindingName));
    }

    for (const [envName, bindingName] of dynamicBindingsConfig) {
      if (!envName) {
        continue;
      }
      defineLazyValue(
        env,
        envName,
        () => createDynamicNamespace(bindingName || envName),
      );
    }

    for (const [envName, bindingName] of dynamicRpcBindingsConfig) {
      if (!envName) {
        continue;
      }
      defineLazyValue(
        env,
        envName,
        () => createDynamicHostRpcNamespace(bindingName || envName),
      );
    }

    for (const [envName, value] of dynamicEnvConfig) {
      if (!envName) {
        continue;
      }
      Object.defineProperty(env, envName, {
        value: String(value ?? ""),
        enumerable: true,
        configurable: true,
        writable: true,
      });
    }

    for (const bindingName of memoryBindingsConfig) {
      if (!bindingName) {
        continue;
      }
      defineLazyValue(env, bindingName, () => createMemoryNamespace(bindingName));
    }

    Object.freeze(env);
    globalThis.__dd_dynamic_metrics = env.__dd_dynamic_metrics ?? null;
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
    const idempotencyKey = String(
      descriptor?.idempotency_key ?? descriptor?.idempotencyKey ?? "",
    ).trim();
    if (!liveToken && !exportName && !source) {
      throw new Error("memory operation executable descriptor must not be empty");
    }
    return {
      descriptor: { liveToken, exportName, source, idempotencyKey },
      args,
    };
  };

  const instantiateExecutable = (descriptor) => {
    const liveToken = String(descriptor?.liveToken ?? "").trim();
    if (liveToken && liveMemoryExecutables.has(liveToken)) {
      const executable = liveMemoryExecutables.get(liveToken);
      liveMemoryExecutables.delete(liveToken);
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

  const executeMemoryTransaction = async (
    entry,
    runtimeRequestId,
    executable,
    args,
    options = undefined,
  ) => {
    await ensureMemoryStorageHydrated(entry, runtimeRequestId, { force: true });
    const commandHandle = Math.max(0, Math.trunc(Number(options?.commandHandle ?? 0) || 0));
    const txn = createMemoryTxn(entry, { commandHandle });
    let encodedResult = null;
    const scopedState = createMemoryAtomicState(
      entry,
      runtimeRequestId,
      true,
      true,
      txn,
    );
    const current = currentRequestContext();
    const previousMemoryEntry = current.memoryEntry;
    const previousMemoryRequestId = current.memoryRequestId;
    const previousSocketRuntimeProvider = current.socketRuntimeProvider;
    const previousTransportRuntimeProvider = current.transportRuntimeProvider;
    current.memoryEntry = entry;
    current.memoryRequestId = runtimeRequestId;
    current.socketRuntimeProvider = () => scopedState.__dd_socket_runtime;
    current.transportRuntimeProvider = () => scopedState.__dd_transport_runtime;
    try {
      const value = withMemoryTxnScope(
        {
          binding: entry.binding,
          memoryKey: entry.memoryKey,
          state: scopedState,
          stub: scopedState.stub,
        },
        () => executable(scopedState, ...args),
      );
      if (value instanceof Promise) {
        throw new Error("stub.atomic callback must be synchronous");
      }
      if (commandHandle > 0) {
        if (encodedResult === null) {
          encodedResult = await encodeRpcResult(value);
        }
        setMemoryBatchCommandResult(txn, encodedResult);
      }
      await finishMemoryTxn(txn, runtimeRequestId);
      return { value, encodedResult };
    } finally {
      closeMemoryTxnBatch(txn);
      current.memoryEntry = previousMemoryEntry;
      current.memoryRequestId = previousMemoryRequestId;
      current.socketRuntimeProvider = previousSocketRuntimeProvider;
      current.transportRuntimeProvider = previousTransportRuntimeProvider;
    }
  };

  const buildWakeEvent = (memoryCall, stub) => {
    const kind = String(memoryCall.kind ?? "").trim();
    const event = {
      type: kind,
      binding: String(memoryCall.binding ?? ""),
      key: String(memoryCall.key ?? ""),
    };
    Object.defineProperty(event, "stub", {
      value: stub,
      enumerable: false,
      configurable: true,
      writable: false,
    });
    if ("handle" in memoryCall) {
      event.handle = String(memoryCall.handle ?? "");
    }
    if (kind === "message") {
      const raw = toArrayBytes(memoryCall.data);
      event.data = memoryCall.is_text === true ? Deno.core.decode(raw) : raw;
      event.isText = memoryCall.is_text === true;
      event.type = "socketmessage";
    } else if (kind === "close") {
      event.code = Number(memoryCall.code ?? 1000);
      event.reason = String(memoryCall.reason ?? "");
      event.type = "socketclose";
    } else if (kind === "transport_datagram" || kind === "datagram") {
      event.data = toArrayBytes(memoryCall.data);
      event.type = "transportdatagram";
    } else if (kind === "transport_stream" || kind === "stream") {
      event.data = toArrayBytes(memoryCall.data);
      event.type = "transportstream";
    } else if (kind === "transport_close" || kind === "transport") {
      event.code = Number(memoryCall.code ?? 0);
      event.reason = String(memoryCall.reason ?? "");
      event.type = "transportclose";
    }
    return event;
  };

  const seedMemoryHandleSnapshots = (entry, memoryCall) => {
    if (!memoryCall || typeof memoryCall !== "object") {
      return;
    }
    if (Array.isArray(memoryCall.socket_handles)) {
      entry.openSocketHandles = new Set(
        memoryCall.socket_handles.map((value) => String(value)),
      );
      entry.openSocketHandlesInitialized = true;
    }
    if (Array.isArray(memoryCall.transport_handles)) {
      entry.openTransportHandles = new Set(
        memoryCall.transport_handles.map((value) => String(value)),
      );
      entry.openTransportHandlesInitialized = true;
    }
  };

  const invokeMemoryCall = async (memoryCall, request, env) => {
    if (!memoryCall || typeof memoryCall !== "object") {
      throw new Error("memory invoke config is missing");
    }
    const binding = String(memoryCall.binding ?? "").trim();
    const memoryKey = String(memoryCall.key ?? "").trim();
    if (!binding || !memoryKey) {
      throw new Error("memory invoke requires binding and key");
    }
    if (!Object.prototype.hasOwnProperty.call(env, binding)) {
      throw new Error(`memory binding not declared for worker: ${binding}`);
    }
    const runtimeRequestId = activeRequestId();
    const entry = await ensureMemoryEntry(binding, memoryKey, runtimeRequestId, { hydrate: false });
    seedMemoryHandleSnapshots(entry, memoryCall);
    const kind = String(memoryCall.kind ?? "");
    const scopedState = createMemoryRuntimeState(entry, runtimeRequestId, false, false);
    const current = currentRequestContext();
    const previousMemoryEntry = current.memoryEntry;
    const previousMemoryRequestId = current.memoryRequestId;
    const previousSocketRuntimeProvider = current.socketRuntimeProvider;
    const previousTransportRuntimeProvider = current.transportRuntimeProvider;
    current.socketRuntimeProvider = () => scopedState.__dd_socket_runtime;
    current.transportRuntimeProvider = () => scopedState.__dd_transport_runtime;
    current.memoryEntry = entry;
    current.memoryRequestId = runtimeRequestId;
    try {
      if (kind === "method") {
        await scopedState.__dd_socket_runtime.refreshOpenHandles();
        await scopedState.__dd_transport_runtime.refreshOpenHandles();
        const methodName = String(memoryCall.name ?? "").trim();
        if (!methodName) {
          throw new Error("memory method invoke requires a method name");
        }
        if (
          memoryMethodNameIsBlocked(methodName)
          && methodName !== MEMORY_ATOMIC_METHOD
        ) {
          throw new Error(`memory method is blocked: ${methodName}`);
        }
        if (methodName !== MEMORY_ATOMIC_METHOD) {
          throw new Error(`unsupported memory method: ${methodName}`);
        }
        const { descriptor, args } = decodeExecPayload(memoryCall.args);
        const command = await beginMemoryCommand(
          entry,
          descriptor.idempotencyKey,
        );
        if (command.hit === true) {
          return new Response(command.value ?? new Uint8Array(), {
            status: 200,
            headers: [["content-type", "application/octet-stream"]],
          });
        }
        try {
          const executable = instantiateExecutable(descriptor);
          const transaction = await executeMemoryTransaction(
            entry,
            runtimeRequestId,
            executable,
            args,
            { commandHandle: command.handle },
          );
          const encoded = transaction.encodedResult ?? await encodeRpcResult(transaction.value);
          return new Response(encoded, {
            status: 200,
            headers: [["content-type", "application/octet-stream"]],
          });
        } finally {
          closeMemoryCommand(command.handle);
        }
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
        const event = buildWakeEvent(memoryCall, createMemoryStub(binding, memoryKey));
        await wakeMethod.call(worker, event, env);
        await gateMemoryOutput(entry, runtimeRequestId, async () => undefined);
        return new Response(null, { status: 204 });
      }
      throw new Error(`unsupported memory invoke kind: ${kind}`);
    } finally {
      current.memoryEntry = previousMemoryEntry;
      current.memoryRequestId = previousMemoryRequestId;
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
    await syncFrozenTime();
    const emitted = callOp(
      "op_emit_wait_until_done",
      requestContext.completionHandle,
    );
    if (emitted === false) {
      return;
    }
    if (requestContext.memoryRequestScopeHandle > 0) {
      callOp("op_memory_request_scope_close", requestContext.memoryRequestScopeHandle);
      requestContext.memoryRequestScopeHandle = 0;
    }
    if (requestContext.requestContextHandle > 0) {
      callOp("op_request_context_close", requestContext.requestContextHandle);
      requestContext.requestContextHandle = 0;
    }
  };

  const storeResponseHeaders = (headers) => {
    return Math.max(
      0,
      Math.trunc(Number(callOp(
        "op_http_store_prepared_headers",
        Array.isArray(headers) ? headers : [],
      ) ?? 0) || 0),
    );
  };

  const emitResponseStart = async (status, headersHandle) => {
    await syncFrozenTime();
    callOp(
      "op_emit_response_start",
      requestContext.completionHandle,
      status,
      headersHandle,
    );
  };

  const emitResponseChunk = async (chunk) => {
    await syncFrozenTime();
    const bytes = toByteChunk(chunk);
    const result = await callOp(
      "op_emit_response_chunk",
      requestContext.completionHandle,
      bytes,
    );
    if (!result || typeof result !== "object" || result.ok === false) {
      throw new Error(String(result?.error ?? "response stream chunk failed"));
    }
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
    callOp("op_request_wait_until_register", requestContext.completionHandle);
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
    try {
      await syncFrozenTime();
      installHostFetch();
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
        : memoryCallConfig
          ? await invokeMemoryCall(memoryCallConfig, workerRequest, env)
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
      const bodyChunks = streamResponse ? null : [];
      let bodyLength = 0;
      if (streamResponse) {
        await emitResponseStart(status, storeResponseHeaders(headers));
      }
      if (!isWebSocketAcceptResponse && response.body) {
        const reader = response.body.getReader();
        while (true) {
          const { done, value } = await reader.read();
          if (done) {
            break;
          }
          const chunk = toByteChunk(value);
          if (chunk.length === 0) {
            continue;
          }
          if (streamResponse) {
            await emitResponseChunk(chunk);
          } else {
            bodyChunks.push(chunk);
            bodyLength += chunk.byteLength;
          }
        }
      }

      const result = {
        status,
        headersHandle: streamResponse ? 0 : storeResponseHeaders(headers),
        bodyHandle: 0,
      };
      if (!streamResponse) {
        result.bodyHandle = callOp(
          "op_http_store_prepared_body",
          concatByteChunks(bodyChunks, bodyLength),
        );
      }
      return result;
    } finally {
      if (requestContext.requestBodyStreamHandle > 0) {
        try {
          await callOp("op_request_body_cancel", requestContext.requestBodyStreamHandle);
        } catch {
        }
      }
      inflightRequests.delete(requestId);
      if (requestContextHandle > 0) {
        globalThis.__dd_inflight_requests_by_context_handle?.delete(
          requestContextHandle,
        );
      }
    }
  })())
    .then(async (result) => {
      await settlePendingKvWriteScheduling();
      callOp(
        "op_emit_completion_ok",
        requestContext.completionHandle,
        Number(result.status ?? 200),
        Math.max(0, Math.trunc(Number(result.headersHandle ?? 0) || 0)),
        Math.max(0, Math.trunc(Number(result.bodyHandle ?? 0) || 0)),
      );

      await emitWaitUntilDone(!(await waitForWaitUntils()));
    })
    .catch(async (error) => {
      await settlePendingKvWriteScheduling();
      const message = String((error && (error.stack || error.message)) || error);
      callOp(
        "op_emit_completion_error",
        requestContext.completionHandle,
        message,
      );

      await emitWaitUntilDone(!(await waitForWaitUntils()));
    });
};

globalThis.__dd_execute_worker_handle = (requestHandle) => {
  const handle = Number(requestHandle);
  const descriptor = Deno.core.ops.op_request_invocation_descriptor(handle);
  if (descriptor === null || descriptor === undefined) {
    throw new Error(`Request handle ${requestHandle} is unavailable`);
  }
  const requestHeadersHandle = Math.max(
    0,
    Math.trunc(Number(descriptor.request_headers_handle ?? 0) || 0),
  );
  const requestBodyHandle = Math.max(
    0,
    Math.trunc(Number(descriptor.request_body_handle ?? 0) || 0),
  );
  const headers = Deno.core.ops.op_http_take_prepared_headers(requestHeadersHandle);
  const body = Deno.core.ops.op_http_take_prepared_body(requestBodyHandle);
  const payload = {
    request_id: String(descriptor.request_id ?? ""),
    request_context_handle: Math.max(
      0,
      Math.trunc(Number(descriptor.request_context_handle ?? 0) || 0),
    ),
    completion_handle: Math.max(
      0,
      Math.trunc(Number(descriptor.completion_handle ?? 0) || 0),
    ),
    memory_request_scope_handle: Math.max(
      0,
      Math.trunc(Number(descriptor.memory_request_scope_handle ?? 0) || 0),
    ),
    memory_call: descriptor.memory_call ?? null,
    host_rpc_call: descriptor.host_rpc_call ?? null,
    request_body_stream_handle: Math.max(
      0,
      Math.trunc(Number(descriptor.request_body_stream_handle ?? 0) || 0),
    ),
    stream_response: descriptor.stream_response === true,
    method: String(descriptor.method ?? "GET"),
    url: String(descriptor.url ?? ""),
    headers: Array.isArray(headers) ? headers : [],
    input_request_id: String(descriptor.input_request_id ?? ""),
    body,
  };
  return globalThis.__dd_execute_worker(payload);
};

globalThis.__dd_install_worker_deployment_handle = (deploymentHandle) => {
  const payload = Deno.core.ops.op_take_worker_deployment_config(Number(deploymentHandle));
  if (payload === null || payload === undefined) {
    throw new Error(`Worker deployment handle ${deploymentHandle} is unavailable`);
  }
  const normalizeBindingPairs = (input) => Object.freeze(
    (Array.isArray(input) ? input : [])
      .map((entry) => {
        const envName = Array.isArray(entry)
          ? String(entry[0] ?? "").trim()
          : String(entry ?? "").trim();
        const bindingName = Array.isArray(entry)
          ? String(entry[1] ?? entry[0] ?? "").trim()
          : envName;
        return Object.freeze([envName, bindingName || envName]);
      })
      .filter(([envName]) => envName.length > 0),
  );
  const normalizeNames = (input) => Object.freeze(
    (Array.isArray(input) ? input : [])
      .map((entry) => String(entry ?? "").trim())
      .filter((entry) => entry.length > 0),
  );
  const normalizeEnv = (input) => Object.freeze(
    (Array.isArray(input) ? input : [])
      .map((entry) => {
        const envName = Array.isArray(entry)
          ? String(entry[0] ?? "").trim()
          : "";
        return Object.freeze([envName, String(Array.isArray(entry) ? entry[1] ?? "" : "")]);
      })
      .filter(([envName]) => envName.length > 0),
  );
  const normalizeKvReadCacheConfig = (input) => {
    const maxEntries = Math.max(
      1,
      Math.trunc(Number(input?.max_entries ?? 16384) || 16384),
    );
    const maxBytes = Math.max(
      1024,
      Math.trunc(Number(input?.max_bytes ?? 16 * 1024 * 1024) || (16 * 1024 * 1024)),
    );
    const hitTtlMs = Math.max(
      1,
      Math.trunc(Number(input?.hit_ttl_ms ?? 300_000) || 300_000),
    );
    const missTtlMs = Math.max(
      1,
      Math.trunc(Number(input?.miss_ttl_ms ?? 30_000) || 30_000),
    );
    return Object.freeze({
      maxEntries,
      maxBytes,
      hitTtlMs,
      missTtlMs,
    });
  };
  globalThis.__dd_worker_deployment = Object.freeze({
    worker_name: String(payload.worker_name ?? ""),
    kv_bindings: normalizeBindingPairs(payload.kv_bindings),
    kv_read_cache_config: normalizeKvReadCacheConfig(payload.kv_read_cache_config ?? null),
    memory_bindings: normalizeNames(payload.memory_bindings),
    dynamic_bindings: normalizeBindingPairs(payload.dynamic_bindings),
    dynamic_rpc_bindings: normalizeBindingPairs(payload.dynamic_rpc_bindings),
    dynamic_env: normalizeEnv(payload.dynamic_env),
  });
};

globalThis.__dd_drain_dynamic_control_queue_handle = () => {
  const drain = globalThis.__dd_drain_dynamic_control_queue;
  if (typeof drain !== "function") {
    return;
  }
  void Promise.resolve(drain()).catch(() => undefined);
};

globalThis.__dd_abort_worker_request_handle = (requestContextHandle) => {
  const handle = Math.max(0, Math.trunc(Number(requestContextHandle ?? 0) || 0));
  if (handle === 0) {
    return false;
  }
  const inflightRequests = globalThis.__dd_inflight_requests_by_context_handle;
  const inflight = inflightRequests?.get(handle);
  if (!inflight) {
    return false;
  }
  if (inflight?.requestBodyStreamHandle > 0) {
    try {
      Deno.core.ops.op_request_body_cancel(inflight.requestBodyStreamHandle);
    } catch {
    }
  }
  if (inflight?.requestContextHandle > 0) {
    try {
      Deno.core.ops.op_request_context_cancel(inflight.requestContextHandle);
    } catch {
    }
  }
  if (inflight?.memoryRequestScopeHandle > 0) {
    try {
      Deno.core.ops.op_memory_request_scope_close(inflight.memoryRequestScopeHandle);
    } catch {
    }
  }
  if (inflight?.controller) {
    inflight.controller.abort(new Error("Request aborted by caller"));
  }
  return true;
};
