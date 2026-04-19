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
    metrics[name] = Number(metrics[name] ?? 0) + Number(count ?? 0);
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
    const rewrittenByPath = new Map();
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
        rewrittenByPath.set(modulePath, rewritten);
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

    const entrySource = rewrittenByPath.get(entrypoint);
    if (!entrySource) {
      throw new Error("dynamic worker entrypoint source could not be built");
    }
    return `${entrySource}\n`;
  };

  const dynamicSourceCacheKey = (entrypointInput, modulesInput) => {
    const entrypoint = normalizeModulePath(entrypointInput || "worker.js");
    if (!isPlainObject(modulesInput)) {
      throw new Error("dynamic worker modules must be an object");
    }
    const keys = Object.keys(modulesInput).sort();
    let out = `${entrypoint}\u001f`;
    for (const key of keys) {
      const modulePath = normalizeModulePath(key);
      const source = String(modulesInput[key] ?? "");
      out += `${modulePath.length}:${modulePath}\u001e${source.length}:${source}\u001f`;
    }
    return out;
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
    const { sourceCache } = getDynamicCacheState();
    const cacheKey = dynamicSourceCacheKey(entrypoint, modules);
    const cached = sourceCache.get(cacheKey);
    if (typeof cached === "string" && cached.length > 0) {
      recordDynamicMetric("sourceCacheHit");
      return cached;
    }
    recordDynamicMetric("sourceCacheMiss");
    const built = buildSourceFromModules(entrypoint, modules);
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
    requestId,
    invokeSeq,
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
    const result = await awaitDynamicReply(
      invokeLabel,
      () => callOp("op_dynamic_worker_fetch_start", {
        request_id: requestId,
        subrequest_id: `${requestId}:dynamic:${invokeSeq}`,
        binding: invokeBase.binding,
        handle: invokeBase.handle,
        method: request.method,
        url: request.url,
        headers: request.headers,
        body: Array.from(request.body),
      }),
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
    const response = new Response(toArrayBytes(result.body), {
      status: Number(result.status ?? 200),
      headers: Array.isArray(result.headers) ? result.headers : [],
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

        const scopedRequestId = activeRequestId();
        const invokeSeq = nextMemoryInvokeSeq();
        recordDynamicMetric("fastFetchPathHit");
        return invokeRemoteDynamicWorkerFetch(
          bindingName,
          activeEntry,
          request,
          scopedRequestId,
          invokeSeq,
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
  const dynamicReplyCanceled = () => (globalThis.__dd_dynamic_reply_canceled ??= new Set());

  const deliverDynamicReply = (payload) => {
    const replyId = String(payload?.reply_id ?? "").trim();
    if (!replyId) {
      return;
    }
    const canceled = dynamicReplyCanceled();
    const waiters = dynamicReplyWaiters();
    const ready = dynamicReplyReady();
    if (canceled.delete(replyId)) {
      return;
    }
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
    dynamicReplyCanceled().add(replyId);
    dynamicReplyReady().delete(replyId);
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
      const scopedRequestId = activeRequestId();
      const lookup = await awaitDynamicReply(
        `dynamic worker lookup (${bindingName}/${instanceId})`,
        () => callOp("op_dynamic_worker_lookup", {
          request_id: scopedRequestId,
          binding: bindingName,
          id: instanceId,
        }),
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
      const source = resolveDynamicWorkerSource(options);
      const policy = normalizeDynamicPolicy(options);
      const envInput = options.env;
      const envConfig = splitDynamicEnvInput(envInput, policy);
      const timeout = normalizeDynamicTimeout(options.timeout);
      const result = await awaitDynamicReply(
        `dynamic worker create (${bindingName}/${instanceId})`,
        () => callOp("op_dynamic_worker_create", {
          request_id: scopedRequestId,
          binding: bindingName,
          id: instanceId,
          source,
          env: envConfig.stringEnv,
          policy,
          host_rpc_bindings: envConfig.hostRpcBindings,
          timeout,
        }),
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
      const scopedRequestId = activeRequestId();
      const result = await awaitDynamicReply(
        `dynamic worker list (${bindingName})`,
        () => callOp("op_dynamic_worker_list", {
          request_id: scopedRequestId,
          binding: bindingName,
        }),
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
      const scopedRequestId = activeRequestId();
      const result = await awaitDynamicReply(
        `dynamic worker delete (${bindingName}/${instanceId})`,
        () => callOp("op_dynamic_worker_delete", {
          request_id: scopedRequestId,
          binding: bindingName,
          id: instanceId,
        }),
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
    Object.defineProperty(env, "__dd_dynamic_handle_cache", {
      value: new Map(),
      enumerable: false,
      configurable: false,
      writable: false,
    });
    Object.defineProperty(env, "__dd_dynamic_source_cache", {
      value: new Map(),
      enumerable: false,
      configurable: false,
      writable: false,
    });
    Object.defineProperty(env, "__dd_dynamic_metrics", {
      value: {
        handleCacheHit: 0,
        handleCacheMiss: 0,
        sourceCacheHit: 0,
        sourceCacheMiss: 0,
        remoteFetchHit: 0,
        remoteFetchFallback: 0,
        normalizeFastPathHit: 0,
        normalizeSlowPathHit: 0,
      },
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

    const memoryBindings = Array.isArray(memoryBindingsConfig) ? memoryBindingsConfig : [];
    for (const entry of memoryBindings) {
      const bindingName = Array.isArray(entry)
        ? String(entry[0] ?? "").trim()
        : entry && typeof entry === "object"
          ? String(entry.binding ?? "").trim()
          : String(entry ?? "").trim();
      if (!bindingName) {
        continue;
      }
      const envName = bindingName;
      defineLazyValue(env, envName, () => createMemoryNamespace(bindingName));
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
  ) => {
    const maxRetries = 256;
    let lastConflict = null;
    for (let attempt = 0; attempt < maxRetries; attempt += 1) {
      for (;;) {
        const txn = createMemoryTxn(entry);
        const txnStarted = performance.now();
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
          if (memoryTxnIsSnapshotOnly(txn)) {
            recordMemoryProfile("js_read_only_total", performance.now() - txnStarted, 1);
            return value;
          }
          if (memoryTxnIsReadOnly(txn)) {
            recordMemoryProfile("js_read_only_total", performance.now() - txnStarted, 1);
            return value;
          }
          const committed = memoryTxnIsBlindWrite(txn)
            ? await commitMemoryBlindTxn(txn, runtimeRequestId)
            : txn.writes.size === 0 && txn.accepted !== true
            ? await validateMemoryTxnReads(txn, runtimeRequestId)
            : await commitMemoryTxn(txn, runtimeRequestId);
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
              await ensureMemoryStorageHydrated(entry, runtimeRequestId);
            } else {
              await ensureMemoryStorageKeysHydrated(entry, runtimeRequestId, error.keys);
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
          current.memoryEntry = previousMemoryEntry;
          current.memoryRequestId = previousMemoryRequestId;
          current.socketRuntimeProvider = previousSocketRuntimeProvider;
          current.transportRuntimeProvider = previousTransportRuntimeProvider;
        }
      }
    }
    throw lastConflict ?? new Error("memory transaction exceeded retry limit");
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
        const executable = instantiateExecutable(descriptor);
        const value = await executeMemoryTransaction(
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
