  const REQUEST_CANCELED_REPLY_MAX_ENTRIES = 4_096;
  const REQUEST_CANCELED_REPLY_TTL_MS = 5 * 60 * 1000;

  const applyRequestReplyBoundary = (result) => {
    if (!result || typeof result !== "object" || result.boundary_changed !== true) {
      return;
    }
    const boundary = normalizeBoundaryValue({
      nowMs: result.boundary_now_ms,
      perfMs: result.boundary_perf_ms,
    });
    if (boundary && typeof dd.setTime === "function") {
      dd.setTime(boundary.nowMs, boundary.perfMs);
    }
  };

  const invokeServiceBindingFetch = async (
    bindingName,
    targetWorker,
    request,
    requestContextHandle,
    signal,
  ) => {
    const invokeLabel = `service binding fetch (${bindingName} -> ${targetWorker})`;
    const result = await awaitRequestReply(
      invokeLabel,
      () => {
        let headersHandle = 0;
        let bodyHandle = 0;
        try {
          headersHandle = storeResponseHeaders(request.headers);
          bodyHandle = callOp("op_http_store_prepared_body", request.body);
          return callOp(
            "op_service_binding_fetch_start",
            requestContextHandle,
            bindingName,
            request.method,
            request.url,
            headersHandle,
            bodyHandle,
          );
        } finally {
          // The start op consumes these handles; a thrown preparation/start op
          // must release any handles that it did not consume.
          if (headersHandle > 0) callOp("op_http_take_prepared_headers", headersHandle);
          if (bodyHandle > 0) callOp("op_http_take_prepared_body", bodyHandle);
        }
      },
      30_000,
      { syncTime: false, signal },
    );
    if (!result || typeof result !== "object" || result.ok === false) {
      applyRequestReplyBoundary(result);
      throw new Error(formatRequestFailure("service binding fetch failed", result));
    }
    applyRequestReplyBoundary(result);
    const replyHeadersHandle = Math.max(0, Math.trunc(Number(result.headers_handle ?? 0) || 0));
    const replyBodyHandle = Math.max(0, Math.trunc(Number(result.body_handle ?? 0) || 0));
    const body = callOp("op_http_take_prepared_body", replyBodyHandle);
    const status = Number(result.status ?? 200);
    const nullBody = request.method === "HEAD" || status === 204 || status === 205 || status === 304;
    const response = new Response(nullBody ? null : body, {
      status,
      headers: callOp("op_http_take_prepared_headers", replyHeadersHandle),
    });
    return response;
  };

  const createServiceBinding = (bindingName, targetWorker) => Object.freeze({
    worker: targetWorker,
    async fetch(inputValue, initValue = undefined) {
      const request = await normalizeFetchInput(inputValue, initValue, true);
      return invokeServiceBindingFetch(
        bindingName,
        targetWorker,
        request,
        activeRequestContextHandle(),
        request.signal,
      );
    },
  });

  const requestReplyWaiters = () => (dd.requestReplyWaiters ??= new Map());
  const requestReplyReady = () => (dd.requestReplyReady ??= new Map());
  const requestReplyCanceled = () => (dd.requestReplyCanceled ??= new Map());
  const requestReplyNow = () => (
    globalThis.performance && typeof globalThis.performance.now === "function"
      ? globalThis.performance.now()
      : Date.now()
  );

  const sweepRequestReplyCanceled = (canceled, now = requestReplyNow()) => {
    for (const [replyId, expiresAt] of canceled.entries()) {
      if (expiresAt > now && canceled.size <= REQUEST_CANCELED_REPLY_MAX_ENTRIES) {
        break;
      }
      canceled.delete(replyId);
    }
    while (canceled.size > REQUEST_CANCELED_REPLY_MAX_ENTRIES) {
      const oldest = canceled.keys().next();
      if (oldest.done) {
        break;
      }
      canceled.delete(oldest.value);
    }
  };

  const discardRequestReplyHandles = (payload) => {
    const headersHandle = Math.max(0, Math.trunc(Number(payload?.headers_handle ?? 0) || 0));
    if (headersHandle > 0) {
      callOp("op_http_take_prepared_headers", headersHandle);
    }
    const bodyHandle = Math.max(0, Math.trunc(Number(payload?.body_handle ?? 0) || 0));
    if (bodyHandle > 0) {
      callOp("op_http_take_prepared_body", bodyHandle);
    }
  };

  const deliverRequestReply = (payload) => {
    const replyId = String(payload?.reply_id ?? "").trim();
    if (!replyId) {
      return;
    }
    const canceled = requestReplyCanceled();
    const canceledUntil = canceled.get(replyId);
    if (canceledUntil !== undefined) {
      canceled.delete(replyId);
      discardRequestReplyHandles(payload);
      return;
    }
    sweepRequestReplyCanceled(canceled);
    const waiters = requestReplyWaiters();
    const ready = requestReplyReady();
    const waiter = waiters.get(replyId);
    if (waiter) {
      waiters.delete(replyId);
      waiter.resolve(payload);
      return;
    }
    ready.set(replyId, payload);
  };

  const waitForRequestReply = (replyId) => {
    const ready = requestReplyReady();
    if (ready.has(replyId)) {
      const payload = ready.get(replyId);
      ready.delete(replyId);
      return Promise.resolve(payload);
    }
    return new Promise((resolve, reject) => {
      requestReplyWaiters().set(replyId, { resolve, reject });
    });
  };

  const cancelRequestReply = (replyId, cause) => {
    if (!replyId) {
      return;
    }
    const canceled = requestReplyCanceled();
    const now = requestReplyNow();
    sweepRequestReplyCanceled(canceled, now);
    canceled.set(replyId, now + REQUEST_CANCELED_REPLY_TTL_MS);
    sweepRequestReplyCanceled(canceled, now);
    const ready = requestReplyReady();
    const readyPayload = ready.get(replyId);
    if (readyPayload) {
      discardRequestReplyHandles(readyPayload);
    }
    ready.delete(replyId);
    const waiters = requestReplyWaiters();
    const waiter = waiters.get(replyId);
    if (waiter) {
      waiters.delete(replyId);
      waiter.reject(cause);
    }
    callOp("op_request_reply_cancel", replyId);
  };

  const awaitRequestReply = async (label, startOp, timeoutMs = 5_000, options = undefined) => {
    const signal = options?.signal;
    if (signal?.aborted) {
      throw abortErrorForSignal(signal);
    }
    const started = startOp();
    if (!started || typeof started !== "object" || started.ok === false) {
      throw new Error(String(started?.error ?? `${label} failed to start`));
    }
    const replyId = String(started.reply_id ?? "").trim();
    if (!replyId) {
      throw new Error(`${label} missing reply id`);
    }
    let timeoutId = null;
    let onAbort;
    try {
      const reply = waitForRequestReply(replyId);
      const timeoutError = new Promise((_, reject) => {
        timeoutId = setTimeout(
          () => reject(new Error(`${label} timed out after ${timeoutMs}ms`)),
          timeoutMs,
        );
      });
      const aborted = new Promise((_, reject) => {
        if (!signal) return;
        onAbort = () => reject(abortErrorForSignal(signal));
        signal.addEventListener("abort", onAbort, { once: true });
        if (signal.aborted) onAbort();
      });
      const result = await Promise.race([reply, timeoutError, aborted]);
      if (options?.syncTime !== false) {
        await syncFrozenTime();
      }
      return result;
    } catch (error) {
      cancelRequestReply(replyId, error);
      throw error;
    } finally {
      clearTimeout(timeoutId);
      if (onAbort) signal.removeEventListener("abort", onAbort);
    }
  };

  const formatRequestFailure = (fallback, result) => {
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

  const drainRequestControlQueue = async () => {
    for (;;) {
      const batch = callOp("op_request_control_take");
      if (!Array.isArray(batch) || batch.length === 0) {
        return;
      }
        for (const item of batch) {
        if (item?.kind === "reply") {
          deliverRequestReply(item.payload);
        }
      }
    }
  };

  dd.drainRequestControlQueue = drainRequestControlQueue;
  dd.awaitRequestReply = awaitRequestReply;

  const getSharedEnv = () => {
    const cache = dd.sharedEnvCache ??= new WeakMap();
    const cacheableWorker = worker && (typeof worker === "object" || typeof worker === "function")
      ? worker
      : null;
    const fallbackCache = dd.sharedEnvFallbackCache ??= new Map();
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
    for (const [envName, bindingName] of kvBindingsConfig) {
      if (!envName) {
        continue;
      }
      defineLazyValue(env, envName, () => createKvBinding(bindingName));
    }

    for (const [envName, targetWorker] of serviceBindingsConfig) {
      if (!envName || !targetWorker) {
        continue;
      }
      defineLazyValue(
        env,
        envName,
        () => createServiceBinding(envName, targetWorker),
      );
    }

    for (const bindingName of memoryBindingsConfig) {
      if (!bindingName) {
        continue;
      }
      defineLazyValue(env, bindingName, () => createMemoryNamespace(bindingName));
    }

    Object.freeze(env);
    if (cacheableWorker) {
      cache.set(cacheableWorker, env);
    } else {
      fallbackCache.set(workerName, env);
    }
    return env;
  };

  const executeMemoryTransaction = async (
    entry,
    callback,
    commandHandle,
  ) => {
    let txn;
    const current = currentRequestContext();
    const previousMemoryEntry = current.memoryEntry;
    const previousSocketRuntimeProvider = current.socketRuntimeProvider;
    try {
      txn = await createMemoryTxn(entry, commandHandle);
      const socketRuntime = createMemorySocketRuntime(entry, { allowSocketAccept: true });
      const scopedState = createMemoryAtomicState(entry, txn, socketRuntime);
      current.memoryEntry = entry;
      current.socketRuntimeProvider = () => socketRuntime;
      txn.callbackActive = true;
      let value;
      try {
        value = withMemoryTxnScope(
          {
            binding: entry.binding,
            memoryKey: entry.memoryKey,
            state: scopedState,
          },
          () => callback(scopedState),
        );
      } finally {
        txn.callbackActive = false;
      }
      if (value != null && (typeof value === "object" || typeof value === "function") && typeof value.then === "function") {
        throw new Error("stub.atomic callback must be synchronous");
      }
      if (commandHandle > 0) {
        setMemoryBatchCommandResult(txn, await encodeMemoryCommandResult(value));
      }
      await finishMemoryTxn(txn);
      return value;
    } finally {
      closeMemoryTxnBatch(txn);
      current.memoryEntry = previousMemoryEntry;
      current.socketRuntimeProvider = previousSocketRuntimeProvider;
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
      event.data = memoryCall.is_text === true ? core.decode(raw) : raw;
      event.isText = memoryCall.is_text === true;
      event.type = "socketmessage";
    } else if (kind === "close") {
      event.code = Number(memoryCall.code ?? 1000);
      event.reason = String(memoryCall.reason ?? "");
      event.type = "socketclose";
    }
    return event;
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
    const entry = ensureMemoryEntry(binding, memoryKey);
    const kind = String(memoryCall.kind ?? "");
    const socketRuntime = createMemorySocketRuntime(entry, {
      allowSocketAccept: false,
      handles: memoryCall.socket_handles,
    });
    const current = currentRequestContext();
    const previousMemoryEntry = current.memoryEntry;
    const previousSocketRuntimeProvider = current.socketRuntimeProvider;
    current.socketRuntimeProvider = () => socketRuntime;
    current.memoryEntry = entry;
    try {
      if (
        kind === "message"
        || kind === "close"
      ) {
        const wakeMethod = worker?.wake;
        if (typeof wakeMethod !== "function") {
          throw new Error("worker does not define wake(event, env)");
        }
        const event = buildWakeEvent(memoryCall, createMemoryStub(binding, memoryKey));
        await wakeMethod.call(worker, event, env);
        await gateMemoryOutput(entry, async () => undefined);
        return new Response(null, { status: 204 });
      }
      throw new Error(`unsupported memory invoke kind: ${kind}`);
    } finally {
      current.memoryEntry = previousMemoryEntry;
      current.socketRuntimeProvider = previousSocketRuntimeProvider;
    }
  };

  const emitWaitUntilDone = async () => {
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
    // The scheduler enforces the deadline from another thread, including while
    // JavaScript is stuck in a CPU loop. Include work registered by earlier work.
    let completed = 0;
    while (completed < requestContext.waitUntilPromises.length) {
      const batch = requestContext.waitUntilPromises.slice(completed);
      completed += batch.length;
      await Promise.allSettled(batch);
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
      const request = new Request(input.url, {
        method: String(input.method || "GET"),
        headers: requestHeaders,
        body: requestBody,
        signal: requestContext.controller.signal,
      });
      const workerRequest = request;
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

      const response = memoryCallConfig
          ? await invokeMemoryCall(memoryCallConfig, workerRequest, env)
          : await worker.fetch(workerRequest, env, ctx);
      await syncFrozenTime();

      const isWebSocketAcceptResponse = Boolean(
        response
          && typeof response === "object"
          && response.__dd_websocket_accept === true,
      );
      if (!(response instanceof Response) && !isWebSocketAcceptResponse) {
        throw new Error("Worker fetch() must return a Response");
      }

      const responseHeaders = isWebSocketAcceptResponse
        ? new Headers(response.headers ?? [])
        : response.headers;
      const status = isWebSocketAcceptResponse
        ? Number(response.status ?? 101)
        : response.status;
      const headers = Array.from(responseHeaders.entries());
      const bodyChunks = streamResponse ? null : [];
      let bodyLength = 0;
      if (streamResponse) {
        await emitResponseStart(status, storeResponseHeaders(headers));
      }
      if (!isWebSocketAcceptResponse && response.body && input.method === "HEAD") {
        // A HEAD reply exposes the same headers but must not consume a producer.
        void response.body.cancel().catch(() => undefined);
      } else if (!isWebSocketAcceptResponse && response.body) {
        const reader = response.body.getReader();
        try {
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
              if (bodyLength + chunk.byteLength > maxResponseBodyBytes) {
                throw new Error(`response body exceeded max_response_body_bytes (${maxResponseBodyBytes} bytes)`);
              }
              bodyChunks.push(chunk);
              bodyLength += chunk.byteLength;
            }
          }
        } catch (error) {
          // Start cancellation without letting an uncooperative cancel callback
          // delay the size-limit failure delivered to the caller.
          void reader.cancel(error).catch(() => undefined);
          throw error;
        } finally {
          reader.releaseLock();
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
        dd.inflightRequestsByContextHandle?.delete(
          requestContextHandle,
        );
      }
    }
  })())
    .then(async (result) => {
      callOp(
        "op_emit_completion_ok",
        requestContext.completionHandle,
        Number(result.status ?? 200),
        Math.max(0, Math.trunc(Number(result.headersHandle ?? 0) || 0)),
        Math.max(0, Math.trunc(Number(result.bodyHandle ?? 0) || 0)),
      );

      await waitForWaitUntils();
      await emitWaitUntilDone();
    })
    .catch(async (error) => {
      const message = String((error && (error.stack || error.message)) || error);
      callOp(
        "op_emit_completion_error",
        requestContext.completionHandle,
        message,
      );

      await waitForWaitUntils();
      await emitWaitUntilDone();
    });
};

// Takes the worker module's namespace once it has evaluated.
dd.installWorker = (workerModule) => {
  const worker = workerModule.default;
  if (worker === undefined) {
    throw new Error("Worker must export default");
  }
  if (typeof worker !== "object" || worker === null) {
    throw new Error("Default export must be an object");
  }
  if (typeof worker.fetch !== "function") {
    throw new Error("Default export must define fetch(request, env, ctx)");
  }
  dd.worker = worker;
};

dd.executeWorkerHandle = (requestHandle) => {
  const handle = Number(requestHandle);
  const descriptor = core.ops.op_request_invocation_descriptor(handle);
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
  const headers = core.ops.op_http_take_prepared_headers(requestHeadersHandle);
  const body = core.ops.op_http_take_prepared_body(requestBodyHandle);
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
    request_body_stream_handle: Math.max(
      0,
      Math.trunc(Number(descriptor.request_body_stream_handle ?? 0) || 0),
    ),
    stream_response: descriptor.stream_response === true,
    max_response_body_bytes: descriptor.max_response_body_bytes,
    max_request_body_bytes: descriptor.max_request_body_bytes,
    method: String(descriptor.method ?? "GET"),
    url: String(descriptor.url ?? ""),
    headers: Array.isArray(headers) ? headers : [],
    input_request_id: String(descriptor.input_request_id ?? ""),
    body,
  };
  return dd.executeWorker(payload);
};

dd.installWorkerDeploymentHandle = (deploymentHandle) => {
  const payload = core.ops.op_take_worker_deployment_config(Number(deploymentHandle));
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
  const normalizeServiceBindings = (input) => Object.freeze(
    (Array.isArray(input) ? input : [])
      .map((entry) => {
        if (Array.isArray(entry)) {
          return Object.freeze([
            String(entry[0] ?? "").trim(),
            String(entry[1] ?? "").trim(),
          ]);
        }
        if (entry && typeof entry === "object") {
          return Object.freeze([
            String(entry.binding ?? "").trim(),
            String(entry.service ?? "").trim(),
          ]);
        }
        return Object.freeze(["", ""]);
      })
      .filter(([envName, targetWorker]) => envName.length > 0 && targetWorker.length > 0),
  );

  dd.workerDeployment = Object.freeze({
    worker_name: String(payload.worker_name ?? ""),
    kv_bindings: normalizeBindingPairs(payload.kv_bindings),
    memory_bindings: normalizeNames(payload.memory_bindings),
    service_bindings: normalizeServiceBindings(payload.service_bindings),
  });
};

dd.drainRequestControlQueueHandle = () => {
  const drain = dd.drainRequestControlQueue;
  if (typeof drain !== "function") {
    return;
  }
  void Promise.resolve(drain()).catch(() => undefined);
};

dd.abortWorkerRequestHandle = (requestContextHandle) => {
  const handle = Math.max(0, Math.trunc(Number(requestContextHandle ?? 0) || 0));
  if (handle === 0) {
    return false;
  }
  const inflightRequests = dd.inflightRequestsByContextHandle;
  const inflight = inflightRequests?.get(handle);
  if (!inflight) {
    return false;
  }
  if (inflight?.requestBodyStreamHandle > 0) {
    try {
      core.ops.op_request_body_cancel(inflight.requestBodyStreamHandle);
    } catch {
    }
  }
  if (inflight?.requestContextHandle > 0) {
    try {
      core.ops.op_request_context_cancel(inflight.requestContextHandle);
    } catch {
    }
  }
  if (inflight?.memoryRequestScopeHandle > 0) {
    try {
      core.ops.op_memory_request_scope_close(inflight.memoryRequestScopeHandle);
    } catch {
    }
  }
  if (inflight?.controller) {
    inflight.controller.abort(new Error("Request aborted by caller"));
  }
  return true;
};
