  const REQUEST_CANCELED_REPLY_MAX_ENTRIES = 4_096;
  const REQUEST_CANCELED_REPLY_TTL_MS = 5 * 60 * 1000;

  const applyRequestReplyBoundary = (result) => {
    if (!result || typeof result !== "object" || result.boundary_changed !== true) {
      return;
    }
    applyBoundary({
      __proto__: null,
      nowMs: result.boundary_now_ms,
      perfMs: result.boundary_perf_ms,
    });
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
      { __proto__: null, syncTime: false, signal },
    );
    if (!result || typeof result !== "object" || result.ok === false) {
      applyRequestReplyBoundary(result);
      throw new Error(formatRequestFailure("service binding fetch failed", result));
    }
    applyRequestReplyBoundary(result);
    const body = callOp("op_http_take_prepared_body", toHandle(result.body_handle));
    const status = Number(result.status ?? 200);
    const nullBody = request.method === "HEAD" || status === 204 || status === 205 || status === 304;
    const headers = callOp("op_http_take_prepared_headers", toHandle(result.headers_handle));
    const response = new Response(nullBody ? null : body, { __proto__: null, status });
    appendHeaderPairs(ResponsePrototypeGetHeaders(response), ArrayIsArray(headers) ? headers : []);
    return response;
  };

  const createServiceBinding = (bindingName, targetWorker) => ObjectFreeze({
    worker: targetWorker,
    async fetch(inputValue, initValue = undefined) {
      const request = await normalizeFetchInput(inputValue, initValue, true);
      return await invokeServiceBindingFetch(
        bindingName,
        targetWorker,
        request,
        activeRequestContextHandle(),
        request.signal,
      );
    },
  });

  const requestReplyWaiters = () => (dd.requestReplyWaiters ??= new SafeMap());
  const requestReplyReady = () => (dd.requestReplyReady ??= new SafeMap());
  const requestReplyCanceled = () => (dd.requestReplyCanceled ??= new SafeMap());

  const sweepRequestReplyCanceled = (canceled, now = frozenPerfNow()) => {
    for (const entry of canceled.entries()) {
      if (entry[1] > now && canceled.size <= REQUEST_CANCELED_REPLY_MAX_ENTRIES) {
        break;
      }
      canceled.delete(entry[0]);
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
    const headersHandle = toHandle(payload?.headers_handle);
    if (headersHandle > 0) {
      callOp("op_http_take_prepared_headers", headersHandle);
    }
    const bodyHandle = toHandle(payload?.body_handle);
    if (bodyHandle > 0) {
      callOp("op_http_take_prepared_body", bodyHandle);
    }
  };

  const deliverRequestReply = (payload) => {
    const replyId = StringPrototypeTrim(String(payload?.reply_id ?? ""));
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
      return PromiseResolve(payload);
    }
    return new Promise((resolve, reject) => {
      requestReplyWaiters().set(replyId, { __proto__: null, resolve, reject });
    });
  };

  const cancelRequestReply = (replyId, cause) => {
    if (!replyId) {
      return;
    }
    const canceled = requestReplyCanceled();
    const now = frozenPerfNow();
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
    if (signal && AbortSignalPrototypeGetAborted(signal)) {
      throw abortErrorForSignal(signal);
    }
    const started = startOp();
    if (!started || typeof started !== "object" || started.ok === false) {
      throw new Error(String(started?.error ?? `${label} failed to start`));
    }
    const replyId = StringPrototypeTrim(String(started.reply_id ?? ""));
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
        AbortSignalPrototypeAddEventListener(signal, "abort", onAbort, ONCE);
        if (AbortSignalPrototypeGetAborted(signal)) onAbort();
      });
      const result = await SafePromiseRace([reply, timeoutError, aborted]);
      if (options?.syncTime !== false) {
        await syncFrozenTime();
      }
      return result;
    } catch (error) {
      cancelRequestReply(replyId, error);
      throw error;
    } finally {
      clearTimeout(timeoutId);
      if (onAbort) AbortSignalPrototypeRemoveEventListener(signal, "abort", onAbort);
    }
  };

  const formatRequestFailure = (fallback, result) => {
    if (result && typeof result === "object") {
      if (typeof result.error === "string" && result.error) {
        return result.error;
      }
      try {
        return JSONStringify(result);
      } catch {
        return fallback;
      }
    }
    return String(result ?? fallback);
  };

  const drainRequestControlQueue = async () => {
    for (;;) {
      const batch = callOp("op_request_control_take");
      if (!ArrayIsArray(batch) || batch.length === 0) {
        return;
      }
      for (let i = 0; i < batch.length; i++) {
        const item = batch[i];
        if (item?.kind === "reply") {
          deliverRequestReply(item.payload);
        }
      }
    }
  };

  dd.drainRequestControlQueue = drainRequestControlQueue;
  dd.awaitRequestReply = awaitRequestReply;

  const getSharedEnv = () => {
    const cache = dd.sharedEnvCache ??= new SafeWeakMap();
    const cacheableWorker = worker && (typeof worker === "object" || typeof worker === "function")
      ? worker
      : null;
    const fallbackCache = dd.sharedEnvFallbackCache ??= new SafeMap();
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
      ObjectDefineProperty(target, propertyName, {
        __proto__: null,
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
    for (let i = 0; i < kvBindingsConfig.length; i++) {
      const envName = kvBindingsConfig[i][0];
      const bindingName = kvBindingsConfig[i][1];
      if (!envName) {
        continue;
      }
      defineLazyValue(env, envName, () => createKvBinding(bindingName));
    }

    for (let i = 0; i < serviceBindingsConfig.length; i++) {
      const envName = serviceBindingsConfig[i][0];
      const targetWorker = serviceBindingsConfig[i][1];
      if (!envName || !targetWorker) {
        continue;
      }
      defineLazyValue(
        env,
        envName,
        () => createServiceBinding(envName, targetWorker),
      );
    }

    for (let i = 0; i < memoryBindingsConfig.length; i++) {
      const bindingName = memoryBindingsConfig[i];
      if (!bindingName) {
        continue;
      }
      defineLazyValue(env, bindingName, () => createMemoryNamespace(bindingName));
    }

    ObjectFreeze(env);
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
      const socketRuntime = createMemorySocketRuntime(entry, {
        __proto__: null,
        allowSocketAccept: true,
        handles: undefined,
      });
      const scopedState = createMemoryAtomicState(entry, txn, socketRuntime);
      current.memoryEntry = entry;
      current.socketRuntimeProvider = () => socketRuntime;
      txn.callbackActive = true;
      let value;
      try {
        value = withMemoryTxnScope(
          {
            __proto__: null,
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
    const kind = StringPrototypeTrim(String(memoryCall.kind ?? ""));
    const event = {
      type: kind,
      binding: String(memoryCall.binding ?? ""),
      key: String(memoryCall.key ?? ""),
    };
    ObjectDefineProperty(event, "stub", {
      __proto__: null,
      value: stub,
      enumerable: false,
      configurable: true,
      writable: false,
    });
    if (ObjectHasOwn(memoryCall, "handle")) {
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
    const binding = StringPrototypeTrim(String(memoryCall.binding ?? ""));
    const memoryKey = StringPrototypeTrim(String(memoryCall.key ?? ""));
    if (!binding || !memoryKey) {
      throw new Error("memory invoke requires binding and key");
    }
    if (!ObjectHasOwn(env, binding)) {
      throw new Error(`memory binding not declared for worker: ${binding}`);
    }
    const entry = ensureMemoryEntry(binding, memoryKey);
    const kind = String(memoryCall.kind ?? "");
    const socketRuntime = createMemorySocketRuntime(entry, {
      __proto__: null,
      allowSocketAccept: false,
      // The host leaves the list out when it is empty.
      handles: ObjectHasOwn(memoryCall, "socket_handles") ? memoryCall.socket_handles : undefined,
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
        await ReflectApply(wakeMethod, worker, [event, env]);
        await gateMemoryOutput(entry, async () => undefined);
        return new Response(null, { __proto__: null, status: 204 });
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

  const storeResponseHeaders = (headers) => toHandle(callOp(
    "op_http_store_prepared_headers",
    ArrayIsArray(headers) ? headers : [],
  ));

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
    const bytes = toBytes(chunk);
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
    const promises = requestContext.waitUntilPromises;
    let completed = 0;
    while (completed < promises.length) {
      const batch = [];
      for (let i = completed; i < promises.length; i++) {
        ArrayPrototypePush(batch, promises[i]);
      }
      completed += batch.length;
      await SafePromiseAllSettled(batch);
    }
  };

  const errorText = (error) => String((error && (error.stack || error.message)) || error);

  function trackWaitUntil(promise) {
    callOp("op_request_wait_until_register", requestContext.completionHandle);
    const tracked = (async () => {
      let value;
      try {
        value = await promise;
      } catch (error) {
        await syncFrozenTime();
        try {
          consoleWarn("waitUntil promise rejected", errorText(error));
        } catch {
          // Ignore logging failures in isolate userland.
        }
        return { ok: false, error: errorText(error) };
      }
      await syncFrozenTime();
      return { ok: true, value };
    })();
    ArrayPrototypePush(requestContext.waitUntilPromises, tracked);
    return tracked;
  }

  const handled = asyncContext.run(requestContext, () => (async () => {
    try {
      await syncFrozenTime();
      installHostFetch();
      const requestSignal = AbortControllerPrototypeGetSignal(requestContext.controller);
      const requestBody = hasRequestBodyStream
        ? createRequestBodyStream()
        : byteLength(input.body) > 0
          ? new Uint8Array(input.body)
          : undefined;
      const request = new Request(input.url, {
        __proto__: null,
        method: String(input.method || "GET"),
        body: requestBody,
        signal: requestSignal,
      });
      appendHeaderPairs(RequestPrototypeGetHeaders(request), input.headers);
      const env = getSharedEnv();
      const ctx = {
        requestId: input.request_id,
        signal: requestSignal,
        waitUntil(promise) {
          return trackWaitUntil(promise);
        },
        async sleep(millis) {
          await sleep(millis);
          await syncFrozenTime();
        },
      };

      const response = memoryCallConfig
          ? await invokeMemoryCall(memoryCallConfig, request, env)
          : await worker.fetch(request, env, ctx);
      await syncFrozenTime();

      const isResponse = ObjectPrototypeIsPrototypeOf(ResponsePrototype, response);
      const isWebSocketAcceptResponse = !isResponse
        && response !== null
        && typeof response === "object"
        && response.__dd_websocket_accept === true;
      if (!isResponse && !isWebSocketAcceptResponse) {
        throw new Error("Worker fetch() must return a Response");
      }

      let responseHeaders;
      if (!isWebSocketAcceptResponse) {
        responseHeaders = ResponsePrototypeGetHeaders(response);
      } else if (ObjectPrototypeIsPrototypeOf(HeadersPrototype, response.headers)) {
        responseHeaders = response.headers;
      } else {
        responseHeaders = new Headers(response.headers ?? undefined);
      }
      const status = isWebSocketAcceptResponse
        ? Number(response.status ?? 101)
        : ResponsePrototypeGetStatus(response);
      const headers = headerPairs(responseHeaders);
      const bodyChunks = streamResponse ? null : [];
      let bodyLength = 0;
      if (streamResponse) {
        await emitResponseStart(status, storeResponseHeaders(headers));
      }
      const responseBody = isWebSocketAcceptResponse ? null : ResponsePrototypeGetBody(response);
      if (responseBody !== null && input.method === "HEAD") {
        // A HEAD reply exposes the same headers but must not consume a producer.
        ignoreRejection(ReadableStreamPrototypeCancel(responseBody));
      } else if (responseBody !== null) {
        const reader = ReadableStreamPrototypeGetReader(responseBody);
        // An aborted request stops reading its body, so a producer waiting
        // for more data cannot keep it, or its isolate, alive.
        const signal = requestSignal;
        const cancelOnAbort = () => {
          ignoreRejection(ReadableStreamDefaultReaderPrototypeCancel(
            reader,
            AbortSignalPrototypeGetReason(signal),
          ));
        };
        AbortSignalPrototypeAddEventListener(signal, "abort", cancelOnAbort, ONCE);
        try {
          if (AbortSignalPrototypeGetAborted(signal)) {
            throw AbortSignalPrototypeGetReason(signal);
          }
          while (true) {
            const { done, value } = await ReadableStreamDefaultReaderPrototypeRead(reader);
            if (done) {
              break;
            }
            const chunk = toBytes(value);
            const chunkLength = byteLength(chunk);
            if (chunkLength === 0) {
              continue;
            }
            if (streamResponse) {
              await emitResponseChunk(chunk);
            } else {
              if (bodyLength + chunkLength > maxResponseBodyBytes) {
                throw new Error(`response body exceeded max_response_body_bytes (${maxResponseBodyBytes} bytes)`);
              }
              ArrayPrototypePush(bodyChunks, chunk);
              bodyLength += chunkLength;
            }
          }
        } catch (error) {
          // Start cancellation without letting an uncooperative cancel callback
          // delay the size-limit failure delivered to the caller.
          ignoreRejection(ReadableStreamDefaultReaderPrototypeCancel(reader, error));
          throw error;
        } finally {
          AbortSignalPrototypeRemoveEventListener(signal, "abort", cancelOnAbort);
          ReadableStreamDefaultReaderPrototypeReleaseLock(reader);
        }
      }

      const result = {
        __proto__: null,
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
  })());
  // Report the outcome, then wait out waitUntil work. A failure while
  // reporting success is reported as the request's error.
  (async () => {
    try {
      const result = await handled;
      callOp(
        "op_emit_completion_ok",
        requestContext.completionHandle,
        Number(result.status ?? 200),
        toHandle(result.headersHandle),
        toHandle(result.bodyHandle),
      );

      await waitForWaitUntils();
      await emitWaitUntilDone();
    } catch (error) {
      callOp(
        "op_emit_completion_error",
        requestContext.completionHandle,
        errorText(error),
      );

      await waitForWaitUntils();
      await emitWaitUntilDone();
    }
  })();
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
  const headers = core.ops.op_http_take_prepared_headers(
    toHandle(descriptor.request_headers_handle),
  );
  const body = core.ops.op_http_take_prepared_body(toHandle(descriptor.request_body_handle));
  const payload = {
    __proto__: null,
    request_id: String(descriptor.request_id ?? ""),
    request_context_handle: toHandle(descriptor.request_context_handle),
    completion_handle: toHandle(descriptor.completion_handle),
    memory_request_scope_handle: toHandle(descriptor.memory_request_scope_handle),
    memory_call: descriptor.memory_call ?? null,
    request_body_stream_handle: toHandle(descriptor.request_body_stream_handle),
    stream_response: descriptor.stream_response === true,
    max_response_body_bytes: descriptor.max_response_body_bytes,
    max_request_body_bytes: descriptor.max_request_body_bytes,
    method: String(descriptor.method ?? "GET"),
    url: String(descriptor.url ?? ""),
    headers: ArrayIsArray(headers) ? headers : [],
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
  const listOf = (input) => (ArrayIsArray(input) ? input : []);
  const trimmed = (value) => StringPrototypeTrim(String(value ?? ""));
  const normalizeBindingPairs = (input) => {
    const entries = listOf(input);
    const pairs = [];
    for (let i = 0; i < entries.length; i++) {
      const entry = entries[i];
      const envName = trimmed(ArrayIsArray(entry) ? entry[0] : entry);
      const bindingName = ArrayIsArray(entry) ? trimmed(entry[1] ?? entry[0]) : envName;
      if (envName.length > 0) {
        ArrayPrototypePush(pairs, ObjectFreeze([envName, bindingName || envName]));
      }
    }
    return ObjectFreeze(pairs);
  };
  const normalizeNames = (input) => {
    const entries = listOf(input);
    const names = [];
    for (let i = 0; i < entries.length; i++) {
      const name = trimmed(entries[i]);
      if (name.length > 0) {
        ArrayPrototypePush(names, name);
      }
    }
    return ObjectFreeze(names);
  };
  const normalizeServiceBindings = (input) => {
    const entries = listOf(input);
    const pairs = [];
    for (let i = 0; i < entries.length; i++) {
      const entry = entries[i];
      let envName = "";
      let targetWorker = "";
      if (ArrayIsArray(entry)) {
        envName = trimmed(entry[0]);
        targetWorker = trimmed(entry[1]);
      } else if (entry && typeof entry === "object") {
        envName = trimmed(entry.binding);
        targetWorker = trimmed(entry.service);
      }
      if (envName.length > 0 && targetWorker.length > 0) {
        ArrayPrototypePush(pairs, ObjectFreeze([envName, targetWorker]));
      }
    }
    return ObjectFreeze(pairs);
  };

  dd.workerDeployment = ObjectFreeze({
    __proto__: null,
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
  ignoreRejection(PromiseResolve(drain()));
};

dd.abortWorkerRequestHandle = (requestContextHandle) => {
  const handle = toHandle(requestContextHandle);
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
    AbortControllerPrototypeAbort(inflight.controller, new Error("Request aborted by caller"));
  }
  return true;
};
