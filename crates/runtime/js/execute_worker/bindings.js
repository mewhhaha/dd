const REQUEST_CANCELED_REPLY_MAX_ENTRIES = 4_096;
const REQUEST_CANCELED_REPLY_TTL_MS = 5 * 60 * 1000;

const applyRequestReplyBoundary = (result) => {
  if (!result || typeof result !== "object" || result.boundary_changed !== true) {
    return;
  }
  setTime(result.boundary_now_ms, result.boundary_perf_ms);
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
        bodyHandle = op_http_body_store(request.body);
        return op_service_binding_fetch_start(
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
        if (headersHandle > 0) op_http_headers_take(headersHandle);
        if (bodyHandle > 0) op_http_body_take(bodyHandle);
      }
    },
    30_000,
    new ReplyWaitOptions(false, signal),
  );
  if (!result || typeof result !== "object" || result.ok === false) {
    applyRequestReplyBoundary(result);
    throw new Error(formatRequestFailure("service binding fetch failed", result));
  }
  applyRequestReplyBoundary(result);
  const body = op_http_body_take(toHandle(result.body_handle));
  const status = Number(result.status ?? 200);
  const nullBody = request.method === "HEAD" || status === 204 || status === 205 || status === 304;
  const headers = op_http_headers_take(toHandle(result.headers_handle));
  const response = new Response(nullBody ? null : body, new ResponseInit(status, undefined));
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

// Replies to the requests this runtime starts, by reply id: who waits for
// each, the replies that arrived before anyone waited, and the ids given up
// on, each until when a late reply is still expected.
const requestReplyWaiters = new SafeMap();
const requestReplyReady = new SafeMap();
const requestReplyCanceled = new SafeMap();

const sweepRequestReplyCanceled = (now = frozenPerfNow()) => {
  const canceled = requestReplyCanceled;
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
    op_http_headers_take(headersHandle);
  }
  const bodyHandle = toHandle(payload?.body_handle);
  if (bodyHandle > 0) {
    op_http_body_take(bodyHandle);
  }
};

const deliverRequestReply = (payload) => {
  const replyId = StringPrototypeTrim(String(payload?.reply_id ?? ""));
  if (!replyId) {
    return;
  }
  const canceledUntil = requestReplyCanceled.get(replyId);
  if (canceledUntil !== undefined) {
    requestReplyCanceled.delete(replyId);
    discardRequestReplyHandles(payload);
    return;
  }
  sweepRequestReplyCanceled();
  const waiter = requestReplyWaiters.get(replyId);
  if (waiter) {
    requestReplyWaiters.delete(replyId);
    waiter.resolve(payload);
    return;
  }
  requestReplyReady.set(replyId, payload);
};

const waitForRequestReply = (replyId) => {
  if (requestReplyReady.has(replyId)) {
    const payload = requestReplyReady.get(replyId);
    requestReplyReady.delete(replyId);
    return PromiseResolve(payload);
  }
  return new Promise((resolve, reject) => {
    requestReplyWaiters.set(replyId, new ReplyWaiter(resolve, reject));
  });
};

const cancelRequestReply = (replyId, cause) => {
  if (!replyId) {
    return;
  }
  const now = frozenPerfNow();
  sweepRequestReplyCanceled(now);
  requestReplyCanceled.set(replyId, now + REQUEST_CANCELED_REPLY_TTL_MS);
  sweepRequestReplyCanceled(now);
  const readyPayload = requestReplyReady.get(replyId);
  if (readyPayload) {
    discardRequestReplyHandles(readyPayload);
  }
  requestReplyReady.delete(replyId);
  const waiter = requestReplyWaiters.get(replyId);
  if (waiter) {
    requestReplyWaiters.delete(replyId);
    waiter.reject(cause);
  }
  op_request_reply_cancel(replyId);
};

const awaitRequestReply = async (label, startOp, timeoutMs = 5_000, options = undefined) => {
  const signal = options?.signal;
  if (signal && AbortSignalPrototypeGetAborted(signal)) {
    throw abortErrorForSignal(signal);
  }
  // The start op returns the reply's id, or throws when the request cannot
  // start.
  const replyId = StringPrototypeTrim(String(startOp() ?? ""));
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
    const batch = op_request_control_take();
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

dd.awaitRequestReply = awaitRequestReply;

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

// Each worker's env, built from the deployment's bindings on the worker's
// first request; a binding is made the first time the worker reads it.
const workerEnvs = new SafeWeakMap();
let cachedEnvWorker = null;
let cachedEnvDeployment = null;
let cachedEnvValue = null;

const workerEnv = (worker, deployment) => {
  if (worker === cachedEnvWorker && deployment === cachedEnvDeployment) {
    return cachedEnvValue;
  }
  const cached = workerEnvs.get(worker);
  if (cached) {
    cachedEnvWorker = worker;
    cachedEnvDeployment = deployment;
    cachedEnvValue = cached;
    return cached;
  }
  const env = {};
  const kvBindings = deployment.kv_bindings;
  for (let i = 0; i < kvBindings.length; i++) {
    const bindingName = kvBindings[i];
    defineLazyValue(env, bindingName, () => createKvBinding(bindingName));
  }

  const serviceBindings = deployment.service_bindings;
  for (let i = 0; i < serviceBindings.length; i++) {
    const envName = serviceBindings[i][0];
    const targetWorker = serviceBindings[i][1];
    defineLazyValue(
      env,
      envName,
      () => createServiceBinding(envName, targetWorker),
    );
  }

  const memoryBindings = deployment.memory_bindings;
  for (let i = 0; i < memoryBindings.length; i++) {
    const bindingName = memoryBindings[i];
    defineLazyValue(env, bindingName, () => createMemoryNamespace(bindingName));
  }

  ObjectFreeze(env);
  workerEnvs.set(worker, env);
  cachedEnvWorker = worker;
  cachedEnvDeployment = deployment;
  cachedEnvValue = env;
  return env;
};

const runMemoryTxnWithBatch = async (
  entry,
  callback,
  commandHandle,
  batchHandle,
  closeLeaseHandle,
) => {
  const txn = new MemoryTxn(entry, batchHandle);
  const current = currentRequestContext();
  const previousMemoryEntry = current.memoryEntry;
  const previousSocketRuntimeProvider = current.socketRuntimeProvider;
  try {
    let socketRuntime = null;
    const ensureSocketRuntime = () => {
      if (socketRuntime === null) {
        socketRuntime = createMemorySocketRuntime(entry, true, undefined);
      }
      return socketRuntime;
    };
    const scopedState = createMemoryAtomicState(entry, txn, ensureSocketRuntime);
    current.memoryEntry = entry;
    current.socketRuntimeProvider = ensureSocketRuntime;
    txn.callbackActive = true;
    let value;
    try {
      value = withMemoryTxnScope(
        new MemoryTxnScope(entry.binding, entry.memoryKey, scopedState),
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
    const handle = txn.batchHandle;
    txn.batchHandle = 0;
    let applyRes;
    try {
      applyRes = closeLeaseHandle > 0
        ? await op_memory_batch_apply_and_close(handle, closeLeaseHandle)
        : await op_memory_batch_apply(handle);
    } catch (error) {
      if (closeLeaseHandle > 0 && current.memoryRequestScopeHandle === closeLeaseHandle) {
        current.memoryRequestScopeHandle = 0;
      }
      syncFrozenTimeNow();
      throw error;
    }
    if (closeLeaseHandle > 0 && current.memoryRequestScopeHandle === closeLeaseHandle) {
      current.memoryRequestScopeHandle = 0;
    }
    setTime(applyRes[3], applyRes[4]);
    if (applyRes[0] === true && applyRes[2] === true) {
      await gateMemoryOutput(entry, async () => undefined);
    }
    return value;
  } finally {
    closeMemoryTxnBatch(txn);
    current.memoryEntry = previousMemoryEntry;
    current.socketRuntimeProvider = previousSocketRuntimeProvider;
  }
};

const executeMemoryTransaction = async (
  entry,
  callback,
  commandHandle,
) => {
  const beginPending = op_memory_batch_begin(
    activeRequestContextHandle(),
    memoryScopedScopeHandle(entry),
    entry.binding,
    entry.memoryKey,
    commandHandle,
  );
  let beginRes;
  try {
    beginRes = await beginPending;
  } catch (error) {
    syncFrozenTimeNow();
    if (isMemoryStorageFailure(error)) {
      return await failMemoryEntry(entry, error);
    }
    throw error;
  }
  setTime(beginRes[1], beginRes[2]);
  return await runMemoryTxnWithBatch(entry, callback, commandHandle, beginRes[0], 0);
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

// Delivers a memory call (a socket message or close) to the worker's wake.
const invokeMemoryCall = async (memoryCall, env, worker) => {
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
  const socketRuntime = createMemorySocketRuntime(
    entry,
    false,
    // The host leaves the list out when it is empty.
    ObjectHasOwn(memoryCall, "socket_handles") ? memoryCall.socket_handles : undefined,
  );
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
      return new Response(null, new ResponseInit(204, undefined));
    }
    throw new Error(`unsupported memory invoke kind: ${kind}`);
  } finally {
    current.memoryEntry = previousMemoryEntry;
    current.socketRuntimeProvider = previousSocketRuntimeProvider;
  }
};

const emitWaitUntilDone = (requestContext) => {
  syncFrozenTimeNow();
  const emitted = op_request_wait_until_finish(
    requestContext.completionHandle,
  );
  if (emitted === false) {
    return;
  }
  if (requestContext.memoryRequestScopeHandle > 0) {
    op_memory_request_scope_close(requestContext.memoryRequestScopeHandle);
    requestContext.memoryRequestScopeHandle = 0;
  }
  if (requestContext.requestContextHandle > 0) {
    op_request_context_close(requestContext.requestContextHandle);
    requestContext.requestContextHandle = 0;
  }
};

const storeResponseHeaders = (headers) => toHandle(op_http_headers_store(
  ArrayIsArray(headers) ? headers : [],
));

const emitResponseStart = (requestContext, status, headersHandle) => {
  syncFrozenTimeNow();
  op_response_start(
    requestContext.completionHandle,
    status,
    headersHandle,
  );
};

const emitResponseChunk = async (requestContext, chunk) => {
  syncFrozenTimeNow();
  const bytes = toBytes(chunk);
  await op_response_write(requestContext.completionHandle, bytes);
  return bytes;
};

const waitForWaitUntils = async (requestContext) => {
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

const trackWaitUntil = (requestContext, promise) => {
  op_request_wait_until_register(requestContext.completionHandle);
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
};

// ctx.sleep, the same for every request.
async function sleep(millis) {
  await op_time_sleep(Number(millis) || 0);
  await syncFrozenTime();
}

// The deployment the host installed: the worker's bindings.
let workerDeployment = null;

// The requests still running, by request context handle, for
// dd.abortWorkerRequestHandle.
const inflightRequests = new SafeMap();

const cleanupRequestResources = (requestContext, invocation) => {
  if (requestContext.requestBodyStreamHandle > 0) {
    const streamHandle = requestContext.requestBodyStreamHandle;
    requestContext.requestBodyStreamHandle = 0;
    try {
      op_request_body_close(streamHandle);
    } catch {
    }
  }
  if (invocation.request_context_handle > 0) {
    inflightRequests.delete(invocation.request_context_handle);
  }
};

const finishWaitUntilWork = (requestContext) => {
  if (requestContext.waitUntilPromises.length === 0) {
    emitWaitUntilDone(requestContext);
    return;
  }
  PromisePrototypeThen(waitForWaitUntils(requestContext), () => {
    emitWaitUntilDone(requestContext);
  });
};

const failRequestOutcome = (requestContext, invocation, error) => {
  cleanupRequestResources(requestContext, invocation);
  op_response_fail(
    requestContext.completionHandle,
    errorText(error),
  );
  finishWaitUntilWork(requestContext);
};

// Consumes a streamed or ReadableStream response body and completes the request.
const completeStreamOrAsyncResponse = async (
  requestContext,
  invocation,
  response,
  isWebSocketAcceptResponse,
  status,
  headers,
  staticBody,
) => {
  try {
    const streamResponse = invocation.stream_response;
    const maxResponseBodyBytes = invocation.max_response_body_bytes;
    const bodyChunks = streamResponse ? null : [];
    let bodyLength = 0;
    if (streamResponse) {
      emitResponseStart(requestContext, status, storeResponseHeaders(headers));
    }
    if (staticBody !== undefined) {
      if (staticBody !== null && invocation.method !== "HEAD") {
        if (isRequestAborted(requestContext)) {
          throw AbortSignalPrototypeGetReason(requestSignalOf(requestContext));
        }
        const chunk = toBytes(staticBody);
        const chunkLength = byteLength(chunk);
        if (chunkLength > 0) {
          if (streamResponse) {
            await emitResponseChunk(requestContext, chunk);
          } else {
            if (chunkLength > maxResponseBodyBytes) {
              throw new Error(`response body exceeded max_response_body_bytes (${maxResponseBodyBytes} bytes)`);
            }
            ArrayPrototypePush(bodyChunks, chunk);
            bodyLength = chunkLength;
          }
        }
      }
    } else {
      const responseBody = isWebSocketAcceptResponse ? null : ResponsePrototypeGetBody(response);
      if (responseBody !== null && invocation.method === "HEAD") {
        // A HEAD reply exposes the same headers but must not consume a producer.
        ignoreRejection(ReadableStreamPrototypeCancel(responseBody));
      } else if (responseBody !== null) {
        const reader = ReadableStreamPrototypeGetReader(responseBody);
        // An aborted request stops reading its body, so a producer waiting
        // for more data cannot keep it, or its isolate, alive.
        const signal = requestSignalOf(requestContext);
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
              await emitResponseChunk(requestContext, chunk);
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
    }

    const result = new WorkerResult(
      status,
      streamResponse ? 0 : storeResponseHeaders(headers),
      0,
    );
    if (!streamResponse) {
      result.bodyHandle = op_http_body_store(
        concatByteChunks(bodyChunks, bodyLength),
      );
    }
    cleanupRequestResources(requestContext, invocation);
    op_response_complete(
      requestContext.completionHandle,
      Number(result.status ?? 200),
      toHandle(result.headersHandle),
      toHandle(result.bodyHandle),
    );
    finishWaitUntilWork(requestContext);
  } catch (error) {
    failRequestOutcome(requestContext, invocation, error);
  }
};

// Completes a settled worker response, using the zero-stream inline path for
// buffered static responses and falling back to the async reader otherwise.
const completeSettledResponse = (requestContext, invocation, response) => {
  try {
    const isResponse = ObjectPrototypeIsPrototypeOf(ResponsePrototype, response);
    const isWebSocketAcceptResponse = !isResponse
      && response !== null
      && typeof response === "object"
      && response.__dd_websocket_accept === true;
    if (!isResponse && !isWebSocketAcceptResponse) {
      throw new Error("Worker fetch() must return a Response");
    }

    let headers;
    if (!isWebSocketAcceptResponse) {
      headers = getResponseHeaderPairs(response);
      if (headers === null) {
        headers = headerPairs(ResponsePrototypeGetHeaders(response));
      }
    } else if (ObjectPrototypeIsPrototypeOf(HeadersPrototype, response.headers)) {
      headers = headerPairs(response.headers);
    } else {
      headers = headerPairs(new Headers(response.headers ?? undefined));
    }
    const status = isWebSocketAcceptResponse
      ? Number(response.status ?? 101)
      : ResponsePrototypeGetStatus(response);
    const staticBody = isWebSocketAcceptResponse
      ? null
      : tryTakeStaticResponseBody(response);

    if (!invocation.stream_response && staticBody !== undefined) {
      const maxResponseBodyBytes = invocation.max_response_body_bytes;
      const finishWaitUntil = requestContext.waitUntilPromises.length === 0;
      let bodyStr = null;
      let bodyBytes = null;
      if (staticBody === null || invocation.method === "HEAD") {
        bodyStr = "";
      } else {
        if (isRequestAborted(requestContext)) {
          throw AbortSignalPrototypeGetReason(requestSignalOf(requestContext));
        }
        if (typeof staticBody === "string") {
          if (staticBody.length * 3 <= maxResponseBodyBytes) {
            bodyStr = staticBody;
          } else {
            const encoded = core.encode(staticBody);
            if (byteLength(encoded) > maxResponseBodyBytes) {
              throw new Error(`response body exceeded max_response_body_bytes (${maxResponseBodyBytes} bytes)`);
            }
            bodyBytes = encoded;
          }
        } else {
          if (byteLength(staticBody) > maxResponseBodyBytes) {
            throw new Error(`response body exceeded max_response_body_bytes (${maxResponseBodyBytes} bytes)`);
          }
          bodyBytes = staticBody;
        }
      }

      cleanupRequestResources(requestContext, invocation);
      const time = bodyStr !== null
        ? op_response_complete_inline(
          requestContext.completionHandle,
          requestContext.requestContextHandle,
          requestContext.memoryRequestScopeHandle,
          status,
          headers,
          bodyStr,
          finishWaitUntil,
        )
        : op_response_complete_inline_bytes(
          requestContext.completionHandle,
          requestContext.requestContextHandle,
          requestContext.memoryRequestScopeHandle,
          status,
          headers,
          bodyBytes,
          finishWaitUntil,
        );
      setTime(time[0], time[1]);
      if (finishWaitUntil) {
        requestContext.memoryRequestScopeHandle = 0;
        requestContext.requestContextHandle = 0;
      } else {
        finishWaitUntilWork(requestContext);
      }
      return;
    }

    syncFrozenTimeNow();
    completeStreamOrAsyncResponse(
      requestContext,
      invocation,
      response,
      isWebSocketAcceptResponse,
      status,
      headers,
      staticBody,
    );
  } catch (error) {
    failRequestOutcome(requestContext, invocation, error);
  }
};

class ExecutionContext {
  #requestContext;
  constructor(requestId, requestContext) {
    this.requestId = requestId;
    this.#requestContext = requestContext;
  }
  get signal() {
    return requestSignalOf(this.#requestContext);
  }
  waitUntil(promise) {
    const ctx = ObjectPrototypeIsPrototypeOf(ExecutionContextPrototype, this)
      ? this.#requestContext
      : currentRequestContext();
    return trackWaitUntil(ctx, promise);
  }
}
const ExecutionContextPrototype = ExecutionContext.prototype;
ExecutionContextPrototype.sleep = sleep;

// One request: its context, the worker run under it, and the report of how
// it went.
const executeWorker = (invocation) => {
  const deployment = workerDeployment;
  if (deployment === null) {
    throw new Error("Worker deployment config is not installed");
  }
  const worker = dd.worker;
  if (worker === undefined) {
    throw new Error("Worker is not installed");
  }
  const requestContext = new RequestContext(
    invocation.request_id,
    null,
    invocation.request_context_handle,
    invocation.completion_handle,
    invocation.memory_request_scope_handle,
    invocation.request_body_stream_handle,
    invocation.max_request_body_bytes,
    invocation.headers.length > 0 && hasCacheBypassStaleHeader(invocation.headers),
  );
  if (invocation.request_context_handle > 0) {
    inflightRequests.set(invocation.request_context_handle, requestContext);
  }
  asyncContext.run(requestContext, () => {
    try {
      const env = workerEnv(worker, deployment);
      let outcome;
      if (invocation.memory_call) {
        outcome = invokeMemoryCall(invocation.memory_call, env, worker);
      } else {
        const requestBody = invocation.request_body_stream_handle > 0
          ? createRequestBodyStream(invocation.request_body_stream_handle)
          : byteLength(invocation.body) > 0
            ? new Uint8Array(invocation.body)
            : undefined;
        const request = fromHostRequest(
          invocation.url,
          String(invocation.method || "GET"),
          requestBody,
          invocation.headers,
          () => requestSignalOf(requestContext),
        );
        const ctx = new ExecutionContext(invocation.input_request_id, requestContext);
        outcome = worker.fetch(request, env, ctx);
      }
      if (
        outcome !== null
        && (typeof outcome === "object" || typeof outcome === "function")
        && typeof outcome.then === "function"
      ) {
        const promise = core.isPromise(outcome) ? outcome : PromiseResolve(outcome);
        PromisePrototypeThen(
          promise,
          (response) => completeSettledResponse(requestContext, invocation, response),
          (error) => failRequestOutcome(requestContext, invocation, error),
        );
      } else {
        completeSettledResponse(requestContext, invocation, outcome);
      }
    } catch (error) {
      failRequestOutcome(requestContext, invocation, error);
    }
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
  const descriptor = op_request_invocation_take(handle);
  if (descriptor === null || descriptor === undefined) {
    throw new Error(`Request handle ${requestHandle} is unavailable`);
  }
  setTime(descriptor[14], descriptor[15]);
  const bodyHandle = descriptor[6];
  const body = bodyHandle > 0 ? op_http_body_take(bodyHandle) : EMPTY_BYTES;
  executeWorker(new WorkerInvocation(descriptor, body));
};

dd.executeWorkerDirect = (
  requestContextHandle,
  completionHandle,
  memoryRequestScopeHandle,
  maxResponseBodyBytes,
  maxRequestBodyBytes,
  nowMs,
  perfMs,
  methodCode,
  requestId,
  url,
  inputRequestId,
) => {
  setTime(nowMs, perfMs);
  executeWorker(new DirectWorkerInvocation(
    requestId,
    requestContextHandle,
    completionHandle,
    memoryRequestScopeHandle,
    maxResponseBodyBytes,
    maxRequestBodyBytes,
    DIRECT_METHODS[methodCode],
    url,
    inputRequestId,
  ));
};

const listOf = (input) => (ArrayIsArray(input) ? input : []);
const trimmed = (value) => StringPrototypeTrim(String(value ?? ""));

// The non-empty names in a deployment's list of bindings. A KV binding's
// env name is its binding name.
const bindingNames = (input) => {
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

// A deployment's `{ binding, service }` service bindings as
// [env name, target worker] pairs.
const serviceBindingPairs = (input) => {
  const entries = listOf(input);
  const pairs = [];
  for (let i = 0; i < entries.length; i++) {
    const envName = trimmed(entries[i]?.binding);
    const targetWorker = trimmed(entries[i]?.service);
    if (envName.length > 0 && targetWorker.length > 0) {
      ArrayPrototypePush(pairs, ObjectFreeze([envName, targetWorker]));
    }
  }
  return ObjectFreeze(pairs);
};

dd.installWorkerDeploymentHandle = (deploymentHandle) => {
  const payload = op_worker_deployment_take(Number(deploymentHandle));
  if (payload === null || payload === undefined) {
    throw new Error(`Worker deployment handle ${deploymentHandle} is unavailable`);
  }
  workerDeployment = ObjectFreeze({
    __proto__: null,
    kv_bindings: bindingNames(payload.kv_bindings),
    memory_bindings: bindingNames(payload.memory_bindings),
    service_bindings: serviceBindingPairs(payload.service_bindings),
  });
};

dd.drainRequestControlQueueHandle = () => {
  ignoreRejection(drainRequestControlQueue());
};

dd.abortWorkerRequestHandle = (requestContextHandle) => {
  const handle = toHandle(requestContextHandle);
  if (handle === 0) {
    return false;
  }
  const inflight = inflightRequests.get(handle);
  if (!inflight) {
    return false;
  }
  if (inflight.requestBodyStreamHandle > 0) {
    try {
      op_request_body_close(inflight.requestBodyStreamHandle);
    } catch {
    }
  }
  if (inflight.requestContextHandle > 0) {
    try {
      op_request_context_cancel(inflight.requestContextHandle);
    } catch {
    }
  }
  if (inflight.memoryRequestScopeHandle > 0) {
    try {
      op_memory_request_scope_close(inflight.memoryRequestScopeHandle);
    } catch {
    }
  }
  const abortReason = new Error("Request aborted by caller");
  inflight.aborted = true;
  inflight.abortReason = abortReason;
  if (inflight.controller !== null) {
    AbortControllerPrototypeAbort(inflight.controller, abortReason);
  }
  return true;
};
