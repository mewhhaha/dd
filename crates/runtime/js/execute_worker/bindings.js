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
        bodyHandle = op_http_store_prepared_body(request.body);
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
        if (headersHandle > 0) op_http_take_prepared_headers(headersHandle);
        if (bodyHandle > 0) op_http_take_prepared_body(bodyHandle);
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
  const body = op_http_take_prepared_body(toHandle(result.body_handle));
  const status = Number(result.status ?? 200);
  const nullBody = request.method === "HEAD" || status === 204 || status === 205 || status === 304;
  const headers = op_http_take_prepared_headers(toHandle(result.headers_handle));
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
    op_http_take_prepared_headers(headersHandle);
  }
  const bodyHandle = toHandle(payload?.body_handle);
  if (bodyHandle > 0) {
    op_http_take_prepared_body(bodyHandle);
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

const workerEnv = (worker, deployment) => {
  const cached = workerEnvs.get(worker);
  if (cached) {
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
    const socketRuntime = createMemorySocketRuntime(entry, true, undefined);
    const scopedState = createMemoryAtomicState(entry, txn, socketRuntime);
    current.memoryEntry = entry;
    current.socketRuntimeProvider = () => socketRuntime;
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

const emitWaitUntilDone = async (requestContext) => {
  await syncFrozenTime();
  const emitted = op_emit_wait_until_done(
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

const storeResponseHeaders = (headers) => toHandle(op_http_store_prepared_headers(
  ArrayIsArray(headers) ? headers : [],
));

const emitResponseStart = async (requestContext, status, headersHandle) => {
  await syncFrozenTime();
  op_emit_response_start(
    requestContext.completionHandle,
    status,
    headersHandle,
  );
};

const emitResponseChunk = async (requestContext, chunk) => {
  await syncFrozenTime();
  const bytes = toBytes(chunk);
  await op_emit_response_chunk(requestContext.completionHandle, bytes);
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
  await op_sleep(Number(millis) || 0);
  await syncFrozenTime();
}

// The deployment the host installed: the worker's bindings.
let workerDeployment = null;

// The requests still running, by request context handle, for
// dd.abortWorkerRequestHandle.
const inflightRequests = new SafeMap();

// Runs the worker on one request, under its context, and settles with what
// it responded, as stored host handles.
const respond = async (requestContext, invocation, worker, deployment) => {
  try {
    await syncFrozenTime();
    const requestSignal = AbortControllerPrototypeGetSignal(requestContext.controller);
    const requestBody = invocation.request_body_stream_handle > 0
      ? createRequestBodyStream(invocation.request_body_stream_handle)
      : byteLength(invocation.body) > 0
        ? new Uint8Array(invocation.body)
        : undefined;
    const request = new Request(
      invocation.url,
      new RequestInit(String(invocation.method || "GET"), requestBody, requestSignal),
    );
    appendHeaderPairs(RequestPrototypeGetHeaders(request), invocation.headers);
    const env = workerEnv(worker, deployment);
    const ctx = {
      requestId: invocation.input_request_id,
      signal: requestSignal,
      waitUntil(promise) {
        return trackWaitUntil(requestContext, promise);
      },
      sleep,
    };

    const response = invocation.memory_call
        ? await invokeMemoryCall(invocation.memory_call, env, worker)
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
    const streamResponse = invocation.stream_response;
    const maxResponseBodyBytes = invocation.max_response_body_bytes;
    const bodyChunks = streamResponse ? null : [];
    let bodyLength = 0;
    if (streamResponse) {
      await emitResponseStart(requestContext, status, storeResponseHeaders(headers));
    }
    const responseBody = isWebSocketAcceptResponse ? null : ResponsePrototypeGetBody(response);
    if (responseBody !== null && invocation.method === "HEAD") {
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

    const result = new WorkerResult(
      status,
      streamResponse ? 0 : storeResponseHeaders(headers),
      0,
    );
    if (!streamResponse) {
      result.bodyHandle = op_http_store_prepared_body(
        concatByteChunks(bodyChunks, bodyLength),
      );
    }
    return result;
  } finally {
    if (requestContext.requestBodyStreamHandle > 0) {
      try {
        await op_request_body_cancel(requestContext.requestBodyStreamHandle);
      } catch {
      }
    }
    if (invocation.request_context_handle > 0) {
      inflightRequests.delete(invocation.request_context_handle);
    }
  }
};

// Reports how the request went, then waits out its waitUntil work. A
// failure while reporting success is reported as the request's error.
const reportOutcome = async (requestContext, handled) => {
  try {
    const result = await handled;
    op_emit_completion_ok(
      requestContext.completionHandle,
      Number(result.status ?? 200),
      toHandle(result.headersHandle),
      toHandle(result.bodyHandle),
    );

    await waitForWaitUntils(requestContext);
    await emitWaitUntilDone(requestContext);
  } catch (error) {
    op_emit_completion_error(
      requestContext.completionHandle,
      errorText(error),
    );

    await waitForWaitUntils(requestContext);
    await emitWaitUntilDone(requestContext);
  }
};

// One request: its context, the worker run under it, and the report of how
// it went, which runs outside the request's scope.
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
    new AbortController(),
    invocation.request_context_handle,
    invocation.completion_handle,
    invocation.memory_request_scope_handle,
    invocation.request_body_stream_handle,
    invocation.max_request_body_bytes,
    hasCacheBypassStaleHeader(invocation.headers),
  );
  if (invocation.request_context_handle > 0) {
    inflightRequests.set(invocation.request_context_handle, requestContext);
  }
  reportOutcome(
    requestContext,
    asyncContext.run(requestContext, respond, requestContext, invocation, worker, deployment),
  );
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
  const descriptor = op_request_invocation_descriptor(handle);
  if (descriptor === null || descriptor === undefined) {
    throw new Error(`Request handle ${requestHandle} is unavailable`);
  }
  const headers = op_http_take_prepared_headers(
    toHandle(descriptor.request_headers_handle),
  );
  const body = op_http_take_prepared_body(toHandle(descriptor.request_body_handle));
  executeWorker(new WorkerInvocation(descriptor, headers, body));
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
  const payload = op_take_worker_deployment_config(Number(deploymentHandle));
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
      op_request_body_cancel(inflight.requestBodyStreamHandle);
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
  AbortControllerPrototypeAbort(inflight.controller, new Error("Request aborted by caller"));
  return true;
};
