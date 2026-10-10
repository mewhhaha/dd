// The execute-worker bundle: the units in units.txt, concatenated by
// build.rs. It runs once while the bootstrap snapshot is built, as the body
// of a function whose parameter `__bootstrap` is the runtime's bootstrap
// object, and leaves its entrypoints on `__bootstrap.dd`, which only the
// host reaches.
//
// Worker code runs in this same context, so the bundle reads nothing worker
// code can change after startup. Built-ins come from primordials, web
// classes from dd.web and their methods from dd.webPrimordials, the clock,
// timers and console from what bootstrap.js left on dd, all captured below
// while the snapshot is built; scripts/check-worker-js.mjs rejects any
// global the bundle names. Index loops and SafeArrayIterator stand in for
// spread, for...of and array destructuring; header lists go into Requests
// and Responses through appendHeaderPairs; the records and option
// dictionaries the bundle builds have null prototypes. Promises are awaited,
// never returned from an async function or a `then` callback, since
// resolving one promise with another looks up the replaceable
// Promise.prototype.then.
//
// What stays open, by the nature of sharing a context with worker code:
// - Species. `then` and `await` on a native promise read its `constructor`;
//   a worker that replaces Promise.prototype.constructor (or
//   Promise[Symbol.species]) along with Promise.prototype.then has them call
//   its code. The array and typed array methods that copy (map, filter,
//   slice, subarray), which the web layer uses, consult species the same way.
// - Properties added to Object.prototype and Array.prototype. A `then` there
//   makes op results, which are ordinary host-made objects, thenable when an
//   async op settles with them; an index accessor on Array.prototype sees
//   pushes into arrays; a field an op result leaves out reads through to
//   Object.prototype.
// - The web layer keeps some internals as symbol-keyed prototype members
//   that worker code can find and replace (AbortSignal's abort steps; the
//   runtime took Headers' entries getter at startup).
// - Values the worker hands over run its code when read: getters and proxy
//   traps on its objects, the thenables it returns or passes to waitUntil,
//   and the iterables and init dictionaries it passes to fetch.
"use strict";
const { core, dd, primordials } = __bootstrap;
const {
  ArrayBufferIsView,
  ArrayFrom,
  ArrayIsArray,
  ArrayPrototypePush,
  ArrayPrototypeSort,
  Boolean,
  DataViewPrototypeGetBuffer,
  DataViewPrototypeGetByteLength,
  DataViewPrototypeGetByteOffset,
  Error,
  ErrorPrototype,
  JSONStringify,
  Map,
  MapPrototypeGet,
  MapPrototypeSet,
  MathMax,
  MathMin,
  MathRound,
  MathTrunc,
  Number,
  NumberIsFinite,
  NumberIsInteger,
  ObjectCreate,
  ObjectDefineProperty,
  ObjectEntries,
  ObjectFreeze,
  ObjectGetPrototypeOf,
  ObjectHasOwn,
  ObjectKeys,
  ObjectPrototype,
  ObjectPrototypeIsPrototypeOf,
  Promise,
  PromisePrototypeCatch,
  PromisePrototypeThen,
  PromiseReject,
  PromiseResolve,
  ReflectApply,
  SafeArrayIterator,
  SafeMap,
  SafeMapIterator,
  SafePromiseAllSettled,
  SafePromiseRace,
  SafeSet,
  SafeSetIterator,
  SafeWeakMap,
  Set,
  SetPrototypeAdd,
  String,
  StringPrototypeIndexOf,
  StringPrototypeLocaleCompare,
  StringPrototypeSlice,
  StringPrototypeToLowerCase,
  StringPrototypeToUpperCase,
  StringPrototypeToWellFormed,
  StringPrototypeTrim,
  TypeError,
  TypedArrayPrototypeGetBuffer,
  TypedArrayPrototypeGetByteLength,
  TypedArrayPrototypeGetByteOffset,
  TypedArrayPrototypeGetLength,
  TypedArrayPrototypeGetSymbolToStringTag,
  TypedArrayPrototypeSet,
  Uint8Array,
} = primordials;
const { AbortController, Headers, ReadableStream, Request, Response, URL } = dd.web;
const {
  AbortControllerPrototypeAbort,
  AbortControllerPrototypeGetSignal,
  AbortSignalAny,
  AbortSignalPrototypeAddEventListener,
  AbortSignalPrototypeGetAborted,
  AbortSignalPrototypeGetReason,
  AbortSignalPrototypeRemoveEventListener,
  HeadersPrototype,
  HeadersPrototypeGet,
  HeadersPrototypeSet,
  ReadableStreamDefaultControllerPrototypeClose,
  ReadableStreamDefaultControllerPrototypeEnqueue,
  ReadableStreamDefaultReaderPrototypeCancel,
  ReadableStreamDefaultReaderPrototypeRead,
  ReadableStreamDefaultReaderPrototypeReleaseLock,
  ReadableStreamPrototypeCancel,
  ReadableStreamPrototypeGetReader,
  RequestPrototype,
  RequestPrototypeArrayBuffer,
  RequestPrototypeClone,
  RequestPrototypeGetBody,
  RequestPrototypeGetHeaders,
  RequestPrototypeGetMethod,
  RequestPrototypeGetRedirect,
  RequestPrototypeGetSignal,
  RequestPrototypeGetUrl,
  ResponsePrototype,
  ResponsePrototypeArrayBuffer,
  ResponsePrototypeClone,
  ResponsePrototypeGetBody,
  ResponsePrototypeGetHeaders,
  ResponsePrototypeGetStatus,
  ResponsePrototypeGetStatusText,
  ResponsePrototypeGetUrl,
  URLPrototypeGetHref,
  URLPrototypeGetOrigin,
  URLPrototypeGetProtocol,
} = dd.webPrimordials;
const {
  appendHeaderPairs,
  clearTimeout,
  consoleWarn,
  frozenPerfNow,
  headerPairs,
  hostFetchResponse,
  setTimeout,
} = dd;

const noop = () => undefined;
const ignoreRejection = (promise) => {
  PromisePrototypeCatch(promise, noop);
};
const ONCE = ObjectFreeze({ __proto__: null, once: true });
const FOR_STORAGE = ObjectFreeze({ __proto__: null, forStorage: true });
const STREAM_PULL_ON_READ = ObjectFreeze({ __proto__: null, highWaterMark: 0 });

/** A handle number from an op result or descriptor: a non-negative integer. */
const toHandle = (value) => MathMax(0, MathTrunc(Number(value ?? 0) || 0));

const callOp = (name, ...args) => {
  const op = core.ops[name];
  if (typeof op !== "function") {
    return undefined;
  }
  return ReflectApply(op, undefined, args);
};

const recordMemoryProfile = (metric, durationMs, items = 1) => {
  const op = core.ops.op_memory_profile_record_js;
  if (typeof op !== "function") {
    return;
  }
  op(
    String(metric),
    MathMax(0, MathRound(Number(durationMs ?? 0) * 1000)),
    MathMax(1, MathTrunc(Number(items ?? 1) || 1)),
  );
};

const sleep = (millis) => callOp("op_sleep", Number(millis) || 0);

const normalizeBoundaryValue = (value) => {
  if (value == null) {
    return null;
  }
  if (typeof value === "number") {
    return { __proto__: null, nowMs: value, perfMs: value };
  }
  if (ArrayIsArray(value)) {
    const nowMs = value[0];
    return { __proto__: null, nowMs, perfMs: value[1] === undefined ? nowMs : value[1] };
  }
  if (typeof value === "object") {
    const nowMs =
      value.nowMs ?? value.now_ms ?? value.now ?? value.wallMs ?? value.wall_ms;
    const perfMs =
      value.perfMs ?? value.perf_ms ?? value.perf ?? value.monotonicMs ?? value.monotonic_ms ?? nowMs;
    return { __proto__: null, nowMs, perfMs };
  }
  return null;
};

const applyBoundary = (value) => {
  const boundary = normalizeBoundaryValue(value);
  if (boundary && typeof dd.setTime === "function") {
    dd.setTime(boundary.nowMs, boundary.perfMs);
  }
};
const syncFrozenTime = async () => {
  applyBoundary(await callOp("op_time_boundary_now"));
};
const syncFrozenTimeNow = () => {
  applyBoundary(callOp("op_time_boundary_now"));
};

const isUint8Array = (value) => TypedArrayPrototypeGetSymbolToStringTag(value) === "Uint8Array";

/** The bytes an ArrayBuffer view covers, as a Uint8Array over the same memory. */
const viewBytes = (view) => core.isDataView(view)
  ? new Uint8Array(
    DataViewPrototypeGetBuffer(view),
    DataViewPrototypeGetByteOffset(view),
    DataViewPrototypeGetByteLength(view),
  )
  : new Uint8Array(
    TypedArrayPrototypeGetBuffer(view),
    TypedArrayPrototypeGetByteOffset(view),
    TypedArrayPrototypeGetByteLength(view),
  );

/**
 * `value` as bytes: a Uint8Array as is, other buffers and views copied,
 * anything else as its string's UTF-8.
 */
const toBytes = (value) => {
  if (value == null) {
    return new Uint8Array();
  }
  if (isUint8Array(value)) {
    return value;
  }
  if (core.isArrayBuffer(value)) {
    return new Uint8Array(value);
  }
  if (ArrayBufferIsView(value)) {
    return new Uint8Array(viewBytes(value));
  }
  return core.encode(String(value));
};

const byteLength = (bytes) => TypedArrayPrototypeGetByteLength(bytes);

const concatByteChunks = (chunks, totalLength) => {
  if (!ArrayIsArray(chunks) || chunks.length === 0 || totalLength <= 0) {
    return new Uint8Array();
  }
  if (chunks.length === 1 && byteLength(chunks[0]) === totalLength) {
    return chunks[0];
  }
  const output = new Uint8Array(totalLength);
  let offset = 0;
  for (let i = 0; i < chunks.length; i++) {
    TypedArrayPrototypeSet(output, chunks[i], offset);
    offset += byteLength(chunks[i]);
  }
  return output;
};

const isBinaryLike = (value) => core.isArrayBuffer(value) || ArrayBufferIsView(value);

const decodeStoredValue = (encoding, valueHandle, context) => {
  const bytes = callOp("op_http_take_prepared_body", toHandle(valueHandle));
  if (encoding === "utf8") {
    return core.decode(bytes);
  }
  if (encoding === "v8sc") {
    try {
      return core.deserialize(bytes, FOR_STORAGE);
    } catch (error) {
      throw new Error(`${context} deserialize failed: ${String(error?.message ?? error)}`);
    }
  }
  throw new Error(`${context} unsupported encoding: ${encoding}`);
};

const createKvBinding = (bindingName) => ObjectFreeze({
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
        "v8sc", new Uint8Array(core.serialize(value, FOR_STORAGE)));
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
    if (!NumberIsInteger(limit) || limit < 1 || limit > 1000) {
      throw new Error(`kv list limit must be an integer between 1 and 1000: ${limit}`);
    }
    const result = await callOp("op_kv_list", bindingName, prefix, limit);
    syncFrozenTimeNow();
    if (!result.ok) throw new Error(`kv list ${prefix}: ${result.error}`);
    const entries = result.entries;
    const list = [];
    for (let i = 0; i < entries.length; i++) {
      const entry = entries[i];
      ArrayPrototypePush(list, {
        key: entry.key,
        value: decodeStoredValue(entry.encoding, entry.value_handle, "kv list"),
      });
    }
    return list;
  },
});

const hasCacheBypassStaleHeader = (headers) => {
  for (let i = 0; i < headers.length; i++) {
    const header = headers[i];
    if (StringPrototypeToLowerCase(String(header[0] || "")) !== "x-dd-cache-bypass-stale") {
      continue;
    }
    const normalized = StringPrototypeToLowerCase(String(header[1] || ""));
    if (normalized === "1" || normalized === "true" || normalized === "yes") {
      return true;
    }
  }
  return false;
};

dd.executeWorker = (payload) => {
  const requestId = String(payload?.request_id ?? "");
  const deploymentConfig = dd.workerDeployment ?? null;
  if (!deploymentConfig || typeof deploymentConfig !== "object") {
    throw new Error("Worker deployment config is not installed");
  }
  const workerName = String(deploymentConfig?.worker_name ?? "");
  const kvBindingsConfig = deploymentConfig?.kv_bindings ?? [];
  const memoryBindingsConfig = deploymentConfig?.memory_bindings ?? [];
  const serviceBindingsConfig = deploymentConfig?.service_bindings ?? [];
  const memoryCallConfig = payload?.memory_call ?? null;
  const requestBodyStreamHandle = toHandle(payload?.request_body_stream_handle);
  const requestContextHandle = toHandle(payload?.request_context_handle);
  const completionHandle = toHandle(payload?.completion_handle);
  const memoryRequestScopeHandle = toHandle(payload?.memory_request_scope_handle);
  const hasRequestBodyStream = requestBodyStreamHandle > 0;
  const streamResponse = payload?.stream_response === true;
  const maxResponseBodyBytes = Number(payload?.max_response_body_bytes ?? 0);
  const maxRequestBodyBytes = Number(payload?.max_request_body_bytes ?? 0);
  const worker = dd.worker;

  if (worker === undefined) {
    throw new Error("Worker is not installed");
  }

  const inflightRequests = dd.inflightRequests ??= new SafeMap();
  const inflightRequestsByContextHandle =
    dd.inflightRequestsByContextHandle ??= new SafeMap();
  const input = {
    __proto__: null,
    method: String(payload?.method ?? "GET"),
    url: String(payload?.url ?? ""),
    headers: ArrayIsArray(payload?.headers) ? payload.headers : [],
    body: isUint8Array(payload?.body) ? payload.body : new Uint8Array(),
    request_id: String(payload?.input_request_id ?? ""),
  };
  const cacheBypassStale = hasCacheBypassStaleHeader(input.headers);
  const controller = new AbortController();
  const asyncContext = dd.asyncContext;
  const requestContext = {
    __proto__: null,
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
    __proto__: null,
    controller,
    requestContextHandle,
    memoryRequestScopeHandle,
    requestBodyStreamHandle,
  };
  inflightRequests.set(requestId, inflightRequest);
  if (requestContextHandle > 0) {
    inflightRequestsByContextHandle.set(requestContextHandle, inflightRequest);
  }

  const activeRequestId = () => {
    const scoped = StringPrototypeTrim(String(currentRequestContext().requestId ?? ""));
    if (!scoped) {
      throw new Error("worker request scope is unavailable");
    }
    return scoped;
  };
  dd.runtimeRequestId ??= activeRequestId;
  const activeRequestContextHandle = () => {
    const handle = toHandle(currentRequestContext().requestContextHandle);
    if (handle <= 0) {
      throw new Error("request context handle is unavailable");
    }
    return handle;
  };
  dd.runtimeRequestContextHandle ??= activeRequestContextHandle;
  const activeCacheBypassStale = () => Boolean(
    currentRequestContext(false)?.cacheBypassStale,
  );
  dd.cacheBypassStale ??= activeCacheBypassStale;

  const memoryScopedScopeHandle = (entry) => {
    const current = currentRequestContext(false);
    if (current?.memoryEntry === entry) {
      return toHandle(current.memoryRequestScopeHandle);
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

  dd.syncTimeBoundary ??= syncFrozenTime;

  // The worker's request body, read from the host as the worker reads it.
  const createRequestBodyStream = () => new ReadableStream({
    __proto__: null,
    async pull(controller) {
      const payload = await callOp("op_request_body_read", requestBodyStreamHandle);
      await syncFrozenTime();
      if (!payload || typeof payload !== "object") {
        ReadableStreamDefaultControllerPrototypeClose(controller);
        return;
      }
      if (payload.ok === false) {
        throw new Error(String(payload.error ?? "request body stream failed"));
      }
      if (payload.done === true) {
        ReadableStreamDefaultControllerPrototypeClose(controller);
        return;
      }
      ReadableStreamDefaultControllerPrototypeEnqueue(
        controller,
        callOp("op_http_take_prepared_body", toHandle(payload.body_handle)),
      );
    },
    async cancel() {
      await callOp("op_request_body_cancel", requestBodyStreamHandle);
      await syncFrozenTime();
    },
  }, STREAM_PULL_ON_READ);
