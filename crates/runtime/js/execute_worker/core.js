// The execute-worker bundle: the units in units.txt, concatenated by
// build.rs. It runs once while the bootstrap snapshot is built, as the body
// of a function whose parameter `__bootstrap` is the runtime's bootstrap
// object, and leaves its entrypoints on `__bootstrap.dd`, which only the
// host reaches.
//
// Each unit defines its helpers once, at the top level of the bundle; a unit
// may use what the units before it define, and any unit's helpers once a
// request runs. Per-request state lives in the request's RequestContext,
// which asyncContext carries through everything the request does, so
// helpers read it from there (currentRequestContext) or take what they need
// as arguments. dd.executeWorkerHandle (bindings.js) is the only per-request
// entrypoint: it builds the request's context and runs the worker under it.
//
// Worker code runs in this same context, so the bundle reads nothing worker
// code can change after startup. Built-ins come from primordials, web
// classes from dd.web and their methods from dd.webPrimordials, ops from
// core.ops, and the clock, timers, console and record constructors from what
// bootstrap.js left on dd, all captured below while the snapshot is built
// (an op function survives into every runtime restored from the snapshot);
// scripts/check-worker-js.mjs rejects any global the bundle names. Index
// loops and SafeArrayIterator stand in for spread, for...of and array
// destructuring; header lists go into Requests and Responses through
// appendHeaderPairs; the records and option dictionaries the bundle builds
// never reach Object.prototype (see RECORD in bootstrap.js). Promises are
// awaited, never returned from an async function or a `then` callback,
// since resolving one promise with another looks up the replaceable
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
  SafeWeakSet,
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
  RECORD,
  ResponseInit,
  STREAM_PULL_ON_READ,
  appendHeaderPairs,
  asyncContext,
  clearTimeout,
  consoleWarn,
  frozenPerfNow,
  headerPairs,
  hostFetchResponse,
  record,
  setTime,
  setTimeout,
  syncTimeBoundary: syncFrozenTime,
  syncTimeBoundaryNow: syncFrozenTimeNow,
} = dd;
const {
  op_http_body_store,
  op_http_body_take,
  op_http_fetch,
  op_http_headers_store,
  op_http_headers_take,
  op_http_prepare,
  op_http_response_close,
  op_http_url_check,
  op_kv_delete,
  op_kv_get,
  op_kv_list,
  op_kv_put,
  op_kv_put_bytes,
  op_memory_batch_accept,
  op_memory_batch_apply,
  op_memory_batch_begin,
  op_memory_batch_close,
  op_memory_batch_command_result_set,
  op_memory_batch_emit,
  op_memory_batch_get,
  op_memory_batch_list,
  op_memory_batch_write,
  op_memory_bytes_take,
  op_memory_command_begin,
  op_memory_command_close,
  op_memory_lease_acquire,
  op_memory_profile_record,
  op_memory_read_begin,
  op_memory_read_close,
  op_memory_read_get,
  op_memory_read_list,
  op_memory_request_scope_close,
  op_memory_socket_close,
  op_memory_socket_list,
  op_register_error_builder,
  op_request_body_close,
  op_request_body_read,
  op_request_context_cancel,
  op_request_context_close,
  op_request_control_take,
  op_request_invocation_take,
  op_request_reply_cancel,
  op_request_wait_until_finish,
  op_request_wait_until_register,
  op_response_complete,
  op_response_fail,
  op_response_start,
  op_response_write,
  op_service_binding_fetch_start,
  op_time_sleep,
  op_worker_deployment_take,
} = core.ops;

const noop = () => undefined;
const ignoreRejection = (promise) => {
  PromisePrototypeCatch(promise, noop);
};
const ONCE = ObjectFreeze({ __proto__: null, once: true });
const FOR_STORAGE = ObjectFreeze({ __proto__: null, forStorage: true });

/** A handle number from an op result or descriptor: a non-negative integer. */
const toHandle = (value) => MathMax(0, MathTrunc(Number(value ?? 0) || 0));

// The records and option dictionaries built per request, from `record` (see
// RECORD in bootstrap.js), so that every request shares one constructor, and
// so one shape, per kind of record.

// What dd.executeWorkerHandle reads from a request's invocation descriptor.
const WorkerInvocation = record(function WorkerInvocation(descriptor, headers, body) {
  this.request_id = String(descriptor.request_id ?? "");
  this.request_context_handle = toHandle(descriptor.request_context_handle);
  this.completion_handle = toHandle(descriptor.completion_handle);
  this.memory_request_scope_handle = toHandle(descriptor.memory_request_scope_handle);
  this.memory_call = descriptor.memory_call ?? null;
  this.request_body_stream_handle = toHandle(descriptor.request_body_stream_handle);
  this.stream_response = descriptor.stream_response === true;
  this.max_response_body_bytes = Number(descriptor.max_response_body_bytes ?? 0);
  this.max_request_body_bytes = Number(descriptor.max_request_body_bytes ?? 0);
  this.method = String(descriptor.method ?? "GET");
  this.url = String(descriptor.url ?? "");
  this.headers = ArrayIsArray(headers) ? headers : [];
  this.input_request_id = String(descriptor.input_request_id ?? "");
  this.body = isUint8Array(body) ? body : new Uint8Array();
});
// The store asyncContext carries for a request. memory.js runs each memory
// call under a copy with its own memory scope. The request's handles here
// are also what dd.abortWorkerRequestHandle releases.
const RequestContext = record(function RequestContext(
  requestId,
  controller,
  requestContextHandle,
  completionHandle,
  memoryRequestScopeHandle,
  requestBodyStreamHandle,
  maxRequestBodyBytes,
  cacheBypassStale,
) {
  this.requestId = requestId;
  this.controller = controller;
  this.waitUntilPromises = [];
  this.requestContextHandle = requestContextHandle;
  this.completionHandle = completionHandle;
  this.memoryRequestScopeHandle = memoryRequestScopeHandle;
  this.requestBodyStreamHandle = requestBodyStreamHandle;
  this.maxRequestBodyBytes = maxRequestBodyBytes;
  this.memoryEntry = null;
  this.memoryTxnScope = null;
  this.socketRuntimeProvider = null;
  this.cacheBypassStale = cacheBypassStale;
});
const copyRequestContext = (context) => {
  const copy = new RequestContext(
    context.requestId,
    context.controller,
    context.requestContextHandle,
    context.completionHandle,
    context.memoryRequestScopeHandle,
    context.requestBodyStreamHandle,
    context.maxRequestBodyBytes,
    context.cacheBypassStale,
  );
  copy.waitUntilPromises = context.waitUntilPromises;
  copy.memoryEntry = context.memoryEntry;
  copy.memoryTxnScope = context.memoryTxnScope;
  copy.socketRuntimeProvider = context.socketRuntimeProvider;
  return copy;
};
// What the request's fetch handler settled with, as stored host handles.
const WorkerResult = record(function WorkerResult(status, headersHandle, bodyHandle) {
  this.status = status;
  this.headersHandle = headersHandle;
  this.bodyHandle = bodyHandle;
});
// Init dictionaries for the Requests the runtime makes. An undefined member
// reads as absent.
const RequestInit = record(function RequestInit(method, body, signal) {
  this.method = method;
  this.body = body;
  this.signal = signal;
});
const ReplyWaitOptions = record(function ReplyWaitOptions(syncTime, signal) {
  this.syncTime = syncTime;
  this.signal = signal;
});
const ReplyWaiter = record(function ReplyWaiter(resolve, reject) {
  this.resolve = resolve;
  this.reject = reject;
});
// A worker's fetch, normalized and with its body read.
const NormalizedFetch = record(function NormalizedFetch(method, url, headers, body, signal, redirect) {
  this.method = method;
  this.url = url;
  this.headers = headers;
  this.body = body;
  this.signal = signal;
  this.redirect = redirect;
});
const CheckedFetchUrl = record(function CheckedFetchUrl(url, clientRid) {
  this.url = url;
  this.clientRid = clientRid;
});
// A memory key's state for the request; storageState is made on first use.
const MemoryEntry = record(function MemoryEntry(binding, memoryKey) {
  this.binding = binding;
  this.memoryKey = memoryKey;
  this.storageState = null;
});
const MemoryStorageState = record(function MemoryStorageState() {
  this.failedError = null;
  this.outputGate = PromiseResolve();
});
const MemoryTxn = record(function MemoryTxn(entry, batchHandle) {
  this.entry = entry;
  this.batchHandle = batchHandle;
});
const MemoryTxnScope = record(function MemoryTxnScope(binding, memoryKey, state) {
  this.binding = binding;
  this.memoryKey = memoryKey;
  this.state = state;
});
const MemoryCommand = record(function MemoryCommand(handle, hit, value) {
  this.handle = handle;
  this.hit = hit;
  this.value = value;
});
// A stored value as an encoding name and its bytes: "utf8" for strings,
// "v8sc" for structured clones.
const EncodedValue = record(function EncodedValue(encoding, value) {
  this.encoding = encoding;
  this.value = value;
});
// What storage.get returns to the worker for a key that is present.
const MemoryGetResult = record(function MemoryGetResult(value) {
  this.value = value;
});
const SocketPayload = record(function SocketPayload(kind, value) {
  this.kind = kind;
  this.value = value;
});
const DataProperty = record(function DataProperty(value) {
  this.value = value;
  this.enumerable = true;
  this.writable = true;
  this.configurable = true;
});

const recordMemoryProfile = (metric, durationMs, items = 1) => {
  op_memory_profile_record(
    String(metric),
    MathMax(0, MathRound(Number(durationMs ?? 0) * 1000)),
    MathMax(1, MathTrunc(Number(items ?? 1) || 1)),
  );
};

/** The current request's context, or null (an error when `required`) outside one. */
const currentRequestContext = (required = true) => {
  const current = asyncContext.getStore();
  if (!current && required) {
    throw new Error("request scope is unavailable");
  }
  return current;
};

const activeRequestContextHandle = () => {
  const handle = toHandle(currentRequestContext().requestContextHandle);
  if (handle <= 0) {
    throw new Error("request context handle is unavailable");
  }
  return handle;
};

// What bootstrap.js reads of the current request: the id console lines are
// tagged with and whether the cache should bypass stale entries. Both read
// as empty outside a request.
dd.runtimeRequestId = () => currentRequestContext(false)?.requestId ?? "";
dd.runtimeRequestContextHandle = activeRequestContextHandle;
dd.cacheBypassStale = () => currentRequestContext(false)?.cacheBypassStale === true;

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
  const bytes = op_http_body_take(toHandle(valueHandle));
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

// Each call syncs the frozen clock once its op settles, whether the op
// succeeded or threw (the KV ops name the call and key in their errors). The
// op starts before the `try`, so a call that throws before its op runs, on a
// value that cannot be serialized, leaves the clock alone.
const createKvBinding = (bindingName) => ObjectFreeze({
  async get(key) {
    const pending = op_kv_get(bindingName, String(key));
    let value;
    try {
      value = await pending;
    } finally {
      syncFrozenTimeNow();
    }
    return value === null ? null : decodeStoredValue(value.encoding, value.value_handle, "kv get");
  },
  async put(key, value) {
    const normalizedKey = String(key);
    const pending = typeof value === "string"
      ? op_kv_put(bindingName, normalizedKey, value)
      : op_kv_put_bytes(bindingName, normalizedKey,
        "v8sc", new Uint8Array(core.serialize(value, FOR_STORAGE)));
    try {
      await pending;
    } finally {
      syncFrozenTimeNow();
    }
  },
  async delete(key) {
    const pending = op_kv_delete(bindingName, String(key));
    try {
      await pending;
    } finally {
      syncFrozenTimeNow();
    }
  },
  async list(options = {}) {
    const prefix = String(options.prefix ?? "");
    const limit = options.limit ?? 100;
    if (!NumberIsInteger(limit) || limit < 1 || limit > 1000) {
      throw new Error(`kv list limit must be an integer between 1 and 1000: ${limit}`);
    }
    const pending = op_kv_list(bindingName, prefix, limit);
    let entries;
    try {
      entries = await pending;
    } finally {
      syncFrozenTimeNow();
    }
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

// The worker's request body, read from the host stream `handle` as the
// worker reads it.
const createRequestBodyStream = (handle) => new ReadableStream({
  __proto__: null,
  async pull(controller) {
    let chunk;
    try {
      chunk = await op_request_body_read(handle);
    } finally {
      await syncFrozenTime();
    }
    if (chunk === null) {
      ReadableStreamDefaultControllerPrototypeClose(controller);
      return;
    }
    ReadableStreamDefaultControllerPrototypeEnqueue(controller, chunk);
  },
  async cancel() {
    await op_request_body_close(handle);
    await syncFrozenTime();
  },
}, STREAM_PULL_ON_READ);
