// dd's worker globals over the web layer. Runs once while the bootstrap
// snapshot is built, with the bootstrap object as `__bootstrap`. What it
// defines on globalThis is the worker-visible platform; what it puts on
// `__bootstrap.dd` stays private to the runtime.
//
// Everything here runs on primordials, the web classes as init.js captured
// them and their prototype methods from `dd.webPrimordials`, so worker code
// that replaces globals or built-in methods cannot change how timers, the
// frozen clock, base64, async context or the cache behave.
const { core, dd, primordials } = __bootstrap;

const {
  ArrayIsArray,
  ArrayFrom,
  Boolean,
  Date,
  DateNow,
  Error,
  FunctionPrototypeBind,
  MapPrototypeForEach,
  MathFloor,
  MathMin,
  Number,
  NumberIsFinite,
  ObjectDefineProperty,
  ObjectFreeze,
  ObjectKeys,
  ObjectPrototypeIsPrototypeOf,
  PromisePrototypeThen,
  PromiseResolve,
  ReflectApply,
  SafeMap,
  String,
  StringFromCharCode,
  StringPrototypeCharCodeAt,
  StringPrototypeTrim,
  TypedArrayPrototypeSubarray,
  Symbol,
  TypeError,
  Uint8Array,
  globalThis,
} = primordials;

// V8's own console, which V8 installs in every context (the snapshot's
// included), taken before dd's replaces it. Only DevTools sees its calls.
const inspectorConsole = globalThis.console;

const {
  AbortController: DenoAbortController,
  AbortSignal: DenoAbortSignal,
  Blob: DenoBlob,
  ByteLengthQueuingStrategy,
  CloseEvent,
  CountQueuingStrategy,
  Crypto,
  CryptoKey: DenoCryptoKey,
  CustomEvent,
  DOMException,
  ErrorEvent,
  Event,
  EventTarget,
  File: DenoFile,
  FormData: DenoFormData,
  Headers: DenoHeaders,
  MessageEvent,
  ProgressEvent,
  PromiseRejectionEvent,
  ReadableByteStreamController,
  ReadableStream: DenoReadableStream,
  ReadableStreamBYOBReader,
  ReadableStreamBYOBRequest,
  ReadableStreamDefaultController,
  ReadableStreamDefaultReader,
  Request: DenoRequest,
  Response: DenoResponse,
  SubtleCrypto,
  TextDecoder: DenoTextDecoder,
  TextDecoderStream,
  TextEncoder: DenoTextEncoder,
  TextEncoderStream,
  TransformStream: DenoTransformStream,
  TransformStreamDefaultController,
  URL: DenoURL,
  URLPattern: DenoURLPattern,
  URLSearchParams: DenoURLSearchParams,
  WritableStream: DenoWritableStream,
  WritableStreamDefaultController,
  WritableStreamDefaultWriter,
  fetch: denoFetch,
  performance: denoPerformance,
  reportError,
  structuredClone: denoStructuredClone,
} = dd.web;

const {
  HeadersPrototype,
  RequestPrototype,
  RequestPrototypeGetHeaders,
  RequestPrototypeGetMethod,
  RequestPrototypeGetUrl,
  ResponsePrototype,
  ResponsePrototypeArrayBuffer,
  ResponsePrototypeGetHeaders,
  ResponsePrototypeGetStatus,
} = dd.webPrimordials;
const { appendHeaderPairs, headerPairs } = dd;

const define = (name, value, enumerable = false) => {
  ObjectDefineProperty(globalThis, name, {
    __proto__: null,
    value,
    enumerable,
    configurable: true,
    writable: true,
  });
};

const requireRuntimeFunction = (name, value) => {
  if (typeof value !== "function") {
    throw new Error(`dd bootstrap is missing the web class ${name}`);
  }
  return value;
};

const RuntimeHeaders = requireRuntimeFunction("Headers", DenoHeaders);
const RuntimeResponse = requireRuntimeFunction("Response", DenoResponse);
const RuntimeFormData = requireRuntimeFunction("FormData", DenoFormData);
const RuntimeRequest = requireRuntimeFunction("Request", DenoRequest);
const RuntimeURL = requireRuntimeFunction("URL", DenoURL);
const RuntimeURLSearchParams = requireRuntimeFunction("URLSearchParams", DenoURLSearchParams);
const RuntimeURLPattern = requireRuntimeFunction("URLPattern", DenoURLPattern);
const RuntimeBlob = requireRuntimeFunction("Blob", DenoBlob);
const RuntimeFile = requireRuntimeFunction("File", DenoFile);
const RuntimeReadableStream = requireRuntimeFunction("ReadableStream", DenoReadableStream);
const RuntimeWritableStream = requireRuntimeFunction("WritableStream", DenoWritableStream);
const RuntimeTransformStream = requireRuntimeFunction("TransformStream", DenoTransformStream);

let frozenNowMs = DateNow();
let frozenPerfMs = globalThis.performance?.now?.() ?? 0;

function ensureAbortGlobals() {
  define("AbortSignal", DenoAbortSignal);
  define("AbortController", DenoAbortController);
}

function setFrozenTime(nowMs, perfMs = nowMs) {
  const nextNowMs = Number(nowMs);
  if (NumberIsFinite(nextNowMs)) {
    frozenNowMs = nextNowMs;
  }
  const nextPerfMs = Number(perfMs);
  if (NumberIsFinite(nextPerfMs)) {
    frozenPerfMs = nextPerfMs;
  }
}

// Workers see the clock frozen between I/O boundaries through Date.now and
// performance.now. The runtime reads the same clock through dd.frozenNow and
// dd.frozenPerfNow, never through those globals.
function ensureFrozenTimeGlobals() {
  Date.now = () => frozenNowMs;

  if (globalThis.performance === undefined) {
    define("performance", denoPerformance);
  }

  try {
    globalThis.performance.now = () => frozenPerfMs;
  } catch {
    define("performance", {
      ...globalThis.performance,
      now: () => frozenPerfMs,
    });
  }
}

function runtimeOp(name, ...args) {
  const op = core.ops[name];
  if (typeof op !== "function") {
    return undefined;
  }
  return ReflectApply(op, undefined, args);
}

function ensureTimerGlobals() {
  let nextTimerId = 1;
  const timers = new SafeMap();

  const clampDelay = (value) => {
    // At least 1 ms, as in Node: a 0 delay would run the callback as a
    // microtask, ahead of work already queued, and setInterval(fn, 0) would
    // never let the event loop turn.
    const parsed = Number(value);
    if (!NumberIsFinite(parsed) || parsed < 1) {
      return 1;
    }
    return MathFloor(parsed);
  };

  const runCallback = (callback, args) => {
    try {
      ReflectApply(callback, undefined, args);
    } catch (error) {
      PromisePrototypeThen(PromiseResolve(), () => {
        throw error;
      });
    }
  };

  const sleepWithBoundarySync = async (delayMs) => {
    let remaining = delayMs;
    while (remaining > 0) {
      const step = MathMin(remaining, 0x7fffffff);
      await runtimeOp("op_sleep", step);
      await syncFrozenTimeBoundary();
      remaining -= step;
    }
  };

  const schedule = (callback, delay, args, repeat) => {
    if (typeof callback !== "function") {
      throw new TypeError("Timer callback must be a function");
    }
    const id = nextTimerId++;
    const state = {
      __proto__: null,
      canceled: false,
      delay: clampDelay(delay),
      repeat: Boolean(repeat),
    };
    timers.set(id, state);

    (async () => {
      while (!state.canceled) {
        await sleepWithBoundarySync(state.delay);
        if (state.canceled) {
          break;
        }
        runCallback(callback, args);
        if (!state.repeat) {
          timers.delete(id);
          break;
        }
      }
    })();

    return id;
  };

  const cancel = (id) => {
    const key = Number(id);
    const state = timers.get(key);
    if (!state) {
      return;
    }
    state.canceled = true;
    timers.delete(key);
  };

  define("setTimeout", (callback, delay = 0, ...args) => schedule(callback, delay, args, false));
  define("clearTimeout", (id) => cancel(id));
  define(
    "setInterval",
    (callback, delay = 0, ...args) => schedule(callback, delay, args, true),
  );
  define("clearInterval", (id) => cancel(id));
  // The runtime's own timeouts, on the same clock as the worker's.
  dd.setTimeout = (callback, delay) => schedule(callback, delay, [], false);
  dd.clearTimeout = cancel;
}

function ensureEncodingGlobals() {
  define("TextEncoder", DenoTextEncoder);
  define("TextDecoder", DenoTextDecoder);
  define("TextEncoderStream", TextEncoderStream);
  define("TextDecoderStream", TextDecoderStream);

  // HTML's forgiving base64 (the encoding ops implement it). Both throw an
  // InvalidCharacterError DOMException for input they cannot take.
  define("btoa", function btoa(data) {
    if (arguments.length === 0) {
      throw new TypeError("btoa requires 1 argument");
    }
    const input = String(data);
    const bytes = new Uint8Array(input.length);
    for (let i = 0; i < input.length; i++) {
      const code = StringPrototypeCharCodeAt(input, i);
      if (code > 0xff) {
        throw new DOMException("btoa input must be Latin1", "InvalidCharacterError");
      }
      bytes[i] = code;
    }
    return core.ops.op_base64_encode_from_buffer(bytes, 0, bytes.length);
  });
  define("atob", function atob(data) {
    if (arguments.length === 0) {
      throw new TypeError("atob requires 1 argument");
    }
    let bytes;
    try {
      bytes = core.ops.op_base64_decode(String(data));
    } catch {
      throw new DOMException("atob input is not valid base64", "InvalidCharacterError");
    }
    let output = "";
    for (let i = 0; i < bytes.length; i += 8192) {
      output += ReflectApply(StringFromCharCode, undefined, TypedArrayPrototypeSubarray(bytes, i, i + 8192));
    }
    return output;
  });
}

function ensureStructuredCloneGlobal() {
  define("structuredClone", denoStructuredClone);
}

// The request a continuation belongs to, and the stores of AsyncLocalStorage
// instances, ride V8's continuation-preserved data. Workers get only the
// AsyncLocalStorage half, as __dd_async_context, which dd-vite's
// node:async_hooks shim builds on; the request half is the runtime's.
function ensureAsyncContextGlobal() {
  const asyncContextFrame = Symbol("dd.asyncContextFrame");
  const getAsyncContext = () => core.getAsyncContext();
  const setAsyncContext = (value) => core.setAsyncContext(value ?? null);
  const frameFor = (context) => context?.[asyncContextFrame] === true
    ? context
    : {
      __proto__: null,
      [asyncContextFrame]: true,
      requestStore: context ?? null,
      asyncLocalStores: new SafeMap(),
    };
  const derivedFrame = (context) => {
    const frame = frameFor(context);
    const asyncLocalStores = new SafeMap();
    MapPrototypeForEach(frame.asyncLocalStores, (store, storage) => {
      asyncLocalStores.set(storage, store);
    });
    return {
      __proto__: null,
      [asyncContextFrame]: true,
      requestStore: frame.requestStore,
      asyncLocalStores,
    };
  };

  const asyncContext = {
    __proto__: null,
    getStore() {
      return frameFor(getAsyncContext()).requestStore;
    },
    enterWith(store) {
      const frame = derivedFrame(getAsyncContext());
      frame.requestStore = store ?? null;
      setAsyncContext(frame);
      return store ?? null;
    },
    run(store, callback, ...args) {
      if (typeof callback !== "function") {
        throw new TypeError("asyncContext.run(store, callback) requires a function");
      }
      const previous = getAsyncContext() ?? null;
      const frame = derivedFrame(previous);
      frame.requestStore = store ?? null;
      setAsyncContext(frame);
      try {
        return ReflectApply(callback, undefined, args);
      } finally {
        setAsyncContext(previous);
      }
    },
    getAsyncLocalStore(storage) {
      return frameFor(getAsyncContext()).asyncLocalStores.get(storage);
    },
    enterWithAsyncLocalStore(storage, store) {
      const frame = derivedFrame(getAsyncContext());
      frame.asyncLocalStores.set(storage, store);
      setAsyncContext(frame);
    },
    runWithAsyncLocalStore(storage, store, callback, ...args) {
      if (typeof callback !== "function") {
        throw new TypeError("AsyncLocalStorage.run(store, callback) requires a function");
      }
      const previous = getAsyncContext() ?? null;
      const frame = derivedFrame(previous);
      frame.asyncLocalStores.set(storage, store);
      setAsyncContext(frame);
      try {
        return ReflectApply(callback, undefined, args);
      } finally {
        setAsyncContext(previous);
      }
    },
    disableAsyncLocalStore(storage) {
      const frame = derivedFrame(getAsyncContext());
      frame.asyncLocalStores.delete(storage);
      setAsyncContext(frame);
    },
  };
  dd.asyncContext = asyncContext;
  define("__dd_async_context", ObjectFreeze({
    getAsyncLocalStore: asyncContext.getAsyncLocalStore,
    enterWithAsyncLocalStore: asyncContext.enterWithAsyncLocalStore,
    runWithAsyncLocalStore: asyncContext.runWithAsyncLocalStore,
    disableAsyncLocalStore: asyncContext.disableAsyncLocalStore,
  }));
}

// Each console call is formatted here and handed to the host as one line,
// tagged with the request it ran under; console.time reads the same frozen
// clock as performance.now. Each method is op_call_console bound to V8's
// method of the same name and dd's: while a DevTools session is attached it
// hands the call to V8's console too, so DevTools shows inspectable values
// at the caller's location (the native op adds no stack frame). The runtime
// logs its own warnings through the console as created, whatever worker code
// does to the global.
function ensureConsoleGlobal() {
  const { createConsole } = core.loadExtScript("ext:deno_web/01_console.js");
  const console = createConsole(
    (level, message) => core.ops.op_console_write(level, message, activeRuntimeRequestId()),
    () => frozenPerfMs,
  );
  if (inspectorConsole !== null && typeof inspectorConsole === "object") {
    const names = ObjectKeys(console);
    for (let i = 0; i < names.length; i++) {
      const name = names[i];
      const inspectorMethod = inspectorConsole[name];
      if (typeof inspectorMethod !== "function") {
        continue;
      }
      const method = FunctionPrototypeBind(
        core.ops.op_call_console,
        console,
        inspectorMethod,
        console[name],
      );
      ObjectDefineProperty(method, "name", { __proto__: null, value: name, configurable: true });
      console[name] = method;
    }
  }
  define("console", console);
  dd.consoleWarn = console.warn;
}

function ensureCryptoGlobals() {
  define("Crypto", Crypto);
  define("CryptoKey", DenoCryptoKey);
  define("SubtleCrypto", SubtleCrypto);
  ObjectDefineProperty(globalThis, "crypto", {
    __proto__: null,
    get() {
      return dd.web.crypto;
    },
    enumerable: false,
    configurable: true,
  });
}

function normalizeTimeBoundaryValue(value) {
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
}

async function syncFrozenTimeBoundary() {
  if (typeof dd.syncTimeBoundary === "function") {
    await dd.syncTimeBoundary();
    return;
  }

  const boundaryRaw = await runtimeOp("op_time_boundary_now");
  const boundary = normalizeTimeBoundaryValue(boundaryRaw);
  if (boundary) {
    setFrozenTime(boundary.nowMs, boundary.perfMs);
  }
}

function activeRuntimeRequestId() {
  return typeof dd.runtimeRequestId === "function"
    ? String(dd.runtimeRequestId() ?? "")
    : "";
}

function activeCacheBypassStale() {
  return typeof dd.cacheBypassStale === "function"
    ? Boolean(dd.cacheBypassStale())
    : false;
}

const toRequest = (request) => ObjectPrototypeIsPrototypeOf(RequestPrototype, request)
  ? request
  : new RuntimeRequest(request);

// The status, header pairs and body of what cache.put was given: a Response,
// or an object shaped like one, read through its own methods.
async function cachedResponseParts(response) {
  if (ObjectPrototypeIsPrototypeOf(ResponsePrototype, response)) {
    return {
      __proto__: null,
      bytes: new Uint8Array(await ResponsePrototypeArrayBuffer(response)),
      headers: headerPairs(ResponsePrototypeGetHeaders(response)),
      status: ResponsePrototypeGetStatus(response),
    };
  }
  if (typeof response?.arrayBuffer !== "function") {
    throw new TypeError("cache.put expects a Response");
  }
  const bytes = new Uint8Array(await response.arrayBuffer());
  const headers = ObjectPrototypeIsPrototypeOf(HeadersPrototype, response.headers)
    ? headerPairs(response.headers)
    : ArrayFrom(response.headers.entries());
  return { __proto__: null, bytes, headers, status: Number(response.status ?? 200) };
}

class Cache {
  constructor(name = "default") {
    this.name = String(name || "default");
  }

  async match(request, _options = undefined) {
    const normalizedRequest = toRequest(request);
    const method = RequestPrototypeGetMethod(normalizedRequest);
    const url = RequestPrototypeGetUrl(normalizedRequest);
    const requestHeaders = headerPairs(RequestPrototypeGetHeaders(normalizedRequest));
    const requestHeadersHandle = runtimeOp(
      "op_http_store_prepared_headers",
      requestHeaders,
    );
    const result = await runtimeOp(
      "op_cache_match",
      this.name,
      method,
      url,
      Number(requestHeadersHandle ?? 0),
      activeCacheBypassStale(),
    );
    await syncFrozenTimeBoundary();

    if (result && typeof result === "object" && result.ok === false) {
      throw new Error(String(result.error ?? "cache match failed"));
    }
    if (!(result && typeof result === "object" && result.found === true)) {
      return undefined;
    }

    if (result.should_revalidate === true) {
      const revalidateHeadersHandle = runtimeOp(
        "op_http_store_prepared_headers",
        requestHeaders,
      );
      runtimeOp(
        "op_emit_cache_revalidate",
        this.name,
        method,
        url,
        Number(revalidateHeadersHandle ?? 0),
      );
      await syncFrozenTimeBoundary();
    }

    const body = runtimeOp(
      "op_http_take_prepared_body",
      Number(result.body_handle ?? 0),
    );
    const headers = runtimeOp(
      "op_http_take_prepared_headers",
      Number(result.headers_handle ?? 0),
    );
    const response = new RuntimeResponse(body, {
      __proto__: null,
      status: Number(result.status ?? 200),
    });
    appendHeaderPairs(ResponsePrototypeGetHeaders(response), ArrayIsArray(headers) ? headers : []);
    return response;
  }

  async put(request, response) {
    const normalizedRequest = toRequest(request);
    const parts = await cachedResponseParts(response);
    const bodyHandle = runtimeOp("op_http_store_prepared_body", parts.bytes);
    const requestHeadersHandle = runtimeOp(
      "op_http_store_prepared_headers",
      headerPairs(RequestPrototypeGetHeaders(normalizedRequest)),
    );
    const responseHeadersHandle = runtimeOp("op_http_store_prepared_headers", parts.headers);
    const result = await runtimeOp(
      "op_cache_put",
      this.name,
      RequestPrototypeGetMethod(normalizedRequest),
      RequestPrototypeGetUrl(normalizedRequest),
      Number(requestHeadersHandle ?? 0),
      Number(parts.status ?? 200),
      Number(responseHeadersHandle ?? 0),
      Number(bodyHandle ?? 0),
    );
    await syncFrozenTimeBoundary();
    if (result && typeof result === "object" && result.ok === false) {
      throw new Error(String(result.error ?? "cache put failed"));
    }
  }

  async delete(request, _options = undefined) {
    const normalizedRequest = toRequest(request);
    const headersHandle = runtimeOp(
      "op_http_store_prepared_headers",
      headerPairs(RequestPrototypeGetHeaders(normalizedRequest)),
    );
    const result = await runtimeOp(
      "op_cache_delete",
      this.name,
      RequestPrototypeGetMethod(normalizedRequest),
      RequestPrototypeGetUrl(normalizedRequest),
      Number(headersHandle ?? 0),
    );
    await syncFrozenTimeBoundary();
    if (result && typeof result === "object" && result.ok === false) {
      throw new Error(String(result.error ?? "cache delete failed"));
    }
    return Boolean(result?.deleted);
  }
}

class CacheStorage {
  #named = new SafeMap();

  constructor() {
    this.default = new Cache("default");
    this.#named.set("default", this.default);
  }

  async open(name) {
    const normalized = StringPrototypeTrim(String(name ?? ""));
    if (!normalized) {
      throw new TypeError("caches.open(name) requires a non-empty cache name");
    }
    const existing = this.#named.get(normalized);
    if (existing) {
      return existing;
    }
    const cache = new Cache(normalized);
    this.#named.set(normalized, cache);
    return cache;
  }
}

// The global scope names itself `self`, as a service worker's and a
// Cloudflare Worker's does.
define("self", globalThis);
ensureAbortGlobals();
ensureFrozenTimeGlobals();
ensureTimerGlobals();
ensureEncodingGlobals();
ensureStructuredCloneGlobal();
ensureAsyncContextGlobal();
ensureCryptoGlobals();
ensureConsoleGlobal();
define("DOMException", DOMException);
define("Event", Event);
define("EventTarget", EventTarget);
define("CustomEvent", CustomEvent);
define("ErrorEvent", ErrorEvent);
define("MessageEvent", MessageEvent);
define("ProgressEvent", ProgressEvent);
define("PromiseRejectionEvent", PromiseRejectionEvent);
define("CloseEvent", CloseEvent);
define("reportError", reportError);
define("Headers", RuntimeHeaders);
define("Request", RuntimeRequest);
define("Response", RuntimeResponse);
define("URL", RuntimeURL);
define("URLSearchParams", RuntimeURLSearchParams);
define("URLPattern", RuntimeURLPattern);
define("Blob", RuntimeBlob);
define("File", RuntimeFile);
define("FormData", RuntimeFormData);
define("WritableStream", RuntimeWritableStream);
define("TransformStream", RuntimeTransformStream);
define("Cache", Cache);
define("CacheStorage", CacheStorage);
define("ReadableStream", RuntimeReadableStream);
define("ReadableStreamDefaultReader", ReadableStreamDefaultReader);
define("ReadableStreamBYOBReader", ReadableStreamBYOBReader);
define("ReadableStreamBYOBRequest", ReadableStreamBYOBRequest);
define("ReadableStreamDefaultController", ReadableStreamDefaultController);
define("ReadableByteStreamController", ReadableByteStreamController);
define("WritableStreamDefaultWriter", WritableStreamDefaultWriter);
define("WritableStreamDefaultController", WritableStreamDefaultController);
define("TransformStreamDefaultController", TransformStreamDefaultController);
define("ByteLengthQueuingStrategy", ByteLengthQueuingStrategy);
define("CountQueuingStrategy", CountQueuingStrategy);
if (typeof denoFetch === "function") {
  define("fetch", denoFetch);
}
define("caches", new CacheStorage());
dd.setTime = setFrozenTime;
dd.frozenNow = () => frozenNowMs;
dd.frozenPerfNow = () => frozenPerfMs;
