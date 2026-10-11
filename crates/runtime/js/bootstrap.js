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
  ObjectCreate,
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
  AbortController,
  AbortSignal,
  Blob,
  ByteLengthQueuingStrategy,
  CloseEvent,
  CountQueuingStrategy,
  Crypto,
  CryptoKey,
  CustomEvent,
  DOMException,
  ErrorEvent,
  Event,
  EventTarget,
  File,
  FormData,
  Headers,
  MessageEvent,
  ProgressEvent,
  PromiseRejectionEvent,
  ReadableByteStreamController,
  ReadableStream,
  ReadableStreamBYOBReader,
  ReadableStreamBYOBRequest,
  ReadableStreamDefaultController,
  ReadableStreamDefaultReader,
  Request,
  Response,
  SubtleCrypto,
  TextDecoder,
  TextDecoderStream,
  TextEncoder,
  TextEncoderStream,
  TransformStream,
  TransformStreamDefaultController,
  URL,
  URLPattern,
  URLSearchParams,
  WritableStream,
  WritableStreamDefaultController,
  WritableStreamDefaultWriter,
  fetch,
  performance,
  reportError,
  structuredClone,
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
const {
  op_base64_decode,
  op_base64_encode_from_buffer,
  op_cache_delete,
  op_cache_match,
  op_cache_put,
  op_cache_revalidate,
  op_call_console,
  op_console_write,
  op_http_body_store,
  op_http_body_take,
  op_http_headers_store,
  op_http_headers_take,
  op_time_now,
  op_time_sleep,
} = core.ops;

// The records and option dictionaries the runtime builds per call, here and
// in the execute-worker bundle, which takes RECORD, `record` and the shared
// constructors from dd. Each constructor's prototype is RECORD, an empty
// object with a null prototype, so nothing worker code adds to
// Object.prototype reads through into them. A `{ __proto__: null }` literal
// would do the same, but V8 builds those as dictionary-mode objects, several
// times slower to create and to read. One constructor per kind of record
// gives every record of that kind one shape.
const RECORD = ObjectFreeze(ObjectCreate(null));
const record = (constructor) => {
  constructor.prototype = RECORD;
  return constructor;
};
const TimerState = record(function TimerState(delay, repeat) {
  this.canceled = false;
  this.delay = delay;
  this.repeat = repeat;
});
const CachedResponseParts = record(function CachedResponseParts(bytes, headers, status) {
  this.bytes = bytes;
  this.headers = headers;
  this.status = status;
});
// The init dictionary of the Responses the runtime makes. An undefined
// member reads as absent.
const ResponseInit = record(function ResponseInit(status, statusText) {
  this.status = status;
  this.statusText = statusText;
});

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

const RuntimeHeaders = requireRuntimeFunction("Headers", Headers);
const RuntimeResponse = requireRuntimeFunction("Response", Response);
const RuntimeFormData = requireRuntimeFunction("FormData", FormData);
const RuntimeRequest = requireRuntimeFunction("Request", Request);
const RuntimeURL = requireRuntimeFunction("URL", URL);
const RuntimeURLSearchParams = requireRuntimeFunction("URLSearchParams", URLSearchParams);
const RuntimeURLPattern = requireRuntimeFunction("URLPattern", URLPattern);
const RuntimeBlob = requireRuntimeFunction("Blob", Blob);
const RuntimeFile = requireRuntimeFunction("File", File);
const RuntimeReadableStream = requireRuntimeFunction("ReadableStream", ReadableStream);
const RuntimeWritableStream = requireRuntimeFunction("WritableStream", WritableStream);
const RuntimeTransformStream = requireRuntimeFunction("TransformStream", TransformStream);

let frozenNowMs = DateNow();
let frozenPerfMs = globalThis.performance?.now?.() ?? 0;

function ensureAbortGlobals() {
  define("AbortSignal", AbortSignal);
  define("AbortController", AbortController);
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
// performance.now. The runtime reads the same clock through dd.frozenPerfNow,
// never through those globals.
function ensureFrozenTimeGlobals() {
  Date.now = () => frozenNowMs;

  if (globalThis.performance === undefined) {
    define("performance", performance);
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

// Moves the frozen clock to the host's, as op_time_now reads it at
// an I/O boundary. The execute-worker bundle syncs through these too.
function applyTimeBoundary(boundary) {
  setFrozenTime(boundary.now_ms, boundary.perf_ms);
}

async function syncFrozenTimeBoundary() {
  applyTimeBoundary(await op_time_now());
}

function syncFrozenTimeBoundaryNow() {
  applyTimeBoundary(op_time_now());
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
      await op_time_sleep(step);
      await syncFrozenTimeBoundary();
      remaining -= step;
    }
  };

  const schedule = (callback, delay, args, repeat) => {
    if (typeof callback !== "function") {
      throw new TypeError("Timer callback must be a function");
    }
    const id = nextTimerId++;
    const state = new TimerState(clampDelay(delay), Boolean(repeat));
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
  define("TextEncoder", TextEncoder);
  define("TextDecoder", TextDecoder);
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
    return op_base64_encode_from_buffer(bytes, 0, bytes.length);
  });
  define("atob", function atob(data) {
    if (arguments.length === 0) {
      throw new TypeError("atob requires 1 argument");
    }
    let bytes;
    try {
      bytes = op_base64_decode(String(data));
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
  define("structuredClone", structuredClone);
}

// The request a continuation belongs to, and the stores of AsyncLocalStorage
// instances, ride V8's continuation-preserved data. Workers get only the
// AsyncLocalStorage half, as __dd_async_context, which dd-vite's
// node:async_hooks shim builds on; the request half is the runtime's.
function ensureAsyncContextGlobal() {
  const asyncContextFrame = Symbol("dd.asyncContextFrame");
  const getAsyncContext = () => core.getAsyncContext();
  const setAsyncContext = (value) => core.setAsyncContext(value ?? null);
  // A frame's map is never changed once the frame is current: each change
  // goes into a copy, in a new frame, so frames that only swap the request
  // store share their parent's map.
  const AsyncContextFrame = record(function AsyncContextFrame(requestStore, asyncLocalStores) {
    this[asyncContextFrame] = true;
    this.requestStore = requestStore;
    this.asyncLocalStores = asyncLocalStores;
  });
  const noAsyncLocalStores = new SafeMap();
  const emptyFrame = new AsyncContextFrame(null, noAsyncLocalStores);
  const frameFor = (context) => {
    if (context == null) return emptyFrame;
    return context[asyncContextFrame] === true
      ? context
      : new AsyncContextFrame(context, noAsyncLocalStores);
  };
  const derivedFrame = (context) => {
    const frame = frameFor(context);
    const asyncLocalStores = new SafeMap();
    MapPrototypeForEach(frame.asyncLocalStores, (store, storage) => {
      asyncLocalStores.set(storage, store);
    });
    return new AsyncContextFrame(frame.requestStore, asyncLocalStores);
  };
  const withRequestStore = (context, store) =>
    new AsyncContextFrame(store ?? null, frameFor(context).asyncLocalStores);

  const asyncContext = {
    __proto__: null,
    getStore() {
      return frameFor(getAsyncContext()).requestStore;
    },
    enterWith(store) {
      setAsyncContext(withRequestStore(getAsyncContext(), store));
      return store ?? null;
    },
    run(store, callback, ...args) {
      if (typeof callback !== "function") {
        throw new TypeError("asyncContext.run(store, callback) requires a function");
      }
      const previous = getAsyncContext() ?? null;
      setAsyncContext(withRequestStore(previous, store));
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
// tagged with the request it ran under (dd.runtimeRequestId, which the
// execute-worker bundle installs); console.time reads the same frozen
// clock as performance.now. Each method is op_call_console bound to V8's
// method of the same name and dd's: while a DevTools session is attached it
// hands the call to V8's console too, so DevTools shows inspectable values
// at the caller's location (the native op adds no stack frame). The runtime
// logs its own warnings through the console as created, whatever worker code
// does to the global.
function ensureConsoleGlobal() {
  const { createConsole } = core.loadExtScript("ext:web/console.js");
  const console = createConsole(
    (level, message) => op_console_write(level, message, dd.runtimeRequestId()),
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
        op_call_console,
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
  define("CryptoKey", CryptoKey);
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

const toRequest = (request) => ObjectPrototypeIsPrototypeOf(RequestPrototype, request)
  ? request
  : new RuntimeRequest(request);

// The status, header pairs and body of what cache.put was given: a Response,
// or an object shaped like one, read through its own methods.
async function cachedResponseParts(response) {
  if (ObjectPrototypeIsPrototypeOf(ResponsePrototype, response)) {
    return new CachedResponseParts(
      new Uint8Array(await ResponsePrototypeArrayBuffer(response)),
      headerPairs(ResponsePrototypeGetHeaders(response)),
      ResponsePrototypeGetStatus(response),
    );
  }
  if (typeof response?.arrayBuffer !== "function") {
    throw new TypeError("cache.put expects a Response");
  }
  const bytes = new Uint8Array(await response.arrayBuffer());
  const headers = ObjectPrototypeIsPrototypeOf(HeadersPrototype, response.headers)
    ? headerPairs(response.headers)
    : ArrayFrom(response.headers.entries());
  return new CachedResponseParts(bytes, headers, Number(response.status ?? 200));
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
    const pending = op_cache_match(
      this.name,
      method,
      url,
      op_http_headers_store(requestHeaders),
      dd.cacheBypassStale(),
    );
    // The cache ops throw what storage failed with; the frozen clock syncs
    // once each settles, either way.
    let result;
    try {
      result = await pending;
    } finally {
      await syncFrozenTimeBoundary();
    }

    if (result === null) {
      return undefined;
    }

    if (result.should_revalidate === true) {
      op_cache_revalidate(
        this.name,
        method,
        url,
        op_http_headers_store(requestHeaders),
      );
      await syncFrozenTimeBoundary();
    }

    const body = op_http_body_take(Number(result.body_handle ?? 0));
    const headers = op_http_headers_take(Number(result.headers_handle ?? 0));
    const response = new RuntimeResponse(
      body,
      new ResponseInit(Number(result.status ?? 200), undefined),
    );
    appendHeaderPairs(ResponsePrototypeGetHeaders(response), ArrayIsArray(headers) ? headers : []);
    return response;
  }

  async put(request, response) {
    const normalizedRequest = toRequest(request);
    const parts = await cachedResponseParts(response);
    const bodyHandle = op_http_body_store(parts.bytes);
    const requestHeadersHandle = op_http_headers_store(
      headerPairs(RequestPrototypeGetHeaders(normalizedRequest)),
    );
    const responseHeadersHandle = op_http_headers_store(parts.headers);
    const pending = op_cache_put(
      this.name,
      RequestPrototypeGetMethod(normalizedRequest),
      RequestPrototypeGetUrl(normalizedRequest),
      requestHeadersHandle,
      Number(parts.status ?? 200),
      responseHeadersHandle,
      bodyHandle,
    );
    try {
      await pending;
    } finally {
      await syncFrozenTimeBoundary();
    }
  }

  async delete(request, _options = undefined) {
    const normalizedRequest = toRequest(request);
    const pending = op_cache_delete(
      this.name,
      RequestPrototypeGetMethod(normalizedRequest),
      RequestPrototypeGetUrl(normalizedRequest),
      op_http_headers_store(headerPairs(RequestPrototypeGetHeaders(normalizedRequest))),
    );
    let deleted;
    try {
      deleted = await pending;
    } finally {
      await syncFrozenTimeBoundary();
    }
    return deleted;
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
define("fetch", fetch);
define("caches", new CacheStorage());
dd.setTime = setFrozenTime;
dd.frozenPerfNow = () => frozenPerfMs;
dd.syncTimeBoundary = syncFrozenTimeBoundary;
dd.syncTimeBoundaryNow = syncFrozenTimeBoundaryNow;
dd.RECORD = RECORD;
dd.record = record;
dd.ResponseInit = ResponseInit;
