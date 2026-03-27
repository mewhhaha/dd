const {
  AbortController: DenoAbortController,
  AbortSignal: DenoAbortSignal,
  Blob: DenoBlob,
  CloseEvent,
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
  ReadableStream: DenoReadableStream,
  Request: DenoRequest,
  Response: DenoResponse,
  SubtleCrypto,
  TextDecoder: DenoTextDecoder,
  TextEncoder: DenoTextEncoder,
  TransformStream: DenoTransformStream,
  URL: DenoURL,
  URLPattern: DenoURLPattern,
  URLSearchParams: DenoURLSearchParams,
  WritableStream: DenoWritableStream,
  crypto: denoCrypto,
  fetch: denoFetch,
  performance: denoPerformance,
  reportError,
  structuredClone: denoStructuredClone,
} = globalThis.__dd_deno_runtime ?? {};

const define = (name, value, enumerable = false) => {
  Object.defineProperty(globalThis, name, {
    value,
    enumerable,
    configurable: true,
    writable: true,
  });
};

const requireRuntimeFunction = (name, value) => {
  if (typeof value !== "function") {
    throw new Error(`dd bootstrap missing required Deno runtime global: ${name}`);
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

const textEncoder = new DenoTextEncoder();
const textDecoder = new DenoTextDecoder("utf-8");

const createAbortError = (reason) => reason ?? new Error("Aborted");
let frozenNowMs = Date.now();
let frozenPerfMs = globalThis.performance?.now?.() ?? 0;

function ensureAbortGlobals() {
  define("AbortSignal", DenoAbortSignal);
  define("AbortController", DenoAbortController);
}

function setFrozenTime(nowMs, perfMs = nowMs) {
  const nextNowMs = Number(nowMs);
  if (Number.isFinite(nextNowMs)) {
    frozenNowMs = nextNowMs;
  }
  const nextPerfMs = Number(perfMs);
  if (Number.isFinite(nextPerfMs)) {
    frozenPerfMs = nextPerfMs;
  }
}

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

const RequestCtor = RuntimeRequest;
const ResponseCtor = RuntimeResponse;

function runtimeOp(name, ...args) {
  const op = Deno?.core?.ops?.[name];
  if (typeof op !== "function") {
    return undefined;
  }
  return op(...args);
}

function ensureTimerGlobals() {
  let nextTimerId = 1;
  const timers = new Map();

  const clampDelay = (value) => {
    const parsed = Number(value);
    if (!Number.isFinite(parsed) || parsed < 0) {
      return 0;
    }
    return Math.floor(parsed);
  };

  const runCallback = (callback, args) => {
    try {
      callback(...args);
    } catch (error) {
      Promise.resolve().then(() => {
        throw error;
      });
    }
  };

  const sleepWithBoundarySync = async (delayMs) => {
    let remaining = delayMs;
    while (remaining > 0) {
      const step = Math.min(remaining, 0x7fffffff);
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
}

const BASE64_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

function ensureEncodingGlobals() {
  define("TextEncoder", DenoTextEncoder);
  define("TextDecoder", DenoTextDecoder);

  if (globalThis.btoa === undefined) {
    define("btoa", (value) => {
      const input = String(value);
      let output = "";
      let i = 0;
      while (i < input.length) {
        const a = input.charCodeAt(i++);
        const b = input.charCodeAt(i++);
        const c = input.charCodeAt(i++);
        if (a > 0xff || (Number.isFinite(b) && b > 0xff) || (Number.isFinite(c) && c > 0xff)) {
          throw new TypeError("btoa input must be Latin1");
        }
        const triplet = (a << 16) | ((b || 0) << 8) | (c || 0);
        output += BASE64_ALPHABET[(triplet >> 18) & 0x3f];
        output += BASE64_ALPHABET[(triplet >> 12) & 0x3f];
        output += Number.isFinite(b) ? BASE64_ALPHABET[(triplet >> 6) & 0x3f] : "=";
        output += Number.isFinite(c) ? BASE64_ALPHABET[triplet & 0x3f] : "=";
      }
      return output;
    });
  }

  if (globalThis.atob === undefined) {
    define("atob", (value) => {
      const input = String(value).replace(/\s+/g, "");
      if (input.length % 4 === 1) {
        throw new TypeError("Invalid base64 input");
      }
      const padded = input.padEnd(Math.ceil(input.length / 4) * 4, "=");
      let output = "";
      for (let i = 0; i < padded.length; i += 4) {
        const chars = padded.slice(i, i + 4);
        const sextets = chars.split("").map((char) => {
          if (char === "=") {
            return 0;
          }
          const idx = BASE64_ALPHABET.indexOf(char);
          if (idx === -1) {
            throw new TypeError("Invalid base64 input");
          }
          return idx;
        });
        const triplet =
          (sextets[0] << 18) | (sextets[1] << 12) | (sextets[2] << 6) | sextets[3];
        output += String.fromCharCode((triplet >> 16) & 0xff);
        if (chars[2] !== "=") {
          output += String.fromCharCode((triplet >> 8) & 0xff);
        }
        if (chars[3] !== "=") {
          output += String.fromCharCode(triplet & 0xff);
        }
      }
      return output;
    });
  }
}

function ensureStructuredCloneGlobal() {
  define("structuredClone", denoStructuredClone);
}

function ensureCryptoGlobals() {
  define("Crypto", Crypto);
  define("CryptoKey", DenoCryptoKey);
  define("SubtleCrypto", SubtleCrypto);
  define("crypto", denoCrypto);
}

function normalizeTimeBoundaryValue(value) {
  if (value == null) {
    return null;
  }
  if (typeof value === "number") {
    return { nowMs: value, perfMs: value };
  }
  if (Array.isArray(value)) {
    const [nowMs, perfMs = nowMs] = value;
    return { nowMs, perfMs };
  }
  if (typeof value === "object") {
    const nowMs =
      value.nowMs ?? value.now_ms ?? value.now ?? value.wallMs ?? value.wall_ms;
    const perfMs =
      value.perfMs ?? value.perf_ms ?? value.perf ?? value.monotonicMs ?? value.monotonic_ms ?? nowMs;
    return { nowMs, perfMs };
  }
  return null;
}

async function syncFrozenTimeBoundary() {
  if (typeof globalThis.__dd_sync_time_boundary === "function") {
    await globalThis.__dd_sync_time_boundary();
    return;
  }

  const boundaryRaw = await runtimeOp("op_time_boundary_now");
  const boundary = normalizeTimeBoundaryValue(boundaryRaw);
  if (boundary && typeof globalThis.__dd_set_time === "function") {
    globalThis.__dd_set_time(boundary.nowMs, boundary.perfMs);
  }
}

class Cache {
  constructor(name = "default") {
    this.name = String(name || "default");
  }

  async match(request, options = {}) {
    const _ = options;
    const normalizedRequest = request instanceof RequestCtor ? request : new RequestCtor(request);
    const requestHeaders = Array.from(normalizedRequest.headers.entries());
    const result = await runtimeOp(
      "op_cache_match",
      JSON.stringify({
        cache_name: this.name,
        method: normalizedRequest.method,
        url: normalizedRequest.url,
        headers: requestHeaders,
        bypass_stale: Boolean(globalThis.__dd_cache_bypass_stale),
      }),
    );
    await syncFrozenTimeBoundary();

    if (result && typeof result === "object" && result.ok === false) {
      throw new Error(String(result.error ?? "cache match failed"));
    }
    if (!(result && typeof result === "object" && result.found === true)) {
      return undefined;
    }

    if (result.should_revalidate === true) {
      runtimeOp(
        "op_emit_cache_revalidate",
        JSON.stringify({
          cache_name: this.name,
          method: normalizedRequest.method,
          url: normalizedRequest.url,
          headers: requestHeaders,
        }),
      );
      await syncFrozenTimeBoundary();
    }

    return new ResponseCtor(new Uint8Array(result.body ?? []), {
      status: Number(result.status ?? 200),
      headers: Array.isArray(result.headers) ? result.headers : [],
    });
  }

  async put(request, response) {
    const normalizedRequest = request instanceof RequestCtor ? request : new RequestCtor(request);
    if (!(response instanceof ResponseCtor) && typeof response?.arrayBuffer !== "function") {
      throw new TypeError("cache.put expects a Response");
    }
    const body = Array.from(new Uint8Array(await response.arrayBuffer()));
    const result = await runtimeOp(
      "op_cache_put",
      JSON.stringify({
        cache_name: this.name,
        method: normalizedRequest.method,
        url: normalizedRequest.url,
        request_headers: Array.from(normalizedRequest.headers.entries()),
        response_status: response.status,
        response_headers: Array.from(response.headers.entries()),
        response_body: body,
      }),
    );
    await syncFrozenTimeBoundary();
    if (result && typeof result === "object" && result.ok === false) {
      throw new Error(String(result.error ?? "cache put failed"));
    }
  }

  async delete(request, options = {}) {
    const _ = options;
    const normalizedRequest = request instanceof RequestCtor ? request : new RequestCtor(request);
    const result = await runtimeOp(
      "op_cache_delete",
      JSON.stringify({
        cache_name: this.name,
        method: normalizedRequest.method,
        url: normalizedRequest.url,
        headers: Array.from(normalizedRequest.headers.entries()),
      }),
    );
    await syncFrozenTimeBoundary();
    if (result && typeof result === "object" && result.ok === false) {
      throw new Error(String(result.error ?? "cache delete failed"));
    }
    return Boolean(result?.deleted);
  }
}

class CacheStorage {
  constructor() {
    this.default = new Cache("default");
    this._named = new Map();
    this._named.set("default", this.default);
  }

  async open(name) {
    const normalized = String(name ?? "").trim();
    if (!normalized) {
      throw new TypeError("caches.open(name) requires a non-empty cache name");
    }
    const existing = this._named.get(normalized);
    if (existing) {
      return existing;
    }
    const cache = new Cache(normalized);
    this._named.set(normalized, cache);
    return cache;
  }
}

ensureAbortGlobals();
ensureFrozenTimeGlobals();
ensureTimerGlobals();
ensureEncodingGlobals();
ensureStructuredCloneGlobal();
ensureCryptoGlobals();
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
define("RpcTarget", class RpcTarget {});
define("ReadableStream", RuntimeReadableStream);
if (typeof denoFetch === "function") {
  define("fetch", denoFetch);
}
define("caches", new CacheStorage());
define("__dd_set_time", setFrozenTime);
