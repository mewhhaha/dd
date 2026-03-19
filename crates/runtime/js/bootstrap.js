const define = (name, value, enumerable = false) => {
  Object.defineProperty(globalThis, name, {
    value,
    enumerable,
    configurable: true,
    writable: true,
  });
};

const textEncoder = globalThis.TextEncoder ? new TextEncoder() : null;
const textDecoder = globalThis.TextDecoder ? new TextDecoder("utf-8") : null;

const createAbortError = (reason) => reason ?? new Error("Aborted");
let frozenNowMs = Date.now();
let frozenPerfMs = globalThis.performance?.now?.() ?? 0;

function ensureAbortGlobals() {
  if (globalThis.AbortSignal === undefined) {
    class AbortSignal {
      constructor() {
        this.aborted = false;
        this.reason = undefined;
        this.onabort = null;
        this._listeners = new Set();
      }

      addEventListener(type, listener) {
        if (type === "abort" && typeof listener === "function") {
          this._listeners.add(listener);
        }
      }

      removeEventListener(type, listener) {
        if (type === "abort") {
          this._listeners.delete(listener);
        }
      }

      dispatchEvent(event) {
        if (event?.type !== "abort") {
          return true;
        }

        if (typeof this.onabort === "function") {
          try {
            this.onabort.call(this, event);
          } catch {
            // Ignore listener errors during abort propagation.
          }
        }
        for (const listener of this._listeners) {
          try {
            listener.call(this, event);
          } catch {
            // Ignore listener errors during abort propagation.
          }
        }
        return true;
      }

      throwIfAborted() {
        if (this.aborted) {
          throw createAbortError(this.reason);
        }
      }
    }

    define("AbortSignal", AbortSignal);
  }

  if (globalThis.AbortController === undefined) {
    class AbortController {
      constructor() {
        this.signal = new AbortSignal();
      }

      abort(reason) {
        const signal = this.signal;
        if (signal.aborted) {
          return;
        }

        signal.aborted = true;
        signal.reason = createAbortError(reason);
        signal.dispatchEvent({ type: "abort", target: signal });
      }
    }

    define("AbortController", AbortController);
  }
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
    define("performance", {
      now: () => frozenPerfMs,
    });
    return;
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

function normalizeBody(body) {
  if (body == null) {
    return null;
  }
  if (typeof body === "string") {
    return encodeUtf8(body);
  }
  if (body instanceof Uint8Array) {
    return body;
  }
  if (body instanceof ArrayBuffer) {
    return new Uint8Array(body);
  }
  if (ArrayBuffer.isView(body)) {
    return new Uint8Array(body.buffer.slice(body.byteOffset, body.byteOffset + body.byteLength));
  }
  throw new TypeError("Unsupported request body type");
}

function isReadableStreamLike(body) {
  return (
    body !== null &&
    typeof body === "object" &&
    (typeof body.getReader === "function" || typeof body[Symbol.asyncIterator] === "function")
  );
}

function normalizeResponseBody(body) {
  if (isReadableStreamLike(body)) {
    return body;
  }
  return normalizeBody(body);
}

function toUint8Array(chunk) {
  if (chunk == null) {
    return null;
  }
  if (chunk instanceof Uint8Array) {
    return chunk;
  }
  if (chunk instanceof ArrayBuffer) {
    return new Uint8Array(chunk);
  }
  if (ArrayBuffer.isView(chunk)) {
    return new Uint8Array(
      chunk.buffer.slice(chunk.byteOffset, chunk.byteOffset + chunk.byteLength),
    );
  }
  if (typeof chunk === "string") {
    return encodeUtf8(chunk);
  }
  return encodeUtf8(String(chunk));
}

async function readStreamBytes(stream) {
  const chunks = [];
  if (typeof stream?.getReader === "function") {
    const reader = stream.getReader();
    try {
      while (true) {
        const { value, done } = await reader.read();
        if (done) {
          break;
        }
        const bytes = toUint8Array(value);
        if (bytes) {
          chunks.push(bytes);
        }
      }
    } finally {
      if (typeof reader.releaseLock === "function") {
        try {
          reader.releaseLock();
        } catch {
          // Ignore release failures in the polyfill path.
        }
      }
    }
    return concatChunks(chunks);
  }

  if (typeof stream?.[Symbol.asyncIterator] === "function") {
    for await (const value of stream) {
      const bytes = toUint8Array(value);
      if (bytes) {
        chunks.push(bytes);
      }
    }
    return concatChunks(chunks);
  }

  const bytes = toUint8Array(stream);
  return bytes ?? new Uint8Array();
}

function concatChunks(chunks) {
  let total = 0;
  for (const chunk of chunks) {
    total += chunk.byteLength;
  }
  const merged = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    merged.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return merged;
}

function encodeUtf8(value) {
  if (textEncoder) {
    return textEncoder.encode(String(value));
  }

  const encoded = unescape(encodeURIComponent(String(value)));
  const out = new Uint8Array(encoded.length);
  for (let i = 0; i < encoded.length; i += 1) {
    out[i] = encoded.charCodeAt(i);
  }
  return out;
}

function decodeUtf8(bytes) {
  if (textDecoder) {
    return textDecoder.decode(bytes);
  }

  let encoded = "";
  for (const byte of bytes) {
    encoded += String.fromCharCode(byte);
  }
  return decodeURIComponent(escape(encoded));
}

function normalizeHeaderName(name) {
  return String(name).toLowerCase();
}

class Headers {
  constructor(init = []) {
    this._entries = new Map();

    if (init instanceof Headers || init?.[Symbol.iterator]) {
      for (const [name, value] of init) {
        this.append(name, value);
      }
    } else if (init && typeof init === "object") {
      for (const [name, value] of Object.entries(init)) {
        this.append(name, value);
      }
    }
  }

  append(name, value) {
    const normalized = normalizeHeaderName(name);
    const values = this._entries.get(normalized) ?? [];
    values.push(String(value));
    this._entries.set(normalized, values);
  }

  delete(name) {
    this._entries.delete(normalizeHeaderName(name));
  }

  entries() {
    return (function* (headers) {
      for (const [name, values] of headers._entries) {
        for (const value of values) {
          yield [name, value];
        }
      }
    })(this);
  }

  get(name) {
    const normalized = normalizeHeaderName(name);
    const values = this._entries.get(normalized);
    return values?.length ? values.join(", ") : null;
  }

  has(name) {
    return this._entries.has(normalizeHeaderName(name));
  }

  set(name, value) {
    this._entries.set(normalizeHeaderName(name), [String(value)]);
  }

  [Symbol.iterator]() {
    return this.entries();
  }
}

class Request {
  constructor(input, init = {}) {
    const source = input instanceof Request ? input : null;
    this.url = typeof input === "string" ? input : source?.url ?? String(input);
    this.method = init.method ?? source?.method ?? "GET";
    this.headers = new Headers(init.headers ?? source?.headers ?? []);
    this.signal = init.signal ?? source?.signal ?? null;
    this._body = normalizeBody(init.body ?? source?._body);
    this.bodyUsed = false;
  }

  async arrayBuffer() {
    this.bodyUsed = true;
    const body = this._body ?? new Uint8Array();
    return body.buffer.slice(body.byteOffset, body.byteOffset + body.byteLength);
  }

  async text() {
    const bytes = new Uint8Array(await this.arrayBuffer());
    return decodeUtf8(bytes);
  }

  async json() {
    return JSON.parse(await this.text());
  }

  clone() {
    return new Request(this.url, {
      method: this.method,
      headers: Array.from(this.headers.entries()),
      body: this._body ? new Uint8Array(this._body) : undefined,
      signal: this.signal,
    });
  }
}

class Response {
  constructor(body = null, init = {}) {
    this.status = init.status ?? 200;
    this.statusText = init.statusText ?? "";
    this.headers = new Headers(init.headers ?? []);
    this._body = normalizeResponseBody(body);
    this.ok = this.status >= 200 && this.status < 300;
    this.bodyUsed = false;
  }

  async arrayBuffer() {
    this.bodyUsed = true;
    if (isReadableStreamLike(this._body)) {
      const body = await readStreamBytes(this._body);
      return body.buffer.slice(body.byteOffset, body.byteOffset + body.byteLength);
    }

    const body = this._body ?? new Uint8Array();
    return body.buffer.slice(body.byteOffset, body.byteOffset + body.byteLength);
  }

  async text() {
    const bytes = new Uint8Array(await this.arrayBuffer());
    return decodeUtf8(bytes);
  }

  async json() {
    return JSON.parse(await this.text());
  }

  clone() {
    return new Response(this._body ? this._body : null, {
      status: this.status,
      statusText: this.statusText,
      headers: Array.from(this.headers.entries()),
    });
  }
}

class ReadableStreamController {
  constructor(stream) {
    this._stream = stream;
  }

  enqueue(chunk) {
    this._stream._enqueue(chunk);
  }

  close() {
    this._stream._close();
  }

  error(reason) {
    this._stream._error(reason);
  }
}

class ReadableStream {
  constructor(underlyingSource = {}) {
    this._queue = [];
    this._pendingReads = [];
    this._closed = false;
    this._errored = null;

    if (typeof underlyingSource.start === "function") {
      underlyingSource.start(new ReadableStreamController(this));
    }
  }

  _enqueue(chunk) {
    if (this._closed || this._errored) {
      return;
    }

    const pending = this._pendingReads.shift();
    if (pending) {
      pending.resolve({ value: chunk, done: false });
      return;
    }

    this._queue.push(chunk);
  }

  _close() {
    if (this._closed || this._errored) {
      return;
    }

    this._closed = true;
    while (this._pendingReads.length) {
      const pending = this._pendingReads.shift();
      pending.resolve({ value: undefined, done: true });
    }
  }

  _error(reason) {
    if (this._closed || this._errored) {
      return;
    }

    this._errored = reason ?? new Error("ReadableStream errored");
    while (this._pendingReads.length) {
      const pending = this._pendingReads.shift();
      pending.reject(this._errored);
    }
  }

  getReader() {
    let released = false;
    return {
      read: () => {
        if (released) {
          return Promise.reject(new TypeError("Reader has been released"));
        }

        if (this._queue.length) {
          return Promise.resolve({ value: this._queue.shift(), done: false });
        }

        if (this._errored) {
          return Promise.reject(this._errored);
        }

        if (this._closed) {
          return Promise.resolve({ value: undefined, done: true });
        }

        return new Promise((resolve, reject) => {
          this._pendingReads.push({ resolve, reject });
        });
      },
      releaseLock: () => {
        released = true;
      },
    };
  }

  [Symbol.asyncIterator]() {
    const reader = this.getReader();
    return {
      next: () => reader.read(),
      return: async () => {
        if (typeof reader.releaseLock === "function") {
          reader.releaseLock();
        }
        return { done: true, value: undefined };
      },
    };
  }
}

class URL {
  constructor(input) {
    const href = String(input);
    const match = href.match(/^[a-zA-Z][a-zA-Z0-9+.-]*:\/\/([^/]*)(.*)$/);
    const pathAndQuery = match ? (match[2] || "/") : href;
    const [pathname, search = ""] = pathAndQuery.split("?");
    this.href = href;
    this.pathname = pathname || "/";
    this.search = search ? "?" + search : "";
  }

  toString() {
    return this.href;
  }
}

ensureAbortGlobals();
ensureFrozenTimeGlobals();
define("Headers", Headers);
define("Request", Request);
define("Response", Response);
define("URL", URL);
if (globalThis.ReadableStream === undefined) {
  define("ReadableStream", ReadableStream);
}
define("__grugd_set_time", setFrozenTime);
