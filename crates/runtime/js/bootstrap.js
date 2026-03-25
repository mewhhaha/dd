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
  if (isReadableStreamLike(body)) {
    return body;
  }
  if (typeof globalThis.URLSearchParams === "function" && body instanceof globalThis.URLSearchParams) {
    return encodeUtf8(body.toString());
  }
  if (isDdBlob(body)) {
    return new Uint8Array(body.__dd_blob_bytes);
  }
  if (typeof globalThis.FormData === "function" && body instanceof globalThis.FormData) {
    throw new TypeError("FormData request bodies are not supported in v1");
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

function isDdBlob(value) {
  return (
    value !== null
    && typeof value === "object"
    && value.__dd_blob_bytes instanceof Uint8Array
  );
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

function isIntegerTypedArray(value) {
  return (
    value instanceof Int8Array
    || value instanceof Uint8Array
    || value instanceof Uint8ClampedArray
    || value instanceof Int16Array
    || value instanceof Uint16Array
    || value instanceof Int32Array
    || value instanceof Uint32Array
    || (typeof BigInt64Array !== "undefined" && value instanceof BigInt64Array)
    || (typeof BigUint64Array !== "undefined" && value instanceof BigUint64Array)
  );
}

function toBufferSourceBytes(value) {
  if (value instanceof ArrayBuffer) {
    return new Uint8Array(value);
  }
  if (ArrayBuffer.isView(value)) {
    return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
  }
  throw new TypeError("Expected ArrayBuffer or ArrayBufferView");
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
    if (isReadableStreamLike(this._body)) {
      throw new TypeError("Cannot clone a Request with a streaming body in v1");
    }
    return new Request(this.url, {
      method: this.method,
      headers: Array.from(this.headers.entries()),
      body: this._body ? new Uint8Array(this._body) : undefined,
      signal: this.signal,
    });
  }

  get body() {
    return isReadableStreamLike(this._body) ? this._body : null;
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
    if (isReadableStreamLike(this._body)) {
      throw new TypeError("Cannot clone a Response with a streaming body in v1");
    }
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

  async pipeTo(destination, options = {}) {
    const _ = options;
    if (!(destination instanceof WritableStream)) {
      throw new TypeError("ReadableStream.pipeTo expects a WritableStream destination");
    }
    const reader = this.getReader();
    const writer = destination.getWriter();
    try {
      while (true) {
        const { value, done } = await reader.read();
        if (done) {
          break;
        }
        await writer.write(value);
      }
      await writer.close();
    } catch (error) {
      await writer.abort(error);
      throw error;
    } finally {
      if (typeof reader.releaseLock === "function") {
        reader.releaseLock();
      }
      if (typeof writer.releaseLock === "function") {
        writer.releaseLock();
      }
    }
  }

  pipeThrough(transform, options = {}) {
    if (!transform || !(transform.readable instanceof ReadableStream)) {
      throw new TypeError("ReadableStream.pipeThrough expects a valid transform");
    }
    this.pipeTo(transform.writable, options).catch(() => {
      // The readable side will surface errors to consumers.
    });
    return transform.readable;
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

function normalizeBlobType(type) {
  if (type === undefined) {
    return "";
  }
  return String(type).trim().toLowerCase();
}

function normalizeBlobPart(part) {
  if (part == null) {
    return encodeUtf8("");
  }
  if (typeof part === "string") {
    return encodeUtf8(part);
  }
  if (part instanceof ArrayBuffer) {
    return new Uint8Array(part);
  }
  if (ArrayBuffer.isView(part)) {
    return new Uint8Array(part.buffer, part.byteOffset, part.byteLength);
  }
  if (isDdBlob(part)) {
    return new Uint8Array(part.__dd_blob_bytes);
  }
  return encodeUtf8(String(part));
}

class Blob {
  constructor(parts = [], options = {}) {
    const normalizedParts = Array.isArray(parts) ? parts : [parts];
    const bytes = [];
    for (const part of normalizedParts) {
      bytes.push(normalizeBlobPart(part));
    }
    const merged = concatChunks(bytes);
    Object.defineProperty(this, "__dd_blob_bytes", {
      value: merged,
      enumerable: false,
      configurable: false,
      writable: false,
    });
    this.size = merged.byteLength;
    this.type = normalizeBlobType(options?.type);
  }

  async arrayBuffer() {
    const bytes = this.__dd_blob_bytes;
    return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength);
  }

  async text() {
    return decodeUtf8(this.__dd_blob_bytes);
  }

  stream() {
    const bytes = this.__dd_blob_bytes;
    return new ReadableStream({
      start(controller) {
        controller.enqueue(new Uint8Array(bytes));
        controller.close();
      },
    });
  }

  slice(start = 0, end = this.size, contentType = "") {
    const size = this.size;
    const relativeStart = start < 0 ? Math.max(size + start, 0) : Math.min(start, size);
    const relativeEnd = end < 0 ? Math.max(size + end, 0) : Math.min(end, size);
    const span = Math.max(relativeEnd - relativeStart, 0);
    const chunk = this.__dd_blob_bytes.slice(relativeStart, relativeStart + span);
    return new Blob([chunk], { type: contentType });
  }

  get [Symbol.toStringTag]() {
    return "Blob";
  }
}

class File extends Blob {
  constructor(parts, name, options = {}) {
    super(parts, options);
    this.name = String(name ?? "");
    const lastModified = Number(options?.lastModified);
    this.lastModified = Number.isFinite(lastModified) ? Math.floor(lastModified) : Date.now();
  }

  get [Symbol.toStringTag]() {
    return "File";
  }
}

function normalizeFormDataValue(value, filename = undefined) {
  if (isDdBlob(value)) {
    if (filename === undefined) {
      return value;
    }
    return new File([value], filename, {
      type: value.type,
      lastModified: Date.now(),
    });
  }
  return String(value);
}

class FormData {
  constructor() {
    this._entries = [];
  }

  append(name, value, filename = undefined) {
    this._entries.push([String(name), normalizeFormDataValue(value, filename)]);
  }

  delete(name) {
    const normalized = String(name);
    this._entries = this._entries.filter(([key]) => key !== normalized);
  }

  entries() {
    return this._entries[Symbol.iterator]();
  }

  forEach(callback, thisArg = undefined) {
    if (typeof callback !== "function") {
      throw new TypeError("FormData.forEach callback must be a function");
    }
    for (const [name, value] of this._entries) {
      callback.call(thisArg, value, name, this);
    }
  }

  get(name) {
    const normalized = String(name);
    for (const [key, value] of this._entries) {
      if (key === normalized) {
        return value;
      }
    }
    return null;
  }

  getAll(name) {
    const normalized = String(name);
    const values = [];
    for (const [key, value] of this._entries) {
      if (key === normalized) {
        values.push(value);
      }
    }
    return values;
  }

  has(name) {
    const normalized = String(name);
    return this._entries.some(([key]) => key === normalized);
  }

  keys() {
    return (function* (entries) {
      for (const [name] of entries) {
        yield name;
      }
    })(this._entries);
  }

  set(name, value, filename = undefined) {
    const normalized = String(name);
    this.delete(normalized);
    this._entries.push([normalized, normalizeFormDataValue(value, filename)]);
  }

  values() {
    return (function* (entries) {
      for (const [, value] of entries) {
        yield value;
      }
    })(this._entries);
  }

  [Symbol.iterator]() {
    return this.entries();
  }
}

class WritableStream {
  constructor(underlyingSink = {}) {
    this._sink = underlyingSink;
    this._locked = false;
    this._closed = false;
  }

  getWriter() {
    if (this._locked) {
      throw new TypeError("WritableStream is already locked");
    }
    this._locked = true;
    let released = false;
    const stream = this;
    return {
      write: async (chunk) => {
        if (released) {
          throw new TypeError("Writer lock has been released");
        }
        if (stream._closed) {
          throw new TypeError("WritableStream is closed");
        }
        if (typeof stream._sink.write === "function") {
          await stream._sink.write(chunk);
        }
      },
      close: async () => {
        if (released) {
          throw new TypeError("Writer lock has been released");
        }
        if (stream._closed) {
          return;
        }
        stream._closed = true;
        if (typeof stream._sink.close === "function") {
          await stream._sink.close();
        }
      },
      abort: async (reason) => {
        if (released) {
          throw new TypeError("Writer lock has been released");
        }
        stream._closed = true;
        if (typeof stream._sink.abort === "function") {
          await stream._sink.abort(reason);
        }
      },
      releaseLock: () => {
        released = true;
        stream._locked = false;
      },
    };
  }
}

class TransformStreamDefaultController {
  constructor(readableController) {
    this._readableController = readableController;
  }

  enqueue(chunk) {
    this._readableController.enqueue(chunk);
  }

  error(reason) {
    this._readableController.error(reason);
  }

  terminate() {
    this._readableController.close();
  }
}

class TransformStream {
  constructor(transformer = {}) {
    let readableControllerRef = null;
    this.readable = new ReadableStream({
      start(controller) {
        readableControllerRef = controller;
      },
    });
    if (readableControllerRef == null) {
      throw new Error("failed to initialize TransformStream readable side");
    }

    const controller = new TransformStreamDefaultController(readableControllerRef);
    this.writable = new WritableStream({
      write: async (chunk) => {
        if (typeof transformer.transform === "function") {
          await transformer.transform(chunk, controller);
          return;
        }
        controller.enqueue(chunk);
      },
      close: async () => {
        if (typeof transformer.flush === "function") {
          await transformer.flush(controller);
        }
        controller.terminate();
      },
      abort: async (reason) => {
        if (typeof transformer.cancel === "function") {
          await transformer.cancel(reason);
        }
        controller.error(reason);
      },
    });

    if (typeof transformer.start === "function") {
      Promise.resolve().then(() => transformer.start(controller));
    }
  }
}

function decodeQueryComponent(value) {
  return decodeURIComponent(String(value).replace(/\+/g, " "));
}

function encodeQueryComponent(value) {
  return encodeURIComponent(String(value)).replace(/%20/g, "+");
}

class URLSearchParams {
  constructor(init = "", onUpdate = null) {
    this._pairs = [];
    this._onUpdate = typeof onUpdate === "function" ? onUpdate : null;
    if (init == null) {
      return;
    }

    if (typeof init === "string") {
      this._parseFromString(init);
      return;
    }

    if (init instanceof URLSearchParams) {
      this._pairs = init._pairs.map(([name, value]) => [name, value]);
      return;
    }

    if (typeof init?.[Symbol.iterator] === "function") {
      for (const entry of init) {
        if (!Array.isArray(entry) || entry.length !== 2) {
          throw new TypeError("URLSearchParams iterable entries must be [name, value]");
        }
        this._pairs.push([String(entry[0]), String(entry[1])]);
      }
      return;
    }

    if (typeof init === "object") {
      for (const [name, value] of Object.entries(init)) {
        this._pairs.push([String(name), String(value)]);
      }
      return;
    }

    throw new TypeError("Unsupported URLSearchParams initializer");
  }

  _parseFromString(value) {
    const source = value.startsWith("?") ? value.slice(1) : value;
    if (!source) {
      return;
    }
    for (const part of source.split("&")) {
      if (!part) {
        continue;
      }
      const eqIndex = part.indexOf("=");
      if (eqIndex === -1) {
        this._pairs.push([decodeQueryComponent(part), ""]);
        continue;
      }
      const name = part.slice(0, eqIndex);
      const rawValue = part.slice(eqIndex + 1);
      this._pairs.push([decodeQueryComponent(name), decodeQueryComponent(rawValue)]);
    }
  }

  _notifyUpdate() {
    if (this._onUpdate) {
      this._onUpdate(this.toString());
    }
  }

  append(name, value) {
    this._pairs.push([String(name), String(value)]);
    this._notifyUpdate();
  }

  delete(name, value = undefined) {
    const normalizedName = String(name);
    if (value === undefined) {
      this._pairs = this._pairs.filter(([key]) => key !== normalizedName);
      this._notifyUpdate();
      return;
    }

    const normalizedValue = String(value);
    this._pairs = this._pairs.filter(
      ([key, currentValue]) => key !== normalizedName || currentValue !== normalizedValue,
    );
    this._notifyUpdate();
  }

  entries() {
    return this._pairs[Symbol.iterator]();
  }

  forEach(callback, thisArg = undefined) {
    if (typeof callback !== "function") {
      throw new TypeError("URLSearchParams.forEach callback must be a function");
    }
    for (const [name, value] of this._pairs) {
      callback.call(thisArg, value, name, this);
    }
  }

  get(name) {
    const normalizedName = String(name);
    for (const [key, value] of this._pairs) {
      if (key === normalizedName) {
        return value;
      }
    }
    return null;
  }

  getAll(name) {
    const normalizedName = String(name);
    const values = [];
    for (const [key, value] of this._pairs) {
      if (key === normalizedName) {
        values.push(value);
      }
    }
    return values;
  }

  has(name, value = undefined) {
    const normalizedName = String(name);
    if (value === undefined) {
      return this._pairs.some(([key]) => key === normalizedName);
    }
    const normalizedValue = String(value);
    return this._pairs.some(([key, currentValue]) => {
      return key === normalizedName && currentValue === normalizedValue;
    });
  }

  keys() {
    return (function* (pairs) {
      for (const [name] of pairs) {
        yield name;
      }
    })(this._pairs);
  }

  set(name, value) {
    const normalizedName = String(name);
    const normalizedValue = String(value);
    let replaced = false;
    const nextPairs = [];
    for (const [key, currentValue] of this._pairs) {
      if (key !== normalizedName) {
        nextPairs.push([key, currentValue]);
        continue;
      }
      if (!replaced) {
        nextPairs.push([normalizedName, normalizedValue]);
        replaced = true;
      }
    }
    if (!replaced) {
      nextPairs.push([normalizedName, normalizedValue]);
    }
    this._pairs = nextPairs;
    this._notifyUpdate();
  }

  sort() {
    this._pairs.sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
    this._notifyUpdate();
  }

  toString() {
    return this._pairs
      .map(([name, value]) => `${encodeQueryComponent(name)}=${encodeQueryComponent(value)}`)
      .join("&");
  }

  values() {
    return (function* (pairs) {
      for (const [, value] of pairs) {
        yield value;
      }
    })(this._pairs);
  }

  [Symbol.iterator]() {
    return this.entries();
  }
}

class URL {
  constructor(input) {
    const href = String(input);
    const match = href.match(/^[a-zA-Z][a-zA-Z0-9+.-]*:\/\/([^/]*)(.*)$/);
    const pathAndQuery = match ? (match[2] || "/") : href;
    const queryIndex = pathAndQuery.indexOf("?");
    const pathname = queryIndex === -1 ? pathAndQuery : pathAndQuery.slice(0, queryIndex);
    const search = queryIndex === -1 ? "" : pathAndQuery.slice(queryIndex);
    const hrefWithoutQuery = queryIndex === -1 ? href : href.slice(0, href.indexOf("?"));

    this.href = href;
    this.pathname = pathname || "/";
    this.search = search;
    this.searchParams = new URLSearchParams(search, (nextQuery) => {
      this.search = nextQuery.length ? `?${nextQuery}` : "";
      this.href = `${hrefWithoutQuery}${this.search}`;
    });
  }

  toString() {
    return this.href;
  }
}

function escapeRegExp(value) {
  return String(value).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function compilePathnamePattern(pattern) {
  const source = String(pattern || "*");
  let regex = "^";
  for (let i = 0; i < source.length; i += 1) {
    const char = source[i];
    if (char === "*") {
      regex += "(.*)";
      continue;
    }
    if (char === ":") {
      let j = i + 1;
      while (j < source.length && /[A-Za-z0-9_]/.test(source[j])) {
        j += 1;
      }
      const name = source.slice(i + 1, j);
      if (!name) {
        regex += ":";
      } else {
        regex += `(?<${name}>[^/]+)`;
        i = j - 1;
      }
      continue;
    }
    regex += escapeRegExp(char);
  }
  regex += "$";
  return new RegExp(regex);
}

function extractUrlPathname(input) {
  if (input instanceof URL) {
    return input.pathname;
  }
  if (input instanceof Request) {
    return new URL(input.url).pathname;
  }
  if (input && typeof input === "object" && typeof input.pathname === "string") {
    return String(input.pathname);
  }
  return new URL(String(input)).pathname;
}

class URLPattern {
  constructor(input = {}, baseURL = undefined) {
    const _ = baseURL;
    if (typeof input === "string") {
      this.pathname = String(input);
    } else if (input && typeof input === "object") {
      this.pathname = String(input.pathname ?? "*");
    } else {
      throw new TypeError("URLPattern input must be a string or object");
    }
    this._pathnameRegex = compilePathnamePattern(this.pathname);
  }

  test(input = "") {
    const pathname = extractUrlPathname(input);
    return this._pathnameRegex.test(pathname);
  }

  exec(input = "") {
    const pathname = extractUrlPathname(input);
    const match = this._pathnameRegex.exec(pathname);
    if (!match) {
      return null;
    }
    return {
      inputs: [String(input)],
      pathname: {
        input: pathname,
        groups: match.groups ?? {},
      },
    };
  }
}

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
  if (globalThis.structuredClone !== undefined) {
    return;
  }
  define("structuredClone", (value, options = undefined) => {
    if (
      options &&
      typeof options === "object" &&
      Array.isArray(options.transfer) &&
      options.transfer.length > 0
    ) {
      throw new TypeError("structuredClone transfer list is not supported");
    }
    if (
      typeof Deno?.core?.serialize !== "function"
      || typeof Deno?.core?.deserialize !== "function"
    ) {
      throw new Error("structuredClone is unavailable in this runtime");
    }
    const serialized = Deno.core.serialize(value, { forStorage: true });
    return Deno.core.deserialize(serialized);
  });
}

function normalizeDigestAlgorithmName(value) {
  const normalized = String(value ?? "")
    .trim()
    .toUpperCase()
    .replace(/\s+/g, "");
  switch (normalized) {
    case "SHA-1":
    case "SHA1":
      return "SHA-1";
    case "SHA-256":
    case "SHA256":
      return "SHA-256";
    case "SHA-384":
    case "SHA384":
      return "SHA-384";
    case "SHA-512":
    case "SHA512":
      return "SHA-512";
    default:
      return null;
  }
}

function normalizeAlgorithmName(value) {
  return String(value ?? "")
    .trim()
    .toUpperCase();
}

function ensureCryptoKeyGlobal() {
  if (typeof globalThis.CryptoKey === "function") {
    return globalThis.CryptoKey;
  }

  class CryptoKey {
    constructor(type, extractable, algorithm, usages, material) {
      this.type = String(type);
      this.extractable = Boolean(extractable);
      this.algorithm = algorithm;
      this.usages = Object.freeze(Array.from(usages));
      Object.defineProperty(this, "__dd_crypto_key_material", {
        value: new Uint8Array(material),
        enumerable: false,
        configurable: false,
        writable: false,
      });
    }
  }

  define("CryptoKey", CryptoKey);
  return CryptoKey;
}

function extractCryptoKeyMaterial(key) {
  if (!(key instanceof globalThis.CryptoKey)) {
    throw new TypeError("Expected a CryptoKey");
  }
  if (!(key.__dd_crypto_key_material instanceof Uint8Array)) {
    throw new TypeError("CryptoKey material is unavailable");
  }
  return key.__dd_crypto_key_material;
}

function normalizeKeyUsages(usages, allowed) {
  if (!Array.isArray(usages)) {
    throw new TypeError("keyUsages must be an array");
  }
  const allowedSet = new Set(allowed.map((value) => String(value)));
  const normalized = [];
  for (const usage of usages) {
    const keyUsage = String(usage);
    if (!allowedSet.has(keyUsage)) {
      throw new TypeError(`Unsupported key usage: ${keyUsage}`);
    }
    if (!normalized.includes(keyUsage)) {
      normalized.push(keyUsage);
    }
  }
  return normalized;
}

function parseHmacImportAlgorithm(algorithm, keyBytes) {
  const hashInput =
    typeof algorithm?.hash === "string" ? algorithm.hash : algorithm?.hash?.name;
  const hashName = normalizeDigestAlgorithmName(hashInput);
  if (!hashName) {
    throw new TypeError("HMAC import requires a supported hash algorithm");
  }
  const providedLength = algorithm?.length;
  const length = providedLength == null ? keyBytes.byteLength * 8 : Number(providedLength);
  if (!Number.isFinite(length) || length <= 0 || !Number.isInteger(length)) {
    throw new TypeError("HMAC key length must be a positive integer (bits)");
  }
  return {
    name: "HMAC",
    hash: { name: hashName },
    length,
  };
}

function parseAesGcmImportAlgorithm(algorithm, keyBytes) {
  const keyLengthBits = keyBytes.byteLength * 8;
  if (![128, 192, 256].includes(keyLengthBits)) {
    throw new TypeError("AES-GCM key length must be 128, 192, or 256 bits");
  }
  const providedLength = algorithm?.length;
  if (providedLength != null && Number(providedLength) !== keyLengthBits) {
    throw new TypeError("AES-GCM key length does not match provided algorithm.length");
  }
  return {
    name: "AES-GCM",
    length: keyLengthBits,
  };
}

function requireCryptoKeyUsage(key, usage) {
  if (!Array.isArray(key?.usages) || !key.usages.includes(usage)) {
    throw new TypeError(`CryptoKey does not allow usage: ${usage}`);
  }
}

function parseAesGcmParams(algorithm) {
  if (!algorithm || typeof algorithm !== "object") {
    throw new TypeError("AES-GCM requires an algorithm object");
  }
  const iv = toBufferSourceBytes(algorithm.iv);
  if (iv.byteLength !== 12) {
    throw new TypeError("AES-GCM iv must be exactly 12 bytes in v1");
  }
  const additionalData =
    algorithm.additionalData == null
      ? new Uint8Array()
      : toBufferSourceBytes(algorithm.additionalData);
  const tagLength = algorithm.tagLength == null ? 128 : Number(algorithm.tagLength);
  if (tagLength !== 128) {
    throw new TypeError("AES-GCM tagLength must be 128 in v1");
  }
  return {
    iv: Array.from(iv),
    additionalData: Array.from(additionalData),
    tagLength,
  };
}

function ensureCryptoGlobals() {
  const CryptoKeyCtor = ensureCryptoKeyGlobal();
  const cryptoObject =
    globalThis.crypto && typeof globalThis.crypto === "object" ? globalThis.crypto : {};
  const subtle =
    cryptoObject.subtle && typeof cryptoObject.subtle === "object" ? cryptoObject.subtle : {};

  if (typeof cryptoObject.getRandomValues !== "function") {
    cryptoObject.getRandomValues = (typedArray) => {
      if (
        !ArrayBuffer.isView(typedArray)
        || typedArray instanceof DataView
        || !isIntegerTypedArray(typedArray)
      ) {
        throw new TypeError("crypto.getRandomValues expects an integer TypedArray");
      }
      if (typedArray.byteLength > 65_536) {
        throw new Error("crypto.getRandomValues() length exceeds 65536 bytes");
      }

      const result = runtimeOp("op_crypto_get_random_values", typedArray.byteLength);
      if (!(result && result.ok === true && Array.isArray(result.bytes))) {
        throw new Error(String(result?.error ?? "crypto.getRandomValues failed"));
      }

      const bytes = Uint8Array.from(result.bytes);
      const view = new Uint8Array(typedArray.buffer, typedArray.byteOffset, typedArray.byteLength);
      view.set(bytes.subarray(0, view.byteLength));
      return typedArray;
    };
  }

  if (typeof cryptoObject.randomUUID !== "function") {
    cryptoObject.randomUUID = () => {
      const value = runtimeOp("op_crypto_random_uuid");
      if (typeof value !== "string" || value.length === 0) {
        throw new Error("crypto.randomUUID failed");
      }
      return value;
    };
  }

  if (typeof subtle.digest !== "function") {
    subtle.digest = async (algorithm, data) => {
      const name =
        typeof algorithm === "string"
          ? algorithm
          : typeof algorithm?.name === "string"
            ? algorithm.name
            : null;
      if (!name) {
        throw new TypeError("subtle.digest requires a valid algorithm name");
      }
      const normalizedName = normalizeDigestAlgorithmName(name);
      if (!normalizedName) {
        throw new TypeError(`Unsupported digest algorithm: ${name}`);
      }

      const bytes = toBufferSourceBytes(data);
      const result = runtimeOp(
        "op_crypto_digest",
        JSON.stringify({
          algorithm: normalizedName,
          data: Array.from(bytes),
        }),
      );
      await syncFrozenTimeBoundary();
      if (!(result && result.ok === true && Array.isArray(result.digest))) {
        throw new Error(String(result?.error ?? "crypto.subtle.digest failed"));
      }

      const digest = Uint8Array.from(result.digest);
      return digest.buffer.slice(digest.byteOffset, digest.byteOffset + digest.byteLength);
    };
  }

  if (typeof subtle.importKey !== "function") {
    subtle.importKey = async (
      format,
      keyData,
      algorithm,
      extractable = false,
      keyUsages = [],
    ) => {
      const normalizedFormat = String(format ?? "").toLowerCase();
      if (normalizedFormat !== "raw") {
        throw new TypeError("Only raw key import is supported in v1");
      }

      const keyBytes = new Uint8Array(toBufferSourceBytes(keyData));
      const algorithmName = normalizeAlgorithmName(algorithm?.name ?? algorithm);
      if (!algorithmName) {
        throw new TypeError("Algorithm name is required");
      }

      if (algorithmName === "HMAC") {
        const normalizedAlgorithm = parseHmacImportAlgorithm(algorithm, keyBytes);
        const usages = normalizeKeyUsages(keyUsages, ["sign", "verify"]);
        return new CryptoKeyCtor(
          "secret",
          extractable,
          normalizedAlgorithm,
          usages,
          keyBytes,
        );
      }

      if (algorithmName === "AES-GCM") {
        const normalizedAlgorithm = parseAesGcmImportAlgorithm(algorithm, keyBytes);
        const usages = normalizeKeyUsages(keyUsages, ["encrypt", "decrypt"]);
        return new CryptoKeyCtor(
          "secret",
          extractable,
          normalizedAlgorithm,
          usages,
          keyBytes,
        );
      }

      throw new TypeError(`Unsupported importKey algorithm: ${algorithmName}`);
    };
  }

  if (typeof subtle.exportKey !== "function") {
    subtle.exportKey = async (format, key) => {
      const normalizedFormat = String(format ?? "").toLowerCase();
      if (normalizedFormat !== "raw") {
        throw new TypeError("Only raw key export is supported in v1");
      }
      if (!(key instanceof CryptoKeyCtor)) {
        throw new TypeError("exportKey expects a CryptoKey");
      }
      if (!key.extractable) {
        throw new TypeError("CryptoKey is not extractable");
      }
      const material = extractCryptoKeyMaterial(key);
      return material.buffer.slice(material.byteOffset, material.byteOffset + material.byteLength);
    };
  }

  if (typeof subtle.sign !== "function") {
    subtle.sign = async (algorithm, key, data) => {
      const algorithmName = normalizeAlgorithmName(algorithm?.name ?? algorithm);
      if (algorithmName !== "HMAC") {
        throw new TypeError("Only HMAC sign is supported in v1");
      }
      if (!(key instanceof CryptoKeyCtor) || normalizeAlgorithmName(key.algorithm?.name) !== "HMAC") {
        throw new TypeError("sign expects an HMAC CryptoKey");
      }
      requireCryptoKeyUsage(key, "sign");

      const keyHash = normalizeDigestAlgorithmName(key.algorithm?.hash?.name);
      const requestedHash = algorithm?.hash
        ? normalizeDigestAlgorithmName(algorithm.hash?.name ?? algorithm.hash)
        : keyHash;
      if (!requestedHash || requestedHash !== keyHash) {
        throw new TypeError("HMAC sign hash must match imported key hash");
      }

      const keyMaterial = extractCryptoKeyMaterial(key);
      const input = toBufferSourceBytes(data);
      const result = runtimeOp(
        "op_crypto_hmac_sign",
        JSON.stringify({
          hash: requestedHash,
          key: Array.from(keyMaterial),
          data: Array.from(input),
        }),
      );
      await syncFrozenTimeBoundary();
      if (!(result && result.ok === true && Array.isArray(result.bytes))) {
        throw new Error(String(result?.error ?? "crypto.subtle.sign failed"));
      }
      const signature = Uint8Array.from(result.bytes);
      return signature.buffer.slice(signature.byteOffset, signature.byteOffset + signature.byteLength);
    };
  }

  if (typeof subtle.verify !== "function") {
    subtle.verify = async (algorithm, key, signature, data) => {
      const algorithmName = normalizeAlgorithmName(algorithm?.name ?? algorithm);
      if (algorithmName !== "HMAC") {
        throw new TypeError("Only HMAC verify is supported in v1");
      }
      if (!(key instanceof CryptoKeyCtor) || normalizeAlgorithmName(key.algorithm?.name) !== "HMAC") {
        throw new TypeError("verify expects an HMAC CryptoKey");
      }
      requireCryptoKeyUsage(key, "verify");

      const keyHash = normalizeDigestAlgorithmName(key.algorithm?.hash?.name);
      const requestedHash = algorithm?.hash
        ? normalizeDigestAlgorithmName(algorithm.hash?.name ?? algorithm.hash)
        : keyHash;
      if (!requestedHash || requestedHash !== keyHash) {
        throw new TypeError("HMAC verify hash must match imported key hash");
      }

      const keyMaterial = extractCryptoKeyMaterial(key);
      const signatureBytes = toBufferSourceBytes(signature);
      const input = toBufferSourceBytes(data);
      const result = runtimeOp(
        "op_crypto_hmac_verify",
        JSON.stringify({
          hash: requestedHash,
          key: Array.from(keyMaterial),
          data: Array.from(input),
          signature: Array.from(signatureBytes),
        }),
      );
      await syncFrozenTimeBoundary();
      if (!(result && result.ok === true && typeof result.value === "boolean")) {
        throw new Error(String(result?.error ?? "crypto.subtle.verify failed"));
      }
      return result.value;
    };
  }

  if (typeof subtle.encrypt !== "function") {
    subtle.encrypt = async (algorithm, key, data) => {
      const algorithmName = normalizeAlgorithmName(algorithm?.name ?? algorithm);
      if (algorithmName !== "AES-GCM") {
        throw new TypeError("Only AES-GCM encrypt is supported in v1");
      }
      if (
        !(key instanceof CryptoKeyCtor)
        || normalizeAlgorithmName(key.algorithm?.name) !== "AES-GCM"
      ) {
        throw new TypeError("encrypt expects an AES-GCM CryptoKey");
      }
      requireCryptoKeyUsage(key, "encrypt");
      const params = parseAesGcmParams(algorithm);
      const keyMaterial = extractCryptoKeyMaterial(key);
      const input = toBufferSourceBytes(data);
      const result = runtimeOp(
        "op_crypto_aes_gcm_encrypt",
        JSON.stringify({
          key: Array.from(keyMaterial),
          iv: params.iv,
          data: Array.from(input),
          additional_data: params.additionalData,
          tag_length: params.tagLength,
        }),
      );
      await syncFrozenTimeBoundary();
      if (!(result && result.ok === true && Array.isArray(result.bytes))) {
        throw new Error(String(result?.error ?? "crypto.subtle.encrypt failed"));
      }
      const ciphertext = Uint8Array.from(result.bytes);
      return ciphertext.buffer.slice(ciphertext.byteOffset, ciphertext.byteOffset + ciphertext.byteLength);
    };
  }

  if (typeof subtle.decrypt !== "function") {
    subtle.decrypt = async (algorithm, key, data) => {
      const algorithmName = normalizeAlgorithmName(algorithm?.name ?? algorithm);
      if (algorithmName !== "AES-GCM") {
        throw new TypeError("Only AES-GCM decrypt is supported in v1");
      }
      if (
        !(key instanceof CryptoKeyCtor)
        || normalizeAlgorithmName(key.algorithm?.name) !== "AES-GCM"
      ) {
        throw new TypeError("decrypt expects an AES-GCM CryptoKey");
      }
      requireCryptoKeyUsage(key, "decrypt");
      const params = parseAesGcmParams(algorithm);
      const keyMaterial = extractCryptoKeyMaterial(key);
      const input = toBufferSourceBytes(data);
      const result = runtimeOp(
        "op_crypto_aes_gcm_decrypt",
        JSON.stringify({
          key: Array.from(keyMaterial),
          iv: params.iv,
          data: Array.from(input),
          additional_data: params.additionalData,
          tag_length: params.tagLength,
        }),
      );
      await syncFrozenTimeBoundary();
      if (!(result && result.ok === true && Array.isArray(result.bytes))) {
        throw new Error(String(result?.error ?? "crypto.subtle.decrypt failed"));
      }
      const plaintext = Uint8Array.from(result.bytes);
      return plaintext.buffer.slice(plaintext.byteOffset, plaintext.byteOffset + plaintext.byteLength);
    };
  }

  cryptoObject.subtle = subtle;
  define("crypto", cryptoObject);
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
    const normalizedRequest = request instanceof Request ? request : new Request(request);
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

    return new Response(new Uint8Array(result.body ?? []), {
      status: Number(result.status ?? 200),
      headers: Array.isArray(result.headers) ? result.headers : [],
    });
  }

  async put(request, response) {
    const normalizedRequest = request instanceof Request ? request : new Request(request);
    if (!(response instanceof Response)) {
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
    const normalizedRequest = request instanceof Request ? request : new Request(request);
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
define("Headers", Headers);
define("Request", Request);
define("Response", Response);
define("URL", URL);
define("URLSearchParams", URLSearchParams);
define("URLPattern", URLPattern);
define("Blob", Blob);
define("File", File);
define("FormData", FormData);
define("WritableStream", WritableStream);
define("TransformStream", TransformStream);
define("Cache", Cache);
define("CacheStorage", CacheStorage);
if (globalThis.ReadableStream === undefined) {
  define("ReadableStream", ReadableStream);
}
define("caches", new CacheStorage());
define("__dd_set_time", setFrozenTime);
