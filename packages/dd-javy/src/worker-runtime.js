const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();
let invocationRandomBytes = new Uint8Array();
let invocationRandomOffset = 0;

class RuntimeEventTarget {
  #listeners = new Map();

  addEventListener(type, listener) {
    if (typeof listener !== "function") {
      return;
    }
    const listeners = this.#listeners.get(type) ?? new Set();
    listeners.add(listener);
    this.#listeners.set(type, listeners);
  }

  removeEventListener(type, listener) {
    this.#listeners.get(type)?.delete(listener);
  }

  dispatchEvent(event) {
    event.target = this;
    for (const listener of this.#listeners.get(event.type) ?? []) {
      listener.call(this, event);
    }
    const propertyListener = this[`on${event.type}`];
    if (typeof propertyListener === "function") {
      propertyListener.call(this, event);
    }
    return !event.defaultPrevented;
  }
}

class RuntimeAbortSignal extends RuntimeEventTarget {
  aborted = false;
  reason;
  onabort = null;

  throwIfAborted() {
    if (this.aborted) {
      throw this.reason;
    }
  }

  static abort(reason = new Error("The operation was aborted")) {
    const signal = new RuntimeAbortSignal();
    signal.aborted = true;
    signal.reason = reason;
    return signal;
  }
}

class RuntimeAbortController {
  signal = new RuntimeAbortSignal();

  abort(reason = new Error("The operation was aborted")) {
    if (this.signal.aborted) {
      return;
    }
    this.signal.aborted = true;
    this.signal.reason = reason;
    this.signal.dispatchEvent({ type: "abort", defaultPrevented: false });
  }
}

class RuntimeHeaders {
  #values = [];

  constructor(init = undefined) {
    if (init instanceof RuntimeHeaders) {
      this.#values = init.#values.map(([name, value]) => [name, value]);
      return;
    }
    if (Array.isArray(init) || init?.[Symbol.iterator]) {
      for (const [name, value] of init) {
        this.append(name, value);
      }
      return;
    }
    if (init && typeof init === "object") {
      for (const [name, value] of Object.entries(init)) {
        if (Array.isArray(value)) {
          for (const part of value) {
            this.append(name, part);
          }
        } else {
          this.append(name, value);
        }
      }
    }
  }

  append(name, value) {
    this.#values.push([headerName(name), headerValue(value)]);
  }

  delete(name) {
    const normalizedName = headerName(name);
    this.#values = this.#values.filter(([candidate]) => candidate !== normalizedName);
  }

  get(name) {
    const normalizedName = headerName(name);
    const values = this.#values
      .filter(([candidate]) => candidate === normalizedName)
      .map(([, value]) => value);
    return values.length === 0 ? null : values.join(", ");
  }

  getSetCookie() {
    return this.#values
      .filter(([name]) => name === "set-cookie")
      .map(([, value]) => value);
  }

  has(name) {
    const normalizedName = headerName(name);
    return this.#values.some(([candidate]) => candidate === normalizedName);
  }

  set(name, value) {
    const normalizedName = headerName(name);
    this.delete(normalizedName);
    this.#values.push([normalizedName, headerValue(value)]);
  }

  *entries() {
    const emitted = new Set();
    for (const [name] of this.#values) {
      if (emitted.has(name)) {
        continue;
      }
      emitted.add(name);
      if (name === "set-cookie") {
        for (const value of this.getSetCookie()) {
          yield [name, value];
        }
      } else {
        yield [name, this.get(name)];
      }
    }
  }

  forEach(callback, thisArg = undefined) {
    for (const [name, value] of this.entries()) {
      callback.call(thisArg, value, name, this);
    }
  }

  *keys() {
    for (const [name] of this.entries()) {
      yield name;
    }
  }

  *values() {
    for (const [, value] of this.entries()) {
      yield value;
    }
  }

  [Symbol.iterator]() {
    return this.entries();
  }
}

function headerName(value) {
  const name = String(value).trim().toLowerCase();
  if (!name || !/^[!#$%&'*+\-.^_`|~0-9a-z]+$/.test(name)) {
    throw new TypeError(`invalid header name ${JSON.stringify(value)}`);
  }
  return name;
}

function headerValue(value) {
  const normalized = String(value).trim();
  if (/[\0\r\n]/.test(normalized)) {
    throw new TypeError("header value contains a forbidden character");
  }
  return normalized;
}

class RuntimeURLSearchParams {
  #pairs = [];
  #onChange;

  constructor(init = "", onChange = undefined) {
    this.#onChange = onChange;
    if (init instanceof RuntimeURLSearchParams) {
      this.#pairs = [...init.#pairs];
      return;
    }
    if (typeof init === "string") {
      const source = init.startsWith("?") ? init.slice(1) : init;
      if (!source) {
        return;
      }
      for (const field of source.split("&")) {
        const [name, ...rest] = field.split("=");
        this.#pairs.push([
          decodeQueryField(name),
          decodeQueryField(rest.join("=")),
        ]);
      }
      return;
    }
    if (Array.isArray(init) || init?.[Symbol.iterator]) {
      for (const [name, value] of init) {
        this.#pairs.push([String(name), String(value)]);
      }
      return;
    }
    if (init && typeof init === "object") {
      this.#pairs = Object.entries(init).map(([name, value]) => [name, String(value)]);
    }
  }

  append(name, value) {
    this.#pairs.push([String(name), String(value)]);
    this.#changed();
  }

  delete(name, value = undefined) {
    const normalizedName = String(name);
    this.#pairs = this.#pairs.filter(
      ([candidateName, candidateValue]) =>
        candidateName !== normalizedName ||
        (value !== undefined && candidateValue !== String(value)),
    );
    this.#changed();
  }

  get(name) {
    const pair = this.#pairs.find(([candidate]) => candidate === String(name));
    return pair?.[1] ?? null;
  }

  getAll(name) {
    return this.#pairs
      .filter(([candidate]) => candidate === String(name))
      .map(([, value]) => value);
  }

  has(name, value = undefined) {
    return this.#pairs.some(
      ([candidateName, candidateValue]) =>
        candidateName === String(name) &&
        (value === undefined || candidateValue === String(value)),
    );
  }

  set(name, value) {
    const normalizedName = String(name);
    const normalizedValue = String(value);
    const firstIndex = this.#pairs.findIndex(([candidate]) => candidate === normalizedName);
    this.#pairs = this.#pairs.filter(([candidate]) => candidate !== normalizedName);
    this.#pairs.splice(firstIndex < 0 ? this.#pairs.length : firstIndex, 0, [
      normalizedName,
      normalizedValue,
    ]);
    this.#changed();
  }

  sort() {
    this.#pairs.sort(([left], [right]) => left.localeCompare(right));
    this.#changed();
  }

  entries() {
    return this.#pairs[Symbol.iterator]();
  }

  keys() {
    return this.#pairs.map(([name]) => name)[Symbol.iterator]();
  }

  values() {
    return this.#pairs.map(([, value]) => value)[Symbol.iterator]();
  }

  forEach(callback, thisArg = undefined) {
    for (const [name, value] of this.#pairs) {
      callback.call(thisArg, value, name, this);
    }
  }

  toString() {
    return this.#pairs
      .map(([name, value]) => `${encodeQueryField(name)}=${encodeQueryField(value)}`)
      .join("&");
  }

  [Symbol.iterator]() {
    return this.entries();
  }

  #changed() {
    this.#onChange?.(this.toString());
  }
}

class RuntimeFormData {
  #entries = [];

  append(name, value) {
    this.#entries.push([String(name), String(value)]);
  }

  delete(name) {
    const normalizedName = String(name);
    this.#entries = this.#entries.filter(([candidate]) => candidate !== normalizedName);
  }

  get(name) {
    return this.#entries.find(([candidate]) => candidate === String(name))?.[1] ?? null;
  }

  getAll(name) {
    return this.#entries
      .filter(([candidate]) => candidate === String(name))
      .map(([, value]) => value);
  }

  has(name) {
    return this.#entries.some(([candidate]) => candidate === String(name));
  }

  set(name, value) {
    this.delete(name);
    this.append(name, value);
  }

  entries() {
    return this.#entries[Symbol.iterator]();
  }

  keys() {
    return this.#entries.map(([name]) => name)[Symbol.iterator]();
  }

  values() {
    return this.#entries.map(([, value]) => value)[Symbol.iterator]();
  }

  forEach(callback, thisArg = undefined) {
    for (const [name, value] of this.#entries) {
      callback.call(thisArg, value, name, this);
    }
  }

  [Symbol.iterator]() {
    return this.entries();
  }
}

function decodeQueryField(value) {
  return decodeURIComponent(String(value).replace(/\+/g, " "));
}

function encodeQueryField(value) {
  return encodeURIComponent(value).replace(/%20/g, "+");
}

class RuntimeURL {
  #protocol;
  #hostname;
  #port;
  #pathname;
  #search;
  #hash;
  #searchParams;

  constructor(input, base = undefined) {
    const parsed = parseURL(String(input), base === undefined ? undefined : String(base));
    this.#protocol = parsed.protocol;
    this.#hostname = parsed.hostname;
    this.#port = parsed.port;
    this.#pathname = parsed.pathname;
    this.#search = parsed.search;
    this.#hash = parsed.hash;
    this.#searchParams = new RuntimeURLSearchParams(this.#search, (search) => {
      this.#search = search ? `?${search}` : "";
    });
  }

  get hash() {
    return this.#hash;
  }

  set hash(value) {
    const hash = String(value);
    this.#hash = !hash ? "" : hash.startsWith("#") ? hash : `#${hash}`;
  }

  get host() {
    return `${this.#hostname}${this.#port ? `:${this.#port}` : ""}`;
  }

  set host(value) {
    const match = String(value).match(/^(\[[^\]]+\]|[^:]+)(?::(\d+))?$/);
    if (!match) {
      throw new TypeError(`invalid URL host ${JSON.stringify(value)}`);
    }
    this.#hostname = match[1];
    this.#port = match[2] ?? "";
  }

  get hostname() {
    return this.#hostname;
  }

  set hostname(value) {
    this.#hostname = String(value).toLowerCase();
  }

  get href() {
    return `${this.origin}${this.#pathname}${this.#search}${this.#hash}`;
  }

  set href(value) {
    const next = new RuntimeURL(value);
    this.#protocol = next.protocol;
    this.#hostname = next.hostname;
    this.#port = next.port;
    this.#pathname = next.pathname;
    this.search = next.search;
    this.#hash = next.hash;
  }

  get origin() {
    return `${this.#protocol}//${this.host}`;
  }

  get pathname() {
    return this.#pathname;
  }

  set pathname(value) {
    const pathname = String(value);
    this.#pathname = normalizePathname(pathname.startsWith("/") ? pathname : `/${pathname}`);
  }

  get port() {
    return this.#port;
  }

  set port(value) {
    const port = String(value);
    if (port && !/^\d+$/.test(port)) {
      throw new TypeError(`invalid URL port ${JSON.stringify(value)}`);
    }
    this.#port = port;
  }

  get protocol() {
    return this.#protocol;
  }

  set protocol(value) {
    const protocol = String(value).toLowerCase();
    this.#protocol = protocol.endsWith(":") ? protocol : `${protocol}:`;
  }

  get search() {
    return this.#search;
  }

  set search(value) {
    const search = String(value);
    this.#search = !search ? "" : search.startsWith("?") ? search : `?${search}`;
    this.#searchParams = new RuntimeURLSearchParams(this.#search, (nextSearch) => {
      this.#search = nextSearch ? `?${nextSearch}` : "";
    });
  }

  get searchParams() {
    return this.#searchParams;
  }

  toJSON() {
    return this.href;
  }

  toString() {
    return this.href;
  }

  static canParse(input, base = undefined) {
    try {
      new RuntimeURL(input, base);
      return true;
    } catch {
      return false;
    }
  }
}

function parseURL(input, base) {
  const absolute = input.match(
    /^([a-zA-Z][a-zA-Z\d+.-]*:)[/][/](\[[^\]]+\]|[^/:?#]+)(?::(\d+))?([^?#]*)(\?[^#]*)?(#.*)?$/,
  );
  if (absolute) {
    return {
      protocol: absolute[1].toLowerCase(),
      hostname: absolute[2].toLowerCase(),
      port: absolute[3] ?? "",
      pathname: normalizePathname(absolute[4] || "/"),
      search: absolute[5] ?? "",
      hash: absolute[6] ?? "",
    };
  }
  if (base === undefined) {
    throw new TypeError(`invalid absolute URL ${JSON.stringify(input)}`);
  }
  const baseURL = new RuntimeURL(base);
  if (input.startsWith("//")) {
    return parseURL(`${baseURL.protocol}${input}`);
  }
  const [withoutHash, hash = ""] = input.split("#", 2);
  const [path, search = ""] = withoutHash.split("?", 2);
  let pathname;
  if (!path) {
    pathname = baseURL.pathname;
  } else if (path.startsWith("/")) {
    pathname = normalizePathname(path);
  } else {
    const parent = baseURL.pathname.slice(0, baseURL.pathname.lastIndexOf("/") + 1);
    pathname = normalizePathname(`${parent}${path}`);
  }
  return {
    protocol: baseURL.protocol,
    hostname: baseURL.hostname,
    port: baseURL.port,
    pathname,
    search: search ? `?${search}` : !path ? baseURL.search : "",
    hash: hash ? `#${hash}` : "",
  };
}

function normalizePathname(pathname) {
  const segments = [];
  for (const segment of pathname.split("/")) {
    if (!segment || segment === ".") {
      continue;
    }
    if (segment === "..") {
      segments.pop();
    } else {
      segments.push(segment);
    }
  }
  const suffix = pathname.endsWith("/") && segments.length > 0 ? "/" : "";
  return `/${segments.join("/")}${suffix}`;
}

class RuntimeReadableStream {
  #closed = false;
  #controller;
  #cancel;
  #error;
  #pull;
  #queue = [];
  #ready;

  constructor(source = {}) {
    this.#cancel = source.cancel;
    this.#pull = source.pull;
    this.#controller = {
      close: () => {
        this.#closed = true;
      },
      enqueue: (chunk) => {
        if (this.#closed) {
          throw new TypeError("cannot enqueue into a closed ReadableStream");
        }
        this.#queue.push(chunk);
      },
      error: (error) => {
        this.#error = error;
        this.#closed = true;
      },
      get desiredSize() {
        return 1;
      },
    };
    this.#ready = Promise.resolve(source.start?.(this.#controller));
  }

  getReader() {
    return {
      read: async () => this.#read(),
      cancel: async (reason) => {
        this.#closed = true;
        await this.#cancel?.(reason);
      },
      releaseLock() {},
    };
  }

  async *[Symbol.asyncIterator]() {
    const reader = this.getReader();
    while (true) {
      const result = await reader.read();
      if (result.done) {
        return;
      }
      yield result.value;
    }
  }

  async #read() {
    await this.#ready;
    if (this.#error) {
      throw this.#error;
    }
    if (this.#queue.length > 0) {
      return { value: this.#queue.shift(), done: false };
    }
    if (this.#closed) {
      return { value: undefined, done: true };
    }
    if (typeof this.#pull !== "function") {
      this.#closed = true;
      return { value: undefined, done: true };
    }
    await this.#pull(this.#controller);
    if (this.#error) {
      throw this.#error;
    }
    if (this.#queue.length > 0) {
      return { value: this.#queue.shift(), done: false };
    }
    return { value: undefined, done: this.#closed };
  }
}

class RuntimeRequest {
  #body;

  constructor(input, init = {}) {
    const source = input instanceof RuntimeRequest ? input : undefined;
    this.url = String(source?.url ?? input);
    this.method = String(init.method ?? source?.method ?? "GET").toUpperCase();
    this.headers = new RuntimeHeaders(init.headers ?? source?.headers);
    this.signal = init.signal ?? source?.signal ?? new RuntimeAbortSignal();
    this.redirect = init.redirect ?? source?.redirect ?? "follow";
    this.#body = init.body ?? (source ? source.#body : null);
    this.bodyUsed = false;
  }

  get body() {
    if (this.#body === null) {
      return null;
    }
    return this.#body instanceof RuntimeReadableStream
      ? this.#body
      : streamFromBytes(bodyBytesSync(this.#body));
  }

  async arrayBuffer() {
    this.bodyUsed = true;
    return (await readBodyBytes(this.#body)).buffer;
  }

  clone() {
    if (this.bodyUsed) {
      throw new TypeError("cannot clone a consumed Request body");
    }
    return new RuntimeRequest(this);
  }

  async json() {
    return JSON.parse(await this.text());
  }

  async formData() {
    const contentType = this.headers.get("content-type") ?? "";
    if (!contentType.toLowerCase().startsWith("application/x-www-form-urlencoded")) {
      throw new TypeError(`unsupported form content type ${JSON.stringify(contentType)}`);
    }
    const form = new RuntimeFormData();
    for (const [name, value] of new RuntimeURLSearchParams(await this.text())) {
      form.append(name, value);
    }
    return form;
  }

  async text() {
    this.bodyUsed = true;
    return textDecoder.decode(await readBodyBytes(this.#body));
  }
}

class RuntimeResponse {
  #body;

  constructor(body = null, init = {}) {
    const status = Number(init.status ?? 200);
    if (!Number.isInteger(status) || status < 200 || status > 599) {
      throw new RangeError(`invalid response status ${status}`);
    }
    this.#body = body;
    this.status = status;
    this.statusText = String(init.statusText ?? "");
    this.headers = new RuntimeHeaders(init.headers);
    this.bodyUsed = false;
  }

  get body() {
    if (this.#body === null) {
      return null;
    }
    return this.#body instanceof RuntimeReadableStream
      ? this.#body
      : streamFromBytes(bodyBytesSync(this.#body));
  }

  get ok() {
    return this.status >= 200 && this.status <= 299;
  }

  async arrayBuffer() {
    this.bodyUsed = true;
    const bytes = await readBodyBytes(this.#body);
    return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength);
  }

  clone() {
    if (this.bodyUsed) {
      throw new TypeError("cannot clone a consumed Response body");
    }
    return new RuntimeResponse(this.#body, {
      status: this.status,
      statusText: this.statusText,
      headers: this.headers,
    });
  }

  async json() {
    return JSON.parse(await this.text());
  }

  async text() {
    this.bodyUsed = true;
    return textDecoder.decode(await readBodyBytes(this.#body));
  }

  static json(value, init = {}) {
    const headers = new RuntimeHeaders(init.headers);
    if (!headers.has("content-type")) {
      headers.set("content-type", "application/json");
    }
    return new RuntimeResponse(JSON.stringify(value), { ...init, headers });
  }

  static redirect(url, status = 302) {
    return new RuntimeResponse(null, {
      status,
      headers: { location: String(url) },
    });
  }
}

function bodyBytesSync(body) {
  if (body === null || body === undefined) {
    return new Uint8Array();
  }
  if (body instanceof Uint8Array) {
    return body;
  }
  if (body instanceof ArrayBuffer) {
    return new Uint8Array(body);
  }
  if (ArrayBuffer.isView(body)) {
    return new Uint8Array(body.buffer, body.byteOffset, body.byteLength);
  }
  return textEncoder.encode(String(body));
}

async function readBodyBytes(body) {
  if (!(body instanceof RuntimeReadableStream)) {
    return bodyBytesSync(body);
  }
  const chunks = [];
  let length = 0;
  const reader = body.getReader();
  while (true) {
    const { value, done } = await reader.read();
    if (done) {
      break;
    }
    const bytes = bodyBytesSync(value);
    chunks.push(bytes);
    length += bytes.byteLength;
  }
  const combined = new Uint8Array(length);
  let offset = 0;
  for (const chunk of chunks) {
    combined.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return combined;
}

function streamFromBytes(bytes) {
  let delivered = false;
  return new RuntimeReadableStream({
    pull(controller) {
      if (!delivered) {
        delivered = true;
        controller.enqueue(bytes);
      }
      controller.close();
    },
  });
}

const runtimeCrypto = {
  getRandomValues(view) {
    if (!ArrayBuffer.isView(view) || view instanceof Float32Array || view instanceof Float64Array) {
      throw new TypeError("crypto.getRandomValues requires an integer typed array");
    }
    const bytes = new Uint8Array(view.buffer, view.byteOffset, view.byteLength);
    bytes.set(takeRandomBytes(bytes.byteLength));
    return view;
  },

  randomUUID() {
    const bytes = takeRandomBytes(16);
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    const hexadecimal = Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0"));
    return [
      hexadecimal.slice(0, 4).join(""),
      hexadecimal.slice(4, 6).join(""),
      hexadecimal.slice(6, 8).join(""),
      hexadecimal.slice(8, 10).join(""),
      hexadecimal.slice(10).join(""),
    ].join("-");
  },
};

function setInvocationRandomBytes(bytes) {
  invocationRandomBytes = new Uint8Array(bytes);
  invocationRandomOffset = 0;
}

function takeRandomBytes(length) {
  const end = invocationRandomOffset + length;
  if (end > invocationRandomBytes.byteLength) {
    throw new Error(
      `worker requested ${length} secure random bytes after consuming ` +
        `${invocationRandomOffset} of ${invocationRandomBytes.byteLength}`,
    );
  }
  const bytes = invocationRandomBytes.slice(invocationRandomOffset, end);
  invocationRandomOffset = end;
  return bytes;
}

class RuntimeKvNamespace {
  #binding;

  constructor(binding) {
    this.#binding = binding;
  }

  async delete(key) {
    callHost({
      operation: "kv_delete",
      binding: this.#binding,
      key: String(key),
    });
  }

  async get(key) {
    return callHost({
      operation: "kv_get",
      binding: this.#binding,
      key: String(key),
    });
  }

  async list(options = {}) {
    return callHost({
      operation: "kv_list",
      binding: this.#binding,
      prefix: String(options.prefix ?? ""),
      limit: Math.min(1000, Math.max(0, Math.trunc(Number(options.limit ?? 1000)))),
    });
  }

  async put(key, value) {
    callHost({
      operation: "kv_put",
      binding: this.#binding,
      key: String(key),
      value,
    });
  }
}

class RuntimeMemoryNamespace {
  #binding;

  constructor(binding) {
    this.#binding = binding;
  }

  idFromName(name) {
    return String(name);
  }

  get(id) {
    return new RuntimeMemoryShard(this.#binding, String(id));
  }
}

class RuntimeMemoryShard {
  #binding;
  #shard;
  #transaction;

  constructor(binding, shard) {
    this.#binding = binding;
    this.#shard = shard;
  }

  async atomic(callback) {
    if (this.#transaction) {
      return await callback();
    }
    for (let attempt = 1; attempt <= 8; attempt += 1) {
      const transaction = { reads: new Map(), writes: new Map() };
      this.#transaction = transaction;
      let returned;
      try {
        returned = await callback();
      } finally {
        this.#transaction = undefined;
      }
      const committed = callHost({
        operation: "memory_commit",
        binding: this.#binding,
        shard: this.#shard,
        reads: Array.from(transaction.reads, ([key, entry]) => ({
          key,
          version: entry.version,
        })),
        writes: Array.from(transaction.writes, ([key, value]) => ({ key, value })),
      });
      if (committed) {
        return returned;
      }
    }
    throw new Error(
      `memory transaction for ${this.#binding}/${this.#shard} conflicted 8 times`,
    );
  }

  async apply(effects) {
    if (Array.isArray(effects) && effects.length === 0) {
      return;
    }
    throw new Error("memory socket effects require the streaming host-call runtime");
  }

  accept() {
    throw new Error("memory websocket accept requires the streaming host-call runtime");
  }

  tvar(key, defaultValue) {
    const normalizedKey = String(key);
    return {
      read: () => this.#readTvar(normalizedKey, defaultValue),
      write: (value) => this.#writeTvar(normalizedKey, value),
    };
  }

  #readTvar(key, defaultValue) {
    const transaction = this.#requireTransaction("read", key);
    if (transaction.writes.has(key)) {
      return transaction.writes.get(key);
    }
    if (!transaction.reads.has(key)) {
      const stored = callHost({
        operation: "memory_read",
        binding: this.#binding,
        shard: this.#shard,
        key,
      });
      transaction.reads.set(key, {
        value: stored.found ? stored.value : defaultValue,
        version: stored.version,
      });
    }
    return transaction.reads.get(key).value;
  }

  #writeTvar(key, value) {
    const transaction = this.#requireTransaction("write", key);
    if (!transaction.reads.has(key)) {
      this.#readTvar(key, undefined);
    }
    transaction.writes.set(key, value);
  }

  #requireTransaction(action, key) {
    if (!this.#transaction) {
      throw new Error(
        `cannot ${action} memory tvar ${this.#binding}/${this.#shard}/${key} outside atomic()`,
      );
    }
    return this.#transaction;
  }
}

function callHost(request) {
  if (typeof globalThis.__ddHostCall !== "function") {
    throw new Error(
      `worker requested ${request.operation}, but it was not built with the dd Javy plugin`,
    );
  }
  const response = JSON.parse(globalThis.__ddHostCall(JSON.stringify(request)));
  if (!response.ok) {
    throw new Error(String(response.error ?? `host call ${request.operation} failed`));
  }
  return response.value;
}

function workerEnvironment(envelope) {
  const env = { ...(envelope.env ?? {}) };
  for (const binding of envelope.bindings ?? []) {
    if (binding.kind === "kv") {
      env[binding.name] = new RuntimeKvNamespace(binding.name);
    } else if (binding.kind === "memory") {
      env[binding.name] = new RuntimeMemoryNamespace(binding.name);
    } else {
      throw new Error(`unsupported Javy binding kind ${JSON.stringify(binding.kind)}`);
    }
  }
  return env;
}

function installGlobals() {
  const globals = {
    AbortController: RuntimeAbortController,
    AbortSignal: RuntimeAbortSignal,
    EventTarget: RuntimeEventTarget,
    FormData: RuntimeFormData,
    Headers: RuntimeHeaders,
    ReadableStream: RuntimeReadableStream,
    Request: RuntimeRequest,
    Response: RuntimeResponse,
    URL: RuntimeURL,
    URLSearchParams: RuntimeURLSearchParams,
    crypto: runtimeCrypto,
  };
  for (const [name, value] of Object.entries(globals)) {
    if (globalThis[name] === undefined) {
      Object.defineProperty(globalThis, name, {
        configurable: true,
        writable: true,
        value,
      });
    }
  }
  if (globalThis.queueMicrotask === undefined) {
    globalThis.queueMicrotask = (callback) => {
      Promise.resolve().then(callback);
    };
  }
  if (globalThis.setTimeout === undefined) {
    let nextTimer = 0;
    const cancelledTimers = new Set();
    globalThis.setTimeout = (callback, _delay = 0, ...args) => {
      const timer = ++nextTimer;
      queueMicrotask(() => {
        if (!cancelledTimers.has(timer)) {
          callback(...args);
        }
      });
      return timer;
    };
    globalThis.clearTimeout = (timer) => {
      cancelledTimers.add(timer);
    };
  }
  globalThis.fetch = async () => {
    throw new Error(
      "fetch is not available in the first Javy runtime milestone; it requires the native host-call plugin",
    );
  };
  installConsole();
}

function installConsole() {
  const writeLog = (...values) => {
    writeFileDescriptor(
      2,
      textEncoder.encode(
        `${values.map((value) => formatLogValue(value)).join(" ")}\n`,
      ),
    );
  };
  globalThis.console = {
    ...globalThis.console,
    debug: writeLog,
    error: writeLog,
    info: writeLog,
    log: writeLog,
    warn: writeLog,
  };
}

function formatLogValue(value) {
  if (typeof value === "string") {
    return value;
  }
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

installGlobals();

export async function runWorker(worker) {
  const envelope = JSON.parse(textDecoder.decode(readFileDescriptor(0)));
  setInvocationRandomBytes(envelope.random_bytes ?? []);
  const invocation = envelope.invocation;
  const request = new RuntimeRequest(invocation.url, {
    method: invocation.method,
    headers: invocation.headers,
    body: new Uint8Array(invocation.body),
  });
  const pendingTasks = [];
  const executionContext = {
    waitUntil(promise) {
      pendingTasks.push(Promise.resolve(promise));
    },
    passThroughOnException() {},
  };
  const fetch = typeof worker === "function" ? worker : worker?.fetch;
  if (typeof fetch !== "function") {
    throw new TypeError("worker default export must be a function or expose fetch(request, env, ctx)");
  }
  const returned = await fetch.call(worker, request, workerEnvironment(envelope), executionContext);
  const response =
    returned instanceof RuntimeResponse ||
    (typeof returned?.arrayBuffer === "function" &&
      typeof returned?.status === "number" &&
      returned?.headers != null)
      ? returned
      : new RuntimeResponse(returned ?? null);
  const body = new Uint8Array(await response.arrayBuffer());
  await Promise.all(pendingTasks);
  writeFileDescriptor(
    1,
    textEncoder.encode(
      JSON.stringify({
        status: response.status,
        headers: Array.from(response.headers),
        body: Array.from(body),
      }),
    ),
  );
}

function readFileDescriptor(descriptor) {
  const chunks = [];
  let totalBytes = 0;
  while (true) {
    const chunk = new Uint8Array(16 * 1024);
    const bytesRead = Javy.IO.readSync(descriptor, chunk);
    if (bytesRead === 0) {
      break;
    }
    chunks.push(chunk.subarray(0, bytesRead));
    totalBytes += bytesRead;
  }
  const bytes = new Uint8Array(totalBytes);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return bytes;
}

function writeFileDescriptor(descriptor, bytes) {
  let offset = 0;
  while (offset < bytes.byteLength) {
    const written = Javy.IO.writeSync(descriptor, bytes.subarray(offset));
    if (written <= 0) {
      throw new Error(`could not write file descriptor ${descriptor}`);
    }
    offset += written;
  }
}
