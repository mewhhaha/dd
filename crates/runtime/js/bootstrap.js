const define = (name, value, enumerable = false) => {
  Object.defineProperty(globalThis, name, {
    value,
    enumerable,
    configurable: true,
    writable: true,
  });
};

function normalizeBody(body) {
  if (body === undefined || body === null) {
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

function encodeUtf8(value) {
  const encoded = unescape(encodeURIComponent(String(value)));
  const out = new Uint8Array(encoded.length);
  for (let i = 0; i < encoded.length; i += 1) {
    out[i] = encoded.charCodeAt(i);
  }
  return out;
}

function decodeUtf8(bytes) {
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
    this._entries = [];
    if (init instanceof Headers) {
      for (const [name, value] of init.entries()) {
        this.append(name, value);
      }
      return;
    }
    if (Array.isArray(init)) {
      for (const [name, value] of init) {
        this.append(name, value);
      }
      return;
    }
    if (init && typeof init === "object") {
      for (const [name, value] of Object.entries(init)) {
        this.append(name, value);
      }
    }
  }

  append(name, value) {
    this._entries.push([normalizeHeaderName(name), String(value)]);
  }

  delete(name) {
    const normalized = normalizeHeaderName(name);
    this._entries = this._entries.filter(([entryName]) => entryName !== normalized);
  }

  entries() {
    return this._entries[Symbol.iterator]();
  }

  get(name) {
    const normalized = normalizeHeaderName(name);
    const values = this._entries
      .filter(([entryName]) => entryName === normalized)
      .map(([, value]) => value);
    return values.length === 0 ? null : values.join(", ");
  }

  has(name) {
    return this.get(name) !== null;
  }

  set(name, value) {
    this.delete(name);
    this.append(name, value);
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
    });
  }
}

class Response {
  constructor(body = null, init = {}) {
    this.status = init.status ?? 200;
    this.statusText = init.statusText ?? "";
    this.headers = new Headers(init.headers ?? []);
    this._body = normalizeBody(body);
    this.ok = this.status >= 200 && this.status < 300;
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
    return new Response(this._body ? new Uint8Array(this._body) : null, {
      status: this.status,
      statusText: this.statusText,
      headers: Array.from(this.headers.entries()),
    });
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

define("Headers", Headers);
define("Request", Request);
define("Response", Response);
define("URL", URL);
