  const toArrayBytes = (value) => {
    if (value instanceof Uint8Array) {
      return value;
    }
    if (ArrayBuffer.isView(value)) {
      return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
    }
    if (value instanceof ArrayBuffer) {
      return new Uint8Array(value);
    }
    return new Uint8Array(Array.isArray(value) ? value : []);
  };

  const isPlainObject = (value) => {
    if (!value || typeof value !== "object") {
      return false;
    }
    const proto = Object.getPrototypeOf(value);
    return proto === Object.prototype || proto === null;
  };

  const encodeMemoryCommandValue = async (value, references, webValues) => {
    if (typeof value === "function" || (value && typeof value.then === "function")) {
      throw new Error("memory command results cannot contain functions or thenables");
    }
    if (references.has(value)) return references.get(value);
    if (value instanceof Request) {
      const encoded = {
        url: String(value.url || ""),
        method: String(value.method || "GET"),
        headers: Array.from(value.headers.entries()),
        body: new Uint8Array(await value.clone().arrayBuffer()),
      };
      references.set(value, encoded);
      webValues.set(encoded, "request");
      return encoded;
    }
    if (value instanceof Response) {
      const encoded = {
        status: Number(value.status || 200),
        statusText: value.statusText,
        headers: Array.from(value.headers.entries()),
        body: new Uint8Array(await value.clone().arrayBuffer()),
      };
      references.set(value, encoded);
      webValues.set(encoded, "response");
      return encoded;
    }
    if (Array.isArray(value)) {
      const out = [];
      references.set(value, out);
      for (const item of value) {
        out.push(await encodeMemoryCommandValue(item, references, webValues));
      }
      return out;
    }
    if (value instanceof Map) {
      const out = new Map();
      references.set(value, out);
      for (const [key, item] of value.entries()) {
        out.set(await encodeMemoryCommandValue(key, references, webValues), await encodeMemoryCommandValue(item, references, webValues));
      }
      return out;
    }
    if (value instanceof Set) {
      const out = new Set();
      references.set(value, out);
      for (const item of value.values()) {
        out.add(await encodeMemoryCommandValue(item, references, webValues));
      }
      return out;
    }
    if (isPlainObject(value)) {
      const out = Object.create(null);
      references.set(value, out);
      for (const [key, item] of Object.entries(value)) {
        out[key] = await encodeMemoryCommandValue(item, references, webValues);
      }
      return out;
    }
    return value;
  };

  const legacyMemoryWebValueKind = (value) => {
    const fields = value.__dd_rpc_type === "request"
      ? ["__dd_rpc_type", "url", "method", "headers", "body"]
      : value.__dd_rpc_type === "response"
      ? ["__dd_rpc_type", "status", "headers", "body"]
      : [];
    const keys = Object.keys(value);
    return fields.length === keys.length && fields.every(key => Object.hasOwn(value, key))
      ? value.__dd_rpc_type
      : undefined;
  };

  const decodeMemoryCommandValue = (value, references, webValues) => {
    if (references.has(value)) return references.get(value);
    if (Array.isArray(value)) {
      const out = [];
      references.set(value, out);
      for (const item of value) out.push(decodeMemoryCommandValue(item, references, webValues));
      return out;
    }
    if (value instanceof Map) {
      const out = new Map();
      references.set(value, out);
      for (const [key, item] of value.entries()) {
        out.set(decodeMemoryCommandValue(key, references, webValues), decodeMemoryCommandValue(item, references, webValues));
      }
      return out;
    }
    if (value instanceof Set) {
      const out = new Set();
      references.set(value, out);
      for (const item of value.values()) {
        out.add(decodeMemoryCommandValue(item, references, webValues));
      }
      return out;
    }
    if (!isPlainObject(value)) {
      return value;
    }
    const kind = webValues ? webValues.get(value) : legacyMemoryWebValueKind(value);
    if (kind === "request") {
      const method = String(value.method || "GET");
      const bodyBytes = toArrayBytes(value.body);
      const init = {
        method,
        headers: Array.isArray(value.headers) ? value.headers : [],
      };
      if (!(bodyBytes.byteLength === 0 && /^(GET|HEAD)$/i.test(method))) {
        init.body = bodyBytes;
      }
      const request = new Request(String(value.url || "http://worker/"), init);
      references.set(value, request);
      return request;
    }
    if (kind === "response") {
      const status = Number(value.status || 200);
      const bodyBytes = toArrayBytes(value.body);
      const init = {
        status,
        statusText: value.statusText ?? "",
        headers: Array.isArray(value.headers) ? value.headers : [],
      };
      const body = (bodyBytes.byteLength === 0 && [101, 204, 205, 304].includes(status))
        ? null
        : bodyBytes;
      const response = new Response(body, init);
      references.set(value, response);
      return response;
    }
    const out = {};
    references.set(value, out);
    for (const [key, item] of Object.entries(value)) {
      Object.defineProperty(out, key, {
        value: decodeMemoryCommandValue(item, references, webValues),
        enumerable: true, writable: true, configurable: true,
      });
    }
    return out;
  };
  const MEMORY_COMMAND_RESULT_MAGIC = new Uint8Array([68, 68, 77, 67]);
  const MEMORY_COMMAND_RESULT_VERSION = 1;

  const encodeMemoryCommandResult = async (value) => {
    const webValues = new Map();
    const encoded = await encodeMemoryCommandValue(value, new Map(), webValues);
    const payload = new Uint8Array(Deno.core.serialize({ value: encoded, webValues }));
    const bytes = new Uint8Array(5 + payload.byteLength);
    bytes.set(MEMORY_COMMAND_RESULT_MAGIC);
    bytes[4] = MEMORY_COMMAND_RESULT_VERSION;
    bytes.set(payload, 5);
    return bytes;
  };

  const decodeMemoryCommandResult = (bytes) => {
    const versioned = MEMORY_COMMAND_RESULT_MAGIC.every((byte, index) => bytes[index] === byte);
    if (!versioned) {
      return decodeMemoryCommandValue(Deno.core.deserialize(bytes), new Map(), undefined);
    }
    if (bytes[4] !== MEMORY_COMMAND_RESULT_VERSION) {
      throw new Error("unsupported stored memory command result version");
    }
    const { value, webValues } = Deno.core.deserialize(bytes.subarray(5));
    if (!(webValues instanceof Map)) {
      throw new Error("invalid stored memory command result metadata");
    }
    return decodeMemoryCommandValue(value, new Map(), webValues);
  };
