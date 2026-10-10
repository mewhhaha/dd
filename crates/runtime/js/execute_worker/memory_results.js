  // `value` as bytes without copying: a Uint8Array as is, a view or buffer
  // as a Uint8Array over its memory, an array of numbers converted.
  const toArrayBytes = (value) => {
    if (TypedArrayPrototypeGetSymbolToStringTag(value) === "Uint8Array") {
      return value;
    }
    if (ArrayBufferIsView(value)) {
      return core.isDataView(value)
        ? new Uint8Array(
          DataViewPrototypeGetBuffer(value),
          DataViewPrototypeGetByteOffset(value),
          DataViewPrototypeGetByteLength(value),
        )
        : new Uint8Array(
          TypedArrayPrototypeGetBuffer(value),
          TypedArrayPrototypeGetByteOffset(value),
          TypedArrayPrototypeGetByteLength(value),
        );
    }
    if (core.isArrayBuffer(value)) {
      return new Uint8Array(value);
    }
    if (!ArrayIsArray(value)) {
      return new Uint8Array();
    }
    const bytes = new Uint8Array(value.length);
    for (let i = 0; i < value.length; i++) {
      bytes[i] = value[i];
    }
    return bytes;
  };

  const isPlainObject = (value) => {
    if (!value || typeof value !== "object") {
      return false;
    }
    const proto = ObjectGetPrototypeOf(value);
    return proto === ObjectPrototype || proto === null;
  };

  const isGetOrHead = (method) => {
    const upper = StringPrototypeToUpperCase(method);
    return upper === "GET" || upper === "HEAD";
  };

  const isNullBodyStatus = (status) => status === 101 || status === 204 || status === 205 || status === 304;

  const encodeMemoryCommandValue = async (value, references, webValues) => {
    if (typeof value === "function" || (value && typeof value.then === "function")) {
      throw new Error("memory command results cannot contain functions or thenables");
    }
    if (references.has(value)) return references.get(value);
    if (ObjectPrototypeIsPrototypeOf(RequestPrototype, value)) {
      const encoded = {
        url: String(RequestPrototypeGetUrl(value) || ""),
        method: String(RequestPrototypeGetMethod(value) || "GET"),
        headers: headerPairs(RequestPrototypeGetHeaders(value)),
        body: new Uint8Array(await RequestPrototypeArrayBuffer(RequestPrototypeClone(value))),
      };
      references.set(value, encoded);
      MapPrototypeSet(webValues, encoded, "request");
      return encoded;
    }
    if (ObjectPrototypeIsPrototypeOf(ResponsePrototype, value)) {
      const encoded = {
        status: Number(ResponsePrototypeGetStatus(value) || 200),
        statusText: ResponsePrototypeGetStatusText(value),
        headers: headerPairs(ResponsePrototypeGetHeaders(value)),
        body: new Uint8Array(await ResponsePrototypeArrayBuffer(ResponsePrototypeClone(value))),
      };
      references.set(value, encoded);
      MapPrototypeSet(webValues, encoded, "response");
      return encoded;
    }
    if (ArrayIsArray(value)) {
      const out = [];
      references.set(value, out);
      for (let i = 0; i < value.length; i++) {
        ArrayPrototypePush(out, await encodeMemoryCommandValue(value[i], references, webValues));
      }
      return out;
    }
    if (core.isMap(value)) {
      const out = new Map();
      references.set(value, out);
      for (const entry of new SafeMapIterator(value)) {
        MapPrototypeSet(
          out,
          await encodeMemoryCommandValue(entry[0], references, webValues),
          await encodeMemoryCommandValue(entry[1], references, webValues),
        );
      }
      return out;
    }
    if (core.isSet(value)) {
      const out = new Set();
      references.set(value, out);
      for (const item of new SafeSetIterator(value)) {
        SetPrototypeAdd(out, await encodeMemoryCommandValue(item, references, webValues));
      }
      return out;
    }
    if (isPlainObject(value)) {
      const out = ObjectCreate(null);
      references.set(value, out);
      const entries = ObjectEntries(value);
      for (let i = 0; i < entries.length; i++) {
        out[entries[i][0]] = await encodeMemoryCommandValue(entries[i][1], references, webValues);
      }
      return out;
    }
    return value;
  };

  const LEGACY_REQUEST_FIELDS = ["__dd_rpc_type", "url", "method", "headers", "body"];
  const LEGACY_RESPONSE_FIELDS = ["__dd_rpc_type", "status", "headers", "body"];

  const legacyMemoryWebValueKind = (value) => {
    const fields = value.__dd_rpc_type === "request"
      ? LEGACY_REQUEST_FIELDS
      : value.__dd_rpc_type === "response"
      ? LEGACY_RESPONSE_FIELDS
      : [];
    if (fields.length !== ObjectKeys(value).length) {
      return undefined;
    }
    for (let i = 0; i < fields.length; i++) {
      if (!ObjectHasOwn(value, fields[i])) {
        return undefined;
      }
    }
    return value.__dd_rpc_type;
  };

  const decodeMemoryCommandValue = (value, references, webValues) => {
    if (references.has(value)) return references.get(value);
    if (ArrayIsArray(value)) {
      const out = [];
      references.set(value, out);
      for (let i = 0; i < value.length; i++) {
        ArrayPrototypePush(out, decodeMemoryCommandValue(value[i], references, webValues));
      }
      return out;
    }
    if (core.isMap(value)) {
      const out = new Map();
      references.set(value, out);
      for (const entry of new SafeMapIterator(value)) {
        MapPrototypeSet(
          out,
          decodeMemoryCommandValue(entry[0], references, webValues),
          decodeMemoryCommandValue(entry[1], references, webValues),
        );
      }
      return out;
    }
    if (core.isSet(value)) {
      const out = new Set();
      references.set(value, out);
      for (const item of new SafeSetIterator(value)) {
        SetPrototypeAdd(out, decodeMemoryCommandValue(item, references, webValues));
      }
      return out;
    }
    if (!isPlainObject(value)) {
      return value;
    }
    const kind = webValues ? MapPrototypeGet(webValues, value) : legacyMemoryWebValueKind(value);
    if (kind === "request") {
      const method = String(value.method || "GET");
      const bodyBytes = toArrayBytes(value.body);
      const init = { __proto__: null, method };
      if (!(TypedArrayPrototypeGetByteLength(bodyBytes) === 0 && isGetOrHead(method))) {
        init.body = bodyBytes;
      }
      const request = new Request(String(value.url || "http://worker/"), init);
      appendHeaderPairs(
        RequestPrototypeGetHeaders(request),
        ArrayIsArray(value.headers) ? value.headers : [],
      );
      references.set(value, request);
      return request;
    }
    if (kind === "response") {
      const status = Number(value.status || 200);
      const bodyBytes = toArrayBytes(value.body);
      const body = (TypedArrayPrototypeGetByteLength(bodyBytes) === 0 && isNullBodyStatus(status))
        ? null
        : bodyBytes;
      const response = new Response(body, {
        __proto__: null,
        status,
        statusText: value.statusText ?? "",
      });
      appendHeaderPairs(
        ResponsePrototypeGetHeaders(response),
        ArrayIsArray(value.headers) ? value.headers : [],
      );
      references.set(value, response);
      return response;
    }
    const out = {};
    references.set(value, out);
    const entries = ObjectEntries(value);
    for (let i = 0; i < entries.length; i++) {
      ObjectDefineProperty(out, entries[i][0], {
        __proto__: null,
        value: decodeMemoryCommandValue(entries[i][1], references, webValues),
        enumerable: true, writable: true, configurable: true,
      });
    }
    return out;
  };
  const MEMORY_COMMAND_RESULT_MAGIC = core.encode("DDMC");
  const MEMORY_COMMAND_RESULT_VERSION = 1;

  const encodeMemoryCommandResult = async (value) => {
    const webValues = new Map();
    const encoded = await encodeMemoryCommandValue(value, new SafeMap(), webValues);
    const payload = new Uint8Array(core.serialize({ value: encoded, webValues }));
    const bytes = new Uint8Array(5 + TypedArrayPrototypeGetByteLength(payload));
    TypedArrayPrototypeSet(bytes, MEMORY_COMMAND_RESULT_MAGIC);
    bytes[4] = MEMORY_COMMAND_RESULT_VERSION;
    TypedArrayPrototypeSet(bytes, payload, 5);
    return bytes;
  };

  const decodeMemoryCommandResult = (bytes) => {
    let versioned = true;
    for (let i = 0; i < 4; i++) {
      if (bytes[i] !== MEMORY_COMMAND_RESULT_MAGIC[i]) {
        versioned = false;
        break;
      }
    }
    if (!versioned) {
      return decodeMemoryCommandValue(core.deserialize(bytes), new SafeMap(), undefined);
    }
    if (bytes[4] !== MEMORY_COMMAND_RESULT_VERSION) {
      throw new Error("unsupported stored memory command result version");
    }
    const { value, webValues } = core.deserialize(new Uint8Array(
      TypedArrayPrototypeGetBuffer(bytes),
      TypedArrayPrototypeGetByteOffset(bytes) + 5,
      TypedArrayPrototypeGetByteLength(bytes) - 5,
    ));
    if (!core.isMap(webValues)) {
      throw new Error("invalid stored memory command result metadata");
    }
    return decodeMemoryCommandValue(value, new SafeMap(), webValues);
  };
