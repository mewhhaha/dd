  const normalizeServiceFetchInputFast = (inputValue, initValue) => {
    if (inputValue instanceof Request) {
      if (inputValue.body != null) {
        return null;
      }
      const headers = initValue?.headers != null
        ? toHeaderEntries(initValue.headers)
        : Array.from(inputValue.headers.entries());
      let body = new Uint8Array();
      if (initValue && Object.prototype.hasOwnProperty.call(initValue, "body")) {
        body = normalizeServiceFetchBody(initValue.body);
        if (body == null) {
          return null;
        }
      }
      return {
        method: String(initValue?.method ?? inputValue.method ?? "GET").toUpperCase(),
        url: String(inputValue.url || "http://worker/"),
        headers,
        body,
      };
    }

    const body = normalizeServiceFetchBody(initValue?.body);
    if (body == null) {
      return null;
    }
    const raw = String(inputValue ?? "/");
    const method = String(initValue?.method ?? "GET").toUpperCase();
    const url = raw.startsWith("http://") || raw.startsWith("https://")
      ? raw
      : new URL(raw, "http://worker").toString();
    const headers = toHeaderEntries(initValue?.headers);
    return {
      method,
      url,
      headers,
      body,
    };
  };

  const normalizeServiceFetchInput = async (inputValue, initValue) => {
    const fast = normalizeServiceFetchInputFast(inputValue, initValue);
    if (fast) {
      return fast;
    }
    return normalizeMemoryFetchInput(inputValue, initValue);
  };

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

  const encodeMemoryCommandValue = async (value, references = new Map()) => {
    if (typeof value === "function" || (value && typeof value.then === "function")) {
      throw new Error("memory command results cannot contain functions or thenables");
    }
    if (references.has(value)) return references.get(value);
    if (value instanceof Request) {
      const encoded = {
        __dd_rpc_type: "request",
        url: String(value.url || ""),
        method: String(value.method || "GET"),
        headers: Array.from(value.headers.entries()),
        body: new Uint8Array(await value.clone().arrayBuffer()),
      };
      references.set(value, encoded);
      return encoded;
    }
    if (value instanceof Response) {
      const encoded = {
        __dd_rpc_type: "response",
        status: Number(value.status || 200),
        headers: Array.from(value.headers.entries()),
        body: new Uint8Array(await value.clone().arrayBuffer()),
      };
      references.set(value, encoded);
      return encoded;
    }
    if (Array.isArray(value)) {
      const out = [];
      references.set(value, out);
      for (const item of value) {
        out.push(await encodeMemoryCommandValue(item, references));
      }
      return out;
    }
    if (value instanceof Map) {
      const out = new Map();
      references.set(value, out);
      for (const [key, item] of value.entries()) {
        out.set(await encodeMemoryCommandValue(key, references), await encodeMemoryCommandValue(item, references));
      }
      return out;
    }
    if (value instanceof Set) {
      const out = new Set();
      references.set(value, out);
      for (const item of value.values()) {
        out.add(await encodeMemoryCommandValue(item, references));
      }
      return out;
    }
    if (isPlainObject(value)) {
      const out = Object.create(null);
      references.set(value, out);
      for (const [key, item] of Object.entries(value)) {
        out[key] = await encodeMemoryCommandValue(item, references);
      }
      return out;
    }
    return value;
  };

  const decodeMemoryCommandValue = (value, references = new Map()) => {
    if (references.has(value)) return references.get(value);
    if (value?.__dd_rpc_type === "socket_message") {
      return normalizeSocketMessageForJs(value);
    }
    if (Array.isArray(value)) {
      const out = [];
      references.set(value, out);
      for (const item of value) out.push(decodeMemoryCommandValue(item, references));
      return out;
    }
    if (value instanceof Map) {
      const out = new Map();
      references.set(value, out);
      for (const [key, item] of value.entries()) {
        out.set(decodeMemoryCommandValue(key, references), decodeMemoryCommandValue(item, references));
      }
      return out;
    }
    if (value instanceof Set) {
      const out = new Set();
      references.set(value, out);
      for (const item of value.values()) {
        out.add(decodeMemoryCommandValue(item, references));
      }
      return out;
    }
    if (!isPlainObject(value)) {
      return value;
    }
    if (value.__dd_rpc_type === "request") {
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
    if (value.__dd_rpc_type === "response") {
      const status = Number(value.status || 200);
      const bodyBytes = toArrayBytes(value.body);
      const init = {
        status,
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
        value: decodeMemoryCommandValue(item, references),
        enumerable: true, writable: true, configurable: true,
      });
    }
    return out;
  };



  const encodeMemoryCommandResult = async (value) => {
    const encoded = await encodeMemoryCommandValue(value);
    return new Uint8Array(Deno.core.serialize(encoded));
  };

  const decodeMemoryCommandResult = (bytes) => decodeMemoryCommandValue(Deno.core.deserialize(bytes));

  const INTERNAL_WS_ACCEPT_HEADER = "x-dd-ws-accept";
  const INTERNAL_WS_SESSION_HEADER = "x-dd-ws-session";
  const INTERNAL_WS_HANDLE_HEADER = "x-dd-ws-handle";
  const INTERNAL_WS_BINDING_HEADER = "x-dd-ws-memory-binding";
  const INTERNAL_WS_KEY_HEADER = "x-dd-ws-memory-key";

  const MEMORY_RUNTIME_EFFECT_VERSION = 1;

  const writeMemoryEffectU16 = (target, offset, value) => {
    const normalized = Math.max(0, Math.min(0xffff, Math.trunc(Number(value ?? 0) || 0)));
    target[offset] = (normalized >>> 8) & 0xff;
    target[offset + 1] = normalized & 0xff;
    return offset + 2;
  };

  const writeMemoryEffectU32 = (target, offset, value) => {
    const normalized = Math.trunc(Number(value ?? 0) || 0);
    if (!Number.isFinite(normalized) || normalized < 0 || normalized > 0xffffffff) {
      throw new Error("memory runtime effect field is too large");
    }
    target[offset] = (normalized >>> 24) & 0xff;
    target[offset + 1] = (normalized >>> 16) & 0xff;
    target[offset + 2] = (normalized >>> 8) & 0xff;
    target[offset + 3] = normalized & 0xff;
    return offset + 4;
  };

  const writeMemoryEffectBytes = (target, offset, value) => {
    const bytes = toArrayBytes(value);
    offset = writeMemoryEffectU32(target, offset, bytes.byteLength);
    target.set(bytes, offset);
    return offset + bytes.byteLength;
  };

  const memoryEffectStringBytes = (value) => toUtf8Bytes(String(value ?? ""));

  const encodeMemorySocketSendEffect = (handle, payload) => {
    const handleBytes = memoryEffectStringBytes(handle);
    const messageBytes = toArrayBytes(payload.value);
    const out = new Uint8Array(10 + handleBytes.byteLength + messageBytes.byteLength);
    let offset = 0;
    out[offset] = MEMORY_RUNTIME_EFFECT_VERSION;
    offset += 1;
    out[offset] = payload.kind === "binary" ? 0 : 1;
    offset += 1;
    offset = writeMemoryEffectBytes(out, offset, handleBytes);
    writeMemoryEffectBytes(out, offset, messageBytes);
    return out;
  };

  const encodeMemoryCloseEffect = (handle, code, reason) => {
    const handleBytes = memoryEffectStringBytes(handle);
    const reasonBytes = memoryEffectStringBytes(reason);
    const out = new Uint8Array(11 + handleBytes.byteLength + reasonBytes.byteLength);
    let offset = 0;
    out[offset] = MEMORY_RUNTIME_EFFECT_VERSION;
    offset += 1;
    offset = writeMemoryEffectU16(out, offset, code);
    offset = writeMemoryEffectBytes(out, offset, handleBytes);
    writeMemoryEffectBytes(out, offset, reasonBytes);
    return out;
  };


  const encodeSocketSendPayload = (value, kind) => {
    if (kind != null) {
      const normalizedKind = String(kind).toLowerCase();
      if (normalizedKind === "text") {
        return {
          kind: "text",
          value: toUtf8Bytes(String(value)),
        };
      }
      if (normalizedKind === "binary") {
        return {
          kind: "binary",
          value: toArrayBytes(value),
        };
      }
      throw new Error(`WebSocket(handle).send unsupported kind: ${kind}`);
    }
    if (isBinaryLike(value)) {
      return {
        kind: "binary",
        value: toArrayBytes(value),
      };
    }
    return {
      kind: "text",
      value: toUtf8Bytes(String(value ?? "")),
    };
  };

  const encodeMemoryStorageValue = (value) => {
    if (typeof value === "string") {
      return {
        encoding: "utf8",
        value: toUtf8Bytes(value),
      };
    }
    return {
      encoding: "v8sc",
      value: new Uint8Array(Deno.core.serialize(value, { forStorage: true })),
    };
  };

  const takeMemoryBytes = (record) => {
    const handle = Math.max(0, Math.trunc(Number(record?.value_handle ?? 0) || 0));
    if (handle > 0) {
      return callOp("op_memory_bytes_take", activeRequestContextHandle(), handle);
    }
    return toArrayBytes(record?.value ?? []);
  };

  const decodeMemoryStorageValue = (record) => {
    if (!record) {
      return null;
    }
    const encoding = String(record.encoding ?? "utf8");
    const bytes = takeMemoryBytes(record);
    if (encoding === "utf8") {
      return Deno.core.decode(bytes);
    }
    if (encoding === "v8sc") {
      return Deno.core.deserialize(bytes, { forStorage: true });
    }
    throw new Error(`memory storage get unsupported encoding: ${encoding}`);
  };

  const ensureMemoryStorageState = (entry) => {
    if (!entry.storageState) {
      entry.storageState = {
        fullSnapshotLoaded: false,
        hydrating: null,
        mirror: new Map(),
        committedVersion: -1,
        failedError: null,
        outputGate: Promise.resolve(),
      };
    }
    return entry.storageState;
  };

  const cloneMemoryRecord = (record) => {
    if (!record) {
      return null;
    }
    return {
      key: String(record.key ?? ""),
      value: takeMemoryBytes(record),
      encoding: String(record.encoding ?? "utf8"),
      version: Number(record.version ?? -1),
      deleted: record.deleted === true,
    };
  };

  const createMemoryTxn = (entry, options = undefined) => ({
    entry,
    batchHandle: beginMemoryBatch(entry, {
      commandHandle: Math.max(0, Math.trunc(Number(options?.commandHandle ?? 0) || 0)),
    }),
  });

  const replaceMemorySnapshot = (storageState, entries, maxVersion) => {
    storageState.mirror.clear();
    for (const entryValue of Array.isArray(entries) ? entries : []) {
      const key = String(entryValue?.key ?? "");
      if (!key) {
        continue;
      }
      storageState.mirror.set(key, {
        key,
        value: takeMemoryBytes(entryValue),
        encoding: String(entryValue?.encoding ?? "utf8"),
        version: Number(entryValue?.version ?? -1),
        deleted: entryValue?.deleted === true,
      });
    }
    const nextCommittedVersion = Number(maxVersion ?? storageState.committedVersion ?? -1);
    storageState.committedVersion = Math.max(
      Number(storageState.committedVersion ?? -1),
      nextCommittedVersion,
    );
    storageState.fullSnapshotLoaded = true;
  };


  const memoryTxnReadRecord = (txn, key) => {
    const normalizedKey = String(key);
    const staged = memoryTxnMutation(txn, normalizedKey);
    if (staged) {
      return staged;
    }
    const storageState = ensureMemoryStorageState(txn.entry);
    return storageState.mirror.get(normalizedKey) ?? null;
  };

  const memoryTxnMutation = (txn, key) => {
    if (!txn) {
      return null;
    }
    const result = callOp("op_memory_batch_get_mutation", txn.batchHandle, String(key));
    if (!result || typeof result !== "object" || result.ok === false) {
      throw new Error(String(result?.error ?? "memory batch mutation lookup failed"));
    }
    return result.record == null ? null : cloneMemoryRecord(result.record);
  };

  const memoryTxnListOverlay = (txn, prefix) => {
    if (!txn) {
      return {
        entries: [],
        mutation_count: 0,
      };
    }
    const result = callOp("op_memory_batch_list_overlay", txn.batchHandle, String(prefix ?? ""));
    if (!result || typeof result !== "object" || result.ok === false) {
      throw new Error(String(result?.error ?? "memory batch list overlay failed"));
    }
    return {
      entries: Array.isArray(result.entries)
        ? result.entries.map((entryValue) => cloneMemoryRecord(entryValue))
        : [],
      mutation_count: Math.max(0, Math.trunc(Number(result.mutation_count ?? 0) || 0)),
    };
  };

  const closeMemoryResourcesOnFailure = async (entry, runtimeRequestId) => {
    const socketHandles = Array.from(entry.openSocketHandles ?? []);
    for (const handle of socketHandles) {
      try {
        const result = await callOp(
          "op_memory_socket_close",
          memoryScopedScopeHandle(entry),
          handle,
          entry.binding,
          entry.memoryKey,
          1011,
          "memory storage flush failed",
        );
        await syncFrozenTime();
        if (result && typeof result === "object" && result.ok === false) {
          console.warn(String(result.error ?? "memory socket close failed"));
        }
      } catch {
        // Ignore follow-up close failures after the memory has already failed.
      }
    }

  };

  const failMemoryEntry = async (entry, runtimeRequestId, error) => {
    const storageState = ensureMemoryStorageState(entry);
    if (storageState.failedError) {
      throw storageState.failedError;
    }
    const failure = error instanceof Error ? error : new Error(String(error ?? "memory namespace failed"));
    storageState.failedError = failure;
    if (entry.cacheKey) {
      memoryStateEntries.delete(entry.cacheKey);
    }
    await closeMemoryResourcesOnFailure(entry, runtimeRequestId);
    throw failure;
  };

  const gateMemoryOutput = (entry, runtimeRequestId, callback) => {
    const storageState = ensureMemoryStorageState(entry);
    const run = async () => {
      if (storageState.failedError) {
        throw storageState.failedError;
      }
      return await callback();
    };
    const gated = storageState.outputGate.then(run, run);
    storageState.outputGate = gated.then(
      () => undefined,
      () => undefined,
    );
    return gated;
  };


  const ensureMemoryStorageHydrated = async (entry, runtimeRequestId, options = {}) => {
    const storageState = ensureMemoryStorageState(entry);
    if (storageState.fullSnapshotLoaded && options?.force !== true) {
      return storageState;
    }
    if (!storageState.hydrating) {
      storageState.hydrating = (async () => {
        const started = performance.now();
        const result = await callOp(
          "op_memory_state_snapshot",
          activeRequestContextHandle(),
          memoryScopedScopeHandle(entry),
          entry.binding,
          entry.memoryKey,
        );
        await syncFrozenTime();
        if (!result || typeof result !== "object" || result.ok === false) {
          throw new Error(String(result?.error ?? "memory storage snapshot failed"));
        }
        replaceMemorySnapshot(
          storageState,
          result.entries,
          result.max_version,
        );
        recordMemoryProfile("js_hydrate_full", performance.now() - started, result.entries?.length ?? 1);
        storageState.hydrating = null;
        return storageState;
      })().catch(async (error) => {
        storageState.hydrating = null;
        return await failMemoryEntry(entry, runtimeRequestId, error);
      });
    }
    return await storageState.hydrating;
  };


  const stageMemoryTxnWrite = (txn, record) => {
    const staged = addMemoryBatchMutation(txn, record);
    if (!staged) {
      throw new Error("memory batch mutation did not return a staged record");
    }
    return staged;
  };

  const stageMemoryTxnEffect = (txn, kind, payload) => {
    requireMemoryBatchOp(
      callOp(
        "op_memory_batch_effect",
        txn.batchHandle,
        kind,
        putMemoryBytes(payload),
      ),
      "effect",
    );
  };


  const beginMemoryCommand = async (entry, idempotencyKey) => {
    const normalizedKey = String(idempotencyKey ?? "").trim();
    if (!normalizedKey) {
      return { handle: 0, hit: false, value: null };
    }
    const result = await callOp(
      "op_memory_command_begin",
      activeRequestContextHandle(),
      memoryScopedScopeHandle(entry),
      entry.binding,
      entry.memoryKey,
      normalizedKey,
    );
    await syncFrozenTime();
    if (!result || typeof result !== "object" || result.ok === false) {
      throw new Error(String(result?.error ?? "memory command begin failed"));
    }
    return {
      handle: Math.max(0, Math.trunc(Number(result.handle ?? 0) || 0)),
      hit: result.hit === true,
      value: result.hit === true ? takeMemoryBytes(result) : null,
    };
  };

  const closeMemoryCommand = (handle) => {
    const normalizedHandle = Math.max(0, Math.trunc(Number(handle ?? 0) || 0));
    if (normalizedHandle > 0) {
      callOp("op_memory_command_close", normalizedHandle);
    }
  };

  const putMemoryBytes = (value) => {
    const bytes = toArrayBytes(value);
    if (bytes.byteLength === 0) {
      return 0;
    }
    const result = callOp("op_memory_bytes_put", activeRequestContextHandle(), bytes);
    if (!result || typeof result !== "object" || result.ok === false) {
      throw new Error(String(result?.error ?? "memory byte handle put failed"));
    }
    return Math.max(0, Math.trunc(Number(result.handle ?? 0) || 0));
  };

  const beginMemoryBatch = (entry, options = undefined) => {
    const result = callOp(
      "op_memory_batch_begin",
      activeRequestContextHandle(),
      memoryScopedScopeHandle(entry),
      entry.binding,
      entry.memoryKey,
      Math.max(0, Math.trunc(Number(options?.commandHandle ?? 0) || 0)),
    );
    if (!result || typeof result !== "object" || result.ok === false) {
      throw new Error(String(result?.error ?? "memory batch begin failed"));
    }
    return Math.max(0, Math.trunc(Number(result.handle ?? 0) || 0));
  };

  const closeMemoryBatch = (handle) => {
    const normalizedHandle = Math.max(0, Math.trunc(Number(handle ?? 0) || 0));
    if (normalizedHandle > 0) {
      callOp("op_memory_batch_close", normalizedHandle);
    }
  };

  const closeMemoryTxnBatch = (txn) => {
    if (txn) {
      closeMemoryBatch(txn.batchHandle);
    }
  };

  const requireMemoryBatchOp = (ok, operation) => {
    if (ok !== true) {
      throw new Error(`memory batch ${operation} failed`);
    }
  };

  const markMemoryBatchAccepted = (txn) => {
    if (!txn) {
      return;
    }
    requireMemoryBatchOp(callOp("op_memory_batch_accept", txn.batchHandle), "accept");
  };

  const setMemoryBatchCommandResult = (txn, value) => {
    if (!txn) {
      return;
    }
    requireMemoryBatchOp(
      callOp(
        "op_memory_batch_command_result",
        txn.batchHandle,
        putMemoryBytes(value),
      ),
      "command result",
    );
  };

  const addMemoryBatchMutation = (txn, mutation) => {
    const result = callOp(
      "op_memory_batch_mutation",
      txn.batchHandle,
      mutation.key,
      putMemoryBytes(mutation.value),
      mutation.encoding,
      mutation.deleted === true,
    );
    if (!result || typeof result !== "object" || result.ok === false) {
      throw new Error(String(result?.error ?? "memory batch mutation failed"));
    }
    return result.record == null ? null : cloneMemoryRecord(result.record);
  };

  const finishMemoryTxn = async (txn, runtimeRequestId) => {
    if (!txn) {
      return;
    }
    const started = performance.now();
    const storageState = ensureMemoryStorageState(txn.entry);
    const result = await callOp("op_memory_batch_apply", txn.batchHandle);
    await syncFrozenTime();
    if (!result || typeof result !== "object" || result.ok === false) {
      throw new Error(String(result?.error ?? "memory transaction commit failed"));
    }
    if (result.read_only === true || result.applied !== true) {
      recordMemoryProfile("js_read_only_commit", performance.now() - started, 1);
      return;
    }
    const committedVersion = Number(result.max_version ?? storageState.committedVersion);
    const mutations = Array.isArray(result.mutations)
      ? result.mutations.map((mutation) => cloneMemoryRecord(mutation))
      : [];
    for (const mutation of mutations) {
      storageState.mirror.set(mutation.key, {
        ...cloneMemoryRecord(mutation),
        version: committedVersion,
      });
    }
    storageState.committedVersion = committedVersion;
    if (result.output_gate_required === true) {
      await gateMemoryOutput(txn.entry, runtimeRequestId, async () => undefined);
    }
    recordMemoryProfile(
      "js_txn_commit",
      performance.now() - started,
      mutations.length + 1,
    );
  };


  const createMemoryStorageBinding = (entry, runtimeRequestId, txn = null) => {
    const rejectStorageOptions = (operation, options) => {
      if (
        options
        && typeof options === "object"
        && Object.keys(options).length > 0
      ) {
        const scope = txn ? "memory atomic" : "memory";
        throw new Error(`${scope} ${operation} options are unsupported`);
      }
    };
    return {
      get(key, options = {}) {
        rejectStorageOptions("get", options);
        const storageState = ensureMemoryStorageState(entry);
        if (storageState.failedError) {
          throw storageState.failedError;
        }
        const normalizedKey = String(key);
        if (txn && !storageState.fullSnapshotLoaded) {
          throw new Error("memory transaction storage must be hydrated before execution");
        }
        const record = txn
          ? memoryTxnReadRecord(txn, normalizedKey)
          : storageState.mirror.get(normalizedKey);
        if (!record || record.deleted) {
          return null;
        }
        return {
          value: decodeMemoryStorageValue(record),
          version: Number(record.version ?? -1),
          encoding: String(record.encoding ?? "utf8"),
        };
      },
      put(key, value, options = {}) {
        rejectStorageOptions("set", options);
        if (!txn) {
          throw new Error("memory storage writes require stub.atomic(...)");
        }
        const storageState = ensureMemoryStorageState(entry);
        if (storageState.failedError) {
          throw storageState.failedError;
        }
        const normalizedKey = String(key);
        const encoded = encodeMemoryStorageValue(value);
        const record = {
          key: normalizedKey,
          value: encoded.value,
          encoding: encoded.encoding,
          deleted: false,
        };
        const staged = stageMemoryTxnWrite(txn, record);
        return {
          ok: true,
          version: Number(staged.version ?? -1),
        };
      },
      delete(key, options = {}) {
        rejectStorageOptions("delete", options);
        if (!txn) {
          throw new Error("memory storage writes require stub.atomic(...)");
        }
        const storageState = ensureMemoryStorageState(entry);
        if (storageState.failedError) {
          throw storageState.failedError;
        }
        const normalizedKey = String(key);
        const record = {
          key: normalizedKey,
          value: new Uint8Array(),
          encoding: "utf8",
          deleted: true,
        };
        const staged = stageMemoryTxnWrite(txn, record);
        return {
          ok: true,
          version: Number(staged.version ?? -1),
        };
      },
      list(options = {}) {
        const storageState = ensureMemoryStorageState(entry);
        if (storageState.failedError) {
          throw storageState.failedError;
        }
        if (txn && !storageState.fullSnapshotLoaded) {
          throw new Error("memory transaction storage must be hydrated before execution");
        }
        const prefix = String(options?.prefix ?? "");
        const limitInput = Number(options?.limit ?? 100);
        const limit = Number.isFinite(limitInput)
          ? Math.max(1, Math.min(1000, Math.trunc(limitInput)))
          : 100;
        const merged = new Map(storageState.mirror);
        if (txn) {
          for (const record of memoryTxnListOverlay(txn, prefix).entries) {
            merged.set(record.key, cloneMemoryRecord(record));
          }
        }
        return Array.from(merged.values())
          .filter((record) => !record.deleted)
          .filter((record) => record.key.startsWith(prefix))
          .sort((left, right) => left.key.localeCompare(right.key))
          .slice(0, limit)
          .map((record) => ({
            key: record.key,
            value: decodeMemoryStorageValue(record),
            version: Number(record.version ?? -1),
          }));
      },
    };
  };
