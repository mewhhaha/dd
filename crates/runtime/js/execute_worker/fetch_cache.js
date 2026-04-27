  const normalizeDynamicFetchInputFast = (inputValue, initValue) => {
    if (inputValue instanceof Request) {
      if (inputValue.body != null) {
        return null;
      }
      const headers = initValue?.headers != null
        ? toHeaderEntries(initValue.headers)
        : Array.from(inputValue.headers.entries());
      let body = new Uint8Array();
      if (initValue && Object.prototype.hasOwnProperty.call(initValue, "body")) {
        body = normalizeDynamicFastBody(initValue.body);
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

    const body = normalizeDynamicFastBody(initValue?.body);
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

  const normalizeDynamicFetchInput = async (inputValue, initValue) => {
    const fast = normalizeDynamicFetchInputFast(inputValue, initValue);
    if (fast) {
      recordDynamicMetric("normalizeFastPathHit");
      return fast;
    }
    recordDynamicMetric("normalizeSlowPathHit");
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

  const encodeRpcValue = async (value) => {
    if (value instanceof Request) {
      return {
        __dd_rpc_type: "request",
        url: String(value.url || ""),
        method: String(value.method || "GET"),
        headers: Array.from(value.headers.entries()),
        body: Array.from(new Uint8Array(await value.arrayBuffer())),
      };
    }
    if (value instanceof Response) {
      return {
        __dd_rpc_type: "response",
        status: Number(value.status || 200),
        headers: Array.from(value.headers.entries()),
        body: Array.from(new Uint8Array(await value.arrayBuffer())),
      };
    }
    if (Array.isArray(value)) {
      const out = [];
      for (const item of value) {
        out.push(await encodeRpcValue(item));
      }
      return out;
    }
    if (value instanceof Map) {
      const out = new Map();
      for (const [key, item] of value.entries()) {
        out.set(await encodeRpcValue(key), await encodeRpcValue(item));
      }
      return out;
    }
    if (value instanceof Set) {
      const out = new Set();
      for (const item of value.values()) {
        out.add(await encodeRpcValue(item));
      }
      return out;
    }
    if (isPlainObject(value)) {
      const out = {};
      for (const [key, item] of Object.entries(value)) {
        out[key] = await encodeRpcValue(item);
      }
      return out;
    }
    return value;
  };

  const decodeRpcValue = (value) => {
    if (value?.__dd_rpc_type === "socket_message") {
      return normalizeSocketMessageForJs(value);
    }
    if (Array.isArray(value)) {
      return value.map((item) => decodeRpcValue(item));
    }
    if (value instanceof Map) {
      const out = new Map();
      for (const [key, item] of value.entries()) {
        out.set(decodeRpcValue(key), decodeRpcValue(item));
      }
      return out;
    }
    if (value instanceof Set) {
      const out = new Set();
      for (const item of value.values()) {
        out.add(decodeRpcValue(item));
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
      return new Request(String(value.url || "http://worker/"), init);
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
      return new Response(body, init);
    }
    const out = {};
    for (const [key, item] of Object.entries(value)) {
      out[key] = decodeRpcValue(item);
    }
    return out;
  };

  const encodeRpcArgs = async (args) => {
    const encoded = [];
    for (const arg of args) {
      encoded.push(await encodeRpcValue(arg));
    }
    return new Uint8Array(Deno.core.serialize(encoded));
  };

  const decodeRpcArgs = (bytes) => {
    const decoded = Deno.core.deserialize(bytes);
    return Array.isArray(decoded) ? decoded.map((value) => decodeRpcValue(value)) : [];
  };

  const encodeRpcResult = async (value) => {
    const encoded = await encodeRpcValue(value);
    return new Uint8Array(Deno.core.serialize(encoded));
  };

  const decodeRpcResult = (bytes) => decodeRpcValue(Deno.core.deserialize(bytes));

  const INTERNAL_WS_ACCEPT_HEADER = "x-dd-ws-accept";
  const INTERNAL_WS_SESSION_HEADER = "x-dd-ws-session";
  const INTERNAL_WS_HANDLE_HEADER = "x-dd-ws-handle";
  const INTERNAL_WS_BINDING_HEADER = "x-dd-ws-memory-binding";
  const INTERNAL_WS_KEY_HEADER = "x-dd-ws-memory-key";
  const INTERNAL_TRANSPORT_ACCEPT_HEADER = "x-dd-transport-accept";
  const INTERNAL_TRANSPORT_SESSION_HEADER = "x-dd-transport-session";
  const INTERNAL_TRANSPORT_HANDLE_HEADER = "x-dd-transport-handle";
  const INTERNAL_TRANSPORT_BINDING_HEADER = "x-dd-transport-memory-binding";
  const INTERNAL_TRANSPORT_KEY_HEADER = "x-dd-transport-memory-key";
  const INTERNAL_TRANSPORT_METHOD_HEADER = "x-dd-transport-method";

  const encodeSocketSendPayload = (value, kind) => {
    if (kind != null) {
      const normalizedKind = String(kind).toLowerCase();
      if (normalizedKind === "text") {
        return {
          kind: "text",
          value: Array.from(toUtf8Bytes(String(value))),
        };
      }
      if (normalizedKind === "binary") {
        return {
          kind: "binary",
          value: Array.from(toArrayBytes(value)),
        };
      }
      throw new Error(`WebSocket(handle).send unsupported kind: ${kind}`);
    }
    if (isBinaryLike(value)) {
      return {
        kind: "binary",
        value: Array.from(toArrayBytes(value)),
      };
    }
    return {
      kind: "text",
      value: Array.from(toUtf8Bytes(String(value ?? ""))),
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

  const decodeMemoryStorageValue = (record) => {
    if (!record) {
      return null;
    }
    const encoding = String(record.encoding ?? "utf8");
    const bytes = toArrayBytes(record.value ?? []);
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
        hydrated: false,
        fullSnapshotLoaded: false,
        hydrating: null,
        mirror: new Map(),
        loadedKeys: new Set(),
        committedVersion: -1,
        snapshotVersion: -1,
        nextVersion: 0,
        freshnessCheckedRequestId: "",
        freshnessCheckedVersion: -1,
        freshnessCheckedAtMs: 0,
        stale: false,
        pendingMutations: [],
        flushRunning: false,
        flushTail: Promise.resolve(),
        pendingSubmissionCount: 0,
        pendingSubmissionPromises: [],
        failedError: null,
        outputGate: Promise.resolve(),
      };
    }
    return entry.storageState;
  };

  const memoryRecordVersion = (record) => (
    record && !record.deleted ? Number(record.version ?? -1) : -1
  );

  const cloneMemoryRecord = (record) => {
    if (!record) {
      return null;
    }
    return {
      key: String(record.key ?? ""),
      value: toArrayBytes(record.value ?? []),
      encoding: String(record.encoding ?? "utf8"),
      version: Number(record.version ?? -1),
      deleted: record.deleted === true,
    };
  };

  const createMemoryTxn = (entry) => ({
    entry,
    reads: new Map(),
    writes: new Map(),
    deferred: [],
    listGateVersion: null,
    stagedVersionSeed: null,
    accepted: false,
    sideEffects: false,
    committed: false,
    committedVersion: null,
  });

  const memoryTxnIsSnapshotOnly = (txn) => !!txn
    && txn.reads.size === 0
    && txn.writes.size === 0
    && txn.deferred.length === 0
    && txn.listGateVersion == null
    && txn.accepted !== true
    && txn.sideEffects !== true;

  const memoryTxnIsReadOnly = (txn) => !!txn
    && txn.writes.size === 0
    && txn.deferred.length === 0
    && txn.accepted !== true
    && txn.sideEffects !== true;

  const memoryTxnIsBlindWrite = (txn) => !!txn
    && txn.writes.size > 0
    && txn.reads.size === 0
    && txn.deferred.length === 0
    && txn.listGateVersion == null
    && txn.accepted !== true
    && txn.sideEffects !== true;

  class MemoryHydrationNeeded extends Error {
    constructor(mode, keys = []) {
      super(mode === "full" ? "memory hydration requires full snapshot" : "memory hydration requires keys");
      this.name = "MemoryHydrationNeeded";
      this.__dd_memory_hydration_needed = true;
      this.mode = mode;
      this.keys = Array.isArray(keys)
        ? keys.map((value) => String(value)).filter((value) => value.length > 0)
        : [];
    }
  }

  const mergeMemorySnapshotEntries = (storageState, entries, maxVersion, mode, requestedKeys = []) => {
    if (mode === "full") {
      storageState.mirror.clear();
      storageState.loadedKeys.clear();
    }
    const seenKeys = new Set();
    for (const entryValue of Array.isArray(entries) ? entries : []) {
      const key = String(entryValue?.key ?? "");
      if (!key) {
        continue;
      }
      seenKeys.add(key);
      storageState.loadedKeys.add(key);
      storageState.mirror.set(key, {
        key,
        value: toArrayBytes(entryValue?.value ?? []),
        encoding: String(entryValue?.encoding ?? "utf8"),
        version: Number(entryValue?.version ?? -1),
        deleted: entryValue?.deleted === true,
      });
    }
    if (mode !== "full") {
      for (const key of requestedKeys) {
        const normalizedKey = String(key ?? "");
        if (!normalizedKey || storageState.loadedKeys.has(normalizedKey)) {
          continue;
        }
        storageState.loadedKeys.add(normalizedKey);
        if (!seenKeys.has(normalizedKey) && !storageState.mirror.has(normalizedKey)) {
          storageState.mirror.set(normalizedKey, {
            key: normalizedKey,
            value: new Uint8Array(),
            encoding: "utf8",
            version: -1,
            deleted: true,
          });
        }
      }
    }
    const nextCommittedVersion = Number(maxVersion ?? storageState.committedVersion ?? -1);
    storageState.committedVersion = Math.max(
      Number(storageState.committedVersion ?? -1),
      nextCommittedVersion,
    );
    storageState.nextVersion = Math.max(
      Number(storageState.nextVersion ?? 0),
      Number(storageState.committedVersion ?? -1) + 1,
    );
    storageState.snapshotVersion = Number(storageState.committedVersion ?? -1);
    storageState.freshnessCheckedAtMs = performance.now();
    storageState.stale = false;
    if (mode === "full") {
      storageState.hydrated = true;
      storageState.fullSnapshotLoaded = true;
    } else {
      storageState.fullSnapshotLoaded = false;
    }
  };

  const invalidateMemorySnapshot = (entry, knownVersion = null) => {
    const storageState = ensureMemoryStorageState(entry);
    storageState.mirror.clear();
    storageState.loadedKeys.clear();
    storageState.hydrated = false;
    storageState.fullSnapshotLoaded = false;
    storageState.hydrating = null;
    storageState.stale = true;
    storageState.freshnessCheckedAtMs = 0;
    if (Number.isFinite(Number(knownVersion))) {
      storageState.committedVersion = Math.max(
        Number(storageState.committedVersion ?? -1),
        Number(knownVersion),
      );
      storageState.snapshotVersion = Math.max(
        Number(storageState.snapshotVersion ?? -1),
        Number(knownVersion),
      );
      storageState.nextVersion = Math.max(
        Number(storageState.nextVersion ?? 0),
        Number(storageState.committedVersion ?? -1) + 1,
      );
    }
    return storageState;
  };

  const memoryTxnReadRecord = (txn, key) => {
    const normalizedKey = String(key);
    if (txn?.writes?.has(normalizedKey)) {
      return txn.writes.get(normalizedKey);
    }
    const storageState = ensureMemoryStorageState(txn.entry);
    return storageState.mirror.get(normalizedKey) ?? null;
  };

  const memoryTxnTrackRead = (txn, key, record, allowConcurrency) => {
    if (!txn || allowConcurrency === true) {
      return;
    }
    const normalizedKey = String(key);
    if (txn.reads.has(normalizedKey)) {
      return;
    }
    if (txn.writes.has(normalizedKey)) {
      return;
    }
    txn.reads.set(normalizedKey, memoryRecordVersion(record));
  };

  const memoryTxnNextVersion = (txn) => {
    if (!txn) {
      throw new Error("transaction is required");
    }
    if (!Number.isFinite(Number(txn.stagedVersionSeed))) {
      const storageState = ensureMemoryStorageState(txn.entry);
      txn.stagedVersionSeed = Number(storageState.committedVersion ?? -1) + 1;
    }
    const version = Number(txn.stagedVersionSeed);
    txn.stagedVersionSeed = version + 1;
    return version;
  };

  const closeMemoryResourcesOnFailure = async (entry, runtimeRequestId) => {
    const socketHandles = Array.from(entry.openSocketHandles ?? []);
    for (const handle of socketHandles) {
      try {
        const result = await callOp("op_memory_socket_close", {
          request_id: runtimeRequestId,
          handle,
          code: 1011,
          reason: "memory storage flush failed",
        });
        await syncFrozenTime();
        if (result && typeof result === "object" && result.ok === false) {
          console.warn(String(result.error ?? "memory socket close failed"));
        }
      } catch {
        // Ignore follow-up close failures after the memory has already failed.
      }
    }

    const transportHandles = Array.from(entry.openTransportHandles ?? []);
    for (const handle of transportHandles) {
      try {
        const result = await callOpAny(
          [
            "op_memory_transport_close",
            "op_memory_transport_terminate",
          ],
          {
            request_id: runtimeRequestId,
            handle,
            code: 1011,
          reason: "memory storage flush failed",
          },
        );
        await syncFrozenTime();
        if (result && typeof result === "object" && result.ok === false) {
          console.warn(String(result.error ?? "memory transport close failed"));
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

  const handleMemoryDirectWriteFailure = (entry, error) => {
    const storageState = ensureMemoryStorageState(entry);
    console.warn(String(error?.message ?? error ?? "memory direct write failed"));
    invalidateMemorySnapshot(entry);
    storageState.freshnessCheckedRequestId = "";
  };

  const waitForMemoryDirectSubmission = async (entry, submissionId) => {
    const storageState = ensureMemoryStorageState(entry);
    try {
      let result = null;
      for (;;) {
        result = await callOp("op_memory_state_await_submission", {
          submission_id: Number(submissionId),
        });
        await syncFrozenTime();
        if (!result || typeof result !== "object" || result.ok === false) {
          throw new Error(String(result?.error ?? "memory direct write submission failed"));
        }
        if (result.pending !== true) {
          break;
        }
        await new Promise((resolve) => setTimeout(resolve, 1));
      }
      storageState.committedVersion = Math.max(
        Number(storageState.committedVersion ?? -1),
        Number(result.max_version ?? -1),
      );
      storageState.nextVersion = Math.max(
        Number(storageState.nextVersion ?? 0),
        Number(storageState.committedVersion ?? -1) + 1,
      );
      if (storageState.pendingSubmissionCount <= 1) {
        storageState.freshnessCheckedVersion = Number(storageState.committedVersion ?? -1);
        storageState.freshnessCheckedAtMs = performance.now();
        storageState.stale = false;
      }
      return Number(result.max_version ?? -1);
    } catch (error) {
      handleMemoryDirectWriteFailure(entry, error);
      throw error;
    } finally {
      storageState.pendingSubmissionCount = Math.max(
        0,
        Number(storageState.pendingSubmissionCount ?? 0) - 1,
      );
    }
  };

  const flushMemoryStorage = (entry, runtimeRequestId) => {
    const storageState = ensureMemoryStorageState(entry);
    if (storageState.flushRunning) {
      return storageState.flushTail;
    }
    storageState.flushRunning = true;
    storageState.flushTail = (async () => {
      while (storageState.pendingMutations.length > 0) {
        const mutations = storageState.pendingMutations.splice(0, storageState.pendingMutations.length);
        const result = await callOp(
          "op_memory_state_enqueue_batch",
          {
            request_id: runtimeRequestId,
            binding: entry.binding,
            key: entry.memoryKey,
            mutations: mutations.map((mutation) => ({
              key: mutation.key,
              value: Array.from(mutation.value),
              encoding: mutation.encoding,
              deleted: mutation.deleted,
            })),
          },
        );
        await syncFrozenTime();
        if (!result || typeof result !== "object" || result.ok === false) {
          throw new Error(String(result?.error ?? "memory storage batch flush failed"));
        }
        storageState.pendingSubmissionCount += 1;
        const submission = waitForMemoryDirectSubmission(entry, Number(result.submission_id ?? 0));
        storageState.pendingSubmissionPromises.push(submission);
        submission.finally(() => {
          const index = storageState.pendingSubmissionPromises.indexOf(submission);
          if (index >= 0) {
            storageState.pendingSubmissionPromises.splice(index, 1);
          }
        }).catch(() => {});
      }
    })()
      .catch((error) => {
        handleMemoryDirectWriteFailure(entry, error);
        throw error;
      })
      .finally(() => {
        storageState.flushRunning = false;
      });
    return storageState.flushTail;
  };

  const queueMemoryMutation = (entry, runtimeRequestId, mutation) => {
    const storageState = ensureMemoryStorageState(entry);
    if (storageState.failedError) {
      throw storageState.failedError;
    }
    storageState.pendingMutations.push(mutation);
  };

  const ensureMemoryStorageQueued = async (entry, runtimeRequestId) => {
    const storageState = ensureMemoryStorageState(entry);
    if (storageState.failedError) {
      throw storageState.failedError;
    }
    if (storageState.flushRunning || storageState.pendingMutations.length > 0) {
      await flushMemoryStorage(entry, runtimeRequestId);
    }
    if (storageState.pendingSubmissionPromises.length > 0) {
      await Promise.all(Array.from(storageState.pendingSubmissionPromises));
    }
    if (storageState.failedError) {
      throw storageState.failedError;
    }
  };

  const waitForMemoryFlush = async (entry, runtimeRequestId) => {
    const storageState = ensureMemoryStorageState(entry);
    if (storageState.failedError) {
      throw storageState.failedError;
    }
    if (storageState.flushRunning || storageState.pendingMutations.length > 0) {
      await flushMemoryStorage(entry, runtimeRequestId);
    }
    if (storageState.pendingSubmissionPromises.length > 0) {
      await Promise.all(Array.from(storageState.pendingSubmissionPromises));
    }
    if (storageState.failedError) {
      throw storageState.failedError;
    }
  };

  const gateMemoryOutput = (entry, runtimeRequestId, callback) => {
    const storageState = ensureMemoryStorageState(entry);
    const run = async () => {
      await waitForMemoryFlush(entry, runtimeRequestId);
      return await callback();
    };
    const gated = storageState.outputGate.then(run, run);
    storageState.outputGate = gated.then(
      () => undefined,
      () => undefined,
    );
    return gated;
  };

  const ensureMemoryStoragePointHydrated = async (entry, runtimeRequestId, key, options = {}) => {
    const storageState = ensureMemoryStorageState(entry);
    const normalizedKey = String(key ?? "");
    if (!normalizedKey) {
      return storageState;
    }
    const force = options?.force === true;
    if (!force && (storageState.fullSnapshotLoaded || storageState.loadedKeys.has(normalizedKey))) {
      return storageState;
    }
    const started = performance.now();
    const result = await callOp("op_memory_state_get", {
      request_id: runtimeRequestId,
      binding: entry.binding,
      key: entry.memoryKey,
      item_key: normalizedKey,
    });
    await syncFrozenTime();
    if (!result || typeof result !== "object" || result.ok === false) {
      throw new Error(String(result?.error ?? "memory storage point read failed"));
    }
    mergeMemorySnapshotEntries(
      storageState,
      result.record ? [result.record] : [],
      result.max_version,
      "keys",
      [normalizedKey],
    );
    storageState.freshnessCheckedRequestId = String(runtimeRequestId ?? "");
    storageState.freshnessCheckedVersion = Number(storageState.committedVersion ?? -1);
    storageState.freshnessCheckedAtMs = performance.now();
    recordMemoryProfile("js_hydrate_keys", performance.now() - started, 1);
    return storageState;
  };

  const ensureMemoryStorageHydrated = async (entry, runtimeRequestId, options = {}) => {
    const storageState = ensureMemoryStorageState(entry);
    if (storageState.fullSnapshotLoaded && options?.force !== true) {
      return storageState;
    }
    if (!storageState.hydrating) {
      storageState.hydrating = (async () => {
        const started = performance.now();
        const result = await callOp("op_memory_state_snapshot", {
          request_id: runtimeRequestId,
          binding: entry.binding,
          key: entry.memoryKey,
        });
        await syncFrozenTime();
        if (!result || typeof result !== "object" || result.ok === false) {
          throw new Error(String(result?.error ?? "memory storage snapshot failed"));
        }
        mergeMemorySnapshotEntries(
          storageState,
          result.entries,
          result.max_version,
          "full",
        );
        storageState.freshnessCheckedRequestId = String(runtimeRequestId ?? "");
        storageState.freshnessCheckedVersion = Number(storageState.committedVersion ?? -1);
        storageState.freshnessCheckedAtMs = performance.now();
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

  const ensureMemoryStorageKeysHydrated = async (entry, runtimeRequestId, keys, options = {}) => {
    const storageState = ensureMemoryStorageState(entry);
    const force = options?.force === true;
    if (storageState.fullSnapshotLoaded && !force) {
      return storageState;
    }
    const pendingKeys = Array.from(new Set(
      (Array.isArray(keys) ? keys : [])
        .map((value) => String(value ?? ""))
        .filter((value) => value.length > 0)
        .filter((value) => force || !storageState.loadedKeys.has(value)),
    ));
    if (pendingKeys.length === 0) {
      return storageState;
    }
    if (pendingKeys.length === 1) {
      return ensureMemoryStoragePointHydrated(entry, runtimeRequestId, pendingKeys[0], { force });
    }
    const started = performance.now();
    const result = await callOp("op_memory_state_snapshot", {
      request_id: runtimeRequestId,
      binding: entry.binding,
      key: entry.memoryKey,
      keys: pendingKeys,
    });
    await syncFrozenTime();
    if (!result || typeof result !== "object" || result.ok === false) {
      throw new Error(String(result?.error ?? "memory storage point snapshot failed"));
    }
    mergeMemorySnapshotEntries(
      storageState,
      result.entries,
      result.max_version,
      "keys",
      pendingKeys,
    );
    storageState.freshnessCheckedRequestId = String(runtimeRequestId ?? "");
    storageState.freshnessCheckedVersion = Number(storageState.committedVersion ?? -1);
    storageState.freshnessCheckedAtMs = performance.now();
    recordMemoryProfile("js_hydrate_keys", performance.now() - started, pendingKeys.length);
    return storageState;
  };

  const stageMemoryTxnWrite = (txn, record) => {
    txn.writes.set(String(record.key ?? ""), cloneMemoryRecord(record));
  };

  const refreshMemoryEntrySnapshot = async (entry, runtimeRequestId) => {
    invalidateMemorySnapshot(entry);
    await ensureMemoryStorageHydrated(entry, runtimeRequestId, { force: true });
  };

  const memorySnapshotTtlExpired = (storageState) => (
    performance.now() - Number(storageState.freshnessCheckedAtMs ?? 0) > memoryReadSnapshotFreshTtlMs
  );

  const ensureMemoryDirectReadReady = async (entry, runtimeRequestId, key) => {
    const normalizedKey = String(key ?? "");
    const storageState = ensureMemoryStorageState(entry);
    if (
      (
        storageState.pendingMutations.length > 0
        || storageState.flushRunning
        || Number(storageState.pendingSubmissionCount ?? 0) > 0
      )
      && (
        storageState.fullSnapshotLoaded
        || storageState.loadedKeys.has(normalizedKey)
        || storageState.mirror.has(normalizedKey)
      )
    ) {
      return storageState;
    }
    if (
      (storageState.fullSnapshotLoaded || storageState.loadedKeys.has(normalizedKey))
      && storageState.stale !== true
      && !memorySnapshotTtlExpired(storageState)
    ) {
      recordMemoryProfile("memory_cache_hit", 0, 1);
      return storageState;
    }
    if (storageState.fullSnapshotLoaded || storageState.loadedKeys.has(normalizedKey)) {
      recordMemoryProfile("memory_cache_stale", 0, 1);
    } else {
      recordMemoryProfile("memory_cache_miss", 0, 1);
    }
    await ensureMemoryStoragePointHydrated(entry, runtimeRequestId, normalizedKey, {
      force: storageState.fullSnapshotLoaded || storageState.loadedKeys.has(normalizedKey),
    });
    return storageState;
  };

  const commitMemoryTxn = async (txn, runtimeRequestId) => {
    if (!txn) {
      return;
    }
    const started = performance.now();
    const storageState = ensureMemoryStorageState(txn.entry);
    const writes = Array.from(txn.writes.values());
    const reads = Array.from(txn.reads.entries()).map(([key, version]) => ({
      key,
      version: Number(version ?? -1),
    }));
    const result = await callOp("op_memory_state_apply_batch", {
      request_id: runtimeRequestId,
      binding: txn.entry.binding,
      key: txn.entry.memoryKey,
      transactional: true,
      expected_base_version: -1,
      reads,
      list_gate_version: txn.listGateVersion == null ? -1 : Number(txn.listGateVersion),
      mutations: writes.map((mutation) => ({
        key: mutation.key,
        value: Array.from(mutation.value),
        encoding: mutation.encoding,
        version: mutation.version,
        deleted: mutation.deleted,
      })),
    });
    await syncFrozenTime();
    if (!result || typeof result !== "object" || result.ok === false) {
      throw new Error(String(result?.error ?? "memory transaction commit failed"));
    }
    if (result.conflict === true) {
      await refreshMemoryEntrySnapshot(txn.entry, runtimeRequestId);
      return false;
    }
    const committedVersion = Number(result.max_version ?? storageState.committedVersion);
    for (const mutation of writes) {
      storageState.loadedKeys.add(mutation.key);
      storageState.mirror.set(mutation.key, {
        ...cloneMemoryRecord(mutation),
        version: committedVersion,
      });
    }
    storageState.committedVersion = committedVersion;
    storageState.nextVersion = Math.max(
      Number(storageState.nextVersion ?? 0),
      Number(storageState.committedVersion ?? -1) + 1,
    );
    storageState.snapshotVersion = committedVersion;
    storageState.freshnessCheckedRequestId = String(runtimeRequestId ?? "");
    storageState.freshnessCheckedVersion = committedVersion;
    storageState.freshnessCheckedAtMs = performance.now();
    storageState.stale = false;
    txn.committed = true;
    txn.committedVersion = committedVersion;
    if (txn.deferred.length > 0 || txn.accepted === true || txn.sideEffects === true) {
      await gateMemoryOutput(txn.entry, runtimeRequestId, async () => undefined);
    }
    recordMemoryProfile("js_txn_commit", performance.now() - started, writes.length + reads.length + 1);
    return true;
  };

  const commitMemoryBlindTxn = async (txn, runtimeRequestId) => {
    if (!txn) {
      return;
    }
    const started = performance.now();
    const storageState = ensureMemoryStorageState(txn.entry);
    const writes = Array.from(txn.writes.values());
    const result = await callOp("op_memory_state_apply_blind_batch", {
      request_id: runtimeRequestId,
      binding: txn.entry.binding,
      key: txn.entry.memoryKey,
      mutations: writes.map((mutation) => ({
        key: mutation.key,
        value: Array.from(mutation.value),
        encoding: mutation.encoding,
        version: mutation.version,
        deleted: mutation.deleted,
      })),
    });
    await syncFrozenTime();
    if (!result || typeof result !== "object" || result.ok === false) {
      throw new Error(String(result?.error ?? "memory blind transaction commit failed"));
    }
    if (result.conflict === true) {
      await refreshMemoryEntrySnapshot(txn.entry, runtimeRequestId);
      return false;
    }
    const committedVersion = Number(result.max_version ?? storageState.committedVersion);
    for (const mutation of writes) {
      storageState.loadedKeys.add(mutation.key);
      storageState.mirror.set(mutation.key, {
        ...cloneMemoryRecord(mutation),
        version: committedVersion,
      });
    }
    storageState.committedVersion = committedVersion;
    storageState.nextVersion = Math.max(
      Number(storageState.nextVersion ?? 0),
      Number(storageState.committedVersion ?? -1) + 1,
    );
    storageState.snapshotVersion = committedVersion;
    storageState.freshnessCheckedRequestId = String(runtimeRequestId ?? "");
    storageState.freshnessCheckedVersion = committedVersion;
    storageState.freshnessCheckedAtMs = performance.now();
    storageState.stale = false;
    txn.committed = true;
    txn.committedVersion = committedVersion;
    recordMemoryProfile("js_txn_blind_commit", performance.now() - started, writes.length + 1);
    return true;
  };

  const validateMemoryTxnReads = async (txn, runtimeRequestId) => {
    if (!txn) {
      return true;
    }
    const started = performance.now();
    const storageState = ensureMemoryStorageState(txn.entry);
    const reads = Array.from(txn.reads.entries()).map(([key, version]) => ({
      key,
      version: Number(version ?? -1),
    }));
    const result = await callOp("op_memory_state_validate_reads", {
      request_id: runtimeRequestId,
      binding: txn.entry.binding,
      key: txn.entry.memoryKey,
      reads,
      list_gate_version: txn.listGateVersion == null ? -1 : Number(txn.listGateVersion),
    });
    await syncFrozenTime();
    if (!result || typeof result !== "object" || result.ok === false) {
      throw new Error(String(result?.error ?? "memory transaction validation failed"));
    }
    if (result.conflict === true) {
      await ensureMemoryStorageKeysHydrated(
        txn.entry,
        runtimeRequestId,
        reads.map((entry) => entry.key),
        { force: true },
      );
      if (txn.listGateVersion != null) {
        await refreshMemoryEntrySnapshot(txn.entry, runtimeRequestId);
      }
      return false;
    }
    storageState.committedVersion = Math.max(
      Number(storageState.committedVersion ?? -1),
      Number(result.max_version ?? storageState.committedVersion ?? -1),
    );
    storageState.nextVersion = Math.max(
      Number(storageState.nextVersion ?? 0),
      Number(storageState.committedVersion ?? -1) + 1,
    );
    txn.committed = true;
    txn.committedVersion = Number(result.max_version ?? storageState.committedVersion);
    recordMemoryProfile("js_txn_validate", performance.now() - started, reads.length + 1);
    return true;
  };

  const ensureMemoryReadTxnFresh = async (txn, runtimeRequestId) => {
    if (!txn || (txn.reads.size === 0 && txn.listGateVersion == null)) {
      return true;
    }
    const started = performance.now();
    const storageState = ensureMemoryStorageState(txn.entry);
    const knownVersion = Number(storageState.committedVersion ?? -1);
    const result = await callOp("op_memory_state_version_if_newer", {
      request_id: runtimeRequestId,
      binding: txn.entry.binding,
      key: txn.entry.memoryKey,
      known_version: knownVersion,
    });
    await syncFrozenTime();
    if (!result || typeof result !== "object" || result.ok === false) {
      throw new Error(String(result?.error ?? "memory transaction freshness check failed"));
    }
    recordMemoryProfile("js_freshness_check", performance.now() - started, txn.reads.size + 1);
    if (result.stale !== true) {
      storageState.freshnessCheckedRequestId = String(runtimeRequestId ?? "");
      storageState.freshnessCheckedVersion = knownVersion;
      storageState.freshnessCheckedAtMs = performance.now();
      storageState.stale = false;
      return true;
    }
    storageState.stale = true;
    return await validateMemoryTxnReads(txn, runtimeRequestId);
  };

  const createMemoryStorageBinding = (entry, runtimeRequestId, txn = null) => {
    const currentStorageRequestId = () => memoryScopedRequestId(entry, runtimeRequestId);
    return {
      get(key, options = {}) {
        const storageState = ensureMemoryStorageState(entry);
        if (storageState.failedError) {
          throw storageState.failedError;
        }
        const normalizedKey = String(key);
        if (txn && !storageState.fullSnapshotLoaded && !storageState.loadedKeys.has(normalizedKey)) {
          recordMemoryProfile("memory_cache_miss", 0, 1);
          throw new MemoryHydrationNeeded("keys", [normalizedKey]);
        }
        const record = txn
          ? memoryTxnReadRecord(txn, normalizedKey)
          : storageState.mirror.get(normalizedKey);
        memoryTxnTrackRead(txn, normalizedKey, record, options?.allowConcurrency === true);
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
        const storageState = ensureMemoryStorageState(entry);
        if (storageState.failedError) {
          throw storageState.failedError;
        }
        const normalizedKey = String(key);
        const expectedInput = options?.expectedVersion;
        const expectedVersion = Number.isFinite(Number(expectedInput))
          ? Math.trunc(Number(expectedInput))
          : -1;
        const current = txn
          ? memoryTxnReadRecord(txn, normalizedKey)
          : storageState.mirror.get(normalizedKey);
        const currentVersion = current ? Number(current.version ?? -1) : -1;
        if (expectedVersion >= 0 && currentVersion !== expectedVersion) {
          return {
            ok: true,
            conflict: true,
            version: currentVersion,
          };
        }
        const encoded = encodeMemoryStorageValue(value);
        const version = txn
          ? memoryTxnNextVersion(txn)
          : Number(storageState.committedVersion ?? -1);
        const record = {
          key: normalizedKey,
          value: encoded.value,
          encoding: encoded.encoding,
          version,
          deleted: false,
        };
        if (txn) {
          stageMemoryTxnWrite(txn, record);
        } else {
          storageState.loadedKeys.add(normalizedKey);
          storageState.mirror.set(normalizedKey, record);
          queueMemoryMutation(entry, currentStorageRequestId(), record);
        }
        return {
          ok: true,
          conflict: false,
          version,
        };
      },
      delete(key, options = {}) {
        const storageState = ensureMemoryStorageState(entry);
        if (storageState.failedError) {
          throw storageState.failedError;
        }
        const normalizedKey = String(key);
        const expectedInput = options?.expectedVersion;
        const expectedVersion = Number.isFinite(Number(expectedInput))
          ? Math.trunc(Number(expectedInput))
          : -1;
        const current = txn
          ? memoryTxnReadRecord(txn, normalizedKey)
          : storageState.mirror.get(normalizedKey);
        const currentVersion = current ? Number(current.version ?? -1) : -1;
        if (expectedVersion >= 0 && currentVersion !== expectedVersion) {
          return {
            ok: true,
            conflict: true,
            version: currentVersion,
          };
        }
        const version = txn
          ? memoryTxnNextVersion(txn)
          : Number(storageState.committedVersion ?? -1);
        const record = {
          key: normalizedKey,
          value: new Uint8Array(),
          encoding: "utf8",
          version,
          deleted: true,
        };
        if (txn) {
          stageMemoryTxnWrite(txn, record);
        } else {
          storageState.loadedKeys.add(normalizedKey);
          storageState.mirror.set(normalizedKey, record);
          queueMemoryMutation(entry, currentStorageRequestId(), record);
        }
        return {
          ok: true,
          conflict: false,
          version,
        };
      },
      list(options = {}) {
        const storageState = ensureMemoryStorageState(entry);
        if (storageState.failedError) {
          throw storageState.failedError;
        }
        if (txn && !storageState.fullSnapshotLoaded) {
          throw new MemoryHydrationNeeded("full");
        }
        const prefix = String(options?.prefix ?? "");
        const limitInput = Number(options?.limit ?? 100);
        const limit = Number.isFinite(limitInput)
          ? Math.max(1, Math.min(1000, Math.trunc(limitInput)))
          : 100;
        const merged = new Map(storageState.mirror);
        if (txn) {
          for (const [key, record] of txn.writes.entries()) {
            merged.set(key, cloneMemoryRecord(record));
          }
          if (options?.allowConcurrency !== true) {
            txn.listGateVersion = Number(storageState.committedVersion ?? -1);
          }
        }
        return Array.from(merged.values())
          .filter((record) => !record.deleted)
          .filter((record) => record.encoding === "utf8")
          .filter((record) => record.key.startsWith(prefix))
          .sort((left, right) => left.key.localeCompare(right.key))
          .slice(0, limit)
          .map((record) => ({
            key: record.key,
            value: Deno.core.decode(toArrayBytes(record.value)),
            version: Number(record.version ?? -1),
          }));
      },
    };
  };
