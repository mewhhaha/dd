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
      value: new Uint8Array(core.serialize(value, { forStorage: true })),
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
      return core.decode(bytes);
    }
    if (encoding === "v8sc") {
      return core.deserialize(bytes, { forStorage: true });
    }
    throw new Error(`memory storage get unsupported encoding: ${encoding}`);
  };

  const ensureMemoryStorageState = (entry) => {
    if (!entry.storageState) {
      entry.storageState = {
        failedError: null,
        outputGate: Promise.resolve(),
      };
    }
    return entry.storageState;
  };

  const memoryTxnReadRecord = (txn, key) => {
    const result = callOp("op_memory_batch_get", txn.batchHandle, key);
    if (result.ok === false) {
      throw new Error(result.error);
    }
    return result.record;
  };

  const memoryTxnKeys = (txn, prefix) => {
    const result = callOp("op_memory_batch_keys", txn.batchHandle, prefix);
    if (result.ok === false) {
      throw new Error(result.error);
    }
    return result.keys;
  };

  const closeMemoryResourcesOnFailure = async (entry) => {
    const socketHandles = listMemorySocketHandles(
      entry.binding,
      entry.memoryKey,
      memoryScopedScopeHandle(entry),
    );
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
      } catch (error) {
        console.warn("memory socket close after storage failure failed", error);
      }
    }
  };

  const failMemoryEntry = async (entry, error) => {
    const storageState = ensureMemoryStorageState(entry);
    if (storageState.failedError) {
      throw storageState.failedError;
    }
    const failure = error instanceof Error ? error : new Error(String(error ?? "memory namespace failed"));
    storageState.failedError = failure;
    try {
      await closeMemoryResourcesOnFailure(entry);
    } catch (closeError) {
      console.warn("memory socket cleanup after storage failure failed", closeError);
    }
    throw failure;
  };

  const gateMemoryOutput = (entry, callback) => {
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

  const stageMemoryTxnEffect = (txn, kind, payload) => {
    requireMemoryBatchOp(
      callOp(
        "op_memory_batch_effect",
        txn.batchHandle,
        kind,
        toArrayBytes(payload),
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

  const createMemoryTxn = async (entry, commandHandle) => {
    const started = performance.now();
    const result = await callOp(
      "op_memory_batch_begin",
      activeRequestContextHandle(),
      memoryScopedScopeHandle(entry),
      entry.binding,
      entry.memoryKey,
      commandHandle,
    );
    await syncFrozenTime();
    if (!result || typeof result !== "object" || result.ok === false) {
      const error = new Error(String(result?.error ?? "memory batch begin failed"));
      if (result?.storage_failure === true) {
        return await failMemoryEntry(entry, error);
      }
      throw error;
    }
    recordMemoryProfile("js_txn_begin", performance.now() - started, 1);
    return { entry, batchHandle: result.handle };
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
        toArrayBytes(value),
      ),
      "command result",
    );
  };

  const addMemoryBatchMutation = (txn, mutation) => {
    const result = callOp(
      "op_memory_batch_mutation",
      txn.batchHandle,
      mutation.key,
      toArrayBytes(mutation.value),
      mutation.encoding,
      mutation.deleted === true,
    );
    if (!result || typeof result !== "object" || result.ok === false) {
      throw new Error(String(result?.error ?? "memory batch mutation failed"));
    }
  };

  const finishMemoryTxn = async (txn) => {
    if (!txn) {
      return;
    }
    const started = performance.now();
    const result = await callOp("op_memory_batch_apply", txn.batchHandle);
    await syncFrozenTime();
    if (!result || typeof result !== "object" || result.ok === false) {
      throw new Error(String(result?.error ?? "memory transaction commit failed"));
    }
    if (result.read_only === true || result.applied !== true) {
      recordMemoryProfile("js_read_only_commit", performance.now() - started, 1);
      return;
    }
    if (result.output_gate_required === true) {
      await gateMemoryOutput(txn.entry, async () => undefined);
    }
    recordMemoryProfile(
      "js_txn_commit",
      performance.now() - started,
      result.mutation_count + 1,
    );
  };


  const memoryStorageKey = (key) => String(key).toWellFormed();

  const compareMemoryKeys = (left, right) => {
    const order = left.localeCompare(right);
    if (order !== 0 || left === right) return order;
    // Match SQL's binary ordering for distinct keys with equal collation.
    const leftBytes = toUtf8Bytes(left);
    const rightBytes = toUtf8Bytes(right);
    for (let index = 0; index < Math.min(leftBytes.length, rightBytes.length); index++) {
      if (leftBytes[index] !== rightBytes[index]) return leftBytes[index] - rightBytes[index];
    }
    return leftBytes.length - rightBytes.length;
  };

  const createMemoryStorageBinding = (entry, txn) => {
    const rejectStorageOptions = (operation, options) => {
      if (
        options
        && typeof options === "object"
        && Object.keys(options).length > 0
      ) {
        throw new Error(`memory atomic ${operation} options are unsupported`);
      }
    };
    return {
      get(key, options = {}) {
        rejectStorageOptions("get", options);
        const storageState = ensureMemoryStorageState(entry);
        if (storageState.failedError) {
          throw storageState.failedError;
        }
        const normalizedKey = memoryStorageKey(key);
        const record = memoryTxnReadRecord(txn, normalizedKey);
        if (!record) {
          return null;
        }
        return {
          value: decodeMemoryStorageValue(record),
        };
      },
      put(key, value, options = {}) {
        rejectStorageOptions("set", options);
        const storageState = ensureMemoryStorageState(entry);
        if (storageState.failedError) {
          throw storageState.failedError;
        }
        const normalizedKey = memoryStorageKey(key);
        const encoded = encodeMemoryStorageValue(value);
        const record = {
          key: normalizedKey,
          value: encoded.value,
          encoding: encoded.encoding,
          deleted: false,
        };
        addMemoryBatchMutation(txn, record);
      },
      delete(key, options = {}) {
        rejectStorageOptions("delete", options);
        const storageState = ensureMemoryStorageState(entry);
        if (storageState.failedError) {
          throw storageState.failedError;
        }
        const normalizedKey = memoryStorageKey(key);
        const record = {
          key: normalizedKey,
          value: new Uint8Array(),
          encoding: "utf8",
          deleted: true,
        };
        addMemoryBatchMutation(txn, record);
      },
      list(options = {}) {
        const storageState = ensureMemoryStorageState(entry);
        if (storageState.failedError) {
          throw storageState.failedError;
        }
        const prefix = memoryStorageKey(options?.prefix ?? "");
        const limitInput = Number(options?.limit ?? 100);
        const limit = Number.isFinite(limitInput)
          ? Math.max(1, Math.min(1000, Math.trunc(limitInput)))
          : 100;
        return memoryTxnKeys(txn, prefix)
          .sort(compareMemoryKeys)
          .slice(0, limit)
          .map(key => ({ key, value: decodeMemoryStorageValue(memoryTxnReadRecord(txn, key)) }));
      },
    };
  };
