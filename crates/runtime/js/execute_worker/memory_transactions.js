  const INTERNAL_WS_ACCEPT_HEADER = "x-dd-ws-accept";
  const INTERNAL_WS_SESSION_HEADER = "x-dd-ws-session";
  const INTERNAL_WS_HANDLE_HEADER = "x-dd-ws-handle";
  const INTERNAL_WS_BINDING_HEADER = "x-dd-ws-memory-binding";
  const INTERNAL_WS_KEY_HEADER = "x-dd-ws-memory-key";

  const MEMORY_RUNTIME_EFFECT_VERSION = 1;

  const writeMemoryEffectU16 = (target, offset, value) => {
    const normalized = MathMax(0, MathMin(0xffff, MathTrunc(Number(value ?? 0) || 0)));
    target[offset] = (normalized >>> 8) & 0xff;
    target[offset + 1] = normalized & 0xff;
    return offset + 2;
  };

  const writeMemoryEffectU32 = (target, offset, value) => {
    const normalized = MathTrunc(Number(value ?? 0) || 0);
    if (!NumberIsFinite(normalized) || normalized < 0 || normalized > 0xffffffff) {
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
    offset = writeMemoryEffectU32(target, offset, byteLength(bytes));
    TypedArrayPrototypeSet(target, bytes, offset);
    return offset + byteLength(bytes);
  };

  const memoryEffectStringBytes = (value) => toBytes(String(value ?? ""));

  const encodeMemorySocketSendEffect = (handle, payload) => {
    const handleBytes = memoryEffectStringBytes(handle);
    const messageBytes = toArrayBytes(payload.value);
    const out = new Uint8Array(10 + byteLength(handleBytes) + byteLength(messageBytes));
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
    const out = new Uint8Array(11 + byteLength(handleBytes) + byteLength(reasonBytes));
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
      const normalizedKind = StringPrototypeToLowerCase(String(kind));
      if (normalizedKind === "text") {
        return {
          __proto__: null,
          kind: "text",
          value: toBytes(String(value)),
        };
      }
      if (normalizedKind === "binary") {
        return {
          __proto__: null,
          kind: "binary",
          value: toArrayBytes(value),
        };
      }
      throw new Error(`WebSocket(handle).send unsupported kind: ${kind}`);
    }
    if (isBinaryLike(value)) {
      return {
        __proto__: null,
        kind: "binary",
        value: toArrayBytes(value),
      };
    }
    return {
      __proto__: null,
      kind: "text",
      value: toBytes(String(value ?? "")),
    };
  };

  const encodeMemoryStorageValue = (value) => {
    if (typeof value === "string") {
      return {
        __proto__: null,
        encoding: "utf8",
        value: toBytes(value),
      };
    }
    return {
      __proto__: null,
      encoding: "v8sc",
      value: new Uint8Array(core.serialize(value, FOR_STORAGE)),
    };
  };

  const takeMemoryBytes = (record) => {
    const handle = toHandle(record?.value_handle);
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
      return core.deserialize(bytes, FOR_STORAGE);
    }
    throw new Error(`memory storage get unsupported encoding: ${encoding}`);
  };

  const ensureMemoryStorageState = (entry) => {
    if (!entry.storageState) {
      entry.storageState = {
        __proto__: null,
        failedError: null,
        outputGate: PromiseResolve(),
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
    for (let i = 0; i < socketHandles.length; i++) {
      try {
        const result = await callOp(
          "op_memory_socket_close",
          memoryScopedScopeHandle(entry),
          socketHandles[i],
          entry.binding,
          entry.memoryKey,
          1011,
          "memory storage flush failed",
        );
        await syncFrozenTime();
        if (result && typeof result === "object" && result.ok === false) {
          consoleWarn(String(result.error ?? "memory socket close failed"));
        }
      } catch (error) {
        consoleWarn("memory socket close after storage failure failed", error);
      }
    }
  };

  const failMemoryEntry = async (entry, error) => {
    const storageState = ensureMemoryStorageState(entry);
    if (storageState.failedError) {
      throw storageState.failedError;
    }
    const failure = ObjectPrototypeIsPrototypeOf(ErrorPrototype, error)
      ? error
      : new Error(String(error ?? "memory namespace failed"));
    storageState.failedError = failure;
    try {
      await closeMemoryResourcesOnFailure(entry);
    } catch (closeError) {
      consoleWarn("memory socket cleanup after storage failure failed", closeError);
    }
    throw failure;
  };

  // Runs `callback` once the entry's earlier output has gone out. The gate
  // itself never rejects.
  const gateMemoryOutput = (entry, callback) => {
    const storageState = ensureMemoryStorageState(entry);
    const previous = storageState.outputGate;
    const gated = (async () => {
      await previous;
      if (storageState.failedError) {
        throw storageState.failedError;
      }
      return await callback();
    })();
    storageState.outputGate = (async () => {
      try {
        await gated;
      } catch {
        // The caller sees the failure; the gate only orders output.
      }
    })();
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
    const normalizedKey = StringPrototypeTrim(String(idempotencyKey ?? ""));
    if (!normalizedKey) {
      return { __proto__: null, handle: 0, hit: false, value: null };
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
      __proto__: null,
      handle: toHandle(result.handle),
      hit: result.hit === true,
      value: result.hit === true ? takeMemoryBytes(result) : null,
    };
  };

  const closeMemoryCommand = (handle) => {
    const normalizedHandle = toHandle(handle);
    if (normalizedHandle > 0) {
      callOp("op_memory_command_close", normalizedHandle);
    }
  };

  const createMemoryTxn = async (entry, commandHandle) => {
    const started = frozenPerfNow();
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
    recordMemoryProfile("js_txn_begin", frozenPerfNow() - started, 1);
    return { __proto__: null, entry, batchHandle: result.handle };
  };

  const closeMemoryBatch = (handle) => {
    const normalizedHandle = toHandle(handle);
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
    const started = frozenPerfNow();
    const result = await callOp("op_memory_batch_apply", txn.batchHandle);
    await syncFrozenTime();
    if (!result || typeof result !== "object" || result.ok === false) {
      throw new Error(String(result?.error ?? "memory transaction commit failed"));
    }
    if (result.read_only === true || result.applied !== true) {
      recordMemoryProfile("js_read_only_commit", frozenPerfNow() - started, 1);
      return;
    }
    if (result.output_gate_required === true) {
      await gateMemoryOutput(txn.entry, async () => undefined);
    }
    recordMemoryProfile(
      "js_txn_commit",
      frozenPerfNow() - started,
      result.mutation_count + 1,
    );
  };


  const memoryStorageKey = (key) => StringPrototypeToWellFormed(String(key));

  const compareMemoryKeys = (left, right) => {
    const order = StringPrototypeLocaleCompare(left, right);
    if (order !== 0 || left === right) return order;
    // Match SQL's binary ordering for distinct keys with equal collation.
    const leftBytes = toBytes(left);
    const rightBytes = toBytes(right);
    const leftLength = TypedArrayPrototypeGetLength(leftBytes);
    const rightLength = TypedArrayPrototypeGetLength(rightBytes);
    for (let index = 0; index < MathMin(leftLength, rightLength); index++) {
      if (leftBytes[index] !== rightBytes[index]) return leftBytes[index] - rightBytes[index];
    }
    return leftLength - rightLength;
  };

  // The first `limit` of `keys` in key order, each with what `read` gives
  // for it.
  const listMemoryEntries = (keys, limit, read) => {
    const sorted = ArrayPrototypeSort(keys, compareMemoryKeys);
    const entries = [];
    for (let i = 0; i < sorted.length && i < limit; i++) {
      ArrayPrototypePush(entries, { key: sorted[i], value: read(sorted[i]) });
    }
    return entries;
  };

  const memoryListLimit = (options) => {
    const limitInput = Number(options?.limit ?? 100);
    return NumberIsFinite(limitInput)
      ? MathMax(1, MathMin(1000, MathTrunc(limitInput)))
      : 100;
  };

  const createMemoryStorageBinding = (entry, txn) => {
    const rejectStorageOptions = (operation, options) => {
      if (
        options
        && typeof options === "object"
        && ObjectKeys(options).length > 0
      ) {
        throw new Error(`memory atomic ${operation} options are unsupported`);
      }
    };
    return {
      __proto__: null,
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
          __proto__: null,
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
        addMemoryBatchMutation(txn, {
          __proto__: null,
          key: normalizedKey,
          value: encoded.value,
          encoding: encoded.encoding,
          deleted: false,
        });
      },
      delete(key, options = {}) {
        rejectStorageOptions("delete", options);
        const storageState = ensureMemoryStorageState(entry);
        if (storageState.failedError) {
          throw storageState.failedError;
        }
        const normalizedKey = memoryStorageKey(key);
        addMemoryBatchMutation(txn, {
          __proto__: null,
          key: normalizedKey,
          value: new Uint8Array(),
          encoding: "utf8",
          deleted: true,
        });
      },
      list(options = {}) {
        const storageState = ensureMemoryStorageState(entry);
        if (storageState.failedError) {
          throw storageState.failedError;
        }
        const prefix = memoryStorageKey(options?.prefix ?? "");
        return listMemoryEntries(
          memoryTxnKeys(txn, prefix),
          memoryListLimit(options),
          (key) => decodeMemoryStorageValue(memoryTxnReadRecord(txn, key)),
        );
      },
    };
  };
