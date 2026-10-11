const createMemoryStub = (namespace, memoryKey) => ObjectFreeze({
  id: createMemoryId(namespace, memoryKey),
  binding: namespace,
  sockets: createMemoryStubSocketApi(namespace, memoryKey),
  read(callback) {
    if (currentRequestContext().memoryTxnScope) {
      throw new Error("memory reads and atomic transactions cannot be nested");
    }
    if (typeof callback !== "function" || callback.constructor?.name === "AsyncFunction") {
      throw new Error("memory read requires a synchronous callback");
    }
    const context = copyRequestContext(currentRequestContext());
    return asyncContext.run(context, async () => {
      let readHandle;
      try {
        readHandle = await op_memory_read_begin(
          activeRequestContextHandle(), namespace, memoryKey,
        );
      } catch (error) {
        if (isMemoryStorageFailure(error)) {
          return await failMemoryEntry(ensureMemoryEntry(namespace, memoryKey), error);
        }
        throw error;
      }
      let active = true;
      const assertActive = () => {
        if (!active) throw new Error("memory snapshot is outside its synchronous callback");
      };
      const readSnapshotValue = (key) => decodeMemoryStorageValue(
        op_memory_read_get(readHandle, memoryStorageKey(key)),
      );
      const snapshot = ObjectFreeze({
        get(key) {
          assertActive();
          return readSnapshotValue(key);
        },
        list(options = {}) {
          assertActive();
          const prefix = memoryStorageKey(options?.prefix ?? "");
          const limit = memoryListLimit(options);
          return listMemoryEntries(op_memory_read_list(readHandle, prefix), limit, readSnapshotValue);
        },
      });
      try {
        await syncFrozenTime();
        let value;
        try {
          value = withMemoryTxnScope(
            new MemoryTxnScope(namespace, memoryKey, snapshot),
            () => callback(snapshot),
          );
        } finally {
          active = false;
        }
        if (value != null && (typeof value === "object" || typeof value === "function") && typeof value.then === "function") {
          throw new Error("stub.read callback must be synchronous");
        }
        return value;
      } finally {
        active = false;
        op_memory_read_close(readHandle);
      }
    });
  },
  atomic(callback, options = {}) {
    if (currentRequestContext().memoryTxnScope) {
      throw new Error("memory atomic transactions cannot be nested");
    }
    if (typeof callback !== "function" || callback.constructor?.name === "AsyncFunction") {
      throw new Error("memory atomic requires a synchronous callback");
    }
    const idempotencyKey = options.idempotencyKey ?? "";
    if (typeof idempotencyKey !== "string" || idempotencyKey.length > 512) {
      throw new Error("memory atomic idempotencyKey must be a string of at most 512 characters");
    }
    const optionKeys = ObjectKeys(options);
    for (let i = 0; i < optionKeys.length; i++) {
      if (optionKeys[i] !== "idempotencyKey") {
        throw new Error("memory atomic accepts only the idempotencyKey option");
      }
    }
    const context = copyRequestContext(currentRequestContext());
    return asyncContext.run(context, async () => {
      const leaseHandle = await op_memory_lease_acquire(
        activeRequestContextHandle(), namespace, memoryKey,
      );
      context.memoryRequestScopeHandle = leaseHandle;
      let command;
      try {
        const entry = ensureMemoryEntry(namespace, memoryKey);
        context.memoryEntry = entry;
        command = await beginMemoryCommand(entry, idempotencyKey);
        if (command.hit) {
          return decodeMemoryCommandResult(command.value);
        }
        return await executeMemoryTransaction(entry, callback, command.handle);
      } finally {
        closeMemoryCommand(command?.handle);
        op_memory_request_scope_close(leaseHandle);
      }
    });
  },
});

const memoryIdKey = (id) => {
  if (typeof id === "string") {
    return id;
  }
  if (
    id
    && typeof id === "object"
    && typeof id.__dd_memory_key === "string"
    && typeof id.__dd_memory_binding === "string"
  ) {
    return id.__dd_memory_key;
  }
  return "";
};

const createMemoryNamespace = (bindingName) => ObjectFreeze({
  idFromName(name) {
    const key = StringPrototypeTrim(String(name ?? ""));
    if (!key) {
      throw new Error("memory idFromName requires a non-empty name");
    }
    return createMemoryId(bindingName, key);
  },
  get(id) {
    if (
      id
      && typeof id === "object"
      && "__dd_memory_binding" in id
      && String(id.__dd_memory_binding) !== bindingName
    ) {
      throw new Error(
        `memory id belongs to namespace ${String(id.__dd_memory_binding)}, not ${bindingName}`,
      );
    }
    const memoryKey = StringPrototypeTrim(memoryIdKey(id));
    if (!memoryKey) {
      throw new Error("memory namespace get() requires a valid memory id");
    }
    return createMemoryStub(bindingName, memoryKey);
  },
});
