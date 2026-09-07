  const createMemoryStub = (namespace, memoryKey) => Object.freeze({
    id: createMemoryId(namespace, memoryKey),
    binding: namespace,
    sockets: createMemoryStubSocketApi(namespace, memoryKey),
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
      if (Object.keys(options).some((key) => key !== "idempotencyKey")) {
        throw new Error("memory atomic accepts only the idempotencyKey option");
      }
      const context = { ...currentRequestContext() };
      return asyncContext.run(context, async () => {
        const lease = await callOp("op_memory_lease_acquire", activeRequestContextHandle(), namespace, memoryKey);
        if (!lease?.handle) {
          throw new Error(String(lease?.error ?? "memory lease is unavailable"));
        }
        context.memoryRequestScopeHandle = lease.handle;
        let command;
        try {
          const runtimeRequestId = activeRequestId();
          const entry = await ensureMemoryEntry(namespace, memoryKey, runtimeRequestId, { hydrate: false });
          context.memoryEntry = entry;
          context.memoryRequestId = runtimeRequestId;
          command = await beginMemoryCommand(entry, idempotencyKey);
          if (command.hit) {
            return decodeMemoryCommandResult(command.value);
          }
          return await executeMemoryTransaction(entry, runtimeRequestId, callback, command.handle);
        } finally {
          closeMemoryCommand(command?.handle);
          callOp("op_memory_request_scope_close", lease.handle);
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

  const createMemoryNamespace = (bindingName) => Object.freeze({
    idFromName(name) {
      const key = String(name ?? "").trim();
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
      const memoryKey = memoryIdKey(id).trim();
      if (!memoryKey) {
        throw new Error("memory namespace get() requires a valid memory id");
      }
      return createMemoryStub(bindingName, memoryKey);
    },
  });
