  const createMemoryStub = (namespace, memoryKey) => Object.freeze({
    id: createMemoryId(namespace, memoryKey),
    binding: namespace,
    sockets: createMemoryStubSocketApi(namespace, memoryKey),
    transports: createMemoryStubTransportApi(namespace, memoryKey),
    read(key, options) {
      return createMemoryStubVarApi(namespace, memoryKey, key).read(options);
    },
    async write(key, value, options) {
      rejectMemoryDirectOptions("set", options);
      const runtimeRequestId = activeRequestId();
      const entry = await ensureMemoryEntry(namespace, memoryKey, runtimeRequestId, { hydrate: false });
      const encoded = encodeMemoryStorageValue(value);
      await applyDirectMemoryMutations(entry, runtimeRequestId, [{
        key: String(key ?? ""),
        value: encoded.value,
        encoding: encoded.encoding,
        deleted: false,
      }]);
      return value;
    },
    async delete(key, options) {
      rejectMemoryDirectOptions("delete", options);
      const runtimeRequestId = activeRequestId();
      const entry = await ensureMemoryEntry(namespace, memoryKey, runtimeRequestId, { hydrate: false });
      await applyDirectMemoryMutations(entry, runtimeRequestId, [{
        key: String(key ?? ""),
        value: new Uint8Array(),
        encoding: "utf8",
        deleted: true,
      }]);
      return true;
    },
    async writeMany(entries) {
      const normalizedEntries = normalizeMemoryWriteManyEntries(entries);
      const runtimeRequestId = activeRequestId();
      const entry = await ensureMemoryEntry(namespace, memoryKey, runtimeRequestId, { hydrate: false });
      await applyDirectMemoryMutations(
        entry,
        runtimeRequestId,
        normalizedEntries.map((item) => {
          const encoded = encodeMemoryStorageValue(item.value);
          return {
            key: item.key,
            value: encoded.value,
            encoding: encoded.encoding,
            deleted: false,
          };
        }),
      );
      return normalizedEntries.length;
    },
    async list(options = {}) {
      const scope = memoryTxnScopeFor(namespace, memoryKey);
      if (scope) {
        return scope.state.list(options);
      }
      const runtimeRequestId = activeRequestId();
      const entry = await ensureMemoryEntry(namespace, memoryKey, runtimeRequestId, { hydrate: false });
      const storageState = ensureMemoryStorageState(entry);
      if (
        !storageState.fullSnapshotLoaded
        || storageState.stale === true
        || memorySnapshotTtlExpired(storageState)
      ) {
        await ensureMemoryStorageHydrated(entry, runtimeRequestId, {
          force: storageState.fullSnapshotLoaded,
        });
      } else {
        recordMemoryProfile("memory_cache_hit", 0, 1);
      }
      return createMemoryStorageBinding(entry, runtimeRequestId).list(options).map((entryValue) => ({
        key: entryValue.key,
        value: entryValue.value,
        version: entryValue.version,
      }));
    },
    var(key) {
      return createMemoryStubVarApi(namespace, memoryKey, key);
    },
    tvar(key, defaultValue = null) {
      return createMemoryStubVarApi(namespace, memoryKey, key, defaultValue);
    },
    emit(kind, payload = null) {
      const scope = memoryTxnScopeFor(namespace, memoryKey);
      if (!scope) {
        throw new Error("stub.emit(kind, payload) requires stub.atomic(...)");
      }
      return scope.state.emit(kind, payload);
    },
    accept(request, options) {
      const scope = memoryTxnScopeFor(namespace, memoryKey);
      if (!scope) {
        throw new Error("stub.accept(request) requires stub.atomic(...)");
      }
      return scope.state.accept(request, options);
    },
    async atomic(executableOrOptions, ...rawArgs) {
      const invocation = normalizeMemoryAtomicInvocation(executableOrOptions, rawArgs);
      return await invokeMemoryExecutable(
        namespace,
        memoryKey,
        MEMORY_ATOMIC_METHOD,
        invocation.executable,
        invocation.args,
        { idempotencyKey: invocation.idempotencyKey },
      );
    },
    async apply(effects) {
      if (effects == null) {
        return;
      }
      if (!Array.isArray(effects)) {
        throw new Error("stub.apply(effects) requires an array");
      }
      for (const effect of effects) {
        if (!effect || typeof effect !== "object") {
          continue;
        }
        const type = String(effect.type ?? "").trim();
        if (type === "socket.send") {
          const socket = new WebSocket(String(effect.handle ?? ""));
          socket.send(effect.payload, effect.kind == null ? undefined : String(effect.kind));
          continue;
        }
        if (type === "socket.close") {
          const socket = new WebSocket(String(effect.handle ?? ""));
          socket.close(Number(effect.code ?? 1000), String(effect.reason ?? ""));
          continue;
        }
        if (type === "transport.stream") {
          const session = await this.transports.session(String(effect.handle ?? ""));
          const writer = session.stream.writable.getWriter();
          try {
            await writer.write(effect.payload ?? new Uint8Array());
          } finally {
            writer.releaseLock();
          }
          continue;
        }
        if (type === "transport.datagram") {
          const session = await this.transports.session(String(effect.handle ?? ""));
          const writer = session.datagrams.writable.getWriter();
          try {
            await writer.write(effect.payload ?? new Uint8Array());
          } finally {
            writer.releaseLock();
          }
          continue;
        }
        if (type === "transport.close") {
          const session = await this.transports.session(String(effect.handle ?? ""));
          await session.close(Number(effect.code ?? 0), String(effect.reason ?? ""));
          continue;
        }
        throw new Error(`unsupported memory effect type: ${type}`);
      }
    },
  });

  const rejectMemoryDirectOptions = (operation, options) => {
    if (
      options
      && typeof options === "object"
      && Object.keys(options).length > 0
    ) {
      throw new Error(`memory ${operation} options are unsupported`);
    }
  };

  const normalizeMemoryAtomicInvocation = (first, args) => {
    if (
      first
      && typeof first === "object"
      && typeof first !== "function"
      && !Array.isArray(first)
    ) {
      const idempotencyKey = String(
        first.idempotencyKey ?? first.idempotency_key ?? "",
      ).trim();
      if (idempotencyKey.length > 512) {
        throw new Error("stub.atomic idempotencyKey must be at most 512 characters");
      }
      return {
        executable: args[0],
        args: args.slice(1),
        idempotencyKey,
      };
    }
    return {
      executable: first,
      args,
      idempotencyKey: "",
    };
  };

  const invokeMemoryExecutable = async (
    namespace,
    memoryKey,
    methodName,
    executable,
    args,
    options = {},
  ) => {
    const scopedRequestContextHandle = activeRequestContextHandle();
    const descriptor = extractExecDescriptor(executable);
    const idempotencyKey = String(options?.idempotencyKey ?? "").trim();
    if (idempotencyKey) {
      descriptor.idempotency_key = idempotencyKey;
    }
    const payload = [
      {
        ...descriptor,
      },
      ...args,
    ];
    const argsBytes = await encodeRpcArgs(payload);
    try {
      const preferCallerIsolate = !descriptor.export_name;
      const result = await callOp(
        "op_memory_invoke_method",
        scopedRequestContextHandle,
        workerName,
        namespace,
        memoryKey,
        methodName,
        preferCallerIsolate,
        argsBytes,
      );
      await syncFrozenTime();
      if (!result || typeof result !== "object" || result.ok === false) {
        throw new Error(String(result?.error ?? "memory operation failed"));
      }
      return decodeRpcResult(takeMemoryBytes(result));
    } finally {
      if (descriptor.live_token) {
        liveMemoryExecutables.delete(String(descriptor.live_token));
      }
    }
  };

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

  const splitDynamicEnvInput = (value, policy = null) => {
    if (value == null) {
      return {
        stringEnv: {},
        hostRpcBindings: [],
      };
    }
    if (typeof value !== "object" || Array.isArray(value)) {
      throw new Error("dynamic worker env must be an object");
    }
    const stringEnv = {};
    const hostRpcBindings = [];
    for (const [key, entry] of Object.entries(value)) {
      const name = String(key ?? "").trim();
      if (!name) {
        throw new Error("dynamic worker env key must not be empty");
      }
      if (isRpcTargetInstance(entry)) {
        if (!(policy && policy.allow_host_rpc === true)) {
          throw new Error(
            `dynamic worker env value for ${name} is RpcTarget, but allow_host_rpc is false`,
          );
        }
        const targetIdRaw = callOp("op_crypto_random_uuid");
        const targetId = String(targetIdRaw ?? "").trim();
        if (!targetId) {
          throw new Error("failed to allocate dynamic host rpc target id");
        }
        const methods = extractRpcTargetMethods(entry);
        hostRpcTargets.set(targetId, entry);
        hostRpcBindings.push({
          binding: name,
          target_id: targetId,
          methods,
        });
        continue;
      }
      if (entry != null && typeof entry === "object") {
        throw new Error(`dynamic worker env value for ${name} must be primitive or RpcTarget`);
      }
      stringEnv[name] = String(entry ?? "");
    }
    return {
      stringEnv,
      hostRpcBindings,
    };
  };
