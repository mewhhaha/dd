  const createActorStub = (namespace, actorKey) => Object.freeze({
    id: createActorId(namespace, actorKey),
    binding: namespace,
    sockets: createActorStubSocketApi(namespace, actorKey),
    transports: createActorStubTransportApi(namespace, actorKey),
    read(key, options) {
      return createActorStubVarApi(namespace, actorKey, key).read(options);
    },
    async write(key, value) {
      const runtimeRequestId = activeRequestId();
      const entry = await ensureActorEntry(namespace, actorKey, runtimeRequestId, { hydrate: false });
      const state = createActorStorageBinding(entry, runtimeRequestId);
      state.put(String(key ?? ""), value);
      await ensureActorStorageQueued(entry, runtimeRequestId);
      return value;
    },
    async delete(key) {
      const runtimeRequestId = activeRequestId();
      const entry = await ensureActorEntry(namespace, actorKey, runtimeRequestId, { hydrate: false });
      const state = createActorStorageBinding(entry, runtimeRequestId);
      state.delete(String(key ?? ""));
      await ensureActorStorageQueued(entry, runtimeRequestId);
      return true;
    },
    async writeMany(entries) {
      const runtimeRequestId = activeRequestId();
      const entry = await ensureActorEntry(namespace, actorKey, runtimeRequestId, { hydrate: false });
      const state = createActorStorageBinding(entry, runtimeRequestId);
      const normalizedEntries = normalizeActorWriteManyEntries(entries);
      for (const item of normalizedEntries) {
        state.put(item.key, item.value);
      }
      await ensureActorStorageQueued(entry, runtimeRequestId);
      return normalizedEntries.length;
    },
    async list(options = {}) {
      const scope = actorTxnScopeFor(namespace, actorKey);
      if (scope) {
        return scope.state.list(options);
      }
      const runtimeRequestId = activeRequestId();
      const entry = await ensureActorEntry(namespace, actorKey, runtimeRequestId, { hydrate: false });
      const storageState = ensureActorStorageState(entry);
      if (
        !storageState.fullSnapshotLoaded
        || storageState.stale === true
        || actorSnapshotTtlExpired(storageState)
      ) {
        await ensureActorStorageHydrated(entry, runtimeRequestId, {
          force: storageState.fullSnapshotLoaded,
        });
      } else {
        recordActorProfile("actor_cache_hit", 0, 1);
      }
      return createActorStorageBinding(entry, runtimeRequestId).list(options).map((entryValue) => ({
        key: entryValue.key,
        value: entryValue.value,
        version: entryValue.version,
      }));
    },
    var(key) {
      return createActorStubVarApi(namespace, actorKey, key);
    },
    tvar(key, defaultValue = null) {
      return createActorStubVarApi(namespace, actorKey, key, defaultValue);
    },
    defer(callback) {
      const scope = actorTxnScopeFor(namespace, actorKey);
      if (!scope) {
        throw new Error("stub.defer(callback) requires stub.atomic(...)");
      }
      return scope.state.defer(callback);
    },
    accept(request, options) {
      const scope = actorTxnScopeFor(namespace, actorKey);
      if (!scope) {
        throw new Error("stub.accept(request) requires stub.atomic(...)");
      }
      return scope.state.accept(request, options);
    },
    async atomic(executable, ...args) {
      return await invokeActorExecutable(
        namespace,
        actorKey,
        ACTOR_ATOMIC_METHOD,
        executable,
        args,
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

  const invokeActorExecutable = async (
    namespace,
    actorKey,
    methodName,
    executable,
    args,
  ) => {
      const scopedRequestId = activeRequestId();
      const localRuntimeRequestId = `${scopedRequestId}:actor-run:${nextActorInvokeSeq()}`;
      if (methodName === ACTOR_ATOMIC_METHOD && typeof executable === "function") {
        const entry = await ensureActorEntry(namespace, actorKey, localRuntimeRequestId, { hydrate: false });
        return await executeActorTransaction(
          entry,
          localRuntimeRequestId,
          executable,
          args,
        );
      }
      const descriptor = extractExecDescriptor(executable);
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
          "op_actor_invoke_method",
          {
            worker_name: workerName,
            binding: namespace,
            key: actorKey,
            method_name: methodName,
            prefer_caller_isolate: preferCallerIsolate,
            args: Array.from(argsBytes),
            caller_request_id: scopedRequestId,
            request_id: localRuntimeRequestId,
          },
        );
        await syncFrozenTime();
        if (!result || typeof result !== "object" || result.ok === false) {
          throw new Error(String(result?.error ?? "memory operation failed"));
        }
        return decodeRpcResult(toArrayBytes(result.value));
      } finally {
        if (descriptor.live_token) {
          liveActorExecutables.delete(String(descriptor.live_token));
        }
      }
    };

  const actorIdKey = (id) => {
    if (typeof id === "string") {
      return id;
    }
    if (
      id
      && typeof id === "object"
      && typeof id.__dd_actor_key === "string"
      && typeof id.__dd_actor_binding === "string"
    ) {
      return id.__dd_actor_key;
    }
    return "";
  };

  const createActorNamespace = (bindingName) => Object.freeze({
    idFromName(name) {
      const key = String(name ?? "").trim();
      if (!key) {
        throw new Error("memory idFromName requires a non-empty name");
      }
      return createActorId(bindingName, key);
    },
    get(id) {
      if (
        id
        && typeof id === "object"
        && "__dd_actor_binding" in id
        && String(id.__dd_actor_binding) !== bindingName
      ) {
        throw new Error(
          `memory id belongs to namespace ${String(id.__dd_actor_binding)}, not ${bindingName}`,
        );
      }
      const actorKey = actorIdKey(id).trim();
      if (!actorKey) {
        throw new Error("memory namespace get() requires a valid memory id");
      }
      return createActorStub(bindingName, actorKey);
    },
  });

  const splitDynamicEnvInput = (value) => {
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

