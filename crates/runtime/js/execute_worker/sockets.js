  const listMemorySocketHandles = (binding, memoryKey, scopeHandle = 0) => {
    const result = callOp("op_memory_socket_list", scopeHandle, binding, memoryKey);
    if (!result || typeof result !== "object" || result.ok === false) {
      throw new Error(String(result?.error ?? "socket values failed"));
    }
    return result.handles.map(String);
  };

  const createMemorySocketRuntime = (entry, { allowSocketAccept, handles }) => {
    let openHandles = handles === undefined ? null : new Set(handles.map(String));
    const loadOpenHandles = () => {
      openHandles ??= new Set(listMemorySocketHandles(
        entry.binding,
        entry.memoryKey,
        memoryScopedScopeHandle(entry),
      ));
      return openHandles;
    };
    const upgradeAccepted = { used: false };
    const sockets = {
      accept(request, options = {}) {
        const _ = options;
        if (!allowSocketAccept) {
          throw new Error("state.sockets.accept is only available during a keyed memory request");
        }
        if (upgradeAccepted.used) {
          throw new Error("state.sockets.accept can only be called once per request");
        }
        if (!(request instanceof Request)) {
          throw new Error("state.sockets.accept requires a Request");
        }
        const connection = String(request.headers.get("connection") ?? "");
        const upgrade = String(request.headers.get("upgrade") ?? "");
        const hasUpgrade = connection
          .split(",")
          .map((value) => value.trim().toLowerCase())
          .includes("upgrade");
        if (!hasUpgrade || upgrade.toLowerCase() !== "websocket") {
          throw new Error("state.sockets.accept requires a websocket upgrade request");
        }
        const sessionId = String(request.headers.get(INTERNAL_WS_SESSION_HEADER) ?? "").trim();
        if (!sessionId) {
          throw new Error("state.sockets.accept missing runtime websocket session metadata");
        }
        const handle = sessionId;
        const headers = new Headers();
        headers.set(INTERNAL_WS_ACCEPT_HEADER, "1");
        headers.set(INTERNAL_WS_SESSION_HEADER, sessionId);
        headers.set(INTERNAL_WS_HANDLE_HEADER, handle);
        headers.set(INTERNAL_WS_BINDING_HEADER, entry.binding);
        headers.set(INTERNAL_WS_KEY_HEADER, entry.memoryKey);
        upgradeAccepted.used = true;
        loadOpenHandles().add(handle);
        return {
          handle,
          response: {
            __dd_websocket_accept: true,
            status: 101,
            headers,
            body: null,
          },
        };
      },
      values() {
        return Array.from(loadOpenHandles());
      },
    };

    return {
      sockets,
      listOpenHandles() {
        return Array.from(loadOpenHandles());
      },
    };
  };

  const createMemoryAtomicState = (
    entry,
    runtimeRequestId,
    txn,
    socketRuntime,
  ) => {
    const storage = createMemoryStorageBinding(entry, runtimeRequestId, txn);
    const assertActive = () => {
      if (!txn.callbackActive) {
        throw new Error("memory transaction is outside its synchronous callback");
      }
    };
    return Object.freeze({
      id: createMemoryId(entry.binding, entry.memoryKey),
      get(key, options) {
        assertActive();
        return storage.get(key, options)?.value ?? null;
      },
      put(key, value, options) {
        assertActive();
        storage.put(key, value, options);
      },
      delete(key, options) {
        assertActive();
        const existed = storage.get(key) !== null;
        storage.delete(key, options);
        return existed;
      },
      list(options) {
        assertActive();
        return storage.list(options).map(({ key, value }) => ({ key, value }));
      },
      sockets: Object.freeze({
        values() {
          assertActive();
          return socketRuntime.sockets.values();
        },
        send(handle, payload) {
          assertActive();
          stageMemoryTxnEffect(txn, "socket.send", encodeMemorySocketSendEffect(String(handle), encodeSocketSendPayload(payload)));
        },
        close(handle, code = 1000, reason = "") {
          assertActive();
          stageMemoryTxnEffect(txn, "socket.close", encodeMemoryCloseEffect(String(handle), code, reason));
        },
      }),
      emit(kind, payload = null) {
        assertActive();
        const normalizedKind = String(kind ?? "").trim();
        if (!normalizedKind) {
          throw new Error("state.emit(kind, payload) requires a non-empty kind");
        }
        stageMemoryTxnEffect(
          txn,
          normalizedKind,
          toUtf8Bytes(JSON.stringify(payload ?? null)),
        );
      },
      accept(request, options = {}) {
        assertActive();
        const accepted = socketRuntime.sockets.accept(request, options);
        markMemoryBatchAccepted(txn);
        return {
          handle: accepted.handle,
          response: new Response(accepted.response.body ?? null, {
            status: accepted.response.status ?? 101,
            headers: accepted.response.headers ?? [],
          }),
        };
      },
    });
  };

  const memoryStateEntries = globalThis.__dd_memory_state_entries ??= new Map();

  const memoryEntryKey = (binding, memoryKey) => JSON.stringify([binding, memoryKey]);

  const createMemoryId = (bindingName, memoryKey) => ({
    __dd_memory_key: memoryKey,
    __dd_memory_binding: bindingName,
    toString() {
      return memoryKey;
    },
  });

  const ensureMemoryEntry = async (
    binding,
    memoryKey,
    hydrationRequestId,
    options = {},
  ) => {
    const cacheKey = memoryEntryKey(binding, memoryKey);
    let entry = memoryStateEntries.get(cacheKey);
    if (!entry) {
      entry = {
        binding,
        memoryKey,
        cacheKey,
      };
      memoryStateEntries.set(cacheKey, entry);
    }
    entry.binding = binding;
    entry.memoryKey = memoryKey;
    entry.cacheKey = cacheKey;
    if (options?.hydrate !== false) {
      await ensureMemoryStorageHydrated(entry, hydrationRequestId);
    }
    return entry;
  };

  const createMemoryStubSocketApi = (bindingName, memoryKey) => Object.freeze({
    async values() {
      const localRuntime = currentLocalSocketRuntime(bindingName, memoryKey);
      if (localRuntime) {
        return localRuntime.listOpenHandles();
      }
      const handles = listMemorySocketHandles(bindingName, memoryKey);
      await syncFrozenTime();
      return handles;
    },
  });
