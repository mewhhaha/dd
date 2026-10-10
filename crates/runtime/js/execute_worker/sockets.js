  const listMemorySocketHandles = (binding, memoryKey, scopeHandle = 0) => {
    const result = callOp("op_memory_socket_list", scopeHandle, binding, memoryKey);
    if (!result || typeof result !== "object" || result.ok === false) {
      throw new Error(String(result?.error ?? "socket values failed"));
    }
    const listed = result.handles;
    const handles = [];
    for (let i = 0; i < listed.length; i++) {
      ArrayPrototypePush(handles, String(listed[i]));
    }
    return handles;
  };

  // Whether a Connection header value lists the `upgrade` token.
  const hasUpgradeToken = (connection) => {
    let start = 0;
    for (;;) {
      const comma = StringPrototypeIndexOf(connection, ",", start);
      const end = comma === -1 ? connection.length : comma;
      const token = StringPrototypeToLowerCase(
        StringPrototypeTrim(StringPrototypeSlice(connection, start, end)),
      );
      if (token === "upgrade") {
        return true;
      }
      if (comma === -1) {
        return false;
      }
      start = comma + 1;
    }
  };

  const createMemorySocketRuntime = (entry, options) => {
    const { allowSocketAccept, handles } = options;
    let openHandles = null;
    if (handles !== undefined) {
      openHandles = new SafeSet();
      for (let i = 0; i < handles.length; i++) {
        openHandles.add(String(handles[i]));
      }
    }
    const loadOpenHandles = () => {
      openHandles ??= new SafeSet(listMemorySocketHandles(
        entry.binding,
        entry.memoryKey,
        memoryScopedScopeHandle(entry),
      ));
      return openHandles;
    };
    const upgradeAccepted = { __proto__: null, used: false };
    const sockets = {
      __proto__: null,
      accept(request) {
        if (!allowSocketAccept) {
          throw new Error("state.sockets.accept is only available during a keyed memory request");
        }
        if (upgradeAccepted.used) {
          throw new Error("state.sockets.accept can only be called once per request");
        }
        if (!ObjectPrototypeIsPrototypeOf(RequestPrototype, request)) {
          throw new Error("state.sockets.accept requires a Request");
        }
        const requestHeaders = RequestPrototypeGetHeaders(request);
        const connection = String(HeadersPrototypeGet(requestHeaders, "connection") ?? "");
        const upgrade = String(HeadersPrototypeGet(requestHeaders, "upgrade") ?? "");
        if (!hasUpgradeToken(connection) || StringPrototypeToLowerCase(upgrade) !== "websocket") {
          throw new Error("state.sockets.accept requires a websocket upgrade request");
        }
        const sessionId = StringPrototypeTrim(
          String(HeadersPrototypeGet(requestHeaders, INTERNAL_WS_SESSION_HEADER) ?? ""),
        );
        if (!sessionId) {
          throw new Error("state.sockets.accept missing runtime websocket session metadata");
        }
        const handle = sessionId;
        const headers = new Headers();
        HeadersPrototypeSet(headers, INTERNAL_WS_ACCEPT_HEADER, "1");
        HeadersPrototypeSet(headers, INTERNAL_WS_SESSION_HEADER, sessionId);
        HeadersPrototypeSet(headers, INTERNAL_WS_HANDLE_HEADER, handle);
        HeadersPrototypeSet(headers, INTERNAL_WS_BINDING_HEADER, entry.binding);
        HeadersPrototypeSet(headers, INTERNAL_WS_KEY_HEADER, entry.memoryKey);
        upgradeAccepted.used = true;
        loadOpenHandles().add(handle);
        return {
          __proto__: null,
          handle,
          response: {
            __proto__: null,
            __dd_websocket_accept: true,
            status: 101,
            headers,
            body: null,
          },
        };
      },
      values() {
        return ArrayFrom(loadOpenHandles());
      },
    };

    return {
      __proto__: null,
      sockets,
      listOpenHandles() {
        return ArrayFrom(loadOpenHandles());
      },
    };
  };

  const createMemoryAtomicState = (
    entry,
    txn,
    socketRuntime,
  ) => {
    const storage = createMemoryStorageBinding(entry, txn);
    const assertActive = () => {
      if (!txn.callbackActive) {
        throw new Error("memory transaction is outside its synchronous callback");
      }
    };
    return ObjectFreeze({
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
        return storage.list(options);
      },
      sockets: ObjectFreeze({
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
        const normalizedKind = StringPrototypeTrim(String(kind ?? ""));
        if (!normalizedKind) {
          throw new Error("state.emit(kind, payload) requires a non-empty kind");
        }
        stageMemoryTxnEffect(
          txn,
          normalizedKind,
          toBytes(JSONStringify(payload ?? null)),
        );
      },
      accept(request) {
        assertActive();
        const accepted = socketRuntime.sockets.accept(request);
        markMemoryBatchAccepted(txn);
        const response = new Response(accepted.response.body ?? null, {
          __proto__: null,
          status: accepted.response.status ?? 101,
        });
        appendHeaderPairs(
          ResponsePrototypeGetHeaders(response),
          headerPairs(accepted.response.headers),
        );
        return {
          handle: accepted.handle,
          response,
        };
      },
    });
  };

  const createMemoryId = (bindingName, memoryKey) => ({
    __dd_memory_key: memoryKey,
    __dd_memory_binding: bindingName,
    toString() {
      return memoryKey;
    },
  });

  const ensureMemoryEntry = (binding, memoryKey) => {
    const current = currentRequestContext(false)?.memoryEntry;
    if (current?.binding === binding && current.memoryKey === memoryKey) return current;
    return { __proto__: null, binding, memoryKey };
  };

  const createMemoryStubSocketApi = (bindingName, memoryKey) => ObjectFreeze({
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
