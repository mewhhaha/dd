const listMemorySocketHandles = (binding, memoryKey, scopeHandle = 0) => {
  const listed = op_memory_socket_list(scopeHandle, binding, memoryKey);
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

const createMemorySocketRuntime = (entry, allowSocketAccept, handles) => {
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
  let upgradeAccepted = false;
  const sockets = {
    __proto__: null,
    accept(request) {
      if (!allowSocketAccept) {
        throw new Error("state.sockets.accept is only available during a keyed memory request");
      }
      if (upgradeAccepted) {
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
      upgradeAccepted = true;
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

class MemoryAtomicState {
  #entry;
  #txn;
  #ensureSocketRuntime;
  #id;
  #sockets;
  constructor(entry, txn, ensureSocketRuntime) {
    this.#entry = entry;
    this.#txn = txn;
    this.#ensureSocketRuntime = ensureSocketRuntime;
    this.#id = null;
    this.#sockets = null;
  }
  #assertActive() {
    if (!this.#txn.callbackActive) {
      throw new Error("memory transaction is outside its synchronous callback");
    }
  }
  get id() {
    if (this.#id === null) {
      this.#id = createMemoryId(this.#entry.binding, this.#entry.memoryKey);
    }
    return this.#id;
  }
  get sockets() {
    if (this.#sockets === null) {
      const self = this;
      this.#sockets = ObjectFreeze({
        values() {
          self.#assertActive();
          return self.#ensureSocketRuntime().sockets.values();
        },
        send(handle, payload) {
          self.#assertActive();
          stageMemoryTxnEffect(
            self.#txn,
            "socket.send",
            encodeMemorySocketSendEffect(String(handle), encodeSocketSendPayload(payload)),
          );
        },
        close(handle, code = 1000, reason = "") {
          self.#assertActive();
          stageMemoryTxnEffect(
            self.#txn,
            "socket.close",
            encodeMemoryCloseEffect(String(handle), code, reason),
          );
        },
      });
    }
    return this.#sockets;
  }
  get(key, options = undefined) {
    this.#assertActive();
    if (options !== undefined) rejectMemoryStorageOptions("get", options);
    const storageState = this.#entry.storageState;
    if (storageState?.failedError) {
      throw storageState.failedError;
    }
    const normalizedKey = memoryStorageKey(key);
    const record = memoryTxnReadRecord(this.#txn, normalizedKey);
    return record ? decodeMemoryStorageValue(record) : null;
  }
  put(key, value, options = undefined) {
    this.#assertActive();
    if (options !== undefined) rejectMemoryStorageOptions("set", options);
    const storageState = this.#entry.storageState;
    if (storageState?.failedError) {
      throw storageState.failedError;
    }
    const normalizedKey = memoryStorageKey(key);
    if (typeof value === "string") {
      op_memory_batch_write_utf8(this.#txn.batchHandle, normalizedKey, value);
      return;
    }
    const encoded = encodeMemoryStorageValue(value);
    addMemoryBatchMutation(this.#txn, normalizedKey, encoded.value, encoded.encoding, false);
  }
  delete(key, options = undefined) {
    this.#assertActive();
    if (options !== undefined) rejectMemoryStorageOptions("delete", options);
    const storageState = this.#entry.storageState;
    if (storageState?.failedError) {
      throw storageState.failedError;
    }
    const normalizedKey = memoryStorageKey(key);
    const existed = memoryTxnReadRecord(this.#txn, normalizedKey) !== null;
    addMemoryBatchMutation(this.#txn, normalizedKey, EMPTY_BYTES, "utf8", true);
    return existed;
  }
  list(options = undefined) {
    this.#assertActive();
    const storageState = this.#entry.storageState;
    if (storageState?.failedError) {
      throw storageState.failedError;
    }
    const prefix = memoryStorageKey(options?.prefix ?? "");
    const txn = this.#txn;
    return listMemoryEntries(
      memoryTxnKeys(txn, prefix),
      memoryListLimit(options),
      (k) => decodeMemoryStorageValue(memoryTxnReadRecord(txn, k)),
    );
  }
  emit(kind, payload = null) {
    this.#assertActive();
    const normalizedKind = StringPrototypeTrim(String(kind ?? ""));
    if (!normalizedKind) {
      throw new Error("state.emit(kind, payload) requires a non-empty kind");
    }
    stageMemoryTxnEffect(
      this.#txn,
      normalizedKind,
      toBytes(JSONStringify(payload ?? null)),
    );
  }
  accept(request) {
    this.#assertActive();
    const accepted = this.#ensureSocketRuntime().sockets.accept(request);
    markMemoryBatchAccepted(this.#txn);
    const response = new Response(
      accepted.response.body ?? null,
      new ResponseInit(accepted.response.status ?? 101, undefined),
    );
    appendHeaderPairs(
      ResponsePrototypeGetHeaders(response),
      headerPairs(accepted.response.headers),
    );
    return {
      handle: accepted.handle,
      response,
    };
  }
}

const createMemoryAtomicState = (
  entry,
  txn,
  ensureSocketRuntime,
) => ObjectFreeze(new MemoryAtomicState(entry, txn, ensureSocketRuntime));

class MemoryId {
  constructor(bindingName, memoryKey) {
    this.__dd_memory_key = memoryKey;
    this.__dd_memory_binding = bindingName;
  }
  toString() {
    return this.__dd_memory_key;
  }
}

const createMemoryId = (bindingName, memoryKey) => new MemoryId(bindingName, memoryKey);

const ensureMemoryEntry = (binding, memoryKey) => {
  const current = currentRequestContext(false)?.memoryEntry;
  if (current?.binding === binding && current.memoryKey === memoryKey) return current;
  return new MemoryEntry(binding, memoryKey);
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
