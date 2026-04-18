  const createActorSocketRuntime = (entry, runtimeRequestId, allowSocketAccept) => {
    const socketsByHandle = entry.socketBindings ??= new Map();
    const openHandles = entry.openSocketHandles ??= new Set();
    const markOpenHandlesInitialized = () => {
      entry.openSocketHandlesInitialized = true;
    };
    const currentSocketRequestId = () => actorScopedRequestId(entry, runtimeRequestId);

    const sendSocketFrame = async (requestIdForOp, normalizedHandle, payload, gated = true) => {
      const perform = async () => {
        const result = await callOp("op_actor_socket_send", {
          request_id: requestIdForOp,
          binding: entry.binding,
          key: entry.actorKey,
          handle: normalizedHandle,
          message_kind: payload.kind,
          message: payload.value,
        });
        await syncFrozenTime();
        if (result && typeof result === "object" && result.ok === false) {
          throw new Error(String(result.error ?? "socket send failed"));
        }
      };
      if (gated) {
        await gateActorOutput(entry, requestIdForOp, perform);
        return;
      }
      await perform();
    };

    const closeSocketFrame = async (requestIdForOp, normalizedHandle, code, reason, gated = true) => {
      const perform = async () => {
        const result = await callOp("op_actor_socket_close", {
          request_id: requestIdForOp,
          binding: entry.binding,
          key: entry.actorKey,
          handle: normalizedHandle,
          code,
          reason,
        });
        await syncFrozenTime();
        if (result && typeof result === "object" && result.ok === false) {
          throw new Error(String(result.error ?? "socket close failed"));
        }
      };
      if (gated) {
        await gateActorOutput(entry, requestIdForOp, perform);
        return;
      }
      await perform();
    };

    const consumeCloseEvents = async (target) => {
      const result = await callOp("op_actor_socket_consume_close", {
        request_id: currentSocketRequestId(),
        binding: entry.binding,
        key: entry.actorKey,
        handle: target.__dd_handle,
      });
      await syncFrozenTime();
      if (result && typeof result === "object" && result.ok === false) {
        throw new Error(String(result.error ?? "socket consumeClose failed"));
      }
      const events = Array.isArray(result?.events) ? result.events : [];
      for (const event of events) {
        target.__dd_dispatchClose(
          Number(event?.code ?? 1000),
          String(event?.reason ?? ""),
        );
      }
    };

    const ensureSocket = (handle) => {
      const normalizedHandle = String(handle ?? "").trim();
      if (!normalizedHandle) {
        throw new Error("WebSocket(handle) requires a non-empty handle");
      }
      const existing = socketsByHandle.get(normalizedHandle);
      if (existing) {
        existing.__dd_bindRequestId(currentSocketRequestId());
        consumeCloseEvents(existing).catch(() => {});
        return existing;
      }

      const listeners = new Map();
      let onmessage = null;
      let onclose = null;
      const closeQueue = [];
      let closed = false;
      let boundRequestId = currentSocketRequestId();
      const target = {
        __dd_handle: normalizedHandle,
        __dd_bindRequestId(nextRequestId) {
          const normalizedRequestId = String(nextRequestId ?? "").trim();
          if (normalizedRequestId) {
            boundRequestId = normalizedRequestId;
          }
        },
        __dd_boundRequestId() {
          return boundRequestId;
        },
        binaryType: "arraybuffer",
        get readyState() {
          return closed ? 3 : 1;
        },
        addEventListener(type, listener) {
          const key = String(type ?? "");
          if (!listeners.has(key)) {
            listeners.set(key, new Set());
          }
          if (typeof listener === "function") {
            listeners.get(key).add(listener);
          }
          if (key === "close") {
            this.__dd_flushCloseQueue();
          }
        },
        removeEventListener(type, listener) {
          const key = String(type ?? "");
          if (!listeners.has(key)) {
            return;
          }
          listeners.get(key).delete(listener);
        },
        get onmessage() {
          return onmessage;
        },
        set onmessage(listener) {
          onmessage = typeof listener === "function" ? listener : null;
        },
        get onclose() {
          return onclose;
        },
        set onclose(listener) {
          onclose = typeof listener === "function" ? listener : null;
          this.__dd_flushCloseQueue();
        },
        async send(value, kind) {
          const payload = encodeSocketSendPayload(value, kind);
          const scope = actorTxnScopeFor(entry.binding, entry.actorKey);
          const perform = async () => {
            if (this.readyState !== 1) {
              return;
            }
            try {
              await sendSocketFrame(boundRequestId, normalizedHandle, payload, false);
            } catch {
              // Best-effort delivery; drop if the handle closed during flush.
            }
          };
          if (scope) {
            scope.state.defer(perform);
            return;
          }
          await perform();
        },
        async close(code, reason) {
          const normalizedCode = Number.isFinite(Number(code))
            ? Math.trunc(Number(code))
            : 1000;
          const perform = async () => {
            if (this.readyState !== 1) {
              return;
            }
            try {
              await closeSocketFrame(
                boundRequestId,
                normalizedHandle,
                normalizedCode,
                reason == null ? "" : String(reason),
                false,
              );
            } catch {
              // Best-effort close; ignore races with already-closed handles.
            }
          };
          const scope = actorTxnScopeFor(entry.binding, entry.actorKey);
          if (scope) {
            scope.state.defer(perform);
            return;
          }
          await perform();
        },
        async __dd_dispatch(type, event) {
          const current = listeners.get(type);
          if (current) {
            for (const listener of current) {
              try {
                await listener.call(this, event);
              } catch (error) {
                try {
                  console.warn(
                    "memory websocket listener failed",
                    String((error && (error.stack || error.message)) || error),
                  );
                } catch {
                  // Ignore logging failures.
                }
              }
            }
          }
        },
        async __dd_dispatchMessage(value) {
          this.__dd_bindRequestId(currentSocketRequestId());
          const event = { type: "message", data: value, target: this };
          await this.__dd_dispatch("message", event);
          if (typeof onmessage === "function") {
            try {
              await onmessage.call(this, event);
            } catch (error) {
              try {
                console.warn(
                  "memory websocket onmessage failed",
                  String((error && (error.stack || error.message)) || error),
                );
              } catch {
                // Ignore logging failures.
              }
            }
          }
        },
        __dd_dispatchClose(code, reason) {
          this.__dd_bindRequestId(currentSocketRequestId());
          const event = {
            type: "close",
            code: Number(code),
            reason: String(reason ?? ""),
            wasClean: true,
            target: this,
          };
          closeQueue.push(event);
          closed = true;
          openHandles.delete(normalizedHandle);
          this.__dd_flushCloseQueue();
        },
        __dd_flushCloseQueue() {
          if (closeQueue.length === 0) {
            return;
          }
          if (!onclose && !(listeners.get("close")?.size > 0)) {
            return;
          }
          while (closeQueue.length > 0) {
            const event = closeQueue.shift();
            this.__dd_dispatch("close", event);
            if (typeof onclose === "function") {
              try {
                onclose.call(this, event);
              } catch {
                // Ignore onclose failures.
              }
            }
          }
        },
      };
      socketsByHandle.set(normalizedHandle, target);
      consumeCloseEvents(target).catch(() => {});
      return target;
    };

    const WebSocketForActor = function WebSocket(handle) {
      return ensureSocket(handle);
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
        headers.set(INTERNAL_WS_KEY_HEADER, entry.actorKey);
        upgradeAccepted.used = true;
        openHandles.add(handle);
        markOpenHandlesInitialized();
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
        return Array.from(openHandles.values());
      },
      async send(handle, value, kind) {
        return ensureSocket(handle).send(value, kind);
      },
      async close(handle, code, reason) {
        return ensureSocket(handle).close(code, reason);
      },
    };

    return {
      sockets,
      WebSocket: WebSocketForActor,
      ensureSocket,
      async sendEffect(handle, value, kind) {
        const socket = ensureSocket(handle);
        const payload = encodeSocketSendPayload(value, kind);
        await sendSocketFrame(
          socket.__dd_boundRequestId(),
          socket.__dd_handle,
          payload,
          false,
        );
      },
      async closeEffect(handle, code, reason) {
        const socket = ensureSocket(handle);
        const normalizedCode = Number.isFinite(Number(code))
          ? Math.trunc(Number(code))
          : 1000;
        await closeSocketFrame(
          socket.__dd_boundRequestId(),
          socket.__dd_handle,
          normalizedCode,
          reason == null ? "" : String(reason),
          false,
        );
      },
      async refreshOpenHandles() {
        const result = await callOp("op_actor_socket_list", {
          request_id: currentSocketRequestId(),
          binding: entry.binding,
          key: entry.actorKey,
        });
        await syncFrozenTime();
        if (result && typeof result === "object" && result.ok === false) {
          throw new Error(String(result.error ?? "socket values failed"));
        }
        openHandles.clear();
        const handles = Array.isArray(result?.handles) ? result.handles : [];
        for (const value of handles) {
          openHandles.add(String(value));
        }
        markOpenHandlesInitialized();
      },
      listOpenHandles() {
        return Array.from(openHandles.values());
      },
      hasOpenHandleSnapshot() {
        return entry.openSocketHandlesInitialized === true;
      },
    };
  };

  const createTransportReadableChannel = () => {
    const queue = [];
    let controller = null;
    let closed = false;
    const readable = new ReadableStream({
      start(nextController) {
        controller = nextController;
        while (queue.length > 0) {
          controller.enqueue(queue.shift());
        }
        if (closed) {
          controller.close();
        }
      },
      cancel() {
        closed = true;
      },
    });
    return {
      readable,
      push(value) {
        if (closed) {
          return;
        }
        const bytes = toArrayBytes(value);
        if (controller) {
          controller.enqueue(bytes);
          return;
        }
        queue.push(bytes);
      },
      close() {
        closed = true;
        if (controller) {
          controller.close();
        }
      },
      error(error) {
        closed = true;
        if (controller) {
          controller.error(error);
        }
      },
    };
  };

  const createActorTransportRuntime = (entry, runtimeRequestId, allowTransportAccept) => {
    const transportsByHandle = entry.transportBindings ??= new Map();
    const openHandles = entry.openTransportHandles ??= new Set();
    const markOpenHandlesInitialized = () => {
      entry.openTransportHandlesInitialized = true;
    };
    const currentTransportRequestId = () => actorScopedRequestId(entry, runtimeRequestId);

    const consumeCloseEvents = async (target) => {
      const result = await callOp("op_actor_transport_consume_close", {
        request_id: currentTransportRequestId(),
        binding: entry.binding,
        key: entry.actorKey,
        handle: target.__dd_handle,
      });
      await syncFrozenTime();
      if (result && typeof result === "object" && result.ok === false) {
        throw new Error(String(result.error ?? "transport consumeClose failed"));
      }
      const events = Array.isArray(result?.events) ? result.events : [];
      for (const event of events) {
        target.__dd_dispatchClose(
          Number(event?.code ?? 0),
          String(event?.reason ?? ""),
        );
      }
    };

    const ensureTransport = (handle) => {
      const normalizedHandle = String(handle ?? "").trim();
      if (!normalizedHandle) {
        throw new Error("WebTransportSession(handle) requires a non-empty handle");
      }
      const existing = transportsByHandle.get(normalizedHandle);
      if (existing) {
        existing.__dd_bindRequestId(currentTransportRequestId());
        consumeCloseEvents(existing).catch(() => {});
        return existing;
      }

      const datagramReadable = createTransportReadableChannel();
      const streamReadable = createTransportReadableChannel();
      let boundRequestId = currentTransportRequestId();
      let closed = false;
      let closedResolved = false;
      let closedResolve = null;
      const closedPromise = new Promise((resolve) => {
        closedResolve = resolve;
      });

      const finishClosed = () => {
        if (closedResolved) {
          return;
        }
        closedResolved = true;
        closed = true;
        openHandles.delete(normalizedHandle);
        datagramReadable.close();
        streamReadable.close();
        if (typeof closedResolve === "function") {
          closedResolve(undefined);
        }
      };

      const sendTransportPayload = async (kind, value) => {
        const bytes = isBinaryLike(value) ? toArrayBytes(value) : toUtf8Bytes(value);
        const payload = Array.from(bytes);
        const body = kind === "datagram"
          ? {
            request_id: boundRequestId,
            handle: normalizedHandle,
            binding: entry.binding,
            key: entry.actorKey,
            datagram: payload,
          }
          : {
            request_id: boundRequestId,
            handle: normalizedHandle,
            binding: entry.binding,
            key: entry.actorKey,
            chunk: payload,
          };
        await gateActorOutput(entry, currentTransportRequestId(), async () => {
          const result = await callOpAny(
            [
              kind === "datagram"
                ? "op_actor_transport_send_datagram"
                : "op_actor_transport_send_stream",
              "op_actor_transport_send",
            ],
            body,
          );
          await syncFrozenTime();
          if (result && typeof result === "object" && result.ok === false) {
            throw new Error(String(result.error ?? `transport ${kind} send failed`));
          }
        });
      };

      const target = {
        __dd_handle: normalizedHandle,
        __dd_bindRequestId(nextRequestId) {
          const normalizedRequestId = String(nextRequestId ?? "").trim();
          if (normalizedRequestId) {
            boundRequestId = normalizedRequestId;
          }
        },
        get readyState() {
          return closed ? 3 : 1;
        },
        ready: Promise.resolve(undefined),
        closed: closedPromise,
        datagrams: {
          readable: datagramReadable.readable,
          writable: new WritableStream({
            write(chunk) {
              return sendTransportPayload("datagram", chunk);
            },
            close() {
              return undefined;
            },
            abort() {
              return undefined;
            },
          }),
        },
        stream: {
          readable: streamReadable.readable,
          writable: new WritableStream({
            write(chunk) {
              return sendTransportPayload("stream", chunk);
            },
            close() {
              return undefined;
            },
            abort() {
              return undefined;
            },
          }),
        },
        async close(code, reason) {
          const normalizedCode = Number.isFinite(Number(code))
            ? Math.trunc(Number(code))
            : 0;
          await gateActorOutput(entry, currentTransportRequestId(), async () => {
            const result = await callOpAny(
              [
                "op_actor_transport_close",
                "op_actor_transport_terminate",
              ],
              {
                request_id: currentTransportRequestId(),
                handle: normalizedHandle,
                binding: entry.binding,
                key: entry.actorKey,
                code: normalizedCode,
                reason: reason == null ? "" : String(reason),
              },
            );
            await syncFrozenTime();
            if (result && typeof result === "object" && result.ok === false) {
              throw new Error(String(result.error ?? "transport close failed"));
            }
          });
          finishClosed();
        },
        __dd_dispatchDatagram(value) {
          target.__dd_bindRequestId(currentTransportRequestId());
          datagramReadable.push(value);
        },
        __dd_dispatchStreamChunk(value) {
          target.__dd_bindRequestId(currentTransportRequestId());
          streamReadable.push(value);
        },
        __dd_dispatchClose(code, reason) {
          target.__dd_bindRequestId(currentTransportRequestId());
          if (closedResolved) {
            return;
          }
          finishClosed();
          target.__dd_lastClose = {
            code: Number(code),
            reason: String(reason ?? ""),
          };
        },
      };

      transportsByHandle.set(normalizedHandle, target);
      consumeCloseEvents(target).catch(() => {});
      return target;
    };

    const WebTransportSessionForActor = function WebTransportSession(handle) {
      return ensureTransport(handle);
    };

    const transportAccepted = { used: false };
    const transports = {
      accept(request, options = {}) {
        const _ = options;
        if (!allowTransportAccept) {
          throw new Error("state.transports.accept is only available during a keyed memory request");
        }
        if (transportAccepted.used) {
          throw new Error("state.transports.accept can only be called once per request");
        }
        if (!(request instanceof Request)) {
          throw new Error("state.transports.accept requires a Request");
        }
        const requestMethod = String(
          request.headers.get(INTERNAL_TRANSPORT_METHOD_HEADER)
          ?? request.method
          ?? "",
        ).toUpperCase();
        if (requestMethod !== "CONNECT") {
          throw new Error("state.transports.accept requires a CONNECT request");
        }
        const protocol = String(
          request.headers.get("protocol")
          ?? request.headers.get("x-dd-transport-protocol")
          ?? "",
        ).trim().toLowerCase();
        if (protocol && protocol !== "webtransport") {
          throw new Error("state.transports.accept requires a webtransport request");
        }
        const sessionId = String(
          request.headers.get(INTERNAL_TRANSPORT_SESSION_HEADER)
          ?? request.headers.get(INTERNAL_TRANSPORT_HANDLE_HEADER)
          ?? "",
        ).trim();
        if (!sessionId) {
          throw new Error("state.transports.accept missing runtime transport session metadata");
        }
        const handle = sessionId;
        const headers = new Headers();
        headers.set(INTERNAL_TRANSPORT_ACCEPT_HEADER, "1");
        headers.set(INTERNAL_TRANSPORT_SESSION_HEADER, sessionId);
        headers.set(INTERNAL_TRANSPORT_HANDLE_HEADER, handle);
        headers.set(INTERNAL_TRANSPORT_BINDING_HEADER, entry.binding);
        headers.set(INTERNAL_TRANSPORT_KEY_HEADER, entry.actorKey);
        transportAccepted.used = true;
        openHandles.add(handle);
        markOpenHandlesInitialized();
        return {
          handle,
          response: {
            __dd_transport_accept: true,
            status: 200,
            headers,
            body: null,
          },
        };
      },
      values() {
        return Array.from(openHandles.values());
      },
      session(handle) {
        return ensureTransport(handle);
      },
    };

    return {
      transports,
      WebTransportSession: WebTransportSessionForActor,
      ensureTransport,
      async refreshOpenHandles() {
        const result = await callOpAny([
          "op_actor_transport_list",
          "op_actor_transport_handles",
        ], {
          request_id: currentTransportRequestId(),
          binding: entry.binding,
          key: entry.actorKey,
        });
        await syncFrozenTime();
        if (result && typeof result === "object" && result.ok === false) {
          throw new Error(String(result.error ?? "transport values failed"));
        }
        openHandles.clear();
        const handles = Array.isArray(result?.handles)
          ? result.handles
          : Array.isArray(result?.values)
            ? result.values
            : [];
        for (const value of handles) {
          openHandles.add(String(value));
        }
        markOpenHandlesInitialized();
      },
      listOpenHandles() {
        return Array.from(openHandles.values());
      },
      hasOpenHandleSnapshot() {
        return entry.openTransportHandlesInitialized === true;
      },
    };
  };

  const createActorRuntimeState = (
    entry,
    runtimeRequestId,
    allowSocketAccept,
    allowTransportAccept,
    txn = null,
  ) => {
    const storage = createActorStorageBinding(entry, runtimeRequestId, txn);
    let socketRuntime = null;
    let transportRuntime = null;
    const ensureSocketRuntime = () => {
      if (!socketRuntime) {
        socketRuntime = createActorSocketRuntime(
          entry,
          runtimeRequestId,
          allowSocketAccept,
        );
        entry.socketRuntime = socketRuntime;
      }
      return socketRuntime;
    };
    const ensureTransportRuntime = () => {
      if (!transportRuntime) {
        transportRuntime = createActorTransportRuntime(
          entry,
          runtimeRequestId,
          allowTransportAccept,
        );
        entry.transportRuntime = transportRuntime;
      }
      return transportRuntime;
    };
    return {
      id: {
        toString() {
          return entry.actorKey;
        },
      },
      get(key, options) {
        return storage.get(key, options);
      },
      set(key, value, options) {
        return storage.put(key, value, options);
      },
      delete(key, options) {
        return storage.delete(key, options);
      },
      list(options) {
        return storage.list(options);
      },
      storage,
      get sockets() {
        return ensureSocketRuntime().sockets;
      },
      get transports() {
        return ensureTransportRuntime().transports;
      },
      get __dd_socket_runtime() {
        return ensureSocketRuntime();
      },
      get __dd_transport_runtime() {
        return ensureTransportRuntime();
      },
    };
  };

  const createActorAtomicState = (
    entry,
    runtimeRequestId,
    allowSocketAccept,
    allowTransportAccept,
    txn,
  ) => {
    const runtimeState = createActorRuntimeState(
      entry,
      runtimeRequestId,
      allowSocketAccept,
      allowTransportAccept,
      txn,
    );
    let stub = null;
    const ensureStub = () => {
      if (!stub) {
        stub = createActorStub(entry.binding, entry.actorKey);
      }
      return stub;
    };
    const isTransportRequest = (request) => {
      if (!(request instanceof Request)) {
        return false;
      }
      const requestMethod = String(
        request.headers.get(INTERNAL_TRANSPORT_METHOD_HEADER)
        ?? request.method
        ?? "",
      ).toUpperCase();
      return requestMethod === "CONNECT";
    };
    const createTxnVar = (key, defaultValue = null) => {
      const normalizedKey = String(key);
      return {
        key: normalizedKey,
        read(options) {
          const record = runtimeState.storage.get(normalizedKey, options);
          return record ? record.value : defaultValue;
        },
        write(value) {
          runtimeState.storage.put(normalizedKey, value);
          return value;
        },
        modify(updater) {
          if (typeof updater !== "function") {
            throw new Error("state.var(key).modify(updater) requires a function updater");
          }
          const record = runtimeState.storage.get(normalizedKey);
          const previous = record ? record.value : defaultValue;
          const next = updater(previous);
          runtimeState.storage.put(normalizedKey, next);
          return next;
        },
        delete() {
          return runtimeState.storage.delete(normalizedKey);
        },
      };
    };
    return {
      id: runtimeState.id,
      get stub() {
        return ensureStub();
      },
      storage: runtimeState.storage,
      __dd_socket_runtime: runtimeState.__dd_socket_runtime,
      __dd_transport_runtime: runtimeState.__dd_transport_runtime,
      get(key, options) {
        return runtimeState.storage.get(key, options)?.value ?? null;
      },
      set(key, value) {
        runtimeState.storage.put(key, value);
        return value;
      },
      put(key, updater) {
        if (typeof updater !== "function") {
          throw new Error("state.put(key, updater) requires a function updater");
        }
        const previous = runtimeState.storage.get(key)?.value ?? null;
        const next = updater(previous);
        runtimeState.storage.put(key, next);
        return next;
      },
      delete(key) {
        return runtimeState.storage.delete(key);
      },
      list(options) {
        return runtimeState.storage.list(options).map((entryValue) => ({
          key: entryValue.key,
          value: entryValue.value,
          version: entryValue.version,
        }));
      },
      var(key) {
        return createTxnVar(key);
      },
      tvar(key, defaultValue = null) {
        return createTxnVar(key, defaultValue);
      },
      defer(callback) {
        if (!txn) {
          throw new Error("state.defer(callback) requires an active transaction");
        }
        if (typeof callback !== "function") {
          throw new Error("state.defer(callback) requires a function");
        }
        txn.deferred.push(callback);
        return callback;
      },
      accept(request, options = {}) {
        if (txn) {
          txn.accepted = true;
        }
        if (isTransportRequest(request)) {
          const accepted = runtimeState.transports.accept(request, options);
          return {
            handle: accepted.handle,
            response: new Response(accepted.response.body ?? null, {
              status: accepted.response.status ?? 200,
              headers: accepted.response.headers ?? [],
            }),
          };
        }
        const accepted = runtimeState.sockets.accept(request, options);
        return {
          handle: accepted.handle,
          response: new Response(accepted.response.body ?? null, {
            status: accepted.response.status ?? 101,
            headers: accepted.response.headers ?? [],
          }),
        };
      },
    };
  };

  const createActorStateFacade = (stateRef) => {
    const facade = {
      id: {
        toString() {
          return stateRef.current.id.toString();
        },
      },
      storage: {
        get(...args) {
          return stateRef.current.storage.get(...args);
        },
        put(...args) {
          return stateRef.current.storage.put(...args);
        },
        delete(...args) {
          return stateRef.current.storage.delete(...args);
        },
        list(...args) {
          return stateRef.current.storage.list(...args);
        },
      },
      get(...args) {
        return stateRef.current.get(...args);
      },
      set(...args) {
        return stateRef.current.set(...args);
      },
      delete(...args) {
        return stateRef.current.delete(...args);
      },
      list(...args) {
        return stateRef.current.list(...args);
      },
      sockets: {
        accept(...args) {
          return stateRef.current.sockets.accept(...args);
        },
        values(...args) {
          return stateRef.current.sockets.values(...args);
        },
      },
      transports: {
        accept(...args) {
          return stateRef.current.transports.accept(...args);
        },
        values(...args) {
          return stateRef.current.transports.values(...args);
        },
      },
    };
    Object.defineProperty(facade, "__dd_socket_runtime", {
      get() {
        return stateRef.current.__dd_socket_runtime;
      },
    });
    Object.defineProperty(facade, "__dd_transport_runtime", {
      get() {
        return stateRef.current.__dd_transport_runtime;
      },
    });
    return facade;
  };

  const actorStateEntries = globalThis.__dd_actor_state_entries ??= new Map();
  const liveActorExecutables = globalThis.__dd_live_actor_executables ??= new Map();
  let liveActorExecutableSeq = globalThis.__dd_live_actor_executable_seq ?? 0;

  const ACTOR_ATOMIC_METHOD = "__dd_atomic";

  const actorMethodNameIsBlocked = (name) => (
    !name
    || name === "constructor"
    || name === "fetch"
    || name === "then"
  );

  const actorMethodNameIsBlockedForProxy = (name) => (
    actorMethodNameIsBlocked(name)
  );

  const hostRpcMethodNameIsBlocked = (name) => (
    !name
    || name === "constructor"
    || name === "fetch"
    || name === "then"
    || name.startsWith("__dd_")
  );

  const isRpcTargetInstance = (value) => (
    value != null
    && typeof value === "object"
    && typeof RpcTarget === "function"
    && value instanceof RpcTarget
  );

  const extractRpcTargetMethods = (target) => {
    const methods = new Set();
    let proto = Object.getPrototypeOf(target);
    while (proto && proto !== Object.prototype) {
      for (const name of Object.getOwnPropertyNames(proto)) {
        if (hostRpcMethodNameIsBlocked(name)) {
          continue;
        }
        const descriptor = Object.getOwnPropertyDescriptor(proto, name);
        if (!descriptor || typeof descriptor.value !== "function") {
          continue;
        }
        methods.add(name);
      }
      if (proto === RpcTarget.prototype) {
        break;
      }
      proto = Object.getPrototypeOf(proto);
    }
    return Array.from(methods.values()).sort();
  };

  const createDynamicHostRpcNamespace = (bindingName) => new Proxy(Object.freeze({}), {
    get(_target, prop) {
      if (typeof prop === "symbol") {
        return undefined;
      }
      const methodName = String(prop);
      if (methodName === "then") {
        return undefined;
      }
      if (hostRpcMethodNameIsBlocked(methodName)) {
        throw new Error(`dynamic host rpc method is blocked: ${methodName}`);
      }
      return async (...args) => {
        const argsBytes = await encodeRpcArgs(args);
        const result = await awaitDynamicReply(
          `dynamic host rpc invoke (${bindingName}.${methodName})`,
          () => callOp("op_dynamic_host_rpc_invoke", {
            request_id: activeRequestId(),
            binding: bindingName,
            method_name: methodName,
            args: Array.from(argsBytes),
          }),
        );
        if (!result || typeof result !== "object" || result.ok === false) {
          throw new Error(formatDynamicFailure(`dynamic host rpc invoke failed: ${methodName}`, result));
        }
        return decodeRpcResult(toArrayBytes(result.value));
      };
    },
    set() {
      return false;
    },
    defineProperty() {
      return false;
    },
    deleteProperty() {
      return false;
    },
  });

  const actorEntryKey = (binding, actorKey) => `${binding}\u001f${actorKey}`;

  const createActorId = (bindingName, actorKey) => ({
    __dd_actor_key: actorKey,
    __dd_actor_binding: bindingName,
    toString() {
      return actorKey;
    },
  });

  const ensureActorEntry = async (
    binding,
    actorKey,
    hydrationRequestId,
    options = {},
  ) => {
    const cacheKey = actorEntryKey(binding, actorKey);
    let entry = actorStateEntries.get(cacheKey);
    if (!entry) {
      entry = {
        binding,
        actorKey,
        cacheKey,
        socketBindings: new Map(),
        openSocketHandles: new Set(),
        socketRuntime: null,
        transportBindings: new Map(),
        openTransportHandles: new Set(),
        transportRuntime: null,
      };
      actorStateEntries.set(cacheKey, entry);
    }
    entry.binding = binding;
    entry.actorKey = actorKey;
    entry.cacheKey = cacheKey;
    if (options?.hydrate !== false) {
      await ensureActorStorageHydrated(entry, hydrationRequestId);
    }
    return entry;
  };

  const registerLiveActorExecutable = (executable) => {
    liveActorExecutableSeq += 1;
    globalThis.__dd_live_actor_executable_seq = liveActorExecutableSeq;
    const token = `${activeRequestId()}:live-actor:${liveActorExecutableSeq}`;
    liveActorExecutables.set(token, executable);
    return token;
  };

  const extractExecDescriptor = (executable) => {
    if (typeof executable !== "function") {
      throw new Error("stub.atomic(fn, ...args) requires a function");
    }
    const workerModule = globalThis.__dd_worker_module;
    const exportName = String(executable.name ?? "").trim();
    const exported = exportName ? workerModule?.[exportName] : undefined;
    const source = String(executable);
    if (!exportName && !source.trim()) {
      throw new Error("stub.atomic(fn, ...args) requires a non-empty function body");
    }
    const liveToken = registerLiveActorExecutable(executable);
    if (exportName && exported === executable) {
      return { live_token: liveToken, export_name: exportName, source };
    }
    return { live_token: liveToken, source };
  };

  const createActorStubSocketApi = (bindingName, actorKey) => Object.freeze({
    values() {
      const localRuntime = currentLocalHandleRuntime(bindingName, actorKey, "socket");
      if (localRuntime) {
        return localRuntime.listOpenHandles();
      }
      return (async () => {
        const result = await callOp("op_actor_socket_list", {
          request_id: activeRequestId(),
          binding: bindingName,
          key: actorKey,
        });
        await syncFrozenTime();
        if (result && typeof result === "object" && result.ok === false) {
          throw new Error(String(result.error ?? "socket values failed"));
        }
        return Array.isArray(result?.handles)
          ? result.handles.map((value) => String(value))
          : [];
      })();
    },
  });

  const createActorStubTransportApi = (bindingName, actorKey) => Object.freeze({
    values() {
      const localRuntime = currentLocalHandleRuntime(bindingName, actorKey, "transport");
      if (localRuntime) {
        return localRuntime.listOpenHandles();
      }
      return (async () => {
        const result = await callOpAny([
          "op_actor_transport_list",
          "op_actor_transport_handles",
        ], {
          request_id: activeRequestId(),
          binding: bindingName,
          key: actorKey,
        });
        await syncFrozenTime();
        if (result && typeof result === "object" && result.ok === false) {
          throw new Error(String(result.error ?? "transport values failed"));
        }
        return Array.isArray(result?.handles)
          ? result.handles.map((value) => String(value))
          : Array.isArray(result?.values)
            ? result.values.map((value) => String(value))
            : [];
      })();
    },
    async session(handle) {
      const runtimeRequestId = activeRequestId();
      const entry = await ensureActorEntry(bindingName, actorKey, runtimeRequestId, { hydrate: false });
      const state = createActorRuntimeState(entry, runtimeRequestId, false, false);
      return state.transports.session(handle);
    },
  });

  const createActorStubVarApi = (bindingName, actorKey, key, defaultValue = null) => {
    const normalizedKey = String(key);
    const requireScope = (operation) => {
      const scope = actorTxnScopeFor(bindingName, actorKey);
      if (!scope) {
        throw new Error(`stub.var(${JSON.stringify(normalizedKey)}).${operation}() requires stub.atomic(...)`);
      }
      return scope;
    };
    return {
      key: normalizedKey,
      read(options) {
        const scope = actorTxnScopeFor(bindingName, actorKey);
        if (scope) {
          const record = scope.state.storage.get(normalizedKey, options);
          return record ? record.value : defaultValue;
        }
        return (async () => {
          const runtimeRequestId = activeRequestId();
          const entry = await ensureActorEntry(bindingName, actorKey, runtimeRequestId, { hydrate: false });
          const storageState = await ensureActorDirectReadReady(entry, runtimeRequestId, normalizedKey);
          const record = storageState.mirror.get(normalizedKey) ?? null;
          if (!record || record.deleted) {
            return defaultValue;
          }
          const value = decodeActorStorageValue(record);
          if (options && typeof options === "object" && options.withVersion === true) {
            return {
              value,
              version: Number(record.version ?? -1),
              encoding: String(record.encoding ?? "utf8"),
            };
          }
          return value;
        })();
      },
      write(value) {
        return requireScope("write").state.set(normalizedKey, value);
      },
      modify(updater) {
        return requireScope("modify").state.put(normalizedKey, updater);
      },
      delete() {
        return requireScope("delete").state.delete(normalizedKey);
      },
    };
  };

  const normalizeActorWriteManyEntries = (entries) => {
    if (!Array.isArray(entries)) {
      throw new Error("stub.writeMany(entries) requires an array");
    }
    return entries.map((entry) => {
      if (Array.isArray(entry)) {
        return {
          key: String(entry[0] ?? ""),
          value: entry[1],
        };
      }
      if (entry && typeof entry === "object") {
        return {
          key: String(entry.key ?? ""),
          value: entry.value,
        };
      }
      throw new Error("stub.writeMany entries must be [key, value] or { key, value }");
    });
  };

