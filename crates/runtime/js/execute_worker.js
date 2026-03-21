(() => {
  const requestId = __REQUEST_ID__;
  const completionToken = __COMPLETION_TOKEN__;
  const workerName = __WORKER_NAME__;
  const kvBindingsConfig = __KV_BINDINGS_JSON__;
  const actorBindingsConfig = __ACTOR_BINDINGS_JSON__;
  const hasRequestBodyStream = __HAS_REQUEST_BODY_STREAM__;
  const worker = globalThis.__dd_worker;

  if (worker === undefined) {
    throw new Error("Worker is not installed");
  }

  const inflightRequests = globalThis.__dd_inflight_requests ??= new Map();
  const input = __REQUEST_JSON__;
  const controller = new AbortController();
  const waitUntilPromises = [];
  let waitUntilDoneSent = false;
  let actorInvokeSeq = 0;

  inflightRequests.set(requestId, controller);

  const callOp = (name, ...args) => {
    const op = Deno?.core?.ops?.[name];
    if (typeof op !== "function") {
      return undefined;
    }
    return op(...args);
  };

  const sleep = (millis) => callOp("op_sleep", Number(millis) || 0);

  const normalizeBoundaryValue = (value) => {
    if (value == null) {
      return null;
    }

    if (typeof value === "number") {
      return { nowMs: value, perfMs: value };
    }

    if (Array.isArray(value)) {
      const [nowMs, perfMs = nowMs] = value;
      return { nowMs, perfMs };
    }

    if (typeof value === "object") {
      const nowMs =
        value.nowMs ?? value.now_ms ?? value.now ?? value.wallMs ?? value.wall_ms;
      const perfMs =
        value.perfMs ?? value.perf_ms ?? value.perf ?? value.monotonicMs ?? value.monotonic_ms ?? nowMs;
      return { nowMs, perfMs };
    }

    return null;
  };

  const syncFrozenTime = async () => {
    const value = await callOp("op_time_boundary_now");
    const boundary = normalizeBoundaryValue(value);
    if (boundary && typeof globalThis.__dd_set_time === "function") {
      globalThis.__dd_set_time(boundary.nowMs, boundary.perfMs);
    }
  };
  globalThis.__dd_sync_time_boundary = syncFrozenTime;
  globalThis.__dd_cache_bypass_stale = Array.isArray(input.headers)
    && input.headers.some(([name, value]) => {
      const key = String(name || "").toLowerCase();
      if (key !== "x-dd-cache-bypass-stale") {
        return false;
      }
      const normalized = String(value || "").toLowerCase();
      return normalized === "1" || normalized === "true" || normalized === "yes";
    });

  const toUtf8Bytes = (value) => {
    if (value == null) {
      return new Uint8Array();
    }
    if (value instanceof Uint8Array) {
      return value;
    }
    if (value instanceof ArrayBuffer) {
      return new Uint8Array(value);
    }
    if (ArrayBuffer.isView(value)) {
      return new Uint8Array(value.buffer.slice(value.byteOffset, value.byteOffset + value.byteLength));
    }
    return new TextEncoder().encode(String(value));
  };

  const createRequestBodyStream = () => {
    let released = false;
    let done = false;

    const read = async () => {
      if (released) {
        throw new TypeError("Reader has been released");
      }
      if (done) {
        return { value: undefined, done: true };
      }

      const payload = await callOp("op_request_body_read", requestId);
      await syncFrozenTime();
      if (!payload || typeof payload !== "object") {
        done = true;
        return { value: undefined, done: true };
      }
      if (payload.ok === false) {
        done = true;
        throw new Error(String(payload.error ?? "request body stream failed"));
      }
      if (payload.done === true) {
        done = true;
        return { value: undefined, done: true };
      }
      return {
        value: new Uint8Array(Array.isArray(payload.chunk) ? payload.chunk : []),
        done: false,
      };
    };

    return {
      getReader() {
        return {
          read,
          releaseLock() {
            released = true;
          },
          async cancel() {
            done = true;
            await callOp("op_request_body_cancel", requestId);
            await syncFrozenTime();
            return undefined;
          },
        };
      },
      [Symbol.asyncIterator]() {
        const reader = this.getReader();
        return {
          next: () => reader.read(),
          return: async () => {
            if (typeof reader.cancel === "function") {
              await reader.cancel();
            }
            if (typeof reader.releaseLock === "function") {
              reader.releaseLock();
            }
            return { done: true, value: undefined };
          },
        };
      },
    };
  };

  const decodeStoredValue = (encoding, rawValue, context) => {
    const bytes = new Uint8Array(Array.isArray(rawValue) ? rawValue : []);
    if (encoding === "utf8") {
      return Deno.core.decode(bytes);
    }
    if (encoding === "v8sc") {
      try {
        return Deno.core.deserialize(bytes, { forStorage: true });
      } catch (error) {
        throw new Error(`${context} deserialize failed: ${String(error?.message ?? error)}`);
      }
    }
    throw new Error(`${context} unsupported encoding: ${encoding}`);
  };

  const createKvBinding = (bindingName) => ({
    async get(key, options = {}) {
      const _ = options;
      const result = await callOp(
        "op_kv_get_value",
        JSON.stringify({
          worker_name: workerName,
          binding: bindingName,
          key: String(key),
        }),
      );
      await syncFrozenTime();
      if (result && typeof result === "object" && result.ok === false) {
        throw new Error(String(result.error ?? "kv get failed"));
      }
      if (result?.found !== true) {
        return null;
      }
      const encoding = String(result.encoding ?? "utf8");
      return decodeStoredValue(encoding, result.value, "kv get");
    },
    async set(key, value, options = {}) {
      const _ = options;
      if (typeof value === "string") {
        const result = await callOp(
          "op_kv_set",
          workerName,
          bindingName,
          String(key),
          value,
        );
        await syncFrozenTime();
        if (result && typeof result === "object" && result.ok === false) {
          throw new Error(String(result.error ?? "kv set failed"));
        }
        return;
      }
      let encoded;
      try {
        encoded = Deno.core.serialize(value, { forStorage: true });
      } catch (error) {
        throw new Error(`kv set serialize failed: ${String(error?.message ?? error)}`);
      }
      const result = await callOp(
        "op_kv_set_value",
        JSON.stringify({
          worker_name: workerName,
          binding: bindingName,
          key: String(key),
          encoding: "v8sc",
          value: Array.from(new Uint8Array(encoded)),
        }),
      );
      await syncFrozenTime();
      if (result && typeof result === "object" && result.ok === false) {
        throw new Error(String(result.error ?? "kv set failed"));
      }
    },
    async delete(key, options = {}) {
      const _ = options;
      const result = await callOp(
        "op_kv_delete",
        workerName,
        bindingName,
        String(key),
      );
      await syncFrozenTime();
      if (result && typeof result === "object" && result.ok === false) {
        throw new Error(String(result.error ?? "kv delete failed"));
      }
    },
    async list(options = {}) {
      const prefix = String(options?.prefix ?? "");
      const limitInput = Number(options?.limit ?? 100);
      const limit = Number.isFinite(limitInput)
        ? Math.max(1, Math.min(1000, Math.trunc(limitInput)))
        : 100;
      const result = await callOp(
        "op_kv_list",
        workerName,
        bindingName,
        prefix,
        limit,
      );
      await syncFrozenTime();
      if (result && typeof result === "object" && result.ok === false) {
        throw new Error(String(result.error ?? "kv list failed"));
      }
      const entries = Array.isArray(result?.entries) ? result.entries : [];
      return entries.map((entry) => {
        const encoding = String(entry?.encoding ?? "utf8");
        return {
          key: String(entry?.key ?? ""),
          value: decodeStoredValue(encoding, entry?.value, "kv list"),
          encoding,
        };
      });
    },
  });

  const toHeaderEntries = (headersInput) => {
    if (!headersInput) {
      return [];
    }
    try {
      return Array.from(new Headers(headersInput).entries());
    } catch {
      return [];
    }
  };

  const normalizeActorFetchInput = async (inputValue, initValue) => {
    let method = "GET";
    let url = "http://worker/";
    let headers = [];
    let body = new Uint8Array();

    if (inputValue instanceof Request) {
      method = String(inputValue.method || "GET").toUpperCase();
      url = String(inputValue.url || "http://worker/");
      headers = Array.from(inputValue.headers.entries());
      body = new Uint8Array(await inputValue.arrayBuffer());
    } else {
      method = String(initValue?.method ?? "GET").toUpperCase();
      const raw = String(inputValue ?? "/");
      url = raw.startsWith("http://") || raw.startsWith("https://")
        ? raw
        : new URL(raw, "http://worker").toString();
      headers = toHeaderEntries(initValue?.headers);
      body = toUtf8Bytes(initValue?.body);
    }

    if (inputValue instanceof Request && initValue) {
      if (initValue.method != null) {
        method = String(initValue.method).toUpperCase();
      }
      if (initValue.headers != null) {
        headers = toHeaderEntries(initValue.headers);
      }
      if (Object.prototype.hasOwnProperty.call(initValue, "body")) {
        body = toUtf8Bytes(initValue.body);
      }
    }

    return {
      method,
      url,
      headers,
      body,
    };
  };

  const createActorStorageBinding = (namespace, actorKey) => ({
    async get(key) {
      const result = await callOp(
        "op_actor_state_get_value",
        JSON.stringify({
          namespace,
          actor_key: actorKey,
          key: String(key),
        }),
      );
      await syncFrozenTime();
      if (result && typeof result === "object" && result.ok === false) {
        throw new Error(String(result.error ?? "actor storage get failed"));
      }
      if (result?.found !== true) {
        return null;
      }
      const encoding = String(result.encoding ?? "utf8");
      const bytes = new Uint8Array(Array.isArray(result.value) ? result.value : []);
      if (encoding === "utf8") {
        return {
          value: Deno.core.decode(bytes),
          version: Number(result.version ?? -1),
          encoding,
        };
      }
      if (encoding === "v8sc") {
        try {
          const decoded = Deno.core.deserialize(bytes, { forStorage: true });
          return {
            value: decoded,
            version: Number(result.version ?? -1),
            encoding,
          };
        } catch (error) {
          throw new Error(`actor storage get deserialize failed: ${String(error?.message ?? error)}`);
        }
      }
      throw new Error(`actor storage get unsupported encoding: ${encoding}`);
    },
    async put(key, value, options = {}) {
      const expectedInput = options?.expectedVersion;
      const expectedVersion = Number.isFinite(Number(expectedInput))
        ? Math.trunc(Number(expectedInput))
        : -1;
      if (typeof value === "string") {
        const result = await callOp(
          "op_actor_state_set",
          namespace,
          actorKey,
          String(key),
          value,
          expectedVersion,
        );
        await syncFrozenTime();
        if (result && typeof result === "object" && result.ok === false) {
          throw new Error(String(result.error ?? "actor storage put failed"));
        }
        return {
          ok: true,
          conflict: result?.conflict === true,
          version: Number(result?.version ?? -1),
        };
      }
      let encoded;
      try {
        encoded = Deno.core.serialize(value, { forStorage: true });
      } catch (error) {
        throw new Error(`actor storage put serialize failed: ${String(error?.message ?? error)}`);
      }
      const bytes = Array.from(new Uint8Array(encoded));
      const result = await callOp(
        "op_actor_state_set_value",
        JSON.stringify({
          namespace,
          actor_key: actorKey,
          key: String(key),
          encoding: "v8sc",
          value: bytes,
          expected_version: expectedVersion,
        }),
      );
      await syncFrozenTime();
      if (result && typeof result === "object" && result.ok === false) {
        throw new Error(String(result.error ?? "actor storage put failed"));
      }
      return {
        ok: true,
        conflict: result?.conflict === true,
        version: Number(result?.version ?? -1),
      };
    },
    async delete(key, options = {}) {
      const expectedInput = options?.expectedVersion;
      const expectedVersion = Number.isFinite(Number(expectedInput))
        ? Math.trunc(Number(expectedInput))
        : -1;
      const result = await callOp(
        "op_actor_state_delete",
        namespace,
        actorKey,
        String(key),
        expectedVersion,
      );
      await syncFrozenTime();
      if (result && typeof result === "object" && result.ok === false) {
        throw new Error(String(result.error ?? "actor storage delete failed"));
      }
      return {
        ok: true,
        conflict: result?.conflict === true,
        version: Number(result?.version ?? -1),
      };
    },
    async list(options = {}) {
      const prefix = String(options?.prefix ?? "");
      const limitInput = Number(options?.limit ?? 100);
      const limit = Number.isFinite(limitInput)
        ? Math.max(1, Math.min(1000, Math.trunc(limitInput)))
        : 100;
      const result = await callOp(
        "op_actor_state_list",
        namespace,
        actorKey,
        prefix,
        limit,
      );
      await syncFrozenTime();
      if (result && typeof result === "object" && result.ok === false) {
        throw new Error(String(result.error ?? "actor storage list failed"));
      }
      return Array.isArray(result?.entries) ? result.entries : [];
    },
  });

  const createActorBinding = (namespace, actorKey) => ({
    storage: createActorStorageBinding(namespace, actorKey),
    async fetch(inputValue, initValue = undefined) {
      const request = await normalizeActorFetchInput(inputValue, initValue);
      actorInvokeSeq += 1;
      const result = await callOp(
        "op_actor_invoke",
        JSON.stringify({
          worker_name: workerName,
          binding: namespace,
          key: actorKey,
          method: request.method,
          url: request.url,
          headers: request.headers,
          body: Array.from(request.body),
          request_id: `${requestId}:actor:${actorInvokeSeq}`,
        }),
      );
      await syncFrozenTime();
      if (!result || typeof result !== "object" || result.ok === false) {
        throw new Error(String(result?.error ?? "actor invoke failed"));
      }
      return new Response(new Uint8Array(Array.isArray(result.body) ? result.body : []), {
        status: Number(result.status ?? 200),
        headers: Array.isArray(result.headers) ? result.headers : [],
      });
    },
  });

  const buildEnv = () => {
    const env = {};
    const kvBindings = Array.isArray(kvBindingsConfig)
      ? kvBindingsConfig
      : kvBindingsConfig && typeof kvBindingsConfig === "object"
        ? Object.entries(kvBindingsConfig).map(([name, binding]) => [name, typeof binding === "string" ? binding : name])
        : [];

    for (const binding of kvBindings) {
      const [envName, bindingName] = Array.isArray(binding)
        ? binding
        : [binding, binding];
      if (typeof envName !== "string" || envName.length === 0) {
        continue;
      }
      Object.defineProperty(env, envName, {
        value: createKvBinding(bindingName),
        enumerable: true,
        configurable: true,
        writable: true,
      });
    }

    const actorBindings = Array.isArray(actorBindingsConfig)
      ? actorBindingsConfig
      : actorBindingsConfig && typeof actorBindingsConfig === "object"
        ? Object.entries(actorBindingsConfig).map(([name, binding]) => [name, typeof binding === "string" ? binding : name])
        : [];
    const actorNamespaces = new Set();
    for (const binding of actorBindings) {
      const [envName, bindingName] = Array.isArray(binding)
        ? binding
        : [binding, binding];
      if (typeof envName !== "string" || envName.length === 0) {
        continue;
      }
      if (typeof bindingName !== "string" || bindingName.length === 0) {
        continue;
      }
      actorNamespaces.add(bindingName);
      Object.defineProperty(env, envName, {
        value: { __dd_actor_binding: bindingName },
        enumerable: true,
        configurable: true,
        writable: false,
      });
    }

    return { env, actorNamespaces };
  };

  const emitWaitUntilDone = async (timedOut) => {
    if (waitUntilDoneSent) {
      return;
    }
    waitUntilDoneSent = true;
    await syncFrozenTime();
    callOp(
      "op_emit_wait_until_done",
      JSON.stringify({
        request_id: requestId,
        completion_token: completionToken,
        wait_until_count: waitUntilPromises.length,
        timed_out: timedOut,
      }),
    );
  };

  const emitResponseStart = async (status, headers) => {
    await syncFrozenTime();
    callOp(
      "op_emit_response_start",
      JSON.stringify({
        request_id: requestId,
        completion_token: completionToken,
        status,
        headers,
      }),
    );
  };

  const emitResponseChunk = async (chunk) => {
    await syncFrozenTime();
    const bytes = toUtf8Bytes(chunk);
    callOp(
      "op_emit_response_chunk",
      JSON.stringify({
        request_id: requestId,
        completion_token: completionToken,
        chunk: Array.from(bytes),
      }),
    );
    return bytes;
  };

  const waitForWaitUntils = async () => {
    if (waitUntilPromises.length === 0) {
      return true;
    }

    let timeoutId = 0;
    const timeout = new Promise((resolve) => {
      timeoutId = setTimeout(() => resolve(false), 30_000);
    });
    const settled = Promise.allSettled(waitUntilPromises).then(() => true);
    try {
      return await Promise.race([settled, timeout]);
    } finally {
      clearTimeout(timeoutId);
    }
  };

  const trackWaitUntil = (promise) => {
    const tracked = Promise.resolve(promise).then(
      async (value) => {
        await syncFrozenTime();
        return { ok: true, value };
      },
      async (error) => {
        await syncFrozenTime();
        try {
          console.warn(
            "waitUntil promise rejected",
            String((error && (error.stack || error.message)) || error),
          );
        } catch {
          // Ignore logging failures in isolate userland.
        }
        return { ok: false, error: String((error && (error.stack || error.message)) || error) };
      },
    );
    waitUntilPromises.push(tracked);
    return tracked;
  };

  (async () => {
    try {
      await syncFrozenTime();
      const requestBody = hasRequestBodyStream
        ? createRequestBodyStream()
        : input.body?.length
          ? new Uint8Array(input.body)
          : undefined;
      const request = new Request(input.url, {
        method: input.method,
        headers: input.headers,
        body: requestBody,
        signal: controller.signal,
      });
      const envResult = buildEnv();
      const env = envResult.env;
      const actorNamespaces = envResult.actorNamespaces;
      const ctx = {
        requestId: input.request_id,
        signal: controller.signal,
        waitUntil(promise) {
          return trackWaitUntil(promise);
        },
        actor(bindingRef, key) {
          let namespace = null;
          if (typeof bindingRef === "string") {
            namespace = bindingRef;
          } else if (
            bindingRef
            && typeof bindingRef === "object"
            && typeof bindingRef.__dd_actor_binding === "string"
          ) {
            namespace = bindingRef.__dd_actor_binding;
          }
          if (typeof namespace !== "string" || namespace.length === 0) {
            throw new Error("ctx.actor requires a declared actor binding");
          }
          if (!actorNamespaces.has(namespace)) {
            throw new Error(`actor binding not declared for worker: ${namespace}`);
          }
          const actorKey = String(key ?? "").trim();
          if (!actorKey) {
            throw new Error("ctx.actor requires a non-empty key");
          }
          return createActorBinding(namespace, actorKey);
        },
        async sleep(millis) {
          await sleep(millis);
          await syncFrozenTime();
        },
      };

      const response = await worker.fetch(request, env, ctx);
      await syncFrozenTime();

      if (!(response instanceof Response)) {
        throw new Error("Worker fetch() must return a Response");
      }

      const headers = Array.from(response.headers.entries());
      await emitResponseStart(response.status, headers);

      const bodyBytes = [];
      if (response.body && typeof response.body.getReader === "function") {
        const reader = response.body.getReader();
        try {
          while (true) {
            const { value, done } = await reader.read();
            if (done) {
              break;
            }
            const emitted = await emitResponseChunk(value);
            bodyBytes.push(...emitted);
          }
        } finally {
          if (typeof reader.releaseLock === "function") {
            try {
              reader.releaseLock();
            } catch {
              // Ignore release failures.
            }
          }
        }
      } else {
        const emitted = await emitResponseChunk(new Uint8Array(await response.arrayBuffer()));
        bodyBytes.push(...emitted);
      }

      return {
        status: response.status,
        headers,
        body: bodyBytes,
      };
    } finally {
      inflightRequests.delete(requestId);
    }
  })()
    .then(async (result) => {
      callOp(
        "op_emit_completion",
        JSON.stringify({
          request_id: requestId,
          completion_token: completionToken,
          ok: true,
          wait_until_count: waitUntilPromises.length,
          result,
        }),
      );

      await emitWaitUntilDone(!(await waitForWaitUntils()));
    })
    .catch(async (error) => {
      const message = String((error && (error.stack || error.message)) || error);
      callOp(
        "op_emit_completion",
        JSON.stringify({
          request_id: requestId,
          completion_token: completionToken,
          ok: false,
          wait_until_count: waitUntilPromises.length,
          error: message,
        }),
      );

      await emitWaitUntilDone(!(await waitForWaitUntils()));
    });
})();
