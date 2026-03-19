(() => {
  const requestId = __REQUEST_ID__;
  const completionToken = __COMPLETION_TOKEN__;
  const workerName = __WORKER_NAME__;
  const kvBindingsConfig = __KV_BINDINGS_JSON__;
  const worker = globalThis.__grugd_worker;

  if (worker === undefined) {
    throw new Error("Worker is not installed");
  }

  const inflightRequests = globalThis.__grugd_inflight_requests ??= new Map();
  const input = __REQUEST_JSON__;
  const controller = new AbortController();
  const waitUntilPromises = [];
  let waitUntilDoneSent = false;

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
    if (boundary && typeof globalThis.__grugd_set_time === "function") {
      globalThis.__grugd_set_time(boundary.nowMs, boundary.perfMs);
    }
  };

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

  const createKvBinding = (bindingName) => ({
    async get(key, options = {}) {
      const _ = options;
      const value = await callOp(
        "op_kv_get",
        workerName,
        bindingName,
        String(key),
      );
      await syncFrozenTime();
      if (value && typeof value === "object" && value.ok === false) {
        throw new Error(String(value.error ?? "kv get failed"));
      }
      if (value && typeof value === "object" && value.found === true) {
        return String(value.value ?? "");
      }
      return null;
    },
    async set(key, value, options = {}) {
      const _ = options;
      const result = await callOp(
        "op_kv_set",
        workerName,
        bindingName,
        String(key),
        String(value),
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
      const entries = result?.entries;
      return Array.isArray(entries) ? entries : [];
    },
  });

  const buildEnv = () => {
    const env = {};
    const bindings = Array.isArray(kvBindingsConfig)
      ? kvBindingsConfig
      : kvBindingsConfig && typeof kvBindingsConfig === "object"
        ? Object.entries(kvBindingsConfig).map(([name, binding]) => [name, typeof binding === "string" ? binding : name])
        : [];

    for (const binding of bindings) {
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

    return env;
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
      const request = new Request(input.url, {
        method: input.method,
        headers: input.headers,
        body: input.body?.length ? new Uint8Array(input.body) : undefined,
        signal: controller.signal,
      });
      const env = buildEnv();
      const ctx = {
        requestId: input.request_id,
        signal: controller.signal,
        waitUntil(promise) {
          return trackWaitUntil(promise);
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
