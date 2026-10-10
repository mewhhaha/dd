  const readFetchBody = async (request, signal, label) => {
    if (signal.aborted) {
      const error = abortErrorForSignal(signal);
      void request.body?.cancel(error).catch(() => undefined);
      throw error;
    }
    if (!request.body) return new Uint8Array();
    const reader = request.body.getReader();
    const cancel = (error) => { void reader.cancel(error).catch(() => undefined); };
    const onAbort = () => cancel(abortErrorForSignal(signal));
    const chunks = [];
    let length = 0;
    try {
      signal.addEventListener("abort", onAbort, { once: true });
      if (signal.aborted) throw abortErrorForSignal(signal);
      for (;;) {
        const { value, done } = await raceAbortSignal(reader.read(), signal);
        if (done) break;
        const chunk = toByteChunk(value);
        if (chunk.byteLength === 0) continue;
        if (length + chunk.byteLength > maxRequestBodyBytes) {
          throw new Error(`${label} request body exceeded max_request_body_bytes (${maxRequestBodyBytes} bytes)`);
        }
        chunks.push(chunk);
        length += chunk.byteLength;
      }
      return concatByteChunks(chunks, length);
    } catch (error) {
      cancel(error);
      throw error;
    } finally {
      signal.removeEventListener("abort", onAbort);
      reader.releaseLock();
    }
  };

  const normalizeFetchInput = async (inputValue, initValue, service = false) => {
    const input = inputValue instanceof Request
      ? inputValue
      : new URL(String(inputValue ?? (service ? "/" : "")), service ? "http://worker" : undefined);
    const request = new Request(input, initValue);
    if (!["http:", "https:"].includes(new URL(request.url).protocol)) {
      throw new TypeError("fetch requires an http(s) URL in this runtime");
    }
    const signal = AbortSignal.any([request.signal, currentRequestContext().controller.signal]);
    const body = await readFetchBody(request, signal, service ? "service" : "host fetch");
    return {
      method: request.method,
      url: request.url,
      headers: Array.from(request.headers.entries()),
      body,
      signal,
      redirect: request.redirect,
    };
  };

  const HOST_FETCH_REDIRECT_STATUSES = new Set([301, 302, 303, 307, 308]);
  const HOST_FETCH_MAX_REDIRECTS = 10;

  const rewriteMethodForRedirect = (status, method) => {
    if (status === 303 && method !== "HEAD") {
      return "GET";
    }
    if ((status === 301 || status === 302) && method !== "GET" && method !== "HEAD") {
      return "GET";
    }
    return method;
  };

  const stripRedirectBodyHeaders = (headers) => headers.filter(([name]) => {
    const lower = String(name || "").toLowerCase();
    return lower !== "content-type"
      && lower !== "content-length"
      && lower !== "content-encoding"
      && lower !== "content-language"
      && lower !== "content-location"
      && lower !== "transfer-encoding";
  });

  const stripCrossOriginRedirectHeaders = (headers) => headers.filter(([name]) => {
    const lower = String(name || "").toLowerCase();
    return lower !== "authorization"
      && lower !== "proxy-authorization"
      && lower !== "cookie";
  });

  const checkHostFetchUrl = async (requestContextHandle, url) => {
    const checked = await callOp(
      "op_http_check_url",
      requestContextHandle,
      url,
    );
    await syncFrozenTime();
    if (!checked || typeof checked !== "object" || checked.ok === false) {
      throw new Error(String(checked?.error ?? "host fetch URL check failed"));
    }
    return {
      url: String(checked.url || url),
      clientRid: Math.max(0, Math.trunc(Number(checked.client_rid ?? 0) || 0)),
    };
  };

  const abortErrorForSignal = (signal) => signal.reason;

  const { hostFetchResponse } = globalThis.__dd_deno_runtime;

  // One request through the pinned client `clientHandle`, as a Response.
  // Aborting rejects at once; a response that arrives after that has its
  // body released.
  const sendHostFetch = async (
    requestContextHandle,
    clientHandle,
    method,
    url,
    headers,
    body,
    signal,
  ) => {
    const headersHandle = callOp("op_http_store_prepared_headers", headers);
    const bodyHandle = body ? callOp("op_http_store_prepared_body", body) : 0;
    const sent = callOp(
      "op_http_fetch",
      requestContextHandle,
      clientHandle,
      method,
      url,
      Math.max(0, Math.trunc(Number(headersHandle ?? 0) || 0)),
      Math.max(0, Math.trunc(Number(bodyHandle ?? 0) || 0)),
    );
    let fetched;
    try {
      fetched = await raceAbortSignal(sent, signal);
    } catch (error) {
      void sent.then((late) => {
        if (late?.body_handle > 0) {
          callOp("op_http_response_close", late.body_handle);
        }
      }, () => undefined);
      throw error;
    }
    if (!fetched || fetched.ok !== true) {
      throw new TypeError(String(fetched?.error ?? "host fetch failed"));
    }
    return hostFetchResponse(fetched, url, method);
  };

  const raceAbortSignal = (promise, signal) => {
    if (!signal) {
      return promise;
    }
    if (signal.aborted) {
      void promise.catch(() => undefined);
      return Promise.reject(abortErrorForSignal(signal));
    }
    return new Promise((resolve, reject) => {
      const onAbort = () => {
        cleanup();
        reject(abortErrorForSignal(signal));
      };
      const cleanup = () => {
        signal.removeEventListener("abort", onAbort);
      };
      signal.addEventListener("abort", onAbort, { once: true });
      promise.then(
        (value) => {
          cleanup();
          resolve(value);
        },
        (error) => {
          cleanup();
          reject(error);
        },
      );
    });
  };

  if (typeof globalThis.__dd_install_host_fetch !== "function") {
    Object.defineProperty(globalThis, "__dd_install_host_fetch", {
      value: function installHostFetch() {
        if (typeof globalThis.__dd_host_fetch === "function") {
          return;
        }
        const scopedFetch = async (inputValue, initValue = undefined) => {
          const run = async () => {
            const current = currentRequestContext();
            const normalized = await normalizeFetchInput(inputValue, initValue);
            const normalizedHeadersHandle = callOp(
              "op_http_store_prepared_headers",
              normalized.headers,
            );
            const normalizedBodyHandle = callOp(
              "op_http_store_prepared_body",
              normalized.body,
            );
            const prepared = await callOp(
              "op_http_prepare",
              current.requestContextHandle,
              normalized.method,
              normalized.url,
              Math.max(0, Math.trunc(Number(normalizedHeadersHandle ?? 0) || 0)),
              Math.max(0, Math.trunc(Number(normalizedBodyHandle ?? 0) || 0)),
            );
            await syncFrozenTime();
            if (!prepared || typeof prepared !== "object" || prepared.ok === false) {
              throw new Error(String(prepared?.error ?? "host fetch prepare failed"));
            }
            const signal = normalized.signal;
            let method = String(prepared.method || "GET");
            let url = String(prepared.url || normalized.url);
            let clientRid = Math.max(
              0,
              Math.trunc(Number(prepared.client_rid ?? 0) || 0),
            );
            let headers = callOp(
              "op_http_take_prepared_headers",
              Math.max(0, Math.trunc(Number(prepared.headers_handle ?? 0) || 0)),
            );
            if (!Array.isArray(headers)) {
              headers = [];
            }
            const preparedBodyHandle = Number(prepared.body_handle ?? 0);
            let body = preparedBodyHandle > 0
              ? callOp("op_http_take_prepared_body", preparedBodyHandle)
              : undefined;
            if (body && body.byteLength === 0) {
              body = undefined;
            }
            const redirectMode = normalized.redirect === "error"
              || normalized.redirect === "manual"
              ? normalized.redirect
              : "follow";
            let redirectsRemaining = HOST_FETCH_MAX_REDIRECTS;
            for (;;) {
              const clientHandle = clientRid;
              clientRid = 0;
              const response = await sendHostFetch(
                current.requestContextHandle,
                clientHandle,
                method,
                url,
                headers,
                body,
                signal,
              );
              if (redirectMode === "manual" || !HOST_FETCH_REDIRECT_STATUSES.has(response.status)) {
                return response;
              }
              const location = response.headers.get("location");
              if (!location) {
                return response;
              }
              await response.body?.cancel();
              if (redirectMode === "error") {
                throw new TypeError(`host fetch redirect blocked: ${response.status}`);
              }
              if (redirectsRemaining <= 0) {
                throw new TypeError("host fetch exceeded redirect limit");
              }
              const nextUrl = new URL(location, response.url || url).toString();
              const previousOrigin = new URL(url).origin;
              const checked = await checkHostFetchUrl(current.requestContextHandle, nextUrl);
              if (new URL(checked.url).origin !== previousOrigin) {
                headers = stripCrossOriginRedirectHeaders(headers);
              }
              url = checked.url;
              clientRid = checked.clientRid;
              const nextMethod = rewriteMethodForRedirect(response.status, method);
              if (nextMethod !== method) {
                headers = stripRedirectBodyHeaders(headers);
                body = undefined;
              }
              method = nextMethod;
              redirectsRemaining -= 1;
            }
          };
          const current = currentRequestContext(false);
          if (current?.memoryEntry) {
            return gateMemoryOutput(current.memoryEntry, run);
          }
          return run();
        };
        Object.defineProperty(scopedFetch, "__dd_host_fetch", { value: true });
        Object.defineProperty(globalThis, "__dd_host_fetch", {
          value: scopedFetch,
          enumerable: false,
          configurable: true,
          writable: true,
        });
        globalThis.fetch = scopedFetch;
      },
      enumerable: false,
      configurable: true,
      writable: true,
    });
  }
  const installHostFetch = globalThis.__dd_install_host_fetch;
