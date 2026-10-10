  const abortErrorForSignal = (signal) => AbortSignalPrototypeGetReason(signal);

  const readFetchBody = async (request, signal, label) => {
    if (AbortSignalPrototypeGetAborted(signal)) {
      const error = abortErrorForSignal(signal);
      const body = RequestPrototypeGetBody(request);
      if (body !== null) {
        ignoreRejection(ReadableStreamPrototypeCancel(body, error));
      }
      throw error;
    }
    const body = RequestPrototypeGetBody(request);
    if (!body) return new Uint8Array();
    const reader = ReadableStreamPrototypeGetReader(body);
    const cancel = (error) => {
      ignoreRejection(ReadableStreamDefaultReaderPrototypeCancel(reader, error));
    };
    const onAbort = () => cancel(abortErrorForSignal(signal));
    const chunks = [];
    let length = 0;
    try {
      AbortSignalPrototypeAddEventListener(signal, "abort", onAbort, ONCE);
      if (AbortSignalPrototypeGetAborted(signal)) throw abortErrorForSignal(signal);
      for (;;) {
        const { value, done } = await raceAbortSignal(
          ReadableStreamDefaultReaderPrototypeRead(reader),
          signal,
        );
        if (done) break;
        const chunk = toBytes(value);
        const chunkLength = byteLength(chunk);
        if (chunkLength === 0) continue;
        if (length + chunkLength > maxRequestBodyBytes) {
          throw new Error(`${label} request body exceeded max_request_body_bytes (${maxRequestBodyBytes} bytes)`);
        }
        ArrayPrototypePush(chunks, chunk);
        length += chunkLength;
      }
      return concatByteChunks(chunks, length);
    } catch (error) {
      cancel(error);
      throw error;
    } finally {
      AbortSignalPrototypeRemoveEventListener(signal, "abort", onAbort);
      ReadableStreamDefaultReaderPrototypeReleaseLock(reader);
    }
  };

  const normalizeFetchInput = async (inputValue, initValue, service = false) => {
    const input = ObjectPrototypeIsPrototypeOf(RequestPrototype, inputValue)
      ? inputValue
      : URLPrototypeGetHref(new URL(
        String(inputValue ?? (service ? "/" : "")),
        service ? "http://worker" : undefined,
      ));
    const request = new Request(input, initValue);
    const url = RequestPrototypeGetUrl(request);
    const protocol = URLPrototypeGetProtocol(new URL(url));
    if (protocol !== "http:" && protocol !== "https:") {
      throw new TypeError("fetch requires an http(s) URL in this runtime");
    }
    const signal = AbortSignalAny(new SafeArrayIterator([
      RequestPrototypeGetSignal(request),
      AbortControllerPrototypeGetSignal(currentRequestContext().controller),
    ]));
    const body = await readFetchBody(request, signal, service ? "service" : "host fetch");
    return {
      __proto__: null,
      method: RequestPrototypeGetMethod(request),
      url,
      headers: headerPairs(RequestPrototypeGetHeaders(request)),
      body,
      signal,
      redirect: RequestPrototypeGetRedirect(request),
    };
  };

  const HOST_FETCH_REDIRECT_STATUSES = new SafeSet([301, 302, 303, 307, 308]);
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

  // The header pairs whose lowercased name `drop` does not reject.
  const keepHeaderPairs = (headers, drop) => {
    const kept = [];
    for (let i = 0; i < headers.length; i++) {
      if (!drop(StringPrototypeToLowerCase(String(headers[i][0] || "")))) {
        ArrayPrototypePush(kept, headers[i]);
      }
    }
    return kept;
  };

  const stripRedirectBodyHeaders = (headers) => keepHeaderPairs(headers, (lower) => (
    lower === "content-type"
      || lower === "content-length"
      || lower === "content-encoding"
      || lower === "content-language"
      || lower === "content-location"
      || lower === "transfer-encoding"
  ));

  const stripCrossOriginRedirectHeaders = (headers) => keepHeaderPairs(headers, (lower) => (
    lower === "authorization"
      || lower === "proxy-authorization"
      || lower === "cookie"
  ));

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
      __proto__: null,
      url: String(checked.url || url),
      clientRid: toHandle(checked.client_rid),
    };
  };

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
      toHandle(headersHandle),
      toHandle(bodyHandle),
    );
    let fetched;
    try {
      fetched = await raceAbortSignal(sent, signal);
    } catch (error) {
      PromisePrototypeThen(sent, (late) => {
        if (late?.body_handle > 0) {
          callOp("op_http_response_close", late.body_handle);
        }
      }, noop);
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
    if (AbortSignalPrototypeGetAborted(signal)) {
      ignoreRejection(promise);
      return PromiseReject(abortErrorForSignal(signal));
    }
    return new Promise((resolve, reject) => {
      const onAbort = () => {
        cleanup();
        reject(abortErrorForSignal(signal));
      };
      const cleanup = () => {
        AbortSignalPrototypeRemoveEventListener(signal, "abort", onAbort);
      };
      AbortSignalPrototypeAddEventListener(signal, "abort", onAbort, ONCE);
      PromisePrototypeThen(
        promise,
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

  // The global fetch (web/init.js) calls dd.hostFetch.
  if (typeof dd.installHostFetch !== "function") {
    dd.installHostFetch = function installHostFetch() {
        if (typeof dd.hostFetch === "function") {
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
              toHandle(normalizedHeadersHandle),
              toHandle(normalizedBodyHandle),
            );
            await syncFrozenTime();
            if (!prepared || typeof prepared !== "object" || prepared.ok === false) {
              throw new Error(String(prepared?.error ?? "host fetch prepare failed"));
            }
            const signal = normalized.signal;
            let method = String(prepared.method || "GET");
            let url = String(prepared.url || normalized.url);
            let clientRid = toHandle(prepared.client_rid);
            let headers = callOp(
              "op_http_take_prepared_headers",
              toHandle(prepared.headers_handle),
            );
            if (!ArrayIsArray(headers)) {
              headers = [];
            }
            const preparedBodyHandle = Number(prepared.body_handle ?? 0);
            let body = preparedBodyHandle > 0
              ? callOp("op_http_take_prepared_body", preparedBodyHandle)
              : undefined;
            if (body && byteLength(body) === 0) {
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
              const status = ResponsePrototypeGetStatus(response);
              if (redirectMode === "manual" || !HOST_FETCH_REDIRECT_STATUSES.has(status)) {
                return response;
              }
              const location = HeadersPrototypeGet(ResponsePrototypeGetHeaders(response), "location");
              if (!location) {
                return response;
              }
              const responseBody = ResponsePrototypeGetBody(response);
              if (responseBody !== null) {
                await ReadableStreamPrototypeCancel(responseBody);
              }
              if (redirectMode === "error") {
                throw new TypeError(`host fetch redirect blocked: ${status}`);
              }
              if (redirectsRemaining <= 0) {
                throw new TypeError("host fetch exceeded redirect limit");
              }
              const nextUrl = URLPrototypeGetHref(
                new URL(location, ResponsePrototypeGetUrl(response) || url),
              );
              const previousOrigin = URLPrototypeGetOrigin(new URL(url));
              const checked = await checkHostFetchUrl(current.requestContextHandle, nextUrl);
              if (URLPrototypeGetOrigin(new URL(checked.url)) !== previousOrigin) {
                headers = stripCrossOriginRedirectHeaders(headers);
              }
              url = checked.url;
              clientRid = checked.clientRid;
              const nextMethod = rewriteMethodForRedirect(status, method);
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
            return await gateMemoryOutput(current.memoryEntry, run);
          }
          return await run();
        };
        dd.hostFetch = scopedFetch;
    };
  }
  const installHostFetch = dd.installHostFetch;
