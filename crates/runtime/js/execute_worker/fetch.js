const abortErrorForSignal = (signal) => AbortSignalPrototypeGetReason(signal);

const readFetchBody = async (request, signal, label, maxBytes) => {
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
      if (length + chunkLength > maxBytes) {
        throw new Error(`${label} request body exceeded max_request_body_bytes (${maxBytes} bytes)`);
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
  const current = currentRequestContext();
  const signal = AbortSignalAny(new SafeArrayIterator([
    RequestPrototypeGetSignal(request),
    AbortControllerPrototypeGetSignal(current.controller),
  ]));
  const body = await readFetchBody(
    request,
    signal,
    service ? "service" : "host fetch",
    current.maxRequestBodyBytes,
  );
  return new NormalizedFetch(
    RequestPrototypeGetMethod(request),
    url,
    headerPairs(RequestPrototypeGetHeaders(request)),
    body,
    signal,
    RequestPrototypeGetRedirect(request),
  );
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
  let checked;
  try {
    checked = await op_http_url_check(requestContextHandle, url);
  } finally {
    await syncFrozenTime();
  }
  return new CheckedFetchUrl(String(checked.url || url), toHandle(checked.client_rid));
};

// One request through the pinned client `clientHandle`, as a Response. A
// fetch that gets no response rejects with the op's TypeError. Aborting
// rejects at once; a response that arrives after that has its body
// released.
const sendHostFetch = async (
  requestContextHandle,
  clientHandle,
  method,
  url,
  headers,
  body,
  signal,
) => {
  const headersHandle = op_http_headers_store(headers);
  const bodyHandle = body ? op_http_body_store(body) : 0;
  const sent = op_http_fetch(
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
      if (late.body_handle > 0) {
        op_http_response_close(late.body_handle);
      }
    }, noop);
    throw error;
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

// A worker's fetch under the request context `current`, following
// redirects the way its redirect mode asks.
const scopedFetch = async (current, inputValue, initValue) => {
  const normalized = await normalizeFetchInput(inputValue, initValue);
  const normalizedHeadersHandle = op_http_headers_store(normalized.headers);
  const normalizedBodyHandle = op_http_body_store(normalized.body);
  let prepared;
  try {
    prepared = await op_http_prepare(
      current.requestContextHandle,
      normalized.method,
      normalized.url,
      toHandle(normalizedHeadersHandle),
      toHandle(normalizedBodyHandle),
    );
  } finally {
    await syncFrozenTime();
  }
  const signal = normalized.signal;
  let method = String(prepared.method || "GET");
  let url = String(prepared.url || normalized.url);
  let clientRid = toHandle(prepared.client_rid);
  let headers = op_http_headers_take(toHandle(prepared.headers_handle));
  if (!ArrayIsArray(headers)) {
    headers = [];
  }
  const preparedBodyHandle = Number(prepared.body_handle ?? 0);
  let body = preparedBodyHandle > 0
    ? op_http_body_take(preparedBodyHandle)
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

// The global fetch (web/init.js) calls dd.hostFetch. A fetch from a memory
// call goes out in order with the memory entry's other output.
dd.hostFetch = async (inputValue, initValue = undefined) => {
  const current = currentRequestContext(false);
  if (!current) {
    throw new TypeError("fetch is only available while handling a request");
  }
  if (current.memoryEntry) {
    return await gateMemoryOutput(
      current.memoryEntry,
      () => scopedFetch(current, inputValue, initValue),
    );
  }
  return await scopedFetch(current, inputValue, initValue);
};
