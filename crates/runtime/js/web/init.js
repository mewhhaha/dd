// The web platform layer as the runtime's scripts see it. Runs once while
// the bootstrap snapshot is built, with the bootstrap object as
// `__bootstrap`, and leaves its classes in `__bootstrap.dd.web`; bootstrap.js
// decides which of them become globals.
//
// Like the rest of the runtime's own JavaScript it runs on primordials and
// on the web classes' methods as captured below, never on what worker code
// can reach and replace later.
const { core, dd, primordials } = __bootstrap;
const {
  ArrayPrototypePush,
  ObjectDefineProperty,
  ObjectFreeze,
  ObjectGetOwnPropertySymbols,
  ObjectPrototypeIsPrototypeOf,
  PromiseReject,
  ReflectGetOwnPropertyDescriptor,
  ReflectOwnKeys,
  String,
  StringPrototypeSlice,
  StringPrototypeToLowerCase,
  StringPrototypeToUpperCase,
  SymbolPrototypeGetDescription,
  TypeError,
  Uint8Array,
  uncurryThis,
} = primordials;
const cryptoRuntime = core.loadExtScript("ext:deno_crypto/00_crypto.js");
const { Crypto, CryptoKey, SubtleCrypto } = cryptoRuntime;
const {
  CloseEvent,
  CustomEvent,
  ErrorEvent,
  Event,
  EventTarget,
  MessageEvent,
  ProgressEvent,
  PromiseRejectionEvent,
  reportError,
} = core.loadExtScript("ext:deno_web/02_event.js");
const { AbortController, AbortSignal } = core.loadExtScript("ext:deno_web/03_abort_signal.js");
const { DOMException } = core.loadExtScript("ext:deno_web/01_dom_exception.js");
// Ops fail with these classes (WebCrypto above all); throw them as the
// DOMExceptions they name.
const domExceptionErrors = ["DataError", "NotSupportedError", "OperationError", "QuotaExceededError"];
for (let i = 0; i < domExceptionErrors.length; i++) {
  const name = domExceptionErrors[i];
  core.ops.op_register_error_builder(
    `DOMException${name}`,
    (message) => new DOMException(message, name),
  );
}
const {
  ByteLengthQueuingStrategy,
  CountQueuingStrategy,
  ReadableByteStreamController,
  ReadableStream,
  ReadableStreamBYOBReader,
  ReadableStreamBYOBRequest,
  ReadableStreamDefaultController,
  ReadableStreamDefaultReader,
  TransformStream,
  TransformStreamDefaultController,
  WritableStream,
  WritableStreamDefaultController,
  WritableStreamDefaultWriter,
} = core.loadExtScript("ext:deno_web/06_streams.js");
const { structuredClone } = core.loadExtScript("ext:deno_web/02_structured_clone.js");
const {
  TextDecoder,
  TextDecoderStream,
  TextEncoder,
  TextEncoderStream,
} = core.loadExtScript("ext:deno_web/08_text_encoding.js");
const { Blob, File } = core.loadExtScript("ext:deno_web/09_file.js");
const { URL, URLSearchParams } = core.loadExtScript("ext:deno_web/00_url.js");
const { URLPattern } = core.loadExtScript("ext:deno_web/01_urlpattern.js");
const { performance } = core.loadExtScript("ext:deno_web/15_performance.js");
const { Headers } = core.loadExtScript("ext:deno_fetch/20_headers.js");
const { FormData } = core.loadExtScript("ext:deno_fetch/21_formdata.js");
const { Request } = core.loadExtScript("ext:deno_fetch/23_request.js");
const { Response } = core.loadExtScript("ext:deno_fetch/23_response.js");

const {
  fromInnerResponse,
  newInnerResponse,
  nullBodyStatus,
} = core.loadExtScript("ext:deno_fetch/23_response.js");
const { InnerBody } = core.loadExtScript("ext:deno_fetch/22_body.js");
const {
  op_http_fetch_unscoped,
  op_http_response_close,
  op_http_response_read,
} = core.ops;

// Uncurried copies of the web classes' prototype methods and accessors,
// named the way primordials are (ResponsePrototypeGetHeaders,
// ReadableStreamDefaultReaderPrototypeRead, ...), with each prototype as
// `${name}Prototype`. They are taken before any worker code runs, so the
// runtime's own use of web objects ignores what worker code later does to
// those prototypes.
const webPrimordials = { __proto__: null };
function copyWebPrototype(name, prototype) {
  webPrimordials[`${name}Prototype`] = prototype;
  const keys = ReflectOwnKeys(prototype);
  for (let i = 0; i < keys.length; i++) {
    const key = keys[i];
    if (typeof key !== "string" || key === "constructor") {
      continue;
    }
    const suffix = `${StringPrototypeToUpperCase(key[0])}${StringPrototypeSlice(key, 1)}`;
    const descriptor = ReflectGetOwnPropertyDescriptor(prototype, key);
    if (descriptor.get !== undefined) {
      webPrimordials[`${name}PrototypeGet${suffix}`] = uncurryThis(descriptor.get);
    }
    if (descriptor.set !== undefined) {
      webPrimordials[`${name}PrototypeSet${suffix}`] = uncurryThis(descriptor.set);
    }
    if (typeof descriptor.value === "function") {
      webPrimordials[`${name}Prototype${suffix}`] = uncurryThis(descriptor.value);
    }
  }
}
copyWebPrototype("AbortController", AbortController.prototype);
// AbortSignal's own addEventListener keeps a dependent signal (one from
// AbortSignal.any) reachable from its sources while it has abort listeners;
// EventTarget's would let it be collected before the abort arrives.
copyWebPrototype("AbortSignal", AbortSignal.prototype);
copyWebPrototype("Headers", Headers.prototype);
copyWebPrototype("ReadableStream", ReadableStream.prototype);
copyWebPrototype("ReadableStreamDefaultController", ReadableStreamDefaultController.prototype);
copyWebPrototype("ReadableStreamDefaultReader", ReadableStreamDefaultReader.prototype);
copyWebPrototype("Request", Request.prototype);
copyWebPrototype("Response", Response.prototype);
copyWebPrototype("URL", URL.prototype);
webPrimordials.AbortSignalAny = AbortSignal.any;

// Headers keep their combined, sorted entries (what iterating them yields)
// behind a symbol-keyed getter on the prototype; take that one too.
const headersSymbols = ObjectGetOwnPropertySymbols(Headers.prototype);
for (let i = 0; i < headersSymbols.length; i++) {
  if (SymbolPrototypeGetDescription(headersSymbols[i]) === "iterable headers") {
    webPrimordials.HeadersPrototypeGetIterableHeaders = uncurryThis(
      ReflectGetOwnPropertyDescriptor(Headers.prototype, headersSymbols[i]).get,
    );
  }
}
if (webPrimordials.HeadersPrototypeGetIterableHeaders === undefined) {
  throw new TypeError("dd bootstrap cannot find the Headers entries getter");
}
ObjectFreeze(webPrimordials);

const {
  HeadersPrototypeAppend,
  HeadersPrototypeGet,
  HeadersPrototypeGetIterableHeaders,
  ReadableStreamDefaultControllerPrototypeClose,
  ReadableStreamDefaultControllerPrototypeEnqueue,
  ReadableStreamDefaultReaderPrototypeRead,
  ReadableStreamPrototypeGetReader,
  RequestPrototypeArrayBuffer,
  RequestPrototypeGetBody,
  RequestPrototypeGetHeaders,
  RequestPrototypeGetMethod,
  RequestPrototypeGetUrl,
  ResponsePrototype,
  ResponsePrototypeGetBody,
  ResponsePrototypeGetHeaders,
  ResponsePrototypeGetOk,
  ResponsePrototypeGetStatus,
  ResponsePrototypeGetUrl,
} = webPrimordials;

/** `headers`' combined, sorted [name, value] pairs, as new arrays. */
function headerPairs(headers) {
  const entries = HeadersPrototypeGetIterableHeaders(headers);
  const pairs = [];
  for (let i = 0; i < entries.length; i++) {
    ArrayPrototypePush(pairs, [entries[i][0], entries[i][1]]);
  }
  return pairs;
}

/**
 * Appends each [name, value] pair to `headers`. Header lists go into a new
 * Request or Response this way rather than as its init's `headers`, which
 * the web layer would iterate through the worker-reachable
 * Array.prototype[Symbol.iterator].
 */
function appendHeaderPairs(headers, pairs) {
  for (let i = 0; i < pairs.length; i++) {
    HeadersPrototypeAppend(headers, pairs[i][0], pairs[i][1]);
  }
  return headers;
}

// A Response for a host fetch result, its body read from Rust as it arrives.
function hostFetchResponse(fetched, url, method) {
  const inner = newInnerResponse(fetched.status, fetched.status_text);
  inner.headerList = fetched.headers;
  inner.urlList = [url];
  const bodyHandle = fetched.body_handle;
  if (bodyHandle > 0) {
    if (nullBodyStatus(fetched.status) || method === "HEAD") {
      op_http_response_close(bodyHandle);
    } else {
      inner.body = new InnerBody(new ReadableStream({
        __proto__: null,
        async pull(controller) {
          const chunk = await op_http_response_read(bodyHandle);
          if (chunk === null) {
            ReadableStreamDefaultControllerPrototypeClose(controller);
          } else {
            ReadableStreamDefaultControllerPrototypeEnqueue(controller, chunk);
          }
        },
        cancel() {
          op_http_response_close(bodyHandle);
        },
      }, { __proto__: null, highWaterMark: 0 }));
    }
  }
  return fromInnerResponse(inner, "immutable");
}

// fetch with no request scope or egress rule. The op refuses it outside the
// development runtime, which exposes it as __dd_raw_host_fetch for dd-vite.
async function unscopedFetch(input, init = undefined) {
  const request = new Request(input, init);
  const method = RequestPrototypeGetMethod(request);
  const url = RequestPrototypeGetUrl(request);
  const body = RequestPrototypeGetBody(request) === null
    ? new Uint8Array()
    : new Uint8Array(await RequestPrototypeArrayBuffer(request));
  const fetched = await op_http_fetch_unscoped(
    method,
    url,
    headerPairs(RequestPrototypeGetHeaders(request)),
    body,
  );
  if (fetched?.ok !== true) {
    throw new TypeError(String(fetched?.error ?? "host fetch failed"));
  }
  return hostFetchResponse(fetched, url, method);
}

// WebAssembly.compileStreaming and instantiateStreaming hand their Response
// here; its body goes to V8's streaming compiler chunk by chunk.
const {
  op_set_wasm_streaming_handler,
  op_wasm_streaming_abort,
  op_wasm_streaming_feed,
  op_wasm_streaming_finish,
  op_wasm_streaming_set_url,
} = core.ops;
op_set_wasm_streaming_handler(async (source, id) => {
  try {
    const response = await source;
    if (!ObjectPrototypeIsPrototypeOf(ResponsePrototype, response)) {
      throw new TypeError(
        "Failed to execute 'WebAssembly.compileStreaming': Argument 1 is not a Response",
      );
    }
    const contentType = HeadersPrototypeGet(ResponsePrototypeGetHeaders(response), "Content-Type");
    if (
      typeof contentType !== "string"
      || StringPrototypeToLowerCase(contentType) !== "application/wasm"
    ) {
      throw new TypeError("Invalid WebAssembly content type");
    }
    if (!ResponsePrototypeGetOk(response)) {
      throw new TypeError(
        `Failed to receive WebAssembly content: HTTP status code ${ResponsePrototypeGetStatus(response)}`,
      );
    }
    op_wasm_streaming_set_url(id, ResponsePrototypeGetUrl(response));
    const body = ResponsePrototypeGetBody(response);
    if (body !== null) {
      const reader = ReadableStreamPrototypeGetReader(body);
      for (;;) {
        const { value, done } = await ReadableStreamDefaultReaderPrototypeRead(reader);
        if (done) {
          break;
        }
        op_wasm_streaming_feed(id, value);
      }
    }
    op_wasm_streaming_finish(id);
  } catch (error) {
    op_wasm_streaming_abort(id, error);
  }
});

// Each request installs dd's host fetch (execute_worker/fetch.js), which
// enforces the worker's egress rules; outside a request there is nothing to
// fetch under.
function fetch(input, init = undefined) {
  const hostFetch = dd.hostFetch;
  if (typeof hostFetch !== "function") {
    return PromiseReject(new TypeError("fetch is only available while handling a request"));
  }
  return hostFetch(input, init);
}

const ddRuntime = {
  AbortController,
  AbortSignal,
  Blob,
  ByteLengthQueuingStrategy,
  CloseEvent,
  CountQueuingStrategy,
  Crypto,
  CryptoKey,
  CustomEvent,
  DOMException,
  ErrorEvent,
  Event,
  EventTarget,
  File,
  FormData,
  Headers,
  MessageEvent,
  ProgressEvent,
  PromiseRejectionEvent,
  ReadableByteStreamController,
  ReadableStream,
  ReadableStreamBYOBReader,
  ReadableStreamBYOBRequest,
  ReadableStreamDefaultController,
  ReadableStreamDefaultReader,
  Request,
  Response,
  SubtleCrypto,
  TextDecoder,
  TextDecoderStream,
  TextEncoder,
  TextEncoderStream,
  TransformStream,
  TransformStreamDefaultController,
  URL,
  URLPattern,
  URLSearchParams,
  WritableStream,
  WritableStreamDefaultController,
  WritableStreamDefaultWriter,
  fetch,
  performance,
  reportError,
  structuredClone,
};

ObjectDefineProperty(ddRuntime, "crypto", {
  __proto__: null,
  get: () => cryptoRuntime.crypto,
  enumerable: true,
});

dd.web = ddRuntime;
dd.webPrimordials = webPrimordials;
dd.headerPairs = headerPairs;
dd.appendHeaderPairs = appendHeaderPairs;
dd.hostFetchResponse = hostFetchResponse;
dd.unscopedFetch = unscopedFetch;
