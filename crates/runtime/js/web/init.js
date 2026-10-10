(function () {
const { core } = globalThis.__bootstrap;
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
for (const name of ["DataError", "NotSupportedError", "OperationError", "QuotaExceededError"]) {
  core.ops.op_register_error_builder(
    `DOMException${name}`,
    (message) => new DOMException(message, name),
  );
}
const { ReadableStream, TransformStream, WritableStream } = core.loadExtScript(
  "ext:deno_web/06_streams.js",
);
const { structuredClone } = core.loadExtScript("ext:deno_web/02_structured_clone.js");
const { TextDecoder, TextEncoder } = core.loadExtScript("ext:deno_web/08_text_encoding.js");
const { Blob, File } = core.loadExtScript("ext:deno_web/09_file.js");
const { URL, URLSearchParams } = core.loadExtScript("ext:deno_web/00_url.js");
const { URLPattern } = core.loadExtScript("ext:deno_web/01_urlpattern.js");
const { performance } = core.loadExtScript("ext:deno_web/15_performance.js");
const { Headers } = core.loadExtScript("ext:deno_fetch/20_headers.js");
const { FormData } = core.loadExtScript("ext:deno_fetch/21_formdata.js");
const { Request } = core.loadExtScript("ext:deno_fetch/23_request.js");
const { Response } = core.loadExtScript("ext:deno_fetch/23_response.js");

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
    if (!(response instanceof Response)) {
      throw new TypeError(
        "Failed to execute 'WebAssembly.compileStreaming': Argument 1 is not a Response",
      );
    }
    const contentType = response.headers.get("Content-Type");
    if (typeof contentType !== "string" || contentType.toLowerCase() !== "application/wasm") {
      throw new TypeError("Invalid WebAssembly content type");
    }
    if (!response.ok) {
      throw new TypeError(
        `Failed to receive WebAssembly content: HTTP status code ${response.status}`,
      );
    }
    op_wasm_streaming_set_url(id, response.url);
    if (response.body !== null) {
      const reader = response.body.getReader();
      for (;;) {
        const { value, done } = await reader.read();
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
const { TypeError } = __bootstrap.primordials;

// Each request installs dd's host fetch (execute_worker/fetch.js), which
// enforces the worker's egress rules; outside a request there is nothing to
// fetch under.
function fetch(input, init = undefined) {
  const hostFetch = globalThis.__dd_host_fetch;
  if (typeof hostFetch !== "function") {
    return Promise.reject(new TypeError("fetch is only available while handling a request"));
  }
  return hostFetch(input, init);
}

const ddRuntime = {
  AbortController,
  AbortSignal,
  Blob,
  CloseEvent,
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
  ReadableStream,
  Request,
  Response,
  SubtleCrypto,
  TextDecoder,
  TextEncoder,
  TransformStream,
  URL,
  URLPattern,
  URLSearchParams,
  WritableStream,
  fetch,
  performance,
  reportError,
  structuredClone,
};

Object.defineProperty(ddRuntime, "crypto", {
  get: () => cryptoRuntime.crypto,
  enumerable: true,
});

Object.defineProperty(globalThis, "__dd_deno_runtime", {
  value: ddRuntime,
  configurable: true,
  writable: true,
});
})();
