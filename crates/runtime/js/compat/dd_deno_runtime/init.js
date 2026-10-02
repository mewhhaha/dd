import { core } from "ext:core/mod.js";
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
const { HttpClient } = core.loadExtScript("ext:deno_fetch/22_http_client.js");
const { Request } = core.loadExtScript("ext:deno_fetch/23_request.js");
const { Response } = core.loadExtScript("ext:deno_fetch/23_response.js");
const { fetch } = core.loadExtScript("ext:deno_fetch/26_fetch.js");

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
  HttpClient,
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
