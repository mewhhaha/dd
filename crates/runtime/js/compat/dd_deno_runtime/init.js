import {
  CloseEvent,
  CustomEvent,
  ErrorEvent,
  Event,
  EventTarget,
  MessageEvent,
  ProgressEvent,
  PromiseRejectionEvent,
  reportError,
} from "ext:deno_web/02_event.js";
import {
  AbortController,
  AbortSignal,
} from "ext:deno_web/03_abort_signal.js";
import { DOMException } from "ext:deno_web/01_dom_exception.js";
import {
  ReadableStream,
  TransformStream,
  WritableStream,
} from "ext:deno_web/06_streams.js";
import { structuredClone } from "ext:deno_web/02_structured_clone.js";
import {
  TextDecoder,
  TextEncoder,
} from "ext:deno_web/08_text_encoding.js";
import { Blob, File } from "ext:deno_web/09_file.js";
import {
  URL,
  URLSearchParams,
} from "ext:deno_web/00_url.js";
import { URLPattern } from "ext:deno_web/01_urlpattern.js";
import { performance } from "ext:deno_web/15_performance.js";
import {
  crypto,
  Crypto,
  CryptoKey,
  SubtleCrypto,
} from "ext:deno_crypto/00_crypto.js";
import { Headers } from "ext:deno_fetch/20_headers.js";
import { FormData } from "ext:deno_fetch/21_formdata.js";
import { Request } from "ext:deno_fetch/23_request.js";
import { Response } from "ext:deno_fetch/23_response.js";
import { fetch } from "ext:deno_fetch/26_fetch.js";

Object.defineProperty(globalThis, "__dd_deno_runtime", {
  value: {
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
    crypto,
    fetch,
    performance,
    reportError,
    structuredClone,
  },
  configurable: true,
  writable: true,
});
