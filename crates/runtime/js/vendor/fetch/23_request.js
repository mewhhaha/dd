// Copyright 2018-2026 the Deno authors. MIT license.

return (function () {
const { core, primordials } = __bootstrap;
const {
  ArrayPrototypeMap,
  ArrayPrototypeSlice,
  ArrayPrototypeSplice,
  ObjectKeys,
  ObjectPrototypeIsPrototypeOf,
  RegExpPrototypeExec,
  SafeArrayIterator,
  StringPrototypeToUpperCase,
  Symbol,
  TypeError,
} = primordials;

const webidl = core.loadExtScript("ext:vendor/webidl/00_webidl.js");
const {
  createFilteredInspectProxy,
  privateCustomInspect,
} = core.loadExtScript("ext:web/console.js");
const { HTTP_TOKEN_CODE_POINT_RE } = core.loadExtScript(
  "ext:vendor/web/00_infra.js",
);
const { URL } = core.loadExtScript("ext:vendor/web/00_url.js");
const { extractBody, mixinBody } = core.loadExtScript(
  "ext:vendor/fetch/22_body.js",
);
const { extractMimeType } = core.loadExtScript("ext:vendor/web/01_mimesniff.js");
const {
  fillHeaders,
  getDecodeSplitHeader,
  guardFromHeaders,
  headerListFromHeaders,
  headersFromHeaderList,
} = core.loadExtScript("ext:vendor/fetch/20_headers.js");
const {
  createDependentAbortSignal,
  newSignal,
} = core.loadExtScript("ext:vendor/web/03_abort_signal.js");

const _request = Symbol("request");
const _headers = Symbol("headers");
const _headersCache = Symbol("headers cache");
const _headersGuard = Symbol("headers guard");
const _signal = Symbol("signal");
const _signalCache = Symbol("signalCache");
const _mimeType = Symbol("mime type");
const _body = Symbol("body");
const _brand = webidl.brand;

/**
 * Fields named `*Mode`, `mode`, `referrer*`, `integrity`, and `keepalive` are
 * only set on the inner object when the constructor's init bag customized
 * them away from the spec default, so most requests share one hidden class.
 * The getters substitute the spec defaults when a field is missing.
 *
 * @typedef InnerRequest
 * @property {string} method
 * @property {string} url
 * @property {[string, string][]} headerList
 * @property {null | typeof __window.bootstrap.fetchBody.InnerBody} body
 * @property {undefined | "default" | "no-store" | "reload" | "no-cache" | "force-cache" | "only-if-cached"} [cacheMode]
 * @property {undefined | "omit" | "same-origin" | "include"} [credentialsMode]
 * @property {undefined | string} [integrity]
 * @property {undefined | boolean} [keepalive]
 * @property {undefined | "same-origin" | "no-cors" | "cors" | "navigate"} [mode]
 * @property {undefined | "auto" | "low" | "high"} [priority]
 * @property {"follow" | "error" | "manual"} redirectMode
 * @property {undefined | string} [referrer] "client", "no-referrer", or a serialized URL
 * @property {undefined | "" | "no-referrer" | "no-referrer-when-downgrade" | "same-origin" | "origin" | "strict-origin" | "origin-when-cross-origin" | "strict-origin-when-cross-origin" | "unsafe-url"} [referrerPolicy]
 */

/**
 * @param {string} method
 * @param {string} url
 * @returns {InnerRequest}
 */
function newInnerRequest(method, url) {
  return {
    method,
    url,
    headerList: [],
    body: null,
    redirectMode: "follow",
  };
}

/**
 * https://fetch.spec.whatwg.org/#concept-request-clone
 * @param {InnerRequest} request
 * @param {boolean} skipBody
 * @returns {InnerRequest}
 */
function cloneInnerRequest(request, skipBody = false) {
  const headerList = ArrayPrototypeMap(
    request.headerList,
    (x) => [x[0], x[1]],
  );

  let body = null;
  if (request.body !== null && !skipBody) {
    body = request.body.clone();
  }

  const cloned = {
    method: request.method,
    url: request.url,
    headerList,
    body,
    redirectMode: request.redirectMode,
  };
  if (request.cacheMode !== undefined) cloned.cacheMode = request.cacheMode;
  if (request.credentialsMode !== undefined) {
    cloned.credentialsMode = request.credentialsMode;
  }
  if (request.integrity !== undefined) cloned.integrity = request.integrity;
  if (request.keepalive !== undefined) cloned.keepalive = request.keepalive;
  if (request.mode !== undefined) cloned.mode = request.mode;
  if (request.priority !== undefined) cloned.priority = request.priority;
  if (request.referrer !== undefined) cloned.referrer = request.referrer;
  if (request.referrerPolicy !== undefined) {
    cloned.referrerPolicy = request.referrerPolicy;
  }
  return cloned;
}

// method => normalized method
const KNOWN_METHODS = {
  __proto__: null,
  "DELETE": "DELETE",
  "delete": "DELETE",
  "GET": "GET",
  "get": "GET",
  "HEAD": "HEAD",
  "head": "HEAD",
  "OPTIONS": "OPTIONS",
  "options": "OPTIONS",
  "PATCH": "PATCH",
  "POST": "POST",
  "post": "POST",
  "PUT": "PUT",
  "put": "PUT",
};

/**
 * @param {string} m
 * @returns {string}
 */
function validateAndNormalizeMethod(m) {
  if (RegExpPrototypeExec(HTTP_TOKEN_CODE_POINT_RE, m) === null) {
    throw new TypeError("Method is not valid");
  }
  const upperCase = StringPrototypeToUpperCase(m);
  switch (upperCase) {
    case "DELETE":
    case "GET":
    case "HEAD":
    case "OPTIONS":
    case "POST":
    case "PUT":
      return upperCase;
    case "CONNECT":
    case "TRACE":
    case "TRACK":
      throw new TypeError("Method is forbidden");
  }
  return m;
}

class Request {
  /** @type {InnerRequest} */
  [_request];
  /** @type {Headers} */
  [_headersCache];
  [_headersGuard];
  [_signalCache];

  /** @type {Headers} */
  get [_headers]() {
    if (this[_headersCache] === undefined) {
      this[_headersCache] = headersFromHeaderList(
        this[_request].headerList,
        this[_headersGuard],
      );
    }
    return this[_headersCache];
  }

  set [_headers](value) {
    this[_headersCache] = value;
  }

  /** @type {AbortSignal} */
  get [_signal]() {
    let signal = this[_signalCache];
    // A request made without a signal gets one that never aborts, on first
    // use.
    if (signal === undefined) {
      signal = newSignal();
      this[_signalCache] = signal;
    }
    return signal;
  }
  get [_mimeType]() {
    const values = getDecodeSplitHeader(
      headerListFromHeaders(this[_headers]),
      "Content-Type",
    );
    return extractMimeType(values);
  }
  get [_body]() {
    return this[_request].body;
  }

  /**
   * https://fetch.spec.whatwg.org/#dom-request
   * @param {RequestInfo} input
   * @param {RequestInit} init
   */
  constructor(input, init = undefined) {
    if (input === _brand) {
      this[_brand] = _brand;
      return;
    }

    const prefix = "Failed to construct 'Request'";
    webidl.requiredArguments(arguments.length, 1, prefix);
    input = webidl.converters["RequestInfo_DOMString"](
      input,
      prefix,
      "Argument 1",
    );
    init = webidl.converters["RequestInit"](init, prefix, "Argument 2");

    this[_brand] = _brand;

    /** @type {InnerRequest} */
    let request;

    // 4.
    let signal = null;

    // 5.
    if (typeof input === "string") {
      const parsedURL = new URL(input);
      request = newInnerRequest("GET", parsedURL.href);
    } else { // 6.
      if (!ObjectPrototypeIsPrototypeOf(RequestPrototype, input)) {
        throw new TypeError("Unreachable");
      }
      const originalReq = input[_request];
      // fold in of step 12 from below
      request = cloneInnerRequest(originalReq, true);
      signal = input[_signal];
    }

    // 12. is folded into the else statement of step 6 above.

    // 17. referrer
    if (init.referrer !== undefined) {
      const referrer = init.referrer;
      if (referrer === "") {
        request.referrer = "no-referrer";
      } else {
        let parsedReferrer;
        try {
          parsedReferrer = new URL(referrer);
        } catch (err) {
          throw new TypeError(`Referrer "${referrer}" is not a valid URL.`, {
            cause: err,
          });
        }
        if (
          (parsedReferrer.protocol === "about:" &&
            parsedReferrer.pathname === "client")
        ) {
          request.referrer = "client";
        } else {
          request.referrer = parsedReferrer.href;
        }
      }
    }

    // 18. referrerPolicy
    if (init.referrerPolicy !== undefined) {
      request.referrerPolicy = init.referrerPolicy;
    }

    // 19. mode
    if (init.mode !== undefined) {
      if (init.mode === "navigate") {
        throw new TypeError("Request mode 'navigate' is not allowed");
      }
      request.mode = init.mode;
    }

    // 20. credentials
    if (init.credentials !== undefined) {
      request.credentialsMode = init.credentials;
    }

    // 21. cache
    if (init.cache !== undefined) {
      request.cacheMode = init.cache;
    }

    // If request's cache mode is "only-if-cached" and request's mode is not
    // "same-origin", then throw a TypeError.
    if (
      request.cacheMode === "only-if-cached" && request.mode !== "same-origin"
    ) {
      throw new TypeError(
        'Request cache mode "only-if-cached" can only be used with same-origin mode',
      );
    }

    // 22.
    if (init.redirect !== undefined) {
      request.redirectMode = init.redirect;
    }

    // 23. integrity
    if (init.integrity !== undefined) {
      request.integrity = init.integrity;
    }

    // 24. keepalive
    if (init.keepalive !== undefined) {
      request.keepalive = init.keepalive;
    }

    // priority
    if (init.priority !== undefined) {
      request.priority = init.priority;
    }

    // 25.
    if (init.method !== undefined) {
      const method = init.method;
      // fast path: check for known methods
      request.method = KNOWN_METHODS[method] ??
        validateAndNormalizeMethod(method);
    }

    // 26.
    if (init.signal !== undefined) {
      signal = init.signal;
    }

    // 28.
    this[_request] = request;

    // 29 & 30.
    if (signal !== null) {
      // dd: a SafeArrayIterator, so the sequence conversion does not walk
      // the array through a replaceable Array.prototype[Symbol.iterator].
      this[_signalCache] = createDependentAbortSignal(
        new SafeArrayIterator([signal]),
        prefix,
      );
    }

    // 31.
    this[_headers] = headersFromHeaderList(request.headerList, "request");

    // 33.
    if (init.headers || ObjectKeys(init).length > 0) {
      const headerList = headerListFromHeaders(this[_headers]);
      const headers = init.headers ?? ArrayPrototypeSlice(
        headerList,
        0,
        headerList.length,
      );
      if (headerList.length !== 0) {
        ArrayPrototypeSplice(headerList, 0, headerList.length);
      }
      fillHeaders(this[_headers], headers);
    }

    // 34.
    let inputBody = null;
    if (ObjectPrototypeIsPrototypeOf(RequestPrototype, input)) {
      inputBody = input[_body];
    }

    // 35.
    if (
      (request.method === "GET" || request.method === "HEAD") &&
      ((init.body !== undefined && init.body !== null) ||
        inputBody !== null)
    ) {
      throw new TypeError("Request with GET/HEAD method cannot have body");
    }

    // 36.
    let initBody = null;

    // 37.
    if (init.body !== undefined && init.body !== null) {
      const res = extractBody(init.body);
      initBody = res.body;
      if (res.contentType !== null && !this[_headers].has("content-type")) {
        this[_headers].append("Content-Type", res.contentType);
      }
    }

    // 38.
    const inputOrInitBody = initBody ?? inputBody;

    // 40.
    let finalBody = inputOrInitBody;

    // 41.
    if (initBody === null && inputBody !== null) {
      if (input[_body] && input[_body].unusable()) {
        throw new TypeError("Input request's body is unusable");
      }
      finalBody = inputBody.createProxy();
    }

    // 42.
    request.body = finalBody;
  }

  get method() {
    webidl.assertBranded(this, RequestPrototype);
    return this[_request].method;
  }

  get url() {
    webidl.assertBranded(this, RequestPrototype);
    return this[_request].url;
  }

  get headers() {
    webidl.assertBranded(this, RequestPrototype);
    return this[_headers];
  }

  get redirect() {
    webidl.assertBranded(this, RequestPrototype);
    return this[_request].redirectMode;
  }

  get cache() {
    webidl.assertBranded(this, RequestPrototype);
    return this[_request].cacheMode ?? "default";
  }

  get credentials() {
    webidl.assertBranded(this, RequestPrototype);
    return this[_request].credentialsMode ?? "same-origin";
  }

  get integrity() {
    webidl.assertBranded(this, RequestPrototype);
    return this[_request].integrity ?? "";
  }

  get keepalive() {
    webidl.assertBranded(this, RequestPrototype);
    return this[_request].keepalive ?? false;
  }

  get mode() {
    webidl.assertBranded(this, RequestPrototype);
    return this[_request].mode ?? "cors";
  }

  get referrer() {
    webidl.assertBranded(this, RequestPrototype);
    const referrer = this[_request].referrer;
    if (referrer === undefined || referrer === "client") {
      return "about:client";
    }
    if (referrer === "no-referrer") {
      return "";
    }
    return referrer;
  }

  get referrerPolicy() {
    webidl.assertBranded(this, RequestPrototype);
    return this[_request].referrerPolicy ?? "";
  }

  get signal() {
    webidl.assertBranded(this, RequestPrototype);
    return this[_signal];
  }

  clone() {
    const prefix = "Failed to execute 'Request.clone'";
    webidl.assertBranded(this, RequestPrototype);
    if (this[_body] && this[_body].unusable()) {
      throw new TypeError("Body is unusable");
    }
    const clonedReq = cloneInnerRequest(this[_request]);

    const materializedSignal = this[_signal];
    const clonedSignal = createDependentAbortSignal(
      new SafeArrayIterator([materializedSignal]),
      prefix,
    );

    const request = new Request(_brand);
    request[_request] = clonedReq;
    request[_signalCache] = clonedSignal;
    headerListFromHeaders(this[_headers]);
    request[_headersGuard] = guardFromHeaders(this[_headers]);
    return request;
  }

  [privateCustomInspect](inspect, inspectOptions) {
    return inspect(
      createFilteredInspectProxy({
        object: this,
        evaluate: ObjectPrototypeIsPrototypeOf(RequestPrototype, this),
        keys: [
          "bodyUsed",
          "headers",
          "method",
          "redirect",
          "url",
        ],
      }),
      inspectOptions,
    );
  }
}

webidl.configureInterface(Request);
const RequestPrototype = Request.prototype;
mixinBody(RequestPrototype, _body, _mimeType);

webidl.converters["Request"] = webidl.createInterfaceConverter(
  "Request",
  RequestPrototype,
);
webidl.converters["RequestInfo_DOMString"] = (V, prefix, context, opts) => {
  // Union for (Request or USVString)
  if (typeof V == "object") {
    if (ObjectPrototypeIsPrototypeOf(RequestPrototype, V)) {
      return webidl.converters["Request"](V, prefix, context, opts);
    }
  }
  // Passed to new URL(...) which implicitly converts DOMString -> USVString
  return webidl.converters["DOMString"](V, prefix, context, opts);
};
webidl.converters["RequestRedirect"] = webidl.createEnumConverter(
  "RequestRedirect",
  [
    "follow",
    "error",
    "manual",
  ],
);
webidl.converters["RequestCache"] = webidl.createEnumConverter(
  "RequestCache",
  [
    "default",
    "no-store",
    "reload",
    "no-cache",
    "force-cache",
    "only-if-cached",
  ],
);
webidl.converters["RequestCredentials"] = webidl.createEnumConverter(
  "RequestCredentials",
  [
    "omit",
    "same-origin",
    "include",
  ],
);
webidl.converters["RequestMode"] = webidl.createEnumConverter(
  "RequestMode",
  [
    "navigate",
    "same-origin",
    "no-cors",
    "cors",
  ],
);
webidl.converters["RequestPriority"] = webidl.createEnumConverter(
  "RequestPriority",
  [
    "auto",
    "low",
    "high",
  ],
);
webidl.converters["ReferrerPolicy"] = webidl.createEnumConverter(
  "ReferrerPolicy",
  [
    "",
    "no-referrer",
    "no-referrer-when-downgrade",
    "same-origin",
    "origin",
    "strict-origin",
    "origin-when-cross-origin",
    "strict-origin-when-cross-origin",
    "unsafe-url",
  ],
);
webidl.converters["RequestInit"] = webidl.createDictionaryConverter(
  "RequestInit",
  [
    { key: "method", converter: webidl.converters["ByteString"] },
    { key: "headers", converter: webidl.converters["HeadersInit"] },
    {
      key: "body",
      converter: webidl.createNullableConverter(
        webidl.converters["BodyInit_DOMString"],
      ),
    },
    { key: "referrer", converter: webidl.converters["USVString"] },
    { key: "referrerPolicy", converter: webidl.converters["ReferrerPolicy"] },
    { key: "mode", converter: webidl.converters["RequestMode"] },
    { key: "credentials", converter: webidl.converters["RequestCredentials"] },
    { key: "cache", converter: webidl.converters["RequestCache"] },
    { key: "redirect", converter: webidl.converters["RequestRedirect"] },
    { key: "integrity", converter: webidl.converters["DOMString"] },
    { key: "keepalive", converter: webidl.converters["boolean"] },
    { key: "priority", converter: webidl.converters["RequestPriority"] },
    {
      key: "signal",
      converter: webidl.createNullableConverter(
        webidl.converters["AbortSignal"],
      ),
    },
  ],
);

return { Request };
})();
