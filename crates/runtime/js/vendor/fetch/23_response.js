// Copyright 2018-2026 the Deno authors. MIT license.

return (function () {
const { core, primordials } = __bootstrap;
const webidl = core.loadExtScript("ext:vendor/webidl/00_webidl.js");
const {
  createFilteredInspectProxy,
  privateCustomInspect,
} = core.loadExtScript("ext:web/console.js");
const {
  byteLowerCase,
  HTTP_TAB_OR_SPACE,
  regexMatcher,
  serializeJSValueToJSONString,
} = core.loadExtScript("ext:vendor/web/00_infra.js");
const { extractBody, mixinBody } = core.loadExtScript(
  "ext:vendor/fetch/22_body.js",
);
const { extractMimeType } = core.loadExtScript("ext:vendor/web/01_mimesniff.js");
const { URL } = core.loadExtScript("ext:vendor/web/00_url.js");
const {
  fillHeaderList,
  getDecodeSplitHeader,
  guardFromHeaders,
  headersFromHeaderList,
} = core.loadExtScript("ext:vendor/fetch/20_headers.js");
const {
  ArrayPrototypeMap,
  ArrayPrototypePush,
  ObjectDefineProperties,
  ObjectHasOwn,
  ObjectPrototypeIsPrototypeOf,
  RangeError,
  RegExpPrototypeExec,
  SafeArrayIterator,
  SafeRegExp,
  Symbol,
  SymbolIterator,
  TypeError,
  TypedArrayPrototypeGetSymbolToStringTag,
} = primordials;

const VCHAR = ["\x21-\x7E"];
const OBS_TEXT = ["\x80-\xFF"];

const REASON_PHRASE = [
  ...new SafeArrayIterator(HTTP_TAB_OR_SPACE),
  ...new SafeArrayIterator(VCHAR),
  ...new SafeArrayIterator(OBS_TEXT),
];
const REASON_PHRASE_MATCHER = regexMatcher(REASON_PHRASE);
const REASON_PHRASE_RE = new SafeRegExp(`^[${REASON_PHRASE_MATCHER}]*$`);

const _response = Symbol("response");
const _headers = Symbol("headers");
const _headersGuard = Symbol("headers guard");
const _mimeType = Symbol("mime type");
const _body = Symbol("body");
const _lazyStaticBody = Symbol("lazy static body");
const _lazyStaticContentType = Symbol("lazy static content type");
const _brand = webidl.brand;

// it's slightly faster to cache these
const webidlConvertersBodyInitDomString =
  webidl.converters["BodyInit_DOMString?"];
const webidlConvertersUSVString = webidl.converters["USVString"];
const webidlConvertersUnsignedShort = webidl.converters["unsigned short"];
const webidlConvertersAny = webidl.converters["any"];
const webidlConvertersByteString = webidl.converters["ByteString"];
const webidlConvertersHeadersInit = webidl.converters["HeadersInit"];

/**
 * @typedef InnerResponse
 * @property {"basic" | "cors" | "default" | "error" | "opaque" | "opaqueredirect"} type
 * @property {() => string | null} url
 * @property {string[]} urlList
 * @property {number} status
 * @property {string} statusMessage
 * @property {[string, string][]} headerList
 * @property {null | typeof __window.bootstrap.fetchBody.InnerBody} body
 */

/**
 * @param {number} status
 * @returns {boolean}
 */
function nullBodyStatus(status) {
  return status === 101 || status === 204 || status === 205 || status === 304;
}

/**
 * @param {number} status
 * @returns {boolean}
 */
function redirectStatus(status) {
  return status === 301 || status === 302 || status === 303 ||
    status === 307 || status === 308;
}

/**
 * https://fetch.spec.whatwg.org/#concept-response-clone
 * @param {InnerResponse} response
 * @returns {InnerResponse}
 */
function cloneInnerResponse(response) {
  const urlList = [...new SafeArrayIterator(response.urlList)];
  const headerList = ArrayPrototypeMap(
    response.headerList,
    (x) => [x[0], x[1]],
  );

  let body = null;
  if (response.body !== null) {
    body = response.body.clone();
  }

  return {
    type: response.type,
    body,
    headerList,
    urlList,
    status: response.status,
    statusMessage: response.statusMessage,
    url() {
      if (this.urlList.length == 0) return null;
      return this.urlList[this.urlList.length - 1];
    },
  };
}

/**
 * @returns {InnerResponse}
 */
function newInnerResponse(status = 200, statusMessage = "") {
  return {
    type: "default",
    body: null,
    headerList: [],
    urlList: [],
    status,
    statusMessage,
    url() {
      if (this.urlList.length == 0) return null;
      return this.urlList[this.urlList.length - 1];
    },
  };
}

/**
 * @param {Response} response
 * @returns {HeaderList}
 */
function responseHeaderList(response) {
  materializeLazyStaticBody(response);
  const lazyStaticContentType = response[_lazyStaticContentType];
  if (lazyStaticContentType !== null) {
    ArrayPrototypePush(response[_response].headerList, [
      "content-type",
      lazyStaticContentType,
    ]);
    response[_lazyStaticContentType] = null;
  }
  return response[_response].headerList;
}

/**
 * @param {Response} response
 * @returns {"immutable" | "request" | "request-no-cors" | "response" | "none"}
 */
function responseHeaderGuard(response) {
  const headers = response[_headers];
  return headers === null ? response[_headersGuard] : guardFromHeaders(headers);
}

/**
 * @param {Response} response
 * @returns {Headers}
 */
function responseHeaders(response) {
  let headers = response[_headers];
  if (headers === null) {
    headers = headersFromHeaderList(
      responseHeaderList(response),
      response[_headersGuard],
    );
    response[_headers] = headers;
  }
  return headers;
}

function initializeResponseBase(response, inner, guard) {
  response[_response] = inner;
  response[_headers] = null;
  response[_headersGuard] = guard;
  response[_lazyStaticBody] = null;
  response[_lazyStaticContentType] = null;
}

function materializeLazyStaticBody(response) {
  const lazyStaticBody = response[_lazyStaticBody];
  if (lazyStaticBody === null || lazyStaticBody === undefined) return;

  const { body, contentType } = extractBody(lazyStaticBody);
  const lazyStaticContentType = response[_lazyStaticContentType];
  response[_response].body = body;
  if (lazyStaticContentType !== null || contentType !== null) {
    ArrayPrototypePush(response[_response].headerList, [
      "Content-Type",
      lazyStaticContentType ?? contentType,
    ]);
  }
  response[_lazyStaticBody] = null;
  response[_lazyStaticContentType] = null;
}

function tryGetSingleContentTypeHeader(object) {
  if (
    typeof object !== "object" || object === null ||
    object[SymbolIterator] !== undefined || core.isProxy(object)
  ) {
    return null;
  }

  let contentType = undefined;
  let headerName = undefined;
  for (const key in object) {
    if (!ObjectHasOwn(object, key)) {
      continue;
    }
    if (headerName !== undefined) {
      return null;
    }
    if (
      key !== "content-type" && key !== "Content-Type" &&
      byteLowerCase(key) !== "content-type"
    ) {
      return null;
    }
    headerName = key;
    contentType = object[key];
  }

  if (headerName === undefined) {
    return null;
  }

  if (
    contentType !== "text/plain" &&
    contentType !== "text/plain;charset=UTF-8" &&
    contentType !== "application/json"
  ) {
    return null;
  }

  return contentType;
}

function tryFillSingleContentTypeHeader(list, object) {
  const contentType = tryGetSingleContentTypeHeader(object);
  if (contentType === null) {
    return false;
  }
  // Safe to bypass normal header validation: tryGetSingleContentTypeHeader()
  // accepts only the exact literal content-type values listed above.
  ArrayPrototypePush(list, ["content-type", contentType]);
  return true;
}

function tryInitializeStaticResponseFast(response, init, bodyWithType) {
  if (
    bodyWithType === null || init.status !== 200 || init.statusText !== ""
  ) {
    return false;
  }

  const { body, contentType } = bodyWithType;
  const staticBody = body.streamOrStatic?.body;
  if (
    typeof staticBody !== "string" &&
    TypedArrayPrototypeGetSymbolToStringTag(staticBody) !== "Uint8Array"
  ) {
    return false;
  }

  if (init.headers !== undefined) {
    const headerContentType = tryGetSingleContentTypeHeader(init.headers);
    if (headerContentType === null) {
      return false;
    }
    response[_lazyStaticContentType] = headerContentType;
  } else if (contentType !== null) {
    response[_lazyStaticContentType] = contentType;
  }

  response[_response].body = body;
  return true;
}

/**
 * https://fetch.spec.whatwg.org#initialize-a-response
 * @param {Response} response
 * @param {ResponseInit} init
 * @param {{ body: fetchBody.InnerBody, contentType: string | null } | null} bodyWithType
 */
function initializeAResponse(
  response,
  init,
  bodyWithType,
  prefix,
  context,
) {
  // 1.
  if ((init.status < 200 || init.status > 599) && init.status != 101) {
    throw new RangeError(
      `The status provided (${init.status}) is not equal to 101 and outside the range [200, 599]`,
    );
  }

  // 2.
  if (
    init.statusText &&
    RegExpPrototypeExec(REASON_PHRASE_RE, init.statusText) === null
  ) {
    throw new TypeError(
      `Invalid status text: "${init.statusText}"`,
    );
  }

  // 3.
  response[_response].status = init.status;

  // 4.
  response[_response].statusMessage = init.statusText;
  // 5.
  if (init.headers !== undefined) {
    const list = responseHeaderList(response);
    if (
      !tryFillSingleContentTypeHeader(
        list,
        init.headers,
      )
    ) {
      fillHeaderList(
        list,
        init.headers,
        prefix,
        context,
      );
    }
  }

  // 6.
  if (bodyWithType !== null) {
    if (nullBodyStatus(response[_response].status)) {
      throw new TypeError(
        "Response with null body status cannot have body",
      );
    }

    const { body, contentType } = bodyWithType;
    response[_response].body = body;

    if (contentType !== null) {
      const list = responseHeaderList(response);
      let hasContentType = false;
      for (let i = 0; i < list.length; i++) {
        if (byteLowerCase(list[i][0]) === "content-type") {
          hasContentType = true;
          break;
        }
      }
      if (!hasContentType) {
        ArrayPrototypePush(list, ["Content-Type", contentType]);
      }
    }
  }
}

/**
 * @param {Response} response
 * @param {{ body: fetchBody.InnerBody, contentType: string | null } | null} bodyWithType
 */
function initializeAResponseDefault(response, bodyWithType) {
  if (bodyWithType === null) {
    return;
  }

  const { body, contentType } = bodyWithType;
  response[_response].body = body;
  if (contentType !== null) {
    ArrayPrototypePush(response[_response].headerList, [
      "Content-Type",
      contentType,
    ]);
  }
}

class Response {
  get [_mimeType]() {
    const values = getDecodeSplitHeader(
      responseHeaderList(this),
      "Content-Type",
    );
    return extractMimeType(values);
  }
  get [_body]() {
    materializeLazyStaticBody(this);
    return this[_response].body;
  }

  /**
   * @returns {Response}
   */
  static error() {
    const inner = newInnerResponse(0);
    inner.type = "error";
    const response = webidl.createBranded(Response);
    initializeResponseBase(response, inner, "immutable");
    return response;
  }

  /**
   * @param {string} url
   * @param {number} status
   * @returns {Response}
   */
  static redirect(url, status = 302) {
    const prefix = "Failed to execute 'Response.redirect'";
    url = webidlConvertersUSVString(url, prefix, "Argument 1");
    status = webidlConvertersUnsignedShort(status, prefix, "Argument 2");

    const parsedURL = new URL(url);
    if (!redirectStatus(status)) {
      throw new RangeError(`Invalid redirect status code: ${status}`);
    }
    const inner = newInnerResponse(status);
    inner.type = "default";
    ArrayPrototypePush(inner.headerList, ["Location", parsedURL.href]);
    const response = webidl.createBranded(Response);
    initializeResponseBase(response, inner, "immutable");
    return response;
  }

  /**
   * @param {any} data
   * @param {ResponseInit} init
   * @returns {Response}
   */
  static json(data = undefined, init = undefined) {
    const prefix = "Failed to execute 'Response.json'";
    data = webidlConvertersAny(data);
    const defaultInit = init === undefined || init === null;
    const str = serializeJSValueToJSONString(data);
    if (defaultInit) {
      const response = webidl.createBranded(Response);
      initializeResponseBase(response, newInnerResponse(), "response");
      response[_lazyStaticBody] = str;
      response[_lazyStaticContentType] = "application/json";
      return response;
    }
    init = webidlConvertersResponseInitFast(init, prefix, "Argument 2");

    const res = extractBody(str);
    res.contentType = "application/json";
    const response = webidl.createBranded(Response);
    initializeResponseBase(response, newInnerResponse(), "response");
    if (tryInitializeStaticResponseFast(response, init, res)) {
      return response;
    }
    initializeAResponse(response, init, res, prefix, "Argument 2");
    return response;
  }

  /**
   * @param {BodyInit | null} body
   * @param {ResponseInit} init
   */
  constructor(body = null, init = undefined) {
    if (body === _brand) {
      this[_brand] = _brand;
      return;
    }

    const prefix = "Failed to construct 'Response'";
    body = webidlConvertersBodyInitDomString(body, prefix, "Argument 1");
    const defaultInit = init === undefined || init === null;
    if (!defaultInit) {
      init = webidlConvertersResponseInitFast(init, prefix, "Argument 2");
    }

    this[_brand] = _brand;
    initializeResponseBase(this, newInnerResponse(), "response");

    let bodyWithType = null;
    if (body !== null) {
      if (defaultInit && typeof body === "string") {
        this[_lazyStaticBody] = body;
        return;
      }
      bodyWithType = extractBody(body);
    }
    if (defaultInit) {
      initializeAResponseDefault(this, bodyWithType);
    } else {
      if (tryInitializeStaticResponseFast(this, init, bodyWithType)) {
        return;
      }
      initializeAResponse(this, init, bodyWithType, prefix, "Argument 2");
    }
  }

  /**
   * @returns {"basic" | "cors" | "default" | "error" | "opaque" | "opaqueredirect"}
   */
  get type() {
    webidl.assertBranded(this, ResponsePrototype);
    return this[_response].type;
  }

  /**
   * @returns {string}
   */
  get url() {
    webidl.assertBranded(this, ResponsePrototype);
    const url = this[_response].url();
    if (url === null) return "";
    const newUrl = new URL(url);
    newUrl.hash = "";
    return newUrl.href;
  }

  /**
   * @returns {boolean}
   */
  get redirected() {
    webidl.assertBranded(this, ResponsePrototype);
    return this[_response].urlList.length > 1;
  }

  /**
   * @returns {number}
   */
  get status() {
    webidl.assertBranded(this, ResponsePrototype);
    return this[_response].status;
  }

  /**
   * @returns {boolean}
   */
  get ok() {
    webidl.assertBranded(this, ResponsePrototype);
    const status = this[_response].status;
    return status >= 200 && status <= 299;
  }

  /**
   * @returns {string}
   */
  get statusText() {
    webidl.assertBranded(this, ResponsePrototype);
    return this[_response].statusMessage;
  }

  /**
   * @returns {Headers}
   */
  get headers() {
    webidl.assertBranded(this, ResponsePrototype);
    return responseHeaders(this);
  }

  /**
   * @returns {Response}
   */
  clone() {
    webidl.assertBranded(this, ResponsePrototype);
    materializeLazyStaticBody(this);
    if (this[_body] && this[_body].unusable()) {
      throw new TypeError("Body is unusable");
    }
    const second = webidl.createBranded(Response);
    const newRes = cloneInnerResponse(this[_response]);
    initializeResponseBase(second, newRes, responseHeaderGuard(this));
    return second;
  }

  [privateCustomInspect](inspect, inspectOptions) {
    return inspect(
      createFilteredInspectProxy({
        object: this,
        evaluate: ObjectPrototypeIsPrototypeOf(ResponsePrototype, this),
        keys: [
          "body",
          "bodyUsed",
          "headers",
          "ok",
          "redirected",
          "status",
          "statusText",
          "url",
        ],
      }),
      inspectOptions,
    );
  }
}

webidl.configureInterface(Response);
ObjectDefineProperties(Response, {
  json: { __proto__: null, enumerable: true },
  redirect: { __proto__: null, enumerable: true },
  error: { __proto__: null, enumerable: true },
});
const ResponsePrototype = Response.prototype;
mixinBody(ResponsePrototype, _body, _mimeType);

webidl.converters["Response"] = webidl.createInterfaceConverter(
  "Response",
  ResponsePrototype,
);
const webidlConvertersResponseInit = webidl.converters["ResponseInit"] = webidl
  .createDictionaryConverter(
    "ResponseInit",
    [{
      key: "status",
      defaultValue: 200,
      converter: webidlConvertersUnsignedShort,
    }, {
      key: "statusText",
      defaultValue: "",
      converter: webidlConvertersByteString,
    }, {
      key: "headers",
      converter: webidlConvertersHeadersInit,
    }],
  );
const webidlConvertersResponseInitFast = webidl
  .converters["ResponseInit_fast"] = function (
    init,
    prefix,
    context,
    opts,
  ) {
    if (init === undefined || init === null) {
      return { status: 200, statusText: "", headers: undefined };
    }
    // Fast path, if not a proxy
    if (typeof init === "object" && !core.isProxy(init)) {
      // Not a proxy fast path
      const status = init.status !== undefined
        ? webidlConvertersUnsignedShort(init.status)
        : 200;
      const statusText = init.statusText !== undefined
        ? webidlConvertersByteString(init.statusText)
        : "";
      return { status, statusText, headers: init.headers };
    }
    // Slow default path
    return webidlConvertersResponseInit(init, prefix, context, opts);
  };

/**
 * @param {InnerResponse} inner
 * @param {"request" | "immutable" | "request-no-cors" | "response" | "none"} guard
 * @returns {Response}
 */
function fromInnerResponse(inner, guard) {
  const response = new Response(_brand);
  initializeResponseBase(response, inner, guard);
  return response;
}

return {
  fromInnerResponse,
  newInnerResponse,
  nullBodyStatus,
  Response,
};
})();
