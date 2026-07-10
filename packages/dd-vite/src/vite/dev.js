import { createHash } from "node:crypto";
import { lstat, readFile, realpath, stat } from "node:fs/promises";
import { dirname, extname, isAbsolute, join, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
import {
  arrayOfStrings,
  deploymentAssetsOutputDir,
  isMissingFileError,
  nonEmptyString,
  resolveViteOutDir,
  uniqueStrings,
} from "./config.js";

const DEFAULT_DEPLOYMENT_CONFIG_FILE = "dd.deploy.json";
const DEFAULT_STATIC_ROUTES_FILE = "_routes.json";
const DD_VITE_BYPASS_HEADER = "x-dd-vite-bypass";
const BODYLESS_METHODS = new Set(["GET", "HEAD"]);
const VITE_BYPASS_PREFIXES = [
  "/@vite", "/@id/", "/@fs/", "/@react-refresh", "/@react-router/", "/__vitest__/",
  "/__vitest_attachment__", "/__vitest_browser__/", "/__vitest_browser_api__/",
  "/__vitest_test__/", "/node_modules/", "/.vite/",
];
const VITE_SOURCE_PREFIXES = ["/app/", "/src/"];
const VITE_ANY_METHOD_BYPASS_PATHS = [
  "/__vite_rsc_findSourceMapURL", "/__vite_rsc_load_module_dev_proxy",
];
const WEBSOCKET_ACCEPT_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
const MAX_DEV_WEBSOCKET_FRAME_BYTES = 8 * 1024 * 1024;
const DD_WS_BINARY_HEADER = "x-dd-ws-binary";
const DD_WS_CLOSE_CODE_HEADER = "x-dd-ws-close-code";
const DD_WS_CLOSE_REASON_HEADER = "x-dd-ws-close-reason";

export function viteRequestForFile(file, root) {
  const normalizedRoot = toVitePath(resolve(root));
  const normalizedFile = toVitePath(isAbsolute(String(file)) ? String(file) : resolve(root, String(file)));
  if (normalizedFile === normalizedRoot) {
    return "/";
  }
  if (normalizedFile.startsWith(`${normalizedRoot}/`)) {
    return `/${normalizedFile.slice(normalizedRoot.length + 1)}`;
  }
  return `/@fs/${normalizedFile}`;
}

export function viteDevServerOrigins(viteServer) {
  const origins = new Set();
  for (const url of viteDevServerUrlCandidates(viteServer)) {
    try {
      origins.add(new URL(url).origin);
    } catch {}
  }
  origins.add("http://127.0.0.1:5173");
  origins.add("http://localhost:5173");
  return [...origins];
}

export function rewriteViteDevServerUrls(code, viteServer) {
  if (!code) {
    return code;
  }
  const actual = viteDevServerActualUrl(viteServer);
  if (!actual) {
    return code;
  }
  let rewritten = code;
  for (const candidate of viteDevServerUrlCandidates(viteServer)) {
    if (candidate !== actual) {
      rewritten = rewritten.split(candidate).join(actual);
    }
  }
  return rewritten;
}

export function viteDevServerUrlCandidates(viteServer) {
  const urls = [];
  for (const entries of Object.values(viteServer.resolvedUrls ?? {})) {
    urls.push(...(entries ?? []));
  }
  const actual = viteDevServerActualUrl(viteServer);
  if (actual) {
    urls.push(actual);
    urls.push("http://127.0.0.1:5173/");
    urls.push("http://localhost:5173/");
  }
  return uniqueStrings(urls);
}

export function viteDevServerActualUrl(viteServer) {
  const address = viteServer.httpServer?.address?.();
  if (address && typeof address === "object") {
    const protocol = viteServer.config.server.https ? "https" : "http";
    const host = viteDevServerAddressHost(address.address);
    return `${protocol}://${host}:${address.port}/`;
  }
  return undefined;
}

export function viteDevServerAddressHost(address) {
  const value = String(address ?? "");
  if (!value || value === "::" || value === "0.0.0.0") {
    return "127.0.0.1";
  }
  if (value.includes(":")) {
    return `[${value}]`;
  }
  return value;
}

export function patchViteResolvedUrls(viteServer) {
  const actual = viteDevServerActualUrl(viteServer);
  if (!actual) {
    return;
  }
  viteServer.resolvedUrls ??= { local: [], network: [] };
  viteServer.resolvedUrls.local ??= [];
  if (viteServer.resolvedUrls.local[0] !== actual) {
    viteServer.resolvedUrls.local = [
      actual,
      ...viteServer.resolvedUrls.local.filter((url) => url !== actual),
    ];
  }
}

export function toVitePath(value) {
  return String(value).replace(/\\/g, "/");
}

export async function nodeRequestToWorkerRequest(req, originalUrl, mount, workerName) {
  const body = await readIncomingBody(req);
  const path = stripMount(originalUrl, mount);
  const headers = new Headers();
  for (const [name, value] of Object.entries(req.headers)) {
    if (Array.isArray(value)) {
      for (const entry of value) {
        headers.append(name, entry);
      }
    } else if (value !== undefined) {
      headers.append(name, value);
    }
  }
  const url = nodeRequestWorkerUrl(req, path, workerName);
  const init = {
    method: req.method ?? "GET",
    headers,
  };
  if (body.length > 0 && !BODYLESS_METHODS.has(init.method.toUpperCase())) {
    init.body = body;
    init.duplex = "half";
  }
  return new Request(url, init);
}

export async function writeNodeResponse(res, response) {
  res.statusCode = response.status;
  response.headers.forEach((value, name) => {
    const lower = name.toLowerCase();
    if (lower === "content-length" || lower === "transfer-encoding") {
      return;
    }
    res.setHeader(name, value);
  });
  if (!response.body) {
    res.setHeader("content-length", "0");
    res.end();
    return;
  }
  for await (const chunk of response.body) {
    if (res.destroyed) {
      return;
    }
    if (!res.write(Buffer.from(chunk))) {
      if (!(await waitForNodeResponseDrain(res))) {
        return;
      }
    }
  }
  res.end();
}

export async function waitForNodeResponseDrain(res) {
  if (res.destroyed) {
    return false;
  }
  return new Promise((resolveWait, rejectWait) => {
    const cleanup = () => {
      res.off("drain", onDrain);
      res.off("close", onClose);
      res.off("error", onError);
    };
    const onDrain = () => {
      cleanup();
      resolveWait(!res.destroyed);
    };
    const onClose = () => {
      cleanup();
      resolveWait(false);
    };
    const onError = (error) => {
      cleanup();
      if (res.destroyed) {
        resolveWait(false);
      } else {
        rejectWait(error);
      }
    };
    res.once("drain", onDrain);
    res.once("close", onClose);
    res.once("error", onError);
  });
}

export async function handleDdWebSocketUpgrade(req, socket, head, options) {
  const originalUrl = req.url ?? "/";
  if (
    !isWebSocketUpgrade(req) ||
    shouldBypassDdWebSocketUpgrade(req, originalUrl, options.mount, options.viteBase)
  ) {
    return;
  }

  socket.pause?.();
  try {
    await options.ensureDeployed();
    const runtime = options.runtime();
    if (!runtime) {
      throw new Error("dd runtime is not available for websocket upgrade");
    }
    const workerName = await options.effectiveWorkerName();
    const opened = await runtime.openWebSocket(
      workerName,
      nodeUpgradeToWorkerInvocation(req, originalUrl, options.mount, workerName),
    );
    if (Number(opened.status) !== 101) {
      writeRawHttpResponse(socket, opened.status ?? 400, opened.headers, opened.body_base64);
      return;
    }

    writeWebSocketHandshake(socket, req, opened.headers);
    const bridge = new DdWebSocketBridge({
      runtime,
      sessionId: opened.session_id,
      socket,
      workerName,
    });
    bridge.start(head);
    bridge.forwardRuntimeOutput(opened);
  } catch (error) {
    if (!socket.destroyed) {
      writeRawHttpResponse(socket, 500, [], Buffer.from(String(error?.message ?? error)).toString("base64"));
    }
  }
}

export function isWebSocketUpgrade(req) {
  return headerValue(req.headers.upgrade).toLowerCase() === "websocket";
}

export function shouldBypassDdWebSocketUpgrade(req, originalUrl, mount, viteBase) {
  if (headerValue(req.headers[DD_VITE_BYPASS_HEADER]) === "1") {
    return true;
  }
  if (!matchesMount(originalUrl, mount)) {
    return true;
  }
  const protocol = headerValue(req.headers["sec-websocket-protocol"])
    .split(",")
    .map((value) => value.trim().toLowerCase());
  if (protocol.includes("vite-hmr")) {
    return true;
  }
  const url = new URL(originalUrl, "http://dd-vite.local");
  return candidatePathnames(url.pathname, viteBase).some((pathname) =>
    isViteBypassPath(pathname) || isViteAnyMethodBypassPath(pathname)
  );
}

export function nodeUpgradeToWorkerInvocation(req, originalUrl, mount, workerName) {
  const path = stripMount(originalUrl, mount);
  const url = nodeRequestWorkerUrl(req, path, workerName);
  return {
    method: req.method ?? "GET",
    url: String(url),
    headers: nodeRequestHeaders(req),
    body_base64: "",
  };
}

export function nodeRequestWorkerUrl(req, path, workerName) {
  const fallbackOrigin = `http://${workerName}.dd.local`;
  const host = headerValue(req.headers.host).trim();
  if (!host) {
    return new URL(path, fallbackOrigin);
  }
  const forwardedProtocol = headerValue(req.headers["x-forwarded-proto"])
    .split(",")[0]
    ?.trim();
  const protocol = forwardedProtocol || (req.socket?.encrypted ? "https" : "http");
  try {
    return new URL(path, `${protocol}://${host}`);
  } catch {
    return new URL(path, fallbackOrigin);
  }
}

export function nodeRequestHeaders(req) {
  const headers = [];
  for (const [name, value] of Object.entries(req.headers)) {
    if (Array.isArray(value)) {
      for (const entry of value) {
        headers.push([name, entry]);
      }
    } else if (value !== undefined) {
      headers.push([name, value]);
    }
  }
  return headers;
}

export function writeWebSocketHandshake(socket, req, headers = []) {
  const key = headerValue(req.headers["sec-websocket-key"]).trim();
  if (!key) {
    throw new Error("missing sec-websocket-key");
  }
  const accept = createHash("sha1").update(`${key}${WEBSOCKET_ACCEPT_GUID}`).digest("base64");
  const lines = [
    "HTTP/1.1 101 Switching Protocols",
    "Upgrade: websocket",
    "Connection: Upgrade",
    `Sec-WebSocket-Accept: ${accept}`,
  ];
  for (const [name, value] of headers) {
    if (shouldSkipWebSocketHandshakeHeader(name)) {
      continue;
    }
    lines.push(`${name}: ${value}`);
  }
  socket.write(`${lines.join("\r\n")}\r\n\r\n`);
}

export function shouldSkipWebSocketHandshakeHeader(name) {
  const lower = String(name).toLowerCase();
  return (
    lower === "connection" ||
    lower === "upgrade" ||
    lower === "sec-websocket-accept" ||
    lower === "content-length" ||
    lower === "transfer-encoding"
  );
}

export function writeRawHttpResponse(socket, status, headers = [], bodyBase64 = "") {
  const body = bodyBase64 ? Buffer.from(bodyBase64, "base64") : Buffer.alloc(0);
  const statusCode = Number(status) || 500;
  const reason = statusReasonPhrase(statusCode);
  const lines = [
    `HTTP/1.1 ${statusCode} ${reason}`,
    "Connection: close",
    `Content-Length: ${body.length}`,
  ];
  for (const [name, value] of headers) {
    const lower = String(name).toLowerCase();
    if (lower === "connection" || lower === "content-length" || lower === "transfer-encoding") {
      continue;
    }
    lines.push(`${name}: ${value}`);
  }
  socket.end(Buffer.concat([Buffer.from(`${lines.join("\r\n")}\r\n\r\n`), body]));
}

export function statusReasonPhrase(status) {
  switch (status) {
    case 400:
      return "Bad Request";
    case 404:
      return "Not Found";
    case 500:
      return "Internal Server Error";
    default:
      return status >= 200 && status < 300 ? "OK" : "Error";
  }
}

class DdWebSocketBridge {
  constructor({ runtime, sessionId, socket, workerName }) {
    this.runtime = runtime;
    this.sessionId = sessionId;
    this.socket = socket;
    this.workerName = workerName;
    this.buffer = Buffer.alloc(0);
    this.closed = false;
    this.closeSent = false;
    this.runtimeClosed = false;
    this.drainLoopRunning = false;
    this.frameQueue = Promise.resolve();
  }

  start(head) {
    this.socket.setNoDelay?.(true);
    this.socket.on("data", (chunk) => this.consume(chunk));
    this.socket.on("close", () => {
      this.closed = true;
      void this.closeRuntime(1006, "socket closed");
    });
    this.socket.on("error", () => {
      this.closed = true;
      void this.closeRuntime(1011, "socket error");
    });
    if (head?.length) {
      this.consume(head);
    }
    this.socket.resume?.();
    void this.runRuntimeFrameLoop();
  }

  consume(chunk) {
    if (this.closed) {
      return;
    }
    this.buffer = Buffer.concat([this.buffer, Buffer.from(chunk)]);
    for (;;) {
      const parsed = parseClientWebSocketFrame(this.buffer);
      if (!parsed) {
        return;
      }
      if (parsed.error) {
        void this.close(1002, parsed.error);
        return;
      }
      this.buffer = parsed.rest;
      this.frameQueue = this.frameQueue
        .then(() => this.handleFrame(parsed.frame))
        .catch((error) => this.fail(error));
    }
  }

  async handleFrame(frame) {
    if (this.closed) {
      return;
    }
    switch (frame.opcode) {
      case 0x1:
      case 0x2: {
        const output = await this.runtime.sendWebSocketFrame(
          this.workerName,
          this.sessionId,
          frame.payload,
          { binary: frame.opcode === 0x2 },
        );
        this.forwardRuntimeOutput(output);
        void this.runRuntimeFrameLoop();
        break;
      }
      case 0x8: {
        const { code, reason } = parseClientClosePayload(frame.payload);
        await this.close(code, reason);
        break;
      }
      case 0x9:
        sendServerWebSocketFrame(this.socket, 0xA, frame.payload);
        break;
      case 0xA:
        break;
      default:
        await this.close(1003, "unsupported websocket frame");
        break;
    }
  }

  async runRuntimeFrameLoop() {
    if (this.closed || this.drainLoopRunning) {
      return;
    }
    this.drainLoopRunning = true;
    try {
      while (!this.closed) {
        const drained = await this.drainRuntimeFrames();
        if (this.closed || drained > 0) {
          continue;
        }
        await this.runtime.waitWebSocketFrame(this.workerName, this.sessionId);
      }
    } catch (error) {
      this.fail(error);
    } finally {
      this.drainLoopRunning = false;
    }
  }

  async drainRuntimeFrames() {
    let drained = 0;
    for (let index = 0; index < 32 && !this.closed; index += 1) {
      const result = await this.runtime.drainWebSocketFrame(this.workerName, this.sessionId);
      if (!result.frame) {
        break;
      }
      drained += 1;
      this.forwardRuntimeOutput(result.frame);
    }
    return drained;
  }

  forwardRuntimeOutput(output) {
    if (this.closed || !output) {
      return;
    }
    const headers = output.headers ?? [];
    const body = output.body_base64 ? Buffer.from(output.body_base64, "base64") : Buffer.alloc(0);
    if (body.length > 0) {
      const binary = headerListValue(headers, DD_WS_BINARY_HEADER) === "1";
      sendServerWebSocketFrame(this.socket, binary ? 0x2 : 0x1, body);
    }
    const closeCode = headerListValue(headers, DD_WS_CLOSE_CODE_HEADER);
    if (closeCode) {
      const reason = headerListValue(headers, DD_WS_CLOSE_REASON_HEADER);
      void this.close(Number(closeCode) || 1000, reason, { closeRuntime: false });
    }
  }

  async close(code = 1000, reason = "", options = {}) {
    if (this.closed) {
      return;
    }
    this.closed = true;
    if (options.closeRuntime !== false) {
      await this.closeRuntime(code, reason);
    }
    if (!this.closeSent && !this.socket.destroyed) {
      this.closeSent = true;
      sendServerWebSocketFrame(this.socket, 0x8, encodeClosePayload(code, reason));
    }
    this.socket.end();
  }

  async closeRuntime(code, reason) {
    if (this.runtimeClosed) {
      return;
    }
    this.runtimeClosed = true;
    await this.runtime.closeWebSocket(this.workerName, this.sessionId, { code, reason }).catch(() => {});
  }

  fail(error) {
    if (!this.closed) {
      void this.close(1011, String(error?.message ?? error));
    }
  }
}

export function parseClientWebSocketFrame(buffer) {
  if (buffer.length < 2) {
    return undefined;
  }
  const first = buffer[0];
  const second = buffer[1];
  const fin = (first & 0x80) !== 0;
  const opcode = first & 0x0f;
  const masked = (second & 0x80) !== 0;
  let length = second & 0x7f;
  let offset = 2;
  if (length === 126) {
    if (buffer.length < offset + 2) {
      return undefined;
    }
    length = buffer.readUInt16BE(offset);
    offset += 2;
  } else if (length === 127) {
    if (buffer.length < offset + 8) {
      return undefined;
    }
    const bigLength = buffer.readBigUInt64BE(offset);
    if (bigLength > BigInt(MAX_DEV_WEBSOCKET_FRAME_BYTES)) {
      return { error: "websocket frame is too large", rest: Buffer.alloc(0) };
    }
    length = Number(bigLength);
    offset += 8;
  }
  if (length > MAX_DEV_WEBSOCKET_FRAME_BYTES) {
    return { error: "websocket frame is too large", rest: Buffer.alloc(0) };
  }
  if (!masked) {
    return { error: "client websocket frames must be masked", rest: Buffer.alloc(0) };
  }
  if (!fin) {
    return { error: "fragmented websocket frames are not supported in dd vite dev", rest: Buffer.alloc(0) };
  }
  if (buffer.length < offset + 4 + length) {
    return undefined;
  }
  const mask = buffer.subarray(offset, offset + 4);
  offset += 4;
  const payload = Buffer.alloc(length);
  for (let index = 0; index < length; index += 1) {
    payload[index] = buffer[offset + index] ^ mask[index % 4];
  }
  return {
    frame: { opcode, payload },
    rest: buffer.subarray(offset + length),
  };
}

export function sendServerWebSocketFrame(socket, opcode, payload) {
  if (socket.destroyed) {
    return;
  }
  const body = Buffer.isBuffer(payload) ? payload : Buffer.from(payload ?? "");
  let header;
  if (body.length < 126) {
    header = Buffer.from([0x80 | opcode, body.length]);
  } else if (body.length <= 0xffff) {
    header = Buffer.alloc(4);
    header[0] = 0x80 | opcode;
    header[1] = 126;
    header.writeUInt16BE(body.length, 2);
  } else {
    header = Buffer.alloc(10);
    header[0] = 0x80 | opcode;
    header[1] = 127;
    header.writeBigUInt64BE(BigInt(body.length), 2);
  }
  socket.write(Buffer.concat([header, body]));
}

export function parseClientClosePayload(payload) {
  if (payload.length < 2) {
    return { code: 1000, reason: "" };
  }
  return {
    code: payload.readUInt16BE(0),
    reason: payload.subarray(2).toString("utf8"),
  };
}

export function encodeClosePayload(code, reason) {
  const text = Buffer.from(String(reason ?? "").slice(0, 120));
  const payload = Buffer.alloc(2 + text.length);
  payload.writeUInt16BE(Number(code) || 1000, 0);
  text.copy(payload, 2);
  return payload;
}

export function headerListValue(headers, name) {
  const found = (headers ?? []).find(([headerName]) =>
    String(headerName).toLowerCase() === String(name).toLowerCase()
  );
  return found ? String(found[1]) : "";
}

export async function readIncomingBody(req) {
  const chunks = [];
  for await (const chunk of req) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }
  return Buffer.concat(chunks);
}

export function normalizeMount(value) {
  const mount = `/${String(value).replace(/^\/+|\/+$/g, "")}`;
  return mount === "/" ? "/" : mount;
}

export function matchesMount(url, mount) {
  if (mount === "/") {
    return true;
  }
  return url === mount || url.startsWith(`${mount}/`) || url.startsWith(`${mount}?`);
}

export function stripMount(url, mount) {
  if (mount === "/") {
    return url;
  }
  const stripped = url.slice(mount.length);
  return stripped.length === 0 ? "/" : stripped;
}

export function shouldBypassViteRequest(req, originalUrl, mount, viteBase) {
  if (headerValue(req.headers[DD_VITE_BYPASS_HEADER]) === "1") {
    return true;
  }
  if (mount !== "/") {
    return false;
  }
  if (headerValue(req.headers.upgrade)) {
    return true;
  }

  const url = new URL(originalUrl, "http://dd-vite.local");
  if (candidatePathnames(url.pathname, viteBase).some(isViteAnyMethodBypassPath)) {
    return true;
  }

  const method = (req.method ?? "GET").toUpperCase();
  if (method !== "GET" && method !== "HEAD") {
    return false;
  }

  const candidatePaths = candidatePathnames(url.pathname, viteBase);
  if (candidatePaths.some(isViteBypassPath)) {
    return true;
  }
  const sourceRequest = candidatePaths.some(isViteSourcePath);
  if (sourceRequest) {
    return true;
  }
  return false;
}

export function candidatePathnames(pathname, viteBase) {
  const paths = [pathname];
  const base = normalizeViteBase(viteBase);
  if (base !== "/" && (pathname === base.slice(0, -1) || pathname.startsWith(base))) {
    paths.push(`/${pathname.slice(base.length)}`);
  }
  return paths;
}

export function isViteBypassPath(pathname) {
  return VITE_BYPASS_PREFIXES.some((prefix) => {
    if (prefix.endsWith("/")) {
      return pathname.startsWith(prefix);
    }
    return pathname === prefix || pathname.startsWith(`${prefix}/`);
  });
}

export function isViteSourcePath(pathname) {
  if (!isViteSourceNamespacePath(pathname)) {
    return false;
  }
  const cleanPath = cleanUrlPathname(pathname);
  const slash = cleanPath.lastIndexOf("/");
  const dot = cleanPath.lastIndexOf(".");
  if (dot <= slash) {
    return false;
  }
  return /^[A-Za-z][A-Za-z0-9_-]*$/.test(cleanPath.slice(dot + 1));
}

export function isViteSourceNamespacePath(pathname) {
  return VITE_SOURCE_PREFIXES.some((prefix) => pathname.startsWith(prefix));
}

export function isViteAnyMethodBypassPath(pathname) {
  return VITE_ANY_METHOD_BYPASS_PATHS.includes(pathname);
}

export function cleanUrlPathname(pathname) {
  try {
    return decodeURIComponent(pathname);
  } catch {
    return pathname;
  }
}

export function normalizeViteBase(value) {
  if (typeof value !== "string" || value.length === 0 || value === "./") {
    return "/";
  }
  let base = value;
  try {
    base = new URL(value).pathname;
  } catch {}
  const normalized = normalizeMount(base);
  return normalized === "/" ? "/" : `${normalized}/`;
}

export function headerValue(value) {
  if (Array.isArray(value)) {
    return value[0] ?? "";
  }
  return value ?? "";
}

export function shouldBypassStaticRoutingRequest(req, originalUrl, mount, routing) {
  const method = (req.method ?? "GET").toUpperCase();
  if (method !== "GET" && method !== "HEAD") {
    return false;
  }
  const url = new URL(stripMount(originalUrl, mount), "http://dd-vite.local");
  if (matchesAnyStaticRoute(routing.exclude, url.pathname)) {
    return true;
  }
  return routing.include.length > 0 && !matchesAnyStaticRoute(routing.include, url.pathname);
}

export async function writeStaticAssetResponse(req, res, originalUrl, mount, routing) {
  const method = (req.method ?? "GET").toUpperCase();
  let pathname;
  try {
    pathname = decodeURIComponent(new URL(stripMount(originalUrl, mount), "http://dd-vite.local").pathname);
  } catch {
    return false;
  }

  const assetRoot = resolve(routing.dir);
  const file = resolve(assetRoot, pathname.replace(/^\/+/, ""));
  if (file === assetRoot || !file.startsWith(`${assetRoot}${sep}`)) {
    return false;
  }

  let fileStat;
  try {
    const linkStat = await lstat(file);
    if (linkStat.isSymbolicLink()) {
      return false;
    }
    const [canonicalRoot, canonicalFile] = await Promise.all([
      realpath(assetRoot),
      realpath(file),
    ]);
    if (canonicalFile === canonicalRoot || !canonicalFile.startsWith(`${canonicalRoot}${sep}`)) {
      return false;
    }
    fileStat = await stat(canonicalFile);
  } catch (error) {
    if (isMissingFileError(error)) {
      return false;
    }
    throw error;
  }
  if (!fileStat.isFile()) {
    return false;
  }

  const body = method === "HEAD" ? undefined : await readFile(file);
  res.statusCode = 200;
  res.setHeader("content-type", staticAssetContentType(file));
  res.setHeader("cache-control", "no-store");
  res.setHeader("content-length", String(fileStat.size));
  res.end(body);
  return true;
}

export async function loadStaticRouting(source, viteConfig, deploymentConfig) {
  const configured = staticRoutingFromDeploymentConfig(viteConfig, deploymentConfig);
  if (configured) {
    return configured;
  }
  for (const path of await staticRoutingCandidatePaths(source, viteConfig)) {
    try {
      const contents = await readFile(path, "utf8");
      const routes = JSON.parse(contents);
      return {
        dir: dirname(path),
        include: arrayOfStrings(routes?.include),
        exclude: arrayOfStrings(routes?.exclude),
      };
    } catch (error) {
      if (!isMissingFileError(error)) {
        throw error;
      }
    }
  }
  return undefined;
}

export function staticRoutingFromDeploymentConfig(viteConfig, deploymentConfig) {
  const routes = deploymentConfig.staticRoutes;
  if (!routes) {
    return undefined;
  }
  const assetsDir = deploymentAssetsOutputDir(
    resolveViteOutDir(viteConfig),
    deploymentConfig.assetsDir,
  );
  if (!assetsDir) {
    return undefined;
  }
  return {
    dir: assetsDir,
    include: arrayOfStrings(routes.include),
    exclude: arrayOfStrings(routes.exclude),
  };
}

export async function staticRoutingCandidatePaths(source, viteConfig) {
  const outDir = resolveViteOutDir(viteConfig);
  const paths = [];
  const sourceAssetsDir = nonEmptyString(source.config.assets_dir);
  if (sourceAssetsDir) {
    paths.push(join(source.dir, sourceAssetsDir, DEFAULT_STATIC_ROUTES_FILE));
  }
  const generatedConfigPath = join(outDir, DEFAULT_DEPLOYMENT_CONFIG_FILE);
  try {
    const generatedConfig = JSON.parse(await readFile(generatedConfigPath, "utf8"));
    const generatedAssetsDir = nonEmptyString(generatedConfig?.assets_dir);
    if (generatedAssetsDir) {
      paths.push(join(outDir, generatedAssetsDir, DEFAULT_STATIC_ROUTES_FILE));
    }
  } catch (error) {
    if (!isMissingFileError(error)) {
      throw error;
    }
  }
  paths.push(join(outDir, DEFAULT_STATIC_ROUTES_FILE));
  return [...new Set(paths)];
}

export function matchesAnyStaticRoute(patterns, pathname) {
  return patterns.some((pattern) => matchesStaticRoute(pattern, pathname));
}

export function matchesStaticRoute(pattern, pathname) {
  const normalized = String(pattern).startsWith("/") ? String(pattern) : `/${pattern}`;
  const regex = new RegExp(`^${normalized.split("*").map(escapeRegex).join(".*")}$`);
  return regex.test(pathname);
}

export function escapeRegex(value) {
  return value.replace(/[|\\{}()[\]^$+?.]/g, "\\$&");
}

export function staticAssetContentType(file) {
  switch (extname(file)) {
    case ".css":
      return "text/css; charset=utf-8";
    case ".html":
      return "text/html; charset=utf-8";
    case ".js":
    case ".mjs":
      return "text/javascript; charset=utf-8";
    case ".json":
    case ".map":
      return "application/json; charset=utf-8";
    case ".svg":
      return "image/svg+xml";
    default:
      return "application/octet-stream";
  }
}

export function normalizeFile(value) {
  if (value instanceof URL) {
    return fileURLToPath(value);
  }
  return resolve(String(value));
}

export function normalizeHotReloadMode(value) {
  if (value === undefined || value === true) {
    return "all";
  }
  if (value === false || value === "all" || value === "entry") {
    return value;
  }
  throw new Error("ddVitePlugin reloadOnHotUpdate must be false, true, 'all', or 'entry'");
}

export async function shouldInvalidateOnHotUpdate(context, mode, effectiveWorkerEntry, options) {
  if (mode === false) {
    return false;
  }
  if (mode === "all") {
    return hasReloadableWorkerSource(options, effectiveWorkerEntry);
  }
  const entry = await effectiveWorkerEntry();
  if (!entry) {
    return typeof options.source === "function";
  }
  return context.file === normalizeFile(entry);
}

export async function hasReloadableWorkerSource(options, effectiveWorkerEntry) {
  return Boolean((await effectiveWorkerEntry()) || typeof options.source === "function");
}

export function isRecoverableRuntimeClientError(error) {
  const message = String(error?.message ?? "");
  return (
    message.includes("stream was destroyed") ||
    message.includes("EPIPE") ||
    message.includes("ERR_STREAM") ||
    message.includes("dd runtime exited")
  );
}
