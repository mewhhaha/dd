import { request as httpRequest, STATUS_CODES } from "node:http";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
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

export function nodeRequestToWorkerRequest(req, originalUrl, mount, workerName, res) {
  const controller = new AbortController();
  req.once("aborted", () => controller.abort());
  res.once("close", () => controller.abort());
  if (req.aborted || res.destroyed) controller.abort();
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
    signal: controller.signal,
  };
  if (!BODYLESS_METHODS.has(init.method.toUpperCase())) {
    init.body = Readable.toWeb(req);
    init.duplex = "half";
  }
  return new Request(url, init);
}

export async function writeNodeResponse(res, response) {
  res.statusCode = response.status;
  response.headers.forEach((value, name) => {
    const lower = name.toLowerCase();
    if (lower === "content-length" || lower === "transfer-encoding" || lower === "set-cookie") {
      return;
    }
    res.setHeader(name, value);
  });
  const cookies = response.headers.getSetCookie();
  if (cookies.length) res.setHeader("set-cookie", cookies);
  if (!response.body) {
    res.setHeader("content-length", "0");
    res.end();
    return;
  }
  await pipeline(Readable.fromWeb(response.body), res);
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
    const path = stripMount(originalUrl, options.mount);
    const url = nodeRequestWorkerUrl(req, path, workerName);
    const target = new URL(runtime.workerUrl(workerName));
    target.pathname = url.pathname;
    target.search = url.search;
    await new Promise((resolveUpgrade, rejectUpgrade) => {
      const upstream = httpRequest(target, {
        method: "GET",
        headers: { ...req.headers, "x-dd-dev-request-url": String(url) },
      });
      socket.once("close", () => {
        upstream.destroy();
        rejectUpgrade(new Error("WebSocket client disconnected during upgrade"));
      });
      upstream.once("error", rejectUpgrade);
      upstream.once("upgrade", (response, upstreamSocket, upstreamHead) => {
        socket.write("HTTP/1.1 101 Switching Protocols\r\n");
        for (let index = 0; index < response.rawHeaders.length; index += 2) {
          socket.write(`${response.rawHeaders[index]}: ${response.rawHeaders[index + 1]}\r\n`);
        }
        socket.write("\r\n");
        if (upstreamHead.length) socket.write(upstreamHead);
        if (head.length) upstreamSocket.write(head);
        socket.once("error", () => upstreamSocket.destroy());
        upstreamSocket.once("error", () => socket.destroy());
        socket.once("close", () => upstreamSocket.destroy());
        upstreamSocket.once("close", () => socket.destroy());
        socket.pipe(upstreamSocket).pipe(socket);
        socket.resume();
        resolveUpgrade();
      });
      upstream.once("response", (response) => {
        socket.write(`HTTP/1.1 ${response.statusCode} ${response.statusMessage}\r\nConnection: close\r\n`);
        for (let index = 0; index < response.rawHeaders.length; index += 2) {
          const name = response.rawHeaders[index];
          if (name.toLowerCase() === "transfer-encoding" || name.toLowerCase() === "connection") continue;
          socket.write(`${name}: ${response.rawHeaders[index + 1]}\r\n`);
        }
        socket.write("\r\n");
        pipeline(response, socket).then(resolveUpgrade, rejectUpgrade);
      });
      upstream.end();
    });
  } catch (error) {
    if (!socket.destroyed) {
      const body = Buffer.from(String(error?.message ?? error));
      socket.end(`HTTP/1.1 500 ${STATUS_CODES[500]}\r\nConnection: close\r\nContent-Length: ${body.length}\r\n\r\n${body}`);
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
