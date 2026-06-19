import { createFetchableDevEnvironment, mergeConfig } from "vite";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, isAbsolute, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { bundleWorkerEntry, createDdRuntime } from "./runtime.js";
import { createWorkerTestRuntime } from "./vitest.js";

const DEFAULT_MOUNT = "/__dd";
const DEFAULT_SOURCE_CONFIG_FILE = "dd.json";
const DEFAULT_DEPLOYMENT_CONFIG_FILE = "dd.deploy.json";
const DEFAULT_DEPLOYMENT_WORKER_FILE = "worker.js";
const DEFAULT_DEPLOYMENT_ASSETS_DIR = ".";
const DEFAULT_WORKER_NAME = "dev-worker";

export function ddEnvironment(options = {}) {
  const environmentOptions = options.viteEnvironment?.options ?? options.environmentOptions ?? {};
  return mergeConfig(
    {
      consumer: "server",
      dev: {
        createEnvironment(name, config) {
          let runtimePromise;
          const getRuntime = () => {
            runtimePromise ??= createWorkerTestRuntime({
              ...options,
              name: options.name ?? name,
            });
            return runtimePromise;
          };
          return createFetchableDevEnvironment(name, config, {
            async handleRequest(request) {
              const runtime = await getRuntime();
              return runtime.fetch(request);
            },
          });
        },
      },
    },
    environmentOptions,
  );
}

export function ddVitePlugin(options = {}) {
  const environmentName = options.viteEnvironment?.name ?? options.environmentName ?? "dd";
  const mount = normalizeMount(options.mount ?? DEFAULT_MOUNT);
  let runtime;
  let deployment;
  let deploying;
  const hotReloadMode = normalizeHotReloadMode(options.reloadOnHotUpdate);
  const deploymentConfig = normalizeDeploymentConfigOptions(options.deploymentConfig);
  let deploymentConfigWritten = false;
  let resolvedConfig;
  let rootHint;
  let sourceConfigRoot;
  let sourceConfigPromise;

  async function sourceConfig() {
    const root = resolve(resolvedConfig?.root ?? rootHint ?? process.cwd());
    if (!sourceConfigPromise || sourceConfigRoot !== root) {
      sourceConfigRoot = root;
      sourceConfigPromise = loadSourceDeploymentConfig(deploymentConfig, root);
    }
    return sourceConfigPromise;
  }

  async function effectiveWorkerName() {
    const source = await sourceConfig();
    return options.name ?? nonEmptyString(source.config.name) ?? DEFAULT_WORKER_NAME;
  }

  async function effectiveWorkerEntry() {
    if (options.entry) {
      return options.entry;
    }
    const source = await sourceConfig();
    const entrypoint = nonEmptyString(source.config.entrypoint);
    return entrypoint ? resolve(source.dir, entrypoint) : undefined;
  }

  async function effectiveRuntimeConfig() {
    const source = await sourceConfig();
    return (
      options.config ??
      source.config.config ??
      topLevelRuntimeConfig(source.config) ??
      { public: true }
    );
  }

  async function workerSource() {
    if (typeof options.source === "function") {
      return options.source();
    }
    if (typeof options.source === "string") {
      return options.source;
    }
    const entry = await effectiveWorkerEntry();
    if (!entry) {
      throw new Error("ddVitePlugin requires either source or entry");
    }
    return bundleWorkerEntry(entry, {
      viteConfig: options.viteConfig,
      target: options.target,
      sourcemap: options.sourcemap,
      minify: options.minify,
      logLevel: options.logLevel,
    });
  }

  async function ensureDeployed() {
    runtime ??= createDdRuntime(options.runtimeOptions);
    if (deployment) {
      return deployment;
    }
    const workerName = await effectiveWorkerName();
    const deployConfig = await effectiveRuntimeConfig();
    deploying ??= runtime
      .deploy(workerName, await workerSource(), deployConfig)
      .catch(async (error) => {
        if (!isRecoverableRuntimeClientError(error)) {
          throw error;
        }
        await runtime?.close().catch(() => {});
        runtime = createDdRuntime(options.runtimeOptions);
        return runtime.deploy(workerName, await workerSource(), deployConfig);
      })
      .finally(() => {
        deploying = undefined;
      });
    deployment = await deploying;
    return deployment;
  }

  async function invalidateDeployment() {
    deployment = undefined;
    if (options.eager === true) {
      await ensureDeployed();
    }
  }

  return {
    name: "dd-vite",
    enforce: "post",
    async config(config) {
      if (options.environment === false) {
        return;
      }
      rootHint = resolve(config.root ?? process.cwd());
      const workerName = await effectiveWorkerName();
      return {
        environments: {
          [environmentName]: ddEnvironment({
            ...options,
            name: workerName,
          }),
        },
      };
    },
    configResolved(config) {
      resolvedConfig = config;
    },
    configureServer(viteServer) {
      viteServer.httpServer?.once("close", () => {
        void runtime?.close();
      });
      if (options.middleware === false) {
        return;
      }
      viteServer.middlewares.use(async (req, res, next) => {
        try {
          const originalUrl = req.url ?? "/";
          if (!matchesMount(originalUrl, mount)) {
            next();
            return;
          }
          const workerName = await effectiveWorkerName();
          await ensureDeployed();
          const request = await nodeRequestToWorkerRequest(req, originalUrl, mount, workerName);
          const response = await runtime.fetch(workerName, request);
          await writeNodeResponse(res, response);
        } catch (error) {
          next(error);
        }
      });
    },
    async handleHotUpdate(context) {
      const source = await sourceConfig();
      if (source.path === context.file) {
        sourceConfigPromise = undefined;
        deployment = undefined;
      }
      if (await shouldInvalidateOnHotUpdate(context, hotReloadMode, effectiveWorkerEntry, options)) {
        await invalidateDeployment();
      }
    },
    async writeBundle() {
      if (
        deploymentConfigWritten ||
        !deploymentConfig.enabled ||
        !(await hasReloadableWorkerSource(options, effectiveWorkerEntry))
      ) {
        return;
      }
      deploymentConfigWritten = true;
      const outDir = resolveViteOutDir(resolvedConfig);
      const workerFile = normalizeOutputPath(
        deploymentConfig.entrypoint ?? DEFAULT_DEPLOYMENT_WORKER_FILE,
        "deploymentConfig.entrypoint",
      );
      const configFile = normalizeOutputPath(
        deploymentConfig.output ?? DEFAULT_DEPLOYMENT_CONFIG_FILE,
        "deploymentConfig.output",
      );
      await writeOutputFile(outDir, workerFile, await workerSource());
      const workerName = await effectiveWorkerName();
      const source = await sourceConfig();
      const config = await buildGeneratedDeploymentConfig({
        base: source.config,
        options,
        workerName,
        workerFile,
        configFile,
        assetsDir: deploymentConfig.assetsDir,
        assetExcludes: deploymentConfig.assetExcludes,
      });
      await writeOutputFile(outDir, configFile, `${JSON.stringify(config, null, 2)}\n`);
    },
    async closeBundle() {
      await runtime?.close();
    },
  };
}

async function nodeRequestToWorkerRequest(req, originalUrl, mount, workerName) {
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
  const url = new URL(path, `http://${workerName}.dd.local`);
  const init = {
    method: req.method ?? "GET",
    headers,
  };
  if (body.length > 0 && req.method !== "GET" && req.method !== "HEAD") {
    init.body = body;
    init.duplex = "half";
  }
  return new Request(url, init);
}

async function writeNodeResponse(res, response) {
  res.statusCode = response.status;
  response.headers.forEach((value, name) => {
    res.setHeader(name, value);
  });
  res.end(Buffer.from(await response.arrayBuffer()));
}

async function readIncomingBody(req) {
  const chunks = [];
  for await (const chunk of req) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }
  return Buffer.concat(chunks);
}

function normalizeMount(value) {
  const mount = `/${String(value).replace(/^\/+|\/+$/g, "")}`;
  return mount === "/" ? "/" : mount;
}

function matchesMount(url, mount) {
  if (mount === "/") {
    return true;
  }
  return url === mount || url.startsWith(`${mount}/`) || url.startsWith(`${mount}?`);
}

function stripMount(url, mount) {
  if (mount === "/") {
    return url;
  }
  const stripped = url.slice(mount.length);
  return stripped.length === 0 ? "/" : stripped;
}

function normalizeFile(value) {
  if (value instanceof URL) {
    return fileURLToPath(value);
  }
  return resolve(String(value));
}

function normalizeHotReloadMode(value) {
  if (value === undefined || value === true) {
    return "all";
  }
  if (value === false || value === "all" || value === "entry") {
    return value;
  }
  throw new Error("ddVitePlugin reloadOnHotUpdate must be false, true, 'all', or 'entry'");
}

async function shouldInvalidateOnHotUpdate(context, mode, effectiveWorkerEntry, options) {
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

async function hasReloadableWorkerSource(options, effectiveWorkerEntry) {
  return Boolean((await effectiveWorkerEntry()) || typeof options.source === "function");
}

function isRecoverableRuntimeClientError(error) {
  const message = String(error?.message ?? "");
  return (
    message.includes("stream was destroyed") ||
    message.includes("EPIPE") ||
    message.includes("ERR_STREAM") ||
    message.includes("dd runtime exited")
  );
}

function normalizeDeploymentConfigOptions(value) {
  if (value === false) {
    return { enabled: false };
  }
  return { enabled: true, ...(value ?? {}) };
}

function resolveViteOutDir(config) {
  const root = config?.root ?? process.cwd();
  const outDir = config?.build?.outDir ?? "dist";
  return isAbsolute(outDir) ? outDir : resolve(root, outDir);
}

async function writeOutputFile(outDir, file, contents) {
  const path = join(outDir, file);
  await mkdir(dirname(path), { recursive: true });
  await writeFile(path, contents);
}

async function buildGeneratedDeploymentConfig({
  base,
  options,
  workerName,
  workerFile,
  configFile,
  assetsDir,
  assetExcludes,
}) {
  const deployment = { ...base };
  const runtimeConfig =
    options.config ?? deployment.config ?? topLevelRuntimeConfig(deployment) ?? { public: true };

  delete deployment.public;
  delete deployment.bindings;
  delete deployment.internal;

  deployment.name = options.name ?? deployment.name ?? workerName;
  deployment.entrypoint = workerFile;
  if (assetsDir === false) {
    delete deployment.assets_dir;
  } else {
    deployment.assets_dir = normalizeConfigRelativePath(
      assetsDir ?? DEFAULT_DEPLOYMENT_ASSETS_DIR,
      "deploymentConfig.assetsDir",
    );
  }
  deployment.asset_excludes = uniquePaths([
    ...arrayOfStrings(deployment.asset_excludes),
    ...arrayOfStrings(assetExcludes),
    workerFile,
    configFile,
  ]);
  deployment.config = runtimeConfig;
  return deployment;
}

async function loadSourceDeploymentConfig(deploymentConfig, root) {
  const explicitInput = deploymentConfig.enabled ? deploymentConfig.input : undefined;
  if (explicitInput !== undefined) {
    return loadDeploymentConfigInput(explicitInput, root);
  }
  const packageRoot = await findPackageRoot(root);
  const path = join(packageRoot, DEFAULT_SOURCE_CONFIG_FILE);
  try {
    return {
      config: JSON.parse(await readFile(path, "utf8")),
      dir: packageRoot,
      path,
    };
  } catch (error) {
    if (!isMissingFileError(error)) {
      throw error;
    }
    return {
      config: {},
      dir: packageRoot,
      path: undefined,
    };
  }
}

async function loadDeploymentConfigInput(input, root) {
  if (input == null) {
    return { config: {}, dir: root, path: undefined };
  }
  if (typeof input === "function") {
    return { config: cloneJson(await input()), dir: root, path: undefined };
  }
  if (typeof input === "string" || input instanceof URL) {
    const path = normalizeFile(input);
    return {
      config: JSON.parse(await readFile(path, "utf8")),
      dir: dirname(path),
      path,
    };
  }
  return { config: cloneJson(input), dir: root, path: undefined };
}

async function findPackageRoot(start) {
  let current = resolve(start);
  for (;;) {
    try {
      await readFile(join(current, "package.json"), "utf8");
      return current;
    } catch (error) {
      if (!isMissingFileError(error)) {
        throw error;
      }
    }
    const parent = dirname(current);
    if (parent === current) {
      return resolve(start);
    }
    current = parent;
  }
}

function isMissingFileError(error) {
  return error?.code === "ENOENT" || error?.code === "ENOTDIR";
}

function topLevelRuntimeConfig(config) {
  const runtimeConfig = {};
  let found = false;
  for (const key of ["public", "bindings", "internal"]) {
    if (Object.hasOwn(config, key)) {
      runtimeConfig[key] = config[key];
      found = true;
    }
  }
  return found ? runtimeConfig : undefined;
}

function arrayOfStrings(value) {
  if (!Array.isArray(value)) {
    return [];
  }
  return value.map((entry) => String(entry));
}

function uniquePaths(paths) {
  return [...new Set(paths.map((path) => normalizeConfigRelativePath(path, "asset_excludes")))];
}

function normalizeOutputPath(value, label) {
  const normalized = normalizeConfigRelativePath(value, label);
  if (normalized === ".") {
    throw new Error(`${label} must be a file path`);
  }
  return normalized;
}

function normalizeConfigRelativePath(value, label) {
  const normalized = String(value).replaceAll("\\", "/").replace(/^\/+/, "");
  const parts = normalized.split("/");
  if (
    normalized.length === 0 ||
    parts.some((part) => part.length === 0 || part === "..") ||
    isAbsolute(String(value))
  ) {
    throw new Error(`${label} must be a relative path inside the Vite output directory`);
  }
  return normalized;
}

function cloneJson(value) {
  return value == null ? {} : JSON.parse(JSON.stringify(value));
}

function nonEmptyString(value) {
  if (typeof value !== "string") {
    return undefined;
  }
  const trimmed = value.trim();
  return trimmed.length === 0 ? undefined : trimmed;
}
