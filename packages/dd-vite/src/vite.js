import {
  createFetchableDevEnvironment,
  createRunnableDevEnvironment,
  fetchModule,
  mergeConfig,
} from "vite";
import { spawn } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { mkdir, readFile, readdir, stat, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { dirname, extname, isAbsolute, join, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
import { bundleWorkerEntry, createDdRuntime } from "./runtime.js";
import { createWorkerTestRuntime } from "./vitest.js";

const DEFAULT_MOUNT = "/";
const DEFAULT_SOURCE_CONFIG_FILE = "dd.json";
const DEFAULT_DEPLOYMENT_CONFIG_FILE = "dd.deploy.json";
const DEFAULT_DEPLOYMENT_WORKER_FILE = "worker.js";
const DEFAULT_DEPLOYMENT_ASSETS_DIR = ".";
const DEFAULT_ASSET_HEADERS_FILE = "_headers";
const DEFAULT_STATIC_ROUTES_FILE = "_routes.json";
const DEFAULT_WORKER_NAME = "dev-worker";
const DD_VITE_FRAMEWORK_BUILD_ONLY_ENV = "DD_VITE_FRAMEWORK_BUILD_ONLY";
const DD_VITE_WORKER_BUNDLE_ENV = "DD_VITE_WORKER_BUNDLE";
const REACT_ROUTER_FRAMEWORK = "react-router";
const REACT_ROUTER_RSC_FRAMEWORK = "react-router-rsc";
const DEFAULT_REACT_ROUTER_WORKER_ENTRY = "src/worker.ts";
const DEFAULT_REACT_ROUTER_BUILD_DIRECTORY = "dist/react-router";
const DEFAULT_REACT_ROUTER_RSC_BUILD_DIRECTORY = "dist/react-router-rsc";
const DEFAULT_REACT_ROUTER_RSC_ENTRY = "app/entry.rsc.ts";
const DD_REACT_ROUTER_RSC_SERVER_MODULE = "virtual:dd-react-router-rsc-server";
const DD_REACT_ROUTER_RSC_SERVER_RESOLVED = "\0dd:react-router-rsc-server";
const DD_NODE_ASYNC_HOOKS_SHIM_MODULE = "virtual:dd-node-async-hooks";
const DD_NODE_ASYNC_HOOKS_SHIM_RESOLVED = "\0dd:node-async-hooks";
const FINGERPRINTED_ASSET_CACHE_CONTROL = "public, max-age=31536000, immutable";
const DD_VITE_BYPASS_HEADER = "x-dd-vite-bypass";
const DD_VITE_MODULE_RUNNER_UPDATE_HEADER = "x-dd-vite-module-runner-update";
const DD_VITE_MODULE_RUNNER_UPDATE_PATH = "/__dd_vite_module_runner_update";
const BODYLESS_METHODS = new Set(["GET", "HEAD"]);
const VITE_BYPASS_PREFIXES = [
  "/@vite",
  "/@id/",
  "/@fs/",
  "/@react-refresh",
  "/__manifest",
  "/__vitest__/",
  "/__vitest_attachment__",
  "/__vitest_browser__/",
  "/__vitest_browser_api__",
  "/__vitest_test__/",
  "/node_modules/",
  "/.vite/",
  "/app/",
  "/src/",
];
const VITE_ANY_METHOD_BYPASS_PATHS = [
  "/__vite_rsc_findSourceMapURL",
  "/__vite_rsc_load_module_dev_proxy",
];
const VITE_BYPASS_QUERY_KEYS = new Set([
  "direct",
  "import",
  "inline",
  "raw",
  "sharedworker",
  "url",
  "used",
  "worker",
]);
const VITE_BYPASS_FETCH_DESTINATIONS = new Set(["script", "style"]);
const WEBSOCKET_ACCEPT_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
const MAX_DEV_WEBSOCKET_FRAME_BYTES = 8 * 1024 * 1024;
const DD_WS_BINARY_HEADER = "x-dd-ws-binary";
const DD_WS_CLOSE_CODE_HEADER = "x-dd-ws-close-code";
const DD_WS_CLOSE_REASON_HEADER = "x-dd-ws-close-reason";
const DD_NODE_ASYNC_HOOKS_SHIM_SOURCE = `
export class AsyncLocalStorage {
  #store;

  run(store, callback, ...args) {
    const previous = this.#store;
    this.#store = store;
    let result;
    try {
      result = callback(...args);
    } catch (error) {
      this.#store = previous;
      throw error;
    }
    if (result && typeof result.then === "function") {
      return Promise.resolve(result).finally(() => {
        this.#store = previous;
      });
    }
    this.#store = previous;
    return result;
  }

  getStore() {
    return this.#store;
  }

  enterWith(store) {
    this.#store = store;
  }

  disable() {
    this.#store = undefined;
  }
}
`;

export function ddEnvironment(options = {}) {
  const environmentOptions = ddEnvironmentUserOptions(options);
  return mergeConfig(
    {
      ...ddEnvironmentBaseOptions(options),
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

function ddRunnableEnvironment(options = {}) {
  const environmentOptions = ddEnvironmentUserOptions(options);
  return mergeConfig(
    {
      ...ddEnvironmentBaseOptions(options),
      dev: {
        createEnvironment(name, config) {
          return createRunnableDevEnvironment(name, config);
        },
      },
    },
    environmentOptions,
  );
}

function ddEnvironmentUserOptions(options) {
  return options.viteEnvironment?.options ?? options.environmentOptions ?? {};
}

function ddEnvironmentBaseOptions(options) {
  const optimizeDepsEntries = arrayOfStrings(options.optimizeDepsEntries);
  return {
    consumer: "server",
    resolve: {
      noExternal: true,
    },
    optimizeDeps: {
      noDiscovery: false,
      ignoreOutdatedRequests: true,
      ...(optimizeDepsEntries.length > 0 ? { entries: optimizeDepsEntries } : {}),
    },
  };
}

export function ddVitePlugin(options = {}) {
  if (
    process.env[DD_VITE_WORKER_BUNDLE_ENV] === "1" ||
    process.env[DD_VITE_FRAMEWORK_BUILD_ONLY_ENV] === "1"
  ) {
    return {
      name: "dd-vite",
    };
  }

  let viteCommand = "serve";
  let resolvedConfig;
  let devServer;
  let rootHint;
  const framework = normalizeFrameworkOptions(options.framework);
  const viteEnvironment = mergeFrameworkViteEnvironment(framework, options.viteEnvironment);
  const environmentName = viteEnvironment?.name ?? options.environmentName ?? "dd";
  const mount = normalizeMount(options.mount ?? DEFAULT_MOUNT);
  const childEnvironmentNames = arrayOfStrings(viteEnvironment?.childEnvironments);
  const moduleRunnerUpdateToken = randomUUID();
  let runtime;
  let deployment;
  let deploymentRuntimeGeneration;
  let deploying;
  const hotReloadMode = normalizeHotReloadMode(options.reloadOnHotUpdate);
  const deploymentConfig = normalizeDeploymentConfigOptions(
    mergeFrameworkDeploymentConfig(framework, options.deploymentConfig),
  );
  let deploymentConfigWritten = false;
  let frameworkDevBuildComplete = false;
  let frameworkDevBuildPromise;
  let reactRouterRscClientUpdateTimer;
  let reactRouterRscClientUpdateFile;
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
    if (entrypoint) {
      return resolve(source.dir, entrypoint);
    }
    return framework ? resolveFrameworkPath(source.dir, framework.workerEntry) : undefined;
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
    if (framework) {
      if (viteCommand === "serve" && devServer && options.devModuleRunner !== false) {
        return viteModuleRunnerWorkerSource(devServer, entry, moduleRunnerOptions());
      }
      return bundleFrameworkWorkerSource(framework, entry);
    }
    if (viteCommand === "serve" && devServer && options.devModuleRunner !== false) {
      return viteModuleRunnerWorkerSource(devServer, entry, moduleRunnerOptions());
    }
    return bundleWorkerEntry(entry, {
      viteConfig: workerBundleViteConfig(),
      target: options.target,
      sourcemap: options.sourcemap,
      minify: options.minify,
      logLevel: options.logLevel,
    });
  }

  function workerBundleViteConfig() {
    const projectConfig = {};
    if (resolvedConfig?.root) {
      projectConfig.root = resolvedConfig.root;
    }
    if (resolvedConfig?.configFile && options.viteConfig?.configFile !== false) {
      projectConfig.configFile = resolvedConfig.configFile;
    }
    return mergeConfig(projectConfig, options.viteConfig ?? {});
  }

  async function bundledWorkerSource() {
    const previous = process.env[DD_VITE_WORKER_BUNDLE_ENV];
    process.env[DD_VITE_WORKER_BUNDLE_ENV] = "1";
    try {
      return await workerSource();
    } finally {
      if (previous === undefined) {
        delete process.env[DD_VITE_WORKER_BUNDLE_ENV];
      } else {
        process.env[DD_VITE_WORKER_BUNDLE_ENV] = previous;
      }
    }
  }

  async function ensureDeployed() {
    runtime ??= createDdRuntime(options.runtimeOptions);
    if (deployment && deploymentRuntimeGeneration === runtime.generation) {
      return deployment;
    }
    deployment = undefined;
    deploymentRuntimeGeneration = undefined;
    const workerName = await effectiveWorkerName();
    const deployConfig = await effectiveRuntimeConfig();
    deploying ??= runtime
      .deploy(workerName, await bundledWorkerSource(), deployConfig)
      .catch(async (error) => {
        if (!isRecoverableRuntimeClientError(error)) {
          throw error;
        }
        await runtime?.close().catch(() => {});
        runtime = createDdRuntime(options.runtimeOptions);
        return runtime.deploy(workerName, await bundledWorkerSource(), deployConfig);
      })
      .finally(() => {
        deploying = undefined;
      });
    deployment = await deploying;
    deploymentRuntimeGeneration = runtime.generation;
    return deployment;
  }

  async function invalidateDeployment() {
    frameworkDevBuildComplete = false;
    if (await updateViteModuleRunnerDeployment()) {
      return;
    }
    deployment = undefined;
    deploymentRuntimeGeneration = undefined;
    if (options.eager === true) {
      await ensureDeployed();
    }
  }

  function moduleRunnerOptions() {
    return {
      environmentName,
      childEnvironmentNames,
      root: resolveFrameworkRoot(),
      updateToken: moduleRunnerUpdateToken,
    };
  }

  async function updateViteModuleRunnerDeployment() {
    if (
      viteCommand !== "serve" ||
      !devServer ||
      options.devModuleRunner === false ||
      !deployment ||
      !runtime ||
      deploymentRuntimeGeneration !== runtime.generation
    ) {
      deployment = undefined;
      deploymentRuntimeGeneration = undefined;
      return false;
    }
    const entry = await effectiveWorkerEntry();
    if (!entry || typeof options.source === "string" || typeof options.source === "function") {
      return false;
    }
    const workerName = await effectiveWorkerName();
    const graph = await collectViteModuleRunnerGraph(devServer, entry, moduleRunnerOptions());
    const response = await runtime.fetch(
      workerName,
      new Request(`http://${workerName}.dd.local${DD_VITE_MODULE_RUNNER_UPDATE_PATH}`, {
        method: "POST",
        headers: {
          [DD_VITE_MODULE_RUNNER_UPDATE_HEADER]: moduleRunnerUpdateToken,
          "content-type": "application/json",
        },
        body: JSON.stringify(graph),
      }),
    );
    if (!response.ok) {
      deployment = undefined;
      deploymentRuntimeGeneration = undefined;
      throw new Error(
        `ddVitePlugin failed to update dev module graph: ${response.status} ${await response.text()}`,
      );
    }
    return true;
  }

  return {
    name: "dd-vite",
    enforce: "pre",
    async config(config, env) {
      viteCommand = env.command;
      if (options.environment === false) {
        return;
      }
      rootHint = resolve(config.root ?? process.cwd());
      const workerName = await effectiveWorkerName();
      const workerEntry = await effectiveWorkerEntry();
      const optimizeDepsEntries = workerEntry
        ? [viteRequestForFile(workerEntry, rootHint)]
        : [];
      const frameworkResolveAlias = await frameworkDevResolveAlias(framework, rootHint);
      return {
        ...(framework?.name === REACT_ROUTER_RSC_FRAMEWORK
          ? {
              rsc: {
                loadModuleDevProxy: true,
              },
            }
          : {}),
        ...(frameworkResolveAlias
          ? {
              resolve: {
                alias: frameworkResolveAlias,
              },
            }
          : {}),
        environments: {
          [environmentName]: ddEnvironment({
            ...options,
            viteEnvironment,
            name: workerName,
            optimizeDepsEntries,
          }),
          ...Object.fromEntries(
            childEnvironmentNames.map((name) => [
              name,
              ddRunnableEnvironment({
                ...options,
                viteEnvironment: { ...(viteEnvironment ?? {}), name },
                name: workerName,
              }),
            ]),
          ),
        },
      };
    },
    configResolved(config) {
      resolvedConfig = config;
    },
    configureServer(viteServer) {
      devServer = viteServer;
      viteServer.httpServer?.once("close", () => {
        if (reactRouterRscClientUpdateTimer) {
          clearTimeout(reactRouterRscClientUpdateTimer);
          reactRouterRscClientUpdateTimer = undefined;
          reactRouterRscClientUpdateFile = undefined;
        }
        void runtime?.close();
      });
      if (options.middleware === false) {
        return;
      }
      const upgradeHandler = (req, socket, head) => {
        void handleDdWebSocketUpgrade(req, socket, head, {
          ensureDeployed,
          effectiveWorkerName,
          runtime: () => runtime,
          mount,
          viteBase: resolvedConfig?.base,
        });
      };
      viteServer.httpServer?.on("upgrade", upgradeHandler);
      viteServer.httpServer?.once("close", () => {
        viteServer.httpServer?.off("upgrade", upgradeHandler);
      });
      viteServer.middlewares.use(async (req, res, next) => {
        try {
          const originalUrl = req.url ?? "/";
          if (shouldBypassViteRequest(req, originalUrl, mount, resolvedConfig?.base)) {
            next();
            return;
          }
          if (!matchesMount(originalUrl, mount)) {
            next();
            return;
          }
          await ensureDeployed();
          const staticRouting = await loadStaticRouting(
            await sourceConfig(),
            resolvedConfig,
            deploymentConfig,
          );
          if (staticRouting && shouldBypassStaticRoutingRequest(req, originalUrl, mount, staticRouting)) {
            if (await writeStaticAssetResponse(req, res, originalUrl, mount, staticRouting)) {
              return;
            }
            next();
            return;
          }
          const workerName = await effectiveWorkerName();
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
        deploymentRuntimeGeneration = undefined;
      }
      if (await shouldInvalidateOnHotUpdate(context, hotReloadMode, effectiveWorkerEntry, options)) {
        await invalidateDeployment();
        scheduleReactRouterRscClientUpdate(context);
      }
    },
    renderStart: {
      order: "post",
      async handler() {
        if (hasFrameworkBuildEnvironments(resolvedConfig)) {
          return;
        }
        await writeDeploymentArtifacts();
      },
    },
    async generateBundle() {
      if (hasFrameworkBuildEnvironments(resolvedConfig)) {
        return;
      }
      await writeDeploymentArtifacts();
    },
    async writeBundle() {
      if (hasFrameworkBuildEnvironments(resolvedConfig)) {
        return;
      }
      await writeDeploymentArtifacts();
    },
    buildApp: {
      order: "post",
      async handler() {
        await writeDeploymentArtifacts();
      },
    },
    async closeBundle() {
      if (!hasFrameworkBuildEnvironments(resolvedConfig)) {
        await writeDeploymentArtifacts();
      }
      await runtime?.close();
    },
    resolveId(id) {
      if (id === DD_NODE_ASYNC_HOOKS_SHIM_MODULE) {
        return DD_NODE_ASYNC_HOOKS_SHIM_RESOLVED;
      }
      if (framework?.name === REACT_ROUTER_RSC_FRAMEWORK && id === DD_REACT_ROUTER_RSC_SERVER_MODULE) {
        return DD_REACT_ROUTER_RSC_SERVER_RESOLVED;
      }
    },
    load(id) {
      if (id === DD_NODE_ASYNC_HOOKS_SHIM_RESOLVED) {
        return DD_NODE_ASYNC_HOOKS_SHIM_SOURCE;
      }
      if (id !== DD_REACT_ROUTER_RSC_SERVER_RESOLVED || framework?.name !== REACT_ROUTER_RSC_FRAMEWORK) {
        return;
      }
      const root = resolveFrameworkRoot();
      const entry = resolveFrameworkPath(root, framework.rscEntry);
      const specifier = viteRequestForFile(entry, root);
      return `export { default } from ${JSON.stringify(specifier)};\n`;
    },
  };

  async function writeDeploymentArtifacts() {
    const outDir = resolveViteOutDir(resolvedConfig);
    const workerFile = normalizeOutputPath(
      deploymentConfig.entrypoint ?? DEFAULT_DEPLOYMENT_WORKER_FILE,
      "deploymentConfig.entrypoint",
    );
    const configFile = normalizeOutputPath(
      deploymentConfig.output ?? DEFAULT_DEPLOYMENT_CONFIG_FILE,
      "deploymentConfig.output",
    );
    if (!deploymentConfig.enabled || !(await hasReloadableWorkerSource(options, effectiveWorkerEntry))) {
      return;
    }
    const artifactsExist =
      deploymentConfigWritten &&
      (await outputFileExists(outDir, workerFile)) &&
      (await outputFileExists(outDir, configFile));
    if (!artifactsExist) {
      await writeOutputFile(outDir, workerFile, await bundledWorkerSource());
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
        staticRoutes: deploymentConfig.staticRoutes,
      });
      await writeOutputFile(outDir, configFile, `${JSON.stringify(config, null, 2)}\n`);
      deploymentConfigWritten = true;
    }
    await writeGeneratedStaticRoutes(outDir, deploymentConfig);
    await writeGeneratedAssetPolicy(outDir, resolvedConfig, deploymentConfig.assetsDir);
  }

  async function bundleFrameworkWorkerSource(framework, entry) {
    if (viteCommand === "serve") {
      await ensureFrameworkDevBuild(framework);
    }
    return bundleWorkerEntry(entry, {
      viteConfig: mergeConfig(
        await frameworkWorkerViteConfig(framework),
        options.viteConfig ?? {},
      ),
      target: options.target,
      sourcemap: options.sourcemap,
      minify: options.minify,
      logLevel: options.logLevel,
    });
  }

  async function ensureFrameworkDevBuild(framework) {
    if (frameworkDevBuildComplete) {
      return;
    }
    frameworkDevBuildPromise ??= buildFramework(framework, resolveFrameworkRoot())
      .then(() => {
        frameworkDevBuildComplete = true;
      })
      .finally(() => {
        frameworkDevBuildPromise = undefined;
      });
    await frameworkDevBuildPromise;
  }

  async function frameworkWorkerViteConfig(framework) {
    const root = resolveFrameworkRoot();
    const alias = {
      [framework.serverBuildModule]: frameworkServerEntry(framework, root),
    };
    if (framework.asyncHooksShim !== false) {
      const asyncHooksShim = await resolveAsyncHooksShim(framework, root);
      if (asyncHooksShim) {
        alias["node:async_hooks"] = asyncHooksShim;
        alias.async_hooks = asyncHooksShim;
      }
    }
    return {
      define: {
        "process.env.NODE_ENV": JSON.stringify("production"),
      },
      resolve: {
        alias,
      },
      plugins: [
        ddNodeAsyncHooksShimPlugin(),
      ],
    };
  }

  function resolveFrameworkRoot() {
    return resolve(resolvedConfig?.root ?? rootHint ?? process.cwd());
  }

  function scheduleReactRouterRscClientUpdate(context) {
    if (framework?.name !== REACT_ROUTER_RSC_FRAMEWORK) {
      return;
    }
    reactRouterRscClientUpdateFile = context.file;
    if (reactRouterRscClientUpdateTimer) {
      clearTimeout(reactRouterRscClientUpdateTimer);
    }
    reactRouterRscClientUpdateTimer = setTimeout(() => {
      const file = reactRouterRscClientUpdateFile;
      reactRouterRscClientUpdateTimer = undefined;
      reactRouterRscClientUpdateFile = undefined;
      context.server?.environments?.client?.hot?.send?.({
        type: "custom",
        event: "rsc:update",
        data: file ? { file } : {},
      });
    }, 25);
  }
}

async function frameworkDevResolveAlias(framework, root) {
  if (!framework || framework.asyncHooksShim === false) {
    return undefined;
  }
  const asyncHooksShim = await resolveAsyncHooksShim(framework, root);
  if (!asyncHooksShim) {
    return undefined;
  }
  return {
    "node:async_hooks": asyncHooksShim,
    async_hooks: asyncHooksShim,
  };
}

function normalizeFrameworkOptions(value) {
  if (value == null || value === false) {
    return undefined;
  }
  const options = typeof value === "string" ? { name: value } : value;
  const name = String(options.name ?? "");
  switch (name) {
    case REACT_ROUTER_FRAMEWORK:
      return {
        name,
        buildDirectory: options.buildDirectory ?? DEFAULT_REACT_ROUTER_BUILD_DIRECTORY,
        workerEntry: options.workerEntry ?? DEFAULT_REACT_ROUTER_WORKER_ENTRY,
        serverEntry: options.serverEntry,
        serverBuildModule: "virtual:react-router/server-build",
        asyncHooksShim: false,
        rscEntry: undefined,
      };
    case REACT_ROUTER_RSC_FRAMEWORK:
      return {
        name,
        buildDirectory: options.buildDirectory ?? DEFAULT_REACT_ROUTER_RSC_BUILD_DIRECTORY,
        workerEntry: options.workerEntry ?? DEFAULT_REACT_ROUTER_WORKER_ENTRY,
        serverEntry: options.serverEntry,
        serverBuildModule: DD_REACT_ROUTER_RSC_SERVER_MODULE,
        asyncHooksShim: options.asyncHooksShim ?? DD_NODE_ASYNC_HOOKS_SHIM_MODULE,
        rscEntry: options.rscEntry ?? DEFAULT_REACT_ROUTER_RSC_ENTRY,
      };
    default:
      throw new Error(
        `ddVitePlugin framework must be "${REACT_ROUTER_FRAMEWORK}" or "${REACT_ROUTER_RSC_FRAMEWORK}"`,
      );
  }
}

async function resolveAsyncHooksShim(framework, root) {
  if (!framework || framework.asyncHooksShim === false) {
    return undefined;
  }
  if (framework.asyncHooksShim === DD_NODE_ASYNC_HOOKS_SHIM_MODULE) {
    return DD_NODE_ASYNC_HOOKS_SHIM_MODULE;
  }
  const asyncHooksShim = resolveFrameworkPath(root, framework.asyncHooksShim);
  if (!(await fileExists(asyncHooksShim))) {
    return undefined;
  }
  return asyncHooksShim;
}

function ddNodeAsyncHooksShimPlugin() {
  return {
    name: "dd-node-async-hooks-shim",
    resolveId(id) {
      if (id === DD_NODE_ASYNC_HOOKS_SHIM_MODULE) {
        return DD_NODE_ASYNC_HOOKS_SHIM_RESOLVED;
      }
    },
    load(id) {
      if (id === DD_NODE_ASYNC_HOOKS_SHIM_RESOLVED) {
        return DD_NODE_ASYNC_HOOKS_SHIM_SOURCE;
      }
    },
  };
}

function mergeFrameworkViteEnvironment(framework, viteEnvironment) {
  if (!framework) {
    return viteEnvironment;
  }
  const defaults =
    framework.name === REACT_ROUTER_RSC_FRAMEWORK
      ? { name: "rsc", childEnvironments: ["ssr"] }
      : { name: "ssr" };
  return {
    ...defaults,
    ...(viteEnvironment ?? {}),
    childEnvironments: uniqueStrings([
      ...arrayOfStrings(defaults.childEnvironments),
      ...arrayOfStrings(viteEnvironment?.childEnvironments),
    ]),
  };
}

function mergeFrameworkDeploymentConfig(framework, deploymentConfig) {
  if (!framework || deploymentConfig === false) {
    return deploymentConfig;
  }
  const defaults = {
    assetsDir: frameworkClientAssetsDir(framework),
    staticRoutes: {
      include: ["/*"],
      exclude:
        framework.name === REACT_ROUTER_RSC_FRAMEWORK
          ? ["/assets/*", "/__manifest"]
          : ["/assets/*"],
    },
  };
  if (!deploymentConfig) {
    return defaults;
  }
  return {
    ...defaults,
    ...deploymentConfig,
    staticRoutes:
      deploymentConfig.staticRoutes === undefined
        ? defaults.staticRoutes
        : deploymentConfig.staticRoutes,
  };
}

function frameworkClientAssetsDir(framework) {
  const buildDirectory = normalizeConfigRelativePath(
    framework.buildDirectory,
    "framework.buildDirectory",
  );
  const relativeBuildDirectory = buildDirectory === "dist"
    ? ""
    : buildDirectory.startsWith("dist/")
      ? buildDirectory.slice("dist/".length)
      : buildDirectory;
  return joinConfigRelativePath(relativeBuildDirectory, "client");
}

function frameworkServerEntry(framework, root) {
  return resolveFrameworkPath(
    root,
    framework.serverEntry ?? joinConfigRelativePath(framework.buildDirectory, "server/index.js"),
  );
}

async function buildFramework(framework, root) {
  await new Promise((resolveBuild, rejectBuild) => {
    const child = spawn(process.execPath, [viteBinPath(root), "build"], {
      cwd: root,
      env: {
        ...process.env,
        [DD_VITE_FRAMEWORK_BUILD_ONLY_ENV]: "1",
      },
      stdio: ["ignore", "pipe", "pipe"],
    });
    let output = "";
    child.stdout.on("data", (chunk) => {
      output += chunk;
    });
    child.stderr.on("data", (chunk) => {
      output += chunk;
    });
    child.on("error", rejectBuild);
    child.on("close", (code) => {
      if (code === 0) {
        resolveBuild();
        return;
      }
      rejectBuild(
        new Error(
          `${frameworkLabel(framework)} build failed with exit code ${code}\n${output}`,
        ),
      );
    });
  });
}

function frameworkLabel(framework) {
  return framework.name === REACT_ROUTER_RSC_FRAMEWORK
    ? "React Router RSC"
    : "React Router";
}

function viteBinPath(root) {
  const require = createRequire(join(root, "package.json"));
  try {
    return require.resolve("vite/bin/vite.js");
  } catch {
    return resolve(root, "node_modules/vite/bin/vite.js");
  }
}

function resolveFrameworkPath(root, value) {
  if (value instanceof URL) {
    return fileURLToPath(value);
  }
  return isAbsolute(String(value)) ? String(value) : resolve(root, String(value));
}

function joinConfigRelativePath(...parts) {
  return parts.filter((part) => String(part).length > 0).join("/");
}

async function fileExists(path) {
  try {
    return (await stat(path)).isFile();
  } catch (error) {
    if (isMissingFileError(error)) {
      return false;
    }
    throw error;
  }
}

async function viteModuleRunnerWorkerSource(viteServer, entry, options) {
  const graph = await collectViteModuleRunnerGraph(viteServer, entry, options);
  return renderViteModuleRunnerWorkerSource(graph);
}

async function collectViteModuleRunnerGraph(viteServer, entry, options) {
  const root = resolve(options.root ?? viteServer.config.root);
  const entryRequest = viteRequestForFile(entry, root);
  const records = new Map();
  const aliases = new Map();
  const pending = new Map();
  patchViteResolvedUrls(viteServer);
  const environmentNames = uniqueStrings([
    options.environmentName,
    ...arrayOfStrings(options.childEnvironmentNames),
  ]);

  await Promise.all(
    environmentNames.map(async (name) => {
      await viteServer.environments[name]?.depsOptimizer?.init?.();
    }),
  );

  const visit = async (environmentName, id, importer) => {
    const environment = viteServer.environments[environmentName];
    if (!environment) {
      throw new Error(`ddVitePlugin could not find Vite environment "${environmentName}"`);
    }

    const requestKey = `${environmentName}\0${id}\0${importer ?? ""}`;
    const pendingRequest = pending.get(requestKey);
    if (pendingRequest) {
      return pendingRequest;
    }

    const promise = (async () => {
      let fetched;
      try {
        fetched = await fetchViteModule(environment, id, importer);
      } catch (error) {
        error.message = `ddVitePlugin failed to fetch ${JSON.stringify(id)} in Vite environment "${environmentName}"` +
          (importer ? ` imported from ${JSON.stringify(importer)}` : "") +
          `: ${error.message}`;
        throw error;
      }
      const record = createViteRunnerRecord(environmentName, id, fetched, environment, viteServer);
      addViteRunnerRecordAliases(record, aliases);

      const existing = records.get(record.key);
      if (existing) {
        return existing.key;
      }

      if (record.externalize && !isSupportedViteRunnerExternal(record.externalize)) {
        throw new Error(
          `ddVitePlugin cannot run external module ${record.externalize} in the dd runtime` +
            ` while loading ${JSON.stringify(record.id)}` +
            (importer ? ` imported from ${JSON.stringify(importer)}` : "") +
            ". " +
            "Make sure the dd Vite environment uses resolve.noExternal/optimizeDeps so dependencies are transformed.",
        );
      }

      records.set(record.key, record);

      if (record.code) {
        const importerId = record.file ?? record.id;
        await Promise.all([
          ...extractViteRunnerImports(record.code).map((specifier) =>
            visit(environmentName, specifier, importerId),
          ),
          ...extractViteEnvironmentImports(record.code).map((dependency) =>
            visit(dependency.environmentName, dependency.id, undefined),
          ),
        ]);
      }

      return record.key;
    })();

    pending.set(requestKey, promise);
    try {
      return await promise;
    } finally {
      pending.delete(requestKey);
    }
  };

  const entryKey = await visit(options.environmentName, entryRequest, undefined);
  return {
    entryEnvironment: options.environmentName,
    entryKey,
    records: [...records.values()],
    aliases: [...aliases.entries()],
    devFetchOrigins: viteDevServerOrigins(viteServer),
    updateToken: options.updateToken,
  };
}

async function fetchViteModule(environment, id, importer) {
  const options = { inlineSourceMap: false };
  if (typeof environment.fetchModule === "function") {
    return environment.fetchModule(id, importer, options);
  }
  return fetchModule(environment, id, importer, options);
}

function createViteRunnerRecord(environmentName, requestedId, fetched, environment, viteServer) {
  const id = fetched.id ?? fetched.externalize ?? requestedId;
  const key = viteRunnerKey(environmentName, id);
  return {
    key,
    environmentName,
    requestedId,
    id,
    url: fetched.url ?? requestedId,
    file: fetched.file,
    code: rewriteViteRunnerCode(fetched.code, viteServer, id),
    externalize: fetched.externalize,
    type: fetched.type,
    importMetaEnv: viteRunnerImportMetaEnv(environment),
  };
}

function rewriteViteRunnerCode(code, viteServer, id) {
  let rewritten = rewriteViteDevServerUrls(code, viteServer);
  if (String(id).includes("virtual:vite-rsc/css?")) {
    rewritten = rewriteViteRscCssVirtualModule(rewritten);
  }
  return rewritten;
}

function rewriteViteRscCssVirtualModule(code) {
  if (!code?.includes("virtual:vite-rsc/remove-duplicate-server-css")) {
    return code;
  }
  const importPattern =
    /const\s+(__vite_ssr_import_\d+__)\s*=\s*await\s+__vite_ssr_import__\("\/@id\/__x00__virtual:vite-rsc\/remove-duplicate-server-css"[^;]*;\n*/;
  const match = code.match(importPattern);
  if (!match) {
    return code;
  }
  return code
    .replace(importPattern, "")
    .split(`(0,${match[1]}.default)`)
    .join("undefined");
}

function addViteRunnerRecordAliases(record, aliases) {
  for (const value of [
    record.id,
    record.url,
    record.file,
    record.requestedId,
    cleanViteRunnerId(record.id),
    cleanViteRunnerId(record.url),
    cleanViteRunnerId(record.file),
    cleanViteRunnerId(record.requestedId),
  ]) {
    if (value) {
      aliases.set(viteRunnerKey(record.environmentName, value), record.key);
    }
  }
}

function viteRunnerImportMetaEnv(environment) {
  const config = environment.config;
  return {
    ...(config.env ?? {}),
    BASE_URL: config.base ?? "/",
    MODE: config.mode,
    DEV: !config.isProduction,
    PROD: Boolean(config.isProduction),
    SSR: true,
  };
}

function isSupportedViteRunnerExternal(value) {
  const external = String(value);
  return external.startsWith("data:") || external === "node:module";
}

function viteRunnerKey(environmentName, id) {
  return `${environmentName}\0${id}`;
}

function cleanViteRunnerId(value) {
  if (!value) {
    return undefined;
  }
  const index = String(value).search(/[?#]/);
  return index === -1 ? String(value) : String(value).slice(0, index);
}

function extractViteRunnerImports(code) {
  const imports = new Set();
  const pattern = /__vite_ssr_(?:dynamic_)?import__\(\s*("(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*')/g;
  for (const match of code.matchAll(pattern)) {
    const value = parseJsStringLiteral(match[1]);
    if (value && !isIgnorableViteRunnerImport(value)) {
      imports.add(value);
    }
  }
  const stringPattern = /"(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*'/g;
  for (const match of code.matchAll(stringPattern)) {
    const value = parseJsStringLiteral(match[0]);
    const importExpression = String(value).match(/^import\((["'])(.*)\1\)$/);
    if (importExpression && isLikelyViteModuleSpecifier(importExpression[2])) {
      imports.add(importExpression[2]);
      continue;
    }
    if (isLikelyViteModuleSpecifier(value)) {
      imports.add(value);
    }
  }
  return [...imports];
}

function isLikelyViteModuleSpecifier(value) {
  const specifier = String(value ?? "");
  if (isIgnorableViteRunnerImport(specifier)) {
    return false;
  }
  if (specifier.startsWith("import(")) {
    return false;
  }
  return (
    specifier.startsWith("/@id/") ||
    specifier.startsWith("/@fs/") ||
    specifier.startsWith("/node_modules/.vite/") ||
    specifier.startsWith("virtual:") ||
    specifier.includes("virtual:vite-rsc")
  );
}

function isIgnorableViteRunnerImport(value) {
  const specifier = String(value ?? "");
  return (
    specifier === "\0" ||
    specifier === "/@id/__x00__" ||
    (specifier.includes("virtual:vite-rsc/css?") && !specifier.includes("type=")) ||
    (specifier.includes("virtual:vite-rsc/reference-validation?") && !specifier.includes("type=")) ||
    specifier.trim() === ""
  );
}

function extractViteEnvironmentImports(code) {
  const imports = [];
  const pattern =
    /__VITE_ENVIRONMENT_RUNNER_IMPORT__\(\s*("(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*')\s*,\s*("(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*')/g;
  for (const match of code.matchAll(pattern)) {
    const environmentName = parseJsStringLiteral(match[1]);
    const id = parseJsStringLiteral(match[2]);
    if (environmentName && id) {
      imports.push({ environmentName, id });
    }
  }
  return imports;
}

function parseJsStringLiteral(value) {
  try {
    return JSON.parse(value);
  } catch {
    return value.slice(1, -1).replace(/\\(['"\\])/g, "$1");
  }
}

function renderViteModuleRunnerWorkerSource(graph) {
  const payload = {
    entryEnvironment: graph.entryEnvironment,
    entryKey: graph.entryKey,
    records: graph.records,
    aliases: graph.aliases,
    devFetchOrigins: graph.devFetchOrigins,
    updateToken: graph.updateToken,
  };
  return `${VITE_MODULE_RUNNER_WORKER_BOOTSTRAP}
const __ddViteGraph = ${JSON.stringify(payload)};
const __ddViteEntry = await __ddViteImportGraph(__ddViteGraph);
export default __ddViteEntry;
`;
}

const VITE_MODULE_RUNNER_WORKER_BOOTSTRAP = `
const __ddViteAsyncFunction = Object.getPrototypeOf(async function () {}).constructor;

async function __ddViteImportGraph(graph) {
  const records = new Map(graph.records.map((record) => [record.key, record]));
  const aliases = new Map(graph.aliases);
  const cache = new Map();
  const devFetchOrigins = new Set(graph.devFetchOrigins ?? []);
  const updateToken = String(graph.updateToken ?? "");
  const nodeModuleShim = Object.freeze({
    createRequire(url) {
      const require = (id) => {
        throw new Error("createRequire(" + url + ") cannot load " + id + " in the dd Vite dev runner");
      };
      require.resolve = require;
      return require;
    },
  });
  const emptyVirtualKey = "__dd_vite_empty_virtual_module__";
  const emptyCssVirtualKey = "__dd_vite_empty_css_virtual_module__";
  installInternalRecords(graph.records[0]?.importMetaEnv ?? {});
  const firstEnv = graph.records[0]?.importMetaEnv ?? {};
  const nodeEnv = firstEnv.PROD ? "production" : "development";
  globalThis.process ??= { env: {} };
  globalThis.process.env ??= {};
  globalThis.process.env.NODE_ENV ??= nodeEnv;
  if (typeof globalThis.TextEncoderStream === "undefined") {
    globalThis.TextEncoderStream = class TextEncoderStream {
      constructor() {
        const encoder = new TextEncoder();
        const transform = new TransformStream({
          transform(chunk, controller) {
            controller.enqueue(encoder.encode(String(chunk)));
          },
        });
        this.readable = transform.readable;
        this.writable = transform.writable;
      }
    };
  }
  if (typeof globalThis.TextDecoderStream === "undefined") {
    globalThis.TextDecoderStream = class TextDecoderStream {
      constructor(label = "utf-8", options = {}) {
        const decoder = new TextDecoder(label, options);
        const transform = new TransformStream({
          transform(chunk, controller) {
            controller.enqueue(decoder.decode(chunk, { stream: true }));
          },
          flush(controller) {
            const tail = decoder.decode();
            if (tail) controller.enqueue(tail);
          },
        });
        this.readable = transform.readable;
        this.writable = transform.writable;
      }
    };
  }
  function installDevFetchBridge() {
    const rawHostFetch = globalThis.__dd_raw_host_fetch ?? globalThis.__dd_deno_runtime?.fetch;
    if (devFetchOrigins.size === 0 || !rawHostFetch || globalThis.fetch?.__ddViteDevFetchBridge) {
      return;
    }
    const policyFetch = globalThis.fetch;
    const bridge = (input, init) => {
      try {
        const url = new URL(input instanceof Request ? input.url : String(input));
        if (devFetchOrigins.has(url.origin)) {
          return rawHostFetch(input, init);
        }
      } catch {
        // Fall through to the policy fetch.
      }
      return policyFetch(input, init);
    };
    Object.defineProperty(bridge, "__ddViteDevFetchBridge", { value: true });
    globalThis.fetch = bridge;
  }

  function installInternalRecords(importMetaEnv) {
    records.set(emptyVirtualKey, {
      key: emptyVirtualKey,
      environmentName: graph.entryEnvironment,
      id: emptyVirtualKey,
      url: emptyVirtualKey,
      code: "",
      importMetaEnv,
    });
    records.set(emptyCssVirtualKey, {
      key: emptyCssVirtualKey,
      environmentName: graph.entryEnvironment,
      id: emptyCssVirtualKey,
      url: emptyCssVirtualKey,
      code: "__vite_ssr_exportName__(\\"default\\", () => []);",
      importMetaEnv,
    });
  }

  function clean(id) {
    const value = String(id);
    const index = value.search(/[?#]/);
    return index === -1 ? value : value.slice(0, index);
  }

  function normalizePath(path) {
    const absolute = path.startsWith("/");
    const parts = path.split("/");
    const out = [];
    for (const part of parts) {
      if (!part || part === ".") continue;
      if (part === "..") out.pop();
      else out.push(part);
    }
    return (absolute ? "/" : "") + out.join("/");
  }

  function dirname(path) {
    const cleaned = clean(path);
    const index = cleaned.lastIndexOf("/");
    return index === -1 ? "" : cleaned.slice(0, index);
  }

  function resolveRelative(base, specifier) {
    if (!specifier.startsWith("./") && !specifier.startsWith("../")) {
      return specifier;
    }
    return normalizePath(dirname(base) + "/" + specifier);
  }

  function resolveKey(environmentName, id, importerKey) {
    if (records.has(String(id))) {
      return String(id);
    }
    const importer = importerKey ? records.get(importerKey) : undefined;
    const raw = String(id);
    const resolved = importer ? resolveRelative(importer.url || importer.id, raw) : raw;
    const candidates = [resolved, clean(resolved)];
    if (resolved.startsWith("/@fs/")) {
      candidates.push(resolved.slice("/@fs".length), clean(resolved.slice("/@fs".length)));
    }
    for (const candidate of candidates) {
      const aliasKey = environmentName + "\\0" + candidate;
      const key = aliases.get(aliasKey) ?? aliasKey;
      if (records.has(key)) {
        return key;
      }
    }
    if (raw.includes("virtual:vite-rsc/")) {
      if (raw.includes("virtual:vite-rsc/reference-validation?")) {
        return emptyVirtualKey;
      }
      if (raw.includes("virtual:vite-rsc/css?") && !raw.includes("type=")) {
        return emptyCssVirtualKey;
      }
      for (const candidate of candidates) {
        const suffix = "\\0" + candidate;
        for (const [aliasKey, key] of aliases) {
          if (aliasKey.endsWith(suffix) && records.has(key)) {
            return key;
          }
        }
      }
    }
    throw new Error("dd Vite module runner could not resolve " + raw + " in " + environmentName);
  }

  async function load(environmentName, id, importerKey) {
    const key = resolveKey(environmentName, id, importerKey);
    const record = records.get(key);
    if (record.externalize) {
      if (record.externalize === "node:module") {
        return nodeModuleShim;
      }
      return import(record.externalize);
    }

    const cached = cache.get(key);
    if (cached?.evaluated) {
      return cached.exports;
    }
    if (cached?.promise) {
      return cached.executing ? cached.exports : cached.promise;
    }

    const exports = Object.create(null);
    Object.defineProperty(exports, Symbol.toStringTag, {
      value: "Module",
      enumerable: false,
      configurable: false,
    });

    const state = { exports, evaluated: false, executing: true, promise: undefined };
    cache.set(key, state);

    const request = (specifier) => load(record.environmentName, String(specifier), key);
    const dynamicRequest = (specifier) => load(record.environmentName, String(specifier), key);
    const exportAll = (sourceModule) => {
      if (
        sourceModule == null ||
        exports === sourceModule ||
        typeof sourceModule !== "object" && typeof sourceModule !== "function"
      ) {
        return;
      }
      for (const name of Object.keys(sourceModule)) {
        if (name === "default" || name === "__esModule" || name in exports) continue;
        Object.defineProperty(exports, name, {
          enumerable: true,
          configurable: true,
          get: () => sourceModule[name],
        });
      }
    };
    const exportName = (name, getter) => {
      Object.defineProperty(exports, name, {
        enumerable: true,
        configurable: true,
        get: getter,
      });
    };

    globalThis.__VITE_ENVIRONMENT_RUNNER_IMPORT__ = (targetEnvironmentName, targetId) =>
      load(String(targetEnvironmentName), String(targetId), undefined);

    const meta = {
      url: record.url || record.id,
      env: { ...(record.importMetaEnv || {}) },
      dirname: dirname(record.file || record.id),
      filename: record.file || record.id,
      resolve(specifier) {
        return specifier;
      },
      glob() {
        throw new Error("import.meta.glob is not available in the dd Vite dev runner");
      },
    };

    state.promise = (async () => {
      try {
        await new __ddViteAsyncFunction(
          "__vite_ssr_exports__",
          "__vite_ssr_import_meta__",
          "__vite_ssr_import__",
          "__vite_ssr_dynamic_import__",
          "__vite_ssr_exportAll__",
          "__vite_ssr_exportName__",
          "\\"use strict\\";\\n" + record.code,
        )(exports, meta, request, dynamicRequest, exportAll, exportName);
        Object.seal(exports);
        state.evaluated = true;
        return exports;
      } finally {
        state.executing = false;
      }
    })();

    return state.promise;
  }

  installDevFetchBridge();

  let activeWorker;
  await activateWorker();

  async function activateWorker() {
    const entry = await load(graph.entryEnvironment, graph.entryKey, undefined);
    const worker = entry.default ?? entry;
    if (!worker || typeof worker.fetch !== "function") {
      throw new Error("dd Vite module runner entry did not export a Worker fetch handler");
    }
    activeWorker = worker;
    return worker;
  }

  async function applyGraphUpdate(nextGraph) {
    records.clear();
    for (const record of nextGraph.records ?? []) {
      records.set(record.key, record);
    }
    aliases.clear();
    for (const [key, value] of nextGraph.aliases ?? []) {
      aliases.set(key, value);
    }
    devFetchOrigins.clear();
    for (const origin of nextGraph.devFetchOrigins ?? []) {
      devFetchOrigins.add(origin);
    }
    graph.entryEnvironment = nextGraph.entryEnvironment;
    graph.entryKey = nextGraph.entryKey;
    graph.records = nextGraph.records ?? [];
    installInternalRecords(graph.records[0]?.importMetaEnv ?? {});
    cache.clear();
    await activateWorker();
  }

  function isGraphUpdateRequest(request) {
    if (!(request instanceof Request)) {
      return false;
    }
    if (request.headers.get("x-dd-vite-module-runner-update") !== updateToken) {
      return false;
    }
    try {
      return new URL(request.url).pathname === "/__dd_vite_module_runner_update";
    } catch {
      return false;
    }
  }

  return new Proxy({}, {
    get(_target, property) {
      if (property === "fetch") {
        return async (request, ...args) => {
          installDevFetchBridge();
          if (isGraphUpdateRequest(request)) {
            await applyGraphUpdate(await request.json());
            return new Response(null, { status: 204 });
          }
          return activeWorker.fetch(request, ...args);
        };
      }
      const value = activeWorker?.[property];
      return typeof value === "function" ? value.bind(activeWorker) : value;
    },
    has(_target, property) {
      return property === "fetch" || property in (activeWorker ?? {});
    },
    ownKeys() {
      return Reflect.ownKeys(activeWorker ?? {});
    },
    getOwnPropertyDescriptor(_target, property) {
      if (property === "fetch" || property in (activeWorker ?? {})) {
        return {
          configurable: true,
          enumerable: true,
          value: this.get(_target, property),
        };
      }
    },
  });
}
`;

function viteRequestForFile(file, root) {
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

function viteDevServerOrigins(viteServer) {
  const origins = new Set();
  for (const url of viteDevServerUrlCandidates(viteServer)) {
    try {
      origins.add(new URL(url).origin);
    } catch {
      // Ignore malformed debug URLs.
    }
  }
  origins.add("http://127.0.0.1:5173");
  origins.add("http://localhost:5173");
  return [...origins];
}

function rewriteViteDevServerUrls(code, viteServer) {
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

function viteDevServerUrlCandidates(viteServer) {
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

function viteDevServerActualUrl(viteServer) {
  const address = viteServer.httpServer?.address?.();
  if (address && typeof address === "object") {
    const protocol = viteServer.config.server.https ? "https" : "http";
    return `${protocol}://127.0.0.1:${address.port}/`;
  }
  return undefined;
}

function patchViteResolvedUrls(viteServer) {
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

function toVitePath(value) {
  return String(value).replace(/\\/g, "/");
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

async function writeNodeResponse(res, response) {
  const body = Buffer.from(await response.arrayBuffer());
  res.statusCode = response.status;
  response.headers.forEach((value, name) => {
    const lower = name.toLowerCase();
    if (lower === "content-length" || lower === "transfer-encoding") {
      return;
    }
    res.setHeader(name, value);
  });
  res.setHeader("content-length", String(body.length));
  res.end(body);
}

async function handleDdWebSocketUpgrade(req, socket, head, options) {
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

function isWebSocketUpgrade(req) {
  return headerValue(req.headers.upgrade).toLowerCase() === "websocket";
}

function shouldBypassDdWebSocketUpgrade(req, originalUrl, mount, viteBase) {
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
  if (url.searchParams.has("token")) {
    return true;
  }
  return candidatePathnames(url.pathname, viteBase).some((pathname) =>
    isViteBypassPath(pathname) || isViteAnyMethodBypassPath(pathname)
  );
}

function nodeUpgradeToWorkerInvocation(req, originalUrl, mount, workerName) {
  const path = stripMount(originalUrl, mount);
  const url = nodeRequestWorkerUrl(req, path, workerName);
  return {
    method: req.method ?? "GET",
    url: String(url),
    headers: nodeRequestHeaders(req),
    body_base64: "",
  };
}

function nodeRequestWorkerUrl(req, path, workerName) {
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

function nodeRequestHeaders(req) {
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

function writeWebSocketHandshake(socket, req, headers = []) {
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

function shouldSkipWebSocketHandshakeHeader(name) {
  const lower = String(name).toLowerCase();
  return (
    lower === "connection" ||
    lower === "upgrade" ||
    lower === "sec-websocket-accept" ||
    lower === "content-length" ||
    lower === "transfer-encoding"
  );
}

function writeRawHttpResponse(socket, status, headers = [], bodyBase64 = "") {
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

function statusReasonPhrase(status) {
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
    this.polling = false;
    this.frameQueue = Promise.resolve();
    this.pollTimer = undefined;
  }

  start(head) {
    this.socket.setNoDelay?.(true);
    this.socket.on("data", (chunk) => this.consume(chunk));
    this.socket.on("close", () => {
      this.closed = true;
      this.stopPolling();
      void this.closeRuntime(1006, "socket closed");
    });
    this.socket.on("error", () => {
      this.closed = true;
      this.stopPolling();
      void this.closeRuntime(1011, "socket error");
    });
    this.pollTimer = setInterval(() => {
      void this.pollRuntimeFrames();
    }, 100);
    if (head?.length) {
      this.consume(head);
    }
    this.socket.resume?.();
    void this.pollRuntimeFrames();
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
        await this.pollRuntimeFrames();
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

  async pollRuntimeFrames() {
    if (this.closed || this.polling) {
      return;
    }
    this.polling = true;
    try {
      for (let index = 0; index < 32 && !this.closed; index += 1) {
        const result = await this.runtime.drainWebSocketFrame(this.workerName, this.sessionId);
        if (!result.frame) {
          break;
        }
        this.forwardRuntimeOutput(result.frame);
      }
    } catch (error) {
      this.fail(error);
    } finally {
      this.polling = false;
    }
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
    this.stopPolling();
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

  stopPolling() {
    if (this.pollTimer) {
      clearInterval(this.pollTimer);
      this.pollTimer = undefined;
    }
  }
}

function parseClientWebSocketFrame(buffer) {
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

function sendServerWebSocketFrame(socket, opcode, payload) {
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

function parseClientClosePayload(payload) {
  if (payload.length < 2) {
    return { code: 1000, reason: "" };
  }
  return {
    code: payload.readUInt16BE(0),
    reason: payload.subarray(2).toString("utf8"),
  };
}

function encodeClosePayload(code, reason) {
  const text = Buffer.from(String(reason ?? "").slice(0, 120));
  const payload = Buffer.alloc(2 + text.length);
  payload.writeUInt16BE(Number(code) || 1000, 0);
  text.copy(payload, 2);
  return payload;
}

function headerListValue(headers, name) {
  const found = (headers ?? []).find(([headerName]) =>
    String(headerName).toLowerCase() === String(name).toLowerCase()
  );
  return found ? String(found[1]) : "";
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

function shouldBypassViteRequest(req, originalUrl, mount, viteBase) {
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

  if (candidatePathnames(url.pathname, viteBase).some(isViteBypassPath)) {
    return true;
  }
  for (const key of VITE_BYPASS_QUERY_KEYS) {
    if (url.searchParams.has(key)) {
      return true;
    }
  }

  const fetchDestination = headerValue(req.headers["sec-fetch-dest"]).toLowerCase();
  return VITE_BYPASS_FETCH_DESTINATIONS.has(fetchDestination);
}

function candidatePathnames(pathname, viteBase) {
  const paths = [pathname];
  const base = normalizeViteBase(viteBase);
  if (base !== "/" && (pathname === base.slice(0, -1) || pathname.startsWith(base))) {
    paths.push(`/${pathname.slice(base.length)}`);
  }
  return paths;
}

function isViteBypassPath(pathname) {
  return VITE_BYPASS_PREFIXES.some((prefix) => {
    if (prefix.endsWith("/")) {
      return pathname.startsWith(prefix);
    }
    return pathname === prefix || pathname.startsWith(`${prefix}/`);
  });
}

function isViteAnyMethodBypassPath(pathname) {
  return VITE_ANY_METHOD_BYPASS_PATHS.includes(pathname);
}

function normalizeViteBase(value) {
  if (typeof value !== "string" || value.length === 0 || value === "./") {
    return "/";
  }
  let base = value;
  try {
    base = new URL(value).pathname;
  } catch {
    // Vite base is commonly a path. Keep it as-is when it is not an absolute URL.
  }
  const normalized = normalizeMount(base);
  return normalized === "/" ? "/" : `${normalized}/`;
}

function headerValue(value) {
  if (Array.isArray(value)) {
    return value[0] ?? "";
  }
  return value ?? "";
}

function shouldBypassStaticRoutingRequest(req, originalUrl, mount, routing) {
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

async function writeStaticAssetResponse(req, res, originalUrl, mount, routing) {
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
    fileStat = await stat(file);
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

async function loadStaticRouting(source, viteConfig, deploymentConfig) {
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

function staticRoutingFromDeploymentConfig(viteConfig, deploymentConfig) {
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

async function staticRoutingCandidatePaths(source, viteConfig) {
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

function matchesAnyStaticRoute(patterns, pathname) {
  return patterns.some((pattern) => matchesStaticRoute(pattern, pathname));
}

function matchesStaticRoute(pattern, pathname) {
  const normalized = String(pattern).startsWith("/") ? String(pattern) : `/${pattern}`;
  const regex = new RegExp(`^${normalized.split("*").map(escapeRegex).join(".*")}$`);
  return regex.test(pathname);
}

function escapeRegex(value) {
  return value.replace(/[|\\{}()[\]^$+?.]/g, "\\$&");
}

function staticAssetContentType(file) {
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

function hasFrameworkBuildEnvironments(config) {
  return Boolean(config?.environments?.ssr || config?.environments?.rsc);
}

async function writeOutputFile(outDir, file, contents) {
  const path = join(outDir, file);
  await mkdir(dirname(path), { recursive: true });
  await writeFile(path, contents);
}

async function outputFileExists(outDir, file) {
  try {
    return (await stat(join(outDir, file))).isFile();
  } catch (error) {
    if (isMissingFileError(error)) {
      return false;
    }
    throw error;
  }
}

async function writeGeneratedStaticRoutes(outDir, deploymentConfig) {
  const routes = deploymentConfig.staticRoutes;
  if (!routes) {
    return;
  }
  const assetsOutDir = deploymentAssetsOutputDir(outDir, deploymentConfig.assetsDir);
  if (!assetsOutDir) {
    return;
  }
  await writeOutputFile(
    assetsOutDir,
    DEFAULT_STATIC_ROUTES_FILE,
    `${JSON.stringify({
      version: routes.version ?? 1,
      include: arrayOfStrings(routes.include),
      exclude: arrayOfStrings(routes.exclude),
    }, null, 2)}\n`,
  );
}

async function writeGeneratedAssetPolicy(outDir, config, deploymentAssetsDir) {
  const assetsOutDir = deploymentAssetsOutputDir(outDir, deploymentAssetsDir);
  if (!assetsOutDir) {
    return;
  }
  const assetsDir = normalizeConfigRelativePath(
    config?.build?.assetsDir ?? "assets",
    "build.assetsDir",
  );
  const assetPaths = new Set(await findGeneratedAssetPaths(assetsOutDir));
  if (assetsDir === "." || (await directoryExists(join(assetsOutDir, assetsDir)))) {
    assetPaths.add(assetsDir === "." ? "/*" : `/${assetsDir}/*`);
  }
  const headersPath = join(assetsOutDir, DEFAULT_ASSET_HEADERS_FILE);
  let existing = "";
  try {
    existing = await readFile(headersPath, "utf8");
  } catch (error) {
    if (!isMissingFileError(error)) {
      throw error;
    }
  }

  const rules = [...assetPaths]
    .sort()
    .filter((assetPath) => !hasHeaderPathRule(existing, assetPath))
    .map((assetPath) => `${assetPath}\n  Cache-Control: ${FINGERPRINTED_ASSET_CACHE_CONTROL}\n`);

  if (rules.length === 0) {
    return;
  }

  const prefix = existing.trimEnd();
  await writeOutputFile(
    assetsOutDir,
    DEFAULT_ASSET_HEADERS_FILE,
    prefix.length > 0 ? `${prefix}\n\n${rules.join("\n")}` : rules.join("\n"),
  );
}

function deploymentAssetsOutputDir(outDir, assetsDir) {
  if (assetsDir === false) {
    return undefined;
  }
  return join(
    outDir,
    normalizeConfigRelativePath(
      assetsDir ?? DEFAULT_DEPLOYMENT_ASSETS_DIR,
      "deploymentConfig.assetsDir",
    ),
  );
}

async function findGeneratedAssetPaths(outDir, relativeDir = "") {
  let entries;
  try {
    entries = await readdir(join(outDir, relativeDir), { withFileTypes: true });
  } catch (error) {
    if (isMissingFileError(error)) {
      return [];
    }
    throw error;
  }

  const paths = [];
  for (const entry of entries) {
    if (!entry.isDirectory()) {
      continue;
    }
    const child = relativeDir.length > 0 ? `${relativeDir}/${entry.name}` : entry.name;
    if (entry.name === "assets") {
      paths.push(`/${child}/*`);
    }
    paths.push(...(await findGeneratedAssetPaths(outDir, child)));
  }
  return paths;
}

async function directoryExists(path) {
  try {
    return (await stat(path)).isDirectory();
  } catch (error) {
    if (isMissingFileError(error)) {
      return false;
    }
    throw error;
  }
}

function hasHeaderPathRule(contents, pathRule) {
  return contents
    .split(/\r?\n/)
    .some((line) => line.trimStart() === line && line.trim() === pathRule);
}

async function buildGeneratedDeploymentConfig({
  base,
  options,
  workerName,
  workerFile,
  configFile,
  assetsDir,
  assetExcludes,
  staticRoutes,
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
    ...(staticRoutes ? [DEFAULT_STATIC_ROUTES_FILE] : []),
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

function uniqueStrings(values) {
  return [...new Set(values)];
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
