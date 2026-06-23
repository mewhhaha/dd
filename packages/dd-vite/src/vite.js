import {
  createFetchableDevEnvironment,
  createRunnableDevEnvironment,
  fetchModule,
  mergeConfig,
} from "vite";
import { spawn } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { mkdir, readFile, readdir, rm, stat, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { dirname, extname, isAbsolute, join, relative, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
import { bundleWorkerEntry, createDdRuntime } from "./runtime.js";
import { createWorkerTestRuntime } from "./vitest.js";

const DEFAULT_MOUNT = "/";
const DEFAULT_SOURCE_CONFIG_FILE = "dd.json";
const DEFAULT_DEPLOYMENT_CONFIG_FILE = "dd.deploy.json";
const DEFAULT_DEPLOYMENT_WORKER_FILE = "worker.js";
const DEFAULT_DEPLOYMENT_ASSETS_DIR = ".";
const DEFAULT_WORKERS_MANIFEST_FILE = "dd.workers.json";
const DEFAULT_ENV_TYPES_FILE = "dd-env.d.ts";
const DEFAULT_CLIENT_OUTPUT_DIR = "client";
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
const DD_AUXILIARY_WORKERS_MODULE = "virtual:dd-auxiliary-workers";
const DD_AUXILIARY_WORKERS_RESOLVED = "\0dd:auxiliary-workers";
const FINGERPRINTED_ASSET_CACHE_CONTROL = "public, max-age=31536000, immutable";
const DD_VITE_BYPASS_HEADER = "x-dd-vite-bypass";
const DD_VITE_MODULE_HEADER = "x-dd-vite-module-token";
const DD_VITE_MODULE_ENDPOINT_PATH = "/__dd_vite/module";
const DD_VITE_MODULE_INVALIDATE_PATH = "/__dd_vite/invalidate";
const BODYLESS_METHODS = new Set(["GET", "HEAD"]);
const VITE_BYPASS_PREFIXES = [
  "/@vite",
  "/@id/",
  "/@fs/",
  "/@react-refresh",
  "/__vitest__/",
  "/__vitest_attachment__",
  "/__vitest_browser__/",
  "/__vitest_browser_api__",
  "/__vitest_test__/",
  "/node_modules/",
  "/.vite/",
];
const VITE_SOURCE_PREFIXES = [
  "/app/",
  "/src/",
];
const VITE_ANY_METHOD_BYPASS_PATHS = [
  "/__vite_rsc_findSourceMapURL",
  "/__vite_rsc_load_module_dev_proxy",
];
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
let workerBundleEnvDepth = 0;
let workerBundleEnvPrevious;

export function ddEnvironment(options = {}) {
  const environmentOptions = ddEnvironmentUserOptions(options);
  const runtimeOptions = ddEnvironmentRuntimeOptions(options);
  return mergeConfig(
    {
      ...ddEnvironmentBaseOptions(options),
      dev: {
        createEnvironment(name, config) {
          let runtimePromise;
          const getRuntime = () => {
            runtimePromise ??= createWorkerTestRuntime({
              ...runtimeOptions,
              name: runtimeOptions.name ?? name,
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
  return mergeConfig(
    options.environmentOptions ?? {},
    options.viteEnvironment?.options ?? {},
  );
}

function ddEnvironmentRuntimeOptions(options) {
  return {
    name: options.name,
    entry: options.entry,
    source: options.source,
    config: options.config,
    runtime: options.runtime,
    runtimeOptions: options.runtimeOptions,
    autoDeploy: options.autoDeploy,
    viteConfig: options.viteConfig,
    target: options.target,
    sourcemap: options.sourcemap,
    minify: options.minify,
    logLevel: options.logLevel,
  };
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

async function withWorkerBundleEnv(callback) {
  if (workerBundleEnvDepth === 0) {
    workerBundleEnvPrevious = process.env[DD_VITE_WORKER_BUNDLE_ENV];
    process.env[DD_VITE_WORKER_BUNDLE_ENV] = "1";
  }
  workerBundleEnvDepth += 1;
  try {
    return await callback();
  } finally {
    workerBundleEnvDepth -= 1;
    if (workerBundleEnvDepth === 0) {
      if (workerBundleEnvPrevious === undefined) {
        delete process.env[DD_VITE_WORKER_BUNDLE_ENV];
      } else {
        process.env[DD_VITE_WORKER_BUNDLE_ENV] = workerBundleEnvPrevious;
      }
      workerBundleEnvPrevious = undefined;
    }
  }
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
  const auxiliaryWorkers = normalizeAuxiliaryWorkers(options.auxiliaryWorkers);
  const mount = normalizeMount(options.mount ?? DEFAULT_MOUNT);
  const moduleRunnerToken = randomUUID();
  let runtime;
  let deployment;
  let deploymentRuntimeGeneration;
  let deploying;
  const hotReloadMode = normalizeHotReloadMode(options.reloadOnHotUpdate);
  const deploymentConfig = normalizeDeploymentConfigOptions(
    mergeFrameworkDeploymentConfig(framework, options.deploymentConfig),
  );
  let frameworkDevBuildComplete = false;
  let frameworkDevBuildPromise;
  let reactRouterRscClientUpdateTimer;
  let reactRouterRscClientUpdateFile;
  let sourceConfigRoot;
  let sourceConfigPromise;
  let resolvedWorkersRoot;
  let resolvedWorkersPromise;
  let resolvedWorkersSnapshot = [];
  let environmentWorkerEntry;
  let buildOutputRoot;
  let buildClientOutputDir = DEFAULT_CLIENT_OUTPUT_DIR;

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
    return withAuxiliaryRuntimeBindings(
      options.config ??
      source.config.config ??
      topLevelRuntimeConfig(source.config) ??
      { public: true },
      auxiliaryWorkers,
    );
  }

  async function resolvedWorkers() {
    const root = resolveFrameworkRoot();
    if (!resolvedWorkersPromise || resolvedWorkersRoot !== root) {
      resolvedWorkersRoot = root;
      resolvedWorkersPromise = createResolvedWorkers(root).then((workers) => {
        resolvedWorkersSnapshot = workers;
        return workers;
      });
    }
    return resolvedWorkersPromise;
  }

  async function createResolvedWorkers(root) {
    const source = await sourceConfig();
    const entryName = options.name ?? nonEmptyString(source.config.name) ?? DEFAULT_WORKER_NAME;
    const entryWorker = {
      role: "entry",
      kind: "entry",
      name: entryName,
      runtimeName: entryName,
      outputName: workerOutputName(entryName),
      environmentName: viteEnvironment?.name ?? defaultEntryWorkerEnvironmentName(framework, viteCommand, entryName),
      childEnvironmentNames: uniqueStrings([
        ...defaultEntryWorkerChildEnvironmentNames(framework, viteCommand),
        ...arrayOfStrings(viteEnvironment?.childEnvironments),
      ]),
      viteEnvironment,
      entry: await effectiveWorkerEntry(),
      source: options.source,
      config: await effectiveRuntimeConfig(),
      deploymentConfig,
      target: options.target,
      sourcemap: options.sourcemap,
      minify: options.minify,
      logLevel: options.logLevel,
      viteConfig: options.viteConfig,
    };
    const workers = [
      entryWorker,
      ...auxiliaryWorkers.map((worker) => {
        const workerViteEnvironment = worker.viteEnvironment;
        const runtimeName = worker.kind === "service" ? worker.service : worker.id;
        return {
          ...worker,
          role: "auxiliary",
          runtimeName,
          outputName: workerOutputName(worker.name),
          environmentName: workerViteEnvironment?.name ?? workerEnvironmentName(worker.name),
          childEnvironmentNames: arrayOfStrings(workerViteEnvironment?.childEnvironments),
          viteEnvironment: workerViteEnvironment,
          entry: worker.entry ? resolveFrameworkPath(root, worker.entry) : undefined,
          source: worker.source,
          config: worker.kind === "service" ? auxiliaryWorkerServiceConfig(worker) : cloneJson(worker.config),
        };
      }),
    ];
    validateResolvedWorkers(workers);
    return workers;
  }

  async function resolvedEntryWorker() {
    return (await resolvedWorkers())[0];
  }

  function currentResolvedWorkerByEnvironment(name) {
    return resolvedWorkersSnapshot.find((worker) => worker.environmentName === name);
  }

  function currentResolvedWorkerByChildEnvironment(name) {
    return resolvedWorkersSnapshot.find((worker) => worker.childEnvironmentNames.includes(name));
  }

  async function workerSource(workerOrOptions = {}, maybeSourceOptions = {}) {
    const worker = workerOrOptions?.role ? workerOrOptions : await resolvedEntryWorker();
    const sourceOptions = workerOrOptions?.role ? maybeSourceOptions : workerOrOptions;
    const production = sourceOptions.production === true;
    if (typeof worker.source === "function") {
      return worker.source();
    }
    if (typeof worker.source === "string") {
      return worker.source;
    }
    const entry = worker.entry ?? (worker.role === "entry" ? await effectiveWorkerEntry() : undefined);
    if (!entry) {
      throw new Error(
        worker.role === "entry"
          ? "ddVitePlugin requires either source or entry"
          : `ddVitePlugin auxiliary worker "${worker.name}" requires source or entry`,
      );
    }
    if (framework && worker.role === "entry") {
      if (usesViteModuleRunner(worker, sourceOptions)) {
        return viteModuleRunnerWorkerSource(devServer, entry, moduleRunnerOptions(worker));
      }
      return bundleFrameworkWorkerSource(framework, entry, { production });
    }
    if (usesViteModuleRunner(worker, sourceOptions)) {
      return viteModuleRunnerWorkerSource(devServer, entry, moduleRunnerOptions(worker));
    }
    return bundleWorkerEntry(entry, {
      viteConfig: mergeConfig(workerBundleViteConfig(), worker.viteConfig ?? {}),
      target: worker.target ?? options.target,
      sourcemap: worker.sourcemap ?? options.sourcemap ?? false,
      minify: workerBundleMinify(worker.minify ?? options.minify, production),
      logLevel: worker.logLevel ?? options.logLevel,
    });
  }

  function workerBundleMinify(minify, production = false) {
    return minify ?? (production || viteCommand === "build");
  }

  function workerBundleViteConfig() {
    const projectConfig = {};
    if (resolvedConfig?.root) {
      projectConfig.root = resolvedConfig.root;
    }
    if (resolvedConfig?.configFile && options.viteConfig?.configFile !== false) {
      projectConfig.configFile = resolvedConfig.configFile;
    }
    const auxiliaryPlugin = auxiliaryWorkers.length > 0
      ? { plugins: [ddAuxiliaryWorkersVirtualPlugin(auxiliaryWorkersModuleSource)] }
      : {};
    return mergeConfig(mergeConfig(projectConfig, auxiliaryPlugin), options.viteConfig ?? {});
  }

  async function bundledWorkerSource(sourceOptions) {
    return withWorkerBundleEnv(() => workerSource(sourceOptions));
  }

  async function auxiliaryWorkersModuleSource() {
    const records = Object.fromEntries(
      await Promise.all(
        auxiliaryWorkers.map(async (worker) => [
          worker.name,
          {
            name: worker.name,
            kind: worker.kind,
            binding: worker.binding,
            id: worker.id,
            service: worker.service,
            config: worker.kind === "dynamic" ? await auxiliaryWorkerDynamicConfig(worker) : cloneJson(worker.config),
          },
        ]),
      ),
    );
    return `const workers = ${JSON.stringify(records)};\nexport { workers };\nexport default workers;\n`;
  }

  async function auxiliaryWorkerDynamicConfig(worker) {
    const config = cloneJson(worker.config);
    if (!config.source && !config.modules) {
      config.source = await auxiliaryWorkerSource(worker);
    }
    if (Array.isArray(config.bindings) && config.bindings.length > 0 && config.allow_state_bindings == null) {
      config.allow_state_bindings = true;
    }
    return config;
  }

  async function auxiliaryWorkerSource(worker) {
    const resolved = (await resolvedWorkers()).find((entry) => entry.role === "auxiliary" && entry.name === worker.name) ?? worker;
    return withWorkerBundleEnv(() => workerSource(resolved));
  }

  async function ensureDeployed() {
    runtime ??= createDdRuntime(options.runtimeOptions);
    if (deployment && deploymentRuntimeGeneration === runtime.generation) {
      return deployment;
    }
    deployment = undefined;
    deploymentRuntimeGeneration = undefined;
    const entryWorker = await resolvedEntryWorker();
    const deployAll = async () => {
      await deployAuxiliaryServiceWorkers(runtime);
      return runtime.deploy(entryWorker.runtimeName, await bundledWorkerSource(), entryWorker.config);
    };
    deploying ??= deployAll()
      .catch(async (error) => {
        if (!isRecoverableRuntimeClientError(error)) {
          throw error;
        }
        await runtime?.close().catch(() => {});
        runtime = createDdRuntime(options.runtimeOptions);
        await deployAuxiliaryServiceWorkers(runtime);
        return runtime.deploy(entryWorker.runtimeName, await bundledWorkerSource(), entryWorker.config);
      })
      .finally(() => {
        deploying = undefined;
      });
    deployment = await deploying;
    deploymentRuntimeGeneration = runtime.generation;
    return deployment;
  }

  async function deployAuxiliaryServiceWorkers(targetRuntime) {
    for (const worker of await resolvedWorkers()) {
      if (worker.kind !== "service") {
        continue;
      }
      await targetRuntime.deploy(
        worker.runtimeName,
        await workerSource(worker),
        worker.config,
      );
    }
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

  function moduleRunnerOptions(worker) {
    return {
      environmentName: worker.environmentName,
      childEnvironmentNames: worker.childEnvironmentNames,
      root: resolveFrameworkRoot(),
      token: moduleRunnerToken,
    };
  }

  async function updateViteModuleRunnerDeployment() {
    if (
      viteCommand !== "serve" ||
      !devServer ||
      !deployment ||
      !runtime ||
      deploymentRuntimeGeneration !== runtime.generation
    ) {
      deployment = undefined;
      deploymentRuntimeGeneration = undefined;
      return false;
    }
    for (const worker of await resolvedWorkers()) {
      if (!worker.entry || typeof worker.source === "string" || typeof worker.source === "function") {
        continue;
      }
      if (worker.role !== "entry" && worker.kind !== "service") {
        continue;
      }
      if (!usesViteModuleRunner(worker)) {
        deployment = undefined;
        deploymentRuntimeGeneration = undefined;
        return false;
      }
      const response = await runtime.fetch(
        worker.runtimeName,
        new Request(`http://${worker.runtimeName}.dd.local${DD_VITE_MODULE_INVALIDATE_PATH}`, {
          method: "POST",
          headers: {
            [DD_VITE_MODULE_HEADER]: moduleRunnerToken,
          },
        }),
      );
      if (!response.ok) {
        deployment = undefined;
        deploymentRuntimeGeneration = undefined;
        throw new Error(
          `ddVitePlugin failed to invalidate dev module cache for ${worker.runtimeName}: ` +
            `${response.status} ${await response.text()}`,
        );
      }
    }
    return true;
  }

  function usesViteModuleRunner(worker, sourceOptions = {}) {
    if (sourceOptions.allowDevModuleRunner === false || viteCommand !== "serve" || !devServer) {
      return false;
    }
    if (!worker.entry || typeof worker.source === "string" || typeof worker.source === "function") {
      return false;
    }
    return !(framework && worker.role === "entry" && framework.name === REACT_ROUTER_RSC_FRAMEWORK);
  }

  function registeredWorkerEnvironmentOptions(workers, baseOutDir) {
    const entries = [];
    for (const worker of workers) {
      entries.push([worker.environmentName, workerBuildEnvironmentOptions(worker, baseOutDir)]);
      for (const childName of worker.childEnvironmentNames) {
        entries.push([childName, {}]);
      }
    }
    return Object.fromEntries(entries);
  }

  function workerBuildEnvironmentOptions(worker, baseOutDir) {
    const entry = worker.entry;
    if (viteCommand === "build" && !entry) {
      throw new Error(`ddVitePlugin worker "${worker.name}" requires an entry for production builds`);
    }
    const environmentBase = ddEnvironmentBaseOptions({
      optimizeDepsEntries: entry ? [viteRequestForFile(entry, rootHint ?? process.cwd())] : [],
    });
    const output = {
      entryFileNames: DEFAULT_DEPLOYMENT_WORKER_FILE,
      chunkFileNames: "assets/[name]-[hash].js",
      assetFileNames: "assets/[name]-[hash][extname]",
      codeSplitting: false,
    };
    return mergeConfig(
      {
        ...environmentBase,
        define: workerRuntimeDefines("production"),
        build: {
          ssr: true,
          outDir: childBuildOutDir(baseOutDir, worker.outputName),
          emptyOutDir: true,
          copyPublicDir: false,
          sourcemap: worker.sourcemap ?? options.sourcemap ?? false,
          minify: workerBundleMinify(worker.minify ?? options.minify, true),
          rollupOptions: {
            input: entry ? { worker: entry } : undefined,
            output: {
              ...output,
              banner: workerRuntimeGlobalsBanner("production"),
            },
          },
        },
      },
      worker.viteEnvironment?.options ?? {},
    );
  }

  async function buildDdApp(builder) {
    if (options.environment === false) {
      return;
    }
    const workers = await resolvedWorkers();
    await emptyBuildOutputRoot(builder.config);
    if (framework) {
      await ensureFrameworkDevBuild(framework);
    } else if (builder.environments.client && !builder.environments.client.isBuilt) {
      await builder.build(builder.environments.client);
    }
    const buildOrder = [
      ...workers.filter((worker) => worker.role !== "entry"),
      ...workers.filter((worker) => worker.role === "entry"),
    ];
    for (const worker of buildOrder) {
      const environment = builder.environments[worker.environmentName];
      if (!environment) {
        throw new Error(`ddVitePlugin could not find Vite build environment "${worker.environmentName}"`);
      }
      if (!environment.isBuilt) {
        await builder.build(environment);
      }
    }
    await writeWorkerDeploymentArtifacts(workers);
  }

  async function emptyBuildOutputRoot(config) {
    if (config?.build?.emptyOutDir === false) {
      return;
    }
    const outRoot = buildOutputRoot ?? resolveBuildOutputRoot(config, resolveFrameworkRoot());
    await rm(outRoot, { recursive: true, force: true });
  }

  async function writeWorkerDeploymentArtifacts(workers) {
    if (viteCommand !== "build") {
      return;
    }
    const outRoot = buildOutputRoot ?? resolveBuildOutputRoot(resolvedConfig, resolveFrameworkRoot());
    const clientAssetsRel = await resolveClientAssetsDir(outRoot);
    const manifestWorkers = [];
    for (const worker of workers) {
      const workerOutDir = join(outRoot, worker.outputName);
      const workerFile = DEFAULT_DEPLOYMENT_WORKER_FILE;
      const configFile = DEFAULT_DEPLOYMENT_CONFIG_FILE;
      const config = await buildWorkerDeploymentConfig({
        worker,
        workerFile,
        configFile,
        clientAssetsRel,
      });
      await writeOutputFile(workerOutDir, configFile, `${JSON.stringify(config, null, 2)}\n`);
      manifestWorkers.push({
        name: worker.name,
        role: worker.role,
        kind: worker.kind,
        service: worker.kind === "service" ? worker.service : undefined,
        binding: worker.binding,
        environment: worker.environmentName,
        outDir: worker.outputName,
        worker: joinConfigRelativePath(worker.outputName, workerFile),
        deployConfig: joinConfigRelativePath(worker.outputName, configFile),
      });
      if (worker.role === "entry") {
        const generatedDeploymentConfig = {
          ...worker.deploymentConfig,
          assetsDir: config.assets_dir ?? false,
        };
        await writeGeneratedStaticRoutes(workerOutDir, generatedDeploymentConfig);
        await writeGeneratedAssetPolicy(workerOutDir, resolvedConfig, generatedDeploymentConfig.assetsDir);
      }
    }
    const entry = workers.find((worker) => worker.role === "entry");
    await writeOutputFile(
      outRoot,
      DEFAULT_WORKERS_MANIFEST_FILE,
      `${JSON.stringify({
        version: 1,
        entry: entry?.name,
        workers: manifestWorkers,
      }, null, 2)}\n`,
    );
  }

  async function resolveClientAssetsDir(outRoot) {
    if (deploymentConfig.assetsDir === false) {
      return false;
    }
    if (deploymentConfig.assetsDir != null) {
      return normalizeDeploymentAssetsDir(deploymentConfig.assetsDir, "deploymentConfig.assetsDir");
    }
    const candidate = framework
      ? frameworkClientAssetsDir(framework)
      : buildClientOutputDir;
    if (await directoryExists(join(outRoot, candidate))) {
      return candidate;
    }
    return false;
  }

  async function buildWorkerDeploymentConfig({ worker, workerFile, configFile, clientAssetsRel }) {
    const source = await sourceConfig();
    if (worker.role === "entry") {
      return buildGeneratedDeploymentConfig({
        base: source.config,
        options,
        workerName: worker.runtimeName,
        workerFile,
        configFile,
        assetsDir: clientAssetsRel === false ? false : relativeDeploymentAssetsDir(worker.outputName, clientAssetsRel),
        assetExcludes: deploymentConfig.assetExcludes,
        extraAssetExcludes: [],
        serverModules: deploymentConfig.serverModules ?? deploymentConfig.server_modules,
        staticRoutes: deploymentConfig.staticRoutes,
      });
    }
    return buildAuxiliaryServiceDeploymentConfig({
      base: source.config,
      worker,
      workerFile,
    });
  }

  return {
    name: "dd-vite",
    sharedDuringBuild: true,
    config: {
      order: "post",
      async handler(config, env) {
        viteCommand = env.command;
        if (options.environment === false) {
          return;
        }
        rootHint = resolve(config.root ?? process.cwd());
        const workers = await resolvedWorkers();
        const entryWorker = workers[0];
        environmentWorkerEntry = entryWorker.entry;
        const frameworkResolveAlias = await frameworkViteResolveAlias(framework, rootHint, viteCommand);
        buildOutputRoot = resolveBuildOutputRoot(config, rootHint);
        buildClientOutputDir = framework
          ? frameworkClientAssetsDir(framework)
          : DEFAULT_CLIENT_OUTPUT_DIR;
        await writeDdEnvTypes(rootHint, workers);
        return {
          builder: {
            sharedPlugins: true,
          },
          ...(env.command === "build" && !framework
            ? {
                build: {
                  outDir: childBuildOutDir(config.build?.outDir, DEFAULT_CLIENT_OUTPUT_DIR),
                },
              }
            : {}),
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
          environments: registeredWorkerEnvironmentOptions(workers, config.build?.outDir),
        };
      },
    },
    configEnvironment(name) {
      if (options.environment === false) {
        return;
      }
      const worker = currentResolvedWorkerByEnvironment(name);
      if (worker) {
        return ddEnvironment({
          ...options,
          ...worker,
          viteEnvironment: worker.viteEnvironment,
          name: worker.runtimeName,
          entry: worker.entry,
          config: worker.config,
          optimizeDepsEntries: worker.entry
            ? [viteRequestForFile(worker.entry, resolveFrameworkRoot())]
            : [],
        });
      }
      const parentWorker = currentResolvedWorkerByChildEnvironment(name);
      if (parentWorker) {
        return ddRunnableEnvironment({
          ...options,
          viteEnvironment: { ...(parentWorker.viteEnvironment ?? {}), name },
          name: parentWorker.runtimeName,
        });
      }
    },
    configResolved(config) {
      resolvedConfig = config;
      if (viteCommand === "build") {
        isolateFrameworkPluginsForWorkerEnvironments(config, resolvedWorkersSnapshot);
      }
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
      installViteModuleMiddleware(viteServer, {
        token: moduleRunnerToken,
      });
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
        resolvedWorkersPromise = undefined;
        resolvedWorkersSnapshot = [];
        deployment = undefined;
        deploymentRuntimeGeneration = undefined;
      }
      if (await shouldInvalidateOnHotUpdate(context, hotReloadMode, effectiveWorkerEntry, options)) {
        await invalidateDeployment();
        scheduleReactRouterRscClientUpdate(context);
      }
    },
    buildApp: {
      order: "post",
      async handler(builder) {
        await buildDdApp(builder);
      },
    },
    async closeBundle() {
      await runtime?.close();
    },
    resolveId(id) {
      if (id === DD_AUXILIARY_WORKERS_MODULE) {
        return DD_AUXILIARY_WORKERS_RESOLVED;
      }
      if (id === DD_NODE_ASYNC_HOOKS_SHIM_MODULE) {
        return DD_NODE_ASYNC_HOOKS_SHIM_RESOLVED;
      }
      if (
        viteCommand === "build" &&
        framework &&
        id === framework.serverBuildModule &&
        currentResolvedWorkerByEnvironment(this.environment?.name)
      ) {
        return frameworkServerEntry(framework, resolveFrameworkRoot());
      }
      if (viteCommand === "serve" && framework?.name === REACT_ROUTER_RSC_FRAMEWORK && id === DD_REACT_ROUTER_RSC_SERVER_MODULE) {
        return DD_REACT_ROUTER_RSC_SERVER_RESOLVED;
      }
    },
    async load(id) {
      if (id === DD_AUXILIARY_WORKERS_RESOLVED) {
        return auxiliaryWorkersModuleSource();
      }
      if (id === DD_NODE_ASYNC_HOOKS_SHIM_RESOLVED) {
        return DD_NODE_ASYNC_HOOKS_SHIM_SOURCE;
      }
      if (
        viteCommand !== "serve" ||
        id !== DD_REACT_ROUTER_RSC_SERVER_RESOLVED ||
        framework?.name !== REACT_ROUTER_RSC_FRAMEWORK
      ) {
        return;
      }
      const root = resolveFrameworkRoot();
      const entry = resolveFrameworkPath(root, framework.rscEntry);
      const specifier = viteRequestForFile(entry, root);
      return `export { default } from ${JSON.stringify(specifier)};\n`;
    },
  };

  async function bundleFrameworkWorkerSource(framework, entry, sourceOptions = {}) {
    if (viteCommand === "serve") {
      await ensureFrameworkDevBuild(framework);
    }
    return bundleWorkerEntry(entry, {
      viteConfig: mergeConfig(
        await frameworkWorkerViteConfig(framework),
        options.viteConfig ?? {},
      ),
      target: options.target,
      sourcemap: options.sourcemap ?? false,
      minify: workerBundleMinify(options.minify, sourceOptions.production === true),
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
      define: workerRuntimeDefines("production"),
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

async function frameworkViteResolveAlias(framework, root, command) {
  if (!framework) {
    return undefined;
  }
  const alias = {};
  if (framework.asyncHooksShim !== false) {
    const asyncHooksShim = await resolveAsyncHooksShim(framework, root);
    if (asyncHooksShim) {
      alias["node:async_hooks"] = asyncHooksShim;
      alias.async_hooks = asyncHooksShim;
    }
  }
  return Object.keys(alias).length > 0 ? alias : undefined;
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

function isolateFrameworkPluginsForWorkerEnvironments(config, workers) {
  const workerEnvironmentNames = new Set(workers.map((worker) => worker.environmentName));
  if (workerEnvironmentNames.size === 0) {
    return;
  }
  for (const plugin of config.plugins ?? []) {
    if (!shouldSkipPluginInDdWorkerEnvironment(plugin)) {
      continue;
    }
    if (plugin.__ddViteWorkerIsolation === true) {
      continue;
    }
    const applyToEnvironment = plugin.applyToEnvironment?.bind(plugin);
    plugin.applyToEnvironment = async (environment) => {
      if (workerEnvironmentNames.has(environment.name)) {
        return false;
      }
      return applyToEnvironment ? applyToEnvironment(environment) : true;
    };
    Object.defineProperty(plugin, "__ddViteWorkerIsolation", {
      value: true,
      enumerable: false,
    });
  }
}

function shouldSkipPluginInDdWorkerEnvironment(plugin) {
  const name = String(plugin?.name ?? "");
  return name === "react-router" || name.startsWith("react-router:");
}

function mergeFrameworkViteEnvironment(framework, viteEnvironment) {
  return viteEnvironment;
}

function workerEnvironmentName(name) {
  return sanitizeViteEnvironmentIdentifier(name) || "dd";
}

function defaultEntryWorkerEnvironmentName(framework, command, workerName) {
  if (command === "serve" && framework?.name === REACT_ROUTER_FRAMEWORK) {
    return "ssr";
  }
  if (command === "serve" && framework?.name === REACT_ROUTER_RSC_FRAMEWORK) {
    return "rsc";
  }
  return workerEnvironmentName(workerName);
}

function defaultEntryWorkerChildEnvironmentNames(framework, command) {
  if (command === "serve" && framework?.name === REACT_ROUTER_RSC_FRAMEWORK) {
    return ["ssr"];
  }
  return [];
}

function workerOutputName(name) {
  return sanitizeWorkerIdentifier(name) || "worker";
}

function sanitizeViteEnvironmentIdentifier(name) {
  return String(name ?? "")
    .trim()
    .replace(/[^a-zA-Z0-9_$]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .toLowerCase();
}

function sanitizeWorkerIdentifier(name) {
  return String(name ?? "")
    .trim()
    .replace(/[^a-zA-Z0-9_-]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .toLowerCase();
}

function validateResolvedWorkers(workers) {
  const names = new Set();
  const environments = new Set();
  const outputs = new Set();
  for (const worker of workers) {
    for (const [set, value, label] of [
      [names, worker.name, "worker name"],
      [environments, worker.environmentName, "Vite environment name"],
      [outputs, worker.outputName, "worker output directory"],
    ]) {
      if (set.has(value)) {
        throw new Error(`duplicate ddVitePlugin ${label}: ${value}`);
      }
      set.add(value);
    }
    for (const childName of worker.childEnvironmentNames) {
      if (environments.has(childName)) {
        throw new Error(`duplicate ddVitePlugin Vite environment name: ${childName}`);
      }
      environments.add(childName);
    }
  }
}

function normalizeAuxiliaryWorkers(value) {
  if (value == null) {
    return [];
  }
  if (!Array.isArray(value)) {
    throw new Error("ddVitePlugin auxiliaryWorkers must be an array");
  }
  const seen = new Set();
  return value.map((entry, index) => {
    const name = nonEmptyString(entry?.name);
    if (!name) {
      throw new Error(`ddVitePlugin auxiliaryWorkers[${index}].name must be a non-empty string`);
    }
    if (seen.has(name)) {
      throw new Error(`duplicate ddVitePlugin auxiliary worker name: ${name}`);
    }
    seen.add(name);
    const kind = normalizeAuxiliaryWorkerKind(entry?.kind);
    return {
      ...entry,
      kind,
      name,
      binding: nonEmptyString(entry?.binding) ?? auxiliaryBindingName(name),
      id: nonEmptyString(entry?.id) ?? name,
      service: kind === "service"
        ? nonEmptyString(entry?.service) ?? nonEmptyString(entry?.workerName) ?? name
        : undefined,
      deployment: entry?.deployment && typeof entry.deployment === "object"
        ? cloneJson(entry.deployment)
        : undefined,
    };
  });
}

function normalizeAuxiliaryWorkerKind(value) {
  if (value == null) {
    return "dynamic";
  }
  const kind = String(value).trim().toLowerCase();
  if (kind !== "dynamic" && kind !== "service") {
    throw new Error(`ddVitePlugin auxiliary worker kind is invalid: ${kind}`);
  }
  return kind;
}

function auxiliaryBindingName(name) {
  const binding = String(name).trim().replace(/[^a-zA-Z0-9]+/g, "_").replace(/^_+|_+$/g, "").toUpperCase();
  return binding ? `${binding}_WORKER` : "AUXILIARY_WORKER";
}

function withAuxiliaryRuntimeBindings(config, auxiliaryWorkers) {
  if (auxiliaryWorkers.length === 0) {
    return config;
  }
  const runtimeConfig = cloneJson(config);
  const bindings = Array.isArray(runtimeConfig.bindings) ? [...runtimeConfig.bindings] : [];
  for (const worker of auxiliaryWorkers) {
    if (worker.kind === "service") {
      if (!bindings.some((binding) =>
        String(binding?.type ?? "").toLowerCase() === "service" && binding?.binding === worker.binding
      )) {
        bindings.push({ type: "service", binding: worker.binding, service: worker.service });
      }
    } else if (!bindings.some((binding) =>
      String(binding?.type ?? "").toLowerCase() === "dynamic" && binding?.binding === worker.binding
    )) {
      bindings.push({ type: "dynamic", binding: worker.binding });
    }
  }
  runtimeConfig.bindings = bindings;
  return runtimeConfig;
}

function auxiliaryWorkerServiceConfig(worker) {
  const config = cloneJson(worker.config);
  if (config.public == null) {
    config.public = false;
  }
  return config;
}

function ddAuxiliaryWorkersVirtualPlugin(loadSource) {
  return {
    name: "dd-auxiliary-workers",
    resolveId(id) {
      if (id === DD_AUXILIARY_WORKERS_MODULE) {
        return DD_AUXILIARY_WORKERS_RESOLVED;
      }
    },
    load(id) {
      if (id === DD_AUXILIARY_WORKERS_RESOLVED) {
        return loadSource();
      }
    },
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

function viteModuleRunnerWorkerSource(viteServer, entry, options) {
  const root = resolve(options.root ?? viteServer.config.root);
  patchViteResolvedUrls(viteServer);
  const endpointBase = viteDevServerActualUrl(viteServer) ?? viteDevServerUrlCandidates(viteServer)[0] ?? "http://127.0.0.1:5173/";
  const payload = {
    entryEnvironment: options.environmentName,
    entryId: viteRequestForFile(entry, root),
    moduleEndpoint: new URL(DD_VITE_MODULE_ENDPOINT_PATH, endpointBase).href,
    invalidatePath: DD_VITE_MODULE_INVALIDATE_PATH,
    devFetchOrigins: viteDevServerOrigins(viteServer),
    token: options.token,
  };
  return `${VITE_MODULE_RUNNER_WORKER_BOOTSTRAP}
const __ddViteConfig = ${JSON.stringify(payload)};
const __ddViteEntry = await __ddViteImportDev(__ddViteConfig);
export default __ddViteEntry;
`;
}

function installViteModuleMiddleware(viteServer, options) {
  viteServer.middlewares.use(DD_VITE_MODULE_ENDPOINT_PATH, async (req, res, next) => {
    try {
      if ((req.method ?? "GET").toUpperCase() !== "POST") {
        res.statusCode = 405;
        res.end("method not allowed");
        return;
      }
      if (headerValue(req.headers[DD_VITE_MODULE_HEADER]) !== options.token) {
        res.statusCode = 403;
        res.end("forbidden");
        return;
      }
      const body = JSON.parse((await readIncomingBody(req)).toString("utf8") || "{}");
      const environmentName = nonEmptyString(body.environmentName);
      const id = nonEmptyString(body.id);
      const importer = nonEmptyString(body.importer);
      if (!environmentName || !id) {
        res.statusCode = 400;
        res.end("environmentName and id are required");
        return;
      }
      const environment = viteServer.environments[environmentName];
      if (!environment) {
        res.statusCode = 404;
        res.end(`unknown Vite environment ${environmentName}`);
        return;
      }
      await environment.depsOptimizer?.init?.();
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
      if (record.externalize && !isSupportedViteRunnerExternal(record.externalize)) {
        throw new Error(
          `ddVitePlugin cannot run external module ${record.externalize} in the dd runtime` +
            ` while loading ${JSON.stringify(record.id)}` +
            (importer ? ` imported from ${JSON.stringify(importer)}` : "") +
            ". " +
            "Make sure the dd Vite environment uses resolve.noExternal/optimizeDeps so dependencies are transformed.",
        );
      }
      res.statusCode = 200;
      res.setHeader("content-type", "application/json; charset=utf-8");
      res.end(JSON.stringify(record));
    } catch (error) {
      next(error);
    }
  });
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

function workerRuntimeDefines(nodeEnv) {
  return {
    "process.env.NODE_ENV": JSON.stringify(nodeEnv),
  };
}

function workerRuntimeGlobalsBanner(nodeEnv) {
  return [
    "globalThis.process ??= { env: {}, emit() { return false; } };",
    "globalThis.process.env ??= {};",
    `globalThis.process.env.NODE_ENV ??= ${JSON.stringify(nodeEnv)};`,
    "globalThis.process.emit ??= function () { return false; };",
  ].join("\n");
}

function isSupportedViteRunnerExternal(value) {
  const external = String(value);
  return external.startsWith("data:") || external === "node:module";
}

function viteRunnerKey(environmentName, id) {
  return `${environmentName}\0${id}`;
}

const VITE_MODULE_RUNNER_WORKER_BOOTSTRAP = `
const __ddViteAsyncFunction = Object.getPrototypeOf(async function () {}).constructor;

async function __ddViteImportDev(config) {
  const records = new Map();
  const aliases = new Map();
  const pending = new Map();
  const cache = new Map();
  const devFetchOrigins = new Set(config.devFetchOrigins ?? []);
  const token = String(config.token ?? "");
  const moduleEndpoint = String(config.moduleEndpoint);
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
  installInternalRecords(config.entryEnvironment, {});
  const nodeEnv = "development";
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
      } catch {}
      return policyFetch(input, init);
    };
    Object.defineProperty(bridge, "__ddViteDevFetchBridge", { value: true });
    globalThis.fetch = bridge;
  }

  function installInternalRecords(environmentName, importMetaEnv) {
    records.set(emptyVirtualKey, {
      key: emptyVirtualKey,
      environmentName,
      id: emptyVirtualKey,
      url: emptyVirtualKey,
      code: "",
      importMetaEnv,
    });
    records.set(emptyCssVirtualKey, {
      key: emptyCssVirtualKey,
      environmentName,
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

  function addAliases(record, importer, requestedId) {
    const values = [
      record.id,
      record.url,
      record.file,
      record.requestedId,
      requestedId,
      clean(record.id),
      clean(record.url),
      clean(record.file),
      clean(record.requestedId),
      clean(requestedId),
    ];
    for (const value of values) {
      if (value) {
        aliases.set(record.environmentName + "\\0" + value + "\\0" + (importer ?? ""), record.key);
        aliases.set(record.environmentName + "\\0" + value, record.key);
      }
    }
  }

  function fallbackRecord(environmentName, id) {
    if (String(id).includes("virtual:vite-rsc/css?")) {
      return records.get(emptyCssVirtualKey);
    }
    if (String(id).includes("virtual:vite-rsc/reference-validation?")) {
      return records.get(emptyVirtualKey);
    }
    return undefined;
  }

  async function fetchRecord(environmentName, id, importerRecord) {
    const importer = importerRecord?.file || importerRecord?.id || importerRecord?.url;
    const raw = String(id);
    const aliasKey = environmentName + "\\0" + raw + "\\0" + (importer ?? "");
    const alias = aliases.get(aliasKey) ?? aliases.get(environmentName + "\\0" + raw);
    if (alias && records.has(alias)) {
      return records.get(alias);
    }
    const fallback = fallbackRecord(environmentName, raw);
    if (fallback) {
      return fallback;
    }
    const pendingKey = aliasKey;
    if (pending.has(pendingKey)) {
      return pending.get(pendingKey);
    }
    const promise = (async () => {
      const response = await hostFetch(moduleEndpoint, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "x-dd-vite-module-token": token,
        },
        body: JSON.stringify({ environmentName, id: raw, importer }),
      });
      if (!response.ok) {
        throw new Error("dd Vite dev runner failed to fetch " + raw + ": " + response.status + " " + await response.text());
      }
      const record = await response.json();
      records.set(record.key, record);
      addAliases(record, importer, raw);
      installInternalRecords(record.environmentName, record.importMetaEnv ?? {});
      return record;
    })();
    pending.set(pendingKey, promise);
    try {
      return await promise;
    } finally {
      pending.delete(pendingKey);
    }
  }

  function hostFetch(input, init) {
    const rawHostFetch = globalThis.__dd_raw_host_fetch ?? globalThis.__dd_deno_runtime?.fetch ?? globalThis.fetch;
    return rawHostFetch(input, init);
  }

  function resolveKey(environmentName, id, importerRecord) {
    const importer = importerRecord?.file || importerRecord?.id || importerRecord?.url;
    const raw = String(id);
    const alias = aliases.get(environmentName + "\\0" + raw + "\\0" + (importer ?? "")) ??
      aliases.get(environmentName + "\\0" + raw);
    if (alias && records.has(alias)) {
      return alias;
    }
    const resolved = importerRecord ? resolveRelative(importerRecord.url || importerRecord.id, raw) : raw;
    const resolvedAlias = aliases.get(environmentName + "\\0" + resolved + "\\0" + (importer ?? "")) ??
      aliases.get(environmentName + "\\0" + resolved);
    if (resolvedAlias && records.has(resolvedAlias)) {
      return resolvedAlias;
    }
    const fallback = fallbackRecord(environmentName, raw);
    if (fallback) {
      return fallback.key;
    }
    return undefined;
  }

  async function load(environmentName, id, importerRecord) {
    let key = resolveKey(environmentName, id, importerRecord);
    let record = key ? records.get(key) : undefined;
    if (!record) {
      record = await fetchRecord(environmentName, id, importerRecord);
      key = record.key;
    }
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

    const request = (specifier) => load(record.environmentName, String(specifier), record);
    const dynamicRequest = (specifier) => load(record.environmentName, String(specifier), record);
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
    const entry = await load(config.entryEnvironment, config.entryId, undefined);
    const worker = entry.default ?? entry;
    if (!worker || typeof worker.fetch !== "function") {
      throw new Error("dd Vite module runner entry did not export a Worker fetch handler");
    }
    activeWorker = worker;
    return worker;
  }

  async function invalidateModules() {
    records.clear();
    aliases.clear();
    cache.clear();
    await activateWorker();
  }

  function isInvalidateRequest(request) {
    if (!(request instanceof Request)) {
      return false;
    }
    if (request.headers.get("x-dd-vite-module-token") !== token) {
      return false;
    }
    try {
      return new URL(request.url).pathname === config.invalidatePath;
    } catch {
      return false;
    }
  }

  return new Proxy({}, {
    get(_target, property) {
      if (property === "fetch") {
        return async (request, ...args) => {
          installDevFetchBridge();
          if (isInvalidateRequest(request)) {
            await invalidateModules();
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
    } catch {}
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
    const host = viteDevServerAddressHost(address.address);
    return `${protocol}://${host}:${address.port}/`;
  }
  return undefined;
}

function viteDevServerAddressHost(address) {
  const value = String(address ?? "");
  if (!value || value === "::" || value === "0.0.0.0") {
    return "127.0.0.1";
  }
  if (value.includes(":")) {
    return `[${value}]`;
  }
  return value;
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

async function waitForNodeResponseDrain(res) {
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

function isViteSourcePath(pathname) {
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

function isViteSourceNamespacePath(pathname) {
  return VITE_SOURCE_PREFIXES.some((prefix) => pathname.startsWith(prefix));
}

function isViteAnyMethodBypassPath(pathname) {
  return VITE_ANY_METHOD_BYPASS_PATHS.includes(pathname);
}

function cleanUrlPathname(pathname) {
  try {
    return decodeURIComponent(pathname);
  } catch {
    return pathname;
  }
}

function normalizeViteBase(value) {
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

function resolveBuildOutputRoot(config, root) {
  const configured = config?.build?.outDir ?? "dist";
  return isAbsolute(configured) ? configured : resolve(root ?? config?.root ?? process.cwd(), configured);
}

function childBuildOutDir(baseOutDir, child) {
  const base = baseOutDir ?? "dist";
  if (isAbsolute(base)) {
    return join(base, child);
  }
  return joinConfigRelativePath(normalizeConfigRelativePath(base, "build.outDir"), child);
}

function relativeDeploymentAssetsDir(workerOutputName, assetsOutputName) {
  const from = workerOutputName;
  const to = assetsOutputName;
  const path = relative(from, to).replaceAll("\\", "/");
  return normalizeDeploymentAssetsDir(path || ".", "deploymentConfig.assetsDir");
}

async function writeDdEnvTypes(root, workers) {
  const lines = [
    "/* Generated by @mewhhaha/vite-plugin-dd. */",
    "export interface DdServiceBinding {",
    "  fetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response>;",
    "}",
    "",
    "export type DdStateBinding = unknown;",
    "",
    "export interface DdWorkerEnvMap {",
  ];
  for (const worker of workers) {
    lines.push(`  ${JSON.stringify(worker.name)}: {`);
    for (const binding of workerEnvBindings(worker)) {
      lines.push(`    ${JSON.stringify(binding.name)}: ${binding.type};`);
    }
    lines.push("  };");
  }
  lines.push(
    "}",
    "",
    "export type DdWorkerEnv<Name extends keyof DdWorkerEnvMap> = DdWorkerEnvMap[Name];",
    "",
  );
  await writeFile(join(root, DEFAULT_ENV_TYPES_FILE), `${lines.join("\n")}`);
}

function workerEnvBindings(worker) {
  const bindings = [];
  for (const binding of Array.isArray(worker.config?.bindings) ? worker.config.bindings : []) {
    const name = nonEmptyString(binding?.binding);
    if (!name) {
      continue;
    }
    const type = String(binding?.type ?? "").toLowerCase() === "service"
      ? "DdServiceBinding"
      : "DdStateBinding";
    bindings.push({ name, type });
  }
  return bindings;
}

async function writeOutputFile(outDir, file, contents) {
  const path = join(outDir, file);
  await mkdir(dirname(path), { recursive: true });
  await writeFile(path, contents);
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
  if (assetsDir !== "." && (await directoryExists(join(assetsOutDir, assetsDir)))) {
    assetPaths.add(`/${assetsDir}/*`);
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
    normalizeDeploymentAssetsDir(
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
  extraAssetExcludes = [],
  serverModules,
  staticRoutes,
}) {
  const deployment = {};
  const runtimeConfig = withAuxiliaryRuntimeBindings(
    options.config ?? base.config ?? topLevelRuntimeConfig(base) ?? { public: true },
    normalizeAuxiliaryWorkers(options.auxiliaryWorkers),
  );

  deployment.name = options.name ?? base.name ?? workerName;
  deployment.entrypoint = workerFile;
  const baseUrl = nonEmptyString(base.base_url) ?? nonEmptyString(base.baseUrl);
  if (baseUrl) {
    deployment.base_url = baseUrl;
  }
  if (base.temporary === true) {
    deployment.temporary = true;
  }
  if (assetsDir === false) {
    delete deployment.assets_dir;
  } else {
    deployment.assets_dir = normalizeDeploymentAssetsDir(
      assetsDir ?? DEFAULT_DEPLOYMENT_ASSETS_DIR,
      "deploymentConfig.assetsDir",
    );
  }
  deployment.asset_excludes = uniquePaths([
    ...arrayOfStrings(base.asset_excludes),
    ...arrayOfStrings(assetExcludes),
    ...arrayOfStrings(extraAssetExcludes),
    ...(staticRoutes ? [DEFAULT_STATIC_ROUTES_FILE] : []),
    workerFile,
    configFile,
  ]);
  const serverModuleConfig = [
    ...arrayOfObjects(base.server_modules),
    ...arrayOfObjects(base.serverModules),
    ...arrayOfObjects(serverModules),
  ];
  if (serverModuleConfig.length > 0) {
    deployment.server_modules = serverModuleConfig;
  }
  deployment.config = runtimeConfig;
  return deployment;
}

function buildAuxiliaryServiceDeploymentConfig({
  base,
  worker,
  workerFile,
}) {
  const deployment = {
    name: worker.kind === "service" ? worker.service : worker.runtimeName,
    entrypoint: workerFile,
    config: worker.kind === "service" ? auxiliaryWorkerServiceConfig(worker) : cloneJson(worker.config),
  };
  const baseUrl = nonEmptyString(base.base_url) ?? nonEmptyString(base.baseUrl);
  if (baseUrl) {
    deployment.base_url = baseUrl;
  }
  if (base.temporary === true) {
    deployment.temporary = true;
  }
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

function arrayOfObjects(value) {
  if (!Array.isArray(value)) {
    return [];
  }
  return value
    .filter((entry) => entry && typeof entry === "object")
    .map((entry) => cloneJson(entry));
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

function normalizeDeploymentAssetsDir(value, label) {
  const normalized = String(value).replaceAll("\\", "/").replace(/^\/+/, "");
  if (normalized === ".") {
    return ".";
  }
  const parts = normalized.split("/");
  if (
    normalized.length === 0 ||
    parts.some((part) => part.length === 0 || part === ".") ||
    isAbsolute(String(value))
  ) {
    throw new Error(`${label} must be a relative asset directory`);
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
