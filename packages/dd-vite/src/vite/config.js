import { mkdir, readFile, readdir, realpath, stat, writeFile } from "node:fs/promises";
import { dirname, isAbsolute, join, relative, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";

const CONFIG_SCHEMA_VERSION = 1;
const DEFAULT_SOURCE_CONFIG_FILE = "dd.json";
const DEFAULT_DEPLOYMENT_CONFIG_FILE = "dd.deploy.json";
const DEFAULT_DEPLOYMENT_WORKER_FILE = "worker.js";
const DEFAULT_DEPLOYMENT_ASSETS_DIR = ".";
const DEFAULT_WORKERS_MANIFEST_FILE = "dd.workers.json";
const DEFAULT_ENV_TYPES_FILE = "dd-env.d.ts";
const DEFAULT_ASSET_HEADERS_FILE = "_headers";
const DEFAULT_STATIC_ROUTES_FILE = "_routes.json";
const FINGERPRINTED_ASSET_CACHE_CONTROL = "public, max-age=31536000, immutable";

function joinConfigRelativePath(...parts) {
  return parts.filter((part) => String(part).length > 0).join("/");
}

function cloneAuxiliaryConfig(value) {
  return value == null ? {} : JSON.parse(JSON.stringify(value));
}

function normalizeAuxiliaryWorkerKind(value) {
  if (value == null) return "dynamic";
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

function normalizeAuxiliaryWorkers(value) {
  if (value == null) return [];
  if (!Array.isArray(value)) throw new Error("ddVitePlugin auxiliaryWorkers must be an array");
  const seen = new Set();
  return value.map((entry, index) => {
    const name = nonEmptyString(entry?.name);
    if (!name) throw new Error(`ddVitePlugin auxiliaryWorkers[${index}].name must be a non-empty string`);
    if (seen.has(name)) throw new Error(`duplicate ddVitePlugin auxiliary worker name: ${name}`);
    seen.add(name);
    const kind = normalizeAuxiliaryWorkerKind(entry?.kind);
    return { ...entry, kind, name, binding: nonEmptyString(entry?.binding) ?? auxiliaryBindingName(name), id: nonEmptyString(entry?.id) ?? name, service: kind === "service" ? nonEmptyString(entry?.service) ?? nonEmptyString(entry?.workerName) ?? name : undefined, deployment: entry?.deployment && typeof entry.deployment === "object" ? cloneAuxiliaryConfig(entry.deployment) : undefined };
  });
}

function withAuxiliaryRuntimeBindings(config, auxiliaryWorkers) {
  if (auxiliaryWorkers.length === 0) return config;
  const runtimeConfig = cloneAuxiliaryConfig(config);
  const bindings = Array.isArray(runtimeConfig.bindings) ? [...runtimeConfig.bindings] : [];
  for (const worker of auxiliaryWorkers) {
    const type = worker.kind === "service" ? "service" : "dynamic";
    if (!bindings.some((binding) => String(binding?.type ?? "").toLowerCase() === type && binding?.binding === worker.binding)) {
      bindings.push(type === "service" ? { type, binding: worker.binding, service: worker.service } : { type, binding: worker.binding });
    }
  }
  runtimeConfig.bindings = bindings;
  return runtimeConfig;
}

function auxiliaryWorkerServiceConfig(worker) {
  const config = cloneAuxiliaryConfig(worker.config);
  if (config.public == null) config.public = false;
  return config;
}

export function normalizeDeploymentConfigOptions(value) {
  if (value === false) {
    return { enabled: false };
  }
  return { enabled: true, ...(value ?? {}) };
}

export function resolveViteOutDir(config) {
  const root = config?.root ?? process.cwd();
  const outDir = config?.build?.outDir ?? "dist";
  return isAbsolute(outDir) ? outDir : resolve(root, outDir);
}

export function resolveBuildOutputRoot(config, root) {
  const configured = config?.build?.outDir ?? "dist";
  return isAbsolute(configured) ? configured : resolve(root ?? config?.root ?? process.cwd(), configured);
}

export async function assertSafeBuildOutputRoot(projectRoot, outRoot) {
  const resolvedProject = resolve(projectRoot);
  const resolvedOutput = resolve(outRoot);
  if (resolvedOutput === resolvedProject || !resolvedOutput.startsWith(`${resolvedProject}${sep}`)) {
    throw new Error(`ddVitePlugin refuses to empty build.outDir outside the project root: ${resolvedOutput}`);
  }

  const canonicalProject = await realpath(resolvedProject);
  let existingAncestor = dirname(resolvedOutput);
  while (true) {
    try {
      existingAncestor = await realpath(existingAncestor);
      break;
    } catch (error) {
      if (!isMissingFileError(error)) {
        throw error;
      }
      const parent = dirname(existingAncestor);
      if (parent === existingAncestor) {
        throw new Error(`ddVitePlugin could not validate build.outDir: ${resolvedOutput}`);
      }
      existingAncestor = parent;
    }
  }
  if (existingAncestor !== canonicalProject && !existingAncestor.startsWith(`${canonicalProject}${sep}`)) {
    throw new Error(`ddVitePlugin refuses to empty build.outDir through a path outside the project root: ${resolvedOutput}`);
  }
}

export function childBuildOutDir(baseOutDir, child) {
  const base = baseOutDir ?? "dist";
  if (isAbsolute(base)) {
    return join(base, child);
  }
  return joinConfigRelativePath(normalizeConfigRelativePath(base, "build.outDir"), child);
}

export function relativeDeploymentAssetsDir(workerOutputName, assetsOutputName) {
  const from = workerOutputName;
  const to = assetsOutputName;
  const path = relative(from, to).replaceAll("\\", "/");
  return normalizeDeploymentAssetsDir(path || ".", "deploymentConfig.assetsDir");
}

export async function writeDdEnvTypes(root, workers) {
  const typeNames = [...new Set(
    workers.flatMap((worker) => workerEnvBindings(worker).map((binding) => binding.type)),
  )].sort();
  const lines = ["/* Generated by @mewhhaha/vite-plugin-dd. */"];
  if (typeNames.length > 0) {
    lines.push(
      "import type {",
      ...typeNames.map((name) => `  ${name},`),
      "} from \"@mewhhaha/vite-plugin-dd\";",
      "",
    );
  }
  lines.push("export interface DdWorkerEnvMap {");
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

export function workerEnvBindings(worker) {
  const bindings = [];
  for (const binding of Array.isArray(worker.config?.bindings) ? worker.config.bindings : []) {
    const name = nonEmptyString(binding?.binding);
    if (!name) {
      continue;
    }
    const type = {
      kv: "DdKvNamespace",
      memory: "DdMemoryNamespace",
      dynamic: "DdDynamicWorkerNamespace",
      service: "DdServiceBinding",
    }[String(binding?.type ?? "").toLowerCase()];
    if (!type) {
      continue;
    }
    bindings.push({ name, type });
  }
  return bindings;
}

export async function writeOutputFile(outDir, file, contents) {
  const path = join(outDir, file);
  await mkdir(dirname(path), { recursive: true });
  await writeFile(path, contents);
}

export async function writeGeneratedStaticRoutes(outDir, deploymentConfig) {
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

export async function writeGeneratedAssetPolicy(outDir, config, deploymentAssetsDir) {
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

export function deploymentAssetsOutputDir(outDir, assetsDir) {
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

export async function findGeneratedAssetPaths(outDir, relativeDir = "") {
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

export async function directoryExists(path) {
  try {
    return (await stat(path)).isDirectory();
  } catch (error) {
    if (isMissingFileError(error)) {
      return false;
    }
    throw error;
  }
}

export function hasHeaderPathRule(contents, pathRule) {
  return contents
    .split(/\r?\n/)
    .some((line) => line.trimStart() === line && line.trim() === pathRule);
}

export async function buildGeneratedDeploymentConfig({
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
  const deployment = { schema_version: CONFIG_SCHEMA_VERSION };
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

export function buildAuxiliaryServiceDeploymentConfig({
  base,
  worker,
  workerFile,
}) {
  const deployment = {
    schema_version: CONFIG_SCHEMA_VERSION,
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

export async function loadSourceDeploymentConfig(deploymentConfig, root) {
  const explicitInput = deploymentConfig.enabled ? deploymentConfig.input : undefined;
  if (explicitInput !== undefined) {
    return loadDeploymentConfigInput(explicitInput, root);
  }
  const packageRoot = await findPackageRoot(root);
  const path = join(packageRoot, DEFAULT_SOURCE_CONFIG_FILE);
  try {
    const source = await readFile(path, "utf8");
    return {
      config: parseDdConfig(source, path),
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

export async function loadDeploymentConfigInput(input, root) {
  if (input == null) {
    return { config: {}, dir: root, path: undefined };
  }
  if (typeof input === "function") {
    return { config: cloneJson(await input()), dir: root, path: undefined };
  }
  if (typeof input === "string" || input instanceof URL) {
    const path = normalizeFile(input);
    const source = await readFile(path, "utf8");
    return {
      config: parseDdConfig(source, path),
      dir: dirname(path),
      path,
    };
  }
  return { config: cloneJson(input), dir: root, path: undefined };
}

export function parseDdConfig(source, path) {
  let value;
  try {
    value = JSON.parse(source);
  } catch (error) {
    throw new Error(`Invalid dd config ${path}: ${error.message}`, { cause: error });
  }
  validateDdConfig(value, path);
  return value;
}

export function validateDdConfig(value, path = DEFAULT_SOURCE_CONFIG_FILE) {
  assertConfigObject(value, path);
  if (value.schema_version !== CONFIG_SCHEMA_VERSION) {
    throw new Error(
      `Invalid dd config ${path}: schema_version must equal ${CONFIG_SCHEMA_VERSION}`,
    );
  }
  rejectUnknownConfigFields(value, new Set([
    "$schema",
    "schema_version",
    "name",
    "entrypoint",
    "base_url",
    "baseUrl",
    "assets_dir",
    "temporary",
    "asset_excludes",
    "server_modules",
    "config",
    "public",
    "cache",
    "bindings",
    "internal",
  ]), path);
  if (value.config !== undefined) {
    validateRuntimeConfig(value.config, `${path}.config`);
  }
  const topLevelRuntime = {};
  for (const key of ["public", "cache", "bindings", "internal"]) {
    if (Object.hasOwn(value, key)) {
      topLevelRuntime[key] = value[key];
    }
  }
  if (Object.keys(topLevelRuntime).length > 0) {
    validateRuntimeConfig(topLevelRuntime, path);
  }
  if (value.server_modules !== undefined) {
    if (!Array.isArray(value.server_modules)) {
      throw new Error(`Invalid dd config ${path}: server_modules must be an array`);
    }
    value.server_modules.forEach((module, index) => {
      const label = `${path}.server_modules[${index}]`;
      assertConfigObject(module, label);
      rejectUnknownConfigFields(module, new Set(["type", "kind", "path", "file"]), label);
    });
  }
}

export function validateRuntimeConfig(value, path) {
  assertConfigObject(value, path);
  rejectUnknownConfigFields(value, new Set(["public", "cache", "bindings", "internal"]), path);
  if (value.cache !== undefined) {
    assertConfigObject(value.cache, `${path}.cache`);
    rejectUnknownConfigFields(value.cache, new Set(["enabled"]), `${path}.cache`);
  }
  if (value.internal !== undefined) {
    assertConfigObject(value.internal, `${path}.internal`);
    rejectUnknownConfigFields(value.internal, new Set(["trace"]), `${path}.internal`);
    if (value.internal.trace != null) {
      assertConfigObject(value.internal.trace, `${path}.internal.trace`);
      rejectUnknownConfigFields(
        value.internal.trace,
        new Set(["worker", "path"]),
        `${path}.internal.trace`,
      );
    }
  }
  if (value.bindings !== undefined) {
    if (!Array.isArray(value.bindings)) {
      throw new Error(`Invalid dd config ${path}: bindings must be an array`);
    }
    value.bindings.forEach((binding, index) => {
      const label = `${path}.bindings[${index}]`;
      assertConfigObject(binding, label);
      const fields = binding.type === "service"
        ? new Set(["type", "binding", "service"])
        : new Set(["type", "binding"]);
      rejectUnknownConfigFields(binding, fields, label);
      if (!["kv", "memory", "dynamic", "service"].includes(binding.type)) {
        throw new Error(`Invalid dd config ${label}: unsupported binding type ${binding.type}`);
      }
    });
  }
}

export function assertConfigObject(value, path) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new Error(`Invalid dd config ${path}: expected an object`);
  }
}

export function rejectUnknownConfigFields(value, allowed, path) {
  const unknown = Object.keys(value).find((key) => !allowed.has(key));
  if (unknown !== undefined) {
    throw new Error(`Invalid dd config ${path}: unknown field ${unknown}`);
  }
}

export async function findPackageRoot(start) {
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

export function isMissingFileError(error) {
  return error?.code === "ENOENT" || error?.code === "ENOTDIR";
}

export function topLevelRuntimeConfig(config) {
  const runtimeConfig = {};
  let found = false;
  for (const key of ["public", "cache", "bindings", "internal"]) {
    if (Object.hasOwn(config, key)) {
      runtimeConfig[key] = config[key];
      found = true;
    }
  }
  return found ? runtimeConfig : undefined;
}

export function arrayOfStrings(value) {
  if (!Array.isArray(value)) {
    return [];
  }
  return value.map((entry) => String(entry));
}

export function arrayOfObjects(value) {
  if (!Array.isArray(value)) {
    return [];
  }
  return value
    .filter((entry) => entry && typeof entry === "object")
    .map((entry) => cloneJson(entry));
}

export function uniqueStrings(values) {
  return [...new Set(values)];
}

export function uniquePaths(paths) {
  return [...new Set(paths.map((path) => normalizeConfigRelativePath(path, "asset_excludes")))];
}

export function normalizeOutputPath(value, label) {
  const normalized = normalizeConfigRelativePath(value, label);
  if (normalized === ".") {
    throw new Error(`${label} must be a file path`);
  }
  return normalized;
}

export function normalizeDeploymentAssetsDir(value, label) {
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

export function normalizeConfigRelativePath(value, label) {
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

export function cloneJson(value) {
  return value == null ? {} : JSON.parse(JSON.stringify(value));
}

export function nonEmptyString(value) {
  if (typeof value !== "string") {
    return undefined;
  }
  const trimmed = value.trim();
  return trimmed.length === 0 ? undefined : trimmed;
}
