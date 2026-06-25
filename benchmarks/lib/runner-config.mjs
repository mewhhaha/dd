import { readdir, readFile } from "node:fs/promises";
import { basename, join, resolve } from "node:path";

export const repoRoot = resolve(new URL("../..", import.meta.url).pathname);
export const configsDir = join(repoRoot, "benchmarks", "configs");
export const defaultMaxRuns = 100;

const knownKeyModes = new Set(["same-shard", "cross-shard", "skewed-hotspot"]);
const knownModes = new Set([
  "async-memory",
  "atomic-read-memory",
  "atomic-read-memory-multikey",
  "direct-write-memory",
  "direct-read-memory",
  "direct-read-memory-multikey",
  "direct-write-memory-multikey",
  "atomic-readwrite-memory",
  "atomic-write-memory",
  "direct-write-memory-wide",
  "direct-read-memory-wide",
  "atomic-readwrite-memory-wide",
  "atomic-write-memory-wide",
  "storage-write-memory-wide",
  "realworld-rate-limiter",
  "realworld-multiworker-auth",
  "fast-fetch-instant-text",
  "dynamic-namespace",
  "sync-memory",
]);

export function parseArgs(args, env = process.env) {
  const parsed = {
    samples: positiveInteger(env.BENCH_SAMPLES ?? 3, "BENCH_SAMPLES"),
    output: null,
    configs: [],
    list: false,
    plan: false,
    keepGoing: false,
    allowLargeRun: env.DD_BENCH_ALLOW_LARGE_RUN === "1",
    configsDir: env.DD_BENCH_CONFIGS_DIR ?? configsDir,
    maxRuns:
      env.DD_BENCH_MAX_RUNS == null
        ? defaultMaxRuns
        : positiveInteger(env.DD_BENCH_MAX_RUNS, "DD_BENCH_MAX_RUNS"),
  };
  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index];
    if (arg === "--samples") {
      parsed.samples = positiveInteger(args[++index], "--samples");
    } else if (arg === "--out") {
      parsed.output = args[++index];
    } else if (arg === "--config") {
      parsed.configs.push(args[++index]);
    } else if (arg === "--list") {
      parsed.list = true;
    } else if (arg === "--plan" || arg === "--dry-run") {
      parsed.plan = true;
    } else if (arg === "--max-runs") {
      parsed.maxRuns = positiveInteger(args[++index], "--max-runs");
    } else if (arg === "--allow-large-run") {
      parsed.allowLargeRun = true;
    } else if (arg === "--keep-going") {
      parsed.keepGoing = true;
    } else if (arg === "--help" || arg === "-h") {
      parsed.help = true;
    } else {
      throw new Error(`unknown argument: ${arg}`);
    }
  }
  return parsed;
}

export function positiveInteger(value, name) {
  const number = Number(value);
  if (!Number.isInteger(number) || number < 1) {
    throw new Error(`${name} must be a positive integer`);
  }
  return number;
}

export async function discoverConfigs(
  selected,
  { includeOptIn = false, configsDirectory = configsDir } = {},
) {
  const entries = await readdir(configsDirectory);
  const all = await Promise.all(
    entries
      .filter((entry) => entry.endsWith(".sh"))
      .sort()
      .map(async (entry) => {
        const path = join(configsDirectory, entry);
        const script = await readFile(path, "utf8");
        const env = parseConfigEnv(script);
        return {
          name: entry,
          path,
          script,
          env,
          matrixOptIn: env.DD_BENCH_MATRIX_OPT_IN === "1",
        };
      }),
  );
  if (selected.length > 0) {
    const selectedNames = new Set(selected.map((value) => basename(value)));
    const availableNames = new Set(all.map((config) => config.name));
    const missing = [...selectedNames].filter((name) => !availableNames.has(name));
    if (missing.length > 0) {
      throw new Error(`unknown benchmark config: ${missing.join(", ")}`);
    }
    return {
      configs: all.filter((config) => selectedNames.has(config.name)),
      skippedOptIn: [],
    };
  }
  return {
    configs: all.filter((config) => includeOptIn || !config.matrixOptIn),
    skippedOptIn: all.filter((config) => config.matrixOptIn).map((config) => config.name),
  };
}

export async function expandConfigs(configs, env = process.env) {
  const expanded = [];
  for (const config of configs) {
    for (const variant of expandMatrixVariants(config.env, env)) {
      expanded.push({
        ...config,
        name:
          variant.labels.length === 0
            ? config.name
            : `${config.name}::${variant.labels.join(",")}`,
        envOverrides: variant.envOverrides,
      });
    }
  }
  return expanded;
}

export function expandMatrixVariants(configEnv, processEnv = process.env) {
  const hasMatrixMarker = configEnv.DD_BENCH_MATRIX_OPT_IN === "1";
  const dimensions = [];
  const isolates = uniqueList(
    envList(processEnv.DD_BENCH_MATRIX_ISOLATES ?? configEnv.DD_BENCH_MATRIX_ISOLATES),
  );
  validateMarkedMatrixDimension(hasMatrixMarker, "DD_BENCH_MATRIX_ISOLATES", configEnv, processEnv);
  if (isolates.length > 0) {
    validateNoConflictingOverride(processEnv, "DD_BENCH_MIN_ISOLATES", "DD_BENCH_MATRIX_ISOLATES");
    validateNoConflictingOverride(processEnv, "DD_BENCH_MAX_ISOLATES", "DD_BENCH_MATRIX_ISOLATES");
    validatePositiveIntegerList(isolates, "DD_BENCH_MATRIX_ISOLATES");
    dimensions.push(
      isolates.map((value) => ({
        label: `isolates=${value}`,
        env: {
          DD_BENCH_MIN_ISOLATES: value,
          DD_BENCH_MAX_ISOLATES: value,
        },
      })),
    );
  }

  const memoryShards = uniqueList(
    envList(
      processEnv.DD_BENCH_MATRIX_MEMORY_NAMESPACE_SHARDS ??
        configEnv.DD_BENCH_MATRIX_MEMORY_NAMESPACE_SHARDS,
    ),
  );
  validateMarkedMatrixDimension(
    hasMatrixMarker,
    "DD_BENCH_MATRIX_MEMORY_NAMESPACE_SHARDS",
    configEnv,
    processEnv,
  );
  if (memoryShards.length > 0) {
    validateNoConflictingOverride(
      processEnv,
      "DD_BENCH_MEMORY_NAMESPACE_SHARDS",
      "DD_BENCH_MATRIX_MEMORY_NAMESPACE_SHARDS",
    );
    validatePositiveIntegerList(memoryShards, "DD_BENCH_MATRIX_MEMORY_NAMESPACE_SHARDS");
    dimensions.push(
      memoryShards.map((value) => ({
        label: `shards=${value}`,
        env: { DD_BENCH_MEMORY_NAMESPACE_SHARDS: value },
      })),
    );
  }

  const keyModes = uniqueList(
    envList(processEnv.DD_BENCH_MATRIX_KEY_MODES ?? configEnv.DD_BENCH_MATRIX_KEY_MODES),
  );
  validateMarkedMatrixDimension(hasMatrixMarker, "DD_BENCH_MATRIX_KEY_MODES", configEnv, processEnv);
  if (keyModes.length > 0) {
    validateNoConflictingOverride(
      processEnv,
      "DD_BENCH_MEMORY_KEY_MODE",
      "DD_BENCH_MATRIX_KEY_MODES",
    );
    validateKnownList(keyModes, knownKeyModes, "DD_BENCH_MATRIX_KEY_MODES");
    dimensions.push(
      keyModes.map((value) => ({
        label: `keys=${value}`,
        env: { DD_BENCH_MEMORY_KEY_MODE: value },
      })),
    );
  }

  const modes = uniqueList(
    envList(processEnv.DD_BENCH_MATRIX_MODES ?? configEnv.DD_BENCH_MATRIX_MODES),
  );
  validateMarkedMatrixDimension(hasMatrixMarker, "DD_BENCH_MATRIX_MODES", configEnv, processEnv);
  if (modes.length > 0) {
    validateNoConflictingOverride(processEnv, "DD_BENCH_MODE", "DD_BENCH_MATRIX_MODES");
    validateKnownList(modes, knownModes, "DD_BENCH_MATRIX_MODES");
    dimensions.push(
      modes.map((value) => ({
        label: `mode=${value}`,
        env: { DD_BENCH_MODE: value },
      })),
    );
  }

  if (dimensions.length === 0) {
    return [{ labels: [], envOverrides: {} }];
  }

  let variants = [{ labels: [], envOverrides: {} }];
  for (const dimension of dimensions) {
    const next = [];
    for (const variant of variants) {
      for (const entry of dimension) {
        next.push({
          labels: [...variant.labels, entry.label],
          envOverrides: { ...variant.envOverrides, ...entry.env },
        });
      }
    }
    variants = next;
  }
  return variants;
}

export function planRun(configs, options, outputPath) {
  return {
    variants: configs.length,
    samples: options.samples,
    totalRuns: configs.length * options.samples,
    maxRuns: options.maxRuns,
    outputPath,
    configs,
  };
}

export function validateRunBudget(plan, options) {
  if (options.allowLargeRun || plan.totalRuns <= options.maxRuns) {
    return;
  }
  throw new Error(
    `benchmark plan has ${plan.variants} variants x ${plan.samples} samples = ${plan.totalRuns} runs, exceeding --max-runs ${options.maxRuns}; narrow the matrix or pass --allow-large-run`,
  );
}

export function renderPlan(plan, skippedOptIn = []) {
  const lines = [];
  if (skippedOptIn.length > 0) {
    lines.push(`Skipped opt-in matrix configs: ${skippedOptIn.join(", ")}`);
  }
  lines.push(`Variants: ${plan.variants}`);
  lines.push(`Samples per variant: ${plan.samples}`);
  lines.push(`Total child-process runs: ${plan.totalRuns}`);
  lines.push(`Output: ${plan.outputPath}`);
  for (const config of plan.configs) {
    const overrides = Object.entries(config.envOverrides ?? {})
      .map(([key, value]) => `${key}=${value}`)
      .join(" ");
    lines.push(overrides.length > 0 ? `${config.name} ${overrides}` : config.name);
  }
  return `${lines.join("\n")}\n`;
}

export function parseConfigEnv(script) {
  const env = {};
  for (const line of script.split(/\r?\n/)) {
    const trimmed = line.trim().replace(/\\$/, "").trim();
    const defaultMatch =
      /^([A-Z][A-Z0-9_]*)=(?:"\$\{[A-Z][A-Z0-9_]*:-([^}]*)\}"|\$\{[A-Z][A-Z0-9_]*:-([^}]*)\})$/.exec(
        trimmed,
      );
    if (defaultMatch) {
      env[defaultMatch[1]] = defaultMatch[2] ?? defaultMatch[3] ?? "";
      continue;
    }
    const match = /^([A-Z][A-Z0-9_]*)=(?:"([^"]*)"|'([^']*)'|([^ \t]+))$/.exec(trimmed);
    if (!match) {
      continue;
    }
    env[match[1]] = match[2] ?? match[3] ?? match[4];
  }
  return env;
}

export function envList(value) {
  return (value ?? "")
    .split(/[,\s]+/)
    .map((entry) => entry.trim())
    .filter((entry) => entry.length > 0);
}

function uniqueList(values) {
  return [...new Set(values)];
}

function validatePositiveIntegerList(values, name) {
  if (values.length === 0) {
    throw new Error(`${name} must not be empty`);
  }
  for (const value of values) {
    positiveInteger(value, name);
  }
}

function validateKnownList(values, known, name) {
  if (values.length === 0) {
    throw new Error(`${name} must not be empty`);
  }
  for (const value of values) {
    if (!known.has(value)) {
      throw new Error(`${name} contains unsupported value: ${value}`);
    }
  }
}

function validateMarkedMatrixDimension(hasMatrixMarker, name, configEnv, processEnv) {
  if (!hasMatrixMarker) {
    return;
  }
  const hasConfigDimension = configEnv[name] != null;
  const hasProcessOverride = processEnv[name] != null;
  if (!hasConfigDimension && !hasProcessOverride) {
    return;
  }
  if (envList(processEnv[name] ?? configEnv[name]).length === 0) {
    throw new Error(`${name} must not be empty`);
  }
}

function validateNoConflictingOverride(processEnv, overrideName, matrixName) {
  if (processEnv[overrideName] == null) {
    return;
  }
  throw new Error(`${overrideName} conflicts with ${matrixName}; set only the matrix dimension`);
}
