#!/usr/bin/env node
import { spawn, spawnSync } from "node:child_process";
import { cpus } from "node:os";
import { mkdir, readdir, readFile, writeFile } from "node:fs/promises";
import { basename, dirname, join, resolve } from "node:path";

const repoRoot = resolve(new URL("..", import.meta.url).pathname);
const configsDir = join(repoRoot, "benchmarks", "configs");

const options = parseArgs(process.argv.slice(2));
const configs = await expandConfigs(await discoverConfigs(options.configs));

if (options.list) {
  for (const config of configs) {
    console.log(config.name);
  }
  process.exit(0);
}

const startedAt = new Date().toISOString();
const run = {
  schema_version: 1,
  started_at: startedAt,
  metadata: collectMetadata(),
  sample_count: options.samples,
  configs: [],
};

for (const config of configs) {
  const script = config.script ?? (await readFile(config.path, "utf8"));
  const env = { ...parseConfigEnv(script), ...(config.envOverrides ?? {}) };
  const configRun = {
    name: config.name,
    path: relativePath(config.path),
    env,
    samples: [],
    summaries: [],
  };
  run.configs.push(configRun);

  for (let index = 1; index <= options.samples; index += 1) {
    console.error(`[bench] ${config.name} sample ${index}/${options.samples}`);
    const sample = await runConfig(config.path, index, config.envOverrides ?? {});
    configRun.samples.push(sample);
  }

  configRun.summaries = summarizeSamples(configRun.samples, env);
}

const outputPath = options.output
  ? resolve(repoRoot, options.output)
  : join(repoRoot, "benchmarks", "results", `${startedAt.replace(/[:.]/g, "-")}.json`);
await mkdir(dirname(outputPath), { recursive: true });
await writeFile(outputPath, `${JSON.stringify(run, null, 2)}\n`);
console.log(relativePath(outputPath));

function parseArgs(args) {
  const parsed = {
    samples: Number(process.env.BENCH_SAMPLES ?? 3),
    output: null,
    configs: [],
    list: false,
  };
  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index];
    if (arg === "--samples") {
      parsed.samples = Number(args[++index]);
    } else if (arg === "--out") {
      parsed.output = args[++index];
    } else if (arg === "--config") {
      parsed.configs.push(args[++index]);
    } else if (arg === "--list") {
      parsed.list = true;
    } else if (arg === "--help" || arg === "-h") {
      printHelp();
      process.exit(0);
    } else {
      throw new Error(`unknown argument: ${arg}`);
    }
  }
  if (!Number.isInteger(parsed.samples) || parsed.samples < 1) {
    throw new Error("--samples must be a positive integer");
  }
  return parsed;
}

function printHelp() {
  console.log(`Usage: node benchmarks/run.mjs [--samples N] [--config NAME] [--out PATH]

Runs benchmark config scripts, captures raw output, parses standard result rows,
computes medians, and writes JSON. Pass --config more than once to select a
subset; values may be script names or paths under benchmarks/configs.

Configs may define DD_BENCH_MATRIX_* variables. The runner expands those into
one sampled config per matrix combination and passes selected values as
environment overrides to the config script.`);
}

async function discoverConfigs(selected) {
  const entries = await readdir(configsDir);
  const all = entries
    .filter((entry) => entry.endsWith(".sh"))
    .sort()
    .map((entry) => ({
      name: entry,
      path: join(configsDir, entry),
    }));
  if (selected.length === 0) {
    return all;
  }
  const selectedNames = new Set(selected.map((value) => basename(value)));
  return all.filter((config) => selectedNames.has(config.name));
}

async function expandConfigs(configs) {
  const expanded = [];
  for (const config of configs) {
    const script = await readFile(config.path, "utf8");
    const env = parseConfigEnv(script);
    for (const variant of expandMatrixVariants(env)) {
      expanded.push({
        ...config,
        script,
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

function expandMatrixVariants(env) {
  const dimensions = [];
  const isolates = envList(process.env.DD_BENCH_MATRIX_ISOLATES ?? env.DD_BENCH_MATRIX_ISOLATES);
  if (isolates.length > 0) {
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

  const memoryShards = envList(
    process.env.DD_BENCH_MATRIX_MEMORY_NAMESPACE_SHARDS ??
      env.DD_BENCH_MATRIX_MEMORY_NAMESPACE_SHARDS,
  );
  if (memoryShards.length > 0) {
    dimensions.push(
      memoryShards.map((value) => ({
        label: `shards=${value}`,
        env: { DD_BENCH_MEMORY_NAMESPACE_SHARDS: value },
      })),
    );
  }

  const keyModes = envList(process.env.DD_BENCH_MATRIX_KEY_MODES ?? env.DD_BENCH_MATRIX_KEY_MODES);
  if (keyModes.length > 0) {
    dimensions.push(
      keyModes.map((value) => ({
        label: `keys=${value}`,
        env: { DD_BENCH_MEMORY_KEY_MODE: value },
      })),
    );
  }

  const modes = envList(process.env.DD_BENCH_MATRIX_MODES ?? env.DD_BENCH_MATRIX_MODES);
  if (modes.length > 0) {
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

function envList(value) {
  return (value ?? "")
    .split(/[,\s]+/)
    .map((entry) => entry.trim())
    .filter((entry) => entry.length > 0);
}

function collectMetadata() {
  return {
    git_commit: commandText("git", ["rev-parse", "HEAD"]),
    git_dirty: commandStatus("git", ["diff", "--quiet"]) !== 0,
    rustc: commandText("rustc", ["--version"]),
    cargo: commandText("cargo", ["--version"]),
    os: commandText("uname", ["-a"]),
    logical_cpus: cpus().length,
  };
}

function commandText(command, args) {
  const result = spawnSync(command, args, {
    cwd: repoRoot,
    encoding: "utf8",
  });
  if (result.status !== 0) {
    return null;
  }
  return result.stdout.trim();
}

function commandStatus(command, args) {
  const result = spawnSync(command, args, {
    cwd: repoRoot,
    encoding: "utf8",
  });
  return result.status ?? (result.signal ? 128 : 1);
}

function parseConfigEnv(script) {
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

async function runConfig(path, sample, envOverrides) {
  const startedAt = new Date().toISOString();
  const started = performance.now();
  const child = spawn("bash", [path], {
    cwd: repoRoot,
    env: { ...process.env, ...envOverrides },
    stdio: ["ignore", "pipe", "pipe"],
  });
  let stdout = "";
  let stderr = "";
  child.stdout.setEncoding("utf8");
  child.stderr.setEncoding("utf8");
  child.stdout.on("data", (chunk) => {
    stdout += chunk;
    process.stdout.write(chunk);
  });
  child.stderr.on("data", (chunk) => {
    stderr += chunk;
    process.stderr.write(chunk);
  });

  const { status, signal } = await new Promise((resolvePromise) => {
    child.on("close", (statusValue, signalValue) => {
      resolvePromise({ status: statusValue, signal: signalValue });
    });
  });
  const durationMs = performance.now() - started;
  return {
    sample,
    started_at: startedAt,
    duration_ms: round(durationMs, 2),
    status,
    signal,
    stdout,
    stderr,
    metrics: parseMetrics(stdout),
  };
}

function parseMetrics(stdout) {
  const metrics = [];
  const pattern =
    /^(.+?)\s+requests=(\d+)\s+concurrency=(\d+)(?:\s+total=([0-9.]+)ms)?\s+throughput=([0-9.]+)\s+req\/s\s+mean=([0-9.]+)ms\s+p50=([0-9.]+)ms\s+p95=([0-9.]+)ms\s+p99=([0-9.]+)ms\b/gm;
  for (const match of stdout.matchAll(pattern)) {
    metrics.push({
      workload: match[1].trim(),
      requests: Number(match[2]),
      concurrency: Number(match[3]),
      total_ms: match[4] == null ? null : Number(match[4]),
      throughput_rps: Number(match[5]),
      mean_ms: Number(match[6]),
      p50_ms: Number(match[7]),
      p95_ms: Number(match[8]),
      p99_ms: Number(match[9]),
    });
  }
  return metrics;
}

function summarizeSamples(samples, env) {
  const byWorkload = new Map();
  for (const sample of samples) {
    if (sample.status !== 0 || sample.signal) {
      continue;
    }
    for (const metric of sample.metrics) {
      const entries = byWorkload.get(metric.workload) ?? [];
      entries.push(metric);
      byWorkload.set(metric.workload, entries);
    }
  }

  return [...byWorkload.entries()].map(([workload, metrics]) => ({
    workload,
    samples: metrics.length,
    requests: median(metrics.map((metric) => metric.requests)),
    concurrency: median(metrics.map((metric) => metric.concurrency)),
    isolate_min: numberOrNull(env.DD_BENCH_MIN_ISOLATES),
    isolate_max: numberOrNull(env.DD_BENCH_MAX_ISOLATES),
    max_inflight: numberOrNull(env.DD_BENCH_MAX_INFLIGHT),
    total_ms: median(metrics.map((metric) => metric.total_ms).filter((value) => value != null)),
    throughput_rps: median(metrics.map((metric) => metric.throughput_rps)),
    mean_ms: median(metrics.map((metric) => metric.mean_ms)),
    p50_ms: median(metrics.map((metric) => metric.p50_ms)),
    p95_ms: median(metrics.map((metric) => metric.p95_ms)),
    p99_ms: median(metrics.map((metric) => metric.p99_ms)),
  }));
}

function median(values) {
  if (values.length === 0) {
    return null;
  }
  const sorted = [...values].sort((left, right) => left - right);
  const middle = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 1) {
    return round(sorted[middle], 2);
  }
  return round((sorted[middle - 1] + sorted[middle]) / 2, 2);
}

function numberOrNull(value) {
  if (value == null || value === "") {
    return null;
  }
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function round(value, places) {
  const factor = 10 ** places;
  return Math.round(value * factor) / factor;
}

function relativePath(path) {
  return path.startsWith(repoRoot) ? path.slice(repoRoot.length + 1) : path;
}
