#!/usr/bin/env node
import { spawn, spawnSync } from "node:child_process";
import { cpus } from "node:os";
import { mkdir, writeFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import {
  discoverConfigs,
  expandConfigs,
  parseArgs,
  parseConfigEnv,
  planRun,
  renderPlan,
  repoRoot,
  validateRunBudget,
} from "./lib/runner-config.mjs";

const options = parseArgs(process.argv.slice(2));
if (options.help) {
  printHelp();
  process.exit(0);
}
const discovered = await discoverConfigs(options.configs, { configsDirectory: options.configsDir });
const configs = await expandConfigs(discovered.configs);

if (options.list) {
  for (const config of configs) {
    console.log(config.name);
  }
  if (discovered.skippedOptIn.length > 0) {
    console.error(`[bench] skipped opt-in configs: ${discovered.skippedOptIn.join(", ")}`);
  }
  process.exit(0);
}

const startedAt = new Date().toISOString();
const outputPath = options.output
  ? resolve(repoRoot, options.output)
  : join(repoRoot, "benchmarks", "results", `${startedAt.replace(/[:.]/g, "-")}.json`);
const plan = planRun(configs, options, relativePath(outputPath));
if (options.plan) {
  process.stdout.write(renderPlan(plan, discovered.skippedOptIn));
  process.exit(0);
}
if (discovered.skippedOptIn.length > 0) {
  console.error(`[bench] skipped opt-in configs: ${discovered.skippedOptIn.join(", ")}`);
}
validateRunBudget(plan, options);

const run = {
  schema_version: 1,
  started_at: startedAt,
  metadata: collectMetadata(),
  sample_count: options.samples,
  complete: true,
  failures: [],
  configs: [],
};

for (const config of configs) {
  const script = config.script;
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
    if (sample.status !== 0 || sample.signal) {
      run.complete = false;
      const failure = {
        config: config.name,
        sample: index,
        status: sample.status,
        signal: sample.signal,
      };
      run.failure ??= failure;
      run.failures.push(failure);
      if (options.keepGoing) {
        continue;
      }
      await writeRun(outputPath, run);
      console.error(
        `[bench] failed ${config.name} sample ${index}: status=${sample.status} signal=${sample.signal}; partial output written to ${relativePath(outputPath)}`,
      );
      throw new Error(
        `benchmark failed: ${config.name} sample ${index} status=${sample.status} signal=${sample.signal}`,
      );
    }
  }

  configRun.summaries = summarizeSamples(configRun.samples, env);
}

await writeRun(outputPath, run);
console.log(relativePath(outputPath));

async function writeRun(outputPath, run) {
  await mkdir(dirname(outputPath), { recursive: true });
  await writeFile(outputPath, `${JSON.stringify(run, null, 2)}\n`);
}

function printHelp() {
  console.log(`Usage: node benchmarks/run.mjs [--samples N] [--config NAME] [--out PATH] [--plan] [--max-runs N] [--allow-large-run]

Runs benchmark config scripts, captures raw output, parses standard result rows,
computes medians, and writes JSON. Pass --config more than once to select a
subset; values may be script names or paths under benchmarks/configs.

Configs may define DD_BENCH_MATRIX_* variables. The runner expands those into
one sampled config per matrix combination and passes selected values as
environment overrides to the config script. Matrix configs marked
DD_BENCH_MATRIX_OPT_IN=1 are skipped unless explicitly selected. Use --plan to
inspect variants without spawning child processes.`);
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

async function runConfig(path, sample, envOverrides) {
  const startedAt = new Date().toISOString();
  const started = performance.now();
  const child = spawn("bash", [path], {
    cwd: repoRoot,
    env: { ...process.env, BENCH_SAMPLE: String(sample), ...envOverrides },
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
