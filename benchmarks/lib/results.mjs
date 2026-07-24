import { readFile } from "node:fs/promises";
import { basename } from "node:path";

export const modeLabels = new Map([
  ["storage-write-memory-wide", "Storage-only direct write"],
  ["direct-write-memory-wide", "Runtime direct write"],
  ["atomic-readwrite-memory-wide", "Atomic read + write"],
  ["atomic-write-memory-wide", "Atomic write + durable effect"],
]);

export async function loadBenchmarkResult(path) {
  return JSON.parse(await readFile(path, "utf8"));
}

export function normalizeBenchmarkResult(run, sourcePath, { allowDirty = false } = {}) {
  validateRun(run, sourcePath, allowDirty);
  const rows = [];
  for (const config of run.configs) {
    validateConfig(config, sourcePath);
    const variant = parseVariantName(config.name);
    const env = config.env ?? {};
    for (const summary of config.summaries) {
      validateSummary(summary, config.name);
      const sampleMetrics = sampleMetricsForWorkload(config.samples ?? [], summary.workload);
      rows.push({
        source: basename(sourcePath),
        startedAt: run.started_at,
        metadata: run.metadata,
        sampleCount: run.sample_count,
        configName: config.name,
        workload: summary.workload,
        mode: variant.mode ?? env.DD_BENCH_MODE,
        keys: variant.keys ?? env.DD_BENCH_MEMORY_KEY_MODE,
        isolates: numberField(variant.isolates ?? env.DD_BENCH_MAX_ISOLATES ?? summary.isolate_max),
        isolateMin: numberField(env.DD_BENCH_MIN_ISOLATES ?? summary.isolate_min),
        isolateMax: numberField(env.DD_BENCH_MAX_ISOLATES ?? summary.isolate_max),
        shards: numberField(variant.shards ?? env.DD_BENCH_MEMORY_NAMESPACE_SHARDS),
        requests: finiteNumber(summary.requests, "requests"),
        concurrency: finiteNumber(summary.concurrency, "concurrency"),
        samples: finiteNumber(summary.samples, "samples"),
        throughputRps: finiteNumber(summary.throughput_rps, "throughput_rps"),
        meanMs: finiteNumber(summary.mean_ms, "mean_ms"),
        p50Ms: finiteNumber(summary.p50_ms, "p50_ms"),
        p95Ms: finiteNumber(summary.p95_ms, "p95_ms"),
        p99Ms: finiteNumber(summary.p99_ms, "p99_ms"),
        sampleMetrics,
      });
    }
  }
  return rows;
}

export function metricValue(row, metric) {
  if (metric === "throughput_rps") {
    return row.throughputRps;
  }
  if (metric === "mean_ms") {
    return row.meanMs;
  }
  if (metric === "p50_ms") {
    return row.p50Ms;
  }
  if (metric === "p95_ms") {
    return row.p95Ms;
  }
  if (metric === "p99_ms") {
    return row.p99Ms;
  }
  throw new Error(`unsupported metric: ${metric}`);
}

export function parseVariantName(name) {
  const [, suffix] = name.split("::", 2);
  const parsed = {};
  if (!suffix) {
    return parsed;
  }
  for (const entry of suffix.split(",")) {
    const [key, value] = entry.split("=", 2);
    if (key && value) {
      parsed[key] = value;
    }
  }
  return parsed;
}

export function selectOne(rows, selector, description = JSON.stringify(selector)) {
  const matches = rows.filter((row) =>
    Object.entries(selector).every(([key, value]) => row[key] === value),
  );
  if (matches.length === 0) {
    throw new Error(`missing benchmark row for ${description}`);
  }
  if (matches.length > 1) {
    throw new Error(`ambiguous benchmark rows for ${description}: ${matches.length} matches`);
  }
  return matches[0];
}

export function compatibleMetadata(rows) {
  if (rows.length === 0) {
    throw new Error("no benchmark rows selected");
  }
  const first = rows[0].metadata;
  for (const row of rows.slice(1)) {
    for (const key of ["git_commit", "git_dirty", "logical_cpus", "os", "rustc", "cargo"]) {
      if (row.metadata?.[key] !== first?.[key]) {
        throw new Error(`incompatible benchmark metadata: ${key}`);
      }
    }
  }
  return first;
}

export function formatInteger(value) {
  return Math.round(value).toLocaleString("en-US");
}

export function formatDecimal(value, places = 2) {
  return Number(value).toFixed(places);
}

export function formatRatio(left, right) {
  return `${formatDecimal(left / right, 1)}x`;
}

function validateRun(run, sourcePath, allowDirty) {
  if (run.schema_version !== 1) {
    throw new Error(`${sourcePath}: unsupported schema_version ${run.schema_version}`);
  }
  if (!Array.isArray(run.configs) || run.configs.length === 0) {
    throw new Error(`${sourcePath}: expected non-empty configs`);
  }
  if (run.complete === false) {
    throw new Error(`${sourcePath}: benchmark result is incomplete`);
  }
  if (!run.metadata?.git_commit) {
    throw new Error(`${sourcePath}: missing git commit metadata`);
  }
  if (!Number.isFinite(run.metadata?.logical_cpus)) {
    throw new Error(`${sourcePath}: missing logical CPU metadata`);
  }
  if (run.metadata.git_dirty && !allowDirty) {
    throw new Error(`${sourcePath}: dirty benchmark result requires --allow-dirty`);
  }
}

function validateConfig(config, sourcePath) {
  if (!Array.isArray(config.summaries) || config.summaries.length === 0) {
    throw new Error(`${sourcePath}: ${config.name} has no summaries`);
  }
  for (const sample of config.samples ?? []) {
    if (sample.status !== 0 || sample.signal) {
      throw new Error(`${sourcePath}: ${config.name} has failed sample ${sample.sample}`);
    }
  }
}

function sampleMetricsForWorkload(samples, workload) {
  return samples
    .flatMap((sample) => sample.metrics ?? [])
    .filter((metric) => metric.workload === workload)
    .map((metric) => ({
      throughput_rps: metric.throughput_rps == null ? null : finiteNumber(metric.throughput_rps, "sample throughput_rps"),
      mean_ms: metric.mean_ms == null ? null : finiteNumber(metric.mean_ms, "sample mean_ms"),
      p50_ms: metric.p50_ms == null ? null : finiteNumber(metric.p50_ms, "sample p50_ms"),
      p95_ms: metric.p95_ms == null ? null : finiteNumber(metric.p95_ms, "sample p95_ms"),
      p99_ms: metric.p99_ms == null ? null : finiteNumber(metric.p99_ms, "sample p99_ms"),
    }));
}

function validateSummary(summary, configName) {
  for (const key of ["throughput_rps", "mean_ms", "p50_ms", "p95_ms", "p99_ms"]) {
    finiteNumber(summary[key], `${configName}.${key}`);
  }
}

function finiteNumber(value, name) {
  const number = Number(value);
  if (!Number.isFinite(number)) {
    throw new Error(`${name} must be finite`);
  }
  return number;
}

function numberField(value) {
  if (value == null || value === "") {
    return null;
  }
  return finiteNumber(value, "numeric benchmark field");
}
