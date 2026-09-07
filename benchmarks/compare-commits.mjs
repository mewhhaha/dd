#!/usr/bin/env node
import { loadBenchmarkResult, normalizeBenchmarkResult } from "./lib/results.mjs";

const DEFAULT_THROUGHPUT_REGRESSION = 0.05;
const DEFAULT_P99_REGRESSION = 0.10;

if (isMain()) {
  const options = parseArgs(process.argv.slice(2));
  const baseline = normalizeBenchmarkResult(
    await loadBenchmarkResult(options.baseline),
    options.baseline,
  );
  const candidate = normalizeBenchmarkResult(
    await loadBenchmarkResult(options.candidate),
    options.candidate,
  );
  const report = compareBenchmarkRows(baseline, candidate, options);
  process.stdout.write(renderReport(report));
  process.exitCode = report.failures.length === 0 ? 0 : 1;
}

export function compareBenchmarkRows(baseline, candidate, options = {}) {
  const throughputRegression =
    options.throughputRegression ?? DEFAULT_THROUGHPUT_REGRESSION;
  const p99Regression = options.p99Regression ?? DEFAULT_P99_REGRESSION;
  const passes = [];
  const failures = [];
  for (const [label, rows] of [["baseline", baseline], ["candidate", candidate]]) {
    if (rows.length === 0) failures.push({ key: label, reason: "no benchmark rows" });
    const seen = new Set();
    for (const row of rows) {
      const key = rowKey(row);
      if (seen.has(key)) failures.push({ key, reason: `${label} contains duplicate rows` });
      seen.add(key);
    }
  }
  if (failures.length > 0) {
    return { passes, failures, throughputRegression, p99Regression };
  }
  const candidateByKey = new Map(candidate.map((row) => [rowKey(row), row]));

  for (const baselineRow of baseline) {
    const key = rowKey(baselineRow);
    const candidateRow = candidateByKey.get(key);
    if (!candidateRow) {
      failures.push({ key, reason: "candidate row is missing" });
      continue;
    }
    const invalidMetric = invalidMetricReason(baselineRow, candidateRow)
      ?? incompatibleRunReason(baselineRow, candidateRow);
    if (invalidMetric) {
      failures.push({ key, reason: invalidMetric });
      candidateByKey.delete(key);
      continue;
    }
    candidateByKey.delete(key);
    const throughputDelta =
      (candidateRow.throughputRps - baselineRow.throughputRps) /
      baselineRow.throughputRps;
    const p99Delta = (candidateRow.p99Ms - baselineRow.p99Ms) / baselineRow.p99Ms;
    const result = { key, throughputDelta, p99Delta };
    if (throughputDelta < -throughputRegression || p99Delta > p99Regression) {
      failures.push({ ...result, reason: "regression budget exceeded" });
    } else {
      passes.push(result);
    }
  }

  for (const key of candidateByKey.keys()) {
    failures.push({ key, reason: "baseline row is missing" });
  }
  return { passes, failures, throughputRegression, p99Regression };
}

function incompatibleRunReason(baseline, candidate) {
  for (const [label, row] of [["baseline", baseline], ["candidate", candidate]]) {
    if (!row.configName || !row.workload || !row.metadata?.git_commit) {
      return `${label} is missing workload or commit identity`;
    }
  }
  for (const field of ["requests", "concurrency", "samples", "sampleCount"]) {
    for (const [label, row] of [["baseline", baseline], ["candidate", candidate]]) {
      if (!Number.isInteger(row[field]) || row[field] <= 0) {
        return `${label} ${field} must be a positive integer`;
      }
    }
    if (baseline[field] !== candidate[field]) return `incompatible ${field}`;
  }
  if (baseline.samples !== baseline.sampleCount || candidate.samples !== candidate.sampleCount) {
    return "incomplete workload samples";
  }
  for (const [label, row] of [["baseline", baseline], ["candidate", candidate]]) {
    if (row.sampleMetrics?.length !== row.samples) return `${label} has incomplete raw samples`;
    if (row.sampleMetrics.some((sample) => !Number.isFinite(sample.throughput_rps)
      || sample.throughput_rps <= 0 || !Number.isFinite(sample.p99_ms) || sample.p99_ms <= 0)) {
      return `${label} has invalid raw sample metrics`;
    }
  }
  if (!baseline.scriptHash || !candidate.scriptHash) return "missing benchmark script fingerprint";
  if (baseline.scriptHash !== candidate.scriptHash) return "incompatible benchmark script or build command";
  for (const field of ["logical_cpus", "cpu_affinity", "cpu_model", "memory_bytes", "memory_limit", "cpu_limit", "disk", "os", "rustc", "cargo", "build"]) {
    if (baseline.metadata?.[field] == null || candidate.metadata?.[field] == null
      || baseline.metadata[field] === "unavailable" || candidate.metadata[field] === "unavailable") {
      return `missing benchmark metadata: ${field}`;
    }
    if (canonical(baseline.metadata[field]) !== canonical(candidate.metadata[field])) {
      return `incompatible benchmark metadata: ${field}`;
    }
  }
  if (baseline.metadata.git_dirty !== false || candidate.metadata.git_dirty !== false) {
    return "comparison requires clean benchmark sources";
  }
  if (!baseline.effectiveConfig || !candidate.effectiveConfig) {
    return "missing effective configuration";
  }
  if (canonical(baseline.effectiveConfig) !== canonical(candidate.effectiveConfig)) {
    return "incompatible effective configuration (including durability)";
  }
  return null;
}

function canonical(value) {
  if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`;
  if (value && typeof value === "object") {
    return `{${Object.keys(value).sort().map((key) => `${JSON.stringify(key)}:${canonical(value[key])}`).join(",")}}`;
  }
  return JSON.stringify(value);
}

function invalidMetricReason(baselineRow, candidateRow) {
  for (const metric of ["throughputRps", "p99Ms"]) {
    for (const [label, row] of [["baseline", baselineRow], ["candidate", candidateRow]]) {
      const value = row[metric];
      if (!Number.isFinite(value) || value <= 0) {
        return `${label} ${metric} must be finite and positive`;
      }
    }
  }
  return null;
}

export function renderReport(report) {
  const lines = [
    `benchmark-commit-comparison passes=${report.passes.length} failures=${report.failures.length}`,
    `budgets throughput=-${percent(report.throughputRegression)} p99=+${percent(report.p99Regression)}`,
  ];
  for (const result of report.passes) {
    lines.push(
      `PASS ${result.key}: throughput=${signedPercent(result.throughputDelta)} p99=${signedPercent(result.p99Delta)}`,
    );
  }
  for (const result of report.failures) {
    const metrics =
      result.throughputDelta == null
        ? ""
        : ` throughput=${signedPercent(result.throughputDelta)} p99=${signedPercent(result.p99Delta)}`;
    lines.push(`FAIL ${result.key}:${metrics} ${result.reason}`);
  }
  return `${lines.join("\n")}\n`;
}

function rowKey(row) {
  return [
    row.configName,
    row.workload,
    row.mode ?? "",
    row.keys ?? "",
    row.isolates ?? "",
  ].join("|");
}

function parseArgs(args) {
  const options = {
    baseline: null,
    candidate: null,
    throughputRegression: DEFAULT_THROUGHPUT_REGRESSION,
    p99Regression: DEFAULT_P99_REGRESSION,
  };
  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index];
    if (arg === "--baseline") options.baseline = args[++index];
    else if (arg === "--candidate") options.candidate = args[++index];
    else if (arg === "--throughput-regression") {
      options.throughputRegression = Number(args[++index]);
    } else if (arg === "--p99-regression") {
      options.p99Regression = Number(args[++index]);
    } else throw new Error(`unknown argument: ${arg}`);
  }
  if (!options.baseline || !options.candidate) {
    throw new Error("--baseline and --candidate are required");
  }
  if (
    !Number.isFinite(options.throughputRegression) ||
    options.throughputRegression < 0 ||
    !Number.isFinite(options.p99Regression) ||
    options.p99Regression < 0
  ) {
    throw new Error("regression budgets must be non-negative numbers");
  }
  return options;
}

function percent(value) {
  return `${(value * 100).toFixed(1)}%`;
}

function signedPercent(value) {
  return `${value >= 0 ? "+" : ""}${percent(value)}`;
}

function isMain() {
  return process.argv[1]?.endsWith("benchmarks/compare-commits.mjs") ?? false;
}
