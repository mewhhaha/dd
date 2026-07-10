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
  process.exit(report.failures.length === 0 ? 0 : 1);
}

export function compareBenchmarkRows(baseline, candidate, options = {}) {
  const throughputRegression =
    options.throughputRegression ?? DEFAULT_THROUGHPUT_REGRESSION;
  const p99Regression = options.p99Regression ?? DEFAULT_P99_REGRESSION;
  const candidateByKey = new Map(candidate.map((row) => [rowKey(row), row]));
  const passes = [];
  const failures = [];

  for (const baselineRow of baseline) {
    const key = rowKey(baselineRow);
    const candidateRow = candidateByKey.get(key);
    if (!candidateRow) {
      failures.push({ key, reason: "candidate row is missing" });
      continue;
    }
    const invalidMetric = invalidMetricReason(baselineRow, candidateRow);
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
    row.shards ?? "",
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
