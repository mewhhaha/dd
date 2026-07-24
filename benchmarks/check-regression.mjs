#!/usr/bin/env node
import { readFile } from "node:fs/promises";
import { inspect } from "node:util";
import {
  loadBenchmarkResult,
  metricValue,
  normalizeBenchmarkResult,
  selectOne,
} from "./lib/results.mjs";

if (isMain()) {
  const options = parseArgs(process.argv.slice(2));
  if (options.help) {
    printHelp();
    process.exit(0);
  }
  const report = await checkRegression(options);
  process.stdout.write(renderHumanReport(report));
  if (options.json) {
    process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
  }
  process.exit(report.failures.length > 0 || report.errors.length > 0 ? 1 : 0);
}

export async function checkRegression(options) {
  const policy = await loadPolicy(options.policy);
  const rows = normalizeBenchmarkResult(await loadBenchmarkResult(options.results), options.results, {
    allowDirty: policy.allow_dirty_results === true || options.allowDirty,
  });
  const report = {
    policy: policy.name ?? options.policy,
    results: options.results,
    passes: [],
    warnings: [],
    failures: [],
    skips: [],
    errors: [],
  };
  for (const assertion of policy.assertions) {
    try {
      const outcome = evaluateAssertion(assertion, rows);
      report[outcome.status].push(outcome);
    } catch (error) {
      report.errors.push({
        name: assertion.name ?? "<unnamed>",
        message: error.message,
      });
    }
  }
  for (const key of ["passes", "warnings", "failures", "skips", "errors"]) {
    report[key].sort((left, right) => left.name.localeCompare(right.name));
  }
  return report;
}

export function evaluateAssertion(assertion, rows) {
  validateAssertion(assertion);
  const metadata = rows[0]?.metadata ?? {};
  if (assertion.min_logical_cpus && metadata.logical_cpus < assertion.min_logical_cpus) {
    return {
      status: "skips",
      name: assertion.name,
      reason: `requires ${assertion.min_logical_cpus} logical CPUs; result has ${metadata.logical_cpus}`,
    };
  }
  if (assertion.operator === ">=ratio" || assertion.operator === "<=ratio") {
    return evaluateRatioAssertion(assertion, rows);
  }
  if (assertion.operator === "baseline") {
    return evaluateBaselineAssertion(assertion, rows);
  }
  throw new Error(`unsupported assertion operator: ${assertion.operator}`);
}

function evaluateRatioAssertion(assertion, rows) {
  const left = selectedMeasuredRow(rows, assertion.left, assertion);
  const right = selectedMeasuredRow(rows, assertion.right, assertion);
  const leftValue = metricValue(left, assertion.metric);
  const rightValue = metricValue(right, assertion.metric);
  const ratio = leftValue / rightValue;
  const passed =
    assertion.operator === ">=ratio" ? ratio >= assertion.value : ratio <= assertion.value;
  return {
    status: passed ? "passes" : "failures",
    name: assertion.name,
    metric: assertion.metric,
    operator: assertion.operator,
    value: assertion.value,
    measured: round(ratio, 3),
    left: round(leftValue, 3),
    right: round(rightValue, 3),
    message: passed
      ? `ratio ${round(ratio, 3)} satisfies ${assertion.operator} ${assertion.value}`
      : `ratio ${round(ratio, 3)} violates ${assertion.operator} ${assertion.value}`,
  };
}

function evaluateBaselineAssertion(assertion, rows) {
  const row = selectedMeasuredRow(rows, assertion.selector, assertion);
  const measured = metricValue(row, assertion.metric);
  const baseline = assertion.baseline;
  const direction = assertion.direction ?? "higher-is-better";
  const delta =
    direction === "lower-is-better" ? (baseline - measured) / baseline : (measured - baseline) / baseline;
  const regression = -delta;
  if (regression >= assertion.failure) {
    return {
      status: "failures",
      name: assertion.name,
      metric: assertion.metric,
      measured,
      baseline,
      regression: round(regression, 3),
      message: `regression ${percent(regression)} exceeds failure threshold ${percent(assertion.failure)}`,
    };
  }
  if (regression >= assertion.warning) {
    return {
      status: "warnings",
      name: assertion.name,
      metric: assertion.metric,
      measured,
      baseline,
      regression: round(regression, 3),
      message: `regression ${percent(regression)} exceeds warning threshold ${percent(assertion.warning)}`,
    };
  }
  return {
    status: "passes",
    name: assertion.name,
    metric: assertion.metric,
    measured,
    baseline,
    regression: round(Math.max(0, regression), 3),
    message: "baseline comparison passed",
  };
}

function selectedMeasuredRow(rows, selector, assertion) {
  const row = selectOne(rows, normalizeSelector(selector), assertion.name);
  if (assertion.min_samples && row.samples < assertion.min_samples) {
    throw new Error(
      `${assertion.name}: requires ${assertion.min_samples} samples; row has ${row.samples}`,
    );
  }
  if (assertion.max_cv != null) {
    const values = row.sampleMetrics
      .map((metric) => metric[assertion.metric])
      .filter((value) => Number.isFinite(value));
    if (values.length >= 2) {
      const cv = coefficientOfVariation(values);
      if (cv > assertion.max_cv) {
        throw new Error(
          `${assertion.name}: coefficient of variation ${round(cv, 3)} exceeds ${assertion.max_cv}`,
        );
      }
    }
  }
  return row;
}

function normalizeSelector(selector) {
  return {
    mode: selector.mode,
    keys: selector.keys,
    isolates: selector.isolates,
    shards: selector.shards,
  };
}

async function loadPolicy(path) {
  const policy = JSON.parse(await readFile(path, "utf8"));
  if (!Array.isArray(policy.assertions) || policy.assertions.length === 0) {
    throw new Error(`${path}: policy requires non-empty assertions`);
  }
  return policy;
}

function validateAssertion(assertion) {
  if (!assertion.name) {
    throw new Error("assertion requires name");
  }
  if (!assertion.metric) {
    throw new Error(`${assertion.name}: assertion requires metric`);
  }
  if (assertion.operator === ">=ratio" || assertion.operator === "<=ratio") {
    if (!assertion.left || !assertion.right || !Number.isFinite(assertion.value)) {
      throw new Error(`${assertion.name}: ratio assertion requires left, right, and numeric value`);
    }
    return;
  }
  if (assertion.operator === "baseline") {
    if (
      !assertion.selector ||
      !Number.isFinite(assertion.baseline) ||
      !Number.isFinite(assertion.warning) ||
      !Number.isFinite(assertion.failure)
    ) {
      throw new Error(
        `${assertion.name}: baseline assertion requires selector, baseline, warning, and failure`,
      );
    }
    if (assertion.failure < assertion.warning) {
      throw new Error(`${assertion.name}: failure threshold must be >= warning threshold`);
    }
    if (
      assertion.direction != null &&
      assertion.direction !== "higher-is-better" &&
      assertion.direction !== "lower-is-better"
    ) {
      throw new Error(`${assertion.name}: unknown baseline direction ${assertion.direction}`);
    }
    return;
  }
  throw new Error(`${assertion.name}: unknown operator ${inspect(assertion.operator)}`);
}

export function renderHumanReport(report) {
  const lines = [
    `benchmark-regression policy=${report.policy} results=${report.results}`,
    `passes=${report.passes.length} warnings=${report.warnings.length} failures=${report.failures.length} skips=${report.skips.length} errors=${report.errors.length}`,
  ];
  for (const [label, entries] of [
    ["PASS", report.passes],
    ["WARN", report.warnings],
    ["FAIL", report.failures],
    ["SKIP", report.skips],
    ["ERROR", report.errors],
  ]) {
    for (const entry of entries) {
      lines.push(`${label} ${entry.name}: ${entry.message ?? entry.reason}`);
    }
  }
  return `${lines.join("\n")}\n`;
}

function coefficientOfVariation(values) {
  const mean = values.reduce((sum, value) => sum + value, 0) / values.length;
  if (mean === 0) {
    return 0;
  }
  const variance =
    values.reduce((sum, value) => sum + (value - mean) ** 2, 0) / values.length;
  return Math.sqrt(variance) / mean;
}

function parseArgs(args) {
  const parsed = {
    results: null,
    policy: null,
    allowDirty: false,
    json: false,
  };
  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index];
    if (arg === "--results") {
      parsed.results = args[++index];
    } else if (arg === "--policy") {
      parsed.policy = args[++index];
    } else if (arg === "--allow-dirty") {
      parsed.allowDirty = true;
    } else if (arg === "--json") {
      parsed.json = true;
    } else if (arg === "--help" || arg === "-h") {
      parsed.help = true;
    } else {
      throw new Error(`unknown argument: ${arg}`);
    }
  }
  if (!parsed.help && (!parsed.results || !parsed.policy)) {
    throw new Error("--results and --policy are required");
  }
  return parsed;
}

function percent(value) {
  return `${round(value * 100, 1)}%`;
}

function round(value, places = 2) {
  const factor = 10 ** places;
  return Math.round(value * factor) / factor;
}

function printHelp() {
  console.log(`Usage: node benchmarks/check-regression.mjs --results FILE --policy FILE

Evaluates policy-driven relative and baseline benchmark assertions. The script
exits non-zero for failures, policy/data errors, incomplete results, failed
samples, ambiguous selectors, and excessive variance.`);
}

function isMain() {
  return process.argv[1]?.endsWith("benchmarks/check-regression.mjs") ?? false;
}
