import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawnSync } from "node:child_process";
import { checkRegression, renderHumanReport } from "../check-regression.mjs";
import { repoRoot } from "./runner-config.mjs";

test("ratio assertions pass and fail deterministically", async () => {
  const passing = await writeRegressionFixture();
  const passReport = await checkRegression({
    results: passing.resultsPath,
    policy: passing.policyPath,
    allowDirty: true,
  });
  assert.equal(passReport.failures.length, 0);
  assert.equal(passReport.passes.length, 2);

  const failing = await writeRegressionFixture({
    atomicCross8: 2200,
    policyAssertions: [ratioAssertion({})],
  });
  const failReport = await checkRegression({
    results: failing.resultsPath,
    policy: failing.policyPath,
    allowDirty: true,
  });
  assert.equal(failReport.failures.length, 1);
  assert.match(failReport.failures[0].message, /violates/);
});

test("baseline comparisons distinguish pass warning and failure", async () => {
  const warning = await writeRegressionFixture({
    policyAssertions: [baselineAssertion({ baseline: 10_000, warning: 0.1, failure: 0.5 })],
  });
  const warningReport = await checkRegression({
    results: warning.resultsPath,
    policy: warning.policyPath,
    allowDirty: true,
  });
  assert.equal(warningReport.warnings.length, 1);
  assert.equal(warningReport.failures.length, 0);

  const failure = await writeRegressionFixture({
    policyAssertions: [baselineAssertion({ baseline: 20_000, warning: 0.1, failure: 0.5 })],
  });
  const failureReport = await checkRegression({
    results: failure.resultsPath,
    policy: failure.policyPath,
    allowDirty: true,
  });
  assert.equal(failureReport.failures.length, 1);
});

test("missing ambiguous incomplete and noisy data are reported as errors", async () => {
  const missing = await writeRegressionFixture({
    policyAssertions: [ratioAssertion({ right: selector("atomic-readwrite-memory-wide", "same-shard", 16) })],
  });
  const missingReport = await checkRegression({
    results: missing.resultsPath,
    policy: missing.policyPath,
    allowDirty: true,
  });
  assert.match(missingReport.errors[0].message, /missing benchmark row/);

  const ambiguous = await writeRegressionFixture({ duplicateAtomicCross8: true });
  const ambiguousReport = await checkRegression({
    results: ambiguous.resultsPath,
    policy: ambiguous.policyPath,
    allowDirty: true,
  });
  assert.match(ambiguousReport.errors[0].message, /ambiguous benchmark rows/);

  const insufficient = await writeRegressionFixture({ sampleCount: 1 });
  const insufficientReport = await checkRegression({
    results: insufficient.resultsPath,
    policy: insufficient.policyPath,
    allowDirty: true,
  });
  assert.match(insufficientReport.errors[0].message, /requires 3 samples/);

  const noisy = await writeRegressionFixture({ noisy: true });
  const noisyReport = await checkRegression({
    results: noisy.resultsPath,
    policy: noisy.policyPath,
    allowDirty: true,
  });
  assert.match(noisyReport.errors[0].message, /coefficient of variation/);
});

test("cpu-aware assertions skip and failed samples fail validation", async () => {
  const skipped = await writeRegressionFixture({ logicalCpus: 2 });
  const skipReport = await checkRegression({
    results: skipped.resultsPath,
    policy: skipped.policyPath,
    allowDirty: true,
  });
  assert.equal(skipReport.skips.length, 1);
  assert.match(skipReport.skips[0].reason, /requires 4 logical CPUs/);

  const failed = await writeRegressionFixture({ failedSample: true });
  await assert.rejects(
    () =>
      checkRegression({
        results: failed.resultsPath,
        policy: failed.policyPath,
        allowDirty: true,
      }),
    /failed sample/,
  );
});

test("warnings do not fail CLI but failures do", async () => {
  const warning = await writeRegressionFixture({
    policyAssertions: [baselineAssertion({ baseline: 10_000, warning: 0.1, failure: 0.5 })],
  });
  const warningRun = spawnSync(
    process.execPath,
    ["benchmarks/check-regression.mjs", "--results", warning.resultsPath, "--policy", warning.policyPath],
    { cwd: repoRoot, encoding: "utf8" },
  );
  assert.equal(warningRun.status, 0, warningRun.stderr);
  assert.match(warningRun.stdout, /WARN direct write baseline/);

  const failure = await writeRegressionFixture({
    atomicCross8: 2200,
    policyAssertions: [ratioAssertion({})],
  });
  const failureRun = spawnSync(
    process.execPath,
    ["benchmarks/check-regression.mjs", "--results", failure.resultsPath, "--policy", failure.policyPath],
    { cwd: repoRoot, encoding: "utf8" },
  );
  assert.equal(failureRun.status, 1);
  assert.match(failureRun.stdout, /FAIL atomic advantage/);
});

test("human report is deterministic", async () => {
  const fixture = await writeRegressionFixture({ atomicCross8: 2200, logicalCpus: 2 });
  const report = await checkRegression({
    results: fixture.resultsPath,
    policy: fixture.policyPath,
    allowDirty: true,
  });
  assert.equal(
    renderHumanReport(report).split("\n").slice(0, 3).join("\n"),
    `benchmark-regression policy=test-policy results=${fixture.resultsPath}\npasses=0 warnings=0 failures=1 skips=1 errors=0\nFAIL atomic advantage: ratio 1.1 violates >=ratio 1.5`,
  );
});

async function writeRegressionFixture(options = {}) {
  const dir = await mkdtemp(join(tmpdir(), "dd-regression-"));
  const resultsPath = join(dir, "results.json");
  const policyPath = join(dir, "policy.json");
  const sampleCount = options.sampleCount ?? 3;
  const result = {
    schema_version: 1,
    started_at: "2026-01-02T03:04:05.000Z",
    metadata: {
      git_commit: "0123456789abcdef0123456789abcdef01234567",
      git_dirty: false,
      logical_cpus: options.logicalCpus ?? 8,
      os: "Linux test",
      rustc: "rustc test",
      cargo: "cargo test",
    },
    sample_count: sampleCount,
    complete: true,
    configs: [
      config("atomic-readwrite-memory-wide", "cross-shard", 1, 2000, sampleCount, options),
      config(
        "atomic-readwrite-memory-wide",
        "cross-shard",
        8,
        options.atomicCross8 ?? 6000,
        sampleCount,
        options,
      ),
      config("atomic-readwrite-memory-wide", "same-shard", 8, 2000, sampleCount, options),
      config("direct-write-memory-wide", "cross-shard", 8, 8000, sampleCount, options),
    ],
  };
  if (options.duplicateAtomicCross8) {
    result.configs.push(config("atomic-readwrite-memory-wide", "cross-shard", 8, 6100, sampleCount, options));
  }
  const policy = {
    name: "test-policy",
    assertions: options.policyAssertions ?? [
      ratioAssertion({}),
      ratioAssertion({
        name: "atomic scales",
        left: selector("atomic-readwrite-memory-wide", "cross-shard", 8),
        right: selector("atomic-readwrite-memory-wide", "cross-shard", 1),
        min_logical_cpus: 4,
      }),
    ],
  };
  await writeFile(resultsPath, JSON.stringify(result, null, 2));
  await writeFile(policyPath, JSON.stringify(policy, null, 2));
  return { dir, resultsPath, policyPath };
}

function ratioAssertion(overrides) {
  return {
    name: "atomic advantage",
    metric: "throughput_rps",
    operator: ">=ratio",
    left: selector("atomic-readwrite-memory-wide", "cross-shard", 8),
    right: selector("atomic-readwrite-memory-wide", "same-shard", 8),
    value: 1.5,
    min_samples: 3,
    max_cv: 0.35,
    ...overrides,
  };
}

function baselineAssertion(overrides) {
  return {
    name: "direct write baseline",
    metric: "throughput_rps",
    operator: "baseline",
    selector: selector("direct-write-memory-wide", "cross-shard", 8),
    baseline: 8000,
    warning: 0.1,
    failure: 0.2,
    direction: "higher-is-better",
    min_samples: 3,
    ...overrides,
  };
}

function selector(mode, keys, isolates) {
  return { mode, keys, isolates, shards: 8 };
}

function config(mode, keys, isolates, throughput, sampleCount, options) {
  const workload = `memory-${mode}`;
  const samples = Array.from({ length: sampleCount }, (_, index) => ({
    sample: index + 1,
    status: options.failedSample ? 1 : 0,
    signal: null,
    metrics: [
      {
        workload,
        requests: 1000,
        concurrency: 64,
        throughput_rps: options.noisy && index === 0 ? throughput * 3 : throughput,
        mean_ms: 10,
        p50_ms: 8,
        p95_ms: 20,
        p99_ms: 30,
      },
    ],
  }));
  return {
    name: `smoke.sh::isolates=${isolates},shards=8,keys=${keys},mode=${mode}`,
    env: {
      DD_BENCH_MODE: mode,
      DD_BENCH_MEMORY_KEY_MODE: keys,
      DD_BENCH_MIN_ISOLATES: String(isolates),
      DD_BENCH_MAX_ISOLATES: String(isolates),
      DD_BENCH_MEMORY_NAMESPACE_SHARDS: "8",
    },
    samples,
    summaries: [
      {
        workload,
        samples: sampleCount,
        requests: 1000,
        concurrency: 64,
        isolate_min: isolates,
        isolate_max: isolates,
        throughput_rps: throughput,
        mean_ms: 10,
        p50_ms: 8,
        p95_ms: 20,
        p99_ms: 30,
      },
    ],
  };
}
