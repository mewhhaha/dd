import test from "node:test";
import assert from "node:assert/strict";
import { compareBenchmarkRows } from "../compare-commits.mjs";

const row = (throughputRps, p99Ms) => ({
  configName: "realworld-rate-limiter.sh",
  workload: "rate-limiter",
  mode: "rate-limiter",
  keys: "cross-shard",
  isolates: 8,
  requests: 4096,
  concurrency: 128,
  samples: 5,
  sampleCount: 5,
  sampleMetrics: Array.from({ length: 5 }, () => ({ throughput_rps: throughputRps, p99_ms: p99Ms })),
  scriptHash: "same-benchmark-and-build-command",
  effectiveConfig: { DD_BENCH_DURABILITY: "committed" },
  metadata: {
    git_commit: "baseline",
    git_dirty: false,
    logical_cpus: 32,
    cpu_affinity: "0-15",
    cpu_model: "test cpu",
    memory_bytes: 32 * 1024 ** 3,
    memory_limit: "max",
    cpu_limit: "max 100000",
    disk: "test disk",
    os: "Linux test",
    rustc: "rustc test",
    cargo: "cargo test",
    build: { profile: "release", rustflags: "" },
  },
  throughputRps,
  p99Ms,
});

test("rejects empty and duplicate comparisons", () => {
  assert.equal(compareBenchmarkRows([], []).passes.length, 0);
  assert.equal(compareBenchmarkRows([], []).failures.length, 2);
  for (const [baseline, candidate] of [
    [[row(1000, 10), row(1000, 10)], [row(1000, 10)]],
    [[row(1000, 10)], [row(1000, 10), row(1000, 10)]],
  ]) {
    assert.match(compareBenchmarkRows(baseline, candidate).failures[0].reason, /duplicate/);
  }
});

test("allows different commits with identical workload and environment", () => {
  const candidate = row(1000, 10);
  candidate.metadata.git_commit = "candidate";
  assert.equal(compareBenchmarkRows([row(1000, 10)], [candidate]).failures.length, 0);
});

test("rejects mismatched workload, durability and execution constraints", () => {
  for (const change of [
    (candidate) => { candidate.requests *= 2; },
    (candidate) => { candidate.concurrency *= 2; },
    (candidate) => { candidate.samples = 1; },
    (candidate) => { candidate.sampleMetrics.pop(); },
    (candidate) => { candidate.scriptHash = "different-build-command"; },
    (candidate) => { candidate.metadata.cpu_affinity = "0"; },
    (candidate) => { candidate.metadata.memory_limit = "1073741824"; },
    (candidate) => { candidate.metadata.build.rustflags = "-C target-cpu=native"; },
    (candidate) => { candidate.metadata.git_dirty = true; },
    (candidate) => { candidate.effectiveConfig.DD_BENCH_DURABILITY = "queued"; },
    (candidate) => { delete candidate.metadata.cpu_affinity; },
  ]) {
    const candidate = row(1000, 10);
    change(candidate);
    const report = compareBenchmarkRows([row(1000, 10)], [candidate]);
    assert.equal(report.passes.length, 0);
    assert.equal(report.failures.length, 1);
  }
});

test("accepts results within the production regression budgets", () => {
  const report = compareBenchmarkRows([row(1000, 10)], [row(951, 10.99)]);
  assert.equal(report.failures.length, 0);
  assert.equal(report.passes.length, 1);
});

test("rejects throughput and p99 regressions beyond the budgets", () => {
  const throughput = compareBenchmarkRows([row(1000, 10)], [row(949, 10)]);
  const latency = compareBenchmarkRows([row(1000, 10)], [row(1000, 11.01)]);
  assert.equal(throughput.failures.length, 1);
  assert.equal(latency.failures.length, 1);
});

test("rejects non-finite and non-positive metrics with a clear failure", () => {
  for (const [metric, value] of [
    ["throughputRps", 0],
    ["throughputRps", Number.NaN],
    ["p99Ms", -1],
    ["p99Ms", Number.POSITIVE_INFINITY],
  ]) {
    const baseline = row(1000, 10);
    baseline[metric] = value;
    const report = compareBenchmarkRows([baseline], [row(1000, 10)]);
    assert.equal(report.failures.length, 1);
    assert.match(report.failures[0].reason, new RegExp(`baseline ${metric}`));
  }
});

test("rejects invalid candidate metrics", () => {
  const candidate = row(1000, 10);
  candidate.p99Ms = Number.NaN;
  const report = compareBenchmarkRows([row(1000, 10)], [candidate]);
  assert.equal(report.failures.length, 1);
  assert.equal(report.failures[0].reason, "candidate p99Ms must be finite and positive");
});
