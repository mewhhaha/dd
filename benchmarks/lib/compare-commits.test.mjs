import test from "node:test";
import assert from "node:assert/strict";
import { compareBenchmarkRows } from "../compare-commits.mjs";

const row = (throughputRps, p99Ms) => ({
  configName: "realworld-rate-limiter.sh",
  workload: "rate-limiter",
  mode: "rate-limiter",
  keys: "cross-shard",
  isolates: 8,
  shards: 8,
  throughputRps,
  p99Ms,
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
