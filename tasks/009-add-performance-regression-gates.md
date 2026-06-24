# Add non-flaky performance regression gates

**Priority:** P2 quality and release confidence  
**Primary area:** benchmark tooling, CI workflows, compact baseline metadata  
**Dependencies:** tasks 006 and 007; task 008 improves diagnosis

## Problem

The repository has demonstrated important performance invariants, but they are
not enforced automatically:

- cross-shard atomic work should outperform same-shard contention;
- atomic read/write throughput should improve as isolates increase up to the
  physical machine's useful parallelism;
- adding isolates beyond hardware capacity should not be treated as required
  scaling;
- hot-shard p95/p99 should not grow without visibility;
- direct-write and atomic paths should not regress dramatically between changes.

Absolute requests-per-second thresholds are unsuitable for ordinary shared CI
runners because CPU type, load, virtualization, and frequency scaling vary.
Running the full 378-variant, five-sample matrix on every pull request is also
impractical.

## Objective

Add a layered performance-check system that catches structural regressions with
relative invariants on pull requests and supports stronger absolute/relative
checks on a controlled scheduled or manually triggered runner.

## Required layers

### Layer 1: deterministic scheduler correctness tests

Keep ordinary PR CI focused on fast, deterministic properties:

- same-owner atomics never overlap;
- independent owners can dispatch concurrently;
- fair selection rotates across ready shards/owners;
- queue operations remain bounded;
- outbox work cannot block manager responsiveness when test delays are injected.

Most of these belong in Rust unit/integration tests from tasks 002 and 003.
This layer must run in `just check`.

### Layer 2: relative smoke benchmark

Add a small benchmark check intended for reasonably stable CI or explicit local
use. It should compare ratios from runs on the same host and invocation rather
than fixed RPS.

Suggested matrix:

- workloads: direct write and atomic read + write;
- key modes: same-shard and cross-shard;
- isolates: 1 and `min(available_parallelism, 8)`;
- physical shards: `min(available_parallelism, 8)` with a minimum of 2;
- requests/concurrency large enough to reduce startup noise but much smaller
  than the full matrix;
- three samples when used as a gate.

Candidate invariants, configurable in a compact baseline policy file:

- cross-shard atomic read/write at N isolates is at least 1.5× same-shard;
- cross-shard atomic read/write at N isolates is at least 1.5× its one-isolate
  result on machines with at least four logical CPUs;
- direct-write cross-shard is not catastrophically below one-isolate throughput;
- error, timeout, signal, or verification failure always fails the gate;
- p99 must remain below a generous relative multiplier, not a machine-specific
  millisecond constant.

Use broad defaults and document that these detect architecture breakage, not
small optimizations.

### Layer 3: controlled-runner regression report

Add a scheduled/manual workflow for a stable self-hosted or otherwise controlled
runner. It may run:

- the fixed five-sample 16-isolate comparison;
- the reduced core curve;
- skewed-hotspot workloads;
- optional full matrix less frequently.

Compare against a small checked-in baseline summary containing only normalized
metrics and provenance, not raw output. The workflow should upload raw JSON as a
CI artifact with retention rather than committing it.

Use threshold classes:

- warning: e.g. 5–10% median throughput or latency movement;
- failure: e.g. >15–20% regression across repeated samples;
- structural failure: cross-shard advantage or scaling invariant disappears.

Make thresholds workload-specific and reviewable.

## Tooling

Add a script such as:

```bash
node benchmarks/check-regression.mjs \
  --results benchmarks/results/local-smoke.json \
  --policy benchmarks/baselines/memory-scaling-policy.json
```

The script should:

- reuse normalized parsing from task 007;
- validate result completeness and clean provenance as configured;
- calculate ratios and percent changes;
- emit a human-readable table;
- emit machine-readable JSON for CI annotations/artifacts;
- exit non-zero only for policy failures, not warnings;
- distinguish missing data from a measured regression;
- report all failures in one run rather than stopping at the first comparison.

A compact policy schema might define selectors and assertions:

```json
{
  "assertions": [
    {
      "name": "cross-shard atomic advantage",
      "metric": "throughput_rps",
      "left": { "mode": "atomic-readwrite-memory-wide", "keys": "cross-shard", "isolates": 8 },
      "operator": ">=ratio",
      "right": { "mode": "atomic-readwrite-memory-wide", "keys": "same-shard", "isolates": 8 },
      "value": 1.5
    }
  ]
}
```

The exact schema may differ, but do not hard-code every assertion in JavaScript.
Validate the policy schema and reject ambiguous selectors.

## Hardware awareness

Record and use:

- logical CPU count;
- optional CPU model;
- OS/kernel;
- Rust/Cargo versions;
- power-management or runner identifier when available.

For the relative smoke gate:

- skip scaling assertions that require more CPUs than the host provides;
- never compare a 32-isolate target on a 16-CPU host as a required improvement;
- report skipped assertions explicitly.

For controlled-runner baselines, require the expected runner identity or
hardware class before applying absolute comparisons.

## Statistical handling

- Require at least three samples for a gating median.
- Keep raw sample values in the CI artifact.
- Detect extreme variance; fail or mark inconclusive when coefficient of
  variation exceeds a configurable limit rather than trusting a noisy median.
- Compare medians for throughput and percentile values as produced by the
  benchmark runner.
- Do not claim statistical significance from a one-sample diagnostic run.

A simple robust policy is acceptable; a full statistical package is not
required.

## CI workflow

Add a workflow with:

- pull-request/manual smoke job, gated by repository policy or label if runner
  time is expensive;
- scheduled controlled benchmark job;
- `workflow_dispatch` inputs for sample count and matrix narrowing;
- concurrency cancellation so obsolete runs do not consume a runner;
- result JSON uploaded as an artifact;
- generated Markdown summary added to the job summary;
- no automatic commits from CI.

If no stable benchmark runner is currently available, implement the scripts,
fixtures, and manual workflow first; mark the scheduled job disabled or manual
rather than pretending shared-host numbers are stable.

## Tests

Use small synthetic result fixtures. Cover at least:

1. ratio assertion passes and fails correctly;
2. percentage regression against a baseline passes, warns, and fails;
3. ambiguous or missing selector is an error;
4. incomplete/failed benchmark sample is an error;
5. insufficient CPU count skips the correct assertion;
6. excessive variance produces the configured inconclusive/failure result;
7. warnings do not set failure exit code;
8. multiple failures are all reported;
9. policy schema validation rejects unknown operators and invalid values;
10. human and JSON output are deterministic;
11. a one-sample input cannot be used for a gating assertion requiring three;
12. controlled-runner hardware mismatch prevents invalid baseline comparison.

Add fast script tests to `just check`.

## Acceptance criteria

- A compact relative smoke matrix can detect loss of cross-shard advantage and
  loss of basic core scaling.
- PR checks do not rely on machine-specific absolute RPS.
- Stronger baseline comparisons run only on identified stable hardware.
- Raw result JSON is uploaded as CI artifacts, not committed.
- Policies and thresholds are data-driven and reviewable.
- Noisy or incomplete runs are reported as such rather than misclassified.
- Job summaries clearly show measured values, ratios, warnings, failures, and
  skipped checks.
- Script tests and `just check` pass.

## Non-goals

- Blocking every pull request on the complete 378-variant matrix.
- Treating micro-regressions on shared CI as reliable.
- Automatically rewriting the checked-in scaling summary.
- Replacing correctness tests with benchmarks.

## Implementation cautions

- Keep benchmark compilation time separate from timed workload metrics.
- Ensure CI CPU contention from parallel jobs is minimized or isolated.
- Avoid thresholds so strict that normal variance trains maintainers to ignore
  failures.
- A structural invariant failure should remain prominent even if absolute
  throughput happens to increase.
