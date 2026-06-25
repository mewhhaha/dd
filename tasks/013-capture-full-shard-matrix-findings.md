# Capture and analyze full shard-matrix findings

**Priority:** P2 performance analysis  
**Primary area:** benchmark tooling and performance documentation  
**Dependencies:** task 007 is recommended first

## Problem

The full atomic memory scaling matrix can sweep isolate counts, physical shard
counts, key distributions, and workloads. The repository now avoids committing
raw JSON results, but there is still no durable analysis of the full shard-count
axis.

Current checked-in conclusions focus on a 16-shard slice and fixed 16-isolate
comparison. That is enough to show useful sharding behavior, but not enough to
answer these operational questions:

- What shard count should be used by default on a 16-core host?
- Does increasing physical shards beyond CPU count help or hurt?
- Where does storage overhead dominate scheduler gains?
- How does skewed-hotspot behavior change as shard count increases?
- Does a low shard count become the limiting factor before isolate count?

## Objective

Add a reproducible analysis step that consumes a full or partial matrix result
and writes a compact Markdown report focused on the shard-count dimension.

This may be implemented as part of the generated summary tooling from task 007
or as a separate report script if that keeps the scope smaller.

## Required report sections

The report should include:

1. best physical shard count per workload/key-mode/isolate-count combination;
2. throughput by shard count at fixed isolate counts, especially 1, 8, and 16;
3. p95/p99 by shard count for cross-shard and skewed-hotspot workloads;
4. degradation when shards exceed logical CPU count;
5. default-shard-count recommendation with caveats;
6. comparison of same-shard, cross-shard, and skewed-hotspot shapes;
7. warnings when source data is one-sample diagnostic rather than repeated
   release evidence;
8. exact provenance: commit, hardware, sample count, logical CPUs, request count,
   concurrency, and max inflight.

## Tooling

Add a command such as:

```bash
node benchmarks/analyze-shards.mjs \
  --input benchmarks/results/local-atomic-memory-scaling-matrix.json \
  --out benchmarks/results/local-shard-analysis.md
```

If task 007 already adds `benchmarks/summarize.mjs`, extend it instead of
creating a second parser.

The analyzer must reject ambiguous or incomplete data rather than selecting the
first matching row.

## Tests

Use small committed fixtures under `benchmarks/testdata/`. Cover at least:

1. best shard count is selected correctly for one workload;
2. ties are reported deterministically;
3. missing shard values are reported;
4. one-sample data is labeled diagnostic;
5. repeated-sample data is labeled median evidence;
6. skewed-hotspot rows are included separately;
7. mixed hardware metadata is rejected by default;
8. malformed result files fail clearly.

## Acceptance criteria

- A developer can run one command to turn full matrix JSON into a shard-focused
  Markdown report.
- The report identifies the shard count that performs best for each selected
  workload and isolate count.
- The output is deterministic and test-covered.
- Raw JSON remains ignored and untracked.
- The main scaling summary links to the shard-analysis workflow.

## Non-goals

- Selecting production defaults automatically at runtime.
- Changing the shard hash algorithm or storage layout.
- Re-running benchmarks from the analyzer.

## Implementation cautions

- Do not overfit to the current local host. The report should describe measured
  results, not declare universal defaults.
- Preserve raw values and ratios; avoid hiding tail-latency regressions behind
  throughput-only summaries.
