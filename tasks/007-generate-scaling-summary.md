# Generate the scaling summary from benchmark output

**Priority:** P1 tooling and documentation correctness  
**Primary area:** `benchmarks/SCALING.md`, new benchmark summarizer, `justfile`  
**Dependencies:** task 006 is recommended so result selection is explicit

## Problem

`benchmarks/SCALING.md` is maintained manually after raw JSON results are
generated locally. Raw result files are intentionally ignored, which is good for
repository size, but the durable tables can now drift from the data used to
produce them.

The summary currently combines at least two evidence classes:

- a five-sample fixed 16-isolate comparison;
- a one-sample diagnostic core curve at 16 physical memory shards.

Manual editing can accidentally:

- copy the wrong variant or sample statistic;
- mix commits, hardware, or dirty-worktree results;
- present a one-sample value as a median;
- omit a key distribution or shard count from the provenance;
- leave stale ratios after updating throughput values;
- change explanatory text without updating the source metadata.

## Objective

Add a deterministic summarizer that reads ignored local benchmark JSON files,
validates their provenance, and generates the measured sections of
`benchmarks/SCALING.md`. Keep explanatory interpretation reviewable, but remove
manual arithmetic and transcription.

## Proposed interface

Add an executable such as:

```bash
node benchmarks/summarize.mjs \
  --fixed benchmarks/results/local-fixed-write-matrix.json \
  --core benchmarks/results/local-core-scaling.json \
  --out benchmarks/SCALING.md
```

Also support:

```bash
node benchmarks/summarize.mjs ... --check
```

`--check` must generate content in memory and fail with a readable diff or
message if the checked-in summary is stale. It must not modify files.

If a single full-matrix result contains all required rows, support selecting the
fixed and core sections from one input file.

## Separate generated data from prose

Choose one maintainable approach:

### Preferred: generated markers in one file

Keep explanatory prose in `SCALING.md` and replace only marked regions:

```markdown
<!-- BEGIN GENERATED: fixed-write-matrix -->
...
<!-- END GENERATED: fixed-write-matrix -->
```

The script must preserve text outside generated regions byte-for-byte.

### Acceptable alternative

Generate `benchmarks/SCALING.generated.md` and have `SCALING.md` link to it.
This is acceptable only if the generated file remains readable and the split
does not duplicate the same tables in two places.

Do not generate the entire document from a large hard-coded template that makes
normal prose edits cumbersome.

## Input validation

Validate every input before producing output:

- supported `schema_version`;
- non-empty `configs` and `summaries`;
- clean worktree for release-evidence sections;
- commit SHA present;
- logical CPU count present;
- sample count matches the intended section;
- all selected rows have successful samples;
- workload, isolate count, shard count, and key mode can be derived
  unambiguously;
- duplicate rows for one selection are rejected unless the selection policy is
  explicit;
- required percentiles and throughput fields are finite numbers;
- all rows in one table come from compatible hardware/toolchain metadata unless
  an explicit override is supplied.

By default, reject dirty results for the durable summary. Add an explicit
`--allow-dirty` escape hatch that marks the generated section visibly as
non-release evidence.

## Selection model

Do not identify rows by fragile substring matching alone. Parse variant names
and environment fields into a normalized record:

```text
workload mode
isolate min/max
physical shard count
key mode
request count
concurrency
sample count
throughput and latency statistics
```

The fixed comparison should select the configured isolate/shard count and the
same-shard/cross-shard rows for:

- storage-only direct write when supplied;
- runtime direct write;
- atomic read + write;
- atomic write + durable effect.

The core curve should select one physical shard count and show requested isolate
counts for both same-shard and cross-shard controls.

Make the selected fixed isolate count, physical shard count, and isolate curve
configurable through CLI flags with defaults matching the current document.

## Derived values

Calculate rather than hand-enter:

- cross-shard advantage ratios;
- one-to-N speedups;
- peak throughput and the isolate count at which it occurs;
- p95/p99 table values;
- whether the 32-isolate result improves, plateaus, or regresses relative to 16
  using a documented tolerance;
- table formatting and thousands separators.

Do not overstate statistical significance. A one-sample diagnostic table should
be labeled as such in generated text.

## Provenance section

Generate a compact provenance block containing:

- input file basename(s);
- benchmark start time;
- measured git commit;
- clean/dirty status;
- sample count;
- logical CPUs;
- OS;
- Rust and Cargo versions;
- request count, concurrency, inflight limit, key space, and shard count for each
  selected section.

Because raw files are ignored, the summary must contain enough context to
reproduce the measurement without claiming the raw artifact is available in
Git.

## Tests

Refactor parsing and rendering into importable pure functions and use
`node:test`. Commit small fixtures under `benchmarks/testdata/`; the gitignore
only excludes `benchmarks/results/*.json`, so fixtures can remain versioned.

Cover at least:

1. valid fixed matrix renders the expected table;
2. valid core matrix renders ordered isolate rows;
3. ratios and speedups use correct arithmetic and formatting;
4. dirty input is rejected by default;
5. `--allow-dirty` labels the section;
6. mixed hardware metadata is rejected;
7. missing required row is reported with the exact selection;
8. duplicate ambiguous row is rejected;
9. failed or signaled sample is rejected;
10. one-sample evidence is labeled diagnostic;
11. generated markers preserve surrounding prose;
12. `--check` succeeds when current and fails when stale;
13. unsupported schema version fails clearly;
14. reordered JSON configs produce deterministic Markdown;
15. non-finite or malformed metrics fail rather than rendering `NaN`.

Add the tests and a summary freshness check to `just check` if fixtures make the
check deterministic and fast.

## Documentation and workflow

Update `benchmarks/README.md` with the exact flow:

1. run selected benchmarks into ignored local JSON;
2. inspect failures and metadata;
3. run the summarizer;
4. review the Markdown diff;
5. run summarizer `--check` and `just check`;
6. commit only Markdown and any intentional baseline metadata.

Add a `just benchmark-summary` recipe and a check recipe if that matches project
conventions.

## Acceptance criteria

- Measured tables and derived ratios in `SCALING.md` are generated from result
  JSON rather than manually copied.
- The summary includes commit, sample count, hardware, and workload provenance.
- Dirty, incomplete, ambiguous, or incompatible input fails safely by default.
- Generated output is deterministic.
- `--check` detects stale checked-in content without modifying files.
- Small committed fixtures provide comprehensive Node tests.
- Raw result JSON remains ignored and untracked.
- `just check` passes.

## Non-goals

- Storing full raw results in Git.
- Uploading benchmark results to a service.
- Automatically deciding whether a performance change is acceptable; task 009
  handles regression policy.
- Generating charts or images as part of the required implementation.

## Implementation cautions

- Result files can be tens of thousands of lines. Standard JSON parsing is
  acceptable at current sizes, but avoid needless copies of raw stdout/stderr
  when normalizing records.
- Preserve integer throughput values and decimal latency precision from the
  summaries; do not recompute medians from rounded stdout text.
- Never silently choose the first row when multiple rows match.
