# Make benchmark matrices opt-in and budgeted

**Priority:** P1 tooling and developer safety  
**Primary area:** `benchmarks/run.mjs`, benchmark config scripts, benchmark docs  
**Dependencies:** none

## Problem

`benchmarks/run.mjs` currently discovers every shell config when no `--config`
argument is supplied and then expands every `DD_BENCH_MATRIX_*` dimension.

The atomic scaling config alone expands to:

```text
6 isolate counts
× 7 shard counts
× 3 key distributions
× 3 workloads
= 378 config variants
```

At five samples that is 1,890 child-process runs. The generic README example
uses the runner without a config filter, so a developer can accidentally launch
a very large benchmark job while expecting the historical small suite.

The runner also begins execution immediately. It does not print a complete plan,
variant count, or safety warning before starting.

## Objective

Make large matrix expansion explicit, inspectable, and bounded. Preserve the
ability to run the full matrix intentionally while making the default command
safe.

## Required behavior

### 1. Mark matrix configs as opt-in

Introduce an explicit marker in config scripts, for example:

```bash
DD_BENCH_MATRIX_OPT_IN=1
```

When the runner is invoked with no `--config` filters:

- discover normal benchmark configs;
- skip configs marked matrix opt-in;
- print a concise note listing skipped opt-in configs;
- do not expand or run them.

When a matrix config is explicitly selected with `--config`, expand it normally.

Do not rely only on a filename prefix such as `scaling-`; use a declarative
marker so future large configs are safe too.

### 2. Add a planning mode

Add `--plan` or `--dry-run`. It must:

- perform config discovery and matrix expansion;
- print every selected variant name and relevant environment overrides;
- print the number of variants, samples per variant, and total child-process
  runs;
- print the output path that would be used;
- exit without spawning benchmark processes or writing a result file.

`--list` may remain a compact name-only view, but document the distinction.

### 3. Add a run budget

Add a maximum child-process run budget. A suggested interface is:

```text
--max-runs N
--allow-large-run
```

Requirements:

- calculate `expanded_variants × samples` before execution;
- use a conservative default maximum, such as 100 runs;
- if the plan exceeds the budget, fail with a message showing the calculation
  and how to narrow dimensions or explicitly allow it;
- explicit full-matrix runs remain possible through `--allow-large-run` or a
  sufficiently large `--max-runs`;
- environment variables may configure the same values for automation;
- invalid, zero, negative, or non-integer values fail clearly.

Do not silently truncate a matrix.

### 4. Validate dimensions

Before running, validate:

- isolate and shard dimensions are positive integers;
- key modes are recognized by the selected benchmark binary;
- workload modes are known benchmark modes;
- duplicate values are removed while preserving input order;
- conflicting overrides are reported rather than producing misleading variant
  names;
- an empty selected dimension produces a clear error when a matrix marker is
  present.

The runner should not need to compile Rust to validate common mistakes.

### 5. Improve failure reporting

When a child process fails:

- stop the run by default rather than continuing through hundreds of variants;
- write the partial JSON result if an output path was supplied, marked
  `complete: false` with failure metadata;
- print the failed variant name, sample number, exit status/signal, and path to
  captured output;
- optionally add `--keep-going` for deliberate batch collection.

Maintain backward compatibility for existing successful result schema fields;
add a schema version bump if necessary.

## Refactoring for testability

Move argument parsing, environment parsing, matrix expansion, and plan
calculation into exported pure functions in a module such as
`benchmarks/lib/runner-config.mjs`.

Keep `benchmarks/run.mjs` as a thin executable wrapper. Avoid testing by spawning
hundreds of real shell scripts.

## Tests

Use Node's built-in `node:test` and temporary fixture configs. Cover at least:

1. no `--config` skips opt-in matrix configs;
2. explicit selection includes and expands an opt-in matrix;
3. the current atomic matrix expands to exactly 378 variants;
4. five samples produce a plan of 1,890 runs;
5. default run budget rejects that plan;
6. explicit override permits it;
7. `--plan` spawns no child processes;
8. environment overrides narrow individual dimensions;
9. duplicate dimension values are deduplicated in stable order;
10. invalid numeric dimensions fail before any process starts;
11. invalid key/workload modes fail clearly;
12. non-matrix configs remain one variant;
13. a failing sample produces a partial result with failure metadata;
14. `--keep-going` continues while preserving failures;
15. output naming is deterministic and variant names remain unambiguous.

Add the Node tests to `just check` or `check-js` without requiring a release Rust
build.

## Documentation updates

Update `benchmarks/README.md` to show:

- a safe default/small-suite command;
- `--plan` before a full matrix;
- explicit `--allow-large-run` or `--max-runs` for the complete matrix;
- examples for narrowing dimensions;
- expected variant/run counts for the default atomic matrix;
- partial-result behavior after failure.

Update `benchmarks/SCALING.md` regeneration commands accordingly.

## Acceptance criteria

- Running `node benchmarks/run.mjs --samples 3` does not execute the large atomic
  matrix.
- Explicitly selecting the matrix still supports the full 378 variants.
- The runner prints or can print the exact plan before execution.
- A default run budget prevents accidental 1,000+ process runs.
- Pure-function Node tests cover discovery, expansion, budgeting, validation,
  and failure output.
- Existing small benchmark configs continue to run unchanged.
- `just check` passes.

## Non-goals

- Parallelizing benchmark child processes.
- Uploading results to an external service.
- Defining performance pass/fail thresholds; that is task 009.
- Changing runtime benchmark semantics.

## Implementation cautions

- Shell scripts use default-value syntax such as `${VAR:-value}`; preserve the
  current parser support.
- Environment overrides supplied by the invoking shell must continue to take
  precedence over script defaults.
- Avoid evaluating arbitrary shell code while planning. Continue parsing the
  declarative assignments rather than sourcing scripts in the Node process.
- A full matrix may intentionally take a long time; safety checks should be
  explicit, not paternalistic or impossible to override.
