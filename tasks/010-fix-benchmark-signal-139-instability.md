# Isolate and fix the benchmark signal-139 instability

**Priority:** P2 reliability  
**Primary area:** `crates/runtime/src/bin/bench.rs`, runtime shutdown/lifecycle, benchmark orchestration  
**Dependencies:** none

## Problem

The benchmark documentation states that the complete
`cargo run -p runtime --bin bench --release` suite is not used as a gate because
previous runs ended with signal 139. Signal 139 normally indicates a segmentation
fault, which is especially concerning in a runtime embedding V8 and managing
multiple isolate threads.

The current benchmark binary runs many scenarios and runtime configurations in
one process. Even if each individual scenario succeeds, repeated runtime
creation, deployment, isolate startup/shutdown, websocket/dynamic work, and V8
teardown may expose lifecycle bugs or unsupported multiple-initialization
patterns.

Leaving the crash undocumented beyond one README note has two costs:

- the broad benchmark suite cannot serve as a reliable validation tool;
- a real runtime teardown/use-after-free defect could also affect tests,
  development servers, or process shutdown in production.

## Objective

Produce a deterministic reproduction or narrow the failure to a specific
lifecycle combination, capture useful crash diagnostics, fix the underlying
runtime defect when feasible, and make the complete benchmark command reliable.

If an upstream V8/deno_core limitation requires process isolation, implement a
robust subprocess orchestrator and document the boundary rather than silently
accepting crashes.

## Phase 1: make reproduction observable

### Add scenario selection and ordering controls

Extend the benchmark CLI or environment handling so every logical section and
configuration can be run independently and in a specified sequence. Existing
`DD_BENCH_ONLY` is section-level; add enough granularity to select:

- one runtime config;
- one scenario;
- lifecycle/cold-start subcases;
- repetitions;
- a deterministic seed/order.

Add `--list` or equivalent output enumerating exact scenario IDs.

### Add lifecycle progress markers

Before and after each major operation, emit a machine-parseable marker with:

- scenario ID;
- runtime config;
- iteration;
- deploy/start/invoke/shutdown phase;
- process ID;
- active isolate count if available.

Flush stdout/stderr after markers so the last completed phase survives a crash.

### Capture crash diagnostics

Provide a documented reproduction command that enables:

- Rust backtraces;
- core dumps where supported;
- AddressSanitizer or another sanitizer build if compatible with V8 artifacts;
- debugger invocation (`gdb`/`lldb`) with symbols;
- thread names and V8 fatal-error output;
- Linux `ulimit -c` guidance.

Add a script under `scripts/` that runs the selected benchmark repeatedly until
failure and archives the last output and core metadata under an ignored temp
path.

Do not commit core files or raw crash dumps.

## Phase 2: bisect the lifecycle

Use the new selectors to answer:

- Does one scenario crash in a fresh process?
- Does the crash require running two or more scenarios sequentially?
- Does it require multiple `RuntimeService` instances in one process?
- Does it require dynamic workers, websocket/transport sessions, or snapshots?
- Does explicit `RuntimeService::shutdown` change the result?
- Does waiting for isolate threads to join change the result?
- Is the crash during benchmark work, runtime shutdown, Tokio shutdown, or
  process-global V8 teardown?
- Does it reproduce in debug, release, or sanitizer builds?

Record the minimal sequence in a test or an issue-style Markdown note in the
fixing pull request.

## Likely areas to inspect

Do not assume the cause, but inspect these ownership boundaries carefully:

- `spawn_runtime_thread` creates a detached runtime manager thread and returns
  without a join handle;
- isolate threads are also spawned and commanded to shut down asynchronously;
- shutdown paths frequently use `try_send(IsolateCommand::Shutdown)` without
  joining the thread;
- pending Tokio tasks may hold V8-related state when a runtime is dropped;
- global deno_core/V8 initialization or platform teardown may not support the
  observed create/drop sequence;
- worker source/snapshot backing memory may be released before every isolate has
  stopped using it;
- dynamic reply callbacks and event-loop wakers may outlive their isolate.

Use evidence from the crash stack before making broad changes.

## Required fix quality

### Preferred: correct thread ownership and shutdown

If detached-thread teardown is the cause:

- retain join handles for the runtime manager and isolate threads;
- add an explicit, idempotent shutdown protocol;
- stop accepting new commands;
- cancel or finish pending requests with errors;
- close channels in a defined order;
- signal isolate shutdown;
- join isolate threads;
- drain/stop background outbox or dynamic tasks;
- join the runtime manager thread;
- only then release snapshots, source modules, V8 handles, and shared state.

Expose shutdown through `RuntimeService` and use it in benchmarks/tests. A `Drop`
implementation may initiate best-effort shutdown, but code requiring deterministic
teardown should have an awaitable method.

Avoid blocking an async runtime thread on a join that needs that same runtime to
make progress. Use a dedicated blocking join or well-defined ownership layer.

### Acceptable fallback: process-isolated orchestrator

If the embedded runtime cannot safely be created/destroyed repeatedly in one
process due to an upstream limitation:

- make the top-level benchmark command an orchestrator;
- run each scenario/config in a fresh child process;
- preserve the current human-readable table and machine-readable result format;
- propagate child failures/signals accurately;
- add timeouts and captured output;
- document why process isolation is required and link to an upstream issue or
  minimal reproduction;
- keep a small in-process lifecycle test to detect when the limitation is fixed.

Do not merely wrap the entire existing suite in one child process; isolate the
minimum unit that prevents state leakage.

## Tests

Add tests appropriate to the identified cause. At minimum:

1. every benchmark scenario can run alone by stable ID;
2. the previously crashing minimal sequence completes repeatedly;
3. explicit shutdown is idempotent;
4. shutdown rejects or completes pending invokes deterministically;
5. runtime and isolate threads terminate within a timeout;
6. no active session or wait-until task prevents shutdown forever;
7. background outbox/dynamic tasks stop cleanly;
8. child-process orchestration reports exit code and signal correctly if used;
9. a deliberately crashing fixture is reported as a signal failure, not a
   successful empty result;
10. repeated create/run/shutdown cycles complete under a stress test;
11. tests do not leave orphan `dd-runtime` or `dd-isolate-*` threads.

A long stress test may be ignored by default and run in a dedicated CI job, but
a short regression must run in normal checks.

## CI and benchmark validation

After the fix:

- run the full release benchmark command repeatedly;
- run each major section separately;
- run under the sanitizer/debugger setup where available;
- add a scheduled stress workflow if the crash required many repetitions;
- remove or update the README statement that the suite is not a gate;
- do not make performance comparisons from sanitizer builds.

Document the number of successful repetitions and the environment used.

## Acceptance criteria

- A minimal reproduction and likely root cause are documented.
- The complete benchmark suite no longer exits with signal 139 across repeated
  release runs on the reproducing machine.
- Runtime/isolate shutdown ownership is deterministic, or benchmark scenarios
  are deliberately isolated in child processes with a documented upstream
  reason.
- Failures and signals are captured accurately.
- Normal checks include a short regression for the fixed lifecycle sequence.
- The obsolete instability warning is removed or replaced with precise current
  guidance.
- `just check` passes.

## Non-goals

- Optimizing benchmark throughput.
- Rewriting all runtime thread architecture without crash evidence.
- Committing core dumps or platform-specific debugger output.
- Hiding a continuing crash by ignoring the child exit status.

## Implementation cautions

- V8 teardown bugs can become timing-sensitive when extra logging is added;
  preserve a no-logging stress mode.
- Do not call V8 APIs after the owning isolate thread has begun teardown.
- Ensure source module and snapshot lifetimes exceed all isolate users.
- Treat a workaround and a root-cause fix differently in documentation.
