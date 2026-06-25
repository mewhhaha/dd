# Implementation tasks

These task files describe concrete improvements found during a review of current
`main`. Each file is written so it can be handed directly to a GPT-5.5-medium
implementation agent without relying on conversational context.

## Recommended order

| Order | Task | Priority | Reason |
| ---: | --- | --- | --- |
| 1 | [Persist and validate the memory shard layout](001-persist-and-validate-memory-shard-layout.md) | P0 correctness | Prevents a shard-count configuration change from silently making persisted memory appear missing. |
| 2 | [Remove intra-shard head-of-line blocking](002-remove-intra-shard-head-of-line-blocking.md) | P0 performance | Lets independent entity IDs progress even when they share one physical storage shard. |
| 3 | [Move outbox storage I/O off the runtime manager](003-move-outbox-io-off-runtime-manager.md) | P1 performance | Prevents durable-effect persistence work from blocking all runtime scheduling. |
| 4 | [Deduplicate memory database initialization without holding shard locks across I/O](004-deduplicate-memory-database-initialization.md) | P1 performance | Removes cold-open lock convoys and duplicate per-thread database objects where safe. |
| 5 | [Replace full-map cache pruning with bounded LRU caches](005-use-bounded-lru-memory-caches.md) | P1 performance/memory | Makes cache operations predictable and enforces real process-wide budgets. |
| 6 | [Make benchmark matrices opt-in and budgeted](006-make-benchmark-matrices-opt-in.md) | P1 tooling | Avoids accidentally launching hundreds or thousands of benchmark processes. |
| 7 | [Generate the scaling summary from benchmark output](007-generate-scaling-summary.md) | P1 tooling | Prevents checked-in performance documentation from drifting from the measured data. |
| 8 | [Add memory scheduler observability](008-add-memory-scheduler-observability.md) | P1 operations | Makes queue contention, affinity, leases, and manager-loop saturation diagnosable. |
| 9 | [Add non-flaky performance regression gates](009-add-performance-regression-gates.md) | P2 quality | Converts the demonstrated sharding invariants into repeatable checks without fragile absolute RPS thresholds. |
| 10 | [Isolate and fix the benchmark signal-139 instability](010-fix-benchmark-signal-139-instability.md) | P2 reliability | Restores the complete runtime benchmark suite as a usable gate. |
| 11 | [Scale runtime scheduling beyond one manager thread](011-scale-runtime-manager-beyond-one-thread.md) | P2 architecture | Removes the next structural ceiling after shard-local scheduling and outbox improvements are measured. |
| 12 | [Add an explicit memory resharding tool](012-add-memory-shard-resharding-tool.md) | P2 operations | Provides a safe path to intentionally change shard counts after layout validation exists. |
| 13 | [Capture and analyze full shard-matrix findings](013-capture-full-shard-matrix-findings.md) | P2 performance analysis | Turns the full shard-count sweep into durable guidance instead of only a 16-shard slice. |
| 14 | [Harden explicit runtime service shutdown](014-harden-runtime-service-shutdown.md) | P2 reliability | Makes tests, benchmarks, and embedded usage less dependent on best-effort process teardown. |

## Task execution rules

- Implement one task per pull request unless a task explicitly lists another as a dependency.
- Preserve public keyed-memory semantics: atomic calls for the same binding and entity ID must remain ordered and must never execute concurrently.
- Add focused unit or integration coverage for every behavior change.
- Run `just check` before requesting review unless the task documents a narrower reason that the full check cannot run.
- Performance claims must identify the benchmark configuration, sample count, commit, hardware, and whether the worktree was clean.
- Do not commit raw files under `benchmarks/results/`; update durable summaries instead.
