# Benchmark Log

Canonical continuous benchmark history for hardening work. New focused runs should be appended here with command, environment, and result.

## Environment

- Date: `2026-06-18T20:32:16+02:00`
- OS: `Linux cachyos-x8664 7.0.9-1-cachyos #1 SMP PREEMPT_DYNAMIC Sun, 17 May 2026 16:56:12 +0000 x86_64 GNU/Linux`
- Logical CPUs: `16`
- Rust: `rustc 1.96.0-nightly (3645249d7 2026-03-16)`
- Cargo: `cargo 1.96.0-nightly (cbb9bb8bd 2026-03-13)`

## 2026-06-18 - MemoryRoute Owner-Key Cache And Owner-Map Cleanup

Change:

- `MemoryRoute` now stores its owner/inflight key when constructed.
- Dispatch, debug dumps, websocket wakes, and transport wakes reuse that key instead of rebuilding it.
- Removed the dead `memory_owners` debug/runtime map and `assign_owner` flag; dispatch never set the flag, so the map stayed empty.

Commands:

```bash
DD_BENCH_MODE=direct-write-memory DD_BENCH_REQUESTS=300 DD_BENCH_CONCURRENCY=16 DD_BENCH_MAX_ISOLATES=4 DD_BENCH_MAX_INFLIGHT=8 cargo run -p runtime --bin bench_memory_storage --release
DD_BENCH_MODE=direct-read-memory DD_BENCH_REQUESTS=300 DD_BENCH_CONCURRENCY=16 DD_BENCH_MAX_ISOLATES=4 DD_BENCH_MAX_INFLIGHT=8 cargo run -p runtime --bin bench_memory_storage --release
```

Results:

| scenario | requests | concurrency | isolates | max inflight | total ms | throughput req/s | mean ms | p50 ms | p95 ms | p99 ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| memory-direct-write-memory | 300 | 16 | 1-4 | 8 | 139.86 | 2145 | 7.37 | 6.69 | 13.11 | 17.53 |
| memory-direct-read-memory | 300 | 16 | 1-4 | 8 | 26.41 | 11358 | 1.38 | 0.76 | 4.07 | 13.05 |

Harness issue:

- Fixed in the next harness pass below.

## 2026-06-18 - Benchmark Harness Help And Mode Validation

Change:

- `bench_memory_storage --help` now prints help and exits without running scenarios.
- Invalid `DD_BENCH_MODE` values now fail before runtime service startup.
- Scenario execution, help text, and mode validation now share one `BENCH_CASES` table.
- Added unit tests for benchmark arg handling, scenario uniqueness, and mode matching.

Verification:

```bash
cargo test -p runtime --bin bench_memory_storage
cargo run -p runtime --bin bench_memory_storage -- --help
DD_BENCH_MODE=not-a-mode cargo run -p runtime --bin bench_memory_storage
DD_BENCH_MODE=direct-read-memory DD_BENCH_REQUESTS=50 DD_BENCH_CONCURRENCY=4 DD_BENCH_MAX_ISOLATES=2 DD_BENCH_MAX_INFLIGHT=4 cargo run -p runtime --bin bench_memory_storage --release
```

Smoke result:

| scenario | requests | concurrency | isolates | max inflight | total ms | throughput req/s | mean ms | p50 ms | p95 ms | p99 ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| memory-direct-read-memory | 50 | 4 | 1-2 | 4 | 13.95 | 3585 | 1.09 | 0.66 | 2.66 | 2.66 |

## 2026-06-18 - Dispatch Selection Cleanup

Change:

- Simplified targeted nested dispatch selection by removing a trivially true capacity branch.
- Regular targeted dispatch now reads directly as capacity plus wait-until-idle gating.
- Removed the debug-only `memory_inflight` map from worker pools; per-request `memory_key` remains in debug dumps.
- Made dispatch candidate selection read-only over `WorkerPool` and centralized targeted-isolate lookup.
- Collapsed worker-pool activity/stat totals into one isolate pass.
- Removed an unreachable targeted-nested branch and memory-key clone from the regular targeted dispatch path.
- Extracted `WorkerPool::has_dispatch_capacity` to keep strict-isolation capacity checks in one place.

Verification:

```bash
cargo test -p runtime dynamic_test_nested_targeted_invoke
cargo test -p runtime service::tests::memory::
cargo test -p runtime service::tests::sessions::
cargo test -p runtime dynamic_
cargo test -p runtime --bin bench_memory_storage
cargo test -p runtime scales_
cargo test -p runtime wait_until
DD_BENCH_MODE=direct-read-memory DD_BENCH_REQUESTS=50 DD_BENCH_CONCURRENCY=4 DD_BENCH_MAX_ISOLATES=2 DD_BENCH_MAX_INFLIGHT=4 cargo run -p runtime --bin bench_memory_storage --release
```

Smoke result:

| scenario | requests | concurrency | isolates | max inflight | total ms | throughput req/s | mean ms | p50 ms | p95 ms | p99 ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| memory-direct-read-memory | 50 | 4 | 1-2 | 4 | 12.27 | 4075 | 0.96 | 0.66 | 2.46 | 2.46 |

## 2026-06-18 - Execute Command Builder And RPC Binding Name Cache

Change:

- Added `WorkerPool::build_execute_command` so normal dispatch and direct dynamic fetch share one execute-command setup path.
- Cached dynamic RPC binding names on each pool, removing per-dispatch map/collect work while keeping provider metadata intact.
- Removed `memory_key` from `DispatchCandidate`; pending replies now derive memory debug metadata once from the owned queued invoke.

Verification:

```bash
cargo test -p runtime --no-run
cargo test -p runtime dynamic_test_nested_targeted_invoke
cargo test -p runtime service::tests::memory::
cargo test -p runtime service::tests::sessions::
cargo test -p runtime --bin bench_memory_storage
DD_BENCH_MODE=direct-read-memory DD_BENCH_REQUESTS=50 DD_BENCH_CONCURRENCY=4 DD_BENCH_MAX_ISOLATES=2 DD_BENCH_MAX_INFLIGHT=4 cargo run -p runtime --bin bench_memory_storage --release
DD_BENCH_MODE=direct-read-memory DD_BENCH_REQUESTS=300 DD_BENCH_CONCURRENCY=16 DD_BENCH_MAX_ISOLATES=4 DD_BENCH_MAX_INFLIGHT=8 cargo run -p runtime --bin bench_memory_storage --release
```

Smoke result:

| scenario | requests | concurrency | isolates | max inflight | total ms | throughput req/s | mean ms | p50 ms | p95 ms | p99 ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| memory-direct-read-memory | 50 | 4 | 1-2 | 4 | 36.74 | 1361 | 2.93 | 2.60 | 5.48 | 5.48 |
| memory-direct-read-memory | 300 | 16 | 1-4 | 8 | 42.11 | 7124 | 2.15 | 1.42 | 4.32 | 17.99 |

Note:

- Nearby repeats were noisy on this machine: the 50-request smoke ranged from 35.39ms to 41.46ms before the final 36.74ms run, and the 300-request smoke ran at 68.67ms before the final 42.11ms run.

## 2026-06-18 - Shared Request Execution Context

Change:

- Added a shared `RequestExecutionContext` built once per worker pool.
- `IsolateCommand::Execute` now carries the shared context instead of raw dynamic binding, host RPC binding, secret replacement, egress, cache, and quota fields.
- `register_request_secret_context` installs pre-normalized sets/maps instead of rebuilding them for every request.
- Host fetch preparation and URL checks now clone the shared context rather than replacement maps and allowlists.

Verification:

```bash
cargo test -p runtime --no-run
cargo test -p runtime dynamic_
cargo test -p runtime service::tests::memory::
cargo test -p runtime service::tests::sessions::
cargo test -p runtime --bin bench_memory_storage
DD_BENCH_MODE=direct-read-memory DD_BENCH_REQUESTS=300 DD_BENCH_CONCURRENCY=16 DD_BENCH_MAX_ISOLATES=4 DD_BENCH_MAX_INFLIGHT=8 cargo run -p runtime --bin bench_memory_storage --release
```

Smoke result:

| scenario | requests | concurrency | isolates | max inflight | total ms | throughput req/s | mean ms | p50 ms | p95 ms | p99 ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| memory-direct-read-memory | 300 | 16 | 1-4 | 8 | 39.37 | 7620 | 1.99 | 1.19 | 4.66 | 18.41 |

## 2026-06-18 - Move Execute Call Payloads

Change:

- Converted `MemoryExecutionCall` and `HostRpcExecutionCall` to engine execute payloads by move instead of by clone.
- Removed unused `Clone` derives from the service-side call payloads and engine-side execute payloads.

Verification:

```bash
cargo test -p runtime --no-run
cargo test -p runtime service::tests::memory::
cargo test -p runtime service::tests::sessions::
cargo test -p runtime dynamic_
cargo test -p runtime --bin bench_memory_storage
DD_BENCH_MODE=direct-write-memory DD_BENCH_REQUESTS=300 DD_BENCH_CONCURRENCY=16 DD_BENCH_MAX_ISOLATES=4 DD_BENCH_MAX_INFLIGHT=8 cargo run -p runtime --bin bench_memory_storage --release
DD_BENCH_MODE=direct-read-memory DD_BENCH_REQUESTS=300 DD_BENCH_CONCURRENCY=16 DD_BENCH_MAX_ISOLATES=4 DD_BENCH_MAX_INFLIGHT=8 cargo run -p runtime --bin bench_memory_storage --release
```

Results:

| scenario | requests | concurrency | isolates | max inflight | total ms | throughput req/s | mean ms | p50 ms | p95 ms | p99 ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| memory-direct-write-memory | 300 | 16 | 1-4 | 8 | 142.43 | 2106 | 7.39 | 6.70 | 12.85 | 17.19 |
| memory-direct-read-memory | 300 | 16 | 1-4 | 8 | 21.10 | 14216 | 1.11 | 0.64 | 4.45 | 12.59 |

## 2026-06-18 - Remove Engine Execute Call Mirrors

Change:

- Removed duplicate engine-side execute call structs.
- Made service-side `MemoryExecutionCall` and `HostRpcExecutionCall` serialize directly with the JS dispatch JSON shape.
- Changed `dispatch_worker_request` to accept generic serializable call payloads.
- Removed the isolate-thread conversion match that repackaged service payloads into engine payloads.

Verification:

```bash
cargo test -p runtime --no-run
cargo test -p runtime service::tests::memory::
cargo test -p runtime service::tests::sessions::
cargo test -p runtime dynamic_
cargo test -p runtime --bin bench_memory_storage
DD_BENCH_MODE=direct-write-memory DD_BENCH_REQUESTS=300 DD_BENCH_CONCURRENCY=16 DD_BENCH_MAX_ISOLATES=4 DD_BENCH_MAX_INFLIGHT=8 cargo run -p runtime --bin bench_memory_storage --release
DD_BENCH_MODE=direct-read-memory DD_BENCH_REQUESTS=300 DD_BENCH_CONCURRENCY=16 DD_BENCH_MAX_ISOLATES=4 DD_BENCH_MAX_INFLIGHT=8 cargo run -p runtime --bin bench_memory_storage --release
```

Results:

| scenario | requests | concurrency | isolates | max inflight | total ms | throughput req/s | mean ms | p50 ms | p95 ms | p99 ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| memory-direct-write-memory | 300 | 16 | 1-4 | 8 | 140.33 | 2138 | 7.39 | 6.99 | 12.89 | 16.19 |
| memory-direct-read-memory | 300 | 16 | 1-4 | 8 | 25.25 | 11881 | 1.33 | 0.88 | 3.83 | 13.81 |

## 2026-06-18 - Request Scope Registration Cleanup

Change:

- Removed an unconditional `request.request_id` clone from isolate execute handling.
- Moved `MemoryRoute` into memory request scope registration instead of cloning its binding/key.
- Registered request body streams, memory scopes, and request secret contexts under one mutable `OpState` borrow.

Verification:

```bash
cargo test -p runtime --no-run
cargo test -p runtime service::tests::memory::
cargo test -p runtime service::tests::sessions::
cargo test -p runtime dynamic_
cargo test -p runtime --bin bench_memory_storage
DD_BENCH_MODE=direct-write-memory DD_BENCH_REQUESTS=300 DD_BENCH_CONCURRENCY=16 DD_BENCH_MAX_ISOLATES=4 DD_BENCH_MAX_INFLIGHT=8 cargo run -p runtime --bin bench_memory_storage --release
DD_BENCH_MODE=direct-read-memory DD_BENCH_REQUESTS=300 DD_BENCH_CONCURRENCY=16 DD_BENCH_MAX_ISOLATES=4 DD_BENCH_MAX_INFLIGHT=8 cargo run -p runtime --bin bench_memory_storage --release
```

Results:

| scenario | requests | concurrency | isolates | max inflight | total ms | throughput req/s | mean ms | p50 ms | p95 ms | p99 ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| memory-direct-write-memory | 300 | 16 | 1-4 | 8 | 140.23 | 2139 | 7.33 | 6.70 | 13.08 | 17.46 |
| memory-direct-read-memory | 300 | 16 | 1-4 | 8 | 24.87 | 12062 | 1.31 | 0.90 | 3.75 | 12.76 |

## 2026-06-18 - Session Registration Ownership

Change:

- Session registration now moves websocket and transport session records into the session maps instead of cloning them.
- Websocket and transport wake paths snapshot only the fields needed to enqueue memory wakes instead of cloning whole session records.

Verification:

```bash
cargo test -p runtime --no-run
cargo test -p runtime service::tests::sessions::
cargo test -p runtime service::tests::memory::
cargo test -p runtime --bin bench_memory_storage
```

Benchmark note:

- No numeric benchmark was recorded for this slice. The existing keyed-memory benchmark does not exercise websocket/transport session registration or wake-path session lookup.

## 2026-06-18 - Trace Completion Metadata

Change:

- Internal trace forwarding now snapshots compact status/error metadata instead of cloning the full `Result<WorkerOutput>`.
- This avoids copying response headers and bodies for trace events that only serialize status/error.

Verification:

```bash
cargo test -p runtime --no-run
cargo test -p runtime internal_trace_
cargo test -p runtime --bin bench_memory_storage
just check
```

Benchmark note:

- No numeric benchmark was recorded for this slice. The keyed-memory benchmark does not exercise internal trace forwarding.

## 2026-06-18 - Stream Completion Ownership

Change:

- Added a dedicated stream pending-reply kind for `invoke_stream`.
- Stream completions now move the owned `Result<WorkerOutput>` into stream registration instead of cloning it for the registration path while sending the original into a discarded reply channel.
- The remaining `result.clone()` in stream completion is limited to a defensive fallback for non-stream reply kinds with a stream registration.
- Stream fallback completion now moves output headers/body and errors directly into their single consumers.

Verification:

```bash
cargo test -p runtime invoke_stream_delivers_chunked_response_body
cargo test -p runtime --no-run
DD_BENCH_MODE=direct-read-memory DD_BENCH_REQUESTS=300 DD_BENCH_CONCURRENCY=16 DD_BENCH_MAX_ISOLATES=4 DD_BENCH_MAX_INFLIGHT=8 cargo run -p runtime --bin bench_memory_storage --release
```

Smoke result:

| scenario | requests | concurrency | isolates | max inflight | total ms | throughput req/s | mean ms | p50 ms | p95 ms | p99 ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| memory-direct-read-memory | 300 | 16 | 1-4 | 8 | 48.39 | 6199 | 2.55 | 1.77 | 6.73 | 18.55 |

Benchmark note:

- The keyed-memory benchmark does not exercise stream response completion. The numeric run keeps the standing direct-read smoke series current after the refactor.
- An adjacent current-tree run of the same command measured `52.62ms` / `5702 req/s`, so this entry should be treated as a noisy smoke check rather than a stream-path performance claim.

## 2026-06-18 - Dynamic Handle Dispatch Snapshot

Change:

- Replaced whole `DynamicWorkerHandle` clones in dynamic lookup/delete/invoke/fetch with narrow field snapshots.
- Added a `DynamicDispatchTarget` snapshot for invoke/fetch and changed inflight acquisition to take quota state plus max concurrency directly.
- Centralized dynamic handle owner/generation/binding validation for invoke and fetch.
- Replaced dynamic host RPC provider clones with a narrow owner/target snapshot after method membership is checked.

Verification:

```bash
cargo test -p runtime dynamic_
DD_BENCH_ONLY=dynamic DD_BENCH_DYNAMIC_REQUESTS=100 DD_BENCH_DYNAMIC_CONCURRENCY=16 DD_BENCH_DYNAMIC_COLD_ROUNDS=10 cargo run -p runtime --bin bench --release
```

Smoke result:

| config | scenario | requests/samples | concurrency | total ms | throughput req/s | mean ms | p50 ms | p95 ms | p99 ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| single-isolate | dynamic-baseline | 100 | 16 | 217.56 | 460 | 32.10 | 34.73 | 36.35 | 36.38 |
| single-isolate | dynamic-hot-fetch | 100 | 16 | 459.97 | 217 | 68.32 | 71.94 | 81.23 | 83.32 |
| single-isolate | dynamic-hot-fetch-host-rpc | 100 | 16 | 674.68 | 148 | 99.82 | 108.42 | 113.25 | 115.31 |
| single-isolate | dynamic-create+invoke | 10 | - | - | - | 17.38 | 17.22 | 19.05 | 19.05 |
| autoscaling-8 | dynamic-baseline | 100 | 16 | 24.02 | 4163 | 3.68 | 1.02 | 19.07 | 21.23 |
| autoscaling-8 | dynamic-hot-fetch | 100 | 16 | 30.64 | 3264 | 4.53 | 4.65 | 6.53 | 6.82 |
| autoscaling-8 | dynamic-hot-fetch-host-rpc | 100 | 16 | 49.84 | 2007 | 7.46 | 6.91 | 8.89 | 10.90 |
| autoscaling-8 | dynamic-create+invoke | 10 | - | - | - | 15.41 | 14.93 | 18.96 | 18.96 |

Dynamic metrics highlights:

- `single-isolate` direct fetch dispatch mean stayed around `0.001ms` for hot fetch and host RPC.
- `autoscaling-8` hot fetch recorded `60` direct fast-path hits and `40` fallbacks; host RPC recorded `63` hits and `37` fallbacks.

## 2026-06-18 - Benchmark CLI Guards

Change:

- Added `--help` / `-h` handling to `bench_fetch_fast`.
- Added the same help/invalid-argument guard to `bench`, `bench_kv`, and `bench_kv_store`.
- Unknown CLI flags now fail before any benchmark scenarios start.
- Added unit coverage for help, empty args, and unknown arg handling.
- Extracted the duplicated CLI parser into shared benchmark support used by all guarded benchmark binaries.

Verification:

```bash
cargo test -p runtime --bins bench_arg_action
cargo run -p runtime --bin bench_fetch_fast -- --help
cargo run -p runtime --bin bench -- --help
cargo run -p runtime --bin bench_kv -- --help
cargo run -p runtime --bin bench_kv_store -- --help
cargo run -p runtime --bin bench_fetch_fast -- --requests 10  # expected unsupported-argument failure
cargo run -p runtime --bin bench -- --requests 10             # expected unsupported-argument failure
cargo run -p runtime --bin bench_kv -- --requests 10          # expected unsupported-argument failure
cargo run -p runtime --bin bench_kv_store -- --requests 10    # expected unsupported-argument failure
DD_BENCH_REQUESTS=20 DD_BENCH_CONCURRENCY=4 DD_BENCH_AUTOSCALING_ISOLATES=2 DD_BENCH_PREWARMED_ISOLATES=2 cargo run -p runtime --bin bench_fetch_fast --release
DD_BENCH_REQUESTS=20 DD_BENCH_CONCURRENCY=4 DD_BENCH_WRITE_REQUESTS=20 DD_BENCH_WRITE_CONCURRENCY=4 DD_BENCH_AUTOSCALING_ISOLATES=2 cargo run -p runtime --bin bench_kv --release
DD_BENCH_REQUESTS=100 DD_BENCH_CONCURRENCY=4 cargo run -p runtime --bin bench_kv_store --release
```

Representative fast-fetch smoke result:

| config | scenario | requests | concurrency | throughput req/s | mean ms | p50 ms | p95 ms | p99 ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| single-isolate | instant-text | 20 | 4 | 1317 | 3.03 | 1.11 | 10.45 | 10.45 |
| single-isolate | instant-text-kv-read | 20 | 4 | 1705 | 2.34 | 2.52 | 3.34 | 3.34 |
| single-isolate | instant-text-kv-write | 20 | 4 | 1244 | 3.21 | 1.18 | 10.93 | 10.94 |
| autoscaling-2 | instant-text | 20 | 4 | 1334 | 2.99 | 0.73 | 10.09 | 10.10 |
| autoscaling-2 | instant-text-kv-read | 20 | 4 | 2646 | 1.51 | 0.57 | 3.32 | 3.32 |
| autoscaling-2 | instant-text-kv-write | 20 | 4 | 1198 | 3.33 | 1.23 | 11.36 | 11.36 |
| prewarmed-2 | instant-text | 20 | 4 | 1216 | 3.28 | 0.73 | 11.42 | 11.43 |
| prewarmed-2 | instant-text-kv-read | 20 | 4 | 2589 | 1.54 | 0.55 | 3.48 | 3.48 |
| prewarmed-2 | instant-text-kv-write | 20 | 4 | 1264 | 3.16 | 1.18 | 10.73 | 10.73 |

Benchmark note:

- The smoke command ran all fast-fetch scenarios; the table records representative plain, KV-read, and KV-write rows to keep this log compact.

Representative KV smoke result:

| config | scenario | requests | concurrency | total ms | throughput req/s | mean ms | p50 ms | p95 ms | p99 ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| single-isolate | fetch-noop | 20 | 4 | 44.20 | 452 | 8.18 | 8.81 | 10.07 | 10.26 |
| single-isolate | kv-read | 20 | 4 | 39.44 | 507 | 7.39 | 8.83 | 9.33 | 9.35 |
| single-isolate | kv-write | 20 | 4 | 41.97 | 477 | 7.87 | 8.87 | 9.36 | 9.37 |
| autoscaling-2 | fetch-noop | 20 | 4 | 13.49 | 1483 | 2.69 | 0.64 | 9.12 | 9.12 |
| autoscaling-2 | kv-read | 20 | 4 | 6.86 | 2915 | 1.37 | 0.43 | 2.92 | 2.92 |
| autoscaling-2 | kv-write | 20 | 4 | 7.55 | 2648 | 1.50 | 0.91 | 2.88 | 2.88 |

Representative KV-store smoke result:

| scenario | requests | concurrency | total ms | throughput req/s | mean ms | p50 ms | p95 ms | p99 ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| get-utf8 | 100 | 4 | 0.63 | 157667 | 0.02 | 0.01 | 0.03 | 0.25 |
| set-utf8 | 100 | 4 | 8.29 | 12066 | 0.18 | 0.02 | 1.12 | 1.26 |

## 2026-06-18 - Host Fetch Fast Benchmark Scenario

Change:

- Added `host-fetch-local` to `bench_fetch_fast`.
- Each scenario child starts a loopback HTTP responder, deploys a dynamic worker with that responder in the egress allowlist, and measures the worker `fetch()` path.
- Updated the benchmark banner to call out that this scenario uses loopback HTTP while most fast-fetch scenarios stay inside `RuntimeService`.

Verification:

```bash
cargo fmt --check
cargo test -p runtime --bin bench_fetch_fast
cargo test -p runtime --bins bench_arg_action
DD_BENCH_INTERNAL_CONFIG=single-isolate DD_BENCH_INTERNAL_SCENARIO=host-fetch-local DD_BENCH_REQUESTS=20 DD_BENCH_CONCURRENCY=4 cargo run -p runtime --bin bench_fetch_fast --release
DD_BENCH_REQUESTS=20 DD_BENCH_CONCURRENCY=4 DD_BENCH_AUTOSCALING_ISOLATES=2 DD_BENCH_PREWARMED_ISOLATES=2 cargo run -p runtime --bin bench_fetch_fast --release
```

Representative fast-fetch smoke result:

| config | scenario | requests | concurrency | throughput req/s | mean ms | p50 ms | p95 ms | p99 ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| single-isolate | instant-text | 20 | 4 | 1612 | 2.48 | 0.68 | 9.88 | 9.89 |
| single-isolate | host-fetch-local | 20 | 4 | 558 | 7.15 | 5.06 | 15.86 | 15.87 |
| single-isolate | instant-text-kv-read | 20 | 4 | 1700 | 2.35 | 2.54 | 3.37 | 3.37 |
| single-isolate | instant-text-kv-write | 20 | 4 | 1278 | 3.12 | 1.15 | 12.01 | 12.02 |
| autoscaling-2 | instant-text | 20 | 4 | 1180 | 3.39 | 2.71 | 10.09 | 10.10 |
| autoscaling-2 | host-fetch-local | 20 | 4 | 566 | 7.06 | 5.14 | 15.68 | 15.69 |
| autoscaling-2 | instant-text-kv-read | 20 | 4 | 1664 | 2.40 | 2.49 | 3.26 | 3.26 |
| autoscaling-2 | instant-text-kv-write | 20 | 4 | 1269 | 3.15 | 1.17 | 10.51 | 10.52 |
| prewarmed-2 | instant-text | 20 | 4 | 1154 | 3.46 | 2.58 | 10.23 | 10.23 |
| prewarmed-2 | host-fetch-local | 20 | 4 | 557 | 7.16 | 4.98 | 16.00 | 16.01 |
| prewarmed-2 | instant-text-kv-read | 20 | 4 | 2007 | 1.99 | 2.48 | 2.99 | 2.99 |
| prewarmed-2 | instant-text-kv-write | 20 | 4 | 1454 | 2.74 | 0.87 | 10.72 | 10.72 |

Benchmark note:

- The smoke command ran all fast-fetch scenarios; the table records representative plain, host-fetch, KV-read, and KV-write rows to keep this log compact.

## 2026-06-18 - Fast Fetch Scenario Model

Change:

- `bench_fetch_fast` scenarios now use explicit `ScenarioWorker` and `ScenarioBinding` enums instead of three independent booleans.
- Static worker source, binding kind, and loopback host-fetch setup are represented by the scenario type, making invalid benchmark combinations unrepresentable.

Verification:

```bash
cargo fmt --check
cargo test -p runtime --bin bench_fetch_fast
cargo test -p runtime --bins bench_arg_action
DD_BENCH_REQUESTS=20 DD_BENCH_CONCURRENCY=4 DD_BENCH_AUTOSCALING_ISOLATES=2 DD_BENCH_PREWARMED_ISOLATES=2 cargo run -p runtime --bin bench_fetch_fast --release
```

Representative fast-fetch smoke result:

| config | scenario | requests | concurrency | throughput req/s | mean ms | p50 ms | p95 ms | p99 ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| single-isolate | instant-text | 20 | 4 | 1106 | 3.61 | 1.22 | 12.94 | 12.94 |
| single-isolate | host-fetch-local | 20 | 4 | 441 | 9.05 | 6.62 | 18.99 | 19.00 |
| single-isolate | instant-text-kv-read | 20 | 4 | 2808 | 1.42 | 0.84 | 3.35 | 3.36 |
| single-isolate | instant-text-kv-write | 20 | 4 | 1155 | 3.46 | 1.21 | 13.30 | 13.31 |
| autoscaling-2 | instant-text | 20 | 4 | 1086 | 3.68 | 1.07 | 14.51 | 14.51 |
| autoscaling-2 | host-fetch-local | 20 | 4 | 513 | 7.77 | 5.66 | 16.82 | 16.84 |
| autoscaling-2 | instant-text-kv-read | 20 | 4 | 2430 | 1.64 | 0.90 | 3.55 | 3.55 |
| autoscaling-2 | instant-text-kv-write | 20 | 4 | 1019 | 3.92 | 1.39 | 13.93 | 13.93 |
| prewarmed-2 | instant-text | 20 | 4 | 1040 | 3.84 | 1.18 | 14.09 | 14.09 |
| prewarmed-2 | host-fetch-local | 20 | 4 | 477 | 8.37 | 6.05 | 19.15 | 19.17 |
| prewarmed-2 | instant-text-kv-read | 20 | 4 | 1826 | 2.18 | 2.81 | 3.78 | 3.78 |
| prewarmed-2 | instant-text-kv-write | 20 | 4 | 1262 | 3.11 | 1.05 | 11.72 | 11.72 |

Benchmark note:

- This is a harness-shape cleanup; benchmark movement should be treated as normal run noise.

## 2026-06-18 - Borrowed Isolate Event Sender

Change:

- Storage/http event ops now share one local `emit_isolate_event` helper.
- The helper sends through the borrowed `IsolateEventSender` instead of cloning the `std::sync::mpsc::Sender` for completion, waitUntil, response-start, response-chunk, and cache-revalidate events.
- Completion and waitUntil cleanup remain in the op bodies before the event is emitted.

Verification:

```bash
cargo fmt --check
cargo test -p runtime --no-run
cargo test -p runtime invoke_stream_delivers_chunked_response_body
cargo test -p runtime wait_until
cargo test -p runtime internal_trace_
DD_BENCH_REQUESTS=20 DD_BENCH_CONCURRENCY=4 DD_BENCH_AUTOSCALING_ISOLATES=2 DD_BENCH_PREWARMED_ISOLATES=2 cargo run -p runtime --bin bench_fetch_fast --release
```

Representative fast-fetch smoke result:

| config | scenario | requests | concurrency | throughput req/s | mean ms | p50 ms | p95 ms | p99 ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| single-isolate | instant-text | 20 | 4 | 749 | 5.23 | 3.74 | 13.86 | 14.71 |
| single-isolate | host-fetch-local | 20 | 4 | 414 | 9.64 | 7.98 | 19.66 | 19.68 |
| single-isolate | instant-text-kv-read | 20 | 4 | 1500 | 2.65 | 2.71 | 3.83 | 3.83 |
| single-isolate | instant-text-kv-write | 20 | 4 | 843 | 4.60 | 2.04 | 17.14 | 17.15 |
| autoscaling-2 | instant-text | 20 | 4 | 811 | 4.91 | 2.86 | 14.46 | 14.46 |
| autoscaling-2 | host-fetch-local | 20 | 4 | 427 | 9.35 | 7.66 | 18.08 | 18.10 |
| autoscaling-2 | instant-text-kv-read | 20 | 4 | 1952 | 2.04 | 2.37 | 4.07 | 4.08 |
| autoscaling-2 | instant-text-kv-write | 20 | 4 | 695 | 5.70 | 3.71 | 17.71 | 17.71 |
| prewarmed-2 | instant-text | 20 | 4 | 885 | 4.50 | 2.45 | 14.21 | 14.23 |
| prewarmed-2 | host-fetch-local | 20 | 4 | 372 | 10.74 | 7.60 | 23.97 | 24.98 |
| prewarmed-2 | instant-text-kv-read | 20 | 4 | 1695 | 2.35 | 2.83 | 3.73 | 3.74 |
| prewarmed-2 | instant-text-kv-write | 20 | 4 | 927 | 4.30 | 2.75 | 13.30 | 13.31 |

Benchmark note:

- This is a tiny hot-path cleanup; the 20-request smoke matrix is kept for continuity and is too noisy to infer a throughput trend.

## 2026-06-18 - Borrowed Memory Event Sender

Change:

- Memory event ops now share one local `emit_isolate_event` helper.
- The helper sends through the borrowed `IsolateEventSender` instead of cloning the `std::sync::mpsc::Sender` for memory method invoke, socket send/close/consumeClose, and transport send/recv/close/consumeClose events.
- Existing unavailable-runtime errors and reply-channel behavior remain unchanged.

Verification:

```bash
cargo fmt --check
cargo test -p runtime --no-run
cargo test -p runtime service::tests::memory::
cargo test -p runtime service::tests::sessions::
DD_BENCH_MODE=direct-read-memory DD_BENCH_REQUESTS=50 DD_BENCH_CONCURRENCY=4 DD_BENCH_MAX_ISOLATES=2 DD_BENCH_MAX_INFLIGHT=4 cargo run -p runtime --bin bench_memory_storage --release
```

Memory storage smoke result:

| scenario | requests | concurrency | isolates | max inflight | total ms | throughput req/s | mean ms | p50 ms | p95 ms | p99 ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| memory-direct-read-memory | 50 | 4 | 1-2 | 4 | 28.14 | 1777 | 2.23 | 2.43 | 3.10 | 3.10 |

Benchmark note:

- The available memory-storage smoke keeps the memory benchmark history continuous, but it does not isolate the memory event sender helper directly.

## 2026-06-18 - Borrowed Dynamic Event Sender

Change:

- Dynamic event ops now share one local `emit_isolate_event` helper.
- The helper sends through the borrowed `IsolateEventSender` instead of cloning the `std::sync::mpsc::Sender` for test async reply, nested targeted invoke, dynamic worker lifecycle/invoke, and dynamic host RPC invoke events.
- Dynamic worker fast-fetch still uses the separate runtime fast command channel.

Verification:

```bash
cargo fmt --check
cargo test -p runtime --no-run
cargo test -p runtime dynamic_
cargo test -p runtime dynamic_test_async_reply
DD_BENCH_ONLY=dynamic DD_BENCH_DYNAMIC_REQUESTS=100 DD_BENCH_DYNAMIC_CONCURRENCY=16 DD_BENCH_DYNAMIC_COLD_ROUNDS=10 cargo run -p runtime --bin bench --release
```

Reduced dynamic benchmark:

| config | scenario | requests | concurrency | total ms | throughput req/s | mean ms | p50 ms | p95 ms | p99 ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| single-isolate | dynamic-baseline | 100 | 16 | 220.71 | 453 | 32.60 | 35.31 | 36.33 | 36.47 |
| single-isolate | dynamic-hot-fetch | 100 | 16 | 471.64 | 212 | 69.84 | 73.84 | 83.35 | 83.56 |
| single-isolate | dynamic-hot-fetch-host-rpc | 100 | 16 | 686.57 | 146 | 101.72 | 109.30 | 112.55 | 113.70 |
| autoscaling-8 | dynamic-baseline | 100 | 16 | 27.56 | 3629 | 4.11 | 1.02 | 19.56 | 23.85 |
| autoscaling-8 | dynamic-hot-fetch | 100 | 16 | 27.65 | 3617 | 4.15 | 4.61 | 6.92 | 7.07 |
| autoscaling-8 | dynamic-hot-fetch-host-rpc | 100 | 16 | 52.80 | 1894 | 7.90 | 6.95 | 13.19 | 13.34 |

Lifecycle sample:

| config | benchmark | samples | mean ms | p50 ms | p95 ms | p99 ms | max ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| single-isolate | dynamic-create+invoke | 10 | 18.00 | 18.43 | 19.23 | 19.23 | 19.23 |
| autoscaling-8 | dynamic-create+invoke | 10 | 17.02 | 16.98 | 19.08 | 19.08 | 19.08 |

Benchmark note:

- This is a mechanical event-send cleanup; benchmark movement should be treated as normal run noise.

## 2026-06-18 - Shared Isolate Event Helper

Change:

- Folded storage/http, memory, and dynamic event-send helpers into the parent ops module.
- Direct `OpState` ops call `emit_isolate_event`; Rc-backed async ops call `emit_isolate_event_from_rc`.
- Fire-and-forget storage/http events still ignore closed-channel errors, while memory and dynamic ops still return their existing unavailable-runtime errors.

Verification:

```bash
cargo fmt --check
cargo test -p runtime --no-run
cargo test -p runtime invoke_stream_delivers_chunked_response_body
cargo test -p runtime wait_until
cargo test -p runtime internal_trace_
cargo test -p runtime service::tests::memory::
cargo test -p runtime service::tests::sessions::
cargo test -p runtime dynamic_
DD_BENCH_REQUESTS=20 DD_BENCH_CONCURRENCY=4 DD_BENCH_AUTOSCALING_ISOLATES=2 DD_BENCH_PREWARMED_ISOLATES=2 cargo run -p runtime --bin bench_fetch_fast --release
DD_BENCH_ONLY=dynamic DD_BENCH_DYNAMIC_REQUESTS=100 DD_BENCH_DYNAMIC_CONCURRENCY=16 DD_BENCH_DYNAMIC_COLD_ROUNDS=10 cargo run -p runtime --bin bench --release
```

Representative fast-fetch smoke result:

| config | scenario | requests | concurrency | throughput req/s | mean ms | p50 ms | p95 ms | p99 ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| single-isolate | instant-text | 20 | 4 | 1135 | 3.52 | 2.61 | 10.72 | 10.72 |
| single-isolate | host-fetch-local | 20 | 4 | 526 | 7.59 | 5.31 | 17.11 | 17.13 |
| single-isolate | instant-text-kv-read | 20 | 4 | 2566 | 1.55 | 1.18 | 2.92 | 2.92 |
| single-isolate | instant-text-kv-write | 20 | 4 | 1214 | 3.29 | 1.20 | 11.27 | 11.27 |
| autoscaling-2 | instant-text | 20 | 4 | 1291 | 3.06 | 1.26 | 10.81 | 10.81 |
| autoscaling-2 | host-fetch-local | 20 | 4 | 522 | 7.65 | 5.51 | 16.34 | 16.35 |
| autoscaling-2 | instant-text-kv-read | 20 | 4 | 2653 | 1.50 | 0.58 | 3.25 | 3.25 |
| autoscaling-2 | instant-text-kv-write | 20 | 4 | 1417 | 2.82 | 0.93 | 10.85 | 10.85 |
| prewarmed-2 | instant-text | 20 | 4 | 1290 | 3.09 | 0.72 | 10.38 | 10.39 |
| prewarmed-2 | host-fetch-local | 20 | 4 | 515 | 7.75 | 5.59 | 16.91 | 16.93 |
| prewarmed-2 | instant-text-kv-read | 20 | 4 | 3318 | 1.20 | 0.71 | 2.93 | 2.94 |
| prewarmed-2 | instant-text-kv-write | 20 | 4 | 1160 | 3.44 | 1.23 | 11.89 | 11.89 |

Reduced dynamic benchmark:

| config | scenario | requests | concurrency | total ms | throughput req/s | mean ms | p50 ms | p95 ms | p99 ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| single-isolate | dynamic-baseline | 100 | 16 | 219.57 | 455 | 32.61 | 34.35 | 36.12 | 36.47 |
| single-isolate | dynamic-hot-fetch | 100 | 16 | 436.68 | 229 | 64.64 | 69.99 | 73.58 | 74.01 |
| single-isolate | dynamic-hot-fetch-host-rpc | 100 | 16 | 682.34 | 147 | 101.46 | 108.90 | 119.39 | 122.59 |
| autoscaling-8 | dynamic-baseline | 100 | 16 | 24.19 | 4134 | 3.70 | 0.98 | 19.45 | 24.16 |
| autoscaling-8 | dynamic-hot-fetch | 100 | 16 | 29.67 | 3370 | 4.46 | 4.67 | 6.76 | 6.78 |
| autoscaling-8 | dynamic-hot-fetch-host-rpc | 100 | 16 | 49.54 | 2019 | 7.35 | 6.99 | 9.70 | 10.05 |

Lifecycle sample:

| config | benchmark | samples | mean ms | p50 ms | p95 ms | p99 ms | max ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| single-isolate | dynamic-create+invoke | 10 | 17.34 | 17.06 | 18.94 | 18.94 | 18.94 |
| autoscaling-8 | dynamic-create+invoke | 10 | 17.49 | 16.98 | 19.35 | 19.35 | 19.35 |

Benchmark note:

- This is a helper-location cleanup; the benchmark runs keep continuity across the affected response and dynamic event paths.

## 2026-06-18 - Shared Memory Scope Helper

Change:

- Storage, socket, and transport memory payload-scope helpers now use one `memory_scope_for_payload` helper.
- Explicit binding/key payloads still take precedence.
- Empty binding/key payloads still fall back to the current request memory scope.

Verification:

```bash
cargo fmt --check
cargo test -p runtime --no-run
cargo test -p runtime service::tests::memory::
cargo test -p runtime service::tests::sessions::
DD_BENCH_MODE=direct-read-memory DD_BENCH_REQUESTS=50 DD_BENCH_CONCURRENCY=4 DD_BENCH_MAX_ISOLATES=2 DD_BENCH_MAX_INFLIGHT=4 cargo run -p runtime --bin bench_memory_storage --release
```

Memory storage smoke result:

| scenario | requests | concurrency | isolates | max inflight | total ms | throughput req/s | mean ms | p50 ms | p95 ms | p99 ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| memory-direct-read-memory | 50 | 4 | 1-2 | 4 | 21.19 | 2360 | 1.68 | 2.40 | 3.16 | 3.17 |

Benchmark note:

- This is a scope-helper cleanup; the benchmark smoke keeps continuity for the memory path but should be treated as noisy.

## 2026-06-18 - Dynamic Pending Reply Allocation Helper

Change:

- Dynamic worker and host RPC ops now allocate pending replies through one `allocate_dynamic_pending_reply` helper.
- Dynamic worker create/lookup/list/delete/invoke, dynamic fast-fetch start, and dynamic host RPC invoke keep the same event construction and runtime-send error handling.

Verification:

```bash
cargo fmt --check
cargo test -p runtime --no-run
cargo test -p runtime dynamic_
cargo test -p runtime dynamic_test_async_reply
DD_BENCH_ONLY=dynamic DD_BENCH_DYNAMIC_REQUESTS=100 DD_BENCH_DYNAMIC_CONCURRENCY=16 DD_BENCH_DYNAMIC_COLD_ROUNDS=10 cargo run -p runtime --bin bench --release
```

Reduced dynamic benchmark:

| config | scenario | requests | concurrency | total ms | throughput req/s | mean ms | p50 ms | p95 ms | p99 ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| single-isolate | dynamic-baseline | 100 | 16 | 208.06 | 481 | 30.82 | 33.61 | 35.62 | 35.65 |
| single-isolate | dynamic-hot-fetch | 100 | 16 | 452.47 | 221 | 66.90 | 71.61 | 76.99 | 81.03 |
| single-isolate | dynamic-hot-fetch-host-rpc | 100 | 16 | 670.15 | 149 | 99.36 | 106.72 | 110.93 | 112.00 |
| autoscaling-8 | dynamic-baseline | 100 | 16 | 22.99 | 4351 | 3.36 | 0.76 | 15.98 | 19.57 |
| autoscaling-8 | dynamic-hot-fetch | 100 | 16 | 25.56 | 3912 | 3.89 | 4.15 | 6.56 | 6.70 |
| autoscaling-8 | dynamic-hot-fetch-host-rpc | 100 | 16 | 49.70 | 2012 | 7.21 | 6.78 | 9.36 | 10.79 |

Lifecycle sample:

| config | benchmark | samples | mean ms | p50 ms | p95 ms | p99 ms | max ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| single-isolate | dynamic-create+invoke | 10 | 16.39 | 16.84 | 18.93 | 18.93 | 18.93 |
| autoscaling-8 | dynamic-create+invoke | 10 | 15.03 | 14.81 | 16.88 | 16.88 | 16.88 |

Benchmark note:

- This is a dynamic-op boilerplate cleanup; benchmark movement should be treated as normal run noise.

## 2026-06-18 - Dynamic Start Result Constructors

Change:

- Dynamic op start results now use `dynamic_start_ok` and `dynamic_start_error`.
- Test async reply, nested targeted invoke, dynamic worker, fast-fetch start, and host RPC start ops keep the same validation strings and event dispatch behavior.

Verification:

```bash
cargo fmt --check
cargo test -p runtime --no-run
cargo test -p runtime dynamic_
cargo test -p runtime dynamic_test_async_reply
DD_BENCH_ONLY=dynamic DD_BENCH_DYNAMIC_REQUESTS=100 DD_BENCH_DYNAMIC_CONCURRENCY=16 DD_BENCH_DYNAMIC_COLD_ROUNDS=10 cargo run -p runtime --bin bench --release
```

Reduced dynamic benchmark:

| config | scenario | requests | concurrency | total ms | throughput req/s | mean ms | p50 ms | p95 ms | p99 ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| single-isolate | dynamic-baseline | 100 | 16 | 234.16 | 427 | 35.15 | 33.16 | 45.90 | 55.30 |
| single-isolate | dynamic-hot-fetch | 100 | 16 | 462.70 | 216 | 68.68 | 73.42 | 78.83 | 79.13 |
| single-isolate | dynamic-hot-fetch-host-rpc | 100 | 16 | 718.40 | 139 | 106.39 | 114.87 | 123.41 | 125.57 |
| autoscaling-8 | dynamic-baseline | 100 | 16 | 37.50 | 2667 | 5.86 | 1.69 | 25.52 | 36.84 |
| autoscaling-8 | dynamic-hot-fetch | 100 | 16 | 36.11 | 2769 | 5.47 | 5.02 | 7.67 | 8.05 |
| autoscaling-8 | dynamic-hot-fetch-host-rpc | 100 | 16 | 55.16 | 1813 | 8.04 | 7.76 | 10.54 | 11.17 |

Lifecycle sample:

| config | benchmark | samples | mean ms | p50 ms | p95 ms | p99 ms | max ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| single-isolate | dynamic-create+invoke | 10 | 20.02 | 19.59 | 25.79 | 25.79 | 25.79 |
| autoscaling-8 | dynamic-create+invoke | 10 | 20.84 | 21.73 | 25.11 | 25.11 | 25.11 |

Benchmark note:

- This is a result-construction cleanup; the run was visibly noisy, especially autoscaling baseline, so treat it as continuity data only.

## 2026-06-18 - Shared Pending Reply Owner

Change:

- Dynamic pending replies and test async replies now share one `PendingReplyOwner` type.
- Both reply stores still carry `(worker_name, generation, isolate_id)` and enqueue replies to the same destination isolate.

Verification:

```bash
cargo fmt --check
cargo test -p runtime --no-run
cargo test -p runtime dynamic_
cargo test -p runtime dynamic_test_async_reply
DD_BENCH_ONLY=dynamic DD_BENCH_DYNAMIC_REQUESTS=100 DD_BENCH_DYNAMIC_CONCURRENCY=16 DD_BENCH_DYNAMIC_COLD_ROUNDS=10 cargo run -p runtime --bin bench --release
```

Reduced dynamic benchmark:

| config | scenario | requests | concurrency | total ms | throughput req/s | mean ms | p50 ms | p95 ms | p99 ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| single-isolate | dynamic-baseline | 100 | 16 | 214.63 | 466 | 31.65 | 34.96 | 35.97 | 36.17 |
| single-isolate | dynamic-hot-fetch | 100 | 16 | 484.59 | 206 | 71.73 | 75.97 | 86.87 | 88.61 |
| single-isolate | dynamic-hot-fetch-host-rpc | 100 | 16 | 790.65 | 126 | 116.90 | 125.49 | 132.84 | 134.02 |
| autoscaling-8 | dynamic-baseline | 100 | 16 | 47.51 | 2105 | 7.32 | 2.60 | 28.83 | 44.24 |
| autoscaling-8 | dynamic-hot-fetch | 100 | 16 | 34.94 | 2862 | 5.38 | 5.20 | 7.67 | 8.14 |
| autoscaling-8 | dynamic-hot-fetch-host-rpc | 100 | 16 | 63.07 | 1586 | 9.28 | 9.13 | 13.17 | 15.20 |

Lifecycle sample:

| config | benchmark | samples | mean ms | p50 ms | p95 ms | p99 ms | max ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| single-isolate | dynamic-create+invoke | 10 | 24.97 | 23.50 | 35.51 | 35.51 | 35.51 |
| autoscaling-8 | dynamic-create+invoke | 10 | 22.84 | 23.23 | 27.50 | 27.50 | 27.50 |

Benchmark note:

- This is a type-shape cleanup. The autoscaling baseline was noisy, so these rows are continuity data rather than a performance signal.

## 2026-06-18 - Static Dynamic Reply Kind

Change:

- `DynamicPendingReplyResult.kind` is now `&'static str`.
- Dynamic create/lookup/list/delete/invoke/host-rpc replies use static protocol labels without allocating a `String` per reply.

Verification:

```bash
cargo fmt --check
cargo test -p runtime --no-run
cargo test -p runtime dynamic_
cargo test -p runtime dynamic_test_async_reply
DD_BENCH_ONLY=dynamic DD_BENCH_DYNAMIC_REQUESTS=100 DD_BENCH_DYNAMIC_CONCURRENCY=16 DD_BENCH_DYNAMIC_COLD_ROUNDS=10 cargo run -p runtime --bin bench --release
```

Reduced dynamic benchmark:

| config | scenario | requests | concurrency | total ms | throughput req/s | mean ms | p50 ms | p95 ms | p99 ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| single-isolate | dynamic-baseline | 100 | 16 | 206.42 | 484 | 31.35 | 33.30 | 36.97 | 37.26 |
| single-isolate | dynamic-hot-fetch | 100 | 16 | 486.40 | 206 | 72.13 | 75.61 | 90.34 | 94.60 |
| single-isolate | dynamic-hot-fetch-host-rpc | 100 | 16 | 742.56 | 135 | 110.02 | 117.67 | 126.90 | 128.50 |
| autoscaling-8 | dynamic-baseline | 100 | 16 | 42.60 | 2347 | 6.51 | 2.56 | 29.88 | 38.04 |
| autoscaling-8 | dynamic-hot-fetch | 100 | 16 | 47.58 | 2102 | 7.26 | 5.87 | 14.70 | 15.22 |
| autoscaling-8 | dynamic-hot-fetch-host-rpc | 100 | 16 | 65.24 | 1533 | 9.60 | 9.09 | 16.03 | 27.49 |

Lifecycle sample:

| config | benchmark | samples | mean ms | p50 ms | p95 ms | p99 ms | max ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| single-isolate | dynamic-create+invoke | 10 | 23.49 | 23.30 | 31.86 | 31.86 | 31.86 |
| autoscaling-8 | dynamic-create+invoke | 10 | 21.65 | 21.67 | 25.91 | 25.91 | 25.91 |

Benchmark note:

- This removes a small per-reply allocation. The reduced dynamic run remains noisy, so use it as continuity data.

## 2026-06-18 - Dynamic Reply Ready Constructor

Change:

- Non-fetch dynamic replies now use `DynamicPendingReplyResult::ready` for shared ready/kind/ok initialization.
- Payload-specific fields remain explicit in each dynamic create/lookup/list/delete/invoke/host-rpc branch.

Verification:

```bash
cargo fmt --check
cargo test -p runtime --no-run
cargo test -p runtime dynamic_
cargo test -p runtime dynamic_test_async_reply
DD_BENCH_ONLY=dynamic DD_BENCH_DYNAMIC_REQUESTS=100 DD_BENCH_DYNAMIC_CONCURRENCY=16 DD_BENCH_DYNAMIC_COLD_ROUNDS=10 cargo run -p runtime --bin bench --release
```

Reduced dynamic benchmark:

| config | scenario | requests | concurrency | total ms | throughput req/s | mean ms | p50 ms | p95 ms | p99 ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| single-isolate | dynamic-baseline | 100 | 16 | 185.65 | 539 | 28.13 | 30.16 | 36.12 | 36.37 |
| single-isolate | dynamic-hot-fetch | 100 | 16 | 516.87 | 193 | 76.63 | 79.29 | 101.57 | 103.26 |
| single-isolate | dynamic-hot-fetch-host-rpc | 100 | 16 | 765.84 | 131 | 113.41 | 120.57 | 131.60 | 133.62 |
| autoscaling-8 | dynamic-baseline | 100 | 16 | 38.72 | 2583 | 6.08 | 1.91 | 29.43 | 33.64 |
| autoscaling-8 | dynamic-hot-fetch | 100 | 16 | 35.31 | 2832 | 5.21 | 5.11 | 7.63 | 8.04 |
| autoscaling-8 | dynamic-hot-fetch-host-rpc | 100 | 16 | 56.45 | 1771 | 8.46 | 8.55 | 11.07 | 11.08 |

Lifecycle sample:

| config | benchmark | samples | mean ms | p50 ms | p95 ms | p99 ms | max ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| single-isolate | dynamic-create+invoke | 10 | 22.94 | 24.28 | 30.46 | 30.46 | 30.46 |
| autoscaling-8 | dynamic-create+invoke | 10 | 20.33 | 19.88 | 22.91 | 22.91 | 22.91 |

Benchmark note:

- This is a reply-construction cleanup; benchmark movement should be treated as run noise.

## 2026-06-18 - Borrowed Protocol Header Lookups

Change:

- `internal_header_value` now returns `Option<&str>` instead of cloning matching header values.
- `traceparent_from_headers` now returns `Option<&str>`.
- Owned conversion remains only for deferred completion metadata and websocket/transport open metadata that must be stored.

Verification:

```bash
cargo fmt --check
cargo test -p runtime --no-run
cargo test -p runtime dynamic_
cargo test -p runtime service::tests::sessions::
cargo test -p runtime internal_trace_
DD_BENCH_ONLY=dynamic DD_BENCH_DYNAMIC_REQUESTS=100 DD_BENCH_DYNAMIC_CONCURRENCY=16 DD_BENCH_DYNAMIC_COLD_ROUNDS=10 cargo run -p runtime --bin bench --release
```

Reduced dynamic benchmark:

| config | scenario | requests | concurrency | total ms | throughput req/s | mean ms | p50 ms | p95 ms | p99 ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| single-isolate | dynamic-baseline | 100 | 16 | 217.60 | 460 | 32.30 | 34.20 | 36.00 | 36.40 |
| single-isolate | dynamic-hot-fetch | 100 | 16 | 447.93 | 223 | 66.45 | 71.04 | 75.52 | 76.92 |
| single-isolate | dynamic-hot-fetch-host-rpc | 100 | 16 | 691.06 | 145 | 102.38 | 110.28 | 116.74 | 116.98 |
| autoscaling-8 | dynamic-baseline | 100 | 16 | 25.89 | 3863 | 3.90 | 0.96 | 20.60 | 24.52 |
| autoscaling-8 | dynamic-hot-fetch | 100 | 16 | 31.90 | 3135 | 4.54 | 4.62 | 6.77 | 7.09 |
| autoscaling-8 | dynamic-hot-fetch-host-rpc | 100 | 16 | 46.56 | 2148 | 6.85 | 6.73 | 8.95 | 9.11 |

Lifecycle sample:

| config | benchmark | samples | mean ms | p50 ms | p95 ms | p99 ms | max ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| single-isolate | dynamic-create+invoke | 10 | 17.89 | 17.35 | 19.31 | 19.31 | 19.31 |
| autoscaling-8 | dynamic-create+invoke | 10 | 17.50 | 17.05 | 19.06 | 19.06 | 19.06 |

Benchmark note:

- This micro-refactor primarily removes tiny clones from protocol/span handling; benchmark movement here is expected to be dominated by normal runtime noise.

## 2026-06-18 - Shared Request Owner Context

Change:

- Worker name and generation now live in the shared per-pool request execution context.
- Isolate request-scope registration no longer allocates a fresh worker name string for every request.
- Host-fetch context lookup no longer clones the worker name unless the quota-kill path needs an owned runtime command payload.

Verification:

```bash
cargo fmt
cargo test -p runtime --no-run
cargo test -p runtime dynamic_worker_fetch
cargo test -p runtime dynamic_
cargo test -p runtime service::tests::memory::
cargo test -p runtime invoke_stream_delivers_chunked_response_body
DD_BENCH_REQUESTS=20 DD_BENCH_CONCURRENCY=4 DD_BENCH_AUTOSCALING_ISOLATES=2 DD_BENCH_PREWARMED_ISOLATES=2 cargo run -p runtime --bin bench_fetch_fast --release
```

Representative fast-fetch smoke result:

| config | scenario | requests | concurrency | throughput req/s | mean ms | p50 ms | p95 ms | p99 ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| single-isolate | instant-text | 20 | 4 | 1178 | 3.39 | 0.74 | 12.22 | 12.22 |
| single-isolate | instant-text-kv-read | 20 | 4 | 1675 | 2.38 | 2.52 | 3.26 | 3.26 |
| single-isolate | instant-text-kv-write | 20 | 4 | 1420 | 2.81 | 0.89 | 10.92 | 10.92 |
| autoscaling-2 | instant-text | 20 | 4 | 1477 | 2.70 | 0.70 | 10.84 | 10.84 |
| autoscaling-2 | instant-text-kv-read | 20 | 4 | 2050 | 1.95 | 2.40 | 2.94 | 2.94 |
| autoscaling-2 | instant-text-kv-write | 20 | 4 | 1252 | 3.19 | 1.27 | 12.21 | 12.21 |
| prewarmed-2 | instant-text | 20 | 4 | 1096 | 3.64 | 2.73 | 10.92 | 10.92 |
| prewarmed-2 | instant-text-kv-read | 20 | 4 | 1692 | 2.36 | 2.53 | 3.26 | 3.27 |
| prewarmed-2 | instant-text-kv-write | 20 | 4 | 1239 | 3.22 | 1.20 | 10.96 | 10.96 |

Benchmark note:

- The smoke command ran all fast-fetch scenarios; the table records representative plain, KV-read, and KV-write rows to keep this log compact.

## 2026-06-18 - Request Owner Scope Helper

Change:

- Consolidated dynamic worker, host RPC, memory invoke, and generic request-owner scope lookups into one helper.
- Preserved caller-specific binding validation and error text while removing repeated request-scope borrow/lookup/owner construction code.

Verification:

```bash
cargo fmt --check
cargo test -p runtime --no-run
cargo test -p runtime dynamic_
cargo test -p runtime service::tests::memory::
cargo test -p runtime dynamic_worker_fetch
cargo test -p runtime dynamic_test_async_reply
DD_BENCH_ONLY=dynamic DD_BENCH_DYNAMIC_REQUESTS=100 DD_BENCH_DYNAMIC_CONCURRENCY=16 DD_BENCH_DYNAMIC_COLD_ROUNDS=10 cargo run -p runtime --bin bench --release
```

Reduced dynamic benchmark:

| config | scenario | requests | concurrency | total ms | throughput req/s | mean ms | p50 ms | p95 ms | p99 ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| single-isolate | dynamic-baseline | 100 | 16 | 216.65 | 462 | 32.15 | 34.16 | 35.75 | 36.11 |
| single-isolate | dynamic-hot-fetch | 100 | 16 | 445.28 | 225 | 65.89 | 69.91 | 74.11 | 74.27 |
| single-isolate | dynamic-hot-fetch-host-rpc | 100 | 16 | 685.94 | 146 | 101.66 | 109.12 | 115.63 | 116.66 |
| autoscaling-8 | dynamic-baseline | 100 | 16 | 22.61 | 4423 | 3.56 | 0.78 | 17.19 | 20.80 |
| autoscaling-8 | dynamic-hot-fetch | 100 | 16 | 28.50 | 3509 | 4.26 | 4.57 | 6.68 | 6.71 |
| autoscaling-8 | dynamic-hot-fetch-host-rpc | 100 | 16 | 46.61 | 2145 | 6.98 | 6.73 | 9.39 | 9.49 |

Lifecycle sample:

| config | benchmark | samples | mean ms | p50 ms | p95 ms | p99 ms | max ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| single-isolate | dynamic-create+invoke | 10 | 16.90 | 16.99 | 18.33 | 18.33 | 18.33 |
| autoscaling-8 | dynamic-create+invoke | 10 | 16.50 | 16.86 | 17.13 | 17.13 | 17.13 |

Benchmark note:

- This is a maintainability refactor on request-owner lookup; benchmark movement should be treated as normal run noise.

## 2026-06-18 - Preparsed Egress Allowlist

Change:

- Dynamic request contexts now store parsed egress allowlist entries instead of raw strings.
- Host fetch URL checks no longer reparse host/port rules or allocate wildcard suffix strings for each request.

Verification:

```bash
cargo fmt --check
cargo test -p runtime parse_egress_allow_host
cargo test -p runtime egress_url_rules
cargo test -p runtime dynamic_worker_fetch
cargo test -p runtime dynamic_worker_config
cargo test -p runtime dynamic_namespace_default_egress_is_denied
cargo test -p runtime --no-run
DD_BENCH_ONLY=dynamic DD_BENCH_DYNAMIC_REQUESTS=100 DD_BENCH_DYNAMIC_CONCURRENCY=16 DD_BENCH_DYNAMIC_COLD_ROUNDS=10 cargo run -p runtime --bin bench --release
```

Reduced dynamic benchmark:

| config | scenario | requests | concurrency | total ms | throughput req/s | mean ms | p50 ms | p95 ms | p99 ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| single-isolate | dynamic-baseline | 100 | 16 | 216.11 | 463 | 31.92 | 34.63 | 35.79 | 35.87 |
| single-isolate | dynamic-hot-fetch | 100 | 16 | 446.93 | 224 | 66.29 | 69.84 | 80.81 | 80.90 |
| single-isolate | dynamic-hot-fetch-host-rpc | 100 | 16 | 681.31 | 147 | 101.42 | 108.74 | 118.74 | 119.87 |
| autoscaling-8 | dynamic-baseline | 100 | 16 | 21.70 | 4609 | 3.37 | 0.76 | 16.70 | 19.91 |
| autoscaling-8 | dynamic-hot-fetch | 100 | 16 | 30.76 | 3251 | 4.54 | 4.62 | 6.91 | 7.41 |
| autoscaling-8 | dynamic-hot-fetch-host-rpc | 100 | 16 | 50.82 | 1968 | 7.27 | 6.75 | 8.89 | 9.51 |

Lifecycle sample:

| config | benchmark | samples | mean ms | p50 ms | p95 ms | p99 ms | max ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| single-isolate | dynamic-create+invoke | 10 | 17.11 | 16.97 | 19.14 | 19.14 | 19.14 |
| autoscaling-8 | dynamic-create+invoke | 10 | 16.15 | 16.94 | 17.08 | 17.08 | 17.08 |

Benchmark note:

- The current benchmark suite does not include a dedicated external host-fetch workload; this run keeps the dynamic benchmark history continuous while focused tests cover egress behavior.

## 2026-03-20 - Runtime Full-Suite Baseline

Command:

```bash
cargo run -p runtime --bin bench --release
```

This benchmark measures runtime service behavior directly, without API/network overhead.

Environment:

- Date: `2026-03-20T17:08:27+01:00`
- OS: `Linux cachyos-x8664 6.19.8-1-cachyos x86_64`
- Logical CPUs: `16`

### steady-state: single-isolate

| scenario | requests | concurrency | total ms | throughput req/s | mean ms | p50 ms | p95 ms | p99 ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| instant-response | 500 | 64 | 286.16 | 1747 | 35.03 | 35.11 | 57.50 | 58.12 |
| cpu-5ms | 400 | 64 | 368.79 | 1085 | 54.14 | 56.92 | 66.17 | 67.06 |
| host-sleep-5ms | 400 | 64 | 3111.16 | 129 | 458.40 | 493.36 | 501.73 | 502.10 |
| page-load-mix | 700 | 96 | 3334.50 | 210 | 425.18 | 454.32 | 469.29 | 474.05 |

### steady-state: autoscaling-8

| scenario | requests | concurrency | total ms | throughput req/s | mean ms | p50 ms | p95 ms | p99 ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| instant-response | 500 | 64 | 48.00 | 10417 | 5.98 | 3.35 | 24.52 | 26.33 |
| cpu-5ms | 400 | 64 | 62.55 | 6394 | 9.46 | 8.05 | 19.41 | 22.27 |
| host-sleep-5ms | 400 | 64 | 121.65 | 3288 | 18.21 | 15.53 | 36.45 | 42.43 |
| page-load-mix | 700 | 96 | 135.22 | 5177 | 16.73 | 15.52 | 25.76 | 33.06 |

### lifecycle: autoscaling-8

| benchmark | samples | mean ms | p50 ms | p95 ms | p99 ms | max ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| cold-start deploy | 40 | 14.34 | 14.24 | 15.27 | 18.82 | 18.82 |
| cold-start first invoke | 40 | 2.63 | 2.54 | 2.65 | 10.32 | 10.32 |
| hot-start invoke | 500 | 0.43 | 0.39 | 0.56 | 0.74 | 5.33 |

Scale-up burst:

- requests: `1000`
- concurrency: `192`
- target isolates: `8`
- time to reach target: `15.16 ms`
- burst throughput: `1740 req/s`
- burst p95/p99: `108.73 / 119.60 ms`

Takeaways:

- `p95`/`p99` are first-class outputs in every workload.
- The autoscaling profile strongly outperforms single-isolate on both CPU-heavy and promise-idle scenarios.
- The page-load-mix scenario shows the biggest practical gap (`210 req/s` vs `5177 req/s`) and much lower tail latencies under autoscaling.
- Cold deploy + first invoke are measurable in low tens of milliseconds on this machine; warm invokes are sub-millisecond at `p50`.
