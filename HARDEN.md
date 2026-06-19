# Hardening Log

This file tracks progress toward a smaller, faster, more maintainable `dd`.

## Standing Constraints

- Local storage uses the `turso` crate directly; do not add `libsql`.
- KV storage is write-last with monotonic versions.
- KV and memory writes must stay contention-safe: `busy_timeout`, bounded busy/lock retry, and persisted version floors.
- Prefer deleting or simplifying over compatibility layers when a breaking change materially improves the system.

## 2026-06-18

### Runtime Memory Dispatch

- Cached the memory ownership key on `MemoryRoute` at construction time.
- Replaced ad hoc dispatch/debug key formatting with the cached `MemoryRoute::owner_key` field.
- Covered all queued memory paths: memory method calls, websocket frame/close wakes, and transport stream/datagram/close wakes.
- Removed the dead `memory_owners` map and `assign_owner` dispatch flag. The flag was always `false`, so the map was never populated and cleanup only maintained empty state.
- Removed the debug-only `memory_inflight` map. Pending debug requests already carry `memory_key`, so this avoided redundant hot-path HashMap updates.
- Simplified targeted nested dispatch selection by removing a trivially true capacity branch and making regular targeted work use the normal capacity/wait-until checks.
- Made dispatch selection take `&WorkerPool` instead of `&mut WorkerPool` and centralized targeted-isolate lookup.
- Collapsed worker-pool activity/stat totals into one isolate pass instead of separate busy/inflight/wait/websocket/transport scans.
- Made the second dispatch scan rely on the first scan's targeted-nested invariant, removing an unreachable branch and memory-key clone from regular targeted dispatch.
- Extracted dispatch capacity checks into `WorkerPool::has_dispatch_capacity`, so strict isolation and waitUntil gating live with the pool activity policy.
- Added `WorkerPool::build_execute_command` and cached dynamic RPC binding names on each pool, removing duplicated execute-command setup and per-dispatch map/collect work.
- Removed `memory_key` from `DispatchCandidate`; pending debug metadata is now derived once from the owned queued invoke immediately before dispatch.

Why it matters:

- Avoids repeated `format!` work while scanning queued memory dispatches.
- Makes the memory owner/inflight key explicit state on the routed request.
- Keeps route construction uniform through `MemoryRoute::new`.
- Drops stale owner bookkeeping without changing effective routing behavior.
- Keeps debug visibility at the request level without maintaining a second aggregate map.
- Keeps the deadlock-avoidance priority for targeted nested work while making the regular targeted path easier to audit.
- Narrows mutability in the scheduler so future changes have a smaller accidental-write surface.
- Makes stats and drain checks cheaper and keeps the pool activity definition in one place.
- Keeps targeted dispatch logic explicit: nested targeted calls get the priority pass, regular targeted calls use ordinary capacity checks.
- Removes duplicated capacity predicates from the dispatch loop.
- Keeps isolate command construction in one place and avoids rebuilding dynamic RPC binding-name vectors for every dispatched request.
- Keeps dispatch candidate selection focused on scheduling and avoids copying memory debug keys during queue scans.

Verification:

- `cargo test -p runtime --no-run`
- `cargo test -p runtime service::tests::memory::`
- `cargo test -p runtime service::tests::sessions::`
- `cargo test -p runtime dynamic_test_nested_targeted_invoke`
- `cargo test -p runtime dynamic_`
- `cargo test -p runtime --bin bench_memory_storage`
- `cargo test -p runtime scales_`
- `cargo test -p runtime wait_until`
- `DD_BENCH_MODE=direct-read-memory DD_BENCH_REQUESTS=50 DD_BENCH_CONCURRENCY=4 DD_BENCH_MAX_ISOLATES=2 DD_BENCH_MAX_INFLIGHT=4 cargo run -p runtime --bin bench_memory_storage --release`
- `just check`

Benchmark notes:

- Added `BENCHMARK.md` as the canonical continuous benchmark log.
- Captured release-mode direct memory read/write slices after the route-key cache change.

### Request Execution Context

- Added a shared `RequestExecutionContext` for normalized per-pool request policy.
- Moved dynamic binding sets, dynamic host RPC binding sets, secret replacement maps, egress allowlists, cache permission, outbound quota, and dynamic quota state into that context.
- `WorkerPool` now stores one request context and `IsolateCommand::Execute` carries a cheap clone instead of raw vectors.
- `register_request_secret_context` now installs the pre-normalized context instead of rebuilding `HashSet`/`HashMap` state for every request.
- Host fetch preparation and URL checks now clone the shared context instead of cloning replacement maps and allowlists per op.
- Removed the now-redundant raw policy fields from `WorkerPool`.

Why it matters:

- Takes immutable policy normalization out of the request hot path.
- Shrinks execute-command payload setup and keeps request policy ownership in one place.
- Reduces repeated map/set/vector allocations for dynamic workers and host fetch policy checks.

Verification:

- `cargo test -p runtime --no-run`
- `cargo test -p runtime dynamic_`
- `cargo test -p runtime service::tests::memory::`
- `cargo test -p runtime service::tests::sessions::`
- `cargo test -p runtime --bin bench_memory_storage`
- `DD_BENCH_MODE=direct-read-memory DD_BENCH_REQUESTS=300 DD_BENCH_CONCURRENCY=16 DD_BENCH_MAX_ISOLATES=4 DD_BENCH_MAX_INFLIGHT=8 cargo run -p runtime --bin bench_memory_storage --release`

### Execute Payload Moves

- Converted `MemoryExecutionCall` and `HostRpcExecutionCall` into engine execute payloads by move instead of by clone.
- Removed the now-unused `Clone` derives from service-side execution call payloads and engine-side execute payloads.
- Kept the engine dispatch API unchanged; the owned converted payloads are still serialized by reference for JS entry generation.
- Removed the duplicate engine-side execute payload structs entirely.
- Made the service-side execution call payloads the serializable source of truth and changed `dispatch_worker_request` to accept generic serializable call payloads.
- Deleted the isolate-thread conversion match between service and engine payload shapes.

Why it matters:

- Avoids copying memory RPC args, websocket/transport payload bytes, handle vectors, and host RPC arg buffers on the isolate dispatch path.
- Makes the internal payload types accurately express single-use ownership.
- Removes a second representation that had to stay JSON-compatible with the service representation.
- Cuts a dispatch-time match that only repackaged fields without changing behavior.

Verification:

- `cargo test -p runtime --no-run`
- `cargo test -p runtime service::tests::memory::`
- `cargo test -p runtime service::tests::sessions::`
- `cargo test -p runtime dynamic_`
- `cargo test -p runtime --bin bench_memory_storage`
- `DD_BENCH_MODE=direct-write-memory DD_BENCH_REQUESTS=300 DD_BENCH_CONCURRENCY=16 DD_BENCH_MAX_ISOLATES=4 DD_BENCH_MAX_INFLIGHT=8 cargo run -p runtime --bin bench_memory_storage --release`
- `DD_BENCH_MODE=direct-read-memory DD_BENCH_REQUESTS=300 DD_BENCH_CONCURRENCY=16 DD_BENCH_MAX_ISOLATES=4 DD_BENCH_MAX_INFLIGHT=8 cargo run -p runtime --bin bench_memory_storage --release`

### Request Scope Registration Cleanup

- Stopped cloning `request.request_id` before the tracing span is built; the span records it directly from the owned request.
- Moved `MemoryRoute` into memory request scope registration instead of cloning its binding/key and then dropping it.
- Collapsed request body stream, memory scope, and request secret context setup into one mutable `OpState` borrow.

Why it matters:

- Removes two small unconditional dispatch-path clones for memory-routed requests.
- Makes per-request isolate setup more compact and reduces `RefCell` borrow churn.

Verification:

- `cargo test -p runtime --no-run`
- `cargo test -p runtime service::tests::memory::`
- `cargo test -p runtime service::tests::sessions::`
- `cargo test -p runtime dynamic_`
- `cargo test -p runtime --bin bench_memory_storage`
- `DD_BENCH_MODE=direct-write-memory DD_BENCH_REQUESTS=300 DD_BENCH_CONCURRENCY=16 DD_BENCH_MAX_ISOLATES=4 DD_BENCH_MAX_INFLIGHT=8 cargo run -p runtime --bin bench_memory_storage --release`
- `DD_BENCH_MODE=direct-read-memory DD_BENCH_REQUESTS=300 DD_BENCH_CONCURRENCY=16 DD_BENCH_MAX_ISOLATES=4 DD_BENCH_MAX_INFLIGHT=8 cargo run -p runtime --bin bench_memory_storage --release`

### Session Registration Ownership

- Stopped cloning whole websocket and transport session structs during registration; the session maps now take ownership directly after the secondary indexes are built.
- Replaced websocket and transport wake-path whole-session clones with narrow snapshots of only the fields needed to enqueue memory wakes.
- Removed the now-unused `Clone` derives from `WorkerWebSocketSession` and `WorkerTransportSession`.

Why it matters:

- Avoids cloning transport session queues and senders when only worker/generation/binding/key/handle are needed.
- Makes session ownership clearer: session records are either moved into the registry or explicitly reduced to small wake metadata snapshots.

Verification:

- `cargo test -p runtime --no-run`
- `cargo test -p runtime service::tests::sessions::`
- `cargo test -p runtime service::tests::memory::`
- `cargo test -p runtime --bin bench_memory_storage`

### Trace Completion Metadata

- Replaced the full `Result<WorkerOutput>` clone used for internal trace forwarding with compact `TraceResultMeta`.
- Trace forwarding now snapshots only response status or error text before the real result is delivered to the caller/stream/session path.

Why it matters:

- Avoids cloning response headers and body solely to construct an internal trace event.
- Preserves existing completion delivery order while narrowing trace forwarding data to exactly what it serializes.

Verification:

- `cargo test -p runtime --no-run`
- `cargo test -p runtime internal_trace_`
- `cargo test -p runtime --bin bench_memory_storage`
- `just check`

### Stream Completion Ownership

- Added an explicit `PendingReplyKind::Stream` for `invoke_stream` requests.
- Stream invokes now move the owned completion `Result<WorkerOutput>` into `complete_stream_registration` instead of cloning it and sending the original result into a discarded normal reply channel.
- Kept the existing clone only as a defensive fallback for any unexpected non-stream reply kind that also has a stream registration.
- Stream fallback completion now moves headers/body and errors into their single consumers instead of cloning them.

Why it matters:

- Avoids copying response headers/body at completion for the real streaming API path.
- Makes the hidden `invoke_stream` reply channel clearly a lifecycle drain rather than a second result consumer.
- Keeps the non-`ResponseStart` stream fallback path allocation-free beyond the channel payloads it must deliver.

Verification:

- `cargo test -p runtime invoke_stream_delivers_chunked_response_body`
- `cargo test -p runtime --no-run`
- `DD_BENCH_MODE=direct-read-memory DD_BENCH_REQUESTS=300 DD_BENCH_CONCURRENCY=16 DD_BENCH_MAX_ISOLATES=4 DD_BENCH_MAX_INFLIGHT=8 cargo run -p runtime --bin bench_memory_storage --release`

### Dynamic Handle Dispatch Snapshot

- Stopped cloning full `DynamicWorkerHandle` entries in lookup/delete/invoke/fetch paths.
- Added a narrow `DynamicDispatchTarget` snapshot for dynamic invoke/fetch that carries only target worker, generation, timeout, policy, quota state, and preferred isolate id.
- Changed dynamic inflight acquisition to use quota state and max concurrency directly instead of requiring a cloned handle entry.
- Replaced dynamic host RPC provider clones with a narrow owner/target snapshot after checking method membership, then removed the provider `Clone` derive.

Why it matters:

- Avoids copying handle metadata that dispatch does not need, including owner fields, id, binding, and host RPC provider id vectors.
- Avoids copying host RPC provider method sets during provider dispatch.
- Centralizes owner/generation/binding validation for invoke and fetch, reducing duplicated mismatch checks.
- Keeps preferred-isolate updates on the live registry entry instead of mutating a cloned copy.

Verification:

- `cargo test -p runtime dynamic_`
- `DD_BENCH_ONLY=dynamic DD_BENCH_DYNAMIC_REQUESTS=100 DD_BENCH_DYNAMIC_CONCURRENCY=16 DD_BENCH_DYNAMIC_COLD_ROUNDS=10 cargo run -p runtime --bin bench --release`

### Documentation Hygiene

- Added this progress log.
- Started canonical benchmark history in `BENCHMARK.md`.
- Updated stale local links from the old `/home/mewhhaha/src/grugd` checkout path to `/home/mewhhaha/src/dd`.
- Folded the older `BENCHMARKS.md` full-suite snapshot into `BENCHMARK.md` and removed the duplicate file.

### Benchmark Harness

- Added explicit `--help` / `-h` handling to `bench_memory_storage`.
- Collapsed scenario selection into one `BENCH_CASES` table used by execution, help, and `DD_BENCH_MODE` validation.
- Added unit coverage for benchmark arg parsing, scenario uniqueness, and mode matching.

Why it matters:

- `cargo run -p runtime --bin bench_memory_storage -- --help` now exits after printing help instead of starting runtime work.
- Invalid modes fail before service startup instead of compiling/running a no-op benchmark.
- Adding or removing a keyed-memory scenario now touches one table instead of separate help, validation, and dispatch branches.

Verification:

- `cargo test -p runtime --bin bench_memory_storage`
- `cargo run -p runtime --bin bench_memory_storage -- --help`
- `DD_BENCH_MODE=not-a-mode cargo run -p runtime --bin bench_memory_storage`
- `DD_BENCH_MODE=direct-read-memory DD_BENCH_REQUESTS=50 DD_BENCH_CONCURRENCY=4 DD_BENCH_MAX_ISOLATES=2 DD_BENCH_MAX_INFLIGHT=4 cargo run -p runtime --bin bench_memory_storage --release`

### Benchmark CLI Guards

- Added explicit `--help` / `-h` handling to `bench_fetch_fast`, `bench_kv`, `bench_kv_store`, and the top-level `bench` binary.
- Unknown CLI flags now fail immediately with a clear message instead of starting a full benchmark run.
- Added unit coverage for help, empty args, and unknown arg handling.
- Extracted the duplicated parser into shared benchmark support so the binaries keep only their custom help text.

Why it matters:

- Prevents accidental expensive benchmark runs from common CLI flags.
- Keeps benchmark configuration consistently env-driven across benchmark binaries.
- Keeps the CLI parser behavior in one place while preserving binary-specific usage text.

Verification:

- `cargo test -p runtime --bins bench_arg_action`
- `cargo run -p runtime --bin bench_fetch_fast -- --help`
- `cargo run -p runtime --bin bench -- --help`
- `cargo run -p runtime --bin bench_kv -- --help`
- `cargo run -p runtime --bin bench_kv_store -- --help`
- `cargo run -p runtime --bin bench_fetch_fast -- --requests 10` exits with `unsupported argument`
- `cargo run -p runtime --bin bench -- --requests 10` exits with `unsupported argument`
- `cargo run -p runtime --bin bench_kv -- --requests 10` exits with `unsupported argument`
- `cargo run -p runtime --bin bench_kv_store -- --requests 10` exits with `unsupported argument`
- `DD_BENCH_REQUESTS=20 DD_BENCH_CONCURRENCY=4 DD_BENCH_AUTOSCALING_ISOLATES=2 DD_BENCH_PREWARMED_ISOLATES=2 cargo run -p runtime --bin bench_fetch_fast --release`
- `DD_BENCH_REQUESTS=20 DD_BENCH_CONCURRENCY=4 DD_BENCH_WRITE_REQUESTS=20 DD_BENCH_WRITE_CONCURRENCY=4 DD_BENCH_AUTOSCALING_ISOLATES=2 cargo run -p runtime --bin bench_kv --release`
- `DD_BENCH_REQUESTS=100 DD_BENCH_CONCURRENCY=4 cargo run -p runtime --bin bench_kv_store --release`

### Host Fetch Benchmark Coverage

- Added `host-fetch-local` to `bench_fetch_fast`.
- The scenario starts a loopback HTTP responder inside each scenario child, deploys a dynamic worker with that address in its egress allowlist, and measures the worker `fetch()` path end to end.
- Updated the benchmark banner so the loopback host-fetch case is explicit instead of implying every scenario avoids HTTP transport.

Why it matters:

- Gives the host-fetch request context, egress allowlist, placeholder replacement, and local HTTP dispatch path a continuous benchmark row.
- Replaces the previous benchmark caveat that host-fetch behavior only had focused tests and no benchmark coverage.
- Keeps the added harness local to `bench_fetch_fast` and reuses its existing per-config/per-scenario process isolation.

Verification:

- `cargo fmt --check`
- `cargo test -p runtime --bin bench_fetch_fast`
- `cargo test -p runtime --bins bench_arg_action`
- `DD_BENCH_INTERNAL_CONFIG=single-isolate DD_BENCH_INTERNAL_SCENARIO=host-fetch-local DD_BENCH_REQUESTS=20 DD_BENCH_CONCURRENCY=4 cargo run -p runtime --bin bench_fetch_fast --release`
- `DD_BENCH_REQUESTS=20 DD_BENCH_CONCURRENCY=4 DD_BENCH_AUTOSCALING_ISOLATES=2 DD_BENCH_PREWARMED_ISOLATES=2 cargo run -p runtime --bin bench_fetch_fast --release`

### Fast Fetch Scenario Model

- Replaced the `bench_fetch_fast` scenario booleans with explicit `ScenarioWorker` and `ScenarioBinding` enums.
- Static workers now carry their source and optional binding kind together.
- The loopback host-fetch scenario is a distinct variant instead of an empty source string plus boolean flags.

Why it matters:

- Makes invalid scenario combinations unrepresentable in the benchmark harness.
- Keeps the newly added host-fetch scenario from spreading conditional booleans through deployment code.
- Leaves per-scenario process isolation and output format unchanged.

Verification:

- `cargo fmt --check`
- `cargo test -p runtime --bin bench_fetch_fast`
- `cargo test -p runtime --bins bench_arg_action`
- `DD_BENCH_REQUESTS=20 DD_BENCH_CONCURRENCY=4 DD_BENCH_AUTOSCALING_ISOLATES=2 DD_BENCH_PREWARMED_ISOLATES=2 cargo run -p runtime --bin bench_fetch_fast --release`

### Borrowed Protocol Header Lookups

- Changed `internal_header_value` to return borrowed header values.
- Changed `traceparent_from_headers` to return a borrowed traceparent value.
- Kept owned string conversion only at boundaries that store metadata beyond the borrowed request/output lifetime.
- Updated websocket, transport, dynamic policy, runtime span, facade span, lifecycle span, and deferred completion metadata call sites.

Why it matters:

- Avoids cloning internal accept/session/handle/binding/key header values for simple comparisons.
- Avoids cloning traceparent headers for immediate span-parent propagation.
- Keeps long-lived completion metadata explicit about the one owned traceparent it still requires.

Verification:

- `cargo fmt --check`
- `cargo test -p runtime --no-run`
- `cargo test -p runtime dynamic_`
- `cargo test -p runtime service::tests::sessions::`
- `cargo test -p runtime internal_trace_`
- `DD_BENCH_ONLY=dynamic DD_BENCH_DYNAMIC_REQUESTS=100 DD_BENCH_DYNAMIC_CONCURRENCY=16 DD_BENCH_DYNAMIC_COLD_ROUNDS=10 cargo run -p runtime --bin bench --release`

### Shared Request Owner Context

- Moved worker name and generation into the shared per-pool `RequestExecutionContext`.
- Removed the per-request `worker_name.to_string()` allocation from isolate request-scope registration.
- Reduced `RequestSecretContext` to isolate-local state, shared execution context, and cancellation handles.
- Host-fetch context lookup no longer clones the worker name on every URL check; the owned worker name is produced only on the quota-kill path that sends a runtime command.

Why it matters:

- Removes a guaranteed allocation from every isolate request dispatch.
- Keeps immutable pool identity with the rest of the pre-normalized request policy.
- Makes request scope ownership clearer: pool-level identity and policy are shared, isolate-local ownership stays per request.

Verification:

- `cargo fmt`
- `cargo test -p runtime --no-run`
- `cargo test -p runtime dynamic_worker_fetch`
- `cargo test -p runtime dynamic_`
- `cargo test -p runtime service::tests::memory::`
- `cargo test -p runtime invoke_stream_delivers_chunked_response_body`
- `DD_BENCH_REQUESTS=20 DD_BENCH_CONCURRENCY=4 DD_BENCH_AUTOSCALING_ISOLATES=2 DD_BENCH_PREWARMED_ISOLATES=2 cargo run -p runtime --bin bench_fetch_fast --release`

### Request Owner Scope Helper

- Collapsed four repeated request-scope lookup helpers in dynamic ops into one `request_owner_for_scope` path.
- Kept the caller-specific validation and error messages for dynamic bindings, host RPC bindings, memory invokes, and generic request-owner lookups.
- Centralized the borrow of `RequestSecretContexts` and construction of `(worker_name, generation, isolate_id)`.

Why it matters:

- Keeps dynamic/memory owner lookup policy in one small place.
- Reduces repeated borrow/lookup/clone structure across request-bound ops.
- Makes future request-context changes less likely to miss one of the owner lookup variants.

Verification:

- `cargo fmt --check`
- `cargo test -p runtime --no-run`
- `cargo test -p runtime dynamic_`
- `cargo test -p runtime service::tests::memory::`
- `cargo test -p runtime dynamic_worker_fetch`
- `cargo test -p runtime dynamic_test_async_reply`
- `DD_BENCH_ONLY=dynamic DD_BENCH_DYNAMIC_REQUESTS=100 DD_BENCH_DYNAMIC_CONCURRENCY=16 DD_BENCH_DYNAMIC_COLD_ROUNDS=10 cargo run -p runtime --bin bench --release`

### Preparsed Egress Allowlist

- Added an explicit `EgressAllowHost` representation for dynamic worker host fetch allowlist entries.
- `RequestExecutionContext` now parses allowlist strings once when the worker pool is built.
- Host fetch URL checks compare pre-parsed host suffix, wildcard, and port fields instead of reparsing every allowlist string for every request.
- Wildcard matching no longer allocates a formatted `.{suffix}` string during each URL check.

Why it matters:

- Moves policy parsing out of the host-fetch hot path.
- Keeps dynamic request context closer to the data the runtime actually needs for enforcement.
- Leaves deploy-time validation as the gate for invalid allowlist strings while making request-time checks smaller and easier to read.

Verification:

- `cargo fmt --check`
- `cargo test -p runtime parse_egress_allow_host`
- `cargo test -p runtime egress_url_rules`
- `cargo test -p runtime dynamic_worker_fetch`
- `cargo test -p runtime dynamic_worker_config`
- `cargo test -p runtime dynamic_namespace_default_egress_is_denied`
- `cargo test -p runtime --no-run`
- `DD_BENCH_ONLY=dynamic DD_BENCH_DYNAMIC_REQUESTS=100 DD_BENCH_DYNAMIC_CONCURRENCY=16 DD_BENCH_DYNAMIC_COLD_ROUNDS=10 cargo run -p runtime --bin bench --release`

### Borrowed Isolate Event Sender

- Storage/http event ops use the shared `emit_isolate_event` helper.
- Completion, waitUntil, response-start, response-chunk, and cache-revalidate ops now share the same send path.
- The helper sends through the borrowed `IsolateEventSender` instead of cloning the `std::sync::mpsc::Sender` for every event.
- Completion and waitUntil cleanup still run before the event is emitted.

Why it matters:

- Removes repeated event-emission boilerplate from the JS op boundary.
- Avoids a small clone on the response/completion hot path.
- Keeps lifecycle cleanup semantics visible in the op bodies that own them.

Verification:

- `cargo fmt --check`
- `cargo test -p runtime --no-run`
- `cargo test -p runtime invoke_stream_delivers_chunked_response_body`
- `cargo test -p runtime wait_until`
- `cargo test -p runtime internal_trace_`
- `DD_BENCH_REQUESTS=20 DD_BENCH_CONCURRENCY=4 DD_BENCH_AUTOSCALING_ISOLATES=2 DD_BENCH_PREWARMED_ISOLATES=2 cargo run -p runtime --bin bench_fetch_fast --release`

### Borrowed Memory Event Sender

- Memory ops use the shared `emit_isolate_event_from_rc` helper.
- Memory method invoke, socket send/close/consumeClose, and transport send/recv/close/consumeClose ops now share one event send path.
- The helper sends through the borrowed `IsolateEventSender` instead of cloning the `std::sync::mpsc::Sender` for every memory event.
- The existing unavailable-runtime errors and reply-channel behavior are unchanged.

Why it matters:

- Removes repeated event-emission boilerplate from memory socket and transport ops.
- Avoids a small sender clone on memory event paths.
- Keeps validation and async reply handling local to each op while centralizing only the mechanical send.

Verification:

- `cargo fmt --check`
- `cargo test -p runtime --no-run`
- `cargo test -p runtime service::tests::memory::`
- `cargo test -p runtime service::tests::sessions::`
- `DD_BENCH_MODE=direct-read-memory DD_BENCH_REQUESTS=50 DD_BENCH_CONCURRENCY=4 DD_BENCH_MAX_ISOLATES=2 DD_BENCH_MAX_INFLIGHT=4 cargo run -p runtime --bin bench_memory_storage --release`

### Borrowed Dynamic Event Sender

- Dynamic ops use the shared `emit_isolate_event_from_rc` helper.
- Test async reply, nested targeted invoke, dynamic worker create/lookup/list/delete/invoke, and dynamic host RPC invoke now share one event send path.
- The helper sends through the borrowed `IsolateEventSender` instead of cloning the `std::sync::mpsc::Sender` for every dynamic event.
- Dynamic worker fast-fetch still uses the existing runtime fast command channel, which is a different command path.

Why it matters:

- Removes the last repeated one-shot `IsolateEventSender` clone pattern from runtime ops.
- Keeps dynamic op validation, owner lookup, pending-reply allocation, and reply errors local to their original functions.
- Makes event emission consistent across storage/http, memory, and dynamic ops.

Verification:

- `cargo fmt --check`
- `cargo test -p runtime --no-run`
- `cargo test -p runtime dynamic_`
- `cargo test -p runtime dynamic_test_async_reply`
- `DD_BENCH_ONLY=dynamic DD_BENCH_DYNAMIC_REQUESTS=100 DD_BENCH_DYNAMIC_CONCURRENCY=16 DD_BENCH_DYNAMIC_COLD_ROUNDS=10 cargo run -p runtime --bin bench --release`

### Shared Isolate Event Helper

- Folded the storage/http, memory, and dynamic event-send helpers into the parent ops module.
- Direct `OpState` ops call `emit_isolate_event`; Rc-backed async ops call `emit_isolate_event_from_rc`.
- Storage/http still ignores closed event-channel errors for fire-and-forget completion/response events.
- Memory and dynamic ops still return their existing unavailable-runtime errors when the event send fails.

Why it matters:

- Removes duplicate helper definitions created during the prior per-module cleanup.
- Keeps the "borrow sender, do not clone sender" invariant in one place.
- Makes future isolate-event ops less likely to reintroduce a clone-then-send pattern.

Verification:

- `cargo fmt --check`
- `cargo test -p runtime --no-run`
- `cargo test -p runtime invoke_stream_delivers_chunked_response_body`
- `cargo test -p runtime wait_until`
- `cargo test -p runtime internal_trace_`
- `cargo test -p runtime service::tests::memory::`
- `cargo test -p runtime service::tests::sessions::`
- `cargo test -p runtime dynamic_`
- `DD_BENCH_REQUESTS=20 DD_BENCH_CONCURRENCY=4 DD_BENCH_AUTOSCALING_ISOLATES=2 DD_BENCH_PREWARMED_ISOLATES=2 cargo run -p runtime --bin bench_fetch_fast --release`
- `DD_BENCH_ONLY=dynamic DD_BENCH_DYNAMIC_REQUESTS=100 DD_BENCH_DYNAMIC_CONCURRENCY=16 DD_BENCH_DYNAMIC_COLD_ROUNDS=10 cargo run -p runtime --bin bench --release`

### Shared Memory Scope Helper

- Collapsed storage, socket, and transport memory payload-scope helpers into one `memory_scope_for_payload` helper.
- Explicit binding/key payloads still take precedence.
- Empty binding/key payloads still fall back to the current request memory scope.

Why it matters:

- Removes three copies of the same binding/key normalization and request-scope fallback rule.
- Keeps memory state, socket, and transport ops aligned on the same scope behavior.
- Makes future changes to memory scope resolution a single-site edit.

Verification:

- `cargo fmt --check`
- `cargo test -p runtime --no-run`
- `cargo test -p runtime service::tests::memory::`
- `cargo test -p runtime service::tests::sessions::`
- `DD_BENCH_MODE=direct-read-memory DD_BENCH_REQUESTS=50 DD_BENCH_CONCURRENCY=4 DD_BENCH_MAX_ISOLATES=2 DD_BENCH_MAX_INFLIGHT=4 cargo run -p runtime --bin bench_memory_storage --release`

### Dynamic Pending Reply Allocation Helper

- Added `allocate_dynamic_pending_reply` for dynamic worker and host RPC ops.
- Dynamic worker create/lookup/list/delete/invoke, dynamic fast-fetch start, and dynamic host RPC invoke now share the same pending-reply owner allocation path.
- Event construction, runtime-send error messages, and reply IDs remain owned by the original op bodies.

Why it matters:

- Removes repeated borrow/clone/allocation boilerplate from seven dynamic ops.
- Keeps pending-reply ownership consistently keyed by `(worker_name, generation, isolate_id)`.
- Makes future changes to dynamic pending-reply ownership a single-site edit.

Verification:

- `cargo fmt --check`
- `cargo test -p runtime --no-run`
- `cargo test -p runtime dynamic_`
- `cargo test -p runtime dynamic_test_async_reply`
- `DD_BENCH_ONLY=dynamic DD_BENCH_DYNAMIC_REQUESTS=100 DD_BENCH_DYNAMIC_CONCURRENCY=16 DD_BENCH_DYNAMIC_COLD_ROUNDS=10 cargo run -p runtime --bin bench --release`

### Dynamic Start Result Constructors

- Added `dynamic_start_ok` and `dynamic_start_error` helpers for dynamic op start results.
- Test async reply, nested targeted invoke, dynamic worker, fast-fetch start, and host RPC start ops now share the same `DynamicPendingReplyStartResult` construction.
- Validation strings, owner lookup, event construction, and runtime-send error points are unchanged.

Why it matters:

- Removes repeated `{ ok, reply_id, error }` result literals from the dynamic op boundary.
- Keeps dynamic start result shape centralized and less error-prone.
- Leaves each op focused on validation, ownership lookup, and event dispatch.

Verification:

- `cargo fmt --check`
- `cargo test -p runtime --no-run`
- `cargo test -p runtime dynamic_`
- `cargo test -p runtime dynamic_test_async_reply`
- `DD_BENCH_ONLY=dynamic DD_BENCH_DYNAMIC_REQUESTS=100 DD_BENCH_DYNAMIC_CONCURRENCY=16 DD_BENCH_DYNAMIC_COLD_ROUNDS=10 cargo run -p runtime --bin bench --release`

### Shared Pending Reply Owner

- Replaced separate dynamic pending-reply and test async reply owner structs with one `PendingReplyOwner`.
- Dynamic pending replies and test async replies still store the same `(worker_name, generation, isolate_id)` fields.
- Control delivery paths continue to enqueue replies to the same owner worker/isolate.

Why it matters:

- Removes duplicate owner structs with identical fields and semantics.
- Keeps dynamic and test async reply delivery aligned on one owner representation.
- Makes future owner-field changes a single type edit instead of two parallel edits.

Verification:

- `cargo fmt --check`
- `cargo test -p runtime --no-run`
- `cargo test -p runtime dynamic_`
- `cargo test -p runtime dynamic_test_async_reply`
- `DD_BENCH_ONLY=dynamic DD_BENCH_DYNAMIC_REQUESTS=100 DD_BENCH_DYNAMIC_CONCURRENCY=16 DD_BENCH_DYNAMIC_COLD_ROUNDS=10 cargo run -p runtime --bin bench --release`

### Static Dynamic Reply Kind

- Changed `DynamicPendingReplyResult.kind` from `String` to `&'static str`.
- Dynamic create/lookup/list/delete/invoke/host-rpc replies now assign static kind literals without allocating.
- JSON output shape is unchanged.

Why it matters:

- Removes one string allocation from every non-fetch dynamic reply delivery.
- Makes the reply kind contract explicit: it is a fixed protocol label, not runtime data.
- Keeps the default result cheap with an empty static string.

Verification:

- `cargo fmt --check`
- `cargo test -p runtime --no-run`
- `cargo test -p runtime dynamic_`
- `cargo test -p runtime dynamic_test_async_reply`
- `DD_BENCH_ONLY=dynamic DD_BENCH_DYNAMIC_REQUESTS=100 DD_BENCH_DYNAMIC_CONCURRENCY=16 DD_BENCH_DYNAMIC_COLD_ROUNDS=10 cargo run -p runtime --bin bench --release`

### Dynamic Reply Ready Constructor

- Added `DynamicPendingReplyResult::ready` for non-fetch dynamic reply payloads.
- Dynamic create/lookup/list/delete/invoke/host-rpc reply branches now share the same ready/kind/ok initialization path.
- Payload-specific fields such as handles, lists, delete flags, status, bodies, values, and errors remain explicit at each branch.

Why it matters:

- Removes repeated ready/kind/ok/default construction from dynamic reply delivery.
- Keeps the dynamic reply protocol shape centralized while preserving the JSON output.
- Makes future changes to dynamic reply readiness or kind handling a single-site edit.

Verification:

- `cargo fmt --check`
- `cargo test -p runtime --no-run`
- `cargo test -p runtime dynamic_`
- `cargo test -p runtime dynamic_test_async_reply`
- `DD_BENCH_ONLY=dynamic DD_BENCH_DYNAMIC_REQUESTS=100 DD_BENCH_DYNAMIC_CONCURRENCY=16 DD_BENCH_DYNAMIC_COLD_ROUNDS=10 cargo run -p runtime --bin bench --release`

### Follow-Ups

- Continue looking for hot paths where repeated allocation exists only to satisfy internal map keys or debug views.
- The latest tiny benchmark runs were noisy; use repeated larger runs on a quieter machine before treating the 50-request smoke deltas as a regression or improvement.

## 2026-06-19

### Review Pass 1 Comments

- [x] `crates/api/src/main.rs`: `DD_PRIVATE_TOKEN` and `PRIVATE_BEARER_TOKEN` are coalesced before trimming. An empty preferred env var suppresses a valid fallback token, so production auth can fail open/closed depending on operator env ordering instead of using the first non-empty configured token.
- [x] `crates/api/src/handlers/invocation.rs`: public host parsing accepts nested subdomains like `foo.bar.example.com` and routes them to worker `foo`. Public routing is documented as `worker.example.com -> worker`; accepting deeper names creates ambiguous host ownership and can misroute traffic behind wildcard DNS or cert setups.
- [x] `crates/api/src/app.rs`: public HTTP/1 responses rebuild and parse the same `Alt-Svc` header string for every response when H3 is enabled. This is tiny per request, but it is avoidable hot-path work in the public listener.

Resolution:

- Added first-non-empty private token selection in the server binary and covered preferred, fallback, and all-empty cases.
- Rejected nested public subdomain prefixes before worker lookup and covered the routing regression.
- Precomputed the public H3 `Alt-Svc` header when the listener starts; response handling now only clones the parsed header value.

Verification:

- `cargo fmt --check`
- `cargo test -p dd_server token_selection`
- `cargo test -p dd_server nested_subdomain_host_is_rejected`
- `cargo test -p dd_server --no-run`

### Review Pass 2 Comments

- [x] `crates/api/src/handlers/invocation.rs`: normal HTTP invokes enter an `http.invoke` tracing span in `invoke_worker_with_target`, then immediately create and enter a second `http.invoke` span in `invoke_worker_from_body_stream`. That duplicates trace events and parent propagation work on the main public/private request path; H3 already uses only the inner path.
- [x] `crates/api/src/handlers/util.rs`: private bearer tokens are compared with ordinary string equality. This is not the hot path, but production auth should avoid data-dependent early exit for secret comparisons.

Resolution:

- Removed the outer normal-HTTP invoke span; all body-stream variants now use the single span in `invoke_worker_from_body_stream`.
- Added constant-time byte comparison for private bearer token matching while preserving existing trim behavior for the provided bearer value.

Verification:

- `cargo fmt --check`
- `cargo test -p dd_server private_token_matching_rejects_non_exact_tokens`
- `cargo test -p dd_server --no-run`

### Review Pass 3

No new actionable comments in the touched control-plane/request paths.

Verification:

- `rg -n "^- \[ \]" HARDEN.md` returned no open review comments.
- `just check`

### General Review Pass 4 Comments

- [x] `crates/runtime/src/memory.rs`: memory-state writes repeat the same deleted/live row upsert SQL in transactional STM, blind STM, direct queue batch flush, and direct queue split flush. The repeated SQL makes write-last versus overwrite semantics harder to audit and repeats UTF-8 `Vec<u8> -> String` allocation on every live UTF-8 write.
- [x] `crates/runtime/src/ops/memory.rs`: point read, snapshot, apply, blind apply, and direct enqueue ops spawn a fresh OS thread and one-shot Tokio runtime per storage op. This keeps Deno isolate ops from blocking, but it is expensive for STM-heavy workloads and should become a direct async boundary rather than per-op runtime construction.

Scope note:

- The STM semantics themselves look sound in this pass: exact read-version validation for read dependencies, conservative max-version gating for list reads, a blind-write fast path, persisted version floors, busy timeouts, bounded busy/lock retry, and direct-queue replay on startup are all present.

Resolution:

- Centralized memory state and meta upserts behind helpers with explicit `Overwrite` and `WriteLast` policies, preserving validated STM overwrite behavior and direct-queue higher-version replay behavior.
- Removed the repeated UTF-8 `Vec<u8> -> String` allocation for live UTF-8 writes; the write helper validates and passes a borrowed `&str`.
- Removed per-storage-op OS thread and one-shot Tokio runtime construction from memory ops. Point reads, snapshots, STM apply, blind apply, and direct enqueue now await the store futures directly, and submission polling uses async sleep.
- Collapsed identical memory storage/socket/transport scope fallback helpers into one payload scope helper.

Verification:

- `cargo fmt --check`
- `cargo test -p runtime memory_ --no-run`
- `cargo test -p runtime service::tests::memory::`
- `cargo test -p runtime memory_direct_queue`
- `cargo test -p runtime memory_blind_writes_complete_past_repeated_commit_threshold`
- `cargo test -p runtime memory_transactional_writes_complete_past_repeated_commit_threshold`

### General Review Pass 5

No new actionable comments in the STM storage or memory op boundary paths after the pass-4 fixes.

Verification:

- `rg -n "^- \\[ \\]" HARDEN.md` returned no open review comments.
- `rg -n "std::thread::spawn|tokio::runtime::Builder::new_current_thread|std::thread::sleep" crates/runtime/src/ops/memory.rs` returned no matches.
- `git diff --check`
- `just check`

### General Review Pass 6 Comments

- [x] `crates/api/src/public_quic.rs`: the public H3/WebTransport accept loop keeps unmatched CONNECT headers and flow objects in per-connection `HashMap`s without a cap. Normal event ordering drains them quickly, but malformed or adversarial streams can grow those maps until the connection closes.

Resolution:

- Added a per-connection cap for pending H3/WebTransport handshake halves. Matching still drains the maps immediately in the normal path; over-limit unmatched headers/flows are dropped with a warning instead of retained indefinitely.

Verification:

- `cargo fmt --check`
- `cargo test -p dd_server --no-run`
- `just check`

### General Review Pass 7

No new actionable comments after the STM/op-boundary cleanup and H3 pending-handshake cap.
