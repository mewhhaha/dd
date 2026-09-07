# Runtime consolidation

The accepted target is a Deno/V8 platform for 8–32 CPU single-node servers.
HTTP streaming, service bindings, WebSockets, Vite/Vitest, React Router and
RSC remain supported. KV writes commit before acknowledgement; keyed memory
is private to a worker and uses explicit synchronous transactions.

Implementation units, each requiring observable verification:

- [x] Benchmark comparability, environment metadata and a shared comparison harness.
- [x] Static deployment validation and persistence outside request scheduling.
- [x] External startup deadline, heap termination and one bootstrap snapshot per process.
- [x] Remove Perry/Wasmtime and the direct HTTP/3 server, build options and CI job.
- [x] Durable KV, grouped commits, bounded shared committed-value cache and public types.
- [x] Move control records into storage and separate the rebuildable cache database.
- [x] Remove dynamic workers, host RPC and remaining WebTransport runtime APIs.
- [x] Private memory namespaces and explicit synchronous transactions; migrate callers.
- [x] Fixed state shards, reader pools, persisted ownership and one BLOB representation.
- [x] Worker scheduling, generation-independent entity ownership and cancellation.
- [x] Native HTTP development transport and shared public/config contracts.
- [x] Offline conversion with namespace mapping, verification and cutover docs.
- [x] Integration, crash, concurrency and automatic-shutdown verification.
- [x] Five paired performance runs at 8 and 16 logical CPUs.
- [ ] Repeat performance validation on a host with at least 32 available CPUs.

Functional verification passed on 2026-09-07: `just check` includes formatting,
all-target/all-feature compilation, warning-as-error Clippy, JS and TS contract
checks, and 384 passing workspace tests. The runtime suite has 223 passing
tests and the API suite has 63. The SIGKILL server probe verifies acknowledged
KV/memory writes and deletes, idempotent replay and worker isolation. Native
transport, streaming, cancellation, WebSockets, chat, four framework/HMR smokes,
four example typechecks, and both React Router/RSC browser suites pass.
The automatic-shutdown subprocess regression verifies process exit with pending
outbox deliveries and recovery of all 2,048 committed effects after restart.

The [performance report](../benchmarks/COHERENCE.md) records 70 completed pairs
and all 14 passing median gates on this shared eight-core/16-thread host.
Storage throughput ratios range from 2.139 to 9.540; instant-response ratios
range from 0.988 to 1.057. Three individual threshold misses remain included.
Peak memory increased in several small workloads. The 32-CPU target remains
unverified because this host exposes only 16 logical CPUs.

No existing store has been converted. Deployment retirement closes old
WebSockets with 1012; old generations have at most the configured request wall
timeout to drain. Undeployed workers release their
schedulers, and service replies wake their originating isolate directly.

Stored state must remain untouched during development. Conversion reads the
current format into a separate destination, requires explicit ownership for
ambiguous namespaces, and marks the destination complete only after logical
verification. Existing stores are retained for rollback.

Performance claims require comparable durable semantics, CPU affinity,
toolchain, build flags, memory limits, disk and effective configuration.
Acceptance uses five paired runs at 8, 16 and 32 CPUs, with no more than 5%
throughput regression or 10% p99 regression. Functional checks on smaller
machines do not establish those performance results.
