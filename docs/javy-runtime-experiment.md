# Javy runtime experiment

This branch adds an experimental runtime that bundles a TypeScript or
JavaScript worker into one ES2023 module, compiles it to QuickJS bytecode and
WebAssembly with Javy, and executes the Wasm under Wasmtime. The existing
V8/Deno runtime remains available while the experiment establishes which
worker behavior can be reproduced without it.

Javy still contains a JavaScript engine: QuickJS is compiled to Wasm and the
worker source is stored as QuickJS bytecode. The experiment removes the native
V8/Deno dependency from this execution path, not JavaScript interpretation.

## Build and run

Install the pinned Javy release and workspace dependencies:

```bash
scripts/install-javy.sh
rustup target add wasm32-wasip1
scripts/build-javy-plugin.sh
pnpm install
```

Build the React fixture and start the experimental server:

```bash
JAVY_BIN=target/tools/javy \
JAVY_PLUGIN=target/tools/dd-javy-plugin.wasm \
  pnpm --filter dd-javy-react-example build
cargo run -p javy_host --bin dd_javy_server -- \
  --worker examples/javy-react/dist/worker.wasm \
  --env GREETING=welcome \
  --kv TEST_KV \
  --memory TEST_MEMORY
```

The build has two explicit stages:

1. `@mewhhaha/dd-javy` uses Vite to transpile TypeScript/JSX and bundle every
   dependency into one ES2023 module.
2. Javy compiles that module against `dd-javy-plugin`, producing QuickJS
   bytecode embedded in a Wasm command module with `dd_host` imports.

The Rust host serializes a request envelope to WASI stdin. The worker
compatibility layer constructs `Request` and `env`, invokes the default
export's `fetch` function, buffers the returned `Response`, and serializes it
to WASI stdout. The custom plugin installs a synchronous `__ddHostCall`
function in QuickJS; KV and memory operations cross that import into
Wasmtime. Stderr is reserved for worker console output so logs cannot corrupt
the response protocol.

`JavyWorker` compiles the module once and keeps a bounded pool of complete
Wasmtime stores, Javy/QuickJS instances, and `_start` functions. A checkout
replaces stdin, stdout, stderr, request randomness, and the epoch deadline.
Successful instances return to the pool; traps and protocol failures discard
the instance. The default pool is the smaller of the machine's available
parallelism and eight instances. Each instance is also replaced after 32
successful requests because Javy's repeated command-bytecode evaluation
retains QuickJS allocations.

## Current surface

Working and covered by the checked-in Wasm fixtures:

- synchronous and Promise-based worker fetch functions
- `Headers`, `Request`, `Response`, `URL`, `URLSearchParams`, and URL-encoded
  `FormData`
- `AbortController`, `AbortSignal`, `EventTarget`, and `queueMicrotask`
- host-seeded `crypto.getRandomValues` and `crypto.randomUUID`
- a buffered `ReadableStream` sufficient for React's
  `renderToReadableStream`
- `Response.json`, request/response body text, JSON, and array buffers
- string environment bindings
- ephemeral host-side KV with get, put, delete, and list
- host-side transactional memory variables with optimistic conflict retries
- React 19 `renderToString` and `renderToReadableStream`
- the repository's bundled React Router storefront, including catalog
  initialization, cart form actions, redirects, and checkout
- static assets in `dd_javy_server`
- bounded concurrent instance pooling with module-level worker state
- execution deadlines, a 128 MiB Wasm memory ceiling, and bounded response
  and log output

The Web APIs are intentionally small compatibility implementations. The KV
and memory stores live only for the lifetime of `JavyWorker`; they are a
host-call proof, not the Turso-backed production stores. These surfaces cover
the checked-in fixtures and storefront probe; they are not yet Web Platform
Tests compliant.

## React Router storefront probe

The existing Vite/React Router example can be compiled without changing its
worker source:

```bash
pnpm --filter dd-vite-react-router-example build
JAVY_BIN=target/tools/javy \
JAVY_PLUGIN=target/tools/dd-javy-plugin.wasm \
  node packages/dd-javy/src/cli.js \
  examples/vite-react-router/dist/vite-react-router/worker.js \
  target/vite-react-router-javy.wasm
cargo run -p javy_host --bin dd_javy_server -- \
  --worker target/vite-react-router-javy.wasm \
  --assets-dir examples/vite-react-router/dist/react-router/client \
  --kv STORE_DB \
  --memory EXAMPLE_MEMORY
```

The verified flow is a GET of `/`, an URL-encoded add-to-cart POST, a second
GET with the session cookie, and checkout. That path exercises React Router
SSR and actions, `FormData`, redirects, secure UUID generation, KV catalog and
order writes, and transactional cart state.

## Missing before replacement

Synchronous plugin host calls work, but operations that need suspension,
streaming, or durable platform integration are not implemented:

- outbound `fetch`
- durable Turso-backed KV and keyed memory
- cache bindings
- service bindings
- websockets and transport upgrades
- dynamic workers
- live response streaming and backpressure
- real timers
- the full URL, Fetch, Streams, Web Crypto, and structured-clone specifications

The next runtime milestone should evolve the proven custom-plugin boundary in
this order:

1. request and response transfer without JSON byte arrays
2. connect KV and memory calls to the existing Turso-backed stores
3. asynchronous cache, service, and outbound HTTP calls
4. streaming bodies, real timers, websockets, and dynamic workers

This is now a working stateful React SSR compatibility runtime, but it is not
yet a replacement for `crates/runtime`.

## Benchmark

Measured on 2026-07-27 using a Ryzen 7 7800X3D, Linux 7.1.5, Rust 1.94.0, and
Javy 9.0.0. The pooled and forced-fresh Javy results below are medians of three
runs.

The instant-response comparison excludes HTTP. Javy invokes the compiled
module directly. Pooled Javy keeps at most eight instances and recycles each
after 32 successful requests; forced-fresh Javy sets that limit to one. V8
uses the existing `RuntimeService` with prewarmed isolates, one in-flight
request per isolate. Javy ran 5,000 requests per sample. The V8 results are
the earlier five-run medians over 100,000 requests.

| Runtime | Concurrency | Throughput | p50 | p95 | p99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Javy, pooled (recycle at 32) | 1 | 1,154 req/s | 0.843 ms | 1.080 ms | 1.233 ms |
| Javy, forced fresh | 1 | 939 req/s | 1.060 ms | 1.094 ms | 1.266 ms |
| V8, warm isolate | 1 | 25,076 req/s | 0.040 ms | 0.060 ms | 0.120 ms |
| Javy, pooled (recycle at 32) | 8 | 6,939 req/s | 1.067 ms | 1.539 ms | 1.804 ms |
| Javy, forced fresh | 8 | 4,965 req/s | 1.450 ms | 2.112 ms | 2.268 ms |
| V8, 8 warm isolates | 8 | 82,528 req/s | 0.070 ms | 0.190 ms | 0.490 ms |

Pooling improved throughput by 23% sequentially and 40% at concurrency 8.
Warm V8 still delivered 21.7 times the sequential throughput and 11.9 times
the throughput at concurrency 8. A separately sampled pooled run peaked at
78.0 MiB RSS, versus 51.5 MiB in forced-fresh mode and the earlier 247.5 MiB
measurement for eight V8 isolates. Raising the recycle limit to 256 improved
throughput only slightly but raised peak RSS to 330 MiB; a limit of 10,000
reached the 128 MiB per-instance ceiling after 813 requests. The conservative
32-request default preserves the small-runtime goal.

Wasmtime compilation of the 1,293,814-byte instant worker took 908 ms in the
sampled run.
That happens once in `JavyWorker::new`, not per request, and should be moved to
a serialized Wasmtime compilation cache.

Earlier fresh-instance three-run medians for the larger Javy workloads:

| Worker | Wasm size | Concurrency | Throughput | p50 | p95 | p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| React 19 string SSR fixture | 1,913,067 B | 1 | 376 req/s | 2.650 ms | 2.714 ms | 2.905 ms |
| React 19 string SSR fixture | 1,913,067 B | 8 | 1,974 req/s | 3.748 ms | 6.167 ms | 7.549 ms |
| React Router storefront SSR | 2,331,529 B | 1 | 68 req/s | 14.698 ms | 15.144 ms | 15.923 ms |
| React Router storefront SSR | 2,331,529 B | 8 | 424 req/s | 17.463 ms | 25.645 ms | 31.245 ms |

The storefront benchmark includes its route loaders, KV catalog reads, memory
transaction, React Router, and React rendering. The stores remain the
experimental in-process implementations.

Reproduce the instant-response Javy measurement with:

```bash
DD_BENCH_REQUESTS=5000 \
DD_BENCH_CONCURRENCY=8 \
DD_BENCH_COMPILE_ROUNDS=3 \
taskset -c 0-7 \
  cargo run -p javy_host --bin bench_javy_worker --release
```

Pass `--max-requests-per-instance 1` to reproduce the forced-fresh baseline.
The current result says Javy is viable when footprint and isolation matter
more than latency, but pooling alone does not make it a performance
replacement for warm V8.

## Regenerating fixtures

```bash
scripts/build-javy-plugin.sh
scripts/build-javy-fixtures.sh
cargo test -p javy_host
```

The binary fixtures are checked in so normal Rust tests do not require the
Javy compiler.
