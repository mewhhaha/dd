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

The stdin/stdout request ABI still requires a fresh Wasm instance for every
request. Synchronous plugin host calls work, but operations that need
suspension, streaming, or durable platform integration are not implemented:

- outbound `fetch`
- durable Turso-backed KV and keyed memory
- cache bindings
- service bindings
- websockets and transport upgrades
- dynamic workers
- persistent module-level state
- live response streaming and backpressure
- real timers
- the full URL, Fetch, Streams, Web Crypto, and structured-clone specifications

The next runtime milestone should evolve the proven custom-plugin boundary in
this order:

1. request and response transfer without JSON byte arrays
2. instance pooling and per-request context reset
3. connect KV and memory calls to the existing Turso-backed stores
4. asynchronous cache, service, and outbound HTTP calls
5. streaming bodies, real timers, websockets, and dynamic workers

This is now a working stateful React SSR compatibility runtime, but it is not
yet a replacement for `crates/runtime`.

## Regenerating fixtures

```bash
scripts/build-javy-plugin.sh
scripts/build-javy-fixtures.sh
cargo test -p javy_host
```

The binary fixtures are checked in so normal Rust tests do not require the
Javy compiler.
