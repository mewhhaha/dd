# Development Guide

Contributor-focused notes; the root README stays product- and usage-focused.

## Prerequisites

- Rust toolchain (see `rust-toolchain.toml`)
- The Perry compiler for building examples and regenerating test fixtures:
  `npm install -g @perryts/perry` (or set `PERRY` to the binary)

## Workspace layout

- `crates/common` — shared error types and the deploy/invoke protocol
- `crates/storage` — turso-backed KV, keyed memory namespaces, response
  cache, and blob storage (no engine dependencies)
- `crates/runtime` — the runtime: Perry wasm ABI host, engine, websockets,
  `dd_server`, and the invoke benchmark
- `crates/cli` — the `dd` CLI (compiles workers with Perry, deploys them)
- `crates/init` — container entrypoint (store setup, exec)

## Local run

`cargo run -p runtime --bin dd_server` defaults to:

- public listener: `http://0.0.0.0:8080` (Host-label routing)
- private listener: `http://[::]:8081` (deploy/list/delete/invoke)

`just check` is the CI path: fmt, clippy (deny warnings), and the full test
suite. `just smoke-examples` deploys every example through the CLI against a
local server and exercises it. `just fixtures` regenerates the vendored
Perry-compiled wasm fixtures in `crates/runtime/fixtures/` (the tests run
against those, so CI does not need Perry).

## Runtime internals

The Perry wasm ABI (NaN-boxed values, the `mem_call` dispatch bus, host
heap, instance pooling, the websocket dispatcher) is documented in
[wasm-runtime.md](wasm-runtime.md). The reference semantics for bridge
functions is `wasm_runtime.js` in the Perry repository — when behavior is
in doubt, match the browser glue.
