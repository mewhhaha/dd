# grugd

A tiny workers platform built in Rust with Axum and autoscaling JavaScript isolates.

## What it does

This MVP supports:

- `POST /deploy` with `{ "name", "source", "config": { "bindings": [...] } }`
- raw HTTP invoke via `ANY /invoke/:worker/*path`
- a tiny CLI for deploying and invoking named workers
- in-memory worker state inside the server process
- per-worker isolate pools with autoscale up/down
- bootstrap snapshot at process start plus worker snapshot on deploy
- isolate reuse (module/global state is preserved for warm isolates)
- `ctx.waitUntil()` with a 30s cap
- optional Turso KV bindings injected in `env`

Workers are single JavaScript modules that export a default object with `fetch(request, env, ctx)`.

## Prerequisites

- Rust toolchain

## Environment

Optional settings:

```bash
export BIND_ADDR="127.0.0.1:3000"
export RUST_LOG="info"
export TURSO_DATABASE_URL="file:./grugd-kv.db"
```

## Run

```bash
cargo run -p api
```

## CLI

```bash
cargo run -p cli -- deploy hello examples/hello.js
cargo run -p cli -- invoke hello --method POST --path /echo --header "content-type: text/plain" --body-file -
```

Workers live in memory, so restarting the server clears them.

## Benchmark

Run the runtime benchmark with:

```bash
cargo run -p runtime --bin bench --release
```

Current baseline results are in `BENCHMARKS.md`.

## Runtime behavior defaults

- single-node only
- per-worker pool min=0, max=8, idle TTL=30s
- unlimited per-worker FIFO queue
- up to 4 inflight requests per isolate by default
- dropped invokes are canceled and signaled via `ctx.signal`

## Example worker

See `examples/hello.js`.

## Deploy

```bash
curl -X POST http://localhost:3000/deploy \
  -H "content-type: application/json" \
  -d @- <<'JSON'
{
  "name": "hello",
  "source": "export default { async fetch(request, env, ctx) { return new Response('hello from worker'); } }",
  "config": {
    "bindings": [
      { "type": "kv", "binding": "MY_KV" }
    ]
  }
}
JSON
```

## Invoke

```bash
cargo run -p cli -- invoke hello --method GET --path /
```

Expected body:

```text
hello from worker
```
