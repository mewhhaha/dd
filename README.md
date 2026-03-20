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
- global in-process Cache API (`caches.default` + `caches.open(name)`) shared across workers
- cache index persisted in Turso; large cache bodies spill to blob storage (local FS now, S3-like backend hook ready)

Workers are single JavaScript modules that export a default object with `fetch(request, env, ctx)`.

## Prerequisites

- Rust toolchain

## Environment

Optional settings:

```bash
export BIND_ADDR="127.0.0.1:3000"
export RUST_LOG="info"
export TURSO_DATABASE_URL="file:./grugd-kv.db"
export GRUGD_BLOB_BACKEND="local"
export GRUGD_BLOB_DIR="/tmp/grugd-cache/blobs"
```

`GRUGD_BLOB_BACKEND=s3` is reserved for the upcoming S3-compatible implementation.

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
- cache capacity: 2048 entries, 64 MiB total, LRU-ish eviction on pressure
- cache metadata lives in Turso; inline bodies <= 64KiB, larger bodies use blob storage refs

## Example workers

- `examples/hello.js` - smallest possible worker
- `examples/router.js` - tiny GET/POST router
- `examples/cache.js` - cache-aside with `caches.default`
- `examples/named-cache.js` - cache-aside with `await caches.open("name")`
- `examples/cache-vary.js` - cache variants with `Vary: accept-language`
- `examples/cache-delete.js` - cache invalidation with `cache.delete`
- `examples/stream.js` - `ReadableStream` response chunks
- `examples/kv.js` - KV binding reads/writes (`env.MY_KV`)
- `examples/kv-counter.js` - tiny counter API (`/value`, `/inc`, `/reset`)
- `examples/wait-until.js` - respond now, finish async work in `ctx.waitUntil`
- `examples/wait-until-kv.js` - `waitUntil` background write into KV

Try them quickly:

```bash
cargo run -p cli -- deploy hello examples/hello.js
cargo run -p cli -- deploy router examples/router.js
cargo run -p cli -- deploy cache examples/cache.js
cargo run -p cli -- deploy named-cache examples/named-cache.js
cargo run -p cli -- deploy cache-vary examples/cache-vary.js
cargo run -p cli -- deploy cache-delete examples/cache-delete.js
cargo run -p cli -- deploy stream examples/stream.js
cargo run -p cli -- deploy kv examples/kv.js --kv-binding MY_KV
cargo run -p cli -- deploy kv-counter examples/kv-counter.js --kv-binding MY_KV
cargo run -p cli -- deploy bg examples/wait-until.js
cargo run -p cli -- deploy bg-kv examples/wait-until-kv.js --kv-binding MY_KV
```

Invoke examples:

```bash
cargo run -p cli -- invoke router --method GET --path /health
printf "ping" | cargo run -p cli -- invoke router --method POST --path /echo --header "content-type: text/plain" --body-file -
cargo run -p cli -- invoke cache-vary --method GET --path /greet --header "accept-language: fr"
cargo run -p cli -- invoke stream --method GET --path /
cargo run -p cli -- invoke kv-counter --method POST --path /inc
cargo run -p cli -- invoke kv-counter --method GET --path /value
printf "req-123" | xargs -I{} cargo run -p cli -- invoke bg-kv --method GET --path / --header "x-request-id: {}"
```

Named cache example inside workers:

```js
const apiCache = await caches.open("api-v1");
await apiCache.put(new Request("http://cache/key"), response.clone());
```

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
