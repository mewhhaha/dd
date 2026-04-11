# dd

A tiny workers platform built in Rust with Deno-backed isolates, Hyper/TCP ingress, and QUIC/HTTP3 public ingress.

## What it does

This MVP supports:

- private control listener (`POST /v1/deploy`, `ANY /v1/invoke/:worker/*path`)
- public listener with host routing (`worker.example.com/* -> worker`)
- a tiny CLI for deploying and invoking named workers
- in-memory worker state inside the server process
- per-worker isolate pools with autoscale up/down
- bootstrap snapshot at process start plus worker snapshot on deploy
- isolate reuse (module/global state is preserved for warm isolates)
- `ctx.waitUntil()` with a 30s cap
- optional Turso KV bindings injected in `env`
- optional keyed memory namespace bindings in `env` (`idFromName()` + `get()`)
- optional dynamic namespace bindings in `env` (`get(id, factory)`, `list()`, `delete(id)`)
- private dynamic worker deploys (`POST /v1/dynamic/deploy`) for ephemeral LLM-style code execution
- global in-process Cache API (`caches.default` + `caches.open(name)`) shared across workers
- cache index persisted in Turso; large cache bodies spill to blob storage (local FS now, S3-like backend hook ready)

Workers are single JavaScript modules that export a default object with `fetch(request, env, ctx)`.

## Prerequisites

- Rust toolchain
- Cap'n Proto compiler (`capnp`) available on `PATH`

## Patch workflow

Patched crate overrides are generated under `./patched-crates`, and the actual source patch lives under `./patches`. The workspace owns the override through `[patch.crates-io]`, but only the patch file is checked in.

```bash
just patch deno_crypto
```

That downloads or reuses the currently locked crates.io source, materializes `patched-crates/deno_crypto`, and applies `patches/deno_crypto.patch` if it exists.

If you edit the generated crate, save the diff back into the checked-in patch file with:

```bash
just patch-save deno_crypto 0.255.0
```

To rebuild the generated copy from crates.io source and reapply the saved patch:

```bash
just patch-refresh deno_crypto 0.255.0
```

## Environment

Standalone binary settings:

```bash
export BIND_PUBLIC_ADDR="0.0.0.0:8080"
export BIND_PRIVATE_ADDR="[::]:8081"
export PUBLIC_BASE_DOMAIN="example.com"
export RUST_LOG="info"
export OTEL_EXPORTER_OTLP_ENDPOINT="http://127.0.0.1:4317"
```

`OTEL_EXPORTER_OTLP_ENDPOINT` (or `DD_OTEL_ENDPOINT`) enables OTLP span export.

Runtime/storage settings are now typed config (no env wiring). See `dd_server::ServerConfig`.

## Run

```bash
cargo run -p dd_server
```

## Embed as a library

```rust
use dd_server::{run, ServerConfig};
use runtime::{BlobStoreConfig, RuntimeServiceConfig, RuntimeStorageConfig};
use std::path::PathBuf;

let store_dir = PathBuf::from("./store");
run(ServerConfig {
    bind_public_addr: "127.0.0.1:3000".parse().unwrap(),
    bind_private_addr: "127.0.0.1:3001".parse().unwrap(),
    public_base_domain: "example.com".to_string(),
    invoke_max_body_bytes: 16 * 1024 * 1024,
    runtime: RuntimeServiceConfig {
        runtime: Default::default(),
        storage: RuntimeStorageConfig {
            store_dir: store_dir.clone(),
            database_url: format!("file:{}/dd-kv.db", store_dir.display()),
            // Internal field name; this controls memory-namespace sharding.
            actor_shards_per_namespace: 64,
            worker_store_enabled: true,
            blob_store: BlobStoreConfig::local(store_dir.join("blobs")),
        },
    },
}).await?;
```

## CLI

```bash
cargo run -p cli -- --server http://127.0.0.1:3001 deploy hello examples/hello.js --public
cargo run -p cli -- --server http://127.0.0.1:3001 deploy static-assets-site examples/static-assets-site/worker.js --public --assets-dir examples/static-assets-site/assets
cargo run -p cli -- --server http://127.0.0.1:3001 invoke hello --method POST --path /echo --header "content-type: text/plain" --body-file -
cargo run -p cli -- --server http://127.0.0.1:3001 dynamic-deploy examples/hello.js --env OPENAI_API_KEY=sk-...
cargo run -p cli -- --server http://127.0.0.1:3001 deploy dynamic examples/dynamic-namespace.js --dynamic-binding SANDBOX
cargo run -p cli -- --server http://127.0.0.1:3001 deploy memory examples/memory.js --memory-binding USER_MEMORY
```

`--memory-binding` is the canonical user-facing flag. `--actor-binding` is still accepted as a legacy alias while the repo finishes converging on memory-namespace terminology.

`deploy --public` exposes a worker on the public listener. Without `--public`, a worker is private-only and can only be invoked through the private listener.

`deploy --assets-dir path/to/assets` bundles exact static files at worker root paths before worker code runs. A root `_headers` file in that directory applies asset-only response headers.

Worker source/config is persisted under `./store/workers` by default and restored at startup.

For Fly private deploys, use `deploy/fly/proxy-private-deploy.sh` and point CLI to `http://127.0.0.1:18081`.

Dynamic deploys are private-only and in-memory (not restored from `./store/workers`).
Dynamic env values are exposed to user code as opaque placeholders and replaced with real values only at host I/O boundaries. Dynamic workers are sandboxed with no direct outbound network access.

Dynamic namespace bindings let a normal worker spawn dynamic workers directly:

```js
class MyApi extends RpcTarget {
  async ping(name) {
    return `hello ${name}`;
  }
}

const child = await env.SANDBOX.get("tool:v1", async () => ({
  entrypoint: "worker.js",
  modules: {
    "worker.js": "export default { async fetch(_req, env) { return new Response(await env.API.ping('worker')); } };",
  },
  env: {
    API_TOKEN: "secret-value",
    API: new MyApi(),
  },
  timeout: 2500,
}));
const response = await child.fetch("http://worker/");
const ids = await env.SANDBOX.list();
const deleted = await env.SANDBOX.delete("tool:v1");
```

## Fly deploy model

On Fly, there is one app process (`dd-private-...`) running `dd_server`.
Workers are not separate Fly apps.

- `flyctl deploy` updates the `dd_server` binary/container.
- `dd deploy` (`cargo run -p cli -- deploy ...`) uploads worker source into that running app via `/v1/deploy`.
- `dd deploy ... --assets-dir path/to/assets` bundles exact static files at worker root paths, and a root `_headers` file in that directory applies asset-only headers.

Deploy a new worker to an already running Fly app:

```bash
./deploy/fly/proxy-private-deploy.sh dd-private-8956e096 18081 8081
cargo run -p cli -- --server http://127.0.0.1:18081 deploy hello examples/hello.js --public
```

This deploys worker `hello` inside `dd-private-8956e096`; it does not create another Fly app.

## Benchmark

Run the runtime benchmark with:

```bash
cargo run -p runtime --bin bench --release
```

Run the keyed memory namespace benchmark with:

```bash
cargo run -p runtime --bin bench_actor_storage
```

Current baseline results are in `BENCHMARKS.md`.

## Runtime behavior defaults

- single-node only
- per-worker pool min=0, max=8, idle TTL=30s
- unlimited per-worker FIFO queue
- up to 4 inflight requests per isolate by default
- memory namespace routing is opt-in; by default workers stay pooled
- same memory key can run with multiple in-flight transactions across isolates
- KV `get/put` accept JS values (strings stay UTF-8; non-strings use structured storage encoding)
- memory namespaces are available through namespace stubs like `env.USER_MEMORY.get(id)`
- `memory.atomic(() => ...)` is the retryable STM region
- `memory.tvar("count", 0)` gives a lazy default; `memory.var("key").read()` is a snapshot read outside `atomic` and becomes transactional inside it
- websocket handles can be reopened with `new WebSocket(handle)` inside memory namespace code; send/close frames through that object, not `stub.sockets`
- memory writes are write-last with monotonic versions
- structured values use V8 storage serialization (`forStorage: true`) and reject unsupported host/function types
- memory namespaces use shards internally (default 64 shards per namespace)
- dropped invokes are canceled and signaled via `ctx.signal`
- Spectre-style timer mitigation is enabled: `Date.now()` / `performance.now()` stay frozen between host I/O boundaries
- cache capacity: 2048 entries, 64 MiB total, LRU-ish eviction on pressure
- cache metadata lives in Turso; inline bodies <= 64KiB, larger bodies use blob storage refs
- local defaults persist into `./store` (`workers`, `dd-kv.db`, `blobs`)
- W3C `traceparent` is extracted/injected on invoke requests
- responses include `x-dd-trace-id` for quick correlation

The user-facing primitive is the memory namespace. STM gives you transactional reads/writes inside a namespace, and you can build actor-style coordination on top of that when you need it. The repo still has some internal binary/file names with `actor` in them, but the model exposed to worker authors is memory-first.
For public host-routed traffic, workers receive the honest external request URL such as `https://chat.example.com/rooms/test?x=1`. Private `/v1/invoke/:worker/...` requests remain synthetic and use `http://worker/...`.

## Example workers

- `examples/hello.js` - smallest possible worker
- `examples/router.js` - tiny GET/POST router
- `examples/bundled-router/` - TypeScript router bundled with `pnpm` + `tsdown`
- `examples/cache.js` - cache-aside with `caches.default`
- `examples/named-cache.js` - cache-aside with `await caches.open("name")`
- `examples/cache-vary.js` - cache variants with `Vary: accept-language`
- `examples/cache-delete.js` - cache invalidation with `cache.delete`
- `examples/stream.js` - `ReadableStream` response chunks
- `examples/kv.js` - KV binding reads/writes (`env.MY_KV`)
- `examples/kv-counter.js` - tiny counter API (`/value`, `/inc`, `/reset`)
- `examples/wait-until.js` - respond now, finish async work in `ctx.waitUntil`
- `examples/wait-until-kv.js` - `waitUntil` background write into KV
- `examples/memory.js` - keyed memory namespace (`env.USER_MEMORY.idFromName/get`, `atomic`, `tvar`)
- `examples/hello-traced.js` - mixed KV + memory + cache demo with a tiny HTML UI
- `examples/transport-memory.js` - memory namespace plus transport accept/wake plumbing
- `examples/receipts.js` - receipt CRUD API (`POST/GET/DELETE /receipts`)
- `examples/trace-hub.js` - minimal internal trace receiver and web view
- `examples/dynamic-namespace.js` - spawn + invoke dynamic workers from `env.SANDBOX`
- `examples/llm-dynamic-exec.js` - pretend LLM planner that executes via dynamic workers
- `examples/preview-dynamic.js` - dynamic preview environments (`/preview/{id}`)
- `examples/chat-worker/` - multi-user chat built on a room memory namespace with deploy-time static assets
- `examples/static-assets-site/` - tiny website where `/index.html`, `/styles.css`, and `/app.js` come from `--assets-dir`

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
cargo run -p cli -- deploy memory examples/memory.js --memory-binding USER_MEMORY
cargo run -p cli -- deploy hello-traced examples/hello-traced.js --kv-binding APP_KV --memory-binding USER_MEMORY
cargo run -p cli -- deploy receipts examples/receipts.js --kv-binding RECEIPTS
cargo run -p cli -- deploy trace-hub examples/trace-hub.js
cargo run -p cli -- deploy dynamic examples/dynamic-namespace.js --dynamic-binding SANDBOX
cargo run -p cli -- deploy llm-dynamic examples/llm-dynamic-exec.js --dynamic-binding SANDBOX
cargo run -p cli -- deploy preview-dynamic examples/preview-dynamic.js --dynamic-binding SANDBOX
cargo run -p cli -- deploy chat examples/chat-worker/src/worker.js --memory-binding CHAT_ROOM --public --assets-dir examples/chat-worker/assets
cargo run -p cli -- deploy static-assets-site examples/static-assets-site/worker.js --public --assets-dir examples/static-assets-site/assets
```

Build/deploy the bundled TypeScript router:

```bash
cd examples/bundled-router
pnpm install
pnpm run build
cd ../..
cargo run -p cli -- deploy bundled-router examples/bundled-router/dist/worker.js
```

Invoke examples:

```bash
cargo run -p cli -- invoke router --method GET --path /health
printf "ping" | cargo run -p cli -- invoke router --method POST --path /echo --header "content-type: text/plain" --body-file -
cargo run -p cli -- invoke bundled-router --method GET --path /health
cargo run -p cli -- invoke cache-vary --method GET --path /greet --header "accept-language: fr"
cargo run -p cli -- invoke stream --method GET --path /
cargo run -p cli -- invoke kv-counter --method GET --path /
cargo run -p cli -- invoke kv-counter --method POST --path /inc
cargo run -p cli -- invoke kv-counter --method GET --path /value
printf "req-123" | xargs -I{} cargo run -p cli -- invoke bg-kv --method GET --path / --header "x-request-id: {}"
cargo run -p cli -- invoke bg-kv --method GET --path / --header "x-request-id: verify-root"
cargo run -p cli -- invoke memory --method GET --path /
cargo run -p cli -- invoke memory --method POST --path /inc?user=alice
cargo run -p cli -- invoke memory --method GET --path /value?user=alice
cargo run -p cli -- invoke receipts --method POST --path /receipts --header "content-type: application/json" --body-file -
cargo run -p cli -- invoke receipts --method GET --path /receipts
cargo run -p cli -- invoke dynamic --method GET --path /run?path=/hello
cargo run -p cli -- invoke llm-dynamic --method GET --path /run?prompt=echo\&input=hello
cargo run -p cli -- invoke llm-dynamic --method GET --path /run?prompt=sum\&n=1\&n=2\&n=3
cargo run -p cli -- invoke preview-dynamic --method GET --path /preview/pr-123
cargo run -p cli -- invoke preview-dynamic --method GET --path /preview/pr-123/api/health
```

Primary public route notes:

- `kv-counter`: use `/` for docs, then `/value`, `/inc`, `/reset`.
- `memory`: use `/` for docs, then `/ping`, `/inc?user=...`, `/value?user=...`.
- `bg-kv`: use `/` for docs, then repeat `/` with an `x-request-id` header for the queued write path.
- `dynamic`, `llm-dynamic`, `preview-dynamic`: use `/` for docs, then their explicit `/run` or `/preview/{id}` paths.
- `trace-hub` includes a root summary; cache examples are best verified on their primary behavior routes.

Run the checked-in public smoke matrix with:

```bash
bash scripts/smoke_examples.sh
```
Deploy a trace hub and route traces from another worker:

```bash
cargo run -p cli -- deploy trace-hub examples/trace-hub.js
cargo run -p cli -- deploy api examples/hello.js --trace-worker trace-hub --trace-path /ingest
```

View traces:

```bash
curl http://localhost:3001/v1/invoke/trace-hub/
curl "http://localhost:3001/v1/invoke/trace-hub/traces?app=api"
```

Named cache example inside workers:

```js
const apiCache = await caches.open("api-v1");
await apiCache.put(new Request("http://cache/key"), response.clone());
```

## Deploy

```bash
curl -X POST http://localhost:3001/v1/deploy \
  -H "content-type: application/json" \
  -d @- <<'JSON'
{
  "name": "hello",
  "source": "export default { async fetch(request, env, ctx) { return new Response('hello from worker'); } }",
  "config": {
    "public": true,
    "bindings": [
      { "type": "kv", "binding": "MY_KV" },
      { "type": "dynamic", "binding": "SANDBOX" }
    ]
  }
}
JSON
```

CLI can do the same via:

```bash
cargo run -p cli -- deploy api examples/hello.js --trace-worker trace-hub --trace-path /ingest
```

## Invoke

```bash
cargo run -p cli -- --server http://127.0.0.1:3001 invoke hello --method GET --path /
```

Expected body:

```text
hello from worker
```

Public invoke shape (when `PUBLIC_BASE_DOMAIN=example.com`) is host-based:

```bash
curl -H "host: hello.example.com" http://127.0.0.1:3000/
```

Inside worker code for that direct request, `request.url` is:

```js
"http://hello.example.com/"
```

If the public listener sits behind a proxy that sends `x-forwarded-host` / `x-forwarded-proto`, workers instead see the honest external URL, for example:

```js
"https://chat.wdyt.chat/rooms/test?x=1"
```
