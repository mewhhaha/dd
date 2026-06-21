# dd

`dd` is single-node worker runtime in Rust with Deno-backed isolates. It is inspired by Cloudflare Workers and Durable Objects, but aimed at "run Cloudflare-like workers on one machine with disk-backed storage" rather than "managed global edge platform."

Public traffic is routed by host name, so `hello.example.com` can map to worker `hello`. State lives on disk. For coordination, `dd` does not use Durable Objects as public model. It uses keyed memory namespaces with STM-like `atomic(...)` callbacks, so you shard state by key and treat each shard like transactional memory backed by durable storage.

Worker shape stays familiar: `fetch(request, env, ctx)` plus worker bindings. KV handles simple persistence, Cache API handles response reuse, and memory namespaces handle shardable coordination.

## Quickstart

Private control plane uses bearer auth. CLI reads `DD_PRIVATE_TOKEN` automatically.

```bash
export DD_PRIVATE_TOKEN=dev-token
cargo run -p dd_server
```

In another shell:

```bash
export DD_PRIVATE_TOKEN=dev-token
cargo run -p cli -- --server http://127.0.0.1:8081 deploy hello examples/hello.js --public
cargo run -p cli -- --server http://127.0.0.1:8081 invoke hello --method GET --path /
curl -H 'host: hello.example.com' http://127.0.0.1:8080/
```

Add `--temporary` to deploy a worker that expires one hour after deployment.
Redeploying the same temporary worker with `--temporary` refreshes the hour;
redeploying it without `--temporary` makes it permanent. Deploying
`--temporary` over an already permanent worker is rejected.

Default ports for `cargo run -p dd_server` are `8080` for public traffic and `8081` for private deploy/invoke traffic.

Project deploy settings can live in `dd.json`:

```json
{
  "name": "hello",
  "entrypoint": "examples/hello.js",
  "base_url": "https://your-dd-app.fly.dev",
  "config": { "public": true }
}
```

`base_url` is non-secret. Store deploy tokens in the OS credential store:

```bash
cargo run -p cli -- auth login
cargo run -p cli -- deploy-config dist/dd.deploy.json
```

CLI server precedence is `--server`, `DD_SERVER`, config `base_url`, then the
local private default.

## Memory namespaces

Memory namespace is main coordination primitive. You pick key, get shard, run synchronous `atomic(...)` callback against that shard.

Deploy with memory binding:

```bash
cargo run -p cli -- --server http://127.0.0.1:8081 \
  deploy counter worker.js --memory-binding COUNTERS
```

Worker:

```js
export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const user = url.searchParams.get("user") ?? "anonymous";
    const memory = env.COUNTERS.get(env.COUNTERS.idFromName(user));
    const count = memory.tvar("count", 0);

    if (request.method === "POST") {
      const next = await memory.atomic(() => {
        const value = Number(count.read()) + 1;
        count.write(value);
        return value;
      });
      return Response.json({ user, count: next });
    }

    const current = await memory.atomic(() => Number(count.read()) || 0);
    return Response.json({ user, count: current });
  },
};
```

This is closest thing to Durable Objects, but model is different. You are not instantiating long-lived object class with special lifecycle. You are reading and writing shard of transactional memory selected by key.

## KV

KV is for simpler key/value storage where you do not need shard-local coordination.

Deploy with KV binding:

```bash
cargo run -p cli -- --server http://127.0.0.1:8081 \
  deploy kv worker.js --kv-binding MY_KV
```

Worker:

```js
export default {
  async fetch(_request, env) {
    const current = Number((await env.MY_KV.get("hits")) ?? "0") || 0;
    const next = current + 1;
    await env.MY_KV.set("hits", String(next));

    return new Response(`hits=${next}`, {
      headers: { "content-type": "text/plain; charset=utf-8" },
    });
  },
};
```

## Cache API

Cache API looks like worker-style response cache. Good for HTTP response reuse, not coordination.

Worker:

```js
export default {
  async fetch(request) {
    const cache = caches.default;
    const key = new Request(request.url, { method: "GET" });
    const cached = await cache.match(key);
    if (cached) {
      return cached;
    }

    const response = new Response("fresh response", {
      headers: {
        "content-type": "text/plain; charset=utf-8",
        "cache-control": "public, max-age=60",
      },
    });

    await cache.put(key, response.clone());
    return response;
  },
};
```

## Dynamic workers and assets

Workers can create other workers with `env.SANDBOX.get/list/delete`. Dynamic workers run in separate Deno isolates inside same process, so they fit buggy or semi-hostile agent code that should not share parent runtime state. That is runtime isolation and containment, not OS process, VM, or container sandboxing.

Default child policy is deny-first. No outbound fetch, no host RPC, no websocket/transport upgrade, no cache access unless child opts in:

```js
const child = await env.SANDBOX.get("agent:v1", async () => ({
  entrypoint: "worker.js",
  modules: {
    "worker.js": `
      export default {
        async fetch(request) {
          return new Response("ok");
        },
      };
    `,
  },
  egress_allow_hosts: ["api.openai.com"],
  max_request_bytes: 1_048_576,
  max_response_bytes: 2_097_152,
  max_outbound_requests: 8,
  max_concurrency: 8,
  timeout: 2_500,
}));
```

If child needs parent callback, opt in explicitly:

```js
const child = await env.SANDBOX.get("preview:v1", async () => ({
  entrypoint: "worker.js",
  modules: previewModules(),
  allow_host_rpc: true,
  env: { PREVIEW: new PreviewControl("preview:v1") },
  timeout: 3_000,
}));
```

See [examples/dynamic-namespace.js](examples/dynamic-namespace.js), [examples/preview-dynamic.js](examples/preview-dynamic.js), and [examples/llm-dynamic-exec.js](examples/llm-dynamic-exec.js).

Static assets can be bundled at deploy time with `--assets-dir`. Files are served before worker code runs, with root `_headers` support similar to Cloudflare static assets. See [examples/static-assets-site](examples/static-assets-site).

Chat app example combines memory namespace, websockets, and deploy-time assets in [examples/chat-worker](examples/chat-worker).

## Vite and Vitest dev mode

Workers can be tested and developed against the native runtime without starting
`dd_server`. The dev package in [packages/dd-vite](packages/dd-vite) launches
`dd_dev_runtime` over stdio, deploys worker source into `RuntimeService`, and
invokes it directly from Vitest helpers or a Vite plugin.

This is the debug/dev path where `eval` and `new Function` are allowed. It is
not a production control plane.

`@mewhhaha/vite-plugin-dd` can use `@mewhhaha/dd`, a small wrapper with platform-specific optional
runtime packages, so installs pull only the binary for the current OS/CPU.

The Vite plugin uses Vite's Environment API shape and preserves normal Vite HMR;
hot updates invalidate the deployed worker and rebuild it lazily on the next
worker request. Framework integrations use subpath presets such as
`@mewhhaha/vite-plugin-dd/react-router` and
`@mewhhaha/vite-plugin-dd/react-router-rsc`.

During `vite build`, the plugin emits `dist/dd.deploy.json` and a bundled
`dist/worker.js`. The generated config keeps the deploy fields the CLI consumes
while pointing at the bundled worker and Vite output assets.

`@mewhhaha/vite-plugin-dd` has a default plugin export, so configs can use any local name:
`import dd from "@mewhhaha/vite-plugin-dd"`. By default it reads `dd.json` from the nearest
package root for the worker name, source entrypoint, and deploy config; inline
plugin options override that file.

See [docs/development.md](docs/development.md#vite-and-vitest-worker-development).

## How to think about it

If you want "Cloudflare-style worker runtime on one box," `dd` is that shape.

If you want "Durable Objects, but expressed as disk-backed STM-like shards instead of object instances," memory namespaces are that shape.

If you want one app process you can deploy to Fly or another VM and then load with named workers, `dd_server` is that shape.

## Fly

Fly runs one `dd_server` app process. Workers are deployed into that app; they are not separate Fly apps.

Canonical flow:

1. deploy app/container with `flyctl deploy`
2. open private tunnel with `just fly-proxy <app>`
3. mint a scoped token with `just fly-worker-mint-token ...`
4. deploy through the public endpoint with `DD_TOKEN`

The private admin resource `/v1/admin/tokens` creates, lists, reads, and deletes
bearer tokens with explicit capabilities: worker names, public/private deploy
permission, allowed bindings, internal trace permission, source and asset size
limits, optional expiry, and optional max uses. Token names are unique
lowercase, dash-delimited ids, so `my-token-at-home` is the value used later for
listing, reading, and deletion. Public `POST /v1/deploy` accepts those scoped
tokens, so GitHub Actions can deploy one worker without carrying the private
control-plane secret. Locally, `dd auth login` stores that deploy token in the
OS credential store, scoped by the resolved `base_url`.

Full guide: [deploy/fly/README.md](deploy/fly/README.md)

## Benchmarks and docs

Runtime benchmark:

```bash
cargo run -p runtime --bin bench --release
```

Keyed memory benchmark:

```bash
cargo run -p runtime --bin bench_memory_storage
```

Current numbers live in [BENCHMARK.md](BENCHMARK.md). Hardening progress lives in [HARDEN.md](HARDEN.md). Contributor/dev notes live in [docs/development.md](docs/development.md).

## Current metrics

Measured on `2026-06-19T09:35:25+02:00` from the current worktree on `Linux 7.0.9-1-cachyos x86_64 GNU/Linux` with `rustc 1.96.0-nightly (3645249d7 2026-03-16)`.

Project shape:

| metric | value |
| --- | ---: |
| workspace crates | 4 |
| tracked files | 199 |
| Rust files | 84 |
| Rust source lines | 52,475 |
| total tracked lines | 106,694 |
| Rust test attributes | 224 |

Distribution artifacts use the `dist` Cargo profile:

```bash
cargo build --locked --profile dist -p dd_server -p cli
just size-report
```

Historical release artifacts from `cargo build --release -p dd_server -p cli`:

| artifact | unstripped | stripped temporary copy |
| --- | ---: | ---: |
| `target/release/dd_server` | 116,086,200 bytes / 110.71 MiB | 88,256,352 bytes / 84.17 MiB |
| `target/release/cli` | 7,458,232 bytes / 7.11 MiB | 5,430,640 bytes / 5.18 MiB |

Current `dd_server` size work and the selected `dist` profile report are summarized in [docs/binary-size-report.md](docs/binary-size-report.md).

Current focused benchmark results:

| command | scenario | requests | concurrency | throughput | mean | p50 | p95 | p99 |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `bench_memory_storage` | direct read memory | 300 | 16 | 12,883 req/s | 1.23ms | 0.75ms | 3.96ms | 12.69ms |
| `bench_memory_storage` | direct write memory | 300 | 16 | 2,077 req/s | 7.52ms | 7.30ms | 10.62ms | 19.53ms |
| `bench` with `DD_BENCH_ONLY=dynamic` | dynamic baseline, autoscaling-8 | 100 | 16 | 4,259 req/s | 3.72ms | 0.86ms | 17.01ms | 23.32ms |
| `bench` with `DD_BENCH_ONLY=dynamic` | dynamic hot fetch, autoscaling-8 | 100 | 16 | 3,445 req/s | 4.13ms | 4.50ms | 6.72ms | 7.36ms |
| `bench` with `DD_BENCH_ONLY=dynamic` | dynamic hot fetch + host RPC, autoscaling-8 | 100 | 16 | 2,091 req/s | 6.92ms | 6.71ms | 8.93ms | 9.65ms |

Benchmark commands:

```bash
DD_BENCH_MODE=direct-read-memory DD_BENCH_REQUESTS=300 DD_BENCH_CONCURRENCY=16 DD_BENCH_MAX_ISOLATES=4 DD_BENCH_MAX_INFLIGHT=8 cargo run -p runtime --bin bench_memory_storage --release
DD_BENCH_MODE=direct-write-memory DD_BENCH_REQUESTS=300 DD_BENCH_CONCURRENCY=16 DD_BENCH_MAX_ISOLATES=4 DD_BENCH_MAX_INFLIGHT=8 cargo run -p runtime --bin bench_memory_storage --release
DD_BENCH_ONLY=dynamic DD_BENCH_DYNAMIC_REQUESTS=100 DD_BENCH_DYNAMIC_CONCURRENCY=16 DD_BENCH_DYNAMIC_COLD_ROUNDS=10 cargo run -p runtime --bin bench --release
```

Current caveat: the full default `cargo run -p runtime --bin bench --release` benchmark still exits with signal `139` after the first `instant-response` row (`464 req/s`, mean `129.13ms`). The focused memory and dynamic benchmark commands above completed successfully.
