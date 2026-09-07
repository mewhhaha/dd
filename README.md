# dd

`dd` runs JavaScript workers on one machine using Rust, Deno/V8 isolates, and durable local storage.

Public traffic is routed by host name, so `hello.example.com` can map to worker `hello`. KV stores structured values. Keyed memory provides ordered transactions that commit state and emitted effects together. Both are private to a worker and binding.

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
  "$schema": "./schema/dd.schema.json",
  "schema_version": 1,
  "name": "hello",
  "entrypoint": "examples/hello.js",
  "base_url": "https://your-dd-app.fly.dev",
  "config": { "public": true }
}
```

Configuration schema version `1` is required. Unknown fields are rejected;
editors can use [schema/dd.schema.json](schema/dd.schema.json) for validation and completion.

`base_url` is non-secret. Store deploy tokens in the OS credential store:

```bash
cargo run -p cli -- auth login
cargo run -p cli -- deploy-config dist/dd.deploy.json
```

CLI server precedence is `--server`, `DD_SERVER`, config `base_url`, then the
local private default.

Workers are private unless `config.public` is `true`. A private worker can still
be called from another worker with a service binding:

```json
{
  "schema_version": 1,
  "name": "frontend",
  "entrypoint": "worker.js",
  "config": {
    "public": true,
    "bindings": [
      { "type": "service", "binding": "AUTH", "service": "auth-worker" }
    ]
  }
}
```

```js
export default {
  async fetch(request, env) {
    return env.AUTH.fetch(new URL("/session", request.url));
  },
};
```

## Memory namespaces

Pick an entity key and call `atomic(tx => ...)`. Transactions for that worker, binding and key are ordered across isolates and redeployments. The synchronous callback runs once in the caller's isolate and can capture local variables. Its writes and emitted effects commit together before the promise resolves. Async callbacks and returned thenables are rejected; failed callbacks commit nothing.

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
    const memory = env.COUNTERS.get(user);

    if (request.method === "POST") {
      const next = await memory.atomic((tx) => {
        const value = Number(tx.get("count") ?? 0) + 1;
        tx.put("count", value);
        return value;
      });
      return Response.json({ user, count: next });
    }

    const current = await memory.atomic((tx) => Number(tx.get("count") ?? 0));
    return Response.json({ user, count: current });
  },
};
```

Pass `{ idempotencyKey: "command-id" }` as the second argument to replay a committed result across retries and restarts. Reads, writes, listing and effects use the explicit transaction object; that object becomes unavailable when the callback returns. Memory identity survives worker redeployment.

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
  async fetch(request, env) {
    await env.MY_KV.put("lastVisit", { path: new URL(request.url).pathname });
    return Response.json(await env.MY_KV.get("lastVisit"));
  },
};
```

`get` returns the stored structured value or `null`. `put` and `delete` resolve
after durable commit; `list` returns an array of `{ key, value }` entries.
Committed writes are visible across isolates. Use keyed memory transactions for
operations that must read and update a value atomically.

## Cache API

Cache API looks like worker-style response cache. Good for HTTP response reuse, not coordination. Cache namespaces are isolated per worker.

The platform front cache is separate and opt-in. Set `config.cache.enabled` in `dd.json`/the deployment document, or pass `dd deploy --cache`. It caches only unauthenticated `GET`/`HEAD` responses that explicitly include `Cache-Control: public` and a positive `s-maxage` or `max-age`; `stale-while-revalidate` refreshes stale entries in the background.

```json
{
  "config": {
    "public": true,
    "cache": { "enabled": true }
  }
}
```

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

## Outbound HTTP and assets

Static deployments declare allowed outbound origins in `config.egress_allow_hosts`, for example `["api.example.com"]`. Outbound fetch is denied by default. The runtime checks the destination and redirects; local infrastructure requires an explicit rule such as `private:127.0.0.1:8080`.

Static assets can be bundled at deploy time with `--assets-dir`. Files are served before worker code runs, with root `_headers` support similar to Cloudflare static assets. See [examples/static-assets-site](examples/static-assets-site).

Chat app example combines memory namespace, websockets, and deploy-time assets in [examples/chat-worker](examples/chat-worker).

## Vite and Vitest dev mode

Workers can be tested and developed against the native runtime without starting
`dd_server`. The dev package in [packages/dd-vite](packages/dd-vite) launches
`dd_dev_runtime`, deploys worker source over stdio, and invokes it through
loopback HTTP and WebSocket connections from Vitest helpers or a Vite plugin.

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

## Runtime and storage

Workers have independent schedulers and share CPU, queue and stream-buffer budgets. Deployment validation runs outside request scheduling. Redeployment sends WebSockets close code `1012` and gives the previous generation a bounded period to drain.

KV and memory share 32 fixed state shards. Deployment records and tokens use `control.db`; rebuildable responses use `cache.db`. Existing stores require an offline conversion into a new directory, followed by redeployment of rebuilt bundles.

See [architecture and resource limits](docs/architecture.md) and [storage conversion](docs/storage-conversion.md).

The [consolidation performance report](benchmarks/COHERENCE.md) records measured
throughput, latency and resource costs, with validation limits and reproduction
instructions.

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

Real HTTP/1 server benchmark (uncached and warmed front-cache traffic):

```bash
cargo run -p dd_server --bin bench_http_server --release
```

Keyed memory benchmark:

```bash
cargo run -p runtime --bin bench_memory_storage --release
```

Benchmark configurations and reproducible measurement instructions live in
[benchmarks/README.md](benchmarks/README.md). Contributor/dev notes live in
[docs/development.md](docs/development.md).

## Reproducible reports

Distribution artifacts use the `dist` Cargo profile. Generate full and lean
server reports with:

```bash
just server-full
just server-lean
just size-report-all
```

Reports are dated and stored below
`target/size-report/<git-sha>/dist/<variant>/`, tying every measurement to the
exact source commit. Benchmark commands and the same commit-addressed result
format are documented in [benchmarks/README.md](benchmarks/README.md), while
binary-size methodology lives in
[docs/binary-size-report.md](docs/binary-size-report.md).

## License

Licensed under the [MIT License](LICENSE).
