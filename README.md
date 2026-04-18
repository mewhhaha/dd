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

Default ports for `cargo run -p dd_server` are `8080` for public traffic and `8081` for private deploy/invoke traffic.

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

Workers can create other workers with `env.SANDBOX.get/list/delete`. Dynamic workers run in separate Deno isolates, so they fit tenant-specific or agent-generated code that should stay runtime-isolated from parent worker state. That is isolate-level separation, not OS process, VM, or container sandboxing. See [examples/dynamic-namespace.js](/home/mewhhaha/src/grugd/examples/dynamic-namespace.js).

Static assets can be bundled at deploy time with `--assets-dir`. Files are served before worker code runs, with root `_headers` support similar to Cloudflare static assets. See [examples/static-assets-site](/home/mewhhaha/src/grugd/examples/static-assets-site).

Chat app example combines memory namespace, websockets, and deploy-time assets in [examples/chat-worker](/home/mewhhaha/src/grugd/examples/chat-worker).

## How to think about it

If you want "Cloudflare-style worker runtime on one box," `dd` is that shape.

If you want "Durable Objects, but expressed as disk-backed STM-like shards instead of object instances," memory namespaces are that shape.

If you want one app process you can deploy to Fly or another VM and then load with named workers, `dd_server` is that shape.

## Fly

Fly runs one `dd_server` app process. Workers are deployed into that app; they are not separate Fly apps.

Canonical flow:

1. deploy app/container with `flyctl deploy`
2. open private tunnel with `just fly-proxy <app>`
3. deploy worker through tunnel with `just fly-worker-deploy ...`

Full guide: [deploy/fly/README.md](/home/mewhhaha/src/grugd/deploy/fly/README.md)

## Benchmarks and docs

Runtime benchmark:

```bash
cargo run -p runtime --bin bench --release
```

Keyed memory benchmark:

```bash
cargo run -p runtime --bin bench_memory_storage
```

Current numbers live in [BENCHMARKS.md](/home/mewhhaha/src/grugd/BENCHMARKS.md). Contributor/dev notes live in [docs/development.md](/home/mewhhaha/src/grugd/docs/development.md).
