# dd

`dd` is single-node workers platform in Rust with Deno-backed isolates, host-routed public traffic, keyed memory namespaces, dynamic workers, deploy-time static assets, KV, cache, websockets, and transport.

## Features

- deploy named workers over private control plane
- serve public traffic by host: `worker.example.com -> worker`
- keyed memory namespaces with STM-style `atomic(...)`
- dynamic workers from normal workers via `env.SANDBOX.get/list/delete`
- deploy-time static assets with root `_headers`
- shared Cache API plus Turso-backed KV
- websocket and transport session support
- honest public `request.url` inside workers

## Quickstart

Private control plane uses bearer auth by default. CLI reads `DD_PRIVATE_TOKEN` automatically.

Start server:

```bash
export DD_PRIVATE_TOKEN=dev-token
cargo run -p dd_server
```

Deploy and invoke from another shell:

```bash
export DD_PRIVATE_TOKEN=dev-token
cargo run -p cli -- --server http://127.0.0.1:8081 deploy hello examples/hello.js --public
cargo run -p cli -- --server http://127.0.0.1:8081 invoke hello --method GET --path /
curl -H 'host: hello.example.com' http://127.0.0.1:8080/
```

Default public/private listener ports for `cargo run -p dd_server` are `8080` and `8081`.

## Main model

- worker code handles HTTP-style `fetch(request, env, ctx)`
- public requests go through host routing
- private deploy/invoke goes through `/v1/deploy`, `/v1/dynamic/deploy`, and `/v1/invoke/...`
- memory namespace is public coordination primitive
- dynamic workers are separate workers created from running workers
- assets are exact file-path matches served before worker code runs

## Core APIs

Memory namespaces:

- `env.USER_MEMORY.get(id)`
- `memory.atomic(async () => { ... })`
- `memory.read(key)`, `memory.list(...)`, `memory.write(...)`, `memory.delete(...)`
- `memory.tvar(name, defaultValue)`

Dynamic workers:

- `env.SANDBOX.get(id, factory)`
- `env.SANDBOX.list()`
- `env.SANDBOX.delete(id)`
- child `fetch(...)`
- `RpcTarget` for parent-provided host RPC bindings

Static assets:

- `dd deploy ... --assets-dir path/to/assets`
- `_headers` in asset root applies asset-only headers
- exact file serving only; worker handles non-asset routes

## CLI examples

```bash
export DD_PRIVATE_TOKEN=dev-token
cargo run -p cli -- --server http://127.0.0.1:8081 deploy memory examples/memory.js --memory-binding USER_MEMORY
cargo run -p cli -- --server http://127.0.0.1:8081 deploy dynamic examples/dynamic-namespace.js --dynamic-binding SANDBOX
cargo run -p cli -- --server http://127.0.0.1:8081 dynamic-deploy examples/hello.js --env OPENAI_API_KEY='<set-via---env>'
cargo run -p cli -- --server http://127.0.0.1:8081 deploy chat examples/chat-worker/src/worker.js --memory-binding CHAT_ROOM --public --assets-dir examples/chat-worker/assets
```

## Examples

- [examples/hello.js](/home/mewhhaha/src/grugd/examples/hello.js): smallest worker
- [examples/memory.js](/home/mewhhaha/src/grugd/examples/memory.js): keyed memory namespace
- [examples/dynamic-namespace.js](/home/mewhhaha/src/grugd/examples/dynamic-namespace.js): dynamic workers plus host RPC
- [examples/chat-worker/](/home/mewhhaha/src/grugd/examples/chat-worker): chat app with memory namespace plus deploy-time assets
- [examples/static-assets-site/](/home/mewhhaha/src/grugd/examples/static-assets-site): asset-first website
- [examples/bundled-router/](/home/mewhhaha/src/grugd/examples/bundled-router): bundled TypeScript worker

## Fly deploys

Fly runs one `dd_server` app process. Workers are deployed into that app; they are not separate Fly apps.

Canonical Fly flow:

1. deploy app/container with `flyctl deploy`
2. open private tunnel with `just fly-proxy <app>`
3. deploy worker through tunnel with `just fly-worker-deploy ...`

Full Fly guide: [deploy/fly/README.md](/home/mewhhaha/src/grugd/deploy/fly/README.md)

## Benchmarks

- runtime benchmark: `cargo run -p runtime --bin bench --release`
- keyed memory benchmark: `cargo run -p runtime --bin bench_memory_storage`
- current numbers: [BENCHMARKS.md](/home/mewhhaha/src/grugd/BENCHMARKS.md)

## More docs

- contributor/dev guide: [docs/development.md](/home/mewhhaha/src/grugd/docs/development.md)
- Fly deployment guide: [deploy/fly/README.md](/home/mewhhaha/src/grugd/deploy/fly/README.md)
- secret audit for public-readiness: [docs/security-audit-2026-04-11.md](/home/mewhhaha/src/grugd/docs/security-audit-2026-04-11.md)
- broader security review: [docs/security-review-2026-04-11.md](/home/mewhhaha/src/grugd/docs/security-review-2026-04-11.md)
