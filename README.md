# dd

`dd` is a single-node worker platform in Rust. Workers are written in
TypeScript, compiled ahead of time to WebAssembly by
[Perry](https://github.com/PerryTS/perry), and executed in wasmtime against a
native host runtime — there is no JavaScript engine in the loop. It is
inspired by Cloudflare Workers, aimed at "run Cloudflare-like workers on one
machine with disk-backed storage" rather than "managed global edge platform."

Public traffic is routed by host name, so `hello.example.com` maps to worker
`hello`. State lives on disk (turso). For coordination, `dd` uses keyed
memory namespaces as durable single-writer actors: shard state by key, run
atomic commands once for that key, and commit state on completion. KV covers
simple persistence, a worker-scoped cache covers response reuse, websockets
cover live connections, and service bindings let workers call each other.

## Quickstart

```bash
npm install -g @perryts/perry     # the TypeScript -> wasm compiler
cargo run -p runtime --bin dd_server
```

In another shell:

```bash
cargo run -p cli --bin dd -- deploy hello examples/hello.ts --public
cargo run -p cli --bin dd -- invoke hello --path /
curl -H 'host: hello.example.com' http://127.0.0.1:8080/
```

`dd deploy` compiles TypeScript through Perry and uploads the wasm; a
prebuilt `.wasm` module is accepted directly. Default ports are `8080` for
public traffic and `8081` for the private control plane. Set
`DD_PRIVATE_TOKEN` to require bearer auth on the private API. Deployed
workers persist under the store directory and reload on restart.

## Worker shape

Workers reach the platform through plain `declare function` statements —
Perry compiles each into a host import:

```ts
declare function dd_register(
  fetchHandler: (method: string, url: string, body: string) => unknown,
): void;
declare function dd_json(value: unknown): string;

dd_register((method, url, body) => ({
  status: 200,
  headers: { "content-type": "application/json" },
  body: dd_json({ hello: new URL(url).pathname }),
}));
```

The full host surface — KV, memory namespaces, cache, outbound fetch,
service bindings, websockets, request headers — is documented in
[docs/wasm-runtime.md](docs/wasm-runtime.md), together with the execution
model and current limitations. Working examples for every feature live in
[examples/](examples), exercised end to end by `just smoke-examples`.

## Memory namespaces

The main coordination primitive. Pick a key, run an atomic command against
it: commands for one key are serialized, and tvar writes commit together
with the command's completion.

```ts
declare function dd_memory_atomic(
  binding: string, key: string, command: () => unknown,
): any;
declare function dd_tvar_read(name: string): any;
declare function dd_tvar_write(name: string, value: unknown): void;

const count = dd_memory_atomic("COUNTERS", user, () => {
  const current = dd_tvar_read("count");
  const next = (current === undefined || current === null ? 0 : current) + 1;
  dd_tvar_write("count", next);
  return next;
});
```

See [examples/memory-counter.ts](examples/memory-counter.ts).

## KV, cache, websockets, services

- **KV** (`dd_kv_get/set/delete/list`): write-last key/value persistence,
  scoped per worker and binding — [examples/kv-counter.ts](examples/kv-counter.ts)
- **Cache** (`dd_cache_match/put/delete`): worker-scoped response reuse
  honoring `cache-control` — [examples/cache.ts](examples/cache.ts)
- **Websockets** (`dd_ws_register/send/close`): callback handlers on one
  dedicated instance per worker, sends from any handler —
  [examples/chat.ts](examples/chat.ts)
- **Service bindings** (`dd_service_fetch`): call co-deployed workers by
  binding — [examples/router.ts](examples/router.ts) +
  [examples/auth.ts](examples/auth.ts)
- **Outbound fetch**: native `fetch(url).then(...)` promise chains plus a
  synchronous `dd_fetch` — [examples/proxy.ts](examples/proxy.ts)

Static assets are served before worker code runs via `--assets-dir`.

## How to think about it

If you want "Cloudflare-style worker runtime on one box," `dd` is that
shape — with ahead-of-time compiled workers, microsecond-scale per-request
dispatch, and a ~31 MB server binary instead of an embedded JS engine.

If you want "Durable Objects, but expressed as disk-backed keyed actors
instead of object instances," memory namespaces are that shape.

## Fly

Fly runs one `dd_server` app process; workers are deployed into it.

1. deploy the container with `just fly-deploy <app>`
2. open the private tunnel with `just fly-proxy <app>`
3. deploy workers with `just fly-worker-deploy <name> <file.ts> --public`

Full guide: [deploy/fly/README.md](deploy/fly/README.md)

## Benchmarks and contributing

```bash
just bench            # worker invoke throughput/latency
just check            # fmt, clippy, tests — the CI path
just smoke-examples   # deploy + exercise every example locally
```

Contributor notes live in [docs/development.md](docs/development.md).

## License

Licensed under the [MIT License](LICENSE).
