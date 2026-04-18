# Development Guide

Contributor-focused notes moved here so the root README can stay product- and usage-focused.

## Prerequisites

- Rust toolchain
- `capnp` only when regenerating checked-in memory RPC bindings after schema changes

## Local run

`cargo run -p dd_server` defaults to:

- public listener: `http://127.0.0.1:8080`
- private listener: `http://127.0.0.1:8081`
- public base domain: `example.com`

Private control plane bearer auth is required by default:

```bash
export DD_PRIVATE_TOKEN=dev-token
cargo run -p dd_server
```

CLI default server is `http://127.0.0.1:3001`, so either pass `--server http://127.0.0.1:8081` explicitly or set:

```bash
export DD_SERVER=http://127.0.0.1:8081
```

Normal builds do not scrape old `target/` artifacts for RPC bindings. Checked-in generated bindings live at [crates/runtime/src/generated/memory_rpc_capnp.rs](/home/mewhhaha/src/grugd/crates/runtime/src/generated/memory_rpc_capnp.rs) and are fingerprint-checked against [crates/runtime/schema/memory_rpc.capnp](/home/mewhhaha/src/grugd/crates/runtime/schema/memory_rpc.capnp) during build.

If you change the schema, regenerate the checked-in bindings on a machine with `capnp` installed, then commit both files together.

Optional tracing env:

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4317
```

## Patch workflow

Patched crate overrides live under `./patched-crates`. Checked-in source of truth stays under `./patches`.

```bash
just patch deno_crypto
just patch-save deno_crypto 0.255.0
just patch-refresh deno_crypto 0.255.0
```

## Library embedding

`dd_server` can run as library through `dd_server::run(ServerConfig { ... })`. Runtime/storage config lives in typed Rust config, not env wiring. See:

- [crates/api/src/lib.rs](/home/mewhhaha/src/grugd/crates/api/src/lib.rs)
- [crates/runtime/src/service.rs](/home/mewhhaha/src/grugd/crates/runtime/src/service.rs)

## Raw deploy/invoke API

Deploy:

```bash
curl -X POST http://127.0.0.1:8081/v1/deploy \
  -H "authorization: Bearer dev-token" \
  -H "content-type: application/json" \
  -d @- <<'JSON'
{
  "name": "hello",
  "source": "export default { async fetch() { return new Response('hello from worker'); } }",
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

Invoke:

```bash
curl -H "authorization: Bearer dev-token" http://127.0.0.1:8081/v1/invoke/hello/
```

Public invoke shape uses host routing:

```bash
curl -H "host: hello.example.com" http://127.0.0.1:8080/
```

## Contributor checks

- smoke examples: `bash scripts/smoke_examples.sh`
- runtime benchmark: `cargo run -p runtime --bin bench --release`
- keyed memory benchmark: `cargo run -p runtime --bin bench_memory_storage`
- public naming guard: `bash scripts/check_public_memory_naming.sh`

## Fly helpers

- proxy private port: `just fly-proxy <app>`
- deploy worker through proxy: `just fly-worker-deploy <name> <file> [flags...]`
- direct store write helper exists as internal recovery path: `just fly-worker-store-deploy ...`

Canonical operational guide: [deploy/fly/README.md](/home/mewhhaha/src/grugd/deploy/fly/README.md)
