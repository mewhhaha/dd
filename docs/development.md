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

CLI default server is `http://127.0.0.1:8081`. To override it, pass `--server`,
set `DD_SERVER`, or put `base_url` in the nearest `dd.json`:

```bash
export DD_SERVER=http://127.0.0.1:8081
```

```json
{
  "name": "my-worker",
  "entrypoint": "src/worker.ts",
  "base_url": "https://your-dd-app.fly.dev",
  "config": { "public": true }
}
```

Server precedence is `--server`, `DD_SERVER`, config `base_url`, then the local
default. `base_url` is not secret; deploy tokens should live in env vars or the
OS credential store:

```bash
cargo run -p cli -- auth login
cargo run -p cli -- auth status
cargo run -p cli -- auth logout
```

Normal builds do not scrape old `target/` artifacts for RPC bindings. Checked-in generated bindings live at [crates/runtime/src/generated/memory_rpc_capnp.rs](../crates/runtime/src/generated/memory_rpc_capnp.rs) and are fingerprint-checked against [crates/runtime/schema/memory_rpc.capnp](../crates/runtime/schema/memory_rpc.capnp) during build.

If you change the schema, regenerate the checked-in bindings on a machine with `capnp` installed, then commit both files together.

Optional tracing env:

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318
```

## Distribution Builds

Shipped server artifacts use the `dist` Cargo profile rather than the ordinary
developer release profile. The full server includes HTTP/3, WebSocket, and OTEL
support. The lean server disables those optional server features.

```bash
just server-full
just server-lean
```

Those commands write stable artifacts at `target/dist/dd_server-full` and
`target/dist/dd_server-lean`. To only run Cargo directly:

```bash
cargo build --locked --profile dist -p dd_server --no-default-features --features http3,websocket,otel
cargo build --locked --profile dist -p dd_server --no-default-features
```

Generate reproducible size reports for both server variants with:

```bash
just size-report-all
```

Reports are written to `target/size-report/<git-sha>/<profile>/<variant>/` and
include exact unstripped/stripped bytes, section sizes, dependency trees, and
optional `cargo bloat`/`bloaty` output when those tools are installed.

## Vite and Vitest worker development

The repo includes a dev-only stdio runtime bridge and a source-only Vite package:

- [crates/runtime/src/bin/dd_dev_runtime.rs](../crates/runtime/src/bin/dd_dev_runtime.rs)
- [packages/dd-vite](../packages/dd-vite)
- [packages/dd-runtime](../packages/dd-runtime)

This path does not start `dd_server` or a separate private control-plane server.
The JS helper launches `dd_dev_runtime`, then sends JSON commands over stdio to
deploy and invoke workers through `RuntimeService` directly.

`@mewhhaha/vite-plugin-dd` can use the optional `@mewhhaha/dd` wrapper package. That wrapper has
platform-specific optional dependencies such as `@mewhhaha/dd-linux-x64`,
`@mewhhaha/dd-linux-arm64`, `@mewhhaha/dd-darwin-arm64`, and
`@mewhhaha/dd-win32-x64`, so package managers install only the runtime binary for
the current `os` and `cpu`. In this source checkout, the JS client still falls
back to `cargo run -p runtime --bin dd_dev_runtime` when no packaged binary is
installed.

Vitest example:

```js
import { afterAll, expect, test } from "vitest";
import { createWorkerTestRuntime } from "@mewhhaha/vite-plugin-dd/vitest";

const worker = await createWorkerTestRuntime({
  entry: new URL("./src/worker.js", import.meta.url),
});

afterAll(() => worker.close());

test("worker fetch runs in dd", async () => {
  const response = await worker.fetch("https://worker.test/");
  expect(response.status).toBe(200);
});
```

Vite example:

```js
import { defineConfig } from "vite";
import dd from "@mewhhaha/vite-plugin-dd";

export default defineConfig({
  plugins: [
    dd(),
  ],
});
```

The package default export is the plugin factory, so local configs can name it
however they prefer. The named `ddVitePlugin` export still exists. By default,
the plugin looks for `dd.json` in the nearest package root and uses that file
for the worker name, source `entrypoint`, and deploy `config`. Inline plugin
options override `dd.json`.

By default, app requests to the Vite dev server hit the worker at the root, so
`localhost:5173/anything` behaves like the eventual deployed app. Vite's own
HMR, module, and source requests bypass the worker. The plugin also registers a
Vite Environment API environment named `dd`, backed by Vite's fetchable dev
environment API. Framework code can dispatch a `Request` to that environment
while the worker still runs in the native `dd` runtime.
During Vite hot updates, the plugin leaves Vite's normal browser and framework
HMR path alone, discards the deployed worker, and lazily rebuilds it on the next
worker request.

For React Router framework mode, use the dedicated subpath preset:

```js
import { reactRouter } from "@react-router/dev/vite";
import { defineConfig } from "vite";
import ddReactRouter from "@mewhhaha/vite-plugin-dd/react-router";

export default defineConfig({
  plugins: [
    ddReactRouter(),
    reactRouter(),
  ],
});
```

React Router RSC uses `@mewhhaha/vite-plugin-dd/react-router-rsc`, which sets
up the dd-backed `rsc` environment and runnable `ssr` child environment expected
by `@vitejs/plugin-rsc`.

This follows the same broad direction as Cloudflare's Vite plugin integration:
use Vite's Environment API and let full-stack frameworks merge with their SSR
environment. The current implementation still rebuilds and redeploys worker
source through the native runtime on demand; full React Router RSC-style module
evaluation inside the isolate needs a dedicated Vite ModuleRunner transport.

During `vite build`, the plugin also writes a deployment config into Vite's
output directory:

```text
dist/dd.deploy.json
dist/worker.js
```

By default, the plugin uses root `dd.json` as the source config. That file can
point at `src/worker.ts` and source assets. The generated output config
preserves only the deploy fields the CLI consumes, such as `name`, `config`,
`base_url`, and `temporary`, then replaces `entrypoint` with the bundled worker
path and `assets_dir` with the Vite output directory. It also excludes the
generated worker and config file from static asset packaging. Arbitrary
source-only config keys are not copied into `dist/dd.deploy.json`. The plugin
also writes `dist/_headers` with an immutable cache policy for Vite's
fingerprinted build assets, such as `/assets/*`.

Server-only module assets can be listed in `server_modules`. These files are
uploaded with the worker, are not served as public static assets, and can be
imported from the worker module graph:

```json
{
  "server_modules": [
    { "type": "Json", "path": "./data/config.json", "file": "./data/config.json" },
    { "type": "Text", "path": "./sql/query.sql", "file": "./sql/query.sql" },
    { "type": "Data", "path": "./fixtures/blob.bin", "file": "./fixtures/blob.bin" },
    { "type": "CompiledWasm", "path": "./wasm/filter.wasm", "file": "./wasm/filter.wasm" }
  ]
}
```

Use import attributes for JSON, text, and bytes modules, for example
`import config from "./data/config.json" with { type: "json" }`. `CompiledWasm`
imports default-export a `WebAssembly.Module`.

```js
dd({
  // Optional: override the root dd.json or provide the config inline.
  deploymentConfig: {
    input: { name: "local-dev", entrypoint: "src/worker.ts", config: { public: true } },
  },
});
```

Package or deploy the generated config with:

```bash
cargo run -p cli -- package-deploy-config dist/dd.deploy.json
cargo run -p cli -- deploy-config dist/dd.deploy.json
cargo run -p cli -- deploy-config dist/dd.deploy.json --temporary
just fly-worker-deploy-config dist/dd.deploy.json
```

`--temporary` keeps a worker deployed for one hour. Redeploying the same
temporary worker with `--temporary` refreshes that hour, and a normal redeploy
makes it permanent. A temporary deploy over an existing permanent worker is
rejected.

For CI, mint a scoped token once through the private control plane, then
deploy through the public endpoint:

```bash
cargo run -p cli -- --server http://127.0.0.1:18081 mint-token \
  --name my-worker-ci \
  --worker my-worker \
  --public \
  --memory-binding ROOM \
  --max-source-bytes 1048576 \
  --max-assets 256 \
  --max-asset-bytes 16777216

export DD_TOKEN=dddt_...
cargo run -p cli -- --server https://your-dd-app.fly.dev deploy-config dist/dd.deploy.json
```

The token capability set controls worker names, public/private deploys,
bindings, internal trace configuration, source and asset size limits, expiry,
and max uses. The token `--name` is a unique lowercase, dash-delimited id used
for listing, reading, and deleting the token. Omit expiry for a long-lived
repository token. For local use, store the returned token with:

```bash
cargo run -p cli -- --server https://your-dd-app.fly.dev auth login
cargo run -p cli -- deploy-config dist/dd.deploy.json
```

Revoke with:

```bash
cargo run -p cli -- --server http://127.0.0.1:18081 delete-token my-worker-ci
```

`dd_dev_runtime` is explicitly a debug/dev surface. The JS client starts it with
`--allow-code-generation` by default so worker code using `eval` or
`new Function` can run during local test/dev. Do not expose this binary as a
production control plane.

For faster startup outside this source checkout, build the bridge once and point
the JS client at it:

```bash
cargo build -p runtime --bin dd_dev_runtime
export DD_DEV_RUNTIME_BIN="$PWD/target/debug/dd_dev_runtime"
```

To produce the package runtime binary for the current host, use the size-oriented
runtime profile:

```bash
just build-dd-runtime-package
```

That writes the binary into the matching `packages/dd-runtime-*/bin` directory.
The `dev-runtime` profile favors package size over peak execution performance.

## Patch workflow

Patched crate overrides live under `./patched-crates` and are checked-in build input. A fresh clone should build with normal Cargo commands without a bootstrap step.

Patch files under `./patches` are audit and refresh artifacts. Keep them in sync when the vendored crate changes.

```bash
just patch deno_crypto
just patch-save deno_crypto 0.255.0
just patch-refresh deno_crypto 0.255.0
```

## Library embedding

`dd_server` can run as library through `dd_server::run(ServerConfig { ... })`. Runtime/storage config lives in typed Rust config, not env wiring. See:

- [crates/api/src/lib.rs](../crates/api/src/lib.rs)
- [crates/runtime/src/service.rs](../crates/runtime/src/service.rs)

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
      { "type": "dynamic", "binding": "SANDBOX" },
      { "type": "service", "binding": "AUTH", "service": "auth-worker" }
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

- main repo check: `just check`
- JS package syntax check: `just check-js`
- smoke examples: `bash scripts/smoke_examples.sh`
- runtime benchmark: `cargo run -p runtime --bin bench --release`
- keyed memory benchmark: `cargo run -p runtime --bin bench_memory_storage`
- public naming guard: `bash scripts/check_public_memory_naming.sh`

## Fly helpers

- proxy private port: `just fly-proxy <app>`
- deploy worker through proxy: `just fly-worker-deploy <name> <file> [flags...]`
- direct store write helper exists as internal recovery path: `just fly-worker-store-deploy ...`

Canonical operational guide: [deploy/fly/README.md](../deploy/fly/README.md)
