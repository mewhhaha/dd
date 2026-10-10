# Development Guide

Contributor-focused notes moved here so the root README can stay product- and usage-focused.

## Prerequisites

- Rust 1.99 (selected by `rust-toolchain.toml`)
- Node.js 24 or newer and the pnpm version declared in `package.json`

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
  "$schema": "./schema/dd.schema.json",
  "schema_version": 1,
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

Runtime fields at the top level of `dd.json` replace the corresponding nested
`config` fields in both CLI packaging and Vite. For example, top-level
`bindings: []` clears nested bindings. An explicit Vite plugin `config` replaces
the resolved project runtime configuration.

Runtime response declarations are generated from the serialized Rust structs.
After changing those structs, run `pnpm generate:runtime-types`; `just check-js`
verifies that the checked-in declarations match.

The JavaScript runtime is `crates/dd_v8`, dd's embedding of V8 through
`rusty_v8`: isolates, startup snapshots, ops, ES modules, and the event loop.
The web platform layer (`URL`, `fetch`'s classes, streams, encoding, events,
Web Crypto) runs on it. Its JavaScript lives under `crates/runtime/js/vendor`
(ported from Deno, MIT) and `crates/runtime/js/web`, its ops under
`crates/runtime/src/web`, and `crates/runtime/js/core` holds the `core` object
those scripts are written against. These files are dd's own code; edit them in
place.

Optional tracing env:

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318
export DD_OTEL_COLLECTOR_VERIFIED=true
```

## Distribution Builds

Shipped server artifacts use the `dist` Cargo profile rather than the ordinary
developer release profile. Default and Fly production builds include WebSocket
and OTEL support. The lean server disables optional server features.
TLS terminates at the deployment edge.

```bash
just server-full
just server-lean
```

Those commands write stable artifacts at `target/dist/dd_server-full` and
`target/dist/dd_server-lean`. To only run Cargo directly:

```bash
cargo build --locked --profile dist -p dd_server --no-default-features --features websocket,otel
cargo build --locked --profile dist -p dd_server --no-default-features
```

Generate reproducible size reports for both server variants with:

```bash
just size-report-all
```

Reports are written to `target/size-report/<git-sha>/<profile>/<variant>/` and
include exact unstripped/stripped bytes, section sizes, dependency trees, and
optional `cargo bloat`/`bloaty` output when those tools are installed.

For a tagged release, the tag, Cargo workspace version, and every publishable
npm package must agree. Check before tagging with
`node scripts/check-release-version.mjs v0.1.0` (substitute the release version).
The release workflow creates a missing GitHub Release, restores executable
permissions lost during artifact transfer, and extracts and smoke-tests the
Linux downloads before upload. npm publishing follows successful GitHub upload.

## Vite and Vitest worker development

The repo includes a native dev runtime and a source-only Vite package:

- [crates/api/src/bin/dd_dev_runtime.rs](../crates/api/src/bin/dd_dev_runtime.rs)
- [packages/dd-vite](../packages/dd-vite)
- [packages/dd-runtime](../packages/dd-runtime)

The JS helper launches `dd_dev_runtime` and sends deployment and control commands
over stdio. Each worker receives a loopback HTTP listener sharing the production
streaming and WebSocket transport. Requests preserve their original URLs and stream
bodies in both directions; disconnects cancel the native invocation.

`@mewhhaha/vite-plugin-dd` can use the optional `@mewhhaha/dd` wrapper package. That wrapper has
platform-specific optional dependencies such as `@mewhhaha/dd-linux-x64`,
`@mewhhaha/dd-linux-arm64`, `@mewhhaha/dd-darwin-arm64`, and
`@mewhhaha/dd-win32-x64`, so package managers install only the runtime binary for
the current `os` and `cpu`. In this source checkout, the JS client still falls
back to `cargo run -p dd_server --no-default-features --features websocket --bin dd_dev_runtime` when no packaged binary is
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
Vite Environment API environment for the entry worker, backed by Vite's fetchable dev
environment API. Framework code can dispatch a `Request` to that environment
while the worker still runs in the native `dd` runtime.
Closing a Vite environment or the dev server closes its native runtime. An
unused environment does not start a runtime, and failed initialization releases
the runtime it created.
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

The plugin uses Vite's Environment API and lets frameworks merge their SSR
environments. A development module runner loads Vite-transformed modules inside
the native isolate and invalidates them on hot updates.

During `vite build`, the plugin also writes a deployment config into Vite's
output directory:

```text
dist/client/
dist/<entry-worker>/dd.deploy.json
dist/<entry-worker>/worker.js
dist/dd.workers.json
```

Set `deploymentConfig: false` or `deploymentConfig: { enabled: false }` to
build worker bundles without deployment configs, private-module copies, or
generated asset policy files. The worker manifest then omits `deployConfig`.
Use `deploymentConfig.entrypoint` and `deploymentConfig.output` to change the
bundle and config filenames inside the entry worker's output directory, for
example `bundle/main.js` and `metadata/deploy.json`. Auxiliary workers accept
the same options under `deployment`. Generated config paths are relative to
the config's directory, including private modules staged beside that config.

By default, the plugin uses root `dd.json` as the source config. That file can
point at `src/worker.ts` and source assets. The generated output config
preserves only the deploy fields the CLI consumes, such as `name`, `config`,
`base_url`, and `temporary`, then replaces `entrypoint` with the bundled worker
path and `assets_dir` with the sibling client output directory. It also excludes the
generated worker and config file from static asset packaging. Unknown source
config keys are rejected against `schema/dd.schema.json`. The plugin
also writes `_headers` in the client output directory with an immutable cache policy for Vite's
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

File paths are relative to the source config directory. Vite stages the private
files into `dist/<entry-worker>/server-modules/` and rewrites the generated
config, so the worker output can be moved without retaining the source tree.

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
cargo run -p cli -- package-deploy-config dist/<entry-worker>/dd.deploy.json --allow-outside-config-root
cargo run -p cli -- deploy-config dist/<entry-worker>/dd.deploy.json --allow-outside-config-root
cargo run -p cli -- deploy-config dist/<entry-worker>/dd.deploy.json --allow-outside-config-root --temporary
just fly-worker-deploy-config dist/<entry-worker>/dd.deploy.json --allow-outside-config-root
```

The generated config points at sibling client assets, so the CLI requires the
explicit `--allow-outside-config-root` flag. Check that path before deploying.

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
cargo run -p cli -- --server https://your-dd-app.fly.dev deploy-config dist/<entry-worker>/dd.deploy.json --allow-outside-config-root
```

The token capability set controls worker names, public/private deploys,
bindings, internal trace configuration, source and asset size limits, expiry,
and max uses. The token `--name` is a unique lowercase, dash-delimited id used
for listing, reading, and deleting the token. Omit expiry for a long-lived
repository token. For local use, store the returned token with:

```bash
cargo run -p cli -- --server https://your-dd-app.fly.dev auth login
cargo run -p cli -- deploy-config dist/<entry-worker>/dd.deploy.json --allow-outside-config-root
```

Revoke with:

```bash
cargo run -p cli -- --server http://127.0.0.1:18081 delete-token my-worker-ci
```

Deployment history and deploy-token metadata live in the durable
`store/control.db`. Each worker retains its five newest successful deployments.
The private control plane exposes the matching lifecycle commands:

```bash
cargo run -p cli -- list-deployments --worker my-worker
cargo run -p cli -- inspect-deployment DEPLOYMENT_ID
cargo run -p cli -- rollback my-worker DEPLOYMENT_ID
cargo run -p cli -- undeploy my-worker
```

On the first startup after upgrading, recognized `workers/*.json` and
`tokens.json` stores are imported transactionally. Unknown formats stop startup
instead of being skipped.

`dd_dev_runtime` is explicitly a debug/dev surface. The JS client starts it with
`--allow-code-generation` by default so worker code using `eval` or
`new Function` can run during local test/dev. Do not expose this binary as a
production control plane.

For faster startup outside this source checkout, build the bridge once and point
the JS client at it:

```bash
cargo build -p dd_server --no-default-features --features websocket --bin dd_dev_runtime
export DD_DEV_RUNTIME_BIN="$PWD/target/debug/dd_dev_runtime"
```

To produce the package runtime binary for the current host, use the size-oriented
runtime profile:

```bash
just build-dd-runtime-package
```

That writes the binary into the matching `packages/dd-runtime-*/bin` directory.
The `dev-runtime` profile favors package size over peak execution performance.

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
- real HTTP/1 server benchmark: `cargo run -p dd_server --bin bench_http_server --release`
- public naming guard: `bash scripts/check_public_memory_naming.sh`

### Fuzzing dd_v8

`crates/dd_v8` is the only path from worker JavaScript into Rust, so it has
cargo-fuzz targets in `crates/dd_v8/fuzz` (a separate crate, outside the
workspace). Install `cargo-fuzz` and a nightly toolchain, then run one:

```bash
just fuzz-dd-v8 deserialize                       # bytes into op_deserialize, storage and message mode
just fuzz-dd-v8 serde_roundtrip                   # arbitrary Rust values through serde_v8 and V8's serializer
just fuzz-dd-v8 js_values -- -max_total_time=300  # a byte grammar of JS values and buffers fed to every op
```

The corpus lands in `crates/dd_v8/fuzz/corpus/<target>` and crashing inputs in
`crates/dd_v8/fuzz/artifacts/<target>`; both are ignored. Pass
`-detect_leaks=0` if LeakSanitizer reports V8's process-lifetime allocations.
The first build downloads rusty_v8's static library again; set
`RUSTY_V8_ARCHIVE` to an existing `librusty_v8.a` of the same version to skip
that. The deterministic edge cases the fuzzers grew out of run in normal CI:
`cargo test -p dd_v8`.

## Fly helpers

- proxy private port: `just fly-proxy <app>`
- deploy worker through proxy: `just fly-worker-deploy <name> <file> [flags...]`
- direct store write helper exists as internal recovery path: `just fly-worker-store-deploy ...`

Canonical operational guide: [deploy/fly/README.md](../deploy/fly/README.md)
