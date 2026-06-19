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
export OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4317
```

## Vite and Vitest worker development

The repo includes a dev-only stdio runtime bridge and a source-only Vite package:

- [crates/runtime/src/bin/dd_dev_runtime.rs](../crates/runtime/src/bin/dd_dev_runtime.rs)
- [packages/dd-vite](../packages/dd-vite)
- [packages/dd-runtime](../packages/dd-runtime)

This path does not start `dd_server` or a separate private control-plane server.
The JS helper launches `dd_dev_runtime`, then sends JSON commands over stdio to
deploy and invoke workers through `RuntimeService` directly.

`@dd/vite` can use the optional `@dd/runtime` wrapper package. That wrapper has
platform-specific optional dependencies such as `@dd/runtime-linux-x64`,
`@dd/runtime-linux-arm64`, `@dd/runtime-darwin-arm64`, and
`@dd/runtime-win32-x64`, so package managers install only the runtime binary for
the current `os` and `cpu`. In this source checkout, the JS client still falls
back to `cargo run -p runtime --bin dd_dev_runtime` when no packaged binary is
installed.

Vitest example:

```js
import { afterAll, expect, test } from "vitest";
import { createWorkerTestRuntime } from "@dd/vite/vitest";

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
import dd from "@dd/vite";

export default defineConfig({
  plugins: [
    dd({
      mount: "/__dd",
    }),
  ],
});
```

The package default export is the plugin factory, so local configs can name it
however they prefer. The named `ddVitePlugin` export still exists. By default,
the plugin looks for `dd.json` in the nearest package root and uses that file
for the worker name, source `entrypoint`, and deploy `config`. Inline plugin
options override `dd.json`.

The plugin also registers a Vite Environment API environment named `dd`, backed
by Vite's fetchable dev environment API. Framework code can dispatch a `Request`
to that environment while the worker still runs in the native `dd` runtime.
During Vite hot updates, the plugin leaves Vite's normal browser and framework
HMR path alone, discards the deployed worker, and lazily rebuilds it on the next
worker request.

For frameworks that own the SSR environment, bind `dd` to the same environment
name:

```js
import { reactRouter } from "@react-router/dev/vite";
import { defineConfig } from "vite";
import ddWorker from "@dd/vite";

export default defineConfig({
  plugins: [
    ddWorker({
      viteEnvironment: { name: "ssr" },
      mount: "/__dd",
    }),
    reactRouter(),
  ],
});
```

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
preserves `name`, `config`, and custom metadata, then replaces `entrypoint` with
the bundled worker path and `assets_dir` with the Vite output directory. It also
excludes the generated worker and config file from static asset packaging.
Fields such as `base_url` are carried into `dist/dd.deploy.json`, so the CLI can
deploy the generated config without a separate `--server`.

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
just fly-worker-deploy-config dist/dd.deploy.json
```

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
