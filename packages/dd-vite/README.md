# @mewhhaha/vite-plugin-dd

Vite and Vitest helpers for running `dd` workers against the native runtime in
debug/dev mode. The helpers do not start a private `dd_server`; they launch
`dd_dev_runtime` as a stdio child process and send deploy/invoke commands
directly to `RuntimeService`.

`@mewhhaha/vite-plugin-dd` optionally installs `@mewhhaha/dd`, which selects a platform-specific
binary package such as `@mewhhaha/dd-linux-x64` or `@mewhhaha/dd-darwin-arm64`.
Package managers install only the runtime package matching the current platform.

## Vitest

```js
import { afterAll, expect, test } from "vitest";
import { createWorkerTestRuntime } from "@mewhhaha/vite-plugin-dd/vitest";

const worker = await createWorkerTestRuntime({
  entry: new URL("./src/worker.js", import.meta.url),
});

afterAll(() => worker.close());

test("responds through the real dd runtime", async () => {
  const response = await worker.fetch("https://worker.test/");
  expect(await response.text()).toBe("ok");
});
```

## Vite

```js
import { defineConfig } from "vite";
import dd from "@mewhhaha/vite-plugin-dd";

export default defineConfig({
  plugins: [
    dd(),
  ],
});
```

App requests to the Vite dev server are invoked through the native runtime by
default, so `localhost:5173/anything` behaves like the eventual deployed worker.
Vite's own HMR, module, and source requests bypass the worker so the dev client
keeps working. In dev, each dd worker is a first-class Vite environment. The
runtime deploys a small dev runner that fetches Vite-transformed modules on
demand, so no Vite module graph is embedded into the worker source. On hot
updates the plugin clears the runner cache and reloads the worker entry.

The default export is the Vite plugin factory, so you can name it whatever fits
your config. The named `ddVitePlugin` export remains available. If the package
root contains `dd.json`, the plugin reads it by default for the worker name,
entrypoint, and deploy config. Inline plugin options override values from
`dd.json`.

The plugin also registers a Vite Environment API environment for the entry
worker, backed by `createFetchableDevEnvironment`, for framework code that wants
to dispatch `Request` objects directly. The environment name defaults to the
worker name from `dd.json`, normalized for Vite, and can be overridden with the
top-level `viteEnvironment.name`.

```js
import { defineConfig } from "vite";
import dd from "@mewhhaha/vite-plugin-dd";

export default defineConfig({
  plugins: [
    dd(),
  ],
});
```

Advanced integrations can set `mount` to place the worker behind a subpath.
Options such as `name`, `entry`, `config`, `viteEnvironment`, and
`deploymentConfig` apply to the entry worker directly at the top level of
`dd({ ... })`. If omitted, `dd.json` remains the source of truth.

Framework examples can use subpath presets instead of wiring the framework
server build by hand:

```js
import ddReactRouter from "@mewhhaha/vite-plugin-dd/react-router";

export default defineConfig({
  plugins: [
    ddReactRouter(),
  ],
});
```

For React Router RSC, import
`@mewhhaha/vite-plugin-dd/react-router-rsc`. The presets set the Vite
environment names, configure React Router's server-build virtual module, keep
Vite's internal RSC dev proxy endpoints out of the worker middleware, and point
the generated deployment config at the framework client assets. The RSC preset
uses a dd-backed `rsc` environment with a runnable `ssr` child environment,
matching the shape expected by `@vitejs/plugin-rsc`.

If a project uses non-standard React Router paths, pass `buildDirectory`,
`workerEntry`, `serverEntry`, or, for RSC, `rscEntry` directly to the subpath
plugin.

## Build output

During `vite build`, the plugin builds the client first, then builds each dd
worker as its own Vite build environment:

```text
dist/
  client/
  <entry-worker>/
    worker.js
    dd.deploy.json
  dd.workers.json
```

By default, each worker deploy config points `entrypoint` at its local
`worker.js`. The entry worker points `assets_dir` at `../client` when client
assets were built. The root `dd.workers.json` manifest lists every worker,
environment name, output directory, and deploy config path. The plugin also
writes `_headers` with an immutable cache policy for Vite's fingerprinted build
assets, such as `/assets/*`.

```js
export default defineConfig({
  plugins: [
    dd(),
  ],
});
```

If `dd.json` points at a TypeScript entrypoint or source asset directory, the
generated output config replaces those with the environment-built worker path
and Vite output asset path while preserving the deploy fields the CLI consumes: `name`,
`config`, `base_url`, and `temporary`. Arbitrary source config keys are not
copied into generated deploy configs, so local-only settings do not leak into build
artifacts.

Pass options inline when you want to override the file:

```js
dd({
  entry: new URL("./src/dev-worker.ts", import.meta.url),
  config: { public: true },
  deploymentConfig: {
    input: {
      name: "local-dev",
      entrypoint: "src/worker.ts",
      config: {
        public: false,
        bindings: [
          { type: "service", binding: "AUTH", service: "auth-worker" },
        ],
      },
    },
  },
});
```

With the default layout, deploy the entry worker from its output directory:

```bash
cargo run -p cli -- package-deploy-config dist/<entry-worker>/dd.deploy.json
cargo run -p cli -- deploy-config dist/<entry-worker>/dd.deploy.json
```

Auxiliary workers can also be built as private service-bound workers:

```js
dd({
  auxiliaryWorkers: [
    {
      name: "auth",
      kind: "service",
      binding: "AUTH",
      service: "my-app-auth",
      entry: "src/auth-worker.ts",
      config: {
        public: false,
        bindings: [{ type: "kv", binding: "AUTH_DB" }],
      },
    },
  ],
});
```

The frontend deploy config gets `{ type: "service", binding: "AUTH", service:
"my-app-auth" }`. Production builds also write
`dist/auth/dd.deploy.json` and `dist/auth/worker.js` for the private auth
worker.

Store the deploy token once with the OS credential store instead of putting it
in `dd.json`:

```bash
cargo run -p cli -- auth login
```

Runtime binary resolution order:

1. `runtimeOptions.binary`
2. `DD_DEV_RUNTIME_BIN`
3. the optional `@mewhhaha/dd` platform binary
4. `cargo run -p runtime --bin dd_dev_runtime` when running inside this source checkout

Packaged runtime binaries are built for small install size, not maximum runtime
throughput. Set `DD_DEV_RUNTIME_BIN=/path/to/dd_dev_runtime` to test a custom
binary.
