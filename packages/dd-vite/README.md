# @dd/vite

Vite and Vitest helpers for running `dd` workers against the native runtime in
debug/dev mode. The helpers do not start a private `dd_server`; they launch
`dd_dev_runtime` as a stdio child process and send deploy/invoke commands
directly to `RuntimeService`.

`@dd/vite` optionally installs `@dd/runtime`, which selects a platform-specific
binary package such as `@dd/runtime-linux-x64` or `@dd/runtime-darwin-arm64`.
Package managers install only the runtime package matching the current platform.

## Vitest

```js
import { afterAll, expect, test } from "vitest";
import { createWorkerTestRuntime } from "@dd/vite/vitest";

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
import dd from "@dd/vite";

export default defineConfig({
  plugins: [
    dd(),
  ],
});
```

App requests to the Vite dev server are invoked through the native runtime by
default, so `localhost:5173/anything` behaves like the eventual deployed worker.
Vite's own HMR, module, and source requests bypass the worker so the dev client
keeps working. On hot updates the plugin discards the deployed worker and lazily
rebuilds it on the next worker request.

The default export is the Vite plugin factory, so you can name it whatever fits
your config. The named `ddVitePlugin` export remains available. If the package
root contains `dd.json`, the plugin reads it by default for the worker name,
entrypoint, and deploy config. Inline plugin options override values from
`dd.json`.

The plugin also registers a Vite Environment API environment named `dd`, backed
by `createFetchableDevEnvironment`, for framework code that wants to dispatch
`Request` objects directly. Normal applications should not need to configure
this environment; `dd()` at the root-mounted default is the path that keeps
development requests shaped like deployed worker requests.

```js
import { defineConfig } from "vite";
import dd from "@dd/vite";

export default defineConfig({
  plugins: [
    dd(),
  ],
});
```

Advanced integrations can rename the registered Vite environment with
`environmentName` or `viteEnvironment.name`, and can set `mount` to place the
worker behind a subpath. Those options are intentionally unnecessary for the
workspace examples: app traffic goes through dd, while Vite-owned module, HMR,
and source requests bypass the worker.

## Build output

During `vite build`, the plugin writes a bundled worker and deployment config
into Vite's output directory:

```text
dist/
  dd.deploy.json
  worker.js
  ...
```

By default, `dd.deploy.json` points `entrypoint` at `worker.js`, sets
`assets_dir` to `.`, excludes `worker.js` and `dd.deploy.json` from static
asset packaging, and carries over the runtime deploy config. The plugin also
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
generated output config replaces those with the bundled worker path and Vite
output asset path while preserving fields such as `name`, `config`, and custom
metadata. Non-secret deploy settings such as `base_url` are preserved too, so
`dd deploy-config dist/dd.deploy.json` can pick the target server from the
generated config.

Pass options inline when you want to override the file:

```js
dd({
  entry: new URL("./src/dev-worker.ts", import.meta.url),
  config: { public: true },
  deploymentConfig: {
    input: { name: "local-dev", entrypoint: "src/worker.ts", config: { public: true } },
  },
});
```

The generated config can be packaged or deployed by the CLI:

```bash
cargo run -p cli -- package-deploy-config dist/dd.deploy.json
cargo run -p cli -- deploy-config dist/dd.deploy.json
```

Store the deploy token once with the OS credential store instead of putting it
in `dd.json`:

```bash
cargo run -p cli -- auth login
```

Runtime binary resolution order:

1. `runtimeOptions.binary`
2. `DD_DEV_RUNTIME_BIN`
3. the optional `@dd/runtime` platform binary
4. `cargo run -p runtime --bin dd_dev_runtime` when running inside this source checkout

Packaged runtime binaries are built for small install size, not maximum runtime
throughput. Set `DD_DEV_RUNTIME_BIN=/path/to/dd_dev_runtime` to test a custom
binary.
