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
import { ddVitePlugin } from "@dd/vite";

export default defineConfig({
  plugins: [
    ddVitePlugin({
      entry: new URL("./src/worker.js", import.meta.url),
      mount: "/__dd",
    }),
  ],
});
```

Requests to `/__dd/*` on the Vite dev server are invoked through the native
runtime. Vite and framework HMR continue to flow normally; on hot updates the
plugin discards the deployed worker and lazily rebuilds it on the next worker
request.

The plugin also registers a Vite Environment API environment named `dd`, backed
by `createFetchableDevEnvironment`, for framework code that wants to dispatch
`Request` objects directly.

Frameworks that own an SSR environment can bind the worker to that environment
name, similar to Cloudflare's Vite plugin shape:

```js
import { reactRouter } from "@react-router/dev/vite";
import { defineConfig } from "vite";
import { ddVitePlugin } from "@dd/vite";

export default defineConfig({
  plugins: [
    ddVitePlugin({
      entry: new URL("./src/worker.js", import.meta.url),
      viteEnvironment: { name: "ssr" },
      mount: "/__dd",
    }),
    reactRouter(),
  ],
});
```

This is still a native-runtime deploy/invoke loop, not a full Vite ModuleRunner
inside the isolate. React Router and RSC integrations that require in-runtime
module evaluation may need a dedicated runner transport in a follow-up.

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
asset packaging, and carries over the runtime deploy config.

```js
export default defineConfig({
  plugins: [
    ddVitePlugin({
      entry: new URL("./src/worker.ts", import.meta.url),
      deploymentConfig: {
        input: new URL("./dd.json", import.meta.url),
      },
    }),
  ],
});
```

If `dd.json` points at a TypeScript entrypoint or source asset directory, the
generated output config replaces those with the bundled worker path and Vite
output asset path while preserving fields such as `name`, `config`, and custom
metadata.

The generated config can be packaged or deployed by the CLI:

```bash
cargo run -p cli -- package-deploy-config dist/dd.deploy.json
cargo run -p cli -- deploy-config dist/dd.deploy.json
```

Runtime binary resolution order:

1. `runtimeOptions.binary`
2. `DD_DEV_RUNTIME_BIN`
3. the optional `@dd/runtime` platform binary
4. `cargo run -p runtime --bin dd_dev_runtime` when running inside this source checkout

Packaged runtime binaries are built for small install size, not maximum runtime
throughput. Set `DD_DEV_RUNTIME_BIN=/path/to/dd_dev_runtime` to test a custom
binary.
