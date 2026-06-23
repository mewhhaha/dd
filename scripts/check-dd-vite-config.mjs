import assert from "node:assert/strict";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { ddVitePlugin } from "../packages/dd-vite/src/vite.js";

const require = createRequire(new URL("../packages/dd-vite/package.json", import.meta.url));
const { createBuilder, resolveConfig } = await import(require.resolve("vite"));
const root = await mkdtemp(join(tmpdir(), "dd-vite-config-"));
const workerBundleEnv = "DD_VITE_WORKER_BUNDLE";
const previousWorkerBundleEnv = process.env[workerBundleEnv];
delete process.env[workerBundleEnv];

try {
  await mkdir(join(root, "src"), { recursive: true });
  await writeFile(join(root, "package.json"), JSON.stringify({ type: "module" }));
  await writeFile(
    join(root, "dd.json"),
    JSON.stringify({
      name: "config-check-worker",
      entrypoint: "src/worker.ts",
      baseUrl: "https://dd.example.test",
      temporary: true,
      deploy_token: "should-not-leak",
      local_only: { also: "should-not-leak" },
      asset_excludes: ["secret-local.txt"],
      public: true,
      bindings: [{ type: "kv", binding: "TOP_LEVEL_SHOULD_NOT_LEAK" }],
      internal: { trace: { worker: "top-level-trace" } },
      config: { public: true },
    }),
  );
  await writeFile(
    join(root, "src/worker.ts"),
    "export default { fetch() { return new Response('ok') } };\n",
  );
  await writeFile(join(root, "src/client.ts"), "export default 'client';\n");

  const config = await resolveConfig(
    {
      root,
      configFile: false,
      plugins: [
        {
          name: "dd-config-before",
          config() {
            return {
              environments: {
                dd: {
                  define: {
                    __BEFORE_CONFIG__: JSON.stringify("before"),
                  },
                  optimizeDeps: {
                    exclude: ["before-excluded"],
                  },
                },
              },
            };
          },
          configEnvironment(name) {
            if (name !== "dd") {
              return;
            }
            return {
              define: {
                __BEFORE_ENV__: JSON.stringify("before-env"),
              },
            };
          },
        },
        ddVitePlugin({
          middleware: false,
          environmentOptions: {
            define: {
              __ENVIRONMENT_OPTIONS__: JSON.stringify("environment-options"),
              __SHARED_ENVIRONMENT_OPTION__: JSON.stringify("environment-options"),
            },
          },
          viteEnvironment: {
            name: "dd",
            options: {
              define: {
                __VITE_ENVIRONMENT_OPTIONS__: JSON.stringify("vite-environment-options"),
                __SHARED_ENVIRONMENT_OPTION__: JSON.stringify("vite-environment-options"),
              },
            },
          },
          runtimeOptions: {
            binary: "/does/not/exist",
          },
        }),
        {
          name: "dd-config-after",
          config() {
            return {
              environments: {
                dd: {
                  define: {
                    __AFTER_CONFIG__: JSON.stringify("after"),
                  },
                },
              },
            };
          },
          configEnvironment(name) {
            if (name !== "dd") {
              return;
            }
            return {
              define: {
                __AFTER_ENV__: JSON.stringify("after-env"),
              },
              optimizeDeps: {
                include: ["after-included"],
              },
            };
          },
        },
      ],
    },
    "serve",
    "development",
  );

  const pluginNames = config.plugins.map((plugin) => plugin.name);
  assert(
    pluginNames.indexOf("dd-config-before") < pluginNames.indexOf("dd-vite"),
    "dd plugin should keep normal plugin order before later plugins",
  );
  assert(
    pluginNames.indexOf("dd-vite") < pluginNames.indexOf("dd-config-after"),
    "dd plugin should keep normal plugin order after earlier plugins",
  );

  assert.notEqual(config.resolve.noExternal, true, "dd resolve defaults should not leak to root config");
  assert.notEqual(
    config.environments.client?.resolve?.noExternal,
    true,
    "dd resolve defaults should not leak to the client environment",
  );

  const ddEnvironment = config.environments.dd;
  assert(ddEnvironment, "dd environment should be registered");
  assert.equal(ddEnvironment.consumer, "server");
  assert.equal(ddEnvironment.resolve.noExternal, true);
  assert.equal(ddEnvironment.define.__BEFORE_CONFIG__, JSON.stringify("before"));
  assert.equal(ddEnvironment.define.__BEFORE_ENV__, JSON.stringify("before-env"));
  assert.equal(ddEnvironment.define.__AFTER_CONFIG__, JSON.stringify("after"));
  assert.equal(ddEnvironment.define.__AFTER_ENV__, JSON.stringify("after-env"));
  assert.equal(ddEnvironment.define.__ENVIRONMENT_OPTIONS__, JSON.stringify("environment-options"));
  assert.equal(ddEnvironment.define.__VITE_ENVIRONMENT_OPTIONS__, JSON.stringify("vite-environment-options"));
  assert.equal(ddEnvironment.define.__SHARED_ENVIRONMENT_OPTION__, JSON.stringify("vite-environment-options"));
  assert(ddEnvironment.optimizeDeps.exclude.includes("before-excluded"));
  assert(ddEnvironment.optimizeDeps.include.includes("after-included"));
  assert(
    ddEnvironment.optimizeDeps.entries.some((entry) => entry.endsWith("/src/worker.ts")),
    "dd environment should optimize the worker entry discovered from dd.json",
  );
  assert.equal(typeof ddEnvironment.dev.createEnvironment, "function");

  await buildApp({
    root,
    configFile: false,
    logLevel: "silent",
    environments: {
      ssr: {},
    },
    build: {
      emptyOutDir: true,
      outDir: "dist",
      rollupOptions: {
        input: join(root, "src/client.ts"),
      },
    },
    plugins: [
      ddVitePlugin({
        middleware: false,
      }),
    ],
  });

  const generatedConfig = JSON.parse(await readFile(join(root, "dist/config-check-worker/dd.deploy.json"), "utf8"));
  assert.equal(generatedConfig.name, "config-check-worker");
  assert.equal(generatedConfig.entrypoint, "worker.js");
  assert.equal(generatedConfig.assets_dir, "../client");
  assert.equal(generatedConfig.base_url, "https://dd.example.test");
  assert.equal(generatedConfig.temporary, true);
  assert(generatedConfig.asset_excludes.includes("secret-local.txt"));
  assert.deepEqual(generatedConfig.config, { public: true });
  assert.equal(generatedConfig.deploy_token, undefined);
  assert.equal(generatedConfig.local_only, undefined);
  assert.equal(generatedConfig.public, undefined);
  assert.equal(generatedConfig.bindings, undefined);
  assert.equal(generatedConfig.internal, undefined);
  assert(
    await readFile(join(root, "dist/config-check-worker/worker.js"), "utf8"),
    "dd worker artifact should be emitted even when an unrelated ssr environment exists",
  );
  const manifest = JSON.parse(await readFile(join(root, "dist/dd.workers.json"), "utf8"));
  assert.equal(manifest.entry, "config-check-worker");
  assert.equal(manifest.workers[0].deployConfig, "config-check-worker/dd.deploy.json");
  assert.equal(
    process.env[workerBundleEnv],
    undefined,
    "dd worker bundle guard should not leak into later Vite plugin initialization",
  );

  await buildApp({
    root,
    configFile: false,
    logLevel: "silent",
    build: {
      emptyOutDir: true,
      outDir: "flat-dist",
      assetsDir: ".",
      rollupOptions: {
        input: join(root, "src/client.ts"),
      },
    },
    plugins: [
      ddVitePlugin({
        middleware: false,
      }),
    ],
  });

  let flatHeaders = "";
  try {
    flatHeaders = await readFile(join(root, "flat-dist/client/_headers"), "utf8");
  } catch (error) {
    if (error?.code !== "ENOENT") {
      throw error;
    }
  }
  assert(
    !flatHeaders.split(/\r?\n/).some((line) => line.trim() === "/*"),
    "flat Vite assets must not generate an immutable root-wide cache policy",
  );

  const lateRoot = join(root, "late-root");
  const lateAppRoot = join(lateRoot, "app");
  await mkdir(join(lateAppRoot, "src"), { recursive: true });
  await writeFile(join(lateAppRoot, "package.json"), JSON.stringify({ type: "module" }));
  await writeFile(
    join(lateAppRoot, "dd.json"),
    JSON.stringify({
      name: "late-root-worker",
      entrypoint: "src/worker.ts",
      config: { public: true },
    }),
  );
  await writeFile(
    join(lateAppRoot, "src/worker.ts"),
    "export default { fetch() { return new Response('late root') } };\n",
  );

  const lateRootConfig = await resolveConfig(
    {
      root: lateRoot,
      configFile: false,
      plugins: [
        ddVitePlugin({
          middleware: false,
          viteEnvironment: {
            name: "dd",
          },
          runtimeOptions: {
            binary: "/does/not/exist",
          },
        }),
        {
          name: "late-root-plugin",
          config() {
            return {
              root: lateAppRoot,
            };
          },
        },
      ],
    },
    "serve",
    "development",
  );
  assert.equal(lateRootConfig.root, lateAppRoot);
  assert(
    lateRootConfig.environments.dd.optimizeDeps.entries.some((entry) =>
      entry.endsWith("/src/worker.ts")
    ),
    "dd plugin should resolve dd.json and worker entries after normal plugins update root",
  );

  const frameworkEnvironmentConfig = await resolveConfig(
    {
      root,
      configFile: false,
      plugins: [
        ddVitePlugin({
          framework: "react-router",
          middleware: false,
          environmentOptions: {
            define: {
              __FRAMEWORK_ENVIRONMENT_OPTIONS__: JSON.stringify("framework-environment-options"),
            },
          },
          runtimeOptions: {
            binary: "/does/not/exist",
          },
        }),
      ],
    },
    "serve",
    "development",
  );
  assert.equal(
    frameworkEnvironmentConfig.environments.ssr.define.__FRAMEWORK_ENVIRONMENT_OPTIONS__,
    JSON.stringify("framework-environment-options"),
    "framework-created environments should still merge environmentOptions",
  );
} finally {
  if (previousWorkerBundleEnv === undefined) {
    delete process.env[workerBundleEnv];
  } else {
    process.env[workerBundleEnv] = previousWorkerBundleEnv;
  }
  await rm(root, { recursive: true, force: true });
}

async function buildApp(config) {
  const builder = await createBuilder(config);
  await builder.buildApp();
}
