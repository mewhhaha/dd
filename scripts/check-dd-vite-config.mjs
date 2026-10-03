import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { mkdir, mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { promisify } from "node:util";
import { ddVitePlugin } from "../packages/dd-vite/src/vite.js";
import { shouldBypassViteRequest } from "../packages/dd-vite/src/vite/dev.js";

const require = createRequire(new URL("../packages/dd-vite/package.json", import.meta.url));
const { createBuilder, resolveConfig } = await import(require.resolve("vite"));
const root = await mkdtemp(join(tmpdir(), "dd-vite-config-"));
const runFile = promisify(execFile);
const privateModules = [
  { type: "ESModule", path: "private/helper.js", file: "private/helper.js", bytes: Buffer.from("export default 42;\n") },
  { type: "Json", path: "private/config.json", file: "private/config.json", bytes: Buffer.from('{"answer":42}\n') },
  { type: "Text", path: "private/query.sql", file: "private/query.sql", bytes: Buffer.from("SELECT 42;\n") },
  { type: "Data", path: "private/value.bin", file: "private/value.bin", bytes: Buffer.from([0, 255, 13, 10]) },
  { type: "CompiledWasm", path: "private/module.wasm", file: "private/module.wasm", bytes: Buffer.from([0, 97, 115, 109, 1, 0, 0, 0]) },
];
const workerBundleEnv = "DD_VITE_WORKER_BUNDLE";
const previousWorkerBundleEnv = process.env[workerBundleEnv];
delete process.env[workerBundleEnv];

const getRequest = { headers: {}, method: "GET" };
assert.equal(
  shouldBypassViteRequest(
    getRequest,
    "/@react-router/critical.css?pathname=%2Fprojects%2Fruntime",
    "/",
    "/",
  ),
  true,
  "React Router development assets must reach the framework middleware",
);
assert.equal(
  shouldBypassViteRequest(getRequest, "/__manifest?p=%2Fprojects%2Fruntime", "/", "/"),
  false,
  "React Router route discovery must continue through the worker",
);

try {
  await mkdir(join(root, "src"), { recursive: true });
  await mkdir(join(root, "private"), { recursive: true });
  for (const module of privateModules) {
    await writeFile(join(root, module.file), module.bytes);
  }
  await writeFile(join(root, "package.json"), JSON.stringify({ type: "module" }));
  await writeFile(
    join(root, "dd.json"),
    JSON.stringify({
      schema_version: 1,
      name: "config-check-worker",
      entrypoint: "src/worker.ts",
      baseUrl: "https://dd.example.test",
      temporary: true,
      asset_excludes: ["secret-local.txt"],
      server_modules: privateModules.map(({ bytes, ...module }) => module),
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
  assert.equal(
    ddEnvironment.define["process.env.NODE_ENV"],
    JSON.stringify("development"),
    "serve environments must optimize framework dependencies in development mode",
  );
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
        deploymentConfig: { input: "dd.json" },
      }),
    ],
  });

  const generatedConfig = JSON.parse(await readFile(join(root, "dist/config-check-worker/dd.deploy.json"), "utf8"));
  assert.equal(generatedConfig.schema_version, 1);
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
  assert.equal(generatedConfig.server_modules.length, privateModules.length);
  for (const [index, module] of generatedConfig.server_modules.entries()) {
    assert.equal(module.path, privateModules[index].path);
    assert.equal(module.type, privateModules[index].type);
    assert(module.file.startsWith("server-modules/"));
    assert.deepEqual(
      await readFile(join(root, "dist/config-check-worker", module.file)),
      privateModules[index].bytes,
      "generated module references must resolve to the original private bytes",
    );
  }
  if (process.env.DD_CLI_BIN) {
    const { stdout } = await runFile(process.env.DD_CLI_BIN, [
      "package-deploy-config", join(root, "dist/config-check-worker/dd.deploy.json"),
      "--allow-outside-config-root",
    ], { maxBuffer: 4 * 1024 * 1024 });
    const request = JSON.parse(stdout);
    assert.equal(request.server_modules.length, privateModules.length);
    for (const [index, module] of request.server_modules.entries()) {
      assert.equal(module.path, privateModules[index].path);
      assert.deepEqual(Buffer.from(module.content_base64, "base64"), privateModules[index].bytes);
    }
    assert(request.assets.every((asset) => !asset.path.includes("private/") && !asset.path.includes("server-modules/")));
  }
  const workerArtifact = await readFile(join(root, "dist/config-check-worker/worker.js"), "utf8");
  assert(workerArtifact, "dd worker artifact should be emitted even when an unrelated ssr environment exists");
  assert(
    workerArtifact.includes("globalThis.process"),
    "production worker bundle should install a minimal process global",
  );
  const manifest = JSON.parse(await readFile(join(root, "dist/dd.workers.json"), "utf8"));
  assert.equal(manifest.entry, "config-check-worker");
  assert.equal(manifest.workers[0].deployConfig, "config-check-worker/dd.deploy.json");
  assert.equal(
    process.env[workerBundleEnv],
    undefined,
    "dd worker bundle guard should not leak into later Vite plugin initialization",
  );
  const urlInputConfig = await resolveConfig({
    root, configFile: false, logLevel: "silent",
    plugins: [ddVitePlugin({ deploymentConfig: { input: pathToFileURL(join(root, "dd.json")) } })],
  }, "build");
  assert(urlInputConfig.environments["config_check_worker"], "file URL config inputs must resolve successfully");

  const inputConfig = JSON.parse(await readFile(join(root, "dd.json"), "utf8"));
  for (const input of [inputConfig, () => inputConfig, async () => inputConfig]) {
    const inlineConfig = await resolveConfig({
      root, configFile: false, logLevel: "silent",
      plugins: [ddVitePlugin({ deploymentConfig: { input } })],
    }, "build");
    assert(inlineConfig.environments.config_check_worker, "object and function config inputs must resolve successfully");
  }
  const partialConfig = { name: "partial-config-worker", entrypoint: "src/worker.ts", config: { public: true } };
  for (const input of [partialConfig, () => partialConfig, async () => partialConfig]) {
    const inlineConfig = await resolveConfig({
      root, configFile: false, logLevel: "silent",
      plugins: [ddVitePlugin({ deploymentConfig: { input } })],
    }, "build");
    assert(inlineConfig.environments.partial_config_worker, "inline configs may omit the schema version");
  }
  const invalidInputFile = join(root, "invalid-input.json");
  for (const [invalid, expectedError] of [
    [{ ...inputConfig, publik: true }, /unknown field publik/],
    [{ ...inputConfig, schema_version: 99 }, /schema_version must equal 1/],
    [{ ...inputConfig, config: { public: "invalid" } }, /public must be a boolean/],
    [{ ...inputConfig, server_modules: "invalid" }, /server_modules must be an array/],
    [{ ...inputConfig, $schema: 42 }, /\$schema.*string/],
    [{ ...inputConfig, name: "" }, /name.*non-empty string/],
    [{ ...inputConfig, entrypoint: null }, /entrypoint.*non-empty string/],
    [{ ...inputConfig, base_url: "relative/path" }, /absolute URI/],
    [{ ...inputConfig, baseUrl: 42 }, /baseUrl.*string/],
    [{ ...inputConfig, assets_dir: false }, /assets_dir.*string/],
    [{ ...inputConfig, temporary: "true" }, /temporary must be a boolean/],
    [{ ...inputConfig, asset_excludes: [99] }, /asset_excludes.*array of strings/],
    [{ ...inputConfig, asset_excludes: null }, /asset_excludes.*array of strings/],
    [{ ...inputConfig, server_modules: [{ path: "private/helper.js", type: "Unknown" }] }, /unsupported module type/],
    [{ ...inputConfig, server_modules: [{ type: "ESModule" }] }, /path.*non-empty string/],
    [{ ...inputConfig, server_modules: [{ path: "private/helper.js" }] }, /exactly one of type or kind/],
    [{ ...inputConfig, server_modules: [{ path: "private/helper.js", type: "ESModule", kind: "Text" }] }, /exactly one of type or kind/],
    [{ ...inputConfig, server_modules: [{ path: "private/helper.js", type: "ESModule", file: false }] }, /file.*string/],
    [{ ...inputConfig, bindings: [{ type: "memory", binding: "" }] }, /binding.*non-empty string/],
    [{ ...inputConfig, bindings: [{ type: "service", binding: "SERVICE", service: "" }] }, /service.*non-empty string/],
    [{ ...inputConfig, internal: { trace: { worker: "" } } }, /worker.*non-empty string/],
    [{ ...inputConfig, internal: { trace: { worker: "trace", path: "relative" } } }, /path must start with/],
    [[], /expected an object/],
  ]) {
    await writeFile(invalidInputFile, JSON.stringify(invalid));
    for (const input of [invalidInputFile, pathToFileURL(invalidInputFile), invalid, () => invalid, async () => invalid]) {
      await assert.rejects(resolveConfig({
        root, configFile: false, logLevel: "silent",
        plugins: [ddVitePlugin({ deploymentConfig: { input } })],
      }, "build"), expectedError);
    }
  }
  for (const incomplete of [
    { schema_version: 1 },
    { schema_version: 1, name: "required-name" },
    { schema_version: 1, entrypoint: "src/worker.ts" },
  ]) {
    await writeFile(invalidInputFile, JSON.stringify(incomplete));
    for (const input of [invalidInputFile, pathToFileURL(invalidInputFile)]) {
      await assert.rejects(resolveConfig({
        root, configFile: false, logLevel: "silent",
        plugins: [ddVitePlugin({ deploymentConfig: { input } })],
      }, "build"), /name.*non-empty string|entrypoint.*non-empty string/);
    }
  }
  for (const settings of [
    { assetExcludes: [99] },
    { serverModules: [{ path: "private/helper.js", type: "Unknown" }] },
  ]) {
    await assert.rejects(async () => resolveConfig({
      root, configFile: false, logLevel: "silent",
      plugins: [ddVitePlugin({ deploymentConfig: settings })],
    }, "build"), /array of strings|unsupported module type/);
  }
  for (const [settings, expectedError] of [
    [{ config: { bindings: "invalid" } }, /bindings must be an array/],
    [{ auxiliaryWorkers: [{ name: "auxiliary", source: "export default {};", config: { public: null } }] }, /public must be a boolean/],
    [{ auxiliaryWorkers: [{ name: "auxiliary", source: "export default {};", config: { bindings: "invalid" } }] }, /bindings must be an array/],
  ]) {
    await assert.rejects(resolveConfig({
      root, configFile: false, logLevel: "silent",
      plugins: [ddVitePlugin({
        auxiliaryWorkers: [{ name: "auxiliary", source: "export default {};" }],
        ...settings,
      })],
    }, "serve"), expectedError, "auxiliary workers must not hide malformed runtime config");
  }
  for (const input of [() => null, async () => undefined]) {
    await assert.rejects(resolveConfig({
      root, configFile: false, logLevel: "silent",
      plugins: [ddVitePlugin({ deploymentConfig: { input } })],
    }, "build"), /expected an object/);
  }
  console.log("dd-vite: file, URL, object, sync and async config inputs validate consistently");

  for (const [label, settings] of [
    ["disabled", false],
    ["disabled-option", { enabled: false, input: "dd.json" }],
    ["custom", { output: "metadata/deploy.json", entrypoint: "bundle/main.js" }],
  ]) {
    const outDir = `options-${label}`;
    await buildApp({
      root, configFile: false, logLevel: "silent",
      build: { outDir, rollupOptions: { input: join(root, "src/client.ts") } },
      plugins: [ddVitePlugin({
        middleware: false,
        deploymentConfig: settings,
        auxiliaryWorkers: [{
          name: "auth", entry: join(root, "src/worker.ts"),
          deployment: { output: "metadata/auth.json", entrypoint: "bundle/auth.js" },
        }],
      })],
    });
    const optionsManifest = JSON.parse(await readFile(join(root, outDir, "dd.workers.json"), "utf8"));
    for (const record of optionsManifest.workers) {
      const artifact = await readFile(join(root, outDir, record.worker), "utf8");
      assert(artifact, "the manifest must point to the actual configured worker artifact");
      if (label !== "custom") {
        assert.equal(record.deployConfig, undefined, "disabled deployment output must not advertise missing config files");
        assert.deepEqual(await readdir(join(root, outDir, record.outDir)), record.role === "entry" ? ["worker.js"] : ["bundle"]);
        continue;
      }
      assert(record.deployConfig.includes("/metadata/"));
      const configPath = join(root, outDir, record.deployConfig);
      const configured = JSON.parse(await readFile(configPath, "utf8"));
      assert.equal(configured.entrypoint, record.role === "entry" ? "../bundle/main.js" : "../bundle/auth.js");
      if (record.role === "entry") {
        assert.equal(configured.assets_dir, "../../client");
        assert.equal(configured.server_modules.length, privateModules.length);
      }
      if (process.env.DD_CLI_BIN) {
        const { stdout } = await runFile(process.env.DD_CLI_BIN, ["package-deploy-config", configPath, "--allow-outside-config-root"]);
        const request = JSON.parse(stdout);
        assert.equal(request.source, artifact, "CLI packaging must resolve custom entrypoint relative to its config directory");
        if (record.role === "entry") {
          assert.equal(request.server_modules.length, privateModules.length);
          for (const [index, module] of request.server_modules.entries()) {
            assert.deepEqual(Buffer.from(module.content_base64, "base64"), privateModules[index].bytes);
          }
        }
      }
    }
  }
  for (const settings of [
    { output: "../escape.json" },
    { entrypoint: "../escape.js" },
    { output: "worker.js" },
  ]) {
    await assert.rejects(resolveConfig({
      root, configFile: false, logLevel: "silent",
      plugins: [ddVitePlugin({ deploymentConfig: settings })],
    }, "build"), /relative path|different files/);
  }

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
      schema_version: 1,
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
