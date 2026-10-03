import assert from "node:assert/strict";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import { createDdRuntime } from "../packages/dd-vite/src/runtime.js";
import { ddVitePlugin } from "../packages/dd-vite/src/vite.js";

const repoRoot = fileURLToPath(new URL("../", import.meta.url));
const require = createRequire(join(repoRoot, "packages/dd-vite/package.json"));
const { createServer } = await import(require.resolve("vite"));
const root = await mkdtemp(join(tmpdir(), "dd-vite-reload-"));
const binary = process.env.DD_DEV_RUNTIME_BIN ?? join(repoRoot, "target/debug/dd_dev_runtime");

async function exerciseReload({ mode, factory = false }) {
  const label = `${mode ?? "default"}-${factory}`;
  const entry = join(root, `${label}.js`);
  const factoryInput = join(root, `${label}.txt`);
  const fileSource = value => `export default { fetch() { return new Response(${JSON.stringify(value)}); } };\n`;
  await writeFile(entry, fileSource("before"));
  await writeFile(factoryInput, "before");
  const runtime = createDdRuntime({ binary, env: { TMPDIR: root } });
  const workers = [{ name: "file-side", binding: "FILE", entry }];
  if (factory) workers.push({
    name: "factory-side", binding: "FACTORY", source: async () => fileSource(await readFile(factoryInput, "utf8")),
  });
  let server;
  const updates = new Set();
  try {
    server = await createServer({
      root, configFile: false, logLevel: "silent", server: { host: "127.0.0.1", port: 0 },
      plugins: [
        ddVitePlugin({
          name: "reload-review", runtime, reloadOnHotUpdate: mode,
          source: `export default { async fetch(request, env) {
            return Response.json({
              file: await (await env.FILE.fetch("https://fixture.test/")).text(),
              factory: env.FACTORY ? await (await env.FACTORY.fetch("https://fixture.test/")).text() : null,
            });
          } };`,
          auxiliaryWorkers: workers,
        }),
        { name: "reload-observer", handleHotUpdate(context) { updates.add(context.file); } },
      ],
    });
    await server.listen();
    const base = `http://127.0.0.1:${server.httpServer.address().port}`;
    const invoke = async () => {
      const response = await fetch(base);
      assert.equal(response.status, 200, `${label}: ${await response.clone().text()}`);
      return response.json();
    };
    const initial = { file: "before", factory: factory ? "before" : null };
    assert.deepEqual(await invoke(), initial);
    const generation = runtime.generation;
    const waitForUpdate = async (file) => {
      for (let attempt = 0; attempt < 200 && !updates.has(file); attempt += 1) await delay(25);
      assert(updates.has(file), `${label}: Vite must observe the file edit`);
    };
    await writeFile(entry, fileSource("after"));
    await waitForUpdate(entry);
    assert.deepEqual(await invoke(), { ...initial, file: mode === false ? "before" : "after" }, `${label}: auxiliary entry edits must respect the reload mode`);
    if (factory) {
      await writeFile(factoryInput, "after");
      await waitForUpdate(factoryInput);
      assert.deepEqual(await invoke(), { file: "after", factory: "after" }, "source factories must regenerate even alongside module-runner workers");
    }
    assert.equal(runtime.generation, generation, "hot reload must reuse the app runtime");
  } finally {
    await server?.close();
    await runtime.close();
  }
}

try {
  await writeFile(join(root, "package.json"), '{"type":"module"}\n');
  for (const mode of [undefined, "entry", false]) await exerciseReload({ mode });
  await exerciseReload({ mode: "all", factory: true });
  console.log("dd-vite: auxiliary hot reload with inline entry, reload modes, source factories and shared runtime passed");
} finally {
  await rm(root, { recursive: true, force: true });
}
