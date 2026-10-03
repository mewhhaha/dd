import assert from "node:assert/strict";
import { chmod, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import { ddEnvironment, ddVitePlugin } from "../packages/dd-vite/src/vite.js";
import { createDdRuntime } from "../packages/dd-vite/src/runtime.js";
import { createWorkerTestRuntime } from "../packages/dd-vite/src/vitest.js";

const repoRoot = fileURLToPath(new URL("../", import.meta.url));
const require = createRequire(join(repoRoot, "packages/dd-vite/package.json"));
const { createServer } = await import(require.resolve("vite"));
const root = await mkdtemp(join(tmpdir(), "dd-vite-lifecycle-"));
const binary = process.env.DD_DEV_RUNTIME_BIN ?? join(repoRoot, "target/debug/dd_dev_runtime");
const wrapper = join(root, "native-runtime");
const pids = new Set();
let server;
let sharedRuntime;

function isAlive(pid) {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    if (error.code === "ESRCH") return false;
    throw error;
  }
}

async function assertExited(pid) {
  for (let attempt = 0; attempt < 100 && isAlive(pid); attempt += 1) await delay(20);
  assert.equal(isAlive(pid), false, "Vite teardown must stop its owned native process");
}

async function startEnvironment(name, pidFile, runtime) {
  return createServer({
    root, configFile: false, logLevel: "silent",
    environments: {
      review: ddEnvironment({
        name,
        source: "export default { fetch() { return new Response('ok') } };",
        runtime,
        runtimeOptions: {
          binary: wrapper,
          env: { DD_LIFECYCLE_NATIVE_BIN: binary, DD_LIFECYCLE_PID_FILE: pidFile, TMPDIR: root },
        },
      }),
    },
  });
}

const counterSource = `export default {
  async fetch(request, env) {
    if (new URL(request.url).pathname === "/service") return env.COUNTER.fetch("https://fixture.test/");
    const count = await env.STATE.get("counter").atomic(tx => {
      const next = (tx.get("count") ?? 0) + 1;
      tx.put("count", next);
      return next;
    });
    return Response.json({ count });
  },
};`;

async function startApp(pidFile, { runtime, source = counterSource, entry, middleware } = {}) {
  return createServer({
    root, configFile: false, logLevel: "silent", server: { port: 0, host: "127.0.0.1" },
    plugins: [ddVitePlugin({
      name: "coherent-app", source: entry ? undefined : source, entry, runtime, middleware,
      config: { bindings: [{ type: "memory", binding: "STATE" }] },
      viteEnvironment: { name: "entry" },
      runtimeOptions: { binary: wrapper, env: { DD_LIFECYCLE_NATIVE_BIN: binary, DD_LIFECYCLE_PID_FILE: pidFile, TMPDIR: root } },
      auxiliaryWorkers: [{
        name: "counter-service", binding: "COUNTER", source: counterSource,
        config: { bindings: [{ type: "memory", binding: "STATE" }] },
        viteEnvironment: { name: "counter" },
      }],
    })],
  });
}

async function assertAppStateCoherent(app, pidFile, environmentFirst) {
  await app.listen();
  const base = `http://127.0.0.1:${app.httpServer.address().port}`;
  const http = async path => {
    const response = await fetch(`${base}${path}`);
    if (response.status !== 200) assert.fail(`HTTP ${path} failed with ${response.status}: ${await response.text()}`);
    return response;
  };
  const environment = () => app.environments.entry.dispatchFetch(new Request("https://fixture.test/"));
  const paths = environmentFirst ? [environment, () => http("/"), environment] : [() => http("/"), environment, () => http("/")];
  for (const [index, request] of paths.entries()) {
    assert.deepEqual(await (await request()).json(), { count: index + 1 }, "HTTP and environment dispatch must share worker state");
  }
  assert.deepEqual(await (await http("/service")).json(), { count: 1 });
  assert.deepEqual(await (await app.environments.counter.dispatchFetch(new Request("https://fixture.test/"))).json(), { count: 2 });
  assert.deepEqual(await (await http("/service")).json(), { count: 3 }, "auxiliary environment and service bindings must share state");
  await app.environments.entry.close();
  assert.deepEqual(await (await http("/")).json(), { count: 4 }, "closing one environment must preserve the app runtime");
  const starts = (await readFile(`${pidFile}.starts`, "utf8")).trim().split("\n");
  assert.equal(starts.length, 1, "all app request paths must use one native process");
  const pid = Number(starts[0]);
  pids.add(pid);
  return pid;
}

try {
  await writeFile(join(root, "package.json"), '{"type":"module"}\n');
  await writeFile(wrapper, '#!/bin/sh\nprintf \'%s\\n\' "$$" > "$DD_LIFECYCLE_PID_FILE"\nprintf \'%s\\n\' "$$" >> "$DD_LIFECYCLE_PID_FILE.starts"\nexec "$DD_LIFECYCLE_NATIVE_BIN" "$@"\n');
  await chmod(wrapper, 0o755);
  const successfulPidFile = join(root, "successful.pid");
  server = await startEnvironment("environment-lifecycle", successfulPidFile);
  const response = await server.environments.review.dispatchFetch(new Request("https://fixture.test/"));
  assert.equal(response.status, 200);
  assert.equal(await response.text(), "ok");
  const pid = Number(await readFile(successfulPidFile, "utf8"));
  pids.add(pid);
  assert(isAlive(pid), "the fixture must exercise a running native process");
  await server.close();
  server = undefined;
  await assertExited(pid);

  const failedPidFile = join(root, "failed.pid");
  server = await startEnvironment("", failedPidFile);
  await assert.rejects(server.environments.review.dispatchFetch(new Request("https://fixture.test/")), /name must not be empty/i);
  const failedPid = Number(await readFile(failedPidFile, "utf8"));
  pids.add(failedPid);
  await server.close();
  server = undefined;
  await assertExited(failedPid);

  const unusedPidFile = join(root, "unused.pid");
  server = await startEnvironment("unused", unusedPidFile);
  await server.close();
  server = undefined;
  await assert.rejects(readFile(unusedPidFile), { code: "ENOENT" });

  for (const environmentFirst of [false, true]) {
    const appPidFile = join(root, `app-${environmentFirst}.pid`);
    server = await startApp(appPidFile);
    const appPid = await assertAppStateCoherent(server, appPidFile, environmentFirst);
    await server.close();
    server = undefined;
    await assertExited(appPid);
  }
  const inlineRestartPidFile = join(root, "inline-restart.pid");
  server = await startApp(inlineRestartPidFile);
  await server.listen();
  const restartedPids = [];
  for (let attempt = 0; attempt < 3; attempt += 1) {
    const response = await fetch(`http://127.0.0.1:${server.httpServer.address().port}`);
    assert.deepEqual(await response.json(), { count: 1 }, "owned restarts must deploy into a fresh runtime");
    const currentPid = Number(await readFile(inlineRestartPidFile, "utf8"));
    pids.add(currentPid);
    assert(!restartedPids.includes(currentPid));
    restartedPids.push(currentPid);
    if (attempt < 2) await server.restart();
    else await server.close();
    await assertExited(currentPid);
  }
  server = undefined;

  const configRestartPidFile = join(root, "config-restart.pid");
  const restartConfigFile = join(root, "restart.config.mjs");
  const restartOptions = {
    name: "config-restart", source: "export default { fetch() { return new Response('ok'); } };",
    runtimeOptions: { binary: wrapper, env: { DD_LIFECYCLE_NATIVE_BIN: binary, DD_LIFECYCLE_PID_FILE: configRestartPidFile, TMPDIR: root } },
  };
  await writeFile(restartConfigFile, `import { ddVitePlugin } from ${JSON.stringify(fileURLToPath(new URL("../packages/dd-vite/src/vite.js", import.meta.url)))};\nexport default { plugins: [ddVitePlugin(${JSON.stringify(restartOptions)})] };\n`);
  server = await createServer({ root, configFile: restartConfigFile, logLevel: "silent", server: { port: 0, host: "127.0.0.1" } });
  for (let attempt = 0; attempt < 3; attempt += 1) {
    if (attempt === 0) await server.listen();
    assert.equal(await (await fetch(`http://127.0.0.1:${server.httpServer.address().port}`)).text(), "ok");
    const currentPid = Number(await readFile(configRestartPidFile, "utf8"));
    pids.add(currentPid);
    if (attempt === 0) await server.restart();
    else if (attempt === 1) {
      await server.watcher.unwatch(restartConfigFile);
      await writeFile(restartConfigFile, "export default }\n");
      await server.restart();
    }
    else await server.close();
    await assertExited(currentPid);
  }
  server = undefined;

  const entryFile = join(root, "counter.js");
  const fileAppPidFile = join(root, "file-app.pid");
  await writeFile(entryFile, counterSource);
  server = await startApp(fileAppPidFile, { entry: entryFile });
  assert.deepEqual(await (await server.environments.entry.dispatchFetch(new Request("https://fixture.test/"))).json(), { count: 1 });
  await server.listen();
  const fileAppBase = `http://127.0.0.1:${server.httpServer.address().port}`;
  assert.deepEqual(await (await fetch(fileAppBase)).json(), { count: 2 }, "starting the module runner must preserve previously dispatched state");
  assert.deepEqual(await (await server.environments.entry.dispatchFetch(new Request("https://fixture.test/"))).json(), { count: 3 });
  const fileAppStarts = (await readFile(`${fileAppPidFile}.starts`, "utf8")).trim().split("\n");
  assert.equal(fileAppStarts.length, 1, "file-backed dispatch must share the app process before and after listen");
  const fileAppPid = Number(fileAppStarts[0]);
  pids.add(fileAppPid);
  await server.close();
  server = undefined;
  await assertExited(fileAppPid);
  const environmentOnlyPidFile = join(root, "environment-only.pid");
  server = await startApp(environmentOnlyPidFile, { entry: entryFile, middleware: false });
  await server.listen();
  for (const count of [1, 2]) {
    const response = await server.environments.entry.dispatchFetch(new Request("https://fixture.test/"));
    assert.deepEqual(await response.json(), { count }, "environment-only servers must support the module runner");
  }
  const environmentOnlyStarts = (await readFile(`${environmentOnlyPidFile}.starts`, "utf8")).trim().split("\n");
  assert.equal(environmentOnlyStarts.length, 1);
  const environmentOnlyPid = Number(environmentOnlyStarts[0]);
  pids.add(environmentOnlyPid);
  await server.close();
  server = undefined;
  await assertExited(environmentOnlyPid);
  const failedAppPidFile = join(root, "failed-app.pid");
  server = await startApp(failedAppPidFile, { source: "export default }" });
  await assert.rejects(server.environments.entry.dispatchFetch(new Request("https://fixture.test/")), /SyntaxError|Unexpected/);
  const failedAppPid = Number(await readFile(failedAppPidFile, "utf8"));
  pids.add(failedAppPid);
  await server.close();
  server = undefined;
  await assertExited(failedAppPid);

  const sharedPidFile = join(root, "shared.pid");
  sharedRuntime = createDdRuntime({
    binary: wrapper, env: { DD_LIFECYCLE_NATIVE_BIN: binary, DD_LIFECYCLE_PID_FILE: sharedPidFile, TMPDIR: root },
  });
  const first = await createWorkerTestRuntime({
    runtime: sharedRuntime, name: "first", source: "export default { fetch() { return new Response('first') } };",
  });
  const sibling = await createWorkerTestRuntime({
    runtime: sharedRuntime, name: "sibling", source: "export default { fetch() { return new Response('sibling') } };",
  });
  const sharedPid = Number(await readFile(sharedPidFile, "utf8"));
  pids.add(sharedPid);
  await first.close();
  assert.equal(await (await sibling.fetch("https://fixture.test/")).text(), "sibling");
  await assert.rejects(createWorkerTestRuntime({ runtime: sharedRuntime, name: "", source: "export default {};" }), /name must not be empty/i);
  server = await startEnvironment("shared-environment", sharedPidFile, sharedRuntime);
  assert.equal(await (await server.environments.review.dispatchFetch(new Request("https://fixture.test/"))).text(), "ok");
  await server.close();
  server = undefined;
  assert(isAlive(sharedPid), "environment teardown must preserve caller-owned runtimes");
  assert.equal(await (await sibling.fetch("https://fixture.test/")).text(), "sibling");
  await sibling.close();
  server = await startApp(sharedPidFile, { runtime: sharedRuntime });
  assert.equal(await assertAppStateCoherent(server, sharedPidFile, true), sharedPid);
  await server.restart();
  const sharedRestartResponse = await fetch(`http://127.0.0.1:${server.httpServer.address().port}`);
  assert.deepEqual(await sharedRestartResponse.json(), { count: 5 }, "restart must preserve caller-owned runtime state");
  assert.equal((await readFile(`${sharedPidFile}.starts`, "utf8")).trim().split("\n").length, 1);
  assert.equal(await (await sharedRuntime.fetch("sibling", "https://fixture.test/")).text(), "sibling");
  await server.close();
  server = undefined;
  assert(isAlive(sharedPid), "plugin teardown must preserve caller-owned runtimes");
  assert.deepEqual(await (await sharedRuntime.fetch("coherent-app", "https://fixture.test/")).json(), { count: 6 });
  await sharedRuntime.close();
  sharedRuntime = undefined;
  await assertExited(sharedPid);
  console.log("dd-vite: coherent HTTP/environment/service state, repeated/config restarts, runtime ownership and teardown passed");
} finally {
  await server?.close();
  await sharedRuntime?.close();
  for (const pid of pids) if (isAlive(pid)) process.kill(pid, "SIGTERM");
  await rm(root, { recursive: true, force: true });
}
