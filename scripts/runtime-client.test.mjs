import assert from "node:assert/strict";
import { chmod, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, before, test } from "node:test";
import { createDdRuntime, formatInspectorEvent, formatWorkerConsole } from "../packages/dd-vite/src/runtime.js";

let root;
let binary;
const clients = new Set();
const pids = new Set();

function isAlive(pid) {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    if (error.code === "ESRCH") return false;
    throw error;
  }
}

before(async () => {
  root = await mkdtemp(join(tmpdir(), "dd-runtime-client-"));
  binary = join(root, "runtime.cjs");
  // A stuck runtime may acknowledge shutdown without exiting and may ignore
  // SIGTERM. Teardown must still reap it, including after a command timeout.
  await writeFile(binary, `#!/usr/bin/env node
const { createInterface } = require("node:readline");
process.on("SIGTERM", () => {});
setInterval(() => {}, 1000);
createInterface({ input: process.stdin }).on("line", line => {
  const command = JSON.parse(line);
  if (command.op === "hang") return;
  if (command.op === "log") {
    process.stdout.write(JSON.stringify({ event: "console", worker: "app", request_id: "req-1", level: "warn", message: "careful\\nnow" }) + "\\n");
  }
  if (command.op === "inspect") {
    process.stdout.write(JSON.stringify({ event: "inspector", address: "127.0.0.1:9339", worker: "app", isolate: 1, devtools: "devtools://devtools/bundled/js_app.html?ws=127.0.0.1:9339/id", websocket: "ws://127.0.0.1:9339/id" }) + "\\n");
  }
  if (command.op === "break_stdin") {
    process.stdin.pause();
    require("node:fs").closeSync(0);
  }
  process.stdout.write(JSON.stringify({ id: command.id, ok: true, result: { pid: process.pid, args: process.argv.slice(2) } }) + "\\n");
});
`);
  await chmod(binary, 0o755);
});

after(async () => {
  try {
    await Promise.all([...clients].map(client => client.close()));
  } finally {
    for (const pid of pids) if (isAlive(pid)) process.kill(pid, "SIGKILL");
    await rm(root, { recursive: true, force: true });
  }
});

function client(options = {}) {
  const runtime = createDdRuntime({ binary, timeoutMs: 2_000, closeTimeoutMs: 20, ...options });
  clients.add(runtime);
  return runtime;
}

test("close waits for process exit and shares concurrent teardown", async () => {
  const runtime = client();
  const { pid } = await runtime.request({ op: "ping" });
  pids.add(pid);
  const closing = runtime.close();
  assert.equal(runtime.close(), closing);
  await assert.rejects(runtime.request({ op: "ping" }), /client is closed/);
  await closing;
  assert.equal(isAlive(pid), false, "close must reap a subprocess that ignores SIGTERM");
});

test("a command timeout rejects other pending work and reaps the replaced process", async () => {
  const runtime = client();
  const { pid: first } = await runtime.request({ op: "ping" });
  pids.add(first);
  const timed = runtime.request({ op: "hang" }, { timeoutMs: 20 });
  const pending = runtime.request({ op: "hang" }, { timeoutMs: 0 });
  await Promise.all([
    assert.rejects(timed, /command timed out after 20ms/),
    assert.rejects(pending, /restarted after command timed out/),
  ]);
  const { pid: second } = await runtime.request({ op: "ping" });
  pids.add(second);
  assert.notEqual(first, second);
  await runtime.close();
  assert.equal(isAlive(first), false, "close must wait for discarded subprocesses too");
  assert.equal(isAlive(second), false);
});

test("a broken subprocess input rejects pending work and remains tracked for teardown", { timeout: 10_000 }, async () => {
  const runtime = client();
  const { pid } = await runtime.request({ op: "ping" });
  pids.add(pid);
  const pending = assert.rejects(runtime.request({ op: "hang" }, { timeoutMs: 0 }), { code: "EPIPE" });
  await runtime.request({ op: "break_stdin" });
  await runtime.request({ op: "ping" }).catch(() => {});
  await pending;
  await runtime.close();
  assert.equal(isAlive(pid), false);
});

test("worker console events reach onConsole instead of the protocol", async () => {
  const events = [];
  const runtime = client({ onConsole: event => events.push(event) });
  await runtime.request({ op: "log" });
  assert.deepEqual(events, [
    { event: "console", worker: "app", request_id: "req-1", level: "warn", message: "careful\nnow" },
  ]);
  assert.equal(formatWorkerConsole("app", "careful\nnow"), "[dd:app] careful\n         now");
});

test("inspect becomes the runtime's --inspect flag and its events reach onInspector", async () => {
  const events = [];
  const runtime = client({ inspect: "127.0.0.1:9339", onInspector: event => events.push(event) });
  const { pid, args } = await runtime.request({ op: "inspect" });
  pids.add(pid);
  assert.ok(args.includes("--inspect=127.0.0.1:9339"), args.join(" "));
  assert.deepEqual(events.map(event => event.worker), ["app"]);
  assert.equal(
    formatInspectorEvent(events[0]),
    "[dd:app] DevTools for isolate 1: devtools://devtools/bundled/js_app.html?ws=127.0.0.1:9339/id",
  );
  assert.match(formatInspectorEvent({ event: "inspector", address: "127.0.0.1:9229" }), /chrome:\/\/inspect/);

  const plain = client({ inspect: true });
  const { pid: plainPid, args: plainArgs } = await plain.request({ op: "ping" });
  pids.add(plainPid);
  assert.ok(plainArgs.includes("--inspect"), plainArgs.join(" "));
  await assert.rejects(client({ inspect: 9229 }).request({ op: "ping" }), /inspect must be true or a "host:port" string/);
});
