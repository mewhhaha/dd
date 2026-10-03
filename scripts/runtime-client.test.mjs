import assert from "node:assert/strict";
import { chmod, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, before, test } from "node:test";
import { createDdRuntime } from "../packages/dd-vite/src/runtime.js";

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
  if (command.op === "break_stdin") {
    process.stdin.pause();
    require("node:fs").closeSync(0);
  }
  process.stdout.write(JSON.stringify({ id: command.id, ok: true, result: { pid: process.pid } }) + "\\n");
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

function client() {
  const runtime = createDdRuntime({ binary, timeoutMs: 2_000, closeTimeoutMs: 20 });
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
