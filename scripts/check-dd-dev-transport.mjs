import assert from "node:assert/strict";
import { once } from "node:events";
import { readFile } from "node:fs/promises";
import { createServer } from "node:http";
import { setTimeout as delay } from "node:timers/promises";
import { createDdRuntime } from "../packages/dd-vite/src/runtime.js";
import {
  handleDdWebSocketUpgrade,
  nodeRequestToWorkerRequest,
  writeNodeResponse,
} from "../packages/dd-vite/src/vite/dev.js";

const runtime = createDdRuntime();
const name = "transport-smoke";
const source = `
export default {
  async fetch(request, env) {
    const path = new URL(request.url).pathname;
    if (path === "/echo") return new Response(request.body);
    if (path === "/endless") {
      return new Response(new ReadableStream({
        async pull(controller) {
          await new Promise(resolve => setTimeout(resolve, 10));
          controller.enqueue(new Uint8Array(65536));
        },
      }));
    }
    if (path === "/ws") {
      return env.SOCKETS.get("echo").atomic(tx => tx.accept(request).response);
    }
    return Response.json({ url: request.url, host: request.headers.get("host"), transportHeader: request.headers.get("x-dd-dev-request-url") });
  },
  async wake(event) {
    if (event.type !== "socketmessage") return;
    await event.stub.atomic(tx => {
      if (event.data === "close") tx.sockets.close(event.handle, 1000, "finished");
      else tx.sockets.send(event.handle, event.data);
    });
  },
};`;

const server = createServer(async (req, res) => {
  try {
    const request = nodeRequestToWorkerRequest(req, req.url, "/", name, res);
    await writeNodeResponse(res, await runtime.fetch(name, request));
  } catch (error) {
    if (!res.destroyed) res.destroy(error);
  }
});
server.on("upgrade", (req, socket, head) => {
  void handleDdWebSocketUpgrade(req, socket, head, {
    mount: "/",
    viteBase: "/",
    ensureDeployed: async () => {},
    runtime: () => runtime,
    effectiveWorkerName: async () => name,
  });
});
const timeout = setTimeout(() => {
  throw new Error("dev transport verification exceeded 30 seconds");
}, 30_000);

try {
  const deployed = await runtime.deploy(name, source, { bindings: [{ type: "memory", binding: "SOCKETS" }] });
  assert.equal(runtime.workerUrl(name), deployed.url);
  const originalUrl = "https://original.example:8443//path?q=one%20two";
  const original = await runtime.fetch(name, originalUrl);
  assert.deepEqual(await original.json(), { url: originalUrl, host: "original.example:8443", transportHeader: null });
  await runtime.deploy("test-worker", source);
  const defaultWorker = await runtime.fetch(originalUrl, { headers: { "x-test": "default worker" } });
  assert.equal((await defaultWorker.json()).url, originalUrl);
  const replacement = await runtime.deploy(name, source, { bindings: [{ type: "memory", binding: "SOCKETS" }] });
  assert.equal(replacement.url, deployed.url, "hot redeployment must preserve the worker listener");

  console.log("dev transport: URL and redeploy checks passed");
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const base = `http://127.0.0.1:${server.address().port}`;
  let upload;
  const body = new ReadableStream({ start(controller) { upload = controller; } });
  upload.enqueue(new TextEncoder().encode("first\n"));
  console.log("dev transport: starting streamed upload");
  const response = await fetch(`${base}/echo`, { method: "POST", body, duplex: "half" });
  console.log("dev transport: upload response headers", response.status);
  assert.equal(response.status, 200);
  const reader = response.body.getReader();
  const first = await reader.read();
  assert.equal(new TextDecoder().decode(first.value), "first\n", "upload and response must stream before upload completion");
  upload.enqueue(new TextEncoder().encode("last\n"));
  upload.close();
  let rest = "";
  for (;;) {
    const chunk = await reader.read();
    if (chunk.done) break;
    rest += new TextDecoder().decode(chunk.value);
  }
  assert.equal(rest, "last\n");

  console.log("dev transport: upload and response streaming passed");
  const endless = await fetch(`${base}/endless`);
  const endlessReader = endless.body.getReader();
  assert.equal((await endlessReader.read()).done, false);
  await endlessReader.cancel();
  for (let attempt = 0; ; attempt += 1) {
    const { stats } = await runtime.stats(name);
    if (stats.inflight_total === 0) break;
    assert.ok(attempt < 100, "browser response cancellation must release native invocation");
    await delay(20);
  }

  const controller = new AbortController();
  const abortBody = new ReadableStream({ start(stream) { stream.enqueue(new Uint8Array([1])); } });
  const aborted = await fetch(`${base}/echo`, { method: "POST", body: abortBody, duplex: "half", signal: controller.signal });
  const abortedReader = aborted.body.getReader();
  await abortedReader.read();
  controller.abort();
  await assert.rejects(abortedReader.read(), { name: "AbortError" });

  console.log("dev transport: cancellation checks passed");
  for (const origin of [deployed.url, base]) {
    const socket = new WebSocket(`${origin.replace(/^http/, "ws")}/ws`);
    socket.binaryType = "arraybuffer";
    socket.addEventListener("error", (event) => { throw new Error(`WebSocket ${origin} failed: ${event.message ?? event.type}`); });
    console.log("dev transport: opening WebSocket", origin);
    await once(socket, "open");
    console.log("dev transport: WebSocket connected");
    const text = once(socket, "message");
    socket.send("hello");
    assert.equal((await text)[0].data, "hello");
    console.log("dev transport: WebSocket text echoed");
    const binary = once(socket, "message");
    socket.send(new Uint8Array([0, 127, 255]));
    assert.deepEqual(new Uint8Array((await binary)[0].data), new Uint8Array([0, 127, 255]));
    const closed = once(socket, "close");
    socket.send("close");
    const [close] = await closed;
    assert.equal(close.code, 1000);
    assert.equal(close.reason, "finished");
  }
  const chatSource = await readFile(new URL("../examples/chat-worker/src/worker.js", import.meta.url), "utf8");
  const chat = await runtime.deploy("chat-smoke", chatSource, { bindings: [{ type: "memory", binding: "CHAT_ROOM" }] });
  const chatSocket = new WebSocket(`${chat.url.replace(/^http/, "ws")}/rooms/test/ws?username=Visitor&participant=smoke`);
  const chatMessages = [];
  chatSocket.addEventListener("message", (event) => chatMessages.push(event.data));
  await once(chatSocket, "open");
  chatSocket.send(JSON.stringify({ type: "ready" }));
  chatSocket.send(JSON.stringify({ type: "message", text: "native chat smoke" }));
  for (let attempt = 0; ; attempt += 1) {
    const state = await (await runtime.fetch("chat-smoke", "http://chat/rooms/test/state")).json();
    if (state.messages.some((message) => message.text === "native chat smoke")
      && chatMessages.some((message) => message.includes("native chat smoke"))) break;
    assert.ok(attempt < 100, "chat message must commit and reach its WebSocket client");
    await delay(20);
  }
  const chatClosed = once(chatSocket, "close");
  chatSocket.close(1000, "finished");
  await chatClosed;
  console.log("dev transport: original URLs, redeploy, streaming upload/response, cancellation, native/proxied WebSockets, and chat transactions passed");
} finally {
  clearTimeout(timeout);
  server.closeAllConnections();
  server.close();
  await runtime.close();
}
