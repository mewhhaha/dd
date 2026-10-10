import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { test } from "node:test";
import { isArrayBuffer, isDataView, isMap, isSet } from "node:util/types";
import { deserialize, serialize } from "node:v8";
import { runInThisContext } from "node:vm";

// The unit runs inside the execute-worker bundle, where it names primordials,
// the captured web classes and their uncurried methods (web/init.js). Here
// those come from the runtime's own primordials and Node's web classes.
const read = (path) => readFile(new URL(`../crates/runtime/js/${path}`, import.meta.url), "utf8");
runInThisContext(await read("core/00_primordials.js"), { filename: "00_primordials.js" });
const { primordials } = globalThis.__bootstrap;
delete globalThis.__bootstrap;
const scope = { Request, Response };
for (const name of Reflect.ownKeys(primordials)) scope[name] = primordials[name];
for (const [name, prototype] of [["Headers", Headers.prototype], ["Request", Request.prototype], ["Response", Response.prototype]]) {
  scope[`${name}Prototype`] = prototype;
  for (const key of Reflect.ownKeys(prototype)) {
    if (typeof key !== "string" || key === "constructor") continue;
    const suffix = `${key[0].toUpperCase()}${key.slice(1)}`;
    const { get, value } = Object.getOwnPropertyDescriptor(prototype, key);
    if (get) scope[`${name}PrototypeGet${suffix}`] = primordials.uncurryThis(get);
    if (typeof value === "function") scope[`${name}Prototype${suffix}`] = primordials.uncurryThis(value);
  }
}
scope.headerPairs = (headers) => [...headers];
scope.appendHeaderPairs = (headers, pairs) => {
  for (const [name, value] of pairs) headers.append(name, value);
  return headers;
};
scope.core = {
  encode: (text) => new TextEncoder().encode(text),
  serialize,
  deserialize: (bytes) => deserialize(Buffer.from(bytes)),
  isArrayBuffer,
  isDataView,
  isMap,
  isSet,
};
const names = Object.keys(scope).filter((name) => /^[A-Za-z_$][\w$]*$/.test(name));
const { encodeMemoryCommandResult, decodeMemoryCommandResult } = runInThisContext(
  `({ ${names.join(", ")} }) => {\n${await read("execute_worker/memory_results.js")}\nreturn { encodeMemoryCommandResult, decodeMemoryCommandResult }; }`,
  { filename: "memory_results.js" },
)(scope);

test("new results preserve user properties that match old web value records", async () => {
  const plain = { __dd_rpc_type: "response", status: 200, headers: [], body: [] };
  const response = new Response("body", { status: 201, statusText: "Created" });
  const value = { plain, response, alias: response, map: new Map([[plain, response]]) };
  value.self = value;
  const replay = decodeMemoryCommandResult(await encodeMemoryCommandResult(value));
  assert.deepEqual(replay.plain, plain);
  assert.equal(replay.plain instanceof Response, false);
  assert(replay.response instanceof Response);
  assert.equal(replay.response.statusText, "Created");
  assert.equal(await replay.response.text(), "body");
  assert.equal(replay.alias, replay.response);
  assert.equal(replay.map.get(replay.plain), replay.response);
  assert.equal(replay.self, replay);
});

test("existing unversioned results retain user markers and graph references", () => {
  const value = { __dd_rpc_type: "response", value: 42, map: new Map() };
  value.self = value;
  value.map.set(value, value);
  const replay = decodeMemoryCommandResult(serialize(value));
  assert.equal(replay instanceof Response, false);
  assert.equal(replay.__dd_rpc_type, "response");
  assert.equal(replay.value, 42);
  assert.equal(replay.self, replay);
  assert.equal(replay.map.get(replay), replay);
});

test("existing unversioned Request and Response records remain readable", async () => {
  const response = decodeMemoryCommandResult(serialize({
    __dd_rpc_type: "response", status: 201, headers: [["x-result", "stored"]], body: new TextEncoder().encode("response body"),
  }));
  assert(response instanceof Response);
  assert.equal(response.status, 201);
  assert.equal(response.headers.get("x-result"), "stored");
  assert.equal(await response.text(), "response body");
  const request = decodeMemoryCommandResult(serialize({
    __dd_rpc_type: "request", url: "https://fixture.test/", method: "POST", headers: [], body: new TextEncoder().encode("request body"),
  }));
  assert(request instanceof Request);
  assert.equal(request.method, "POST");
  assert.equal(await request.text(), "request body");
});

test("unknown stored result versions fail explicitly", async () => {
  const bytes = await encodeMemoryCommandResult({ value: 42 });
  bytes[4] = 99;
  assert.throws(() => decodeMemoryCommandResult(bytes), /unsupported stored memory command result version/);
});
