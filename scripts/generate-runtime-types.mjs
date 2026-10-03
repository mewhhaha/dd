import assert from "node:assert/strict";
import { readFile, writeFile } from "node:fs/promises";

const root = new URL("../", import.meta.url);
const structs = [
  ["crates/runtime/src/service/facade.rs", "WorkerStats", "DdRuntimeWorkerStats"],
  ["crates/runtime/src/service/facade.rs", "RuntimeWorkerStatus", "DdRuntimeWorkerStatus"],
  ["crates/runtime/src/service/facade.rs", "RuntimeRestoreFailure", "DdRuntimeRestoreFailure"],
  ["crates/runtime/src/service/facade.rs", "RuntimeReadiness", "DdRuntimeReadiness"],
  ["crates/storage/src/state.rs", "StateTimingSnapshot", "DdStateStorageTimings"],
  ["crates/storage/src/state.rs", "StatePerformanceSnapshot", "DdStateStorageStats"],
  ["crates/runtime/src/service/facade.rs", "RuntimeAdminSnapshot", "DdRuntimeAdminSnapshot"],
  ["crates/runtime/src/service/facade.rs", "RuntimeCheckpointResult", "DdRuntimeCheckpointResult"],
];
const names = new Map(structs.map(([, name, type]) => [name, type]));
const sources = new Map(await Promise.all([...new Set(structs.map(([path]) => path))].map(async path => [
  path, await readFile(new URL(path, root), "utf8"),
])));

function typeOf(rust, optional = false) {
  const option = /^Option<(.+)>$/.exec(rust);
  if (option) return typeOf(option[1]) + (optional ? "" : " | null");
  const vector = /^Vec<(.+)>$/.exec(rust);
  if (vector) return `Array<${typeOf(vector[1])}>`;
  if (/^(?:u(?:8|16|32|64|128)|i(?:8|16|32|64|128)|usize|isize|f32|f64)$/.test(rust)) return "number";
  if (rust === "bool") return "boolean";
  if (rust === "String") return "string";
  const name = names.get(rust.split("::").at(-1));
  assert(name, `Unsupported serialized Rust type: ${rust}; extend the contract generator`);
  return name;
}

function declaration(path, name, type) {
  const match = new RegExp(`^((?:#\\[[^\\n]+\\]\\n)+)pub struct ${name} \\{([\\s\\S]*?)^\\}`, "m").exec(sources.get(path));
  assert(match, `${path} must declare ${name} with explicit serialization attributes`);
  const [, attributes, body] = match;
  assert(/\bSerialize\b/.test(attributes), `${name} must derive Serialize`);
  for (const attribute of attributes.trim().split("\n")) {
    assert(/^#\[derive\([\w, ]+\)\]$/.test(attribute), `${name}: unsupported struct attribute ${attribute}; extend the contract generator`);
  }
  const fields = [];
  const parents = [];
  let attribute;
  for (const raw of body.split("\n")) {
    const line = raw.trim();
    if (!line || line.startsWith("///")) continue;
    if (line.startsWith("#[")) {
      assert(["#[serde(flatten)]", '#[serde(skip_serializing_if = "Option::is_none")]'].includes(line), `${name}: unsupported serialization attribute ${line}`);
      assert(!attribute, `${name}: multiple field attributes require explicit support`);
      attribute = line;
      continue;
    }
    const field = /^pub (\w+): (.+),$/.exec(line);
    assert(field, `${name}: unsupported field declaration ${line}`);
    const [, key, rust] = field;
    if (attribute === "#[serde(flatten)]") {
      parents.push(typeOf(rust));
    } else {
      const optional = attribute != null;
      assert(!optional || rust.startsWith("Option<"), `${name}.${key}: only optional fields may skip None`);
      fields.push(`  ${key}${optional ? "?" : ""}: ${typeOf(rust, optional)};`);
    }
    attribute = undefined;
  }
  assert(!attribute, `${name}: trailing attribute has no field`);
  return `export interface ${type}${parents.length ? ` extends ${parents.join(", ")}` : ""} {\n${fields.join("\n")}\n}`;
}

const generated = "// Generated from serialized Rust structs. Run node scripts/generate-runtime-types.mjs.\n\n" +
  structs.map(args => declaration(...args)).join("\n\n") + "\n";
const output = new URL("packages/dd-vite/src/runtime-contract.d.ts", root);
if (process.argv.includes("--check")) {
  assert.equal(await readFile(output, "utf8"), generated, "Runtime response types have drifted; run node scripts/generate-runtime-types.mjs");
  console.log(`Runtime response types match ${structs.length} serialized Rust structs.`);
} else {
  await writeFile(output, generated);
}
