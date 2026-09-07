import assert from "node:assert/strict";
import { readFile, readdir } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { join } from "node:path";
import { buildGeneratedDeploymentConfig, normalizeRuntimeConfig } from "../packages/dd-vite/src/vite/config.js";
import { parseConfigEnv } from "../benchmarks/lib/runner-config.mjs";

const root = fileURLToPath(new URL("../", import.meta.url));
const cases = JSON.parse(await readFile(join(root, "fixtures/config/deploy-config.json"), "utf8"));
for (const scenario of cases) {
  if (scenario.reject) {
    assert.throws(() => normalizeRuntimeConfig(scenario.input, scenario.name), undefined, scenario.name);
  } else {
    assert.deepEqual(normalizeRuntimeConfig(scenario.input, scenario.name), scenario.expected, scenario.name);
    for (const [source, config] of Object.entries({
      nested: { base: { config: scenario.input }, options: {} },
      topLevel: { base: scenario.input, options: {} },
      plugin: { base: {}, options: { config: scenario.input } },
    })) {
      const deployment = await buildGeneratedDeploymentConfig({
        ...config,
        workerName: "contract",
        workerFile: "worker.js",
        configFile: "dd.deploy.json",
        assetsDir: false,
      });
      assert.deepEqual(normalizeRuntimeConfig(deployment.config), scenario.expected, `${source}: ${scenario.name}`);
    }
  }
}

const retired = /(?:\b(?:DdDynamicWorker\w*|DynamicWorker\w*|DynamicHostRpc\w*|HostRpc\w*|RpcTarget|WebTransport(?:Session)?|DynamicDeployRequest|DynamicDeployResponse)\b|\bop_dynamic_\w+|\bop_memory_transport_\w+|type:\s*["']dynamic["']|\/v1\/dynamic\/deploy)/;
const paths = [
  "packages/dd-vite/src/index.d.ts",
  "crates/common/src/lib.rs",
  "crates/cli/src/main.rs",
  "crates/api/src/handlers/routing.rs",
  "crates/runtime/src/ops.rs",
  "crates/runtime/js/bootstrap.js",
];
for (const name of await readdir(join(root, "crates/runtime/js/execute_worker"))) {
  if (name.endsWith(".js")) paths.push(`crates/runtime/js/execute_worker/${name}`);
}
for (const path of paths) {
  const match = retired.exec(await readFile(join(root, path), "utf8"));
  assert.equal(match?.[0], undefined, `${path} exposes a retired runtime API`);
}
const schema = JSON.parse(await readFile(join(root, "schema/dd.schema.json"), "utf8"));
assert.deepEqual(schema.$defs.binding.oneOf.map((branch) => branch.properties.type.const), ["kv", "memory", "service"]);
for (const path of ["Cargo.toml", "Cargo.lock", "deploy/fly/Dockerfile"]) {
  assert.doesNotMatch(await readFile(join(root, path), "utf8"), /\b(?:capnp|capnpc|capnproto|wasmtime(?:-[\w-]+)?)\b/, path);
}
const obsoleteConfig = /\b(?:db_url|memory_namespace_shards|memory_databases_dir|memory_connections|memory_database_cache_max_entries|cache_db_url|memory_rpc|DD_BENCH_(?:MATRIX_)?MEMORY_NAMESPACE_SHARDS|DD_(?:BENCH_)?MEMORY_DB_\w+)\b/;
for (const path of [
  ...paths,
  "crates/runtime/src/service.rs",
  "crates/runtime/src/service/model.rs",
  "crates/api/src/main.rs",
  "README.md",
  "docs/development.md",
  "benchmarks/README.md",
  "benchmarks/SCALING.md",
  "deploy/fly/fly.toml",
  "deploy/fly/README.md",
  ".github/workflows/benchmarks.yml",
]) {
  const match = obsoleteConfig.exec(await readFile(join(root, path), "utf8"));
  assert.equal(match?.[0], undefined, `${path} exposes obsolete runtime configuration`);
}
const benchmarkSource = await readFile(join(root, "crates/runtime/src/bin/bench_memory_storage.rs"), "utf8");
const stateSource = await readFile(join(root, "crates/storage/src/state.rs"), "utf8");
const shardCount = Number(/pub const STATE_SHARDS: usize = (\d+);/.exec(stateSource)?.[1]);
assert(Number.isInteger(shardCount), "state storage must declare its fixed shard count");
for (const path of ["benchmarks/README.md", "benchmarks/SCALING.md", "benchmarks/summarize.mjs"]) {
  const contents = await readFile(join(root, path), "utf8");
  const documentedCount = Number(/State storage (?:uses|has) (\d+) fixed shards/.exec(contents)?.[1]);
  assert.equal(documentedCount, shardCount, `${path} must match STATE_SHARDS`);
}
const benchmarkModes = new Set([...benchmarkSource.matchAll(/mode: "([^"]+)"/g)].map((match) => match[1]));
benchmarkModes.add("fast-fetch-instant-text");
for (const name of await readdir(join(root, "benchmarks/configs"))) {
  if (!name.endsWith(".sh")) continue;
  const script = await readFile(join(root, "benchmarks/configs", name), "utf8");
  assert.doesNotMatch(script, obsoleteConfig, name);
  const config = parseConfigEnv(script);
  for (const mode of [config.DD_BENCH_MODE, config.DD_BENCH_MATRIX_MODES].filter(Boolean).flatMap((value) => value.split(/\s+/))) {
    if (!mode.startsWith("$")) assert(benchmarkModes.has(mode), `${name} selects unavailable native workload ${mode}`);
  }
}
const retiredMemory = /\.t?var\s*\(|\b(?:DdMemoryVariable|DdMemoryAtomicOptions)\b|new WebSocket\((?:handle|event\.handle)\)|\b(?:transportstream|transportdatagram|socketopen)\b/;
for (const path of ["README.md", "docs/architecture.md", "docs/development.md", "packages/dd-vite/README.md"]) {
  assert.doesNotMatch(await readFile(join(root, path), "utf8"), retiredMemory, path);
}
for (const name of await readdir(join(root, "examples"))) {
  if (!name.endsWith(".js")) continue;
  assert.doesNotMatch(await readFile(join(root, "examples", name), "utf8"), retiredMemory, name);
}
const metricSource = await readFile(join(root, "crates/storage/src/memory.rs"), "utf8");
assert.doesNotMatch(metricSource, /\b(?:JsHydrateKeys|JsCacheHit|JsCacheMiss|JsCacheStale|OpRead|OpVersionIfNewer|StoreReaderPoolWait|StoreWriterLaneWait|RuntimeAtomic\w+)\b/);
console.log(`Runtime contract passed: ${cases.length} shared config cases, ${paths.length} public surface files, and source coherence checks.`);
