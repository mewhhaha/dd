import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawnSync } from "node:child_process";
import { generateScalingSummary, replaceGeneratedBlock } from "../summarize.mjs";
import { repoRoot } from "./runner-config.mjs";

const modes = [
  "direct-write-memory-wide",
  "atomic-readwrite-memory-wide",
  "atomic-write-memory-wide",
];
const coreModes = modes;
const isolates = [1, 2, 4, 8, 16, 32];

test("valid fixed and core matrices render deterministic tables and provenance", async () => {
  const { fixedPath, corePath } = await writeFixtures();
  const markdown = await generateScalingSummary({ ...defaultOptions(), fixed: fixedPath, core: corePath });

  assert.match(markdown, /Five-sample 16-isolate comparison/);
  assert.match(markdown, /\| Atomic read \+ write \| 8,620 req\/s \| 2,342 req\/s \| 3\.7x \|/);
  assert.match(markdown, /\| 1 \| 6,107 req\/s \| 2,549 req\/s \| 1,565 req\/s \|/);
  assert.match(markdown, /The atomic read\/write path scales by about 3\.4x/);
  assert.match(markdown, /Inputs: core\.json, fixed\.json/);
});

test("allow dirty labels non-release evidence while default rejects it", async () => {
  const { fixedPath, corePath } = await writeFixtures({ dirty: true });
  await assert.rejects(
    () => generateScalingSummary({ ...defaultOptions(), fixed: fixedPath, core: corePath }),
    /dirty benchmark result/,
  );
  const markdown = await generateScalingSummary({
    ...defaultOptions(),
    fixed: fixedPath,
    core: corePath,
    allowDirty: true,
  });
  assert.match(markdown, /non-release evidence from a dirty worktree/);
});

test("mixed hardware metadata is rejected", async () => {
  const { fixedPath, corePath } = await writeFixtures({ coreCpu: 32 });
  await assert.rejects(
    () => generateScalingSummary({ ...defaultOptions(), fixed: fixedPath, core: corePath }),
    /incompatible benchmark metadata: logical_cpus/,
  );
});

test("missing and duplicate rows fail with exact selectors", async () => {
  const { fixedPath, corePath } = await writeFixtures({ omitFixedMode: "atomic-write-memory-wide" });
  await assert.rejects(
    () => generateScalingSummary({ ...defaultOptions(), fixed: fixedPath, core: corePath }),
    /missing benchmark row for atomic-write-memory-wide cross-shard isolates=16 shards=16/,
  );

  const duplicate = await writeFixtures({ duplicateFixed: true });
  await assert.rejects(
    () =>
      generateScalingSummary({
        ...defaultOptions(),
        fixed: duplicate.fixedPath,
        core: duplicate.corePath,
      }),
    /ambiguous benchmark rows/,
  );
});

test("failed samples, unsupported schemas, and malformed metrics are rejected", async () => {
  const failed = await writeFixtures({ failedSample: true });
  await assert.rejects(
    () => generateScalingSummary({ ...defaultOptions(), fixed: failed.fixedPath, core: failed.corePath }),
    /failed sample/,
  );

  const badSchema = await writeFixtures({ schemaVersion: 99 });
  await assert.rejects(
    () =>
      generateScalingSummary({
        ...defaultOptions(),
        fixed: badSchema.fixedPath,
        core: badSchema.corePath,
      }),
    /unsupported schema_version 99/,
  );

  const badMetric = await writeFixtures({ badMetric: true });
  await assert.rejects(
    () =>
      generateScalingSummary({
        ...defaultOptions(),
        fixed: badMetric.fixedPath,
        core: badMetric.corePath,
      }),
    /throughput_rps must be finite/,
  );
});

test("generated markers preserve surrounding prose and --check detects staleness", async () => {
  const { fixedPath, corePath, dir } = await writeFixtures();
  const markdown = await generateScalingSummary({ ...defaultOptions(), fixed: fixedPath, core: corePath });
  const document = `before\n<!-- BEGIN GENERATED: scaling-summary -->\nstale\n<!-- END GENERATED: scaling-summary -->\nafter\n`;
  const updated = replaceGeneratedBlock(document, "scaling-summary", markdown);
  assert(updated.startsWith("before\n<!-- BEGIN GENERATED"));
  assert(updated.endsWith("<!-- END GENERATED: scaling-summary -->\nafter\n"));

  const outPath = join(dir, "SCALING.md");
  await writeFile(outPath, document);
  const stale = spawnSync(
    process.execPath,
    ["benchmarks/summarize.mjs", "--fixed", fixedPath, "--core", corePath, "--out", outPath, "--check"],
    { cwd: repoRoot, encoding: "utf8" },
  );
  assert.equal(stale.status, 1);
  assert.match(stale.stderr, /is stale/);

  await writeFile(outPath, updated);
  const fresh = spawnSync(
    process.execPath,
    ["benchmarks/summarize.mjs", "--fixed", fixedPath, "--core", corePath, "--out", outPath, "--check"],
    { cwd: repoRoot, encoding: "utf8" },
  );
  assert.equal(fresh.status, 0, fresh.stderr);
});

function defaultOptions() {
  return {
    allowDirty: false,
    fixedIsolates: 16,
    fixedShards: 16,
    coreShards: 16,
    coreIsolates: isolates,
  };
}

async function writeFixtures(options = {}) {
  const dir = await mkdtemp(join(tmpdir(), "dd-summary-"));
  const fixedPath = join(dir, "fixed.json");
  const corePath = join(dir, "core.json");
  const fixed = resultFixture({ sampleCount: 5, dirty: options.dirty, schemaVersion: options.schemaVersion });
  const core = resultFixture({
    sampleCount: 1,
    dirty: options.dirty,
    logicalCpus: options.coreCpu ?? 16,
    schemaVersion: options.schemaVersion,
  });

  for (const mode of modes) {
    if (mode === options.omitFixedMode) {
      continue;
    }
    fixed.configs.push(configFixture(mode, "cross-shard", 16, throughput(mode, "cross-shard", 16), options));
    fixed.configs.push(configFixture(mode, "same-shard", 16, throughput(mode, "same-shard", 16), options));
  }
  if (options.duplicateFixed) {
    fixed.configs.push(configFixture("direct-write-memory-wide", "cross-shard", 16, 13_206, options));
  }
  for (const mode of coreModes) {
    for (const keys of ["cross-shard", "same-shard"]) {
      for (const isolate of isolates) {
        core.configs.push(configFixture(mode, keys, isolate, throughput(mode, keys, isolate), options));
      }
    }
  }

  await writeFile(fixedPath, JSON.stringify(fixed, null, 2));
  await writeFile(corePath, JSON.stringify(core, null, 2));
  return { dir, fixedPath, corePath };
}

function resultFixture({ sampleCount, dirty = false, logicalCpus = 16, schemaVersion = 1 }) {
  return {
    schema_version: schemaVersion,
    started_at: "2026-01-02T03:04:05.000Z",
    metadata: {
      git_commit: "0123456789abcdef0123456789abcdef01234567",
      git_dirty: dirty,
      logical_cpus: logicalCpus,
      os: "Linux test",
      rustc: "rustc test",
      cargo: "cargo test",
    },
    sample_count: sampleCount,
    complete: true,
    configs: [],
  };
}

function configFixture(mode, keys, isolate, throughputRps, options) {
  return {
    name: `scaling-atomic-memory-matrix.sh::isolates=${isolate},shards=16,keys=${keys},mode=${mode}`,
    env: {
      DD_BENCH_MODE: mode,
      DD_BENCH_MEMORY_KEY_MODE: keys,
      DD_BENCH_MIN_ISOLATES: String(isolate),
      DD_BENCH_MAX_ISOLATES: String(isolate),
      DD_BENCH_MEMORY_NAMESPACE_SHARDS: "16",
    },
    samples: [{ sample: 1, status: options.failedSample ? 1 : 0, signal: null }],
    summaries: [
      {
        workload: `memory-${mode}`,
        samples: 1,
        requests: 4096,
        concurrency: 128,
        isolate_min: isolate,
        isolate_max: isolate,
        throughput_rps: options.badMetric ? "not-a-number" : throughputRps,
        mean_ms: 10,
        p50_ms: 5,
        p95_ms: p95(mode, keys),
        p99_ms: p99(mode, keys),
      },
    ],
  };
}

function throughput(mode, keys, isolate) {
  const data = {
    "direct-write-memory-wide": {
      "cross-shard": { 1: 6_107, 2: 11_196, 4: 17_540, 8: 18_723, 16: 13_206, 32: 15_843 },
      "same-shard": { 1: 6_194, 2: 6_049, 4: 5_991, 8: 4_472, 16: 4_237, 32: 5_137 },
    },
    "atomic-readwrite-memory-wide": {
      "cross-shard": { 1: 2_549, 2: 4_307, 4: 6_413, 8: 8_405, 16: 8_620, 32: 9_123 },
      "same-shard": { 1: 2_828, 2: 2_581, 4: 2_387, 8: 2_050, 16: 2_342, 32: 2_359 },
    },
    "atomic-write-memory-wide": {
      "cross-shard": { 1: 1_565, 2: 2_321, 4: 2_999, 8: 3_918, 16: 3_521, 32: 4_222 },
      "same-shard": { 1: 1_256, 2: 921, 4: 930, 8: 782, 16: 715, 32: 852 },
    },
  };
  return data[mode][keys][isolate];
}

function p95(mode, keys) {
  if (mode === "direct-write-memory-wide") {
    return keys === "cross-shard" ? 23.02 : 104.04;
  }
  if (mode === "atomic-readwrite-memory-wide") {
    return keys === "cross-shard" ? 20.7 : 113.74;
  }
  return keys === "cross-shard" ? 41.38 : 563.94;
}

function p99(mode, keys) {
  if (mode === "direct-write-memory-wide") {
    return keys === "cross-shard" ? 34.06 : 329.39;
  }
  if (mode === "atomic-readwrite-memory-wide") {
    return keys === "cross-shard" ? 101.83 : 179.16;
  }
  return keys === "cross-shard" ? 121.03 : 820.33;
}
