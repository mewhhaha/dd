import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, readFile, writeFile } from "node:fs/promises";
import { existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { spawnSync } from "node:child_process";
import {
  configsDir,
  discoverConfigs,
  expandConfigs,
  expandMatrixVariants,
  parseArgs,
  parseConfigEnv,
  planRun,
  repoRoot,
  renderPlan,
  validateRunBudget,
} from "./runner-config.mjs";

test("default discovery skips opt-in matrix configs", async () => {
  const { configs, skippedOptIn } = await discoverConfigs([]);
  assert(!configs.some((config) => config.name === "scaling-atomic-memory-matrix.sh"));
  assert.deepEqual(skippedOptIn, ["scaling-atomic-memory-matrix.sh"]);
});

test("explicit selection includes and expands opt-in atomic matrix", async () => {
  const { configs } = await discoverConfigs(["scaling-atomic-memory-matrix.sh"]);
  const expanded = await expandConfigs(configs, {});
  assert.equal(expanded.length, 378);
});

test("five samples of atomic matrix produce 1890 planned runs", async () => {
  const { configs } = await discoverConfigs(["scaling-atomic-memory-matrix.sh"]);
  const expanded = await expandConfigs(configs, {});
  const plan = planRun(expanded, parseArgs(["--samples", "5"]), "out.json");
  assert.equal(plan.totalRuns, 1890);
  assert.throws(() => validateRunBudget(plan, parseArgs(["--samples", "5"])), /exceeding/);
  assert.doesNotThrow(() =>
    validateRunBudget(plan, parseArgs(["--samples", "5", "--allow-large-run"])),
  );
});

test("plan output lists variants and output without spawning", async () => {
  const { configs } = await discoverConfigs(["focused-memory-write.sh"]);
  const expanded = await expandConfigs(configs, {});
  const plan = planRun(expanded, parseArgs(["--plan", "--samples", "2"]), "planned.json");
  assert.equal(plan.totalRuns, 2);
  const rendered = renderPlan(plan);
  assert.match(rendered, /Variants: 1/);
  assert.match(rendered, /Output: planned\.json/);
  assert.match(rendered, /focused-memory-write\.sh/);
});

test("environment overrides narrow dimensions and deduplicate values", async () => {
  const script = await readFile(join(configsDir, "scaling-atomic-memory-matrix.sh"), "utf8");
  const env = parseConfigEnv(script);
  const variants = expandMatrixVariants(env, {
    DD_BENCH_MATRIX_ISOLATES: "1 1 2",
    DD_BENCH_MATRIX_MEMORY_NAMESPACE_SHARDS: "16",
    DD_BENCH_MATRIX_KEY_MODES: "same-shard same-shard",
    DD_BENCH_MATRIX_MODES: "atomic-readwrite-memory-wide",
  });
  assert.equal(variants.length, 2);
  assert.deepEqual(
    variants.map((variant) => variant.envOverrides.DD_BENCH_MAX_ISOLATES),
    ["1", "2"],
  );
});

test("invalid dimensions fail before execution", () => {
  assert.throws(
    () =>
      expandMatrixVariants(
        {
          DD_BENCH_MATRIX_ISOLATES: "0",
        },
        {},
      ),
    /positive integer/,
  );
  assert.throws(
    () =>
      expandMatrixVariants(
        {
          DD_BENCH_MATRIX_KEY_MODES: "unknown",
        },
        {},
      ),
    /unsupported value/,
  );
  assert.throws(
    () =>
      expandMatrixVariants(
        {
          DD_BENCH_MATRIX_OPT_IN: "1",
          DD_BENCH_MATRIX_MODES: "",
        },
        {},
      ),
    /must not be empty/,
  );
  assert.throws(
    () =>
      expandMatrixVariants(
        {
          DD_BENCH_MATRIX_MODES: "direct-write-memory-wide",
        },
        {
          DD_BENCH_MODE: "direct-read-memory-wide",
        },
      ),
    /conflicts/,
  );
});

test("non-matrix configs remain one variant", async () => {
  const { configs } = await discoverConfigs(["focused-memory-write.sh"]);
  const expanded = await expandConfigs(configs, {});
  assert.equal(expanded.length, 1);
  assert.equal(expanded[0].name, "focused-memory-write.sh");
});

test("unknown selected configs fail clearly", async () => {
  await assert.rejects(() => discoverConfigs(["missing.sh"]), /unknown benchmark config: missing\.sh/);
});

test("--plan does not spawn fixture config scripts", async () => {
  const fixtureDir = await mkdtemp(join(tmpdir(), "dd-bench-plan-"));
  const markerPath = join(fixtureDir, "spawned");
  await writeFile(
    join(fixtureDir, "touch-marker.sh"),
    `#!/usr/bin/env bash\nprintf touched > ${markerPath}\n`,
    { mode: 0o755 },
  );

  const result = spawnSync(
    process.execPath,
    ["benchmarks/run.mjs", "--plan", "--config", "touch-marker.sh", "--out", "benchmarks/results/plan-test.json"],
    {
      cwd: repoRoot,
      env: {
        ...process.env,
        DD_BENCH_CONFIGS_DIR: fixtureDir,
      },
      encoding: "utf8",
    },
  );

  assert.equal(result.status, 0, result.stderr);
  assert.match(result.stdout, /Variants: 1/);
  assert.equal(existsSync(markerPath), false);
});

test("failing samples write partial failure metadata", async () => {
  const fixtureDir = await mkdtemp(join(tmpdir(), "dd-bench-fail-"));
  const outputPath = join(fixtureDir, "result.json");
  await writeFile(
    join(fixtureDir, "fail.sh"),
    "#!/usr/bin/env bash\nprintf 'fixture output\\n'\nexit 7\n",
    { mode: 0o755 },
  );

  const result = spawnSync(
    process.execPath,
    ["benchmarks/run.mjs", "--samples", "2", "--config", "fail.sh", "--out", outputPath],
    {
      cwd: repoRoot,
      env: {
        ...process.env,
        DD_BENCH_CONFIGS_DIR: fixtureDir,
      },
      encoding: "utf8",
    },
  );

  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /partial output written/);
  const run = JSON.parse(await readFile(resolve(repoRoot, outputPath), "utf8"));
  assert.equal(run.complete, false);
  assert.deepEqual(run.failure, {
    config: "fail.sh",
    sample: 1,
    status: 7,
    signal: null,
  });
  assert.equal(run.configs[0].samples.length, 1);
});

test("--keep-going preserves failures while collecting later samples", async () => {
  const fixtureDir = await mkdtemp(join(tmpdir(), "dd-bench-keep-"));
  const outputPath = join(fixtureDir, "result.json");
  await writeFile(
    join(fixtureDir, "flaky.sh"),
    "#!/usr/bin/env bash\nif [ \"$BENCH_SAMPLE\" = \"1\" ]; then exit 9; fi\nprintf 'fixture requests=1 concurrency=1 throughput=2 req/s mean=3ms p50=3ms p95=4ms p99=5ms\\n'\n",
    { mode: 0o755 },
  );

  const result = spawnSync(
    process.execPath,
    [
      "benchmarks/run.mjs",
      "--keep-going",
      "--samples",
      "2",
      "--config",
      "flaky.sh",
      "--out",
      outputPath,
    ],
    {
      cwd: repoRoot,
      env: {
        ...process.env,
        DD_BENCH_CONFIGS_DIR: fixtureDir,
      },
      encoding: "utf8",
    },
  );

  assert.equal(result.status, 0, result.stderr);
  const run = JSON.parse(await readFile(resolve(repoRoot, outputPath), "utf8"));
  assert.equal(run.complete, false);
  assert.deepEqual(run.failure, {
    config: "flaky.sh",
    sample: 1,
    status: 9,
    signal: null,
  });
  assert.equal(run.failures.length, 1);
  assert.equal(run.configs[0].samples.length, 2);
});
