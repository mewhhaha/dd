#!/usr/bin/env node
import { readFile, writeFile } from "node:fs/promises";
import { basename, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { inspect } from "node:util";
import {
  compatibleMetadata,
  formatDecimal,
  formatInteger,
  formatRatio,
  loadBenchmarkResult,
  modeLabels,
  normalizeBenchmarkResult,
  selectOne,
} from "./lib/results.mjs";

const generatedBlock = "scaling-summary";
const defaultCoreIsolates = [1, 2, 4, 8, 16, 32];
const fixedModes = [
  "direct-write-memory-wide",
  "atomic-readwrite-memory-wide",
  "atomic-write-memory-wide",
];
const coreModes = [
  "direct-write-memory-wide",
  "atomic-readwrite-memory-wide",
  "atomic-write-memory-wide",
];

if (isMain()) {
  const options = parseArgs(process.argv.slice(2));
  if (options.help) {
    printHelp();
    process.exit(0);
  }

  const markdown = await generateScalingSummary(options);
  if (options.out) {
    const outPath = resolve(options.out);
    const current = await readFile(outPath, "utf8");
    const updated = replaceGeneratedBlock(current, generatedBlock, markdown);
    if (options.check) {
      if (updated !== current) {
        console.error(`${options.out} is stale; run benchmarks/summarize.mjs without --check`);
        process.exit(1);
      }
    } else {
      await writeFile(outPath, updated);
    }
  } else {
    process.stdout.write(markdown);
  }
}

export async function generateScalingSummary(options) {
  const fixedRows = await loadRows(options.fixed, options);
  const coreRows = options.core === options.fixed ? fixedRows : await loadRows(options.core, options);
  const fixed = selectFixedRows(fixedRows, options);
  const core = selectCoreRows(coreRows, options);
  const selectedRows = [...Object.values(fixed).flat(), ...coreSelectionRows(core)];
  const metadata = compatibleMetadata(selectedRows);
  const dirtyLabel = metadata.git_dirty ? " (non-release evidence from a dirty worktree)" : "";

  return [
    `## Generated benchmark evidence${dirtyLabel}`,
    "",
    "Generated from local benchmark JSON. Raw result files remain ignored; this section stores the selected rows, derived ratios, and provenance needed to reproduce the measurement.",
    "",
    renderFixedTable(fixed),
    renderCoreTable(core, "cross-shard", "Cross-shard core scaling", options.coreShards),
    renderCoreInterpretation(core, "cross-shard"),
    renderCoreTable(core, "same-shard", "Same-shard control", options.coreShards),
    renderTailLatency(fixed),
    renderProvenance(selectedRows, metadata, options),
  ].join("\n");
}

export function replaceGeneratedBlock(document, name, content) {
  const begin = `<!-- BEGIN GENERATED: ${name} -->`;
  const end = `<!-- END GENERATED: ${name} -->`;
  const pattern = new RegExp(`${escapeRegExp(begin)}[\\s\\S]*?${escapeRegExp(end)}`);
  if (!pattern.test(document)) {
    throw new Error(`missing generated block markers for ${name}`);
  }
  return document.replace(pattern, `${begin}\n${content.trimEnd()}\n${end}`);
}

export function selectFixedRows(rows, options) {
  const selected = {};
  for (const mode of fixedModes) {
    selected[mode] = ["cross-shard", "same-shard"].map((keys) =>
      selectOne(
        rows,
        {
          mode,
          keys,
          isolates: options.fixedIsolates,
          shards: options.fixedShards,
        },
        `${mode} ${keys} isolates=${options.fixedIsolates} shards=${options.fixedShards}`,
      ),
    );
  }
  return selected;
}

export function selectCoreRows(rows, options) {
  const selected = {};
  for (const mode of coreModes) {
    selected[mode] = {};
    for (const keys of ["cross-shard", "same-shard"]) {
      selected[mode][keys] = options.coreIsolates.map((isolates) =>
        selectOne(
          rows,
          {
            mode,
            keys,
            isolates,
            shards: options.coreShards,
          },
          `${mode} ${keys} isolates=${isolates} shards=${options.coreShards}`,
        ),
      );
    }
  }
  return selected;
}

function renderFixedTable(fixed) {
  const lines = [
    "## Five-sample 16-isolate comparison",
    "",
    "| Workload | Cross-shard | Same-shard | Cross-shard advantage |",
    "| --- | ---: | ---: | ---: |",
  ];
  for (const mode of fixedModes) {
    const [cross, same] = fixed[mode];
    lines.push(
      `| ${modeLabels.get(mode)} | ${formatInteger(cross.throughputRps)} req/s | ${formatInteger(same.throughputRps)} req/s | ${formatRatio(cross.throughputRps, same.throughputRps)} |`,
    );
  }
  lines.push(
    "",
    "These results show the intended behavior: unrelated memory IDs can progress in parallel, while work concentrated on one storage shard remains contention-bound.",
    "",
  );
  return lines.join("\n");
}

function renderCoreTable(core, keys, title, shards) {
  const lines = [
    `## ${title}`,
    "",
    keys === "cross-shard"
      ? `This five-sample matrix slice fixes the memory shard count at ${shards}.`
      : "Adding isolates does not improve one hot shard. That is expected: ordering and transactional correctness deliberately serialize conflicting work.",
    "",
    "| Isolates | Direct write | Atomic read + write | Atomic write + effect |",
    "| ---: | ---: | ---: | ---: |",
  ];
  const isolates = coreModes.length === 0 ? [] : core[coreModes[0]][keys].map((row) => row.isolates);
  for (const isolate of isolates) {
    const values = coreModes.map((mode) =>
      core[mode][keys].find((row) => row.isolates === isolate).throughputRps,
    );
    lines.push(
      `| ${isolate} | ${formatInteger(values[0])} req/s | ${formatInteger(values[1])} req/s | ${formatInteger(values[2])} req/s |`,
    );
  }
  lines.push("");
  return lines.join("\n");
}

function renderCoreInterpretation(core, keys) {
  const atomic = core["atomic-readwrite-memory-wide"][keys];
  const effect = core["atomic-write-memory-wide"][keys];
  const direct = core["direct-write-memory-wide"][keys];
  const oneAtomic = atomic.find((row) => row.isolates === 1);
  const sixteenAtomic = atomic.find((row) => row.isolates === 16);
  const oneEffect = effect.find((row) => row.isolates === 1);
  const sixteenEffect = effect.find((row) => row.isolates === 16);
  const peakDirect = maxBy(direct, (row) => row.throughputRps);
  const thirtyTwoAtomic = atomic.find((row) => row.isolates === 32);
  const movement = plateauLabel(sixteenAtomic.throughputRps, thirtyTwoAtomic?.throughputRps);
  return [
    `The atomic read/write path scales by about ${formatRatio(sixteenAtomic.throughputRps, oneAtomic.throughputRps)} from one to sixteen isolates. The write/effect path scales by about ${formatRatio(sixteenEffect.throughputRps, oneEffect.throughputRps)} over the same interval. Moving from 16 to 32 isolates ${movement} for atomic read/write throughput on this machine.`,
    "",
    `Direct writes peak at ${peakDirect.isolates} isolates in this matrix slice. They are cheap enough that storage and coordination overhead can become limiting before isolate execution does.`,
    "",
  ].join("\n");
}

function renderTailLatency(fixed) {
  const lines = [
    "## Tail latency at 16 isolates",
    "",
    "| Workload | Distribution | P95 | P99 |",
    "| --- | --- | ---: | ---: |",
  ];
  for (const mode of coreModes) {
    const label = mode === "direct-write-memory-wide" ? "Direct write" : modeLabels.get(mode);
    for (const row of fixed[mode]) {
      const distribution = row.keys === "cross-shard" ? "Cross-shard" : "Same-shard";
      lines.push(
        `| ${label} | ${distribution} | ${formatDecimal(row.p95Ms)} ms | ${formatDecimal(row.p99Ms)} ms |`,
      );
    }
  }
  lines.push(
    "",
    "The hot-shard tail is the main remaining risk for skewed production workloads. Use the `skewed-hotspot` matrix mode when evaluating fairness or scheduler changes.",
    "",
  );
  return lines.join("\n");
}

function renderProvenance(rows, metadata, options) {
  const sources = [...new Set(rows.map((row) => row.source))].sort();
  const sampleCounts = [...new Set(rows.map((row) => row.sampleCount))].sort((a, b) => a - b);
  const requestCounts = [...new Set(rows.map((row) => row.requests))].sort((a, b) => a - b);
  const concurrency = [...new Set(rows.map((row) => row.concurrency))].sort((a, b) => a - b);
  const startedAt = [...new Set(rows.map((row) => row.startedAt))].sort().join(", ");
  return [
    "## Provenance",
    "",
    `- Inputs: ${sources.join(", ")}`,
    `- Started at: ${startedAt}`,
    `- Git commit: ${metadata.git_commit}${metadata.git_dirty ? " (dirty)" : " (clean)"}`,
    `- Samples: ${sampleCounts.join(", ")}`,
    `- Logical CPUs: ${metadata.logical_cpus}`,
    `- OS: ${metadata.os}`,
    `- Rust: ${metadata.rustc}`,
    `- Cargo: ${metadata.cargo}`,
    `- Requests: ${requestCounts.join(", ")}`,
    `- Concurrency: ${concurrency.join(", ")}`,
    `- Fixed comparison: isolates=${options.fixedIsolates}, shards=${options.fixedShards}`,
    `- Core curve: isolates=${options.coreIsolates.join(" ")}, shards=${options.coreShards}`,
    "",
  ].join("\n");
}

function coreSelectionRows(core) {
  return Object.values(core).flatMap((byKey) => Object.values(byKey).flat());
}

async function loadRows(path, options) {
  const result = await loadBenchmarkResult(path);
  return normalizeBenchmarkResult(result, path, { allowDirty: options.allowDirty });
}

function parseArgs(args) {
  const parsed = {
    fixed: null,
    core: null,
    out: null,
    check: false,
    allowDirty: false,
    fixedIsolates: 16,
    fixedShards: 16,
    coreShards: 16,
    coreIsolates: defaultCoreIsolates,
  };
  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index];
    if (arg === "--fixed") {
      parsed.fixed = args[++index];
    } else if (arg === "--core") {
      parsed.core = args[++index];
    } else if (arg === "--out") {
      parsed.out = args[++index];
    } else if (arg === "--check") {
      parsed.check = true;
    } else if (arg === "--allow-dirty") {
      parsed.allowDirty = true;
    } else if (arg === "--fixed-isolates") {
      parsed.fixedIsolates = positiveInteger(args[++index], arg);
    } else if (arg === "--fixed-shards") {
      parsed.fixedShards = positiveInteger(args[++index], arg);
    } else if (arg === "--core-shards") {
      parsed.coreShards = positiveInteger(args[++index], arg);
    } else if (arg === "--core-isolates") {
      parsed.coreIsolates = args[++index].split(/[,\s]+/).filter(Boolean).map((value) =>
        positiveInteger(value, "--core-isolates"),
      );
    } else if (arg === "--help" || arg === "-h") {
      parsed.help = true;
    } else {
      throw new Error(`unknown argument: ${arg}`);
    }
  }
  if (!parsed.help && !parsed.fixed) {
    throw new Error("--fixed is required");
  }
  parsed.core ??= parsed.fixed;
  return parsed;
}

function positiveInteger(value, name) {
  const number = Number(value);
  if (!Number.isInteger(number) || number < 1) {
    throw new Error(`${name} must be a positive integer, got ${inspect(value)}`);
  }
  return number;
}

function maxBy(values, getValue) {
  return values.reduce((best, value) => (getValue(value) > getValue(best) ? value : best), values[0]);
}

function plateauLabel(base, value) {
  if (value == null) {
    return "was not measured";
  }
  const ratio = value / base;
  if (ratio >= 1.05) {
    return "improves modestly";
  }
  if (ratio <= 0.95) {
    return "regresses";
  }
  return "plateaus";
}

function escapeRegExp(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function printHelp() {
  console.log(`Usage: node benchmarks/summarize.mjs --fixed FILE [--core FILE] [--out benchmarks/SCALING.md] [--check]

Generates the marked benchmark evidence section in benchmarks/SCALING.md from
ignored local benchmark JSON. Use --allow-dirty only for non-release evidence.`);
}

function isMain() {
  return process.argv[1] != null && import.meta.url === pathToFileURL(process.argv[1]).href;
}
