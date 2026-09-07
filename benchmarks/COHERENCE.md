# Consolidation performance: 2026-09-07

All 14 workload/CPU groups pass the median throughput and p99 gates. Storage throughput ratios range from 2.139 to 9.540; instant-response throughput ratios range from 0.988 to 1.057. These are shared-host measurements on one eight-core processor using 8 or 16 logical CPUs. The 32-CPU gate remains unrun.

This report uses the final binaries after the confirmed runtime shutdown fix. The earlier incomplete comparison contributes no samples.

Five alternating AB/BA pairs per workload and CPU count, 20,000 requests per process; 140 successful process runs. Each process starts a fresh store on btrfs under /home on NVMe. Gate: median paired candidate/baseline throughput >=0.95 and p99 <=1.10. Ratios below are medians of per-pair ratios; displayed baseline/candidate values are separate medians.

| Logical CPUs | Workload | Baseline rps | Candidate rps | Throughput ratio | Baseline p99 ms | Candidate p99 ms | p99 ratio | Gate |
|---:|---|---:|---:|---:|---:|---:|---:|---|
| 8 | kv-store-put | 711 | 2584 | 3.899 | 51.170000 | 9.860000 | 0.244 | PASS |
| 8 | memory-store-put | 273 | 845 | 2.911 | 69.150000 | 23.240000 | 0.333 | PASS |
| 8 | memory-atomic-read | 8818 | 18936 | 2.139 | 1.830000 | 1.000000 | 0.546 | PASS |
| 8 | memory-atomic-readwrite | 799 | 1175 | 2.559 | 54.260000 | 24.590000 | 0.229 | PASS |
| 8 | memory-atomic-effect | 87 | 563 | 6.443 | 174.300000 | 33.270000 | 0.197 | PASS |
| 8 | runtime-instant-text | 45285 | 45188 | 0.990 | 0.368648 | 0.351718 | 0.964 | PASS |
| 8 | runtime-instant-json | 45381 | 44799 | 0.988 | 0.361718 | 0.360948 | 1.000 | PASS |
| 16 | kv-store-put | 224 | 1889 | 8.460 | 152.930000 | 16.730000 | 0.112 | PASS |
| 16 | memory-store-put | 238 | 1155 | 4.651 | 152.790000 | 29.270000 | 0.211 | PASS |
| 16 | memory-atomic-read | 2220 | 9484 | 4.289 | 14.950000 | 5.890000 | 0.395 | PASS |
| 16 | memory-atomic-readwrite | 971 | 2581 | 2.546 | 53.190000 | 13.730000 | 0.290 | PASS |
| 16 | memory-atomic-effect | 98 | 958 | 9.540 | 267.470000 | 35.530000 | 0.156 | PASS |
| 16 | runtime-instant-text | 22572 | 23982 | 1.057 | 2.877792 | 2.663123 | 0.898 | PASS |
| 16 | runtime-instant-json | 18996 | 25466 | 1.001 | 3.366940 | 3.142341 | 0.862 | PASS |

## Individual threshold misses

The final gate uses five-pair medians. Every individual pair that misses either threshold remains included and is listed here; no completed samples were excluded or rerun. The interrupted baseline had no completion sample; its whole pair was repeated as documented below.

| Pair | Throughput ratio | p99 ratio | Missed threshold |
|---|---:|---:|---|
| [cpu-16-runtime-instant-json-pair-2](/home/mewhhaha/dd-coherence-performance-20260907-v2/cpu-16-runtime-instant-json-pair-2/pair.json) | 0.791 | 1.191 | throughput, p99 |
| [cpu-16-runtime-instant-text-pair-1](/home/mewhhaha/dd-coherence-performance-20260907-v2/cpu-16-runtime-instant-text-pair-1/pair.json) | 0.971 | 1.152 | p99 |
| [cpu-8-memory-atomic-readwrite-pair-3](/home/mewhhaha/dd-coherence-performance-20260907-v2/cpu-8-memory-atomic-readwrite-pair-3/pair.json) | 1.078 | 1.255 | p99 |

## Interrupted runner and audited continuation

The outer runner unexpectedly exited with signal-derived status 143 after 59 completed pairs, while the baseline of 16-CPU effect pair 5 was progressing at 15,383/20,000 requests. The signal origin is unknown; no source or binary change was found. The partial baseline has no completion sample and is excluded from statistics. Its logs/store were archived intact under `interrupted-attempts/`. All 59 completed pairs were retained with file hashes; effect pair 5 was repeated in full (baseline then candidate), followed by the remaining 10 instant pairs in their original AB/BA order. Completed samples were not selectively rerun. The continuation used the same frozen binaries, request counts, affinities, defaults and 600-second per-process timeout through the unchanged run_sample function. Final comparison still contains 70 complete pairs/140 successful accepted process runs.

Continuation adapter: [resume.py](/home/mewhhaha/dd-coherence-performance-20260907-v2/resume.py), SHA256 `bfbb4eb12e3a011bfcc14d8e85ede6b4aebdd2f30080b6bda0a794c8f60cb9d6`. Full interruption and before/after preservation evidence: [interruption-and-resume.json](/home/mewhhaha/dd-coherence-performance-20260907-v2/interruption-and-resume.json). Adapter source and interruption record are embedded in the compact JSON archive.

## Whole-process resource observations

CPU seconds are user+system time; peak RSS and CPU time include setup, verification, and shutdown. They are not timed-region-only metrics. RSS/CPU ratios are median paired ratios. Lower ratios use fewer resources.

| Logical CPUs | Workload | Baseline RSS MiB | Candidate RSS MiB | RSS ratio | Baseline CPU s | Candidate CPU s | CPU ratio | Baseline wall s | Candidate wall s |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 8 | kv-store-put | 31.4 | 91.4 | 2.905 | 4.714 | 1.329 | 0.292 | 28.140 | 8.067 |
| 8 | memory-store-put | 112.3 | 167.5 | 1.487 | 4.661 | 5.002 | 1.039 | 73.260 | 24.852 |
| 8 | memory-atomic-read | 127.7 | 171.5 | 1.345 | 5.875 | 1.813 | 0.309 | 2.525 | 2.998 |
| 8 | memory-atomic-readwrite | 216.0 | 241.6 | 1.124 | 11.639 | 9.095 | 0.734 | 25.228 | 17.463 |
| 8 | memory-atomic-effect | 301.8 | 272.0 | 0.908 | 30.180 | 19.181 | 0.626 | 231.131 | 37.139 |
| 8 | runtime-instant-text | 95.5 | 163.3 | 1.720 | 0.726 | 0.816 | 1.126 | 0.543 | 1.740 |
| 8 | runtime-instant-json | 95.6 | 159.7 | 1.680 | 0.719 | 0.816 | 1.134 | 0.543 | 1.640 |
| 16 | kv-store-put | 31.4 | 90.9 | 2.890 | 4.699 | 0.771 | 0.163 | 89.368 | 11.819 |
| 16 | memory-store-put | 122.1 | 167.0 | 1.367 | 5.853 | 5.038 | 0.851 | 83.933 | 18.572 |
| 16 | memory-atomic-read | 146.5 | 195.4 | 1.334 | 9.633 | 2.343 | 0.240 | 9.250 | 3.588 |
| 16 | memory-atomic-readwrite | 271.2 | 270.9 | 0.998 | 16.480 | 8.936 | 0.560 | 20.955 | 8.476 |
| 16 | memory-atomic-effect | 356.7 | 298.9 | 0.845 | 28.506 | 17.830 | 0.485 | 203.869 | 22.738 |
| 16 | runtime-instant-text | 113.3 | 178.4 | 1.572 | 1.033 | 1.124 | 1.089 | 1.008 | 2.686 |
| 16 | runtime-instant-json | 112.8 | 182.3 | 1.620 | 1.172 | 1.216 | 1.087 | 1.207 | 2.322 |

The candidate uses more peak memory in several small workloads. At 8 logical CPUs, native KV rises from 31.4 to 91.4 MiB, and instant text rises from 95.5 to 163.3 MiB. Whole-process CPU ratios for instant text and JSON are 1.126 and 1.134. Fixed shard and connection ownership may contribute, but these measurements do not isolate the cause. Throughput and p99 gates impose no memory or whole-process CPU limit.

## Machine, comparability, and limits

AMD Ryzen 7 7800X3D, 8 physical cores and 16 hardware threads, with 93.4 GiB RAM. Affinity `0-7` uses one thread per core; `0-15` adds the SMT siblings on those same cores. These are two affinity settings on one processor. The host cannot run the 32-CPU gate. Exact topology is retained in the results archive.

Stores were on `/home`, a btrfs filesystem on `/dev/nvme0n1p2` (NVMe). The machine ran Linux `7.1.5-1-cachyos`; both revisions used Rust and Cargo 1.94.0 and the optimized Cargo `dist` profile. Exact flags, profile definitions, Cargo configuration, source snapshots, and binary hashes are retained in the manifest and build records.

This was a shared host, not a dedicated machine. Observed start/end one-minute load ranged from 1.52 to 41.58 (median 10.75); each run retains complete start/end /proc/stat. AB/BA pairing reduces order effects but cannot remove interference. No unrelated processes were stopped. Gate results apply to these samples, not an uncontended-host guarantee.

Workload contracts:

- `kv-store-put`: Native KV put returns after FULL commit on both revisions; one shared key. Baseline selector `set-utf8`; candidate selector `set-utf8`.
- `memory-store-put`: Native apply_batch returns after FULL commit; one assignment per request across 256 entities. Baseline selector `storage-write-memory-wide`; candidate selector `storage-write-memory-wide`.
- `memory-atomic-read`: An atomic callback reads one seeded entity; no durable mutation in the timed region. Baseline selector `atomic-read-memory`; candidate selector `atomic-read-memory`.
- `memory-atomic-readwrite`: Atomic read and assignment across 256 entities; response follows FULL commit on both revisions. Baseline selector `atomic-readwrite-memory-wide`; candidate selector `atomic-readwrite-memory-wide`.
- `memory-atomic-effect`: Atomic assignment and persisted outbox effect across 256 entities; response follows FULL commit. Baseline selector `atomic-write-memory-wide`; candidate selector `atomic-write-effect-memory-wide`.
- `runtime-instant-text`: The same static worker returns an immediate text response; no bindings or state writes. Measures runtime dispatch and scheduling overhead. Baseline selector `instant-text`; candidate selector `instant-text`.
- `runtime-instant-json`: The same static worker returns an immediate JSON response; no bindings or state writes. Measures runtime dispatch and scheduling overhead. Baseline selector `instant-json`; candidate selector `instant-json`.

Validation scope: the fast-fetch timed loop awaits each invocation but does not inspect every response status or body. Memory invocation loops check status 200 and verify the post-write sum. These benchmarks do not prove exact response bytes or end-to-end effect delivery; separate functional suites cover those behaviors. The effect benchmark measures committed outbox enqueue, with effects retained in the store, rather than delivery. The runner independently checks one parseable benchmark result, exact request/concurrency counts, finite positive statistics, and successful process exit. Deployment, seed, verification, and shutdown are outside timed throughput on both revisions. The baseline formats the 256 pool-key strings just after starting the aggregate timer; the candidate precomputes them. This is a one-time cost over 20,000 requests; its effect was not measured separately.

The effect workload retained all 20,000 requests and pending effects per run. An earlier 20k pilot showed increasing request cost at higher row counts with concurrent shared-host load changes, so samples were not shortened. JavaScript KV queue-ack baseline timings are excluded; native KV and memory writes both acknowledge FULL commits. Baseline uses 16 namespace shards; candidate uses 32 shared state shards, an intentional architectural difference. Wide memory workloads use the same 256 logical entities; their physical shard placement intentionally differs.

## Provenance

- baseline: source `/tmp/dd-state-baseline-0922`, commit `0922cce01e319a587a23eb5cf1dbe2d362caeb66`, patch SHA256 `a1758781485afa4246b4248cb699420d3a7edbf01f21585ccb9c567665b008b8`.
  - `bench_fetch_fast` SHA256 `73b98dee550f229e087ee71096077942b9fa92b318180c17b1d43b8511834bb7`.
  - `bench_kv_store` SHA256 `d7c70852fbd816d2c773d9e38ab22a3fdec21ce33cc407c91aefe131c1b898ec`.
  - `bench_memory_storage` SHA256 `f8db94c5989ac6bb21dbd5d3dbe412fe40ff9a521deb98d8172d6ce4f5254b8c`.
- candidate: source `/home/mewhhaha/src/dd`, commit `0922cce01e319a587a23eb5cf1dbe2d362caeb66`, patch SHA256 `6747b5a0810079df4fa85614dee292457008d915b7f5226c9f6e7bf6a69fe0ba`.
  - `bench_fetch_fast` SHA256 `ee201601fadff1654f013f1c8ad329905d6155eb13431789c7e863f589f65cb4`.
  - `bench_kv_store` SHA256 `828b03b206f780a2632dd6d402ce429d4b5a84b4a92c6df7ef0e32110cb61b7d`.
  - `bench_memory_storage` SHA256 `11e99d7d0e0e24d09491051b202133cd3f36b744068a3ad6934caa56185c47d7`.

Baseline is commit 0922cce01e319a587a23eb5cf1dbe2d362caeb66 plus the explicitly verified harness-only patch: three benchmark roots use temp_dir for physical-disk TMPDIR, and fast-fetch prints latency to six decimals. No baseline business logic was changed. Candidate source includes the complete consolidation and shutdown fix; build provenance verified source unchanged during the build. Both source snapshots and all six binary hashes were verified unchanged after measurement. The report and documentation links were added afterward. This records the measured working tree through its patch and untracked-file hashes; it is not a clean release-commit certification.

Raw evidence is under `/home/mewhhaha/dd-coherence-performance-20260907-v2`: `manifest.json`, `summary.json`, every `pair.json`, and each process's `run.json`, `stdout.log` and `stderr.log`. The earlier failed run and causal shutdown regression are documented separately under `/home/mewhhaha/dd-coherence-performance-20260907/incomplete-run.md`.

## Evidence and reproduction

The [complete results archive](/home/mewhhaha/dd-coherence-performance-20260907-v2/coherence-2026-09-07.json) contains all 70 pairs, both sides' parsed samples, resource measurements, source/build provenance, the continuation adapter and the final preservation checks. Archive SHA256: `2b002b71f312b320ec814ec2fa0b17834c5216e95f32a904d057e19cb1a4e10c`. [Final hash verification](/home/mewhhaha/dd-coherence-performance-20260907-v2/final-hash-verification.json) confirms unchanged binaries, sources and preserved pairs. Raw logs remain in the artifact directory. The [report generator](/tmp/summarize-state-benchmarks-v2.py) recomputes the tables from those artifacts.

Follow the [build and comparison commands](../docs/storage-conversion.md) to repeat the comparison. `scripts/compare-state-benchmarks.py` produces fresh five-pair comparisons with equivalent durable semantics and records unavailable CPU sizes. A 32-CPU run on suitable hardware remains required before claiming coverage of that target.

The earlier comparison exposed an automatic-shutdown deadlock: the coordinator retained queued outbox reply senders while waiting for the outbox worker to stop. Dropping the receiver before joining the worker fixed it. The [subprocess regression](../crates/runtime/src/service/tests/shutdown.rs) failed twice before the fix, then verified process exit and recovery of all 2,048 committed effects. All ten candidate effect processes in this comparison exited normally. See the [earlier diagnostic record](/home/mewhhaha/dd-coherence-performance-20260907/incomplete-run.md).

The [architecture notes](../docs/architecture.md) describe resource budgets and remaining limits, including buffering service-binding responses under a per-response size limit.
