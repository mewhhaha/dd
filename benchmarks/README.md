# Benchmark configurations

The scripts in [`configs/`](configs/) are the canonical commands for the
workloads we intentionally test. Raw sampler output is generated under
`benchmarks/results/` and is not committed. The [scaling matrix guide](SCALING.md)
describes the separate scaling workloads; its historical results do not
establish performance of the current storage layout.

Two paired runners compare a baseline build with a candidate outside the
sampler: the [state comparison](../docs/storage-conversion.md) for durable KV,
memory and dispatch workloads, and the
[concurrent memory comparison](#paired-memory-comparisons). Both verify prebuilt
binaries against exact source snapshots, including working-tree patches. The
general sampler below requires clean source trees for its commit-to-commit
acceptance checks. [Fly public API case studies](#fly-public-api-case-studies)
measure a deployed app over HTTPS, and
[what earlier investigations found](#what-earlier-investigations-found)
summarizes the measurements behind the current memory design.

The per-worker isolate limit is `DD_BENCH_MAX_ISOLATES_PER_WORKER`.

## Running benchmarks

Run one configuration from the repository root:

```bash
./benchmarks/configs/saturated-fast-fetch-instant-text.sh
```

Run the reproducible sampler and write machine-readable local output:

```bash
node benchmarks/run.mjs --samples 3 \
  --out benchmarks/results/local.json
```

The default sampler skips benchmark configs marked `DD_BENCH_MATRIX_OPT_IN=1`,
including the full atomic scaling matrix. It prints a note for skipped opt-in
configs so the default command remains a small-suite run.

Pass `--config` more than once to select a subset:

```bash
node benchmarks/run.mjs --samples 5 \
  --config scaling-memory-write-cross-shard.sh \
  --config atomic-memory-readwrite-cross-shard.sh \
  --out benchmarks/results/local-write-check.json
```

Run the real-world memory workloads:

```bash
node benchmarks/run.mjs --samples 3 \
  --config realworld-rate-limiter.sh \
  --config realworld-multiworker-auth.sh \
  --out benchmarks/results/local-realworld.json
```

The runner records staged, unstaged and untracked changes; toolchain and build
flags; CPU affinity and model; memory and CPU limits; filesystem devices;
effective benchmark environment; and the benchmark script fingerprint. It also
records raw output, exit status, individual measurements and median summaries.

Compare two clean, same-host runs with the production budgets (no more than 5%
throughput loss or 10% p99 increase):

```bash
node benchmarks/compare-commits.mjs \
  --baseline benchmarks/results/baseline.json \
  --candidate benchmarks/results/candidate.json
```

Comparisons reject empty or duplicate rows, missing samples, dirty source trees,
different workload settings and incompatible environments. Use the same harness
for both source revisions so the metadata and benchmark definitions match:

```bash
DD_BENCH_SOURCE_DIR=/path/to/baseline-checkout \
  node benchmarks/run.mjs --samples 5 --config fly-production.sh \
  --out /tmp/dd-baseline.json
node benchmarks/run.mjs --samples 5 --config fly-production.sh \
  --out /tmp/dd-candidate.json
```

`DD_BENCH_SOURCE_DIR` selects the source checkout to build; the invoking
harness supplies the benchmark scripts. The Fly profile records its selected
CPU and defaults to the first CPU allowed by the current affinity mask.

Inspect the exact plan without spawning child processes or writing a result
file:

```bash
node benchmarks/run.mjs --plan --samples 5 \
  --config scaling-atomic-memory-matrix.sh
```

`--list` remains a compact name-only view. `--plan` expands matrices, shows
environment overrides, prints the variant count, sample count, total child
process runs, and the output path that would be used.

Runs are budgeted before execution. The default budget is 100 child processes
and can be changed with `--max-runs N` or `DD_BENCH_MAX_RUNS=N`. Pass
`--allow-large-run` or set `DD_BENCH_ALLOW_LARGE_RUN=1` only for intentional
large jobs. A child failure stops the run by default after writing partial JSON
with `complete: false`, `failure`, and `failures`; use `--keep-going` for
deliberate batch collection.

All throughput runs measure `RuntimeService` directly unless noted. The
`http1-server-front-cache.sh` configuration is deliberately separate: it runs
the complete `dd_server` against a temporary store and includes the public API,
TCP listener, and persistent HTTP/1.1 connection overhead.

Run five same-host samples of the network benchmark with:

```bash
node benchmarks/run.mjs --samples 5 \
  --config http1-server-front-cache.sh \
  --out benchmarks/results/local-http1-server-front-cache.json
```

Each sample deploys one uncached worker and one front-cache-enabled worker,
warms the latter, then reports `http1-server/uncached-small` and
`http1-server/front-cache-hit` as distinct result rows under `INFO` tracing.
Every measured response is checked for its body, status, cache header, and
propagated trace id. Captured server output also makes the run fail on a panic
or a `WHOPPER_*` debug probe.

## Fly production profile

[`fly-production.sh`](configs/fly-production.sh) pins benchmark processes to one
CPU when `taskset` is available and uses the isolate, inflight, and snapshot
cache limits from `deploy/fly/fly.toml`. It covers the real HTTP server, memory
transactions, durable effects, the rate
limiter, multi-worker auth, and lifecycle cold starts under the two-isolate
global budget.

Run it directly or through the sampler:

```bash
node benchmarks/run.mjs --samples 3 \
  --config fly-production.sh \
  --out benchmarks/results/local-fly-production.json
```

Set `DD_BENCH_CPUSET` to select a different CPU. The scheduled production
regression workflow compares this profile against the previous main commit;
the wider scaling matrix remains separate because it answers a different
question.

## Atomic scaling matrix

[`scaling-atomic-memory-matrix.sh`](configs/scaling-atomic-memory-matrix.sh)
expands into one sampled configuration for every selected matrix combination.

Default dimensions:

| Dimension | Values |
| --- | --- |
| Isolates | `1`, `2`, `4`, `8`, `16`, `32` |
| Key modes | `same-shard`, `cross-shard`, `skewed-hotspot` |
| Workloads | atomic write only, atomic read + write, atomic write + effect |

The default matrix has `6 * 3 * 3 = 54` variants. At five samples that is
270 child-process runs, so the command below requires either
`--allow-large-run` or a sufficiently high `--max-runs`.

Run the complete matrix from a clean worktree when producing release evidence:

```bash
git diff --quiet
node benchmarks/run.mjs --plan --samples 5 \
  --config scaling-atomic-memory-matrix.sh
node benchmarks/run.mjs --samples 5 \
  --config scaling-atomic-memory-matrix.sh \
  --allow-large-run \
  --out benchmarks/results/local-atomic-memory-scaling-matrix.json
```

Override matrix dimensions for a shorter development check:

```bash
DD_BENCH_MATRIX_ISOLATES="1 2 4 8 16" \
DD_BENCH_MATRIX_KEY_MODES="same-shard cross-shard" \
DD_BENCH_MATRIX_MODES="atomic-readwrite-memory-wide" \
node benchmarks/run.mjs --samples 1 \
  --config scaling-atomic-memory-matrix.sh \
  --max-runs 12 \
  --out benchmarks/results/local-atomic-memory-scaling-smoke.json
```

Run the relative smoke regression policy on a small same-host matrix:

```bash
DD_BENCH_MATRIX_ISOLATES="1 8" \
DD_BENCH_MATRIX_KEY_MODES="same-shard cross-shard" \
DD_BENCH_MATRIX_MODES="atomic-write-only-memory-wide atomic-readwrite-memory-wide" \
node benchmarks/run.mjs --samples 3 \
  --config scaling-atomic-memory-matrix.sh \
  --out benchmarks/results/local-memory-smoke.json

node benchmarks/check-regression.mjs \
  --results benchmarks/results/local-memory-smoke.json \
  --policy benchmarks/baselines/memory-scaling-policy.json
```

This gate checks structural ratios such as cross-shard atomic advantage and
basic scaling from one isolate. It intentionally avoids machine-specific
absolute throughput thresholds for ordinary local and pull-request use.

Use `--samples 3` for a shorter repeated check. Use `--samples 5` on a clean
worktree before treating medians as release or main-branch evidence.

## Workload notes

- Focused memory configs preserve the historical gate shape: 300 requests, 16
  concurrency, 1-4 isolates, and 8 max inflight.
- Saturated fast fetch prestarts 16 isolates and uses 256 concurrent client
  tasks. It checks peak simple-fetch throughput.
- HTTP/1 server coverage uses a dedicated server process, a temporary Turso
  store, and persistent raw HTTP/1.1 connections. It is intentionally not
  comparable to the in-process `RuntimeService` result rows.
- Saturated wide memory read also prestarts 16 isolates with 256 concurrency,
  but measures keyed-memory routing and hydration.
- State storage has 32 fixed shards. Scaling configs compare transactions on
  keys in the same shard with keys spread across shards.
- Storage-only write configs invoke `MemoryStore.apply_batch()` directly. They
  isolate storage behavior from JavaScript and runtime scheduling.
- Atomic read/write configs read the current value and write `payload = "1"`.
- Atomic write/effect configs write `payload = "1"` and emit an
  `audit.bench.write` durable effect.
- Real-world rate limiter checks a memory-backed per-client window, writes the
  request count/reset metadata atomically, and verifies the summed counters
  equal total requests.
- Real-world multi-worker auth deploys a private auth worker with KV-backed
  users plus memory-backed session state, then benchmarks a frontend worker
  calling it through an `AUTH` service binding.
- `DD_BENCH_MODE=realworld-auth-worker-direct` is a diagnostic auth mode that
  hits the auth worker directly with KV and memory bindings, bypassing the
  frontend and service binding layer.
- Atomic wide-write workloads verify all distinct written keys.

## Paired memory comparisons

`bench_memory_fanout` measures a worker that calls several independent memories
with `Promise.all`. It validates every response and the exact stored state
before shutdown and after reopening. `scripts/compare-memory-fanout.py` runs a
baseline and a candidate executable in alternating pairs, on CPU sets that take
distinct physical cores before SMT siblings, with a fresh store for every
process on a physical disk. It needs Linux CPU affinity and `taskset`, and a
build record for each executable:

```sh
python3 scripts/build-state-benchmarks.py --bin bench_memory_fanout \
  --source /path/to/revision --target-dir /path/to/cargo-target \
  --output /path/to/new-build-record

python3 scripts/compare-memory-fanout.py \
  --baseline /path/to/before/bench_memory_fanout \
  --baseline-record /path/to/before/build-record.json \
  --candidate /path/to/after/bench_memory_fanout \
  --candidate-record /path/to/after/build-record.json \
  --cpu-count 1 --cpu-count 4 --cpu-count 8 \
  --case mixed:16:256:4096:16:varied --case read:16:256:4096:16:varied \
  --concurrency 32 --pairs 3 --warmup-ms 2000 --duration-ms 8000 \
  --output /physical/disk/new-results

python3 scripts/summarize-memory-fanout.py /physical/disk/new-results
```

Copy each executable out of its target directory before building the next
revision. The runner rejects builds whose benchmark sources, toolchain, profile
or Cargo configuration differ, so an older checkout needs the current
`bench_memory_fanout` sources copied in first. A case is
`MODE:WIDTH:POPULATION:PAYLOAD_BYTES[:KEYS_PER_ENTITY:PAYLOAD_KIND]`: mode
`read`, `write` or `mixed`, width 1, 4 or 16 memories per request, and payload
kind `repeated` or `varied`. The summarizer prints Markdown tables of paired
medians, latency by operation, cache misses, commit density, whole-process CPU
milliseconds per request and peak RSS; `--plot` also draws core scaling and
needs matplotlib.

- `--baseline-read-api atomic --candidate-read-api snapshot` compares
  `memory.atomic()` with `memory.read()` using one executable and build record
  on both sides. The executable takes `DD_FANOUT_READ_API=atomic|snapshot`;
  only read and verification operations use it, while seeding and writes
  always use `atomic()`.
- `--requests-per-second N` (`DD_FANOUT_REQUESTS_PER_SECOND`) schedules arrivals
  at a fixed rate instead of saturating the callers. Latency starts at the
  scheduled arrival, so waiting for a free caller counts, and every scheduled
  request drains before verification. `measurements.timed.by_operation` splits
  read and write latency, and `dispatch_delay` reports the backlog. A capped
  rate measures latency, not maximum throughput.
- `--profile` adds native phase timings and thread wait samples, and
  `--v8-profile` also writes per-isolate V8 logs. Summarize those runs with
  `python3 scripts/summarize-memory-profile.py RESULTS --output summary.json`.
  Profiling slows the service, so use it to attribute time, not to compare
  speed. Native timings are overlapping elapsed waits, not CPU time.

## Fly public API case studies

`scripts/bench-fly-api.py` measures latency through a deployed app's public
HTTPS endpoint at a small, capped load. It deploys temporary workers through
the private control plane, undeploys them on success or failure, and never
calls an external service from a worker. The cases are a durable rate limiter
(one memory transaction per request), an auth dashboard (a service-binding
call, a durable session touch and a KV user lookup), and an inventory
dashboard reading eight memories concurrently
(`scripts/fly-api-case-studies/inventory.js`).

By default it sends 600 timed requests in ten-second phases at 5 and 15
requests/s per case, with at most two in flight. It refuses more than 20
requests/s, phases over 15 seconds or budgets over 1,000 requests, stops on the
first failed request or once recent p95 exceeds one second, and then checks
exact durable counters. The target must already run the current storage
format; the runner does not migrate it.

```sh
just fly-proxy your-dd-app 18089

python3 scripts/bench-fly-api.py \
  --public-origin https://your-dd-app.fly.dev \
  --worker-domain workers.example.com \
  --private-origin http://127.0.0.1:18089 \
  --private-token-file /path/to/private-token \
  --output /path/to/new-results-directory
```

Repeat `--case rate-limiter`, `--case auth-dashboard` or
`--case inventory-dashboard` to select cases. Public requests go to the origin
with the worker's domain in the Host header, so that domain must be configured
on the app. `results.json` keeps every response's latency and validation, phase
summaries, source hashes, deployment IDs and cleanup results; the token is
never written. `python3 scripts/test-bench-fly-api.py`, part of `just check`,
tests the runner's limits without a network. For a local end-to-end run, point
both origins at a disposable `dd_server` and pass `--seconds 1 --rate 2`.

## Broad runtime suite diagnostics

The broad runtime benchmark supports stable listing and filtering:

```bash
cargo run -p runtime --bin bench -- --list
DD_BENCH_ONLY=steady-state \
DD_BENCH_CONFIG=single-isolate \
DD_BENCH_SCENARIO=instant-response \
cargo run -p runtime --bin bench --release
```

Enable machine-parseable lifecycle markers when investigating a crash:

```bash
DD_BENCH_PROGRESS_MARKERS=1 RUST_BACKTRACE=1 \
cargo run -p runtime --bin bench --release
```

The suite now explicitly shuts down each `RuntimeService` between sections and
restore-cycle iterations. To stress a suspected lifecycle issue repeatedly and
capture the last markers under ignored `tmp/bench-crash/` output:

```bash
bash scripts/run_bench_until_failure.sh
```

## Result retention

Do not commit raw JSON output. Keep durable conclusions and selected generated
tables in [`SCALING.md`](SCALING.md), including enough environment and workload
context to reproduce them.

Recommended summary workflow:

1. Run selected benchmarks into ignored `benchmarks/results/*.json` files.
2. Inspect child failures, `complete`, `failure`, and metadata before using the
   result as evidence.
3. Regenerate the measured Markdown section:

   ```bash
   just benchmark-summary \
     benchmarks/results/local-atomic-memory-scaling-matrix.json
   ```

4. Review the Markdown diff, especially provenance and dirty-worktree status.
5. Check freshness without modifying files:

   ```bash
   node benchmarks/summarize.mjs \
     --fixed benchmarks/results/local-atomic-memory-scaling-matrix.json \
     --core benchmarks/results/local-atomic-memory-scaling-matrix.json \
     --out benchmarks/SCALING.md \
     --check
   ```

6. Run `just check` and commit only Markdown, scripts, fixtures, or intentional
   baseline metadata.

The summarizer rejects dirty, incomplete, failed, ambiguous, incompatible, or
malformed results by default. Use `--allow-dirty` only when deliberately marking
generated text as non-release evidence.

## Request instruction counts

`bench_req_micro` runs fixed hello and memory workloads with eight callers,
up to two isolates and four dispatch slots per isolate. The memory workload
commits one write per request over 64 entity keys. The Linux counter includes
user-mode instructions from every process thread and excludes kernel work.

For each build, subtract the N=2,000 count from N=12,000 and divide by 10,000.
Two ABBA cycles provide four marginal samples per build and workload; report
their median. Pin both builds to the same CPU. Wall-clock time is not used as
performance evidence on a loaded host. This compares the whole cleanup from
`bc258d7`, so it cannot isolate the contribution of each change.

Measured on 2026-10-11, comparing `bc258d7` with `8a2088e` on CPU 3:

| Workload | Baseline instructions/request | Candidate instructions/request | Reduction |
| --- | ---: | ---: | ---: |
| Hello | 246,494 | 198,252 | 19.6% |
| Memory atomic | 723,354 | 660,132 | 8.7% |

The four samples ranged from 245,464–248,342 versus 197,955–198,458 for
hello, and 720,183–729,590 versus 654,240–669,066 for memory. All 32 raw
runs had equal counter enabled/running times. Raw evidence from this run is
in `target/cleanup/request-instructions.json`; generated results remain ignored.

Reproduce with an isolated baseline build cache:

```bash
export CARGO_BUILD_JOBS=8 TMPDIR="$PWD/target/tmp"
mkdir -p "$TMPDIR"
cargo build --release --locked -p runtime --bin bench_req_micro
git worktree add --detach ../dd-instruction-baseline bc258d7
mkdir -p ../dd-instruction-baseline/target
cp -r --reflink=always target/release ../dd-instruction-baseline/target/release
python3 - <<'PY'
from pathlib import Path
source = Path("crates/runtime/src/bin/bench_req_micro.rs").read_text()
source = source.replace("max_isolates_per_worker: 2", "max_isolates: 2")
source = source.replace("RuntimeService::start(", "RuntimeService::start_with_service_config(")
Path("../dd-instruction-baseline/crates/runtime/src/bin/bench_req_micro.rs").write_text(source)
PY
cargo build --release --locked -p runtime --bin bench_req_micro \
  --manifest-path ../dd-instruction-baseline/Cargo.toml
cc -O2 -Wall -Wextra -Werror scripts/count-instructions.c -o target/count-instructions
python3 scripts/measure-request-instructions.py \
  ../dd-instruction-baseline/target/release/bench_req_micro \
  target/release/bench_req_micro --counter target/count-instructions \
  --baseline-revision bc258d7 --candidate-revision "$(git rev-parse HEAD)" \
  --cpu 3 --out benchmarks/results/request-instructions.json
```

Choose an available CPU if CPU 3 is outside the process affinity. The runner
rejects failed commands and multiplexed counters, and records every raw count,
binary hashes, revisions and kernel version in the ignored JSON output.

## What earlier investigations found

These investigations are finished. Their full write-ups, raw tables, charts
and reproduction notes are in git history; `bc258d7` is the last commit that
has them, at the paths named below. All of them measured the runtime while it
still ran on the Deno crates, before the move to rusty_v8 on 2026-10-10, so
their absolute numbers do not describe the current runtime.

- **Parallel performance review** (`tasks/PARALLEL_PERFORMANCE.md`,
  2026-06-25). Bounded changes, including a CPU-aware global isolate budget,
  concurrent outbox shard drains, striped memory locks and pooled readers
  behind one writer, made memory throughput scale well to 8 isolates before
  it levelled off on a 16-thread host. Partitioning the worker manager was
  deferred until a profile shows its loop dominating.
- **Storage consolidation** (`benchmarks/COHERENCE.md`, `tasks/COHERENCE.md`,
  2026-09-07). Moving KV and memory into 32 fixed state shards with grouped
  `FULL` commits passed every paired throughput and p99 gate at 8 and 16 CPUs:
  durable storage throughput rose 2.1-9.5x and instant responses held at
  0.99-1.06x. The comparison also found a shutdown deadlock, fixed by dropping
  queued outbox reply senders before joining the outbox worker. The planned
  32-CPU run never happened.
- **Concurrent memory fan-out** (`benchmarks/MEMORY-FANOUT.md`, 2026-09-07).
  Workers calling 16 memories at once missed the snapshot cache on almost
  every transaction, because any write invalidated every cached memory on its
  shard. Writes now invalidate only the memory they change, and one bounded
  snapshot cache serves the whole store: 8-core mixed throughput rose
  1.5-2.0x, while all-write and single-memory cases moved less than 10% either
  way. A shared-lock cache prototype gained at most 7% more and was discarded.
- **Fan-out follow-up** (`benchmarks/MEMORY-FANOUT-FOLLOWUP.md`, 2026-09-07).
  Mutations now enter native batches directly, cached snapshots are shared
  instead of copied, and the writer updates cached snapshots after `COMMIT`
  rather than dropping them. Larger multi-key memories gained 16-58% at 8
  cores; a small four-memory mixed case lost 25% and was never explained.
- **Native snapshots** (`benchmarks/MEMORY-NATIVE-SNAPSHOTS.md`, 2026-09-08).
  Profiles showed transactions reloading whole snapshots into JavaScript to
  read 2 of 16 values. Transactions now keep the snapshot in Rust and copy
  only the values the callback reads, and the isolate's own snapshot cache is
  gone. Large mixed workloads gained 1.6-2.5x, reads 3x at 8 cores, and peak
  RSS fell; durable writes were unchanged.
- **Snapshot reads** (`benchmarks/MEMORY-READ.md`, 2026-09-08).
  `memory.read()` runs a synchronous callback over one committed snapshot
  without the lease, idempotency lookup or commit of `atomic()`. Read-heavy
  throughput rose 1.1-1.8x over reading through `atomic()`, and mixed
  throughput scaled 4.9x from 1 to 8 cores instead of 3.1x. A per-entity load
  lock now stops overlapping cold loads from filling the cache out of order.
- **Memory latency** (`benchmarks/MEMORY-LATENCY.md`, 2026-09-08). At fixed
  arrival rates, snapshot reads cut hot-entity read p99 from 22.6 to 2.6 ms,
  but write latency did not improve. Durable `COMMIT`, about 6-12 ms per group
  on btrfs, and the entity lease held through it dominate writes, so further
  write work should start by measuring commit cost on the deployment's
  storage. Only the benchmark changed: it gained fixed-rate arrivals and
  per-operation latency.
- **Memory overhead** (`benchmarks/MEMORY-OVERHEAD.md`, 2026-09-08). A
  concurrent CLOCK snapshot cache, synchronous clock refreshes and
  request-context reuse showed no reliable gain and were reverted. Two changes
  stayed: shard routing hashes only when a snapshot needs a SQL load, and the
  fan-out summarizer reports CPU time per request.
- **Fly public API** (`benchmarks/FLY-API.md`, `benchmarks/FLY-API-RESULTS.md`,
  2026-09-08). On one shared Fly CPU, all 600 public HTTPS requests across the
  three case studies succeeded at 5 and 15 requests/s, with medians of 36-39 ms
  and p95 under 51 ms. The latency includes TLS and Fly routing, so this shows
  correct behaviour at a modest load, not capacity.
