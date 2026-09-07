# State storage and offline conversion

The runtime stores KV and memory in `STORE/state/shard-00.db` through
`shard-31.db`. `state-layout.json` fixes the format and SHA-256 routing algorithm;
worker, binding and entity identities are length-prefixed before hashing. Each
shard owns one durable writer and two pooled readers. The state directory is
locked against a second owner. KV and memory use explicit ownership columns and
one BLOB value plus an encoding, and preserve deletion tombstones and version
floors across restarts.

Writes enter FIFO queues capped at 4,096 commands and 16 MiB per shard, including
commands being committed. Writers group up to 128 commands under a
`synchronous=FULL` transaction and acknowledge only after commit. Lock conflicts
have a busy timeout and bounded retries. A failed rollback discards the
connection. Memory entity leases are shared across generations, with at most
4,096 admissions and 1,024 identity bytes per admission.

`control.db` and the rebuildable `cache.db` are separate. Response-cache hits
update RAM recency only. The runtime never reads or migrates old KV, memory,
worker JSON, token JSON or external cache-body formats during startup.

Stop the old runtime before converting. Use a new destination outside the source:

```sh
cargo run -p cli -- storage convert \
  --source /path/to/old-store \
  --destination /path/to/new-store \
  --namespace-map /path/to/namespaces.json
```

The optional namespace map has one owner for each ambiguous or orphan legacy
namespace:

```json
{
  "MY_MEMORY": { "worker": "orders", "binding": "MY_MEMORY" }
}
```

A namespace with exactly one owner in stored deployment configurations is
inferred automatically. Qualified namespaces already identify their worker and
binding. Shared namespaces are never split between workers. Duplicate map keys,
unknown owner fields and empty owners are rejected. If two old records would
collide under the selected mapping, conversion fails rather than merging them.

The converter copies the source into `NEW/archive` and reads that copy, leaving
the source untouched. It imports the current legacy `dd-kv.db` and
`memory/HEX_NAMESPACE/shard-NNNN.db` format, including binary values, tombstones,
revision floors, owner epochs, command results and outbox records. Existing
control tokens and migration records are copied and verified. Old deployments
are retained in the archive and removed from live control records; redeploy
bundles built for the new runtime. Cache entries are omitted from the live store.

Sources containing `tokens.json`, `deploy-tokens.json`, `workers.json`, or JSON
records under `workers/` require an existing `control.db`. JSON-only control
stores are rejected before the destination is created; migrate those records
into `control.db` first. Archiving these files does not import live tokens or
deployment records.

`conversion-report.json` records counts and SHA-256 hashes of canonical rows.
The destination becomes usable only after every state table and preserved
control table verifies and the original source still has identical file hashes.
A failed or interrupted conversion leaves an incomplete destination that startup
rejects. Keep it for diagnosis and rerun into another new directory. Conversion
is intentionally not an in-place migration and does not resume partial output.

Point the runtime at `NEW` after successful conversion and redeploy the workers.
The source and archived bundles remain available for manual recovery.

Run the reproducible storage checks with:

```sh
cargo test -p storage -- --test-threads=1
cargo test -p runtime --bin bench_memory_storage -- --test-threads=1
```

The storage integration suite exercises restart ordering, ownership isolation,
lease cancellation and fencing, control archival, binary conversion, ambiguous
mapping rejection, and acknowledged writes after abrupt subprocess exit. It
checks process-crash recovery; it does not simulate a power failure or faulty
storage hardware.

`scripts/compare-state-benchmarks.py` compares prebuilt baseline and candidate
`dist` binaries. Build the baseline in a detached checkout with a separate
target directory, using the same toolchain and profile as the candidate. Both
benchmark harnesses must construct stores using `std::env::temp_dir()`; retain
the baseline's identical harness-only patch as an artifact. The fast-fetch
harness prints latency to six decimal places on both revisions. Build records must
include the exact command, build environment, toolchain, profile, source patch
and resulting binary hashes. Run the candidate's integration checks before
starting the comparison:

```sh
scripts/build-state-benchmarks.py \
  --target-dir target \
  --output /home/you/dd-state-candidate-build

scripts/compare-state-benchmarks.py \
  --baseline-dir /tmp/dd-state-baseline-target-0922/dist \
  --baseline-source /tmp/dd-state-baseline-0922 \
  --baseline-build-record /tmp/dd-state-baseline-build-metadata.json \
  --candidate-dir target/dist \
  --candidate-build-record /home/you/dd-state-candidate-build/build-record.json \
  --output /home/you/dd-state-comparison
```

The runner performs five alternating baseline/candidate pairs for each workload
on every available size among 8, 16 and 32 CPUs using `taskset`. At least 8 allowed
CPUs are required. Repeat `--cpu-count`, for example `--cpu-count 8 --cpu-count 32`,
to select specific sizes; requesting an unavailable size fails before execution.
The manifest records skipped default sizes and their reasons. Concurrency matches the CPU count;
isolate counts and inflight limits match within each pair. Every process starts
a fresh disposable store under its artifact directory through `TMPDIR`; the
runner rejects `tmpfs` and `ramfs` output. The manifest records binary hashes,
source patches, machine and disk details, workload mappings and durability
contracts. Each pair keeps stdout, stderr, exact environment,
throughput and latency, process CPU time, peak RSS and wall duration. Process
resource measurements include setup and verification; benchmark latency excludes
those phases. Starting and ending host load averages and `/proc/stat` counters
make background contention visible on shared machines.

Each workload and CPU count must have median paired throughput at least 95% of
baseline and median paired p99 latency at most 110% of baseline. Failed gates
produce a nonzero exit status after all comparisons finish. Nonpositive or
nonfinite measurements fail immediately.

The default comparison covers native durable KV and memory writes, atomic reads,
atomic read/write callbacks, atomic writes with persisted outbox effects, and
instant text and JSON responses to measure dispatch and scheduling overhead.
JavaScript KV is excluded because the baseline acknowledges queue admission.
The optional `memory-write-api-migration` workload compares a former direct write
with a required atomic callback and explicitly measures that API change.
The baseline's 16 namespace memory shards and the candidate's 32 shared state
shards remain part of the architectural comparison. Pool keys avoid pretending
that old and new shard selectors describe the same physical placement.
