# Bounded public API case studies

The [2026-09-08 Fly results](FLY-API-RESULTS.md) cover the fresh-store deployment
and 600 public HTTPS workload requests on one shared CPU.

`scripts/bench-fly-api.py` exercises three workloads through the public HTTP
API. It deploys temporary workers through the private control plane, then
undeploys them on success or failure. Unique worker names isolate their state;
the public fixtures also require a per-run request header. It never calls an
external service from a worker.

| Case | Work per public request |
|---|---|
| Rate limiter | One durable memory transaction updating a client's count and window |
| Auth dashboard | Frontend service-binding call, durable session touch, and KV user lookup |
| Inventory dashboard | Eight concurrent committed memory snapshots, returning availability and prices |

The first two reuse the existing real-world fixtures in
`crates/runtime/src/bin/bench_memory_storage/workers.rs`. They model storage
and service calls; the auth fixture does not implement production credential
verification. The inventory fixture is
`scripts/fly-api-case-studies/inventory.js`, using 32 small product memories.

Defaults are **600 timed requests total**, split between 5 and 15 requests/s
for ten seconds per rate and case. At most two requests are in flight.
Control, seeding, and verification calls are sequential and spaced by 200 ms.
These calls close their connections explicitly so a long timed phase cannot
leave the next control call reusing an expired idle connection.
The runner rejects more than 20 requests/s, phases longer than 15 seconds,
or a timed budget above 1,000 requests. It skips scheduled slots when both
callers are occupied instead of accumulating a request backlog. There are no
automatic HTTP retries.

The runner stops on a request/validation failure or once the recent p95
exceeds one second, after at least ten results. Requests already in flight
finish under their connection timeouts. Readiness is checked before every
phase. Successful write workloads must have exact per-client counters matching
the completed requests; every inventory response must match all eight products.

## Run

The target must already run the current storage/runtime format. This command
does not upgrade or migrate the platform. Supply the private API token through
a restricted file; it is never included in the result JSON.

Start a private control-plane tunnel:

```sh
flyctl proxy 18089:8081 --app dd-private-8956e096
```

In another terminal:

```sh
python3 scripts/bench-fly-api.py \
  --public-origin https://dd-private-8956e096.fly.dev \
  --worker-domain wdyt.chat \
  --private-origin http://127.0.0.1:18089 \
  --private-token-file /path/to/private-token \
  --output /path/to/new-results-directory
```

Use `--case rate-limiter`, `--case auth-dashboard`, or
`--case inventory-dashboard` to select a subset; repeat `--case` for multiple
workloads. The request budget applies to the selected cases.

Public connections use the origin for DNS/TLS and send the worker's domain in
the HTTP Host header. The domain must be configured on the target Fly app.
The control plane uses the private tunnel; timed workload requests use the
public endpoint. Connections are reused within each caller thread. Each
phase creates two caller threads, so its initial connection establishment is
included in latency. There is no excluded warmup; seeding has already exercised
the workers and storage.

`results.json` retains each response's latency, status, validation outcome,
dispatch delay, skipped slots, phase summaries, source hashes, deployment IDs,
verification counts, and cleanup results. Latency starts at the scheduled
arrival for admitted requests. Skipped requests are reported separately and
have no latency sample. Throughput divides successful completions by actual
phase duration, including a final drain when necessary. These small capped
samples describe user-visible latency at the offered load, including network
time; they do not establish saturation throughput or core scaling. Tail
percentiles from 50 or 150 responses are coarse.

Undeployment removes active workers. Their small namespaced state and deployment
history can remain in the store. The runner does not delete shared databases,
restart the app, resize machines, or create Fly infrastructure.

## Verify the runner

```sh
python3 scripts/test-bench-fly-api.py
node --check scripts/fly-api-case-studies/inventory.js
```

The guard tests check control-connection closure and reconnection, the
concurrency ceiling under slow responses, skipped slots, early abort on an
error, response validation, and rejection of excessive
request budgets before accessing credentials or the network. For a local
end-to-end check, point both origins at a disposable `dd_server` and use
`--seconds 1 --rate 2`; this exercises deployment, all three cases, exact
counter verification, and cleanup with six timed requests.
