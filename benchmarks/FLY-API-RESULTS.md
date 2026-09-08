# Fly public API case studies — 2026-09-08

The current runtime is deployed to `dd-private-8956e096` in Amsterdam, using
the existing **one shared CPU, 1 GiB RAM, and volume**. No machines or volumes
were added and no resource limits were increased. With the user's approval,
the old store was cleared rather than converted. That reset includes prior
KV, memory, and persisted deployment-token state. The private API credential
in Fly secrets was retained.

All six example workers were republished from current source: `chat`,
`vite-effect`, `vite-effect-auth`, `vite-hono`, `vite-react-router`, and
`vite-react-router-rsc`. The five public roots returned HTTP 200; the auth
backend retains its private service binding. Final readiness is healthy,
worker restoration has no failures, and only those six workers remain active.

## Public HTTPS results

All **600 timed requests succeeded**, with no validation errors or skipped
request slots. Each row is one ten-second phase, capped at the offered rate
and two concurrent callers. Each case was measured at 5 and 15 requests/s.
Requests traverse public TLS and Fly's proxy; control-plane deployment and
verification calls use a private WireGuard tunnel.

| Case | Offered requests/s | Completed | Achieved requests/s | Median ms | p95 ms | p99 ms |
|---|---:|---:|---:|---:|---:|---:|
| Durable rate limiter | 5 | 50 | 5.00 | 37.74 | 42.57 | 86.87 |
| Durable rate limiter | 15 | 150 | 15.00 | 37.03 | 39.71 | 86.93 |
| Auth dashboard | 5 | 50 | 5.00 | 38.54 | 50.41 | 93.99 |
| Auth dashboard | 15 | 150 | 15.00 | 38.59 | 41.80 | 90.46 |
| Eight-memory inventory dashboard | 5 | 50 | 5.00 | 36.52 | 39.21 | 90.17 |
| Eight-memory inventory dashboard | 15 | 150 | 15.00 | 36.33 | 39.22 | 84.05 |

The rate limiter performs one durable count/window update per request across
sixteen clients. The auth dashboard calls a separate worker through a service
binding, updates durable session state, and reads a KV-backed user. It uses the
repository's synthetic auth fixture, not password or cryptographic credential
verification. The inventory dashboard reads eight independent memories with
`Promise.all`, returning exact stock and price values from a 32-product pool.

Verification confirmed **400 durable updates** through exact per-client
counters, and validated all **1,600 inventory snapshots** in their responses.
Seeding and counter verification are outside the timed phases. This was not a
crash-recovery test on the live app. All four temporary benchmark workers were
undeployed. Small namespaced benchmark records and deployment history remain
in the store; no shared database was deleted during benchmark cleanup.

## Interpretation and limitations

At 15 requests/s, all three cases delivered p95 below 42 ms. This establishes
successful behavior at a modest offered load, **not maximum throughput or
core scaling**. The 15 requests/s rate was deliberately imposed by the client.
The inventory case therefore exercised 120 snapshot reads/s at that rate;
it did not determine the service's snapshot-read capacity.

Latency includes client scheduling, TLS, network, Fly routing, the entire
worker operation, and response validation. Connections are reused within each
phase, but initial connection establishment is included. The first request
was the slowest in every phase; in each 15 requests/s phase the first two
requests were the slowest. No startup samples were discarded. With only 50
or 150 responses per row, p99 is coarse and is strongly affected by those
initial requests.

A separate ten-request readiness probe collected afterward had a median of
48.42 ms. It was not simultaneous with the workloads and is not subtracted
to estimate server processing time. These measurements do not isolate server
CPU or storage latency from the public network and client.

The initial runner completed and verified the rate-limiter case, then found
an expired private control connection during cleanup. Its exit cleanup
successfully undeployed that worker, but the run correctly remains marked
incomplete. The driver was fixed to close control connections explicitly,
with a local HTTP regression test. The remaining two cases ran separately;
the rate limiter was not repeated. A development attempt between those runs
failed locally while constructing a header, before sending any workload
request. All three invocation records are retained. The total timed workload
remained 600 requests, and none failed at the API.

## Deployment and evidence

The deployed service image was remotely built from commit
`f49691de2e4626458e076c3bf551b1d45c5ad2b5`:

```text
registry.fly.io/dd-private-8956e096:deployment-01M20FJBHPAHK4K0FH1SABTNMB
sha256:af89d892f083d04717c40d25df59f65a690675d03a39785dec195bb4ef6ce817
```

Machine: `83099eb7367328`; volume: `vol_4qlqeg0j15jdxp6r`.
The previously built image was reused for the successful fresh-store rollout;
no second remote compilation was needed. The worker bundles were rebuilt to
remove their obsolete `tvar` calls before publication.

The [runner guide](FLY-API.md) documents limits, workloads, and reproduction.
The five local guard tests pass, including real HTTP control reconnection,
concurrency admission, error abort, response validation, and budget rejection.
An earlier local end-to-end run exercised all three cases, exact counters, and
cleanup using six timed requests.

`just check` passes on the final tree: 407 Rust tests, four existing ignored
tests, all five API-driver tests, Clippy with warnings denied, formatting,
JavaScript, TypeScript, naming, and vendored-source checks.

Raw measurements and deployment evidence are under
`/home/mewhhaha/dd-fly-api-20260908/`. The two workload result files are
`live-results/results.json` and `live-remaining-final-results/results.json`;
the zero-workload development failure is `live-remaining-results/results.json`.
The evidence archive omits credentials, old database backups, and application
bundles containing unrelated state. It includes the raw benchmark samples,
cleanup outcomes, deployment IDs, image/machine details, final health status,
driver source, and check logs.

Archive: `/home/mewhhaha/dd-fly-api-results-20260908.tar.gz`
(53,154 bytes, 23 files). SHA-256:
`a7c803cbad34510d49b4306340b1cbbe963b9d0eabae05e6301e5a7b4131ccd8`.
Its internal manifest hashes the included evidence files; those hashes were
verified by reading the completed archive.
