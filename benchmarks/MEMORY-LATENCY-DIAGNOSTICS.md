# Concurrent memory measurements

## dd-memory-tail-profile-20260908

Raw artifacts: `/home/mewhhaha/dd-memory-tail-profile-20260908`. Each row has 1 alternating paired runs; 2s warmup and 8s timed work. Caller concurrency: 32.

Offered load: saturated caller pool. For fixed-rate runs, latency starts at scheduled arrival and includes caller backlog; all arrivals drain before verification.

Values are medians. Gain is the median candidate/baseline ratio of matched pairs; the range shows every pair, not a confidence interval. Request latency includes all concurrent memory transactions and response validation. CPU time and RSS cover the whole process, including setup, verification, and reopening.

- Baseline binary SHA-256: `3c801e67610a5448d4519e5cb311c42a3f54b1545c2b943d8a427f0beae57f61`; read API: `atomic`; source patch SHA-256: `577209fd4f373213a607496482739852a46752487ef961258db2842475fe3f32`.
- Candidate binary SHA-256: `3c801e67610a5448d4519e5cb311c42a3f54b1545c2b943d8a427f0beae57f61`; read API: `snapshot`; source patch SHA-256: `577209fd4f373213a607496482739852a46752487ef961258db2842475fe3f32`.

| CPUs | Mode / fanout / population / bytes / keys / payload | Requests/s before → after | Transactions/s before → after | Gain (pair range) | p99 ms before → after |
|---:|---|---:|---:|---:|---:|
| 8 | mixed / 16 / 256 / 4096 / 16 / varied | 4,329 → 7,717 | 69,262 → 123,473 | 1.78× (1.78–1.78) | 38.55 → 47.03 |
| 8 | mixed / 16 / 16 / 128 / 1 / repeated | 1,005 → 505 | 16,077 → 8,081 | 0.50× (0.50–0.50) | 157.98 → 713.83 |

Latency below is separated by completed request operation. Values are medians of runs; p99 ranges retain every run. A request waits for its entire memory fanout.

| CPUs | Case | Operation | Mean ms before → after | p99 ms before → after | p99 range ms before → after |
|---:|---|---|---:|---:|---|
| 8 | mixed-16-256-4096-16-varied | read | 5.84 → 1.20 | 34.16 → 4.16 | 34.16–34.16 → 4.16–4.16 |
| 8 | mixed-16-256-4096-16-varied | write | 21.22 → 30.64 | 59.89 → 59.45 | 59.89–59.89 → 59.45–59.45 |
| 8 | mixed-16-16-128 | read | 31.52 → 0.35 | 157.70 → 2.91 | 157.70–157.70 → 2.91–2.91 |
| 8 | mixed-16-16-128 | write | 34.09 → 613.04 | 163.43 → 794.82 | 163.43–163.43 → 794.82–794.82 |

| CPUs | Case | Cache misses % before → after | Commands/commit before → after | CPU seconds before → after | Peak RSS MiB before → after |
|---:|---|---:|---:|---:|---:|
| 8 | mixed-16-256-4096-16-varied | 0.00 → 0.00 | 1.67 → 2.85 | 37.43 → 44.66 | 629.97 → 626.51 |
| 8 | mixed-16-16-128 | 0.00 → 0.00 | 1.00 → 1.00 | 11.65 → 4.95 | 340.89 → 321.95 |

All 4 processes validated responses and exact final state before shutdown and after reopening, covering 212,016 completed writes. Host one-minute load ranged from 8.49 to 18.34.

