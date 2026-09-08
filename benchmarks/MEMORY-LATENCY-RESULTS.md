# Concurrent memory measurements

## dd-memory-tail-distributed-20260908

Raw artifacts: `/home/mewhhaha/dd-memory-tail-distributed-20260908`. Each row has 3 alternating paired runs; 3s warmup and 12s timed work. Caller concurrency: 32.

Offered load: 3000 scheduled requests/s. For fixed-rate runs, latency starts at scheduled arrival and includes caller backlog; all arrivals drain before verification.

Values are medians. Gain is the median candidate/baseline ratio of matched pairs; the range shows every pair, not a confidence interval. Request latency includes all concurrent memory transactions and response validation. CPU time and RSS cover the whole process, including setup, verification, and reopening.

- Baseline binary SHA-256: `916734e9d3f57011fe2eb4f38b3d1e68b5981259cf26d79574e2eef83c733d75`; read API: `atomic`; source patch SHA-256: `0d9c1d135d248cbe96b78a1436ea7cb244420a77b31378691ba4ade8214ec818`.
- Candidate binary SHA-256: `916734e9d3f57011fe2eb4f38b3d1e68b5981259cf26d79574e2eef83c733d75`; read API: `snapshot`; source patch SHA-256: `0d9c1d135d248cbe96b78a1436ea7cb244420a77b31378691ba4ade8214ec818`.

| CPUs | Mode / fanout / population / bytes / keys / payload | Requests/s before → after | Transactions/s before → after | Gain (pair range) | p99 ms before → after |
|---:|---|---:|---:|---:|---:|
| 8 | mixed / 16 / 256 / 4096 / 16 / varied | 2,041 → 2,983 | 32,659 → 47,733 | 1.46× (1.00–1.57) | 5552.77 → 621.82 |

Latency below is separated by completed request operation. Values are medians of runs; p99 ranges retain every run. A request waits for its entire memory fanout.

| CPUs | Case | Operation | Mean ms before → after | p99 ms before → after | p99 range ms before → after |
|---:|---|---|---:|---:|---|
| 8 | mixed-16-256-4096-16-varied | read | 2780.93 → 73.61 | 5543.69 → 615.20 | 164.68–6847.61 → 60.24–685.00 |
| 8 | mixed-16-256-4096-16-varied | write | 2817.00 → 121.37 | 5584.13 → 719.48 | 186.83–6890.54 → 174.28–793.03 |

| CPUs | Case | Cache misses % before → after | Commands/commit before → after | CPU seconds before → after | Peak RSS MiB before → after |
|---:|---|---:|---:|---:|---:|
| 8 | mixed-16-256-4096-16-varied | 0.00 → 0.00 | 1.74 → 2.38 | 44.55 → 33.60 | 631.12 → 610.66 |

All 6 processes validated responses and exact final state before shutdown and after reopening, covering 432,000 completed writes. Host one-minute load ranged from 6.18 to 31.96.

## dd-memory-tail-hot-20260908

Raw artifacts: `/home/mewhhaha/dd-memory-tail-hot-20260908`. Each row has 3 alternating paired runs; 3s warmup and 12s timed work. Caller concurrency: 32.

Offered load: 200 scheduled requests/s. For fixed-rate runs, latency starts at scheduled arrival and includes caller backlog; all arrivals drain before verification.

Values are medians. Gain is the median candidate/baseline ratio of matched pairs; the range shows every pair, not a confidence interval. Request latency includes all concurrent memory transactions and response validation. CPU time and RSS cover the whole process, including setup, verification, and reopening.

- Baseline binary SHA-256: `916734e9d3f57011fe2eb4f38b3d1e68b5981259cf26d79574e2eef83c733d75`; read API: `atomic`; source patch SHA-256: `0d9c1d135d248cbe96b78a1436ea7cb244420a77b31378691ba4ade8214ec818`.
- Candidate binary SHA-256: `916734e9d3f57011fe2eb4f38b3d1e68b5981259cf26d79574e2eef83c733d75`; read API: `snapshot`; source patch SHA-256: `0d9c1d135d248cbe96b78a1436ea7cb244420a77b31378691ba4ade8214ec818`.

| CPUs | Mode / fanout / population / bytes / keys / payload | Requests/s before → after | Transactions/s before → after | Gain (pair range) | p99 ms before → after |
|---:|---|---:|---:|---:|---:|
| 8 | mixed / 16 / 16 / 128 / 1 / repeated | 200 → 200 | 3,197 → 3,197 | 1.00× (1.00–1.00) | 27.36 → 25.39 |

Latency below is separated by completed request operation. Values are medians of runs; p99 ranges retain every run. A request waits for its entire memory fanout.

| CPUs | Case | Operation | Mean ms before → after | p99 ms before → after | p99 range ms before → after |
|---:|---|---|---:|---:|---|
| 8 | mixed-16-16-128 | read | 3.55 → 1.64 | 22.56 → 2.58 | 19.69–23.63 → 2.13–2.78 |
| 8 | mixed-16-16-128 | write | 17.29 → 18.57 | 34.54 → 37.57 | 33.78–38.26 → 36.11–96.29 |

| CPUs | Case | Cache misses % before → after | Commands/commit before → after | CPU seconds before → after | Peak RSS MiB before → after |
|---:|---|---:|---:|---:|---:|
| 8 | mixed-16-16-128 | 0.00 → 0.00 | 1.05 → 1.05 | 3.46 → 2.59 | 238.02 → 197.55 |

All 6 processes validated responses and exact final state before shutdown and after reopening, covering 28,800 completed writes. Host one-minute load ranged from 12.06 to 29.88.

