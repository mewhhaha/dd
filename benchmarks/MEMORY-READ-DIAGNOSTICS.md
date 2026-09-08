# Concurrent memory measurements

## dd-memory-read-smoke-20260908

Raw artifacts: `/home/mewhhaha/dd-memory-read-smoke-20260908`. Each row has 1 alternating paired runs; 0.5s warmup and 1s timed work. Caller concurrency: 32.

Values are medians. Gain is the median candidate/baseline ratio of matched pairs; the range shows every pair, not a confidence interval. Request latency includes all concurrent memory transactions and response validation. CPU time and RSS cover the whole process, including setup, verification, and reopening.

- Baseline binary SHA-256: `ce3490329d0c7c60716b1736d5ee4af35b24ef4a7d4167a141602bb08608a69e`; source patch SHA-256: `39afa41b9a4dfc8ee18896b4f531d0919612dd289ccda1a1d9ff5dba6370ee5b`; read API: `atomic`.
- Candidate binary SHA-256: `ce3490329d0c7c60716b1736d5ee4af35b24ef4a7d4167a141602bb08608a69e`; source patch SHA-256: `39afa41b9a4dfc8ee18896b4f531d0919612dd289ccda1a1d9ff5dba6370ee5b`; read API: `snapshot`.

| CPUs | Mode / fanout / population / bytes / keys / payload | Requests/s baseline → candidate | Transactions/s baseline → candidate | Gain (pair range) | p99 ms baseline → candidate |
|---:|---|---:|---:|---:|---:|
| 4 | mixed / 16 / 256 / 4096 / 16 / varied | 3,174 → 5,938 | 50,776 → 95,004 | 1.87× (1.87–1.87) | 46.59 → 31.24 |

| CPUs | Case | Cache misses % before → after | Commands/commit before → after | CPU seconds before → after | Peak RSS MiB before → after |
|---:|---|---:|---:|---:|---:|
| 4 | mixed-16-256-4096-16-varied | 0.00 → 0.00 | 1.45 → 1.85 | 5.46 → 6.12 | 447.73 → 444.74 |

All 2 processes validated responses and exact final state before shutdown and after reopening, covering 20,000 completed writes. Host one-minute load ranged from 2.53 to 5.13.

## dd-memory-read-cores-20260908

Raw artifacts: `/home/mewhhaha/dd-memory-read-cores-20260908`. Each row has 3 alternating paired runs; 2s warmup and 8s timed work. Caller concurrency: 32.

Values are medians. Gain is the median candidate/baseline ratio of matched pairs; the range shows every pair, not a confidence interval. Request latency includes all concurrent memory transactions and response validation. CPU time and RSS cover the whole process, including setup, verification, and reopening.

- Baseline binary SHA-256: `ce3490329d0c7c60716b1736d5ee4af35b24ef4a7d4167a141602bb08608a69e`; source patch SHA-256: `39afa41b9a4dfc8ee18896b4f531d0919612dd289ccda1a1d9ff5dba6370ee5b`; read API: `atomic`.
- Candidate binary SHA-256: `ce3490329d0c7c60716b1736d5ee4af35b24ef4a7d4167a141602bb08608a69e`; source patch SHA-256: `39afa41b9a4dfc8ee18896b4f531d0919612dd289ccda1a1d9ff5dba6370ee5b`; read API: `snapshot`.

| CPUs | Mode / fanout / population / bytes / keys / payload | Requests/s baseline → candidate | Transactions/s baseline → candidate | Gain (pair range) | p99 ms baseline → candidate |
|---:|---|---:|---:|---:|---:|
| 1 | mixed / 16 / 256 / 4096 / 16 / varied | 1,689 → 1,948 | 27,027 → 31,163 | 1.16× (1.13–1.17) | 32.82 → 30.93 |
| 1 | read / 16 / 256 / 4096 / 16 / varied | 3,445 → 4,998 | 55,122 → 79,967 | 1.44× (1.43–1.51) | 15.89 → 13.21 |
| 4 | mixed / 16 / 256 / 4096 / 16 / varied | 4,068 → 6,123 | 65,088 → 97,974 | 1.51× (1.37–1.53) | 26.10 → 32.17 |
| 4 | read / 16 / 256 / 4096 / 16 / varied | 10,461 → 14,680 | 167,382 → 234,882 | 1.40× (1.34–1.45) | 7.30 → 4.92 |
| 8 | mixed / 16 / 256 / 4096 / 16 / varied | 5,401 → 9,449 | 86,418 → 151,178 | 1.78× (1.65–1.78) | 29.36 → 40.70 |
| 8 | read / 16 / 256 / 4096 / 16 / varied | 14,987 → 22,277 | 239,798 → 356,433 | 1.48× (1.45–1.60) | 9.15 → 3.46 |

| CPUs | Case | Cache misses % before → after | Commands/commit before → after | CPU seconds before → after | Peak RSS MiB before → after |
|---:|---|---:|---:|---:|---:|
| 1 | mixed-16-256-4096-16-varied | 0.00 → 0.00 | 1.24 → 1.24 | 10.52 → 10.53 | 321.72 → 321.65 |
| 1 | read-16-256-4096-16-varied | 0.00 → 0.00 | 0.00 → 0.00 | 10.87 → 10.87 | 314.86 → 319.84 |
| 4 | mixed-16-256-4096-16-varied | 0.00 → 0.00 | 1.46 → 1.90 | 30.32 → 33.28 | 456.70 → 454.87 |
| 4 | read-16-256-4096-16-varied | 0.00 → 0.00 | 0.00 → 0.00 | 35.90 → 35.09 | 462.39 → 458.10 |
| 8 | mixed-16-256-4096-16-varied | 0.00 → 0.00 | 1.70 → 2.96 | 45.58 → 51.61 | 630.65 → 630.06 |
| 8 | read-16-256-4096-16-varied | 0.00 → 0.00 | 0.00 → 0.00 | 58.82 → 59.32 | 635.57 → 632.33 |

All 36 processes validated responses and exact final state before shutdown and after reopening, covering 1,344,144 completed writes. Host one-minute load ranged from 3.29 to 20.99.

## dd-memory-read-write-control-20260908

Raw artifacts: `/home/mewhhaha/dd-memory-read-write-control-20260908`. Each row has 3 alternating paired runs; 5s warmup and 20s timed work. Caller concurrency: 32.

Values are medians. Gain is the median candidate/baseline ratio of matched pairs; the range shows every pair, not a confidence interval. Request latency includes all concurrent memory transactions and response validation. CPU time and RSS cover the whole process, including setup, verification, and reopening.

- Baseline binary SHA-256: `9bf02775e50faf8e23fa9552ba8bc36a9aac68dc73e684b07bf38352db07b810`; source patch SHA-256: `880fa9ef723e6ca20268d29da8625c8e0702d59ed87d7bfa7894621ba345846f`; read API: `atomic`.
- Candidate binary SHA-256: `9bf02775e50faf8e23fa9552ba8bc36a9aac68dc73e684b07bf38352db07b810`; source patch SHA-256: `880fa9ef723e6ca20268d29da8625c8e0702d59ed87d7bfa7894621ba345846f`; read API: `atomic`.

| CPUs | Mode / fanout / population / bytes / keys / payload | Requests/s baseline → candidate | Transactions/s baseline → candidate | Gain (pair range) | p99 ms baseline → candidate |
|---:|---|---:|---:|---:|---:|
| 8 | write / 16 / 256 / 4096 / 16 / varied | 516 → 501 | 8,250 → 8,008 | 0.90× (0.88–0.97) | 105.80 → 141.26 |

| CPUs | Case | Cache misses % before → after | Commands/commit before → after | CPU seconds before → after | Peak RSS MiB before → after |
|---:|---|---:|---:|---:|---:|
| 8 | write-16-256-4096-16-varied | 0.00 → 0.00 | 3.91 → 3.85 | 40.59 → 40.06 | 623.19 → 627.39 |

All 6 processes validated responses and exact final state before shutdown and after reopening, covering 1,235,472 completed writes. Host one-minute load ranged from 10.31 to 39.28.

