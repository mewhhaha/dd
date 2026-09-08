# Concurrent memory measurements

## dd-memory-read-final-cores-20260908

Raw artifacts: `/home/mewhhaha/dd-memory-read-final-cores-20260908`. Each row has 3 alternating paired runs; 2s warmup and 8s timed work. Caller concurrency: 32.

Values are medians. Gain is the median candidate/baseline ratio of matched pairs; the range shows every pair, not a confidence interval. Request latency includes all concurrent memory transactions and response validation. CPU time and RSS cover the whole process, including setup, verification, and reopening.

- Baseline binary SHA-256: `9bf02775e50faf8e23fa9552ba8bc36a9aac68dc73e684b07bf38352db07b810`; source patch SHA-256: `880fa9ef723e6ca20268d29da8625c8e0702d59ed87d7bfa7894621ba345846f`; read API: `atomic`.
- Candidate binary SHA-256: `9bf02775e50faf8e23fa9552ba8bc36a9aac68dc73e684b07bf38352db07b810`; source patch SHA-256: `880fa9ef723e6ca20268d29da8625c8e0702d59ed87d7bfa7894621ba345846f`; read API: `snapshot`.

| CPUs | Mode / fanout / population / bytes / keys / payload | Requests/s baseline → candidate | Transactions/s baseline → candidate | Gain (pair range) | p99 ms baseline → candidate |
|---:|---|---:|---:|---:|---:|
| 1 | mixed / 16 / 256 / 4096 / 16 / varied | 1,681 → 1,916 | 26,897 → 30,648 | 1.14× (1.11–1.14) | 33.77 → 33.27 |
| 1 | read / 16 / 256 / 4096 / 16 / varied | 3,384 → 5,020 | 54,149 → 80,317 | 1.43× (1.10–1.49) | 15.65 → 13.46 |
| 4 | mixed / 16 / 256 / 4096 / 16 / varied | 3,780 → 6,008 | 60,473 → 96,133 | 1.59× (1.48–1.61) | 28.97 → 33.85 |
| 4 | read / 16 / 256 / 4096 / 16 / varied | 10,990 → 13,973 | 175,834 → 223,575 | 1.23× (1.20–1.52) | 6.84 → 5.23 |
| 8 | mixed / 16 / 256 / 4096 / 16 / varied | 5,286 → 9,371 | 84,575 → 149,939 | 1.77× (1.75–2.02) | 32.85 → 40.23 |
| 8 | read / 16 / 256 / 4096 / 16 / varied | 12,788 → 20,129 | 204,604 → 322,070 | 1.57× (1.48–1.58) | 10.34 → 3.95 |

| CPUs | Case | Cache misses % before → after | Commands/commit before → after | CPU seconds before → after | Peak RSS MiB before → after |
|---:|---|---:|---:|---:|---:|
| 1 | mixed-16-256-4096-16-varied | 0.00 → 0.00 | 1.24 → 1.25 | 10.41 → 10.37 | 318.49 → 315.64 |
| 1 | read-16-256-4096-16-varied | 0.00 → 0.00 | 0.00 → 0.00 | 10.88 → 10.87 | 319.94 → 320.66 |
| 4 | mixed-16-256-4096-16-varied | 0.00 → 0.00 | 1.46 → 1.92 | 29.11 → 32.88 | 457.83 → 459.17 |
| 4 | read-16-256-4096-16-varied | 0.00 → 0.00 | 0.00 → 0.00 | 36.48 → 33.01 | 472.70 → 456.57 |
| 8 | mixed-16-256-4096-16-varied | 0.00 → 0.00 | 1.68 → 2.87 | 43.15 → 50.71 | 624.85 → 626.95 |
| 8 | read-16-256-4096-16-varied | 0.00 → 0.00 | 0.00 → 0.00 | 53.72 → 53.98 | 636.52 → 632.66 |

All 36 processes validated responses and exact final state before shutdown and after reopening, covering 1,291,744 completed writes. Host one-minute load ranged from 2.82 to 21.63.

## dd-memory-read-holdouts-20260908

Raw artifacts: `/home/mewhhaha/dd-memory-read-holdouts-20260908`. Each row has 3 alternating paired runs; 3s warmup and 12s timed work. Caller concurrency: 32.

Values are medians. Gain is the median candidate/baseline ratio of matched pairs; the range shows every pair, not a confidence interval. Request latency includes all concurrent memory transactions and response validation. CPU time and RSS cover the whole process, including setup, verification, and reopening.

- Baseline binary SHA-256: `9bf02775e50faf8e23fa9552ba8bc36a9aac68dc73e684b07bf38352db07b810`; source patch SHA-256: `880fa9ef723e6ca20268d29da8625c8e0702d59ed87d7bfa7894621ba345846f`; read API: `atomic`.
- Candidate binary SHA-256: `9bf02775e50faf8e23fa9552ba8bc36a9aac68dc73e684b07bf38352db07b810`; source patch SHA-256: `880fa9ef723e6ca20268d29da8625c8e0702d59ed87d7bfa7894621ba345846f`; read API: `snapshot`.

| CPUs | Mode / fanout / population / bytes / keys / payload | Requests/s baseline → candidate | Transactions/s baseline → candidate | Gain (pair range) | p99 ms baseline → candidate |
|---:|---|---:|---:|---:|---:|
| 8 | mixed / 4 / 1024 / 128 / 1 / repeated | 9,521 → 11,260 | 38,084 → 45,042 | 1.18× (1.17–1.23) | 35.96 → 33.89 |
| 8 | mixed / 16 / 1024 / 128 / 1 / repeated | 6,523 → 9,106 | 104,375 → 145,694 | 1.67× (1.32–1.80) | 39.01 → 47.79 |
| 8 | read / 16 / 16 / 128 / 1 / repeated | 8,348 → 43,150 | 133,568 → 690,407 | 5.21× (5.17–5.22) | 6.12 → 1.80 |
| 8 | mixed / 16 / 16 / 128 / 1 / repeated | 934 → 2,036 | 14,939 → 32,573 | 1.68× (1.01–2.18) | 93.43 → 181.13 |
| 8 | write / 16 / 256 / 4096 / 16 / varied | 1,116 → 537 | 17,854 → 8,590 | 0.86× (0.44–1.19) | 126.95 → 93.60 |
| 8 | read / 16 / 512 / 16384 / 16 / varied | 800 → 845 | 12,800 → 13,524 | 1.06× (1.00–1.08) | 124.68 → 107.43 |

| CPUs | Case | Cache misses % before → after | Commands/commit before → after | CPU seconds before → after | Peak RSS MiB before → after |
|---:|---|---:|---:|---:|---:|
| 8 | mixed-4-1024-128 | 0.00 → 0.00 | 1.91 → 2.07 | 35.49 → 38.42 | 417.41 → 418.41 |
| 8 | mixed-16-1024-128 | 0.00 → 0.00 | 3.51 → 6.24 | 51.74 → 44.84 | 428.28 → 426.45 |
| 8 | read-16-16-128 | 0.00 → 0.00 | 0.00 → 0.00 | 37.06 → 84.57 | 351.11 → 381.88 |
| 8 | mixed-16-16-128 | 0.00 → 0.00 | 1.00 → 1.00 | 14.60 → 22.17 | 343.65 → 352.51 |
| 8 | write-16-256-4096-16-varied | 0.00 → 0.00 | 3.85 → 3.85 | 43.97 → 27.46 | 632.74 → 619.40 |
| 8 | read-16-512-16384-16-varied | 93.02 → 92.86 | 0.00 → 0.00 | 95.76 → 96.38 | 1617.52 → 1605.78 |

All 36 processes validated responses and exact final state before shutdown and after reopening, covering 3,427,340 completed writes. Host one-minute load ranged from 12.40 to 30.84.

