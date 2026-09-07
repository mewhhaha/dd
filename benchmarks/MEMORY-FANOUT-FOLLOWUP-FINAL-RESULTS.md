# Concurrent memory measurements

## dd-fanout-next-unicode-large-cores-20260907

Raw artifacts: `/home/mewhhaha/dd-fanout-next-unicode-large-cores-20260907`. Each row has 3 alternating paired runs; 1s warmup and 6s timed work. Caller concurrency: 32.

Values are medians. Gain is the median candidate/baseline ratio of matched pairs; the range shows every pair, not a confidence interval. Request latency includes all concurrent memory transactions and response validation. CPU time and RSS cover the whole process, including setup, verification, and reopening.

- Baseline binary SHA-256: `fb61a7e21e7bb521f8f47051b7b54cb6c922b378be22e03373eed668bab1f963`; source patch SHA-256: `09c4407c1ac538b68d71b6141dd00218b37a8972b193aef76a0b8ceabc75a507`.
- Candidate binary SHA-256: `eee87e4736cb2a9456d7b7094e769828fba41f5dbbbc19e665bebe8ad98a6105`; source patch SHA-256: `352a82a6c6af5980f7d9b9e5c914ea4ae11eb7df030dc5c0ca4efe6b66bb3b4f`.

| CPUs | Mode / fanout / population / bytes / keys / payload | Requests/s before → after | Transactions/s before → after | Gain (pair range) | p99 ms before → after |
|---:|---|---:|---:|---:|---:|
| 1 | mixed / 16 / 256 / 4096 / 16 / varied | 536 → 624 | 8,571 → 9,987 | 1.21× (1.14–1.22) | 94.99 → 95.63 |
| 2 | mixed / 16 / 256 / 4096 / 16 / varied | 898 → 1,125 | 14,364 → 18,002 | 1.23× (0.95–1.69) | 77.91 → 67.42 |
| 4 | mixed / 16 / 256 / 4096 / 16 / varied | 1,331 → 1,509 | 21,293 → 24,137 | 1.13× (1.13–1.35) | 77.62 → 74.08 |
| 8 | mixed / 16 / 256 / 4096 / 16 / varied | 1,653 → 2,008 | 26,442 → 32,125 | 1.16× (1.12–1.58) | 86.97 → 83.27 |
| 16 | mixed / 16 / 256 / 4096 / 16 / varied | 1,866 → 1,983 | 29,853 → 31,733 | 1.06× (1.05–1.07) | 83.10 → 85.22 |

| CPUs | Case | Cache misses % before → after | Commands/commit before → after | CPU seconds before → after | Peak RSS MiB before → after |
|---:|---|---:|---:|---:|---:|
| 1 | mixed-16-256-4096-16-varied | 10.06 → 0.00 | 1.21 → 1.21 | 7.92 → 7.71 | 353.82 → 337.80 |
| 2 | mixed-16-256-4096-16-varied | 10.04 → 0.00 | 1.26 → 1.31 | 13.97 → 12.30 | 441.88 → 411.30 |
| 4 | mixed-16-256-4096-16-varied | 10.02 → 0.00 | 1.41 → 1.46 | 23.66 → 18.83 | 578.73 → 516.08 |
| 8 | mixed-16-256-4096-16-varied | 10.03 → 0.00 | 1.62 → 1.68 | 34.63 → 28.78 | 838.50 → 723.45 |
| 16 | mixed-16-256-4096-16-varied | 10.00 → 0.00 | 1.66 → 1.70 | 47.93 → 36.74 | 835.38 → 727.54 |

All 30 processes validated responses and exact final state before shutdown and after reopening, covering 466,832 completed writes. Host one-minute load ranged from 3.89 to 24.61.

## dd-fanout-next-unicode-holdouts-20260907

Raw artifacts: `/home/mewhhaha/dd-fanout-next-unicode-holdouts-20260907`. Each row has 3 alternating paired runs; 1s warmup and 6s timed work. Caller concurrency: 32.

Values are medians. Gain is the median candidate/baseline ratio of matched pairs; the range shows every pair, not a confidence interval. Request latency includes all concurrent memory transactions and response validation. CPU time and RSS cover the whole process, including setup, verification, and reopening.

- Baseline binary SHA-256: `fb61a7e21e7bb521f8f47051b7b54cb6c922b378be22e03373eed668bab1f963`; source patch SHA-256: `09c4407c1ac538b68d71b6141dd00218b37a8972b193aef76a0b8ceabc75a507`.
- Candidate binary SHA-256: `eee87e4736cb2a9456d7b7094e769828fba41f5dbbbc19e665bebe8ad98a6105`; source patch SHA-256: `352a82a6c6af5980f7d9b9e5c914ea4ae11eb7df030dc5c0ca4efe6b66bb3b4f`.

| CPUs | Mode / fanout / population / bytes / keys / payload | Requests/s before → after | Transactions/s before → after | Gain (pair range) | p99 ms before → after |
|---:|---|---:|---:|---:|---:|
| 8 | read / 16 / 256 / 4096 / 16 / varied | 2,974 → 4,671 | 47,578 → 74,730 | 1.58× (1.57–1.64) | 66.41 → 35.82 |
| 8 | write / 16 / 256 / 4096 / 16 / varied | 817 → 1,135 | 13,064 → 18,155 | 1.36× (0.90–1.41) | 88.13 → 50.84 |
| 8 | mixed / 4 / 1024 / 128 / 1 / repeated | 20,436 → 14,697 | 81,744 → 58,786 | 0.75× (0.69–0.99) | 13.90 → 28.05 |
| 8 | mixed / 16 / 1024 / 128 / 1 / repeated | 5,195 → 5,231 | 83,115 → 83,699 | 1.01× (0.98–1.05) | 41.10 → 41.16 |

| CPUs | Case | Cache misses % before → after | Commands/commit before → after | CPU seconds before → after | Peak RSS MiB before → after |
|---:|---|---:|---:|---:|---:|
| 8 | read-16-256-4096-16-varied | 0.00 → 0.00 | 0.00 → 0.00 | 41.99 → 42.19 | 899.75 → 762.70 |
| 8 | write-16-256-4096-16-varied | 100.00 → 0.00 | 3.26 → 3.70 | 35.33 → 34.05 | 698.46 → 737.17 |
| 8 | mixed-4-1024-128 | 10.02 → 0.00 | 1.68 → 1.84 | 33.49 → 26.30 | 438.11 → 433.93 |
| 8 | mixed-16-1024-128 | 10.00 → 0.00 | 3.72 → 3.75 | 22.92 → 22.59 | 442.53 → 432.77 |

All 24 processes validated responses and exact final state before shutdown and after reopening, covering 1,151,172 completed writes. Host one-minute load ranged from 18.32 to 30.40.
