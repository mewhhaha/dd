# Concurrent memory measurements

## dd-fanout-next-baseline-self-control-20260907

Raw artifacts: `/home/mewhhaha/dd-fanout-next-baseline-self-control-20260907`. Each row has 3 alternating paired runs; 1s warmup and 6s timed work. Caller concurrency: 32.

Values are medians. Gain is the median candidate/baseline ratio of matched pairs; the range shows every pair, not a confidence interval. Request latency includes all concurrent memory transactions and response validation. CPU time and RSS cover the whole process, including setup, verification, and reopening.

- Baseline binary SHA-256: `fb61a7e21e7bb521f8f47051b7b54cb6c922b378be22e03373eed668bab1f963`; source patch SHA-256: `09c4407c1ac538b68d71b6141dd00218b37a8972b193aef76a0b8ceabc75a507`.
- Candidate binary SHA-256: `fb61a7e21e7bb521f8f47051b7b54cb6c922b378be22e03373eed668bab1f963`; source patch SHA-256: `09c4407c1ac538b68d71b6141dd00218b37a8972b193aef76a0b8ceabc75a507`.

| CPUs | Mode / fanout / population / bytes / keys / payload | Requests/s before → after | Transactions/s before → after | Gain (pair range) | p99 ms before → after |
|---:|---|---:|---:|---:|---:|
| 8 | mixed / 4 / 1024 / 128 / 1 / repeated | 10,167 → 10,872 | 40,666 → 43,486 | 1.02× (0.90–1.07) | 33.89 → 32.37 |

| CPUs | Case | Cache misses % before → after | Commands/commit before → after | CPU seconds before → after | Peak RSS MiB before → after |
|---:|---|---:|---:|---:|---:|
| 8 | mixed-4-1024-128 | 10.01 → 10.02 | 1.85 → 1.66 | 20.88 → 22.54 | 436.32 → 437.55 |

All 6 processes validated responses and exact final state before shutdown and after reopening, covering 241,640 completed writes. Host one-minute load ranged from 3.24 to 17.73.

## dd-fanout-next-small-long-20260907

Raw artifacts: `/home/mewhhaha/dd-fanout-next-small-long-20260907`. Each row has 3 alternating paired runs; 5s warmup and 20s timed work. Caller concurrency: 32.

Values are medians. Gain is the median candidate/baseline ratio of matched pairs; the range shows every pair, not a confidence interval. Request latency includes all concurrent memory transactions and response validation. CPU time and RSS cover the whole process, including setup, verification, and reopening.

- Baseline binary SHA-256: `fb61a7e21e7bb521f8f47051b7b54cb6c922b378be22e03373eed668bab1f963`; source patch SHA-256: `09c4407c1ac538b68d71b6141dd00218b37a8972b193aef76a0b8ceabc75a507`.
- Candidate binary SHA-256: `eee87e4736cb2a9456d7b7094e769828fba41f5dbbbc19e665bebe8ad98a6105`; source patch SHA-256: `352a82a6c6af5980f7d9b9e5c914ea4ae11eb7df030dc5c0ca4efe6b66bb3b4f`.

| CPUs | Mode / fanout / population / bytes / keys / payload | Requests/s before → after | Transactions/s before → after | Gain (pair range) | p99 ms before → after |
|---:|---|---:|---:|---:|---:|
| 8 | mixed / 4 / 1024 / 128 / 1 / repeated | 10,117 → 10,340 | 40,467 → 41,358 | 0.92× (0.60–1.15) | 33.77 → 33.12 |

| CPUs | Case | Cache misses % before → after | Commands/commit before → after | CPU seconds before → after | Peak RSS MiB before → after |
|---:|---|---:|---:|---:|---:|
| 8 | mixed-4-1024-128 | 10.00 → 0.00 | 1.94 → 1.93 | 68.20 → 65.50 | 445.67 → 439.82 |

All 6 processes validated responses and exact final state before shutdown and after reopening, covering 726,984 completed writes. Host one-minute load ranged from 5.24 to 32.62.
