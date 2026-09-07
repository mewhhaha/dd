# Concurrent memory measurements

## dd-fanout-next-cores-20260907

Raw artifacts: `/home/mewhhaha/dd-fanout-next-cores-20260907`. Each row has 3 alternating paired runs; 1s warmup and 6s timed work. Caller concurrency: 32.

Values are medians. Gain is the median candidate/baseline ratio of matched pairs; the range shows every pair, not a confidence interval. Request latency includes all concurrent memory transactions and response validation. CPU time and RSS cover the whole process, including setup, verification, and reopening.

- Baseline binary SHA-256: `fb61a7e21e7bb521f8f47051b7b54cb6c922b378be22e03373eed668bab1f963`; source patch SHA-256: `09c4407c1ac538b68d71b6141dd00218b37a8972b193aef76a0b8ceabc75a507`.
- Candidate binary SHA-256: `5dd1b626068487e9311eed72c74e7d54564c82159353525864b1302b2de61144`; source patch SHA-256: `20082bff80b871b7ba80fe454268c963777368b647956ff404c444256e10417b`.

| CPUs | Mode / fanout / population / bytes / keys / payload | Requests/s before → after | Transactions/s before → after | Gain (pair range) | p99 ms before → after |
|---:|---|---:|---:|---:|---:|
| 1 | mixed / 16 / 1024 / 128 / 1 / repeated | 1,433 → 1,446 | 22,935 → 23,133 | 1.01× (1.00–1.13) | 49.87 → 49.86 |
| 2 | mixed / 16 / 1024 / 128 / 1 / repeated | 2,145 → 2,177 | 34,313 → 34,831 | 1.03× (0.84–1.10) | 47.45 → 48.28 |
| 4 | mixed / 16 / 1024 / 128 / 1 / repeated | 3,427 → 3,529 | 54,839 → 56,456 | 1.04× (1.00–1.38) | 42.38 → 41.95 |
| 8 | mixed / 16 / 1024 / 128 / 1 / repeated | 5,108 → 4,941 | 81,728 → 79,050 | 0.97× (0.60–0.99) | 41.20 → 42.68 |
| 16 | mixed / 16 / 1024 / 128 / 1 / repeated | 5,155 → 5,451 | 82,475 → 87,214 | 1.00× (0.79–1.22) | 39.65 → 39.83 |

| CPUs | Case | Cache misses % before → after | Commands/commit before → after | CPU seconds before → after | Peak RSS MiB before → after |
|---:|---|---:|---:|---:|---:|
| 1 | mixed-16-1024-128 | 10.01 → 0.00 | 1.34 → 1.34 | 6.03 → 5.82 | 248.34 → 246.70 |
| 2 | mixed-16-1024-128 | 10.00 → 0.00 | 1.68 → 1.69 | 8.91 → 8.71 | 276.01 → 273.33 |
| 4 | mixed-16-1024-128 | 10.00 → 0.00 | 2.44 → 2.45 | 14.62 → 13.91 | 342.47 → 333.03 |
| 8 | mixed-16-1024-128 | 10.00 → 0.00 | 3.73 → 3.79 | 23.04 → 20.86 | 442.86 → 436.05 |
| 16 | mixed-16-1024-128 | 10.00 → 0.00 | 3.82 → 3.95 | 31.83 → 32.66 | 440.58 → 437.76 |

All 30 processes validated responses and exact final state before shutdown and after reopening, covering 1,275,872 completed writes. Host one-minute load ranged from 2.43 to 31.13.

## dd-fanout-next-large-cores-20260907

Raw artifacts: `/home/mewhhaha/dd-fanout-next-large-cores-20260907`. Each row has 3 alternating paired runs; 1s warmup and 6s timed work. Caller concurrency: 32.

Values are medians. Gain is the median candidate/baseline ratio of matched pairs; the range shows every pair, not a confidence interval. Request latency includes all concurrent memory transactions and response validation. CPU time and RSS cover the whole process, including setup, verification, and reopening.

- Baseline binary SHA-256: `fb61a7e21e7bb521f8f47051b7b54cb6c922b378be22e03373eed668bab1f963`; source patch SHA-256: `09c4407c1ac538b68d71b6141dd00218b37a8972b193aef76a0b8ceabc75a507`.
- Candidate binary SHA-256: `5dd1b626068487e9311eed72c74e7d54564c82159353525864b1302b2de61144`; source patch SHA-256: `20082bff80b871b7ba80fe454268c963777368b647956ff404c444256e10417b`.

| CPUs | Mode / fanout / population / bytes / keys / payload | Requests/s before → after | Transactions/s before → after | Gain (pair range) | p99 ms before → after |
|---:|---|---:|---:|---:|---:|
| 1 | mixed / 16 / 256 / 4096 / 16 / varied | 562 → 664 | 8,987 → 10,619 | 1.18× (1.12–1.28) | 91.02 → 80.83 |
| 2 | mixed / 16 / 256 / 4096 / 16 / varied | 887 → 1,083 | 14,188 → 17,331 | 1.25× (1.22–1.39) | 77.61 → 67.62 |
| 4 | mixed / 16 / 256 / 4096 / 16 / varied | 1,338 → 1,492 | 21,407 → 23,879 | 1.13× (1.09–1.52) | 75.70 → 71.63 |
| 8 | mixed / 16 / 256 / 4096 / 16 / varied | 1,846 → 2,060 | 29,534 → 32,960 | 1.12× (1.09–1.45) | 79.34 → 81.40 |
| 16 | mixed / 16 / 256 / 4096 / 16 / varied | 1,865 → 1,981 | 29,843 → 31,694 | 1.06× (0.94–1.07) | 84.93 → 87.30 |

| CPUs | Case | Cache misses % before → after | Commands/commit before → after | CPU seconds before → after | Peak RSS MiB before → after |
|---:|---|---:|---:|---:|---:|
| 1 | mixed-16-256-4096-16-varied | 10.05 → 0.00 | 1.20 → 1.21 | 7.93 → 7.79 | 360.09 → 337.14 |
| 2 | mixed-16-256-4096-16-varied | 10.05 → 0.00 | 1.25 → 1.30 | 13.93 → 12.53 | 457.09 → 410.19 |
| 4 | mixed-16-256-4096-16-varied | 10.01 → 0.00 | 1.42 → 1.46 | 23.50 → 18.41 | 588.43 → 524.74 |
| 8 | mixed-16-256-4096-16-varied | 10.01 → 0.00 | 1.65 → 1.70 | 36.56 → 28.81 | 840.12 → 733.11 |
| 16 | mixed-16-256-4096-16-varied | 10.01 → 0.00 | 1.66 → 1.69 | 46.78 → 35.17 | 835.61 → 732.46 |

All 30 processes validated responses and exact final state before shutdown and after reopening, covering 518,464 completed writes. Host one-minute load ranged from 6.34 to 25.65.

## dd-fanout-next-holdouts-20260907

Raw artifacts: `/home/mewhhaha/dd-fanout-next-holdouts-20260907`. Each row has 3 alternating paired runs; 1s warmup and 6s timed work. Caller concurrency: 32.

Values are medians. Gain is the median candidate/baseline ratio of matched pairs; the range shows every pair, not a confidence interval. Request latency includes all concurrent memory transactions and response validation. CPU time and RSS cover the whole process, including setup, verification, and reopening.

- Baseline binary SHA-256: `fb61a7e21e7bb521f8f47051b7b54cb6c922b378be22e03373eed668bab1f963`; source patch SHA-256: `09c4407c1ac538b68d71b6141dd00218b37a8972b193aef76a0b8ceabc75a507`.
- Candidate binary SHA-256: `5dd1b626068487e9311eed72c74e7d54564c82159353525864b1302b2de61144`; source patch SHA-256: `20082bff80b871b7ba80fe454268c963777368b647956ff404c444256e10417b`.

| CPUs | Mode / fanout / population / bytes / keys / payload | Requests/s before → after | Transactions/s before → after | Gain (pair range) | p99 ms before → after |
|---:|---|---:|---:|---:|---:|
| 8 | read / 16 / 1024 / 128 / 1 / repeated | 19,321 → 20,934 | 309,137 → 334,949 | 1.09× (1.08–1.10) | 3.91 → 3.55 |
| 8 | write / 16 / 1024 / 4096 / 1 / varied | 1,961 → 2,020 | 31,379 → 32,321 | 1.01× (0.72–1.10) | 31.04 → 29.84 |
| 8 | read / 16 / 256 / 4096 / 16 / varied | 2,985 → 4,697 | 47,761 → 75,160 | 1.57× (1.56–1.61) | 60.18 → 39.50 |
| 8 | mixed / 16 / 256 / 4096 / 16 / varied | 2,116 → 3,098 | 33,858 → 49,565 | 1.46× (1.45–1.49) | 58.18 → 43.22 |
| 8 | write / 16 / 256 / 4096 / 16 / varied | 881 → 1,153 | 14,089 → 18,443 | 1.39× (1.30–1.56) | 61.77 → 46.74 |
| 8 | mixed / 4 / 1024 / 128 / 1 / repeated | 23,136 → 17,305 | 92,542 → 69,219 | 0.76× (0.75–1.03) | 11.18 → 27.53 |
| 8 | read / 16 / 8192 / 128 / 1 / repeated | 8,728 → 8,765 | 139,655 → 140,245 | 0.99× (0.98–1.02) | 11.40 → 11.11 |
| 8 | mixed / 16 / 8192 / 128 / 1 / repeated | 6,292 → 6,195 | 100,674 → 99,127 | 0.97× (0.95–1.02) | 22.35 → 24.65 |

| CPUs | Case | Cache misses % before → after | Commands/commit before → after | CPU seconds before → after | Peak RSS MiB before → after |
|---:|---|---:|---:|---:|---:|
| 8 | read-16-1024-128 | 0.00 → 0.00 | 0.00 → 0.00 | 39.26 → 38.98 | 452.52 → 443.12 |
| 8 | write-16-1024-4096-1-varied | 100.00 → 0.00 | 5.95 → 6.07 | 28.46 → 27.57 | 509.14 → 556.75 |
| 8 | read-16-256-4096-16-varied | 0.00 → 0.00 | 0.00 → 0.00 | 42.11 → 41.68 | 898.90 → 754.64 |
| 8 | mixed-16-256-4096-16-varied | 10.11 → 0.00 | 1.40 → 1.53 | 41.73 → 39.88 | 852.72 → 738.94 |
| 8 | write-16-256-4096-16-varied | 100.00 → 0.00 | 3.24 → 3.69 | 38.95 → 34.28 | 699.36 → 729.65 |
| 8 | mixed-4-1024-128 | 10.02 → 0.00 | 1.56 → 1.66 | 36.22 → 29.27 | 442.38 → 437.21 |
| 8 | read-16-8192-128 | 100.00 → 100.00 | 0.00 → 0.00 | 41.00 → 41.30 | 539.46 → 537.20 |
| 8 | mixed-16-8192-128 | 100.00 → 100.00 | 2.07 → 2.27 | 41.02 → 41.71 | 537.81 → 542.91 |

All 48 processes validated responses and exact final state before shutdown and after reopening, covering 2,877,324 completed writes. Host one-minute load ranged from 18.29 to 29.70.

## dd-fanout-next-fixed-transactions-1-20260907

Raw artifacts: `/home/mewhhaha/dd-fanout-next-fixed-transactions-1-20260907`. Each row has 3 alternating paired runs; 1s warmup and 6s timed work. Caller concurrency: 32.

Values are medians. Gain is the median candidate/baseline ratio of matched pairs; the range shows every pair, not a confidence interval. Request latency includes all concurrent memory transactions and response validation. CPU time and RSS cover the whole process, including setup, verification, and reopening.

- Baseline binary SHA-256: `fb61a7e21e7bb521f8f47051b7b54cb6c922b378be22e03373eed668bab1f963`; source patch SHA-256: `09c4407c1ac538b68d71b6141dd00218b37a8972b193aef76a0b8ceabc75a507`.
- Candidate binary SHA-256: `5dd1b626068487e9311eed72c74e7d54564c82159353525864b1302b2de61144`; source patch SHA-256: `20082bff80b871b7ba80fe454268c963777368b647956ff404c444256e10417b`.

| CPUs | Mode / fanout / population / bytes / keys / payload | Requests/s before → after | Transactions/s before → after | Gain (pair range) | p99 ms before → after |
|---:|---|---:|---:|---:|---:|
| 8 | mixed / 1 / 1024 / 128 / 1 / repeated | 24,435 → 27,564 | 24,435 → 27,564 | 1.02× (1.01–1.13) | 21.40 → 21.31 |

| CPUs | Case | Cache misses % before → after | Commands/commit before → after | CPU seconds before → after | Peak RSS MiB before → after |
|---:|---|---:|---:|---:|---:|
| 8 | mixed-1-1024-128 | 10.02 → 0.00 | 1.13 → 1.11 | 26.35 → 26.55 | 451.18 → 449.46 |

All 6 processes validated responses and exact final state before shutdown and after reopening, covering 118,405 completed writes. Host one-minute load ranged from 22.08 to 27.92.

## dd-fanout-next-fixed-transactions-4-20260907

Raw artifacts: `/home/mewhhaha/dd-fanout-next-fixed-transactions-4-20260907`. Each row has 3 alternating paired runs; 1s warmup and 6s timed work. Caller concurrency: 8.

Values are medians. Gain is the median candidate/baseline ratio of matched pairs; the range shows every pair, not a confidence interval. Request latency includes all concurrent memory transactions and response validation. CPU time and RSS cover the whole process, including setup, verification, and reopening.

- Baseline binary SHA-256: `fb61a7e21e7bb521f8f47051b7b54cb6c922b378be22e03373eed668bab1f963`; source patch SHA-256: `09c4407c1ac538b68d71b6141dd00218b37a8972b193aef76a0b8ceabc75a507`.
- Candidate binary SHA-256: `5dd1b626068487e9311eed72c74e7d54564c82159353525864b1302b2de61144`; source patch SHA-256: `20082bff80b871b7ba80fe454268c963777368b647956ff404c444256e10417b`.

| CPUs | Mode / fanout / population / bytes / keys / payload | Requests/s before → after | Transactions/s before → after | Gain (pair range) | p99 ms before → after |
|---:|---|---:|---:|---:|---:|
| 8 | mixed / 4 / 1024 / 128 / 1 / repeated | 3,330 → 3,417 | 13,322 → 13,669 | 1.01× (0.99–1.07) | 28.62 → 27.65 |

| CPUs | Case | Cache misses % before → after | Commands/commit before → after | CPU seconds before → after | Peak RSS MiB before → after |
|---:|---|---:|---:|---:|---:|
| 8 | mixed-4-1024-128 | 10.00 → 0.00 | 1.12 → 1.12 | 9.34 → 8.95 | 272.86 → 270.06 |

All 6 processes validated responses and exact final state before shutdown and after reopening, covering 57,392 completed writes. Host one-minute load ranged from 19.49 to 22.08.

## dd-fanout-next-fixed-transactions-16-20260907

Raw artifacts: `/home/mewhhaha/dd-fanout-next-fixed-transactions-16-20260907`. Each row has 3 alternating paired runs; 1s warmup and 6s timed work. Caller concurrency: 2.

Values are medians. Gain is the median candidate/baseline ratio of matched pairs; the range shows every pair, not a confidence interval. Request latency includes all concurrent memory transactions and response validation. CPU time and RSS cover the whole process, including setup, verification, and reopening.

- Baseline binary SHA-256: `fb61a7e21e7bb521f8f47051b7b54cb6c922b378be22e03373eed668bab1f963`; source patch SHA-256: `09c4407c1ac538b68d71b6141dd00218b37a8972b193aef76a0b8ceabc75a507`.
- Candidate binary SHA-256: `5dd1b626068487e9311eed72c74e7d54564c82159353525864b1302b2de61144`; source patch SHA-256: `20082bff80b871b7ba80fe454268c963777368b647956ff404c444256e10417b`.

| CPUs | Mode / fanout / population / bytes / keys / payload | Requests/s before → after | Transactions/s before → after | Gain (pair range) | p99 ms before → after |
|---:|---|---:|---:|---:|---:|
| 8 | mixed / 16 / 1024 / 128 / 1 / repeated | 791 → 788 | 12,656 → 12,606 | 1.01× (0.87–1.01) | 27.40 → 28.89 |

| CPUs | Case | Cache misses % before → after | Commands/commit before → after | CPU seconds before → after | Peak RSS MiB before → after |
|---:|---|---:|---:|---:|---:|
| 8 | mixed-16-1024-128 | 10.00 → 0.00 | 1.14 → 1.13 | 5.92 → 5.72 | 246.22 → 243.18 |

All 6 processes validated responses and exact final state before shutdown and after reopening, covering 52,880 completed writes. Host one-minute load ranged from 17.69 to 19.81.

## dd-fanout-next-small-followup-20260907

Raw artifacts: `/home/mewhhaha/dd-fanout-next-small-followup-20260907`. Each row has 5 alternating paired runs; 1s warmup and 6s timed work. Caller concurrency: 32.

Values are medians. Gain is the median candidate/baseline ratio of matched pairs; the range shows every pair, not a confidence interval. Request latency includes all concurrent memory transactions and response validation. CPU time and RSS cover the whole process, including setup, verification, and reopening.

- Baseline binary SHA-256: `fb61a7e21e7bb521f8f47051b7b54cb6c922b378be22e03373eed668bab1f963`; source patch SHA-256: `09c4407c1ac538b68d71b6141dd00218b37a8972b193aef76a0b8ceabc75a507`.
- Candidate binary SHA-256: `5dd1b626068487e9311eed72c74e7d54564c82159353525864b1302b2de61144`; source patch SHA-256: `20082bff80b871b7ba80fe454268c963777368b647956ff404c444256e10417b`.

| CPUs | Mode / fanout / population / bytes / keys / payload | Requests/s before → after | Transactions/s before → after | Gain (pair range) | p99 ms before → after |
|---:|---|---:|---:|---:|---:|
| 8 | mixed / 4 / 1024 / 128 / 1 / repeated | 10,186 → 10,299 | 40,745 → 41,195 | 1.03× (0.74–1.09) | 32.85 → 32.71 |
| 8 | mixed / 16 / 1024 / 128 / 1 / repeated | 5,226 → 5,239 | 83,624 → 83,827 | 1.01× (0.83–1.11) | 39.56 → 40.53 |

| CPUs | Case | Cache misses % before → after | Commands/commit before → after | CPU seconds before → after | Peak RSS MiB before → after |
|---:|---|---:|---:|---:|---:|
| 8 | mixed-4-1024-128 | 10.00 → 0.00 | 1.93 → 1.93 | 20.90 → 20.25 | 433.31 → 429.35 |
| 8 | mixed-16-1024-128 | 10.00 → 0.00 | 3.75 → 3.73 | 22.98 → 22.37 | 444.37 → 435.95 |

All 20 processes validated responses and exact final state before shutdown and after reopening, covering 1,020,400 completed writes. Host one-minute load ranged from 6.45 to 30.77.
