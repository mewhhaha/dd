# Concurrent memory measurements

## dd-fanout-native-final-cores-20260908

Raw artifacts: `/home/mewhhaha/dd-fanout-native-final-cores-20260908`. Each row has 3 alternating paired runs; 2s warmup and 8s timed work. Caller concurrency: 32.

Values are medians. Gain is the median candidate/baseline ratio of matched pairs; the range shows every pair, not a confidence interval. Request latency includes all concurrent memory transactions and response validation. CPU time and RSS cover the whole process, including setup, verification, and reopening.

- Baseline binary SHA-256: `f8f948707a07171cae9537cfceb40ebb00373d81d7ea4e55f09a0883ac88fb44`; source patch SHA-256: `a65bf99b692e335512a74f601907ae528f6a78e55ddb59e3eb4e0cda5707389d`.
- Candidate binary SHA-256: `fe9ab8087e2f68999d152e700376e311e94439acb8ea57db00f9e794f804c645`; source patch SHA-256: `3d05db133ea8dc1cb8b88bbaef552521162217324dfb9853cc952cd5bc79ee76`.

| CPUs | Mode / fanout / population / bytes / keys / payload | Requests/s before → after | Transactions/s before → after | Gain (pair range) | p99 ms before → after |
|---:|---|---:|---:|---:|---:|
| 1 | mixed / 16 / 256 / 4096 / 16 / varied | 664 → 1,654 | 10,625 → 26,466 | 2.49× (2.45–2.54) | 75.29 → 33.73 |
| 1 | read / 16 / 256 / 4096 / 16 / varied | 927 → 3,453 | 14,832 → 55,253 | 3.70× (3.66–3.89) | 46.67 → 15.51 |
| 4 | mixed / 16 / 256 / 4096 / 16 / varied | 2,483 → 4,202 | 39,730 → 67,233 | 1.70× (1.65–1.71) | 38.07 → 24.88 |
| 4 | read / 16 / 256 / 4096 / 16 / varied | 3,825 → 10,901 | 61,205 → 174,417 | 2.80× (2.71–2.88) | 23.09 → 6.94 |
| 8 | mixed / 16 / 256 / 4096 / 16 / varied | 3,402 → 5,585 | 54,435 → 89,355 | 1.64× (1.54–1.68) | 39.26 → 28.24 |
| 8 | read / 16 / 256 / 4096 / 16 / varied | 5,277 → 16,135 | 84,435 → 258,160 | 3.03× (2.78–3.12) | 31.59 → 8.74 |

| CPUs | Case | Cache misses % before → after | Commands/commit before → after | CPU seconds before → after | Peak RSS MiB before → after |
|---:|---|---:|---:|---:|---:|
| 1 | mixed-16-256-4096-16-varied | 0.00 → 0.00 | 1.19 → 1.24 | 10.81 → 10.44 | 342.31 → 318.46 |
| 1 | read-16-256-4096-16-varied | 0.00 → 0.00 | 0.00 → 0.00 | 10.95 → 10.87 | 342.40 → 317.83 |
| 4 | mixed-16-256-4096-16-varied | 0.00 → 0.00 | 1.37 → 1.46 | 37.29 → 30.27 | 523.64 → 457.01 |
| 4 | read-16-256-4096-16-varied | 0.00 → 0.00 | 0.00 → 0.00 | 39.44 → 36.74 | 552.66 → 468.68 |
| 8 | mixed-16-256-4096-16-varied | 0.00 → 0.00 | 1.55 → 1.68 | 58.11 → 44.97 | 736.95 → 628.14 |
| 8 | read-16-256-4096-16-varied | 0.00 → 0.00 | 0.00 → 0.00 | 61.88 → 60.60 | 762.07 → 636.82 |

All 36 processes validated responses and exact final state before shutdown and after reopening, covering 839,920 completed writes. Host one-minute load ranged from 10.34 to 20.85.

## dd-fanout-native-final-holdouts-20260908

Raw artifacts: `/home/mewhhaha/dd-fanout-native-final-holdouts-20260908`. Each row has 3 alternating paired runs; 2s warmup and 8s timed work. Caller concurrency: 32.

Values are medians. Gain is the median candidate/baseline ratio of matched pairs; the range shows every pair, not a confidence interval. Request latency includes all concurrent memory transactions and response validation. CPU time and RSS cover the whole process, including setup, verification, and reopening.

- Baseline binary SHA-256: `f8f948707a07171cae9537cfceb40ebb00373d81d7ea4e55f09a0883ac88fb44`; source patch SHA-256: `a65bf99b692e335512a74f601907ae528f6a78e55ddb59e3eb4e0cda5707389d`.
- Candidate binary SHA-256: `fe9ab8087e2f68999d152e700376e311e94439acb8ea57db00f9e794f804c645`; source patch SHA-256: `3d05db133ea8dc1cb8b88bbaef552521162217324dfb9853cc952cd5bc79ee76`.

| CPUs | Mode / fanout / population / bytes / keys / payload | Requests/s before → after | Transactions/s before → after | Gain (pair range) | p99 ms before → after |
|---:|---|---:|---:|---:|---:|
| 8 | mixed / 4 / 1024 / 128 / 1 / repeated | 8,746 → 10,950 | 34,985 → 43,800 | 1.15× (1.12–1.25) | 39.86 → 31.51 |
| 8 | mixed / 16 / 1024 / 128 / 1 / repeated | 6,296 → 4,417 | 100,729 → 70,670 | 0.70× (0.65–0.83) | 34.97 → 68.05 |
| 8 | write / 16 / 256 / 4096 / 16 / varied | 507 → 508 | 8,105 → 8,133 | 1.00× (0.96–1.00) | 91.28 → 97.29 |

| CPUs | Case | Cache misses % before → after | Commands/commit before → after | CPU seconds before → after | Peak RSS MiB before → after |
|---:|---|---:|---:|---:|---:|
| 8 | mixed-4-1024-128 | 0.00 → 0.00 | 1.90 → 1.91 | 24.16 → 27.15 | 436.72 → 419.75 |
| 8 | mixed-16-1024-128 | 0.00 → 0.00 | 3.68 → 3.66 | 34.76 → 25.11 | 442.02 → 422.04 |
| 8 | write-16-256-4096-16-varied | 0.00 → 0.00 | 3.88 → 3.87 | 26.23 → 17.77 | 722.79 → 591.24 |

All 18 processes validated responses and exact final state before shutdown and after reopening, covering 1,482,852 completed writes. Host one-minute load ranged from 15.31 to 31.23.
