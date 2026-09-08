# Concurrent memory measurements

## dd-fanout-direct-trial-20260908

Raw artifacts: `/home/mewhhaha/dd-fanout-direct-trial-20260908`. Each row has 3 alternating paired runs; 2s warmup and 6s timed work. Caller concurrency: 32.

Values are medians. Gain is the median candidate/baseline ratio of matched pairs; the range shows every pair, not a confidence interval. Request latency includes all concurrent memory transactions and response validation. CPU time and RSS cover the whole process, including setup, verification, and reopening.

- Baseline binary SHA-256: `f8f948707a07171cae9537cfceb40ebb00373d81d7ea4e55f09a0883ac88fb44`; source patch SHA-256: `a65bf99b692e335512a74f601907ae528f6a78e55ddb59e3eb4e0cda5707389d`.
- Candidate binary SHA-256: `0b6ec8cbb226250f0cc926bffb0ac9d315003952838f0247bd5898bc3687f538`; source patch SHA-256: `e7051b69cb3244b315a0f1c2efc80a66836b238884a465566b85c9ce47cc7e32`.

| CPUs | Mode / fanout / population / bytes / keys / payload | Requests/s before → after | Transactions/s before → after | Gain (pair range) | p99 ms before → after |
|---:|---|---:|---:|---:|---:|
| 1 | mixed / 16 / 256 / 4096 / 16 / varied | 672 → 634 | 10,753 → 10,146 | 1.03× (0.94–1.04) | 82.51 → 80.67 |
| 1 | read / 16 / 256 / 4096 / 16 / varied | 756 → 925 | 12,089 → 14,807 | 1.15× (0.90–1.22) | 72.14 → 60.09 |
| 1 | mixed / 4 / 1024 / 128 / 1 / repeated | 4,848 → 4,850 | 19,390 → 19,400 | 0.99× (0.94–1.05) | 14.21 → 14.99 |
| 8 | mixed / 16 / 256 / 4096 / 16 / varied | 3,206 → 3,248 | 51,294 → 51,965 | 1.01× (1.01–1.11) | 41.03 → 40.77 |
| 8 | read / 16 / 256 / 4096 / 16 / varied | 4,876 → 5,217 | 78,018 → 83,468 | 1.02× (1.00–1.15) | 39.15 → 32.76 |
| 8 | mixed / 4 / 1024 / 128 / 1 / repeated | 21,750 → 20,750 | 87,000 → 82,999 | 1.00× (0.95–2.44) | 12.97 → 13.90 |

| CPUs | Case | Cache misses % before → after | Commands/commit before → after | CPU seconds before → after | Peak RSS MiB before → after |
|---:|---|---:|---:|---:|---:|
| 1 | mixed-16-256-4096-16-varied | 0.00 → 0.00 | 1.19 → 1.19 | 8.83 → 9.03 | 338.69 → 339.82 |
| 1 | read-16-256-4096-16-varied | 0.00 → 0.00 | 0.00 → 0.00 | 9.03 → 9.01 | 341.23 → 345.31 |
| 1 | mixed-4-1024-128 | 0.00 → 0.00 | 1.05 → 1.05 | 8.03 → 7.96 | 248.09 → 244.53 |
| 8 | mixed-16-256-4096-16-varied | 0.00 → 0.00 | 1.52 → 1.53 | 46.25 → 45.27 | 739.96 → 742.82 |
| 8 | read-16-256-4096-16-varied | 0.00 → 0.00 | 0.00 → 0.00 | 47.37 → 48.28 | 756.26 → 766.96 |
| 8 | mixed-4-1024-128 | 0.00 → 0.00 | 1.69 → 1.70 | 39.21 → 37.99 | 436.23 → 439.01 |

All 36 processes validated responses and exact final state before shutdown and after reopening, covering 736,528 completed writes. Host one-minute load ranged from 4.87 to 23.91.

## dd-fanout-native-cores-20260908

Raw artifacts: `/home/mewhhaha/dd-fanout-native-cores-20260908`. Each row has 3 alternating paired runs; 2s warmup and 8s timed work. Caller concurrency: 32.

Values are medians. Gain is the median candidate/baseline ratio of matched pairs; the range shows every pair, not a confidence interval. Request latency includes all concurrent memory transactions and response validation. CPU time and RSS cover the whole process, including setup, verification, and reopening.

- Baseline binary SHA-256: `f8f948707a07171cae9537cfceb40ebb00373d81d7ea4e55f09a0883ac88fb44`; source patch SHA-256: `a65bf99b692e335512a74f601907ae528f6a78e55ddb59e3eb4e0cda5707389d`.
- Candidate binary SHA-256: `1fbb1890d638cf2e09a3aa115a33601839dece0546b6cffd49a2de9de88d71b3`; source patch SHA-256: `f20842a4cff518dde3f3ed7598e32378c85f80b34bdc83a6a1bfa6047671045f`.

| CPUs | Mode / fanout / population / bytes / keys / payload | Requests/s before → after | Transactions/s before → after | Gain (pair range) | p99 ms before → after |
|---:|---|---:|---:|---:|---:|
| 1 | mixed / 16 / 256 / 4096 / 16 / varied | 645 → 1,072 | 10,316 → 17,146 | 1.67× (1.57–2.43) | 83.61 → 60.77 |
| 2 | mixed / 16 / 256 / 4096 / 16 / varied | 1,095 → 1,363 | 17,512 → 21,805 | 1.41× (1.20–1.91) | 69.95 → 63.49 |
| 4 | mixed / 16 / 256 / 4096 / 16 / varied | 1,509 → 1,710 | 24,145 → 27,356 | 1.13× (1.09–1.63) | 74.13 → 69.06 |
| 8 | mixed / 16 / 256 / 4096 / 16 / varied | 2,010 → 2,122 | 32,158 → 33,951 | 1.06× (0.77–1.06) | 82.74 → 87.05 |

| CPUs | Case | Cache misses % before → after | Commands/commit before → after | CPU seconds before → after | Peak RSS MiB before → after |
|---:|---|---:|---:|---:|---:|
| 1 | mixed-16-256-4096-16-varied | 0.00 → 0.00 | 1.22 → 1.27 | 10.62 → 7.52 | 339.52 → 317.74 |
| 2 | mixed-16-256-4096-16-varied | 0.00 → 0.00 | 1.30 → 1.37 | 17.19 → 10.44 | 412.21 → 365.43 |
| 4 | mixed-16-256-4096-16-varied | 0.00 → 0.00 | 1.46 → 1.50 | 25.73 → 15.05 | 521.70 → 459.85 |
| 8 | mixed-16-256-4096-16-varied | 0.00 → 0.00 | 1.71 → 1.74 | 39.25 → 23.46 | 739.64 → 622.07 |

All 24 processes validated responses and exact final state before shutdown and after reopening, covering 636,512 completed writes. Host one-minute load ranged from 3.56 to 23.94.

## dd-fanout-native-small-self-control-20260908

Raw artifacts: `/home/mewhhaha/dd-fanout-native-small-self-control-20260908`. Each row has 3 alternating paired runs; 5s warmup and 20s timed work. Caller concurrency: 32.

Values are medians. Gain is the median candidate/baseline ratio of matched pairs; the range shows every pair, not a confidence interval. Request latency includes all concurrent memory transactions and response validation. CPU time and RSS cover the whole process, including setup, verification, and reopening.

- Baseline binary SHA-256: `f8f948707a07171cae9537cfceb40ebb00373d81d7ea4e55f09a0883ac88fb44`; source patch SHA-256: `a65bf99b692e335512a74f601907ae528f6a78e55ddb59e3eb4e0cda5707389d`.
- Candidate binary SHA-256: `f8f948707a07171cae9537cfceb40ebb00373d81d7ea4e55f09a0883ac88fb44`; source patch SHA-256: `a65bf99b692e335512a74f601907ae528f6a78e55ddb59e3eb4e0cda5707389d`.

| CPUs | Mode / fanout / population / bytes / keys / payload | Requests/s before → after | Transactions/s before → after | Gain (pair range) | p99 ms before → after |
|---:|---|---:|---:|---:|---:|
| 8 | mixed / 16 / 1024 / 128 / 1 / repeated | 5,191 → 4,789 | 83,061 → 76,631 | 0.92× (0.52–0.94) | 41.92 → 47.13 |

| CPUs | Case | Cache misses % before → after | Commands/commit before → after | CPU seconds before → after | Peak RSS MiB before → after |
|---:|---|---:|---:|---:|---:|
| 8 | mixed-16-1024-128 | 0.00 → 0.00 | 3.71 → 3.75 | 74.43 → 67.40 | 439.57 → 439.55 |

All 6 processes validated responses and exact final state before shutdown and after reopening, covering 1,434,656 completed writes. Host one-minute load ranged from 3.25 to 33.27.

## dd-fanout-native-small-long-20260908

Raw artifacts: `/home/mewhhaha/dd-fanout-native-small-long-20260908`. Each row has 3 alternating paired runs; 5s warmup and 20s timed work. Caller concurrency: 32.

Values are medians. Gain is the median candidate/baseline ratio of matched pairs; the range shows every pair, not a confidence interval. Request latency includes all concurrent memory transactions and response validation. CPU time and RSS cover the whole process, including setup, verification, and reopening.

- Baseline binary SHA-256: `f8f948707a07171cae9537cfceb40ebb00373d81d7ea4e55f09a0883ac88fb44`; source patch SHA-256: `a65bf99b692e335512a74f601907ae528f6a78e55ddb59e3eb4e0cda5707389d`.
- Candidate binary SHA-256: `fe9ab8087e2f68999d152e700376e311e94439acb8ea57db00f9e794f804c645`; source patch SHA-256: `3d05db133ea8dc1cb8b88bbaef552521162217324dfb9853cc952cd5bc79ee76`.

| CPUs | Mode / fanout / population / bytes / keys / payload | Requests/s before → after | Transactions/s before → after | Gain (pair range) | p99 ms before → after |
|---:|---|---:|---:|---:|---:|
| 8 | mixed / 16 / 1024 / 128 / 1 / repeated | 5,221 → 5,375 | 83,542 → 85,998 | 1.03× (0.91–1.05) | 41.45 → 41.26 |

| CPUs | Case | Cache misses % before → after | Commands/commit before → after | CPU seconds before → after | Peak RSS MiB before → after |
|---:|---|---:|---:|---:|---:|
| 8 | mixed-16-1024-128 | 0.00 → 0.00 | 3.72 → 3.75 | 73.40 → 68.87 | 442.66 → 427.22 |

All 6 processes validated responses and exact final state before shutdown and after reopening, covering 1,293,584 completed writes. Host one-minute load ranged from 32.87 to 38.15.
