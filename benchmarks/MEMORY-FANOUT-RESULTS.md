# Concurrent memory measurements

## dd-fanout-core-paired-20260907

Raw artifacts: `/home/mewhhaha/dd-fanout-core-paired-20260907`. Each row has 3 alternating paired runs; 1s warmup and 6s timed work. Caller concurrency: 4 per allowed CPU.

Values are medians. Gain is the median candidate/baseline ratio of matched pairs; the range shows every pair, not a confidence interval. Request latency includes all concurrent memory transactions and response validation. CPU time and RSS cover the whole process, including setup, verification, and reopening.

- Baseline binary SHA-256: `48fd745090f50ed4cea5fd5317d010e27e778b208dd5ecebeb6835c1373f034a`; source patch SHA-256: `ebe0e72a4e213b3330f5473ae287268ef7534dcaa60192a78df3cdbdbab948b8`.
- Candidate binary SHA-256: `8e122a07ac13863805012913a9be94514647df56e96b56f95ee99fdafa69731c`; source patch SHA-256: `87235f8655cc17830f1966eb1e741dc9403ac1d5d1fecd23829af88be9476630`.

| CPUs | Mode / fanout / population / bytes | Requests/s before → after | Transactions/s before → after | Gain (pair range) | p99 ms before → after |
|---:|---|---:|---:|---:|---:|
| 1 | read / 16 / 1024 / 128 | 2,068 → 3,092 | 33,096 → 49,480 | 1.29× (1.08–1.50) | 7.41 → 5.42 |
| 1 | mixed / 16 / 1024 / 128 | 880 → 1,162 | 14,080 → 18,584 | 1.31× (1.30–1.33) | 21.35 → 17.93 |
| 1 | write / 16 / 1024 / 128 | 176 → 179 | 2,819 → 2,867 | 1.02× (1.01–1.06) | 47.29 → 42.09 |
| 1 | mixed / 4 / 1024 / 128 | 2,803 → 3,109 | 11,212 → 12,437 | 1.13× (1.11–1.20) | 9.04 → 10.56 |
| 2 | read / 16 / 1024 / 128 | 2,206 → 1,917 | 35,288 → 30,671 | 1.10× (0.72–1.28) | 11.96 → 13.59 |
| 2 | mixed / 16 / 1024 / 128 | 1,240 → 1,724 | 19,846 → 27,588 | 1.40× (1.30–1.41) | 29.48 → 26.52 |
| 2 | write / 16 / 1024 / 128 | 322 → 305 | 5,144 → 4,882 | 0.92× (0.92–1.04) | 44.91 → 45.78 |
| 2 | mixed / 4 / 1024 / 128 | 2,918 → 3,737 | 11,673 → 14,948 | 1.26× (0.94–1.30) | 14.96 → 13.13 |
| 4 | read / 16 / 1024 / 128 | 1,833 → 2,297 | 29,330 → 36,746 | 1.11× (0.72–1.32) | 23.06 → 18.23 |
| 4 | mixed / 16 / 1024 / 128 | 2,065 → 1,577 | 33,043 → 25,238 | 1.14× (0.76–1.63) | 37.26 → 43.38 |
| 4 | write / 16 / 1024 / 128 | 477 → 462 | 7,632 → 7,389 | 0.93× (0.88–1.12) | 64.57 → 62.61 |
| 4 | mixed / 4 / 1024 / 128 | 2,856 → 3,486 | 11,425 → 13,946 | 1.24× (1.22–1.29) | 23.92 → 23.10 |
| 8 | read / 16 / 1024 / 128 | 3,743 → 4,546 | 59,887 → 72,730 | 1.21× (1.17–1.52) | 23.91 → 20.51 |
| 8 | mixed / 16 / 1024 / 128 | 1,126 → 2,263 | 18,010 → 36,203 | 2.01× (1.88–2.16) | 91.17 → 61.14 |
| 8 | write / 16 / 1024 / 128 | 693 → 737 | 11,090 → 11,786 | 1.04× (1.02–1.14) | 89.70 → 78.90 |
| 8 | mixed / 4 / 1024 / 128 | 5,353 → 6,548 | 21,414 → 26,193 | 1.22× (1.04–1.32) | 39.54 → 28.16 |
| 16 | read / 16 / 1024 / 128 | 11,974 → 15,221 | 191,584 → 243,528 | 1.25× (1.23–1.40) | 16.96 → 11.88 |
| 16 | mixed / 16 / 1024 / 128 | 5,154 → 5,529 | 82,463 → 88,465 | 1.27× (1.07–1.34) | 47.68 → 53.46 |
| 16 | write / 16 / 1024 / 128 | 2,366 → 2,938 | 37,849 → 47,008 | 1.11× (1.11–1.24) | 48.83 → 38.19 |
| 16 | mixed / 4 / 1024 / 128 | 14,157 → 13,367 | 56,628 → 53,468 | 1.10× (0.94–1.28) | 34.60 → 39.38 |

| CPUs | Case | Cache misses % before → after | Commands/commit before → after | CPU seconds before → after | Peak RSS MiB before → after |
|---:|---|---:|---:|---:|---:|
| 1 | read-16-1024-128 | 0.00 → 0.00 | 0.00 → 0.00 | 6.60 → 6.92 | 247.63 → 250.22 |
| 1 | mixed-16-1024-128 | 96.80 → 10.01 | 1.20 → 1.22 | 7.42 → 7.32 | 245.84 → 243.62 |
| 1 | write-16-1024-128 | 100.00 → 100.00 | 1.43 → 1.42 | 6.12 → 5.50 | 243.42 → 243.86 |
| 1 | mixed-4-1024-128 | 96.13 → 10.00 | 1.05 → 1.05 | 7.00 → 6.56 | 245.97 → 247.06 |
| 2 | read-16-1024-128 | 0.00 → 0.00 | 0.00 → 0.00 | 7.48 → 6.88 | 277.63 → 274.46 |
| 2 | mixed-16-1024-128 | 96.81 → 10.01 | 1.26 → 1.32 | 8.57 → 8.50 | 281.05 → 279.57 |
| 2 | write-16-1024-128 | 100.00 → 100.00 | 1.86 → 1.88 | 7.89 → 6.71 | 275.91 → 271.69 |
| 2 | mixed-4-1024-128 | 96.18 → 10.01 | 1.06 → 1.06 | 8.55 → 8.55 | 276.70 → 274.85 |
| 4 | read-16-1024-128 | 0.00 → 0.00 | 0.00 → 0.00 | 9.78 → 9.76 | 331.81 → 330.15 |
| 4 | mixed-16-1024-128 | 96.42 → 10.09 | 1.45 → 1.37 | 13.25 → 11.70 | 337.14 → 340.05 |
| 4 | write-16-1024-128 | 100.00 → 100.00 | 2.69 → 2.95 | 10.53 → 9.66 | 326.93 → 327.33 |
| 4 | mixed-4-1024-128 | 96.25 → 10.01 | 1.07 → 1.08 | 10.23 → 10.02 | 327.77 → 328.17 |
| 8 | read-16-1024-128 | 0.00 → 0.00 | 0.00 → 0.00 | 19.08 → 19.25 | 457.15 → 450.01 |
| 8 | mixed-16-1024-128 | 96.21 → 10.15 | 1.27 → 1.67 | 14.17 → 17.32 | 435.93 → 444.55 |
| 8 | write-16-1024-128 | 100.00 → 100.00 | 4.63 → 5.01 | 15.09 → 14.45 | 429.03 → 421.33 |
| 8 | mixed-4-1024-128 | 96.41 → 10.03 | 1.34 → 1.25 | 15.77 → 15.68 | 434.90 → 433.86 |
| 16 | read-16-1024-128 | 0.00 → 0.00 | 0.00 → 0.00 | 54.39 → 53.86 | 664.93 → 683.32 |
| 16 | mixed-16-1024-128 | 78.23 → 10.05 | 2.78 → 3.25 | 46.65 → 35.88 | 645.58 → 653.55 |
| 16 | write-16-1024-128 | 100.00 → 100.00 | 12.06 → 12.09 | 40.51 → 42.19 | 647.26 → 630.27 |
| 16 | mixed-4-1024-128 | 92.17 → 10.00 | 2.66 → 2.53 | 43.33 → 33.77 | 638.59 → 634.50 |

All 120 processes validated responses and exact final state before shutdown and after reopening, covering 4,206,400 completed writes. Host one-minute load ranged from 14.18 to 50.23.

## dd-fanout-holdouts-20260907

Raw artifacts: `/home/mewhhaha/dd-fanout-holdouts-20260907`. Each row has 3 alternating paired runs; 1s warmup and 6s timed work. Caller concurrency: 4 per allowed CPU.

Values are medians. Gain is the median candidate/baseline ratio of matched pairs; the range shows every pair, not a confidence interval. Request latency includes all concurrent memory transactions and response validation. CPU time and RSS cover the whole process, including setup, verification, and reopening.

- Baseline binary SHA-256: `48fd745090f50ed4cea5fd5317d010e27e778b208dd5ecebeb6835c1373f034a`; source patch SHA-256: `ebe0e72a4e213b3330f5473ae287268ef7534dcaa60192a78df3cdbdbab948b8`.
- Candidate binary SHA-256: `8e122a07ac13863805012913a9be94514647df56e96b56f95ee99fdafa69731c`; source patch SHA-256: `87235f8655cc17830f1966eb1e741dc9403ac1d5d1fecd23829af88be9476630`.

| CPUs | Mode / fanout / population / bytes | Requests/s before → after | Transactions/s before → after | Gain (pair range) | p99 ms before → after |
|---:|---|---:|---:|---:|---:|
| 8 | read / 4 / 1024 / 128 | 17,437 → 17,050 | 69,747 → 68,199 | 0.94× (0.51–0.98) | 5.64 → 6.35 |
| 8 | write / 4 / 1024 / 128 | 1,190 → 1,253 | 4,759 → 5,012 | 1.05× (1.02–1.16) | 50.43 → 53.10 |
| 8 | read / 16 / 1024 / 4096 | 2,681 → 4,282 | 42,903 → 68,518 | 1.29× (1.00–1.60) | 30.73 → 22.35 |
| 8 | mixed / 16 / 1024 / 4096 | 948 → 1,950 | 15,172 → 31,207 | 2.01× (1.92–2.06) | 94.84 → 66.29 |
| 8 | write / 16 / 1024 / 4096 | 589 → 702 | 9,426 → 11,224 | 1.19× (0.97–1.20) | 111.23 → 85.66 |
| 8 | read / 16 / 8192 / 128 | 1,533 → 1,510 | 24,526 → 24,160 | 0.99× (0.91–1.08) | 68.40 → 75.15 |
| 8 | mixed / 16 / 8192 / 128 | 1,061 → 1,333 | 16,974 → 21,320 | 1.09× (1.09–2.44) | 90.38 → 80.19 |
| 8 | read / 1 / 1 / 128 | 6,756 → 7,689 | 6,756 → 7,689 | 1.02× (0.94–1.14) | 12.12 → 10.71 |
| 8 | write / 1 / 1 / 128 | 184 → 192 | 184 → 192 | 0.97× (0.94–1.09) | 207.30 → 209.01 |

| CPUs | Case | Cache misses % before → after | Commands/commit before → after | CPU seconds before → after | Peak RSS MiB before → after |
|---:|---|---:|---:|---:|---:|
| 8 | read-4-1024-128 | 0.00 → 0.00 | 0.00 → 0.00 | 17.54 → 17.96 | 437.39 → 438.84 |
| 8 | write-4-1024-128 | 100.00 → 100.00 | 1.91 → 1.94 | 11.13 → 10.58 | 414.46 → 411.04 |
| 8 | read-16-1024-4096 | 0.00 → 0.00 | 0.00 → 0.00 | 18.84 → 20.54 | 514.34 → 534.80 |
| 8 | mixed-16-1024-4096 | 96.37 → 10.24 | 1.23 → 1.46 | 13.98 → 18.38 | 480.96 → 523.16 |
| 8 | write-16-1024-4096 | 100.00 → 100.00 | 3.55 → 4.43 | 16.58 → 15.59 | 511.86 → 474.72 |
| 8 | read-16-8192-128 | 100.00 → 100.00 | 0.00 → 0.00 | 17.63 → 16.56 | 544.84 → 520.67 |
| 8 | mixed-16-8192-128 | 100.00 → 100.00 | 1.26 → 1.30 | 17.94 → 17.95 | 510.77 → 510.64 |
| 8 | read-1-1-128 | 0.00 → 0.00 | 0.00 → 0.00 | 7.21 → 6.66 | 334.56 → 327.52 |
| 8 | write-1-1-128 | 100.00 → 100.00 | 1.00 → 1.00 | 1.46 → 1.44 | 212.12 → 211.12 |

All 54 processes validated responses and exact final state before shutdown and after reopening, covering 837,888 completed writes. Host one-minute load ranged from 12.07 to 55.66.

## dd-fanout-fixed-callers-20260907

Raw artifacts: `/home/mewhhaha/dd-fanout-fixed-callers-20260907`. Each row has 3 alternating paired runs; 1s warmup and 6s timed work. Caller concurrency: 32.

Values are medians. Gain is the median candidate/baseline ratio of matched pairs; the range shows every pair, not a confidence interval. Request latency includes all concurrent memory transactions and response validation. CPU time and RSS cover the whole process, including setup, verification, and reopening.

- Baseline binary SHA-256: `48fd745090f50ed4cea5fd5317d010e27e778b208dd5ecebeb6835c1373f034a`; source patch SHA-256: `ebe0e72a4e213b3330f5473ae287268ef7534dcaa60192a78df3cdbdbab948b8`.
- Candidate binary SHA-256: `8e122a07ac13863805012913a9be94514647df56e96b56f95ee99fdafa69731c`; source patch SHA-256: `87235f8655cc17830f1966eb1e741dc9403ac1d5d1fecd23829af88be9476630`.

| CPUs | Mode / fanout / population / bytes | Requests/s before → after | Transactions/s before → after | Gain (pair range) | p99 ms before → after |
|---:|---|---:|---:|---:|---:|
| 1 | mixed / 16 / 1024 / 128 | 786 → 1,004 | 12,575 → 16,065 | 1.28× (1.19–1.30) | 72.39 → 63.01 |
| 2 | mixed / 16 / 1024 / 128 | 1,261 → 1,507 | 20,168 → 24,118 | 1.23× (0.98–1.35) | 61.56 → 51.07 |
| 4 | mixed / 16 / 1024 / 128 | 1,909 → 2,368 | 30,541 → 37,881 | 1.27× (1.24–1.43) | 63.05 → 54.54 |
| 8 | mixed / 16 / 1024 / 128 | 2,748 → 3,640 | 43,976 → 58,237 | 1.53× (1.24–1.76) | 53.18 → 50.80 |
| 16 | mixed / 16 / 1024 / 128 | 3,957 → 4,843 | 63,317 → 77,483 | 1.22× (1.03–2.07) | 43.66 → 39.13 |

| CPUs | Case | Cache misses % before → after | Commands/commit before → after | CPU seconds before → after | Peak RSS MiB before → after |
|---:|---|---:|---:|---:|---:|
| 1 | mixed-16-1024-128 | 96.76 → 9.99 | 1.25 → 1.30 | 7.12 → 6.56 | 243.64 → 251.61 |
| 2 | mixed-16-1024-128 | 96.69 → 10.02 | 1.36 → 1.35 | 8.37 → 8.58 | 279.96 → 276.38 |
| 4 | mixed-16-1024-128 | 95.63 → 10.04 | 1.58 → 1.61 | 12.48 → 12.28 | 340.31 → 341.61 |
| 8 | mixed-16-1024-128 | 94.60 → 10.02 | 1.68 → 2.30 | 17.81 → 17.21 | 439.94 → 444.41 |
| 16 | mixed-16-1024-128 | 86.64 → 10.02 | 2.39 → 2.73 | 31.85 → 30.56 | 441.00 → 441.88 |

All 30 processes validated responses and exact final state before shutdown and after reopening, covering 757,184 completed writes. Host one-minute load ranged from 29.29 to 41.38.

## dd-fanout-fixed-transactions-width-1-20260907

Raw artifacts: `/home/mewhhaha/dd-fanout-fixed-transactions-width-1-20260907`. Each row has 3 alternating paired runs; 1s warmup and 6s timed work. Caller concurrency: 32.

Values are medians. Gain is the median candidate/baseline ratio of matched pairs; the range shows every pair, not a confidence interval. Request latency includes all concurrent memory transactions and response validation. CPU time and RSS cover the whole process, including setup, verification, and reopening.

- Baseline binary SHA-256: `48fd745090f50ed4cea5fd5317d010e27e778b208dd5ecebeb6835c1373f034a`; source patch SHA-256: `ebe0e72a4e213b3330f5473ae287268ef7534dcaa60192a78df3cdbdbab948b8`.
- Candidate binary SHA-256: `8e122a07ac13863805012913a9be94514647df56e96b56f95ee99fdafa69731c`; source patch SHA-256: `87235f8655cc17830f1966eb1e741dc9403ac1d5d1fecd23829af88be9476630`.

| CPUs | Mode / fanout / population / bytes | Requests/s before → after | Transactions/s before → after | Gain (pair range) | p99 ms before → after |
|---:|---|---:|---:|---:|---:|
| 8 | mixed / 1 / 1024 / 128 | 9,681 → 10,379 | 9,681 → 10,379 | 1.07× (1.01–1.09) | 26.55 → 27.02 |

| CPUs | Case | Cache misses % before → after | Commands/commit before → after | CPU seconds before → after | Peak RSS MiB before → after |
|---:|---|---:|---:|---:|---:|
| 8 | mixed-1-1024-128 | 96.38 → 10.01 | 1.08 → 1.09 | 11.77 → 10.99 | 427.76 → 426.75 |

All 6 processes validated responses and exact final state before shutdown and after reopening, covering 41,242 completed writes. Host one-minute load ranged from 25.19 to 29.29.

## dd-fanout-fixed-transactions-width-4-20260907

Raw artifacts: `/home/mewhhaha/dd-fanout-fixed-transactions-width-4-20260907`. Each row has 3 alternating paired runs; 1s warmup and 6s timed work. Caller concurrency: 8.

Values are medians. Gain is the median candidate/baseline ratio of matched pairs; the range shows every pair, not a confidence interval. Request latency includes all concurrent memory transactions and response validation. CPU time and RSS cover the whole process, including setup, verification, and reopening.

- Baseline binary SHA-256: `48fd745090f50ed4cea5fd5317d010e27e778b208dd5ecebeb6835c1373f034a`; source patch SHA-256: `ebe0e72a4e213b3330f5473ae287268ef7534dcaa60192a78df3cdbdbab948b8`.
- Candidate binary SHA-256: `8e122a07ac13863805012913a9be94514647df56e96b56f95ee99fdafa69731c`; source patch SHA-256: `87235f8655cc17830f1966eb1e741dc9403ac1d5d1fecd23829af88be9476630`.

| CPUs | Mode / fanout / population / bytes | Requests/s before → after | Transactions/s before → after | Gain (pair range) | p99 ms before → after |
|---:|---|---:|---:|---:|---:|
| 8 | mixed / 4 / 1024 / 128 | 2,780 → 3,000 | 11,121 → 12,001 | 1.11× (1.04–1.22) | 29.62 → 29.17 |

| CPUs | Case | Cache misses % before → after | Commands/commit before → after | CPU seconds before → after | Peak RSS MiB before → after |
|---:|---|---:|---:|---:|---:|
| 8 | mixed-4-1024-128 | 96.47 → 10.00 | 1.12 → 1.13 | 7.45 → 6.87 | 269.84 → 272.58 |

All 6 processes validated responses and exact final state before shutdown and after reopening, covering 49,384 completed writes. Host one-minute load ranged from 23.43 to 25.19.

## dd-fanout-fixed-transactions-width-16-20260907

Raw artifacts: `/home/mewhhaha/dd-fanout-fixed-transactions-width-16-20260907`. Each row has 3 alternating paired runs; 1s warmup and 6s timed work. Caller concurrency: 2.

Values are medians. Gain is the median candidate/baseline ratio of matched pairs; the range shows every pair, not a confidence interval. Request latency includes all concurrent memory transactions and response validation. CPU time and RSS cover the whole process, including setup, verification, and reopening.

- Baseline binary SHA-256: `48fd745090f50ed4cea5fd5317d010e27e778b208dd5ecebeb6835c1373f034a`; source patch SHA-256: `ebe0e72a4e213b3330f5473ae287268ef7534dcaa60192a78df3cdbdbab948b8`.
- Candidate binary SHA-256: `8e122a07ac13863805012913a9be94514647df56e96b56f95ee99fdafa69731c`; source patch SHA-256: `87235f8655cc17830f1966eb1e741dc9403ac1d5d1fecd23829af88be9476630`.

| CPUs | Mode / fanout / population / bytes | Requests/s before → after | Transactions/s before → after | Gain (pair range) | p99 ms before → after |
|---:|---|---:|---:|---:|---:|
| 8 | mixed / 16 / 1024 / 128 | 648 → 707 | 10,363 → 11,311 | 1.08× (0.91–1.13) | 30.42 → 30.73 |

| CPUs | Case | Cache misses % before → after | Commands/commit before → after | CPU seconds before → after | Peak RSS MiB before → after |
|---:|---|---:|---:|---:|---:|
| 8 | mixed-16-1024-128 | 96.62 → 10.00 | 1.21 → 1.22 | 5.23 → 4.63 | 246.41 → 247.08 |

All 6 processes validated responses and exact final state before shutdown and after reopening, covering 44,736 completed writes. Host one-minute load ranged from 18.84 to 23.90.

