# Memory overhead prototype measurements — 2026-09-08

All candidates below were rejected and reverted. These are diagnostic trials,
not performance results for the retained shard-hash change. See
[the investigation report](MEMORY-OVERHEAD.md) for decisions and limitations.

## dd-memory-clock-smoke-20260908

Raw artifacts: `/home/mewhhaha/dd-memory-clock-smoke-20260908`. Each row has 1 alternating paired runs; 2s warmup and 8s timed work. Caller concurrency: 32.

Offered load: saturated caller pool. For fixed-rate runs, latency starts at scheduled arrival and includes caller backlog; all arrivals drain before verification.

Values are medians. Gain is the median candidate/baseline ratio of matched pairs; the range shows every pair, not a confidence interval. Request latency includes all concurrent memory transactions and response validation. CPU time and RSS cover the whole process, including setup, verification, and reopening.

- Baseline binary SHA-256: `916734e9d3f57011fe2eb4f38b3d1e68b5981259cf26d79574e2eef83c733d75`; read API: `snapshot`; source patch SHA-256: `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`.
- Candidate binary SHA-256: `63091c1283a6989228e5766369f9f3640b73f9e2e345b0cbc419d622edbe4ec9`; read API: `snapshot`; source patch SHA-256: `384a2392d120193f369270792098e1c023adc21bee4ec95b333e6b58d501f833`.

| CPUs | Mode / fanout / population / bytes / keys / payload | Requests/s before → after | Transactions/s before → after | Gain (pair range) | p99 ms before → after |
|---:|---|---:|---:|---:|---:|
| 8 | read / 16 / 1024 / 128 / 1 / repeated | 42,680 → 44,935 | 682,884 → 718,960 | 1.05× (1.05–1.05) | 1.85 → 1.79 |
| 8 | read / 16 / 256 / 4096 / 16 / varied | 20,381 → 20,492 | 326,101 → 327,870 | 1.01× (1.01–1.01) | 4.00 → 3.87 |
| 8 | mixed / 16 / 256 / 4096 / 16 / varied | 8,127 → 8,581 | 130,033 → 137,298 | 1.06× (1.06–1.06) | 47.93 → 45.20 |

Latency below is separated by completed request operation. Values are medians of runs; p99 ranges retain every run. A request waits for its entire memory fanout.

| CPUs | Case | Operation | Mean ms before → after | p99 ms before → after | p99 range ms before → after |
|---:|---|---|---:|---:|---|
| 8 | read-16-1024-128 | read | 0.75 → 0.71 | 1.85 → 1.79 | 1.85–1.85 → 1.79–1.79 |
| 8 | read-16-256-4096-16-varied | read | 1.57 → 1.56 | 4.00 → 3.87 | 4.00–4.00 → 3.87–3.87 |
| 8 | mixed-16-256-4096-16-varied | read | 0.97 → 0.94 | 3.35 → 3.24 | 3.35–3.35 → 3.24–3.24 |
| 8 | mixed-16-256-4096-16-varied | write | 30.62 → 28.80 | 75.72 → 64.02 | 75.72–75.72 → 64.02–64.02 |

CPU ms/request divides whole-process CPU time by completed warmup and timed requests. It includes setup and verification CPU, so it is an amortized process cost, not isolated handler CPU.

| CPUs | Case | Cache misses % before → after | Commands/commit before → after | CPU seconds before → after | CPU ms/request before → after | Peak RSS MiB before → after |
|---:|---|---:|---:|---:|---:|---:|
| 8 | read-16-1024-128 | 0.00 → 0.00 | 0.00 → 0.00 | 58.24 → 58.66 | 0.14 → 0.13 | 435.35 → 439.84 |
| 8 | read-16-256-4096-16-varied | 0.00 → 0.00 | 0.00 → 0.00 | 57.65 → 53.68 | 0.28 → 0.27 | 636.82 → 633.78 |
| 8 | mixed-16-256-4096-16-varied | 0.00 → 0.00 | 2.93 → 2.93 | 47.76 → 48.24 | 0.59 → 0.58 | 620.97 → 616.11 |

All 6 processes validated responses and exact final state before shutdown and after reopening, covering 264,496 completed writes. Host one-minute load ranged from 9.05 to 19.05.

## dd-memory-clock-cores-20260908

Raw artifacts: `/home/mewhhaha/dd-memory-clock-cores-20260908`. Each row has 3 alternating paired runs; 2s warmup and 8s timed work. Caller concurrency: 32.

Offered load: saturated caller pool. For fixed-rate runs, latency starts at scheduled arrival and includes caller backlog; all arrivals drain before verification.

Values are medians. Gain is the median candidate/baseline ratio of matched pairs; the range shows every pair, not a confidence interval. Request latency includes all concurrent memory transactions and response validation. CPU time and RSS cover the whole process, including setup, verification, and reopening.

- Baseline binary SHA-256: `916734e9d3f57011fe2eb4f38b3d1e68b5981259cf26d79574e2eef83c733d75`; read API: `snapshot`; source patch SHA-256: `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`.
- Candidate binary SHA-256: `63091c1283a6989228e5766369f9f3640b73f9e2e345b0cbc419d622edbe4ec9`; read API: `snapshot`; source patch SHA-256: `384a2392d120193f369270792098e1c023adc21bee4ec95b333e6b58d501f833`.

| CPUs | Mode / fanout / population / bytes / keys / payload | Requests/s before → after | Transactions/s before → after | Gain (pair range) | p99 ms before → after |
|---:|---|---:|---:|---:|---:|
| 1 | read / 16 / 256 / 4096 / 16 / varied | 4,777 → 5,016 | 76,425 → 80,253 | 1.01× (0.86–1.05) | 13.58 → 14.05 |
| 1 | mixed / 16 / 256 / 4096 / 16 / varied | 1,800 → 1,750 | 28,801 → 27,998 | 1.01× (0.95–1.02) | 33.95 → 35.28 |
| 4 | read / 16 / 256 / 4096 / 16 / varied | 12,950 → 12,282 | 207,198 → 196,513 | 0.95× (0.81–1.03) | 6.07 → 6.33 |
| 4 | mixed / 16 / 256 / 4096 / 16 / varied | 5,456 → 4,793 | 87,297 → 76,681 | 0.94× (0.63–1.11) | 35.63 → 39.89 |
| 8 | read / 16 / 256 / 4096 / 16 / varied | 21,154 → 20,040 | 338,458 → 320,646 | 1.01× (0.95–1.04) | 3.60 → 4.00 |
| 8 | mixed / 16 / 256 / 4096 / 16 / varied | 6,737 → 6,883 | 107,787 → 110,126 | 0.75× (0.71–1.02) | 51.83 → 72.37 |

Latency below is separated by completed request operation. Values are medians of runs; p99 ranges retain every run. A request waits for its entire memory fanout.

| CPUs | Case | Operation | Mean ms before → after | p99 ms before → after | p99 range ms before → after |
|---:|---|---|---:|---:|---|
| 1 | read-16-256-4096-16-varied | read | 6.70 → 6.38 | 13.58 → 14.05 | 12.74–17.15 → 13.23–18.64 |
| 1 | mixed-16-256-4096-16-varied | read | 16.79 → 17.13 | 29.65 → 29.17 | 26.81–29.94 → 26.64–31.60 |
| 1 | mixed-16-256-4096-16-varied | write | 26.43 → 27.41 | 42.98 → 42.86 | 38.50–43.95 → 38.56–67.19 |
| 4 | read-16-256-4096-16-varied | read | 2.47 → 2.61 | 6.07 → 6.33 | 4.96–6.19 → 6.12–8.60 |
| 4 | mixed-16-256-4096-16-varied | read | 3.80 → 4.40 | 9.40 → 11.49 | 8.33–10.74 → 7.47–25.63 |
| 4 | mixed-16-256-4096-16-varied | write | 24.72 → 27.07 | 60.27 → 56.12 | 49.44–65.61 → 40.96–156.81 |
| 8 | read-16-256-4096-16-varied | read | 1.51 → 1.60 | 3.60 → 4.00 | 3.38–6.57 → 3.35–6.12 |
| 8 | mixed-16-256-4096-16-varied | read | 0.85 → 0.78 | 2.71 → 2.69 | 2.00–5.36 → 1.87–5.84 |
| 8 | mixed-16-256-4096-16-varied | write | 33.78 → 37.41 | 103.57 → 200.73 | 75.12–167.41 → 63.58–1316.53 |

CPU ms/request divides whole-process CPU time by completed warmup and timed requests. It includes setup and verification CPU, so it is an amortized process cost, not isolated handler CPU.

| CPUs | Case | Cache misses % before → after | Commands/commit before → after | CPU seconds before → after | CPU ms/request before → after | Peak RSS MiB before → after |
|---:|---|---:|---:|---:|---:|---:|
| 1 | read-16-256-4096-16-varied | 0.00 → 0.00 | 0.00 → 0.00 | 10.87 → 10.85 | 0.23 → 0.22 | 316.46 → 320.50 |
| 1 | mixed-16-256-4096-16-varied | 0.00 → 0.00 | 1.24 → 1.24 | 10.44 → 10.41 | 0.57 → 0.59 | 318.31 → 316.98 |
| 4 | read-16-256-4096-16-varied | 0.00 → 0.00 | 0.00 → 0.00 | 31.05 → 30.69 | 0.25 → 0.26 | 458.50 → 459.79 |
| 4 | mixed-16-256-4096-16-varied | 0.00 → 0.00 | 1.85 → 1.84 | 31.20 → 30.79 | 0.61 → 0.61 | 460.20 → 456.70 |
| 8 | read-16-256-4096-16-varied | 0.00 → 0.00 | 0.00 → 0.00 | 56.43 → 53.87 | 0.28 → 0.27 | 634.38 → 632.27 |
| 8 | mixed-16-256-4096-16-varied | 0.00 → 0.00 | 2.88 → 2.96 | 38.83 → 40.66 | 0.60 → 0.59 | 622.43 → 622.40 |

All 36 processes validated responses and exact final state before shutdown and after reopening, covering 1,241,104 completed writes. Host one-minute load ranged from 10.12 to 26.28.

## dd-memory-time-smoke-20260908

Raw artifacts: `/home/mewhhaha/dd-memory-time-smoke-20260908`. Each row has 1 alternating paired runs; 2s warmup and 8s timed work. Caller concurrency: 32.

Offered load: saturated caller pool. For fixed-rate runs, latency starts at scheduled arrival and includes caller backlog; all arrivals drain before verification.

Values are medians. Gain is the median candidate/baseline ratio of matched pairs; the range shows every pair, not a confidence interval. Request latency includes all concurrent memory transactions and response validation. CPU time and RSS cover the whole process, including setup, verification, and reopening.

- Baseline binary SHA-256: `916734e9d3f57011fe2eb4f38b3d1e68b5981259cf26d79574e2eef83c733d75`; read API: `snapshot`; source patch SHA-256: `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`.
- Candidate binary SHA-256: `48941fdec78f16eb378037f04e19e8467a6fb5b8e2fa232b809fb4915aa312e0`; read API: `snapshot`; source patch SHA-256: `e15f28267b165c52fd0763d4a9f940d51ec76365882ac54dd4c35b572cb8b7a3`.

| CPUs | Mode / fanout / population / bytes / keys / payload | Requests/s before → after | Transactions/s before → after | Gain (pair range) | p99 ms before → after |
|---:|---|---:|---:|---:|---:|
| 8 | read / 16 / 1024 / 128 / 1 / repeated | 28,096 → 28,932 | 449,538 → 462,918 | 1.03× (1.03–1.03) | 3.03 → 3.14 |
| 8 | read / 16 / 256 / 4096 / 16 / varied | 15,102 → 14,962 | 241,625 → 239,395 | 0.99× (0.99–0.99) | 5.60 → 5.49 |
| 8 | mixed / 16 / 256 / 4096 / 16 / varied | 7,225 → 6,738 | 115,595 → 107,812 | 0.93× (0.93–0.93) | 48.17 → 50.89 |

Latency below is separated by completed request operation. Values are medians of runs; p99 ranges retain every run. A request waits for its entire memory fanout.

| CPUs | Case | Operation | Mean ms before → after | p99 ms before → after | p99 range ms before → after |
|---:|---|---|---:|---:|---|
| 8 | read-16-1024-128 | read | 1.14 → 1.11 | 3.03 → 3.14 | 3.03–3.03 → 3.14–3.14 |
| 8 | read-16-256-4096-16-varied | read | 2.12 → 2.14 | 5.60 → 5.49 | 5.60–5.60 → 5.49–5.49 |
| 8 | mixed-16-256-4096-16-varied | read | 1.45 → 1.57 | 5.14 → 5.57 | 5.14–5.14 → 5.57–5.57 |
| 8 | mixed-16-256-4096-16-varied | write | 31.17 → 33.28 | 63.11 → 65.84 | 63.11–63.11 → 65.84–65.84 |

CPU ms/request divides whole-process CPU time by completed warmup and timed requests. It includes setup and verification CPU, so it is an amortized process cost, not isolated handler CPU.

| CPUs | Case | Cache misses % before → after | Commands/commit before → after | CPU seconds before → after | CPU ms/request before → after | Peak RSS MiB before → after |
|---:|---|---:|---:|---:|---:|---:|
| 8 | read-16-1024-128 | 0.00 → 0.00 | 0.00 → 0.00 | 39.80 → 42.20 | 0.15 → 0.14 | 430.71 → 422.14 |
| 8 | read-16-256-4096-16-varied | 0.00 → 0.00 | 0.00 → 0.00 | 43.03 → 42.34 | 0.30 → 0.30 | 626.18 → 634.88 |
| 8 | mixed-16-256-4096-16-varied | 0.00 → 0.00 | 2.72 → 2.68 | 40.89 → 38.28 | 0.59 → 0.60 | 639.51 → 627.16 |

All 6 processes validated responses and exact final state before shutdown and after reopening, covering 212,640 completed writes. Host one-minute load ranged from 9.27 to 21.02.

## dd-memory-boundary-cores-20260908

Raw artifacts: `/home/mewhhaha/dd-memory-boundary-cores-20260908`. Each row has 3 alternating paired runs; 2s warmup and 8s timed work. Caller concurrency: 32.

Offered load: saturated caller pool. For fixed-rate runs, latency starts at scheduled arrival and includes caller backlog; all arrivals drain before verification.

Values are medians. Gain is the median candidate/baseline ratio of matched pairs; the range shows every pair, not a confidence interval. Request latency includes all concurrent memory transactions and response validation. CPU time and RSS cover the whole process, including setup, verification, and reopening.

- Baseline binary SHA-256: `916734e9d3f57011fe2eb4f38b3d1e68b5981259cf26d79574e2eef83c733d75`; read API: `snapshot`; source patch SHA-256: `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`.
- Candidate binary SHA-256: `3091c9d7e420bff655ed06b76c7cb83121b7280ff173522c0d429c77dbf53499`; read API: `snapshot`; source patch SHA-256: `3656a81d119921608466ddf513e7236b67e3367234886c9ca280c48f01c88fae`.

| CPUs | Mode / fanout / population / bytes / keys / payload | Requests/s before → after | Transactions/s before → after | Gain (pair range) | p99 ms before → after |
|---:|---|---:|---:|---:|---:|
| 1 | read / 16 / 256 / 4096 / 16 / varied | 4,417 → 4,805 | 70,673 → 76,881 | 1.06× (0.85–1.11) | 15.38 → 14.11 |
| 1 | mixed / 16 / 256 / 4096 / 16 / varied | 1,583 → 1,696 | 25,334 → 27,129 | 1.02× (0.99–1.07) | 37.06 → 35.16 |
| 4 | read / 16 / 256 / 4096 / 16 / varied | 11,088 → 11,335 | 177,404 → 181,365 | 1.06× (1.02–1.08) | 6.85 → 6.80 |
| 4 | mixed / 16 / 256 / 4096 / 16 / varied | 4,770 → 4,738 | 76,326 → 75,816 | 1.06× (0.99–1.06) | 38.98 → 39.09 |
| 8 | read / 16 / 256 / 4096 / 16 / varied | 11,452 → 12,626 | 183,238 → 202,017 | 1.04× (0.55–1.12) | 7.74 → 7.17 |
| 8 | mixed / 16 / 256 / 4096 / 16 / varied | 5,868 → 5,461 | 93,889 → 87,373 | 0.94× (0.93–1.18) | 58.63 → 64.47 |

Latency below is separated by completed request operation. Values are medians of runs; p99 ranges retain every run. A request waits for its entire memory fanout.

| CPUs | Case | Operation | Mean ms before → after | p99 ms before → after | p99 range ms before → after |
|---:|---|---|---:|---:|---|
| 1 | read-16-256-4096-16-varied | read | 7.24 → 6.66 | 15.38 → 14.11 | 14.40–15.49 → 13.83–28.04 |
| 1 | mixed-16-256-4096-16-varied | read | 19.21 → 17.84 | 31.58 → 30.62 | 28.35–35.15 → 29.78–31.04 |
| 1 | mixed-16-256-4096-16-varied | write | 28.96 → 27.90 | 46.10 → 44.01 | 40.49–49.38 → 43.16–52.40 |
| 4 | read-16-256-4096-16-varied | read | 2.89 → 2.82 | 6.85 → 6.80 | 6.65–8.17 → 5.98–7.81 |
| 4 | mixed-16-256-4096-16-varied | read | 4.46 → 4.48 | 10.16 → 9.87 | 9.78–10.96 → 8.47–10.68 |
| 4 | mixed-16-256-4096-16-varied | write | 26.89 → 27.11 | 52.11 → 49.80 | 49.72–60.39 → 47.56–58.78 |
| 8 | read-16-256-4096-16-varied | read | 2.79 → 2.53 | 7.74 → 7.17 | 5.80–7.80 → 5.78–27.33 |
| 8 | mixed-16-256-4096-16-varied | read | 1.80 → 1.85 | 8.01 → 7.82 | 6.09–20.83 → 5.91–8.46 |
| 8 | mixed-16-256-4096-16-varied | write | 37.33 → 40.75 | 79.10 → 107.40 | 66.57–168.10 → 74.57–199.00 |

CPU ms/request divides whole-process CPU time by completed warmup and timed requests. It includes setup and verification CPU, so it is an amortized process cost, not isolated handler CPU.

| CPUs | Case | Cache misses % before → after | Commands/commit before → after | CPU seconds before → after | CPU ms/request before → after | Peak RSS MiB before → after |
|---:|---|---:|---:|---:|---:|---:|
| 1 | read-16-256-4096-16-varied | 0.00 → 0.00 | 0.00 → 0.00 | 10.87 → 10.83 | 0.25 → 0.23 | 318.28 → 315.68 |
| 1 | mixed-16-256-4096-16-varied | 0.00 → 0.00 | 1.23 → 1.24 | 10.45 → 10.41 | 0.66 → 0.61 | 319.71 → 314.60 |
| 4 | read-16-256-4096-16-varied | 0.00 → 0.00 | 0.00 → 0.00 | 29.11 → 29.38 | 0.28 → 0.26 | 453.34 → 460.91 |
| 4 | mixed-16-256-4096-16-varied | 0.00 → 0.00 | 1.83 → 1.85 | 29.37 → 28.87 | 0.65 → 0.62 | 457.06 → 454.76 |
| 8 | read-16-256-4096-16-varied | 0.00 → 0.00 | 0.00 → 0.00 | 35.81 → 34.34 | 0.31 → 0.30 | 621.23 → 623.53 |
| 8 | mixed-16-256-4096-16-varied | 0.00 → 0.00 | 2.73 → 2.68 | 34.20 → 32.56 | 0.63 → 0.62 | 626.33 → 622.18 |

All 36 processes validated responses and exact final state before shutdown and after reopening, covering 1,124,432 completed writes. Host one-minute load ranged from 5.81 to 31.34.

## dd-memory-boundary-holdouts-20260908

Raw artifacts: `/home/mewhhaha/dd-memory-boundary-holdouts-20260908`. Each row has 3 alternating paired runs; 3s warmup and 12s timed work. Caller concurrency: 32.

Offered load: saturated caller pool. For fixed-rate runs, latency starts at scheduled arrival and includes caller backlog; all arrivals drain before verification.

Values are medians. Gain is the median candidate/baseline ratio of matched pairs; the range shows every pair, not a confidence interval. Request latency includes all concurrent memory transactions and response validation. CPU time and RSS cover the whole process, including setup, verification, and reopening.

- Baseline binary SHA-256: `916734e9d3f57011fe2eb4f38b3d1e68b5981259cf26d79574e2eef83c733d75`; read API: `snapshot`; source patch SHA-256: `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`.
- Candidate binary SHA-256: `3091c9d7e420bff655ed06b76c7cb83121b7280ff173522c0d429c77dbf53499`; read API: `snapshot`; source patch SHA-256: `3656a81d119921608466ddf513e7236b67e3367234886c9ca280c48f01c88fae`.

| CPUs | Mode / fanout / population / bytes / keys / payload | Requests/s before → after | Transactions/s before → after | Gain (pair range) | p99 ms before → after |
|---:|---|---:|---:|---:|---:|
| 8 | mixed / 4 / 1024 / 128 / 1 / repeated | 7,345 → 16,811 | 29,380 → 67,243 | 1.01× (0.94–2.29) | 32.28 → 29.28 |
| 8 | read / 16 / 1024 / 128 / 1 / repeated | 9,857 → 9,003 | 157,717 → 144,053 | 1.04× (0.83–1.09) | 11.49 → 11.66 |
| 8 | mixed / 16 / 1024 / 128 / 1 / repeated | 7,250 → 3,400 | 115,992 → 54,407 | 1.03× (0.47–1.12) | 42.54 → 62.94 |
| 8 | read / 16 / 512 / 16384 / 16 / varied | 345 → 265 | 5,526 → 4,240 | 0.84× (0.54–1.00) | 328.35 → 381.41 |
| 8 | write / 16 / 256 / 4096 / 16 / varied | 474 → 432 | 7,587 → 6,912 | 1.02× (0.91–1.05) | 113.26 → 159.35 |

Latency below is separated by completed request operation. Values are medians of runs; p99 ranges retain every run. A request waits for its entire memory fanout.

| CPUs | Case | Operation | Mean ms before → after | p99 ms before → after | p99 range ms before → after |
|---:|---|---|---:|---:|---|
| 8 | mixed-4-1024-128 | read | 2.65 → 0.70 | 13.25 → 2.57 | 2.71–13.82 → 1.88–15.55 |
| 8 | mixed-4-1024-128 | write | 19.70 → 15.10 | 49.39 → 54.67 | 26.85–50.38 → 27.37–55.50 |
| 8 | read-16-1024-128 | read | 3.25 → 3.55 | 11.49 → 11.66 | 3.69–13.11 → 3.53–13.24 |
| 8 | mixed-16-1024-128 | read | 2.36 → 5.72 | 17.60 → 29.24 | 4.26–58.62 → 4.07–53.52 |
| 8 | mixed-16-1024-128 | write | 22.86 → 42.51 | 68.02 → 110.64 | 34.81–138.99 → 36.92–150.03 |
| 8 | read-16-512-16384-16-varied | read | 92.44 → 120.45 | 328.35 → 381.41 | 235.94–473.56 → 316.77–514.26 |
| 8 | write-16-256-4096-16-varied | write | 67.37 → 73.88 | 113.26 → 159.35 | 112.96–323.38 → 136.26–306.90 |

CPU ms/request divides whole-process CPU time by completed warmup and timed requests. It includes setup and verification CPU, so it is an amortized process cost, not isolated handler CPU.

| CPUs | Case | Cache misses % before → after | Commands/commit before → after | CPU seconds before → after | CPU ms/request before → after | Peak RSS MiB before → after |
|---:|---|---:|---:|---:|---:|---:|
| 8 | mixed-4-1024-128 | 0.00 → 0.00 | 1.36 → 1.61 | 24.63 → 44.45 | 0.24 → 0.18 | 420.44 → 426.81 |
| 8 | read-16-1024-128 | 0.00 → 0.00 | 0.00 → 0.00 | 37.99 → 31.48 | 0.20 → 0.25 | 421.89 → 417.41 |
| 8 | mixed-16-1024-128 | 0.00 → 0.00 | 3.48 → 2.53 | 39.18 → 26.30 | 0.39 → 0.53 | 434.26 → 427.36 |
| 8 | read-16-512-16384-16-varied | 93.36 → 92.88 | 0.00 → 0.00 | 60.90 → 55.19 | 12.81 → 14.30 | 1573.33 → 1575.15 |
| 8 | write-16-256-4096-16-varied | 0.00 → 0.00 | 3.72 → 3.76 | 24.28 → 23.02 | 3.35 → 3.32 | 607.89 → 601.88 |

All 30 processes validated responses and exact final state before shutdown and after reopening, covering 2,076,772 completed writes. Host one-minute load ranged from 20.47 to 51.80.
