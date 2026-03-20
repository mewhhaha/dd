# Benchmarks

This repo includes a runtime benchmark:

```bash
cargo run -p runtime --bin bench --release
```

It measures the runtime service directly (no API/network overhead) and prints:

- steady-state throughput + latency (`mean`, `p50`, `p95`, `p99`)
- a page-load-like mixed workload
- cold-start (`deploy` + first invoke)
- hot-start invoke latency
- time-to-scale-up under burst traffic

## Environment

- Date: `2026-03-20T17:08:27+01:00`
- OS: `Linux cachyos-x8664 6.19.8-1-cachyos x86_64`
- Logical CPUs: `16`

## Results

### steady-state: single-isolate

| scenario | requests | concurrency | total ms | throughput req/s | mean ms | p50 ms | p95 ms | p99 ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| instant-response | 500 | 64 | 286.16 | 1747 | 35.03 | 35.11 | 57.50 | 58.12 |
| cpu-5ms | 400 | 64 | 368.79 | 1085 | 54.14 | 56.92 | 66.17 | 67.06 |
| host-sleep-5ms | 400 | 64 | 3111.16 | 129 | 458.40 | 493.36 | 501.73 | 502.10 |
| page-load-mix | 700 | 96 | 3334.50 | 210 | 425.18 | 454.32 | 469.29 | 474.05 |

### steady-state: autoscaling-8

| scenario | requests | concurrency | total ms | throughput req/s | mean ms | p50 ms | p95 ms | p99 ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| instant-response | 500 | 64 | 48.00 | 10417 | 5.98 | 3.35 | 24.52 | 26.33 |
| cpu-5ms | 400 | 64 | 62.55 | 6394 | 9.46 | 8.05 | 19.41 | 22.27 |
| host-sleep-5ms | 400 | 64 | 121.65 | 3288 | 18.21 | 15.53 | 36.45 | 42.43 |
| page-load-mix | 700 | 96 | 135.22 | 5177 | 16.73 | 15.52 | 25.76 | 33.06 |

### lifecycle: autoscaling-8

| benchmark | samples | mean ms | p50 ms | p95 ms | p99 ms | max ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| cold-start deploy | 40 | 14.34 | 14.24 | 15.27 | 18.82 | 18.82 |
| cold-start first invoke | 40 | 2.63 | 2.54 | 2.65 | 10.32 | 10.32 |
| hot-start invoke | 500 | 0.43 | 0.39 | 0.56 | 0.74 | 5.33 |

Scale-up burst:

- requests: `1000`
- concurrency: `192`
- target isolates: `8`
- time to reach target: `15.16 ms`
- burst throughput: `1740 req/s`
- burst p95/p99: `108.73 / 119.60 ms`

## Takeaways

- `p95`/`p99` are now first-class outputs in every workload.
- The autoscaling profile strongly outperforms single-isolate on both CPU-heavy and promise-idle scenarios.
- The page-load-mix scenario shows the biggest practical gap (`210 req/s` vs `5177 req/s`) and much lower tail latencies under autoscaling.
- Cold deploy + first invoke are measurable in low tens of milliseconds on this machine; warm invokes are sub-millisecond at `p50`.
