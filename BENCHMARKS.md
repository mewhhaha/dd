# Benchmarks

This repo includes a small runtime benchmark:

```bash
cargo run -p runtime --bin bench --release
```

It measures the current runtime service directly, without API or HTTP overhead.

## Notes

- The benchmark compares `single-isolate` (`min=1`, `max=1`, `max_inflight_per_isolate=1`) against an autoscaling profile (`min=0`, `max=8`, `idle_ttl=30s`, `max_inflight_per_isolate=4`).
- `max_inflight_per_isolate=4` in this benchmark matches the runtime default today.
- The `host-sleep-5ms` scenario uses the runtime's host async `op_sleep(ms)` op, so it represents a worker that is mostly idle on a promise.
- This is still synthetic compared to real network I/O, but it is a much better proxy for promise-idle behavior than microtasks alone.

## Environment

- Date: `2026-03-19T09:10:16+01:00`
- OS: `Linux 6.19.8-1-cachyos x86_64 GNU/Linux`
- Logical CPUs: `16`

## Results

### single-isolate

| scenario | requests | concurrency | total ms | throughput req/s | mean ms | p50 ms | p95 ms | p99 ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| instant-response | 500 | 64 | 39.26 | 12737 | 4.74 | 4.75 | 5.53 | 6.27 |
| cpu-5ms | 400 | 64 | 2003.05 | 200 | 295.28 | 320.00 | 321.00 | 321.02 |
| microtask-1000 | 300 | 64 | 35.45 | 8464 | 6.84 | 6.49 | 9.74 | 11.21 |
| host-sleep-5ms | 400 | 64 | 2878.63 | 139 | 423.83 | 458.34 | 466.97 | 468.84 |

### autoscaling-8

| scenario | requests | concurrency | total ms | throughput req/s | mean ms | p50 ms | p95 ms | p99 ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| instant-response | 500 | 64 | 29.32 | 17051 | 3.71 | 0.76 | 24.20 | 24.24 |
| cpu-5ms | 400 | 64 | 262.14 | 1526 | 39.04 | 39.02 | 51.98 | 54.95 |
| microtask-1000 | 300 | 64 | 25.17 | 11921 | 5.06 | 1.48 | 18.80 | 19.70 |
| host-sleep-5ms | 400 | 64 | 99.89 | 4005 | 15.01 | 14.33 | 21.30 | 25.31 |

## Takeaways

- The current model (autoscaling pools plus multi-inflight per isolate) helps both CPU-heavy and promise-idle work. In `cpu-5ms`, throughput improved from `200` req/s to `1526` req/s (about `7.6x`).
- For promise-idle behavior (`host-sleep-5ms`), throughput improved from `139` req/s to `4005` req/s (about `28.8x`), showing strong benefit from allowing overlap inside warm isolates.
- Tail latency remains wider under higher concurrency because requests still queue behind pool limits and isolate startup/reuse dynamics.
