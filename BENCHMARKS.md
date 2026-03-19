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
- The `cpu-5ms` scenario now uses a fixed CPU loop (not `Date.now()`), because runtime time-locking freezes clocks between host I/O boundaries.
- This is still synthetic compared to real network I/O, but it is a much better proxy for promise-idle behavior than microtasks alone.

## Environment

- Date: `2026-03-19T21:21:11+01:00`
- OS: `Linux 6.19.8-1-cachyos x86_64 GNU/Linux`
- Logical CPUs: `16`

## Results

### single-isolate

| scenario | requests | concurrency | total ms | throughput req/s | mean ms | p50 ms | p95 ms | p99 ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| instant-response | 500 | 64 | 205.35 | 2435 | 24.65 | 25.49 | 30.41 | 31.03 |
| cpu-5ms | 400 | 64 | 340.35 | 1175 | 50.31 | 53.45 | 56.11 | 56.44 |
| microtask-1000 | 300 | 64 | 133.73 | 2243 | 25.66 | 27.69 | 29.21 | 29.98 |
| host-sleep-5ms | 400 | 64 | 3100.80 | 129 | 456.85 | 494.43 | 502.66 | 503.52 |

### autoscaling-8

| scenario | requests | concurrency | total ms | throughput req/s | mean ms | p50 ms | p95 ms | p99 ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| instant-response | 500 | 64 | 52.11 | 9595 | 6.49 | 4.42 | 22.63 | 24.13 |
| cpu-5ms | 400 | 64 | 72.74 | 5499 | 11.14 | 9.11 | 24.84 | 29.12 |
| microtask-1000 | 300 | 64 | 41.72 | 7191 | 8.52 | 4.87 | 24.37 | 25.98 |
| host-sleep-5ms | 400 | 64 | 109.91 | 3639 | 16.35 | 15.55 | 23.80 | 29.28 |

## Takeaways

- The current model (autoscaling pools plus multi-inflight per isolate) helps both CPU-heavy and promise-idle work. In `cpu-5ms`, throughput improved from `1175` req/s to `5499` req/s (about `4.7x`).
- For promise-idle behavior (`host-sleep-5ms`), throughput improved from `129` req/s to `3639` req/s (about `28.2x`), showing strong benefit from allowing overlap inside warm isolates.
- Tail latency remains wider under higher concurrency because requests still queue behind pool limits and isolate startup/reuse dynamics.
