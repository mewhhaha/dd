# Binary Size Report

## Binary Size

Baseline commit: `96bbb2eca0cd7b1808179efe819901bfe5dc9502`
Candidate: working tree based on `96bbb2eca0cd7b1808179efe819901bfe5dc9502`
Target: `x86_64-unknown-linux-gnu`
Rustc: `rustc 1.96.0-nightly (3645249d7 2026-03-16)`
Profile: `dist` (`release`, `debug=false`, `incremental=false`, `codegen-units=1`, `lto="thin"`, `opt-level=3`, `strip="symbols"`)

| Artifact | Baseline bytes | Candidate bytes | Delta bytes | Delta % |
| --- | ---: | ---: | ---: | ---: |
| `dd_server` unstripped | 116,363,984 | 82,488,512 | -33,875,472 | -29.11% |
| `dd_server` stripped | 88,473,160 | 82,488,216 | -5,984,944 | -6.76% |

Final report path: `target/size-report/96bbb2eca0cd7b1808179efe819901bfe5dc9502/dist/`.
Final stripped SHA-256: `163834c9e59951e17ec040acf66d454651592a5718c66f488a3e24604eb22f06`.

Distribution profile variants were compared before selecting the committed default:

| Variant | opt-level | LTO | Stripped bytes | Decision |
| --- | ---: | --- | ---: | --- |
| A | 3 | thin | 82,488,216 | selected |
| B | 3 | fat | 79,702,936 | not selected |
| C | s | thin | 76,409,752 | not selected |
| D | s | fat | 72,264,600 | not selected |
| E | z | thin | 75,226,008 | not selected |
| F | z | fat | 70,359,960 | rejected by latency gate |

Variant F was the smallest artifact, but the five-run direct-write benchmark median p95 latency was 11.74ms versus the baseline 10.65ms (+10.2%), and p99 was 21.75ms versus 18.20ms (+19.5%). The committed profile keeps variant A because it clears the required gates without changing failure behavior or relying on panic abort.

Largest final sections:

| Section | Bytes |
| --- | ---: |
| `.text` | 52,562,257 |
| `.rodata` | 18,785,728 |
| `.eh_frame` | 4,932,568 |
| `.rela.dyn` | 2,718,432 |
| `.data.rel.ro` | 1,510,920 |

`cargo bloat` and `bloaty` were not installed locally; the report records that explicitly.

## Dependency Graph

Removed direct/default paths:

- `reqwest` default features, which removed `openssl`, `native-tls`, and `hyper-tls` from `dd_server`.
- Direct `h3`, `h3-quinn`, and `quinn`.
- Direct `quiche` test dependency; HTTP/3 black-box tests now import `tokio_quiche::quiche`.
- Direct `rustls` and `rustls-pemfile` from `crates/api`.
- Direct runtime crypto crates `aes-gcm`, `hmac`, and `sha1`.
- Direct workspace Tokio features are now explicit: `fs`, `io-std`, `io-util`, `macros`, `net`, `process`, `rt`, `rt-multi-thread`, `signal`, `sync`, and `time`.

Remaining expected providers:

- AWS-LC/Rustls through `deno_crypto`, `deno_fetch`, `reqwest`, and runtime Rustls provider setup.
- Ring through Rustls/Deno Fetch and dev-only `rcgen`.
- BoringSSL through `tokio-quiche`/Quiche for the current HTTP/3 implementation.

Remaining large duplicate paths:

- `tonic@0.12.3` remains through `opentelemetry-proto@0.27.0` generated message types used by the OTLP/HTTP protobuf exporter. The default exporter no longer enables `grpc-tonic`.
- `tonic@0.14.5` remains through `tokio-quiche`'s `foundations` dependency tree.
- `prost@0.13.5` remains through OpenTelemetry 0.27; `prost@0.14.3` remains through Turso and `tokio-quiche`/`foundations`.
- Tokio `full` still reaches the final graph through transitive dependencies, so the explicit direct Tokio feature set did not change final artifact bytes.

## Performance

Each focused benchmark was run five times on the same host for baseline and candidate. Values are medians.

| Benchmark | Baseline median | Candidate median | Delta |
| --- | ---: | ---: | ---: |
| direct memory read throughput | 12,292 req/s | 12,955 req/s | +5.4% |
| direct memory write throughput | 2,024 req/s | 2,052 req/s | +1.4% |
| dynamic baseline throughput | 4,479 req/s | 4,387 req/s | -2.1% |
| dynamic hot fetch throughput | 3,464 req/s | 3,513 req/s | +1.4% |
| dynamic hot fetch + host RPC throughput | 2,058 req/s | 2,116 req/s | +2.8% |
| startup-to-ready | 77ms | 76ms | -1.3% |
| idle RSS | 124,064 KiB | 79,948 KiB | -35.6% |

Turso allocator experiment: rejected. Temporarily disabling Turso defaults and accidentally upgrading `mimalloc` showed unacceptable read-memory samples; the final manifest keeps Turso defaults and the lockfile restores `mimalloc 0.1.48` / `libmimalloc-sys 0.1.44`.

## Correctness

Tests run:

- `cargo fmt --all -- --check`
- `cargo check --workspace --all-targets --all-features`
- `cargo test --workspace`
- `cargo build --locked --profile dist -p dd_server -p cli`
- `just check`
- `cargo test -p runtime crypto_globals_work_with_deno_crypto_ops -- --nocapture`
- `cargo test -p dd_server otlp_http_endpoint -- --nocapture`

Known check status:

- `cargo clippy --workspace --all-targets --all-features -- -D warnings` still fails on existing repository warnings such as `too_many_arguments`, `manual_is_multiple_of`, `large_enum_variant`, and `type_complexity`. Those broad warning cleanups were not mixed into the binary-size change set.

Smoke checks:

- Candidate `target/dist/dd_server` started with public/private listeners.
- Authenticated private `GET /v1/admin/tokens` returned `{"ok":true,"tokens":[]}`.
- Public listener returned `404` for an undeployed worker host.

Compatibility notes:

- V8/`deno_core`, Deno Web/Fetch/WebCrypto, Turso, and Cap'n Proto remain in place.
- HTTP/3, WebSocket over HTTP/3, and WebTransport black-box tests pass.
- OTLP remains available through HTTP protobuf; `OTEL_EXPORTER_OTLP_ENDPOINT` and `DD_OTEL_ENDPOINT` base URLs are normalized to `/v1/traces`.
