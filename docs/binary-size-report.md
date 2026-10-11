# Binary Size Report

This is a historical measurement from 2026-06-19. The runtime was then built on
`deno_core` and Deno's Web, Fetch and WebCrypto crates, and it served HTTP/3
through `tokio-quiche`. Since then the Wasmtime experiment and the HTTP/3 server
have been removed, and on 2026-10-10 the runtime moved to its own rusty_v8
embedding (`crates/dd_v8`) with no Deno crates. None of the sizes or timings
below describe the current tree, and the dependency notes are kept only to
explain them. Regenerate measurements with `scripts/measure-binary-size.sh`
(`just size-report-all`).

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

Direct and default paths the change removed:

- `reqwest` default features, which removed `openssl`, `native-tls`, and `hyper-tls` from `dd_server`.
- Direct `h3`, `h3-quinn`, and `quinn`.
- Direct `quiche` test dependency; the HTTP/3 black-box tests imported `tokio_quiche::quiche` instead.
- Direct `rustls` and `rustls-pemfile` from `crates/api`.
- Direct runtime crypto crates `aes-gcm`, `hmac`, and `sha1`.
- Tokio's `full` feature in the workspace dependency, which lists `fs`, `io-std`, `io-util`, `macros`, `net`, `process`, `rt`, `rt-multi-thread`, `signal`, `sync`, and `time` explicitly instead.

Crypto providers left in the measured graph were AWS-LC (through Rustls,
`reqwest` and the Deno crypto and fetch crates), Ring (through Deno Fetch and
the dev-only `rcgen`) and BoringSSL (through `tokio-quiche`). Today only AWS-LC
remains: through Rustls for `reqwest` and the runtime, and directly in the
runtime's WebCrypto.

Large duplicate paths left in the measured graph:

- `tonic@0.12.3` through `opentelemetry-proto@0.27.0` generated message types used by the OTLP/HTTP protobuf exporter. The default exporter no longer enabled `grpc-tonic`.
- `tonic@0.14.5` through `tokio-quiche`'s `foundations` dependency tree.
- `prost@0.13.5` through OpenTelemetry 0.27, and `prost@0.14.3` through Turso and `tokio-quiche`/`foundations`.
- Tokio `full` through transitive dependencies, so the explicit direct Tokio feature set did not change final artifact bytes.

Today neither `tonic` nor Tokio `full` is in the graph, and OpenTelemetry 0.33
and Turso share one `prost`.

## Performance

Each focused benchmark was run five times on the same host for baseline and candidate. Values are medians.

| Benchmark | Baseline median | Candidate median | Delta |
| --- | ---: | ---: | ---: |
| direct memory read throughput | 12,292 req/s | 12,955 req/s | +5.4% |
| direct memory write throughput | 2,024 req/s | 2,052 req/s | +1.4% |
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

- The change kept V8 through `deno_core`, the Deno Web, Fetch and WebCrypto crates, and Turso. The Deno crates have since been removed.
- OTLP remains available through HTTP protobuf; `OTEL_EXPORTER_OTLP_ENDPOINT` and `DD_OTEL_ENDPOINT` base URLs are normalized to `/v1/traces`.
