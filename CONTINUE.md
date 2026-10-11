# Continuing the cleanup

Notes for picking the cleanup back up. Last updated 2026-10-11. Delete this file once the remaining steps are done.

## Where things stand

dd runs on its own rusty_v8 embedding (`crates/dd_v8`), without the Deno crates. After that migration, a survey found eight areas to clean up ("hills"); they are listed below. Every finished one is on main and verified locally. CI was green through `a24dd72`. Run `git log bc258d7..main` for the per-commit detail.

| Hill | State |
| --- | --- |
| 1. Broken tooling and repo hygiene | Done. `just check` works again, unused crates are dropped, CI calls the same recipes, and tool output is untracked. |
| 2. Dead code | Done. Covers the unused op and entry points, the shapes no producer sends, the legacy memory result format, test-only ops (now behind `cfg(test)`), and about 5,300 lines of vendored JS dd never reached. |
| 3. Per-request worker bundle | Done. `execute_worker` defines its helpers once; a request now creates about 1 to 4 functions instead of about 134. |
| 4. Op error convention | Done. dd's ops return their value or throw (`OpError`); there are no `{ ok, error }` records. Ops are named `op_<domain>_<object>_<verb>`. |
| 5. Duplication | Done. Covers storage retries (one helper), handle tables (`ops/handles.rs`), isolate policy, bench env parsing, one major version per RustCrypto crate (lockfile duplicates went from 46 to 31), and the dev runtime's error body. |
| 6. Deno-era names | Done. Specifiers are `ext:` plus the path under `crates/runtime/js`, `vendor/{web,fetch,crypto,webidl}`, and the brand is `dd.hostObject`. The console honours only Node's inspect symbol, and `vendor/README.md` records where the code came from. |
| 7. Oversized files | Partly done. The CLI is split into modules and the service tests are in topical modules. The rest is below. |
| 8. Docs and examples | Done. Finished investigations are folded into `benchmarks/README.md`, and the kept example workers are tested in `service/tests/examples.rs`. |

These follow-ups also landed:
- Vendored JS now imports the web classes it used to read from the replaceable global.
- `scripts/check-worker-js.mjs` lints the vendored scripts too.
- `performance.timeOrigin` is defined (it was `undefined`).
- Two bugs were fixed: CryptoKey exposed its raw key material through a registered symbol, and `reportError` was broken.

## Next steps

### 1. Split the oversized runtime source files (hill 7)
This is pure restructuring. The rules:
- Move code only; change no logic.
- Keep every existing item path working by re-exporting from the original module, so tests don't change.
- Make one commit per file split.
- Give each new module a short doc comment.
- Aim for no non-test file over about 900 lines, unless it is one cohesive concern.

Seams found by the survey. Re-check them against the code first.
- **`service/model.rs`:**
  - `WorkerManager`;
  - the websocket frame types and outbound queue;
  - `InvocationReply`;
  - `WorkerPool`;
  - the pending-queue lanes, keys and shard queues, plus `PendingInvokeQueue` (about 650 lines, the natural `pending_queue` module);
  - the stats and dispatch-attempt types.
- **`service/facade.rs`:**
  - the config types (`RuntimeConfig`, `RuntimeStorageConfig`, `RuntimeServiceConfig`);
  - the debug and admin DTOs;
  - the public stream and websocket types;
  - shutdown;
  - start;
  - deploy;
  - invoke, stream and websocket;
  - stats and admin;
  - asset and front-cache resolution;
  - the test hooks.

  `RuntimeService` can have several `impl` blocks.
- **`service/dispatch.rs`:**
  - the pre-cancel tracker;
  - queue accounting;
  - enqueue, reject and expire;
  - `cancel_invoke`;
  - `dispatch_pool`;
  - spawn and scale-up;
  - `finish_request`;
  - trace forwarding.
- **`service/control.rs`:**
  - the command and event enums;
  - the event handlers;
  - websocket frame plumbing;
  - the `cfg(test)` hooks.
- **`engine.rs`:**
  - the snapshot;
  - `IsolatePolicy`;
  - load and dispatch;
  - runtime construction;
  - the module loader and `dd:` specifiers (about 300 lines);
  - tests.
- **`ops/memory.rs`:** lease, read, batch, command, socket and profile.
- **`service/lifecycle.rs`, `sessions.rs` and `runtime.rs`:** split only where there is a clear seam.

### 2. Finish the test reorganization (hill 7)
- **Echo servers.** Merge the two TCP echo servers into one fixture in `service/tests/fixtures/`. They are `echo_once` in `tests/hardening.rs` and `echo_requests` in `tests/host_fetch.rs`. Keep host_fetch's 30 s hang guard.
- **Polling helpers.** Share one helper between `wait_for_child_state` / `wait_for_child_inflight` (`tests/service_fetch.rs`) and `wait_for_isolate_total` (`fixtures/runtime.rs`).
- **Deploy helpers.** Share `deploy_memory_contract_worker` and `deploy_read_worker` where clean.
- **Repeated config.** Add named fixture constructors for the `RuntimeConfig` shapes that many tests repeat.
- **API tests.** Move `crates/api/src/handlers_runtime_tests.rs` next to the handlers it tests, split by topic. Keep it in-crate if it uses private items.
- **Test count.** It must not drop. Compare `cargo test -- --list` per crate before and after.

### 3. Measure what the restructuring bought
Phase 1 and 2 moved helpers out of the per-request closure and removed the op result wrappers. Neither has been measured yet. Compare `bc258d7` with main using the instruction-count method described below.

Expect fewer instructions per request. The memory path was 726K at `bc258d7`; the "hello" path was 246K.

### 4. Decisions for the owner
- **Unhandled rejections and `reportError`.** dd fails a request on an unhandled rejection; workerd only logs it. `reportError` and exceptions thrown in event listeners now take that same path. Keep it strict, or move all three to logging?
- **The RSA exception in `SECURITY.md`** (`rsa 0.10.0-rc.19`, RUSTSEC-2023-0071) expires 2026-11-01. Re-review it then: upstream still lists no fixed release.
- **`storage::claim_outbox_records`** is used only by tests. It is the one entry point to `claim`'s per-entity filter. Keep both, or drop both?
- **The memory fan-out tools.** `scripts/compare-memory-fanout.py`, `summarize-memory-fanout.py`, `summarize-memory-profile.py` and `test-memory-profile.py` only make sense together. Keep the set, or delete all of it?
- **Small items:**
  - `DD_BENCH_MAX_ISOLATES` sets the per-worker limit, so its name is ambiguous.
  - `deploy/fly/proxy-private-deploy.sh` defaults to the production app name.
  - `examples/hello-traced.js` says ReadableStream responses are unstable; that is probably stale.

### 5. Known gaps (backlog)
- **Web platform:**
  - `structuredClone` ignores transfer lists.
  - P-521 ECDSA sign and verify are unsupported.
  - dd's revivers still run inside V8's deserializer.
  - The global is not an EventTarget.
- **Runtime internals:**
  - Some web ops can panic when called directly with bad arguments.
  - The crypto ops `unwrap` their `spawn_blocking` join, so a panic propagates.
  - Ops no longer get V8's fast-call path, about 77 ns per sync call. That is under 1% of a request.
- **Hardening:** what remains is inherent to sharing a context with worker code: Promise species, and `then` added to `Object.prototype`. See the header comment of `execute_worker/core.js`.
- **Untested:** the DevTools inspector (`--inspect`) has not been tried against a real Chrome.

## Verifying a change

```sh
export CARGO_BUILD_JOBS=8 TMPDIR=$PWD/target/tmp WPT_DIR=$PWD/.cache/wpt
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --release --locked
just check-js
cargo build --locked -p dd_server --no-default-features --features websocket --bin dd_dev_runtime
node scripts/check-dd-vite-lifecycle.mjs
node scripts/check-dd-vite-reload.mjs
cargo build --locked -p cli && DD_CLI_BIN=target/debug/cli pnpm check:dd-vite-config
node scripts/check-dd-dev-transport.mjs
pnpm smoke:vite-examples
cargo run --release --locked -p runtime --bin wpt   # 0 unexpected against wpt/expectations.json
```

At `a24dd72`, WPT passes 40,207 of 53,031 subtests, with 0 unexpected.

### Notes on this machine
- **Load.** Other sessions usually keep the load average at 10 to 40, and free memory is tight. Run one cargo command at a time.
- **Temp files.** `/tmp` is a RAM tmpfs, so keep `TMPDIR` on disk.
- **Slow suites.** Under that load the runtime suite takes 10 to 15 minutes, and "running for over 60 seconds" warnings are normal. Re-run any timing-sensitive failure in isolation before suspecting a change.
- **Worktrees.** Seed a worktree's build cache with `cp -r --reflink=always <main>/target ./target` (the filesystem is btrfs). Don't share one `CARGO_TARGET_DIR` between worktrees, or cargo will reuse the wrong crate builds.
- **Measuring performance.** Wall-clock throughput is dominated by load, so count user-mode instructions with a `perf_event_open` wrapper instead; `perf` is not installed. Compare the marginal cost per request: the N=12000 run minus the N=2000 run, divided by 10000. Use alternating ABBA ordering for any throughput comparison.
