# Runtime follow-ups

The cleanup is complete. These are separate behavior and platform tasks.

- Keep strict request failures for unhandled rejections, `reportError()` and
  exceptions thrown in event listeners; this is documented in the README.
- Keep `storage::claim_outbox_records` and its per-entity filter. The storage
  tests exercise that path; production drains use the shard-level entry point.
- Keep the memory fan-out comparison, summarization and profile tools together.
- Re-review the RSA advisory exception on **2026-11-01**; see
  [SECURITY.md](../SECURITY.md).

## Web platform

- `structuredClone` ignores transfer lists.
- P-521 ECDSA sign and verify are unsupported.
- dd's revivers run inside V8's deserializer.
- The global is not an EventTarget.

## Runtime internals

- Some web ops can panic when called directly with bad arguments.
- Crypto ops unwrap their `spawn_blocking` join, so a panic propagates.
- Ops lack V8's fast-call path, previously measured at about 77 ns per sync call.
- Sharing a context with worker code leaves Promise species and `then` on
  `Object.prototype` as hardening limits. See the header of
  `crates/runtime/js/execute_worker/core.js`.
- Try the DevTools inspector (`--inspect`) against a real Chrome.
