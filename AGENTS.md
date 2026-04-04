# AGENTS.md

## Terminology

- When we say `Turso`, we mean only the project at `https://github.com/tursodatabase/turso`.
- This is a greenfield project. Radical improvements, large refactors, and breaking changes are encouraged when they simplify the system or materially improve the outcome.

## Storage Rules

- Use the `turso` crate directly for local DB access in this repo. Do not introduce `libsql` as a separate dependency.
- KV storage semantics are `write-last` with monotonic versions.
- KV and actor writes must remain contention-safe:
  - set a connection `busy_timeout`
  - retry lock/busy conflicts with bounded backoff
  - keep a persisted/version-floor strategy so restarts do not regress version ordering
