# AGENTS.md

## Terminology

- When we say `Turso`, we mean only the project at `https://github.com/tursodatabase/turso`.
- This is a greenfield project. Radical improvements, large refactors, and breaking changes are encouraged when they simplify the system or materially improve the outcome.
- Prefer the simplest architecture that materially improves the project, even when that means deleting or replacing existing approaches.
- Performance, ergonomics, and conceptual simplicity are valid reasons to make breaking changes in this repo.

## Storage Rules

- Use the `turso` crate directly for local DB access in this repo. Do not introduce `libsql` as a separate dependency.
- KV storage semantics are `write-last` with monotonic versions.
- KV and memory writes must remain contention-safe:
  - set a connection `busy_timeout`
  - retry lock/busy conflicts with bounded backoff
  - keep a persisted/version-floor strategy so restarts do not regress version ordering
