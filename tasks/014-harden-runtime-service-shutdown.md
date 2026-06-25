# Harden explicit runtime service shutdown

**Priority:** P2 reliability  
**Primary area:** `RuntimeService`, runtime manager thread lifecycle, isolate thread lifecycle  
**Dependencies:** coordinate with task 010 if it identifies shutdown as the signal-139 cause

## Problem

The runtime starts a manager thread and isolate threads and communicates through
channels. Many shutdown paths send `IsolateCommand::Shutdown` with `try_send`,
then continue without deterministic confirmation that the target thread stopped.
This may be acceptable for ordinary process exit, but it makes tests,
benchmarks, and repeated runtime creation less reliable.

The benchmark signal-139 task may find a more specific root cause, but even
without that crash the code would benefit from a first-class, awaitable shutdown
protocol for tests, tools, and embedded users.

## Objective

Add an explicit, idempotent shutdown path for `RuntimeService` that drains or
rejects work deterministically and confirms that runtime-owned threads/tasks have
stopped within a timeout.

## Required behavior

- `RuntimeService::shutdown()` or equivalent can be awaited by tests and
  benchmarks.
- Calling shutdown more than once is safe.
- New invokes after shutdown begins fail with a clear error.
- Queued work is rejected exactly once.
- Inflight work is allowed to finish or is cancelled according to a documented
  timeout policy.
- Isolate threads receive shutdown and are joined or otherwise observed.
- The manager thread exits and is joined or observed.
- Stream registrations, websocket sessions, transport sessions, dynamic workers,
  outbox workers, and pending replies are closed consistently.

## Tests

Cover:

1. shutdown with no workers;
2. shutdown with queued requests;
3. shutdown with inflight requests;
4. shutdown with streams;
5. shutdown with websocket or transport sessions;
6. shutdown with dynamic workers;
7. repeated shutdown calls;
8. invoke after shutdown;
9. shutdown timeout path;
10. no leaked runtime or isolate threads in a repeated create/shutdown loop.

## Acceptance criteria

- Benchmarks and tests can choose deterministic shutdown instead of relying on
  drop/process exit.
- Shutdown errors are reported clearly.
- No request reply is sent twice.
- No thread/task leak is observable in the new tests.
- `just check` passes.

## Non-goals

- Replacing all runtime threading.
- Changing public request execution semantics outside shutdown.
- Implementing graceful distributed draining for production deploys.

## Implementation cautions

- Avoid blocking a Tokio runtime on a thread join that needs that runtime to
  make progress.
- Keep shutdown cancellation distinct from user-request cancellation where error
  messages are surfaced.
- Ensure best-effort `Drop` remains safe even if explicit shutdown was not
  called.
