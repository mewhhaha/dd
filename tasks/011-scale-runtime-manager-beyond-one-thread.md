# Scale runtime scheduling beyond one manager thread

**Priority:** P2 architecture / long-term throughput  
**Primary area:** `crates/runtime/src/service/runtime.rs`, scheduler ownership, worker pool routing  
**Dependencies:** task 008 should land first so the bottleneck is measurable; tasks 002 and 003 should land before this if possible

## Problem

The memory scheduler has improved substantially, and current benchmarks show useful scaling for cross-shard work up to the physical machine size. However, all scheduling still funnels through a single runtime manager thread:

- one `dd-runtime` thread owns `WorkerManager`;
- one current-thread Tokio runtime processes commands, isolate events, cancellation commands, ticks, dispatch, cleanup, and outbox events;
- every worker pool and every memory-routed invocation is ultimately coordinated by this one manager.

This is no longer the first bottleneck for the demonstrated 16-logical-CPU benchmark, but it is the next architectural ceiling for larger hosts, multiple busy workers, high socket/transport event rates, and effect-heavy atomic workloads.

## Objective

Design and implement a scalable runtime-manager topology that preserves correctness while letting independent workers or memory shards progress on different manager execution contexts.

This is a larger architecture task. It should start with instrumentation and a narrowly scoped prototype rather than a whole-runtime rewrite.

## Required investigation

Before implementing, use task 008 metrics to answer:

- manager event queue wait at peak cross-shard throughput;
- ready-work budget exhaustion frequency;
- time spent in dispatch versus completion handling versus tick maintenance;
- outbox event contribution;
- websocket/transport contribution;
- number of independent workers active during saturation;
- whether contention is worker-local or global.

Document the measured bottleneck and the selected sharding boundary.

## Candidate designs

### Option A: manager per worker generation

Each worker generation owns a manager actor with its queue, isolate set, memory leases, shard affinity, and sessions. A root coordinator handles deploy, retirement, global limits, and routing.

Pros:

- simple mental model;
- worker-local state stays local;
- independent workers scale naturally;
- fewer cross-manager session migrations.

Cons:

- one hot worker with many memory shards still bottlenecks on one actor;
- deploy/retire and dynamic worker relationships need careful coordination.

### Option B: manager per memory-shard group

Memory-routed work is routed to shard-group managers while general fetch work remains on worker managers.

Pros:

- directly targets keyed-memory scaling;
- lets one hot worker with many independent keys use more cores.

Cons:

- worker isolate pools, sessions, dynamic RPC, and memory leases become cross-manager state;
- preserving same-owner ordering and isolate affinity is harder.

### Option C: hybrid

Keep one worker manager for ordinary fetch/session lifecycle and add dedicated memory-scheduler actors that own queueing and leases but dispatch into a shared isolate pool through a constrained API.

Pros:

- incremental migration path;
- lowers memory scheduling pressure without fully splitting worker state.

Cons:

- needs clear ownership to avoid races;
- shared isolate pool becomes a contention point unless carefully modeled.

Choose one design and write an architecture note before coding.

## Correctness constraints

- Same memory binding + entity ID atomics remain strictly ordered and non-concurrent.
- Worker generation retirement rejects or drains outstanding work exactly once.
- Dynamic worker handles and host RPC provider mappings remain consistent.
- Websocket and transport sessions are owned by exactly one runtime context.
- Global queue byte/request limits remain enforceable.
- Cancellation must route to the owner of the pending or inflight request.
- Debug dumps and stats must remain coherent.
- No manager may hold a lock while awaiting another manager in a cycle.

## Suggested incremental implementation

1. Add a `RuntimePartitionId` abstraction and a routing function.
2. Move only metadata-free command routing through that abstraction while preserving the current single-partition implementation.
3. Introduce a second partition for either memory-routed work or worker-local pools behind a feature flag or test-only config.
4. Add tests for routing, cancellation, shutdown, and deployment under two partitions.
5. Benchmark with one worker and many memory shards, then many workers.
6. Only then make the partitioning configurable or default.

## Tests

Add tests for:

1. independent workers execute on separate partitions without interfering;
2. one worker generation deploy/retire does not leak work to the wrong partition;
3. cancellation reaches queued and inflight requests in the owning partition;
4. same-owner memory atomic ordering remains intact across partition boundaries;
5. cross-shard memory work can run on multiple partitions if that design is chosen;
6. websocket/transport session events route to the correct owner;
7. dynamic worker lookup/fetch/RPC flows survive partitioning;
8. debug dump aggregates all partitions deterministically;
9. shutdown stops every partition and isolate thread;
10. global queue limits are enforced under concurrent submissions to multiple partitions.

## Benchmark validation

Run at least:

- current single-manager baseline;
- multi-partition prototype on 16 logical CPUs;
- if available, a host larger than 16 logical CPUs;
- many-worker workload;
- one-worker many-memory-shard workload;
- effect-heavy atomic workload;
- websocket/transport wakeup workload.

Report manager event wait and throughput. The expected improvement is not necessarily visible on every current local benchmark; the purpose is to remove the next structural ceiling.

## Acceptance criteria

- An architecture note records the chosen partition model and rejected alternatives.
- A feature-flagged or configurable multi-partition runtime exists.
- Correctness tests pass under both one-partition and multi-partition configurations.
- Manager event-loop wait drops for workloads that previously saturated the manager.
- Throughput improves or remains stable for cross-shard memory workloads.
- No public API behavior changes unintentionally.
- `just check` passes.

## Non-goals

- Rewriting storage or changing memory shard file layout.
- Making one entity ID execute atomics concurrently.
- Removing all central coordination in one PR.
- Guaranteeing improvement on hosts with fewer cores than partitions.

## Implementation cautions

- Avoid ad-hoc shared mutable maps between managers.
- Prefer message passing and explicit ownership transfer.
- Be careful with `!Send` state and current-thread Tokio runtimes.
- Do not let debug or stats APIs deadlock by synchronously querying every partition.
