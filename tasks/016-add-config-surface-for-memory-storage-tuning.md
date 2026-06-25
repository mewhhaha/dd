# Add a first-class configuration surface for memory storage tuning

**Priority:** P2 operations  
**Primary area:** runtime/server configuration, CLI/env docs, `RuntimeStorageConfig`  
**Dependencies:** coordinate with tasks 001, 005, and 012

## Problem

`RuntimeStorageConfig` has important memory storage settings such as
`memory_namespace_shards`, `memory_db_cache_max_open`, and `memory_db_idle_ttl`.
Those defaults are sensible for development, but production operators need a
clear and validated way to set them without editing Rust structs.

As tasks add layout manifests, cache budgets, resharding, and performance gates,
these settings become operationally significant. Configuration should be
explicit, documented, and validated at the server boundary.

## Objective

Expose memory storage tuning through the project's normal configuration path and
document how each setting affects correctness, resources, and performance.

## Required behavior

Add or confirm support for configuring:

- memory namespace shard count;
- database cache open-entry budget;
- database idle TTL;
- snapshot cache entry and byte budgets if task 005 lands;
- outbox drain concurrency/batch sizes if task 003 lands;
- memory profiling and scheduler observability flags if task 008 lands.

Every setting should have:

- a documented default;
- validation with clear error messages;
- environment/config-file examples;
- tests for valid and invalid values;
- compatibility guidance for changing it after data exists.

## Acceptance criteria

- Operators can set memory storage tuning values through documented server
  configuration.
- Invalid values fail startup clearly.
- Shard-count changes are tied to layout validation/resharding guidance.
- Docs explain resource tradeoffs and benchmark impact.
- Tests cover parsing and validation.

## Non-goals

- Changing the default values without benchmark evidence.
- Automatically resharding data.
- Building a dynamic runtime tuning API.
