# Persist and validate the memory shard layout

**Priority:** P0 correctness  
**Primary area:** `crates/runtime/src/memory.rs`, runtime storage configuration  
**Dependencies:** none

## Problem

The physical file containing an entity is selected with
`stable_memory_shard_index(memory_key, memory_namespace_shards)`. The configured
shard count therefore forms part of the on-disk data format.

Current startup code accepts any positive `memory_namespace_shards` value and
uses it immediately:

- `MemoryStore::new` creates an in-memory shard array of that size.
- `detect_memory_floors` only examines the configured shard range.
- `MemoryStore::shard_index` uses the configured count for every subsequent
  read and write.
- shard databases live at `<memory-root>/<encoded-namespace>/shard-NNNN.db`.

There is no persisted layout manifest. If an operator changes the configured
count after data exists, an entity may hash to a different file. The runtime can
then return empty state and write a second copy of that entity without reporting
that the layout is incompatible. Reducing the count can additionally leave
higher-numbered shard files unexamined during startup.

This must fail closed rather than presenting silent data loss.

## Objective

Persist the memory storage topology as versioned metadata and validate it before
`MemoryStore` serves any request. A configuration mismatch must produce an
explicit startup error with migration guidance. Do not automatically reshard
user data in this task.

## Required design

### 1. Introduce a versioned layout manifest

Add a small manifest under the memory root, for example:

```json
{
  "format_version": 1,
  "shard_hash_version": 1,
  "namespace_shards": 16
}
```

Use names that clearly separate:

- the manifest schema version;
- the shard hash algorithm/domain version;
- the configured physical shard count.

The hash version should correspond to the current
`dd-memory-shard-v1\0` domain. Future hash changes must be detectable without
reinterpreting existing files.

Write the manifest atomically:

1. serialize into a temporary file in the same directory;
2. flush the file;
3. rename it over the destination;
4. avoid leaving a partially written manifest after a crash.

Keep the serialization deterministic and human-readable.

### 2. Validate normal startup

When a manifest exists:

- reject an unsupported `format_version`;
- reject an unsupported `shard_hash_version`;
- reject a configured `memory_namespace_shards` value that differs from the
  persisted value;
- include the configured value, persisted value, memory root, and an actionable
  message in the error;
- perform validation before scanning floors, creating new shard databases, or
  accepting writes.

Do not silently rewrite the manifest to match configuration.

### 3. Safely adopt legacy stores

Existing stores have no manifest, so startup needs a one-time adoption path.

Distinguish these cases:

#### Empty memory root

If no shard database files exist, create a manifest using the current
configuration and continue.

#### Legacy root containing shard databases

Do not infer the original shard count only from the largest existing shard
number. Shard files are created lazily, so a 16-shard store may currently have
only `shard-0000.db` and `shard-0003.db`.

Instead, validate that persisted entities are compatible with the configured
layout:

- enumerate encoded namespace directories and every `shard-*.db` file;
- parse the shard index from the file name and reject malformed names rather
  than silently ignoring them when they look like managed shard files;
- open each database read-only where practical;
- read every distinct `entity_key` represented by `memory_meta`; if older data
  can exist without a `memory_meta` row, also obtain distinct keys from the
  state, command-result, and outbox tables as needed;
- verify that `stable_memory_shard_index(entity_key, configured_count)` equals
  the file's shard index;
- reject any database whose index is outside the configured range;
- if every discovered entity is compatible, atomically write the manifest and
  continue.

An empty legacy shard file is not enough to prove the old count. It may be
accepted if all non-empty files are compatible, but document this limitation in
code comments and the startup log.

The validation is allowed to be relatively expensive because it runs only once
for legacy adoption. It must not run on every normal startup after the manifest
exists.

### 4. Give migration guidance

The mismatch error should state that changing the shard count requires an
explicit reshard operation. It should not suggest deleting the store.

A separate migration tool is out of scope, but structure the manifest types and
validation functions so such a tool can reuse them later.

### 5. Keep benchmark stores convenient

Benchmark and test stores use fresh temporary directories. They should create a
manifest automatically with no additional setup. Existing benchmark scripts
must continue to work.

## Suggested code structure

Use small testable helpers rather than putting all logic in `MemoryStore::new`:

```rust
struct MemoryLayoutManifest { ... }

async fn load_or_adopt_memory_layout(
    root: &Path,
    configured_shards: usize,
) -> Result<MemoryLayoutManifest>;

fn read_memory_layout(path: &Path) -> Result<MemoryLayoutManifest>;
fn write_memory_layout_atomic(path: &Path, manifest: &MemoryLayoutManifest) -> Result<()>;
async fn validate_legacy_layout(root: &Path, configured_shards: usize) -> Result<()>;
```

If JSON adds an undesirable dependency to the runtime crate, use an existing
serialization dependency already present in the workspace. Do not invent an
unversioned ad-hoc text format.

## Tests

Add focused tests using temporary directories. Cover at least:

1. fresh empty root creates a manifest;
2. reopening with the same shard count succeeds;
3. reopening with a different shard count fails before creating any new shard
   file;
4. unsupported manifest format version fails clearly;
5. unsupported hash version fails clearly;
6. legacy store with entities correctly placed for the configured count is
   adopted and receives a manifest;
7. legacy store with at least one entity that hashes to a different shard under
   the configured count is rejected;
8. a higher-numbered shard file is not ignored when the configured count is
   lower;
9. malformed or truncated manifest fails without being overwritten;
10. concurrent initialization attempts do not produce a corrupt manifest.

Prefer testing through `MemoryStore::new` for externally visible behavior, with
unit tests for parsing and atomic-write helpers.

## Acceptance criteria

- Changing `memory_namespace_shards` for a populated manifested store causes a
  deterministic startup error.
- Legacy stores are validated once and adopted without moving data.
- Startup never silently creates a second location for an existing entity after
  a topology mismatch.
- The manifest records both shard count and hash version.
- All existing memory tests pass.
- New tests exercise fresh, matching, mismatched, legacy, and corrupt layouts.
- `just check` passes.

## Non-goals

- Implementing online or offline resharding.
- Changing the current shard hash algorithm.
- Changing public keyed-memory APIs.
- Automatically deleting, renaming, or copying existing shard databases.

## Implementation cautions

- Do not validate legacy placement by loading complete user values into memory;
  stream distinct entity keys.
- Avoid following arbitrary symlinks outside the memory root during directory
  enumeration.
- Preserve useful context when mapping filesystem, JSON, or Turso errors into
  `PlatformError`.
- The manifest must be written only after legacy validation succeeds completely.
