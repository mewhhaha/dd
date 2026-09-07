CREATE TABLE IF NOT EXISTS state_floor (singleton INTEGER PRIMARY KEY, version INTEGER NOT NULL);
INSERT INTO state_floor (singleton, version) VALUES (1, 0) ON CONFLICT(singleton) DO NOTHING;
CREATE TABLE IF NOT EXISTS worker_kv (
 worker TEXT NOT NULL, binding TEXT NOT NULL, key TEXT NOT NULL,
 value BLOB NOT NULL, encoding TEXT NOT NULL, deleted INTEGER NOT NULL, version INTEGER NOT NULL,
 PRIMARY KEY(worker, binding, key)
);
CREATE TABLE IF NOT EXISTS memory_state (
 worker TEXT NOT NULL, binding TEXT NOT NULL, entity_key TEXT NOT NULL, item_key TEXT NOT NULL,
 value BLOB NOT NULL, encoding TEXT NOT NULL, deleted INTEGER NOT NULL, version INTEGER NOT NULL,
 PRIMARY KEY(worker, binding, entity_key, item_key)
);
CREATE TABLE IF NOT EXISTS memory_meta (
 worker TEXT NOT NULL, binding TEXT NOT NULL, entity_key TEXT NOT NULL,
 max_version INTEGER NOT NULL, owner_epoch INTEGER NOT NULL,
 PRIMARY KEY(worker, binding, entity_key)
);
CREATE TABLE IF NOT EXISTS memory_commands (
 worker TEXT NOT NULL, binding TEXT NOT NULL, entity_key TEXT NOT NULL, idempotency_key TEXT NOT NULL,
 result_blob BLOB NOT NULL, revision INTEGER NOT NULL,
 PRIMARY KEY(worker, binding, entity_key, idempotency_key)
);
CREATE TABLE IF NOT EXISTS memory_outbox (
 worker TEXT NOT NULL, binding TEXT NOT NULL, entity_key TEXT NOT NULL, effect_id TEXT NOT NULL,
 revision INTEGER NOT NULL, kind TEXT NOT NULL, payload_blob BLOB NOT NULL, status TEXT NOT NULL,
 attempt_count INTEGER NOT NULL, next_attempt_at_ms INTEGER NOT NULL,
 PRIMARY KEY(worker, binding, entity_key, effect_id)
);
CREATE INDEX IF NOT EXISTS memory_outbox_due ON memory_outbox(status, next_attempt_at_ms, revision, effect_id);
