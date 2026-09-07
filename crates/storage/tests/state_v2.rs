use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use storage::kv::KvStore;
use storage::memory::{
    MemoryBatchMutation, MemoryCommandResultWrite, MemoryOutboxEffectWrite, MemoryStore,
    worker_namespace,
};
use storage::state::StateStore;

fn temporary_root() -> PathBuf {
    std::env::temp_dir().join(format!("dd-state-v2-{}", uuid::Uuid::new_v4()))
}

fn mutation(key: &str, value: &[u8]) -> MemoryBatchMutation {
    MemoryBatchMutation {
        key: key.into(),
        value: value.into(),
        encoding: "utf8".into(),
        deleted: false,
    }
}

#[tokio::test]
async fn state_preserves_isolation_tombstones_and_fencing_across_restart() {
    let root = temporary_root();
    let state = StateStore::open(&root).await.unwrap();
    assert_eq!(
        std::fs::read_dir(&root)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| entry
                .path()
                .extension()
                .is_some_and(|extension| extension == "db"))
            .count(),
        32
    );
    assert!(
        StateStore::open(&root).await.is_err(),
        "a second store must not create independent shard writers"
    );
    let kv = KvStore::from_state(Arc::clone(&state));
    let memory = MemoryStore::from_state(Arc::clone(&state));
    let namespace = worker_namespace("alpha", "MEMORY");
    let other_namespace = worker_namespace("beta", "MEMORY");
    kv.put("alpha", "KV", "key", "first").await.unwrap();
    assert_eq!(
        kv.get_utf8("alpha", "KV", "key").await.unwrap().unwrap(),
        "first"
    );
    kv.put("alpha", "KV", "key", "last").await.unwrap();
    assert_eq!(
        kv.get_utf8("alpha", "KV", "key").await.unwrap().unwrap(),
        "last"
    );
    assert!(kv.get("beta", "KV", "key").await.unwrap().is_none());
    let version = kv.delete("alpha", "KV", "key").await.unwrap();
    assert!(kv.get("alpha", "KV", "key").await.unwrap().is_none());
    let lease = memory.acquire_lease(&namespace, "entity").await.unwrap();
    let owner = lease.owner_epoch();
    let revision = memory
        .apply_batch(
            &namespace,
            "entity",
            storage::memory::MemoryCommit {
                mutations: &[mutation("count", b"42")],
                command_result: Some(&MemoryCommandResultWrite {
                    idempotency_key: "command".into(),
                    result: b"result".to_vec(),
                }),
                outbox_effects: &[MemoryOutboxEffectWrite {
                    kind: "fetch".into(),
                    payload: b"effect".to_vec(),
                }],
                owner_epoch: Some(owner),
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .max_version;
    assert_eq!(
        memory.snapshot(&namespace, "entity").await.unwrap().entries[0].value,
        b"42"
    );
    assert!(
        memory
            .snapshot(&other_namespace, "entity")
            .await
            .unwrap()
            .entries
            .is_empty()
    );
    assert!(
        tokio::time::timeout(
            Duration::from_millis(10),
            memory.acquire_lease(&namespace, "entity")
        )
        .await
        .is_err()
    );
    let other = memory
        .acquire_lease(&other_namespace, "entity")
        .await
        .unwrap();
    drop(other);
    assert_eq!(
        kv.put_value(
            "alpha",
            "KV",
            "oversized",
            &vec![0; 17 * 1024 * 1024],
            "v8sc"
        )
        .await
        .unwrap_err()
        .kind(),
        common::ErrorKind::BadRequest
    );
    drop(lease);
    drop(memory);
    drop(kv);
    drop(state);

    let state = StateStore::open(&root).await.unwrap();
    let kv = KvStore::from_state(Arc::clone(&state));
    let memory = MemoryStore::from_state(Arc::clone(&state));
    assert!(kv.get("alpha", "KV", "key").await.unwrap().is_none());
    assert!(kv.put("alpha", "KV", "key", "restarted").await.unwrap() > version);
    let next_lease = memory.acquire_lease(&namespace, "entity").await.unwrap();
    assert!(next_lease.owner_epoch() > owner);
    assert_eq!(
        memory
            .command_result(&namespace, "entity", "command")
            .await
            .unwrap()
            .unwrap()
            .result,
        b"result"
    );
    let claims = memory
        .claim_due_outbox_records(10, Duration::from_secs(60), &["fetch"])
        .await
        .unwrap();
    assert_eq!(claims.len(), 1);
    assert_eq!(claims[0].namespace, namespace);
    assert_eq!(claims[0].record.payload, b"effect");
    memory
        .mark_outbox_delivered(&namespace, "entity", &claims[0].record.effect_id)
        .await
        .unwrap();
    assert!(
        memory
            .claim_due_outbox_records(10, Duration::ZERO, &["fetch"])
            .await
            .unwrap()
            .is_empty()
    );
    let current = memory
        .apply_batch(
            &namespace,
            "entity",
            storage::memory::MemoryCommit {
                mutations: &[mutation("count", b"43")],
                owner_epoch: Some(next_lease.owner_epoch()),
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .max_version;
    assert!(current > revision);
    assert!(
        memory
            .apply_batch(
                &namespace,
                "entity",
                storage::memory::MemoryCommit {
                    mutations: &[mutation("count", b"stale")],
                    owner_epoch: Some(owner),
                    ..Default::default()
                },
            )
            .await
            .is_err()
    );
    assert_eq!(
        memory.snapshot(&namespace, "entity").await.unwrap().entries[0].value,
        b"43"
    );
    drop(next_lease);
    drop(memory);
    drop(kv);
    drop(state);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn snapshot_cache_preserves_recent_entities_and_refreshes_committed_values() {
    let root = temporary_root();
    let state = StateStore::open(&root).await.unwrap();
    let mut memory = MemoryStore::from_state(state);
    memory.set_snapshot_cache_limits(2, 1024 * 1024);
    let namespace = worker_namespace("worker", "MEMORY");
    for entity in ["a", "b", "a", "c", "a"] {
        assert!(
            memory
                .snapshot(&namespace, entity)
                .await
                .unwrap()
                .entries
                .is_empty()
        );
    }
    let counters = memory.cache_performance_snapshot();
    assert_eq!(counters.snapshot_hits, 2);
    assert_eq!(counters.snapshot_evictions, 1);
    memory.snapshot(&namespace, "b").await.unwrap();
    assert_eq!(memory.cache_performance_snapshot().snapshot_misses, 4);
    memory
        .apply_batch(
            &namespace,
            "a",
            storage::memory::MemoryCommit {
                mutations: &[mutation("value", b"updated")],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        memory.snapshot(&namespace, "a").await.unwrap().entries[0].value,
        b"updated"
    );
    drop(memory);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn commits_preserve_unrelated_memory_snapshots_on_the_same_shard() {
    let root = temporary_root();
    let state = StateStore::open(&root).await.unwrap();
    let memory = MemoryStore::from_state(Arc::clone(&state));
    let kv = KvStore::from_state(state);
    let namespace = worker_namespace("worker", "MEMORY");
    let shard = memory.shard_index_for_key(&namespace, "a");
    let other = (0..4096)
        .map(|index| format!("entity-{index}"))
        .find(|entity| memory.shard_index_for_key(&namespace, entity) == shard)
        .expect("fixture must contain a different entity on the same shard");
    for entity in ["a", other.as_str()] {
        memory
            .apply_batch(
                &namespace,
                entity,
                storage::memory::MemoryCommit {
                    mutations: &[mutation("value", b"before")],
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        memory.snapshot(&namespace, entity).await.unwrap();
    }
    let before = memory.cache_performance_snapshot();
    memory
        .apply_batch(
            &namespace,
            &other,
            storage::memory::MemoryCommit {
                mutations: &[mutation("value", b"after")],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    kv.put("worker", "MEMORY", "a", "separate KV value")
        .await
        .unwrap();
    assert_eq!(
        memory.snapshot(&namespace, "a").await.unwrap().entries[0].value,
        b"before"
    );
    assert_eq!(
        memory.cache_performance_snapshot().snapshot_hits,
        before.snapshot_hits + 1
    );
    assert_eq!(
        memory.snapshot(&namespace, &other).await.unwrap().entries[0].value,
        b"after"
    );
    assert_eq!(
        memory.cache_performance_snapshot().snapshot_misses,
        before.snapshot_misses + 1
    );
    drop(memory);
    drop(kv);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn memory_facades_share_snapshot_freshness_and_cache_limits() {
    let root = temporary_root();
    let state = StateStore::open(&root).await.unwrap();
    let mut first = MemoryStore::from_state(Arc::clone(&state));
    first.set_snapshot_cache_limits(1, 1024 * 1024);
    let second = MemoryStore::from_state(state);
    let namespace = worker_namespace("worker", "MEMORY");
    assert!(
        first
            .snapshot(&namespace, "a")
            .await
            .unwrap()
            .entries
            .is_empty()
    );
    second
        .apply_batch(
            &namespace,
            "a",
            storage::memory::MemoryCommit {
                mutations: &[mutation("value", b"committed")],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        first.snapshot(&namespace, "a").await.unwrap().entries[0].value,
        b"committed"
    );
    let before = first.cache_performance_snapshot();
    second.snapshot(&namespace, "b").await.unwrap();
    assert_eq!(
        first.snapshot(&namespace, "a").await.unwrap().entries[0].value,
        b"committed"
    );
    assert_eq!(
        first.cache_performance_snapshot().snapshot_misses,
        before.snapshot_misses + 1
    );
    drop(first);
    drop(second);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn returned_snapshots_do_not_modify_cached_values() {
    let root = temporary_root();
    let state = StateStore::open(&root).await.unwrap();
    let memory = MemoryStore::from_state(state);
    let namespace = worker_namespace("worker", "MEMORY");
    memory
        .apply_batch(
            &namespace,
            "entity",
            storage::memory::MemoryCommit {
                mutations: &[mutation("value", b"original")],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let mut first = memory.snapshot(&namespace, "entity").await.unwrap();
    first.entries[0].value.clear();
    let mut second = memory.snapshot(&namespace, "entity").await.unwrap();
    assert_eq!(second.entries[0].value, b"original");
    second.entries.clear();
    assert_eq!(
        memory.snapshot(&namespace, "entity").await.unwrap().entries[0].value,
        b"original"
    );
    drop(memory);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn effect_commits_refresh_snapshots_but_delivery_maintenance_preserves_them() {
    let root = temporary_root();
    let state = StateStore::open(&root).await.unwrap();
    let memory = MemoryStore::from_state(state);
    let namespace = worker_namespace("worker", "MEMORY");
    assert_eq!(
        memory
            .snapshot(&namespace, "entity")
            .await
            .unwrap()
            .max_version,
        -1
    );
    let applied = memory
        .apply_batch(
            &namespace,
            "entity",
            storage::memory::MemoryCommit {
                outbox_effects: &[MemoryOutboxEffectWrite {
                    kind: "deliver".into(),
                    payload: b"payload".to_vec(),
                }],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        memory
            .snapshot(&namespace, "entity")
            .await
            .unwrap()
            .max_version,
        applied.max_version
    );
    let before = memory.cache_performance_snapshot();
    let claims = memory
        .claim_outbox_records(&namespace, "entity", 1, Duration::from_secs(30))
        .await
        .unwrap();
    assert_eq!(claims.len(), 1);
    assert_eq!(
        memory
            .snapshot(&namespace, "entity")
            .await
            .unwrap()
            .max_version,
        applied.max_version
    );
    memory
        .mark_outbox_delivered(&namespace, "entity", &claims[0].effect_id)
        .await
        .unwrap();
    assert_eq!(
        memory
            .snapshot(&namespace, "entity")
            .await
            .unwrap()
            .max_version,
        applied.max_version
    );
    assert_eq!(
        memory.cache_performance_snapshot().snapshot_hits,
        before.snapshot_hits + 2
    );
    drop(memory);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn converter_requires_orphan_ownership_and_verifies_legacy_values() {
    let source = temporary_root();
    let destination = temporary_root();
    let rejected = temporary_root();
    let namespace_dir = source.join("memory").join("4d454d4f5259");
    std::fs::create_dir_all(&namespace_dir).unwrap();
    let database = turso::Builder::new_local(
        namespace_dir
            .join("shard-0000.db")
            .to_string_lossy()
            .as_ref(),
    )
    .build()
    .await
    .unwrap();
    let conn = database.connect().unwrap();
    conn.execute("CREATE TABLE memory_state(entity_key TEXT,item_key TEXT,value TEXT,value_blob BLOB,encoding TEXT,deleted INTEGER,version INTEGER)", ()).await.unwrap();
    conn.execute(
        "CREATE TABLE memory_meta(entity_key TEXT,max_version INTEGER,owner_epoch INTEGER)",
        (),
    )
    .await
    .unwrap();
    conn.execute("CREATE TABLE memory_commands(entity_key TEXT,idempotency_key TEXT,result_blob BLOB,revision INTEGER)", ()).await.unwrap();
    conn.execute("CREATE TABLE memory_outbox(entity_key TEXT,effect_id TEXT,revision INTEGER,kind TEXT,payload_blob BLOB,status TEXT,attempt_count INTEGER,next_attempt_at_ms INTEGER)", ()).await.unwrap();
    conn.execute(
        "INSERT INTO memory_state VALUES ('entity','text','legacy text',NULL,'utf8',0,80)",
        (),
    )
    .await
    .unwrap();
    conn.execute(
        "INSERT INTO memory_state VALUES ('entity','deleted','',?1,'utf8',1,90)",
        (Vec::<u8>::new(),),
    )
    .await
    .unwrap();
    conn.execute("INSERT INTO memory_meta VALUES ('entity',90,700)", ())
        .await
        .unwrap();
    conn.execute(
        "INSERT INTO memory_commands VALUES ('entity','command',?1,90)",
        (b"stored result".to_vec(),),
    )
    .await
    .unwrap();
    conn.execute(
        "INSERT INTO memory_outbox VALUES ('entity','old-effect',90,'fetch',?1,'pending',3,0)",
        (b"payload".to_vec(),),
    )
    .await
    .unwrap();
    storage::turso_util::checkpoint_database(&database)
        .await
        .unwrap();
    drop(conn);
    drop(database);
    let kv_database = turso::Builder::new_local(source.join("dd-kv.db").to_string_lossy().as_ref())
        .build()
        .await
        .unwrap();
    let conn = kv_database.connect().unwrap();
    conn.execute("CREATE TABLE worker_kv(worker_name TEXT,binding TEXT,key TEXT,value TEXT,value_blob BLOB,encoding TEXT,deleted INTEGER,version INTEGER)", ()).await.unwrap();
    conn.execute(
        "INSERT INTO worker_kv VALUES ('alpha','KV','binary','',?1,'v8sc',0,250)",
        (vec![0u8, 255, 17],),
    )
    .await
    .unwrap();
    storage::turso_util::checkpoint_database(&kv_database)
        .await
        .unwrap();
    drop(conn);
    drop(kv_database);
    let before = std::fs::read(source.join("dd-kv.db")).unwrap();
    let error = storage::convert::convert(&source, &rejected, None)
        .await
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("namespace-map"), "{error}");
    assert!(rejected.join("conversion-incomplete").exists());
    let control = storage::control::ControlStore::open(&source).await.unwrap();
    let database = turso::Builder::new_local(control.path().to_string_lossy().as_ref())
        .build()
        .await
        .unwrap();
    let conn = database.connect().unwrap();
    for worker in ["alpha", "beta"] {
        conn.execute("INSERT INTO deployments(deployment_id,worker_name,source,config_json,assets_json,server_modules_json,created_at_ms) VALUES (?1,?1,'old bundle',?2,'[]','[]',1)", (worker, r#"{"bindings":[{"type":"memory","binding":"MEMORY"}]}"#)).await.unwrap();
        conn.execute("INSERT INTO active_deployments VALUES (?1,?1)", (worker,))
            .await
            .unwrap();
    }
    conn.execute("INSERT INTO deploy_tokens(id,token_hash,created_at_unix,uses,capabilities_json) VALUES ('token','hash',1,7,'{}')", ()).await.unwrap();
    storage::turso_util::checkpoint_database(&database)
        .await
        .unwrap();
    drop(conn);
    drop(database);
    drop(control);
    let ambiguous = temporary_root();
    let error = storage::convert::convert(&source, &ambiguous, None)
        .await
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("2 possible owners"), "{error}");
    std::fs::remove_dir_all(ambiguous).unwrap();
    let map = temporary_root();
    std::fs::write(&map, br#"{"MEMORY":{"worker":"alpha","binding":"MEMORY"}}"#).unwrap();
    let report = storage::convert::convert(&source, &destination, Some(&map))
        .await
        .unwrap();
    assert_eq!(report.archived_deployments, 2);
    assert_eq!(report.rows["control.deploy_tokens"], 1);
    let control = storage::control::ControlStore::open(&destination)
        .await
        .unwrap();
    assert_eq!(control.token_count().await.unwrap(), 1);
    assert!(control.list_deployments(None).await.unwrap().is_empty());
    drop(control);
    assert_eq!(report.rows["worker_kv"], 1);
    assert_eq!(report.rows["memory_state"], 2);
    assert_eq!(std::fs::read(source.join("dd-kv.db")).unwrap(), before);
    assert!(destination.join("conversion-complete").exists());
    let state = StateStore::open(destination.join("state")).await.unwrap();
    let kv = KvStore::from_state(Arc::clone(&state));
    let memory = MemoryStore::from_state(Arc::clone(&state));
    assert_eq!(
        kv.get("alpha", "KV", "binary")
            .await
            .unwrap()
            .unwrap()
            .value,
        [0, 255, 17]
    );
    assert!(kv.put("alpha", "KV", "binary", "next").await.unwrap() > 250);
    let namespace = worker_namespace("alpha", "MEMORY");
    let snapshot = memory.snapshot(&namespace, "entity").await.unwrap();
    assert_eq!(snapshot.max_version, 90);
    assert!(
        snapshot
            .entries
            .iter()
            .any(|entry| entry.key == "deleted" && entry.deleted)
    );
    assert!(
        snapshot
            .entries
            .iter()
            .any(|entry| entry.value == b"legacy text")
    );
    assert!(memory.next_owner_epoch().unwrap() > 700);
    assert_eq!(
        memory.outbox_records(&namespace, "entity").await.unwrap()[0].attempt_count,
        3
    );
    drop(memory);
    drop(kv);
    drop(state);
    for root in [source, destination, rejected] {
        std::fs::remove_dir_all(root).unwrap();
    }
    std::fs::remove_file(map).unwrap();
}

#[test]
fn acknowledged_writes_survive_abrupt_process_exit() {
    let root = temporary_root();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--ignored", "--exact", "crash_child", "--nocapture"])
        .env("DD_STATE_CRASH_TEST_ROOT", &root)
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(77));
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let state = StateStore::open(&root).await.unwrap();
        let kv = KvStore::from_state(Arc::clone(&state));
        assert_eq!(
            kv.get_utf8("crash", "KV", "durable")
                .await
                .unwrap()
                .unwrap(),
            "acknowledged"
        );
        let memory = MemoryStore::from_state(state);
        assert_eq!(
            memory
                .snapshot(&worker_namespace("crash", "MEMORY"), "entity")
                .await
                .unwrap()
                .entries[0]
                .value,
            b"acknowledged"
        );
        let namespace = worker_namespace("crash", "MEMORY");
        let command = memory
            .command_result(&namespace, "entity", "crash-command")
            .await
            .unwrap()
            .unwrap();
        let outbox = memory.outbox_records(&namespace, "entity").await.unwrap();
        assert_eq!(command.result, b"saved result");
        assert_eq!(outbox.len(), 1);
        assert_eq!(outbox[0].payload, b"saved effect");
        assert_eq!(outbox[0].revision, command.revision);
    });
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
#[ignore = "subprocess fixture for abrupt exit"]
fn crash_child() {
    let root = std::env::var("DD_STATE_CRASH_TEST_ROOT").expect("crash subprocess root");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let state = StateStore::open(root).await.unwrap();
        let kv = KvStore::from_state(Arc::clone(&state));
        kv.put("crash", "KV", "durable", "acknowledged")
            .await
            .unwrap();
        let memory = MemoryStore::from_state(state);
        memory
            .apply_batch(
                &worker_namespace("crash", "MEMORY"),
                "entity",
                storage::memory::MemoryCommit {
                    mutations: &[mutation("key", b"acknowledged")],
                    command_result: Some(&MemoryCommandResultWrite {
                        idempotency_key: "crash-command".into(),
                        result: b"saved result".to_vec(),
                    }),
                    outbox_effects: &[MemoryOutboxEffectWrite {
                        kind: "fetch".into(),
                        payload: b"saved effect".to_vec(),
                    }],
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        std::process::exit(77);
    });
}

#[tokio::test]
async fn converter_rejects_json_control_records_without_control_database() {
    for relative in [
        "tokens.json",
        "deploy-tokens.json",
        "workers.json",
        "workers/example.json",
    ] {
        let source = temporary_root();
        let destination = temporary_root();
        let record = source.join(relative);
        std::fs::create_dir_all(record.parent().unwrap()).unwrap();
        let contents = br#"{"version":1,"retained":"control record"}"#;
        std::fs::write(&record, contents).unwrap();

        let error = storage::convert::convert(&source, &destination, None)
            .await
            .expect_err("JSON control records must not be silently archived");
        assert_eq!(error.kind(), common::ErrorKind::BadRequest);
        assert!(error.to_string().contains(relative), "{error}");
        assert!(error.to_string().contains("no control.db"), "{error}");
        assert!(!destination.exists());
        assert_eq!(std::fs::read(&record).unwrap(), contents);
        std::fs::remove_dir_all(source).unwrap();
    }
}

#[tokio::test]
async fn converter_rejects_unsafe_destinations_and_duplicate_namespace_keys_before_writing() {
    let source = temporary_root();
    std::fs::create_dir(&source).unwrap();
    let nested = source.join("converted");
    assert!(
        storage::convert::convert(&source, &nested, None)
            .await
            .is_err()
    );
    assert!(!nested.exists());
    assert!(
        storage::convert::convert(&source, &source, None)
            .await
            .is_err()
    );
    let map = temporary_root();
    let destination = temporary_root();
    for invalid in [
        r#"{"MEMORY":{"worker":"a","binding":"M"},"MEMORY":{"worker":"b","binding":"M"}}"#,
        r#"{"MEMORY":{"worker":"","binding":"M"}}"#,
        r#"{"MEMORY":{"worker":"a","binding":"M","unexpected":true}}"#,
    ] {
        std::fs::write(&map, invalid).unwrap();
        assert!(
            storage::convert::convert(&source, &destination, Some(&map))
                .await
                .is_err()
        );
        assert!(!destination.exists());
    }
    std::fs::remove_file(map).unwrap();
    std::fs::remove_dir_all(source).unwrap();
}
