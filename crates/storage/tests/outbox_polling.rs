use std::time::Duration;
use storage::memory::{MemoryCommit, MemoryOutboxEffectWrite, MemoryStore, worker_namespace};
use storage::state::StateStore;

#[tokio::test]
async fn empty_outbox_polling_does_not_commit_state_transactions() {
    let root = std::env::temp_dir().join(format!("dd-empty-outbox-{}", uuid::Uuid::new_v4()));
    let state = StateStore::open(&root).await.unwrap();
    let memory = MemoryStore::from_state(state);
    for _ in 0..3 {
        assert!(
            memory
                .claim_due_outbox_records(32, Duration::from_secs(30), &["fetch"])
                .await
                .unwrap()
                .is_empty()
        );
    }
    let snapshot = memory.state_performance_snapshot();
    assert_eq!(
        snapshot.committed_groups, 0,
        "empty polls must not create durable commits"
    );
    assert_eq!(snapshot.pending_commands, 0);
    drop(memory);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn unmatched_outbox_kinds_do_not_commit_state_transactions() {
    let root = std::env::temp_dir().join(format!("dd-unmatched-outbox-{}", uuid::Uuid::new_v4()));
    let state = StateStore::open(&root).await.unwrap();
    let memory = MemoryStore::from_state(state);
    let namespace = worker_namespace("worker", "MEMORY");
    memory
        .apply_batch(
            &namespace,
            "entity",
            MemoryCommit {
                outbox_effects: &[MemoryOutboxEffectWrite {
                    kind: "audit.custom".into(),
                    payload: b"retained effect".to_vec(),
                }],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let committed = memory.state_performance_snapshot().committed_groups;
    for _ in 0..3 {
        assert!(
            memory
                .claim_due_outbox_records(32, Duration::from_secs(30), &["socket.*"])
                .await
                .unwrap()
                .is_empty()
        );
    }
    assert_eq!(
        memory.state_performance_snapshot().committed_groups,
        committed
    );
    let claims = memory
        .claim_due_outbox_records(32, Duration::from_secs(30), &["audit.*"])
        .await
        .unwrap();
    assert_eq!(claims.len(), 1);
    assert_eq!(claims[0].record.payload, b"retained effect");
    drop(memory);
    std::fs::remove_dir_all(root).unwrap();
}
