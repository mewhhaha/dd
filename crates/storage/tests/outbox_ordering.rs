use std::sync::Arc;
use std::time::Duration;
use storage::memory::{
    MemoryCommit, MemoryOutboxDeliveryAction, MemoryOutboxDeliveryOutcome, MemoryOutboxEffectWrite,
    MemoryStore, worker_namespace,
};
use storage::state::StateStore;

fn effect(value: u8) -> MemoryOutboxEffectWrite {
    MemoryOutboxEffectWrite {
        kind: "audit.order".into(),
        payload: vec![value],
    }
}

#[tokio::test]
async fn ordinal_order_survives_restart_and_deferred_predecessors_block_successors() {
    let root = std::env::temp_dir().join(format!("dd-outbox-order-{}", uuid::Uuid::new_v4()));
    let namespace = worker_namespace("worker", "MEMORY");
    let memory = MemoryStore::from_state(StateStore::open(&root).await.unwrap());
    memory
        .apply_batch(
            &namespace,
            "entity",
            MemoryCommit {
                outbox_effects: (0..8).map(effect).collect(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    drop(memory);
    let memory = MemoryStore::from_state(StateStore::open(&root).await.unwrap());
    let first = memory
        .claim_outbox_records(&namespace, "entity", 2, Duration::from_secs(30))
        .await
        .unwrap();
    assert_eq!(
        first
            .iter()
            .map(|record| record.payload[0])
            .collect::<Vec<_>>(),
        [0, 1]
    );
    assert_eq!(
        first
            .iter()
            .map(|record| record.ordinal)
            .collect::<Vec<_>>(),
        [0, 1]
    );
    assert!(
        memory
            .claim_outbox_records(&namespace, "entity", 64, Duration::from_secs(30))
            .await
            .unwrap()
            .is_empty(),
        "an active predecessor lease blocks the suffix"
    );
    memory
        .apply_outbox_delivery_outcomes(&[
            MemoryOutboxDeliveryOutcome {
                namespace: namespace.clone(),
                memory_key: "entity".into(),
                effect_id: first[0].effect_id.clone(),
                action: MemoryOutboxDeliveryAction::Delivered,
            },
            MemoryOutboxDeliveryOutcome {
                namespace: namespace.clone(),
                memory_key: "entity".into(),
                effect_id: first[1].effect_id.clone(),
                action: MemoryOutboxDeliveryAction::Retry {
                    retry_after: Duration::from_secs(3600),
                },
            },
        ])
        .await
        .unwrap();
    memory
        .apply_batch(
            &namespace,
            "entity",
            MemoryCommit {
                outbox_effects: vec![effect(8)],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(
        memory
            .claim_due_outbox_records(64, Duration::from_secs(30), &["audit.*"])
            .await
            .unwrap()
            .is_empty(),
        "a deferred retry blocks later effects and revisions"
    );
    memory
        .retry_outbox_record(&namespace, "entity", &first[1].effect_id, Duration::ZERO)
        .await
        .unwrap();
    drop(first);
    let suffix = memory
        .claim_outbox_records(&namespace, "entity", 64, Duration::from_secs(30))
        .await
        .unwrap();
    assert_eq!(
        suffix
            .iter()
            .map(|record| record.payload[0])
            .collect::<Vec<_>>(),
        [1, 2, 3, 4, 5, 6, 7, 8]
    );
    drop(suffix);
    drop(memory);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn payload_leases_share_a_byte_budget_across_stores_and_shards() {
    let root = std::env::temp_dir().join(format!("dd-outbox-bytes-{}", uuid::Uuid::new_v4()));
    let state = StateStore::open(&root).await.unwrap();
    let mut first_store = MemoryStore::from_state(Arc::clone(&state));
    let second_store = MemoryStore::from_state(Arc::clone(&state));
    first_store.set_outbox_claim_byte_limit(1000).unwrap();
    let namespace = worker_namespace("worker", "MEMORY");
    let first_entity = "entity-0";
    let second_entity = (1..)
        .map(|index| format!("entity-{index}"))
        .find(|entity| {
            StateStore::shard_index("worker", "MEMORY", entity)
                != StateStore::shard_index("worker", "MEMORY", first_entity)
        })
        .unwrap();
    for entity in [first_entity, second_entity.as_str()] {
        first_store
            .apply_batch(
                &namespace,
                entity,
                MemoryCommit {
                    outbox_effects: vec![MemoryOutboxEffectWrite {
                        kind: "audit.bytes".into(),
                        payload: vec![0; 256],
                    }],
                    ..Default::default()
                },
            )
            .await
            .unwrap();
    }
    let claims = first_store
        .claim_due_outbox_records_for_shard_index(
            StateStore::shard_index("worker", "MEMORY", first_entity),
            64,
            Duration::ZERO,
            &["audit.*"],
        )
        .await
        .unwrap();
    assert_eq!(claims.len(), 1);
    let guard = claims[0].payload_lease().unwrap();
    let charged = first_store.outbox_claimed_bytes();
    assert!(
        (512..=1000).contains(&charged),
        "charge covers payload and parsed payload copy"
    );
    drop(claims);
    assert_eq!(
        second_store.outbox_claimed_bytes(),
        charged,
        "delivery guard retains admission after claims are consumed"
    );
    assert!(
        second_store
            .claim_due_outbox_records_for_shard_index(
                StateStore::shard_index("worker", "MEMORY", &second_entity),
                64,
                Duration::ZERO,
                &["audit.*"]
            )
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        first_store.set_outbox_claim_byte_limit(2000).is_err(),
        "active budget cannot be replaced"
    );
    drop(guard);
    assert_eq!(first_store.outbox_claimed_bytes(), 0);
    let records = second_store
        .claim_outbox_records(&namespace, &second_entity, 64, Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(records.len(), 1);
    assert!(
        first_store.outbox_claimed_bytes() > 0,
        "record-returning API also retains the lease"
    );
    drop(records);
    assert_eq!(first_store.outbox_claimed_bytes(), 0);
    assert!(
        first_store
            .apply_batch(
                &namespace,
                "too-large",
                MemoryCommit {
                    outbox_effects: vec![MemoryOutboxEffectWrite {
                        kind: "audit.bytes".into(),
                        payload: vec![0; 1000]
                    }],
                    ..Default::default()
                }
            )
            .await
            .is_err()
    );
    assert!(
        first_store
            .outbox_records(&namespace, "too-large")
            .await
            .unwrap()
            .is_empty()
    );
    drop(second_store);
    drop(first_store);
    drop(state);
    std::fs::remove_dir_all(root).unwrap();
}
