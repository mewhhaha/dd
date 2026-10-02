use storage::kv::KvStore;
use storage::state::StateStore;

#[tokio::test]
async fn prefix_lists_match_literal_case_sensitive_keys_and_global_limits() {
    let root = std::env::temp_dir().join(format!("dd-kv-prefix-{}", uuid::Uuid::new_v4()));
    let kv = KvStore::from_state(StateStore::open(&root).await.unwrap());
    let keys = [
        "Apple",
        "Apple:1",
        "Apricot",
        "apple",
        "A%",
        "A%one",
        "Axone",
        "A_one",
        "A\\one",
        "A\0one",
        "A\0two",
        "A\u{d7ff}",
        "A\u{e000}",
        "A\u{10ffff}",
        "A\u{10ffff}one",
        "B",
        "éclair",
        "Éclair",
        "\u{10ffff}",
        "\u{10ffff}one",
        "\u{10ffff}\u{10ffff}",
    ];
    for key in keys {
        kv.put("worker", "KV", key, key).await.unwrap();
    }
    kv.put("other", "KV", "Apple:other", "hidden")
        .await
        .unwrap();
    kv.delete("worker", "KV", "Apple:1").await.unwrap();
    for prefix in [
        "",
        "A",
        "Ap",
        "a",
        "A%",
        "A_",
        "A\\",
        "A\0",
        "A\u{d7ff}",
        "A\u{10ffff}",
        "é",
        "\u{10ffff}",
        "\u{10ffff}\u{10ffff}",
        "missing",
    ] {
        let mut expected: Vec<_> = keys
            .iter()
            .filter(|key| key.starts_with(prefix) && **key != "Apple:1")
            .copied()
            .collect();
        expected.sort();
        for limit in [0, 1, 3, 100] {
            let entries = kv.list("worker", "KV", prefix, limit).await.unwrap();
            let actual: Vec<_> = entries.iter().map(|entry| entry.key.as_str()).collect();
            assert_eq!(
                actual,
                expected[..expected.len().min(limit)],
                "prefix {prefix:?}, limit {limit}"
            );
        }
    }
    drop(kv);
    std::fs::remove_dir_all(root).unwrap();
}
