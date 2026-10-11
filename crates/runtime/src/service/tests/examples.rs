use super::*;

#[tokio::test]
#[serial]
async fn memory_examples_commit_ping_counts() {
    let service = test_service(RuntimeConfig::default()).await;
    for (name, source, path) in [
        (
            "memory-example",
            include_str!("../../../../../examples/memory.js"),
            "/ping?user=example",
        ),
        (
            "traced-example",
            include_str!("../../../../../examples/hello-traced.js"),
            "/api/memory/ping?user=example",
        ),
    ] {
        service
            .deploy_with_config(
                name.to_string(),
                source.to_string(),
                DeployConfig {
                    bindings: vec![DeployBinding::Memory {
                        binding: "USER_MEMORY".to_string(),
                    }],
                    ..DeployConfig::default()
                },
            )
            .await
            .expect("example should deploy");
        for count in 1..=2 {
            let output = service
                .invoke(name.to_string(), test_invocation_with_path(path, name))
                .await
                .expect("example ping should succeed");
            assert_eq!(output.status, 200, "{name}");
            let body: Value = serde_json::from_slice(&output.body).expect("JSON ping");
            assert_eq!(body["pings"], count, "{name}");
        }
    }
}

fn example_request(
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> WorkerInvocation {
    let mut invocation = test_invocation_with_path(path, "example");
    invocation.method = method.to_string();
    invocation.headers = headers
        .iter()
        .map(|(name, value)| (name.to_string(), value.to_string()))
        .collect();
    invocation.body = body.to_vec();
    invocation
}

async fn deploy_example(service: &RuntimeService, name: &str, source: &str, kv: Option<&str>) {
    service
        .deploy_with_config(
            name.to_string(),
            source.to_string(),
            DeployConfig {
                bindings: kv
                    .map(|binding| DeployBinding::Kv {
                        binding: binding.to_string(),
                    })
                    .into_iter()
                    .collect(),
                ..DeployConfig::default()
            },
        )
        .await
        .expect("example should deploy");
}

#[tokio::test]
#[serial]
async fn cache_vary_example_keys_entries_by_language() {
    let service = test_service(RuntimeConfig::default()).await;
    deploy_example(
        &service,
        "cache-vary",
        include_str!("../../../../../examples/cache-vary.js"),
        None,
    )
    .await;
    // The repeated "fr" is answered from the cache entry the first one stored.
    for (language, greeting) in [
        ("fr", "salut"),
        ("es", "hola"),
        ("fr", "salut"),
        ("en", "hello"),
    ] {
        let output = service
            .invoke(
                "cache-vary".to_string(),
                example_request("GET", "/greet", &[("accept-language", language)], b""),
            )
            .await
            .expect("greeting should succeed");
        assert_eq!(output.status, 200, "{language}");
        assert_eq!(output.body, greeting.as_bytes(), "{language}");
    }
}

#[tokio::test]
#[serial]
async fn receipts_example_round_trips_through_kv() {
    let service = test_service(RuntimeConfig::default()).await;
    deploy_example(
        &service,
        "receipts",
        include_str!("../../../../../examples/receipts.js"),
        Some("RECEIPTS"),
    )
    .await;
    let invoke = |method: &str, path: &str, body: &[u8]| {
        service.invoke(
            "receipts".to_string(),
            example_request(method, path, &[], body),
        )
    };

    let created = invoke(
        "POST",
        "/receipts",
        br#"{"merchant":"Bakery","tax_rate":0.25,"items":[{"description":"Bread","quantity":2,"unit_price":3}]}"#,
    )
    .await
    .expect("create should succeed");
    assert_eq!(created.status, 201);
    let created: Value = serde_json::from_slice(&created.body).expect("JSON receipt");
    assert_eq!(created["receipt"]["total"], 7.5);
    let id = created["receipt"]["id"].as_str().expect("receipt id");
    let path = format!("/receipts/{id}");

    let listed = invoke("GET", "/receipts", b"")
        .await
        .expect("list should succeed");
    let listed: Value = serde_json::from_slice(&listed.body).expect("JSON list");
    assert_eq!(listed["count"], 1);
    assert_eq!(listed["receipts"][0]["id"], id);

    let fetched = invoke("GET", &path, b"").await.expect("get should succeed");
    assert_eq!(fetched.status, 200);
    let deleted = invoke("DELETE", &path, b"")
        .await
        .expect("delete should succeed");
    assert_eq!(deleted.status, 200);
    let missing = invoke("GET", &path, b"").await.expect("get should answer");
    assert_eq!(missing.status, 404);
}

#[tokio::test]
#[serial]
async fn wait_until_kv_example_writes_after_responding() {
    let service = test_service(RuntimeConfig::default()).await;
    deploy_example(
        &service,
        "wait-until-kv",
        include_str!("../../../../../examples/wait-until-kv.js"),
        Some("MY_KV"),
    )
    .await;
    let request_id = [("x-request-id", "example")];
    let queued = service
        .invoke(
            "wait-until-kv".to_string(),
            example_request("GET", "/queue", &request_id, b""),
        )
        .await
        .expect("queue should succeed");
    assert_eq!(queued.status, 200);
    assert_eq!(queued.body, b"queued:example");

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let status = service
            .invoke(
                "wait-until-kv".to_string(),
                example_request("GET", "/status", &request_id, b""),
            )
            .await
            .expect("status should succeed");
        if status.body == b"done" {
            break;
        }
        assert!(Instant::now() < deadline, "waitUntil write never landed");
        sleep(Duration::from_millis(20)).await;
    }
}
