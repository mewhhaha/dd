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
