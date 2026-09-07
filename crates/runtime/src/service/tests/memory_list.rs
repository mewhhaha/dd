use super::*;

#[tokio::test]
#[serial]
async fn memory_list_returns_committed_and_staged_structured_values() {
    let service = test_service(RuntimeConfig::default()).await;
    service
        .deploy_with_config(
            "memory-list".to_string(),
            r#"
export default {
  async fetch(_request, env) {
    const stub = env.ROOM.get(env.ROOM.idFromName("list"));
    await stub.atomic((tx) => {
      tx.put("object", { count: 2 });
      tx.put("text", "hello");
    });
    const entries = await stub.atomic((tx) => {
      tx.put("array", [1, 2]);
      return tx.list();
    });
    return Response.json(entries);
  },
};
"#
            .to_string(),
            DeployConfig {
                bindings: vec![DeployBinding::Memory {
                    binding: "ROOM".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("memory worker should deploy");
    let output = service
        .invoke("memory-list".to_string(), test_invocation())
        .await
        .expect("memory list should succeed");
    assert_eq!(output.status, 200);
    assert_eq!(
        serde_json::from_slice::<Value>(&output.body).expect("JSON memory list"),
        serde_json::json!([
            { "key": "array", "value": [1, 2] },
            { "key": "object", "value": { "count": 2 } },
            { "key": "text", "value": "hello" },
        ]),
    );
}
