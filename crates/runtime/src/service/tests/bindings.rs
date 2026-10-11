//! The bindings a deployment declares and the `env` a worker reaches them
//! through.
use super::*;

#[tokio::test]
#[serial]
async fn worker_exposes_static_bindings_and_transactional_websockets() {
    let service = test_service(RuntimeConfig::default()).await;
    service
        .deploy_with_config(
            "public-surface".to_string(),
            r#"
export default {
  async fetch(_request, env) {
    const room = env.ROOM.get(env.ROOM.idFromName("room"));
    return Response.json({
      fetch: typeof fetch,
      socketAccept: await room.atomic((tx) => typeof tx.accept),
      rpcTarget: typeof globalThis.RpcTarget,
      webTransport: typeof globalThis.WebTransport,
      webTransportSession: typeof globalThis.WebTransportSession,
      kvGet: typeof env.KV.get,
      memoryAtomic: typeof room.atomic,
      serviceFetch: typeof env.SERVICE.fetch,
    });
  },
};
"#
            .to_string(),
            DeployConfig {
                bindings: vec![
                    DeployBinding::Kv {
                        binding: "KV".to_string(),
                    },
                    DeployBinding::Memory {
                        binding: "ROOM".to_string(),
                    },
                    DeployBinding::Service {
                        binding: "SERVICE".to_string(),
                        service: "public-surface".to_string(),
                    },
                ],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("static deployment should succeed");
    let output = service
        .invoke("public-surface".to_string(), test_invocation())
        .await
        .expect("worker surface should be observable");
    assert_eq!(output.status, 200);
    assert_eq!(
        serde_json::from_slice::<Value>(&output.body).expect("JSON worker surface"),
        serde_json::json!({
            "fetch": "function",
            "socketAccept": "function",
            "rpcTarget": "undefined",
            "webTransport": "undefined",
            "webTransportSession": "undefined",
            "kvGet": "function",
            "memoryAtomic": "function",
            "serviceFetch": "function",
        }),
    );
}

#[tokio::test]
#[serial]
async fn shared_env_is_reused_safely_across_requests() {
    let service = test_service(test_pool_config(1, 1, 8)).await;

    let worker_name = "shared-env-reuse".to_string();
    service
        .deploy_with_config(
            worker_name.clone(),
            reusable_env_worker(),
            DeployConfig {
                bindings: vec![DeployBinding::Kv {
                    binding: "MY_KV".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");

    let first = service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/", "shared-env-first-request"),
        )
        .await
        .expect("first invoke should succeed");
    let second = service
        .invoke(
            worker_name,
            test_invocation_with_path("/", "shared-env-second-request"),
        )
        .await
        .expect("second invoke should succeed");

    let first_payload: Value =
        crate::json::from_string(String::from_utf8(first.body).expect("first body should be utf8"))
            .expect("first response should parse");
    let second_payload: Value = crate::json::from_string(
        String::from_utf8(second.body).expect("second body should be utf8"),
    )
    .expect("second response should parse");

    assert_eq!(first_payload["sameEnv"], Value::Bool(false));
    assert_eq!(first_payload["sameKv"], Value::Bool(false));
    assert_eq!(second_payload["sameEnv"], Value::Bool(true));
    assert_eq!(second_payload["sameKv"], Value::Bool(true));

    for payload in [&first_payload, &second_payload] {
        assert_eq!(payload["envExtensible"], Value::Bool(false));
        assert_eq!(payload["kvExtensible"], Value::Bool(false));
        assert_eq!(payload["envMutationResult"], Value::Bool(false));
        assert_eq!(payload["kvMutationResult"], Value::Bool(false));
        assert_eq!(payload["envHasTemp"], Value::Bool(false));
        assert_eq!(payload["kvHasTemp"], Value::Bool(false));
    }
}

#[test]
fn extract_bindings_collects_declared_bindings() {
    let bindings = crate::service::extract_bindings(&DeployConfig {
        bindings: vec![
            DeployBinding::Kv {
                binding: "MY_KV".to_string(),
            },
            DeployBinding::Memory {
                binding: "STATE".to_string(),
            },
            DeployBinding::Service {
                binding: "AUTH".to_string(),
                service: "auth-worker".to_string(),
            },
        ],
        ..DeployConfig::default()
    })
    .expect("bindings should parse");

    assert_eq!(bindings.kv, vec!["MY_KV".to_string()]);
    assert_eq!(bindings.memory, vec!["STATE".to_string()]);
    assert_eq!(bindings.service.len(), 1);
    assert_eq!(bindings.service[0].binding, "AUTH");
    assert_eq!(bindings.service[0].service, "auth-worker");
}

#[test]
fn extract_bindings_rejects_duplicate_service_name() {
    let result = crate::service::extract_bindings(&DeployConfig {
        bindings: vec![
            DeployBinding::Service {
                service: "child".to_string(),
                binding: "SANDBOX".to_string(),
            },
            DeployBinding::Service {
                service: "child".to_string(),
                binding: "SANDBOX".to_string(),
            },
        ],
        ..DeployConfig::default()
    });
    assert!(result.is_err());
}
