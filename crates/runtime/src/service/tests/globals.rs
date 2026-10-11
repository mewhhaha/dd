//! Globals a worker relies on beyond fetch: crypto, time frozen between I/O
//! boundaries, and AsyncLocalStorage.
use super::*;

#[tokio::test]
#[serial]
async fn deployed_worker_imports_async_local_storage_from_node_async_hooks() {
    let service = test_service(test_pool_config(1, 1, 1)).await;

    service
        .deploy(
            "async-local-storage".to_string(),
            r#"
import { AsyncLocalStorage } from "node:async_hooks";

const storage = new AsyncLocalStorage();

export default {
  async fetch(request, env, ctx) {
    const requestId = __dd_internals.dd.runtimeRequestId?.();
    const values = await Promise.all([
      storage.run("slow", async () => {
        await ctx.sleep(20);
        return storage.getStore();
      }),
      storage.run("fast", async () => {
        await ctx.sleep(1);
        return storage.getStore();
      }),
    ]);
    storage.enterWith("entered");
    const entered = storage.getStore();
    storage.disable();
    return Response.json({
      values,
      entered,
      disabled: storage.getStore() ?? null,
      requestContextPreserved: __dd_internals.dd.runtimeRequestId?.() === requestId,
    });
  },
};
"#
            .to_string(),
        )
        .await
        .expect("deploy should succeed");

    let output = timeout(
        Duration::from_secs(2),
        service.invoke("async-local-storage".to_string(), test_invocation()),
    )
    .await
    .expect("invoke should not hang")
    .expect("request should succeed");
    assert_eq!(output.status, 200);
    let body: Value = serde_json::from_slice(&output.body).expect("body should be json");
    assert_eq!(body["values"], serde_json::json!(["slow", "fast"]));
    assert_eq!(body["entered"], "entered");
    assert_eq!(body["disabled"], Value::Null);
    assert_eq!(body["requestContextPreserved"], true);
}

#[tokio::test]
#[serial]
async fn spectre_time_mitigation_freezes_time_between_io_boundaries() {
    let service = test_service(test_pool_config(0, 1, 1)).await;

    service
        .deploy("frozen-time".to_string(), frozen_time_worker())
        .await
        .expect("deploy should succeed");

    let output = service
        .invoke("frozen-time".to_string(), test_invocation())
        .await
        .expect("invoke should succeed");
    let payload: FrozenTimeState = crate::json::from_string(
        String::from_utf8(output.body).expect("frozen-time body should be utf8"),
    )
    .expect("frozen-time response should parse");

    assert_eq!(
        payload.now0, payload.now1,
        "Date.now should remain frozen during pure compute"
    );
    assert_eq!(
        payload.perf0, payload.perf1,
        "performance.now should remain frozen during pure compute"
    );
    assert!(
        payload.now2 >= payload.now1,
        "Date.now should not move backwards across I/O boundaries"
    );
    assert!(
        payload.perf2 >= payload.perf1,
        "performance.now should not move backwards across I/O boundaries"
    );
    assert!(
        payload.now2 > payload.now1 || payload.perf2 > payload.perf1,
        "expected frozen clocks to advance after an I/O boundary"
    );
    assert!(payload.guard > 0, "worker should run local compute loop");
}

#[tokio::test]
#[serial]
async fn crypto_globals_work_in_a_worker() {
    let service = test_service(test_pool_config(0, 1, 1)).await;

    service
        .deploy("crypto-worker".to_string(), crypto_worker())
        .await
        .expect("deploy should succeed");

    let output = service
        .invoke("crypto-worker".to_string(), test_invocation())
        .await
        .expect("invoke should succeed");
    let payload: CryptoState =
        crate::json::from_string(String::from_utf8(output.body).expect("body should be utf8"))
            .expect("response should parse");

    assert_eq!(payload.random_length, 16);
    assert!(
        payload.random_non_zero,
        "random bytes should not be all zero"
    );
    assert_eq!(payload.digest_length, 32);
    assert_eq!(
        payload.digest_hex,
        "fdc8751e9cb507759ed6fb3f14b311bb5427acb288ebc5c70e4e06f5c8471d04"
    );
    assert_eq!(payload.hmac_signature_length, 32);
    assert!(payload.hmac_verified, "HMAC signature should verify");
    assert!(
        payload.aes_ciphertext_length > "secret-data".len(),
        "AES-GCM ciphertext should include authentication tag"
    );
    assert_eq!(payload.aes_roundtrip, "secret-data");
    assert!(
        payload.asymmetric_signature_length > 0,
        "asymmetric signature should be non-empty"
    );
    assert!(
        payload.asymmetric_verified,
        "asymmetric signature should verify"
    );
    assert_eq!(payload.uuid.len(), 36, "uuid should be canonical v4 length");
}

#[tokio::test]
#[serial]
async fn async_context_store_survives_promise_boundaries_and_nested_runs() {
    let service = test_service(RuntimeConfig::default()).await;

    service
        .deploy_with_config(
            "async-context".to_string(),
            async_context_worker(),
            DeployConfig {
                egress_allow_hosts: Vec::new(),
                public: false,
                cache: Default::default(),
                internal: DeployInternalConfig { trace: None },
                bindings: Vec::new(),
            },
        )
        .await
        .expect("deploy should succeed");

    let promise = service
        .invoke(
            "async-context".to_string(),
            test_invocation_with_path("/promise", "async-context-promise"),
        )
        .await
        .expect("promise request should succeed");
    assert_eq!(String::from_utf8(promise.body).expect("utf8"), "outer");

    let nested = service
        .invoke(
            "async-context".to_string(),
            test_invocation_with_path("/nested", "async-context-nested"),
        )
        .await
        .expect("nested request should succeed");
    assert_eq!(
        String::from_utf8(nested.body).expect("utf8"),
        "outer:inner:outer"
    );

    let restore = service
        .invoke(
            "async-context".to_string(),
            test_invocation_with_path("/restore", "async-context-restore"),
        )
        .await
        .expect("restore request should succeed");
    assert_eq!(
        String::from_utf8(restore.body).expect("utf8"),
        "missing:missing"
    );
}
