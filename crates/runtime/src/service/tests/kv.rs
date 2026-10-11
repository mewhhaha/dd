//! KV from a worker: batched reads and writes, visibility across isolates,
//! durability, and KV work left to waitUntil.
use super::*;

#[tokio::test]
#[serial]
async fn concurrent_kv_reads_match_sequential_reads_and_preserve_duplicates() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates_per_worker: 1,
        max_inflight_per_isolate: 8,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    let worker_name = "kv-batching-equality".to_string();
    service
        .deploy_with_config(
            worker_name.clone(),
            kv_batching_worker(),
            DeployConfig {
                bindings: vec![DeployBinding::Kv {
                    binding: "MY_KV".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");
    service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/seed", "kv-seed-request"),
        )
        .await
        .expect("seed should succeed");

    let sequential = service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/sequential", "kv-sequential-request"),
        )
        .await
        .expect("sequential read should succeed");
    let queued = service
        .invoke(
            worker_name,
            test_invocation_with_path("/queued", "kv-queued-request"),
        )
        .await
        .expect("queued read should succeed");

    let sequential_values: Vec<Value> = crate::json::from_string(
        String::from_utf8(sequential.body).expect("sequential body should be utf8"),
    )
    .expect("sequential response should parse");
    let queued_values: Vec<Value> = crate::json::from_string(
        String::from_utf8(queued.body).expect("queued body should be utf8"),
    )
    .expect("queued response should parse");

    assert_eq!(queued_values, sequential_values);
    assert_eq!(queued_values.len(), 10);
    assert!(queued_values.iter().all(|value| value == "1"));
}

#[tokio::test]
#[serial]
async fn kv_reads_decode_mixed_values_and_surface_corrupt_values() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates_per_worker: 1,
        max_inflight_per_isolate: 8,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    let worker_name = "kv-batching-mixed".to_string();
    service
        .deploy_with_config(
            worker_name.clone(),
            kv_batching_worker(),
            DeployConfig {
                bindings: vec![DeployBinding::Kv {
                    binding: "MY_KV".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");
    service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/seed", "kv-mixed-seed-request"),
        )
        .await
        .expect("seed should succeed");

    let mixed = service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/mixed", "kv-mixed-request"),
        )
        .await
        .expect("mixed read should succeed");
    let mixed_values: Vec<Value> =
        crate::json::from_string(String::from_utf8(mixed.body).expect("mixed body should be utf8"))
            .expect("mixed response should parse");
    assert_eq!(
        mixed_values,
        vec![
            Value::String("plain".to_string()),
            crate::json::from_string::<Value>(r#"{"ok":true,"n":7}"#.to_string())
                .expect("object value should parse"),
            Value::Null,
            Value::String("plain".to_string()),
            crate::json::from_string::<Value>(r#"{"ok":true,"n":7}"#.to_string())
                .expect("object value should parse"),
        ]
    );

    let listed = service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/list-object", "kv-list-object-request"),
        )
        .await
        .expect("list object read should succeed");
    let listed_values: Vec<Value> =
        crate::json::from_string(String::from_utf8(listed.body).expect("list body should be utf8"))
            .expect("list response should parse");
    assert_eq!(listed_values.len(), 1);
    assert_eq!(
        listed_values[0]["key"],
        Value::String("obj-list".to_string())
    );
    assert_eq!(listed_values[0]["value"]["ok"], Value::Bool(true));
    assert_eq!(listed_values[0]["value"]["n"], Value::from(17));

    let rejected = service
        .invoke(
            worker_name,
            test_invocation_with_path("/reject", "kv-reject-request"),
        )
        .await
        .expect("reject route should return response");
    assert_eq!(rejected.status, 500);
    let body = String::from_utf8(rejected.body).expect("reject body should be utf8");
    assert!(
        body.contains("deserialize failed"),
        "expected queued batch rejection body, got: {body}"
    );
}

#[tokio::test]
#[serial]
async fn concurrent_kv_reads_preserve_each_requests_keys() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates_per_worker: 1,
        max_inflight_per_isolate: 16,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    let worker_name = "kv-batching-scope".to_string();
    service
        .deploy_with_config(
            worker_name.clone(),
            kv_batching_worker(),
            DeployConfig {
                bindings: vec![DeployBinding::Kv {
                    binding: "MY_KV".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");
    service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/seed", "kv-scope-seed-request"),
        )
        .await
        .expect("seed should succeed");

    let left_request = test_invocation_with_path("/scoped?key=left", "kv-scope-left-request");
    let right_request = test_invocation_with_path("/scoped?key=right", "kv-scope-right-request");
    let (left, right) = tokio::join!(
        service.invoke(worker_name.clone(), left_request),
        service.invoke(worker_name, right_request)
    );
    let left_values: Vec<Value> = crate::json::from_string(
        String::from_utf8(left.expect("left invoke should succeed").body)
            .expect("left body should be utf8"),
    )
    .expect("left response should parse");
    let right_values: Vec<Value> = crate::json::from_string(
        String::from_utf8(right.expect("right invoke should succeed").body)
            .expect("right body should be utf8"),
    )
    .expect("right response should parse");

    assert_eq!(left_values.len(), 10);
    assert_eq!(right_values.len(), 10);
    assert!(left_values.iter().all(|value| value == "L"));
    assert!(right_values.iter().all(|value| value == "R"));
}

#[tokio::test]
#[serial]
async fn kv_write_batching_preserves_last_write_wins() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates_per_worker: 1,
        max_inflight_per_isolate: 8,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    let worker_name = "kv-write-batching".to_string();
    service
        .deploy_with_config(
            worker_name.clone(),
            kv_write_worker(),
            DeployConfig {
                bindings: vec![DeployBinding::Kv {
                    binding: "MY_KV".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");
    service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/seed", "kv-write-batch-seed-request"),
        )
        .await
        .expect("seed should succeed");

    let output = service
        .invoke(
            worker_name,
            test_invocation_with_path("/write-batch", "kv-write-batch-request"),
        )
        .await
        .expect("write batch should succeed");
    assert_eq!(
        String::from_utf8(output.body).expect("body should be utf8"),
        "4"
    );
}

#[tokio::test]
#[serial]
async fn kv_commits_are_visible_across_isolates_and_reads_are_independent_values() {
    let service = test_service(RuntimeConfig {
        min_isolates: 2,
        max_isolates_per_worker: 2,
        max_inflight_per_isolate: 1,
        ..RuntimeConfig::default()
    })
    .await;
    service
        .deploy_with_config(
            "kv-visibility".to_string(),
            r#"
const isolate = crypto.randomUUID();
export default {
  async fetch(request, env) {
    const path = new URL(request.url).pathname;
    if (path === "/write") {
      const result = await env.STATE.put("value", { count: 42 });
      return Response.json({ returnedUndefined: result === undefined });
    }
    if (path === "/delete") {
      await env.STATE.delete("value");
      return Response.json(await env.STATE.get("value"));
    }
    const value = await env.STATE.get("value");
    if (value) value.count = 99;
    const reread = await env.STATE.get("value");
    await new Promise((resolve) => setTimeout(resolve, 100));
    return Response.json({ isolate, value: reread });
  },
};
"#
            .to_string(),
            DeployConfig {
                bindings: vec![DeployBinding::Kv {
                    binding: "STATE".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy");
    for result in futures_util::future::join_all(
        (0..4).map(|_| service.invoke("kv-visibility".to_string(), test_invocation())),
    )
    .await
    {
        result.expect("warm isolate pool under concurrent demand");
    }
    wait_for_isolate_total(&service, "kv-visibility", 2).await;

    for expected in [Value::Null, serde_json::json!({ "count": 42 }), Value::Null] {
        let (left, right) = tokio::join!(
            service.invoke("kv-visibility".to_string(), test_invocation()),
            service.invoke("kv-visibility".to_string(), test_invocation()),
        );
        let left: Value =
            serde_json::from_slice(&left.expect("left request").body).expect("left JSON");
        let right: Value =
            serde_json::from_slice(&right.expect("right request").body).expect("right JSON");
        assert_ne!(
            left["isolate"], right["isolate"],
            "requests must exercise both isolate caches"
        );
        assert_eq!(left["value"], expected);
        assert_eq!(right["value"], expected);
        let path = if expected.is_null() {
            "/write"
        } else {
            "/delete"
        };
        let output = service
            .invoke(
                "kv-visibility".to_string(),
                test_invocation_with_path(path, "kv-write"),
            )
            .await
            .expect("mutation");
        if path == "/write" {
            assert_eq!(
                serde_json::from_slice::<Value>(&output.body).expect("write JSON"),
                serde_json::json!({ "returnedUndefined": true })
            );
        }
    }
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
#[serial]
async fn kv_committed_durability_awaits_canonical_write() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates_per_worker: 1,
        max_inflight_per_isolate: 8,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    let worker_name = "kv-committed-durability".to_string();
    service
        .deploy_with_config(
            worker_name.clone(),
            kv_write_worker(),
            DeployConfig {
                bindings: vec![DeployBinding::Kv {
                    binding: "MY_KV".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");
    service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/seed", "kv-committed-durability-seed"),
        )
        .await
        .expect("seed should succeed");

    let put = service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/put-committed-read", "kv-committed-durability-put"),
        )
        .await
        .expect("committed put should succeed");
    let put_body = String::from_utf8(put.body).expect("put body should be utf8");
    assert_eq!(put_body, "12");

    let put_object = service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path(
                "/put-committed-object-read",
                "kv-committed-durability-put-object",
            ),
        )
        .await
        .expect("committed object put should succeed");
    let object_body: Value =
        serde_json::from_slice(&put_object.body).expect("object response should be json");
    assert_eq!(object_body["value"]["ok"], Value::Bool(true));
    assert_eq!(object_body["value"]["n"], Value::from(12));

    let delete = service
        .invoke(
            worker_name,
            test_invocation_with_path("/delete-committed-read", "kv-committed-durability-delete"),
        )
        .await
        .expect("committed delete should succeed");
    let delete_body = String::from_utf8(delete.body).expect("delete body should be utf8");
    assert_eq!(delete_body, "missing");
}

#[tokio::test]
#[serial]
async fn kv_wait_until_write_flushes_after_response() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates_per_worker: 1,
        max_inflight_per_isolate: 8,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    let worker_name = "kv-write-wait-until".to_string();
    service
        .deploy_with_config(
            worker_name.clone(),
            kv_write_worker(),
            DeployConfig {
                bindings: vec![DeployBinding::Kv {
                    binding: "MY_KV".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");
    service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/seed", "kv-write-wait-until-seed-request"),
        )
        .await
        .expect("seed should succeed");

    service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/write-wait-until", "kv-write-wait-until-request"),
        )
        .await
        .expect("wait-until request should succeed");

    let mut observed = None;
    for _ in 0..20 {
        let output = service
            .invoke(
                worker_name.clone(),
                test_invocation_with_path("/read", "kv-write-wait-until-read-request"),
            )
            .await
            .expect("read request should succeed");
        let body = String::from_utf8(output.body).expect("read body should be utf8");
        if body == "8" {
            observed = Some(body);
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let status = service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path(
                "/write-wait-until-result",
                "kv-write-wait-until-status-request",
            ),
        )
        .await
        .expect("status request should succeed");
    let status_body = String::from_utf8(status.body).expect("status body should be utf8");

    assert_eq!(status_body, "ok");
    assert_eq!(observed.as_deref(), Some("8"));
}

#[tokio::test]
#[serial]
async fn kv_wait_until_read_runs_after_response() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates_per_worker: 1,
        max_inflight_per_isolate: 8,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    let worker_name = "kv-read-wait-until".to_string();
    service
        .deploy_with_config(
            worker_name.clone(),
            kv_wait_until_read_worker(),
            DeployConfig {
                bindings: vec![DeployBinding::Kv {
                    binding: "MY_KV".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");
    service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/seed", "kv-read-wait-until-seed-request"),
        )
        .await
        .expect("seed should succeed");

    service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/read-wait-until", "kv-read-wait-until-request"),
        )
        .await
        .expect("wait-until read request should succeed");

    let mut observed = None;
    for _ in 0..20 {
        let output = service
            .invoke(
                worker_name.clone(),
                test_invocation_with_path(
                    "/read-wait-until-result",
                    "kv-read-wait-until-result-request",
                ),
            )
            .await
            .expect("result request should succeed");
        let body = String::from_utf8(output.body).expect("result body should be utf8");
        if body == "1" {
            observed = Some(body);
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert_eq!(observed.as_deref(), Some("1"));
}
