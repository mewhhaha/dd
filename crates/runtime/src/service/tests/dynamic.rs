use super::*;

#[tokio::test]
#[serial]
async fn dynamic_namespace_can_create_and_invoke_dynamic_workers() {
    let service = test_service(dynamic_autoscaling_config()).await;

    service
        .deploy_with_config(
            "dynamic-parent".to_string(),
            dynamic_namespace_worker(),
            DeployConfig {
                bindings: vec![DeployBinding::Dynamic {
                    binding: "SANDBOX".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");

    let one = invoke_with_timeout_and_dump(
        &service,
        "dynamic-parent",
        test_invocation(),
        "dynamic host rpc autoscaling first invoke",
    )
    .await;
    let two = invoke_with_timeout_and_dump(
        &service,
        "dynamic-parent",
        test_invocation(),
        "dynamic host rpc autoscaling second invoke",
    )
    .await;

    assert_eq!(String::from_utf8(one.body).expect("utf8"), "1:1");
    assert_eq!(String::from_utf8(two.body).expect("utf8"), "2:2");
}

#[tokio::test]
#[serial]
async fn dynamic_namespace_default_host_rpc_requires_explicit_policy() {
    let service = test_service(dynamic_single_isolate_config()).await;

    service
        .deploy_with_config(
            "dynamic-rpc-policy".to_string(),
            dynamic_policy_default_host_rpc_denied_worker(),
            DeployConfig {
                bindings: vec![DeployBinding::Dynamic {
                    binding: "SANDBOX".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");

    let output = invoke_with_timeout_and_dump(
        &service,
        "dynamic-rpc-policy",
        test_invocation(),
        "dynamic host rpc default policy denial",
    )
    .await;
    let body = String::from_utf8(output.body).expect("utf8");
    assert!(body.contains("allow_host_rpc"));
}

#[tokio::test]
#[serial]
async fn dynamic_namespace_default_egress_is_denied() {
    let service = test_service(dynamic_single_isolate_config()).await;

    service
        .deploy_with_config(
            "dynamic-egress-policy".to_string(),
            dynamic_policy_default_egress_denied_worker(),
            DeployConfig {
                bindings: vec![DeployBinding::Dynamic {
                    binding: "SANDBOX".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");

    let output = invoke_with_timeout_and_dump(
        &service,
        "dynamic-egress-policy",
        test_invocation(),
        "dynamic default egress denial",
    )
    .await;
    let body = String::from_utf8(output.body).expect("utf8");
    assert!(body.contains("egress origin is not allowed"));
}

#[tokio::test]
#[serial]
async fn dynamic_namespace_default_cache_access_is_denied() {
    let service = test_service(dynamic_single_isolate_config()).await;

    service
        .deploy_with_config(
            "dynamic-cache-policy".to_string(),
            dynamic_policy_default_cache_denied_worker(),
            DeployConfig {
                bindings: vec![DeployBinding::Dynamic {
                    binding: "SANDBOX".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");

    let output = invoke_with_timeout_and_dump(
        &service,
        "dynamic-cache-policy",
        test_invocation(),
        "dynamic default cache denial",
    )
    .await;
    let body = String::from_utf8(output.body).expect("utf8");
    assert!(body.contains("blocks cache access"));
}

#[tokio::test]
#[serial]
async fn dynamic_namespace_quota_kill_recreates_child_cleanly() {
    let service = test_service(dynamic_single_isolate_config()).await;

    service
        .deploy_with_config(
            "dynamic-quota-recovery".to_string(),
            dynamic_policy_quota_kill_recovery_worker(),
            DeployConfig {
                bindings: vec![DeployBinding::Dynamic {
                    binding: "SANDBOX".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");

    let first = invoke_with_timeout_and_dump(
        &service,
        "dynamic-quota-recovery",
        test_invocation_with_path("/boom", "dynamic-quota-boom"),
        "dynamic quota kill first invoke",
    )
    .await;
    assert_eq!(first.status, 502);
    let first_body = String::from_utf8(first.body).expect("utf8");
    assert!(first_body.contains("max_response_bytes"));

    let second = invoke_with_timeout_and_dump(
        &service,
        "dynamic-quota-recovery",
        test_invocation_with_path("/recover", "dynamic-quota-recover"),
        "dynamic quota recovery second invoke",
    )
    .await;
    assert_eq!(second.status, 200);
    assert_eq!(String::from_utf8(second.body).expect("utf8"), "ok");
}

#[tokio::test]
#[serial]
async fn dynamic_namespace_plain_child_fetch_completes_in_single_isolate() {
    let service = test_service(dynamic_single_isolate_config()).await;

    service
        .deploy_with_config(
            "dynamic-plain-single".to_string(),
            dynamic_plain_namespace_worker(),
            DeployConfig {
                bindings: vec![DeployBinding::Dynamic {
                    binding: "SANDBOX".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");

    let one = invoke_with_timeout_and_dump(
        &service,
        "dynamic-plain-single",
        test_invocation(),
        "dynamic plain single-isolate first invoke",
    )
    .await;
    let two = invoke_with_timeout_and_dump(
        &service,
        "dynamic-plain-single",
        test_invocation(),
        "dynamic plain single-isolate second invoke",
    )
    .await;

    assert_eq!(String::from_utf8(one.body).expect("utf8"), "1");
    assert_eq!(String::from_utf8(two.body).expect("utf8"), "2");
}

#[tokio::test]
#[serial]
async fn dynamic_namespace_concurrent_hot_fetch_completes_in_single_isolate() {
    let service = test_service(dynamic_single_isolate_config()).await;

    service
        .deploy_with_config(
            "dynamic-plain-concurrent".to_string(),
            dynamic_plain_handle_cache_worker(),
            DeployConfig {
                bindings: vec![DeployBinding::Dynamic {
                    binding: "SANDBOX".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");

    let mut tasks = Vec::new();
    for idx in 0..32 {
        let service = service.clone();
        tasks.push(tokio::spawn(async move {
            invoke_with_timeout_and_dump(
                &service,
                "dynamic-plain-concurrent",
                test_invocation_with_path("/", &format!("dynamic-plain-concurrent-{idx}")),
                &format!("dynamic plain concurrent single-isolate {idx}"),
            )
            .await
        }));
    }

    let mut bodies = Vec::new();
    for task in tasks {
        let output = task.await.expect("task should join");
        assert_eq!(output.status, 200);
        bodies.push(String::from_utf8(output.body).expect("utf8"));
    }

    assert_eq!(bodies.len(), 32);
}

#[derive(Deserialize)]
struct DynamicFastFetchMetrics {
    #[serde(default, rename = "remoteFetchHit")]
    remote_fetch_hit: u64,
    #[serde(default, rename = "remoteFetchFallback")]
    remote_fetch_fallback: u64,
    #[serde(default, rename = "fastFetchPathHit")]
    fast_fetch_path_hit: u64,
    #[serde(default, rename = "direct_fetch_fast_path_hit")]
    direct_fetch_fast_path_hit: u64,
    #[serde(default, rename = "direct_fetch_fast_path_fallback")]
    direct_fetch_fast_path_fallback: u64,
    #[serde(default, rename = "warm_isolate_hit")]
    warm_isolate_hit: u64,
    #[serde(default, rename = "fallback_dispatch")]
    fallback_dispatch: u64,
    #[serde(default, rename = "timeSyncApplied")]
    time_sync_applied: u64,
    #[serde(default, rename = "direct_fetch_dispatch_count")]
    direct_fetch_dispatch_count: u64,
    #[serde(default, rename = "control_drain_batch")]
    control_drain_batch: u64,
}

#[tokio::test]
#[serial]
async fn dynamic_fast_fetch_preserves_request_response_shape_and_records_direct_metrics() {
    let service = test_service(dynamic_single_isolate_config()).await;

    service
        .deploy_with_config(
            "dynamic-fast-fetch".to_string(),
            dynamic_fast_fetch_worker(),
            DeployConfig {
                bindings: vec![DeployBinding::Dynamic {
                    binding: "SANDBOX".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");

    invoke_with_timeout_and_dump(
        &service,
        "dynamic-fast-fetch",
        test_invocation_with_path("/__dynamic_metrics_reset", "dynamic-fast-reset"),
        "dynamic fast metrics reset",
    )
    .await;

    invoke_with_timeout_and_dump(
        &service,
        "dynamic-fast-fetch",
        test_invocation_with_path("/hot", "dynamic-fast-hot-1"),
        "dynamic fast first hot invoke",
    )
    .await;
    invoke_with_timeout_and_dump(
        &service,
        "dynamic-fast-fetch",
        test_invocation_with_path("/hot", "dynamic-fast-hot-2"),
        "dynamic fast second hot invoke",
    )
    .await;

    let binary_body = vec![0, 1, 2, 3, 255];
    let parity = invoke_with_timeout_and_dump(
        &service,
        "dynamic-fast-fetch",
        WorkerInvocation {
            method: "POST".to_string(),
            url: "http://worker/echo?q=hello&status=207".to_string(),
            headers: vec![
                (
                    "content-type".to_string(),
                    "application/octet-stream".to_string(),
                ),
                ("x-test".to_string(), "alpha".to_string()),
            ],
            body: binary_body.clone(),
            request_id: "dynamic-fast-echo".to_string(),
        },
        "dynamic fast parity invoke",
    )
    .await;
    assert_eq!(parity.status, 207);
    assert_eq!(parity.body, binary_body);
    assert!(parity
        .headers
        .iter()
        .any(|(name, value)| name.eq_ignore_ascii_case("x-method") && value == "POST"));
    assert!(parity
        .headers
        .iter()
        .any(|(name, value)| name.eq_ignore_ascii_case("x-query") && value == "hello"));
    assert!(parity
        .headers
        .iter()
        .any(|(name, value)| name.eq_ignore_ascii_case("x-test") && value == "alpha"));

    let metrics_output = invoke_with_timeout_and_dump(
        &service,
        "dynamic-fast-fetch",
        test_invocation_with_path("/__dynamic_metrics", "dynamic-fast-metrics"),
        "dynamic fast metrics read",
    )
    .await;
    let metrics: DynamicFastFetchMetrics = crate::json::from_string(
        String::from_utf8(metrics_output.body).expect("metrics body should be utf8"),
    )
    .expect("metrics should parse");
    assert!(
        metrics.fast_fetch_path_hit >= 3,
        "{:?}",
        metrics.fast_fetch_path_hit
    );
    assert!(
        metrics.remote_fetch_hit >= 3,
        "{:?}",
        metrics.remote_fetch_hit
    );
    assert_eq!(metrics.remote_fetch_fallback, 0);
    assert!(metrics.direct_fetch_fast_path_hit >= 2);
    assert!(metrics.direct_fetch_dispatch_count >= 2);
    assert!(metrics.control_drain_batch >= 1);
    assert!(
        metrics.time_sync_applied >= 1,
        "{:?}",
        metrics.time_sync_applied
    );
}

#[tokio::test]
#[serial]
async fn dynamic_fast_fetch_repeated_warm_requests_stay_on_direct_path() {
    let service = test_service(dynamic_single_isolate_config()).await;

    service
        .deploy_with_config(
            "dynamic-fast-repeat".to_string(),
            dynamic_fast_fetch_worker(),
            DeployConfig {
                bindings: vec![DeployBinding::Dynamic {
                    binding: "SANDBOX".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");

    invoke_with_timeout_and_dump(
        &service,
        "dynamic-fast-repeat",
        test_invocation_with_path("/hot", "dynamic-fast-repeat-warm"),
        "dynamic fast repeat warm invoke",
    )
    .await;

    invoke_with_timeout_and_dump(
        &service,
        "dynamic-fast-repeat",
        test_invocation_with_path("/__dynamic_metrics_reset", "dynamic-fast-repeat-reset"),
        "dynamic fast repeat reset",
    )
    .await;

    for index in 0..64 {
        let request_id = format!("dynamic-fast-repeat-{index}");
        invoke_with_timeout_and_dump(
            &service,
            "dynamic-fast-repeat",
            test_invocation_with_path("/hot", &request_id),
            "dynamic fast repeat hot invoke",
        )
        .await;
    }

    let metrics_output = invoke_with_timeout_and_dump(
        &service,
        "dynamic-fast-repeat",
        test_invocation_with_path("/__dynamic_metrics", "dynamic-fast-repeat-metrics"),
        "dynamic fast repeat metrics",
    )
    .await;
    let metrics: DynamicFastFetchMetrics = crate::json::from_string(
        String::from_utf8(metrics_output.body).expect("metrics body should be utf8"),
    )
    .expect("metrics should parse");
    assert!(metrics.remote_fetch_hit >= 64);
    assert!(metrics.direct_fetch_fast_path_hit >= 64);
    assert_eq!(metrics.direct_fetch_fast_path_fallback, 0);
    assert_eq!(metrics.fallback_dispatch, 0);
    assert!(metrics.warm_isolate_hit >= 64);
}

#[tokio::test]
#[serial]
async fn dynamic_fast_fetch_recovers_after_preferred_isolate_failure_and_rewarms() {
    let service = test_service(dynamic_single_isolate_config()).await;

    service
        .deploy_with_config(
            "dynamic-fast-rewarm".to_string(),
            dynamic_remote_fast_fetch_worker(),
            DeployConfig {
                bindings: vec![DeployBinding::Dynamic {
                    binding: "SANDBOX".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");

    invoke_with_timeout_and_dump(
        &service,
        "dynamic-fast-rewarm",
        test_invocation_with_path("/hot", "dynamic-fast-rewarm-warm-1"),
        "dynamic fast rewarm first invoke",
    )
    .await;
    invoke_with_timeout_and_dump(
        &service,
        "dynamic-fast-rewarm",
        test_invocation_with_path("/hot", "dynamic-fast-rewarm-warm-2"),
        "dynamic fast rewarm second invoke",
    )
    .await;

    let child_worker = service
        .dynamic_debug_dump()
        .await
        .handles
        .into_iter()
        .find(|handle| handle.owner_worker == "dynamic-fast-rewarm" && handle.binding == "SANDBOX")
        .map(|handle| handle.worker_name)
        .expect("dynamic child worker should exist");
    let child_dump = service
        .debug_dump(child_worker.clone())
        .await
        .expect("child debug dump should exist");
    let child_isolate_id = child_dump
        .isolates
        .first()
        .map(|isolate| isolate.id)
        .expect("child isolate should exist");
    assert!(
        service
            .force_fail_isolate_for_test(child_worker, child_dump.generation, child_isolate_id)
            .await
    );

    invoke_with_timeout_and_dump(
        &service,
        "dynamic-fast-rewarm",
        test_invocation_with_path("/__dynamic_metrics_reset", "dynamic-fast-rewarm-reset"),
        "dynamic fast rewarm metrics reset",
    )
    .await;

    invoke_with_timeout_and_dump(
        &service,
        "dynamic-fast-rewarm",
        test_invocation_with_path("/hot", "dynamic-fast-rewarm-after-fail-1"),
        "dynamic fast rewarm invoke after fail",
    )
    .await;
    invoke_with_timeout_and_dump(
        &service,
        "dynamic-fast-rewarm",
        test_invocation_with_path("/hot", "dynamic-fast-rewarm-after-fail-2"),
        "dynamic fast rewarm second invoke after fail",
    )
    .await;

    let metrics_output = invoke_with_timeout_and_dump(
        &service,
        "dynamic-fast-rewarm",
        test_invocation_with_path("/__dynamic_metrics", "dynamic-fast-rewarm-metrics"),
        "dynamic fast rewarm metrics",
    )
    .await;
    let metrics: DynamicFastFetchMetrics = crate::json::from_string(
        String::from_utf8(metrics_output.body).expect("metrics body should be utf8"),
    )
    .expect("metrics should parse");
    assert!(
        metrics.fallback_dispatch >= 1,
        "{:?}",
        metrics.fallback_dispatch
    );
    assert!(
        metrics.direct_fetch_fast_path_fallback >= 1,
        "{:?}",
        metrics.direct_fetch_fast_path_fallback
    );
    assert!(metrics.remote_fetch_hit >= 2);
    assert!(
        metrics.warm_isolate_hit >= 1,
        "{:?}",
        metrics.warm_isolate_hit
    );
}

#[tokio::test]
#[serial]
async fn dynamic_namespace_plain_child_fetch_completes_in_autoscaling_config() {
    let service = test_service(dynamic_autoscaling_config()).await;

    service
        .deploy_with_config(
            "dynamic-plain-auto".to_string(),
            dynamic_plain_namespace_worker(),
            DeployConfig {
                bindings: vec![DeployBinding::Dynamic {
                    binding: "SANDBOX".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");

    let one = invoke_with_timeout_and_dump(
        &service,
        "dynamic-plain-auto",
        test_invocation(),
        "dynamic plain autoscaling first invoke",
    )
    .await;
    let two = invoke_with_timeout_and_dump(
        &service,
        "dynamic-plain-auto",
        test_invocation(),
        "dynamic plain autoscaling second invoke",
    )
    .await;

    assert_eq!(String::from_utf8(one.body).expect("utf8"), "1");
    assert_eq!(String::from_utf8(two.body).expect("utf8"), "2");
}

#[tokio::test]
#[serial]
async fn dynamic_namespace_ops_complete_without_child_fetch_in_single_isolate() {
    let service = test_service(dynamic_single_isolate_config()).await;

    service
        .deploy_with_config(
            "dynamic-ops-single".to_string(),
            dynamic_namespace_ops_worker(),
            DeployConfig {
                bindings: vec![DeployBinding::Dynamic {
                    binding: "SANDBOX".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");

    let empty = invoke_with_timeout_and_dump(
        &service,
        "dynamic-ops-single",
        test_invocation_with_path("/list", "dyn-ops-list-empty"),
        "lookup-reply empty list",
    )
    .await;
    assert_eq!(String::from_utf8(empty.body).expect("utf8"), "[]");

    let create = invoke_with_timeout_and_dump(
        &service,
        "dynamic-ops-single",
        test_invocation_with_path("/get-create", "dyn-ops-get-create"),
        "create-reply first get",
    )
    .await;
    assert_eq!(String::from_utf8(create.body).expect("utf8"), "1");

    let hit = invoke_with_timeout_and_dump(
        &service,
        "dynamic-ops-single",
        test_invocation_with_path("/get-hit", "dyn-ops-get-hit"),
        "lookup-reply cached get",
    )
    .await;
    assert_eq!(String::from_utf8(hit.body).expect("utf8"), "0");

    let ids = invoke_with_timeout_and_dump(
        &service,
        "dynamic-ops-single",
        test_invocation_with_path("/list", "dyn-ops-list-populated"),
        "list-reply populated",
    )
    .await;
    assert_eq!(
        String::from_utf8(ids.body).expect("utf8"),
        "[\"control:v1\"]"
    );

    let deleted = invoke_with_timeout_and_dump(
        &service,
        "dynamic-ops-single",
        test_invocation_with_path("/delete", "dyn-ops-delete"),
        "delete-reply control worker",
    )
    .await;
    assert_eq!(String::from_utf8(deleted.body).expect("utf8"), "true");

    let ids_after = invoke_with_timeout_and_dump(
        &service,
        "dynamic-ops-single",
        test_invocation_with_path("/list", "dyn-ops-list-final"),
        "list-reply after delete",
    )
    .await;
    assert_eq!(String::from_utf8(ids_after.body).expect("utf8"), "[]");
}

#[tokio::test]
#[serial]
async fn dynamic_namespace_supports_list_delete_and_timeout() {
    let service = test_service(dynamic_autoscaling_config()).await;

    service
        .deploy_with_config(
            "dynamic-admin".to_string(),
            dynamic_namespace_admin_worker(),
            DeployConfig {
                bindings: vec![DeployBinding::Dynamic {
                    binding: "SANDBOX".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");

    let ensure = invoke_with_timeout_and_dump(
        &service,
        "dynamic-admin",
        test_invocation_with_path("/ensure", "dyn-admin-ensure"),
        "dynamic admin ensure",
    )
    .await;
    assert_eq!(ensure.status, 200);

    let ids = invoke_with_timeout_and_dump(
        &service,
        "dynamic-admin",
        test_invocation_with_path("/ids", "dyn-admin-ids-1"),
        "dynamic admin ids before delete",
    )
    .await;
    assert_eq!(String::from_utf8(ids.body).expect("utf8"), "[\"admin:v1\"]");

    let deleted = invoke_with_timeout_and_dump(
        &service,
        "dynamic-admin",
        test_invocation_with_path("/delete", "dyn-admin-delete"),
        "dynamic admin delete",
    )
    .await;
    assert_eq!(String::from_utf8(deleted.body).expect("utf8"), "true");

    let ids_after = invoke_with_timeout_and_dump(
        &service,
        "dynamic-admin",
        test_invocation_with_path("/ids", "dyn-admin-ids-2"),
        "dynamic admin ids after delete",
    )
    .await;
    assert_eq!(String::from_utf8(ids_after.body).expect("utf8"), "[]");

    let timeout_out = invoke_with_timeout_and_dump(
        &service,
        "dynamic-admin",
        test_invocation_with_path("/timeout", "dyn-admin-timeout"),
        "dynamic admin timeout route",
    )
    .await;
    assert_eq!(timeout_out.status, 504);
}

#[tokio::test]
#[serial]
async fn dynamic_namespace_repeated_list_delete_complete_past_threshold() {
    let service = test_service(dynamic_autoscaling_config()).await;

    service
        .deploy_with_config(
            "dynamic-admin-loop".to_string(),
            dynamic_namespace_admin_worker(),
            DeployConfig {
                bindings: vec![DeployBinding::Dynamic {
                    binding: "SANDBOX".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");

    for idx in 0..64 {
        let ensure = invoke_with_timeout_and_dump(
            &service,
            "dynamic-admin-loop",
            test_invocation_with_path("/ensure", &format!("dyn-admin-loop-ensure-{idx}")),
            &format!("dynamic admin loop ensure {idx}"),
        )
        .await;
        assert_eq!(ensure.status, 200);

        let ids = invoke_with_timeout_and_dump(
            &service,
            "dynamic-admin-loop",
            test_invocation_with_path("/ids", &format!("dyn-admin-loop-ids-{idx}")),
            &format!("dynamic admin loop ids {idx}"),
        )
        .await;
        assert_eq!(String::from_utf8(ids.body).expect("utf8"), "[\"admin:v1\"]");

        let deleted = invoke_with_timeout_and_dump(
            &service,
            "dynamic-admin-loop",
            test_invocation_with_path("/delete", &format!("dyn-admin-loop-delete-{idx}")),
            &format!("dynamic admin loop delete {idx}"),
        )
        .await;
        assert_eq!(String::from_utf8(deleted.body).expect("utf8"), "true");
    }

    let ids_after = invoke_with_timeout_and_dump(
        &service,
        "dynamic-admin-loop",
        test_invocation_with_path("/ids", "dyn-admin-loop-final-ids"),
        "dynamic admin loop final ids",
    )
    .await;
    assert_eq!(String::from_utf8(ids_after.body).expect("utf8"), "[]");
}

#[tokio::test]
#[serial]
async fn dynamic_test_async_reply_immediate_completion_resumes_request() {
    let service = test_service(dynamic_single_isolate_config()).await;

    service
        .deploy(
            "dynamic-wake-probe".to_string(),
            dynamic_wake_probe_worker(),
        )
        .await
        .expect("deploy should succeed");

    let output = invoke_with_timeout_and_dump(
        &service,
        "dynamic-wake-probe",
        test_invocation_with_path("/async/immediate", "dyn-wake-immediate"),
        "dynamic test async immediate",
    )
    .await;
    assert_eq!(output.status, 200);
    assert_eq!(String::from_utf8(output.body).expect("utf8"), "immediate");
}

#[tokio::test]
#[serial]
async fn dynamic_test_async_reply_delayed_completion_resumes_after_yield() {
    let service = test_service(dynamic_single_isolate_config()).await;

    service
        .deploy(
            "dynamic-wake-probe".to_string(),
            dynamic_wake_probe_worker(),
        )
        .await
        .expect("deploy should succeed");

    let output = invoke_with_timeout_and_dump(
        &service,
        "dynamic-wake-probe",
        test_invocation_with_path("/async/delayed", "dyn-wake-delayed"),
        "dynamic test async delayed",
    )
    .await;
    assert_eq!(output.status, 200);
    assert_eq!(String::from_utf8(output.body).expect("utf8"), "delayed");
}

#[tokio::test]
#[serial]
async fn dynamic_test_async_reply_timeout_surfaces_explicit_error() {
    let service = test_service(dynamic_single_isolate_config()).await;

    service
        .deploy(
            "dynamic-wake-probe".to_string(),
            dynamic_wake_probe_worker(),
        )
        .await
        .expect("deploy should succeed");

    let output = invoke_with_timeout_and_dump(
        &service,
        "dynamic-wake-probe",
        test_invocation_with_path("/async/timeout", "dyn-wake-timeout"),
        "dynamic test async timeout",
    )
    .await;
    assert_eq!(output.status, 504);
    assert_eq!(
        String::from_utf8(output.body).expect("utf8"),
        "test async reply timed out after 25ms"
    );
}

#[tokio::test]
#[serial]
async fn dynamic_test_nested_targeted_invoke_completes_on_same_isolate() {
    let service = test_service(dynamic_single_isolate_config()).await;

    service
        .deploy(
            "dynamic-wake-probe".to_string(),
            dynamic_wake_probe_worker(),
        )
        .await
        .expect("deploy should succeed");

    let warm = invoke_with_timeout_and_dump(
        &service,
        "dynamic-wake-probe",
        test_invocation_with_path("/warm-provider", "dyn-nested-same-warm"),
        "dynamic test nested same warm",
    )
    .await;
    assert_eq!(warm.status, 200);

    let output = invoke_with_timeout_and_dump(
        &service,
        "dynamic-wake-probe",
        test_invocation_with_path("/nested/same", "dyn-nested-same"),
        "dynamic test nested same",
    )
    .await;
    let body = String::from_utf8(output.body).expect("utf8");
    assert_eq!(output.status, 200, "{body}");
    assert!(body.starts_with("ok:"), "{body}");
}

#[tokio::test]
#[serial]
async fn dynamic_test_nested_targeted_invoke_completes_across_isolates() {
    let service = test_service(RuntimeConfig {
        min_isolates: 2,
        max_isolates: 2,
        max_inflight_per_isolate: 1,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy(
            "dynamic-wake-probe".to_string(),
            dynamic_wake_probe_worker(),
        )
        .await
        .expect("deploy should succeed");

    let mut warm_tasks = Vec::new();
    for idx in 0..4 {
        let service = service.clone();
        warm_tasks.push(tokio::spawn(async move {
            invoke_with_timeout_and_dump(
                &service,
                "dynamic-wake-probe",
                test_invocation_with_path(
                    "/warm-provider?hold=50",
                    &format!("dyn-nested-other-warm-{idx}"),
                ),
                &format!("dynamic test nested other warm {idx}"),
            )
            .await
        }));
    }
    for task in warm_tasks {
        let output = task.await.expect("join");
        assert_eq!(output.status, 200);
    }
    wait_for_isolate_total(&service, "dynamic-wake-probe", 2).await;

    let output = invoke_with_timeout_and_dump(
        &service,
        "dynamic-wake-probe",
        test_invocation_with_path("/nested/other", "dyn-nested-other"),
        "dynamic test nested other",
    )
    .await;
    let body = String::from_utf8(output.body).expect("utf8");
    assert_eq!(output.status, 200, "{body}");
    assert!(body.starts_with("ok:"));
    assert_ne!(body, "ok:1");
}

#[tokio::test]
#[serial]
async fn dynamic_handle_cache_invalidates_on_delete_before_recreate() {
    let service = test_service(dynamic_single_isolate_config()).await;

    service
        .deploy_with_config(
            "dynamic-cache-delete".to_string(),
            dynamic_handle_cache_delete_worker(),
            DeployConfig {
                bindings: vec![DeployBinding::Dynamic {
                    binding: "SANDBOX".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");

    let output = invoke_with_timeout_and_dump(
        &service,
        "dynamic-cache-delete",
        test_invocation_with_path("/delete-recreate", "dynamic-cache-delete-recreate"),
        "dynamic delete should invalidate handle cache",
    )
    .await;

    assert_eq!(output.status, 200);
    assert_eq!(String::from_utf8(output.body).expect("utf8"), "one:two");
}

#[tokio::test]
#[serial]
async fn dynamic_handle_cache_clears_on_owner_generation_change() {
    let service = test_service(dynamic_single_isolate_config()).await;

    service
        .deploy_with_config(
            "dynamic-generation-cache".to_string(),
            dynamic_plain_namespace_worker_with_source(
                "export default { async fetch() { return new Response('one'); } };",
            ),
            DeployConfig {
                bindings: vec![DeployBinding::Dynamic {
                    binding: "SANDBOX".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("first deploy should succeed");

    let first = invoke_with_timeout_and_dump(
        &service,
        "dynamic-generation-cache",
        test_invocation_with_path("/", "dynamic-generation-cache-one"),
        "dynamic generation cache first invoke",
    )
    .await;
    assert_eq!(String::from_utf8(first.body).expect("utf8"), "one");

    service
        .deploy_with_config(
            "dynamic-generation-cache".to_string(),
            dynamic_plain_namespace_worker_with_source(
                "export default { async fetch() { return new Response('two'); } };",
            ),
            DeployConfig {
                bindings: vec![DeployBinding::Dynamic {
                    binding: "SANDBOX".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("second deploy should succeed");

    let second = invoke_with_timeout_and_dump(
        &service,
        "dynamic-generation-cache",
        test_invocation_with_path("/", "dynamic-generation-cache-two"),
        "dynamic generation cache second invoke",
    )
    .await;
    assert_eq!(String::from_utf8(second.body).expect("utf8"), "two");
}

#[tokio::test]
#[serial]
async fn dynamic_snapshot_cache_reuses_snapshot_for_same_source() {
    let service = test_service(dynamic_single_isolate_config()).await;

    service
        .deploy_with_config(
            "dynamic-snapshot-cache".to_string(),
            dynamic_snapshot_cache_worker(),
            DeployConfig {
                bindings: vec![DeployBinding::Dynamic {
                    binding: "SANDBOX".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");

    let first = invoke_with_timeout_and_dump(
        &service,
        "dynamic-snapshot-cache",
        test_invocation_with_path("/?id=one", "dynamic-snapshot-cache-one"),
        "dynamic snapshot cache first create",
    )
    .await;
    assert_eq!(first.status, 200);

    let after_first = service.dynamic_debug_dump().await;
    assert_eq!(after_first.snapshot_cache_entries, 1);
    assert_eq!(after_first.snapshot_cache_failures, 0);

    let second = invoke_with_timeout_and_dump(
        &service,
        "dynamic-snapshot-cache",
        test_invocation_with_path("/?id=two", "dynamic-snapshot-cache-two"),
        "dynamic snapshot cache second create",
    )
    .await;
    assert_eq!(second.status, 200);

    let after_second = service.dynamic_debug_dump().await;
    assert_eq!(after_second.snapshot_cache_entries, 1);
    assert_eq!(after_second.snapshot_cache_failures, 0);
}

#[tokio::test]
#[serial]
async fn dynamic_namespace_child_can_return_json_response() {
    let service = test_service(dynamic_autoscaling_config()).await;

    service
            .deploy_with_config(
                "dynamic-parent-json".to_string(),
                r#"
let child = null;

export default {
  async fetch(_request, env) {
    if (!child) {
      child = await env.SANDBOX.get("json-child", async () => ({
        entrypoint: "worker.js",
        modules: {
          "worker.js": "export default { async fetch() { return Response.json({ ok: true }, { status: 201 }); } };",
        },
        timeout: 1500,
      }));
    }
    return child.fetch("http://worker/");
  },
};
"#
                .to_string(),
                DeployConfig {
                    bindings: vec![DeployBinding::Dynamic {
                        binding: "SANDBOX".to_string(),
                    }],
                    ..DeployConfig::default()
                },
            )
            .await
            .expect("deploy should succeed");

    let output = invoke_with_timeout_and_dump(
        &service,
        "dynamic-parent-json",
        test_invocation(),
        "dynamic json child invoke",
    )
    .await;

    assert_eq!(output.status, 201);
    assert_eq!(
        String::from_utf8(output.body).expect("utf8"),
        r#"{"ok":true}"#
    );
    assert!(output
        .headers
        .iter()
        .any(|(name, value)| name.eq_ignore_ascii_case("content-type")
            && value == "application/json"));
}

#[tokio::test]
#[serial]
async fn dynamic_namespace_host_rpc_works_with_single_inflight_parent_isolate() {
    let service = test_service(dynamic_single_isolate_config()).await;

    service
        .deploy_with_config(
            "dynamic-parent".to_string(),
            dynamic_namespace_worker(),
            DeployConfig {
                bindings: vec![DeployBinding::Dynamic {
                    binding: "SANDBOX".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");

    let output = invoke_with_timeout_and_dump(
        &service,
        "dynamic-parent",
        test_invocation(),
        "dynamic host rpc single-isolate invoke",
    )
    .await;

    assert_eq!(String::from_utf8(output.body).expect("utf8"), "1:1");
}

#[tokio::test]
#[serial]
async fn dynamic_namespace_host_rpc_completes_under_bench_like_autoscaling_load() {
    let service = test_service(dynamic_bench_autoscaling_config()).await;

    service
        .deploy_with_config(
            "dynamic-parent-autoscaling-host-rpc".to_string(),
            dynamic_namespace_worker(),
            DeployConfig {
                bindings: vec![DeployBinding::Dynamic {
                    binding: "SANDBOX".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");

    let warm = invoke_with_timeout_and_dump(
        &service,
        "dynamic-parent-autoscaling-host-rpc",
        test_invocation_with_path("/", "dynamic-host-rpc-autoscaling-warm"),
        "dynamic host rpc autoscaling warm",
    )
    .await;
    assert_eq!(warm.status, 200);

    let mut tasks = Vec::new();
    for idx in 0..32 {
        let service = service.clone();
        tasks.push(tokio::spawn(async move {
            invoke_with_timeout_and_dump(
                &service,
                "dynamic-parent-autoscaling-host-rpc",
                test_invocation_with_path("/", &format!("dynamic-host-rpc-autoscaling-{idx}")),
                &format!("dynamic host rpc autoscaling request {idx}"),
            )
            .await
        }));
    }

    let mut bodies = Vec::new();
    for task in tasks {
        let output = task.await.expect("task should join");
        assert_eq!(output.status, 200);
        bodies.push(String::from_utf8(output.body).expect("utf8"));
    }

    assert_eq!(bodies.len(), 32);
    assert!(bodies.iter().all(|body| body.contains(':')));
}

#[tokio::test]
#[serial]
async fn dynamic_namespace_host_rpc_reports_provider_isolate_loss_promptly() {
    let service = test_service(dynamic_single_isolate_config()).await;

    service
        .deploy_with_config(
            "dynamic-parent-stale-provider".to_string(),
            dynamic_namespace_worker(),
            DeployConfig {
                bindings: vec![DeployBinding::Dynamic {
                    binding: "SANDBOX".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");

    let warm = invoke_with_timeout_and_dump(
        &service,
        "dynamic-parent-stale-provider",
        test_invocation_with_path("/", "dynamic-host-rpc-stale-provider-warm"),
        "dynamic host rpc stale provider warm",
    )
    .await;
    assert_eq!(String::from_utf8(warm.body).expect("utf8"), "1:1");

    let dump = service
        .debug_dump("dynamic-parent-stale-provider".to_string())
        .await
        .expect("debug dump should exist");
    let isolate_id = dump
        .isolates
        .first()
        .map(|isolate| isolate.id)
        .expect("provider isolate should exist");
    assert!(
        service
            .force_fail_isolate_for_test(
                "dynamic-parent-stale-provider".to_string(),
                dump.generation,
                isolate_id,
            )
            .await
    );

    let error = timeout(
        Duration::from_secs(5),
        service.invoke(
            "dynamic-parent-stale-provider".to_string(),
            test_invocation_with_path("/", "dynamic-host-rpc-stale-provider"),
        ),
    )
    .await
    .expect("stale provider invoke should not hang")
    .expect_err("invoke should fail once provider isolate is gone");
    let error_text = error.to_string();
    assert!(
        error_text.contains("dynamic host rpc provider isolate is unavailable"),
        "unexpected error: {error_text}"
    );
}

#[tokio::test]
#[serial]
async fn dynamic_namespace_repeated_create_and_invoke_complete_past_threshold() {
    let service = test_service(dynamic_autoscaling_config()).await;

    service
        .deploy_with_config(
            "dynamic-repeat".to_string(),
            dynamic_repeated_create_worker(),
            DeployConfig {
                bindings: vec![DeployBinding::Dynamic {
                    binding: "SANDBOX".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");

    for idx in 0..64 {
        let output = invoke_with_timeout_and_dump(
            &service,
            "dynamic-repeat",
            test_invocation_with_path(&format!("/?id={idx}"), &format!("dynamic-repeat-{idx}")),
            &format!("dynamic repeat create+invoke {idx}"),
        )
        .await;
        assert_eq!(output.status, 200);
        assert_eq!(
            String::from_utf8(output.body).expect("utf8"),
            idx.to_string()
        );
    }
}

#[tokio::test]
#[serial]
async fn dynamic_namespace_child_can_use_response_json() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 2,
        max_inflight_per_isolate: 4,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy_with_config(
            "dynamic-json".to_string(),
            dynamic_response_json_worker(),
            DeployConfig {
                public: false,
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Dynamic {
                    binding: "SANDBOX".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    let output = service
        .invoke("dynamic-json".to_string(), test_invocation())
        .await
        .expect("invoke should succeed");
    assert_eq!(output.status, 200);
    assert_eq!(
        String::from_utf8(output.body).expect("utf8"),
        r#"{"ok":true,"source":"dynamic-child"}"#
    );
}

#[tokio::test]
#[serial]
async fn normal_and_dynamic_workers_expose_same_runtime_surface() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 2,
        max_inflight_per_isolate: 4,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy_with_config(
            "dynamic-surface".to_string(),
            dynamic_runtime_surface_worker(),
            DeployConfig {
                public: false,
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Dynamic {
                    binding: "SANDBOX".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    let output = service
        .invoke("dynamic-surface".to_string(), test_invocation())
        .await
        .expect("invoke should succeed");
    assert_eq!(output.status, 200);
    let body = String::from_utf8(output.body).expect("utf8");
    assert!(body.contains(r#""same":true"#), "body was {body}");
    assert!(body.contains(r#""fetch":true"#), "body was {body}");
    assert!(body.contains(r#""request":true"#), "body was {body}");
    assert!(body.contains(r#""response":true"#), "body was {body}");
    assert!(body.contains(r#""headers":true"#), "body was {body}");
    assert!(body.contains(r#""formData":true"#), "body was {body}");
    assert!(
        body.contains(r#""responseJsonType":"application/json""#),
        "body was {body}"
    );
}
