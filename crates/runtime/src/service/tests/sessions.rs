use super::*;

#[tokio::test]
#[serial]
async fn transport_open_works_with_deno_request_compatibility() {
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
            "transport-runtime".to_string(),
            transport_echo_worker(),
            DeployConfig {
                public: false,
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "MEDIA".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    let (stream_tx, mut stream_rx) = mpsc::unbounded_channel();
    let (datagram_tx, _datagram_rx) = mpsc::unbounded_channel();
    let opened = service
        .open_transport(
            "transport-runtime".to_string(),
            test_transport_invocation(),
            stream_tx,
            datagram_tx,
        )
        .await
        .expect("transport open should succeed");

    assert_eq!(opened.output.status, 200);

    service
        .transport_push_stream(
            "transport-runtime".to_string(),
            opened.session_id.clone(),
            b"hello-transport".to_vec(),
            false,
        )
        .await
        .expect("transport push should succeed");

    let echoed = timeout(Duration::from_secs(2), stream_rx.recv())
        .await
        .expect("stream echo should arrive")
        .expect("stream echo channel should stay open");
    assert_eq!(echoed, b"hello-transport");

    service
        .transport_close(
            "transport-runtime".to_string(),
            opened.session_id,
            0,
            "done".to_string(),
        )
        .await
        .expect("transport close should succeed");
}

#[tokio::test]
#[serial]
async fn transport_open_preserves_connect_shape_for_memory_namespace_code() {
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
            "transport-shape".to_string(),
            transport_shape_worker(),
            DeployConfig {
                public: false,
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "MEDIA".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    let (stream_tx, _stream_rx) = mpsc::unbounded_channel();
    let (datagram_tx, _datagram_rx) = mpsc::unbounded_channel();
    let opened = service
        .open_transport(
            "transport-shape".to_string(),
            test_transport_invocation(),
            stream_tx,
            datagram_tx,
        )
        .await
        .expect("transport open should succeed");

    assert_eq!(opened.output.status, 200);
}

#[tokio::test]
#[serial]
async fn transport_wake_can_list_transport_handles_without_deadlock() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 1,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy_with_config(
            "transport-values".to_string(),
            transport_values_worker(),
            DeployConfig {
                public: false,
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "MEDIA".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    let (stream_tx, mut stream_rx) = mpsc::unbounded_channel();
    let (datagram_tx, _datagram_rx) = mpsc::unbounded_channel();
    let opened = tokio::time::timeout(
        Duration::from_secs(5),
        service.open_transport(
            "transport-values".to_string(),
            test_transport_invocation(),
            stream_tx,
            datagram_tx,
        ),
    )
    .await
    .expect("transport open should not hang")
    .expect("transport open should succeed");
    assert_eq!(opened.output.status, 200);

    tokio::time::timeout(
        Duration::from_secs(5),
        service.transport_push_stream(
            "transport-values".to_string(),
            opened.session_id.clone(),
            b"ping".to_vec(),
            false,
        ),
    )
    .await
    .expect("transport push should not hang")
    .expect("transport push should succeed");

    let echoed = tokio::time::timeout(Duration::from_secs(5), stream_rx.recv())
        .await
        .expect("transport reply should arrive")
        .expect("transport reply channel should stay open");
    assert_eq!(echoed, b"ready:1");

    service
        .transport_close(
            "transport-values".to_string(),
            opened.session_id,
            0,
            "done".to_string(),
        )
        .await
        .expect("transport close should succeed");
}

#[tokio::test]
#[serial]
async fn transport_session_survives_idle_ttl_and_scales_down_after_close() {
    let service = test_service(RuntimeConfig {
        min_isolates: 0,
        max_isolates: 1,
        max_inflight_per_isolate: 1,
        idle_ttl: Duration::from_millis(200),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy_with_config(
            "transport-idle".to_string(),
            transport_echo_worker(),
            DeployConfig {
                public: false,
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "MEDIA".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    let (stream_tx, mut stream_rx) = mpsc::unbounded_channel();
    let (datagram_tx, _datagram_rx) = mpsc::unbounded_channel();
    let opened = service
        .open_transport(
            "transport-idle".to_string(),
            test_transport_invocation(),
            stream_tx,
            datagram_tx,
        )
        .await
        .expect("transport open should succeed");

    sleep(Duration::from_millis(500)).await;
    let stats = service
        .stats("transport-idle".to_string())
        .await
        .expect("worker stats should exist");
    assert_eq!(stats.isolates_total, 1);

    service
        .transport_push_stream(
            "transport-idle".to_string(),
            opened.session_id.clone(),
            b"idle-transport".to_vec(),
            false,
        )
        .await
        .expect("transport push should succeed");
    let echoed = timeout(Duration::from_secs(2), stream_rx.recv())
        .await
        .expect("transport echo should arrive")
        .expect("transport stream should stay open");
    assert_eq!(echoed, b"idle-transport");

    service
        .transport_close(
            "transport-idle".to_string(),
            opened.session_id,
            0,
            "done".to_string(),
        )
        .await
        .expect("transport close should succeed");
}

#[tokio::test]
#[serial]
async fn transport_session_reaped_when_owner_isolate_fails() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 1,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy_with_config(
            "transport-reap".to_string(),
            transport_echo_worker(),
            DeployConfig {
                public: false,
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "MEDIA".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    let (stream_tx, _stream_rx) = mpsc::unbounded_channel();
    let (datagram_tx, _datagram_rx) = mpsc::unbounded_channel();
    let opened = service
        .open_transport(
            "transport-reap".to_string(),
            test_transport_invocation(),
            stream_tx,
            datagram_tx,
        )
        .await
        .expect("transport open should succeed");

    let dump = service
        .debug_dump("transport-reap".to_string())
        .await
        .expect("debug dump should exist");
    let isolate_id = dump
        .isolates
        .first()
        .map(|isolate| isolate.id)
        .expect("transport isolate should exist");
    assert!(
        service
            .force_fail_isolate_for_test("transport-reap".to_string(), dump.generation, isolate_id,)
            .await
    );

    let error = service
        .transport_push_stream(
            "transport-reap".to_string(),
            opened.session_id,
            b"after-fail".to_vec(),
            false,
        )
        .await
        .expect_err("reaped transport session should fail promptly");
    assert!(
        error.to_string().contains("transport session not found"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
#[serial]
async fn websocket_message_handler_can_use_memory_storage_after_handshake() {
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
            "ws-storage".to_string(),
            websocket_storage_worker(),
            DeployConfig {
                public: false,
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "CHAT".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    let opened = service
        .open_websocket(
            "ws-storage".to_string(),
            test_websocket_invocation("/ws", "ws-open"),
            None,
        )
        .await
        .expect("websocket open should succeed");

    assert_eq!(opened.output.status, 101);

    let echoed = service
        .websocket_send_frame(
            "ws-storage".to_string(),
            opened.session_id.clone(),
            b"ready".to_vec(),
            false,
        )
        .await
        .expect("websocket message should succeed");
    assert_eq!(echoed.status, 204);
    assert_eq!(
        String::from_utf8(echoed.body).expect("utf8"),
        r#"{"seen":"ready","count":1}"#
    );

    let state = service
        .invoke(
            "ws-storage".to_string(),
            test_invocation_with_path("/state", "ws-state"),
        )
        .await
        .expect("state invoke should succeed");
    assert_eq!(state.status, 200);
    let state_json: serde_json::Value =
        serde_json::from_slice(&state.body).expect("state body should be json");
    assert_eq!(state_json["count"], 1);
    assert_eq!(state_json["last"], "ready");

    service
        .websocket_close(
            "ws-storage".to_string(),
            opened.session_id,
            1000,
            "done".to_string(),
        )
        .await
        .expect("websocket close should succeed");
}

#[tokio::test]
#[serial]
async fn websocket_wake_can_list_socket_handles_without_deadlock() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 1,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy_with_config(
            "ws-values".to_string(),
            websocket_values_worker(),
            DeployConfig {
                public: false,
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "CHAT".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    let opened = tokio::time::timeout(
        Duration::from_secs(5),
        service.open_websocket(
            "ws-values".to_string(),
            test_websocket_invocation("/ws", "ws-values-open"),
            None,
        ),
    )
    .await
    .expect("websocket open should not hang")
    .expect("websocket open should succeed");
    assert_eq!(opened.output.status, 101);

    let echoed = tokio::time::timeout(
        Duration::from_secs(5),
        service.websocket_send_frame(
            "ws-values".to_string(),
            opened.session_id.clone(),
            b"ping".to_vec(),
            false,
        ),
    )
    .await
    .expect("websocket message should not hang")
    .expect("websocket message should succeed");
    assert_eq!(echoed.status, 204);
    let echoed_json: serde_json::Value =
        serde_json::from_slice(&echoed.body).expect("wake payload should be json");
    assert_eq!(echoed_json["count"], 1);

    let handles = service
        .invoke(
            "ws-values".to_string(),
            test_invocation_with_path("/handles", "ws-values-handles"),
        )
        .await
        .expect("handles invoke should succeed");
    assert_eq!(handles.status, 200);
    let handles_json: serde_json::Value =
        serde_json::from_slice(&handles.body).expect("handles body should be json");
    assert_eq!(handles_json["count"], 1);

    let txn_handles = service
        .invoke(
            "ws-values".to_string(),
            test_invocation_with_path("/txn-handles", "ws-values-txn-handles"),
        )
        .await
        .expect("txn handles invoke should succeed");
    assert_eq!(txn_handles.status, 200);
    let txn_handles_json: serde_json::Value =
        serde_json::from_slice(&txn_handles.body).expect("txn handles body should be json");
    assert_eq!(txn_handles_json["first_is_array"], true);
    assert_eq!(txn_handles_json["second_is_array"], true);
    assert_eq!(txn_handles_json["first_count"], 1);
    assert_eq!(txn_handles_json["second_count"], 1);

    service
        .websocket_close(
            "ws-values".to_string(),
            opened.session_id,
            1000,
            "done".to_string(),
        )
        .await
        .expect("websocket close should succeed");
}

#[tokio::test]
#[serial]
async fn websocket_session_survives_idle_ttl_and_scales_down_after_close() {
    let service = test_service(RuntimeConfig {
        min_isolates: 0,
        max_isolates: 1,
        max_inflight_per_isolate: 1,
        idle_ttl: Duration::from_millis(200),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy_with_config(
            "ws-idle".to_string(),
            websocket_storage_worker(),
            DeployConfig {
                public: false,
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "CHAT".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    let opened = service
        .open_websocket(
            "ws-idle".to_string(),
            test_websocket_invocation("/ws", "ws-idle-open"),
            None,
        )
        .await
        .expect("websocket open should succeed");

    sleep(Duration::from_millis(500)).await;
    let stats = service
        .stats("ws-idle".to_string())
        .await
        .expect("worker stats should exist");
    assert_eq!(stats.isolates_total, 1);

    let echoed = service
        .websocket_send_frame(
            "ws-idle".to_string(),
            opened.session_id.clone(),
            b"idle".to_vec(),
            false,
        )
        .await
        .expect("websocket message should succeed");
    assert_eq!(echoed.status, 204);
    assert_eq!(
        String::from_utf8(echoed.body).expect("utf8"),
        r#"{"seen":"idle","count":1}"#
    );

    service
        .websocket_close(
            "ws-idle".to_string(),
            opened.session_id,
            1000,
            "done".to_string(),
        )
        .await
        .expect("websocket close should succeed");

    wait_for_isolate_total(&service, "ws-idle", 0).await;
}

#[tokio::test]
#[serial]
async fn websocket_session_reaped_when_owner_isolate_fails() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 1,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy_with_config(
            "ws-reap".to_string(),
            websocket_storage_worker(),
            DeployConfig {
                public: false,
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "CHAT".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    let opened = service
        .open_websocket(
            "ws-reap".to_string(),
            test_websocket_invocation("/ws", "ws-reap-open"),
            None,
        )
        .await
        .expect("websocket open should succeed");

    let dump = service
        .debug_dump("ws-reap".to_string())
        .await
        .expect("debug dump should exist");
    let isolate_id = dump
        .isolates
        .first()
        .map(|isolate| isolate.id)
        .expect("websocket isolate should exist");
    assert!(
        service
            .force_fail_isolate_for_test("ws-reap".to_string(), dump.generation, isolate_id)
            .await
    );

    let error = service
        .websocket_send_frame(
            "ws-reap".to_string(),
            opened.session_id,
            b"after-fail".to_vec(),
            false,
        )
        .await
        .expect_err("reaped websocket session should fail promptly");
    assert!(
        error.to_string().contains("websocket session not found"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
#[serial]
async fn websocket_session_survives_redeploy_while_old_generation_stays_live() {
    let service = test_service(RuntimeConfig {
        min_isolates: 0,
        max_isolates: 1,
        max_inflight_per_isolate: 1,
        idle_ttl: Duration::from_millis(200),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    let deploy_config = DeployConfig {
        public: false,
        internal: DeployInternalConfig { trace: None },
        bindings: vec![DeployBinding::Memory {
            binding: "CHAT".to_string(),
        }],
    };

    service
        .deploy_with_config(
            "ws-redeploy".to_string(),
            websocket_storage_worker(),
            deploy_config.clone(),
        )
        .await
        .expect("initial deploy should succeed");

    let opened = service
        .open_websocket(
            "ws-redeploy".to_string(),
            test_websocket_invocation("/ws", "ws-redeploy-open"),
            None,
        )
        .await
        .expect("websocket open should succeed");

    service
        .deploy_with_config(
            "ws-redeploy".to_string(),
            websocket_storage_worker(),
            deploy_config,
        )
        .await
        .expect("redeploy should succeed");

    sleep(Duration::from_millis(500)).await;

    let echoed = service
        .websocket_send_frame(
            "ws-redeploy".to_string(),
            opened.session_id.clone(),
            b"after-redeploy".to_vec(),
            false,
        )
        .await
        .expect("old generation websocket should stay live");
    assert_eq!(echoed.status, 204);
    assert_eq!(
        String::from_utf8(echoed.body).expect("utf8"),
        r#"{"seen":"after-redeploy","count":1}"#
    );

    service
        .websocket_close(
            "ws-redeploy".to_string(),
            opened.session_id,
            1000,
            "done".to_string(),
        )
        .await
        .expect("websocket close should succeed");
}

#[tokio::test]
#[serial]
async fn websocket_stub_surface_uses_handle_backed_send_close_only() {
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
            "ws-surface".to_string(),
            websocket_socket_surface_worker(),
            DeployConfig {
                public: false,
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "CHAT".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    let output = service
        .invoke("ws-surface".to_string(), test_invocation())
        .await
        .expect("invoke should succeed");
    assert_eq!(output.status, 200);
    let surface: serde_json::Value =
        serde_json::from_slice(&output.body).expect("surface body should be json");
    assert_eq!(surface["stubSurface"]["values"], "function");
    assert_eq!(surface["stubSurface"]["send"], "undefined");
    assert_eq!(surface["stubSurface"]["close"], "undefined");
    assert_eq!(surface["stateSurface"]["accept"], "function");
    assert_eq!(surface["stateSurface"]["sockets"], "undefined");
}

#[tokio::test]
#[serial]
async fn websocket_message_handler_can_close_handle_backed_socket() {
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
            "ws-close".to_string(),
            websocket_storage_worker(),
            DeployConfig {
                public: false,
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "CHAT".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    let opened = service
        .open_websocket(
            "ws-close".to_string(),
            test_websocket_invocation("/ws", "ws-close-open"),
            None,
        )
        .await
        .expect("websocket open should succeed");
    assert_eq!(opened.output.status, 101);

    let closed = service
        .websocket_send_frame(
            "ws-close".to_string(),
            opened.session_id,
            b"close-me".to_vec(),
            false,
        )
        .await
        .expect("websocket close message should succeed");
    assert_eq!(closed.status, 204);
    let close_code = closed
        .headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("x-dd-ws-close-code"))
        .map(|(_, value)| value.as_str());
    let close_reason = closed
        .headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("x-dd-ws-close-reason"))
        .map(|(_, value)| value.as_str());
    assert_eq!(close_code, Some("1000"));
    assert_eq!(close_reason, Some("server-close"));
}

#[tokio::test]
#[serial]
async fn websocket_storage_uses_current_request_scope_on_warm_memory_instance() {
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
            "ws-storage".to_string(),
            websocket_storage_worker(),
            DeployConfig {
                public: false,
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "CHAT".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    let opened = service
        .open_websocket(
            "ws-storage".to_string(),
            test_websocket_invocation("/ws", "ws-open-warm"),
            None,
        )
        .await
        .expect("websocket open should succeed");

    let first = service
        .websocket_send_frame(
            "ws-storage".to_string(),
            opened.session_id.clone(),
            b"first".to_vec(),
            false,
        )
        .await
        .expect("first websocket message should succeed");
    assert_eq!(
        String::from_utf8(first.body).expect("utf8"),
        r#"{"seen":"first","count":1}"#
    );

    let second = service
        .websocket_send_frame(
            "ws-storage".to_string(),
            opened.session_id.clone(),
            b"second".to_vec(),
            false,
        )
        .await
        .expect("second websocket message should succeed");
    assert_eq!(
        String::from_utf8(second.body).expect("utf8"),
        r#"{"seen":"second","count":2}"#
    );

    service
        .websocket_close(
            "ws-storage".to_string(),
            opened.session_id,
            1000,
            "done".to_string(),
        )
        .await
        .expect("websocket close should succeed");
}

#[tokio::test]
#[serial]
async fn chat_worker_second_join_and_message_do_not_hang() {
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
            "chat".to_string(),
            include_str!("../../../../../examples/chat-worker/src/worker.js").to_string(),
            DeployConfig {
                public: false,
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "CHAT_ROOM".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    let alice = tokio::time::timeout(
        Duration::from_secs(5),
        service.open_websocket(
            "chat".to_string(),
            test_websocket_invocation(
                "/rooms/test/ws?username=alice&participant=alice",
                "chat-open-alice",
            ),
            None,
        ),
    )
    .await
    .expect("alice websocket open should not hang")
    .expect("alice websocket open should succeed");
    assert_eq!(alice.output.status, 101);

    let alice_ready = tokio::time::timeout(
        Duration::from_secs(5),
        service.websocket_send_frame(
            "chat".to_string(),
            alice.session_id.clone(),
            br#"{"type":"ready"}"#.to_vec(),
            false,
        ),
    )
    .await
    .expect("alice ready should not hang")
    .expect("alice ready should succeed");
    assert_eq!(alice_ready.status, 204);

    let bob = tokio::time::timeout(
        Duration::from_secs(5),
        service.open_websocket(
            "chat".to_string(),
            test_websocket_invocation(
                "/rooms/test/ws?username=bob&participant=bob",
                "chat-open-bob",
            ),
            None,
        ),
    )
    .await
    .expect("bob websocket open should not hang")
    .expect("bob websocket open should succeed");
    assert_eq!(bob.output.status, 101);

    let bob_ready = tokio::time::timeout(
        Duration::from_secs(5),
        service.websocket_send_frame(
            "chat".to_string(),
            bob.session_id.clone(),
            br#"{"type":"ready"}"#.to_vec(),
            false,
        ),
    )
    .await
    .expect("bob ready should not hang")
    .expect("bob ready should succeed");
    assert_eq!(bob_ready.status, 204);
    let bob_ready_body = String::from_utf8(bob_ready.body).expect("utf8");
    assert!(
        bob_ready_body.contains("alice"),
        "bob ready payload should include alice: {bob_ready_body}"
    );

    tokio::time::timeout(
        Duration::from_secs(5),
        service.websocket_wait_frame("chat".to_string(), alice.session_id.clone()),
    )
    .await
    .expect("alice participant update should not hang")
    .expect("alice participant update should succeed");
    let alice_participants = tokio::time::timeout(
        Duration::from_secs(5),
        service.websocket_drain_frame("chat".to_string(), alice.session_id.clone()),
    )
    .await
    .expect("alice participant drain should not hang")
    .expect("alice participant drain should succeed")
    .expect("alice should have a pending participant update");
    let alice_participants_body =
        String::from_utf8(alice_participants.body).expect("participant payload utf8");
    assert!(
        alice_participants_body.contains("bob"),
        "alice participant payload should include bob: {alice_participants_body}"
    );

    let alice_message = tokio::time::timeout(
        Duration::from_secs(5),
        service.websocket_send_frame(
            "chat".to_string(),
            alice.session_id.clone(),
            br#"{"type":"message","text":"hello"}"#.to_vec(),
            false,
        ),
    )
    .await
    .expect("alice message should not hang")
    .expect("alice message should succeed");
    assert_eq!(alice_message.status, 204);
    let alice_message_body = String::from_utf8(alice_message.body).expect("utf8");
    assert!(
        alice_message_body.contains("hello"),
        "alice message payload should include the sent message: {alice_message_body}"
    );

    tokio::time::timeout(
        Duration::from_secs(5),
        service.websocket_wait_frame("chat".to_string(), bob.session_id.clone()),
    )
    .await
    .expect("bob message update should not hang")
    .expect("bob message update should succeed");
    let bob_message = tokio::time::timeout(
        Duration::from_secs(5),
        service.websocket_drain_frame("chat".to_string(), bob.session_id.clone()),
    )
    .await
    .expect("bob message drain should not hang")
    .expect("bob message drain should succeed")
    .expect("bob should have a pending message update");
    let bob_message_body = String::from_utf8(bob_message.body).expect("utf8");
    assert!(
        bob_message_body.contains("hello"),
        "bob message payload should include the sent message: {bob_message_body}"
    );

    service
        .websocket_close(
            "chat".to_string(),
            alice.session_id,
            1000,
            "done".to_string(),
        )
        .await
        .expect("alice websocket close should succeed");
    service
        .websocket_close("chat".to_string(), bob.session_id, 1000, "done".to_string())
        .await
        .expect("bob websocket close should succeed");
}

#[tokio::test]
#[serial]
async fn chat_worker_refresh_replaces_prior_participant_socket() {
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
            "chat".to_string(),
            include_str!("../../../../../examples/chat-worker/src/worker.js").to_string(),
            DeployConfig {
                public: false,
                internal: DeployInternalConfig { trace: None },
                bindings: vec![DeployBinding::Memory {
                    binding: "CHAT_ROOM".to_string(),
                }],
            },
        )
        .await
        .expect("deploy should succeed");

    let first = tokio::time::timeout(
        Duration::from_secs(5),
        service.open_websocket(
            "chat".to_string(),
            test_websocket_invocation(
                "/rooms/test/ws?username=alice&participant=alice",
                "chat-refresh-open-1",
            ),
            None,
        ),
    )
    .await
    .expect("first websocket open should not hang")
    .expect("first websocket open should succeed");
    assert_eq!(first.output.status, 101);

    let second = tokio::time::timeout(
        Duration::from_secs(5),
        service.open_websocket(
            "chat".to_string(),
            test_websocket_invocation(
                "/rooms/test/ws?username=alice&participant=alice",
                "chat-refresh-open-2",
            ),
            None,
        ),
    )
    .await
    .expect("refreshed websocket open should not hang")
    .expect("refreshed websocket open should succeed");
    assert_eq!(second.output.status, 101);

    let state = service
        .invoke(
            "chat".to_string(),
            test_invocation_with_path("/rooms/test/state", "chat-refresh-state"),
        )
        .await
        .expect("state invoke should succeed");
    assert_eq!(state.status, 200);
    let state_json: serde_json::Value =
        serde_json::from_slice(&state.body).expect("state body should be json");
    let participants = state_json["participants"]
        .as_array()
        .expect("participants should be array");
    assert_eq!(participants.len(), 1);
    assert_eq!(participants[0]["id"], "alice");

    let refreshed_ready = tokio::time::timeout(
        Duration::from_secs(5),
        service.websocket_send_frame(
            "chat".to_string(),
            second.session_id.clone(),
            br#"{"type":"ready"}"#.to_vec(),
            false,
        ),
    )
    .await
    .expect("refreshed ready should not hang")
    .expect("refreshed ready should succeed");
    assert_eq!(refreshed_ready.status, 204);

    let refreshed_message = tokio::time::timeout(
        Duration::from_secs(5),
        service.websocket_send_frame(
            "chat".to_string(),
            second.session_id.clone(),
            br#"{"type":"message","text":"after-refresh"}"#.to_vec(),
            false,
        ),
    )
    .await
    .expect("refreshed message should not hang")
    .expect("refreshed message should succeed");
    let refreshed_message_body =
        String::from_utf8(refreshed_message.body).expect("message payload utf8");
    assert!(
        refreshed_message_body.contains("after-refresh"),
        "refreshed message payload should include the sent message: {refreshed_message_body}"
    );

    service
        .websocket_close(
            "chat".to_string(),
            second.session_id,
            1000,
            "done".to_string(),
        )
        .await
        .expect("refreshed websocket close should succeed");
    let _ = service
        .websocket_close(
            "chat".to_string(),
            first.session_id,
            1000,
            "done".to_string(),
        )
        .await;
}
