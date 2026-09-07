use super::*;

#[tokio::test]
#[serial]
async fn a_service_call_to_its_own_worker_can_use_reserved_capacity() {
    let service = test_service(RuntimeConfig {
        max_global_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 1,
        ..RuntimeConfig::default()
    })
    .await;
    service
        .deploy_with_config(
            "self-service".into(),
            r#"
export default {
  async fetch(request, env) {
    if (new URL(request.url).pathname === '/inner') return new Response('inner');
    return env.SELF.fetch('http://self-service/inner');
  }
};
"#
            .into(),
            DeployConfig {
                bindings: vec![DeployBinding::Service {
                    binding: "SELF".into(),
                    service: "self-service".into(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("self service should deploy");
    let output = timeout(
        Duration::from_secs(2),
        service.invoke("self-service".into(), test_invocation()),
    )
    .await
    .expect("a full regular worker must leave capacity for its own service call")
    .expect("self service call should succeed");
    assert_eq!(output.body, b"inner");
    let stats = service
        .stats("self-service".into())
        .await
        .expect("worker stats should exist");
    assert_eq!(stats.global_isolates_total, 2);
    assert_eq!(stats.global_internal_rescue_isolates, 1);
    service.shutdown().await.expect("runtime should shut down");
}

#[tokio::test]
#[serial]
async fn canceling_a_queued_body_releases_the_global_budget_for_another_worker() {
    let service = test_service(RuntimeConfig {
        max_global_isolates: 2,
        max_isolates: 1,
        max_inflight_per_isolate: 1,
        max_global_queued_bytes: 4096,
        ..RuntimeConfig::default()
    })
    .await;
    service
        .deploy(
            "queued-body".into(),
            r#"
export default { async fetch() {
  await new Promise(resolve => setTimeout(resolve, 500));
  return new Response('finished');
} };
"#
            .into(),
        )
        .await
        .expect("slow worker should deploy");
    service
        .deploy(
            "other-body".into(),
            "export default { fetch() { return new Response('other'); } };".into(),
        )
        .await
        .expect("other worker should deploy");
    let first_service = service.clone();
    let first = tokio::spawn(async move {
        first_service
            .invoke("queued-body".into(), test_invocation())
            .await
    });
    timeout(Duration::from_secs(2), async {
        while service
            .stats("queued-body".into())
            .await
            .expect("stats")
            .inflight_total
            == 0
        {
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("first request should start");
    let queued_service = service.clone();
    let queued = tokio::spawn(async move {
        let mut request = test_invocation();
        request.method = "POST".into();
        request.body = vec![1; 3000];
        queued_service.invoke("queued-body".into(), request).await
    });
    timeout(Duration::from_secs(2), async {
        while service
            .stats("queued-body".into())
            .await
            .expect("stats")
            .queued
            == 0
        {
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("second request should queue");
    let mut other_request = test_invocation();
    other_request.method = "POST".into();
    other_request.body = vec![2; 3000];
    let error = service
        .invoke("other-body".into(), other_request.clone())
        .await
        .expect_err("another worker must share the queued body byte limit");
    assert_eq!(error.kind(), ErrorKind::Overloaded);
    queued.abort();
    let _ = queued.await;
    timeout(Duration::from_secs(2), async {
        while service
            .stats("queued-body".into())
            .await
            .expect("stats")
            .queued
            != 0
        {
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("canceled queued request should release its admission");
    let output = service
        .invoke("other-body".into(), other_request)
        .await
        .expect("released bytes must admit another worker");
    assert_eq!(output.body, b"other");
    first
        .await
        .expect("first task should join")
        .expect("first request should complete");
    service.shutdown().await.expect("runtime should stop");
}

#[tokio::test]
#[serial]
async fn undeployed_worker_names_release_their_schedulers() {
    let service = test_service(RuntimeConfig::default()).await;
    for index in 0..24 {
        let name = format!("retire-{index}");
        service
            .deploy(
                name.clone(),
                "export default { fetch() { return new Response('ready'); } };".into(),
            )
            .await
            .expect("worker should deploy");
        service
            .undeploy(name)
            .await
            .expect("worker should undeploy");
    }
    timeout(Duration::from_secs(2), async {
        while service.admin_snapshot().await.worker_schedulers != 0 {
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("undeployed names must release their routes and scheduler tasks");
    service.shutdown().await.expect("runtime should stop");
}

#[tokio::test]
#[serial]
async fn redeployment_survives_the_previous_scheduler_retiring() {
    let root = std::env::temp_dir().join(format!("dd-scheduler-redeploy-{}", Uuid::new_v4()));
    let service = test_service_with_paths(RuntimeConfig::default(), root.clone(), true).await;
    for index in 0..12 {
        service
            .deploy(
                "replace-scheduler".into(),
                format!("export default {{ fetch() {{ return new Response('{index}'); }} }};"),
            )
            .await
            .expect("deployment must publish across scheduler retirement");
        let output = service
            .invoke("replace-scheduler".into(), test_invocation())
            .await
            .expect("replacement scheduler must accept requests");
        assert_eq!(output.body, index.to_string().as_bytes());
        service
            .undeploy("replace-scheduler".into())
            .await
            .expect("worker should undeploy");
    }
    service.shutdown().await.expect("runtime should stop");
    tokio::fs::remove_dir_all(root)
        .await
        .expect("test store should be removed");
}
