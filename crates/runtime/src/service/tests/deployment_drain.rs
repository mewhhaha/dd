use super::*;

#[tokio::test]
#[serial]
async fn disconnected_deployment_remains_in_quiescence_until_publication() {
    let service = test_service(RuntimeConfig::default()).await;
    let deployment = tokio::spawn({
        let service = service.clone();
        async move {
            service
                .deploy(
                    "detached".into(),
                    "await new Promise(resolve => setTimeout(resolve, 300)); export default { fetch() { return new Response('ok'); } };".into(),
                )
                .await
        }
    });
    timeout(Duration::from_secs(5), async {
        while !service.deployment_validation_active() {
            sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("validation starts");
    deployment.abort();
    assert!(deployment.await.unwrap_err().is_cancelled());
    assert_eq!(service.active_deployment_operations(), 1);
    assert!(!service.is_quiescent().await);
    assert!(!service.wait_for_quiescence(Duration::from_millis(10)).await);
    assert!(service.wait_for_quiescence(Duration::from_secs(5)).await);
    assert!(service.stats("detached".into()).await.is_some());
    service.shutdown().await.unwrap();
}

#[tokio::test]
#[serial]
async fn shutdown_stops_validation_and_queued_deployments_before_returning() {
    let store = TestStoreDir::new("dd-deployment-stop");
    let service = test_service_with_store(RuntimeConfig::default(), store.clone(), true).await;
    let validating = tokio::spawn({
        let service = service.clone();
        async move {
            service
                .deploy(
                    "validating".into(),
                    "while (true) {} export default { fetch() { return new Response('late'); } };"
                        .into(),
                )
                .await
        }
    });
    timeout(Duration::from_secs(5), async {
        while !service.deployment_validation_active() {
            sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("validation thread starts");
    let queued = tokio::spawn({
        let service = service.clone();
        async move {
            service
                .deploy(
                    "queued".into(),
                    "export default { fetch() { return new Response('queued'); } };".into(),
                )
                .await
        }
    });
    timeout(Duration::from_secs(5), async {
        while service.active_deployment_operations() != 2 {
            sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("second deployment is admitted");
    validating.abort();
    assert!(validating.await.unwrap_err().is_cancelled());
    timeout(Duration::from_secs(5), service.shutdown())
        .await
        .expect("shutdown interrupts validation")
        .unwrap();
    assert_eq!(service.active_deployment_operations(), 0);
    assert!(!service.deployment_validation_active());
    assert_eq!(
        queued.await.unwrap().unwrap_err().kind(),
        ErrorKind::Overloaded
    );
    assert!(
        service
            .control_store()
            .active_deployments()
            .await
            .unwrap()
            .is_empty()
    );
    let error = service
        .deploy("after-stop".into(), "".into())
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Overloaded);
    drop(service);
}

#[tokio::test]
#[serial]
async fn front_cache_namespace_uses_persisted_deployment_identity_after_restart() {
    let store = TestStoreDir::new("dd-cache-identity");
    let service = test_service_with_store(RuntimeConfig::default(), store.clone(), true).await;
    service
        .deploy_with_config(
            "cached".into(),
            "export default { fetch() { return new Response('ok'); } };".into(),
            DeployConfig {
                cache: common::DeployCacheConfig { enabled: true },
                ..DeployConfig::default()
            },
        )
        .await
        .unwrap();
    let namespace = service.front_cache_namespace("cached").unwrap();
    service.shutdown().await.unwrap();
    drop(service);
    let service = test_service_with_store(RuntimeConfig::default(), store.clone(), true).await;
    assert_eq!(service.front_cache_namespace("cached").unwrap(), namespace);
    service.shutdown().await.unwrap();
    drop(service);
}
