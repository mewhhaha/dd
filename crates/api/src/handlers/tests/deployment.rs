//! Deploy permissions, deployment history and worker activation.
use super::fixtures::TestState;
use crate::handlers::{
    deploy_worker, handle_private_request, handle_public_request, invoke_worker_private,
    invoke_worker_public,
};
use bytes::Bytes;
use common::{
    DeployBinding, DeployConfig, DeployRequest, DeployTokenCapabilities, DeployTokenMintRequest,
    DeployTokenMintResponse, DeploymentInspectResponse, DeploymentListResponse, RollbackRequest,
    RollbackResponse, UndeployResponse, WorkerInvocation, WorkerNameRequest,
};
use http::{Request, StatusCode};
use http_body_util::{BodyExt, Empty, Full};
use serial_test::serial;

#[tokio::test]
#[serial]
async fn public_listener_requires_deploy_token() {
    let state = TestState::new("example.com").await;
    let request = Request::builder()
        .method("POST")
        .uri("/v1/deploy")
        .header("host", "echo.example.com")
        .body(Empty::<Bytes>::new())
        .expect("request");
    let response = handle_public_request(state.app(), request).await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    state.shutdown().await;
}

#[tokio::test]
#[serial]
async fn private_admin_mints_scoped_deploy_token_for_public_deploy() {
    let state = TestState::new("example.com").await;
    let mint = DeployTokenMintRequest {
        name: Some("ci".to_string()),
        max_uses: Some(1),
        capabilities: DeployTokenCapabilities {
            workers: vec!["echo".to_string()],
            allow_public: true,
            bindings: vec![DeployBinding::Memory {
                binding: "ROOM".to_string(),
            }],
            max_source_bytes: Some(1024),
            max_assets: Some(0),
            ..DeployTokenCapabilities::default()
        },
        ..DeployTokenMintRequest::default()
    };
    let request = Request::builder()
        .method("POST")
        .uri("/v1/admin/tokens")
        .header("authorization", "Bearer test-private-token")
        .header("content-type", "application/json")
        .body(http_body_util::Full::new(Bytes::from(
            serde_json::to_vec(&mint).expect("mint json"),
        )))
        .expect("request");
    let response = handle_private_request(state.app(), request).await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    let minted: DeployTokenMintResponse = serde_json::from_slice(&body).expect("minted token");
    assert!(minted.ok);
    assert_eq!(minted.id, "ci");
    assert_eq!(minted.name.as_deref(), Some("ci"));

    let deploy = DeployRequest {
        name: "echo".to_string(),
        source: "export default { async fetch() { return new Response('public-ok'); } }"
            .to_string(),
        config: DeployConfig {
            public: true,
            cache: Default::default(),
            bindings: vec![DeployBinding::Memory {
                binding: "ROOM".to_string(),
            }],
            ..DeployConfig::default()
        },
        assets: Vec::new(),
        server_modules: Vec::new(),
        asset_headers: None,
        temporary: false,
    };
    let request = Request::builder()
        .method("POST")
        .uri("/v1/deploy")
        .header("host", "example.com")
        .header("authorization", format!("Bearer {}", minted.token))
        .header("content-type", "application/json")
        .body(http_body_util::Full::new(Bytes::from(
            serde_json::to_vec(&deploy).expect("deploy json"),
        )))
        .expect("request");
    let response = handle_public_request(state.app(), request).await;
    assert_eq!(response.status(), StatusCode::OK);

    let request = Request::builder()
        .method("GET")
        .uri("/")
        .header("host", "echo.example.com")
        .body(Empty::<Bytes>::new())
        .expect("request");
    let response = invoke_worker_public(state.app(), request, None)
        .await
        .expect("invoke");
    let body = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    assert_eq!(body.as_ref(), b"public-ok");
    state.shutdown().await;
}

#[tokio::test]
#[serial]
async fn public_deploy_token_rejects_unscoped_binding() {
    let state = TestState::new("example.com").await;
    let minted = state
        .app()
        .deploy_tokens
        .mint(DeployTokenMintRequest {
            capabilities: DeployTokenCapabilities {
                workers: vec!["echo".to_string()],
                allow_public: true,
                bindings: vec![DeployBinding::Memory {
                    binding: "ROOM".to_string(),
                }],
                ..DeployTokenCapabilities::default()
            },
            ..DeployTokenMintRequest::default()
        })
        .await
        .expect("mint");
    let deploy = DeployRequest {
        name: "echo".to_string(),
        source: "export default { async fetch() { return new Response('bad'); } }".to_string(),
        config: DeployConfig {
            public: true,
            cache: Default::default(),
            bindings: vec![DeployBinding::Kv {
                binding: "ROOM".to_string(),
            }],
            ..DeployConfig::default()
        },
        assets: Vec::new(),
        server_modules: Vec::new(),
        asset_headers: None,
        temporary: false,
    };
    let request = Request::builder()
        .method("POST")
        .uri("/v1/deploy")
        .header("host", "example.com")
        .header("authorization", format!("Bearer {}", minted.token))
        .header("content-type", "application/json")
        .body(http_body_util::Full::new(Bytes::from(
            serde_json::to_vec(&deploy).expect("deploy json"),
        )))
        .expect("request");
    let response = handle_public_request(state.app(), request).await;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    state.shutdown().await;
}

#[tokio::test]
#[serial]
async fn private_deployment_history_rolls_back_and_undeploys() {
    let state = TestState::new("example.com").await;
    let deploy = |source: &str| DeployRequest {
        name: "history-name".to_string(),
        source: format!(
            "export default {{ async fetch() {{ return new Response({source:?}); }} }}"
        ),
        config: DeployConfig::default(),
        assets: Vec::new(),
        server_modules: Vec::new(),
        asset_headers: None,
        temporary: false,
    };
    let first = deploy_worker(state.app(), deploy("first"))
        .await
        .expect("first deploy");
    let second = deploy_worker(state.app(), deploy("second"))
        .await
        .expect("second deploy");

    let list = Request::builder()
        .method("GET")
        .uri("/v1/admin/deployments?worker=history%2Dname")
        .header("authorization", "Bearer test-private-token")
        .body(Empty::<Bytes>::new())
        .expect("list request");
    let list = handle_private_request(state.app(), list).await;
    assert_eq!(list.status(), StatusCode::OK);
    let list: DeploymentListResponse = serde_json::from_slice(
        &list
            .into_body()
            .collect()
            .await
            .expect("list body")
            .to_bytes(),
    )
    .expect("deployment list");
    assert_eq!(list.deployments.len(), 2);
    assert!(
        list.deployments.iter().any(
            |deployment| deployment.deployment_id == second.deployment_id && deployment.active
        )
    );

    let inspect = Request::builder()
        .method("GET")
        .uri(format!("/v1/admin/deployment?id={}", first.deployment_id))
        .header("authorization", "Bearer test-private-token")
        .body(Empty::<Bytes>::new())
        .expect("inspect request");
    let inspect = handle_private_request(state.app(), inspect).await;
    assert_eq!(inspect.status(), StatusCode::OK);
    let inspect: DeploymentInspectResponse = serde_json::from_slice(
        &inspect
            .into_body()
            .collect()
            .await
            .expect("inspect body")
            .to_bytes(),
    )
    .expect("deployment inspect");
    assert_eq!(
        inspect.deployment.summary.deployment_id,
        first.deployment_id
    );
    assert!(inspect.deployment.source.contains("first"));

    let rollback = Request::builder()
        .method("POST")
        .uri("/v1/admin/rollback")
        .header("authorization", "Bearer test-private-token")
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(
            serde_json::to_vec(&RollbackRequest {
                worker: "history-name".to_string(),
                deployment_id: first.deployment_id.clone(),
            })
            .expect("rollback json"),
        )))
        .expect("rollback request");
    let rollback = handle_private_request(state.app(), rollback).await;
    assert_eq!(rollback.status(), StatusCode::OK);
    let rollback: RollbackResponse = serde_json::from_slice(
        &rollback
            .into_body()
            .collect()
            .await
            .expect("rollback body")
            .to_bytes(),
    )
    .expect("rollback response");
    assert_eq!(rollback.deployment_id, first.deployment_id);

    let output = state
        .app()
        .runtime
        .invoke(
            "history-name".to_string(),
            WorkerInvocation {
                method: "GET".to_string(),
                url: "http://history-name/".to_string(),
                headers: Vec::new(),
                body: Vec::new(),
                request_id: "history-after-rollback".to_string(),
            },
        )
        .await
        .expect("invoke rollback");
    assert_eq!(output.body, b"first");

    let undeploy = Request::builder()
        .method("POST")
        .uri("/v1/admin/undeploy")
        .header("authorization", "Bearer test-private-token")
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(
            serde_json::to_vec(&WorkerNameRequest {
                worker: "history-name".to_string(),
            })
            .expect("undeploy json"),
        )))
        .expect("undeploy request");
    let undeploy = handle_private_request(state.app(), undeploy).await;
    assert_eq!(undeploy.status(), StatusCode::OK);
    let undeploy: UndeployResponse = serde_json::from_slice(
        &undeploy
            .into_body()
            .collect()
            .await
            .expect("undeploy body")
            .to_bytes(),
    )
    .expect("undeploy response");
    assert_eq!(undeploy.worker, "history-name");
    assert!(
        state
            .app()
            .runtime
            .stats("history-name".to_string())
            .await
            .is_none()
    );
    assert!(
        state
            .app()
            .runtime
            .deployments(Some("history-name"))
            .await
            .expect("history remains inspectable")
            .iter()
            .all(|deployment| !deployment.active)
    );

    state.shutdown().await;
}

#[tokio::test]
#[serial]
async fn private_deploy_and_invoke_succeeds() {
    let state = TestState::new("example.com").await;
    let deploy = DeployRequest {
        name: "echo".to_string(),
        source: "export default { async fetch() { return new Response('ok'); } }".to_string(),
        config: DeployConfig {
            egress_allow_hosts: Vec::new(),
            public: false,
            cache: Default::default(),
            bindings: vec![],
            ..Default::default()
        },
        assets: Vec::new(),
        server_modules: Vec::new(),
        asset_headers: None,
        temporary: false,
    };
    let response = deploy_worker(state.app(), deploy).await.expect("deploy");
    assert!(response.ok);

    let request = Request::builder()
        .method("GET")
        .uri("/v1/invoke/echo")
        .header("authorization", "Bearer test-private-token")
        .body(Empty::<Bytes>::new())
        .expect("request");
    let response = invoke_worker_private(state.app(), request, None)
        .await
        .expect("invoke");
    let body = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    assert_eq!(body.as_ref(), b"ok");
    state.shutdown().await;
}
