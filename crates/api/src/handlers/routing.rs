use super::*;
pub async fn handle_private_request<B>(
    state: AppState,
    request: Request<B>,
) -> Response<ResponseBody>
where
    B: HttpBody<Data = Bytes> + Send + Unpin + 'static,
    B::Error: std::fmt::Display + Send + Sync + 'static,
{
    if request_may_run_while_draining(request.method(), request.uri().path()) {
        return respond(route_private_request(state, request).await);
    }
    let Some(active_request) = state.operations.try_begin_request() else {
        return respond(Err(PlatformError::overloaded("service is draining").into()));
    };
    track_response(
        respond(route_private_request(state, request).await),
        active_request,
    )
}

pub async fn handle_public_request<B>(
    state: AppState,
    request: Request<B>,
) -> Response<ResponseBody>
where
    B: HttpBody<Data = Bytes> + Send + Unpin + 'static,
    B::Error: std::fmt::Display + Send + Sync + 'static,
{
    if request_may_run_while_draining(request.method(), request.uri().path()) {
        return respond(route_public_request(state, request).await);
    }
    let Some(active_request) = state.operations.try_begin_request() else {
        return respond(Err(PlatformError::overloaded("service is draining").into()));
    };
    track_response(
        respond(route_public_request(state, request).await),
        active_request,
    )
}

#[cfg(feature = "http3")]
pub async fn handle_public_h3_request(
    state: AppState,
    request: Request<()>,
    request_body_stream: Option<runtime::InvokeRequestBodyReceiver>,
) -> Response<ResponseBody> {
    if request_may_run_while_draining(request.method(), request.uri().path()) {
        return respond(route_public_h3_request(state, request, request_body_stream).await);
    }
    let Some(active_request) = state.operations.try_begin_request() else {
        return respond(Err(PlatformError::overloaded("service is draining").into()));
    };
    track_response(
        respond(route_public_h3_request(state, request, request_body_stream).await),
        active_request,
    )
}

async fn route_private_request<B>(
    state: AppState,
    mut request: Request<B>,
) -> ApiResult<Response<ResponseBody>>
where
    B: HttpBody<Data = Bytes> + Send + Unpin + 'static,
    B::Error: std::fmt::Display + Send + Sync + 'static,
{
    let path = request.uri().path().to_string();
    if request.method() == Method::GET && path == "/healthz" {
        return Ok(json_response(
            StatusCode::OK,
            &serde_json::json!({"ok": true}),
        )?);
    }
    if request.method() == Method::GET && path == "/readyz" {
        return readiness_response(&state).await;
    }
    if private_route_requires_auth(&path)
        && !private_request_is_authorized(&state, request.headers())
    {
        return Ok(private_auth_response());
    }
    if request.method() == Method::GET && path == "/v1/admin/status" {
        return observability::status_response(&state).await;
    }
    if request.method() == Method::GET && path == "/metrics" {
        return observability::metrics_response(&state).await;
    }
    if request.method() == Method::POST && path == "/v1/admin/drain" {
        state.operations.begin_drain();
        let timeout = std::time::Duration::from_secs(20);
        let started_at = std::time::Instant::now();
        let requests_drained = state.operations.wait_for_drain(timeout).await;
        let runtime_drained = state
            .runtime
            .wait_for_quiescence(timeout.saturating_sub(started_at.elapsed()))
            .await;
        let drained = requests_drained && runtime_drained;
        return Ok(json_response(
            StatusCode::OK,
            &serde_json::json!({
                "ok": true,
                "draining": true,
                "drained": drained,
                "requests_drained": requests_drained,
                "runtime_drained": runtime_drained,
                "active_requests": state.operations.active_requests(),
            }),
        )?);
    }
    if request.method() == Method::POST && path == "/v1/admin/resume" {
        if !state.operations.resume() {
            return Err(PlatformError::conflict(
                "service shutdown is in progress and cannot be resumed",
            )
            .into());
        }
        return Ok(json_response(
            StatusCode::OK,
            &serde_json::json!({"ok": true, "draining": false}),
        )?);
    }
    if request.method() == Method::POST && path == "/v1/admin/checkpoint" {
        return observability::checkpoint_response(&state).await;
    }
    if request.method() == Method::GET && path == "/v1/admin/deployments" {
        let worker = query_parameter(request.uri(), "worker");
        let response = DeploymentListResponse {
            ok: true,
            deployments: state.runtime.deployments(worker.as_deref()).await?,
        };
        return Ok(json_response(StatusCode::OK, &response)?);
    }
    if request.method() == Method::GET && path == "/v1/admin/deployment" {
        let deployment_id = query_parameter(request.uri(), "id")
            .ok_or_else(|| PlatformError::bad_request("missing deployment id"))?;
        let response = DeploymentInspectResponse {
            ok: true,
            deployment: state.runtime.deployment(&deployment_id).await?,
        };
        return Ok(json_response(StatusCode::OK, &response)?);
    }
    if request.method() == Method::POST && path == "/v1/admin/undeploy" {
        let payload: WorkerNameRequest =
            read_json_body(request.into_body(), state.invoke_max_body_bytes).await?;
        let worker = validate_worker_name(&payload.worker)?;
        state.runtime.undeploy(worker.clone()).await?;
        return Ok(json_response(
            StatusCode::OK,
            &UndeployResponse { ok: true, worker },
        )?);
    }
    if request.method() == Method::POST && path == "/v1/admin/rollback" {
        let payload: RollbackRequest =
            read_json_body(request.into_body(), state.invoke_max_body_bytes).await?;
        let worker = validate_worker_name(&payload.worker)?;
        if payload.deployment_id.trim().is_empty() {
            return Err(PlatformError::bad_request("deployment id must not be empty").into());
        }
        let deployment_id = state
            .runtime
            .rollback(worker.clone(), payload.deployment_id)
            .await?;
        return Ok(json_response(
            StatusCode::OK,
            &RollbackResponse {
                ok: true,
                worker,
                deployment_id,
            },
        )?);
    }
    if request.method() == Method::POST && path == "/v1/deploy" {
        let payload: DeployRequest =
            read_json_body(request.into_body(), state.invoke_max_body_bytes).await?;
        let response = deploy_worker(state, payload).await?;
        return Ok(json_response(StatusCode::OK, &response)?);
    }
    if request.method() == Method::POST && path == "/v1/admin/tokens" {
        let payload: DeployTokenMintRequest =
            read_json_body(request.into_body(), state.invoke_max_body_bytes).await?;
        let response = mint_deploy_token(state, payload).await?;
        return Ok(json_response(StatusCode::OK, &response)?);
    }
    if request.method() == Method::GET && path == "/v1/admin/tokens" {
        let response = list_tokens(state).await?;
        return Ok(json_response(StatusCode::OK, &response)?);
    }
    if let Some(token_id) = token_id_from_path(&path) {
        if request.method() == Method::GET {
            let response = get_token(state, token_id).await?;
            return Ok(json_response(StatusCode::OK, &response)?);
        }
        if request.method() == Method::DELETE {
            let response = delete_token(state, token_id).await?;
            return Ok(json_response(StatusCode::OK, &response)?);
        }
    }
    if request.method() == Method::POST && path == "/v1/dynamic/deploy" {
        let payload: DynamicDeployRequest =
            read_json_body(request.into_body(), state.invoke_max_body_bytes).await?;
        let response = deploy_dynamic_worker(state, payload).await?;
        return Ok(json_response(StatusCode::OK, &response)?);
    }
    if path == "/v1/invoke" || path.starts_with("/v1/invoke/") {
        let ws_upgrade = if is_websocket_upgrade(request.headers()) {
            Some(prepare_websocket_upgrade(&mut request)?)
        } else {
            None
        };
        return invoke_worker_private(state, request, ws_upgrade).await;
    }
    Err(PlatformError::not_found("not found").into())
}

async fn route_public_request<B>(
    state: AppState,
    mut request: Request<B>,
) -> ApiResult<Response<ResponseBody>>
where
    B: HttpBody<Data = Bytes> + Send + Unpin + 'static,
    B::Error: std::fmt::Display + Send + Sync + 'static,
{
    let path = request.uri().path().to_string();
    if request.method() == Method::GET && path == "/healthz" {
        return Ok(json_response(
            StatusCode::OK,
            &serde_json::json!({"ok": true}),
        )?);
    }
    if request.method() == Method::GET && path == "/readyz" {
        return readiness_response(&state).await;
    }
    if request.method() == Method::POST && path == "/v1/deploy" {
        let token = bearer_token_from_headers(request.headers())
            .ok_or_else(|| PlatformError::unauthorized("public deploy requires a token"))?
            .to_string();
        state.deploy_tokens.preflight(&token).await?;
        let payload: DeployRequest =
            read_json_body(request.into_body(), state.invoke_max_body_bytes).await?;
        validate_deploy_request(&payload)?;
        state
            .deploy_tokens
            .authorize_deploy(&token, &payload)
            .await?;
        let response = deploy_worker(state, payload).await?;
        return Ok(json_response(StatusCode::OK, &response)?);
    }
    if public_route_is_reserved(&path) {
        return Err(PlatformError::not_found("not found").into());
    }
    let ws_upgrade = if is_websocket_upgrade(request.headers()) {
        Some(prepare_websocket_upgrade(&mut request)?)
    } else {
        None
    };
    invoke_worker_public(state, request, ws_upgrade).await
}

async fn readiness_response(state: &AppState) -> ApiResult<Response<ResponseBody>> {
    let runtime = state.runtime.readiness().await;
    let ready = state.operations.is_ready() && runtime.ready;
    json_response(
        if ready {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        },
        &serde_json::json!({
            "ok": ready,
            "ready": ready,
            "draining": state.operations.is_draining(),
            "shutting_down": state.operations.is_shutting_down(),
            "active_requests": state.operations.active_requests(),
            "runtime_ready": runtime.runtime_ready,
            "migrations_ready": runtime.migrations_ready,
            "storage_ready": runtime.storage_ready,
            "worker_restoration_ready": runtime.worker_restoration_ready,
            "restore_failure_count": runtime.restore_failure_count,
            "failed_components": runtime.failed_components,
        }),
    )
    .map_err(Into::into)
}

fn request_may_run_while_draining(method: &Method, path: &str) -> bool {
    (*method == Method::GET
        && matches!(
            path,
            "/healthz" | "/readyz" | "/v1/admin/status" | "/metrics"
        ))
        || (*method == Method::POST
            && matches!(
                path,
                "/v1/admin/drain" | "/v1/admin/resume" | "/v1/admin/checkpoint"
            ))
}

#[cfg(feature = "http3")]
async fn route_public_h3_request(
    state: AppState,
    request: Request<()>,
    request_body_stream: Option<runtime::InvokeRequestBodyReceiver>,
) -> ApiResult<Response<ResponseBody>> {
    let path = request.uri().path().to_string();
    if request.method() == Method::GET && path == "/healthz" {
        return Ok(json_response(
            StatusCode::OK,
            &serde_json::json!({"ok": true}),
        )?);
    }
    if request.method() == Method::GET && path == "/readyz" {
        return readiness_response(&state).await;
    }
    if public_route_is_reserved(&path) {
        return Err(PlatformError::not_found("not found").into());
    }
    if is_websocket_upgrade(request.headers()) {
        return Err(
            PlatformError::bad_request("websocket upgrade is unsupported over http/3").into(),
        );
    }
    if request.method() == Method::CONNECT {
        return Err(PlatformError::bad_request("CONNECT is unsupported over http/3").into());
    }
    invoke_worker_public_h3(state, request, request_body_stream).await
}

pub async fn deploy_worker(state: AppState, payload: DeployRequest) -> ApiResult<DeployResponse> {
    let name = validate_deploy_request(&payload)?;
    let span = tracing::info_span!("http.deploy", worker.name = %name);
    async move {
        let deployment_id = state
            .runtime
            .deploy_with_bundle_config_lifecycle_and_server_modules(
                name.clone(),
                payload.source,
                payload.config,
                payload.assets,
                payload.server_modules,
                payload.asset_headers,
                payload.temporary,
            )
            .await?;
        tracing::info!(deployment_id = %deployment_id, "worker deployed");

        Ok(DeployResponse {
            ok: true,
            worker: name,
            deployment_id,
        })
    }
    .instrument(span)
    .await
}

fn validate_deploy_request(payload: &DeployRequest) -> ApiResult<String> {
    let name = validate_worker_name(&payload.name)?;

    validate_deploy_bindings(&payload.config.bindings)?;
    validate_internal_config(&payload.config.internal)?;
    Ok(name)
}

pub async fn mint_deploy_token(
    state: AppState,
    payload: DeployTokenMintRequest,
) -> ApiResult<DeployTokenMintResponse> {
    let response = state.deploy_tokens.mint(payload).await?;
    Ok(response)
}

pub async fn list_tokens(state: AppState) -> ApiResult<DeployTokenListResponse> {
    let response = state.deploy_tokens.list().await?;
    Ok(response)
}

pub async fn get_token(state: AppState, id: &str) -> ApiResult<DeployTokenGetResponse> {
    let response = state.deploy_tokens.get(id).await?;
    Ok(response)
}

pub async fn delete_token(state: AppState, id: &str) -> ApiResult<DeployTokenDeleteResponse> {
    let response = state.deploy_tokens.delete(id).await?;
    Ok(response)
}

fn token_id_from_path(path: &str) -> Option<&str> {
    let id = path.strip_prefix("/v1/admin/tokens/")?;
    if id.is_empty() || id.contains('/') {
        return None;
    }
    Some(id)
}

fn query_parameter(uri: &http::Uri, name: &str) -> Option<String> {
    url::form_urlencoded::parse(uri.query()?.as_bytes())
        .find(|(key, _)| key == name)
        .map(|(_, value)| value.into_owned())
        .filter(|value| !value.is_empty())
}

pub async fn deploy_dynamic_worker(
    state: AppState,
    payload: DynamicDeployRequest,
) -> ApiResult<DynamicDeployResponse> {
    if payload.source.trim().is_empty() {
        return Err(PlatformError::bad_request("Worker source must not be empty").into());
    }
    let deployed = state
        .runtime
        .deploy_dynamic(payload.source, payload.env, payload.egress_allow_hosts)
        .await?;

    Ok(DynamicDeployResponse {
        ok: true,
        worker: deployed.worker,
        deployment_id: deployed.deployment_id,
        env_placeholders: deployed.env_placeholders,
    })
}
