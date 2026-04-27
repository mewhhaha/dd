use super::*;
pub async fn handle_private_request<B>(
    state: AppState,
    request: Request<B>,
) -> Response<ResponseBody>
where
    B: HttpBody<Data = Bytes> + Send + Unpin + 'static,
    B::Error: std::fmt::Display + Send + Sync + 'static,
{
    respond(route_private_request(state, request).await)
}

pub async fn handle_public_request<B>(
    state: AppState,
    request: Request<B>,
) -> Response<ResponseBody>
where
    B: HttpBody<Data = Bytes> + Send + Unpin + 'static,
    B::Error: std::fmt::Display + Send + Sync + 'static,
{
    respond(route_public_request(state, request).await)
}

pub async fn handle_public_h3_request(
    state: AppState,
    request: Request<()>,
    request_body_stream: Option<runtime::InvokeRequestBodyReceiver>,
) -> Response<ResponseBody> {
    respond(route_public_h3_request(state, request, request_body_stream).await)
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
    if private_route_requires_auth(&path)
        && !private_request_is_authorized(&state, request.headers())
    {
        return Ok(private_auth_response());
    }
    if request.method() == Method::POST && path == "/v1/deploy" {
        let payload: DeployRequest =
            read_json_body(request.into_body(), state.invoke_max_body_bytes).await?;
        let response = deploy_worker(state, payload).await?;
        return Ok(json_response(StatusCode::OK, &response)?);
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
    if path.starts_with("/v1/deploy")
        || path.starts_with("/v1/dynamic")
        || path.starts_with("/v1/invoke")
    {
        return Err(PlatformError::not_found("not found").into());
    }
    let ws_upgrade = if is_websocket_upgrade(request.headers()) {
        Some(prepare_websocket_upgrade(&mut request)?)
    } else {
        None
    };
    invoke_worker_public(state, request, ws_upgrade).await
}

async fn route_public_h3_request(
    state: AppState,
    request: Request<()>,
    request_body_stream: Option<runtime::InvokeRequestBodyReceiver>,
) -> ApiResult<Response<ResponseBody>> {
    let path = request.uri().path().to_string();
    if path.starts_with("/v1/deploy")
        || path.starts_with("/v1/dynamic")
        || path.starts_with("/v1/invoke")
    {
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
    let name = payload.name.trim();
    let span = tracing::info_span!("http.deploy", worker.name = %name);
    let _guard = span.enter();
    if name.is_empty() {
        return Err(PlatformError::bad_request("Worker name must not be empty").into());
    }

    validate_deploy_bindings(&payload.config.bindings)?;
    validate_internal_config(&payload.config.internal)?;
    let deployment_id = state
        .runtime
        .deploy_with_bundle_config(
            name.to_string(),
            payload.source,
            payload.config,
            payload.assets,
            payload.asset_headers,
        )
        .await?;
    tracing::info!(deployment_id = %deployment_id, "worker deployed");

    Ok(DeployResponse {
        ok: true,
        worker: name.to_string(),
        deployment_id,
    })
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
