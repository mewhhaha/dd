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
    if request.method() == Method::POST && path == "/v1/deploy" {
        let token = bearer_token_from_headers(request.headers())
            .ok_or_else(|| PlatformError::unauthorized("public deploy requires a token"))?
            .to_string();
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

async fn route_public_h3_request(
    state: AppState,
    request: Request<()>,
    request_body_stream: Option<runtime::InvokeRequestBodyReceiver>,
) -> ApiResult<Response<ResponseBody>> {
    let path = request.uri().path().to_string();
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
    let _guard = span.enter();
    let deployment_id = state
        .runtime
        .deploy_with_bundle_config_lifecycle(
            name.clone(),
            payload.source,
            payload.config,
            payload.assets,
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

fn validate_deploy_request(payload: &DeployRequest) -> ApiResult<String> {
    let name = payload.name.trim();
    if name.is_empty() {
        return Err(PlatformError::bad_request("Worker name must not be empty").into());
    }

    validate_deploy_bindings(&payload.config.bindings)?;
    validate_internal_config(&payload.config.internal)?;
    Ok(name.to_string())
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
