use super::*;

pub(super) async fn invoke_worker_websocket_private<B>(
    _state: AppState,
    _request: Request<B>,
    _ws_upgrade: Option<PreparedWebSocketUpgrade>,
) -> ApiResult<Response<ResponseBody>>
where
    B: HttpBody<Data = Bytes> + Send + Unpin + 'static,
    B::Error: std::fmt::Display + Send + Sync + 'static,
{
    Err(PlatformError::bad_request("websocket support is disabled").into())
}

pub(super) async fn invoke_worker_websocket_public<B>(
    _state: AppState,
    _request: Request<B>,
    _ws_upgrade: Option<PreparedWebSocketUpgrade>,
) -> ApiResult<Response<ResponseBody>>
where
    B: HttpBody<Data = Bytes> + Send + Unpin + 'static,
    B::Error: std::fmt::Display + Send + Sync + 'static,
{
    Err(PlatformError::bad_request("websocket support is disabled").into())
}

pub(super) fn is_websocket_upgrade(headers: &HeaderMap) -> bool {
    let Some(connection_value) = headers
        .get("connection")
        .and_then(|value| value.to_str().ok())
    else {
        return false;
    };
    let Some(upgrade_value) = headers.get("upgrade").and_then(|value| value.to_str().ok()) else {
        return false;
    };
    if !connection_value
        .split(',')
        .map(|value| value.trim())
        .any(|value| value.eq_ignore_ascii_case("upgrade"))
    {
        return false;
    }
    upgrade_value.trim().eq_ignore_ascii_case("websocket")
}

pub(super) fn prepare_websocket_upgrade<B>(
    _request: &mut Request<B>,
) -> Result<PreparedWebSocketUpgrade, PlatformError> {
    Err(PlatformError::bad_request("websocket support is disabled"))
}

pub struct PreparedWebSocketUpgrade;
