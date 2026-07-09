use crate::ServerLimits;
use crate::handlers::{handle_private_request, handle_public_request};
#[cfg(feature = "http3")]
use crate::public_quic;
use crate::state::AppState;
use common::{PlatformError, Result};
use http::header::{ALT_SVC, HeaderValue};
use http::{Request, Response};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::server::conn::auto::Builder as AutoBuilder;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::{error, info, warn};

pub async fn serve(
    public_addr: SocketAddr,
    private_addr: SocketAddr,
    state: AppState,
    limits: ServerLimits,
) -> Result<()> {
    let public_listener = tokio::net::TcpListener::bind(public_addr)
        .await
        .map_err(|error| PlatformError::internal(error.to_string()))?;
    let private_listener = tokio::net::TcpListener::bind(private_addr)
        .await
        .map_err(|error| PlatformError::internal(error.to_string()))?;

    info!("public tcp listener on http://{}", public_addr);
    info!("private tcp listener on http://{}", private_addr);

    match public_h3_enabled(&state) {
        true => {
            info!("public quic listener on https://{} (http/3)", public_addr);
            #[cfg(feature = "http3")]
            tokio::select! {
                result = serve_public_listener(public_listener, state.clone(), build_alt_svc_header(public_addr.port()), limits.clone()) => result,
                result = serve_private_listener(private_listener, state.clone(), limits.clone()) => result,
                result = public_quic::serve_public_h3(public_addr, state) => result,
            }
            #[cfg(not(feature = "http3"))]
            unreachable!("public_h3_enabled is false without the http3 feature")
        }
        false => {
            warn!(
                "public http/3 disabled because PUBLIC_TLS_CERT_PATH/PUBLIC_TLS_KEY_PATH are not configured"
            );
            tokio::select! {
                result = serve_public_listener(public_listener, state.clone(), None, limits.clone()) => result,
                result = serve_private_listener(private_listener, state, limits) => result,
            }
        }
    }
}

#[cfg(feature = "http3")]
fn public_h3_enabled(state: &AppState) -> bool {
    state.public_tls_cert_path.is_some() && state.public_tls_key_path.is_some()
}

#[cfg(not(feature = "http3"))]
fn public_h3_enabled(_state: &AppState) -> bool {
    false
}

async fn serve_public_listener(
    listener: tokio::net::TcpListener,
    state: AppState,
    alt_svc_header: Option<HeaderValue>,
    limits: ServerLimits,
) -> Result<()> {
    let max_connections = limits.max_public_connections;
    serve_listener(
        listener,
        state,
        ListenerKind::Public { alt_svc_header },
        limits,
        max_connections,
    )
    .await
}

async fn serve_private_listener(
    listener: tokio::net::TcpListener,
    state: AppState,
    limits: ServerLimits,
) -> Result<()> {
    let max_connections = limits.max_private_connections;
    serve_listener(
        listener,
        state,
        ListenerKind::Private,
        limits,
        max_connections,
    )
    .await
}

#[derive(Clone)]
enum ListenerKind {
    Public { alt_svc_header: Option<HeaderValue> },
    Private,
}

async fn serve_listener(
    listener: tokio::net::TcpListener,
    state: AppState,
    kind: ListenerKind,
    limits: ServerLimits,
    max_connections: usize,
) -> Result<()> {
    if max_connections == 0 {
        return Err(PlatformError::internal(
            "listener max connections must be greater than zero",
        ));
    }
    let admission = Arc::new(Semaphore::new(max_connections));
    loop {
        let permit = Arc::clone(&admission)
            .acquire_owned()
            .await
            .map_err(|_| PlatformError::internal("listener admission semaphore closed"))?;
        let (stream, remote_addr) = listener
            .accept()
            .await
            .map_err(|error| PlatformError::internal(error.to_string()))?;
        let io = TokioIo::new(stream);
        let state = state.clone();
        let kind = kind.clone();
        let limits = limits.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let service = service_fn(move |request: Request<Incoming>| {
                let state = state.clone();
                let kind = kind.clone();
                async move {
                    let mut response = match &kind {
                        ListenerKind::Public { .. } => handle_public_request(state, request).await,
                        ListenerKind::Private => handle_private_request(state, request).await,
                    };
                    if let ListenerKind::Public {
                        alt_svc_header: Some(value),
                    } = &kind
                    {
                        annotate_alt_svc_header(&mut response, value);
                    }
                    Ok::<_, Infallible>(response)
                }
            });

            let mut builder = AutoBuilder::new(TokioExecutor::new());
            builder
                .http1()
                .timer(TokioTimer::new())
                .header_read_timeout(limits.header_read_timeout)
                .max_headers(limits.max_headers);
            builder
                .http2()
                .timer(TokioTimer::new())
                .keep_alive_interval(limits.http2_keep_alive_interval)
                .keep_alive_timeout(limits.http2_keep_alive_timeout);
            if let Err(error) = builder.serve_connection_with_upgrades(io, service).await {
                error!(%remote_addr, error = %error, "http connection failed");
            }
        });
    }
}

#[cfg(feature = "http3")]
fn build_alt_svc_header(port: u16) -> Option<HeaderValue> {
    let value = format!("h3=\":{port}\"; ma=86400");
    HeaderValue::from_str(&value).ok()
}

fn annotate_alt_svc_header<T>(response: &mut Response<T>, value: &HeaderValue) {
    response.headers_mut().insert(ALT_SVC, value.clone());
}
