use crate::handlers::{handle_private_request, handle_public_request};
use crate::public_quic;
use crate::state::AppState;
use common::{PlatformError, Result};
use http::header::{HeaderValue, ALT_SVC};
use http::{Request, Response};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as AutoBuilder;
use std::convert::Infallible;
use std::net::SocketAddr;
use tracing::{error, info, warn};

pub async fn serve(
    public_addr: SocketAddr,
    private_addr: SocketAddr,
    state: AppState,
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
            tokio::select! {
                result = serve_public_listener(public_listener, state.clone(), Some(public_addr.port())) => result,
                result = serve_private_listener(private_listener, state.clone()) => result,
                result = public_quic::serve_public_h3(public_addr, state) => result,
            }
        }
        false => {
            warn!("public http/3 disabled because PUBLIC_TLS_CERT_PATH/PUBLIC_TLS_KEY_PATH are not configured");
            tokio::select! {
                result = serve_public_listener(public_listener, state.clone(), None) => result,
                result = serve_private_listener(private_listener, state) => result,
            }
        }
    }
}

fn public_h3_enabled(state: &AppState) -> bool {
    state.public_tls_cert_path.is_some() && state.public_tls_key_path.is_some()
}

async fn serve_public_listener(
    listener: tokio::net::TcpListener,
    state: AppState,
    alt_svc_port: Option<u16>,
) -> Result<()> {
    serve_listener(listener, state, ListenerKind::Public { alt_svc_port }).await
}

async fn serve_private_listener(listener: tokio::net::TcpListener, state: AppState) -> Result<()> {
    serve_listener(listener, state, ListenerKind::Private).await
}

#[derive(Clone, Copy)]
enum ListenerKind {
    Public { alt_svc_port: Option<u16> },
    Private,
}

async fn serve_listener(
    listener: tokio::net::TcpListener,
    state: AppState,
    kind: ListenerKind,
) -> Result<()> {
    loop {
        let (stream, remote_addr) = listener
            .accept()
            .await
            .map_err(|error| PlatformError::internal(error.to_string()))?;
        let io = TokioIo::new(stream);
        let state = state.clone();
        tokio::spawn(async move {
            let service = service_fn(move |request: Request<Incoming>| {
                let state = state.clone();
                async move {
                    let mut response = match kind {
                        ListenerKind::Public { .. } => handle_public_request(state, request).await,
                        ListenerKind::Private => handle_private_request(state, request).await,
                    };
                    if let ListenerKind::Public { alt_svc_port } = kind {
                        if let Some(port) = alt_svc_port {
                            annotate_alt_svc_header(&mut response, port);
                        }
                    }
                    Ok::<_, Infallible>(response)
                }
            });

            let builder = AutoBuilder::new(TokioExecutor::new());
            if let Err(error) = builder.serve_connection_with_upgrades(io, service).await {
                error!(%remote_addr, error = %error, "http connection failed");
            }
        });
    }
}

fn annotate_alt_svc_header<T>(response: &mut Response<T>, port: u16) {
    let value = format!("h3=\":{port}\"; ma=86400");
    if let Ok(value) = HeaderValue::from_str(&value) {
        response.headers_mut().insert(ALT_SVC, value);
    }
}
