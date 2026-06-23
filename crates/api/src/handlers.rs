mod invocation;
mod routing;
mod util;
#[cfg(feature = "websocket")]
mod websocket;
#[cfg(not(feature = "websocket"))]
#[path = "handlers/websocket_disabled.rs"]
mod websocket;

use crate::state::AppState;
use bytes::Bytes;
use common::{
    DeployBinding, DeployInternalConfig, DeployRequest, DeployResponse, DeployTokenDeleteResponse,
    DeployTokenGetResponse, DeployTokenListResponse, DeployTokenMintRequest,
    DeployTokenMintResponse, DynamicDeployRequest, DynamicDeployResponse, ErrorBody, ErrorKind,
    PlatformError, WorkerInvocation, WorkerOutput,
};
use futures_util::StreamExt;
#[cfg(feature = "websocket")]
use futures_util::{stream::SplitSink, SinkExt};
use http::header::{
    HeaderName, HeaderValue, AUTHORIZATION, CONTENT_LENGTH, HOST, TRANSFER_ENCODING,
    WWW_AUTHENTICATE,
};
use http::{HeaderMap, Method, Request, Response, StatusCode};
use http_body::Body as HttpBody;
use http_body_util::combinators::BoxBody;
#[cfg(feature = "websocket")]
use http_body_util::Empty;
use http_body_util::{BodyExt, Full, StreamBody};
use hyper::body::Frame;
#[cfg(feature = "websocket")]
use hyper::upgrade::OnUpgrade;
#[cfg(feature = "websocket")]
use hyper_util::rt::TokioIo;
#[cfg(feature = "otel")]
use opentelemetry::global;
#[cfg(feature = "otel")]
use opentelemetry::propagation::{Extractor, Injector};
#[cfg(feature = "otel")]
use opentelemetry::trace::TraceContextExt;
use runtime::{CacheLookup, CacheRequest, CacheResponse};
use std::collections::HashSet;
use std::convert::Infallible;
use tokio::sync::mpsc;
#[cfg(feature = "websocket")]
use tokio_tungstenite::tungstenite::handshake::server::create_response as create_ws_response;
#[cfg(feature = "websocket")]
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, Message, Role};
#[cfg(feature = "websocket")]
use tokio_tungstenite::WebSocketStream;
use tracing::Span;
#[cfg(feature = "otel")]
use tracing_opentelemetry::OpenTelemetrySpanExt;
use uuid::Uuid;

pub type ApiResult<T> = std::result::Result<T, ApiError>;
type BoxError = Box<dyn std::error::Error + Send + Sync>;
pub type ResponseBody = BoxBody<Bytes, BoxError>;

#[derive(Debug)]
pub struct ApiError(pub PlatformError);

const REQUEST_BODY_STREAM_CAPACITY: usize = 8;
const HEADER_CACHE: &str = "x-dd-cache";
const HEADER_CACHE_FALLBACK: &str = "x-dd-cache-fallback";
const HEADER_CACHE_BYPASS_STALE: &str = "x-dd-cache-bypass-stale";
#[cfg(feature = "otel")]
const HEADER_TRACE_ID: &str = "x-dd-trace-id";
#[cfg(feature = "websocket")]
const HEADER_WS_INTERNAL_PREFIX: &str = "x-dd-";
#[cfg(feature = "websocket")]
const HEADER_WS_SESSION: &str = "x-dd-ws-session";
#[cfg(feature = "websocket")]
const HEADER_WS_BINARY: &str = "x-dd-ws-binary";
#[cfg(feature = "websocket")]
const HEADER_WS_CLOSE_CODE: &str = "x-dd-ws-close-code";
#[cfg(feature = "websocket")]
const HEADER_WS_CLOSE_REASON: &str = "x-dd-ws-close-reason";

pub(crate) fn request_method_forbids_body(method: &Method) -> bool {
    *method == Method::GET || *method == Method::HEAD
}

pub(crate) fn request_content_length(
    headers: &HeaderMap,
) -> std::result::Result<Option<u64>, PlatformError> {
    headers
        .get(CONTENT_LENGTH)
        .map(|value| {
            value
                .to_str()
                .map_err(|error| {
                    PlatformError::bad_request(format!("invalid content-length header: {error}"))
                })
                .and_then(|value| {
                    value.trim().parse::<u64>().map_err(|error| {
                        PlatformError::bad_request(format!(
                            "invalid content-length header: {error}"
                        ))
                    })
                })
        })
        .transpose()
}

pub(crate) fn request_headers_declare_body(
    headers: &HeaderMap,
) -> std::result::Result<bool, PlatformError> {
    if request_content_length(headers)?.is_some_and(|value| value > 0) {
        return Ok(true);
    }
    Ok(headers
        .get(TRANSFER_ENCODING)
        .is_some_and(|value| !value.as_bytes().is_empty()))
}

pub(crate) fn validate_request_body_headers(
    method: &Method,
    headers: &HeaderMap,
    max_body_bytes: usize,
) -> std::result::Result<(), PlatformError> {
    if request_content_length(headers)?.is_some_and(|value| value > max_body_bytes as u64) {
        return Err(PlatformError::bad_request(format!(
            "request body too large (max {max_body_bytes} bytes)"
        )));
    }
    if request_method_forbids_body(method) && request_headers_declare_body(headers)? {
        return Err(request_body_not_supported(method));
    }
    Ok(())
}

pub(crate) fn request_body_not_supported(method: &Method) -> PlatformError {
    PlatformError::bad_request(format!(
        "{} request bodies are not supported",
        method.as_str()
    ))
}

#[cfg(all(test, not(feature = "websocket")))]
use self::invocation::build_public_request_url;
#[cfg(feature = "http3")]
pub use self::invocation::invoke_worker_public_h3;
#[cfg(any(feature = "websocket", test))]
use self::invocation::parse_invoke_request_uri;
#[cfg(test)]
use self::invocation::parse_worker_from_host;
#[cfg(feature = "websocket")]
pub(crate) use self::invocation::{
    build_public_request_url, ensure_public_worker, parse_public_worker_name_from_request,
};
pub use self::invocation::{invoke_worker_private, invoke_worker_public};
#[cfg(test)]
pub(crate) use self::routing::deploy_worker;
#[cfg(feature = "http3")]
pub use self::routing::handle_public_h3_request;
pub use self::routing::{handle_private_request, handle_public_request};
#[cfg(feature = "websocket")]
use self::util::empty_body;
use self::util::inject_current_trace_context;
pub(crate) use self::util::{annotate_response_with_trace_id, full_body};
use self::util::{
    bearer_token_from_headers, json_response, private_auth_response, private_request_is_authorized,
    private_route_requires_auth, public_route_is_reserved, read_json_body, respond,
    set_span_parent_from_http_headers, validate_deploy_bindings, validate_internal_config,
};
#[cfg(feature = "websocket")]
pub(crate) use self::websocket::{
    handle_websocket_session, open_transport_session_from_parts, open_websocket_session_from_parts,
    sanitize_websocket_handshake_headers,
};
use self::websocket::{
    invoke_worker_websocket_private, invoke_worker_websocket_public, is_websocket_upgrade,
    prepare_websocket_upgrade, PreparedWebSocketUpgrade,
};
#[cfg(test)]
mod tests {
    use super::util::private_token_matches;
    use super::{
        build_public_request_url, parse_invoke_request_uri, parse_worker_from_host, read_json_body,
        validate_deploy_bindings, validate_internal_config,
    };
    use bytes::Bytes;
    use common::{DeployBinding, DeployInternalConfig, DeployTraceDestination, ErrorKind};
    use futures_util::stream;
    use http::{HeaderMap, Request};
    use http_body_util::StreamBody;
    use hyper::body::Frame;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    #[tokio::test]
    async fn read_json_body_rejects_oversized_stream_before_polling_rest() {
        let polled_second_chunk = Arc::new(AtomicBool::new(false));
        let stream_flag = polled_second_chunk.clone();
        let body_stream = stream::unfold(0, move |index| {
            let stream_flag = stream_flag.clone();
            async move {
                match index {
                    0 => Some((
                        Ok::<_, std::convert::Infallible>(Frame::data(Bytes::from_static(
                            br#"{"name":"#,
                        ))),
                        1,
                    )),
                    1 => {
                        stream_flag.store(true, Ordering::SeqCst);
                        Some((
                            Ok::<_, std::convert::Infallible>(Frame::data(Bytes::from_static(
                                br#""oversized"}"#,
                            ))),
                            2,
                        ))
                    }
                    _ => None,
                }
            }
        });
        let error = read_json_body::<serde_json::Value, _>(StreamBody::new(body_stream), 4)
            .await
            .expect_err("oversized body should fail");

        assert_eq!(error.kind(), ErrorKind::BadRequest);
        assert_eq!(error.to_string(), "request body too large (max 4 bytes)");
        assert!(!polled_second_chunk.load(Ordering::SeqCst));
    }

    #[test]
    fn parse_invoke_path_and_query() {
        let uri: http::Uri = "/v1/invoke/hello/api/v1?x=1&y=2".parse().expect("uri");
        let (worker, url) = parse_invoke_request_uri(uri.path(), uri.path_and_query()).expect("ok");
        assert_eq!(worker, "hello");
        assert_eq!(url, "http://worker/api/v1?x=1&y=2");
    }

    #[test]
    fn parse_invoke_root_path() {
        let uri: http::Uri = "/v1/invoke/hello".parse().expect("uri");
        let (worker, url) = parse_invoke_request_uri(uri.path(), uri.path_and_query()).expect("ok");
        assert_eq!(worker, "hello");
        assert_eq!(url, "http://worker/");
    }

    #[test]
    fn build_public_request_url_ignores_spoofed_forwarded_host_and_proto() {
        let uri: http::Uri = "/rooms/test?x=1".parse().expect("uri");
        let request = Request::builder()
            .uri(uri)
            .header("host", "chat.example.com")
            .header("x-forwarded-host", "chat.wdyt.chat")
            .header("x-forwarded-proto", "https")
            .body(())
            .expect("request");
        let url = build_public_request_url(request.headers(), request.uri()).expect("url");
        assert_eq!(url, "https://chat.example.com/rooms/test?x=1");
    }

    #[test]
    fn build_public_request_url_ignores_spoofed_forwarded_proto() {
        let uri: http::Uri = "/ws".parse().expect("uri");
        let request = Request::builder()
            .uri(uri)
            .header("host", "chat.example.com")
            .header("x-forwarded-proto", "wss")
            .body(())
            .expect("request");
        let url = build_public_request_url(request.headers(), request.uri()).expect("url");
        assert_eq!(url, "https://chat.example.com/ws");
    }

    #[test]
    fn duplicate_bindings_are_rejected() {
        let bindings = vec![
            DeployBinding::Kv {
                binding: "MY_KV".to_string(),
            },
            DeployBinding::Kv {
                binding: "MY_KV".to_string(),
            },
        ];
        assert!(validate_deploy_bindings(&bindings).is_err());
    }

    #[test]
    fn kv_and_memory_binding_name_collision_is_rejected() {
        let bindings = vec![
            DeployBinding::Kv {
                binding: "SHARED".to_string(),
            },
            DeployBinding::Memory {
                binding: "SHARED".to_string(),
            },
        ];
        assert!(validate_deploy_bindings(&bindings).is_err());
    }

    #[test]
    fn empty_memory_binding_name_is_rejected() {
        let bindings = vec![DeployBinding::Memory {
            binding: String::new(),
        }];
        assert!(validate_deploy_bindings(&bindings).is_err());
    }

    #[test]
    fn dynamic_binding_name_collision_is_rejected() {
        let bindings = vec![
            DeployBinding::Kv {
                binding: "SHARED".to_string(),
            },
            DeployBinding::Dynamic {
                binding: "SHARED".to_string(),
            },
        ];
        assert!(validate_deploy_bindings(&bindings).is_err());
    }

    #[test]
    fn accepts_no_trace_config() {
        let config = DeployInternalConfig::default();
        assert!(validate_internal_config(&config).is_ok());
    }

    #[test]
    fn rejects_empty_trace_worker() {
        let config = DeployInternalConfig {
            trace: Some(DeployTraceDestination {
                worker: " ".to_string(),
                path: "/ingest".to_string(),
            }),
        };
        assert!(validate_internal_config(&config).is_err());
    }

    #[test]
    fn rejects_empty_trace_path() {
        let config = DeployInternalConfig {
            trace: Some(DeployTraceDestination {
                worker: "worker".to_string(),
                path: " ".to_string(),
            }),
        };
        assert!(validate_internal_config(&config).is_err());
    }

    #[test]
    fn rejects_trace_path_without_leading_slash() {
        let config = DeployInternalConfig {
            trace: Some(DeployTraceDestination {
                worker: "worker".to_string(),
                path: "ingest".to_string(),
            }),
        };
        assert!(validate_internal_config(&config).is_err());
    }

    #[test]
    fn accepts_valid_trace_config() {
        let config = DeployInternalConfig {
            trace: Some(DeployTraceDestination {
                worker: "worker".to_string(),
                path: "/ingest".to_string(),
            }),
        };
        assert!(validate_internal_config(&config).is_ok());
    }

    #[test]
    fn private_token_matching_rejects_non_exact_tokens() {
        assert!(private_token_matches("secret-token", " secret-token "));
        assert!(!private_token_matches("secret-token", "secret"));
        assert!(!private_token_matches("secret-token", "secret-token-extra"));
        assert!(!private_token_matches("secret-token", "different-token"));
    }

    #[test]
    fn worker_name_is_extracted_from_subdomain_host() {
        let worker = parse_worker_from_host("echo.example.com:443", "example.com").expect("ok");
        assert_eq!(worker, "echo");
    }

    #[test]
    fn worker_name_is_extracted_with_case_insensitive_host() {
        let worker = parse_worker_from_host("Echo.Example.Com", "EXAMPLE.COM").expect("ok");
        assert_eq!(worker, "echo");
    }

    #[test]
    fn apex_host_is_rejected() {
        let error = parse_worker_from_host("example.com", "example.com").expect_err("error");
        assert_eq!(error.kind(), common::ErrorKind::NotFound);
    }

    #[test]
    fn foreign_host_is_rejected() {
        let error = parse_worker_from_host("echo.other.com", "example.com").expect_err("error");
        assert_eq!(error.kind(), ErrorKind::NotFound);
    }

    #[test]
    fn nested_subdomain_host_is_rejected() {
        let error =
            parse_worker_from_host("echo.preview.example.com", "example.com").expect_err("error");
        assert_eq!(error.kind(), ErrorKind::NotFound);
    }

    #[test]
    fn websocket_upgrade_checks_required_headers() {
        let mut headers = HeaderMap::new();
        headers.insert("connection", "Upgrade".parse().expect("header"));
        headers.insert("upgrade", "websocket".parse().expect("header"));
        assert!(super::is_websocket_upgrade(&headers));

        let mut non_ws = HeaderMap::new();
        non_ws.insert("connection", "keep-alive".parse().expect("header"));
        non_ws.insert("upgrade", "websocket".parse().expect("header"));
        assert!(!super::is_websocket_upgrade(&non_ws));
    }
}
