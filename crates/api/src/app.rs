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

#[cfg(test)]
mod tests {
    use crate::test_support::{
        create_h3_test_state, deploy_public_worker, deploy_public_worker_with_config, h3_test_lock,
        start_h3_harness, RawQuicheH3Client, WebSocketFrame,
    };
    use bytes::{Buf, Bytes};
    use common::{DeployBinding, DeployConfig};
    use futures_util::{SinkExt, StreamExt};
    use http::StatusCode;
    use quiche::h3::{Header, NameValue};
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::time::timeout;
    use tokio_tungstenite::{connect_async, tungstenite::Message};

    #[tokio::test]
    async fn public_h3_request_body_streams_before_finish() {
        let _guard = h3_test_lock().lock().await;
        let (state, cert, public_addr, private_addr, _store_dir) = create_h3_test_state().await;
        deploy_public_worker(
            &state,
            "echo",
            r#"
                export default {
                  async fetch(request) {
                    const reader = request.body.getReader();
                    const first = await reader.read();
                    const chunk = first.value ?? new Uint8Array();
                    return new Response(new TextDecoder().decode(chunk));
                  }
                };
                "#,
        )
        .await;

        let (server, mut driver, mut send_request) =
            start_h3_harness(state.clone(), cert, public_addr, private_addr).await;

        let request = http::Request::builder()
            .method("POST")
            .uri(format!(
                "https://echo.localhost:{}/stream",
                public_addr.port()
            ))
            .body(())
            .expect("request");
        let mut stream = send_request
            .send_request(request)
            .await
            .expect("send request");
        stream
            .send_data(Bytes::from_static(b"hello"))
            .await
            .expect("send first chunk");

        let response = timeout(Duration::from_millis(500), stream.recv_response())
            .await
            .expect("response should arrive before request finish")
            .expect("recv response");
        assert_eq!(response.status(), http::StatusCode::OK);
        assert_eq!(response.version(), http::Version::HTTP_3);

        let mut body = Vec::new();
        while let Some(mut chunk) = stream.recv_data().await.expect("recv response data") {
            body.extend_from_slice(&chunk.copy_to_bytes(chunk.remaining()));
        }
        assert_eq!(body, b"hello");

        stream.finish().await.expect("finish request");
        drop(send_request);
        let _ = driver.wait_idle().await;
        server.abort();
    }

    #[tokio::test]
    #[ignore = "raw quiche h3 websocket harness is unstable in-process; black-box coverage lives in tests/public_h3_websocket_blackbox.rs"]
    async fn public_h3_websocket_connect_echoes_text_and_binary() {
        let _guard = h3_test_lock().lock().await;
        let (state, _cert, public_addr, private_addr, _store_dir) = create_h3_test_state().await;
        deploy_public_worker_with_config(
            &state,
            "echo",
            r#"
                export default {
                  async fetch(request, env) {
                    const actor = env.CHAT.get(env.CHAT.idFromName("global"));
                    return actor.fetch(request);
                  }
                };

                export class ChatActor {
                  constructor(state) {
                    this.state = state;
                  }

                  async fetch(request) {
                    const { handle, response } = await this.state.sockets.accept(request);
                    const ws = new WebSocket(handle);
                    ws.addEventListener("message", async (event) => {
                      if (event.data === "bye") {
                        await ws.close(4001, "done");
                        return;
                      }
                      if (ArrayBuffer.isView(event.data) || event.data instanceof ArrayBuffer) {
                        await ws.send(event.data, "binary");
                        return;
                      }
                      await ws.send(`echo:${event.data}`);
                    });
                    return response;
                  }
                }
            "#,
            DeployConfig {
                bindings: vec![DeployBinding::Actor {
                    binding: "CHAT".to_string(),
                    class: "ChatActor".to_string(),
                }],
                ..Default::default()
            },
        )
        .await;

        let server = tokio::spawn({
            let state = state.clone();
            async move { super::serve(public_addr, private_addr, state).await }
        });
        tokio::time::sleep(Duration::from_millis(250)).await;

        let mut client = RawQuicheH3Client::connect(public_addr).await;
        let stream_id = client
            .send_request(
                vec![
                    Header::new(b":method", b"CONNECT"),
                    Header::new(b":scheme", b"https"),
                    Header::new(
                        b":authority",
                        format!("echo.localhost:{}", public_addr.port()).as_bytes(),
                    ),
                    Header::new(b":path", b"/ws"),
                    Header::new(b":protocol", b"websocket"),
                    Header::new(b"sec-websocket-version", b"13"),
                    Header::new(b"sec-websocket-key", b"dGhlIHNhbXBsZSBub25jZQ=="),
                ],
                false,
            )
            .await;
        let response = client.recv_response_headers(stream_id).await;
        assert_eq!(quiche_header_value(&response, ":status"), Some("200"));

        client
            .send_websocket_frame(stream_id, WebSocketFrame::Text("hello".to_string()))
            .await;
        assert_eq!(
            client.recv_websocket_frame(stream_id).await,
            WebSocketFrame::Text("echo:hello".to_string()),
        );

        client
            .send_websocket_frame(stream_id, WebSocketFrame::Binary(vec![1, 2, 3, 4]))
            .await;
        assert_eq!(
            client.recv_websocket_frame(stream_id).await,
            WebSocketFrame::Binary(vec![1, 2, 3, 4]),
        );

        client
            .send_websocket_frame(stream_id, WebSocketFrame::Text("bye".to_string()))
            .await;
        assert_eq!(
            client.recv_websocket_frame(stream_id).await,
            WebSocketFrame::Close {
                code: 4001,
                reason: "done".to_string(),
            },
        );

        timeout(Duration::from_secs(2), async {
            loop {
                if state.websocket_sessions.lock().await.is_empty() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("websocket session cleanup timeout");

        server.abort();
    }

    #[tokio::test]
    #[ignore = "raw quiche h3 websocket harness is unstable in-process; keep as a local debug repro only"]
    async fn public_h3_websocket_rejects_missing_key() {
        let _guard = h3_test_lock().lock().await;
        let (state, _cert, public_addr, private_addr, _store_dir) = create_h3_test_state().await;
        deploy_public_worker_with_config(
            &state,
            "echo",
            r#"
                export default {
                  async fetch(request, env) {
                    const actor = env.CHAT.get(env.CHAT.idFromName("global"));
                    return actor.fetch(request);
                  }
                };

                export class ChatActor {
                  async fetch(request) {
                    return new Response("unexpected", { status: 500 });
                  }
                }
            "#,
            DeployConfig {
                bindings: vec![DeployBinding::Actor {
                    binding: "CHAT".to_string(),
                    class: "ChatActor".to_string(),
                }],
                ..Default::default()
            },
        )
        .await;

        let server = tokio::spawn({
            let state = state.clone();
            async move { super::serve(public_addr, private_addr, state).await }
        });
        tokio::time::sleep(Duration::from_millis(250)).await;

        let mut client = RawQuicheH3Client::connect(public_addr).await;
        let stream_id = client
            .send_request(
                vec![
                    Header::new(b":method", b"CONNECT"),
                    Header::new(b":scheme", b"https"),
                    Header::new(
                        b":authority",
                        format!("echo.localhost:{}", public_addr.port()).as_bytes(),
                    ),
                    Header::new(b":path", b"/ws"),
                    Header::new(b":protocol", b"websocket"),
                    Header::new(b"sec-websocket-version", b"13"),
                ],
                false,
            )
            .await;
        let response = client.recv_response_headers(stream_id).await;
        assert_eq!(quiche_header_value(&response, ":status"), Some("400"));

        let body = client.recv_body(stream_id).await;
        assert!(
            String::from_utf8_lossy(&body).contains("sec-websocket-key"),
            "unexpected invalid websocket body: {}",
            String::from_utf8_lossy(&body)
        );

        server.abort();
    }

    #[tokio::test]
    async fn public_tcp_websocket_actor_echoes_text_and_binary() {
        let _guard = h3_test_lock().lock().await;
        let (state, _cert, public_addr, private_addr, _store_dir) = create_h3_test_state().await;
        deploy_public_worker_with_config(
            &state,
            "echo",
            r#"
                export default {
                  async fetch(request, env) {
                    const actor = env.CHAT.get(env.CHAT.idFromName("global"));
                    return actor.fetch(request);
                  }
                };

                export class ChatActor {
                  async fetch(request) {
                    const { handle, response } = await this.state.sockets.accept(request);
                    const ws = new WebSocket(handle);
                    ws.addEventListener("message", async (event) => {
                      if (ArrayBuffer.isView(event.data) || event.data instanceof ArrayBuffer) {
                        await ws.send(event.data, "binary");
                        return;
                      }
                      await ws.send(`echo:${event.data}`);
                    });
                    return response;
                  }
                }
            "#,
            DeployConfig {
                bindings: vec![DeployBinding::Actor {
                    binding: "CHAT".to_string(),
                    class: "ChatActor".to_string(),
                }],
                ..Default::default()
            },
        )
        .await;

        let server = tokio::spawn({
            let state = state.clone();
            async move { super::serve(public_addr, private_addr, state).await }
        });
        tokio::time::sleep(Duration::from_millis(250)).await;

        let (mut websocket, _response) = timeout(
            Duration::from_secs(2),
            connect_async(format!("ws://echo.localhost:{}/ws", public_addr.port())),
        )
        .await
        .expect("tcp websocket connect timeout")
        .expect("connect public tcp websocket");

        websocket
            .send(Message::Text("hello".into()))
            .await
            .expect("send tcp text");
        let text = timeout(Duration::from_secs(2), websocket.next())
            .await
            .expect("tcp text timeout")
            .expect("tcp text stream item")
            .expect("tcp text message");
        assert_eq!(text.into_text().expect("tcp text"), "echo:hello");

        websocket
            .send(Message::Binary(vec![1, 2, 3].into()))
            .await
            .expect("send tcp binary");
        let binary = timeout(Duration::from_secs(2), websocket.next())
            .await
            .expect("tcp binary timeout")
            .expect("tcp binary stream item")
            .expect("tcp binary message");
        assert_eq!(binary.into_data().as_ref(), &[1, 2, 3]);

        let _ = websocket.close(None).await;
        server.abort();
    }

    #[tokio::test]
    async fn public_h3_response_body_streams_incrementally() {
        let _guard = h3_test_lock().lock().await;
        let (state, cert, public_addr, private_addr, _store_dir) = create_h3_test_state().await;
        deploy_public_worker(
            &state,
            "streamer",
            r#"
                export default {
                  async fetch() {
                    let sentFirst = false;
                    return new Response(new ReadableStream({
                      async pull(controller) {
                        if (!sentFirst) {
                          sentFirst = true;
                          controller.enqueue(new TextEncoder().encode("first"));
                          return;
                        }
                        await Deno.core.ops.op_sleep(150);
                        if (sentFirst) {
                          controller.enqueue(new TextEncoder().encode("-second"));
                          controller.close();
                        }
                      }
                    }));
                  }
                };
                "#,
        )
        .await;

        let (server, mut driver, mut send_request) =
            start_h3_harness(state.clone(), cert, public_addr, private_addr).await;

        let request = http::Request::builder()
            .method("GET")
            .uri(format!(
                "https://streamer.localhost:{}/stream",
                public_addr.port()
            ))
            .body(())
            .expect("request");
        let mut stream = send_request
            .send_request(request)
            .await
            .expect("send request");
        stream.finish().await.expect("finish request");

        let response = timeout(Duration::from_millis(200), stream.recv_response())
            .await
            .expect("response head timeout")
            .expect("recv response");
        if response.status() != http::StatusCode::OK {
            let mut error_body = Vec::new();
            while let Some(mut chunk) = stream.recv_data().await.expect("recv error body") {
                error_body.extend_from_slice(&chunk.copy_to_bytes(chunk.remaining()));
            }
            panic!(
                "unexpected h3 response status {} body {}",
                response.status(),
                String::from_utf8_lossy(&error_body)
            );
        }
        assert_eq!(response.version(), http::Version::HTTP_3);

        let mut first = timeout(Duration::from_millis(75), stream.recv_data())
            .await
            .expect("first body chunk should arrive before delayed second chunk")
            .expect("recv first chunk")
            .expect("first chunk");
        let mut body = first.copy_to_bytes(first.remaining()).to_vec();
        assert!(
            body.starts_with(b"first"),
            "stream should start with first chunk bytes"
        );

        while let Some(mut chunk) = stream.recv_data().await.expect("recv remaining body") {
            body.extend_from_slice(&chunk.copy_to_bytes(chunk.remaining()));
        }
        assert_eq!(body, b"first-second");

        drop(send_request);
        let _ = driver.wait_idle().await;
        server.abort();
    }

    #[tokio::test]
    async fn public_h3_blocks_control_plane_routes() {
        let _guard = h3_test_lock().lock().await;
        let (state, cert, public_addr, private_addr, _store_dir) = create_h3_test_state().await;
        let (server, mut driver, mut send_request) =
            start_h3_harness(state.clone(), cert, public_addr, private_addr).await;

        let request = http::Request::builder()
            .method("POST")
            .uri(format!(
                "https://localhost:{}/v1/deploy",
                public_addr.port()
            ))
            .body(())
            .expect("request");
        let mut stream = send_request
            .send_request(request)
            .await
            .expect("send request");
        stream.finish().await.expect("finish request");

        let response = timeout(Duration::from_millis(200), stream.recv_response())
            .await
            .expect("response timeout")
            .expect("recv response");
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(response.version(), http::Version::HTTP_3);

        drop(send_request);
        let _ = driver.wait_idle().await;
        server.abort();
    }

    #[tokio::test]
    async fn public_h3_rejects_connect_tunnels() {
        let _guard = h3_test_lock().lock().await;
        let (state, cert, public_addr, private_addr, _store_dir) = create_h3_test_state().await;
        let (server, mut driver, mut send_request) =
            start_h3_harness(state.clone(), cert, public_addr, private_addr).await;

        let request = http::Request::builder()
            .method("CONNECT")
            .uri(format!(
                "https://echo.localhost:{}/socket",
                public_addr.port()
            ))
            .body(())
            .expect("request");
        let mut stream = send_request
            .send_request(request)
            .await
            .expect("send request");
        stream.finish().await.expect("finish request");

        let response = timeout(Duration::from_millis(200), stream.recv_response())
            .await
            .expect("response timeout")
            .expect("recv response");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(response.version(), http::Version::HTTP_3);

        let mut body = Vec::new();
        while let Some(mut chunk) = stream.recv_data().await.expect("recv body") {
            body.extend_from_slice(&chunk.copy_to_bytes(chunk.remaining()));
        }
        assert!(
            String::from_utf8_lossy(&body).contains("CONNECT is unsupported over http/3"),
            "unexpected error body: {}",
            String::from_utf8_lossy(&body)
        );

        drop(send_request);
        let _ = driver.wait_idle().await;
        server.abort();
    }

    #[tokio::test]
    async fn public_tcp_advertises_h3_with_alt_svc() {
        let _guard = h3_test_lock().lock().await;
        let (state, _cert, public_addr, private_addr, _store_dir) = create_h3_test_state().await;
        deploy_public_worker(
            &state,
            "echo",
            r#"
                export default {
                  async fetch(request) {
                    return new Response(request.url);
                  }
                };
                "#,
        )
        .await;

        let server = tokio::spawn({
            let state = state.clone();
            async move { super::serve(public_addr, private_addr, state).await }
        });
        tokio::time::sleep(Duration::from_millis(250)).await;

        let mut socket = tokio::net::TcpStream::connect(public_addr)
            .await
            .expect("connect public tcp listener");
        let request = format!(
            "GET / HTTP/1.1\r\nHost: echo.localhost:{}\r\nConnection: close\r\n\r\n",
            public_addr.port()
        );
        socket
            .write_all(request.as_bytes())
            .await
            .expect("write request");

        let head = read_http_response_head(&mut socket).await;
        assert!(
            head.starts_with("HTTP/1.1 200") || head.starts_with("HTTP/1.1 404"),
            "unexpected response head: {head}"
        );
        assert!(
            head.to_ascii_lowercase().contains(&format!(
                "alt-svc: h3=\":{}\"; ma=86400",
                public_addr.port()
            )),
            "missing Alt-Svc header in response head: {head}"
        );

        server.abort();
    }
    async fn read_http_response_head(socket: &mut tokio::net::TcpStream) -> String {
        let mut bytes = Vec::new();
        let mut buffer = [0_u8; 1024];
        loop {
            let read = timeout(Duration::from_millis(500), socket.read(&mut buffer))
                .await
                .expect("response head timeout")
                .expect("read response head");
            if read == 0 {
                break;
            }
            bytes.extend_from_slice(&buffer[..read]);
            if bytes.windows(4).any(|window| window == b"\r\n\r\n") {
                break;
            }
        }
        String::from_utf8(bytes).expect("response head utf8")
    }

    fn quiche_header_value<'a>(headers: &'a [Header], name: &str) -> Option<&'a str> {
        headers.iter().find_map(|header| {
            if header.name() == name.as_bytes() {
                std::str::from_utf8(header.value()).ok()
            } else {
                None
            }
        })
    }
}
