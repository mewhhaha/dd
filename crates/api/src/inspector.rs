//! The development runtime's DevTools endpoints: Chrome's target discovery
//! (`/json/version`, `/json/list`) and, per worker isolate, a WebSocket that
//! bridges the Chrome DevTools Protocol to the isolate's inspector.

use bytes::Bytes;
use common::{PlatformError, Result};
use futures_util::{SinkExt, StreamExt};
use http::header::{
    CONNECTION, CONTENT_TYPE, HOST, SEC_WEBSOCKET_ACCEPT, SEC_WEBSOCKET_KEY, UPGRADE,
};
use http::{HeaderMap, Method, Request, Response, StatusCode};
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use runtime::{InspectorSession, RuntimeService};
use serde_json::json;
use std::convert::Infallible;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, ToSocketAddrs};
use tokio::net::TcpListener;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::handshake::derive_accept_key;
use tokio_tungstenite::tungstenite::protocol::{Message, Role};
use uuid::Uuid;

/// Where `--inspect` listens unless told otherwise, as Node does.
pub const DEFAULT_INSPECT_ADDRESS: SocketAddr =
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9229);

/// Parses an `--inspect=` value: `port`, `host`, or `host:port` (empty for
/// the default). A host name must resolve to a loopback address; reaching
/// the inspector from elsewhere takes an explicit IP address.
pub fn inspect_address(value: &str) -> std::result::Result<SocketAddr, String> {
    let value = value.trim();
    if value.is_empty() {
        return Ok(DEFAULT_INSPECT_ADDRESS);
    }
    if let Ok(port) = value.parse::<u16>() {
        return Ok(SocketAddr::new(DEFAULT_INSPECT_ADDRESS.ip(), port));
    }
    if let Ok(address) = value.parse::<SocketAddr>() {
        return Ok(address);
    }
    if let Ok(ip) = value.trim_matches(['[', ']']).parse::<IpAddr>() {
        return Ok(SocketAddr::new(ip, DEFAULT_INSPECT_ADDRESS.port()));
    }
    let (host, port) = match value.rsplit_once(':') {
        Some((host, port)) => (
            host,
            port.parse::<u16>()
                .map_err(|_| format!("invalid inspector port in {value:?}"))?,
        ),
        None => (value, DEFAULT_INSPECT_ADDRESS.port()),
    };
    let addresses = (host, port)
        .to_socket_addrs()
        .map_err(|error| format!("cannot resolve inspector host {host:?}: {error}"))?
        .collect::<Vec<_>>();
    if addresses.is_empty() || addresses.iter().any(|address| !address.ip().is_loopback()) {
        return Err(format!(
            "refusing to serve the inspector on {host:?}, which is not only a loopback address; pass an explicit IP address to listen beyond this machine"
        ));
    }
    Ok(addresses
        .iter()
        .find(|address| address.is_ipv4())
        .copied()
        .unwrap_or(addresses[0]))
}

/// Serves the DevTools endpoints for `runtime`'s worker isolates on
/// `listener` until it fails.
pub async fn serve_inspector(listener: TcpListener, runtime: RuntimeService) -> Result<()> {
    let bound = listener
        .local_addr()
        .map_err(|error| PlatformError::internal(format!("inspector listener address: {error}")))?;
    loop {
        let (stream, _) = listener
            .accept()
            .await
            .map_err(|error| PlatformError::internal(format!("inspector accept: {error}")))?;
        let runtime = runtime.clone();
        tokio::spawn(async move {
            let service = service_fn(move |request| {
                let runtime = runtime.clone();
                async move { Ok::<_, Infallible>(respond(request, &runtime, bound)) }
            });
            let _ = hyper::server::conn::http1::Builder::new()
                .serve_connection(TokioIo::new(stream), service)
                .with_upgrades()
                .await;
        });
    }
}

type Body = Full<Bytes>;

fn respond(
    mut request: Request<Incoming>,
    runtime: &RuntimeService,
    bound: SocketAddr,
) -> Response<Body> {
    // A web page can point a hostname of its own at this port (DNS
    // rebinding); only requests naming an IP address or localhost get in.
    let Some(host) = request_host(request.headers(), bound) else {
        return text(
            StatusCode::FORBIDDEN,
            "Host must be an IP address or localhost",
        );
    };
    if request.method() != Method::GET {
        return text(StatusCode::METHOD_NOT_ALLOWED, "only GET");
    }
    match request.uri().path() {
        "/json/version" => json_response(json!({
            "Browser": concat!("dd/", env!("CARGO_PKG_VERSION")),
            "Protocol-Version": "1.3",
        })),
        "/json" | "/json/list" => json_response(targets(runtime, &host)),
        path => {
            let target = Uuid::parse_str(path.trim_start_matches('/'))
                .ok()
                .and_then(|id| {
                    runtime
                        .inspector_targets()
                        .into_iter()
                        .find(|target| target.id == id)
                });
            let Some(target) = target else {
                return text(StatusCode::NOT_FOUND, "no such inspector target");
            };
            let Some(accept) = websocket_accept(request.headers()) else {
                return text(StatusCode::BAD_REQUEST, "expected a WebSocket upgrade");
            };
            let upgrade = hyper::upgrade::on(&mut request);
            let session = target.connect();
            tokio::spawn(async move {
                if let Ok(upgraded) = upgrade.await {
                    let socket = WebSocketStream::from_raw_socket(
                        TokioIo::new(upgraded),
                        Role::Server,
                        None,
                    )
                    .await;
                    bridge(socket, session).await;
                }
            });
            Response::builder()
                .status(StatusCode::SWITCHING_PROTOCOLS)
                .header(UPGRADE, "websocket")
                .header(CONNECTION, "Upgrade")
                .header(SEC_WEBSOCKET_ACCEPT, accept)
                .body(Body::default())
                .expect("valid upgrade response")
        }
    }
}

/// The targets as Chrome's `/json/list` describes them; `type: "node"` lets
/// chrome://inspect and its dedicated Node DevTools window pick them up.
fn targets(runtime: &RuntimeService, host: &str) -> serde_json::Value {
    let mut targets = runtime.inspector_targets();
    targets.sort_by(|left, right| {
        (&left.worker, left.generation, left.isolate_id).cmp(&(
            &right.worker,
            right.generation,
            right.isolate_id,
        ))
    });
    targets
        .iter()
        .map(|target| {
            let socket = format!("{host}/{}", target.id);
            json!({
                "description": "dd worker isolate",
                "devtoolsFrontendUrl": format!("devtools://devtools/bundled/js_app.html?experiments=true&v8only=true&ws={socket}"),
                "devtoolsFrontendUrlCompat": format!("devtools://devtools/bundled/inspector.html?experiments=true&v8only=true&ws={socket}"),
                "faviconUrl": "",
                "id": target.id.to_string(),
                "title": format!("{} (isolate {})", target.worker, target.isolate_id),
                "type": "node",
                "url": format!("dd://{}/", target.worker),
                "webSocketDebuggerUrl": format!("ws://{socket}"),
            })
        })
        .collect()
}

/// Passes CDP messages both ways until either side closes.
async fn bridge(
    socket: WebSocketStream<TokioIo<hyper::upgrade::Upgraded>>,
    mut session: InspectorSession,
) {
    let (mut sink, mut stream) = socket.split();
    loop {
        tokio::select! {
            incoming = stream.next() => match incoming {
                Some(Ok(Message::Text(text))) => session.send(text.as_str()),
                Some(Ok(Message::Binary(bytes))) => match std::str::from_utf8(&bytes) {
                    Ok(text) => session.send(text),
                    Err(_) => break,
                },
                Some(Ok(Message::Close(_)) | Err(_)) | None => break,
                Some(Ok(_)) => {}
            },
            outgoing = session.recv() => match outgoing {
                Some(message) => {
                    if sink.send(Message::Text(message.into())).await.is_err() {
                        break;
                    }
                }
                // The isolate is gone.
                None => {
                    let _ = sink.send(Message::Close(None)).await;
                    break;
                }
            },
        }
    }
}

fn request_host(headers: &HeaderMap, bound: SocketAddr) -> Option<String> {
    let Some(host) = headers.get(HOST) else {
        return Some(bound.to_string());
    };
    let host = host.to_str().ok()?;
    let name = match host.strip_prefix('[') {
        Some(rest) => rest.split_once(']')?.0,
        None => host.rsplit_once(':').map_or(host, |(name, _)| name),
    };
    (name.eq_ignore_ascii_case("localhost") || name.parse::<IpAddr>().is_ok())
        .then(|| host.to_string())
}

fn websocket_accept(headers: &HeaderMap) -> Option<String> {
    let upgrade = headers.get(UPGRADE)?.to_str().ok()?;
    if !upgrade.trim().eq_ignore_ascii_case("websocket") {
        return None;
    }
    Some(derive_accept_key(
        headers.get(SEC_WEBSOCKET_KEY)?.as_bytes(),
    ))
}

fn json_response(value: serde_json::Value) -> Response<Body> {
    Response::builder()
        .header(CONTENT_TYPE, "application/json; charset=UTF-8")
        .body(Body::from(value.to_string()))
        .expect("valid JSON response")
}

fn text(status: StatusCode, message: &'static str) -> Response<Body> {
    Response::builder()
        .status(status)
        .header(CONTENT_TYPE, "text/plain; charset=UTF-8")
        .body(Body::from(message))
        .expect("valid text response")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inspect_addresses_default_to_loopback() {
        assert_eq!(inspect_address(""), Ok(DEFAULT_INSPECT_ADDRESS));
        assert_eq!(
            inspect_address("9339"),
            Ok("127.0.0.1:9339".parse().unwrap())
        );
        assert_eq!(
            inspect_address("localhost:9339").map(|address| address.ip().is_loopback()),
            Ok(true)
        );
        assert_eq!(
            inspect_address("[::1]:9339"),
            Ok("[::1]:9339".parse().unwrap())
        );
        // An explicit address may reach beyond this machine.
        assert_eq!(
            inspect_address("0.0.0.0:9229"),
            Ok("0.0.0.0:9229".parse().unwrap())
        );
        assert_eq!(
            inspect_address("0.0.0.0"),
            Ok("0.0.0.0:9229".parse().unwrap())
        );
        assert!(inspect_address("localhost:port").is_err());
    }

    #[test]
    fn only_ip_and_localhost_hosts_reach_the_endpoints() {
        let bound = DEFAULT_INSPECT_ADDRESS;
        let host = |value: &str| {
            let mut headers = HeaderMap::new();
            headers.insert(HOST, value.parse().unwrap());
            request_host(&headers, bound)
        };
        assert_eq!(host("127.0.0.1:9229").as_deref(), Some("127.0.0.1:9229"));
        assert_eq!(host("localhost:9229").as_deref(), Some("localhost:9229"));
        assert_eq!(host("[::1]:9229").as_deref(), Some("[::1]:9229"));
        assert_eq!(host("evil.example:9229"), None);
        assert_eq!(
            request_host(&HeaderMap::new(), bound).as_deref(),
            Some("127.0.0.1:9229")
        );
    }

    const WORKER: &str = r#"export default {
  async fetch() {
    const answer = 40 + 2;
    return new Response(String(answer));
  },
};
"#;

    fn invocation(request_id: &str) -> common::WorkerInvocation {
        common::WorkerInvocation {
            method: "GET".into(),
            url: "http://worker/".into(),
            headers: Vec::new(),
            body: Vec::new(),
            request_id: request_id.into(),
        }
    }

    async fn get(address: SocketAddr, path: &str, host: &str) -> (u16, String) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        stream
            .write_all(
                format!("GET {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n")
                    .as_bytes(),
            )
            .await
            .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        let (head, body) = response.split_once("\r\n\r\n").unwrap();
        let status = head.split(' ').nth(1).unwrap().parse().unwrap();
        (status, body.to_string())
    }

    type Client = WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

    async fn next(
        client: &mut Client,
        matches: impl Fn(&serde_json::Value) -> bool,
    ) -> serde_json::Value {
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                let Message::Text(text) = client.next().await.unwrap().unwrap() else {
                    continue;
                };
                let message: serde_json::Value = serde_json::from_str(text.as_str()).unwrap();
                if matches(&message) {
                    return message;
                }
            }
        })
        .await
        .expect("expected protocol message")
    }

    async fn call(
        client: &mut Client,
        id: u32,
        method: &str,
        params: serde_json::Value,
    ) -> serde_json::Value {
        let message = json!({ "id": id, "method": method, "params": params });
        client
            .send(Message::Text(message.to_string().into()))
            .await
            .unwrap();
        next(client, |message| message["id"] == id).await
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn devtools_pauses_a_request_at_a_breakpoint() {
        let store_dir = std::env::temp_dir().join(format!("dd-inspector-{}", Uuid::new_v4()));
        let runtime = RuntimeService::start_with_service_config(runtime::RuntimeServiceConfig {
            runtime: runtime::RuntimeConfig {
                inspector: runtime::InspectorMode::On,
                max_isolates: 1,
                ..runtime::RuntimeConfig::default()
            },
            storage: runtime::RuntimeStorageConfig {
                store_dir: store_dir.clone(),
                ..runtime::RuntimeStorageConfig::default()
            },
        })
        .await
        .unwrap();
        runtime
            .deploy("debugged".into(), WORKER.into())
            .await
            .unwrap();
        // The isolate, and so the target, starts with the first request.
        let first = runtime
            .invoke("debugged".into(), invocation("first"))
            .await
            .unwrap();
        assert_eq!(first.body, b"42");

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(serve_inspector(listener, runtime.clone()));
        let host = address.to_string();
        assert_eq!(get(address, "/json/list", "evil.example").await.0, 403);
        let (status, version) = get(address, "/json/version", &host).await;
        assert_eq!(status, 200);
        let version: serde_json::Value = serde_json::from_str(&version).unwrap();
        assert!(version["Browser"].as_str().unwrap().starts_with("dd/"));
        let (status, list) = get(address, "/json/list", &host).await;
        assert_eq!(status, 200);
        let list: serde_json::Value = serde_json::from_str(&list).unwrap();
        let [target] = list.as_array().unwrap().as_slice() else {
            panic!("one isolate, one target: {list}");
        };
        assert_eq!(target["title"], "debugged (isolate 1)");
        assert_eq!(target["type"], "node");
        let socket_url = target["webSocketDebuggerUrl"].as_str().unwrap();
        let id = target["id"].as_str().unwrap();
        assert_eq!(socket_url, format!("ws://{host}/{id}"));
        assert!(
            target["devtoolsFrontendUrl"]
                .as_str()
                .unwrap()
                .starts_with("devtools://devtools/bundled/js_app.html?")
        );

        let (mut client, _) = tokio_tungstenite::connect_async(socket_url).await.unwrap();
        call(&mut client, 1, "Debugger.enable", json!({})).await;
        let breakpoint = call(
            &mut client,
            2,
            "Debugger.setBreakpointByUrl",
            json!({ "url": "file:///dd/worker.js", "lineNumber": 2 }),
        )
        .await;
        assert_eq!(
            breakpoint["result"]["locations"].as_array().map(Vec::len),
            Some(1)
        );

        let request = tokio::spawn({
            let runtime = runtime.clone();
            async move {
                runtime
                    .invoke("debugged".into(), invocation("paused"))
                    .await
            }
        });
        let paused = next(&mut client, |message| {
            message["method"] == "Debugger.paused"
        })
        .await;
        assert_eq!(
            paused["params"]["hitBreakpoints"].as_array().map(Vec::len),
            Some(1)
        );
        let frame = &paused["params"]["callFrames"][0];
        assert_eq!(frame["location"]["lineNumber"], 2);
        let evaluated = call(
            &mut client,
            3,
            "Debugger.evaluateOnCallFrame",
            json!({ "callFrameId": frame["callFrameId"], "expression": "typeof Response" }),
        )
        .await;
        assert_eq!(evaluated["result"]["result"]["value"], "function");
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(
            !request.is_finished(),
            "the request waits at the breakpoint"
        );
        call(&mut client, 4, "Debugger.resume", json!({})).await;
        let output = tokio::time::timeout(std::time::Duration::from_secs(10), request)
            .await
            .expect("request finishes once resumed")
            .unwrap()
            .unwrap();
        assert_eq!(output.body, b"42");

        client.close(None).await.unwrap();
        server.abort();
        runtime.shutdown().await.unwrap();
        let _ = tokio::fs::remove_dir_all(store_dir).await;
    }
}
