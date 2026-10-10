//! Worker code shares its context with the runtime's request machinery and
//! may replace any built-in or global. The machinery runs on what it captured
//! at startup, so requests keep working and keep their own handles.
use super::*;

/// Replaces, at module evaluation, the built-ins and globals the request
/// machinery used to call: prototype methods and statics throw, Math answers
/// 1, and the web globals are gone. The worker keeps what it needs first.
const SABOTAGE: &str = r#"
const RealResponse = Response;
const RealReadableStream = ReadableStream;
const RealURL = URL;
const realFetch = fetch;
const stringify = JSON.stringify;
const encoder = new TextEncoder();
const encode = (text) => encoder.encode(text);
{
  const boom = function () { throw new Error("sabotaged built-in"); };
  const define = Reflect.defineProperty;
  const getPrototypeOf = Object.getPrototypeOf;
  const replace = (target, keys, value) => {
    for (let i = 0; i < keys.length; i++) {
      define(target, keys[i], { value, writable: true, configurable: true });
    }
  };
  const iteratorPrototypes = [
    getPrototypeOf([][Symbol.iterator]()),
    getPrototypeOf(new Map()[Symbol.iterator]()),
    getPrototypeOf(new Set()[Symbol.iterator]()),
    getPrototypeOf(""[Symbol.iterator]()),
  ];
  for (let i = 0; i < iteratorPrototypes.length; i++) {
    replace(iteratorPrototypes[i], ["next"], boom);
  }
  replace(Array.prototype, [
    "push", "pop", "shift", "unshift", "slice", "splice", "map", "filter", "forEach",
    "some", "every", "includes", "indexOf", "join", "concat", "sort", "reduce", "find",
    "flat", "flatMap", "keys", "values", "entries", Symbol.iterator,
  ], boom);
  replace(Array, ["from", "of", "isArray"], boom);
  replace(Object, [
    "keys", "values", "entries", "assign", "freeze", "defineProperty", "defineProperties",
    "getOwnPropertyDescriptor", "getOwnPropertyNames", "getPrototypeOf", "setPrototypeOf",
    "create", "hasOwn", "fromEntries", "is",
  ], boom);
  replace(Object.prototype, ["hasOwnProperty", "isPrototypeOf", "propertyIsEnumerable"], boom);
  replace(Map.prototype, [
    "get", "set", "has", "delete", "clear", "forEach", "entries", "keys", "values", Symbol.iterator,
  ], boom);
  replace(Set.prototype, [
    "add", "has", "delete", "clear", "forEach", "entries", "keys", "values", Symbol.iterator,
  ], boom);
  replace(WeakMap.prototype, ["get", "set", "has", "delete"], boom);
  replace(Promise.prototype, ["then", "catch", "finally"], boom);
  replace(Promise, ["all", "allSettled", "any", "race", "resolve", "reject", "withResolvers"], boom);
  replace(String.prototype, [
    "split", "slice", "substring", "trim", "trimStart", "trimEnd", "toLowerCase", "toUpperCase",
    "indexOf", "includes", "startsWith", "endsWith", "replace", "replaceAll", "padEnd",
    "padStart", "charCodeAt", "codePointAt", "localeCompare", "toWellFormed", "concat", "at",
    Symbol.iterator,
  ], boom);
  replace(String, ["fromCharCode"], boom);
  replace(Number, ["isFinite", "isInteger", "isNaN", "parseInt", "parseFloat"], boom);
  replace(Number.prototype, ["toString", "toFixed"], boom);
  replace(getPrototypeOf(Uint8Array.prototype), ["set", "subarray", "slice", "map", "fill"], boom);
  replace(ArrayBuffer.prototype, ["slice"], boom);
  replace(ArrayBuffer, ["isView"], boom);
  replace(Function.prototype, ["call", "apply", "bind"], boom);
  replace(JSON, ["stringify", "parse"], boom);
  replace(Math, ["trunc", "max", "min", "floor", "ceil", "round", "abs"], () => 1);
  replace(Date, ["now"], boom);
  replace(performance, ["now"], boom);
  replace(console, ["log", "warn", "error"], boom);
  replace(globalThis, [
    "Array", "Object", "Map", "Set", "WeakMap", "Promise", "String", "Number", "Boolean",
    "Symbol", "JSON", "Math", "Request", "Headers", "URL", "TextEncoder", "TextDecoder",
    "AbortController", "AbortSignal", "Uint8Array", "ArrayBuffer", "ReadableStream",
    "setTimeout", "clearTimeout", "fetch",
  ], boom);
  replace(globalThis, ["Response"], class Response {
    constructor(body) { this.body = body; }
  });
  replace(Reflect, [
    "apply", "construct", "defineProperty", "get", "getOwnPropertyDescriptor",
    "getPrototypeOf", "has", "ownKeys", "set",
  ], boom);
}
"#;

const HARDENED_WORKER: &str = r#"
export default {
  async fetch(request, env, ctx) {
    const path = new RealURL(request.url).pathname;
    if (path === "/kv") {
      const body = await request.text();
      await env.KV.put("text", body);
      await env.KV.put("object", { list: [1, 2], nested: { ok: true } });
      const text = await env.KV.get("text");
      const object = await env.KV.get("object");
      const listed = await env.KV.list({ prefix: "" });
      const keys = [];
      for (let i = 0; i < listed.length; i++) keys[i] = listed[i].key;
      ctx.waitUntil((async () => {
        await env.KV.put("after", "waited");
      })());
      const echoed = await realFetch(target, {
        method: "POST",
        body: "ping",
        headers: { "x-probe": "hardened" },
      });
      const echo = await echoed.json();
      return new RealResponse(stringify({ text, object, keys, echo, status: echoed.status }), {
        headers: { "content-type": "application/json", "x-route": "kv" },
      });
    }
    if (path === "/after") {
      return new RealResponse((await env.KV.get("after")) ?? "pending");
    }
    if (path === "/body") {
      return new RealResponse(`body:${await request.text()}`);
    }
    if (path === "/stream") {
      let sent = 0;
      return new RealResponse(new RealReadableStream({
        async pull(controller) {
          if (sent === 3) {
            controller.close();
            return;
          }
          sent += 1;
          controller.enqueue(encode(`chunk${sent};`));
        },
      }), { status: 201, headers: { "x-route": "stream" } });
    }
    return new RealResponse("unknown", { status: 404 });
  },
};
"#;

/// Answers one HTTP request with its method, `x-probe` header and body.
async fn echo_once(listener: TcpListener) {
    let (mut socket, _) = listener.accept().await.unwrap();
    let mut request = Vec::new();
    let mut buffer = [0_u8; 2048];
    let header_end = loop {
        let length = socket.read(&mut buffer).await.unwrap();
        assert!(length > 0, "request ended before its headers");
        request.extend_from_slice(&buffer[..length]);
        if let Some(end) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
            break end + 4;
        }
    };
    let headers = String::from_utf8(request[..header_end].to_vec()).unwrap();
    let header = |name: &str| {
        headers
            .lines()
            .filter_map(|line| line.split_once(':'))
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.trim().to_string())
    };
    let body_length = header("content-length").map_or(0, |value| value.parse().unwrap());
    while request.len() < header_end + body_length {
        let length = socket.read(&mut buffer).await.unwrap();
        assert!(length > 0, "request ended before its body");
        request.extend_from_slice(&buffer[..length]);
    }
    let body = serde_json::to_vec(&serde_json::json!({
        "method": headers.split_whitespace().next().unwrap(),
        "probe": header("x-probe"),
        "body": String::from_utf8(request[header_end..header_end + body_length].to_vec()).unwrap(),
    }))
    .unwrap();
    let response = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        body.len()
    );
    socket.write_all(response.as_bytes()).await.unwrap();
    socket.write_all(&body).await.unwrap();
}

fn header<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
}

#[tokio::test]
#[serial]
async fn request_machinery_survives_replaced_builtins() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        timeout(Duration::from_secs(10), echo_once(listener))
            .await
            .expect("the worker's host fetch arrives");
    });
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        request_wall_timeout: Duration::from_secs(10),
        ..RuntimeConfig::default()
    })
    .await;
    let target = serde_json::to_string(&format!("http://{address}/echo")).unwrap();
    service
        .deploy_with_config(
            "hardened".into(),
            format!("const target = {target};\n{SABOTAGE}\n{HARDENED_WORKER}"),
            DeployConfig {
                egress_allow_hosts: vec![format!("private:{address}")],
                bindings: vec![DeployBinding::Kv {
                    binding: "KV".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("sabotaging worker deploys");

    // KV, the request body, a host fetch and waitUntil.
    let mut request = test_invocation_with_path("/kv", "hardened-kv");
    request.method = "POST".into();
    request.body = b"posted body".to_vec();
    let output = timeout(
        Duration::from_secs(10),
        service.invoke("hardened".into(), request),
    )
    .await
    .expect("kv request finishes")
    .expect("kv request succeeds");
    assert_eq!(output.status, 200);
    assert_eq!(header(&output.headers, "x-route"), Some("kv"));
    let result: Value = serde_json::from_slice(&output.body).unwrap();
    assert_eq!(
        result,
        serde_json::json!({
            "text": "posted body",
            "object": { "list": [1, 2], "nested": { "ok": true } },
            "keys": ["object", "text"],
            "echo": { "method": "POST", "probe": "hardened", "body": "ping" },
            "status": 200,
        })
    );
    server.await.unwrap();

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let after = service
            .invoke(
                "hardened".into(),
                test_invocation_with_path("/after", "hardened-after"),
            )
            .await
            .expect("waitUntil probe succeeds");
        if after.body == b"waited" {
            break;
        }
        assert!(Instant::now() < deadline, "waitUntil work never finished");
        sleep(Duration::from_millis(20)).await;
    }

    // A request body streamed from the host.
    let (sender, receiver) = mpsc::channel(4);
    let mut request = test_invocation_with_path("/body", "hardened-body");
    request.method = "POST".into();
    let streamed = {
        let service = service.clone();
        tokio::spawn(async move {
            service
                .invoke_with_request_body("hardened".into(), request, Some(receiver))
                .await
        })
    };
    sender
        .send(Ok(Bytes::from_static(b"streamed ")))
        .await
        .unwrap();
    sender
        .send(Ok(Bytes::from_static(b"chunks")))
        .await
        .unwrap();
    drop(sender);
    let output = timeout(Duration::from_secs(10), streamed)
        .await
        .expect("streamed body request finishes")
        .unwrap()
        .expect("streamed body request succeeds");
    assert_eq!(output.body, b"body:streamed chunks");

    // A stream body, buffered and streamed.
    let output = service
        .invoke(
            "hardened".into(),
            test_invocation_with_path("/stream", "hardened-buffered"),
        )
        .await
        .expect("buffered stream succeeds");
    assert_eq!(output.status, 201);
    assert_eq!(header(&output.headers, "x-route"), Some("stream"));
    assert_eq!(output.body, b"chunk1;chunk2;chunk3;");
    let mut stream = service
        .invoke_stream(
            "hardened".into(),
            test_invocation_with_path("/stream", "hardened-streamed"),
        )
        .await
        .expect("streamed response starts");
    assert_eq!(stream.status, 201);
    assert_eq!(header(&stream.headers, "x-route"), Some("stream"));
    let mut body = Vec::new();
    while let Some(chunk) = timeout(Duration::from_secs(5), stream.body.recv())
        .await
        .expect("stream chunk arrives")
    {
        body.extend_from_slice(&chunk.expect("stream chunk is ok"));
    }
    assert_eq!(body, b"chunk1;chunk2;chunk3;");
    service.shutdown().await.expect("runtime shuts down");
}

#[tokio::test]
#[serial]
async fn a_worker_response_class_is_not_a_response() {
    let service = test_service(RuntimeConfig {
        max_isolates: 1,
        ..RuntimeConfig::default()
    })
    .await;
    service
        .deploy(
            "fake-response".into(),
            r#"
const RealResponse = Response;
globalThis.Response = class Response {
  constructor(body) {
    this.body = body;
    this.status = 200;
    this.headers = new Headers();
  }
};
export default {
  fetch(request) {
    if (new URL(request.url).pathname === "/borrowed") {
      return Object.setPrototypeOf({ status: 200, headers: new Headers() }, RealResponse.prototype);
    }
    return new Response("fake");
  },
};
"#
            .into(),
        )
        .await
        .expect("worker deploys");
    let error = service
        .invoke("fake-response".into(), test_invocation())
        .await
        .expect_err("a look-alike Response is refused");
    assert!(
        error.to_string().contains("must return a Response"),
        "{error}"
    );
    service
        .invoke(
            "fake-response".into(),
            test_invocation_with_path("/borrowed", "borrowed-prototype"),
        )
        .await
        .expect_err("an object that only inherits Response.prototype is refused");
    service.shutdown().await.expect("runtime shuts down");
}

#[tokio::test]
#[serial]
async fn math_and_number_overrides_do_not_change_op_handles() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 4,
        ..RuntimeConfig::default()
    })
    .await;
    service
        .deploy(
            "handle-steering".into(),
            r#"
Math.trunc = () => 1;
Math.max = () => 1;
Math.min = () => 1;
Math.round = () => 1;
Math.floor = () => 1;
globalThis.Number = () => 1;
export default {
  async fetch(request) {
    const body = await request.text();
    return new Response(`echo:${body}`, { headers: { "x-url": request.url } });
  },
};
"#
            .into(),
        )
        .await
        .expect("worker deploys");

    let mut requests = Vec::new();
    for name in ["alpha", "beta", "gamma", "delta"] {
        let service = service.clone();
        let mut request = test_invocation_with_path(&format!("/{name}"), name);
        request.method = "POST".into();
        request.body = name.as_bytes().to_vec();
        requests.push(tokio::spawn(async move {
            service.invoke("handle-steering".into(), request).await
        }));
    }
    let (sender, receiver) = mpsc::channel(2);
    let mut streamed = test_invocation_with_path("/streamed", "streamed");
    streamed.method = "POST".into();
    let streamed = {
        let service = service.clone();
        tokio::spawn(async move {
            service
                .invoke_with_request_body("handle-steering".into(), streamed, Some(receiver))
                .await
        })
    };
    sender
        .send(Ok(Bytes::from_static(b"streamed")))
        .await
        .unwrap();
    drop(sender);

    for (request, name) in requests
        .into_iter()
        .zip(["alpha", "beta", "gamma", "delta"])
    {
        let output = timeout(Duration::from_secs(10), request)
            .await
            .expect("request finishes")
            .unwrap()
            .expect("request succeeds");
        assert_eq!(output.body, format!("echo:{name}").as_bytes());
        assert_eq!(
            header(&output.headers, "x-url"),
            Some(format!("http://worker/{name}").as_str())
        );
    }
    let output = timeout(Duration::from_secs(10), streamed)
        .await
        .expect("streamed request finishes")
        .unwrap()
        .expect("streamed request succeeds");
    assert_eq!(output.body, b"echo:streamed");
    service.shutdown().await.expect("runtime shuts down");
}
