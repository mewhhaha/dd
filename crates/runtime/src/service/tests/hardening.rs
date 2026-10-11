//! Worker code is untrusted. Outside a debug runtime it cannot generate code
//! from strings, it reaches none of the runtime's internals, its op arguments
//! cannot make the runtime copy past its budget, and its forged completions
//! are ignored.
//!
//! Worker code also shares its context with the runtime's request machinery
//! and may replace any built-in or global. The machinery runs on what it
//! captured at startup, so requests keep working and keep their own handles.
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
        timeout(
            Duration::from_secs(10),
            echo_requests(listener, 1, ("probe", "x-probe"), false),
        )
        .await
        .expect("the worker's host fetch arrives");
    });
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates_per_worker: 1,
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
        max_isolates_per_worker: 1,
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
        max_isolates_per_worker: 1,
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

#[tokio::test]
#[serial]
async fn production_runtime_rejects_string_code_generation() {
    let service = test_service(test_pool_config(0, 1, 1)).await;
    service
        .deploy(
            "production-codegen".to_string(),
            r#"
export default {
  async fetch() {
    return new Response(String(eval("20 + 1")));
  },
};
"#
            .to_string(),
        )
        .await
        .expect("deploy should succeed");

    let error = service
        .invoke(
            "production-codegen".to_string(),
            test_invocation_with_path("/", "production-codegen"),
        )
        .await
        .expect_err("production invoke should reject eval");
    assert_eq!(error.kind(), ErrorKind::Runtime);
    assert!(
        error.to_string().contains("Code generation from strings"),
        "{error}"
    );
}

#[tokio::test]
#[serial]
async fn debug_runtime_allows_string_code_generation() {
    let service = test_service(RuntimeConfig {
        min_isolates: 0,
        max_isolates_per_worker: 1,
        max_inflight_per_isolate: 1,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        debug_code_generation: true,
        ..RuntimeConfig::default()
    })
    .await;
    service
        .deploy(
            "debug-codegen".to_string(),
            r#"
export default {
  async fetch() {
    const evalValue = eval("20 + 1");
    const functionValue = new Function("value", "return value + 1")(evalValue);
    return new Response(String(functionValue));
  },
};
"#
            .to_string(),
        )
        .await
        .expect("deploy should succeed");

    let output = invoke_with_timeout_and_dump(
        &service,
        "debug-codegen",
        test_invocation_with_path("/", "debug-codegen"),
        "debug code generation",
    )
    .await;
    assert_eq!(String::from_utf8(output.body).expect("utf8"), "22");
}

#[tokio::test]
#[serial]
async fn op_arguments_cannot_expand_past_the_copy_budget() {
    let service = test_service(RuntimeConfig::default()).await;
    service
        .deploy(
            "expanding".to_string(),
            r#"
export default {
  fetch() {
    // 1 MiB of string, referenced 300 times: copying it would take 600 MiB.
    const big = "x".repeat(1 << 20);
    const params = new URLSearchParams();
    for (let i = 0; i < 300; i++) params.append(big, big);
    try {
      params.toString();
      return new Response("copied");
    } catch (error) {
      return new Response(`refused: ${error.message}`);
    }
  },
};
"#
            .to_string(),
        )
        .await
        .expect("deploy should succeed");
    let output = service
        .invoke("expanding".to_string(), test_invocation())
        .await
        .expect("the worker survives the refused copy");
    let body = String::from_utf8(output.body.to_vec()).expect("utf8");
    assert!(
        body.starts_with("refused:") && body.contains("too large"),
        "{body}"
    );
    service.shutdown().await.expect("service should shut down");
}

#[tokio::test]
#[serial]
async fn deployed_workers_reach_no_runtime_internals() {
    // test_service exposes the internals; a default service must not.
    let store = TestStoreDir::new("dd-internals");
    let service = RuntimeService::start(RuntimeServiceConfig {
        runtime: RuntimeConfig::default(),
        storage: RuntimeStorageConfig {
            store_dir: store.path().to_path_buf(),
            ..RuntimeStorageConfig::default()
        },
    })
    .await
    .expect("service should start");
    service
        .deploy(
            "internals".to_string(),
            r#"
export default {
  async fetch() {
    return Response.json({
      internals: typeof __dd_internals,
      deno: typeof Deno,
      bootstrap: typeof __bootstrap,
      ddGlobals: Object.getOwnPropertyNames(globalThis).filter((name) => name.startsWith("__")),
    });
  },
};
"#
            .to_string(),
        )
        .await
        .expect("deploy should succeed");
    let output = service
        .invoke(
            "internals".to_string(),
            test_invocation_with_path("/", "internals"),
        )
        .await
        .expect("invoke should succeed");
    let body: Value = serde_json::from_slice(&output.body).expect("body should be json");
    assert_eq!(
        body,
        serde_json::json!({
            "internals": "undefined",
            "deno": "undefined",
            "bootstrap": "undefined",
            "ddGlobals": ["__dd_async_context"],
        })
    );
    service.shutdown().await.expect("service should shut down");
}

#[tokio::test]
#[serial]
async fn forged_and_invalid_completion_payloads_are_ignored() {
    let service = test_service(test_pool_config(1, 1, 2)).await;

    service
        .deploy("malicious".to_string(), malicious_completion_worker())
        .await
        .expect("deploy should succeed");

    let first = service
        .invoke("malicious".to_string(), test_invocation())
        .await
        .expect("first invoke should succeed");
    assert_eq!(String::from_utf8(first.body).expect("utf8"), "1");

    let second = service
        .invoke("malicious".to_string(), test_invocation())
        .await
        .expect("second invoke should succeed");
    assert_eq!(String::from_utf8(second.body).expect("utf8"), "2");
}
