//! Runtime bootstrap, loading and dispatch tests.

use super::*;
use serial_test::serial;

fn simple_worker_source() -> &'static str {
    r#"
    export default {
      async fetch(_request) {
        return new Response("ok");
      }
    };
    "#
}

/// Runs `source` as a function body with the runtime's internals as
/// `__bootstrap`, as the runtime's own scripts see them.
fn execute_internal(runtime: &mut JsRuntime, source: &str) -> v8::Global<v8::Value> {
    let internals = runtime.internals().expect("runtime internals");
    runtime
        .execute_function("<dd:test>", source, &["__bootstrap"], &[internals])
        .expect("internal script should run")
}

fn string(runtime: &mut JsRuntime, value: v8::Global<v8::Value>) -> String {
    dd_v8::scope!(scope, runtime);
    let value = v8::Local::new(scope, value);
    value
        .to_string(scope)
        .expect("value should stringify")
        .to_rust_string_lossy(scope)
}

#[tokio::test]
#[serial]
async fn bootstrap_snapshot_builds() {
    let _ = build_bootstrap_snapshot()
        .await
        .expect("bootstrap snapshot should build");
}

#[tokio::test]
#[serial]
async fn runtime_starts_from_bootstrap_snapshot() {
    let snapshot = build_bootstrap_snapshot()
        .await
        .expect("bootstrap snapshot should build");
    let _ = new_runtime_from_snapshot(snapshot, false, ModuleRegistry::default())
        .expect("runtime should start from snapshot");
}

#[test]
#[serial]
fn fetch_classes_work_from_bootstrap_snapshot() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime should build");
    let snapshot = runtime
        .block_on(build_bootstrap_snapshot())
        .expect("bootstrap snapshot should build");
    let mut js_runtime = new_runtime_from_snapshot(snapshot, false, ModuleRegistry::default())
        .expect("runtime should start from snapshot");

    js_runtime
        .execute_script(
            "<dd:test>",
            r#"
            globalThis.__dd_test_request = new Request("http://example.com/test", {
              method: "POST",
              body: "hi",
            });
            globalThis.__dd_test_response = Response.json({ ok: true });
            globalThis.__dd_test_headers = new Headers([["x-dd", "ok"]]);
            globalThis.__dd_test_form_data = new FormData();
            globalThis.__dd_test_form_data.append("name", "value");
            "#,
        )
        .expect("fetch classes should construct");
    let ctor_match = execute_internal(
        &mut js_runtime,
        r#"
        const { web } = __bootstrap.dd;
        return JSON.stringify({
          fetch: typeof globalThis.fetch === "function" && globalThis.fetch === web.fetch,
          request: globalThis.Request === web.Request,
          headers: globalThis.Headers === web.Headers,
          response: globalThis.Response === web.Response,
          formData: globalThis.FormData === web.FormData,
        });
        "#,
    );
    assert_eq!(
        string(&mut js_runtime, ctor_match),
        r#"{"fetch":true,"request":true,"headers":true,"response":true,"formData":true}"#
    );

    let mut eval = |source: &str| {
        let value = js_runtime
            .execute_script("<dd:test>", source)
            .expect("script should execute");
        let value = runtime
            .block_on(js_runtime.resolve(value))
            .expect("value should resolve");
        string(&mut js_runtime, value)
    };
    assert_eq!(
        eval("globalThis.__dd_test_request.url"),
        "http://example.com/test"
    );
    assert_eq!(
        eval("globalThis.__dd_test_response.text()"),
        r#"{"ok":true}"#
    );
    assert_eq!(
        eval("globalThis.__dd_test_response.headers.get('content-type')"),
        "application/json"
    );
    assert_eq!(eval("globalThis.__dd_test_headers.get('x-dd')"), "ok");
    assert_eq!(eval("globalThis.__dd_test_form_data.get('name')"), "value");
}

#[test]
#[serial]
fn teed_and_piped_streams_deliver_every_chunk() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime should build");
    let snapshot = runtime
        .block_on(build_bootstrap_snapshot())
        .expect("bootstrap snapshot should build");
    let mut js_runtime = new_runtime_from_snapshot(snapshot, false, ModuleRegistry::default())
        .expect("runtime should start from bootstrap snapshot");
    // Tee and pipe chunk steps run through the web layer's queueMicrotask.
    let result = js_runtime
        .execute_script(
            "<dd:test>",
            r#"
            (async () => {
              const bytes = new ReadableStream({
                type: "bytes",
                start(controller) {
                  controller.enqueue(new TextEncoder().encode("hello"));
                  controller.close();
                },
              });
              const [left, right] = bytes.tee();
              const teed = await Promise.all([new Response(left).text(), new Response(right).text()]);
              const [first, second] = new Response("plain").body.tee();
              const plain = await Promise.all([new Response(first).text(), new Response(second).text()]);
              const piped = await new Response(
                new Response("piped").body.pipeThrough(new TransformStream()),
              ).text();
              return [...teed, ...plain, piped].join(",");
            })()
            "#,
        )
        .expect("stream script should run");
    let result = runtime
        .block_on(js_runtime.resolve(result))
        .expect("streams should deliver");
    assert_eq!(
        string(&mut js_runtime, result),
        "hello,hello,plain,plain,piped"
    );
}

#[test]
#[serial]
fn workers_get_self_and_every_stream_class() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime should build");
    let snapshot = runtime
        .block_on(build_bootstrap_snapshot())
        .expect("bootstrap snapshot should build");
    let mut js_runtime = new_runtime_from_snapshot(snapshot, false, ModuleRegistry::default())
        .expect("runtime should start from snapshot");
    let missing = js_runtime
        .execute_script(
            "<dd:test>",
            r#"
            [
              "ByteLengthQueuingStrategy", "CountQueuingStrategy",
              "ReadableByteStreamController", "ReadableStreamBYOBReader",
              "ReadableStreamBYOBRequest", "ReadableStreamDefaultController",
              "ReadableStreamDefaultReader", "TextDecoderStream", "TextEncoderStream",
              "TransformStreamDefaultController", "WritableStreamDefaultController",
              "WritableStreamDefaultWriter",
            ].filter((name) => typeof globalThis[name] !== "function")
              .concat(self === globalThis ? [] : ["self"])
              .join(",")
            "#,
        )
        .expect("globals probe should run");
    assert_eq!(string(&mut js_runtime, missing), "");
}

#[test]
#[serial]
fn worker_code_reaches_no_runtime_internals() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime should build");
    let snapshot = runtime
        .block_on(build_bootstrap_snapshot())
        .expect("bootstrap snapshot should build");
    let mut js_runtime = new_runtime_from_snapshot(snapshot, false, ModuleRegistry::default())
        .expect("runtime should start from bootstrap snapshot");
    let source = r#"
        const probes = {
          Deno: typeof Deno,
          __bootstrap: typeof __bootstrap,
          core: typeof core,
          ops: typeof ops,
          dd: typeof dd,
          primordials: typeof primordials,
          internals: typeof internals,
          RECORD: typeof RECORD,
          define: typeof define,
        };
        const leaked = Object.keys(probes).filter((name) => probes[name] !== "undefined");
        const ddGlobals = Object.getOwnPropertyNames(globalThis)
          .filter((name) => name.startsWith("__"));
        const asyncContext = Object.keys(globalThis.__dd_async_context).sort();
        export default {
          fetch() {
            return Response.json({ leaked, ddGlobals, asyncContext });
          },
        };
    "#;
    runtime
        .block_on(load_worker(&mut js_runtime, source))
        .expect("worker should load");
    let body = execute_internal(
        &mut js_runtime,
        "return Promise.resolve(__bootstrap.dd.worker.fetch(new Request('http://worker/'))).then((response) => response.text())",
    );
    let body = runtime
        .block_on(js_runtime.resolve(body))
        .expect("worker fetch should resolve");
    assert_eq!(
        string(&mut js_runtime, body),
        r#"{"leaked":[],"ddGlobals":["__dd_async_context"],"asyncContext":["disableAsyncLocalStore","enterWithAsyncLocalStore","getAsyncLocalStore","runWithAsyncLocalStore"]}"#
    );
}

/// Console methods are `op_call_console` bound to V8's method and dd's.
/// Bound functions hide their target and arguments, and no receiver or
/// extra argument changes which two functions the op calls.
#[test]
#[serial]
fn console_methods_hide_the_op_they_are_bound_to() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime should build");
    let snapshot = runtime
        .block_on(build_bootstrap_snapshot())
        .expect("bootstrap snapshot should build");
    let mut js_runtime = new_runtime_from_snapshot(snapshot, false, ModuleRegistry::default())
        .expect("runtime should start from bootstrap snapshot");
    let probe = js_runtime
        .execute_script(
            "<dd:test>",
            r#"
            (() => {
              let called = false;
              const spy = () => { called = true; };
              console.log.call(spy, spy, spy);
              console.log.apply(spy, [spy, spy]);
              Reflect.apply(console.log, spy, [spy]);
              console.log.bind(spy, spy)(spy);
              let constructed;
              try {
                new console.log(spy);
                constructed = "constructed";
              } catch (error) {
                constructed = error.constructor.name;
              }
              return JSON.stringify({
                called,
                constructed,
                name: console.log.name,
                own: Object.getOwnPropertyNames(console.log).sort(),
                prototype: console.log.prototype === undefined,
                source: Function.prototype.toString.call(console.log),
                proto: Object.getPrototypeOf(console.log) === Function.prototype,
              });
            })()
            "#,
        )
        .expect("probe should run");
    assert_eq!(
        string(&mut js_runtime, probe),
        r#"{"called":false,"constructed":"TypeError","name":"log","own":["length","name"],"prototype":true,"source":"function () { [native code] }","proto":true}"#
    );
}

/// The runtime's scripts take built-ins from primordials, web methods
/// from `dd.webPrimordials`, ops from `core.ops` and each other's helpers
/// from `dd` by name; a misspelt name would stay undefined until the code
/// path using it runs.
#[test]
#[serial]
fn runtime_scripts_capture_only_names_that_exist() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime should build");
    let snapshot = runtime
        .block_on(build_bootstrap_snapshot())
        .expect("bootstrap snapshot should build");
    let mut js_runtime = new_runtime_from_snapshot(snapshot, false, ModuleRegistry::default())
        .expect("runtime should start from bootstrap snapshot");
    let sources = serde_json::to_string(&[
        include_str!("../../js/web/console.js"),
        WEB_INIT_JS,
        BOOTSTRAP_JS,
        EXECUTE_WORKER_JS,
    ])
    .expect("sources serialize");
    let missing = execute_internal(
        &mut js_runtime,
        &format!(
            r#"
            const {{ core, dd, primordials }} = __bootstrap;
            const capture = /const \{{([^}}]*)\}} = (primordials|dd\.webPrimordials|core\.ops|dd);/g;
            const targets = {{
              primordials,
              "dd.webPrimordials": dd.webPrimordials,
              "core.ops": core.ops,
              dd,
            }};
            const missing = [];
            let checked = 0;
            for (const source of {sources}) {{
              for (const [, list, from] of source.matchAll(capture)) {{
                const target = targets[from];
                for (const entry of list.split(",")) {{
                  const name = entry.split(":")[0].trim();
                  if (!name) continue;
                  checked++;
                  if (target[name] === undefined) missing.push(name);
                }}
              }}
            }}
            return JSON.stringify({{ missing, enough: checked > 200 }});
            "#
        ),
    );
    assert_eq!(
        string(&mut js_runtime, missing),
        r#"{"missing":[],"enough":true}"#
    );
}

#[test]
#[serial]
fn direct_worker_fetch_works_after_loading_worker_into_runtime() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime should build");
    let snapshot = runtime.block_on(async {
        let bootstrap = build_bootstrap_snapshot()
            .await
            .expect("bootstrap snapshot should build");
        validate_worker(bootstrap, simple_worker_source(), false)
            .await
            .expect("worker should validate");
        bootstrap
    });
    let mut js_runtime = new_runtime_from_snapshot(snapshot, false, ModuleRegistry::default())
        .expect("runtime should start from bootstrap snapshot");
    runtime
        .block_on(load_worker(&mut js_runtime, simple_worker_source()))
        .expect("worker should load into runtime");

    let response_promise = execute_internal(
        &mut js_runtime,
        r#"
        return __bootstrap.dd.worker
          .fetch(new Request("http://worker/"), {}, { waitUntil() {} })
          .then((response) => String(response.status));
        "#,
    );
    let response_value = runtime
        .block_on(js_runtime.resolve(response_promise))
        .expect("response promise should resolve");
    assert_eq!(string(&mut js_runtime, response_value), "200");
}

#[test]
#[serial]
fn response_constructor_works_after_loading_worker_into_runtime() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime should build");
    let snapshot = runtime.block_on(async {
        let bootstrap = build_bootstrap_snapshot()
            .await
            .expect("bootstrap snapshot should build");
        validate_worker(bootstrap, simple_worker_source(), false)
            .await
            .expect("worker should validate");
        bootstrap
    });
    let mut js_runtime = new_runtime_from_snapshot(snapshot, false, ModuleRegistry::default())
        .expect("runtime should start from bootstrap snapshot");
    runtime
        .block_on(load_worker(&mut js_runtime, simple_worker_source()))
        .expect("worker should load into runtime");

    let response_value = js_runtime
        .execute_script("<dd:test>", r#"String(new Response("ok").status)"#)
        .expect("response constructor should execute");
    assert_eq!(string(&mut js_runtime, response_value), "200");
}

#[test]
#[serial]
fn a_new_isolate_reads_the_current_time() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime should build");
    let snapshot = runtime
        .block_on(build_bootstrap_snapshot())
        .expect("bootstrap snapshot should build");
    // Let the snapshot's own clock reading fall well behind.
    std::thread::sleep(std::time::Duration::from_millis(1200));
    let mut js_runtime = new_runtime_from_snapshot(snapshot, false, ModuleRegistry::default())
        .expect("runtime should start from snapshot");
    let wall_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after epoch")
        .as_millis();
    let value = js_runtime
        .execute_script(
            "<dd:test>",
            &format!(
                "JSON.stringify([Math.abs(Date.now() - {wall_ms}) < 500, performance.now() > 0, \
                 performance.timeOrigin + performance.now() === Date.now()])"
            ),
        )
        .expect("clock script should run");
    assert_eq!(string(&mut js_runtime, value), "[true,true,true]");
}

#[test]
#[serial]
fn zero_delay_timers_are_tasks_and_base64_follows_html() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime should build");
    let snapshot = runtime
        .block_on(build_bootstrap_snapshot())
        .expect("bootstrap snapshot should build");
    let _context = runtime.enter();
    let mut js_runtime = new_runtime_from_snapshot(snapshot, false, ModuleRegistry::default())
        .expect("runtime should start from snapshot");
    let result = js_runtime
        .execute_script(
            "<dd:test>",
            r#"
            (async () => {
              const order = [];
              setTimeout(() => order.push("timeout"), 0);
              queueMicrotask(() => order.push("microtask"));
              Promise.resolve().then(() => order.push("then"));
              await new Promise((resolve) => setTimeout(resolve, 5));
              // A zero-delay interval must let the event loop turn.
              let ticks = 0;
              const interval = setInterval(() => ticks++, 0);
              await new Promise((resolve) => setTimeout(resolve, 20));
              clearInterval(interval);
              const error = (f) => { try { f(); return "none"; } catch (e) { return `${e.name}`; } };
              return JSON.stringify({
                order,
                intervalYields: ticks > 0 && ticks < 100,
                atob: [atob("aGk="), atob(" aG k "), atob("YQ")],
                btoa: [btoa("hi"), btoa("é")],
                errors: [error(() => atob("a")), error(() => atob("a=b")), error(() => btoa("€")), error(() => atob())],
              });
            })()
            "#,
        )
        .expect("timer script should run");
    let result = runtime
        .block_on(js_runtime.resolve(result))
        .expect("timers settle");
    assert_eq!(
        string(&mut js_runtime, result),
        r#"{"order":["microtask","then","timeout"],"intervalYields":true,"atob":["hi","hi","a"],"btoa":["aGk=","6Q=="],"errors":["InvalidCharacterError","InvalidCharacterError","InvalidCharacterError","TypeError"]}"#
    );
}

#[test]
#[serial]
fn pbkdf2_work_is_bounded_before_it_starts() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime should build");
    let snapshot = runtime
        .block_on(build_bootstrap_snapshot())
        .expect("bootstrap snapshot should build");
    let _context = runtime.enter();
    let mut js_runtime = new_runtime_from_snapshot(snapshot, false, ModuleRegistry::default())
        .expect("runtime should start from snapshot");
    let result = js_runtime
        .execute_script(
            "<dd:test>",
            r#"
            (async () => {
              const subtle = crypto.subtle;
              const key = await subtle.importKey("raw", new Uint8Array(16), "PBKDF2", false, ["deriveBits"]);
              const derive = (iterations, length) => subtle
                .deriveBits({ name: "PBKDF2", hash: "SHA-256", salt: new Uint8Array(16), iterations }, key, length)
                .then((bits) => `ok:${bits.byteLength}`, (error) => `${error.name}`);
              return [
                await derive(600_000, 256),
                // -8 converts to 4294967288 bits: half a gigabyte.
                await derive(1, -8),
                await derive(1, 16 * 1024 * 1024),
                await derive(4_000_000_000, 256),
                // Four blocks, one iteration past the 50,000,000-round limit.
                await derive(12_500_001, 1024),
              ].join(",");
            })()
            "#,
        )
        .expect("derive script should run");
    let started = std::time::Instant::now();
    let result = runtime
        .block_on(js_runtime.resolve(result))
        .expect("derivations settle");
    assert_eq!(
        string(&mut js_runtime, result),
        "ok:32,OperationError,OperationError,OperationError,OperationError"
    );
    assert!(
        started.elapsed() < std::time::Duration::from_secs(30),
        "refusals happen before any work: {:?}",
        started.elapsed()
    );
}

#[test]
#[serial]
fn web_crypto_covers_common_algorithms() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime should build");
    let snapshot = runtime
        .block_on(build_bootstrap_snapshot())
        .expect("bootstrap snapshot should build");
    let mut js_runtime = new_runtime_from_snapshot(snapshot, false, ModuleRegistry::default())
        .expect("runtime should start from snapshot");
    let result = js_runtime
        .execute_script(
            "<dd:test>",
            r#"
            (async () => {
              const subtle = crypto.subtle;
              const data = new TextEncoder().encode("dd");
              const results = [];

              const ed = await subtle.generateKey({ name: "Ed25519" }, true, ["sign", "verify"]);
              const edSignature = await subtle.sign("Ed25519", ed.privateKey, data);
              results.push(await subtle.verify("Ed25519", ed.publicKey, edSignature, data));
              const edSpki = await subtle.exportKey("spki", ed.publicKey);
              const edPublic = await subtle.importKey("spki", edSpki, "Ed25519", true, ["verify"]);
              results.push(await subtle.verify("Ed25519", edPublic, edSignature, data));
              const edPkcs8 = await subtle.exportKey("pkcs8", ed.privateKey);
              await subtle.importKey("pkcs8", edPkcs8, "Ed25519", true, ["sign"]);
              const edJwk = await subtle.exportKey("jwk", ed.privateKey);
              results.push(edJwk.crv === "Ed25519" && typeof edJwk.x === "string");

              const alice = await subtle.generateKey({ name: "X25519" }, true, ["deriveBits"]);
              const bob = await subtle.generateKey({ name: "X25519" }, true, ["deriveBits"]);
              const aliceShared = new Uint8Array(await subtle.deriveBits({ name: "X25519", public: bob.publicKey }, alice.privateKey, 256));
              const bobShared = new Uint8Array(await subtle.deriveBits({ name: "X25519", public: alice.publicKey }, bob.privateKey, 256));
              results.push(aliceShared.length === 32 && aliceShared.every((byte, index) => byte === bobShared[index]));
              const xSpki = await subtle.exportKey("spki", alice.publicKey);
              await subtle.importKey("spki", xSpki, { name: "X25519" }, true, []);

              const rsa = await subtle.generateKey(
                { name: "RSASSA-PKCS1-v1_5", modulusLength: 1024, publicExponent: new Uint8Array([1, 0, 1]), hash: "SHA-256" },
                true,
                ["sign", "verify"],
              );
              const rsaSignature = await subtle.sign("RSASSA-PKCS1-v1_5", rsa.privateKey, data);
              results.push(await subtle.verify("RSASSA-PKCS1-v1_5", rsa.publicKey, rsaSignature, data));

              const password = await subtle.importKey("raw", data, "PBKDF2", false, ["deriveBits"]);
              const pbkdf2 = await subtle.deriveBits({ name: "PBKDF2", hash: "SHA-256", salt: data, iterations: 1000 }, password, 256);
              results.push(pbkdf2.byteLength === 32);
              const ikm = await subtle.importKey("raw", data, "HKDF", false, ["deriveBits"]);
              const hkdf = await subtle.deriveBits({ name: "HKDF", hash: "SHA-256", salt: data, info: data }, ikm, 128);
              results.push(hkdf.byteLength === 16);

              const kek = await subtle.generateKey({ name: "AES-KW", length: 128 }, true, ["wrapKey", "unwrapKey"]);
              const aes = await subtle.generateKey({ name: "AES-GCM", length: 256 }, true, ["encrypt", "decrypt"]);
              const wrapped = await subtle.wrapKey("raw", aes, kek, "AES-KW");
              const unwrapped = await subtle.unwrapKey("raw", wrapped, kek, "AES-KW", "AES-GCM", true, ["encrypt"]);
              results.push(unwrapped.algorithm.length === 256);

              const jwk = await subtle.exportKey("jwk", aes);
              const reimported = await subtle.importKey("jwk", jwk, "AES-GCM", true, ["decrypt"]);
              const iv = new Uint8Array(12);
              const ciphertext = await subtle.encrypt({ name: "AES-GCM", iv }, aes, data);
              results.push(new TextDecoder().decode(await subtle.decrypt({ name: "AES-GCM", iv }, reimported, ciphertext)) === "dd");

              const other = await subtle.generateKey({ name: "AES-GCM", length: 256 }, false, ["decrypt"]);
              try {
                await subtle.decrypt({ name: "AES-GCM", iv }, other, ciphertext);
                results.push("decrypt with the wrong key succeeded");
              } catch (error) {
                results.push(error instanceof DOMException && error.name === "OperationError");
              }

              return results.join(",");
            })()
            "#,
        )
        .expect("crypto script should run");
    let result = runtime
        .block_on(js_runtime.resolve(result))
        .expect("crypto script should resolve");
    assert_eq!(
        string(&mut js_runtime, result),
        "true,true,true,true,true,true,true,true,true,true"
    );
}

/// A branded object whose description has no `type` makes the
/// deserializer read `type` through Object.prototype. With a getter
/// there, that ran JavaScript inside V8's no-JavaScript deserializer
/// scope, which V8 answers by aborting the process.
#[test]
#[serial]
fn structured_clone_survives_scripts_run_by_the_deserializer() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime should build");
    let snapshot = runtime
        .block_on(build_bootstrap_snapshot())
        .expect("bootstrap snapshot should build");
    let mut js_runtime = new_runtime_from_snapshot(snapshot, false, ModuleRegistry::default())
        .expect("runtime should start from snapshot");
    let result = js_runtime
        .execute_script(
            "<dd:test>",
            r#"
            const branded = { [Symbol.for("dd.hostObject")]() { return {}; } };
            Object.defineProperty(Object.prototype, "type", { configurable: true, get() { return "Blob"; } });
            let outcome;
            try {
              outcome = typeof structuredClone([branded]);
            } catch (error) {
              outcome = error.name;
            } finally {
              delete Object.prototype.type;
            }
            outcome
            "#,
        )
        .expect("structuredClone should not abort");
    let outcome = string(&mut js_runtime, result);
    assert!(
        ["object", "TypeError", "DataCloneError"].contains(&outcome.as_str()),
        "{outcome}"
    );
}

/// The AES-OCB cipher only takes 12-byte nonces, but any nonce of 1 to
/// 15 bytes reached `GenericArray::from_slice`, which panicked on the
/// blocking thread; `.await.unwrap()` then re-raised the panic on the
/// isolate's thread, from plain `crypto.subtle.encrypt` calls.
#[test]
#[serial]
fn web_crypto_aes_ocb_rejects_nonces_it_cannot_use() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime should build");
    let snapshot = runtime
        .block_on(build_bootstrap_snapshot())
        .expect("bootstrap snapshot should build");
    let mut js_runtime = new_runtime_from_snapshot(snapshot, false, ModuleRegistry::default())
        .expect("runtime should start from snapshot");
    // The script starts async ops before its first await.
    let _context = runtime.enter();
    let result = js_runtime
        .execute_script(
            "<dd:test>",
            r#"
            (async () => {
              const subtle = crypto.subtle;
              const data = new TextEncoder().encode("dd");
              const key = await subtle.generateKey({ name: "AES-OCB", length: 128 }, false, ["encrypt", "decrypt"]);
              const iv = new Uint8Array(12);
              const sealed = await subtle.encrypt({ name: "AES-OCB", iv }, key, data);
              const results = [new TextDecoder().decode(await subtle.decrypt({ name: "AES-OCB", iv }, key, sealed))];
              for (const length of [1, 6, 8, 11, 13, 15]) {
                for (const [op, input] of [["encrypt", data], ["decrypt", sealed]]) {
                  try {
                    await subtle[op]({ name: "AES-OCB", iv: new Uint8Array(length) }, key, input);
                    results.push(`${op} with ${length} bytes succeeded`);
                  } catch (error) {
                    results.push(error instanceof DOMException ? error.name : String(error));
                  }
                }
              }
              return results.join(",");
            })()
            "#,
        )
        .expect("crypto script should run");
    let result = runtime
        .block_on(js_runtime.resolve(result))
        .expect("crypto script should resolve");
    let expected = std::iter::once("dd")
        .chain(std::iter::repeat_n("NotSupportedError", 12))
        .collect::<Vec<_>>()
        .join(",");
    assert_eq!(string(&mut js_runtime, result), expected);
}

#[test]
#[serial]
fn webassembly_streaming_compiles_responses() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime should build");
    let snapshot = runtime
        .block_on(build_bootstrap_snapshot())
        .expect("bootstrap snapshot should build");
    let mut js_runtime = new_runtime_from_snapshot(snapshot, false, ModuleRegistry::default())
        .expect("runtime should start from snapshot");
    // (module (func (export "add") (param i32 i32) (result i32) local.get 0 local.get 1 i32.add))
    let result = js_runtime
        .execute_script(
            "<dd:test>",
            r#"
            (async () => {
              const bytes = new Uint8Array([0,97,115,109,1,0,0,0,1,7,1,96,2,127,127,1,127,3,2,1,0,7,7,1,3,97,100,100,0,0,10,9,1,7,0,32,0,32,1,106,11]);
              const wasm = (body) => new Response(body, { headers: { "content-type": "application/wasm" } });
              const { instance } = await WebAssembly.instantiateStreaming(wasm(bytes));
              const module = await WebAssembly.compileStreaming(Promise.resolve(wasm(bytes)));
              let rejected = "no";
              try {
                await WebAssembly.compileStreaming(new Response(bytes));
              } catch (error) {
                rejected = error.message;
              }
              return [instance.exports.add(19, 23), module instanceof WebAssembly.Module, rejected].join("|");
            })()
            "#,
        )
        .expect("wasm script should run");
    let result = runtime
        .block_on(js_runtime.resolve(result))
        .expect("wasm script should resolve");
    assert_eq!(
        string(&mut js_runtime, result),
        "42|true|Invalid WebAssembly content type"
    );
}

#[test]
#[serial]
fn unscoped_fetch_is_a_development_runtime_capability() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime should build");
    let _entered = runtime.enter();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("listener");
    let address = listener.local_addr().expect("address");
    let server = std::thread::spawn(move || {
        use std::io::{Read, Write};
        let (mut socket, _) = listener.accept().expect("accept");
        let mut request = [0u8; 4096];
        let _ = socket.read(&mut request);
        socket
            .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 7\r\nconnection: close\r\n\r\nmodules")
            .expect("respond");
    });
    let snapshot = runtime
        .block_on(build_bootstrap_snapshot())
        .expect("bootstrap snapshot should build");
    let source = format!(
        r#"
        const body = await globalThis.__dd_raw_host_fetch("http://{address}/module").then((response) => response.text());
        export default {{ fetch() {{ return new Response(body); }} }};
        "#
    );

    let dev = IsolatePolicy {
        unscoped_fetch: true,
        ..IsolatePolicy::default()
    };
    let mut js_runtime =
        new_runtime_from_snapshot_with_heap_limit(snapshot, dev, 0, ModuleRegistry::default())
            .expect("dev runtime should start");
    runtime
        .block_on(load_worker(&mut js_runtime, &source))
        .expect("module evaluation should fetch from the dev server");
    let body = execute_internal(
        &mut js_runtime,
        "return Promise.resolve(__bootstrap.dd.worker.fetch(new Request('http://worker/'))).then((response) => response.text())",
    );
    let body = runtime
        .block_on(js_runtime.resolve(body))
        .expect("worker fetch should resolve");
    assert_eq!(string(&mut js_runtime, body), "modules");
    server.join().expect("server thread");
    drop(js_runtime);

    let mut js_runtime = new_runtime_from_snapshot(snapshot, false, ModuleRegistry::default())
        .expect("runtime should start");
    let refused = execute_internal(
        &mut js_runtime,
        r#"
        return typeof globalThis.__dd_raw_host_fetch === "undefined"
          ? __bootstrap.dd.unscopedFetch("http://127.0.0.1:9/").then(() => "fetched", (error) => error.message)
          : "exposed";
        "#,
    );
    let refused = runtime
        .block_on(js_runtime.resolve(refused))
        .expect("probe should resolve");
    assert_eq!(
        string(&mut js_runtime, refused),
        "fetch outside a request is only available in the development runtime"
    );
}

#[test]
fn bare_imports_need_a_relative_prefix() {
    assert_eq!(
        resolve_import("./b.js", "file:///dd/a.js")
            .unwrap()
            .as_str(),
        "file:///dd/b.js"
    );
    assert_eq!(
        resolve_import("https://example.com/x.js", "file:///dd/a.js")
            .unwrap()
            .as_str(),
        "https://example.com/x.js"
    );
    assert_eq!(
        resolve_import("lodash", "file:///dd/a.js").unwrap_err(),
        "Relative import path \"lodash\" not prefixed with / or ./ or ../ from \"file:///dd/a.js\""
    );
}
