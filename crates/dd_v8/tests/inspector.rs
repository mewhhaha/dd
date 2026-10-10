use dd_v8::{InspectorHandle, InspectorSession, JsRuntime, RuntimeHandle, RuntimeOptions};
use serde_json::{Value, json};
use std::future::poll_fn;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use std::thread::JoinHandle;
use std::time::Duration;

fn runtime() -> JsRuntime {
    JsRuntime::new(RuntimeOptions {
        ops: dd_v8::builtins::ops(),
        ..Default::default()
    })
    .expect("runtime")
}

fn string(runtime: &mut JsRuntime, value: dd_v8::v8::Global<dd_v8::v8::Value>) -> String {
    dd_v8::scope!(scope, runtime);
    let value = dd_v8::v8::Local::new(scope, value);
    value.to_rust_string_lossy(scope)
}

fn eval(runtime: &mut JsRuntime, source: &str) -> String {
    let value = runtime.execute_script("<test>", source).expect("script");
    string(runtime, value)
}

/// One turn of the event loop, which dispatches queued inspector messages.
fn turn(runtime: &mut JsRuntime) {
    let waker = futures_util::task::noop_waker();
    let _ = runtime.poll_event_loop(&mut Context::from_waker(&waker));
}

fn drain(session: &mut InspectorSession) -> Vec<Value> {
    std::iter::from_fn(|| session.try_recv())
        .map(|message| serde_json::from_str(&message).expect("protocol JSON"))
        .collect()
}

fn send(session: &InspectorSession, id: u32, method: &str, params: Value) {
    session.send(json!({ "id": id, "method": method, "params": params }).to_string());
}

/// Reads messages until one matches, skipping the rest.
async fn next(session: &mut InspectorSession, matches: impl Fn(&Value) -> bool) -> Value {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let message = session.recv().await.expect("session open");
            let message: Value = serde_json::from_str(&message).expect("protocol JSON");
            if matches(&message) {
                return message;
            }
        }
    })
    .await
    .expect("expected protocol message")
}

async fn response(session: &mut InspectorSession, id: u32) -> Value {
    next(session, |message| message["id"] == id).await
}

async fn notification(session: &mut InspectorSession, method: &str) -> Value {
    next(session, |message| message["method"] == method).await
}

#[test]
fn sessions_evaluate_and_see_console_calls() {
    let mut runtime = runtime();
    runtime
        .execute_with_ops("<ops>", "globalThis.ops = ops; globalThis.calls = [];")
        .expect("expose ops");
    let record = "(...args) => calls.push(args.join(' '))";
    let inspector = runtime.enable_inspector("test context").expect("inspector");

    // Not attached yet: the call skips V8's console, which would otherwise
    // keep the message and replay it once Runtime.enable arrives.
    eval(
        &mut runtime,
        &format!("ops.op_call_console(console.log, {record}, 'before', 'attach')"),
    );

    let mut session = inspector.connect();
    send(&session, 1, "Runtime.enable", json!({}));
    send(
        &session,
        2,
        "Runtime.evaluate",
        json!({ "expression": "'é' + (40 + 2)" }),
    );
    turn(&mut runtime);
    let messages = drain(&mut session);
    let created = messages
        .iter()
        .find(|message| message["method"] == "Runtime.executionContextCreated")
        .expect("context created");
    assert_eq!(created["params"]["context"]["name"], "test context");
    let evaluated = messages
        .iter()
        .find(|message| message["id"] == 2)
        .expect("evaluate response");
    assert_eq!(evaluated["result"]["result"]["value"], "é42");
    assert!(
        !messages
            .iter()
            .any(|message| message["method"] == "Runtime.consoleAPICalled"),
        "no console call before a session attached: {messages:?}"
    );

    eval(&mut runtime, "console.log('plain', { answer: 42 })");
    eval(
        &mut runtime,
        &format!("ops.op_call_console(console.warn, {record}, 'after', 'attach')"),
    );
    let messages = drain(&mut session);
    let console = messages
        .iter()
        .filter(|message| message["method"] == "Runtime.consoleAPICalled")
        .map(|message| &message["params"])
        .collect::<Vec<_>>();
    assert_eq!(console.len(), 2, "{messages:?}");
    assert_eq!(console[0]["type"], "log");
    assert_eq!(console[0]["args"][0]["value"], "plain");
    assert_eq!(console[0]["args"][1]["type"], "object");
    assert_eq!(console[1]["type"], "warning");
    assert_eq!(console[1]["args"][0]["value"], "after");
    assert_eq!(
        eval(&mut runtime, "calls.join('|')"),
        "before attach|after attach"
    );
}

#[test]
fn snapshot_contexts_keep_v8s_console() {
    let mut creator = JsRuntime::new_for_snapshot(RuntimeOptions::default()).expect("creator");
    assert_eq!(
        eval(&mut creator, "typeof console + ' ' + typeof console.log"),
        "object function"
    );
    creator
        .execute_script("<keep>", "globalThis.kept = console.log;")
        .expect("keep console");
    let snapshot: &'static [u8] = Box::leak(creator.snapshot().expect("snapshot"));
    let mut restored = JsRuntime::new(RuntimeOptions {
        startup_snapshot: Some(snapshot),
        ..Default::default()
    })
    .expect("restored");
    assert_eq!(eval(&mut restored, "String(kept === console.log)"), "true");
}

struct IsolateThread {
    inspector: InspectorHandle,
    handle: RuntimeHandle,
    stop: Arc<AtomicBool>,
    thread: JoinHandle<String>,
}

/// A runtime on its own thread that waits for a debugger, runs `script`,
/// then serves the inspector until stopped, returning the script's result.
fn isolate_thread(script: &'static str) -> IsolateThread {
    let (sender, receiver) = std::sync::mpsc::channel();
    let stop = Arc::new(AtomicBool::new(false));
    let thread_stop = Arc::clone(&stop);
    let thread = std::thread::spawn(move || {
        let executor = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("executor");
        executor.block_on(async move {
            let mut runtime = runtime();
            let inspector = runtime.enable_inspector("paused").expect("inspector");
            sender
                .send((inspector, runtime.handle()))
                .expect("send handles");
            runtime.wait_for_debugger();
            let result = match runtime.execute_script("file:///pause.js", script) {
                Ok(value) => string(&mut runtime, value),
                Err(error) => format!("error: {error}"),
            };
            poll_fn(|cx| {
                let _ = runtime.poll_event_loop(cx);
                if thread_stop.load(Ordering::Acquire) {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            })
            .await;
            result
        })
    });
    let (inspector, handle) = receiver.recv().expect("isolate handles");
    IsolateThread {
        inspector,
        handle,
        stop,
        thread,
    }
}

fn block_on<T>(future: impl Future<Output = T>) -> T {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("executor")
        .block_on(future)
}

#[test]
fn breakpoints_pause_the_isolate_until_resumed() {
    let isolate = isolate_thread("let x = 20;\ndebugger;\nconsole.log('resumed', x);\nx * 2 + 2");
    let mut session = isolate.inspector.connect();
    block_on(async {
        send(&session, 1, "Runtime.enable", json!({}));
        send(&session, 2, "Debugger.enable", json!({}));
        response(&mut session, 2).await;
        send(&session, 3, "Runtime.runIfWaitingForDebugger", json!({}));

        let paused = notification(&mut session, "Debugger.paused").await;
        let frame = &paused["params"]["callFrames"][0];
        assert_eq!(frame["location"]["lineNumber"], 1);
        // The paused thread still answers.
        send(
            &session,
            4,
            "Debugger.evaluateOnCallFrame",
            json!({ "callFrameId": frame["callFrameId"], "expression": "x + 1" }),
        );
        assert_eq!(
            response(&mut session, 4).await["result"]["result"]["value"],
            21
        );
        send(&session, 5, "Debugger.resume", json!({}));
        let logged = notification(&mut session, "Runtime.consoleAPICalled").await;
        assert_eq!(logged["params"]["args"][0]["value"], "resumed");

        // A pause inside a dispatched message nests in that dispatch.
        send(
            &session,
            6,
            "Runtime.evaluate",
            json!({ "expression": "debugger; 'nested'" }),
        );
        notification(&mut session, "Debugger.paused").await;
        send(&session, 7, "Debugger.resume", json!({}));
        assert_eq!(
            response(&mut session, 6).await["result"]["result"]["value"],
            "nested"
        );
    });
    isolate.stop.store(true, Ordering::Release);
    drop(session);
    assert_eq!(isolate.thread.join().expect("isolate thread"), "42");
}

#[test]
fn terminating_lets_go_of_a_paused_isolate() {
    let isolate = isolate_thread("debugger;\nwhile (true) {}");
    let mut session = isolate.inspector.connect();
    block_on(async {
        send(&session, 1, "Debugger.enable", json!({}));
        send(&session, 2, "Runtime.runIfWaitingForDebugger", json!({}));
        notification(&mut session, "Debugger.paused").await;
    });
    isolate.stop.store(true, Ordering::Release);
    assert!(isolate.handle.terminate_execution());
    let result = isolate.thread.join().expect("isolate thread");
    assert!(result.starts_with("error:"), "{result}");
    // The session ends with its runtime.
    block_on(async { while session.recv().await.is_some() {} });
}

#[test]
fn terminating_lets_go_of_an_isolate_waiting_for_a_debugger() {
    let isolate = isolate_thread("'ran'");
    isolate.stop.store(true, Ordering::Release);
    assert!(isolate.handle.terminate_execution());
    let result = isolate.thread.join().expect("isolate thread");
    assert!(result.starts_with("error:"), "{result}");
}

#[test]
fn busy_isolates_still_take_messages() {
    let isolate = isolate_thread("let n = 0;\nwhile (true) { n++; }");
    let mut session = isolate.inspector.connect();
    block_on(async {
        send(&session, 1, "Debugger.enable", json!({}));
        send(&session, 2, "Runtime.runIfWaitingForDebugger", json!({}));
        response(&mut session, 2).await;
        // Once the loop runs, only an interrupt can dispatch this.
        tokio::time::sleep(Duration::from_millis(100)).await;
        send(&session, 3, "Debugger.pause", json!({}));
        let paused = notification(&mut session, "Debugger.paused").await;
        assert_eq!(
            paused["params"]["callFrames"][0]["location"]["lineNumber"],
            1
        );
    });
    isolate.stop.store(true, Ordering::Release);
    assert!(isolate.handle.terminate_execution());
    let result = isolate.thread.join().expect("isolate thread");
    assert!(result.starts_with("error:"), "{result}");
}
