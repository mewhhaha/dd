//! Engine-level websocket tests: drive the dispatcher through the same
//! registry and event channel the server uses, without a network in between.

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::UnboundedReceiver;
use wasm_host::{WorkerModule, WsEvent, WsOutbound};

fn chat_module() -> Arc<WorkerModule> {
    let path = format!("{}/fixtures/chat_worker.wasm", env!("CARGO_MANIFEST_DIR"));
    let bytes = std::fs::read(&path).unwrap_or_else(|e| panic!("missing fixture {path}: {e}"));
    Arc::new(WorkerModule::from_bytes(&bytes).expect("module"))
}

fn recv(receiver: &mut UnboundedReceiver<WsOutbound>) -> WsOutbound {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        match receiver.try_recv() {
            Ok(frame) => return frame,
            Err(_) if std::time::Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(5));
            }
            Err(error) => panic!("no websocket frame within 5s: {error}"),
        }
    }
}

#[test]
fn chat_worker_welcomes_broadcasts_and_disconnects() {
    let module = chat_module();
    let events = module.websocket_events();

    let (alice, mut alice_rx) = module.ws_connections().register();
    let (bob, mut bob_rx) = module.ws_connections().register();

    events
        .send(WsEvent::Open {
            connection: alice,
            url: "http://w.local/chat".into(),
        })
        .expect("dispatcher alive");
    assert_eq!(
        recv(&mut alice_rx),
        WsOutbound::Text(format!("welcome {alice}"))
    );

    events
        .send(WsEvent::Open {
            connection: bob,
            url: "http://w.local/chat".into(),
        })
        .expect("dispatcher alive");
    assert_eq!(
        recv(&mut bob_rx),
        WsOutbound::Text(format!("welcome {bob}"))
    );

    events
        .send(WsEvent::Message {
            connection: alice,
            text: "hi all".into(),
        })
        .expect("dispatcher alive");
    assert_eq!(
        recv(&mut alice_rx),
        WsOutbound::Text(format!("{alice}: hi all"))
    );
    assert_eq!(
        recv(&mut bob_rx),
        WsOutbound::Text(format!("{alice}: hi all"))
    );

    // "quit" asks the worker to close the sender's connection.
    events
        .send(WsEvent::Message {
            connection: bob,
            text: "quit".into(),
        })
        .expect("dispatcher alive");
    assert_eq!(recv(&mut bob_rx), WsOutbound::Close);

    // After bob's close event, broadcasts reach only alice.
    module.ws_connections().remove(bob);
    events
        .send(WsEvent::Closed { connection: bob })
        .expect("dispatcher alive");
    events
        .send(WsEvent::Message {
            connection: alice,
            text: "still here".into(),
        })
        .expect("dispatcher alive");
    assert_eq!(
        recv(&mut alice_rx),
        WsOutbound::Text(format!("{alice}: still here"))
    );
    assert!(
        bob_rx.try_recv().is_err(),
        "bob must not receive after close"
    );
}

#[test]
fn fetch_handler_sees_connection_count_from_ws_instance_state() {
    // The fetch pool and the websocket dispatcher use separate instances;
    // module state is per-instance, so the fetch handler's count stays 0.
    // This documents the isolation boundary rather than papering over it.
    let module = chat_module();
    let events = module.websocket_events();
    let (conn, mut rx) = module.ws_connections().register();
    events
        .send(WsEvent::Open {
            connection: conn,
            url: "http://w.local/".into(),
        })
        .expect("dispatcher alive");
    assert_eq!(recv(&mut rx), WsOutbound::Text(format!("welcome {conn}")));

    let output = module
        .invoke(
            common::WorkerInvocation {
                method: "GET".into(),
                url: "http://w.local/".into(),
                headers: Vec::new(),
                body: Vec::new(),
                request_id: "t".into(),
            },
            wasm_host::InvokeOptions::default(),
        )
        .expect("invoke");
    assert_eq!(output.body, b"chat:0");
}
