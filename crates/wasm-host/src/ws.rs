//! Websocket event dispatch for Perry wasm workers.
//!
//! Each worker gets one dedicated long-lived instance for websocket traffic;
//! every open/message/close event across all of the worker's connections is
//! dispatched to it sequentially, so module-level guest state spans
//! connections the way it does inside a single V8 isolate. Outbound sends go
//! through the per-worker connection registry ([`crate::state::WsConnections`])
//! and are therefore also available to regular fetch handlers.

use crate::bridge::call_closure;
use crate::engine::WorkerModule;
use crate::heap::HostValue;
use crate::nanbox::{JsValue, decode, encode_number};
use common::{PlatformError, Result};
use std::sync::mpsc;

/// Frames leaving the host toward one websocket client.
#[derive(Debug, Clone, PartialEq)]
pub enum WsOutbound {
    Text(String),
    Close,
}

/// Events arriving from websocket clients, dispatched to the guest.
#[derive(Debug)]
pub enum WsEvent {
    Open { connection: u64, url: String },
    Message { connection: u64, text: String },
    Closed { connection: u64 },
}

/// Run the worker's websocket dispatcher until every event sender is gone.
/// A guest failure logs and replaces the instance rather than killing the
/// remaining connections.
pub(crate) fn run_dispatcher(module: &WorkerModule, events: mpsc::Receiver<WsEvent>) {
    let mut instance = None;
    while let Ok(event) = events.recv() {
        if instance.is_none() {
            instance = match module.websocket_instance() {
                Ok(ready) => Some(ready),
                Err(error) => {
                    tracing::error!("websocket instance failed to start: {error}");
                    continue;
                }
            };
        }
        let Some(ready) = instance.as_mut() else {
            continue;
        };
        if let Err(error) = dispatch_event(ready, &event) {
            tracing::error!("websocket handler failed on {event:?}: {error}");
            instance = None;
        }
    }
}

fn dispatch_event(ready: &mut crate::engine::ReadyInstance, event: &WsEvent) -> Result<()> {
    let store = ready.store_mut();
    let handlers = store
        .data()
        .ws_handlers
        .ok_or_else(|| PlatformError::bad_request("worker never called dd_ws_register"))?;

    let (name, args): (&str, Vec<u64>) = match event {
        WsEvent::Open { connection, url } => {
            let url_bits = store.data_mut().heap.intern_bits(url.clone());
            ("open", vec![encode_number(*connection as f64), url_bits])
        }
        WsEvent::Message { connection, text } => {
            let text_bits = store.data_mut().heap.intern_bits(text.clone());
            (
                "message",
                vec![encode_number(*connection as f64), text_bits],
            )
        }
        WsEvent::Closed { connection } => ("close", vec![encode_number(*connection as f64)]),
    };

    let callback = {
        let state = store.data();
        let JsValue::Handle(id) = decode(handlers) else {
            return Err(PlatformError::runtime("ws handlers value is not an object"));
        };
        match state.heap.handle(id) {
            Some(HostValue::Object(object)) => object.get(name),
            _ => None,
        }
    };
    // A worker may register only the callbacks it needs.
    let Some(callback) = callback else {
        return Ok(());
    };
    if matches!(decode(callback), JsValue::Undefined | JsValue::Null) {
        return Ok(());
    }

    call_closure(&mut *store, callback, &args)
        .map_err(|error| PlatformError::runtime(format!("{error:#}")))?;
    if let Some(exception) = store.data_mut().pending_exception.take() {
        let message = store.data().heap.display(exception);
        return Err(PlatformError::runtime(format!(
            "websocket {name} handler threw: {message}"
        )));
    }
    crate::engine::pump_microtasks(store)?;
    Ok(())
}
