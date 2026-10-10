//! Chrome DevTools debugging through V8's inspector.
//!
//! The client callbacks, the default-context lookup, and the pause loop are
//! ported from deno_core's `inspector.rs` (Copyright 2018-2026 the Deno
//! authors, MIT license), cut down to one context and plain sessions.
//!
//! A runtime's inspector lives on its isolate's thread. Other threads reach
//! it through an [`InspectorHandle`], which opens [`InspectorSession`]s.
//! What a session sends queues in an inbox the isolate's thread drains:
//! from the event loop while the isolate idles, from an interrupt while
//! JavaScript runs, from the pause loop while the debugger holds the thread
//! at a breakpoint, and while the runtime waits for a debugger to attach.
//! Responses and notifications go back over the session's channel.

use futures_util::task::AtomicWaker;
use std::cell::{Cell, RefCell};
use std::collections::{HashMap, VecDeque};
use std::ffi::c_void;
use std::rc::{Rc, Weak};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use tokio::sync::mpsc;
use v8::inspector::{
    Channel, ChannelImpl, StringBuffer, StringView, V8Inspector, V8InspectorClient,
    V8InspectorClientImpl, V8InspectorClientTrustLevel, V8InspectorSession,
};

/// The one context group: a runtime has one context.
const CONTEXT_GROUP_ID: i32 = 1;

enum Event {
    Connect {
        id: u64,
        outbound: mpsc::UnboundedSender<String>,
    },
    Message {
        id: u64,
        message: String,
    },
    Disconnect {
        id: u64,
    },
}

#[derive(Default)]
struct Inbox {
    events: VecDeque<Event>,
    /// Set when the runtime goes away or is terminated: nothing queues and
    /// nothing blocks any more.
    closed: bool,
}

/// What the isolate's thread shares with every other thread.
struct Shared {
    inbox: Mutex<Inbox>,
    /// Signalled whenever the inbox changes, for a thread blocked in the
    /// pause loop or waiting for a debugger.
    changed: Condvar,
    /// The runtime's event loop waker, for an idle isolate.
    waker: Arc<AtomicWaker>,
    /// Interrupts the isolate, for one busy running JavaScript.
    isolate: v8::IsolateHandle,
    interrupt_requested: AtomicBool,
    next_session: AtomicU64,
}

impl Shared {
    fn inbox(&self) -> MutexGuard<'_, Inbox> {
        self.inbox.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn push(&self, event: Event) {
        {
            let mut inbox = self.inbox();
            if inbox.closed {
                // Dropping a connect drops its sender, which ends the session.
                return;
            }
            inbox.events.push_back(event);
        }
        self.changed.notify_all();
        self.waker.wake();
        if !self.interrupt_requested.swap(true, Ordering::AcqRel) {
            self.isolate
                .request_interrupt(handle_interrupt, std::ptr::null_mut());
        }
    }

    fn pop(&self) -> Option<Event> {
        let mut inbox = self.inbox();
        if inbox.closed {
            return None;
        }
        inbox.events.pop_front()
    }

    /// Blocks until an event queues or the inbox closes.
    fn wait(&self) {
        let inbox = self.inbox();
        if inbox.events.is_empty() && !inbox.closed {
            drop(
                self.changed
                    .wait(inbox)
                    .unwrap_or_else(PoisonError::into_inner),
            );
        }
    }

    fn close(&self) {
        let events = {
            let mut inbox = self.inbox();
            inbox.closed = true;
            std::mem::take(&mut inbox.events)
        };
        drop(events);
        self.changed.notify_all();
        self.waker.wake();
    }

    fn is_closed(&self) -> bool {
        self.inbox().closed
    }
}

/// Opens DevTools sessions on a runtime's inspector from any thread.
#[derive(Clone)]
pub struct InspectorHandle {
    shared: Arc<Shared>,
}

impl InspectorHandle {
    /// Opens a session. It starts on the isolate's thread the next time that
    /// thread drains its inspector inbox; messages sent before then wait.
    pub fn connect(&self) -> InspectorSession {
        let id = self.shared.next_session.fetch_add(1, Ordering::Relaxed);
        let (outbound, receiver) = mpsc::unbounded_channel();
        self.shared.push(Event::Connect { id, outbound });
        InspectorSession {
            id,
            shared: Arc::clone(&self.shared),
            receiver,
        }
    }

    /// Ends every wait for a debugger: a paused or waiting isolate runs on,
    /// and the inspector neither pauses nor dispatches again.
    pub(crate) fn close(&self) {
        self.shared.close();
    }
}

impl std::fmt::Debug for InspectorHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InspectorHandle").finish_non_exhaustive()
    }
}

/// One DevTools session: Chrome DevTools Protocol messages in, responses and
/// notifications out. Dropping it disconnects.
pub struct InspectorSession {
    id: u64,
    shared: Arc<Shared>,
    receiver: mpsc::UnboundedReceiver<String>,
}

impl InspectorSession {
    /// Queues a protocol message for the isolate's thread.
    pub fn send(&self, message: impl Into<String>) {
        self.shared.push(Event::Message {
            id: self.id,
            message: message.into(),
        });
    }

    /// The next response or notification; `None` once the runtime, or its
    /// inspector, is gone.
    pub async fn recv(&mut self) -> Option<String> {
        self.receiver.recv().await
    }

    /// The next response or notification, if one is waiting.
    pub fn try_recv(&mut self) -> Option<String> {
        self.receiver.try_recv().ok()
    }
}

impl Drop for InspectorSession {
    fn drop(&mut self) {
        self.shared.push(Event::Disconnect { id: self.id });
    }
}

impl std::fmt::Debug for InspectorSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InspectorSession")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

/// The isolate thread's side of a runtime's inspector.
pub(crate) struct Inspector {
    state: Rc<InspectorState>,
}

struct InspectorState {
    shared: Arc<Shared>,
    v8: RefCell<Option<Rc<V8Inspector>>>,
    sessions: RefCell<HashMap<u64, Rc<Session>>>,
    session_count: Cell<usize>,
    /// Protocol messages being dispatched right now, nested.
    dispatching: Cell<u32>,
    /// Nested pause loops; `quit_message_loop_on_pause` ends the innermost.
    pause_depth: Cell<u32>,
    waiting_for_debugger: Cell<bool>,
    context: v8::Global<v8::Context>,
    isolate: v8::UnsafeRawIsolatePtr,
}

struct Session {
    v8: V8InspectorSession,
}

impl Inspector {
    pub(crate) fn new(
        scope: &mut v8::PinScope<'_, '_>,
        context: v8::Local<v8::Context>,
        waker: Arc<AtomicWaker>,
        name: &str,
    ) -> Self {
        let shared = Arc::new(Shared {
            inbox: Mutex::new(Inbox::default()),
            changed: Condvar::new(),
            waker,
            isolate: scope.thread_safe_handle(),
            interrupt_requested: AtomicBool::new(false),
            next_session: AtomicU64::new(1),
        });
        let state = Rc::new(InspectorState {
            shared,
            v8: RefCell::new(None),
            sessions: RefCell::new(HashMap::new()),
            session_count: Cell::new(0),
            dispatching: Cell::new(0),
            pause_depth: Cell::new(0),
            waiting_for_debugger: Cell::new(false),
            context: v8::Global::new(scope, context),
            // SAFETY: the pointer is only turned back into an isolate on the
            // isolate's own thread, while the runtime owns it.
            isolate: unsafe { scope.as_raw_isolate_ptr() },
        });
        let client = V8InspectorClient::new(Box::new(Client(Rc::downgrade(&state))));
        let v8_inspector = Rc::new(V8Inspector::create(scope, client));
        let name = name.encode_utf16().collect::<Vec<_>>();
        v8_inspector.context_created(
            context,
            CONTEXT_GROUP_ID,
            StringView::from(&name[..]),
            StringView::from(&br#"{"isDefault":true,"type":"default"}"#[..]),
        );
        *state.v8.borrow_mut() = Some(v8_inspector);
        Self { state }
    }

    pub(crate) fn handle(&self) -> InspectorHandle {
        InspectorHandle {
            shared: Arc::clone(&self.state.shared),
        }
    }

    pub(crate) fn has_sessions(&self) -> bool {
        self.state.session_count.get() > 0
    }

    /// Dispatches what the inbox holds, unless the thread is already inside
    /// a dispatch or a pause, whose own loop drains it.
    pub(crate) fn poll(&self) {
        let state = &self.state;
        if state.dispatching.get() > 0
            || state.pause_depth.get() > 0
            || state.waiting_for_debugger.get()
        {
            return;
        }
        while state.pump_one() {}
    }

    /// Blocks until a session sends `Runtime.runIfWaitingForDebugger`, or
    /// the inspector closes, dispatching every message meanwhile.
    pub(crate) fn wait_for_debugger(&self) {
        let state = &self.state;
        state.waiting_for_debugger.set(true);
        while state.waiting_for_debugger.get() && !state.shared.is_closed() {
            if !state.pump_one() {
                state.shared.wait();
            }
        }
        state.waiting_for_debugger.set(false);
    }

    pub(crate) fn context_destroyed(&self, context: v8::Local<v8::Context>) {
        if let Some(v8_inspector) = self.state.v8.borrow().as_ref() {
            v8_inspector.context_destroyed(context);
        }
    }
}

impl Drop for Inspector {
    fn drop(&mut self) {
        self.state.shared.close();
        // V8 requires every session to go before the inspector itself.
        let sessions = std::mem::take(&mut *self.state.sessions.borrow_mut());
        self.state.session_count.set(0);
        drop(sessions);
        let v8_inspector = self.state.v8.borrow_mut().take();
        drop(v8_inspector);
    }
}

impl InspectorState {
    /// Handles the next queued event; false when there was none.
    fn pump_one(&self) -> bool {
        let Some(event) = self.shared.pop() else {
            return false;
        };
        match event {
            Event::Connect { id, outbound } => {
                let v8_inspector = self.v8.borrow().clone();
                if let Some(v8_inspector) = v8_inspector {
                    let session = v8_inspector.connect(
                        CONTEXT_GROUP_ID,
                        Channel::new(Box::new(SessionChannel(outbound))),
                        StringView::empty(),
                        V8InspectorClientTrustLevel::FullyTrusted,
                    );
                    self.sessions
                        .borrow_mut()
                        .insert(id, Rc::new(Session { v8: session }));
                    self.session_count.set(self.session_count.get() + 1);
                }
            }
            Event::Message { id, message } => {
                let session = self.sessions.borrow().get(&id).cloned();
                if let Some(session) = session {
                    let message = message.encode_utf16().collect::<Vec<_>>();
                    self.dispatching.set(self.dispatching.get() + 1);
                    session
                        .v8
                        .dispatch_protocol_message(StringView::from(&message[..]));
                    self.dispatching.set(self.dispatching.get() - 1);
                }
            }
            Event::Disconnect { id } => {
                let session = self.sessions.borrow_mut().remove(&id);
                if session.is_some() {
                    self.session_count.set(self.session_count.get() - 1);
                }
                // Dropping the last debugging session resumes a paused
                // isolate, through `quit_message_loop_on_pause`.
                drop(session);
            }
        }
        true
    }

    fn run_pause_loop(&self) {
        let depth = self.pause_depth.get() + 1;
        self.pause_depth.set(depth);
        while self.pause_depth.get() >= depth {
            if self.shared.is_closed() || self.session_count.get() == 0 {
                break;
            }
            if !self.pump_one() {
                self.shared.wait();
            }
        }
        self.pause_depth.set(depth - 1);
    }
}

struct Client(Weak<InspectorState>);

impl V8InspectorClientImpl for Client {
    fn run_message_loop_on_pause(&self, _context_group_id: i32) {
        if let Some(state) = self.0.upgrade() {
            state.run_pause_loop();
        }
    }

    fn quit_message_loop_on_pause(&self) {
        if let Some(state) = self.0.upgrade() {
            state
                .pause_depth
                .set(state.pause_depth.get().saturating_sub(1));
        }
    }

    fn run_if_waiting_for_debugger(&self, _context_group_id: i32) {
        if let Some(state) = self.0.upgrade() {
            state.waiting_for_debugger.set(false);
        }
    }

    fn ensure_default_context_in_group(
        &self,
        _context_group_id: i32,
    ) -> Option<v8::Local<'_, v8::Context>> {
        let state = self.0.upgrade()?;
        // SAFETY: V8 calls this on the isolate's thread.
        let mut isolate = unsafe { v8::Isolate::from_raw_isolate_ptr(state.isolate) };
        // SAFETY: a callback scope on an isolate opens no handle scope, so
        // the local lands in the handle scope V8 opened around this call.
        v8::callback_scope!(unsafe scope, &mut isolate);
        let context = v8::Local::new(scope, &state.context);
        // SAFETY: as above, the local lives as long as V8's caller needs it.
        Some(unsafe { context.extend_lifetime_unchecked() })
    }
}

struct SessionChannel(mpsc::UnboundedSender<String>);

impl SessionChannel {
    fn send(&self, message: v8::UniquePtr<StringBuffer>) {
        if let Some(message) = message.as_ref() {
            let _ = self.0.send(message.string().to_string());
        }
    }
}

impl ChannelImpl for SessionChannel {
    fn send_response(&self, _call_id: i32, message: v8::UniquePtr<StringBuffer>) {
        self.send(message);
    }

    fn send_notification(&self, message: v8::UniquePtr<StringBuffer>) {
        self.send(message);
    }

    fn flush_protocol_notifications(&self) {}
}

/// Drains the inbox while JavaScript runs, so a busy isolate still answers
/// (and can be paused).
unsafe extern "C" fn handle_interrupt(isolate: v8::UnsafeRawIsolatePtr, _data: *mut c_void) {
    // SAFETY: V8 runs interrupts on the isolate's thread.
    let isolate = unsafe { v8::Isolate::from_raw_isolate_ptr(isolate) };
    let Some(inspector) = crate::runtime::runtime_inspector(&isolate) else {
        return;
    };
    inspector
        .state
        .shared
        .interrupt_requested
        .store(false, Ordering::Release);
    inspector.poll();
}
