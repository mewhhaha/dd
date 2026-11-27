//! Chrome DevTools Protocol (CDP) inspector for the Scriptw + wtime backend.
//!
//! Bridges DevTools sessions (WebSocket or in-process tests) to a [`JsRuntime`],
//! supporting `Runtime.{enable,disable,evaluate,runIfWaitingForDebugger,consoleAPICalled}`,
//! `Debugger.{enable,disable,setBreakpointByUrl,removeBreakpoint,pause,resume,evaluateOnCallFrame,paused,resumed,scriptParsed}`,
//! `debugger;` statements, top-level & per-request breakpoints, busy-loop pause
//! interrupts, and immediate unblocking on `RuntimeHandle::terminate_execution()`.

use futures_util::task::AtomicWaker;
use serde_json::{Value, json};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use tokio::sync::mpsc;

#[derive(Clone, Debug)]
struct ParsedScript {
    id: u64,
    url: String,
    end_line: u32,
    end_column: u32,
}

#[derive(Clone, Debug)]
struct Breakpoint {
    id: String,
    url: String,
    line_number: u32,
}

struct SessionState {
    sender: mpsc::UnboundedSender<String>,
    runtime_enabled: bool,
    debugger_enabled: bool,
}

#[derive(Debug)]
pub(crate) enum PendingEvalRequest {
    Global {
        session_id: u64,
        req_id: Value,
        expression: String,
    },
    CallFrame {
        session_id: u64,
        req_id: Value,
        expression: String,
    },
}

#[derive(Debug)]
pub(crate) enum PauseStep {
    Resume,
    Terminate,
    Eval(PendingEvalRequest),
}

struct InspectorInner {
    closed: bool,
    next_session_id: u64,
    sessions: HashMap<u64, SessionState>,
    inbox: VecDeque<(u64, String)>,
    waiting_for_debugger: bool,
    run_if_waiting_received: bool,
    pause_requested: bool,
    next_script_id: u64,
    scripts: Vec<ParsedScript>,
    next_bp_id: u64,
    breakpoints: Vec<Breakpoint>,
    next_call_frame_id: u64,
}

pub(crate) struct InspectorCore {
    context_name: String,
    state_waker: Arc<AtomicWaker>,
    terminated: Arc<AtomicBool>,
    session_count: AtomicUsize,
    runtime_enabled_count: AtomicUsize,
    debugger_enabled_count: AtomicUsize,
    /// Lock-free fast check: true when any breakpoint is registered OR `Debugger.pause` is armed.
    has_breakpoints_or_pause: AtomicBool,
    pause_flag: AtomicBool,
    inner: Mutex<InspectorInner>,
    condvar: Condvar,
}

impl InspectorCore {
    pub(crate) fn new(
        context_name: String,
        state_waker: Arc<AtomicWaker>,
        terminated: Arc<AtomicBool>,
    ) -> Arc<Self> {
        Arc::new(Self {
            context_name,
            state_waker,
            terminated,
            session_count: AtomicUsize::new(0),
            runtime_enabled_count: AtomicUsize::new(0),
            debugger_enabled_count: AtomicUsize::new(0),
            has_breakpoints_or_pause: AtomicBool::new(false),
            pause_flag: AtomicBool::new(false),
            inner: Mutex::new(InspectorInner {
                closed: false,
                next_session_id: 1,
                sessions: HashMap::new(),
                inbox: VecDeque::new(),
                waiting_for_debugger: false,
                run_if_waiting_received: false,
                pause_requested: false,
                next_script_id: 1,
                scripts: Vec::new(),
                next_bp_id: 1,
                breakpoints: Vec::new(),
                next_call_frame_id: 1,
            }),
            condvar: Condvar::new(),
        })
    }

    pub(crate) fn has_sessions(&self) -> bool {
        self.session_count.load(Ordering::Acquire) > 0
    }

    pub(crate) fn has_runtime_enabled(&self) -> bool {
        self.runtime_enabled_count.load(Ordering::Acquire) > 0
    }

    pub(crate) fn has_debugger_enabled(&self) -> bool {
        self.debugger_enabled_count.load(Ordering::Acquire) > 0
    }

    pub(crate) fn has_breakpoints_or_pause(&self) -> bool {
        self.has_breakpoints_or_pause.load(Ordering::Acquire)
            || self.pause_flag.load(Ordering::Acquire)
    }

    pub(crate) fn is_terminated(&self) -> bool {
        self.terminated.load(Ordering::Acquire)
    }

    pub(crate) fn wake_for_termination(&self) {
        self.condvar.notify_all();
        self.state_waker.wake();
    }

    pub(crate) fn close(&self) {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.closed = true;
        inner.sessions.clear();
        self.session_count.store(0, Ordering::Release);
        self.runtime_enabled_count.store(0, Ordering::Release);
        self.debugger_enabled_count.store(0, Ordering::Release);
        self.condvar.notify_all();
        self.state_waker.wake();
    }

    fn update_flags(&self, inner: &InspectorInner) {
        self.session_count
            .store(inner.sessions.len(), Ordering::Release);
        let rt_cnt = inner
            .sessions
            .values()
            .filter(|s| s.runtime_enabled)
            .count();
        let dbg_cnt = inner
            .sessions
            .values()
            .filter(|s| s.debugger_enabled)
            .count();
        self.runtime_enabled_count.store(rt_cnt, Ordering::Release);
        self.debugger_enabled_count
            .store(dbg_cnt, Ordering::Release);
        self.pause_flag
            .store(inner.pause_requested, Ordering::Release);
        self.has_breakpoints_or_pause.store(
            inner.pause_requested || !inner.breakpoints.is_empty(),
            Ordering::Release,
        );
    }

    fn send_to_session(inner: &InspectorInner, session_id: u64, msg: &Value) {
        if let Some(sess) = inner.sessions.get(&session_id) {
            let _ = sess.sender.send(msg.to_string());
        }
    }

    fn broadcast_runtime(inner: &InspectorInner, msg: &Value) {
        let text = msg.to_string();
        for sess in inner.sessions.values() {
            if sess.runtime_enabled {
                let _ = sess.sender.send(text.clone());
            }
        }
    }

    fn broadcast_debugger(inner: &InspectorInner, msg: &Value) {
        let text = msg.to_string();
        for sess in inner.sessions.values() {
            if sess.debugger_enabled {
                let _ = sess.sender.send(text.clone());
            }
        }
    }

    pub(crate) fn register_script(&self, url: &str, source: &str) -> u64 {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let id = inner.next_script_id;
        inner.next_script_id += 1;
        let mut end_line = 0u32;
        let mut last_line_start = 0usize;
        for (i, b) in source.bytes().enumerate() {
            if b == b'\n' {
                end_line += 1;
                last_line_start = i + 1;
            }
        }
        let end_column = (source.len() - last_line_start) as u32;
        let parsed = ParsedScript {
            id,
            url: url.to_string(),
            end_line,
            end_column,
        };
        let event = script_parsed_event(&parsed);
        inner.scripts.push(parsed);
        Self::broadcast_debugger(&inner, &event);
        id
    }

    pub(crate) fn script_url(&self, script_id: u64) -> String {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner
            .scripts
            .iter()
            .find(|s| s.id == script_id)
            .map(|s| s.url.clone())
            .unwrap_or_default()
    }

    /// Emits `Runtime.consoleAPICalled` to all `Runtime.enable`d sessions.
    pub(crate) fn emit_console_api_called(
        &self,
        cdp_type: &str,
        args_json: &str,
        script_id: u64,
        url: &str,
        line: u32,
        col: u32,
    ) {
        if !self.has_runtime_enabled() {
            return;
        }
        let args: Value = serde_json::from_str(args_json).unwrap_or_else(|_| json!([]));
        let mut call_frames = Vec::new();
        if !url.is_empty() || script_id > 0 {
            call_frames.push(json!({
                "functionName": "",
                "scriptId": script_id.to_string(),
                "url": url,
                "lineNumber": line,
                "columnNumber": col,
            }));
        }
        let event = json!({
            "method": "Runtime.consoleAPICalled",
            "params": {
                "type": cdp_type,
                "args": args,
                "executionContextId": 1,
                "timestamp": 0.0,
                "stackTrace": {
                    "callFrames": call_frames,
                },
            }
        });
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        Self::broadcast_runtime(&inner, &event);
    }

    /// Checks whether execution at `(script_id, line)` should pause now (and returns
    /// `(reason, hit_breakpoints)` if so).
    pub(crate) fn check_break(
        &self,
        script_id: u64,
        line: u32,
        is_debugger_stmt: bool,
    ) -> Option<(&'static str, Vec<String>)> {
        if !self.has_debugger_enabled() {
            return None;
        }
        if !is_debugger_stmt && !self.has_breakpoints_or_pause() {
            return None;
        }
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        // Process any queued non-eval messages (e.g. Debugger.pause / setBreakpointByUrl) first.
        while let Some((sid, raw)) = inner.inbox.pop_front() {
            if let Some(eval_req) = self.handle_non_blocking_message(&mut inner, sid, &raw) {
                // Put eval requests back at the front for idle/pause dispatch.
                let _ = eval_req;
                inner.inbox.push_front((sid, raw));
                break;
            }
        }
        if !inner.sessions.values().any(|s| s.debugger_enabled) {
            return None;
        }
        if is_debugger_stmt {
            inner.pause_requested = false;
            self.update_flags(&inner);
            return Some(("other", Vec::new()));
        }
        let url = inner
            .scripts
            .iter()
            .find(|s| s.id == script_id)
            .map(|s| s.url.as_str())
            .unwrap_or("");
        let hit_bps: Vec<String> = inner
            .breakpoints
            .iter()
            .filter(|bp| bp.line_number == line && bp.url == url)
            .map(|bp| bp.id.clone())
            .collect();
        if !hit_bps.is_empty() {
            inner.pause_requested = false;
            self.update_flags(&inner);
            return Some(("other", hit_bps));
        }
        if inner.pause_requested {
            inner.pause_requested = false;
            self.update_flags(&inner);
            return Some(("other", Vec::new()));
        }
        None
    }

    /// Emits `Debugger.paused` and returns the allocated `callFrameId`.
    pub(crate) fn emit_paused(
        &self,
        script_id: u64,
        line: u32,
        col: u32,
        reason: &str,
        hit_breakpoints: &[String],
    ) -> String {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let cf_num = inner.next_call_frame_id;
        inner.next_call_frame_id += 1;
        let call_frame_id = format!("cf-{cf_num}");
        let url = inner
            .scripts
            .iter()
            .find(|s| s.id == script_id)
            .map(|s| s.url.clone())
            .unwrap_or_default();
        let event = json!({
            "method": "Debugger.paused",
            "params": {
                "callFrames": [{
                    "callFrameId": call_frame_id,
                    "functionName": "",
                    "location": {
                        "scriptId": script_id.to_string(),
                        "lineNumber": line,
                        "columnNumber": col,
                    },
                    "url": url,
                    "scopeChain": [],
                    "this": { "type": "undefined" },
                }],
                "reason": reason,
                "hitBreakpoints": hit_breakpoints,
            }
        });
        Self::broadcast_debugger(&inner, &event);
        call_frame_id
    }

    /// Blocks while paused until a `Debugger.resume`, an evaluation request,
    /// or `terminate_execution()` arrives.
    pub(crate) fn wait_pause_step(&self) -> PauseStep {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        loop {
            if self.terminated.load(Ordering::Acquire) || inner.closed {
                return PauseStep::Terminate;
            }
            if let Some((sid, raw)) = inner.inbox.pop_front() {
                let parsed: Value = match serde_json::from_str(&raw) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                let method = parsed["method"].as_str().unwrap_or("");
                let req_id = parsed["id"].clone();
                match method {
                    "Debugger.resume" => {
                        Self::send_to_session(&inner, sid, &json!({ "id": req_id, "result": {} }));
                        Self::broadcast_debugger(
                            &inner,
                            &json!({ "method": "Debugger.resumed", "params": {} }),
                        );
                        return PauseStep::Resume;
                    }
                    "Debugger.evaluateOnCallFrame" => {
                        let expression = parsed["params"]["expression"]
                            .as_str()
                            .unwrap_or("")
                            .to_string();
                        return PauseStep::Eval(PendingEvalRequest::CallFrame {
                            session_id: sid,
                            req_id,
                            expression,
                        });
                    }
                    "Runtime.evaluate" => {
                        let expression = parsed["params"]["expression"]
                            .as_str()
                            .unwrap_or("")
                            .to_string();
                        return PauseStep::Eval(PendingEvalRequest::Global {
                            session_id: sid,
                            req_id,
                            expression,
                        });
                    }
                    _ => {
                        let _ = self.handle_non_blocking_message(&mut inner, sid, &raw);
                    }
                }
                continue;
            }
            inner = self.condvar.wait(inner).unwrap_or_else(|e| e.into_inner());
        }
    }

    pub(crate) fn send_eval_response(
        &self,
        session_id: u64,
        req_id: Value,
        remote_object_json: &str,
        exception_desc: Option<&str>,
    ) {
        let remote: Value = serde_json::from_str(remote_object_json)
            .unwrap_or_else(|_| json!({ "type": "undefined" }));
        let mut result = json!({ "result": remote });
        if let Some(desc) = exception_desc {
            result["exceptionDetails"] = json!({
                "exceptionId": 1,
                "text": "Uncaught",
                "lineNumber": 0,
                "columnNumber": 0,
                "exception": {
                    "type": "object",
                    "subtype": "error",
                    "description": desc,
                },
            });
        }
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        Self::send_to_session(
            &inner,
            session_id,
            &json!({ "id": req_id, "result": result }),
        );
    }

    /// Blocks the isolate thread until `Runtime.runIfWaitingForDebugger` or `terminate_execution()`.
    pub(crate) fn wait_for_debugger(&self) {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if inner.run_if_waiting_received {
            return;
        }
        inner.waiting_for_debugger = true;
        loop {
            if self.terminated.load(Ordering::Acquire)
                || inner.closed
                || inner.run_if_waiting_received
            {
                inner.waiting_for_debugger = false;
                return;
            }
            if let Some((sid, raw)) = inner.inbox.pop_front() {
                if let Some(eval_req) = self.handle_non_blocking_message(&mut inner, sid, &raw) {
                    // Defer Runtime.evaluate until after runIfWaitingForDebugger.
                    let _ = eval_req;
                    inner.inbox.push_back((sid, raw));
                }
                continue;
            }
            inner = self.condvar.wait(inner).unwrap_or_else(|e| e.into_inner());
        }
    }

    /// Drains all queued non-eval messages and returns any queued `Runtime.evaluate`
    /// requests so `JsRuntime::poll_event_loop` can execute them on the isolate thread.
    pub(crate) fn drain_idle_messages(&self) -> Vec<PendingEvalRequest> {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let mut evals = Vec::new();
        while let Some((sid, raw)) = inner.inbox.pop_front() {
            if let Some(req) = self.handle_non_blocking_message(&mut inner, sid, &raw) {
                evals.push(req);
            }
        }
        evals
    }

    /// Handles a CDP message that doesn't require running JS; returns `Some(PendingEvalRequest)`
    /// for `Runtime.evaluate`.
    fn handle_non_blocking_message(
        &self,
        inner: &mut InspectorInner,
        sid: u64,
        raw: &str,
    ) -> Option<PendingEvalRequest> {
        let parsed: Value = serde_json::from_str(raw).ok()?;
        let method = parsed["method"].as_str().unwrap_or("");
        let req_id = parsed["id"].clone();
        match method {
            "Runtime.enable" => {
                let send_created = if let Some(sess) = inner.sessions.get_mut(&sid) {
                    let was = sess.runtime_enabled;
                    sess.runtime_enabled = true;
                    !was
                } else {
                    false
                };
                self.update_flags(inner);
                if send_created {
                    Self::send_to_session(
                        inner,
                        sid,
                        &json!({
                            "method": "Runtime.executionContextCreated",
                            "params": {
                                "context": {
                                    "id": 1,
                                    "origin": "",
                                    "name": self.context_name,
                                }
                            }
                        }),
                    );
                }
                Self::send_to_session(inner, sid, &json!({ "id": req_id, "result": {} }));
                None
            }
            "Runtime.disable" => {
                if let Some(sess) = inner.sessions.get_mut(&sid) {
                    sess.runtime_enabled = false;
                }
                self.update_flags(inner);
                Self::send_to_session(inner, sid, &json!({ "id": req_id, "result": {} }));
                None
            }
            "Runtime.runIfWaitingForDebugger" => {
                inner.run_if_waiting_received = true;
                Self::send_to_session(inner, sid, &json!({ "id": req_id, "result": {} }));
                self.condvar.notify_all();
                None
            }
            "Runtime.evaluate" => {
                let expression = parsed["params"]["expression"]
                    .as_str()
                    .unwrap_or("")
                    .to_string();
                Some(PendingEvalRequest::Global {
                    session_id: sid,
                    req_id,
                    expression,
                })
            }
            "Debugger.enable" => {
                let send_scripts = if let Some(sess) = inner.sessions.get_mut(&sid) {
                    let was = sess.debugger_enabled;
                    sess.debugger_enabled = true;
                    !was
                } else {
                    false
                };
                self.update_flags(inner);
                if send_scripts {
                    let events: Vec<Value> =
                        inner.scripts.iter().map(script_parsed_event).collect();
                    for ev in events {
                        Self::send_to_session(inner, sid, &ev);
                    }
                }
                Self::send_to_session(
                    inner,
                    sid,
                    &json!({ "id": req_id, "result": { "debuggerId": "dd-debugger-1" } }),
                );
                None
            }
            "Debugger.disable" => {
                if let Some(sess) = inner.sessions.get_mut(&sid) {
                    sess.debugger_enabled = false;
                }
                self.update_flags(inner);
                Self::send_to_session(inner, sid, &json!({ "id": req_id, "result": {} }));
                None
            }
            "Debugger.setBreakpointByUrl" => {
                let url = parsed["params"]["url"].as_str().unwrap_or("").to_string();
                let line_number = parsed["params"]["lineNumber"].as_u64().unwrap_or(0) as u32;
                let column_number = parsed["params"]["columnNumber"].as_u64().unwrap_or(0) as u32;
                let bp_num = inner.next_bp_id;
                inner.next_bp_id += 1;
                let bp_id = format!("bp-{bp_num}:{line_number}:{column_number}:{url}");
                let locations: Vec<Value> = inner
                    .scripts
                    .iter()
                    .filter(|s| !url.is_empty() && s.url == url)
                    .map(|s| {
                        json!({
                            "scriptId": s.id.to_string(),
                            "lineNumber": line_number,
                            "columnNumber": column_number,
                        })
                    })
                    .collect();
                inner.breakpoints.push(Breakpoint {
                    id: bp_id.clone(),
                    url,
                    line_number,
                });
                self.update_flags(inner);
                Self::send_to_session(
                    inner,
                    sid,
                    &json!({
                        "id": req_id,
                        "result": {
                            "breakpointId": bp_id,
                            "locations": locations,
                        }
                    }),
                );
                None
            }
            "Debugger.removeBreakpoint" => {
                if let Some(bp_id) = parsed["params"]["breakpointId"].as_str() {
                    inner.breakpoints.retain(|bp| bp.id != bp_id);
                    self.update_flags(inner);
                }
                Self::send_to_session(inner, sid, &json!({ "id": req_id, "result": {} }));
                None
            }
            "Debugger.pause" => {
                inner.pause_requested = true;
                self.update_flags(inner);
                Self::send_to_session(inner, sid, &json!({ "id": req_id, "result": {} }));
                None
            }
            "Debugger.resume" => {
                inner.pause_requested = false;
                self.update_flags(inner);
                Self::send_to_session(inner, sid, &json!({ "id": req_id, "result": {} }));
                None
            }
            _ => {
                if !req_id.is_null() {
                    Self::send_to_session(inner, sid, &json!({ "id": req_id, "result": {} }));
                }
                None
            }
        }
    }
}

fn script_parsed_event(script: &ParsedScript) -> Value {
    json!({
        "method": "Debugger.scriptParsed",
        "params": {
            "scriptId": script.id.to_string(),
            "url": script.url,
            "startLine": 0,
            "startColumn": 0,
            "endLine": script.end_line,
            "endColumn": script.end_column,
            "executionContextId": 1,
            "hash": "",
        }
    })
}

/// Opens DevTools sessions on a runtime's inspector from any thread.
#[derive(Clone)]
pub struct InspectorHandle {
    core: Arc<InspectorCore>,
}

impl InspectorHandle {
    pub(crate) fn new(core: Arc<InspectorCore>) -> Self {
        Self { core }
    }

    pub(crate) fn core(&self) -> &Arc<InspectorCore> {
        &self.core
    }

    pub fn connect(&self) -> InspectorSession {
        let mut inner = self.core.inner.lock().unwrap_or_else(|e| e.into_inner());
        let id = inner.next_session_id;
        inner.next_session_id += 1;
        let (tx, receiver) = mpsc::unbounded_channel();
        if !inner.closed {
            inner.sessions.insert(
                id,
                SessionState {
                    sender: tx,
                    runtime_enabled: false,
                    debugger_enabled: false,
                },
            );
            self.core.update_flags(&inner);
        }
        InspectorSession {
            id,
            core: Arc::clone(&self.core),
            receiver,
        }
    }

    pub(crate) fn wake_for_termination(&self) {
        self.core.wake_for_termination();
    }

    pub(crate) fn close(&self) {
        self.core.close();
    }
}

impl std::fmt::Debug for InspectorHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InspectorHandle").finish_non_exhaustive()
    }
}

/// One DevTools session.
pub struct InspectorSession {
    id: u64,
    core: Arc<InspectorCore>,
    receiver: mpsc::UnboundedReceiver<String>,
}

impl InspectorSession {
    pub fn send(&self, message: impl Into<String>) {
        let message = message.into();
        // Fast-path flag so tight loops notice `Debugger.pause` immediately without acquiring lock.
        if message.contains("\"Debugger.pause\"") {
            self.core.pause_flag.store(true, Ordering::Release);
            self.core
                .has_breakpoints_or_pause
                .store(true, Ordering::Release);
        }
        let mut inner = self.core.inner.lock().unwrap_or_else(|e| e.into_inner());
        if inner.closed {
            return;
        }
        inner.inbox.push_back((self.id, message));
        self.core.condvar.notify_all();
        self.core.state_waker.wake();
    }

    pub async fn recv(&mut self) -> Option<String> {
        self.receiver.recv().await
    }

    pub fn try_recv(&mut self) -> Option<String> {
        self.receiver.try_recv().ok()
    }
}

impl Drop for InspectorSession {
    fn drop(&mut self) {
        let mut inner = self.core.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.sessions.remove(&self.id);
        self.core.update_flags(&inner);
        self.core.condvar.notify_all();
        self.core.state_waker.wake();
    }
}

impl std::fmt::Debug for InspectorSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InspectorSession")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}
