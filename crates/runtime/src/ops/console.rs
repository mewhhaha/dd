use super::*;
use tokio::sync::broadcast;

/// Longest console message kept, in bytes; the rest is cut and counted.
const MAX_CONSOLE_MESSAGE_BYTES: usize = 16 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum WorkerConsoleLevel {
    Debug,
    Info,
    Warn,
    Error,
}

/// One worker `console` call, formatted.
#[derive(Clone, Debug, Serialize)]
pub struct WorkerConsoleLine {
    pub worker: String,
    /// The runtime request the call ran under; empty outside a request.
    pub request_id: String,
    pub level: WorkerConsoleLevel,
    pub message: String,
}

/// Where an isolate's console output goes besides `tracing`: the
/// subscribers of `RuntimeService::subscribe_console`.
#[derive(Clone)]
pub(crate) struct WorkerConsoleSink(pub(crate) broadcast::Sender<WorkerConsoleLine>);

impl WorkerConsoleSink {
    pub(crate) fn new() -> Self {
        Self(broadcast::channel(1024).0)
    }
}

/// `op_console_write(level, message, request_id)`: logs one console call as a
/// `dd::worker` tracing event and hands it to console subscribers. Deployment
/// validation isolates have no worker and log nothing, so a module's
/// top-level output appears once, when it really loads.
pub(crate) fn op_console_write(
    state: &mut OpState,
    level: u32,
    message: String,
    request_id: String,
) {
    let Some(WorkerCacheNamespace(worker)) = state.try_borrow::<WorkerCacheNamespace>() else {
        return;
    };
    let level = match level {
        0 => WorkerConsoleLevel::Debug,
        1 => WorkerConsoleLevel::Info,
        2 => WorkerConsoleLevel::Warn,
        _ => WorkerConsoleLevel::Error,
    };
    let message = truncate_console_message(message);
    match level {
        WorkerConsoleLevel::Debug => {
            tracing::debug!(target: "dd::worker", worker = %worker, request_id = %request_id, "{message}")
        }
        WorkerConsoleLevel::Info => {
            tracing::info!(target: "dd::worker", worker = %worker, request_id = %request_id, "{message}")
        }
        WorkerConsoleLevel::Warn => {
            tracing::warn!(target: "dd::worker", worker = %worker, request_id = %request_id, "{message}")
        }
        WorkerConsoleLevel::Error => {
            tracing::error!(target: "dd::worker", worker = %worker, request_id = %request_id, "{message}")
        }
    }
    if let Some(sink) = state.try_borrow::<WorkerConsoleSink>()
        && sink.0.receiver_count() > 0
    {
        let _ = sink.0.send(WorkerConsoleLine {
            worker: worker.clone(),
            request_id,
            level,
            message,
        });
    }
}

fn truncate_console_message(mut message: String) -> String {
    if message.len() <= MAX_CONSOLE_MESSAGE_BYTES {
        return message;
    }
    let mut end = MAX_CONSOLE_MESSAGE_BYTES;
    while !message.is_char_boundary(end) {
        end -= 1;
    }
    let cut = message.len() - end;
    message.truncate(end);
    message.push_str(&format!("... [{cut} more bytes]"));
    message
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn long_messages_are_cut_on_a_char_boundary() {
        let message = "é".repeat(MAX_CONSOLE_MESSAGE_BYTES);
        let cut = truncate_console_message(message);
        assert!(cut.ends_with(&format!("... [{} more bytes]", MAX_CONSOLE_MESSAGE_BYTES)));
        assert_eq!(truncate_console_message("short".into()), "short");
    }
}
