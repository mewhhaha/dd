use super::*;
pub(super) enum IsolateCommand {
    Execute {
        runtime_request_id: String,
        completion_token: String,
        request_context: Box<RequestExecutionContext>,
        request: Box<WorkerInvocation>,
        request_body: Option<InvokeRequestBodyReceiver>,
        stream_response: bool,
        memory_call: Box<Option<MemoryExecutionCall>>,
        memory_route: Box<Option<MemoryRoute>>,
        dispatched_at: Instant,
        profile_memory_atomic: bool,
    },
    Abort {
        runtime_request_id: String,
    },
    DrainRequestControl,
}

pub(super) struct IsolateEventLoopWaker {
    pub(super) notify: Arc<Notify>,
}

impl Wake for IsolateEventLoopWaker {
    fn wake(self: Arc<Self>) {
        self.notify.notify_one();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.notify.notify_one();
    }
}

#[derive(Clone, Debug, serde::Serialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub(crate) enum MemoryExecutionCall {
    Message {
        binding: String,
        key: String,
        handle: String,
        is_text: bool,
        data: Vec<u8>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        socket_handles: Vec<String>,
    },
    Close {
        binding: String,
        key: String,
        handle: String,
        code: u16,
        reason: String,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        socket_handles: Vec<String>,
    },
}

impl MemoryExecutionCall {
    pub(super) fn queued_bytes(&self) -> usize {
        match self {
            Self::Message {
                binding,
                key,
                handle,
                data,
                socket_handles,
                ..
            } => {
                binding.len()
                    + key.len()
                    + handle.len()
                    + data.len()
                    + socket_handles.iter().map(String::len).sum::<usize>()
            }
            Self::Close {
                binding,
                key,
                handle,
                reason,
                socket_handles,
                ..
            } => {
                binding.len()
                    + key.len()
                    + handle.len()
                    + reason.len()
                    + socket_handles.iter().map(String::len).sum::<usize>()
            }
        }
    }
}

#[derive(Clone)]
pub(super) struct InvokeCancelGuard {
    pub(super) cancel_sender: RuntimeCancellationSender,
    pub(super) worker_name: String,
    pub(super) runtime_request_id: String,
    pub(super) armed: bool,
}

impl InvokeCancelGuard {
    pub(super) fn new(
        cancel_sender: RuntimeCancellationSender,
        worker_name: String,
        runtime_request_id: String,
    ) -> Self {
        Self {
            cancel_sender,
            worker_name,
            runtime_request_id,
            armed: true,
        }
    }

    pub(super) fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for InvokeCancelGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }

        let _ = self.cancel_sender.send(RuntimeCommand::Cancel {
            worker_name: self.worker_name.clone(),
            runtime_request_id: self.runtime_request_id.clone(),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancellation_survives_a_scheduler_backlog() {
        let (sender, mut receiver) = mpsc::unbounded_channel();
        for request in 0..8192 {
            drop(InvokeCancelGuard::new(
                RuntimeCancellationSender::new(sender.clone(), WorkerRoutes::default()),
                "worker".to_string(),
                request.to_string(),
            ));
        }
        drop(sender);
        for request in 0..8192 {
            match receiver
                .try_recv()
                .expect("accepted request cancellation must arrive")
            {
                RuntimeCommand::Cancel {
                    runtime_request_id, ..
                } => assert_eq!(runtime_request_id, request.to_string()),
                _ => panic!("unexpected command on cancellation channel"),
            }
        }
    }
}
