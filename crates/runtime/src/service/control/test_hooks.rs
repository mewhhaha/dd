//! Test-only asynchronous reply delivery.

use super::*;

impl WorkerManager {
    #[cfg(test)]
    pub(super) fn handle_test_async_reply(
        &mut self,
        payload: crate::ops::TestAsyncReplyEvent,
        event_tx: &RuntimeEventSender,
    ) {
        let result = if payload.ok {
            Ok(payload.value)
        } else {
            Err(PlatformError::runtime(if payload.error.trim().is_empty() {
                "test async reply failed".to_string()
            } else {
                payload.error
            }))
        };
        if payload.delay_ms == 0 {
            self.complete_test_async_reply(payload.reply_id, payload.replies, result);
            return;
        }
        let event_tx = event_tx.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(payload.delay_ms)).await;
            let _ = event_tx
                .send(RuntimeEvent::TestAsyncReplyComplete {
                    reply_id: payload.reply_id,
                    replies: payload.replies,
                    result,
                })
                .await;
        });
    }

    #[cfg(test)]
    pub(super) fn complete_test_async_reply(
        &mut self,
        reply_id: String,
        replies: crate::ops::TestAsyncReplies,
        result: Result<String>,
    ) {
        let Some(delivery) = replies.finish(reply_id, result) else {
            return;
        };
        self.enqueue_isolate_reply(
            &delivery.owner.worker_name,
            delivery.owner.generation,
            delivery.owner.isolate_id,
            crate::ops::PushedReplyPayload::TestAsync(delivery.payload),
        );
    }

    #[cfg(test)]
    fn enqueue_isolate_reply(
        &mut self,
        worker_name: &str,
        generation: u64,
        isolate_id: u64,
        payload: crate::ops::PushedReplyPayload,
    ) {
        let Some(inbox) = self
            .workers
            .get(worker_name)
            .and_then(|entry| entry.pools.get(&generation))
            .and_then(|pool| {
                pool.isolates
                    .iter()
                    .find(|isolate| isolate.id == isolate_id)
                    .map(|isolate| isolate.request_control_inbox.clone())
            })
        else {
            return;
        };
        inbox.push_reply(payload);
    }
}
