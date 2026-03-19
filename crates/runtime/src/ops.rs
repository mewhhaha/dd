use std::time::Duration;
use deno_core::OpState;

#[derive(Clone)]
pub struct CompletionSender(pub tokio::sync::mpsc::UnboundedSender<String>);

#[deno_core::op2]
async fn op_sleep(millis: u32) {
    tokio::time::sleep(Duration::from_millis(u64::from(millis))).await;
}

#[deno_core::op2(fast)]
fn op_emit_completion(state: &mut OpState, #[string] payload: String) {
    let sender = state.borrow::<CompletionSender>().clone();
    let _ = sender.0.send(payload);
}

deno_core::extension!(grugd_runtime_ops, ops = [op_sleep, op_emit_completion]);

pub fn runtime_extension() -> deno_core::Extension {
    grugd_runtime_ops::init()
}
