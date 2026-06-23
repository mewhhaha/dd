use crate::cache::{CacheLookup, CacheRequest, CacheResponse, CacheStore};
use crate::kv::{
    KvBatchMutation, KvEntry, KvProfileMetricKind, KvProfileSnapshot, KvStore, KvUtf8Lookup,
};
use crate::memory::{
    MemoryBatchMutation, MemoryCommandResultWrite, MemoryOutboxEffectWrite,
    MemoryProfileMetricKind, MemorySnapshotEntry, MemoryStore,
};
use crate::memory_rpc::{
    decode_memory_invoke_response, encode_memory_invoke_request, MemoryInvokeCall,
    MemoryInvokeRequest, MemoryInvokeResponse,
};
use crate::service::{HostRpcExecutionCall, MemoryExecutionCall};
use bytes::Bytes;
use common::{PlatformError, Result, WorkerInvocation, WorkerOutput};
use deno_core::{JsBuffer, OpState};
use deno_permissions::{PermissionsContainer, RuntimePermissionDescriptorParser};
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet, VecDeque};
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::OnceLock;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use sys_traits::impls::RealSys;
use tokio::sync::{mpsc, oneshot, Mutex, Notify};

#[path = "ops/dynamic.rs"]
mod dynamic_ops;
#[path = "ops/dynamic_types.rs"]
mod dynamic_types;
#[path = "ops/memory.rs"]
mod memory_ops;
#[path = "ops/memory_types.rs"]
mod memory_types;
#[path = "ops/request.rs"]
mod request_ops;
#[path = "ops/request_types.rs"]
mod request_types;
#[path = "ops/storage_http.rs"]
mod storage_http_ops;

use self::dynamic_ops::*;
pub(crate) use self::dynamic_types::*;
use self::memory_ops::*;
pub(crate) use self::memory_ops::{
    clear_memory_batch_handles, clear_memory_byte_handles, clear_memory_command_handles,
};
pub(crate) use self::memory_types::*;
use self::request_ops::*;
pub(crate) use self::request_types::*;
use self::storage_http_ops::*;

static PROCESS_MONO_START: OnceLock<Instant> = OnceLock::new();

#[derive(Clone)]
pub struct IsolateEventSender(pub Rc<dyn Fn(IsolateEventPayload) -> bool>);

#[derive(Clone, Default)]
pub struct MemoryOpenHandleRegistry {
    socket_handles: Arc<StdMutex<HashMap<String, HashSet<String>>>>,
    transport_handles: Arc<StdMutex<HashMap<String, HashSet<String>>>>,
}

impl MemoryOpenHandleRegistry {
    fn owner_key(binding: &str, key: &str) -> String {
        format!("{binding}\u{001f}{key}")
    }

    pub fn list_socket_handles(&self, binding: &str, key: &str) -> Vec<String> {
        let owner_key = Self::owner_key(binding, key);
        let mut handles = self
            .socket_handles
            .lock()
            .expect("socket handle registry mutex poisoned")
            .get(&owner_key)
            .map(|values| values.iter().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        handles.sort();
        handles
    }

    pub fn list_transport_handles(&self, binding: &str, key: &str) -> Vec<String> {
        let owner_key = Self::owner_key(binding, key);
        let mut handles = self
            .transport_handles
            .lock()
            .expect("transport handle registry mutex poisoned")
            .get(&owner_key)
            .map(|values| values.iter().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        handles.sort();
        handles
    }

    pub fn add_socket_handle(&self, binding: &str, key: &str, handle: &str) {
        let owner_key = Self::owner_key(binding, key);
        self.socket_handles
            .lock()
            .expect("socket handle registry mutex poisoned")
            .entry(owner_key)
            .or_default()
            .insert(handle.to_string());
    }

    pub fn remove_socket_handle(&self, binding: &str, key: &str, handle: &str) {
        let owner_key = Self::owner_key(binding, key);
        let mut handles = self
            .socket_handles
            .lock()
            .expect("socket handle registry mutex poisoned");
        let remove_owner = if let Some(values) = handles.get_mut(&owner_key) {
            values.remove(handle);
            values.is_empty()
        } else {
            false
        };
        if remove_owner {
            handles.remove(&owner_key);
        }
    }

    pub fn add_transport_handle(&self, binding: &str, key: &str, handle: &str) {
        let owner_key = Self::owner_key(binding, key);
        self.transport_handles
            .lock()
            .expect("transport handle registry mutex poisoned")
            .entry(owner_key)
            .or_default()
            .insert(handle.to_string());
    }

    pub fn remove_transport_handle(&self, binding: &str, key: &str, handle: &str) {
        let owner_key = Self::owner_key(binding, key);
        let mut handles = self
            .transport_handles
            .lock()
            .expect("transport handle registry mutex poisoned");
        let remove_owner = if let Some(values) = handles.get_mut(&owner_key) {
            values.remove(handle);
            values.is_empty()
        } else {
            false
        };
        if remove_owner {
            handles.remove(&owner_key);
        }
    }

    pub fn clear(&self) {
        self.socket_handles
            .lock()
            .expect("socket handle registry mutex poisoned")
            .clear();
        self.transport_handles
            .lock()
            .expect("transport handle registry mutex poisoned")
            .clear();
    }
}

pub enum IsolateEventPayload {
    Completion {
        request_id: String,
        completion_token: String,
        wait_until_count: usize,
        result: Result<WorkerOutput>,
    },
    WaitUntilDone {
        request_id: String,
        completion_token: String,
    },
    ResponseStart {
        request_id: String,
        completion_token: String,
        status: u16,
        headers: Vec<(String, String)>,
    },
    ResponseChunk {
        request_id: String,
        completion_token: String,
        chunk: Bytes,
        reply: oneshot::Sender<Result<()>>,
    },
    CacheRevalidate(CacheRevalidatePayload),
    MemoryInvoke(MemoryInvokeEvent),
    MemorySocketSend(MemorySocketSendEvent),
    MemorySocketClose(MemorySocketCloseEvent),
    MemorySocketConsumeClose(MemorySocketConsumeCloseEvent),
    MemoryTransportSendStream(MemoryTransportSendStreamEvent),
    MemoryTransportSendDatagram(MemoryTransportSendDatagramEvent),
    MemoryTransportClose(MemoryTransportCloseEvent),
    MemoryTransportConsumeClose(MemoryTransportConsumeCloseEvent),
    DynamicWorkerCreate(DynamicWorkerCreateEvent),
    DynamicWorkerLookup(DynamicWorkerLookupEvent),
    DynamicWorkerList(DynamicWorkerListEvent),
    DynamicWorkerDelete(DynamicWorkerDeleteEvent),
    DynamicHostRpcInvoke(DynamicHostRpcInvokeEvent),
    TestAsyncReply(TestAsyncReplyEvent),
    TestNestedTargetedInvoke(TestNestedTargetedInvokeEvent),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct CacheRevalidatePayload {
    pub(crate) cache_name: String,
    pub(crate) method: String,
    pub(crate) url: String,
    #[serde(default)]
    pub(crate) headers: Vec<(String, String)>,
}

#[derive(Clone, Debug)]
pub(crate) struct RuntimeExecutionLimits {
    pub(crate) max_request_body_bytes: usize,
    pub(crate) max_isolate_heap_bytes: usize,
}

fn emit_isolate_event(
    state: &OpState,
    payload: IsolateEventPayload,
) -> std::result::Result<(), ()> {
    let sender = state.borrow::<IsolateEventSender>();
    if (sender.0)(payload) {
        Ok(())
    } else {
        Err(())
    }
}

fn emit_isolate_event_from_rc(
    state: &Rc<RefCell<OpState>>,
    payload: IsolateEventPayload,
) -> std::result::Result<(), ()> {
    let state = state.borrow();
    emit_isolate_event(&state, payload)
}

pub(crate) fn current_time_boundary() -> TimeBoundary {
    let now_ms = wall_ms();
    let perf_ms = PROCESS_MONO_START
        .get_or_init(Instant::now)
        .elapsed()
        .as_secs_f64()
        * 1000.0;
    TimeBoundary { now_ms, perf_ms }
}

#[deno_core::op2]
async fn op_sleep(millis: u32) {
    tokio::time::sleep(Duration::from_millis(u64::from(millis))).await;
}

#[deno_core::op2]
#[serde]
fn op_time_boundary_now() -> TimeBoundary {
    current_time_boundary()
}

deno_core::extension!(
    dd_runtime_ops,
    ops = [
        op_sleep,
        op_time_boundary_now,
        op_kv_get,
        op_kv_get_many_utf8,
        op_kv_get_value,
        op_kv_profile_record_js,
        op_kv_profile_take,
        op_kv_profile_reset,
        op_kv_take_failed_write_version,
        op_kv_put,
        op_kv_put_value_bytes,
        op_kv_delete,
        op_kv_enqueue_put,
        op_kv_enqueue_put_value_bytes,
        op_kv_enqueue_delete,
        op_kv_list,
        op_cache_match,
        op_cache_put,
        op_cache_delete,
        op_dynamic_profile_take,
        op_dynamic_profile_reset,
        op_dynamic_module_graph_register,
        op_http_prepare,
        op_http_check_url,
        op_dynamic_cancel_reply,
        op_dynamic_take_control_items,
        op_test_async_reply_start,
        op_test_async_reply_cancel,
        op_test_nested_targeted_invoke_start,
        op_dynamic_worker_create,
        op_dynamic_worker_lookup,
        op_dynamic_worker_list,
        op_dynamic_worker_delete,
        op_dynamic_worker_fetch_start,
        op_dynamic_host_rpc_invoke,
        op_request_invocation_descriptor,
        op_take_worker_deployment_config,
        op_request_wait_until_register,
        op_request_body_read,
        op_request_body_cancel,
        op_request_context_close,
        op_request_context_cancel,
        op_memory_request_scope_close,
        op_memory_invoke_method,
        op_memory_profile_record_js,
        op_memory_profile_take,
        op_memory_profile_reset,
        op_memory_state_get,
        op_memory_state_snapshot,
        op_memory_state_version_if_newer,
        op_memory_bytes_put,
        op_memory_bytes_take,
        op_memory_batch_begin,
        op_memory_batch_close,
        op_memory_batch_accept,
        op_memory_batch_mutation,
        op_memory_batch_get_mutation,
        op_memory_batch_list_overlay,
        op_memory_batch_effect,
        op_memory_batch_command_result,
        op_memory_batch_apply,
        op_memory_command_begin,
        op_memory_command_close,
        op_memory_socket_send,
        op_memory_socket_close,
        op_memory_socket_list,
        op_memory_socket_consume_close,
        op_memory_transport_send_stream,
        op_memory_transport_send_datagram,
        op_memory_transport_close,
        op_memory_transport_list,
        op_memory_transport_consume_close,
        op_emit_completion_ok,
        op_emit_completion_error,
        op_emit_wait_until_done,
        op_emit_response_start,
        op_emit_response_chunk,
        op_http_store_prepared_body,
        op_http_take_prepared_body,
        op_http_store_prepared_headers,
        op_http_take_prepared_headers,
        op_emit_cache_revalidate
    ],
    state = |state| {
        let parser = Arc::new(RuntimePermissionDescriptorParser::new(RealSys));
        state.put(PermissionsContainer::allow_all(parser));
    }
);

pub fn runtime_extension() -> deno_core::Extension {
    dd_runtime_ops::init()
}

pub fn register_request_body_stream(
    state: &mut OpState,
    receiver: RequestBodyReceiver,
    max_bytes: usize,
) -> u32 {
    state
        .borrow_mut::<RequestBodyStreams>()
        .insert(RequestBodyStream::new(receiver, max_bytes))
}

pub fn register_request_invocation(state: &mut OpState, payload: WorkerRequestPayload) -> u32 {
    state
        .borrow_mut::<RequestInvocationHandles>()
        .insert(payload)
}

pub fn clear_request_invocation(state: &mut OpState, handle: u32) {
    if let Some(payload) = state
        .borrow_mut::<RequestInvocationHandles>()
        .remove(handle)
    {
        state
            .borrow_mut::<HttpPreparedHeaders>()
            .take(payload.request_headers_handle);
        state
            .borrow_mut::<HttpPreparedBodies>()
            .take(payload.request_body_handle);
    }
}

pub fn register_worker_deployment_config(
    state: &mut OpState,
    payload: WorkerDeploymentPayload,
) -> u32 {
    state
        .borrow_mut::<WorkerDeploymentHandles>()
        .insert(payload)
}

pub fn clear_worker_deployment_config(state: &mut OpState, handle: u32) {
    state.borrow_mut::<WorkerDeploymentHandles>().remove(handle);
}

pub fn register_memory_request_scope(
    state: &mut OpState,
    namespace: String,
    memory_key: String,
    owner_epoch: i64,
) -> u32 {
    state
        .borrow_mut::<MemoryRequestScopes>()
        .insert(MemoryRequestScope {
            namespace,
            memory_key,
            owner_epoch,
        })
}

pub fn register_request_secret_context(
    state: &mut OpState,
    isolate_id: u64,
    execution: RequestExecutionContext,
) -> u32 {
    state
        .borrow_mut::<RequestSecretContexts>()
        .insert(RequestSecretContext {
            isolate_id,
            execution,
            canceled: Arc::new(AtomicBool::new(false)),
            canceled_notify: Arc::new(Notify::new()),
        })
}

pub fn register_active_request_context(
    state: &mut OpState,
    request_id: String,
    completion_token: String,
    request_context_handle: u32,
) -> u32 {
    state
        .borrow_mut::<ActiveRequestContextHandles>()
        .insert(ActiveRequestContext {
            request_context_handle,
            completion_handle: 0,
            request_id,
            completion_token,
            wait_until_count: 0,
            wait_until_done_sent: false,
        })
}

pub fn active_request_context_handle_for_request(state: &OpState, request_id: &str) -> Option<u32> {
    state
        .borrow::<ActiveRequestContextHandles>()
        .get_request(request_id)
}

pub fn active_request_context_for_completion(
    state: &OpState,
    completion_handle: u32,
) -> Option<ActiveRequestContext> {
    state
        .borrow::<ActiveRequestContextHandles>()
        .get_completion(completion_handle)
        .cloned()
}

pub fn clear_request_body_stream(state: &mut OpState, stream_handle: u32) {
    if let Some(stream) = state
        .borrow_mut::<RequestBodyStreams>()
        .remove(stream_handle)
    {
        stream.cancel();
    }
}

pub fn clear_memory_request_scope(state: &mut OpState, memory_scope_handle: u32) {
    state
        .borrow_mut::<MemoryRequestScopes>()
        .remove(memory_scope_handle);
}

pub fn clear_request_secret_context(state: &mut OpState, request_context_handle: u32) {
    state
        .borrow_mut::<ActiveRequestContextHandles>()
        .remove_handle(request_context_handle);
    if let Some(context) = state
        .borrow_mut::<RequestSecretContexts>()
        .remove(request_context_handle)
    {
        context.canceled.store(true, Ordering::SeqCst);
        context.canceled_notify.notify_waiters();
    }
}

pub fn cancel_request_secret_context(state: &mut OpState, request_context_handle: u32) {
    if let Some(context) = state
        .borrow::<RequestSecretContexts>()
        .get(request_context_handle)
    {
        context.canceled.store(true, Ordering::SeqCst);
        context.canceled_notify.notify_waiters();
    }
}

fn wall_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::{
        is_egress_url_allowed, parse_egress_allow_host, DynamicControlInbox, DynamicControlItem,
        DynamicPendingReplyResult, DynamicPushedReplyPayload, EgressAllowHost,
    };

    #[test]
    fn parse_egress_allow_host_supports_exact_and_port_rules() {
        assert_eq!(
            parse_egress_allow_host("api.example.com"),
            Some(EgressAllowHost {
                host: "api.example.com".to_string(),
                wildcard: false,
                port: None,
            })
        );
        assert_eq!(
            parse_egress_allow_host("api.example.com:8443"),
            Some(EgressAllowHost {
                host: "api.example.com".to_string(),
                wildcard: false,
                port: Some(8443),
            })
        );
        assert_eq!(
            parse_egress_allow_host("*.example.com:8443"),
            Some(EgressAllowHost {
                host: "example.com".to_string(),
                wildcard: true,
                port: Some(8443),
            })
        );
    }

    #[test]
    fn egress_url_rules_require_matching_port() {
        let allowed = vec![parse_egress_allow_host("api.example.com").expect("allow host")];
        let https_default = reqwest::Url::parse("https://api.example.com/path").expect("url");
        let https_alt = reqwest::Url::parse("https://api.example.com:8443/path").expect("url");
        assert!(is_egress_url_allowed(&https_default, &allowed));
        assert!(!is_egress_url_allowed(&https_alt, &allowed));
    }

    #[test]
    fn egress_url_rules_support_explicit_ports_and_wildcards() {
        let allowed = vec![
            parse_egress_allow_host("api.example.com:8443").expect("exact allow host"),
            parse_egress_allow_host("*.internal.example.com").expect("wildcard allow host"),
        ];
        let exact = reqwest::Url::parse("https://api.example.com:8443/path").expect("url");
        let wildcard_ok =
            reqwest::Url::parse("https://svc.internal.example.com/path").expect("url");
        let wildcard_bad =
            reqwest::Url::parse("https://svc.internal.example.com:8443/path").expect("url");
        assert!(is_egress_url_allowed(&exact, &allowed));
        assert!(is_egress_url_allowed(&wildcard_ok, &allowed));
        assert!(!is_egress_url_allowed(&wildcard_bad, &allowed));
    }

    #[test]
    fn dynamic_control_inbox_batches_reply_items_under_one_schedule() {
        let inbox = DynamicControlInbox::default();
        assert!(inbox.push_reply(DynamicPushedReplyPayload::Dynamic(
            DynamicPendingReplyResult {
                reply_id: "dynr-1".to_string(),
                ready: true,
                ..DynamicPendingReplyResult::default()
            }
        )));
        assert!(!inbox.push_reply(DynamicPushedReplyPayload::Dynamic(
            DynamicPendingReplyResult {
                reply_id: "dynr-2".to_string(),
                ready: true,
                ..DynamicPendingReplyResult::default()
            }
        )));

        let first = inbox.take_batch();
        assert_eq!(first.len(), 2);
        assert!(matches!(
            &first[0],
            DynamicControlItem::Reply(DynamicPushedReplyPayload::Dynamic(payload))
                if payload.reply_id == "dynr-1"
        ));
        assert!(matches!(
            &first[1],
            DynamicControlItem::Reply(DynamicPushedReplyPayload::Dynamic(payload))
                if payload.reply_id == "dynr-2"
        ));
        assert!(inbox.take_batch().is_empty());
        assert!(inbox.push_reply(DynamicPushedReplyPayload::Dynamic(
            DynamicPendingReplyResult {
                reply_id: "dynr-3".to_string(),
                ready: true,
                ..DynamicPendingReplyResult::default()
            }
        )));
    }

    #[test]
    fn dynamic_control_inbox_reschedules_after_empty_drain() {
        let inbox = DynamicControlInbox::default();
        assert!(inbox.push_reply(DynamicPushedReplyPayload::Dynamic(
            DynamicPendingReplyResult {
                reply_id: "dynr-1".to_string(),
                ready: true,
                ..DynamicPendingReplyResult::default()
            }
        )));

        let first = inbox.take_batch();
        assert_eq!(first.len(), 1);
        assert!(matches!(
            &first[0],
            DynamicControlItem::Reply(DynamicPushedReplyPayload::Dynamic(payload))
                if payload.reply_id == "dynr-1"
        ));

        assert!(
            inbox.push_reply(DynamicPushedReplyPayload::Dynamic(
                DynamicPendingReplyResult {
                    reply_id: "dynr-2".to_string(),
                    ready: true,
                    ..DynamicPendingReplyResult::default()
                }
            )) == false
        );
        let second = inbox.take_batch();
        assert_eq!(second.len(), 1);
        assert!(matches!(
            &second[0],
            DynamicControlItem::Reply(DynamicPushedReplyPayload::Dynamic(payload))
                if payload.reply_id == "dynr-2"
        ));
        assert!(inbox.take_batch().is_empty());
        assert!(inbox.push_reply(DynamicPushedReplyPayload::Dynamic(
            DynamicPendingReplyResult {
                reply_id: "dynr-3".to_string(),
                ready: true,
                ..DynamicPendingReplyResult::default()
            }
        )));
    }
}
