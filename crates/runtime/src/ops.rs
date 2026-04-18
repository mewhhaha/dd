use crate::cache::{CacheLookup, CacheRequest, CacheResponse, CacheStore};
use crate::kv::{
    KvBatchMutation, KvEntry, KvProfileMetricKind, KvProfileSnapshot, KvStore, KvUtf8Lookup,
};
use crate::memory::{
    MemoryBatchMutation, MemoryDirectMutation, MemoryProfileMetricKind, MemoryReadDependency,
    MemoryStore,
};
use crate::memory_rpc::{
    decode_memory_invoke_response, encode_memory_invoke_request, MemoryInvokeCall,
    MemoryInvokeRequest, MemoryInvokeResponse,
};
use aes_gcm::aead::{Aead, Payload};
use aes_gcm::{Aes128Gcm, Aes256Gcm, KeyInit, Nonce};
use common::{PlatformError, Result, WorkerInvocation, WorkerOutput};
use deno_core::OpState;
use deno_permissions::{PermissionsContainer, RuntimePermissionDescriptorParser};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha1::Sha1;
use sha2::{Digest, Sha256, Sha384, Sha512};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet, VecDeque};
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::OnceLock;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use sys_traits::impls::RealSys;
use tokio::sync::{mpsc, oneshot, Mutex, Notify};

#[path = "ops/crypto.rs"]
mod crypto_ops;
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

use self::crypto_ops::*;
use self::dynamic_ops::*;
pub(crate) use self::dynamic_types::*;
use self::memory_ops::*;
pub(crate) use self::memory_types::*;
use self::request_ops::*;
pub(crate) use self::request_types::*;
use self::storage_http_ops::*;

#[derive(Clone)]
pub struct IsolateEventSender(pub std::sync::mpsc::Sender<IsolateEventPayload>);

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
    Completion(String),
    WaitUntilDone(String),
    ResponseStart(String),
    ResponseChunk(String),
    CacheRevalidate(String),
    MemoryInvoke(MemoryInvokeEvent),
    MemorySocketSend(MemorySocketSendEvent),
    MemorySocketClose(MemorySocketCloseEvent),
    MemorySocketConsumeClose(MemorySocketConsumeCloseEvent),
    MemoryTransportSendStream(MemoryTransportSendStreamEvent),
    MemoryTransportSendDatagram(MemoryTransportSendDatagramEvent),
    MemoryTransportRecvStream(MemoryTransportRecvStreamEvent),
    MemoryTransportRecvDatagram(MemoryTransportRecvDatagramEvent),
    MemoryTransportClose(MemoryTransportCloseEvent),
    MemoryTransportConsumeClose(MemoryTransportConsumeCloseEvent),
    DynamicWorkerCreate(DynamicWorkerCreateEvent),
    DynamicWorkerLookup(DynamicWorkerLookupEvent),
    DynamicWorkerList(DynamicWorkerListEvent),
    DynamicWorkerDelete(DynamicWorkerDeleteEvent),
    DynamicWorkerInvoke(DynamicWorkerInvokeEvent),
    DynamicHostRpcInvoke(DynamicHostRpcInvokeEvent),
    TestAsyncReply(TestAsyncReplyEvent),
    TestNestedTargetedInvoke(TestNestedTargetedInvokeEvent),
}

pub(crate) fn current_time_boundary() -> TimeBoundary {
    let now_ms = wall_ms();
    let perf_ms = crypto_ops::PROCESS_MONO_START
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
        op_crypto_digest,
        op_crypto_hmac_sign,
        op_crypto_hmac_verify,
        op_crypto_aes_gcm_encrypt,
        op_crypto_aes_gcm_decrypt,
        op_kv_get,
        op_kv_get_many_utf8,
        op_kv_get_value,
        op_kv_profile_record_js,
        op_kv_profile_take,
        op_kv_profile_reset,
        op_kv_take_failed_write_version,
        op_kv_put,
        op_kv_put_value,
        op_kv_delete,
        op_kv_enqueue_put,
        op_kv_enqueue_put_value,
        op_kv_enqueue_delete,
        op_kv_apply_batch,
        op_kv_list,
        op_cache_match,
        op_cache_put,
        op_cache_delete,
        op_dynamic_profile_take,
        op_dynamic_profile_reset,
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
        op_dynamic_worker_invoke,
        op_dynamic_worker_fetch_start,
        op_dynamic_host_rpc_invoke,
        op_request_body_read,
        op_request_body_cancel,
        op_memory_invoke_method,
        op_memory_profile_record_js,
        op_memory_profile_take,
        op_memory_profile_reset,
        op_memory_state_get,
        op_memory_state_snapshot,
        op_memory_state_validate_reads,
        op_memory_state_version_if_newer,
        op_memory_state_apply_batch,
        op_memory_state_apply_blind_batch,
        op_memory_state_enqueue_batch,
        op_memory_state_await_submission,
        op_memory_socket_send,
        op_memory_socket_close,
        op_memory_socket_list,
        op_memory_socket_consume_close,
        op_memory_scope_enter,
        op_memory_scope_exit,
        op_memory_transport_send_stream,
        op_memory_transport_send_datagram,
        op_memory_transport_recv_stream,
        op_memory_transport_recv_datagram,
        op_memory_transport_close,
        op_memory_transport_list,
        op_memory_transport_consume_close,
        op_emit_completion,
        op_emit_wait_until_done,
        op_emit_response_start,
        op_emit_response_chunk,
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
    request_id: String,
    receiver: RequestBodyReceiver,
) {
    state
        .borrow_mut::<RequestBodyStreams>()
        .streams
        .insert(request_id, Arc::new(RequestBodyStream::new(receiver)));
}

pub fn register_memory_request_scope(
    state: &mut OpState,
    request_id: String,
    namespace: String,
    memory_key: String,
) {
    state.borrow_mut::<MemoryRequestScopes>().scopes.insert(
        request_id,
        MemoryRequestScope {
            namespace,
            memory_key,
        },
    );
}

pub fn register_request_secret_context(
    state: &mut OpState,
    request_id: String,
    worker_name: String,
    generation: u64,
    isolate_id: u64,
    dynamic_bindings: Vec<String>,
    dynamic_rpc_bindings: Vec<String>,
    replacements: Vec<(String, String)>,
    egress_allow_hosts: Vec<String>,
) {
    let dynamic_bindings: HashSet<String> = dynamic_bindings
        .into_iter()
        .map(|binding| binding.trim().to_string())
        .filter(|binding| !binding.is_empty())
        .collect();
    let dynamic_rpc_bindings: HashSet<String> = dynamic_rpc_bindings
        .into_iter()
        .map(|binding| binding.trim().to_string())
        .filter(|binding| !binding.is_empty())
        .collect();
    let replacements = replacements
        .into_iter()
        .filter_map(|(placeholder, value)| {
            let key = placeholder.trim().to_string();
            if key.is_empty() {
                return None;
            }
            Some((key, value))
        })
        .collect();
    if let Some(previous) = state.borrow_mut::<RequestSecretContexts>().contexts.insert(
        request_id,
        RequestSecretContext {
            worker_name,
            generation,
            isolate_id,
            dynamic_bindings,
            dynamic_rpc_bindings,
            replacements,
            egress_allow_hosts,
            canceled: Arc::new(AtomicBool::new(false)),
            canceled_notify: Arc::new(Notify::new()),
        },
    ) {
        previous.canceled.store(true, Ordering::SeqCst);
        previous.canceled_notify.notify_waiters();
    }
}

pub fn cancel_request_body_stream(state: &mut OpState, request_id: &str) {
    if let Some(stream) = state.borrow::<RequestBodyStreams>().streams.get(request_id) {
        stream.cancel();
    }
}

pub fn clear_request_body_stream(state: &mut OpState, request_id: &str) {
    if let Some(stream) = state
        .borrow_mut::<RequestBodyStreams>()
        .streams
        .remove(request_id)
    {
        stream.cancel();
    }
}

pub fn clear_memory_request_scope(state: &mut OpState, request_id: &str) {
    state
        .borrow_mut::<MemoryRequestScopes>()
        .scopes
        .remove(request_id);
}

pub fn clear_request_secret_context(state: &mut OpState, request_id: &str) {
    if let Some(context) = state
        .borrow_mut::<RequestSecretContexts>()
        .contexts
        .remove(request_id)
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
        DynamicPendingReplyResult, DynamicPushedReplyPayload,
    };

    #[test]
    fn parse_egress_allow_host_supports_exact_and_port_rules() {
        assert_eq!(
            parse_egress_allow_host("api.example.com"),
            Some(("api.example.com".to_string(), None))
        );
        assert_eq!(
            parse_egress_allow_host("api.example.com:8443"),
            Some(("api.example.com".to_string(), Some(8443)))
        );
        assert_eq!(
            parse_egress_allow_host("*.example.com:8443"),
            Some(("*.example.com".to_string(), Some(8443)))
        );
    }

    #[test]
    fn egress_url_rules_require_matching_port() {
        let allowed = vec!["api.example.com".to_string()];
        let https_default = reqwest::Url::parse("https://api.example.com/path").expect("url");
        let https_alt = reqwest::Url::parse("https://api.example.com:8443/path").expect("url");
        assert!(is_egress_url_allowed(&https_default, &allowed));
        assert!(!is_egress_url_allowed(&https_alt, &allowed));
    }

    #[test]
    fn egress_url_rules_support_explicit_ports_and_wildcards() {
        let allowed = vec![
            "api.example.com:8443".to_string(),
            "*.internal.example.com".to_string(),
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
