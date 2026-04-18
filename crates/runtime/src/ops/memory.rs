use super::*;

#[deno_core::op2]
#[serde]
pub(super) async fn op_memory_invoke_method(
    state: Rc<RefCell<OpState>>,
    #[serde] payload: MemoryInvokeMethodPayload,
) -> MemoryInvokeMethodResult {
    if payload.caller_request_id.trim().is_empty()
        || payload.worker_name.trim().is_empty()
        || payload.binding.trim().is_empty()
        || payload.key.trim().is_empty()
        || payload.method_name.trim().is_empty()
        || payload.request_id.trim().is_empty()
    {
        return MemoryInvokeMethodResult {
            ok: false,
            value: Vec::new(),
            error: "memory method invoke requires caller_request_id, worker_name, binding, key, method_name, request_id"
                .to_string(),
        };
    }
    let (caller_worker_name, caller_generation, caller_isolate_id) =
        match memory_invoke_owner_for_request(&state, &payload.caller_request_id) {
            Ok(value) => value,
            Err(error) => {
                return MemoryInvokeMethodResult {
                    ok: false,
                    value: Vec::new(),
                    error: error.to_string(),
                };
            }
        };
    let request_frame = match encode_memory_invoke_request(&MemoryInvokeRequest {
        worker_name: payload.worker_name,
        binding: payload.binding,
        key: payload.key,
        call: MemoryInvokeCall::Method {
            name: payload.method_name,
            args: payload.args,
            request_id: payload.request_id,
        },
    }) {
        Ok(frame) => frame,
        Err(error) => {
            return MemoryInvokeMethodResult {
                ok: false,
                value: Vec::new(),
                error: format!("memory method invoke encode failed: {error}"),
            };
        }
    };

    let (reply_tx, reply_rx) = oneshot::channel();
    let sender = state.borrow().borrow::<IsolateEventSender>().clone();
    if sender
        .0
        .send(IsolateEventPayload::MemoryInvoke(MemoryInvokeEvent {
            request_frame,
            caller_worker_name,
            caller_generation,
            caller_isolate_id,
            prefer_caller_isolate: payload.prefer_caller_isolate,
            reply: reply_tx,
        }))
        .is_err()
    {
        return MemoryInvokeMethodResult {
            ok: false,
            value: Vec::new(),
            error: "memory method runtime is unavailable".to_string(),
        };
    }

    match reply_rx.await {
        Ok(Ok(frame)) => match decode_memory_invoke_response(&frame) {
            Ok(MemoryInvokeResponse::Method { value }) => MemoryInvokeMethodResult {
                ok: true,
                value,
                error: String::new(),
            },
            Ok(MemoryInvokeResponse::Error(error)) => MemoryInvokeMethodResult {
                ok: false,
                value: Vec::new(),
                error,
            },
            Ok(MemoryInvokeResponse::Fetch(_)) => MemoryInvokeMethodResult {
                ok: false,
                value: Vec::new(),
                error: "memory method invoke received fetch response".to_string(),
            },
            Err(error) => MemoryInvokeMethodResult {
                ok: false,
                value: Vec::new(),
                error: format!("memory method invoke decode failed: {error}"),
            },
        },
        Ok(Err(error)) => MemoryInvokeMethodResult {
            ok: false,
            value: Vec::new(),
            error: error.to_string(),
        },
        Err(_) => MemoryInvokeMethodResult {
            ok: false,
            value: Vec::new(),
            error: "memory method invoke response channel closed".to_string(),
        },
    }
}

pub(super) fn memory_scope_for_request(
    state: &Rc<RefCell<OpState>>,
    request_id: &str,
) -> Result<(String, String)> {
    state
        .borrow()
        .borrow::<MemoryRequestScopes>()
        .scopes
        .get(request_id)
        .cloned()
        .map(|scope| (scope.namespace, scope.memory_key))
        .ok_or_else(|| PlatformError::runtime("memory storage scope is unavailable"))
}

pub(super) fn memory_socket_scope_for_payload(
    state: &Rc<RefCell<OpState>>,
    request_id: &str,
    binding: &str,
    key: &str,
) -> Result<(String, String)> {
    let binding = binding.trim();
    let key = key.trim();
    if !binding.is_empty() && !key.is_empty() {
        return Ok((binding.to_string(), key.to_string()));
    }
    memory_scope_for_request(state, request_id)
}

pub(super) fn memory_storage_scope_for_payload(
    state: &Rc<RefCell<OpState>>,
    request_id: &str,
    binding: &str,
    key: &str,
) -> Result<(String, String)> {
    let binding = binding.trim();
    let key = key.trim();
    if !binding.is_empty() && !key.is_empty() {
        return Ok((binding.to_string(), key.to_string()));
    }
    memory_scope_for_request(state, request_id)
}

#[deno_core::op2]
#[serde]
pub(super) async fn op_memory_state_get(
    state: Rc<RefCell<OpState>>,
    #[serde] payload: MemoryStateGetPayload,
) -> MemoryStateGetResult {
    let started = Instant::now();
    let (namespace, memory_key) = match memory_storage_scope_for_payload(
        &state,
        &payload.request_id,
        &payload.binding,
        &payload.key,
    ) {
        Ok(scope) => scope,
        Err(error) => {
            return MemoryStateGetResult {
                ok: false,
                record: None,
                max_version: -1,
                error: error.to_string(),
            };
        }
    };
    let store = state.borrow().borrow::<MemoryStore>().clone();
    let store_for_read = store.clone();
    let item_key = payload.item_key;
    let read_result = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| PlatformError::internal(error.to_string()))?;
        runtime.block_on(store_for_read.point_read(&namespace, &memory_key, &item_key))
    })
    .join()
    .unwrap_or_else(|_| Err(PlatformError::internal("memory point read worker panicked")));
    match read_result {
        Ok(point) => {
            store.record_profile(
                MemoryProfileMetricKind::OpRead,
                started.elapsed().as_micros() as u64,
                1,
            );
            MemoryStateGetResult {
                ok: true,
                record: point.record.map(|entry| MemoryStateGetEntry {
                    key: entry.key,
                    value: entry.value,
                    encoding: entry.encoding,
                    version: entry.version,
                    deleted: entry.deleted,
                }),
                max_version: point.max_version,
                error: String::new(),
            }
        }
        Err(error) => MemoryStateGetResult {
            ok: false,
            record: None,
            max_version: -1,
            error: error.to_string(),
        },
    }
}

#[deno_core::op2]
#[serde]
pub(super) async fn op_memory_state_snapshot(
    state: Rc<RefCell<OpState>>,
    #[serde] payload: MemoryStateSnapshotPayload,
) -> MemoryStateSnapshotResult {
    let started = Instant::now();
    let (namespace, memory_key) = match memory_storage_scope_for_payload(
        &state,
        &payload.request_id,
        &payload.binding,
        &payload.key,
    ) {
        Ok(scope) => scope,
        Err(error) => {
            return MemoryStateSnapshotResult {
                ok: false,
                entries: Vec::new(),
                max_version: -1,
                error: error.to_string(),
            };
        }
    };
    let store = state.borrow().borrow::<MemoryStore>().clone();
    let keys = payload
        .keys
        .into_iter()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    let profile_items = keys.len().max(1) as u64;
    let store_for_snapshot = store.clone();
    let snapshot_result = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| PlatformError::internal(error.to_string()))?;
        runtime.block_on(async move {
            if keys.is_empty() {
                store_for_snapshot.snapshot(&namespace, &memory_key).await
            } else {
                store_for_snapshot
                    .snapshot_keys(&namespace, &memory_key, &keys)
                    .await
            }
        })
    })
    .join()
    .unwrap_or_else(|_| Err(PlatformError::internal("memory snapshot worker panicked")));
    match snapshot_result {
        Ok(snapshot) => {
            store.record_profile(
                MemoryProfileMetricKind::OpSnapshot,
                started.elapsed().as_micros() as u64,
                profile_items,
            );
            MemoryStateSnapshotResult {
                ok: true,
                entries: snapshot
                    .entries
                    .into_iter()
                    .map(|entry| MemoryStateSnapshotEntry {
                        key: entry.key,
                        value: entry.value,
                        encoding: entry.encoding,
                        version: entry.version,
                        deleted: entry.deleted,
                    })
                    .collect(),
                max_version: snapshot.max_version,
                error: String::new(),
            }
        }
        Err(error) => MemoryStateSnapshotResult {
            ok: false,
            entries: Vec::new(),
            max_version: -1,
            error: error.to_string(),
        },
    }
}

#[deno_core::op2]
#[serde]
pub(super) async fn op_memory_state_version_if_newer(
    state: Rc<RefCell<OpState>>,
    #[serde] payload: MemoryStateVersionIfNewerPayload,
) -> MemoryStateVersionIfNewerResult {
    let started = Instant::now();
    let (namespace, memory_key) = match memory_storage_scope_for_payload(
        &state,
        &payload.request_id,
        &payload.binding,
        &payload.key,
    ) {
        Ok(scope) => scope,
        Err(error) => {
            return MemoryStateVersionIfNewerResult {
                ok: false,
                stale: false,
                max_version: -1,
                error: error.to_string(),
            };
        }
    };
    let store = state.borrow().borrow::<MemoryStore>().clone();
    match store
        .version_if_newer(&namespace, &memory_key, payload.known_version)
        .await
    {
        Ok(Some(max_version)) => {
            store.record_profile(
                MemoryProfileMetricKind::OpVersionIfNewer,
                started.elapsed().as_micros() as u64,
                1,
            );
            MemoryStateVersionIfNewerResult {
                ok: true,
                stale: true,
                max_version,
                error: String::new(),
            }
        }
        Ok(None) => {
            store.record_profile(
                MemoryProfileMetricKind::OpVersionIfNewer,
                started.elapsed().as_micros() as u64,
                1,
            );
            MemoryStateVersionIfNewerResult {
                ok: true,
                stale: false,
                max_version: payload.known_version,
                error: String::new(),
            }
        }
        Err(error) => MemoryStateVersionIfNewerResult {
            ok: false,
            stale: false,
            max_version: -1,
            error: error.to_string(),
        },
    }
}

#[deno_core::op2]
#[serde]
pub(super) async fn op_memory_state_validate_reads(
    state: Rc<RefCell<OpState>>,
    #[serde] payload: MemoryStateValidateReadsPayload,
) -> MemoryStateApplyBatchResult {
    let started = Instant::now();
    let (namespace, memory_key) = match memory_storage_scope_for_payload(
        &state,
        &payload.request_id,
        &payload.binding,
        &payload.key,
    ) {
        Ok(scope) => scope,
        Err(error) => {
            return MemoryStateApplyBatchResult {
                ok: false,
                conflict: false,
                max_version: -1,
                error: error.to_string(),
            };
        }
    };
    let reads = payload
        .reads
        .into_iter()
        .map(|dependency| MemoryReadDependency {
            key: dependency.key,
            version: dependency.version,
        })
        .collect::<Vec<_>>();
    let list_gate_version = if payload.list_gate_version < 0 {
        None
    } else {
        Some(payload.list_gate_version)
    };
    let store = state.borrow().borrow::<MemoryStore>().clone();
    match store
        .validate_reads(&namespace, &memory_key, &reads, list_gate_version)
        .await
    {
        Ok(result) => {
            store.record_profile(
                MemoryProfileMetricKind::OpValidateReads,
                started.elapsed().as_micros() as u64,
                reads.len().max(1) as u64,
            );
            MemoryStateApplyBatchResult {
                ok: true,
                conflict: result.conflict,
                max_version: result.max_version,
                error: String::new(),
            }
        }
        Err(error) => MemoryStateApplyBatchResult {
            ok: false,
            conflict: false,
            max_version: -1,
            error: error.to_string(),
        },
    }
}

#[deno_core::op2(fast)]
pub(super) fn op_memory_profile_record_js(
    state: &mut OpState,
    #[string] metric: String,
    duration_us: u32,
    items: u32,
) {
    let kind = match metric.as_str() {
        "js_read_only_total" => MemoryProfileMetricKind::JsReadOnlyTotal,
        "js_freshness_check" => MemoryProfileMetricKind::JsFreshnessCheck,
        "js_hydrate_full" => MemoryProfileMetricKind::JsHydrateFull,
        "js_hydrate_keys" => MemoryProfileMetricKind::JsHydrateKeys,
        "js_txn_commit" => MemoryProfileMetricKind::JsTxnCommit,
        "js_txn_blind_commit" => MemoryProfileMetricKind::JsTxnBlindCommit,
        "js_txn_validate" => MemoryProfileMetricKind::JsTxnValidate,
        "memory_cache_hit" => MemoryProfileMetricKind::JsCacheHit,
        "memory_cache_miss" => MemoryProfileMetricKind::JsCacheMiss,
        "memory_cache_stale" => MemoryProfileMetricKind::JsCacheStale,
        _ => return,
    };
    let store = state.borrow::<MemoryStore>().clone();
    store.record_profile(kind, u64::from(duration_us), u64::from(items.max(1)));
}

#[deno_core::op2]
#[serde]
pub(super) fn op_memory_profile_take(state: &mut OpState) -> MemoryProfileResult {
    let store = state.borrow::<MemoryStore>().clone();
    MemoryProfileResult {
        ok: true,
        snapshot: Some(store.take_profile_snapshot_and_reset()),
        error: String::new(),
    }
}

#[deno_core::op2(fast)]
pub(super) fn op_memory_profile_reset(state: &mut OpState) {
    let store = state.borrow::<MemoryStore>().clone();
    store.reset_profile();
}

#[deno_core::op2]
#[serde]
pub(super) async fn op_memory_state_apply_batch(
    state: Rc<RefCell<OpState>>,
    #[serde] payload: MemoryStateApplyBatchPayload,
) -> MemoryStateApplyBatchResult {
    let started = std::time::Instant::now();
    let mutation_count = payload.mutations.len() as u64;
    let read_count = payload.reads.len() as u64;
    let (namespace, memory_key) = match memory_storage_scope_for_payload(
        &state,
        &payload.request_id,
        &payload.binding,
        &payload.key,
    ) {
        Ok(scope) => scope,
        Err(error) => {
            return MemoryStateApplyBatchResult {
                ok: false,
                conflict: false,
                max_version: -1,
                error: error.to_string(),
            };
        }
    };
    let mutations = payload
        .mutations
        .into_iter()
        .map(|mutation| MemoryBatchMutation {
            key: mutation.key,
            value: mutation.value,
            encoding: mutation.encoding,
            version: mutation.version,
            deleted: mutation.deleted,
        })
        .collect::<Vec<_>>();
    let reads = payload
        .reads
        .into_iter()
        .map(|dependency| MemoryReadDependency {
            key: dependency.key,
            version: dependency.version,
        })
        .collect::<Vec<_>>();
    let expected_base_version = if payload.expected_base_version < 0 {
        Some(-1)
    } else {
        Some(payload.expected_base_version)
    };
    let list_gate_version = if payload.list_gate_version < 0 {
        None
    } else {
        Some(payload.list_gate_version)
    };
    let store = state.borrow().borrow::<MemoryStore>().clone();
    let store_for_apply = store.clone();
    let apply_result = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| PlatformError::internal(error.to_string()))?;
        runtime.block_on(store_for_apply.apply_batch(
            &namespace,
            &memory_key,
            &reads,
            &mutations,
            expected_base_version,
            list_gate_version,
            payload.transactional,
        ))
    })
    .join()
    .unwrap_or_else(|_| Err(PlatformError::internal("memory apply worker panicked")));
    match apply_result {
        Ok(result) => {
            store.record_profile(
                MemoryProfileMetricKind::OpApplyBatch,
                started.elapsed().as_micros() as u64,
                mutation_count + read_count + 1,
            );
            MemoryStateApplyBatchResult {
                ok: true,
                conflict: result.conflict,
                max_version: result.max_version,
                error: String::new(),
            }
        }
        Err(error) => MemoryStateApplyBatchResult {
            ok: false,
            conflict: false,
            max_version: -1,
            error: error.to_string(),
        },
    }
}

#[deno_core::op2]
#[serde]
pub(super) async fn op_memory_state_apply_blind_batch(
    state: Rc<RefCell<OpState>>,
    #[serde] payload: MemoryStateApplyBlindBatchPayload,
) -> MemoryStateApplyBatchResult {
    let started = std::time::Instant::now();
    let mutation_count = payload.mutations.len() as u64;
    let (namespace, memory_key) = match memory_storage_scope_for_payload(
        &state,
        &payload.request_id,
        &payload.binding,
        &payload.key,
    ) {
        Ok(scope) => scope,
        Err(error) => {
            return MemoryStateApplyBatchResult {
                ok: false,
                conflict: false,
                max_version: -1,
                error: error.to_string(),
            };
        }
    };
    let mutations = payload
        .mutations
        .into_iter()
        .map(|mutation| MemoryBatchMutation {
            key: mutation.key,
            value: mutation.value,
            encoding: mutation.encoding,
            version: mutation.version,
            deleted: mutation.deleted,
        })
        .collect::<Vec<_>>();
    let store = state.borrow().borrow::<MemoryStore>().clone();
    let store_for_apply = store.clone();
    let apply_result = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| PlatformError::internal(error.to_string()))?;
        runtime.block_on(store_for_apply.apply_blind_batch(&namespace, &memory_key, &mutations))
    })
    .join()
    .unwrap_or_else(|_| {
        Err(PlatformError::internal(
            "memory blind apply worker panicked",
        ))
    });
    match apply_result {
        Ok(result) => {
            store.record_profile(
                MemoryProfileMetricKind::OpApplyBlindBatch,
                started.elapsed().as_micros() as u64,
                mutation_count + 1,
            );
            MemoryStateApplyBatchResult {
                ok: true,
                conflict: result.conflict,
                max_version: result.max_version,
                error: String::new(),
            }
        }
        Err(error) => MemoryStateApplyBatchResult {
            ok: false,
            conflict: false,
            max_version: -1,
            error: error.to_string(),
        },
    }
}

#[deno_core::op2]
#[serde]
pub(super) async fn op_memory_state_enqueue_batch(
    state: Rc<RefCell<OpState>>,
    #[serde] payload: MemoryStateEnqueueBatchPayload,
) -> MemoryStateEnqueueBatchResult {
    let (namespace, memory_key) = match memory_storage_scope_for_payload(
        &state,
        &payload.request_id,
        &payload.binding,
        &payload.key,
    ) {
        Ok(scope) => scope,
        Err(error) => {
            return MemoryStateEnqueueBatchResult {
                ok: false,
                submission_id: 0,
                error: error.to_string(),
            };
        }
    };
    let mutations = payload
        .mutations
        .into_iter()
        .map(|mutation| MemoryDirectMutation {
            key: mutation.key,
            value: mutation.value,
            encoding: mutation.encoding,
            deleted: mutation.deleted,
        })
        .collect::<Vec<_>>();
    let store = state.borrow().borrow::<MemoryStore>().clone();
    let store_for_enqueue = store.clone();
    let enqueue_result = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| PlatformError::internal(error.to_string()))?;
        runtime.block_on(store_for_enqueue.enqueue_direct_batch(
            &namespace,
            &memory_key,
            &mutations,
        ))
    })
    .join()
    .unwrap_or_else(|_| {
        Err(PlatformError::internal(
            "memory direct enqueue worker panicked",
        ))
    });
    match enqueue_result {
        Ok(submission_id) => MemoryStateEnqueueBatchResult {
            ok: true,
            submission_id,
            error: String::new(),
        },
        Err(error) => MemoryStateEnqueueBatchResult {
            ok: false,
            submission_id: 0,
            error: error.to_string(),
        },
    }
}

#[deno_core::op2]
#[serde]
pub(super) async fn op_memory_state_await_submission(
    state: Rc<RefCell<OpState>>,
    #[serde] payload: MemoryStateAwaitSubmissionPayload,
) -> MemoryStateAwaitSubmissionResult {
    let store = state.borrow().borrow::<MemoryStore>().clone();
    let started = Instant::now();
    loop {
        match store.try_poll_direct_submission(payload.submission_id) {
            Ok(Some(max_version)) => {
                store.record_profile(
                    MemoryProfileMetricKind::StoreDirectAwait,
                    started.elapsed().as_micros() as u64,
                    1,
                );
                return MemoryStateAwaitSubmissionResult {
                    ok: true,
                    pending: false,
                    max_version,
                    error: String::new(),
                };
            }
            Ok(None) if started.elapsed() < Duration::from_secs(30) => {
                std::thread::sleep(Duration::from_millis(1));
            }
            Ok(None) => {
                return MemoryStateAwaitSubmissionResult {
                    ok: false,
                    pending: false,
                    max_version: -1,
                    error: format!(
                        "memory direct write submission {} timed out",
                        payload.submission_id
                    ),
                };
            }
            Err(error) => {
                return MemoryStateAwaitSubmissionResult {
                    ok: false,
                    pending: false,
                    max_version: -1,
                    error: error.to_string(),
                };
            }
        }
    }
}

#[deno_core::op2]
#[serde]
pub(super) async fn op_memory_socket_send(
    state: Rc<RefCell<OpState>>,
    #[serde] payload: MemorySocketSendPayload,
) -> MemorySocketSendResult {
    if payload.request_id.trim().is_empty() {
        return MemorySocketSendResult {
            ok: false,
            error: "memory socket send requires request_id".to_string(),
        };
    }
    if payload.handle.trim().is_empty() {
        return MemorySocketSendResult {
            ok: false,
            error: "memory socket send requires handle".to_string(),
        };
    }
    let normalized_kind = payload.message_kind.as_str();
    let is_text = match normalized_kind {
        "text" => true,
        "binary" => false,
        _ => {
            return MemorySocketSendResult {
                ok: false,
                error: format!("unsupported message kind: {normalized_kind}"),
            };
        }
    };

    let (binding, key) = match memory_socket_scope_for_payload(
        &state,
        &payload.request_id,
        &payload.binding,
        &payload.key,
    ) {
        Ok(value) => value,
        Err(error) => {
            return MemorySocketSendResult {
                ok: false,
                error: error.to_string(),
            }
        }
    };

    let (reply_tx, reply_rx) = oneshot::channel();
    let sender = state.borrow().borrow::<IsolateEventSender>().clone();
    if sender
        .0
        .send(IsolateEventPayload::MemorySocketSend(
            MemorySocketSendEvent {
                reply: reply_tx,
                handle: payload.handle,
                binding,
                key,
                is_text,
                message: payload.message,
            },
        ))
        .is_err()
    {
        return MemorySocketSendResult {
            ok: false,
            error: "memory socket send runtime is unavailable".to_string(),
        };
    }

    drop(reply_rx);
    MemorySocketSendResult {
        ok: true,
        error: String::new(),
    }
}

#[deno_core::op2]
#[serde]
pub(super) async fn op_memory_socket_close(
    state: Rc<RefCell<OpState>>,
    #[serde] payload: MemorySocketClosePayload,
) -> MemorySocketCloseResult {
    if payload.request_id.trim().is_empty() {
        return MemorySocketCloseResult {
            ok: false,
            error: "memory socket close requires request_id".to_string(),
        };
    }
    if payload.handle.trim().is_empty() {
        return MemorySocketCloseResult {
            ok: false,
            error: "memory socket close requires handle".to_string(),
        };
    }

    let (binding, key) = match memory_socket_scope_for_payload(
        &state,
        &payload.request_id,
        &payload.binding,
        &payload.key,
    ) {
        Ok(value) => value,
        Err(error) => {
            return MemorySocketCloseResult {
                ok: false,
                error: error.to_string(),
            };
        }
    };

    let (reply_tx, reply_rx) = oneshot::channel();
    let sender = state.borrow().borrow::<IsolateEventSender>().clone();
    if sender
        .0
        .send(IsolateEventPayload::MemorySocketClose(
            MemorySocketCloseEvent {
                reply: reply_tx,
                handle: payload.handle,
                binding,
                key,
                code: payload.code,
                reason: payload.reason,
            },
        ))
        .is_err()
    {
        return MemorySocketCloseResult {
            ok: false,
            error: "memory socket close runtime is unavailable".to_string(),
        };
    }

    drop(reply_rx);
    MemorySocketCloseResult {
        ok: true,
        error: String::new(),
    }
}

#[deno_core::op2]
#[serde]
pub(super) async fn op_memory_socket_list(
    state: Rc<RefCell<OpState>>,
    #[serde] payload: MemorySocketListPayload,
) -> MemorySocketListResult {
    if payload.request_id.trim().is_empty() {
        return MemorySocketListResult {
            ok: false,
            handles: Vec::new(),
            error: "memory socket list requires request_id".to_string(),
        };
    }

    let (binding, key) = match memory_socket_scope_for_payload(
        &state,
        &payload.request_id,
        &payload.binding,
        &payload.key,
    ) {
        Ok(value) => value,
        Err(error) => {
            return MemorySocketListResult {
                ok: false,
                handles: Vec::new(),
                error: error.to_string(),
            };
        }
    };

    let registry = state.borrow().borrow::<MemoryOpenHandleRegistry>().clone();
    MemorySocketListResult {
        ok: true,
        handles: registry.list_socket_handles(&binding, &key),
        error: String::new(),
    }
}

#[deno_core::op2]
#[serde]
pub(super) async fn op_memory_socket_consume_close(
    state: Rc<RefCell<OpState>>,
    #[serde] payload: MemorySocketConsumeClosePayload,
) -> MemorySocketConsumeCloseResult {
    if payload.request_id.trim().is_empty() {
        return MemorySocketConsumeCloseResult {
            ok: false,
            events: Vec::new(),
            error: "memory socket consumeClose requires request_id".to_string(),
        };
    }
    if payload.handle.trim().is_empty() {
        return MemorySocketConsumeCloseResult {
            ok: false,
            events: Vec::new(),
            error: "memory socket consumeClose requires handle".to_string(),
        };
    }

    let (binding, key) = match memory_socket_scope_for_payload(
        &state,
        &payload.request_id,
        &payload.binding,
        &payload.key,
    ) {
        Ok(value) => value,
        Err(error) => {
            return MemorySocketConsumeCloseResult {
                ok: false,
                events: Vec::new(),
                error: error.to_string(),
            };
        }
    };

    let (reply_tx, reply_rx) = oneshot::channel();
    let sender = state.borrow().borrow::<IsolateEventSender>().clone();
    if sender
        .0
        .send(IsolateEventPayload::MemorySocketConsumeClose(
            MemorySocketConsumeCloseEvent {
                reply: reply_tx,
                binding,
                key,
                handle: payload.handle,
            },
        ))
        .is_err()
    {
        return MemorySocketConsumeCloseResult {
            ok: false,
            events: Vec::new(),
            error: "memory socket consumeClose runtime is unavailable".to_string(),
        };
    }

    match reply_rx.await {
        Ok(Ok(events)) => MemorySocketConsumeCloseResult {
            ok: true,
            events: events
                .into_iter()
                .map(|event| MemorySocketReplayClose {
                    code: event.code,
                    reason: event.reason,
                })
                .collect(),
            error: String::new(),
        },
        Ok(Err(error)) => MemorySocketConsumeCloseResult {
            ok: false,
            events: Vec::new(),
            error: error.to_string(),
        },
        Err(_) => MemorySocketConsumeCloseResult {
            ok: false,
            events: Vec::new(),
            error: "memory socket consumeClose response channel closed".to_string(),
        },
    }
}

pub(super) fn memory_transport_scope_for_payload(
    state: &Rc<RefCell<OpState>>,
    request_id: &str,
    binding: &str,
    key: &str,
) -> std::result::Result<(String, String), PlatformError> {
    let binding = binding.trim();
    let key = key.trim();
    if !binding.is_empty() && !key.is_empty() {
        return Ok((binding.to_string(), key.to_string()));
    }
    memory_scope_for_request(state, request_id)
}

#[deno_core::op2]
#[serde]
pub(super) fn op_memory_scope_enter(
    state: &mut OpState,
    #[serde] payload: MemoryScopePayload,
) -> MemoryScopeResult {
    if payload.request_id.trim().is_empty() {
        return MemoryScopeResult {
            ok: false,
            error: "memory scope enter requires request_id".to_string(),
        };
    }
    if payload.binding.trim().is_empty() || payload.key.trim().is_empty() {
        return MemoryScopeResult {
            ok: false,
            error: "memory scope enter requires binding and key".to_string(),
        };
    }
    register_memory_request_scope(state, payload.request_id, payload.binding, payload.key);
    MemoryScopeResult {
        ok: true,
        error: String::new(),
    }
}

#[deno_core::op2]
#[serde]
pub(super) fn op_memory_scope_exit(
    state: &mut OpState,
    #[serde] payload: MemoryScopeClearPayload,
) -> MemoryScopeResult {
    if payload.request_id.trim().is_empty() {
        return MemoryScopeResult {
            ok: false,
            error: "memory scope exit requires request_id".to_string(),
        };
    }
    clear_memory_request_scope(state, &payload.request_id);
    MemoryScopeResult {
        ok: true,
        error: String::new(),
    }
}

#[deno_core::op2]
#[serde]
pub(super) async fn op_memory_transport_send_stream(
    state: Rc<RefCell<OpState>>,
    #[serde] payload: MemoryTransportSendStreamPayload,
) -> MemoryTransportSendResult {
    if payload.request_id.trim().is_empty() {
        return MemoryTransportSendResult {
            ok: false,
            error: "memory transport sendStream requires request_id".to_string(),
        };
    }
    if payload.handle.trim().is_empty() {
        return MemoryTransportSendResult {
            ok: false,
            error: "memory transport sendStream requires handle".to_string(),
        };
    }

    let (binding, key) = match memory_transport_scope_for_payload(
        &state,
        &payload.request_id,
        &payload.binding,
        &payload.key,
    ) {
        Ok(value) => value,
        Err(error) => {
            return MemoryTransportSendResult {
                ok: false,
                error: error.to_string(),
            };
        }
    };

    let (reply_tx, reply_rx) = oneshot::channel();
    let sender = state.borrow().borrow::<IsolateEventSender>().clone();
    if sender
        .0
        .send(IsolateEventPayload::MemoryTransportSendStream(
            MemoryTransportSendStreamEvent {
                reply: reply_tx,
                handle: payload.handle,
                binding,
                key,
                chunk: payload.chunk,
            },
        ))
        .is_err()
    {
        return MemoryTransportSendResult {
            ok: false,
            error: "memory transport sendStream runtime is unavailable".to_string(),
        };
    }

    match reply_rx.await {
        Ok(Ok(())) => MemoryTransportSendResult {
            ok: true,
            error: String::new(),
        },
        Ok(Err(error)) => MemoryTransportSendResult {
            ok: false,
            error: error.to_string(),
        },
        Err(_) => MemoryTransportSendResult {
            ok: false,
            error: "memory transport sendStream response channel closed".to_string(),
        },
    }
}

#[deno_core::op2]
#[serde]
pub(super) async fn op_memory_transport_send_datagram(
    state: Rc<RefCell<OpState>>,
    #[serde] payload: MemoryTransportSendDatagramPayload,
) -> MemoryTransportSendResult {
    if payload.request_id.trim().is_empty() {
        return MemoryTransportSendResult {
            ok: false,
            error: "memory transport sendDatagram requires request_id".to_string(),
        };
    }
    if payload.handle.trim().is_empty() {
        return MemoryTransportSendResult {
            ok: false,
            error: "memory transport sendDatagram requires handle".to_string(),
        };
    }

    let (binding, key) = match memory_transport_scope_for_payload(
        &state,
        &payload.request_id,
        &payload.binding,
        &payload.key,
    ) {
        Ok(value) => value,
        Err(error) => {
            return MemoryTransportSendResult {
                ok: false,
                error: error.to_string(),
            };
        }
    };

    let (reply_tx, reply_rx) = oneshot::channel();
    let sender = state.borrow().borrow::<IsolateEventSender>().clone();
    if sender
        .0
        .send(IsolateEventPayload::MemoryTransportSendDatagram(
            MemoryTransportSendDatagramEvent {
                reply: reply_tx,
                handle: payload.handle,
                binding,
                key,
                datagram: payload.datagram,
            },
        ))
        .is_err()
    {
        return MemoryTransportSendResult {
            ok: false,
            error: "memory transport sendDatagram runtime is unavailable".to_string(),
        };
    }

    match reply_rx.await {
        Ok(Ok(())) => MemoryTransportSendResult {
            ok: true,
            error: String::new(),
        },
        Ok(Err(error)) => MemoryTransportSendResult {
            ok: false,
            error: error.to_string(),
        },
        Err(_) => MemoryTransportSendResult {
            ok: false,
            error: "memory transport sendDatagram response channel closed".to_string(),
        },
    }
}

#[deno_core::op2]
#[serde]
pub(super) async fn op_memory_transport_recv_stream(
    state: Rc<RefCell<OpState>>,
    #[serde] payload: MemoryTransportRecvPayload,
) -> MemoryTransportRecvResult {
    if payload.request_id.trim().is_empty() {
        return MemoryTransportRecvResult {
            ok: false,
            done: true,
            chunk: Vec::new(),
            error: "memory transport recvStream requires request_id".to_string(),
        };
    }
    if payload.handle.trim().is_empty() {
        return MemoryTransportRecvResult {
            ok: false,
            done: true,
            chunk: Vec::new(),
            error: "memory transport recvStream requires handle".to_string(),
        };
    }

    let (binding, key) = match memory_transport_scope_for_payload(
        &state,
        &payload.request_id,
        &payload.binding,
        &payload.key,
    ) {
        Ok(value) => value,
        Err(error) => {
            return MemoryTransportRecvResult {
                ok: false,
                done: true,
                chunk: Vec::new(),
                error: error.to_string(),
            };
        }
    };

    let (reply_tx, reply_rx) = oneshot::channel();
    let sender = state.borrow().borrow::<IsolateEventSender>().clone();
    if sender
        .0
        .send(IsolateEventPayload::MemoryTransportRecvStream(
            MemoryTransportRecvStreamEvent {
                reply: reply_tx,
                handle: payload.handle,
                binding,
                key,
            },
        ))
        .is_err()
    {
        return MemoryTransportRecvResult {
            ok: false,
            done: true,
            chunk: Vec::new(),
            error: "memory transport recvStream runtime is unavailable".to_string(),
        };
    }

    match reply_rx.await {
        Ok(Ok(event)) => MemoryTransportRecvResult {
            ok: true,
            done: event.done,
            chunk: event.chunk,
            error: String::new(),
        },
        Ok(Err(error)) => MemoryTransportRecvResult {
            ok: false,
            done: true,
            chunk: Vec::new(),
            error: error.to_string(),
        },
        Err(_) => MemoryTransportRecvResult {
            ok: false,
            done: true,
            chunk: Vec::new(),
            error: "memory transport recvStream response channel closed".to_string(),
        },
    }
}

#[deno_core::op2]
#[serde]
pub(super) async fn op_memory_transport_recv_datagram(
    state: Rc<RefCell<OpState>>,
    #[serde] payload: MemoryTransportRecvPayload,
) -> MemoryTransportRecvResult {
    if payload.request_id.trim().is_empty() {
        return MemoryTransportRecvResult {
            ok: false,
            done: true,
            chunk: Vec::new(),
            error: "memory transport recvDatagram requires request_id".to_string(),
        };
    }
    if payload.handle.trim().is_empty() {
        return MemoryTransportRecvResult {
            ok: false,
            done: true,
            chunk: Vec::new(),
            error: "memory transport recvDatagram requires handle".to_string(),
        };
    }

    let (binding, key) = match memory_transport_scope_for_payload(
        &state,
        &payload.request_id,
        &payload.binding,
        &payload.key,
    ) {
        Ok(value) => value,
        Err(error) => {
            return MemoryTransportRecvResult {
                ok: false,
                done: true,
                chunk: Vec::new(),
                error: error.to_string(),
            };
        }
    };

    let (reply_tx, reply_rx) = oneshot::channel();
    let sender = state.borrow().borrow::<IsolateEventSender>().clone();
    if sender
        .0
        .send(IsolateEventPayload::MemoryTransportRecvDatagram(
            MemoryTransportRecvDatagramEvent {
                reply: reply_tx,
                handle: payload.handle,
                binding,
                key,
            },
        ))
        .is_err()
    {
        return MemoryTransportRecvResult {
            ok: false,
            done: true,
            chunk: Vec::new(),
            error: "memory transport recvDatagram runtime is unavailable".to_string(),
        };
    }

    match reply_rx.await {
        Ok(Ok(event)) => MemoryTransportRecvResult {
            ok: true,
            done: event.done,
            chunk: event.chunk,
            error: String::new(),
        },
        Ok(Err(error)) => MemoryTransportRecvResult {
            ok: false,
            done: true,
            chunk: Vec::new(),
            error: error.to_string(),
        },
        Err(_) => MemoryTransportRecvResult {
            ok: false,
            done: true,
            chunk: Vec::new(),
            error: "memory transport recvDatagram response channel closed".to_string(),
        },
    }
}

#[deno_core::op2]
#[serde]
pub(super) async fn op_memory_transport_close(
    state: Rc<RefCell<OpState>>,
    #[serde] payload: MemoryTransportClosePayload,
) -> MemoryTransportCloseResult {
    if payload.request_id.trim().is_empty() {
        return MemoryTransportCloseResult {
            ok: false,
            error: "memory transport close requires request_id".to_string(),
        };
    }
    if payload.handle.trim().is_empty() {
        return MemoryTransportCloseResult {
            ok: false,
            error: "memory transport close requires handle".to_string(),
        };
    }

    let (binding, key) = match memory_transport_scope_for_payload(
        &state,
        &payload.request_id,
        &payload.binding,
        &payload.key,
    ) {
        Ok(value) => value,
        Err(error) => {
            return MemoryTransportCloseResult {
                ok: false,
                error: error.to_string(),
            };
        }
    };

    let (reply_tx, reply_rx) = oneshot::channel();
    let sender = state.borrow().borrow::<IsolateEventSender>().clone();
    if sender
        .0
        .send(IsolateEventPayload::MemoryTransportClose(
            MemoryTransportCloseEvent {
                reply: reply_tx,
                handle: payload.handle,
                binding,
                key,
                code: payload.code,
                reason: payload.reason,
            },
        ))
        .is_err()
    {
        return MemoryTransportCloseResult {
            ok: false,
            error: "memory transport close runtime is unavailable".to_string(),
        };
    }

    match reply_rx.await {
        Ok(Ok(())) => MemoryTransportCloseResult {
            ok: true,
            error: String::new(),
        },
        Ok(Err(error)) => MemoryTransportCloseResult {
            ok: false,
            error: error.to_string(),
        },
        Err(_) => MemoryTransportCloseResult {
            ok: false,
            error: "memory transport close response channel closed".to_string(),
        },
    }
}

#[deno_core::op2]
#[serde]
pub(super) async fn op_memory_transport_list(
    state: Rc<RefCell<OpState>>,
    #[serde] payload: MemoryTransportListPayload,
) -> MemoryTransportListResult {
    if payload.request_id.trim().is_empty() {
        return MemoryTransportListResult {
            ok: false,
            handles: Vec::new(),
            error: "memory transport list requires request_id".to_string(),
        };
    }

    let (binding, key) = match memory_transport_scope_for_payload(
        &state,
        &payload.request_id,
        &payload.binding,
        &payload.key,
    ) {
        Ok(value) => value,
        Err(error) => {
            return MemoryTransportListResult {
                ok: false,
                handles: Vec::new(),
                error: error.to_string(),
            };
        }
    };

    let registry = state.borrow().borrow::<MemoryOpenHandleRegistry>().clone();
    MemoryTransportListResult {
        ok: true,
        handles: registry.list_transport_handles(&binding, &key),
        error: String::new(),
    }
}

#[deno_core::op2]
#[serde]
pub(super) async fn op_memory_transport_consume_close(
    state: Rc<RefCell<OpState>>,
    #[serde] payload: MemoryTransportConsumeClosePayload,
) -> MemoryTransportConsumeCloseResult {
    if payload.request_id.trim().is_empty() {
        return MemoryTransportConsumeCloseResult {
            ok: false,
            events: Vec::new(),
            error: "memory transport consumeClose requires request_id".to_string(),
        };
    }
    if payload.handle.trim().is_empty() {
        return MemoryTransportConsumeCloseResult {
            ok: false,
            events: Vec::new(),
            error: "memory transport consumeClose requires handle".to_string(),
        };
    }

    let (binding, key) = match memory_transport_scope_for_payload(
        &state,
        &payload.request_id,
        &payload.binding,
        &payload.key,
    ) {
        Ok(value) => value,
        Err(error) => {
            return MemoryTransportConsumeCloseResult {
                ok: false,
                events: Vec::new(),
                error: error.to_string(),
            };
        }
    };

    let (reply_tx, reply_rx) = oneshot::channel();
    let sender = state.borrow().borrow::<IsolateEventSender>().clone();
    if sender
        .0
        .send(IsolateEventPayload::MemoryTransportConsumeClose(
            MemoryTransportConsumeCloseEvent {
                reply: reply_tx,
                binding,
                key,
                handle: payload.handle,
            },
        ))
        .is_err()
    {
        return MemoryTransportConsumeCloseResult {
            ok: false,
            events: Vec::new(),
            error: "memory transport consumeClose runtime is unavailable".to_string(),
        };
    }

    match reply_rx.await {
        Ok(Ok(events)) => MemoryTransportConsumeCloseResult {
            ok: true,
            events: events
                .into_iter()
                .map(|event| MemoryTransportReplayClose {
                    code: event.code,
                    reason: event.reason,
                })
                .collect(),
            error: String::new(),
        },
        Ok(Err(error)) => MemoryTransportConsumeCloseResult {
            ok: false,
            events: Vec::new(),
            error: error.to_string(),
        },
        Err(_) => MemoryTransportConsumeCloseResult {
            ok: false,
            events: Vec::new(),
            error: "memory transport consumeClose response channel closed".to_string(),
        },
    }
}
