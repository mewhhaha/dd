pub const WORKER_SPECIFIER: &str = "file:///dd/worker.js";
pub const BOOTSTRAP_SPECIFIER: &str = "file:///dd/bootstrap.js";
pub const INSTALL_SPECIFIER: &str = "file:///dd/install.js";

pub const BOOTSTRAP_JS: &str = include_str!("../js/bootstrap.js");

const INSTALL_WORKER_TEMPLATE: &str = include_str!("../js/install_worker.js");
const EXECUTE_WORKER_TEMPLATE: &str = include_str!("../js/execute_worker.js");
const ABORT_WORKER_TEMPLATE: &str = include_str!("../js/abort_worker.js");

pub fn install_worker_js() -> String {
    INSTALL_WORKER_TEMPLATE.replace("__WORKER_SPECIFIER__", WORKER_SPECIFIER)
}

pub fn execute_worker_js(
    worker_name: &str,
    kv_bindings_json: &str,
    actor_bindings_json: &str,
    dynamic_bindings_json: &str,
    dynamic_rpc_bindings_json: &str,
    dynamic_env_json: &str,
    actor_call_json: &str,
    host_rpc_call_json: &str,
    request_id: &str,
    completion_token: &str,
    has_request_body_stream: bool,
    request_json: &str,
) -> String {
    EXECUTE_WORKER_TEMPLATE
        .replace("__WORKER_NAME__", worker_name)
        .replace("__KV_BINDINGS_JSON__", kv_bindings_json)
        .replace("__ACTOR_BINDINGS_JSON__", actor_bindings_json)
        .replace("__DYNAMIC_BINDINGS_JSON__", dynamic_bindings_json)
        .replace("__DYNAMIC_RPC_BINDINGS_JSON__", dynamic_rpc_bindings_json)
        .replace("__DYNAMIC_ENV_JSON__", dynamic_env_json)
        .replace("__ACTOR_CALL_JSON__", actor_call_json)
        .replace("__HOST_RPC_CALL_JSON__", host_rpc_call_json)
        .replace("__REQUEST_ID__", request_id)
        .replace("__COMPLETION_TOKEN__", completion_token)
        .replace(
            "__HAS_REQUEST_BODY_STREAM__",
            if has_request_body_stream {
                "true"
            } else {
                "false"
            },
        )
        .replace("__REQUEST_JSON__", request_json)
}

pub fn abort_worker_js(request_id: &str) -> String {
    ABORT_WORKER_TEMPLATE.replace("__REQUEST_ID__", request_id)
}
