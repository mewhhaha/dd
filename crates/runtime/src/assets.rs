pub const WORKER_SPECIFIER: &str = "file:///dd/worker.js";
pub const BOOTSTRAP_SPECIFIER: &str = "file:///dd/bootstrap.js";
pub const INSTALL_SPECIFIER: &str = "file:///dd/install.js";

pub const BOOTSTRAP_JS: &str = include_str!("../js/bootstrap.js");

const INSTALL_WORKER_TEMPLATE: &str = include_str!("../js/install_worker.js");
const EXECUTE_WORKER_TEMPLATE: &str = include_str!("../js/execute_worker.js");
const ABORT_WORKER_TEMPLATE: &str = include_str!("../js/abort_worker.js");

pub fn install_worker_js() -> String {
    let install = INSTALL_WORKER_TEMPLATE.replace("__WORKER_SPECIFIER__", WORKER_SPECIFIER);
    format!("{install}\n{EXECUTE_WORKER_TEMPLATE}")
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
    stream_response: bool,
    request_json: &str,
) -> String {
    format!(
        "globalThis.__dd_execute_worker({{\
request_id:{request_id},\
completion_token:{completion_token},\
worker_name:{worker_name},\
kv_bindings:{kv_bindings_json},\
actor_bindings:{actor_bindings_json},\
dynamic_bindings:{dynamic_bindings_json},\
dynamic_rpc_bindings:{dynamic_rpc_bindings_json},\
dynamic_env:{dynamic_env_json},\
actor_call:{actor_call_json},\
host_rpc_call:{host_rpc_call_json},\
has_request_body_stream:{has_request_body_stream},\
stream_response:{stream_response},\
request:{request_json}\
}});",
        has_request_body_stream = if has_request_body_stream {
            "true"
        } else {
            "false"
        },
        stream_response = if stream_response { "true" } else { "false" },
    )
}

pub fn abort_worker_js(request_id: &str) -> String {
    ABORT_WORKER_TEMPLATE.replace("__REQUEST_ID__", request_id)
}
