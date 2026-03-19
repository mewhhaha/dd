pub const WORKER_SPECIFIER: &str = "file:///grugd/worker.js";
pub const BOOTSTRAP_SPECIFIER: &str = "file:///grugd/bootstrap.js";
pub const INSTALL_SPECIFIER: &str = "file:///grugd/install.js";

pub const BOOTSTRAP_JS: &str = include_str!("../js/bootstrap.js");

const INSTALL_WORKER_TEMPLATE: &str = include_str!("../js/install_worker.js");
const EXECUTE_WORKER_TEMPLATE: &str = include_str!("../js/execute_worker.js");

pub fn install_worker_js() -> String {
    INSTALL_WORKER_TEMPLATE.replace("__WORKER_SPECIFIER__", WORKER_SPECIFIER)
}

pub fn execute_worker_js(request_id: &str, request_json: &str) -> String {
    EXECUTE_WORKER_TEMPLATE
        .replace("__REQUEST_ID__", request_id)
        .replace("__REQUEST_JSON__", request_json)
}
