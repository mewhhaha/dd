pub const WORKER_SPECIFIER: &str = "file:///dd/worker.js";
pub const BOOTSTRAP_SPECIFIER: &str = "file:///dd/bootstrap.js";
pub const INSTALL_SPECIFIER: &str = "file:///dd/install.js";

pub const BOOTSTRAP_JS: &str = include_str!("../js/bootstrap.js");

const INSTALL_WORKER_TEMPLATE: &str = include_str!("../js/install_worker.js");
const EXECUTE_WORKER_TEMPLATE: &str =
    include_str!(concat!(env!("OUT_DIR"), "/execute_worker.generated.js"));

pub fn install_worker_js() -> String {
    let install = INSTALL_WORKER_TEMPLATE.replace("__WORKER_SPECIFIER__", WORKER_SPECIFIER);
    format!("{install}\n{EXECUTE_WORKER_TEMPLATE}")
}
