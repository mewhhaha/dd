pub const WORKER_SPECIFIER: &str = "file:///dd/worker.js";
pub const BOOTSTRAP_SPECIFIER: &str = "file:///dd/bootstrap.js";
pub const EXECUTE_WORKER_SPECIFIER: &str = "file:///dd/execute_worker.js";

pub const BOOTSTRAP_JS: &str = include_str!("../js/bootstrap.js");

/// The units of js/execute_worker/units.txt, concatenated by build.rs.
pub const EXECUTE_WORKER_JS: &str =
    include_str!(concat!(env!("OUT_DIR"), "/execute_worker.generated.js"));
