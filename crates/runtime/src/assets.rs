pub const WORKER_SPECIFIER: &str = "file:///dd/worker.js";
pub const BOOTSTRAP_SPECIFIER: &str = "ext:bootstrap.js";
/// The units under js/execute_worker run as one script, named for their
/// directory.
pub const EXECUTE_WORKER_SPECIFIER: &str = "ext:execute_worker";

pub const BOOTSTRAP_JS: &str = include_str!("../js/bootstrap.js");

/// The units of js/execute_worker/units.txt, concatenated by build.rs.
pub const EXECUTE_WORKER_JS: &str =
    include_str!(concat!(env!("OUT_DIR"), "/execute_worker.generated.js"));
