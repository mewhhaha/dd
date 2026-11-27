//! Process-wide runtime setup (Scriptw + wtime x64 AOT).

use std::sync::{Once, OnceLock};

static INIT: Once = Once::new();
static USER_FLAGS: OnceLock<Vec<String>> = OnceLock::new();

/// Records optional engine flags. Must run before the first isolate is created;
/// a later call with the same flags is a no-op and a call with different flags
/// is an error.
pub fn set_flags(flags: &[String]) -> Result<(), String> {
    let normalized = flags
        .iter()
        .map(|flag| flag.trim())
        .filter(|flag| !flag.is_empty())
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    if INIT.is_completed() && !normalized.is_empty() && USER_FLAGS.get() != Some(&normalized) {
        return Err(format!(
            "engine flags {normalized:?} must be set before the first isolate is created"
        ));
    }
    if let Some(existing) = USER_FLAGS.get() {
        if existing != &normalized {
            return Err(format!(
                "engine flags were already initialized as {existing:?}; cannot reinitialize with {normalized:?}"
            ));
        }
        return Ok(());
    }
    let _ = USER_FLAGS.set(normalized);
    Ok(())
}

/// Initializes process-wide Scriptw compilation job count once per process.
pub fn init() {
    INIT.call_once(|| {
        // Default to bounded single-job compilation unless configured earlier.
        let _ = scriptw_engine::configure_compilation_jobs(1);
    });
}
