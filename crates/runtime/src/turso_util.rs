use common::{PlatformError, Result};
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use turso::Connection;

const BUSY_TIMEOUT: Duration = Duration::from_millis(5_000);
const MAX_BUSY_RETRY_ATTEMPTS: usize = 8;

pub(crate) fn configure_turso_connection(
    conn: &Connection,
    map_error: impl FnOnce(turso::Error) -> PlatformError,
) -> Result<()> {
    conn.busy_timeout(BUSY_TIMEOUT).map_err(map_error)?;
    Ok(())
}

pub(crate) async fn retry_turso_busy<F, Fut, T, E>(
    mut execute: F,
    map_error: impl Fn(E) -> PlatformError,
) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = std::result::Result<T, E>>,
    E: std::fmt::Display,
{
    let mut attempt = 0usize;
    loop {
        match execute().await {
            Ok(value) => return Ok(value),
            Err(error) => {
                attempt += 1;
                if is_retryable_turso_error(&error) && attempt < MAX_BUSY_RETRY_ATTEMPTS {
                    tokio::time::sleep(Duration::from_millis(5 * attempt as u64)).await;
                    continue;
                }
                return Err(map_error(error));
            }
        }
    }
}

pub(crate) fn is_retryable_turso_error(error: &impl std::fmt::Display) -> bool {
    let message = error.to_string().to_ascii_lowercase();
    message.contains("database is locked")
        || message.contains("database table is locked")
        || message.contains("database is busy")
        || message.contains("busy")
        || message.contains("conflict")
}

pub(crate) struct VersionFloor;

impl VersionFloor {
    pub(crate) fn next_i64(counter: &AtomicU64) -> i64 {
        counter.fetch_add(1, Ordering::SeqCst) as i64
    }

    pub(crate) fn set_floor(counter: &AtomicU64, floor: u64) {
        let mut current = counter.load(Ordering::SeqCst);
        while current < floor {
            match counter.compare_exchange(current, floor, Ordering::SeqCst, Ordering::SeqCst) {
                Ok(_) => return,
                Err(observed) => current = observed,
            }
        }
    }

    pub(crate) fn observe_i64(counter: &AtomicU64, version: i64) {
        if version < 0 {
            return;
        }
        Self::set_floor(counter, version as u64 + 1);
    }
}
