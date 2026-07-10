use common::{PlatformError, Result};
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use turso::{Connection, IntoParams, Rows};

const BUSY_TIMEOUT: Duration = Duration::from_millis(5_000);
const MAX_BUSY_RETRY_ATTEMPTS: usize = 8;
static STORAGE_RETRY_COUNT: AtomicU64 = AtomicU64::new(0);

pub(crate) fn configure_turso_connection(
    conn: &Connection,
    map_error: impl FnOnce(turso::Error) -> PlatformError,
) -> Result<()> {
    conn.busy_timeout(BUSY_TIMEOUT).map_err(map_error)?;
    Ok(())
}

/// Query using Turso's per-connection prepared-statement cache.
///
/// Requiring a static SQL string keeps generated selectors and variable-sized
/// `IN` queries from growing the cache without bound.
pub(crate) async fn query_cached(
    conn: &Connection,
    sql: &'static str,
    params: impl IntoParams,
) -> turso::Result<Rows> {
    let mut statement = conn.prepare_cached(sql).await?;
    statement.query(params).await
}

/// Execute using Turso's per-connection prepared-statement cache.
///
/// See [`query_cached`] for why this accepts only static SQL.
pub(crate) async fn execute_cached(
    conn: &Connection,
    sql: &'static str,
    params: impl IntoParams,
) -> turso::Result<u64> {
    let mut statement = conn.prepare_cached(sql).await?;
    statement.execute(params).await
}

pub(crate) async fn ensure_storage_migration_table(conn: &Connection) -> turso::Result<()> {
    conn.execute(
        "CREATE TABLE IF NOT EXISTS dd_storage_schema_migrations (
           component TEXT NOT NULL,
           version INTEGER NOT NULL,
           applied_at_ms INTEGER NOT NULL,
           PRIMARY KEY (component, version)
         )",
        (),
    )
    .await?;
    Ok(())
}

pub(crate) async fn storage_schema_version(
    conn: &Connection,
    component: &str,
) -> turso::Result<i64> {
    let mut rows = query_cached(
        conn,
        "SELECT COALESCE(MAX(version), 0)
         FROM dd_storage_schema_migrations
         WHERE component = ?1",
        (component,),
    )
    .await?;
    let version = rows
        .next()
        .await?
        .map(|row| row.get::<i64>(0))
        .transpose()?
        .unwrap_or(0);
    Ok(version)
}

pub(crate) async fn record_storage_schema_version(
    conn: &Connection,
    component: &str,
    version: i64,
    applied_at_ms: i64,
) -> turso::Result<()> {
    execute_cached(
        conn,
        "INSERT INTO dd_storage_schema_migrations (component, version, applied_at_ms)
         VALUES (?1, ?2, ?3)
         ON CONFLICT(component, version) DO NOTHING",
        (component, version, applied_at_ms),
    )
    .await?;
    Ok(())
}

pub(crate) async fn retry_turso_busy<F, Fut, T>(
    mut execute: F,
    map_error: impl Fn(turso::Error) -> PlatformError,
) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = turso::Result<T>>,
{
    let mut attempt = 0usize;
    loop {
        match execute().await {
            Ok(value) => return Ok(value),
            Err(error) => {
                attempt += 1;
                if is_retryable_turso_error(&error) && attempt < MAX_BUSY_RETRY_ATTEMPTS {
                    record_storage_retry();
                    tokio::time::sleep(Duration::from_millis(5 * attempt as u64)).await;
                    continue;
                }
                return Err(map_error(error));
            }
        }
    }
}

pub(crate) fn record_storage_retry() {
    STORAGE_RETRY_COUNT.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn storage_retry_count() -> u64 {
    STORAGE_RETRY_COUNT.load(Ordering::Relaxed)
}

pub(crate) async fn checkpoint_database(database: &turso::Database) -> turso::Result<()> {
    let conn = database.connect()?;
    conn.busy_timeout(BUSY_TIMEOUT)?;
    let mut rows = conn.query("PRAGMA wal_checkpoint(TRUNCATE)", ()).await?;
    while let Some(row) = rows.next().await? {
        let busy = row.get::<i64>(0)?;
        if busy != 0 {
            return Err(turso::Error::Busy(
                "database checkpoint could not acquire every writer lock".to_string(),
            ));
        }
    }
    Ok(())
}

pub(crate) async fn health_check_database(database: &turso::Database) -> turso::Result<()> {
    let conn = database.connect()?;
    conn.busy_timeout(BUSY_TIMEOUT)?;
    let mut rows = conn.query("SELECT 1", ()).await?;
    let Some(row) = rows.next().await? else {
        return Err(turso::Error::Error(
            "database health query returned no rows".to_string(),
        ));
    };
    let _: i64 = row.get(0)?;
    Ok(())
}

pub(crate) fn is_retryable_turso_error(error: &turso::Error) -> bool {
    matches!(error, turso::Error::Busy(_) | turso::Error::BusySnapshot(_))
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_structured_busy_errors_are_retryable() {
        assert!(is_retryable_turso_error(&turso::Error::Busy(
            "database is locked".to_string()
        )));
        assert!(is_retryable_turso_error(&turso::Error::BusySnapshot(
            "snapshot conflict".to_string()
        )));
        assert!(!is_retryable_turso_error(&turso::Error::Error(
            "worker reported a busy conflict".to_string()
        )));
        assert!(!is_retryable_turso_error(&turso::Error::Constraint(
            "conflict".to_string()
        )));
    }
}
