//! DuckLake storage layer.
//!
//! Owns the **single writable handle** to a DuckLake catalog ([`Writer`]) and a
//! factory for **read-only** reader connections ([`open_reader`]). Per decision
//! D3, at most one [`Writer`] exists per catalog; the ingestion actor enforces
//! singleness, and the query path only ever reads.
//!
//! All identifiers (`system`, `entity`, column names) crossing this boundary are
//! validated against a strict allowlist (`^[a-zA-Z0-9_-]{1,64}$`) — per
//! `AGENTS.md` § Injection Prevention, identifiers are never interpolated from
//! raw caller input, and row values are always bound parameters, never literals.

#![forbid(unsafe_code)]
#![warn(rust_2024_compatibility, missing_docs, missing_debug_implementations)]

use std::path::Path;

use consumer_engine_core::{
    BoxError, Error, READ_ONLY_CATALOG_ALIAS, Result, SnapshotSpec, WRITE_CATALOG_ALIAS,
    validate_ident,
};
use duckdb::{Connection, types::Value};
use fs2::FileExt;

/// Build the qualified raw table name `raw_<system>_<entity>` (validated).
fn raw_table_name(system: &str, entity: &str) -> Result<String> {
    validate_ident(system)?;
    validate_ident(entity)?;
    Ok(format!("raw_{system}_{entity}"))
}

/// Load the `ducklake` extension on a connection (install is idempotent).
fn load_ducklake(conn: &Connection) -> Result<()> {
    conn.execute_batch("INSTALL ducklake; LOAD ducklake;")
        .map_err(|e| Error::Storage(BoxError::from(e)))?;
    Ok(())
}

/// The single writable DuckLake handle.
///
/// Construct via [`Writer::attach`]. Move-only: it cannot be cloned, ensuring a
/// single owner. The ingestion actor takes it into a dedicated thread. An
/// exclusive file lock on a sibling of the catalog path enforces the
/// single-writer invariant **across processes** (decision D3): a second
/// [`Writer::attach`] against the same catalog returns [`Error::WriterAlreadyHeld`].
pub struct Writer {
    conn: Connection,
    /// Held until drop; releases the exclusive catalog lock.
    #[allow(clippy::disallowed_types)]
    _lock: std::fs::File,
}

impl std::fmt::Debug for Writer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Writer").finish_non_exhaustive()
    }
}

impl Writer {
    /// Attach a writable DuckLake catalog at `catalog_path` with Parquet data
    /// under `data_path`. Acquires an exclusive lock; a second attach to the
    /// same catalog fails with [`Error::WriterAlreadyHeld`].
    ///
    /// # Errors
    /// - [`Error::WriterAlreadyHeld`] if the catalog is already locked.
    /// - [`Error::Storage`] if DuckDB/DuckLake attach fails.
    pub fn attach(catalog_path: &Path, data_path: &Path) -> Result<Self> {
        // Single-writer enforcement (D3): exclusive lock on a sibling file.
        let lock_path = catalog_path.with_extension("db.writelock");
        // Advisory file locking (fs2::FileExt) is only impl'd for std::fs::File,
        // not tokio::fs::File, and this is a synchronous startup operation that
        // runs outside the async runtime — so std::fs::OpenOptions is correct
        // here despite the workspace's tokio::fs preference (AGENTS.md § Async).
        #[allow(clippy::disallowed_types)]
        let lock_file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(&lock_path)
            .map_err(|e| Error::Storage(BoxError::from(e)))?;
        lock_file
            .try_lock_exclusive()
            .map_err(|_| Error::WriterAlreadyHeld)?;

        let conn = Connection::open_in_memory().map_err(|e| Error::Storage(BoxError::from(e)))?;
        load_ducklake(&conn)?;
        let sql = format!(
            "ATTACH 'ducklake:{}' AS {WRITE_CATALOG_ALIAS} (DATA_PATH '{}');",
            escape_for_sql_literal(&catalog_path.to_string_lossy()),
            escape_for_sql_literal(&data_path.to_string_lossy()),
        );
        conn.execute_batch(&sql)
            .map_err(|e| Error::Storage(BoxError::from(e)))?;
        Ok(Self {
            conn,
            _lock: lock_file,
        })
    }

    /// Create `raw_<system>_<entity>` with the given VARCHAR columns if absent,
    /// then insert all rows. Returns the number of rows inserted.
    ///
    /// Column names are validated; row values are bound parameters.
    ///
    /// # Errors
    /// - [`Error::InvalidInput`] on a bad identifier.
    /// - [`Error::Storage`] on DDL/insert failure.
    pub fn ingest_raw(
        &self,
        system: &str,
        entity: &str,
        columns: &[String],
        rows: &[Vec<Option<String>>],
    ) -> Result<usize> {
        if columns.is_empty() {
            return Err(Error::InvalidInput("columns must not be empty".into()));
        }
        for c in columns {
            validate_ident(c)?;
        }
        let table = raw_table_name(system, entity)?;
        let cols_names = columns.join(", ");
        let cols_typed = columns
            .iter()
            .map(|c| format!("{c} VARCHAR"))
            .collect::<Vec<_>>()
            .join(", ");
        let create =
            format!("CREATE TABLE IF NOT EXISTS {WRITE_CATALOG_ALIAS}.{table} ({cols_typed})");
        self.conn
            .execute_batch(&create)
            .map_err(|e| Error::Storage(BoxError::from(e)))?;

        if rows.is_empty() {
            return Ok(0);
        }
        let placeholders = format!("({})", vec!["?"; columns.len()].join(", "));
        let insert = format!(
            "INSERT INTO {WRITE_CATALOG_ALIAS}.{table} ({cols_names}) VALUES {placeholders}"
        );
        let mut stmt = self
            .conn
            .prepare(&insert)
            .map_err(|e| Error::Storage(BoxError::from(e)))?;
        let mut bound = 0usize;
        for row in rows {
            let params: Vec<Option<&str>> = row.iter().map(|v| v.as_deref()).collect();
            bound += stmt
                .execute(duckdb::params_from_iter(params))
                .map_err(|e| Error::Storage(BoxError::from(e)))?;
        }
        Ok(bound)
    }

    /// Run DuckLake compaction (`ducklake_rewrite_data_files`) on a table.
    ///
    /// # Errors
    /// - [`Error::InvalidInput`] on a bad identifier.
    /// - [`Error::Storage`] on failure.
    pub fn compact(&self, system: &str, entity: &str) -> Result<()> {
        let table = raw_table_name(system, entity)?;
        let sql = format!("CALL ducklake_rewrite_data_files('{WRITE_CATALOG_ALIAS}', '{table}');");
        self.conn
            .execute_batch(&sql)
            .map_err(|e| Error::Storage(BoxError::from(e)))?;
        Ok(())
    }

    /// Create the `audience_snapshot` table if absent. No PRIMARY KEY/UNIQUE —
    /// DuckLake rejects them (`specs/10`, `specs/20 §4`). Called at materialise
    /// time (defensive) and at engine startup so reads never hit a missing
    /// table.
    pub fn ensure_audience_snapshot_table(&self) -> Result<()> {
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS {WRITE_CATALOG_ALIAS}.audience_snapshot (snapshot_id \
             UUID, campaign_id VARCHAR, as_of_ts TIMESTAMPTZ, user_id VARCHAR, features JSON, \
             hit_reason JSON)"
        );
        self.conn
            .execute_batch(&sql)
            .map_err(|e| Error::Storage(BoxError::from(e)))
    }

    /// Atomically materialise a DSL segment's distinct keys into
    /// `audience_snapshot` via a single `INSERT … SELECT` (one catalog
    /// transaction ⇒ a partial snapshot is never observable, `specs/20 I4`).
    ///
    /// `subquery_sql` must reference the **write** alias (`dl.raw_*`); its `?`
    /// placeholders are bound by `subquery_params`. `key_column` is validated.
    ///
    /// # Errors
    /// - [`Error::InvalidInput`] on a bad key column.
    /// - [`Error::Storage`] on table/insert failure.
    pub fn materialize_snapshot(
        &self,
        subquery_sql: &str,
        subquery_params: &[Value],
        key_column: &str,
        spec: &SnapshotSpec,
    ) -> Result<u64> {
        validate_ident(key_column)?;
        self.ensure_audience_snapshot_table()?;
        let sql = format!(
            "INSERT INTO {WRITE_CATALOG_ALIAS}.audience_snapshot (snapshot_id, campaign_id, \
             as_of_ts, user_id, features, hit_reason) SELECT CAST(? AS UUID), ?, CAST(? AS \
             TIMESTAMPTZ), sub.{key_column}, CAST(? AS JSON), CAST(? AS JSON) FROM \
             ({subquery_sql}) sub"
        );
        let mut params: Vec<Value> = vec![
            Value::Text(spec.snapshot_id.clone()),
            Value::Text(spec.campaign_id.clone()),
            Value::Text(spec.as_of_ts.clone()),
            Value::Text(spec.features.clone()),
            Value::Text(spec.hit_reason.clone()),
        ];
        params.extend_from_slice(subquery_params);
        let n = self
            .conn
            .execute(&sql, duckdb::params_from_iter(params.iter()))
            .map_err(|e| Error::Storage(BoxError::from(e)))?;
        Ok(n as u64)
    }

    /// Export a snapshot to a Parquet file at `dest` (server-controlled path).
    ///
    /// # Errors
    /// - [`Error::Storage`] on failure.
    pub fn export_snapshot_parquet(&self, snapshot_id: &str, dest: &Path) -> Result<()> {
        let dest = escape_for_sql_literal(&dest.to_string_lossy());
        let sql = format!(
            "COPY (SELECT snapshot_id, campaign_id, as_of_ts, user_id, features, hit_reason FROM \
             {WRITE_CATALOG_ALIAS}.audience_snapshot WHERE snapshot_id = CAST(? AS UUID)) TO \
             '{dest}' (FORMAT 'PARQUET')"
        );
        self.conn
            .execute(&sql, duckdb::params![snapshot_id])
            .map_err(|e| Error::Storage(BoxError::from(e)))?;
        Ok(())
    }
}

/// Build the `ATTACH ... AS dro (READ_ONLY)` SQL for the catalog. Centralised
/// so the execution reader can re-issue it to refresh its snapshot (a
/// long-lived read-only attach does not see commits made after attach).
#[must_use]
pub fn read_only_attach_sql(catalog_path: &Path, data_path: &Path) -> String {
    format!(
        "ATTACH 'ducklake:{}' AS {READ_ONLY_CATALOG_ALIAS} (DATA_PATH '{}', READ_ONLY);",
        escape_for_sql_literal(&catalog_path.to_string_lossy()),
        escape_for_sql_literal(&data_path.to_string_lossy()),
    )
}

/// Open a **read-only** connection attached to the same catalog. Inserts on this
/// connection are rejected by DuckLake. Note: a connection attached this way is
/// pinned to the snapshot at attach time; callers needing to see later commits
/// must re-attach (see [`read_only_attach_sql`]).
///
/// # Errors
/// - [`Error::Execution`] if attach fails.
pub fn open_reader(catalog_path: &Path, data_path: &Path) -> Result<Connection> {
    let conn = Connection::open_in_memory().map_err(|e| Error::Execution(BoxError::from(e)))?;
    load_ducklake(&conn).map_err(|e| match e {
        Error::Storage(b) => Error::Execution(b),
        other => other,
    })?;
    let sql = read_only_attach_sql(catalog_path, data_path);
    conn.execute_batch(&sql)
        .map_err(|e| Error::Execution(BoxError::from(e)))?;
    Ok(conn)
}

/// Escape backticks/quotes/backslashes in a filesystem path before embedding it
/// in an `ATTACH` literal. This is for the **path** (operator-supplied config),
/// not for row data; row data is always parameterized.
fn escape_for_sql_literal(s: &str) -> String {
    s.replace('\'', "''")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_writer() -> (tempfile::TempDir, Writer) {
        let tmp = tempfile::tempdir().expect("tmp");
        let w =
            Writer::attach(&tmp.path().join("cat.db"), &tmp.path().join("data")).expect("attach");
        (tmp, w)
    }

    #[test]
    fn test_should_ingest_and_count_raw_table() {
        let (_tmp, w) = tmp_writer();
        let n = w
            .ingest_raw(
                "erp",
                "users",
                &["id".into(), "name".into()],
                &[
                    vec![Some("u1".into()), Some("alice".into())],
                    vec![Some("u2".into()), Some("bob".into())],
                ],
            )
            .expect("ingest");
        assert_eq!(n, 2);
        let count: i64 = w
            .conn
            .query_row("SELECT count(*) FROM dl.raw_erp_users", [], |r| r.get(0))
            .expect("count");
        assert_eq!(count, 2);
    }

    #[test]
    fn test_should_reject_invalid_identifier() {
        let (_tmp, w) = tmp_writer();
        let res = w.ingest_raw("erp", "users; DROP", &["id".into()], &[]);
        assert!(matches!(res, Err(Error::InvalidInput(_))));
    }

    #[test]
    fn test_should_run_compact_without_error() {
        let (_tmp, w) = tmp_writer();
        w.ingest_raw("erp", "orders", &["id".into()], &[vec![Some("o1".into())]])
            .expect("ingest");
        w.compact("erp", "orders")
            .expect("compact is best-effort ok");
    }

    #[test]
    fn test_should_refuse_second_writer() {
        let (tmp, w) = tmp_writer();
        let second = Writer::attach(&tmp.path().join("cat.db"), &tmp.path().join("data"));
        assert!(
            matches!(second, Err(Error::WriterAlreadyHeld)),
            "a second writer on the same catalog must be refused"
        );
        drop(w);
        // Once the first writer is released, a fresh attach must succeed.
        let again = Writer::attach(&tmp.path().join("cat.db"), &tmp.path().join("data"));
        assert!(
            again.is_ok(),
            "writer must re-attach after the lock is released"
        );
    }

    #[test]
    fn test_should_persist_across_restart() {
        let (tmp, w) = tmp_writer();
        w.ingest_raw(
            "erp",
            "users",
            &["id".into()],
            &[vec![Some("u1".into())], vec![Some("u2".into())]],
        )
        .expect("ingest");
        drop(w);
        // The engine holds no unflushed state: re-attaching read-only must see
        // the previously committed rows (DuckLake durability).
        let r =
            open_reader(&tmp.path().join("cat.db"), &tmp.path().join("data")).expect("read attach");
        let mut stmt = r
            .prepare("SELECT count(*) FROM dro.raw_erp_users")
            .expect("prepare");
        let n: i64 = stmt.query_row([], |row| row.get(0)).expect("count");
        assert_eq!(n, 2);
    }
}
