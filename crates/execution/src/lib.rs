//! Read-only execution layer.
//!
//! Owns a single read-only DuckDB connection attached to the DuckLake catalog
//! (under the `dro` alias). The [`duckdb::Connection`] is not `Sync`, so it lives
//! in a dedicated OS thread; the async side sends commands over a `flume`
//! channel and awaits replies.
//!
//! This is the read half of the engine; the query path is strictly read-only.
//! An `INSERT`/`UPDATE`/`DELETE` submitted here is rejected by DuckLake's
//! `READ_ONLY` attach (verified by `test_should_assert_query_path_is_read_only`).

#![forbid(unsafe_code)]
#![warn(rust_2024_compatibility, missing_docs, missing_debug_implementations)]

use std::thread;

use consumer_engine_core::{BoxError, Error, Result};
use duckdb::{Connection, types::Value};
use serde::Serialize;

/// A single result row as a JSON array of cells, aligned with [`QueryResult::columns`].
pub type RowCells = Vec<serde_json::Value>;

/// The result of a read query: column names plus rows (each a vector of JSON
/// cells in column order).
#[derive(Debug, Clone, Serialize, PartialEq)]
#[non_exhaustive]
pub struct QueryResult {
    /// Column names in order.
    pub columns: Vec<String>,
    /// Rows, each a vector of JSON cells in column order.
    pub rows: Vec<RowCells>,
}

/// Commands sent to the reader thread.
enum Cmd {
    /// Run a read query and return its rows.
    Query {
        /// SQL to execute (must reference the `dro` catalog alias).
        sql: String,
        /// Bound parameters for the SQL `?` placeholders (I1: never interpolate
        /// user values).
        params: Vec<duckdb::types::Value>,
        /// Reply channel for the result.
        reply: flume::Sender<Result<QueryResult>>,
    },
    /// Stop the reader thread.
    Shutdown,
}

/// Handle to the read-only reader thread. Cheap to clone.
#[derive(Clone)]
pub struct Reader {
    tx: flume::Sender<Cmd>,
}

impl std::fmt::Debug for Reader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Reader").finish_non_exhaustive()
    }
}

impl Reader {
    /// Start the reader thread owning `conn`, already attached read-only.
    /// `attach_sql` is re-issued (as `DETACH dro; <attach_sql>`) before every
    /// query so the reader sees commits made after its initial attach — a
    /// long-lived read-only DuckLake attach is otherwise pinned to the snapshot
    /// at attach time. `limits` are applied as DuckDB PRAGMAs once at thread
    /// start.
    ///
    /// # Errors
    /// - [`Error::Execution`] if the thread cannot be spawned.
    pub fn start(conn: Connection, attach_sql: String, limits: ReaderLimits) -> Result<Self> {
        let (tx, rx) = flume::bounded::<Cmd>(64);
        thread::Builder::new()
            .name("ce-reader".into())
            .spawn(move || reader_loop(conn, rx, attach_sql, limits))
            .map_err(|e| Error::Execution(BoxError::from(e)))?;
        Ok(Self { tx })
    }

    /// Run a read query with no parameters (thin delegate).
    ///
    /// # Errors
    /// Propagates [`Error::Execution`] on prepare/query failure (including
    /// read-only violations for non-SELECT statements).
    pub async fn query(&self, sql: &str) -> Result<QueryResult> {
        self.query_with_params(sql, Vec::new()).await
    }

    /// Run a read query with bound parameters.
    ///
    /// # Errors
    /// Propagates [`Error::Execution`] on prepare/query failure.
    pub async fn query_with_params(
        &self,
        sql: &str,
        params: Vec<duckdb::types::Value>,
    ) -> Result<QueryResult> {
        let (rtx, rrx) = flume::bounded(1);
        self.tx
            .send(Cmd::Query {
                sql: sql.to_string(),
                params,
                reply: rtx,
            })
            .map_err(|e| Error::Execution(BoxError::from(e)))?;
        rrx.recv_async()
            .await
            .map_err(|e| Error::Execution(BoxError::from(e)))?
    }

    /// Signal the reader thread to stop. Best-effort; the thread exits after
    /// draining in-flight commands.
    pub fn shutdown(&self) {
        let _ = self.tx.send(Cmd::Shutdown);
    }
}

/// The reader thread body: own the connection, re-attach read-only before each
/// command to refresh the snapshot, serve until shutdown.
fn reader_loop(
    conn: Connection,
    rx: flume::Receiver<Cmd>,
    attach_sql: String,
    limits: ReaderLimits,
) {
    // Apply connection-scoped PRAGMAs once (persist across per-query re-attach).
    let pragma = format!(
        "PRAGMA memory_limit='{}'; PRAGMA threads={};",
        escape_single_quotes(&limits.memory_limit),
        limits.threads
    );
    if let Err(e) = conn.execute_batch(&pragma) {
        tracing::warn!(error = %e, "reader PRAGMA setup failed; using DuckDB defaults");
    }
    let refresh = format!("DETACH dro; {attach_sql}");
    for cmd in rx.iter() {
        match cmd {
            Cmd::Query { sql, params, reply } => {
                let res = match conn
                    .execute_batch(&refresh)
                    .map_err(|e| Error::Execution(BoxError::from(e)))
                {
                    Ok(()) => run_query(&conn, &sql, &params),
                    Err(e) => Err(e),
                };
                let _ = reply.send(res);
            }
            Cmd::Shutdown => break,
        }
    }
}

/// Escape single quotes in a PRAGMA value (operator config, not user input).
fn escape_single_quotes(s: &str) -> String {
    s.replace('\'', "''")
}

/// Connection-scoped DuckDB limits applied at reader start.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReaderLimits {
    /// DuckDB memory limit, e.g. `"8GB"`.
    pub memory_limit: String,
    /// DuckDB thread count.
    pub threads: usize,
}

impl Default for ReaderLimits {
    fn default() -> Self {
        Self {
            memory_limit: "8GB".to_string(),
            threads: std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(8),
        }
    }
}

/// Prepare and run a read query, materialising all rows as JSON cells.
///
/// Column metadata is read from the executed result via `Rows::as_ref`
/// (DuckDB only populates output-column metadata after execution).
fn run_query(conn: &Connection, sql: &str, params: &[duckdb::types::Value]) -> Result<QueryResult> {
    let mut stmt = conn
        .prepare(sql)
        .map_err(|e| Error::Execution(BoxError::from(e)))?;
    let mut rs = stmt
        .query(duckdb::params_from_iter(params.iter()))
        .map_err(|e| Error::Execution(BoxError::from(e)))?;
    let col_count = rs.as_ref().map_or(0, |s| s.column_count());
    let columns: Vec<String> = (0..col_count)
        .map(|i| {
            rs.as_ref()
                .and_then(|s| s.column_name(i).ok())
                .cloned()
                .unwrap_or_default()
        })
        .collect();
    let mut rows: Vec<RowCells> = Vec::new();
    while let Some(row) = rs.next().map_err(|e| Error::Execution(BoxError::from(e)))? {
        let mut cells = Vec::with_capacity(col_count);
        for i in 0..col_count {
            let val: Value = row
                .get(i)
                .map_err(|e| Error::Execution(BoxError::from(e)))?;
            cells.push(value_to_json(val));
        }
        rows.push(cells);
    }
    Ok(QueryResult { columns, rows })
}

/// Convert a DuckDB [`Value`] to a JSON value.
///
/// T1 queries return `VARCHAR` (raw tables) and `BIGINT` (`count(*)`), which
/// are mapped precisely. List/Array recurse. Huge ints map to string to stay
/// exact. Temporal/decimal/struct/map/blob/geometry map to null as best-effort
/// for T1 (these types are not produced by the T1 query surface); richer
/// mapping lands with the DSL compiler (T2).
#[must_use]
pub fn value_to_json(v: Value) -> serde_json::Value {
    use serde_json::json;
    match v {
        Value::Null => serde_json::Value::Null,
        Value::Boolean(b) => json!(b),
        Value::TinyInt(i) => json!(i),
        Value::SmallInt(i) => json!(i),
        Value::Int(i) => json!(i),
        Value::BigInt(i) => json!(i),
        Value::UTinyInt(i) => json!(i),
        Value::USmallInt(i) => json!(i),
        Value::UInt(i) => json!(i),
        Value::UBigInt(i) => json!(i),
        Value::Float(f) => json!(f),
        Value::Double(f) => json!(f),
        Value::Text(s) => json!(s),
        Value::Enum(s) => json!(s),
        Value::List(vs) | Value::Array(vs) => {
            json!(vs.into_iter().map(value_to_json).collect::<Vec<_>>())
        }
        // Huge ints lose precision beyond f64; map to string to stay exact.
        Value::HugeInt(i) => json!(i.to_string()),
        Value::UHugeInt(i) => json!(i.to_string()),
        // Best-effort for remaining scalar/structural kinds (temporal, decimal,
        // struct, map, union, blob, geometry): null for T1.
        _ => serde_json::Value::Null,
    }
}
