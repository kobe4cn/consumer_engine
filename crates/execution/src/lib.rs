//! Read-only execution layer.
//!
//! Owns a **pool** of read-only DuckDB connections attached to the DuckLake
//! catalog (under the `dro` alias), one worker thread per connection
//! (specs/11 §2a). The [`duckdb::Connection`] is not `Sync`, so each lives in
//! its own OS thread; the async side sends commands over `flume` channels and
//! awaits typed replies.
//!
//! This is the read half of the engine; the query path is strictly read-only.
//! An `INSERT`/`UPDATE`/`DELETE` submitted here is rejected by DuckLake's
//! `READ_ONLY` attach (verified by `test_should_assert_query_path_is_read_only`).
//!
//! Refresh (P1-1, issue #20): a long-lived read-only attach is pinned to the
//! snapshot at attach time. With a wired write generation the workers re-attach
//! only when the single writer advances it (plus a cadence backstop) — the hot
//! path is attach-free; standalone/test readers fall back to re-attaching
//! before every query.

#![forbid(unsafe_code)]
#![warn(rust_2024_compatibility, missing_docs, missing_debug_implementations)]

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use consumer_engine_core::{BoxError, Error, READ_ONLY_CATALOG_ALIAS, Result};
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

/// Handle to the read-only reader **pool**: `N` worker threads, each owning one
/// read-only DuckDB connection attached to the DuckLake catalog (specs/11 §2a:
/// N workers each owning a read-only attach). Cheap to clone.
#[derive(Clone)]
pub struct Reader {
    workers: Arc<Vec<flume::Sender<Cmd>>>,
    next: Arc<AtomicUsize>,
}

impl std::fmt::Debug for Reader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Reader")
            .field("workers", &self.workers.len())
            .finish_non_exhaustive()
    }
}

/// When a worker must re-attach (the only way a long-lived read-only DuckLake
/// attach sees post-attach commits — it is pinned to the snapshot at attach
/// time). `Always` refreshes before every query (the pre-pool behaviour, used
/// by standalone/test readers). `OnWrite` refreshes only when the shared write
/// generation has advanced since the worker's last attach — the single writer
/// (D3) bumps it after every committed write — or when the attach is older
/// than `interval` (cadence backstop keeping connections warm).
#[derive(Debug, Clone)]
enum RefreshPolicy {
    /// Re-attach before every query.
    Always,
    /// Re-attach only on a write-generation change or `interval` age.
    OnWrite {
        /// Advanced by the single writer after every committed write.
        write_gen: Arc<AtomicU64>,
        /// Cadence backstop: re-attach even without writes after this long.
        interval: Duration,
    },
}

impl Reader {
    /// Start a single read-only worker owning `conn`, already attached
    /// read-only, that re-attaches before **every** query (the pre-pool
    /// behaviour). Production wiring should use [`Self::start_pooled`] with a
    /// write generation so the hot path is attach-free; this constructor keeps
    /// standalone/test readers correct without that wiring.
    ///
    /// `limits` are applied as DuckDB PRAGMAs once per worker thread.
    ///
    /// # Errors
    /// - [`Error::Execution`] if the thread cannot be spawned.
    pub fn start(conn: Connection, attach_sql: String, limits: ReaderLimits) -> Result<Self> {
        Self::start_pooled(vec![conn], attach_sql, limits, None, Duration::ZERO)
    }

    /// Start a pool of `conns.len()` read-only workers (one per connection),
    /// each attached read-only. Queries round-robin across the workers, so
    /// concurrent reads are served in parallel (the single-threaded reader
    /// serialised every query).
    ///
    /// Refresh policy: when `write_gen` is wired (the single writer bumps it
    /// after every committed write — issue #20 / P1-1), a worker re-attaches
    /// only when that generation advanced since its last attach, or when the
    /// attach is older than `refresh_interval` (cadence backstop). With no
    /// generation wired, the pool falls back to re-attaching before every
    /// query (correct, just not attach-free).
    ///
    /// # Errors
    /// - [`Error::InvalidInput`] if `conns` is empty (a zero-worker pool would divide by zero on
    ///   dispatch).
    /// - [`Error::Execution`] if any worker thread cannot be spawned.
    pub fn start_pooled(
        conns: Vec<Connection>,
        attach_sql: String,
        limits: ReaderLimits,
        write_gen: Option<Arc<AtomicU64>>,
        refresh_interval: Duration,
    ) -> Result<Self> {
        if conns.is_empty() {
            return Err(Error::InvalidInput(
                "read pool needs at least one worker connection".into(),
            ));
        }
        let policy = match write_gen {
            Some(write_gen) => RefreshPolicy::OnWrite {
                write_gen,
                interval: refresh_interval.max(Duration::from_secs(1)),
            },
            None => RefreshPolicy::Always,
        };
        let mut workers = Vec::with_capacity(conns.len());
        for (i, conn) in conns.into_iter().enumerate() {
            let (tx, rx) = flume::bounded::<Cmd>(64);
            let policy = policy.clone();
            let attach_sql = attach_sql.clone();
            let limits = limits.clone();
            thread::Builder::new()
                .name(format!("ce-reader-{i}"))
                .spawn(move || worker_loop(conn, rx, attach_sql, limits, policy))
                .map_err(|e| Error::Execution(BoxError::from(e)))?;
            workers.push(tx);
        }
        Ok(Self {
            workers: Arc::new(workers),
            next: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// The number of worker threads in the pool.
    #[must_use]
    pub fn workers(&self) -> usize {
        self.workers.len()
    }

    /// Run a read query with no parameters (thin delegate).
    ///
    /// # Errors
    /// Propagates [`Error::Execution`] on prepare/query failure (including
    /// read-only violations for non-SELECT statements).
    pub async fn query(&self, sql: &str) -> Result<QueryResult> {
        self.query_with_params(sql, Vec::new()).await
    }

    /// Run a read query with bound parameters on the next round-robin worker.
    ///
    /// # Errors
    /// Propagates [`Error::Execution`] on prepare/query failure.
    pub async fn query_with_params(
        &self,
        sql: &str,
        params: Vec<duckdb::types::Value>,
    ) -> Result<QueryResult> {
        let (rtx, rrx) = flume::bounded(1);
        let idx = self.next.fetch_add(1, Ordering::Relaxed) % self.workers.len();
        let worker = self
            .workers
            .get(idx)
            .ok_or_else(|| Error::Execution(BoxError::from("read pool worker vanished")))?;
        worker
            .send_async(Cmd::Query {
                sql: sql.to_string(),
                params,
                reply: rtx,
            })
            .await
            .map_err(|e| Error::Execution(BoxError::from(e)))?;
        rrx.recv_async()
            .await
            .map_err(|e| Error::Execution(BoxError::from(e)))?
    }

    /// Signal every worker to stop. Best-effort; each thread exits after
    /// draining in-flight commands.
    pub fn shutdown(&self) {
        for w in self.workers.iter() {
            let _ = w.send(Cmd::Shutdown);
        }
    }
}

/// One worker's loop: own the connection, re-attach read-only per the refresh
/// policy (so the snapshot stays current without paying an attach on the hot
/// path), serve commands until shutdown.
fn worker_loop(
    conn: Connection,
    rx: flume::Receiver<Cmd>,
    attach_sql: String,
    limits: ReaderLimits,
    policy: RefreshPolicy,
) {
    // Apply connection-scoped PRAGMAs once (persist across re-attach).
    let pragma = format!(
        "PRAGMA memory_limit='{}'; PRAGMA threads={};",
        escape_single_quotes(&limits.memory_limit),
        limits.threads
    );
    if let Err(e) = conn.execute_batch(&pragma) {
        tracing::warn!(error = %e, "reader PRAGMA setup failed; using DuckDB defaults");
    }
    let refresh = format!("DETACH {READ_ONLY_CATALOG_ALIAS}; {attach_sql}");
    let mut last_gen: u64 = 0;
    let mut last_refresh = Instant::now();
    for cmd in rx.iter() {
        match cmd {
            Cmd::Query { sql, params, reply } => {
                // Decide whether this query needs a re-attach BEFORE serving.
                // State is advanced only after a SUCCESSFUL refresh — if the
                // re-attach fails, the dirty signal survives so the next query
                // retries instead of silently serving stale data.
                let needs_refresh = match &policy {
                    RefreshPolicy::Always => true,
                    RefreshPolicy::OnWrite {
                        write_gen,
                        interval,
                    } => {
                        let g = write_gen.load(Ordering::Relaxed);
                        (g != last_gen) || (last_refresh.elapsed() >= *interval)
                    }
                };
                let res = if needs_refresh {
                    match conn
                        .execute_batch(&refresh)
                        .map_err(|e| Error::Execution(BoxError::from(e)))
                    {
                        Ok(()) => {
                            // Committed only now, post-success.
                            if let RefreshPolicy::OnWrite { write_gen, .. } = &policy {
                                last_gen = write_gen.load(Ordering::Relaxed);
                            }
                            last_refresh = Instant::now();
                            run_query(&conn, &sql, &params)
                        }
                        Err(e) => Err(e),
                    }
                } else {
                    run_query(&conn, &sql, &params)
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
        let threads = match std::thread::available_parallelism() {
            Ok(n) => n.get(),
            Err(_) => 8,
        };
        Self {
            memory_limit: "8GB".to_string(),
            threads,
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
        // Timestamps → ISO-8601 UTC strings (P2-2, issue #21): the wire
        // contract is ISO-8601, so a TIMESTAMPTZ cell must never degrade to
        // null — except an unrepresentable (out-of-range) value, where null is
        // more honest than fabricating a timestamp.
        Value::Timestamp(unit, ts) => match timestamp_to_iso(unit, ts) {
            Some(iso) => json!(iso),
            None => serde_json::Value::Null,
        },
        // Best-effort for remaining scalar/structural kinds (decimal, struct,
        // map, union, blob, geometry): null.
        _ => serde_json::Value::Null,
    }
}

/// Convert a DuckDB timestamp (epoch in `unit`'s resolution) to an ISO-8601
/// UTC string.
fn timestamp_to_iso(unit: duckdb::types::TimeUnit, ts: i64) -> Option<String> {
    use duckdb::types::TimeUnit;
    let (secs, nanos) = match unit {
        TimeUnit::Second => (ts, 0),
        TimeUnit::Millisecond => (
            ts.div_euclid(1000),
            (ts.rem_euclid(1000) * 1_000_000) as u32,
        ),
        TimeUnit::Microsecond => (
            ts.div_euclid(1_000_000),
            (ts.rem_euclid(1_000_000) * 1000) as u32,
        ),
        TimeUnit::Nanosecond => (
            ts.div_euclid(1_000_000_000),
            (ts.rem_euclid(1_000_000_000)) as u32,
        ),
    };
    chrono::DateTime::from_timestamp(secs, nanos)
        .map(|d| d.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Shared setup: a writer wired to `writer_gen`, a seeded raw table, and a
    /// pooled reader wired to `reader_gen` (pass the same `Arc` for both to
    /// mimic production). Returns (tempdir, writer, reader).
    fn setup(
        writer_gen: Arc<AtomicU64>,
        reader_gen: Arc<AtomicU64>,
        workers: usize,
    ) -> (tempfile::TempDir, consumer_engine_storage::Writer, Reader) {
        let tmp = tempfile::tempdir().expect("tmp");
        let writer = consumer_engine_storage::Writer::attach_with_gen(
            &tmp.path().join("cat.db"),
            &tmp.path().join("data"),
            &consumer_engine_core::CompactionConfig::default(),
            Some(Arc::clone(&writer_gen)),
        )
        .expect("attach writer");
        writer
            .ingest_raw(
                "erp",
                "orders",
                &["user_id".into(), "sku".into()],
                &[
                    vec![Some("u1".into()), Some("A".into())],
                    vec![Some("u2".into()), Some("B".into())],
                ],
            )
            .expect("seed");
        let attach_sql = consumer_engine_storage::read_only_attach_sql(
            &tmp.path().join("cat.db"),
            &tmp.path().join("data"),
        );
        let conns: Vec<duckdb::Connection> = (0..workers)
            .map(|_| {
                consumer_engine_storage::open_reader(
                    &tmp.path().join("cat.db"),
                    &tmp.path().join("data"),
                )
                .expect("read attach")
            })
            .collect();
        let reader = Reader::start_pooled(
            conns,
            attach_sql,
            ReaderLimits::default(),
            Some(Arc::clone(&reader_gen)),
            Duration::from_secs(3600),
        )
        .expect("pool");
        (tmp, writer, reader)
    }

    async fn count_orders(reader: &Reader) -> i64 {
        let qr = reader
            .query("SELECT count(*) FROM dro.raw_erp_orders")
            .await
            .expect("count");
        qr.rows
            .first()
            .and_then(|r| r.first())
            .and_then(serde_json::Value::as_i64)
            .expect("count cell")
    }

    #[test]
    fn test_should_reject_empty_pool() {
        let res = Reader::start_pooled(
            Vec::new(),
            "ATTACH ...".to_string(),
            ReaderLimits::default(),
            None,
            Duration::ZERO,
        );
        assert!(
            matches!(res, Err(Error::InvalidInput(_))),
            "an empty pool must be rejected, not divide-by-zero on dispatch"
        );
    }

    #[tokio::test]
    async fn test_should_pool_serve_queries_across_workers() {
        let write_gen = Arc::new(AtomicU64::new(0));
        let (_tmp, _writer, reader) = setup(Arc::clone(&write_gen), Arc::clone(&write_gen), 3);
        assert_eq!(reader.workers(), 3, "pool must spawn the requested workers");
        // Round-robin: three sequential queries hit three different workers and
        // all see the seeded rows (each worker attached at pool start).
        for _ in 0..6 {
            assert_eq!(
                count_orders(&reader).await,
                2,
                "every worker serves the seed"
            );
        }
        reader.shutdown();
    }

    #[tokio::test]
    async fn test_should_onwrite_refresh_only_when_generation_advances() {
        let writer_gen = Arc::new(AtomicU64::new(0));
        let reader_gen = Arc::new(AtomicU64::new(0));
        let (tmp, writer, reader) = setup(Arc::clone(&writer_gen), Arc::clone(&reader_gen), 1);
        // Baseline: the seed (2 rows) is visible — the initial attach happened
        // after the seed's generation bump.
        assert_eq!(count_orders(&reader).await, 2);
        // A write whose generation is NOT advanced is invisible (lazy refresh
        // is gen-gated, not per-query) — this is the contract the pool relies
        // on: no attach on the hot path, so stale until the writer signals.
        // (Direct writer use below simulates a writer that forgot to wire the
        // counter — production always bumps via the shared Arc.)
        writer
            .ingest_raw(
                "erp",
                "orders",
                &["user_id".into(), "sku".into()],
                &[vec![Some("u3".into()), Some("C".into())]],
            )
            .expect("ingest without bump visibility");
        // Same generation → no re-attach → still 2.
        assert_eq!(
            count_orders(&reader).await,
            2,
            "no refresh without a gen bump"
        );
        // Bump the READER's generation (the single writer does this after every
        // commit via the shared Arc) → the next query re-attaches and sees 3.
        reader_gen.fetch_add(1, Ordering::Relaxed);
        assert_eq!(
            count_orders(&reader).await,
            3,
            "gen bump must trigger refresh"
        );
        // Idempotent: a second query with no further bump serves without
        // re-attach and still sees 3.
        assert_eq!(count_orders(&reader).await, 3);
        reader.shutdown();
        let _ = tmp;
    }
}

#[cfg(test)]
mod timestamp_tests {
    use super::*;

    #[test]
    fn test_should_render_timestamptz_as_iso8601() {
        // P2-2 / issue #21: a TIMESTAMPTZ cell must serialize as ISO-8601 UTC,
        // never degrade to null.
        let epoch_micros = 1_736_899_200_000_000_i64; // 2025-01-15T00:00:00Z
        let v = Value::Timestamp(duckdb::types::TimeUnit::Microsecond, epoch_micros);
        let json = value_to_json(v);
        assert_eq!(
            json.as_str().expect("string"),
            "2025-01-15T00:00:00Z",
            "microsecond epoch must render ISO-8601"
        );
        let v = Value::Timestamp(duckdb::types::TimeUnit::Second, 1_736_899_200);
        assert_eq!(
            value_to_json(v).as_str().expect("string"),
            "2025-01-15T00:00:00Z"
        );
        // Sub-second precision is preserved.
        let v = Value::Timestamp(duckdb::types::TimeUnit::Millisecond, 1_736_899_200_123);
        assert_eq!(
            value_to_json(v).as_str().expect("string"),
            "2025-01-15T00:00:00.123Z"
        );
    }
}
