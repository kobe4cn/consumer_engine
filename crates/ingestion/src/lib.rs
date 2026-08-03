//! The single ingestion writer actor.
//!
//! Owns the sole [`Writer`] to DuckLake (decision D3) inside a dedicated OS
//! thread — `duckdb::Connection` is not `Sync`. The async side sends commands
//! over a `flume` channel and awaits typed replies.
//!
//! For T1, an `IngestRaw` command flushes the supplied batch immediately via the
//! writer's multi-row parameterised insert (the batch is the micro-batch at the
//! SQL level). CDC-driven cross-call accumulation on the configured flush
//! threshold lands with the CDC adapter (survey-cdc-adapter.md). A
//! [`IngestionHandle::compact_all`] entry point plus the server's interval task
//! wire compaction (decision D6).

#![forbid(unsafe_code)]
#![warn(rust_2024_compatibility, missing_docs, missing_debug_implementations)]

use std::{collections::HashSet, path::PathBuf, sync::Arc, thread};

use consumer_engine_core::{
    BoxError, CatalogRow, Error, FeatureRow, Result, SnapshotSpec, SuppressionRow,
};
use consumer_engine_storage::Writer;
use duckdb::types::Value;

mod producer;
mod producers;

pub use producer::{FeatureProducer, ProducerOutput, ProducerRegistry};
pub use producers::CadenceRegularityProducer;

/// Commands sent to the writer thread.
enum Cmd {
    /// Create/insert into a `raw_*` table.
    IngestRaw {
        /// Source system identifier.
        system: String,
        /// Source entity (table) identifier.
        entity: String,
        /// Column names.
        columns: Vec<String>,
        /// Rows of optional string cells.
        rows: Vec<Vec<Option<String>>>,
        /// Reply channel carrying the inserted row count.
        reply: flume::Sender<Result<usize>>,
    },
    /// Compact every table this actor has ingested.
    CompactAll {
        /// Reply channel.
        reply: flume::Sender<Result<()>>,
    },
    /// Atomically materialise a DSL segment into `audience_snapshot` via the
    /// single writer (one catalog transaction ⇒ a partial snapshot is never
    /// observable, `specs/20 I4`).
    Materialize {
        /// The materialise subquery SQL (must reference the **write** alias).
        subquery_sql: String,
        /// Bound parameters for the subquery's `?` placeholders.
        subquery_params: Vec<Value>,
        /// The key column projected by the subquery (validated).
        key_column: String,
        /// The snapshot's scalar row payload.
        spec: SnapshotSpec,
        /// Reply channel carrying the inserted row count.
        reply: flume::Sender<Result<u64>>,
    },
    /// Export a snapshot to a Parquet file (server-controlled path).
    ExportParquet {
        /// The bare snapshot UUID.
        snapshot_id: String,
        /// Destination Parquet file.
        dest: PathBuf,
        /// Reply channel.
        reply: flume::Sender<Result<()>>,
    },
    /// Append feature rows to `feature_store` and refresh the wide pivot views
    /// for every distinct family touched (D9).
    WriteFeatures {
        /// The feature rows to append.
        rows: Vec<FeatureRow>,
        /// Reply channel carrying the inserted row count.
        reply: flume::Sender<Result<usize>>,
    },
    /// Append semantic-catalog rows (Profiler output).
    WriteCatalog {
        /// The catalog rows to append.
        rows: Vec<CatalogRow>,
        /// Reply channel carrying the inserted row count.
        reply: flume::Sender<Result<usize>>,
    },
    /// Append suppression writebacks idempotently (Q3, E1).
    WriteSuppression {
        /// The suppression rows to append (deduped by `suppression_id`).
        rows: Vec<SuppressionRow>,
        /// Reply channel carrying the inserted row count.
        reply: flume::Sender<Result<usize>>,
    },
    /// Stop the writer thread.
    Shutdown,
}

/// Handle to the single ingestion writer. Cheap to clone.
#[derive(Clone)]
pub struct IngestionHandle {
    tx: flume::Sender<Cmd>,
    registry: Arc<ProducerRegistry>,
}

impl std::fmt::Debug for IngestionHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IngestionHandle").finish_non_exhaustive()
    }
}

impl IngestionHandle {
    /// Start the writer thread owning `writer` and bound to `registry`. Exactly
    /// one handle should be built per catalog (the writer's file lock enforces
    /// singleness). The registry holds the Feature Store producers (D9).
    ///
    /// # Errors
    /// - [`Error::Ingestion`] if the thread cannot be spawned.
    pub fn start(writer: Writer, registry: Arc<ProducerRegistry>) -> Result<Self> {
        let (tx, rx) = flume::bounded::<Cmd>(64);
        thread::Builder::new()
            .name("ce-ingestion".into())
            .spawn(move || writer_loop(writer, rx))
            .map_err(|e| Error::Ingestion(BoxError::from(e)))?;
        Ok(Self { tx, registry })
    }

    /// Ingest a batch into `raw_<system>_<entity>`. Returns the row count.
    ///
    /// # Errors
    /// Propagates validation/storage errors from the writer.
    pub async fn ingest_raw(
        &self,
        system: &str,
        entity: &str,
        columns: Vec<String>,
        rows: Vec<Vec<Option<String>>>,
    ) -> Result<usize> {
        let (rtx, rrx) = flume::bounded(1);
        self.tx
            .send(Cmd::IngestRaw {
                system: system.to_string(),
                entity: entity.to_string(),
                columns,
                rows,
                reply: rtx,
            })
            .map_err(|e| Error::Ingestion(BoxError::from(e)))?;
        rrx.recv_async()
            .await
            .map_err(|e| Error::Ingestion(BoxError::from(e)))?
    }

    /// Compact every table ingested so far (best-effort; last error wins).
    ///
    /// # Errors
    /// Propagates the first storage error encountered.
    pub async fn compact_all(&self) -> Result<()> {
        let (rtx, rrx) = flume::bounded(1);
        self.tx
            .send(Cmd::CompactAll { reply: rtx })
            .map_err(|e| Error::Ingestion(BoxError::from(e)))?;
        rrx.recv_async()
            .await
            .map_err(|e| Error::Ingestion(BoxError::from(e)))?
    }

    /// Atomically materialise `subquery_sql` into `audience_snapshot`, returning
    /// the number of rows written. The subquery must reference the **write**
    /// alias (`dl.raw_*`); its `?` placeholders are bound by `subquery_params`.
    ///
    /// # Errors
    /// - [`Error::InvalidInput`] on a bad key column.
    /// - [`Error::Ingestion`] if the writer thread has exited.
    /// - [`Error::Storage`] on table/insert failure.
    pub async fn materialize_snapshot(
        &self,
        subquery_sql: &str,
        subquery_params: Vec<Value>,
        key_column: &str,
        spec: SnapshotSpec,
    ) -> Result<u64> {
        let (rtx, rrx) = flume::bounded(1);
        self.tx
            .send(Cmd::Materialize {
                subquery_sql: subquery_sql.to_string(),
                subquery_params,
                key_column: key_column.to_string(),
                spec,
                reply: rtx,
            })
            .map_err(|e| Error::Ingestion(BoxError::from(e)))?;
        rrx.recv_async()
            .await
            .map_err(|e| Error::Ingestion(BoxError::from(e)))?
    }

    /// Export a snapshot to a Parquet file at `dest` (server-controlled path).
    ///
    /// # Errors
    /// - [`Error::Ingestion`] if the writer thread has exited.
    /// - [`Error::Storage`] on export failure.
    pub async fn export_parquet(&self, snapshot_id: &str, dest: PathBuf) -> Result<()> {
        let (rtx, rrx) = flume::bounded(1);
        self.tx
            .send(Cmd::ExportParquet {
                snapshot_id: snapshot_id.to_string(),
                dest,
                reply: rtx,
            })
            .map_err(|e| Error::Ingestion(BoxError::from(e)))?;
        rrx.recv_async()
            .await
            .map_err(|e| Error::Ingestion(BoxError::from(e)))?
    }

    /// Append feature rows to `feature_store` and refresh the wide pivot views
    /// for every distinct family touched. Returns the row count written.
    ///
    /// # Errors
    /// Propagates validation/storage errors from the writer.
    pub async fn write_features(&self, rows: Vec<FeatureRow>) -> Result<usize> {
        let (rtx, rrx) = flume::bounded(1);
        self.tx
            .send(Cmd::WriteFeatures { rows, reply: rtx })
            .map_err(|e| Error::Ingestion(BoxError::from(e)))?;
        rrx.recv_async()
            .await
            .map_err(|e| Error::Ingestion(BoxError::from(e)))?
    }

    /// Append semantic-catalog rows. Returns the row count written.
    ///
    /// # Errors
    /// Propagates validation/storage errors from the writer.
    pub async fn write_catalog(&self, rows: Vec<CatalogRow>) -> Result<usize> {
        let (rtx, rrx) = flume::bounded(1);
        self.tx
            .send(Cmd::WriteCatalog { rows, reply: rtx })
            .map_err(|e| Error::Ingestion(BoxError::from(e)))?;
        rrx.recv_async()
            .await
            .map_err(|e| Error::Ingestion(BoxError::from(e)))?
    }

    /// Append suppression writebacks idempotently (Q3, E1). Returns the number
    /// of rows actually inserted (duplicates by `suppression_id` are skipped).
    ///
    /// # Errors
    /// Propagates storage errors from the writer.
    pub async fn write_suppression(&self, rows: Vec<SuppressionRow>) -> Result<usize> {
        let (rtx, rrx) = flume::bounded(1);
        self.tx
            .send(Cmd::WriteSuppression { rows, reply: rtx })
            .map_err(|e| Error::Ingestion(BoxError::from(e)))?;
        rrx.recv_async()
            .await
            .map_err(|e| Error::Ingestion(BoxError::from(e)))?
    }

    /// Run the producer registered under `id` at `as_of` and persist its output
    /// to `feature_store`. The producer reads on the caller's async task; only
    /// the write crosses into the single writer thread (spec 20 I1).
    ///
    /// # Errors
    /// - [`Error::InvalidInput`] if `id` is not a registered producer.
    /// - Propagates producer read/compute and writer storage errors.
    pub async fn run_producer(&self, id: &str, as_of: &str) -> Result<usize> {
        let producer = self
            .registry
            .get(id)
            .ok_or_else(|| Error::InvalidInput(format!("unknown producer {id:?}")))?;
        let output = producer.run(as_of).await?;
        self.write_features(output.rows).await
    }

    /// Signal the writer thread to stop. Best-effort.
    pub fn shutdown(&self) {
        let _ = self.tx.send(Cmd::Shutdown);
    }
}

/// The writer thread body: own the writer, track known tables, serve commands.
fn writer_loop(writer: Writer, rx: flume::Receiver<Cmd>) {
    let mut known: HashSet<(String, String)> = HashSet::new();
    for cmd in rx.iter() {
        match cmd {
            Cmd::IngestRaw {
                system,
                entity,
                columns,
                rows,
                reply,
            } => {
                let res = writer.ingest_raw(&system, &entity, &columns, &rows);
                if res.is_ok() {
                    known.insert((system, entity));
                }
                let _ = reply.send(res);
            }
            Cmd::CompactAll { reply } => {
                let mut last: Result<()> = Ok(());
                for (s, e) in &known {
                    if let Err(err) = writer.compact(s, e) {
                        last = Err(err);
                    }
                }
                let _ = reply.send(last);
            }
            Cmd::Materialize {
                subquery_sql,
                subquery_params,
                key_column,
                spec,
                reply,
            } => {
                let res = writer.materialize_snapshot(
                    &subquery_sql,
                    &subquery_params,
                    &key_column,
                    &spec,
                );
                let _ = reply.send(res);
            }
            Cmd::ExportParquet {
                snapshot_id,
                dest,
                reply,
            } => {
                let res = writer.export_snapshot_parquet(&snapshot_id, &dest);
                let _ = reply.send(res);
            }
            Cmd::WriteFeatures { rows, reply } => {
                let res = writer.write_features_and_refresh(&rows);
                let _ = reply.send(res);
            }
            Cmd::WriteCatalog { rows, reply } => {
                let res = writer.write_catalog_rows(&rows);
                let _ = reply.send(res);
            }
            Cmd::WriteSuppression { rows, reply } => {
                let res = writer.write_suppression_idempotent(&rows);
                let _ = reply.send(res);
            }
            Cmd::Shutdown => break,
        }
    }
}

#[cfg(test)]
mod tests {
    use consumer_engine_core::SnapshotSpec;
    use consumer_engine_storage::Writer;
    use duckdb::types::Value;

    use super::*;

    #[tokio::test]
    async fn test_should_materialize_via_handle() {
        let tmp = tempfile::tempdir().expect("tmp");
        let writer =
            Writer::attach(&tmp.path().join("cat.db"), &tmp.path().join("data")).expect("attach");
        // Seed rows so two distinct users match `sku = 'A'`.
        writer
            .ingest_raw(
                "erp",
                "orders",
                &["user_id".into(), "sku".into()],
                &[
                    vec![Some("u1".into()), Some("A".into())],
                    vec![Some("u2".into()), Some("A".into())],
                    vec![Some("u3".into()), Some("B".into())],
                ],
            )
            .expect("ingest");

        let handle = IngestionHandle::start(writer, Arc::new(ProducerRegistry::new()))
            .expect("start handle");
        let spec = SnapshotSpec {
            snapshot_id: uuid::Uuid::now_v7().to_string(),
            campaign_id: "c1".into(),
            as_of_ts: chrono::Utc::now().to_rfc3339(),
            features: "{}".into(),
            hit_reason: "{}".into(),
        };
        let rows = handle
            .materialize_snapshot(
                "SELECT DISTINCT base.user_id FROM dl.raw_erp_orders base WHERE base.sku = ?",
                vec![Value::Text("A".into())],
                "user_id",
                spec.clone(),
            )
            .await
            .expect("materialize");
        assert!(
            rows >= 2,
            "expected at least 2 distinct users matching sku=A, got {rows}"
        );

        // Verify the snapshot is observable read-only with non-null
        // hit_reason/features (atomicity + D11). A fresh read-only attach sees
        // the committed rows (DuckLake durability).
        let r = consumer_engine_storage::open_reader(
            &tmp.path().join("cat.db"),
            &tmp.path().join("data"),
        )
        .expect("read attach");
        let mut stmt = r
            .prepare(
                "SELECT count(*), count(hit_reason), count(features) FROM dro.audience_snapshot \
                 WHERE snapshot_id = CAST(? AS UUID)",
            )
            .expect("prepare");
        let row: (i64, i64, i64) = stmt
            .query_row(duckdb::params![&spec.snapshot_id], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })
            .expect("query");
        assert_eq!(
            row.0, rows as i64,
            "row count must match materialise result"
        );
        assert_eq!(
            row.1, rows as i64,
            "hit_reason must be non-null on every row"
        );
        assert_eq!(row.2, rows as i64, "features must be non-null on every row");

        handle.shutdown();
    }

    #[tokio::test]
    async fn test_should_run_producer_writes_feature_store_and_view() {
        use consumer_engine_core::Dataset;
        use consumer_engine_execution::{Reader, ReaderLimits};

        let tmp = tempfile::tempdir().expect("tmp");
        let writer =
            Writer::attach(&tmp.path().join("cat.db"), &tmp.path().join("data")).expect("attach");
        writer
            .ingest_raw(
                "erp",
                "orders",
                &["user_id".into(), "ts".into()],
                &[
                    vec![Some("reg".into()), Some("2025-01-01T00:00:00Z".into())],
                    vec![Some("reg".into()), Some("2025-01-08T00:00:00Z".into())],
                ],
            )
            .expect("ingest");

        let read_conn = consumer_engine_storage::open_reader(
            &tmp.path().join("cat.db"),
            &tmp.path().join("data"),
        )
        .expect("read attach");
        let attach_sql = consumer_engine_storage::read_only_attach_sql(
            &tmp.path().join("cat.db"),
            &tmp.path().join("data"),
        );
        let reader = Reader::start(read_conn, attach_sql, ReaderLimits::default()).expect("reader");

        let registry = Arc::new(ProducerRegistry::new());
        registry
            .register(Arc::new(CadenceRegularityProducer::new(
                reader.clone(),
                Dataset {
                    system: "erp".into(),
                    entity: "orders".into(),
                },
            )))
            .expect("register producer");

        let handle = IngestionHandle::start(writer, registry).expect("start handle");
        let n = handle
            .run_producer("cadence_sql", "2025-12-31T00:00:00Z")
            .await
            .expect("run");
        assert_eq!(n, 1, "one feature row for the single regular buyer");

        // The wide view must be readable via a fresh read-only attach (the
        // load-bearing cross-alias resolution the compiler relies on).
        let r = consumer_engine_storage::open_reader(
            &tmp.path().join("cat.db"),
            &tmp.path().join("data"),
        )
        .expect("read attach");
        let mut stmt = r
            .prepare("SELECT count(*) FROM dro.feature_wide_cadence")
            .expect("prepare wide view");
        let count: i64 = stmt.query_row([], |row| row.get(0)).expect("count");
        assert_eq!(count, 1, "wide view must expose the produced feature");

        handle.shutdown();
    }

    #[tokio::test]
    async fn test_should_keep_existing_columns_on_partial_refresh() {
        // Regression: a second batch emitting a *subset* of a family's features
        // must NOT drop the previously-written columns from the wide view
        // (specs/10 §2 — the wide pivot must cover all stored features).
        use consumer_engine_core::FeatureRow;

        let tmp = tempfile::tempdir().expect("tmp");
        let writer =
            Writer::attach(&tmp.path().join("cat.db"), &tmp.path().join("data")).expect("attach");
        let handle = IngestionHandle::start(writer, Arc::new(ProducerRegistry::new()))
            .expect("start handle");

        // Batch 1: cadence.regularity only.
        handle
            .write_features(vec![FeatureRow {
                user_id: "u1".into(),
                feature_name: "cadence.regularity".into(),
                num_value: 0.9,
                as_of_ts: "2025-01-01T00:00:00Z".into(),
                producer_id: "cadence_sql".into(),
            }])
            .await
            .expect("write batch 1");

        // Batch 2: cadence.volume only — a subset that omits regularity.
        handle
            .write_features(vec![FeatureRow {
                user_id: "u1".into(),
                feature_name: "cadence.volume".into(),
                num_value: 5.0,
                as_of_ts: "2025-01-01T00:00:00Z".into(),
                producer_id: "cadence_sql".into(),
            }])
            .await
            .expect("write batch 2");

        // The wide view must still expose BOTH columns (union with stored names).
        let r = consumer_engine_storage::open_reader(
            &tmp.path().join("cat.db"),
            &tmp.path().join("data"),
        )
        .expect("read attach");
        let mut stmt = r
            .prepare("SELECT regularity, volume FROM dro.feature_wide_cadence")
            .expect("both columns must survive the partial refresh");
        let row: (Option<f64>, Option<f64>) = stmt
            .query_row([], |row| Ok((row.get(0)?, row.get(1)?)))
            .expect("query");
        assert_eq!(
            row.0,
            Some(0.9),
            "regularity (batch 1) must survive batch 2's partial refresh"
        );
        assert_eq!(row.1, Some(5.0), "volume (batch 2) must be present");

        handle.shutdown();
    }
}
