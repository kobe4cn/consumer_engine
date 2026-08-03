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

use std::{collections::HashSet, path::PathBuf, thread};

use consumer_engine_core::{BoxError, Error, Result, SnapshotSpec};
use consumer_engine_storage::Writer;
use duckdb::types::Value;

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
    /// Stop the writer thread.
    Shutdown,
}

/// Handle to the single ingestion writer. Cheap to clone.
#[derive(Clone)]
pub struct IngestionHandle {
    tx: flume::Sender<Cmd>,
}

impl std::fmt::Debug for IngestionHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IngestionHandle").finish_non_exhaustive()
    }
}

impl IngestionHandle {
    /// Start the writer thread owning `writer`. Exactly one handle should be
    /// built per catalog (the writer's file lock enforces singleness).
    ///
    /// # Errors
    /// - [`Error::Ingestion`] if the thread cannot be spawned.
    pub fn start(writer: Writer) -> Result<Self> {
        let (tx, rx) = flume::bounded::<Cmd>(64);
        thread::Builder::new()
            .name("ce-ingestion".into())
            .spawn(move || writer_loop(writer, rx))
            .map_err(|e| Error::Ingestion(BoxError::from(e)))?;
        Ok(Self { tx })
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

        let handle = IngestionHandle::start(writer).expect("start handle");
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
}
