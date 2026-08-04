//! The single ingestion writer actor.
//!
//! Owns the sole [`Writer`] to DuckLake (decision D3) inside a dedicated OS
//! thread — `duckdb::Connection` is not `Sync`. The async side sends commands
//! over a `flume` channel and awaits typed replies.
//!
//! Raw source batches (`Cmd::IngestRaw`) accumulate in a per-`(system, entity)`
//! micro-batcher (decision D6, specs/71 §4): a batch is flushed when its row
//! count reaches `MicroBatchConfig::flush_rows` or its age reaches
//! `flush_age_secs` (checked on every command arrival), and always on
//! shutdown. Flushing is a multi-row `VALUES` insert — one DuckLake commit per
//! flush (issue #12: per-row commits balloon the catalog). Feature / catalog /
//! suppression / materialise commands write through immediately (they are
//! transactional with their own refresh/derivation).

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
        /// The caller's tenant, stamped on every row (issue #22).
        tenant: String,
        /// Reply channel carrying the inserted row count.
        reply: flume::Sender<Result<usize>>,
    },
    /// Compact every table this actor has ingested, then run the catalog
    /// maintenance pass — expire snapshots older than `retention_days` and
    /// delete orphaned files (specs/71 §4, issue #17).
    CompactAll {
        /// Time-travel retention window for snapshot expiry (days).
        retention_days: u64,
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
        /// The caller's tenant, stamped on the snapshot (issue #22).
        tenant: String,
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
    /// Upsert a dimension batch by `key` (specs/20 §4): adapter-boundary dedup
    /// by key (last wins) + `WHEN MATCHED THEN UPDATE / WHEN NOT MATCHED THEN
    /// INSERT` MERGE.
    UpsertRaw {
        /// Source system identifier.
        system: String,
        /// Source entity (table) identifier.
        entity: String,
        /// Column names.
        columns: Vec<String>,
        /// The merge key column.
        key: String,
        /// Rows of optional string cells.
        rows: Vec<Vec<Option<String>>>,
        /// The caller's tenant, stamped on every row (issue #22).
        tenant: String,
        /// Reply channel carrying the rows affected.
        reply: flume::Sender<Result<usize>>,
    },
    /// Logically delete rows by `key` (specs/20 §4).
    DeleteRaw {
        /// Source system identifier.
        system: String,
        /// Source entity (table) identifier.
        entity: String,
        /// The key column.
        key: String,
        /// Key values to delete.
        keys: Vec<String>,
        /// The caller's tenant — deletes are tenant-scoped (issue #22).
        tenant: String,
        /// Reply channel carrying the rows affected.
        reply: flume::Sender<Result<usize>>,
    },
    /// Append feature rows to `feature_store` and refresh the wide pivot views
    /// for every distinct family touched (D9).
    WriteFeatures {
        /// The feature rows to append.
        rows: Vec<FeatureRow>,
        /// The caller's tenant, stamped on every row (issue #22).
        tenant: String,
        /// Reply channel carrying the inserted row count.
        reply: flume::Sender<Result<usize>>,
    },
    /// Append semantic-catalog rows (Profiler output).
    WriteCatalog {
        /// The catalog rows to append.
        rows: Vec<CatalogRow>,
        /// The caller's tenant, stamped on every row (issue #22).
        tenant: String,
        /// Reply channel carrying the inserted row count.
        reply: flume::Sender<Result<usize>>,
    },
    /// Append suppression writebacks idempotently (Q3, E1).
    WriteSuppression {
        /// The suppression rows to append (deduped by `suppression_id`).
        rows: Vec<SuppressionRow>,
        /// The caller's tenant, stamped on every row (issue #22).
        tenant: String,
        /// Reply channel carrying the inserted row count.
        reply: flume::Sender<Result<usize>>,
    },
    /// Stop the writer thread (graceful drain; the ack fires after the flush).
    Shutdown {
        /// Acknowledges the drain — the writer sends after flushing buffered
        /// batches and before exiting its loop.
        ack: flume::Sender<()>,
    },
}

/// Handle to the single ingestion writer. Cheap to clone.
#[derive(Clone)]
pub struct IngestionHandle {
    tx: flume::Sender<Cmd>,
    registry: Arc<ProducerRegistry>,
}

/// Micro-batch flush policy for raw source batches (decision D6, specs/71 §4).
/// `flush_rows` of `0` disables buffering entirely — every `IngestRaw` flushes
/// immediately.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MicroBatchConfig {
    /// Flush a `(system, entity)` batch once this many rows are queued.
    pub flush_rows: u64,
    /// Flush a batch once it has been buffered this long (checked on command
    /// arrival and on shutdown). `0` disables age-based flush.
    pub flush_age_secs: u64,
}

impl MicroBatchConfig {
    /// Spec defaults (specs/71 §4): 50k rows or 30 s, whichever comes first.
    #[must_use]
    pub const fn default_config() -> Self {
        Self {
            flush_rows: 50_000,
            flush_age_secs: 30,
        }
    }

    /// Unit-test convenience: flush on every ingest (equivalent to the pre-
    /// micro-batch behaviour). Production wiring uses
    /// [`Self::default_config`] or the engine config values.
    #[must_use]
    pub const fn immediate() -> Self {
        Self {
            flush_rows: 1,
            flush_age_secs: 0,
        }
    }
}

impl Default for MicroBatchConfig {
    fn default() -> Self {
        Self::default_config()
    }
}

/// A not-yet-flushed raw batch for one `(tenant, system, entity)`, with its
/// shape, its owning tenant, and buffering start time (decision D6). The tenant
/// is part of the KEY (issue #22): interleaved tenants' rows never merge into
/// one batch, so a flush can never stamp one tenant's rows with another's.
struct PendingBatch {
    columns: Vec<String>,
    rows: Vec<Vec<Option<String>>>,
    tenant: String,
    since: std::time::Instant,
}

impl std::fmt::Debug for IngestionHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IngestionHandle").finish_non_exhaustive()
    }
}

impl IngestionHandle {
    /// Start the writer thread owning `writer` and bound to `registry`, with
    /// the micro-batch policy from `config`. Exactly one handle should be built
    /// per catalog (the writer's file lock enforces singleness). The registry
    /// holds the Feature Store producers (D9).
    ///
    /// # Errors
    /// - [`Error::Ingestion`] if the thread cannot be spawned.
    pub fn start_with(
        writer: Writer,
        registry: Arc<ProducerRegistry>,
        config: MicroBatchConfig,
    ) -> Result<Self> {
        let (tx, rx) = flume::bounded::<Cmd>(64);
        thread::Builder::new()
            .name("ce-ingestion".into())
            .spawn(move || writer_loop(writer, rx, config))
            .map_err(|e| Error::Ingestion(BoxError::from(e)))?;
        Ok(Self { tx, registry })
    }

    /// Start the writer thread with immediate-flush behaviour. **Test
    /// convenience only** — production wiring passes an explicit
    /// [`MicroBatchConfig`] via [`Self::start_with`]; this default silently
    /// disables micro-batching, which is why it is hidden from the docs.
    ///
    /// # Errors
    /// - [`Error::Ingestion`] if the thread cannot be spawned.
    #[doc(hidden)]
    pub fn start(writer: Writer, registry: Arc<ProducerRegistry>) -> Result<Self> {
        Self::start_with(writer, registry, MicroBatchConfig::immediate())
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
        tenant: &str,
    ) -> Result<usize> {
        let (rtx, rrx) = flume::bounded(1);
        self.tx
            .send(Cmd::IngestRaw {
                system: system.to_string(),
                entity: entity.to_string(),
                columns,
                rows,
                tenant: tenant.to_string(),
                reply: rtx,
            })
            .map_err(|e| Error::Ingestion(BoxError::from(e)))?;
        rrx.recv_async()
            .await
            .map_err(|e| Error::Ingestion(BoxError::from(e)))?
    }

    /// Compact every table ingested so far, expire old snapshots, and clean
    /// orphaned files (best-effort; last error wins). `retention_days` is the
    /// time-travel window for snapshot expiry (specs/71 §4, issue #17).
    ///
    /// # Errors
    /// Propagates the first storage error encountered.
    pub async fn compact_all(&self, retention_days: u64) -> Result<()> {
        let (rtx, rrx) = flume::bounded(1);
        self.tx
            .send(Cmd::CompactAll {
                retention_days,
                reply: rtx,
            })
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
        tenant: &str,
    ) -> Result<u64> {
        let (rtx, rrx) = flume::bounded(1);
        self.tx
            .send(Cmd::Materialize {
                subquery_sql: subquery_sql.to_string(),
                subquery_params,
                key_column: key_column.to_string(),
                spec,
                tenant: tenant.to_string(),
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
    pub async fn write_features(&self, rows: Vec<FeatureRow>, tenant: &str) -> Result<usize> {
        let (rtx, rrx) = flume::bounded(1);
        self.tx
            .send(Cmd::WriteFeatures {
                rows,
                tenant: tenant.to_string(),
                reply: rtx,
            })
            .map_err(|e| Error::Ingestion(BoxError::from(e)))?;
        rrx.recv_async()
            .await
            .map_err(|e| Error::Ingestion(BoxError::from(e)))?
    }

    /// Append semantic-catalog rows. Returns the row count written.
    ///
    /// # Errors
    /// Propagates validation/storage errors from the writer.
    pub async fn write_catalog(&self, rows: Vec<CatalogRow>, tenant: &str) -> Result<usize> {
        let (rtx, rrx) = flume::bounded(1);
        self.tx
            .send(Cmd::WriteCatalog {
                rows,
                tenant: tenant.to_string(),
                reply: rtx,
            })
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
    pub async fn write_suppression(
        &self,
        rows: Vec<SuppressionRow>,
        tenant: &str,
    ) -> Result<usize> {
        let (rtx, rrx) = flume::bounded(1);
        self.tx
            .send(Cmd::WriteSuppression {
                rows,
                tenant: tenant.to_string(),
                reply: rtx,
            })
            .map_err(|e| Error::Ingestion(BoxError::from(e)))?;
        rrx.recv_async()
            .await
            .map_err(|e| Error::Ingestion(BoxError::from(e)))?
    }

    /// Upsert a dimension batch by `key` through the single writer (specs/20
    /// §4: dedup by key, update-or-insert). Returns the rows affected.
    ///
    /// # Errors
    /// Propagates validation/storage errors from the writer.
    pub async fn upsert_raw(
        &self,
        system: &str,
        entity: &str,
        columns: Vec<String>,
        key: String,
        rows: Vec<Vec<Option<String>>>,
        tenant: &str,
    ) -> Result<usize> {
        let (rtx, rrx) = flume::bounded(1);
        self.tx
            .send(Cmd::UpsertRaw {
                system: system.to_string(),
                entity: entity.to_string(),
                columns,
                key,
                rows,
                tenant: tenant.to_string(),
                reply: rtx,
            })
            .map_err(|e| Error::Ingestion(BoxError::from(e)))?;
        rrx.recv_async()
            .await
            .map_err(|e| Error::Ingestion(BoxError::from(e)))?
    }

    /// Logically delete rows by `key` through the single writer (specs/20 §4).
    /// Returns the rows deleted.
    ///
    /// # Errors
    /// Propagates validation/storage errors from the writer.
    pub async fn delete_raw(
        &self,
        system: &str,
        entity: &str,
        key: String,
        keys: Vec<String>,
        tenant: &str,
    ) -> Result<usize> {
        let (rtx, rrx) = flume::bounded(1);
        self.tx
            .send(Cmd::DeleteRaw {
                system: system.to_string(),
                entity: entity.to_string(),
                key,
                keys,
                tenant: tenant.to_string(),
                reply: rtx,
            })
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
    pub async fn run_producer(&self, id: &str, as_of: &str, tenant: &str) -> Result<usize> {
        let producer = self
            .registry
            .get(id)
            .ok_or_else(|| Error::InvalidInput(format!("unknown producer {id:?}")))?;
        let output = producer.run(as_of).await?;
        self.write_features(output.rows, tenant).await
    }

    /// Ask the writer thread to stop after a graceful drain (specs/11 I3).
    /// **Non-blocking**: safe to call from `Drop` on an async runtime thread
    /// (the writer drains its buffered micro-batches before exiting, in the
    /// background). Tests that must observe durability before continuing use
    /// [`Self::shutdown_and_wait`].
    pub fn shutdown(&self) {
        let (ack_tx, _ack_rx) = flume::bounded::<()>(0);
        let _ = self.tx.send(Cmd::Shutdown { ack: ack_tx });
    }

    /// Like [`Self::shutdown`] but blocks until the writer has drained every
    /// buffered micro-batch and exited (an acked ingest is durable when this
    /// returns). Use in tests/CLI shutdown, not on an async runtime thread.
    pub fn shutdown_and_wait(&self) {
        let (ack_tx, ack_rx) = flume::bounded::<()>(0);
        if self.tx.send(Cmd::Shutdown { ack: ack_tx }).is_ok() {
            // Rendezvous: the writer acks after the drain flush and before
            // exiting its loop.
            let _ = ack_rx.recv();
        }
    }
}

/// The writer thread body: own the writer, track known tables, micro-batch raw
/// source batches (D6), serve commands until shutdown.
///
/// The loop waits up to [`AGE_TICK`] for a command and flushes expired batches
/// on every timeout — a slow trickle of small batches is committed by wall
/// clock, not only when a command happens to arrive (specs/71 §4 freshness
/// SLA: flush age 30 s ⇒ end-to-end freshness well inside the ≤ 5 min bound).
fn writer_loop(mut writer: Writer, rx: flume::Receiver<Cmd>, micro: MicroBatchConfig) {
    let mut known: HashSet<(String, String)> = HashSet::new();
    let mut pending: std::collections::BTreeMap<(String, String, String), PendingBatch> =
        std::collections::BTreeMap::new();
    loop {
        let cmd = match rx.recv_timeout(AGE_TICK) {
            Ok(cmd) => cmd,
            Err(flume::RecvTimeoutError::Timeout) => {
                flush_expired(&mut writer, &mut pending, &mut known, micro.flush_age_secs);
                continue;
            }
            // All senders dropped: no graceful shutdown was requested — flush
            // what is buffered so nothing acked is lost, then exit.
            Err(flume::RecvTimeoutError::Disconnected) => {
                flush_pending(&mut writer, &mut pending, &mut known);
                break;
            }
        };
        // Age-based flush before every command (incl. shutdown).
        flush_expired(&mut writer, &mut pending, &mut known, micro.flush_age_secs);
        match cmd {
            Cmd::IngestRaw {
                system,
                entity,
                columns,
                rows,
                tenant,
                reply,
            } => {
                // The writer stamps its current tenant on every committed row
                // — set it to the caller's before buffering (issue #22).
                writer.set_tenant(tenant.clone());
                let res = if micro.flush_rows == 0 {
                    // Buffering disabled: flush immediately (pre-micro-batch path).
                    let r = writer.ingest_raw(&system, &entity, &columns, &rows);
                    if r.is_ok() {
                        known.insert((system, entity));
                    }
                    r
                } else {
                    // The buffer is keyed by (tenant, system, entity): two
                    // tenants ingesting to the same table never share a batch,
                    // so a flush can never stamp one tenant's rows with the
                    // other's (issue #22).
                    let key = (tenant.clone(), system.clone(), entity.clone());
                    // A schema change (different columns) forces the old batch out
                    // first — buffered rows must not mix shapes.
                    if let Some(existing) = pending.get(&key)
                        && existing.columns != columns
                        && let Some(old) = pending.remove(&key)
                        && let Err(e) = flush_batch(&mut writer, &key, &old, &mut known)
                    {
                        tracing::warn!(error = %e, "schema-change flush failed");
                    }
                    let batch = pending.entry(key.clone()).or_insert_with(|| PendingBatch {
                        columns: columns.clone(),
                        rows: Vec::new(),
                        tenant: tenant.clone(),
                        since: std::time::Instant::now(),
                    });
                    batch.rows.extend(rows);
                    if batch.rows.len() as u64 >= micro.flush_rows {
                        match pending.remove(&key) {
                            Some(batch) => flush_batch(&mut writer, &key, &batch, &mut known),
                            None => Ok(0),
                        }
                    } else {
                        // Buffered; nothing committed yet — the caller sees 0
                        // inserted until the threshold/age triggers the flush.
                        Ok(0)
                    }
                };
                let _ = reply.send(res);
            }
            Cmd::CompactAll {
                retention_days,
                reply,
            } => {
                // Buffered rows are part of the table's latest state; compact
                // only after they land, then expire old snapshots and reclaim
                // orphaned files (issue #17).
                flush_pending(&mut writer, &mut pending, &mut known);
                let mut last: Result<()> = Ok(());
                for (s, e) in &known {
                    if let Err(err) = writer.compact(s, e) {
                        last = Err(err);
                    }
                }
                if let Err(err) = writer.expire_snapshots(retention_days) {
                    last = Err(err);
                }
                if let Err(err) = writer.delete_orphaned_files(retention_days) {
                    last = Err(err);
                }
                let _ = reply.send(last);
            }
            Cmd::UpsertRaw {
                system,
                entity,
                columns,
                key,
                rows,
                tenant,
                reply,
            } => {
                writer.set_tenant(tenant);
                let res = writer.upsert_raw(&system, &entity, &columns, &key, &rows);
                if res.is_ok() {
                    known.insert((system, entity));
                }
                let _ = reply.send(res);
            }
            Cmd::DeleteRaw {
                system,
                entity,
                key,
                keys,
                tenant,
                reply,
            } => {
                writer.set_tenant(tenant);
                let res = writer.delete_raw(&system, &entity, &key, &keys);
                if res.is_ok() {
                    known.insert((system, entity));
                }
                let _ = reply.send(res);
            }
            Cmd::Materialize {
                subquery_sql,
                subquery_params,
                key_column,
                spec,
                tenant,
                reply,
            } => {
                writer.set_tenant(tenant);
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
            Cmd::WriteFeatures {
                rows,
                tenant,
                reply,
            } => {
                writer.set_tenant(tenant);
                let res = writer.write_features_and_refresh(&rows);
                let _ = reply.send(res);
            }
            Cmd::WriteCatalog {
                rows,
                tenant,
                reply,
            } => {
                writer.set_tenant(tenant);
                let res = writer.write_catalog_rows(&rows);
                let _ = reply.send(res);
            }
            Cmd::WriteSuppression {
                rows,
                tenant,
                reply,
            } => {
                writer.set_tenant(tenant);
                let res = writer.write_suppression_idempotent(&rows);
                let _ = reply.send(res);
            }
            Cmd::Shutdown { ack } => {
                // Graceful drain (specs/11 I3): force-flush every buffered batch
                // so no acked ingest is lost on shutdown, then ack and exit.
                flush_pending(&mut writer, &mut pending, &mut known);
                let _ = ack.send(());
                break;
            }
        }
    }
}

/// Wall-clock cadence at which the writer loop wakes to age-flush batches
/// (specs/71 §4: flush age 30 s ⇒ ≤ 5 min end-to-end freshness).
const AGE_TICK: std::time::Duration = std::time::Duration::from_secs(1);

/// Flush one pending batch through the writer; registers the table as known on
/// success (so compaction covers it).
fn flush_batch(
    writer: &mut Writer,
    key: &(String, String, String),
    batch: &PendingBatch,
    known: &mut HashSet<(String, String)>,
) -> Result<usize> {
    // The batch carries its own tenant — an age/shutdown drain may run after
    // other tenants' commands, so stamp the BATCH's tenant, not the writer's
    // current one (issue #22).
    writer.set_tenant(batch.tenant.clone());
    let n = writer.ingest_raw(&key.1, &key.2, &batch.columns, &batch.rows)?;
    known.insert((key.1.clone(), key.2.clone()));
    Ok(n)
}

/// Shared drain loop: flush every pending batch for which `should_flush`
/// holds. A failed flush is logged — the writer thread must keep serving — and
/// the rows are dropped (a retry would need a CDC-style offset, which is #24's
/// scope).
fn flush_some(
    writer: &mut Writer,
    pending: &mut std::collections::BTreeMap<(String, String, String), PendingBatch>,
    known: &mut HashSet<(String, String)>,
    should_flush: impl Fn(&(String, String, String), &PendingBatch) -> bool,
) {
    let keys: Vec<(String, String, String)> = pending
        .iter()
        .filter(|(k, b)| should_flush(k, b))
        .map(|(k, _)| k.clone())
        .collect();
    for k in keys {
        if let Some(b) = pending.remove(&k)
            && let Err(e) = flush_batch(writer, &k, &b, known)
        {
            tracing::warn!(
                error = %e,
                tenant = %k.0,
                system = %k.1,
                entity = %k.2,
                "micro-batch flush failed"
            );
        }
    }
}

/// Flush every pending batch (compaction pre-step and shutdown drain,
/// specs/11 I3).
fn flush_pending(
    writer: &mut Writer,
    pending: &mut std::collections::BTreeMap<(String, String, String), PendingBatch>,
    known: &mut HashSet<(String, String)>,
) {
    flush_some(writer, pending, known, |_, _| true);
}

/// Flush any pending batch whose buffering age exceeds `age_secs` (`0`
/// disables). Called on every loop wake (command arrival or [`AGE_TICK`] timeout)
/// so freshness is wall-clock, not command-gated.
fn flush_expired(
    writer: &mut Writer,
    pending: &mut std::collections::BTreeMap<(String, String, String), PendingBatch>,
    known: &mut HashSet<(String, String)>,
    age_secs: u64,
) {
    if age_secs == 0 {
        return;
    }
    let age = std::time::Duration::from_secs(age_secs);
    let now = std::time::Instant::now();
    flush_some(writer, pending, known, |_, b| {
        now.duration_since(b.since) >= age
    });
}

#[cfg(test)]
mod tests {
    use consumer_engine_core::SnapshotSpec;
    use consumer_engine_storage::Writer;
    use duckdb::types::Value;

    use super::*;

    #[tokio::test]
    async fn test_should_accumulate_rows_until_flush_threshold() {
        // Micro-batch (D6): rows accumulate per (system, entity) and are
        // committed only when the configured row threshold is reached — the
        // pre-threshold ingests report 0 inserted.
        let tmp = tempfile::tempdir().expect("tmp");
        let writer =
            Writer::attach(&tmp.path().join("cat.db"), &tmp.path().join("data")).expect("attach");
        let handle = IngestionHandle::start_with(
            writer,
            Arc::new(ProducerRegistry::new()),
            MicroBatchConfig {
                flush_rows: 3,
                flush_age_secs: 0,
            },
        )
        .expect("start");
        let cols = vec!["user_id".into(), "sku".into()];
        let n1 = handle
            .ingest_raw(
                "erp",
                "orders",
                cols.clone(),
                vec![vec![Some("u1".into()), Some("A".into())]],
                "default",
            )
            .await
            .expect("ingest 1");
        let n2 = handle
            .ingest_raw(
                "erp",
                "orders",
                cols.clone(),
                vec![vec![Some("u2".into()), Some("B".into())]],
                "default",
            )
            .await
            .expect("ingest 2");
        assert_eq!(n1, 0, "below threshold: nothing committed yet");
        assert_eq!(n2, 0, "below threshold: nothing committed yet");
        // The third row crosses the threshold of 3 → the whole batch flushes.
        let n3 = handle
            .ingest_raw(
                "erp",
                "orders",
                cols.clone(),
                vec![vec![Some("u3".into()), Some("C".into())]],
                "default",
            )
            .await
            .expect("ingest 3");
        assert_eq!(n3, 3, "threshold reached: all buffered rows commit at once");
        let r = consumer_engine_storage::open_reader(
            &tmp.path().join("cat.db"),
            &tmp.path().join("data"),
        )
        .expect("read attach");
        let count: i64 = r
            .query_row("SELECT count(*) FROM dro.raw_erp_orders", [], |row| {
                row.get(0)
            })
            .expect("count");
        assert_eq!(count, 3, "all three rows must be durable after the flush");
        handle.shutdown();
    }

    #[tokio::test]
    async fn test_should_flush_on_age_by_wall_clock() {
        // Age-based flush (specs/71 §4) must be wall-clock, not command-gated:
        // a batch below the row threshold is committed once buffered for
        // `flush_age_secs`, even with no further commands arriving.
        let tmp = tempfile::tempdir().expect("tmp");
        let writer =
            Writer::attach(&tmp.path().join("cat.db"), &tmp.path().join("data")).expect("attach");
        let handle = IngestionHandle::start_with(
            writer,
            Arc::new(ProducerRegistry::new()),
            MicroBatchConfig {
                flush_rows: 10_000,
                flush_age_secs: 1,
            },
        )
        .expect("start");
        handle
            .ingest_raw(
                "erp",
                "orders",
                vec!["user_id".into(), "sku".into()],
                vec![vec![Some("u1".into()), Some("A".into())]],
                "default",
            )
            .await
            .expect("ingest");
        // No command is sent: the writer loop's wall-clock tick must flush.
        tokio::time::sleep(std::time::Duration::from_millis(1_400)).await;
        let r = consumer_engine_storage::open_reader(
            &tmp.path().join("cat.db"),
            &tmp.path().join("data"),
        )
        .expect("read attach");
        let count: i64 = r
            .query_row("SELECT count(*) FROM dro.raw_erp_orders", [], |row| {
                row.get(0)
            })
            .expect("count");
        assert_eq!(
            count, 1,
            "wall-clock age-flush must commit the buffered row"
        );
        handle.shutdown_and_wait();
    }

    #[tokio::test]
    async fn test_should_force_flush_on_shutdown() {
        // Graceful drain (specs/11 I3): shutdown force-flushes every buffered
        // batch — an acked ingest is never lost.
        let tmp = tempfile::tempdir().expect("tmp");
        let writer =
            Writer::attach(&tmp.path().join("cat.db"), &tmp.path().join("data")).expect("attach");
        let handle = IngestionHandle::start_with(
            writer,
            Arc::new(ProducerRegistry::new()),
            MicroBatchConfig {
                flush_rows: 10_000,
                flush_age_secs: 0,
            },
        )
        .expect("start");
        handle
            .ingest_raw(
                "erp",
                "orders",
                vec!["user_id".into(), "sku".into()],
                vec![vec![Some("u1".into()), Some("A".into())]],
                "default",
            )
            .await
            .expect("ingest");
        handle.shutdown_and_wait();
        let r = consumer_engine_storage::open_reader(
            &tmp.path().join("cat.db"),
            &tmp.path().join("data"),
        )
        .expect("read attach");
        let count: i64 = r
            .query_row("SELECT count(*) FROM dro.raw_erp_orders", [], |row| {
                row.get(0)
            })
            .expect("count");
        assert_eq!(count, 1, "shutdown must drain the buffered batch");
    }

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
        };
        // The subquery emits `<key>, features, hit_reason` per row (issue #13:
        // frozen features + predicate chain are per-row columns, not scalars).
        let rows = handle
            .materialize_snapshot(
                "SELECT DISTINCT base.user_id, CAST('{}' AS JSON) AS features, CAST('{}' AS JSON) \
                 AS hit_reason FROM dl.raw_erp_orders base WHERE base.sku = ?",
                vec![Value::Text("A".into())],
                "user_id",
                spec.clone(),
                "default",
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
    async fn test_should_upsert_and_delete_via_handle() {
        // The adapter seam: dim upsert (dedup + update-or-insert) and logical
        // delete both route through the single writer (specs/20 §4).
        let tmp = tempfile::tempdir().expect("tmp");
        let writer =
            Writer::attach(&tmp.path().join("cat.db"), &tmp.path().join("data")).expect("attach");
        let handle = IngestionHandle::start(writer, Arc::new(ProducerRegistry::new()))
            .expect("start handle");
        let cols = vec!["id".into(), "tier".into()];
        handle
            .upsert_raw(
                "erp",
                "users",
                cols.clone(),
                "id".into(),
                vec![
                    vec![Some("u1".into()), Some("gold".into())],
                    vec![Some("u2".into()), Some("silver".into())],
                ],
                "default",
            )
            .await
            .expect("upsert");
        // u1 updated (last wins dedup), u3 inserted.
        handle
            .upsert_raw(
                "erp",
                "users",
                cols.clone(),
                "id".into(),
                vec![
                    vec![Some("u1".into()), Some("platinum".into())],
                    vec![Some("u3".into()), Some("diamond".into())],
                ],
                "default",
            )
            .await
            .expect("upsert 2");
        let deleted = handle
            .delete_raw("erp", "users", "id".into(), vec!["u2".into()], "default")
            .await
            .expect("delete");
        assert_eq!(deleted, 1, "u2 logically deleted");
        let r = consumer_engine_storage::open_reader(
            &tmp.path().join("cat.db"),
            &tmp.path().join("data"),
        )
        .expect("read attach");
        let rows: Vec<(String, String)> = {
            let mut stmt = r
                .prepare("SELECT id, tier FROM dro.raw_erp_users ORDER BY id")
                .expect("prepare");
            stmt.query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .expect("query")
            .map(|r| r.expect("row"))
            .collect()
        };
        assert_eq!(
            rows,
            vec![
                ("u1".into(), "platinum".into()),
                ("u3".into(), "diamond".into()),
            ],
            "updated + inserted, deleted key gone, no duplicates"
        );
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
            .run_producer("cadence_sql", "2025-12-31T00:00:00Z", "default")
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
            .write_features(
                vec![FeatureRow {
                    user_id: "u1".into(),
                    feature_name: "cadence.regularity".into(),
                    num_value: 0.9,
                    as_of_ts: "2025-01-01T00:00:00Z".into(),
                    producer_id: "cadence_sql".into(),
                }],
                "default",
            )
            .await
            .expect("write batch 1");

        // Batch 2: cadence.volume only — a subset that omits regularity.
        handle
            .write_features(
                vec![FeatureRow {
                    user_id: "u1".into(),
                    feature_name: "cadence.volume".into(),
                    num_value: 5.0,
                    as_of_ts: "2025-01-01T00:00:00Z".into(),
                    producer_id: "cadence_sql".into(),
                }],
                "default",
            )
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
