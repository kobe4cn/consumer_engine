//! CDC ingestion: the [`SourceAdapter`] contract (specs/20 §2), the
//! [`SourceBatch`] shape, and the [`run_cdc_pump`] driver that moves batches
//! into the single writer with **atomic data + offset commit** (specs/20 I2,
//! issue #24) and marks the source's freshness as CDC (D5).
//!
//! The pump resumes from the last committed offset on start (restart is
//! at-least-once from the source; the writer's per-key MERGE dedup makes the
//! catalog effectively-once — see the survey memo). A Kafka/Debezium adapter
//! ships behind the `ingestion-cdc` feature; the trait is feature-independent
//! so tests drive the machinery with a deterministic mock.

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use consumer_engine_core::{Error, FreshnessRegistry, Result, SourceType, now_epoch};

use crate::IngestionHandle;

/// One CDC change batch: rows to upsert (deduped by `key`, last wins) and keys
/// to logically delete, plus the source's **offsets after this batch** — one
/// `(partition, offset)` per partition, committed atomically with the data
/// (I2) so a multi-partition topic resumes exactly.
#[derive(Debug, Clone, PartialEq)]
pub struct SourceBatch {
    /// Source system identifier.
    pub system: String,
    /// Source entity (table) identifier.
    pub entity: String,
    /// Column names (the `key` must be one of them).
    pub columns: Vec<String>,
    /// The merge key column.
    pub key: String,
    /// Rows to upsert (each aligned with `columns`).
    pub upserts: Vec<Vec<Option<String>>>,
    /// Keys to logically delete.
    pub deletes: Vec<String>,
    /// `(partition, offset)` positions to commit with this batch (the last
    /// consumed offset per partition).
    pub offsets: Vec<(i32, i64)>,
}

/// A CDC source adapter (specs/20 §2): yields batches and can seek to a
/// previously committed offset for restart (I2). Implementations are
/// `dyn`-dispatched, hence `async_trait`.
#[async_trait]
pub trait SourceAdapter: Send {
    /// The source key used in the offset store (`system.entity`).
    fn source_id(&self) -> &str;
    /// Yield the next change batch, or `None` when nothing is available (the
    /// pump sleeps and polls again).
    ///
    /// # Errors
    /// Propagates source failures; the pump surfaces them (the caller decides
    /// retry/backoff).
    async fn next_batch(&mut self) -> Result<Option<SourceBatch>>;
    /// Seek the source to previously committed offsets, one per partition
    /// (restart recovery, I2).
    ///
    /// # Errors
    /// Propagates seek failures.
    async fn resume(&mut self, offsets: &[(i32, i64)]) -> Result<()>;
}

/// Drive `adapter` forever: resume from the last committed offset, apply each
/// batch atomically, and mark the source's freshness as CDC (D5 / 71 §2).
/// Polls `poll_interval` when the adapter reports no data.
///
/// # Errors
/// Propagates the first adapter/storage failure (the caller supervises and
/// restarts; the committed offset guarantees no replay loss).
pub async fn run_cdc_pump(
    adapter: &mut dyn SourceAdapter,
    handle: &IngestionHandle,
    freshness: &Arc<FreshnessRegistry>,
    tenant: &str,
    poll_interval: Duration,
) -> Result<()> {
    let offsets = handle.read_cdc_offsets(adapter.source_id()).await?;
    if !offsets.is_empty() {
        adapter.resume(&offsets).await?;
        tracing::info!(
            source = %adapter.source_id(),
            offsets = ?offsets,
            "CDC pump resumed from committed offsets"
        );
    }
    let mut backoff = poll_interval;
    loop {
        // The pump NEVER exits on a transient failure: adapter/storage errors
        // are logged and retried with backoff, so a bad message or a
        // momentary storage error cannot silently halt the source (the
        // supervisor only acts on a hard panic).
        let outcome = async {
            match adapter.next_batch().await? {
                Some(batch) => {
                    let rows = handle.apply_cdc_batch(batch.clone(), tenant).await?;
                    let epoch = now_epoch();
                    freshness.set(&batch.system, &batch.entity, SourceType::Cdc, epoch)?;
                    tracing::debug!(
                        source = %adapter.source_id(),
                        rows = rows,
                        offsets = ?batch.offsets,
                        "CDC batch applied"
                    );
                    Ok::<(), Error>(())
                }
                None => {
                    tokio::time::sleep(poll_interval).await;
                    Ok(())
                }
            }
        }
        .await;
        match outcome {
            Ok(()) => backoff = poll_interval,
            Err(e) => {
                tracing::warn!(error = %e, source = %adapter.source_id(), "CDC batch failed; retrying");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(30));
            }
        }
    }
}

/// `source.system.entity` — the offset-store key the pump uses.
#[must_use]
pub fn source_key(system: &str, entity: &str) -> String {
    format!("{system}.{entity}")
}
