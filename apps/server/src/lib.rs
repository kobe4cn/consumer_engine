//! Server wiring: constructs the engine (storage writer, read-only reader,
//! single ingestion actor, REST router) from [`EngineConfig`] and serves it.
//!
//! See `specs/11-runtime-core.md` for the actor topology and the single-writer
//! invariant this module realises.

#![forbid(unsafe_code)]
#![warn(rust_2024_compatibility, missing_docs, missing_debug_implementations)]

use std::{
    sync::{Arc, atomic::AtomicI64},
    time::Duration,
};

use axum::Router;
use consumer_engine_core::{EngineConfig, Result};
use consumer_engine_execution::{Reader, ReaderLimits};
use consumer_engine_ingestion::IngestionHandle;
use consumer_engine_ingress::{AppState, router};
use consumer_engine_storage::{self as storage, Writer};
use tokio::task::JoinHandle;
use tracing::warn;

/// Owns the engine's long-lived handles. Dropping it shuts the actor threads and
/// the compaction task down.
#[must_use]
pub struct Engine {
    ingestion: IngestionHandle,
    reader: Reader,
    #[allow(dead_code)]
    last_ingest_epoch: Arc<AtomicI64>,
    compaction: JoinHandle<()>,
}

impl std::fmt::Debug for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Engine").finish_non_exhaustive()
    }
}

impl Engine {
    /// Build the engine from `config`: attach the writer (acquiring the
    /// single-writer lock), start the read-only reader, start the ingestion
    /// actor, spawn the compaction sweep, and return the assembled router +
    /// owning [`Engine`].
    ///
    /// # Errors
    /// Propagates storage/execution/ingestion setup errors.
    pub fn build(config: &EngineConfig) -> Result<(Router, Engine)> {
        let writer = Writer::attach(&config.catalog_path, &config.data_path)?;
        let read_conn = storage::open_reader(&config.catalog_path, &config.data_path)?;
        let attach_sql = storage::read_only_attach_sql(&config.catalog_path, &config.data_path);
        let limits = ReaderLimits {
            memory_limit: config.guardrails.memory_limit.clone(),
            threads: config.guardrails.threads,
        };
        let reader = Reader::start(read_conn, attach_sql, limits)?;
        let ingestion = IngestionHandle::start(writer)?;

        let last_ingest_epoch = Arc::new(AtomicI64::new(0));
        let query_engine = consumer_engine_query::QueryEngine::new(
            reader.clone(),
            config.guardrails.clone(),
            Arc::clone(&last_ingest_epoch),
        );
        let compaction = tokio::spawn(supervise_compaction(
            ingestion.clone(),
            config.compaction_interval_secs,
        ));

        let state = AppState {
            ingestion: ingestion.clone(),
            query_engine,
            last_ingest_epoch: Arc::clone(&last_ingest_epoch),
        };
        let router = router(state);
        let engine = Engine {
            ingestion,
            reader,
            last_ingest_epoch,
            compaction,
        };
        Ok((router, engine))
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        self.ingestion.shutdown();
        self.reader.shutdown();
        self.compaction.abort();
    }
}

/// Supervise the compaction loop: if it ever panics, log and respawn. Per
/// AGENTS.md § Async ("always handle task panics"). The loop is infinite, so
/// the supervisor only observes a result on panic; `Engine::drop` aborts the
/// supervisor, whose drop aborts the inner task.
async fn supervise_compaction(ingestion: IngestionHandle, interval_secs: u64) {
    let mut set = tokio::task::JoinSet::new();
    set.spawn(compaction_loop(ingestion.clone(), interval_secs));
    while let Some(res) = set.join_next().await {
        if let Err(e) = res {
            tracing::error!(error = %e, "compaction task panicked; respawning");
            set.spawn(compaction_loop(ingestion.clone(), interval_secs));
        }
    }
}

/// Periodic best-effort compaction sweep over all ingested tables.
async fn compaction_loop(ingestion: IngestionHandle, interval_secs: u64) {
    if interval_secs == 0 {
        return;
    }
    let mut ticker = tokio::time::interval(Duration::from_secs(interval_secs));
    // The first tick fires immediately; skip it so the first real sweep waits
    // a full interval.
    ticker.tick().await;
    loop {
        ticker.tick().await;
        if let Err(e) = ingestion.compact_all().await {
            warn!(error = %e, "compaction sweep failed");
        }
    }
}
