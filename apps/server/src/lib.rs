//! Server wiring: constructs the engine (storage writer, read-only reader,
//! single ingestion actor, REST router) from [`EngineConfig`] and serves it.
//!
//! See `specs/11-runtime-core.md` for the actor topology and the single-writer
//! invariant this module realises.

#![forbid(unsafe_code)]
#![warn(rust_2024_compatibility, missing_docs, missing_debug_implementations)]

use std::{sync::Arc, time::Duration};

use axum::Router;
use consumer_engine_core::{BoxError, Dataset, EngineConfig, Error, FreshnessRegistry, Result};
use consumer_engine_execution::{Reader, ReaderLimits};
use consumer_engine_ingestion::{CadenceRegularityProducer, IngestionHandle, ProducerRegistry};
use consumer_engine_ingress::{AppState, JobRegistry, router};
use consumer_engine_semantic::{IntentRag, Profiler, StubEmbed, StubLlm};
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
    freshness: Arc<FreshnessRegistry>,
    #[allow(dead_code)]
    registry: Arc<ProducerRegistry>,
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
        // Initialise the materialise schema up front so read-only `snapshot_meta`
        // queries never hit a missing `audience_snapshot` table before the first
        // materialise (the writer creates it lazily too, but a startup init keeps
        // reads clean).
        writer.ensure_audience_snapshot_table()?;
        // Initialise the Feature Store + semantic catalog tables at startup so a
        // producer run / profile never races a lazy DDL (D9 / spec 13).
        writer.ensure_feature_store_table()?;
        writer.ensure_semantic_catalog_table()?;
        let read_conn = storage::open_reader(&config.catalog_path, &config.data_path)?;
        let attach_sql = storage::read_only_attach_sql(&config.catalog_path, &config.data_path);
        let limits = ReaderLimits {
            memory_limit: config.guardrails.memory_limit.clone(),
            threads: config.guardrails.threads,
        };
        let reader = Reader::start(read_conn, attach_sql, limits)?;

        // Graded per-source freshness registry (D5).
        let freshness = Arc::new(FreshnessRegistry::new());

        // Semantic layer (M3 stub clients: deterministic, no network).
        let embed: Arc<dyn consumer_engine_semantic::EmbeddingModel> =
            Arc::new(StubEmbed::default());
        let llm: Arc<dyn consumer_engine_semantic::LlmClient> = Arc::new(StubLlm);
        let profiler = Arc::new(Profiler::new(reader.clone(), llm, Arc::clone(&embed)));
        let intent_rag = Arc::new(IntentRag::new(reader.clone(), Arc::clone(&embed)));

        // Feature Store producers (D9). M3 wires the PRD demo cadence producer
        // over `erp.orders`; producer wiring becomes config-driven in a later
        // phase.
        let registry = Arc::new(ProducerRegistry::new());
        registry.register(Arc::new(CadenceRegularityProducer::new(
            reader.clone(),
            Dataset {
                system: "erp".into(),
                entity: "orders".into(),
            },
        )))?;

        let ingestion = IngestionHandle::start(writer, Arc::clone(&registry))?;
        // M2 signing key: 32 bytes of OS randomness (AGENTS.md § Crypto forbids
        // thread_rng for secrets; getrandom is the OsRng-equivalent source).
        let mut signing_key = [0u8; 32];
        getrandom::fill(&mut signing_key).map_err(|e| Error::Execution(BoxError::from(e)))?;
        let query_engine = consumer_engine_query::QueryEngine::new(
            reader.clone(),
            ingestion.clone(),
            config.guardrails.clone(),
            Arc::clone(&freshness),
        );
        let compaction = tokio::spawn(supervise_compaction(
            ingestion.clone(),
            config.compaction_interval_secs,
        ));

        let jobs = Arc::new(JobRegistry::new());
        // Materialisation concurrency cap: one slot per guardrail thread (the
        // sync query path already bounds in-flight work the same way).
        let materialise_slots = Arc::new(tokio::sync::Semaphore::new(
            config.guardrails.threads.max(1),
        ));
        let state = AppState {
            ingestion: ingestion.clone(),
            query_engine,
            freshness: Arc::clone(&freshness),
            profiler,
            intent_rag,
            jobs: Arc::clone(&jobs),
            materialise_slots,
            signing_key: Arc::new(signing_key),
        };
        let router = router(state);
        let engine = Engine {
            ingestion,
            reader,
            freshness,
            registry,
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
