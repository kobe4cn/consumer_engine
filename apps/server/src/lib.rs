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
#[cfg(feature = "ingestion-cdc")]
use consumer_engine_ingestion::SourceAdapter;
use consumer_engine_ingestion::{
    CadenceRegularityProducer, IngestionHandle, MicroBatchConfig, ProducerRegistry,
};
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
        // Shared write generation (issue #20 / P1-1): the single writer bumps
        // it after every committed write; the pooled readers re-attach only
        // when it advances — the hot path is attach-free, and readers never
        // serve stale data after a commit.
        let write_gen = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let mut writer = Writer::attach_with_gen(
            &config.catalog_path,
            &config.data_path,
            &config.compaction,
            Some(Arc::clone(&write_gen)),
        )?;
        // Tenant stamping (issue #14): the single writer stamps every committed
        // row with the engine's configured tenant; per-caller isolation from
        // auth claims lands with #22.
        writer.set_tenant(config.tenant_id.clone());
        // Initialise the materialise schema up front so read-only `snapshot_meta`
        // queries never hit a missing `audience_snapshot` table before the first
        // materialise (the writer creates it lazily too, but a startup init keeps
        // reads clean).
        writer.ensure_audience_snapshot_table()?;
        // Initialise the Feature Store + semantic catalog tables at startup so a
        // producer run / profile never races a lazy DDL (D9 / spec 13).
        writer.ensure_feature_store_table()?;
        writer.ensure_semantic_catalog_table()?;
        // The suppression table must exist before any Exclude query (the write
        // path creates it lazily too, but a startup init keeps Exclude reads
        // clean on a fresh engine).
        writer.ensure_suppression_table()?;
        let attach_sql = storage::read_only_attach_sql(&config.catalog_path, &config.data_path);
        let limits = ReaderLimits {
            memory_limit: config.guardrails.memory_limit.clone(),
            threads: config.guardrails.threads,
        };
        // Read pool (specs/11 §2a): one read-only worker per guardrail thread,
        // refreshed on the writer's generation bump (P1-1, issue #20) with the
        // cadence as a warm-connection backstop.
        let workers = config.guardrails.threads.max(1);
        let mut conns = Vec::with_capacity(workers);
        for _ in 0..workers {
            conns.push(storage::open_reader(
                &config.catalog_path,
                &config.data_path,
            )?);
        }
        let reader = Reader::start_pooled(
            conns,
            attach_sql,
            limits,
            Some(Arc::clone(&write_gen)),
            Duration::from_secs(config.read_refresh_interval_secs),
        )?;

        // Graded per-source freshness registry (D5).
        let freshness = Arc::new(FreshnessRegistry::new());

        // Semantic layer: real HTTP clients when `llm` is configured (spec 13
        // §4), otherwise the deterministic stubs (M3 default, no network).
        let (embed, llm): (
            Arc<dyn consumer_engine_semantic::EmbeddingModel>,
            Arc<dyn consumer_engine_semantic::LlmClient>,
        ) = match &config.llm {
            Some(cfg) => {
                #[cfg(feature = "semantic-llm")]
                {
                    (
                        Arc::new(consumer_engine_semantic::HttpEmbedding::new(cfg)),
                        Arc::new(consumer_engine_semantic::HttpLlm::new(cfg)),
                    )
                }
                #[cfg(not(feature = "semantic-llm"))]
                {
                    let _ = cfg;
                    tracing::warn!(
                        "llm configured but the `semantic-llm` feature is off; using stubs"
                    );
                    (Arc::new(StubEmbed::default()), Arc::new(StubLlm))
                }
            }
            None => (Arc::new(StubEmbed::default()), Arc::new(StubLlm)),
        };
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

        let ingestion = IngestionHandle::start_with(
            writer,
            Arc::clone(&registry),
            MicroBatchConfig {
                flush_rows: config.micro_batch_flush_rows,
                flush_age_secs: config.micro_batch_flush_age_secs,
            },
        )?;
        // M2 signing key: 32 bytes of OS randomness (AGENTS.md § Crypto forbids
        // thread_rng for secrets; getrandom is the OsRng-equivalent source).
        let mut signing_key = [0u8; 32];
        getrandom::fill(&mut signing_key).map_err(|e| Error::Execution(BoxError::from(e)))?;
        let query_engine = consumer_engine_query::QueryEngine::new(
            reader.clone(),
            ingestion.clone(),
            config.guardrails.clone(),
            Arc::clone(&freshness),
            config.suppression.clone(),
        );
        let compaction = tokio::spawn(supervise_compaction(
            ingestion.clone(),
            config.compaction_interval_secs,
            config.compaction.snapshot_retention_days,
        ));

        let jobs = Arc::new(JobRegistry::new());
        // Materialisation concurrency cap: one slot per guardrail thread (the
        // sync query path already bounds in-flight work the same way).
        let materialise_slots = Arc::new(tokio::sync::Semaphore::new(
            config.guardrails.threads.max(1),
        ));
        // Bearer authN: hash the configured tokens once; the middleware
        // compares hashes in constant time and resolves the caller's tenant
        // (AGENTS.md § Crypto, issue #22).
        let auth_token_hash = config
            .auth_token
            .as_deref()
            .map(|t| Arc::new(consumer_engine_ingress::auth::hash_token(t)));
        let tenants: Vec<(String, [u8; 32])> = config
            .tenants
            .iter()
            .map(|t| {
                (
                    t.id.clone(),
                    consumer_engine_ingress::auth::hash_token(&t.token),
                )
            })
            .collect();
        let sql_approval_hash = config
            .sql_approval_token
            .as_deref()
            .map(|t| Arc::new(consumer_engine_ingress::auth::hash_token(t)));
        let state = AppState {
            ingestion: ingestion.clone(),
            query_engine,
            freshness: Arc::clone(&freshness),
            profiler,
            intent_rag,
            embed,
            jobs: Arc::clone(&jobs),
            materialise_slots,
            signing_key: Arc::new(signing_key),
            auth_token_hash,
            tenants,
            default_tenant: config.tenant_id.clone(),
            sql_approval_hash,
        };
        let router = router(state);
        // CDC pumps (issue #24): one SUPERVISOR task per configured topic that
        // owns the adapter and recreates it on ANY pump exit — the pump retries
        // transient errors internally with backoff, so the supervisor only acts
        // on a hard panic or a clean exit. The Kafka transport is behind
        // `ingestion-cdc`; without the feature the config is ignored with a
        // warning.
        if let Some(cdc) = &config.cdc {
            #[cfg(feature = "ingestion-cdc")]
            {
                for topic in &cdc.topics {
                    let handle = ingestion.clone();
                    let freshness = Arc::clone(&freshness);
                    let tenant = config.tenant_id.clone();
                    let brokers = cdc.brokers.clone();
                    let group = cdc.group_id.clone();
                    let topic_cfg = topic.clone();
                    tokio::spawn(async move {
                        loop {
                            let adapter = match consumer_engine_ingestion::KafkaCdcAdapter::new(
                                &brokers,
                                &group,
                                &topic_cfg.topic,
                                &topic_cfg.system,
                                &topic_cfg.entity,
                                topic_cfg.columns.clone(),
                                &topic_cfg.key,
                            ) {
                                Ok(a) => a,
                                Err(e) => {
                                    tracing::error!(error = %e, "CDC adapter init failed; retrying");
                                    tokio::time::sleep(Duration::from_secs(5)).await;
                                    continue;
                                }
                            };
                            let handle = handle.clone();
                            let freshness = Arc::clone(&freshness);
                            let tenant = tenant.clone();
                            let poll = Duration::from_secs(1);
                            // Run the pump in its own task so a PANIC inside it
                            // does not kill the supervisor loop — the pump task
                            // ends and the supervisor recreates the adapter.
                            let source = adapter.source_id().to_string();
                            let pump = tokio::spawn({
                                let mut adapter_ref = adapter;
                                async move {
                                    let _ = consumer_engine_ingestion::run_cdc_pump(
                                        &mut adapter_ref,
                                        &handle,
                                        &freshness,
                                        &tenant,
                                        poll,
                                    )
                                    .await;
                                }
                            });
                            if pump.await.is_err() {
                                tracing::error!(
                                    source = %source,
                                    "CDC pump panicked; recreating adapter"
                                );
                            } else {
                                tracing::warn!(
                                    source = %source,
                                    "CDC pump exited; recreating adapter"
                                );
                            }
                            tokio::time::sleep(Duration::from_secs(5)).await;
                        }
                    });
                }
            }
            #[cfg(not(feature = "ingestion-cdc"))]
            {
                let _ = cdc;
                tracing::warn!("cdc configured but the `ingestion-cdc` feature is off; ignoring");
            }
        }
        let engine = Engine {
            ingestion,
            reader,
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
async fn supervise_compaction(ingestion: IngestionHandle, interval_secs: u64, retention_days: u64) {
    let mut set = tokio::task::JoinSet::new();
    set.spawn(compaction_loop(
        ingestion.clone(),
        interval_secs,
        retention_days,
    ));
    while let Some(res) = set.join_next().await {
        if let Err(e) = res {
            tracing::error!(error = %e, "compaction task panicked; respawning");
            set.spawn(compaction_loop(
                ingestion.clone(),
                interval_secs,
                retention_days,
            ));
        }
    }
}

/// Periodic best-effort compaction sweep over all ingested tables.
async fn compaction_loop(ingestion: IngestionHandle, interval_secs: u64, retention_days: u64) {
    if interval_secs == 0 {
        return;
    }
    let mut ticker = tokio::time::interval(Duration::from_secs(interval_secs));
    // The first tick fires immediately; skip it so the first real sweep waits
    // a full interval.
    ticker.tick().await;
    loop {
        ticker.tick().await;
        if let Err(e) = ingestion.compact_all(retention_days).await {
            warn!(error = %e, "compaction sweep failed");
        }
    }
}
