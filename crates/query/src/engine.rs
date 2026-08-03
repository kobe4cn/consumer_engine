//! The query engine: parse → compile → guard → run (sync).

use std::{
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use consumer_engine_core::{BoxError, Error, Freshness, GuardrailConfig};
use consumer_engine_execution::{QueryResult, Reader, RowCells};
use tokio::sync::Semaphore;

use crate::{
    ast::SegmentQuery,
    compiler::{CompiledQuery, compile},
    error::{QueryError, Result},
    guardrail::{Mode, Plan, enforce, explain_cost},
};

/// The query engine. Cheap to share via `Arc`.
#[derive(Clone)]
pub struct QueryEngine {
    reader: Reader,
    guardrails: GuardrailConfig,
    inflight: Arc<Semaphore>,
    last_ingest_epoch: Arc<AtomicI64>,
}

impl std::fmt::Debug for QueryEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryEngine")
            .field("guardrails", &self.guardrails)
            .finish_non_exhaustive()
    }
}

/// The result of a synchronous query.
#[derive(Debug, Clone, PartialEq)]
pub struct SyncResult {
    /// Column names in order.
    pub columns: Vec<String>,
    /// Rows, each a vector of JSON cells.
    pub rows: Vec<RowCells>,
    /// Number of rows returned.
    pub count: u64,
    /// Freshness label (graded per source; M1 = batch only).
    pub freshness: Freshness,
    /// A unique id for this query.
    pub query_id: String,
}

impl QueryEngine {
    /// Construct a query engine over `reader` with `guardrails`. The
    /// `last_ingest_epoch` clock drives the freshness label.
    #[must_use]
    pub fn new(
        reader: Reader,
        guardrails: GuardrailConfig,
        last_ingest_epoch: Arc<AtomicI64>,
    ) -> Self {
        let permits = guardrails.threads.max(1);
        Self {
            reader,
            guardrails,
            inflight: Arc::new(Semaphore::new(permits)),
            last_ingest_epoch,
        }
    }

    /// Run a DSL JSON value end-to-end: parse/validate → compile → guard → run.
    ///
    /// # Errors
    /// Propagates parse, guardrail, and execution errors.
    pub async fn run(&self, dsl: serde_json::Value) -> Result<SyncResult> {
        let q = crate::parse::parse(dsl)?;
        self.run_sync(&q).await
    }

    /// Compile + best-effort EXPLAIN estimate a plan, enforcing row budgets
    /// **before execution** (AC#3). M1 promotes to [`Mode::Async`] when the
    /// estimated rows exceed `sync_row_cap` (then `run_sync` rejects it).
    ///
    /// # Errors
    /// Propagates compile/guardrail errors.
    pub async fn plan(&self, q: &SegmentQuery) -> Result<Plan> {
        let (plan, _compiled) = self.prepare(q).await?;
        Ok(plan)
    }

    /// Compile, run the EXPLAIN pre-flight, enforce budgets, and choose a mode.
    /// Returns the [`Plan`] and the [`CompiledQuery`] (for execution).
    ///
    /// # Errors
    /// Propagates compile/guardrail errors.
    async fn prepare(&self, q: &SegmentQuery) -> Result<(Plan, CompiledQuery)> {
        let compiled = compile(q)?;
        let est = explain_cost(&self.reader, &compiled).await;
        enforce(&est, &self.guardrails)?;
        let mode = if est.est_rows > self.guardrails.sync_row_cap {
            Mode::Async
        } else {
            Mode::Sync
        };
        Ok((
            Plan {
                mode,
                est,
                sources: compiled.sources.clone(),
            },
            compiled,
        ))
    }

    /// Run a parsed segment query synchronously under all guardrails.
    ///
    /// # Errors
    /// [`QueryError::Guardrail`] on timeout or output-row cap;
    /// [`QueryError::TooLarge`] if the plan is async (not in M1);
    /// [`QueryError::Execution`] on reader failure.
    pub async fn run_sync(&self, q: &SegmentQuery) -> Result<SyncResult> {
        // Concurrency cap (acquired before the EXPLAIN pre-flight + execute).
        let _permit = self
            .inflight
            .acquire()
            .await
            .map_err(|e| QueryError::Execution {
                source: Error::Execution(BoxError::from(e)),
            })?;

        // Pre-flight: compile + EXPLAIN + enforce budgets (AC#3: over-budget is
        // rejected here, before the query executes).
        let (plan, compiled) = self.prepare(q).await?;
        if plan.mode == Mode::Async {
            return Err(QueryError::TooLarge);
        }

        // Statement timeout.
        let timeout = Duration::from_secs(self.guardrails.statement_timeout_secs);
        let QueryResult { columns, rows, .. } = tokio::time::timeout(
            timeout,
            self.reader
                .query_with_params(&compiled.sql, compiled.params),
        )
        .await
        .map_err(|_| QueryError::Guardrail {
            rule: "statement_timeout".into(),
            limit: format!("{}s", self.guardrails.statement_timeout_secs),
        })??;

        // Output row cap.
        if rows.len() as u64 > self.guardrails.max_output_rows {
            return Err(QueryError::Guardrail {
                rule: "max_output_rows".into(),
                limit: self.guardrails.max_output_rows.to_string(),
            });
        }

        let lag = now_epoch() - self.last_ingest_epoch.load(Ordering::Relaxed);
        Ok(SyncResult {
            columns,
            count: rows.len() as u64,
            rows,
            freshness: Freshness::batch(lag),
            query_id: format!("q_{}", uuid::Uuid::now_v7()),
        })
    }
}

/// Current epoch seconds (0 if the clock is before the epoch).
fn now_epoch() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}
