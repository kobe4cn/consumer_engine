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
    compiler::{CompiledQuery, compile},
    error::{QueryError, Result},
    guardrail::{Estimate, enforce},
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

    /// Compile + best-effort estimate a plan (M1 always returns mode `Sync`;
    /// EXPLAIN-based async promotion is deferred — see `specs/93`).
    ///
    /// # Errors
    /// Propagates compile/guardrail errors.
    pub async fn plan(&self, q: &crate::ast::SegmentQuery) -> Result<CompiledQuery> {
        let compiled = compile(q)?;
        // M1: no EXPLAIN estimate — unknown, so `enforce` passes; runtime guards
        // (timeout, fetch cap) do the real work.
        enforce(&Estimate::unknown(), &self.guardrails)?;
        Ok(compiled)
    }

    /// Run a parsed segment query synchronously under all guardrails.
    ///
    /// # Errors
    /// [`QueryError::Guardrail`] on timeout or output-row cap;
    /// [`QueryError::TooLarge`] if the plan is async (not in M1);
    /// [`QueryError::Execution`] on reader failure.
    pub async fn run_sync(&self, q: &crate::ast::SegmentQuery) -> Result<SyncResult> {
        let compiled = self.plan(q).await?;

        // Concurrency cap.
        let _permit = self
            .inflight
            .acquire()
            .await
            .map_err(|e| QueryError::Execution {
                source: Error::Execution(BoxError::from(e)),
            })?;

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
