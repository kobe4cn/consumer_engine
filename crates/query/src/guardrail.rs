//! Query guardrails.
//!
//! Two layers: (1) [`explain_cost`] runs `EXPLAIN (FORMAT JSON)` pre-flight
//! and [`enforce`] rejects row-budget overruns **before execution** (AC#3);
//! (2) runtime guards in [`crate::engine::QueryEngine::run_sync`] — a
//! `tokio::time::timeout`, an output-row fetch cap, an in-flight `Semaphore`,
//! and DuckDB `memory_limit`/`threads` PRAGMAs. EXPLAIN does not expose
//! bytes-scanned or memory, so those budgets remain runtime-only (DuckDB
//! limitation; see `specs/93`).

use consumer_engine_core::GuardrailConfig;
use consumer_engine_execution::{QueryResult, Reader};

use crate::{
    ast::Dataset,
    compiler::CompiledQuery,
    error::{QueryError, Result},
};

/// A best-effort cost estimate. DuckDB's EXPLAIN exposes only an estimated
/// output-cardinality (`est_rows`); bytes-scanned and peak memory are not
/// exposed (specs/12 §4 limitation), so those budgets are bounded at runtime by
/// the `memory_limit` PRAGMA + statement timeout, not pre-flight.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Estimate {
    /// Estimated output rows.
    pub est_rows: u64,
}

impl Estimate {
    /// An estimate with no signal (EXPLAIN failed to produce a cardinality).
    #[must_use]
    pub const fn unknown() -> Self {
        Self { est_rows: 0 }
    }
}

/// Deterministically enforce the row budget against an [`Estimate`].
///
/// # Errors
/// [`QueryError::Guardrail`] if the estimated rows exceed `max_output_rows`.
pub fn enforce(est: &Estimate, cfg: &GuardrailConfig) -> Result<()> {
    if est.est_rows > cfg.max_output_rows {
        return Err(QueryError::Guardrail {
            rule: "max_output_rows".into(),
            limit: cfg.max_output_rows.to_string(),
        });
    }
    Ok(())
}

/// Execution mode chosen by [`QueryEngine::plan`](crate::engine::QueryEngine::plan).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Synchronous execution.
    Sync,
    /// Too large for sync — `run_sync` rejects it as [`QueryError::TooLarge`].
    Async,
}

/// A compiled-and-costed plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Plan {
    /// Chosen execution mode.
    pub mode: Mode,
    /// Best-effort cost estimate (rows from EXPLAIN; bytes/memory unknown).
    pub est: Estimate,
    /// Source datasets the query touches.
    pub sources: Vec<Dataset>,
}

/// Best-effort EXPLAIN-based cost estimate (AC#3 pre-flight).
///
/// Runs `EXPLAIN (FORMAT JSON) <sql>` and parses the maximum `Estimated
/// Cardinality` across plan nodes into `est_rows`. **Bytes-scanned and memory
/// are not exposed by EXPLAIN** (DuckDB limitation), so those stay `0` and are
/// bounded at runtime by the `memory_limit` PRAGMA + statement timeout. Any
/// EXPLAIN/parse/timeout failure degrades to [`Estimate::unknown`] so the
/// runtime guards still apply. The EXPLAIN is wrapped in `timeout_secs` — in
/// DuckDB `EXPLAIN` does execute the query, so an unbounded estimate would be
/// an untimed execution on the shared reader (AGENTS.md § Resource Limits).
#[must_use]
pub async fn explain_cost(
    reader: &Reader,
    compiled: &CompiledQuery,
    timeout_secs: u64,
) -> Estimate {
    let sql = format!("EXPLAIN (FORMAT JSON) {}", compiled.sql);
    let qr = match tokio::time::timeout(
        std::time::Duration::from_secs(timeout_secs),
        reader.query_with_params(&sql, compiled.params.clone()),
    )
    .await
    {
        Ok(Ok(qr)) => qr,
        _ => return Estimate::unknown(),
    };
    match max_cardinality(&qr) {
        Some(rows) => Estimate { est_rows: rows },
        None => Estimate::unknown(),
    }
}

/// Extract the maximum `Estimated Cardinality` from an EXPLAIN JSON result.
/// Returns `None` if no cardinality was found.
fn max_cardinality(qr: &QueryResult) -> Option<u64> {
    // EXPLAIN (FORMAT JSON) yields one row: ["physical_plan", "<json array>"].
    let json_text = qr.rows.first()?.get(1)?.as_str()?;
    let parsed: serde_json::Value = serde_json::from_str(json_text).ok()?;
    let mut max: u64 = 0;
    walk_cardinality(&parsed, &mut max);
    (max > 0).then_some(max)
}

/// Recursively collect the largest `Estimated Cardinality` across plan nodes.
fn walk_cardinality(v: &serde_json::Value, max: &mut u64) {
    if let Some(arr) = v.as_array() {
        for e in arr {
            walk_cardinality(e, max);
        }
        return;
    }
    if let Some(card) = v
        .get("extra_info")
        .and_then(|e| e.get("Estimated Cardinality"))
        .and_then(serde_json::Value::as_str)
        && let Ok(n) = card.parse::<u64>()
        && n > *max
    {
        *max = n;
    }
    if let Some(children) = v.get("children").and_then(serde_json::Value::as_array) {
        for c in children {
            walk_cardinality(c, max);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_should_reject_query_over_max_output_rows() {
        let cfg = GuardrailConfig {
            max_output_rows: 1,
            ..GuardrailConfig::default()
        };
        let est = Estimate { est_rows: 2 };
        let res = enforce(&est, &cfg);
        assert!(
            matches!(res, Err(QueryError::Guardrail { rule, .. }) if rule == "max_output_rows")
        );
    }

    #[test]
    fn test_should_allow_unknown_estimate() {
        // Unknown estimate must pass (runtime guards do the work).
        let res = enforce(&Estimate::unknown(), &GuardrailConfig::default());
        assert!(res.is_ok());
    }
}
