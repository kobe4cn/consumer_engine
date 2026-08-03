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

/// A best-effort cost estimate. Unknown fields are `0` (no signal).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Estimate {
    /// Estimated output rows.
    pub est_rows: u64,
    /// Estimated bytes scanned.
    pub est_bytes: u64,
    /// Estimated peak memory in bytes.
    pub est_memory: u64,
}

impl Estimate {
    /// An estimate with no signal (M1 default — EXPLAIN parsing is deferred).
    #[must_use]
    pub const fn unknown() -> Self {
        Self {
            est_rows: 0,
            est_bytes: 0,
            est_memory: 0,
        }
    }
}

/// Deterministically enforce the cost budgets against an [`Estimate`].
///
/// # Errors
/// [`QueryError::Guardrail`] if any budget is exceeded.
pub fn enforce(est: &Estimate, cfg: &GuardrailConfig) -> Result<()> {
    if est.est_rows > cfg.max_output_rows {
        return Err(QueryError::Guardrail {
            rule: "max_output_rows".into(),
            limit: cfg.max_output_rows.to_string(),
        });
    }
    if est.est_bytes > cfg.max_bytes_scanned {
        return Err(QueryError::Guardrail {
            rule: "max_bytes_scanned".into(),
            limit: cfg.max_bytes_scanned.to_string(),
        });
    }
    if est.est_memory > 0 && est.est_memory > bytes_of(&cfg.memory_limit) {
        return Err(QueryError::Guardrail {
            rule: "memory_limit".into(),
            limit: cfg.memory_limit.clone(),
        });
    }
    Ok(())
}

/// Best-effort parse of a memory-limit string like `"8GB"` into bytes. Returns
/// `u64::MAX` when unparseable so an unknown estimate never trips the check.
fn bytes_of(s: &str) -> u64 {
    let s = s.trim();
    let (num, unit) = s.split_at(s.find(|c: char| c.is_alphabetic()).unwrap_or(s.len()));
    let n: u64 = num.parse().unwrap_or(0);
    let factor = match unit.to_ascii_uppercase().as_str() {
        "" | "B" => 1,
        "KB" => 1024,
        "MB" => 1024 * 1024,
        "GB" => 1024 * 1024 * 1024,
        "TB" => 1024 * 1024 * 1024 * 1024,
        _ => return u64::MAX,
    };
    n.saturating_mul(factor)
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
/// EXPLAIN/parse failure degrades to [`Estimate::unknown`] so the runtime
/// guards still apply. EXPLAIN only plans (never executes), so it is not
/// itself wrapped in a timeout.
#[must_use]
pub async fn explain_cost(reader: &Reader, compiled: &CompiledQuery) -> Estimate {
    let sql = format!("EXPLAIN (FORMAT JSON) {}", compiled.sql);
    let qr = match reader
        .query_with_params(&sql, compiled.params.clone())
        .await
    {
        Ok(qr) => qr,
        Err(_) => return Estimate::unknown(),
    };
    match max_cardinality(&qr) {
        Some(rows) => Estimate {
            est_rows: rows,
            ..Estimate::unknown()
        },
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
    fn test_should_reject_query_over_memory_limit() {
        let cfg = GuardrailConfig {
            memory_limit: "1GB".into(),
            ..GuardrailConfig::default()
        };
        let est = Estimate {
            est_memory: 2 * 1024 * 1024 * 1024, // 2GB > 1GB
            ..Estimate::unknown()
        };
        let res = enforce(&est, &cfg);
        assert!(matches!(res, Err(QueryError::Guardrail { rule, .. }) if rule == "memory_limit"));
    }

    #[test]
    fn test_should_reject_query_over_scan_budget() {
        let cfg = GuardrailConfig {
            max_bytes_scanned: 1024,
            ..GuardrailConfig::default()
        };
        let est = Estimate {
            est_bytes: 4096,
            ..Estimate::unknown()
        };
        let res = enforce(&est, &cfg);
        assert!(
            matches!(res, Err(QueryError::Guardrail { rule, .. }) if rule == "max_bytes_scanned")
        );
    }

    #[test]
    fn test_should_allow_unknown_estimate() {
        // M1 default: unknown estimate must pass (runtime guards do the work).
        let res = enforce(&Estimate::unknown(), &GuardrailConfig::default());
        assert!(res.is_ok());
    }
}
