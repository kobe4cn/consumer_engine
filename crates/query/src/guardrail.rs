//! Query guardrails.
//!
//! M1's hard DoS defenses are runtime guards enforced in
//! [`crate::engine::QueryEngine::run_sync`]: a per-query `tokio::time::timeout`,
//! an output row cap at fetch, an in-flight `Semaphore`, and DuckDB
//! `memory_limit`/`threads` PRAGMAs set on the reader. [`enforce`] is the
//! deterministic check over a cost [`Estimate`] (populated by a future
//! EXPLAIN-based pre-flight; M1 estimates are unknown, so `enforce` passes and
//! the runtime guards do the work — see `specs/93` for the EXPLAIN follow-up).

use consumer_engine_core::GuardrailConfig;

use crate::error::{QueryError, Result};

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
