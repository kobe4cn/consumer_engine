//! Data freshness labelling.
//!
//! Per decision D5, freshness is graded per source and surfaced on every query
//! result so operators are never silently misled by a stale source.

use serde::{Deserialize, Serialize};

/// The freshness of a query result, reported alongside the rows.
///
/// `worst_source` names the least-fresh source the query touched (`"batch"` or
/// `"cdc"`), and `lag_seconds` is the observed lag for that source (seconds
/// since the data was last refreshed).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Freshness {
    /// The least-fresh source touched by the query.
    pub worst_source: String,
    /// Observed lag in seconds for `worst_source`.
    pub lag_seconds: i64,
}

impl Freshness {
    /// Build a batch-source freshness label from the wall-clock seconds elapsed
    /// since the last successful ingest.
    #[must_use]
    pub fn batch(lag_seconds: i64) -> Self {
        Self {
            worst_source: "batch".to_string(),
            lag_seconds: lag_seconds.max(0),
        }
    }
}
