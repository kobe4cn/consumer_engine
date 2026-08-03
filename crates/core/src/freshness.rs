//! Data freshness labelling.
//!
//! Per decision D5, freshness is graded per source and surfaced on every query
//! result so operators are never silently misled by a stale source. The
//! [`FreshnessRegistry`] records each source's type (`batch`/`cdc`) and the
//! epoch at which it was last ingested; [`FreshnessRegistry::worst`] computes
//! the worst (maximum-lag) source touched by a query.

use std::collections::HashSet;

use dashmap::DashMap;
use serde::{Deserialize, Serialize};

use crate::{Dataset, Result, validate_ident};

/// The kind of source adapter feeding a dataset (D5: freshness is graded).
///
/// Serialises as lowercase (`"batch"`/`"cdc"`) to match the established
/// `worstSource: "batch"` wire contract.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SourceType {
    /// A batch source (e.g. a daily file/pull). Larger, more variable lag.
    #[default]
    Batch,
    /// A change-data-capture source (e.g. Debezium/Kafka). Smaller lag.
    Cdc,
}

impl SourceType {
    /// The wire label for this source type (`"batch"` / `"cdc"`).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Batch => "batch",
            Self::Cdc => "cdc",
        }
    }
}

impl std::fmt::Display for SourceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Metadata recorded per source under the key `"{system}.{entity}"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceMeta {
    /// The source adapter kind (batch or cdc).
    pub source_type: SourceType,
    /// Wall-clock epoch seconds of the source's last successful ingest.
    pub last_epoch_secs: i64,
}

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

/// A concurrent registry of per-source freshness metadata (D5). Keyed by
/// `"{system}.{entity}"` (both validated on insert). Cheap to clone via the
/// interior `DashMap` (`Arc` under the hood).
#[derive(Clone)]
pub struct FreshnessRegistry {
    sources: DashMap<String, SourceMeta>,
}

impl std::fmt::Debug for FreshnessRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FreshnessRegistry")
            .field("len", &self.sources.len())
            .finish()
    }
}

impl Default for FreshnessRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl FreshnessRegistry {
    /// Build an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            sources: DashMap::new(),
        }
    }

    /// Record (or update) the freshness metadata for `system`.`entity`.
    ///
    /// # Errors
    /// [`crate::Error::InvalidInput`] if `system`/`entity` are bad identifiers.
    pub fn set(
        &self,
        system: &str,
        entity: &str,
        source_type: SourceType,
        epoch: i64,
    ) -> Result<()> {
        validate_ident(system)?;
        validate_ident(entity)?;
        let key = format!("{system}.{entity}");
        self.sources.insert(
            key,
            SourceMeta {
                source_type,
                last_epoch_secs: epoch,
            },
        );
        Ok(())
    }

    /// Read the freshness metadata for `system`.`entity`, if present.
    #[must_use]
    pub fn get(&self, system: &str, entity: &str) -> Option<SourceMeta> {
        let key = format!("{system}.{entity}");
        self.sources.get(&key).map(|r| *r)
    }

    /// Compute the worst (maximum-lag) freshness over the distinct sources a
    /// query touched.
    ///
    /// For each distinct source, lag = `now_epoch − last_epoch_secs`. Unknown
    /// sources default to `Batch` with lag `0` (no signal). The result reports
    /// the max-lag source's type as `worst_source`. If `sources` is empty, the
    /// result is `Batch` with lag `0` (the historical M1 default).
    #[must_use]
    pub fn worst<'a>(
        &'a self,
        sources: impl IntoIterator<Item = &'a Dataset>,
        now_epoch: i64,
    ) -> Freshness {
        let mut seen: HashSet<(&str, &str)> = HashSet::new();
        let mut worst_type = SourceType::Batch;
        let mut worst_lag: i64 = 0;
        for d in sources {
            if !seen.insert((&d.system, &d.entity)) {
                continue;
            }
            let meta = self.get(&d.system, &d.entity).unwrap_or(SourceMeta {
                source_type: SourceType::Batch,
                last_epoch_secs: now_epoch,
            });
            let lag = (now_epoch - meta.last_epoch_secs).max(0);
            // The worst source is the one with the largest lag; ties resolve to
            // the type first seen (Batch prefers itself — Batch is the worse
            // posture by default).
            if lag > worst_lag
                || (lag == worst_lag
                    && meta.source_type == SourceType::Batch
                    && worst_type != SourceType::Batch)
            {
                worst_lag = lag;
                worst_type = meta.source_type;
            }
        }
        Freshness {
            worst_source: worst_type.to_string(),
            lag_seconds: worst_lag,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_should_report_batch_as_default_for_empty_sources() {
        let reg = FreshnessRegistry::new();
        let f = reg.worst([], 1_000);
        assert_eq!(f.worst_source, "batch");
        assert_eq!(f.lag_seconds, 0);
    }

    #[test]
    fn test_should_grade_worst_source_by_lag() {
        let reg = FreshnessRegistry::new();
        reg.set("erp", "orders", SourceType::Batch, 500)
            .expect("set");
        reg.set("erp", "events", SourceType::Cdc, 990).expect("set");
        let sources = vec![
            Dataset {
                system: "erp".into(),
                entity: "orders".into(),
            },
            Dataset {
                system: "erp".into(),
                entity: "events".into(),
            },
        ];
        let f = reg.worst(&sources, 1_000);
        assert_eq!(f.worst_source, "batch", "batch (lag 500) is worst");
        assert_eq!(f.lag_seconds, 500);
    }

    #[test]
    fn test_should_unknown_source_defaults_to_batch_zero_lag() {
        let reg = FreshnessRegistry::new();
        let sources = vec![Dataset {
            system: "erp".into(),
            entity: "orders".into(),
        }];
        let f = reg.worst(&sources, 1_000);
        assert_eq!(f.worst_source, "batch");
        assert_eq!(f.lag_seconds, 0);
    }

    #[test]
    fn test_should_reject_invalid_ident_on_set() {
        let reg = FreshnessRegistry::new();
        assert!(
            reg.set("erp; DROP", "orders", SourceType::Batch, 1)
                .is_err()
        );
    }

    #[test]
    fn test_should_dedupe_repeated_sources() {
        let reg = FreshnessRegistry::new();
        reg.set("erp", "orders", SourceType::Batch, 100)
            .expect("set");
        let sources = vec![
            Dataset {
                system: "erp".into(),
                entity: "orders".into(),
            },
            Dataset {
                system: "erp".into(),
                entity: "orders".into(),
            },
        ];
        let f = reg.worst(&sources, 1_000);
        assert_eq!(f.lag_seconds, 900);
    }

    #[test]
    fn test_source_type_serialises_lowercase() {
        let json = serde_json::to_string(&SourceType::Cdc).expect("ser");
        assert_eq!(json, "\"cdc\"");
        let b: SourceType = serde_json::from_str("\"batch\"").expect("de");
        assert_eq!(b, SourceType::Batch);
    }
}
