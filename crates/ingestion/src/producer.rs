//! Feature Store producer contract + registry (decision D9).
//!
//! A [`FeatureProducer`] computes scalar features at a point in time
//! (`run(as_of)`, point-in-time correct per spec 20 I3) and emits
//! [`FeatureRow`]s in entity-attribute-value form. The ingestion writer persists
//! them and refreshes the wide pivot view per family.
//!
//! `FeatureProducer` is used through `dyn` dispatch in [`ProducerRegistry`], so
//! per AGENTS.md § Async it uses [`async_trait`] (native `async fn in trait` is
//! not object-safe). The producer reads via its own [`Reader`] on the caller's
//! async task — the single writer thread never blocks on async.

use std::sync::Arc;

use async_trait::async_trait;
use consumer_engine_core::{Error, FeatureRow, Result, validate_feature_name};
use dashmap::DashMap;

/// The output of a producer run: feature rows in EAV form (spec 20 §2).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ProducerOutput {
    /// The computed feature rows.
    pub rows: Vec<FeatureRow>,
}

/// A Feature Store producer (D9): SQL (M3) or ML (phase 2) behind one contract.
#[async_trait]
pub trait FeatureProducer: Send + Sync {
    /// The producer's registry id (validated as a feature name on register).
    fn id(&self) -> &str;
    /// Compute features correct-as-of `as_of` (an `ISO-8601` UTC string). Only raw
    /// / feature rows with `as_of_ts ≤ as_of` may be read (spec 20 I3).
    ///
    /// # Errors
    /// Propagates read/compute failures via [`Error`].
    async fn run(&self, as_of: &str) -> Result<ProducerOutput>;
}

/// A concurrent registry of producers keyed by id (D9). Cheap to clone (`Arc`
/// under the hood).
#[derive(Clone, Default)]
pub struct ProducerRegistry {
    producers: Arc<DashMap<String, Arc<dyn FeatureProducer>>>,
}

impl std::fmt::Debug for ProducerRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProducerRegistry")
            .field("len", &self.producers.len())
            .finish()
    }
}

impl ProducerRegistry {
    /// Build an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            producers: Arc::new(DashMap::new()),
        }
    }

    /// Register `producer` under its `id()`. The id is validated as a feature
    /// name (boundary allowlist) and must be unique.
    ///
    /// # Errors
    /// - [`Error::InvalidInput`] if the id fails the feature-name allowlist.
    /// - [`Error::InvalidInput`] if the id is already registered.
    pub fn register(&self, producer: Arc<dyn FeatureProducer>) -> Result<()> {
        let id = producer.id().to_string();
        validate_feature_name(&id).map_err(|e| Error::InvalidInput(format!("producer id: {e}")))?;
        if self.producers.contains_key(&id) {
            return Err(Error::InvalidInput(format!(
                "producer {id:?} is already registered"
            )));
        }
        self.producers.insert(id, producer);
        Ok(())
    }

    /// Look up a producer by id.
    #[must_use]
    pub fn get(&self, id: &str) -> Option<Arc<dyn FeatureProducer>> {
        self.producers.get(id).map(|r| Arc::clone(&r))
    }

    /// All registered producer ids (sorted for determinism).
    #[must_use]
    pub fn ids(&self) -> Vec<String> {
        let mut ids: Vec<String> = self.producers.iter().map(|r| r.key().clone()).collect();
        ids.sort();
        ids
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct EchoProducer {
        id: String,
        rows: Vec<FeatureRow>,
    }

    #[async_trait]
    impl FeatureProducer for EchoProducer {
        fn id(&self) -> &str {
            &self.id
        }
        async fn run(&self, _as_of: &str) -> Result<ProducerOutput> {
            Ok(ProducerOutput {
                rows: self.rows.clone(),
            })
        }
    }

    #[test]
    fn test_should_register_and_lookup_producer() {
        let reg = ProducerRegistry::new();
        reg.register(Arc::new(EchoProducer {
            id: "cadence_sql".into(),
            rows: vec![],
        }))
        .expect("register");
        assert!(reg.get("cadence_sql").is_some());
        assert!(reg.get("nope").is_none());
        assert_eq!(reg.ids(), vec!["cadence_sql".to_string()]);
    }

    #[test]
    fn test_should_reject_duplicate_producer() {
        let reg = ProducerRegistry::new();
        reg.register(Arc::new(EchoProducer {
            id: "cadence_sql".into(),
            rows: vec![],
        }))
        .expect("register");
        assert!(
            reg.register(Arc::new(EchoProducer {
                id: "cadence_sql".into(),
                rows: vec![],
            }))
            .is_err()
        );
    }
}
