//! LLM + embedding client traits and M3 deterministic stubs.
//!
//! Both traits need `dyn` dispatch (the server wires concrete impls behind
//! `Arc<dyn …>` so future HTTP clients slot in without touching callers), so per
//! AGENTS.md § Async they use `async_trait` rather than native `async fn in
//! trait` (which is not object-safe). [`EmbeddingModel`] has no async methods,
//! so it is a plain trait usable as `dyn`. M3 ships stubs with no network
//! dependence (spec 13 §4: degrade gracefully; G5 satisfied by heuristic stubs).

use async_trait::async_trait;
use consumer_engine_core::Result;

/// A client that generates a human-editable description for a column from its
/// name + a bounded sample. Failures degrade to a stub description (spec 13 §4).
#[async_trait]
pub trait LlmClient: Send + Sync {
    /// Describe one column: `system.table.column` with `data_type` and a bounded
    /// sample of non-PII values.
    ///
    /// # Errors
    /// Implementations surface transient failures via [`consumer_engine_core::Error`].
    async fn describe_column(
        &self,
        system: &str,
        table: &str,
        column: &str,
        data_type: &str,
        sample: &[String],
    ) -> Result<String>;
}

/// An embedding model: maps text to a fixed-dimension vector. Async because a
/// real HTTP service is behind it (spec 13 §4); failures surface as an error
/// so the caller can decide (Profiler degrades, IntentRag propagates
/// `CatalogueUnavailable`). `dyn`-dispatched, hence `async_trait`.
#[async_trait]
pub trait EmbeddingModel: Send + Sync {
    /// The fixed dimension of every emitted vector.
    fn dim(&self) -> usize;
    /// Embed `text` as a vector of length [`Self::dim`].
    ///
    /// # Errors
    /// [`consumer_engine_core::Error`] when the embedding service is unreachable.
    async fn embed(&self, text: &str) -> consumer_engine_core::Result<Vec<f32>>;
}

/// A heuristic, deterministic LLM stub (no network). Produces a short
/// description from the column identity + type. Good enough for G5
/// (auto-generated descriptions) and fully reproducible in tests.
#[derive(Debug, Default, Clone, Copy)]
pub struct StubLlm;

#[async_trait]
impl LlmClient for StubLlm {
    async fn describe_column(
        &self,
        system: &str,
        table: &str,
        column: &str,
        data_type: &str,
        sample: &[String],
    ) -> Result<String> {
        let head = sample.first().map(String::as_str).unwrap_or("—");
        Ok(format!(
            "Column '{column}' ({data_type}) of {system}.{table}; e.g. '{head}'."
        ))
    }
}

/// A deterministic embedding stub: a bag-of-bytes hash mapped to a fixed-dim
/// unit vector. Reproducible (no RNG) so cosine similarities are stable across
/// runs; texts with similar byte distributions score similarly.
#[derive(Debug, Clone, Copy)]
pub struct StubEmbed {
    /// The fixed vector dimension.
    dim: usize,
}

impl StubEmbed {
    /// Build a stub embedding model with `dim` dimensions.
    ///
    /// # Panics
    /// Debug-only: panics if `dim == 0` (a zero-dim embedding is meaningless and
    /// is a programmer error at construction, not external input).
    #[must_use]
    pub fn new(dim: usize) -> Self {
        assert!(dim > 0, "embedding dimension must be non-zero");
        Self { dim }
    }
}

impl Default for StubEmbed {
    fn default() -> Self {
        Self::new(crate::limits::DEFAULT_EMBED_DIM)
    }
}

#[async_trait]
impl EmbeddingModel for StubEmbed {
    fn dim(&self) -> usize {
        self.dim
    }

    async fn embed(&self, text: &str) -> consumer_engine_core::Result<Vec<f32>> {
        let mut v = vec![0.0_f32; self.dim];
        // Mix each byte into a dimension via an FNV-1a-style fold so the
        // contribution depends on both the byte and its position (spreading
        // signal across dimensions). The 0x9E37… constant is the golden-ratio
        // multiplier; arithmetic is wrapping so hostile/large inputs never
        // overflow (AGENTS.md § Safety: checked arithmetic on external values).
        for (i, &b) in text.as_bytes().iter().enumerate() {
            let dim_i = i % self.dim;
            let folded = (b as u64)
                .wrapping_mul(0x9E37_79B9_7F4A_7C15)
                .wrapping_add((i as u64).wrapping_mul(0x0000_0100_0000_01B3));
            // Map the folded u64 to a signed value in roughly [-1, 1).
            let mapped = (folded as f64 / (1u64 << 62) as f64) - 1.0;
            if let Some(slot) = v.get_mut(dim_i) {
                *slot += mapped as f32;
            }
        }
        // Normalise to a unit vector so cosine reduces to a dot product.
        let norm: f64 = v
            .iter()
            .map(|x| f64::from(*x) * f64::from(*x))
            .sum::<f64>()
            .sqrt();
        if norm > 0.0 {
            for x in &mut v {
                *x = (f64::from(*x) / norm) as f32;
            }
        }
        Ok(v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_should_embed_to_unit_length() {
        let model = StubEmbed::new(16);
        let v = model.embed("the orders table").await.expect("embed");
        let norm: f64 = v
            .iter()
            .map(|x| f64::from(*x) * f64::from(*x))
            .sum::<f64>()
            .sqrt();
        assert!(
            (norm - 1.0).abs() < 1e-5,
            "embedding must be unit-length: {norm}"
        );
    }

    #[tokio::test]
    async fn test_should_embed_deterministically() {
        let model = StubEmbed::new(32);
        assert_eq!(
            model.embed("periodic buyers").await.expect("embed"),
            model.embed("periodic buyers").await.expect("embed")
        );
    }

    #[tokio::test]
    async fn test_should_stub_describe_column() {
        let s = StubLlm
            .describe_column("erp", "orders", "amount", "VARCHAR", &["12.0".into()])
            .await
            .expect("describe");
        assert!(
            s.contains("amount"),
            "description must name the column: {s}"
        );
        assert!(
            s.contains("erp.orders"),
            "description must name the table: {s}"
        );
    }
}
