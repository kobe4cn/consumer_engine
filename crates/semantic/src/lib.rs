//! Semantic layer: L0 Profiler (onboarding) + L1 Intent RAG.
//!
//! Solves "how does the agent know what tables/columns exist and what they
//! mean" ([`specs/13-semantic-layer.md`](../../specs/13-semantic-layer.md)).
//! - [`Profiler`] runs once per source onboarding (D4): samples a `raw_*` table, classifies
//!   columns, redacts PII, and builds `CatalogRow`s for the agent.
//! - [`IntentRag`] runs at query time: embeds the operator utterance and retrieves a bounded
//!   candidate set of tables/columns.
//!
//! M3 ships deterministic stub LLM/embedding clients (no network) so onboarding
//! is fast and tests are reproducible; a real HTTP client is gated behind a
//! future `semantic-llm` feature (spec 13 §4).

#![forbid(unsafe_code)]
#![warn(rust_2024_compatibility, missing_docs, missing_debug_implementations)]

pub mod intent_rag;
pub mod llm;
pub mod profiler;

// Real HTTP LLM/embedding clients behind the same trait seams (spec 13 §4),
// compiled only with the `semantic-llm` feature.
#[cfg(feature = "semantic-llm")]
pub mod http;

#[cfg(feature = "semantic-llm")]
pub use http::{HttpEmbedding, HttpLlm};
pub use intent_rag::IntentRag;
pub use llm::{EmbeddingModel, LlmClient, StubEmbed, StubLlm};
pub use profiler::Profiler;

/// Bounds enforced by the Profiler / IntentRag (spec 13 §3 I2/I3).
///
/// Re-used across modules to keep the limits in one place (DRY).
mod limits {
    /// Default sample-row cap (I2: bounded sampling).
    pub const DEFAULT_SAMPLE_ROWS: usize = 1000;
    /// Default per-value byte cap (I2: bounded sampling).
    pub const DEFAULT_SAMPLE_VALUE_BYTES: usize = 64;
    /// Maximum sample values retained per column.
    pub const MAX_SAMPLE_VALUES: usize = 20;
    /// Default retrieval cap (I3: bounded retrieval).
    pub const DEFAULT_K: usize = 20;
    /// Default stub-embedding dimension (deterministic, no network).
    pub const DEFAULT_EMBED_DIM: usize = 64;
}
