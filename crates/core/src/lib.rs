//! Consumer engine core: shared domain types, the error model, and configuration.
//!
//! This crate is the dependency root of the workspace. It deliberately depends
//! on no engine-internal crate and no storage driver (such as `duckdb`), so that
//! every other crate can surface failures through a single [`Error`] type
//! without pulling heavyweight dependencies into the type root.

#![forbid(unsafe_code)]
#![warn(rust_2024_compatibility, missing_docs, missing_debug_implementations)]

pub mod catalog;
pub mod config;
mod dataset;
mod error;
mod feature;
pub mod freshness;
pub mod ident;
pub mod semantic;
pub mod snapshot;
pub mod suppression;

pub use catalog::{READ_ONLY_CATALOG_ALIAS, WRITE_CATALOG_ALIAS};
pub use config::{
    CompactionConfig, EngineConfig, FrequencyCap, GuardrailConfig, LlmConfig, SuppressionRules,
};
pub use dataset::Dataset;
pub use error::{Error, Result};
pub use feature::{FeatureRow, split_feature_name};
pub use freshness::{Freshness, FreshnessRegistry, SourceMeta, SourceType};
pub use ident::{validate_feature_name, validate_ident};
pub use semantic::{CatalogHit, CatalogRow, SemanticType};
pub use snapshot::SnapshotSpec;
pub use suppression::{SuppressionAction, SuppressionChannel, SuppressionRow};

/// Boxed, source-preserving error carrying crate, used to embed an upstream
/// failure (e.g. a `duckdb` error) inside [`Error`] without `core` depending on
/// the originating crate.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;
