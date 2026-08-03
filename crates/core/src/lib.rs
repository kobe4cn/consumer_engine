//! Consumer engine core: shared domain types, the error model, and configuration.
//!
//! This crate is the dependency root of the workspace. It deliberately depends
//! on no engine-internal crate and no storage driver (such as `duckdb`), so that
//! every other crate can surface failures through a single [`Error`] type
//! without pulling heavyweight dependencies into the type root.

#![forbid(unsafe_code)]
#![warn(rust_2024_compatibility, missing_docs, missing_debug_implementations)]

pub mod config;
mod error;
pub mod freshness;
pub mod ident;

pub use config::{EngineConfig, GuardrailConfig};
pub use error::{Error, Result};
pub use freshness::Freshness;
pub use ident::validate_ident;

/// Boxed, source-preserving error carrying crate, used to embed an upstream
/// failure (e.g. a `duckdb` error) inside [`Error`] without `core` depending on
/// the originating crate.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;
