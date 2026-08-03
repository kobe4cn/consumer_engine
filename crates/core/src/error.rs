//! Engine-wide error model.
//!
//! Per `AGENTS.md` § Error Handling, libraries use a `thiserror` enum with
//! `#[source]` chaining and return `Result<T>`. The single [`Error`] type is
//! shared across crates; an upstream failure (e.g. from `duckdb`) is boxed via
//! [`BoxError`](crate::BoxError) so that `core` stays free of storage-driver
//! dependencies.

use crate::BoxError;

/// The single error type surfaced by every engine crate.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// A failure in the storage layer (DuckLake attach, DDL, writes).
    #[error("storage failure: {0}")]
    Storage(#[source] BoxError),

    /// A failure in the read-only execution layer (DuckDB query).
    #[error("execution failure: {0}")]
    Execution(#[source] BoxError),

    /// A failure in the ingestion actor (writer lifecycle, batching).
    #[error("ingestion failure: {0}")]
    Ingestion(#[source] BoxError),

    /// A second writer was attempted against an already-held catalog.
    #[error("writer already held: a single IngestionActor owns the catalog")]
    WriterAlreadyHeld,

    /// Caller-supplied input failed validation at the trust boundary.
    #[error("invalid input: {0}")]
    InvalidInput(String),
}

/// Convenience `Result` alias used throughout the engine.
pub type Result<T> = std::result::Result<T, Error>;
