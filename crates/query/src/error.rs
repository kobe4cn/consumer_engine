//! Query-engine error model.
//!
//! Per `specs/12-query-engine.md §4` and AGENTS.md § Error Handling. Distinct
//! from `consumer_engine_core::Error` so the shared enum stays clean (crate
//! map: the query layer owns its own failures).

/// A query-engine failure.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum QueryError {
    /// The DSL failed to parse or failed validation.
    #[error("invalid DSL: {0}")]
    InvalidDsl(String),
    /// A guardrail budget was exceeded.
    #[error("guardrail {rule} exceeded (limit {limit})")]
    Guardrail {
        /// The rule that fired (e.g. `memory_limit`, `statement_timeout`).
        rule: String,
        /// The limit that was breached.
        limit: String,
    },
    /// The query is too large for synchronous execution (rows or cost).
    #[error("query too large for sync execution")]
    TooLarge,
    /// A JIT `Derive` would run over an unbounded survivor set.
    #[error("JIT derive over unbounded survivor set; narrow first or precompute")]
    SurvivorUnbounded,
    /// An underlying execution-layer failure.
    #[error("execution failure")]
    Execution {
        /// The source error.
        #[source]
        source: consumer_engine_core::Error,
    },
}

/// Convenience alias.
pub type Result<T> = std::result::Result<T, QueryError>;

impl From<consumer_engine_core::Error> for QueryError {
    fn from(source: consumer_engine_core::Error) -> Self {
        Self::Execution { source }
    }
}
