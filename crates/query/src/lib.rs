//! `consumer_engine-query` — turn a Boolean/temporal DSL into guarded,
//! parameterised DuckDB SQL and run it.
//!
//! M1 implements capability **B** (`Filter`, `Recency`, `Lapsed`, `SetOp`) with
//! non-bypassable guardrails and a synchronous runner. The F/J/S/P capabilities
//! and `Exclude` are forward-contract stubs rejected by the validator. See
//! `specs/12-query-engine.md` and `specs/10-data-model.md §3`.

#![forbid(unsafe_code)]
#![warn(rust_2024_compatibility, missing_docs, missing_debug_implementations)]

pub mod ast;
pub mod compiler;
pub mod engine;
pub mod error;
pub mod guardrail;
pub mod parse;

pub use compiler::CompiledQuery;
pub use engine::{QueryEngine, SyncResult};
pub use error::{QueryError, Result};
