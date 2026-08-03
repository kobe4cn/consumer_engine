//! DSL AST for `consumer_engine-query`.
//!
//! The structured query the agent composes (decision D2: DSL-primary). See
//! `specs/10-data-model.md §3` and `specs/80-glossary.md` (capability codes).
//! M1 implements the Boolean/temporal subset (**B**): `Filter`, `Recency`,
//! `Lapsed`, `SetOp`. `Exclude` and the F/J/S/P variants are forward-contract
//! stubs the parser/validator rejects in M1 with a clear "not supported" error.

use serde::{Deserialize, Serialize};

/// A source table: compiles to `dro.raw_{system}_{entity}`.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Dataset {
    /// Source system identifier (validated).
    pub system: String,
    /// Source entity (table) identifier (validated).
    pub entity: String,
}

/// A segment query: a set of `key` values from `source`, narrowed by `ops`.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SegmentQuery {
    /// The base relation.
    pub source: Dataset,
    /// The subject column projected `DISTINCT` (e.g. `user_id`).
    pub key: String,
    /// Composed operations.
    pub ops: Vec<Op>,
}

/// A single DSL operation. Tagged by `kind` on the wire.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum Op {
    /// Boolean predicate on the base relation (B).
    #[serde(rename_all = "camelCase")]
    Filter {
        /// The predicate.
        predicate: Predicate,
    },
    /// Users with a matching event within the last `within_days` (B, temporal).
    #[serde(rename_all = "camelCase")]
    Recency {
        /// The event relation.
        event: Dataset,
        /// Column joining event rows to the base key.
        user_key: String,
        /// Event timestamp column.
        ts_column: String,
        /// Window in days.
        within_days: u32,
        /// Optional event-matching predicate.
        predicate: Option<Predicate>,
    },
    /// Users with a matching event before the window but not within it (B,
    /// temporal) — "lapsed".
    #[serde(rename_all = "camelCase")]
    Lapsed {
        /// The event relation.
        event: Dataset,
        /// Column joining event rows to the base key.
        user_key: String,
        /// Event timestamp column.
        ts_column: String,
        /// Window in days.
        within_days: u32,
        /// Optional event-matching predicate.
        predicate: Option<Predicate>,
    },
    /// Set combination with another segment (B).
    #[serde(rename_all = "camelCase")]
    SetOp {
        /// The set operator.
        op: SetOpKind,
        /// The other segment.
        other: Box<SegmentQuery>,
    },
    // --- Forward-contract variants; rejected in M1 (see `parse::validate`). ---
    /// Exclude users suppressed for a campaign (phase 5).
    #[serde(rename_all = "camelCase")]
    Exclude {
        /// Campaign id.
        campaign_id: String,
    },
    /// Feature predicate (F) — phase T4.
    Feature,
    /// JIT derive (J) — phase T7a.
    Derive,
    /// Similarity / lookalike (S) — phase 2.
    Similar,
    /// Comparative characterisation (P) — phase T7b.
    Characterize,
}

/// Set operator.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum SetOpKind {
    /// Intersection.
    Intersect,
    /// Union.
    Union,
    /// Difference.
    Minus,
}

/// A comparison predicate. `column` and `op` are allowlisted/closed; `value` is
/// bound as a parameter (never interpolated).
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Predicate {
    /// Column name (validated identifier).
    pub column: String,
    /// Comparison operator.
    pub op: Cmp,
    /// Comparison value (bound, not interpolated).
    pub value: serde_json::Value,
}

/// Comparison operators (closed enum — safe to render to SQL symbols).
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum Cmp {
    /// `=`
    Eq,
    /// `<>`
    Ne,
    /// `<`
    Lt,
    /// `<=`
    Le,
    /// `>`
    Gt,
    /// `>=`
    Ge,
    /// `IN (...)`
    In,
    /// `NOT IN (...)`
    NotIn,
    /// `LIKE`
    Like,
    /// `NOT LIKE`
    NotLike,
}
