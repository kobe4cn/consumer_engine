//! DSL AST for `consumer_engine-query`.
//!
//! The structured query the agent composes (decision D2: DSL-primary). See
//! `specs/10-data-model.md §3` and `specs/80-glossary.md` (capability codes).
//! M1 implements the Boolean/temporal subset (**B**): `Filter`, `Recency`,
//! `Lapsed`, `SetOp`. `Exclude` and the F/J/S/P variants are forward-contract
//! stubs the parser/validator rejects in M1 with a clear "not supported" error.

// Re-export the shared `Dataset` from the dependency root (DRY: it is the unit
// both the compiler and the freshness registry read over; spec 10 §2).
pub use consumer_engine_core::Dataset;
use serde::{Deserialize, Serialize};

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
    /// Feature predicate (F): a comparison against a computed feature value
    /// in the wide pivot view `feature_wide_{family}`. The feature `name` is
    /// namespaced `family.short` (e.g. `"cadence.regularity"`) so it maps to the
    /// view `feature_wide_{family}` and column `{short}`. Only the numeric
    /// comparison operators are permitted (no `in`/`like`), and `value` is a
    /// number — validated in [`crate::parse`] and rendered with a bound
    /// parameter in [`crate::compiler`].
    #[serde(rename_all = "camelCase")]
    Feature {
        /// Namespaced feature name `family.short`.
        name: String,
        /// Comparison operator (only `eq`/`ne`/`lt`/`le`/`gt`/`ge`).
        op: Cmp,
        /// Comparison value (a JSON number, deserialised to `f64`).
        value: f64,
    },
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
