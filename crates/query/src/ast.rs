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

/// A raw-table column referenced by a segment: `system.entity.column`. Used by
/// catalogue enforcement (spec 13 §1: the agent may only query catalogued
/// columns — no invented names).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ReferencedColumn {
    /// Source system identifier.
    pub system: String,
    /// Source entity (table) identifier.
    pub entity: String,
    /// Column name.
    pub column: String,
}

/// Collect every raw-table column a segment references, descending into
/// `SetOp` children. `Feature` ops are skipped — they reference derived
/// wide-view columns (`feature_wide_{family}.{short}`), not raw catalogued
/// columns; `Exclude` references a campaign id, not a column (spec 13 §1).
#[must_use]
pub fn referenced_columns(q: &SegmentQuery) -> Vec<ReferencedColumn> {
    let mut out = Vec::new();
    collect_columns(q, &mut out);
    out
}

/// Recursive worker for [`referenced_columns`].
fn collect_columns(q: &SegmentQuery, out: &mut Vec<ReferencedColumn>) {
    out.push(ReferencedColumn {
        system: q.source.system.clone(),
        entity: q.source.entity.clone(),
        column: q.key.clone(),
    });
    for op in &q.ops {
        match op {
            Op::Filter { predicate } => out.push(ReferencedColumn {
                system: q.source.system.clone(),
                entity: q.source.entity.clone(),
                column: predicate.column.clone(),
            }),
            Op::Recency {
                event,
                user_key,
                ts_column,
                predicate,
                ..
            }
            | Op::Lapsed {
                event,
                user_key,
                ts_column,
                predicate,
                ..
            } => {
                let (sys, ent) = (event.system.as_str(), event.entity.as_str());
                out.push(ReferencedColumn {
                    system: sys.into(),
                    entity: ent.into(),
                    column: user_key.clone(),
                });
                out.push(ReferencedColumn {
                    system: sys.into(),
                    entity: ent.into(),
                    column: ts_column.clone(),
                });
                if let Some(p) = predicate {
                    out.push(ReferencedColumn {
                        system: sys.into(),
                        entity: ent.into(),
                        column: p.column.clone(),
                    });
                }
            }
            Op::SetOp { other, .. } => collect_columns(other, out),
            // Derived/campaign references are not raw catalogued columns.
            Op::Feature { .. }
            | Op::Exclude { .. }
            | Op::Derive
            | Op::Similar
            | Op::Characterize => {}
        }
    }
}

/// Collect the namespaced feature names a segment references via `Feature` ops
/// (descending into `SetOp` children). Used by catalogue enforcement (spec 13
/// §1 / issue #6 AC#3) to reject unregistered features: a feature no producer
/// has ever written must fail as `InvalidDsl`, not at execution time.
#[must_use]
pub fn referenced_features(q: &SegmentQuery) -> Vec<String> {
    let mut out = Vec::new();
    collect_features(q, &mut out);
    out
}

/// Recursive worker for [`referenced_features`].
fn collect_features(q: &SegmentQuery, out: &mut Vec<String>) {
    for op in &q.ops {
        match op {
            Op::Feature { name, .. } => out.push(name.clone()),
            Op::SetOp { other, .. } => collect_features(other, out),
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn orders_q(ops: Vec<Op>) -> SegmentQuery {
        SegmentQuery {
            source: Dataset {
                system: "erp".into(),
                entity: "orders".into(),
            },
            key: "user_id".into(),
            ops,
        }
    }

    #[test]
    fn test_should_collect_key_filter_and_temporal_columns() {
        let q = orders_q(vec![
            Op::Filter {
                predicate: Predicate {
                    column: "sku".into(),
                    op: Cmp::Eq,
                    value: serde_json::json!(0.0),
                },
            },
            Op::Lapsed {
                event: Dataset {
                    system: "erp".into(),
                    entity: "events".into(),
                },
                user_key: "user_id".into(),
                ts_column: "ts".into(),
                within_days: 30,
                predicate: Some(Predicate {
                    column: "kind".into(),
                    op: Cmp::Eq,
                    value: serde_json::json!(0.0),
                }),
            },
        ]);
        let cols = referenced_columns(&q);
        // base.user_id, base.sku, events.user_id, events.ts, events.kind.
        assert!(cols.contains(&ReferencedColumn {
            system: "erp".into(),
            entity: "orders".into(),
            column: "user_id".into()
        }));
        assert!(cols.contains(&ReferencedColumn {
            system: "erp".into(),
            entity: "orders".into(),
            column: "sku".into()
        }));
        assert!(cols.contains(&ReferencedColumn {
            system: "erp".into(),
            entity: "events".into(),
            column: "kind".into()
        }));
    }

    #[test]
    fn test_should_descend_into_setop_and_skip_feature() {
        let q = orders_q(vec![Op::SetOp {
            op: SetOpKind::Intersect,
            other: Box::new(SegmentQuery {
                source: Dataset {
                    system: "erp".into(),
                    entity: "events".into(),
                },
                key: "user_id".into(),
                ops: vec![Op::Feature {
                    name: "cadence.regularity".into(),
                    op: Cmp::Gt,
                    value: 0.7,
                }],
            }),
        }]);
        let cols = referenced_columns(&q);
        // base orders.user_id + other events.user_id; Feature skipped.
        assert_eq!(
            cols.len(),
            2,
            "Feature must not contribute a raw column: {cols:?}"
        );
        assert!(cols.iter().all(|c| c.column != "regularity"));
        // But the Feature name must surface via referenced_features.
        let feats = referenced_features(&q);
        assert_eq!(feats, vec!["cadence.regularity".to_string()]);
    }
}
