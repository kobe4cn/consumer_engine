//! DSL parser + validator.
//!
//! JSON → [`SegmentQuery`] (`serde`, honouring `deny_unknown_fields`), then
//! structural validation: identifier allowlists (`core::validate_ident` on every
//! system/entity/column/key), op-count and value-size caps, `withinDays` range,
//! and rejection of the M1-unsupported variants (`Exclude`, F/J/S/P). Per
//! AGENTS.md § Input Validation: reject, never sanitise.

use consumer_engine_core::{split_feature_name, validate_ident};

use crate::{
    ast::{Cmp, Dataset, JitMetric, Op, Predicate, SegmentQuery},
    error::{QueryError, Result},
};

/// Maximum ops in one segment.
const MAX_OPS: usize = 32;
/// Maximum bytes of a single predicate string value.
const MAX_PRED_VALUE_BYTES: usize = 4096;
/// Maximum values in an `IN`/`NOT IN` list.
const MAX_IN_VALUES: usize = 1024;
/// Maximum window length in days.
const MAX_WITHIN_DAYS: u32 = 3650;

/// Parse a DSL JSON value into a validated [`SegmentQuery`].
///
/// # Errors
/// [`QueryError::InvalidDsl`] on any parse or validation failure.
pub fn parse(value: serde_json::Value) -> Result<SegmentQuery> {
    let q: SegmentQuery =
        serde_json::from_value(value).map_err(|e| QueryError::InvalidDsl(e.to_string()))?;
    validate(&q)?;
    Ok(q)
}

/// Validate a parsed segment query in place.
///
/// # Errors
/// [`QueryError::InvalidDsl`] on any violation.
pub fn validate(q: &SegmentQuery) -> Result<()> {
    validate_ident_field(&q.source.system, "source.system")?;
    validate_ident_field(&q.source.entity, "source.entity")?;
    validate_ident_field(&q.key, "key")?;
    if q.ops.len() > MAX_OPS {
        return Err(invalid(format!("ops exceed cap of {MAX_OPS}")));
    }
    for op in &q.ops {
        validate_op(op)?;
    }
    validate_positions(q)?;
    Ok(())
}

/// Validate a single op.
fn validate_op(op: &Op) -> Result<()> {
    match op {
        Op::Filter { predicate } => validate_predicate(predicate),
        Op::Recency {
            event,
            user_key,
            ts_column,
            within_days,
            predicate,
        }
        | Op::Lapsed {
            event,
            user_key,
            ts_column,
            within_days,
            predicate,
        } => {
            validate_dataset(event)?;
            validate_ident_field(user_key, "userKey")?;
            validate_ident_field(ts_column, "tsColumn")?;
            if *within_days == 0 || *within_days > MAX_WITHIN_DAYS {
                return Err(invalid(format!(
                    "withinDays must be in 1..={MAX_WITHIN_DAYS}"
                )));
            }
            if let Some(p) = predicate {
                validate_predicate(p)?;
            }
            Ok(())
        }
        Op::SetOp { other, .. } => validate(other),
        Op::Exclude { campaign_id } => validate_ident_field(campaign_id, "exclude.campaignId"),
        Op::Feature { name, op, .. } => validate_feature(name, op),
        Op::Derive { name, metric } => validate_derive(name, metric),
        Op::Characterize {
            event,
            ts_column,
            monetary_column,
            category_column,
        } => validate_characterize(event, ts_column, monetary_column, category_column),
        Op::Similar => Err(invalid("Similar is not supported yet")),
    }
}

/// Validate a `Characterize` op: the metric source is a valid dataset and the
/// three named columns are sound identifiers. Position invariants (terminal,
/// may follow narrowing) are enforced by [`validate_positions`].
fn validate_characterize(
    event: &Dataset,
    ts_column: &str,
    monetary_column: &str,
    category_column: &str,
) -> Result<()> {
    validate_dataset(event)?;
    validate_ident_field(ts_column, "characterize.tsColumn")?;
    validate_ident_field(monetary_column, "characterize.monetaryColumn")?;
    validate_ident_field(category_column, "characterize.categoryColumn")?;
    Ok(())
}

/// Validate a `Derive` op: the metric name and any metric column are sound
/// identifiers, and the metric's event relation is a valid dataset. Position
/// invariants (must follow B/F narrowing, must be terminal) are enforced by
/// [`validate_positions`].
fn validate_derive(name: &str, metric: &JitMetric) -> Result<()> {
    validate_ident_field(name, "derive.name")?;
    let (event, column): (&Dataset, Option<&str>) = match metric {
        JitMetric::Count { event } => (event, None),
        JitMetric::Sum { event, column }
        | JitMetric::Avg { event, column }
        | JitMetric::Min { event, column }
        | JitMetric::Max { event, column } => (event, Some(column)),
    };
    validate_dataset(event)?;
    if let Some(c) = column {
        validate_ident_field(c, "derive.metric.column")?;
    }
    Ok(())
}

/// Enforce op-position invariants (specs/12 §4 I5): a terminal metric/profile
/// op (`Derive`, and later `Characterize`) must follow at least one B/F
/// narrowing op and must be the final op of the segment.
fn validate_positions(q: &SegmentQuery) -> Result<()> {
    let mut narrowing_seen = false;
    for (i, op) in q.ops.iter().enumerate() {
        match op {
            Op::Filter { .. }
            | Op::Recency { .. }
            | Op::Lapsed { .. }
            | Op::Feature { .. }
            | Op::SetOp { .. }
            | Op::Exclude { .. } => narrowing_seen = true,
            Op::Derive { .. } | Op::Characterize { .. } => {
                if !narrowing_seen {
                    return Err(invalid(
                        "Derive must follow B/F narrowing (filter/lapsed/recency/feature)",
                    ));
                }
                if i + 1 != q.ops.len() {
                    return Err(invalid("Derive must be the final op of the segment"));
                }
                return Ok(());
            }
            Op::Similar => return Err(invalid("Similar is not supported yet")),
        }
    }
    Ok(())
}

/// Validate a `Feature` op: the namespaced name must split into two sound
/// identifiers (`family.short`), the operator must be a numeric comparison, and
/// the value must be a JSON number (no strings/arrays/objects).
fn validate_feature(name: &str, op: &Cmp) -> Result<()> {
    // `split_feature_name` validates that the name has exactly one `.` and both
    // parts are valid identifiers — the invariant the compiler relies on to map
    // `family`→view and `short`→column.
    split_feature_name(name).map_err(|e| invalid(format!("feature name: {e}")))?;
    if !matches!(
        op,
        Cmp::Eq | Cmp::Ne | Cmp::Lt | Cmp::Le | Cmp::Gt | Cmp::Ge
    ) {
        return Err(invalid(
            "feature op must be one of eq/ne/lt/le/gt/ge (no in/like)",
        ));
    }
    Ok(())
}

/// Validate a dataset's identifiers.
fn validate_dataset(d: &Dataset) -> Result<()> {
    validate_ident_field(&d.system, "event.system")?;
    validate_ident_field(&d.entity, "event.entity")?;
    Ok(())
}

/// Validate a predicate: column identifier + value size caps.
fn validate_predicate(p: &Predicate) -> Result<()> {
    validate_ident_field(&p.column, "predicate.column")?;
    validate_value(&p.value, &p.op)?;
    Ok(())
}

/// Enforce byte/count caps on predicate values (defence against oversized
/// payloads). Type-vs-operator compatibility is enforced at compile time.
fn validate_value(v: &serde_json::Value, op: &Cmp) -> Result<()> {
    match v {
        serde_json::Value::String(s) => {
            if s.len() > MAX_PRED_VALUE_BYTES {
                return Err(invalid(format!(
                    "predicate value exceeds {MAX_PRED_VALUE_BYTES} bytes"
                )));
            }
        }
        serde_json::Value::Array(arr) => {
            if !matches!(op, Cmp::In | Cmp::NotIn) {
                return Err(invalid("array value requires op 'in' or 'notIn'"));
            }
            if arr.len() > MAX_IN_VALUES {
                return Err(invalid(format!("IN list exceeds {MAX_IN_VALUES} values")));
            }
            for item in arr {
                validate_value(item, &Cmp::Eq)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn validate_ident_field(name: &str, kind: &str) -> Result<()> {
    validate_ident(name).map_err(|e| invalid(format!("{kind}: {e}")))
}

fn invalid(msg: impl Into<String>) -> QueryError {
    QueryError::InvalidDsl(msg.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{Cmp, Dataset, Op, Predicate, SegmentQuery};

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
    fn test_should_parse_and_validate_dsl() {
        let json = serde_json::json!({
            "source": {"system":"erp","entity":"orders"},
            "key": "user_id",
            "ops": [
                {"kind":"filter","predicate":{"column":"sku","op":"eq","value":"A"}},
                {"kind":"lapsed","event":{"system":"erp","entity":"orders"},
                 "userKey":"user_id","tsColumn":"ts","withinDays":30,
                 "predicate":{"column":"sku","op":"eq","value":"A"}}
            ]
        });
        let q = parse(json).expect("valid DSL");
        assert_eq!(q.ops.len(), 2);
    }

    #[test]
    fn test_should_reject_bad_identifier() {
        let mut q = orders_q(vec![]);
        q.source.system = "erp; DROP".into();
        assert!(matches!(validate(&q), Err(QueryError::InvalidDsl(_))));
    }

    #[test]
    fn test_should_reject_unsupported_capability() {
        // Similar remains a forward-contract stub.
        let q = orders_q(vec![Op::Similar]);
        assert!(matches!(validate(&q), Err(QueryError::InvalidDsl(_))));
    }

    #[test]
    fn test_should_validate_derive_with_narrowing() {
        let q = orders_q(vec![
            Op::Filter {
                predicate: Predicate {
                    column: "sku".into(),
                    op: Cmp::Eq,
                    value: serde_json::json!("A"),
                },
            },
            Op::Derive {
                name: "total_revenue".into(),
                metric: JitMetric::Sum {
                    event: Dataset {
                        system: "erp".into(),
                        entity: "orders".into(),
                    },
                    column: "amount".into(),
                },
            },
        ]);
        assert!(
            validate(&q).is_ok(),
            "narrowing + terminal derive must pass"
        );
    }

    #[test]
    fn test_should_reject_derive_without_narrowing() {
        let q = orders_q(vec![Op::Derive {
            name: "total_revenue".into(),
            metric: JitMetric::Count {
                event: Dataset {
                    system: "erp".into(),
                    entity: "orders".into(),
                },
            },
        }]);
        assert!(matches!(validate(&q), Err(QueryError::InvalidDsl(_))));
    }

    #[test]
    fn test_should_reject_derive_not_terminal() {
        let q = orders_q(vec![
            Op::Filter {
                predicate: Predicate {
                    column: "sku".into(),
                    op: Cmp::Eq,
                    value: serde_json::json!("A"),
                },
            },
            Op::Derive {
                name: "total_revenue".into(),
                metric: JitMetric::Count {
                    event: Dataset {
                        system: "erp".into(),
                        entity: "orders".into(),
                    },
                },
            },
            Op::Filter {
                predicate: Predicate {
                    column: "sku".into(),
                    op: Cmp::Eq,
                    value: serde_json::json!("B"),
                },
            },
        ]);
        assert!(matches!(validate(&q), Err(QueryError::InvalidDsl(_))));
    }

    #[test]
    fn test_should_reject_derive_with_bad_metric_column() {
        let q = orders_q(vec![
            Op::Filter {
                predicate: Predicate {
                    column: "sku".into(),
                    op: Cmp::Eq,
                    value: serde_json::json!("A"),
                },
            },
            Op::Derive {
                name: "total_revenue".into(),
                metric: JitMetric::Sum {
                    event: Dataset {
                        system: "erp".into(),
                        entity: "orders".into(),
                    },
                    column: "bad col!".into(),
                },
            },
        ]);
        assert!(matches!(validate(&q), Err(QueryError::InvalidDsl(_))));
    }

    #[test]
    fn test_should_validate_feature_op() {
        let q = orders_q(vec![Op::Feature {
            name: "cadence.regularity".into(),
            op: Cmp::Gt,
            value: 0.7,
        }]);
        assert!(
            validate(&q).is_ok(),
            "a valid feature op must pass M3 validation"
        );
    }

    #[test]
    fn test_should_reject_feature_with_non_number_value() {
        // `value` is typed `f64`, so a non-number is rejected at serde parse
        // time with a clear InvalidDsl (not at validate()).
        let json = serde_json::json!({
            "source": {"system":"erp","entity":"orders"},
            "key": "user_id",
            "ops": [{"kind":"feature","name":"cadence.regularity","op":"gt","value":"high"}]
        });
        assert!(matches!(parse(json), Err(QueryError::InvalidDsl(_))));
    }

    #[test]
    fn test_should_reject_feature_with_bad_op() {
        let q = orders_q(vec![Op::Feature {
            name: "cadence.regularity".into(),
            op: Cmp::In,
            value: 0.7,
        }]);
        assert!(matches!(validate(&q), Err(QueryError::InvalidDsl(_))));
    }

    #[test]
    fn test_should_reject_feature_with_unnamespaced_name() {
        let q = orders_q(vec![Op::Feature {
            name: "regularity".into(),
            op: Cmp::Gt,
            value: 0.7,
        }]);
        assert!(matches!(validate(&q), Err(QueryError::InvalidDsl(_))));
    }

    #[test]
    fn test_should_validate_exclude_campaign_id() {
        // A valid campaign id passes; a bad one is rejected.
        let ok = orders_q(vec![Op::Exclude {
            campaign_id: "c1".into(),
        }]);
        assert!(validate(&ok).is_ok());
        let bad = orders_q(vec![Op::Exclude {
            campaign_id: "bad id!".into(),
        }]);
        assert!(matches!(validate(&bad), Err(QueryError::InvalidDsl(_))));
    }

    #[test]
    fn test_should_reject_array_value_without_in_op() {
        let q = orders_q(vec![Op::Filter {
            predicate: Predicate {
                column: "sku".into(),
                op: Cmp::Eq,
                value: serde_json::json!(["a", "b"]),
            },
        }]);
        assert!(matches!(validate(&q), Err(QueryError::InvalidDsl(_))));
    }

    #[test]
    fn test_should_reject_oversized_within_days() {
        let q = orders_q(vec![Op::Recency {
            event: Dataset {
                system: "erp".into(),
                entity: "orders".into(),
            },
            user_key: "user_id".into(),
            ts_column: "ts".into(),
            within_days: 9_999,
            predicate: None,
        }]);
        assert!(matches!(validate(&q), Err(QueryError::InvalidDsl(_))));
    }
}
