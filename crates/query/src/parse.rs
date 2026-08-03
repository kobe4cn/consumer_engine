//! DSL parser + validator.
//!
//! JSON → [`SegmentQuery`] (`serde`, honouring `deny_unknown_fields`), then
//! structural validation: identifier allowlists (`core::validate_ident` on every
//! system/entity/column/key), op-count and value-size caps, `withinDays` range,
//! and rejection of the M1-unsupported variants (`Exclude`, F/J/S/P). Per
//! AGENTS.md § Input Validation: reject, never sanitise.

use consumer_engine_core::validate_ident;

use crate::{
    ast::{Cmp, Dataset, Op, Predicate, SegmentQuery},
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
        Op::Exclude { .. } => Err(invalid(
            "Exclude is not supported in M1 (requires the suppression table, phase 5)",
        )),
        Op::Feature | Op::Derive | Op::Similar | Op::Characterize => {
            Err(invalid("this capability is not supported in M1"))
        }
    }
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
        let q = orders_q(vec![Op::Feature]);
        assert!(matches!(validate(&q), Err(QueryError::InvalidDsl(_))));
    }

    #[test]
    fn test_should_reject_exclude_in_m1() {
        let q = orders_q(vec![Op::Exclude {
            campaign_id: "c1".into(),
        }]);
        let res = validate(&q);
        assert!(matches!(res, Err(QueryError::InvalidDsl(_))));
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
