//! DSL → parameterised DuckDB SQL compiler (capability B).
//!
//! Compiles a validated [`SegmentQuery`] into a single SQL string plus bound
//! parameters. Identifiers (`system`/`entity`/columns) are allowlisted upstream
//! by `parse::validate`, so they are rendered directly; **values are always
//! pushed into `params` and bound as `?` placeholders — never interpolated**
//! (invariant I1, `specs/12-query-engine.md`). Temporal windows use integer
//! `INTERVAL '<n>' DAY` constants only.

use duckdb::types::Value;

use crate::{
    ast::{Cmp, Dataset, Op, Predicate, SegmentQuery, SetOpKind},
    error::{QueryError, Result},
};

/// A compiled query: SQL text, bound parameters (in placeholder order), and the
/// source datasets touched (for the freshness label, D5).
#[derive(Debug, Clone, PartialEq)]
pub struct CompiledQuery {
    /// The SQL text (uses `?` placeholders, references `dro.*` tables).
    pub sql: String,
    /// Bound parameter values, in `?` order.
    pub params: Vec<Value>,
    /// Every `raw_*` table the query reads.
    pub sources: Vec<Dataset>,
}

/// Compile a validated segment query.
///
/// # Errors
/// [`QueryError::InvalidDsl`] for M1-unsupported op orderings (e.g. an op after
/// a `SetOp`, or a second `SetOp`).
pub fn compile(q: &SegmentQuery) -> Result<CompiledQuery> {
    let mut params: Vec<Value> = Vec::new();
    let mut sources: Vec<Dataset> = vec![q.source.clone()];
    let mut conjuncts: Vec<String> = Vec::new();

    let mut setop: Option<(&SetOpKind, &SegmentQuery)> = None;
    for op in &q.ops {
        // Once a SetOp is seen, no further op may follow (M1 semantics: the SetOp
        // combines the accumulated base with `other`). This also forbids a
        // second SetOp.
        if setop.is_some() {
            return Err(QueryError::InvalidDsl(
                "ops after a SetOp are not supported in M1".into(),
            ));
        }
        match op {
            Op::Filter { predicate } => {
                conjuncts.push(compile_predicate("base", predicate, &mut params)?);
            }
            Op::Recency {
                event,
                user_key,
                ts_column,
                within_days,
                predicate,
            } => {
                sources.push(event.clone());
                conjuncts.push(compile_recency(
                    event,
                    user_key,
                    &q.key,
                    ts_column,
                    *within_days,
                    predicate.as_ref(),
                    &mut params,
                )?);
            }
            Op::Lapsed {
                event,
                user_key,
                ts_column,
                within_days,
                predicate,
            } => {
                sources.push(event.clone());
                conjuncts.push(compile_lapsed(
                    event,
                    user_key,
                    &q.key,
                    ts_column,
                    *within_days,
                    predicate.as_ref(),
                    &mut params,
                )?);
            }
            Op::SetOp { op, other } => {
                setop = Some((op, other));
            }
            // parse::validate rejects these for M1; defensive double-check.
            Op::Exclude { .. } | Op::Feature | Op::Derive | Op::Similar | Op::Characterize => {
                return Err(QueryError::InvalidDsl(
                    "capability not supported in M1".into(),
                ));
            }
        }
    }

    // Reject any op appearing after the SetOp (the loop above sets `setop`; if a
    // later iteration added a conjunct, that's an op-after-setop).
    if let Some((kind, other)) = setop {
        let other_c = compile(other)?;
        let kw = setop_keyword(*kind);
        sources.extend(other_c.sources);
        let this_sql = base_select(&q.source, &q.key, &conjuncts);
        let mut all_params = params;
        all_params.extend(other_c.params);
        let sql = format!("({this_sql}) {kw} ({})", other_c.sql);
        Ok(CompiledQuery {
            sql,
            params: all_params,
            sources,
        })
    } else {
        let sql = base_select(&q.source, &q.key, &conjuncts);
        Ok(CompiledQuery {
            sql,
            params,
            sources,
        })
    }
}

/// Build `SELECT DISTINCT <key> FROM dro.raw_<s>_<e> base [WHERE <conjuncts>]`.
fn base_select(source: &Dataset, key: &str, conjuncts: &[String]) -> String {
    let table = raw_table(source);
    if conjuncts.is_empty() {
        format!("SELECT DISTINCT base.{key} FROM {table} base")
    } else {
        format!(
            "SELECT DISTINCT base.{key} FROM {table} base WHERE {}",
            conjuncts.join(" AND ")
        )
    }
}

/// Qualified raw table name `dro.raw_<system>_<entity>` (idents pre-validated).
fn raw_table(d: &Dataset) -> String {
    format!("dro.raw_{}_{}", d.system, d.entity)
}

/// Render a predicate on alias `rel` (e.g. `base` or `e`), pushing its value(s)
/// into `params`.
fn compile_predicate(rel: &str, p: &Predicate, params: &mut Vec<Value>) -> Result<String> {
    let col = format!("{rel}.{}", p.column);
    let sql = match p.op {
        Cmp::Eq => {
            params.push(to_value(&p.value)?);
            format!("{col} = ?")
        }
        Cmp::Ne => {
            params.push(to_value(&p.value)?);
            format!("{col} <> ?")
        }
        Cmp::Lt => {
            params.push(to_value(&p.value)?);
            format!("{col} < ?")
        }
        Cmp::Le => {
            params.push(to_value(&p.value)?);
            format!("{col} <= ?")
        }
        Cmp::Gt => {
            params.push(to_value(&p.value)?);
            format!("{col} > ?")
        }
        Cmp::Ge => {
            params.push(to_value(&p.value)?);
            format!("{col} >= ?")
        }
        Cmp::Like => {
            params.push(to_value(&p.value)?);
            format!("{col} LIKE ?")
        }
        Cmp::NotLike => {
            params.push(to_value(&p.value)?);
            format!("{col} NOT LIKE ?")
        }
        Cmp::In => compile_in(&col, &p.value, params, false)?,
        Cmp::NotIn => compile_in(&col, &p.value, params, true)?,
    };
    Ok(sql)
}

/// Render an `IN`/`NOT IN` list, pushing each value into `params`.
fn compile_in(
    col: &str,
    value: &serde_json::Value,
    params: &mut Vec<Value>,
    negate: bool,
) -> Result<String> {
    let arr = value
        .as_array()
        .ok_or_else(|| QueryError::InvalidDsl("IN/NotIn requires an array value".into()))?;
    if arr.is_empty() {
        return Ok(if negate {
            "1=1".to_string()
        } else {
            "1=0".to_string()
        });
    }
    for item in arr {
        params.push(to_value(item)?);
    }
    let placeholders = vec!["?"; arr.len()].join(", ");
    Ok(if negate {
        format!("{col} NOT IN ({placeholders})")
    } else {
        format!("{col} IN ({placeholders})")
    })
}

/// Compile a `Recency` op into an `EXISTS` subquery (matching event within the
/// window).
#[allow(clippy::too_many_arguments)]
fn compile_recency(
    event: &Dataset,
    user_key: &str,
    base_key: &str,
    ts_column: &str,
    within_days: u32,
    predicate: Option<&Predicate>,
    params: &mut Vec<Value>,
) -> Result<String> {
    let table = raw_table(event);
    let mut where_parts = vec![format!("e.{user_key} = base.{base_key}")];
    if let Some(p) = predicate {
        where_parts.push(compile_predicate("e", p, params)?);
    }
    where_parts.push(format!(
        "e.{ts_column} >= now() - INTERVAL '{within_days}' DAY"
    ));
    Ok(format!(
        "EXISTS (SELECT 1 FROM {table} e WHERE {})",
        where_parts.join(" AND ")
    ))
}

/// Compile a `Lapsed` op into `EXISTS(before window) AND NOT EXISTS(within window)`.
#[allow(clippy::too_many_arguments)]
fn compile_lapsed(
    event: &Dataset,
    user_key: &str,
    base_key: &str,
    ts_column: &str,
    within_days: u32,
    predicate: Option<&Predicate>,
    params: &mut Vec<Value>,
) -> Result<String> {
    let table = raw_table(event);
    let mut before_parts = vec![format!("e.{user_key} = base.{base_key}")];
    if let Some(p) = predicate {
        before_parts.push(compile_predicate("e", p, params)?);
    }
    before_parts.push(format!(
        "e.{ts_column} < now() - INTERVAL '{within_days}' DAY"
    ));
    let mut recent_parts = vec![format!("e.{user_key} = base.{base_key}")];
    if let Some(p) = predicate {
        recent_parts.push(compile_predicate("e", p, params)?);
    }
    recent_parts.push(format!(
        "e.{ts_column} >= now() - INTERVAL '{within_days}' DAY"
    ));
    Ok(format!(
        "EXISTS (SELECT 1 FROM {table} e WHERE {before}) AND NOT EXISTS (SELECT 1 FROM {table} e \
         WHERE {recent})",
        before = before_parts.join(" AND "),
        recent = recent_parts.join(" AND "),
    ))
}

/// Map a [`SetOpKind`] to its SQL keyword.
fn setop_keyword(k: SetOpKind) -> &'static str {
    match k {
        SetOpKind::Intersect => "INTERSECT",
        SetOpKind::Union => "UNION",
        SetOpKind::Minus => "EXCEPT",
    }
}

/// Convert a JSON scalar to a DuckDB [`Value`].
///
/// # Errors
/// [`QueryError::InvalidDsl`] if the value is not a bindable scalar.
fn to_value(v: &serde_json::Value) -> Result<Value> {
    match v {
        serde_json::Value::Null => Ok(Value::Null),
        serde_json::Value::Bool(b) => Ok(Value::Boolean(*b)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(Value::BigInt(i))
            } else if let Some(f) = n.as_f64() {
                Ok(Value::Double(f))
            } else {
                Err(QueryError::InvalidDsl("unsupported numeric value".into()))
            }
        }
        serde_json::Value::String(s) => Ok(Value::Text(s.clone())),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            Err(QueryError::InvalidDsl("scalar value required".into()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{Cmp, Dataset, Op, Predicate, SegmentQuery};

    fn orders(ops: Vec<Op>) -> SegmentQuery {
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
    fn test_should_parameterise_all_user_values() {
        let q = orders(vec![Op::Filter {
            predicate: Predicate {
                column: "sku".into(),
                op: Cmp::Eq,
                value: serde_json::json!("A"),
            },
        }]);
        let c = compile(&q).expect("compile");
        // The literal value must never appear in the SQL; only `?`.
        assert!(
            !c.sql.contains("'A'"),
            "value must be bound, not interpolated: {}",
            c.sql
        );
        assert!(c.sql.contains("?"), "expected a placeholder: {}", c.sql);
        assert_eq!(c.params, vec![Value::Text("A".into())]);
        assert!(c.sql.contains("dro.raw_erp_orders"));
    }

    #[test]
    fn test_should_compile_lapsed_with_bound_predicate_twice() {
        let q = orders(vec![Op::Lapsed {
            event: Dataset {
                system: "erp".into(),
                entity: "orders".into(),
            },
            user_key: "user_id".into(),
            ts_column: "ts".into(),
            within_days: 30,
            predicate: Some(Predicate {
                column: "sku".into(),
                op: Cmp::Eq,
                value: serde_json::json!("A"),
            }),
        }]);
        let c = compile(&q).expect("compile");
        // Predicate value bound once per subquery (before + recent).
        assert_eq!(c.params.len(), 2);
        assert!(c.sql.contains("INTERVAL '30' DAY"));
        assert!(c.sql.contains("NOT EXISTS"));
    }

    #[test]
    fn test_should_compile_in_list() {
        let q = orders(vec![Op::Filter {
            predicate: Predicate {
                column: "sku".into(),
                op: Cmp::In,
                value: serde_json::json!(["A", "B", "C"]),
            },
        }]);
        let c = compile(&q).expect("compile");
        assert_eq!(c.params.len(), 3);
        assert!(c.sql.contains("IN (?, ?, ?)"));
    }

    #[test]
    fn test_should_reject_op_after_setop() {
        let q = orders(vec![
            Op::SetOp {
                op: SetOpKind::Intersect,
                other: Box::new(orders(vec![])),
            },
            Op::Filter {
                predicate: Predicate {
                    column: "sku".into(),
                    op: Cmp::Eq,
                    value: serde_json::json!("A"),
                },
            },
        ]);
        assert!(matches!(compile(&q), Err(QueryError::InvalidDsl(_))));
    }
}
