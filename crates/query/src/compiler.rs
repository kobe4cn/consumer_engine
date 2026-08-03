//! DSL → parameterised DuckDB SQL compiler (capability B).
//!
//! Compiles a validated [`SegmentQuery`] into a single SQL string plus bound
//! parameters. Identifiers (`system`/`entity`/columns) are allowlisted upstream
//! by `parse::validate`, so they are rendered directly; **values are always
//! pushed into `params` and bound as `?` placeholders — never interpolated**
//! (invariant I1, `specs/12-query-engine.md`). Temporal windows use integer
//! `INTERVAL '<n>' DAY` constants only.

use consumer_engine_core::{READ_ONLY_CATALOG_ALIAS, SuppressionRules, split_feature_name};
use duckdb::types::Value;

use crate::{
    ast::{Cmp, Dataset, JitMetric, Op, Predicate, SegmentQuery, SetOpKind},
    error::{QueryError, Result},
};

/// Compilation context: the catalog alias to render, the suppression rules for
/// `Exclude` anti-joins (specs/20 §5), and the survivor-set `LIMIT` injected
/// into a `Derive` CTE (specs/12 §4; set by the engine from the prior B/F
/// stages' EXPLAIN).
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub struct CompileOptions<'a> {
    /// The catalog alias to render (`dro` for reads, `dl` for the write path).
    pub alias: &'a str,
    /// Suppression rules governing `Exclude`.
    pub suppression: &'a SuppressionRules,
    /// Survivor-set count for a `Derive` CTE `LIMIT` (`None` = no Derive).
    pub derive_limit: Option<u64>,
}

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

/// Compile a validated segment query against the **read-only** catalog alias
/// (`dro`). This is the read path used by `EXPLAIN` and the synchronous query
/// runner; it is unchanged in behaviour from M1.
///
/// # Errors
/// [`QueryError::InvalidDsl`] for M1-unsupported op orderings (e.g. an op after
/// a `SetOp`, or a second `SetOp`).
pub fn compile(q: &SegmentQuery) -> Result<CompiledQuery> {
    compile_with_opts(
        q,
        &CompileOptions {
            alias: READ_ONLY_CATALOG_ALIAS,
            suppression: &SuppressionRules::default(),
            derive_limit: None,
        },
    )
}

/// Compile a validated segment query against an explicit catalog alias. The
/// write path (`materialize`) passes [`consumer_engine_core::WRITE_CATALOG_ALIAS`]
/// so the writer's `INSERT … SELECT` runs under the writable `dl` attach while
/// the reader EXPLAINs under the read-only `dro` attach. Uses default
/// suppression rules.
///
/// # Errors
/// [`QueryError::InvalidDsl`] for unsupported op orderings.
pub fn compile_with_alias(q: &SegmentQuery, alias: &str) -> Result<CompiledQuery> {
    compile_with_opts(
        q,
        &CompileOptions {
            alias,
            suppression: &SuppressionRules::default(),
            derive_limit: None,
        },
    )
}

/// Compile a validated segment query with a full [`CompileOptions`] (the engine
/// passes its suppression rules and, for a `Derive`, the survivor-set limit).
///
/// # Errors
/// [`QueryError::InvalidDsl`] for unsupported op orderings.
pub fn compile_with_opts(q: &SegmentQuery, opts: &CompileOptions<'_>) -> Result<CompiledQuery> {
    compile_at(q, 0, opts)
}

/// Maximum SetOp nesting depth (defense-in-depth beyond serde_json's parse
/// recursion limit; AGENTS.md § Resource Limits — set explicit depth limits).
const MAX_NESTING: u8 = 8;

fn compile_at(q: &SegmentQuery, depth: u8, opts: &CompileOptions<'_>) -> Result<CompiledQuery> {
    let alias = opts.alias;
    if depth > MAX_NESTING {
        return Err(QueryError::InvalidDsl(format!(
            "segment nesting exceeds depth limit of {MAX_NESTING}"
        )));
    }
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
                    alias,
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
                    alias,
                )?);
            }
            Op::Feature { name, op, value } => {
                conjuncts.push(compile_feature(
                    name,
                    *op,
                    *value,
                    &q.key,
                    &mut params,
                    alias,
                )?);
            }
            Op::SetOp { op, other } => {
                setop = Some((op, other));
            }
            // Exclude: anti-join against `suppression` (specs/20 §5).
            Op::Exclude { campaign_id } => {
                conjuncts.push(compile_exclude(campaign_id, &q.key, opts, &mut params)?);
            }
            // JIT Derive: terminal — wraps the survivor set in a CTE with an
            // inner LIMIT (the survivor count from the prior B/F stages' plan)
            // and computes the metric over the survivors' event rows.
            Op::Derive { name, metric } => {
                let limit = opts.derive_limit.ok_or_else(|| {
                    QueryError::InvalidDsl(
                        "Derive requires the survivor count; the engine must plan the prior B/F \
                         stages first"
                            .into(),
                    )
                })?;
                // The metric's event table is a freshness source (D5).
                sources.push(metric_event(metric).clone());
                let survivor = survivor_cte(&q.source, &q.key, &conjuncts, alias, limit);
                let sql = compile_derive_metric(&survivor, name, metric, &mut params, alias)?;
                return Ok(CompiledQuery {
                    sql,
                    params,
                    sources,
                });
            }
            // parse::validate rejects these until their phases land;
            // defensive double-check.
            Op::Similar => {
                return Err(QueryError::InvalidDsl(
                    "capability not supported yet".into(),
                ));
            }
            // Characterize is compiled by `compile_characterize`, not here.
            Op::Characterize { .. } => {
                return Err(QueryError::InvalidDsl(
                    "Characterize must go through the profile path".into(),
                ));
            }
        }
    }

    // Reject any op appearing after the SetOp (the loop above sets `setop`; if a
    // later iteration added a conjunct, that's an op-after-setop).
    if let Some((kind, other)) = setop {
        let other_c = compile_at(other, depth + 1, opts)?;
        let kw = setop_keyword(*kind);
        sources.extend(other_c.sources);
        let this_sql = base_select(&q.source, &q.key, &conjuncts, alias);
        let mut all_params = params;
        all_params.extend(other_c.params);
        let sql = format!("({this_sql}) {kw} ({})", other_c.sql);
        Ok(CompiledQuery {
            sql,
            params: all_params,
            sources,
        })
    } else {
        let sql = base_select(&q.source, &q.key, &conjuncts, alias);
        Ok(CompiledQuery {
            sql,
            params,
            sources,
        })
    }
}

/// Build `SELECT DISTINCT <key> FROM <alias>.raw_<s>_<e> base [WHERE <conjuncts>]`.
fn base_select(source: &Dataset, key: &str, conjuncts: &[String], alias: &str) -> String {
    let table = raw_table(source, alias);
    if conjuncts.is_empty() {
        format!("SELECT DISTINCT base.{key} FROM {table} base")
    } else {
        format!(
            "SELECT DISTINCT base.{key} FROM {table} base WHERE {}",
            conjuncts.join(" AND ")
        )
    }
}

/// Qualified raw table name `<alias>.raw_<system>_<entity>` (idents pre-validated).
fn raw_table(d: &Dataset, alias: &str) -> String {
    format!("{alias}.raw_{}_{}", d.system, d.entity)
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
    alias: &str,
) -> Result<String> {
    let table = raw_table(event, alias);
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
    alias: &str,
) -> Result<String> {
    let table = raw_table(event, alias);
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

/// Compile a `Feature` op into an `EXISTS` conjunct against the wide pivot view
/// `feature_wide_{family}`. The feature value is bound as a parameter (I1);
/// `family`/`short` are validated identifiers rendered into the view/column
/// names. The feature view is **not** added to `sources` — freshness is graded
/// over raw sources only (D5), and the view is derived from already-graded data.
///
/// # Errors
/// [`QueryError::InvalidDsl`] if the name cannot split into sound identifiers
/// or the operator is not a numeric comparison (defence-in-depth; parse already
/// rejects these).
/// Build `SELECT DISTINCT base.<key> AS user_id FROM <alias>.raw_<s>_<e> base
/// [WHERE <conjuncts>] LIMIT <limit>` — the survivor CTE a `Derive` computes
/// over (specs/12 §4: inner LIMIT = survivor count).
fn survivor_cte(
    source: &Dataset,
    key: &str,
    conjuncts: &[String],
    alias: &str,
    limit: u64,
) -> String {
    let table = raw_table(source, alias);
    let where_sql = if conjuncts.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", conjuncts.join(" AND "))
    };
    format!("SELECT DISTINCT base.{key} AS user_id FROM {table} base{where_sql} LIMIT {limit}")
}

/// The event relation a `JitMetric` reads.
fn metric_event(m: &JitMetric) -> &Dataset {
    match m {
        JitMetric::Count { event }
        | JitMetric::Sum { event, .. }
        | JitMetric::Avg { event, .. }
        | JitMetric::Min { event, .. }
        | JitMetric::Max { event, .. } => event,
    }
}

/// Build the `Derive` metric SELECT over the survivor CTE: the event table
/// joins survivors on `user_id` and aggregates per the metric. Emits one row
/// `(name, value)`.
///
/// # Errors
/// [`QueryError::InvalidDsl`] for an unsupported metric variant (defensive;
/// parse validates the closed set).
fn compile_derive_metric(
    survivor: &str,
    name: &str,
    metric: &JitMetric,
    params: &mut Vec<Value>,
    alias: &str,
) -> Result<String> {
    let (event, agg): (&Dataset, String) = match metric {
        JitMetric::Count { event } => (event, "count(*)".to_string()),
        // Raw tables store every column as VARCHAR (ingest_raw), so numeric
        // columns are cast to DOUBLE for aggregation.
        JitMetric::Sum { event, column } => (event, format!("sum(CAST(e.{column} AS DOUBLE))")),
        JitMetric::Avg { event, column } => (event, format!("avg(CAST(e.{column} AS DOUBLE))")),
        JitMetric::Min { event, column } => (event, format!("min(CAST(e.{column} AS DOUBLE))")),
        JitMetric::Max { event, column } => (event, format!("max(CAST(e.{column} AS DOUBLE))")),
    };
    // Bind the metric name (I1: no interpolated user values).
    params.push(Value::Text(name.to_string()));
    let table = raw_table(event, alias);
    Ok(format!(
        "WITH survivor AS ({survivor}) SELECT ? AS name, {agg} AS value FROM survivor s JOIN \
         {table} e ON e.user_id = s.user_id",
    ))
}

/// The three SQL queries a `Characterize` segment runs (P, specs/12 §4):
/// row-level numeric metrics, per-user recency, and the category mix. Each
/// embeds the same `segment` CTE (the survivors of the preceding ops) left-joined
/// to the event table with an `in_seg` flag; the event table defines the
/// population (baseline).
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct CharacterizeQueries {
    /// Row-level aggregates: seg/base users, AOV, frequency.
    pub metrics: CompiledQuery,
    /// Per-user recency (avg days since last event) for segment vs baseline.
    pub recency: CompiledQuery,
    /// Category counts for segment vs baseline (ordered by segment count).
    pub categories: CompiledQuery,
}

/// Compile a terminal `Characterize` segment into its three profile queries.
/// The segment CTE is the preceding narrowing compile; columns are validated
/// identifiers; timestamps are cast to `TIMESTAMP` for interval arithmetic
/// (DuckDB lacks `TIMESTAMPTZ - TIMESTAMPTZ`).
///
/// # Errors
/// [`QueryError::InvalidDsl`] if the segment does not end in `Characterize`.
pub fn compile_characterize(
    q: &SegmentQuery,
    opts: &CompileOptions<'_>,
) -> Result<CharacterizeQueries> {
    let Some(Op::Characterize {
        event,
        ts_column,
        monetary_column,
        category_column,
    }) = q.ops.last()
    else {
        return Err(QueryError::InvalidDsl(
            "segment does not end in Characterize".into(),
        ));
    };

    // The segment CTE = the narrowing part's compiled SQL (`SELECT DISTINCT …`).
    let narrowing = strip_last_op(q);
    let seg = compile_with_opts(&narrowing, opts)?;
    let with_seg = format!("WITH segment AS ({})", seg.sql);
    let event_table = raw_table(event, opts.alias);
    let params = seg.params.clone();
    let mut sources = seg.sources.clone();
    sources.push(event.clone());

    // Row-level metrics: users, AOV (avg amount), frequency (events per user).
    let metrics = CompiledQuery {
        sql: format!(
            "{with_seg}, ev AS (SELECT e.user_id, CAST(e.{monetary_column} AS DOUBLE) AS amount, \
             (s.user_id IS NOT NULL) AS in_seg FROM {event_table} e LEFT JOIN segment s ON \
             s.user_id = e.user_id) SELECT (SELECT count(*) FROM segment) AS seg_users, (SELECT \
             count(DISTINCT user_id) FROM ev) AS base_users, (SELECT sum(amount) FROM ev WHERE \
             in_seg) * 1.0 / NULLIF((SELECT count(*) FROM ev WHERE in_seg), 0) AS seg_aov, \
             (SELECT sum(amount) FROM ev) * 1.0 / NULLIF((SELECT count(*) FROM ev), 0) AS \
             base_aov, (SELECT count(*) FROM ev WHERE in_seg) * 1.0 / NULLIF((SELECT \
             count(DISTINCT user_id) FROM ev WHERE in_seg), 0) AS seg_freq, (SELECT count(*) FROM \
             ev) * 1.0 / NULLIF((SELECT count(DISTINCT user_id) FROM ev), 0) AS base_freq, \
             (SELECT count(*) FROM ev WHERE in_seg) AS seg_orders, (SELECT count(*) FROM ev) AS \
             base_orders",
        ),
        params: params.clone(),
        sources: sources.clone(),
    };

    // Per-user recency: avg days since each user's last event.
    let recency = CompiledQuery {
        sql: format!(
            "{with_seg}, ev AS (SELECT e.user_id, CAST(e.{ts_column} AS TIMESTAMP) AS ts, \
             (s.user_id IS NOT NULL) AS in_seg FROM {event_table} e LEFT JOIN segment s ON \
             s.user_id = e.user_id), per_user AS (SELECT user_id, bool_or(in_seg) AS in_seg, \
             max(ts) AS last_ts FROM ev GROUP BY user_id) SELECT avg(extract(epoch FROM \
             (CAST(now() AS TIMESTAMP) - last_ts)) / 86400.0) FILTER (WHERE in_seg) AS \
             seg_recency_days, avg(extract(epoch FROM (CAST(now() AS TIMESTAMP) - last_ts)) / \
             86400.0) AS base_recency_days FROM per_user",
        ),
        params: params.clone(),
        sources: sources.clone(),
    };

    // Category mix: per-category counts for segment vs baseline (top by segment).
    let categories = CompiledQuery {
        sql: format!(
            "{with_seg}, ev AS (SELECT e.user_id, e.{category_column} AS category, (s.user_id IS \
             NOT NULL) AS in_seg FROM {event_table} e LEFT JOIN segment s ON s.user_id = \
             e.user_id) SELECT category, count(*) FILTER (WHERE in_seg) AS seg_n, count(*) AS \
             base_n FROM ev GROUP BY category ORDER BY seg_n DESC LIMIT 3",
        ),
        params,
        sources,
    };

    Ok(CharacterizeQueries {
        metrics,
        recency,
        categories,
    })
}

/// The narrowing segment: `q` with its terminal op removed.
fn strip_last_op(q: &SegmentQuery) -> SegmentQuery {
    SegmentQuery {
        source: q.source.clone(),
        key: q.key.clone(),
        ops: q
            .ops
            .iter()
            .take(q.ops.len().saturating_sub(1))
            .cloned()
            .collect(),
    }
}

/// Compile an `Exclude` op into anti-join conjuncts against `suppression`,
/// governed by the suppression rules (specs/20 §5):
/// - **per-campaign no-repeat** (default on): a user with any `targeted`/ `delivered` writeback for
///   `campaign_id` is excluded from that campaign.
/// - **global frequency cap** (configurable): a user with `>= max_contacts` `targeted`/`delivered`
///   writebacks in the last `window_days` days across campaigns is excluded.
///
/// With both rules disabled the conjunct is a tautology (`1=1`). Values are
/// bound; the action set and window are fixed validated constants.
fn compile_exclude(
    campaign_id: &str,
    base_key: &str,
    opts: &CompileOptions<'_>,
    params: &mut Vec<Value>,
) -> Result<String> {
    let mut clauses: Vec<String> = Vec::new();
    if opts.suppression.per_campaign_no_repeat {
        params.push(Value::Text(campaign_id.to_string()));
        clauses.push(format!(
            "NOT EXISTS (SELECT 1 FROM {alias}.suppression s WHERE s.user_id = base.{base_key} \
             AND s.campaign_id = ? AND s.action IN ('targeted', 'delivered'))",
            alias = opts.alias,
        ));
    }
    if let Some(cap) = opts.suppression.frequency_cap {
        let (n, d) = (cap.max_contacts, cap.window_days);
        params.push(Value::BigInt(i64::from(n)));
        clauses.push(format!(
            "NOT EXISTS (SELECT 1 FROM {alias}.suppression s WHERE s.user_id = base.{base_key} \
             AND s.action IN ('targeted', 'delivered') AND s.occurred_ts >= CAST(CAST(now() AS \
             TIMESTAMP) - INTERVAL '{d}' DAY AS TIMESTAMPTZ) GROUP BY s.user_id HAVING count(*) \
             >= ?)",
            alias = opts.alias,
        ));
    }
    if clauses.is_empty() {
        return Ok("1=1".to_string());
    }
    Ok(clauses.join(" AND "))
}

fn compile_feature(
    name: &str,
    op: Cmp,
    value: f64,
    base_key: &str,
    params: &mut Vec<Value>,
    alias: &str,
) -> Result<String> {
    let (family, short) =
        split_feature_name(name).map_err(|e| QueryError::InvalidDsl(e.to_string()))?;
    let cmp = cmp_symbol(op)?;
    params.push(Value::Double(value));
    Ok(format!(
        "EXISTS (SELECT 1 FROM {alias}.feature_wide_{family} f WHERE f.user_id = base.{base_key} \
         AND f.{short} {cmp} ?)"
    ))
}

/// Map a numeric comparison operator to its SQL symbol.
///
/// # Errors
/// [`QueryError::InvalidDsl`] for `In`/`NotIn`/`Like`/`NotLike` (not valid on
/// a scalar feature value).
fn cmp_symbol(op: Cmp) -> Result<&'static str> {
    Ok(match op {
        Cmp::Eq => "=",
        Cmp::Ne => "<>",
        Cmp::Lt => "<",
        Cmp::Le => "<=",
        Cmp::Gt => ">",
        Cmp::Ge => ">=",
        Cmp::In | Cmp::NotIn | Cmp::Like | Cmp::NotLike => {
            return Err(QueryError::InvalidDsl(
                "feature op must be a numeric comparison (eq/ne/lt/le/gt/ge)".into(),
            ));
        }
    })
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

    #[test]
    fn test_should_compile_with_write_alias() {
        let q = orders(vec![Op::Filter {
            predicate: Predicate {
                column: "sku".into(),
                op: Cmp::Eq,
                value: serde_json::json!("A"),
            },
        }]);
        let c = compile_with_alias(&q, consumer_engine_core::WRITE_CATALOG_ALIAS).expect("compile");
        assert!(
            c.sql.contains("dl.raw_erp_orders"),
            "write alias must appear: {}",
            c.sql
        );
        // The read alias must not leak into the write-compiled SQL.
        assert!(!c.sql.contains("dro.raw_erp_orders"));
    }

    #[test]
    fn test_should_compile_feature_as_exists_subquery() {
        let q = orders(vec![Op::Feature {
            name: "cadence.regularity".into(),
            op: Cmp::Gt,
            value: 0.7,
        }]);
        let c = compile(&q).expect("compile");
        assert!(
            c.sql
                .contains("EXISTS (SELECT 1 FROM dro.feature_wide_cadence f"),
            "expected feature_wide view exists-subquery: {}",
            c.sql
        );
        assert!(
            c.sql.contains("f.regularity > ?"),
            "expected bound value on short name: {}",
            c.sql
        );
        assert_eq!(c.params, vec![Value::Double(0.7)]);
        // The feature view must NOT contribute to freshness sources (D5).
        assert!(
            c.sources.iter().all(|d| d.entity != "feature_wide_cadence"),
            "feature view must not be a freshness source: {:?}",
            c.sources
        );
    }

    #[test]
    fn test_should_compile_exclude_as_anti_join() {
        use consumer_engine_core::SuppressionRules;
        let q = orders(vec![Op::Exclude {
            campaign_id: "c1".into(),
        }]);
        let c = compile_with_opts(
            &q,
            &CompileOptions {
                alias: READ_ONLY_CATALOG_ALIAS,
                suppression: &SuppressionRules::default(),
                derive_limit: None,
            },
        )
        .expect("compile");
        assert!(
            c.sql
                .contains("NOT EXISTS (SELECT 1 FROM dro.suppression s"),
            "expected anti-join against suppression: {}",
            c.sql
        );
        assert!(c.sql.contains("s.campaign_id = ?"), "{}", c.sql);
        assert!(
            c.sql.contains("'targeted', 'delivered'"),
            "no-repeat action set must be targeted/delivered: {}",
            c.sql
        );
        assert_eq!(c.params, vec![Value::Text("c1".into())]);
        // suppression must not become a freshness source.
        assert!(c.sources.iter().all(|d| d.entity != "suppression"));
    }

    #[test]
    fn test_should_compile_frequency_cap_when_configured() {
        use consumer_engine_core::{FrequencyCap, SuppressionRules};
        let q = orders(vec![Op::Exclude {
            campaign_id: "c1".into(),
        }]);
        let c = compile_with_opts(
            &q,
            &CompileOptions {
                alias: READ_ONLY_CATALOG_ALIAS,
                suppression: &SuppressionRules {
                    per_campaign_no_repeat: true,
                    frequency_cap: Some(FrequencyCap {
                        max_contacts: 3,
                        window_days: 30,
                    }),
                },
                derive_limit: None,
            },
        )
        .expect("compile");
        assert!(
            c.sql.contains("INTERVAL '30' DAY"),
            "frequency window must be injected: {}",
            c.sql
        );
        assert!(
            c.sql.contains("HAVING count(*) >= ?"),
            "cap threshold must be bound: {}",
            c.sql
        );
        // campaign_id + max_contacts bound.
        assert_eq!(c.params.len(), 2);
    }

    #[test]
    fn test_should_compile_exclude_tautology_when_all_rules_off() {
        use consumer_engine_core::SuppressionRules;
        let q = orders(vec![Op::Exclude {
            campaign_id: "c1".into(),
        }]);
        let c = compile_with_opts(
            &q,
            &CompileOptions {
                alias: READ_ONLY_CATALOG_ALIAS,
                suppression: &SuppressionRules {
                    per_campaign_no_repeat: false,
                    frequency_cap: None,
                },
                derive_limit: None,
            },
        )
        .expect("compile");
        assert!(
            c.sql.contains("1=1"),
            "all rules off must be a tautology: {}",
            c.sql
        );
        assert!(c.params.is_empty());
    }

    #[test]
    fn test_should_compile_derive_with_survivor_cte() {
        use consumer_engine_core::SuppressionRules;
        let q = orders(vec![
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
        let c = compile_with_opts(
            &q,
            &CompileOptions {
                alias: READ_ONLY_CATALOG_ALIAS,
                suppression: &SuppressionRules::default(),
                derive_limit: Some(42),
            },
        )
        .expect("compile");
        assert!(
            c.sql.contains(
                "WITH survivor AS (SELECT DISTINCT base.user_id AS user_id FROM \
                 dro.raw_erp_orders base WHERE base.sku = ? LIMIT 42)"
            ),
            "expected survivor CTE with inner LIMIT: {}",
            c.sql
        );
        assert!(
            c.sql.contains(
                "SELECT ? AS name, sum(CAST(e.amount AS DOUBLE)) AS value FROM survivor s JOIN \
                 dro.raw_erp_orders e ON e.user_id = s.user_id"
            ),
            "expected metric join over survivors: {}",
            c.sql
        );
        // sku bound + metric name bound (no interpolated values).
        assert_eq!(
            c.params,
            vec![Value::Text("A".into()), Value::Text("total_revenue".into())]
        );
    }

    #[test]
    fn test_should_require_derive_limit_to_compile() {
        use consumer_engine_core::SuppressionRules;
        let q = orders(vec![Op::Derive {
            name: "n".into(),
            metric: JitMetric::Count {
                event: Dataset {
                    system: "erp".into(),
                    entity: "orders".into(),
                },
            },
        }]);
        // No derive_limit (and no narrowing) must fail at compile.
        let res = compile_with_opts(
            &q,
            &CompileOptions {
                alias: READ_ONLY_CATALOG_ALIAS,
                suppression: &SuppressionRules::default(),
                derive_limit: None,
            },
        );
        assert!(matches!(res, Err(QueryError::InvalidDsl(_))));
    }

    #[test]
    fn test_should_reject_deeply_nested_setop() {
        // 10 levels of SetOp nesting > MAX_NESTING (8).
        let mut inner = orders(vec![]);
        for _ in 0..10 {
            inner = SegmentQuery {
                source: Dataset {
                    system: "erp".into(),
                    entity: "orders".into(),
                },
                key: "user_id".into(),
                ops: vec![Op::SetOp {
                    op: SetOpKind::Intersect,
                    other: Box::new(inner),
                }],
            };
        }
        assert!(matches!(compile(&inner), Err(QueryError::InvalidDsl(_))));
    }
}
