//! The query engine: parse → compile → guard → run (sync), plus the Q2
//! materialise bridge (`materialize` → `snap_<uuid>`). The job lifecycle
//! (jobId mint, poll) lives in the ingress layer (`consumer_engine-ingress`);
//! here we only do the materialise work and snapshot metadata read.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use consumer_engine_core::{
    BoxError, Error, Freshness, FreshnessRegistry, GuardrailConfig, READ_ONLY_CATALOG_ALIAS,
    SnapshotSpec, SuppressionRules, WRITE_CATALOG_ALIAS, now_epoch,
};
use consumer_engine_execution::{QueryResult, Reader, RowCells};
use consumer_engine_ingestion::IngestionHandle;
use duckdb::types::Value;
use tokio::sync::Semaphore;

use crate::{
    ast::SegmentQuery,
    compiler::{CompileOptions, CompiledQuery, compile_with_opts},
    error::{QueryError, Result},
    guardrail::{Mode, Plan, enforce, explain_cost},
};

/// The query engine. Cheap to share via `Arc`.
#[derive(Clone)]
pub struct QueryEngine {
    reader: Reader,
    ingestion: IngestionHandle,
    guardrails: GuardrailConfig,
    inflight: Arc<Semaphore>,
    freshness: Arc<FreshnessRegistry>,
    suppression: SuppressionRules,
}

impl std::fmt::Debug for QueryEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryEngine")
            .field("guardrails", &self.guardrails)
            .finish_non_exhaustive()
    }
}

/// The result of a synchronous query.
#[derive(Debug, Clone, PartialEq)]
pub struct SyncResult {
    /// Column names in order.
    pub columns: Vec<String>,
    /// Rows, each a vector of JSON cells.
    pub rows: Vec<RowCells>,
    /// Number of rows returned.
    pub count: u64,
    /// Freshness label (graded per source; M1 = batch only).
    pub freshness: Freshness,
    /// A unique id for this query.
    pub query_id: String,
}

impl QueryEngine {
    /// Construct a query engine over `reader` with `guardrails`. `freshness`
    /// drives the graded per-source freshness label (D5); `ingestion` is the
    /// single writer handle used by [`Self::materialize`].
    #[must_use]
    pub fn new(
        reader: Reader,
        ingestion: IngestionHandle,
        guardrails: GuardrailConfig,
        freshness: Arc<FreshnessRegistry>,
        suppression: SuppressionRules,
    ) -> Self {
        let permits = guardrails.threads.max(1);
        Self {
            reader,
            ingestion,
            guardrails,
            inflight: Arc::new(Semaphore::new(permits)),
            freshness,
            suppression,
        }
    }

    /// Run a DSL JSON value end-to-end: parse/validate → compile → guard → run,
    /// scoped to `tenant` (issue #22: the compiler injects the caller's tenant
    /// into every SQL — never trusted from the DSL).
    ///
    /// # Errors
    /// Propagates parse, guardrail, and execution errors.
    pub async fn run(&self, dsl: serde_json::Value, tenant: &str) -> Result<SyncResult> {
        let q = crate::parse::parse(dsl)?;
        self.run_sync(&q, tenant).await
    }

    /// Run a read-only reader query under the statement-timeout guardrail
    /// (AGENTS.md § Resource Limits: every storage read needs a timeout — a
    /// stalled probe must not block the single reader thread). Used by the
    /// catalogue guardrail and the snapshot-metadata read.
    async fn timed_read(&self, sql: &str, params: Vec<Value>) -> Result<QueryResult> {
        let timeout = Duration::from_secs(self.guardrails.statement_timeout_secs);
        match tokio::time::timeout(timeout, self.reader.query_with_params(sql, params)).await {
            Ok(inner) => inner.map_err(Into::into),
            Err(_) => Err(QueryError::Guardrail {
                rule: "statement_timeout".into(),
                limit: format!("{}s", self.guardrails.statement_timeout_secs),
            }),
        }
    }

    /// Compile a segment and run the catalogue guardrail (spec 13 §1). Shared by
    /// the sync path (`prepare`) and the materialise path so both enforce
    /// equally (a materialise IS a query, spec 12 §2a). Compiles with the
    /// engine's suppression rules so `Exclude` anti-joins honour the configured
    /// frequency cap (specs/20 §5). A terminal `Derive` is compiled with the
    /// survivor-set `LIMIT` from the prior B/F stages' EXPLAIN (specs/12 §4).
    async fn compile_and_check(&self, q: &SegmentQuery, tenant: &str) -> Result<CompiledQuery> {
        self.enforce_catalogue(q, tenant).await?;
        let base = CompileOptions {
            alias: READ_ONLY_CATALOG_ALIAS,
            suppression: &self.suppression,
            derive_limit: None,
            tenant,
        };
        if has_derive(q) {
            let limit = self.derive_survivor_limit(q, tenant).await?;
            return compile_with_opts(
                q,
                &CompileOptions {
                    derive_limit: Some(limit),
                    ..base
                },
            );
        }
        compile_with_opts(q, &base)
    }

    /// Plan the prior B/F narrowing of a `Derive` segment and return the
    /// survivor-set `LIMIT` to inject into the CTE — rejecting when the **actual**
    /// survivor count exceeds `j_survivor_cap` (specs/12 §4 I5 / I2: guardrails
    /// non-bypassable — an EXPLAIN estimate could both bypass the cap and
    /// silently truncate, so the count is measured, not estimated).
    async fn derive_survivor_limit(&self, q: &SegmentQuery, tenant: &str) -> Result<u64> {
        let narrowing = strip_derive(q);
        let c = compile_with_opts(
            &narrowing,
            &CompileOptions {
                alias: READ_ONLY_CATALOG_ALIAS,
                suppression: &self.suppression,
                derive_limit: None,
                tenant,
            },
        )?;
        let count_sql = format!("SELECT count(*) FROM ({}) sub", c.sql);
        let qr = self.timed_read(&count_sql, c.params).await?;
        let count = qr
            .rows
            .first()
            .and_then(|r| r.first())
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        if count > self.guardrails.j_survivor_cap {
            return Err(QueryError::SurvivorUnbounded);
        }
        Ok(count)
    }

    /// Run a terminal `Characterize` segment (P): compile the three profile
    /// queries, run them read-only under the statement-timeout, and assemble a
    /// structured segment-vs-baseline profile (specs/12 §4, issue #9).
    async fn run_characterize(&self, q: &SegmentQuery, tenant: &str) -> Result<SyncResult> {
        self.enforce_catalogue(q, tenant).await?;
        let opts = CompileOptions {
            alias: READ_ONLY_CATALOG_ALIAS,
            suppression: &self.suppression,
            derive_limit: None,
            tenant,
        };
        let queries = crate::compiler::compile_characterize(q, &opts)?;
        let metrics = self
            .timed_read(&queries.metrics.sql, queries.metrics.params)
            .await?;
        let recency = self
            .timed_read(&queries.recency.sql, queries.recency.params)
            .await?;
        let categories = self
            .timed_read(&queries.categories.sql, queries.categories.params)
            .await?;
        let profile = assemble_profile(&metrics, &recency, &categories);
        let freshness = self.freshness.worst(&queries.metrics.sources, now_epoch());
        Ok(SyncResult {
            columns: vec!["profile".to_string()],
            rows: vec![vec![profile]],
            count: 1,
            freshness,
            query_id: format!("q_{}", uuid::Uuid::now_v7()),
        })
    }

    /// Reject a segment that references a raw column absent from the
    /// `semantic_catalog` or a `Feature` name no producer has ever written
    /// (spec 13 §1 / issue #6 AC#3: the agent may only query catalogued names —
    /// no invented columns or features). No-op when `enforce_catalogue` is off.
    /// The raw-column check is batched per referenced table; the feature check
    /// probes `feature_store` for each referenced feature name.
    ///
    /// # Errors
    /// [`QueryError::InvalidDsl`] naming the first missing column/feature.
    async fn enforce_catalogue(&self, q: &SegmentQuery, tenant: &str) -> Result<()> {
        if !self.guardrails.enforce_catalogue {
            return Ok(());
        }
        let refs = crate::ast::referenced_columns(q);
        let mut by_table: BTreeMap<(String, String), BTreeSet<String>> = BTreeMap::new();
        for r in refs {
            by_table
                .entry((r.system, r.entity))
                .or_default()
                .insert(r.column);
        }
        for ((system, entity), cols) in by_table {
            let qr = self
                .timed_read(
                    &format!(
                        "SELECT DISTINCT column_name FROM \
                         {READ_ONLY_CATALOG_ALIAS}.semantic_catalog WHERE system = ? AND \
                         table_name = ? AND column_name IS NOT NULL"
                    ),
                    vec![Value::Text(system.clone()), Value::Text(entity.clone())],
                )
                .await?;
            let catalogued: BTreeSet<String> = qr
                .rows
                .iter()
                .filter_map(|row| {
                    row.first()
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_string)
                })
                .collect();
            for col in cols {
                if !catalogued.contains(&col) {
                    return Err(QueryError::InvalidDsl(format!(
                        "column {system}.{entity}.{col} is not in the semantic catalogue; onboard \
                         + profile {system}.{entity} first (spec 13 §1)"
                    )));
                }
            }
            // Catalogue freshness (spec 13 I5 / issue #18): warn when the
            // newest catalogue entry for this table predates the source's
            // latest ingest — the agent may be building on stale semantics.
            // (Membership is unaffected; this is a warning, not a rejection.)
            if let Some(meta) = self.freshness.get(&system, &entity) {
                let qr = self
                    .timed_read(
                        &format!(
                            "SELECT max(source_epoch) FROM \
                             {READ_ONLY_CATALOG_ALIAS}.semantic_catalog WHERE system = ? AND \
                             table_name = ? AND tenant_id = ?"
                        ),
                        vec![
                            Value::Text(system.clone()),
                            Value::Text(entity.clone()),
                            Value::Text(tenant.to_string()),
                        ],
                    )
                    .await?;
                let catalogue_epoch = qr
                    .rows
                    .first()
                    .and_then(|r| r.first())
                    .and_then(serde_json::Value::as_i64);
                if let Some(cat_epoch) = catalogue_epoch
                    && cat_epoch < meta.last_epoch_secs
                {
                    tracing::warn!(
                        system = %system,
                        entity = %entity,
                        catalogue_epoch = cat_epoch,
                        source_epoch = meta.last_epoch_secs,
                        "semantic catalogue older than the source's latest ingest; re-onboard to \
                         refresh descriptions (spec 13 I5)"
                    );
                }
            }
        }
        for name in crate::ast::referenced_features(q) {
            let qr = self
                .timed_read(
                    &format!(
                        "SELECT 1 FROM {READ_ONLY_CATALOG_ALIAS}.feature_store WHERE feature_name \
                         = ? AND tenant_id = ? LIMIT 1"
                    ),
                    vec![Value::Text(name.clone()), Value::Text(tenant.to_string())],
                )
                .await?;
            if qr.rows.is_empty() {
                return Err(QueryError::InvalidDsl(format!(
                    "feature {name} is not registered; run the producer first (spec 13 §1)"
                )));
            }
        }
        Ok(())
    }

    /// Compile + best-effort EXPLAIN estimate a plan, enforcing row budgets
    /// **before execution** (AC#3). M1 promotes to [`Mode::Async`] when the
    /// estimated rows exceed `sync_row_cap` (then `run_sync` rejects it).
    ///
    /// # Errors
    /// Propagates compile/guardrail errors.
    pub async fn plan(&self, q: &SegmentQuery, tenant: &str) -> Result<Plan> {
        let (plan, _compiled) = self.prepare(q, tenant).await?;
        Ok(plan)
    }

    /// Compile, run the EXPLAIN pre-flight, enforce budgets, and choose a mode.
    /// Returns the [`Plan`] and the [`CompiledQuery`] (for execution).
    ///
    /// # Errors
    /// Propagates compile/guardrail errors.
    async fn prepare(&self, q: &SegmentQuery, tenant: &str) -> Result<(Plan, CompiledQuery)> {
        let compiled = self.compile_and_check(q, tenant).await?;
        let est = explain_cost(
            &self.reader,
            &compiled,
            self.guardrails.statement_timeout_secs,
        )
        .await;
        enforce(&est, &self.guardrails)?;
        let mode = if est.est_rows > self.guardrails.sync_row_cap {
            Mode::Async
        } else {
            Mode::Sync
        };
        Ok((
            Plan {
                mode,
                est,
                sources: compiled.sources.clone(),
            },
            compiled,
        ))
    }

    /// Run a parsed segment query synchronously under all guardrails.
    ///
    /// # Errors
    /// [`QueryError::Guardrail`] on timeout or output-row cap;
    /// [`QueryError::TooLarge`] if the plan is async (not in M1);
    /// [`QueryError::Execution`] on reader failure.
    pub async fn run_sync(&self, q: &SegmentQuery, tenant: &str) -> Result<SyncResult> {
        // Concurrency cap (acquired before the EXPLAIN pre-flight + execute).
        let _permit = self
            .inflight
            .acquire()
            .await
            .map_err(|e| QueryError::Execution {
                source: Error::Execution(BoxError::from(e)),
            })?;

        // A terminal Characterize emits a structured profile, not rows — run
        // the profile path instead of the row pipeline.
        if has_characterize(q) {
            return self.run_characterize(q, tenant).await;
        }

        // Pre-flight: compile + EXPLAIN + enforce budgets (AC#3: over-budget is
        // rejected here, before the query executes).
        let (plan, compiled) = self.prepare(q, tenant).await?;
        if plan.mode == Mode::Async {
            return Err(QueryError::TooLarge);
        }

        // Statement timeout.
        let timeout = Duration::from_secs(self.guardrails.statement_timeout_secs);
        let QueryResult { columns, rows, .. } = tokio::time::timeout(
            timeout,
            self.reader
                .query_with_params(&compiled.sql, compiled.params),
        )
        .await
        .map_err(|_| QueryError::Guardrail {
            rule: "statement_timeout".into(),
            limit: format!("{}s", self.guardrails.statement_timeout_secs),
        })??;

        // Output row cap.
        if rows.len() as u64 > self.guardrails.max_output_rows {
            return Err(QueryError::Guardrail {
                rule: "max_output_rows".into(),
                limit: self.guardrails.max_output_rows.to_string(),
            });
        }

        let freshness = self.freshness.worst(&compiled.sources, now_epoch());
        Ok(SyncResult {
            columns,
            count: rows.len() as u64,
            rows,
            freshness,
            query_id: format!("q_{}", uuid::Uuid::now_v7()),
        })
    }

    /// Run an **approved** raw-SQL escape-hatch statement (specs/21 §4 E2)
    /// under the same guardrails as the DSL path: concurrency cap, statement
    /// timeout, output-row cap. The caller is responsible for having verified
    /// the approval token (the ingress layer does, and audit-logs). Freshness
    /// is unknown for arbitrary SQL (no parsed sources) → the default label.
    ///
    /// # Errors
    /// [`QueryError::Guardrail`] on timeout/row-cap; [`QueryError::Execution`]
    /// on reader failure.
    pub async fn run_sql_approved(&self, sql: &str, tenant: &str) -> Result<SyncResult> {
        // NOTE (issue #22): raw SQL cannot have a tenant filter injected by the
        // compiler — it runs tenant-unscooped, gated only by the separate
        // approval token. The caller (ingress) audit-logs it with the tenant.
        let _ = tenant;
        let _permit = self
            .inflight
            .acquire()
            .await
            .map_err(|e| QueryError::Execution {
                source: Error::Execution(BoxError::from(e)),
            })?;
        let timeout = Duration::from_secs(self.guardrails.statement_timeout_secs);
        let qr = tokio::time::timeout(timeout, self.reader.query(sql))
            .await
            .map_err(|_| QueryError::Guardrail {
                rule: "statement_timeout".into(),
                limit: format!("{}s", self.guardrails.statement_timeout_secs),
            })??;
        if qr.rows.len() as u64 > self.guardrails.max_output_rows {
            return Err(QueryError::Guardrail {
                rule: "max_output_rows".into(),
                limit: self.guardrails.max_output_rows.to_string(),
            });
        }
        let count = qr.rows.len() as u64;
        Ok(SyncResult {
            columns: qr.columns,
            rows: qr.rows,
            count,
            freshness: Freshness::batch(0),
            query_id: format!("q_{}", uuid::Uuid::now_v7()),
        })
    }

    /// Materialise a validated DSL segment into `audience_snapshot` via the
    /// single writer, returning the opaque snapshot id `snap_<uuidv7>`.
    ///
    /// This is the Q2 work half of the async path; the REST job lifecycle
    /// (jobId mint/poll) lives in `consumer_engine-ingress`. Guardrails are
    /// **not** enforced here — a large result set is the whole point of
    /// materialising — but the T2 `EXPLAIN` is run best-effort so a malformed
    /// segment fails fast with a clear error (`specs/20 I4`).
    ///
    /// The snapshot is written as one atomic `INSERT … SELECT` where each row
    /// carries its **frozen feature values** (a point-in-time pivot per family
    /// bounded by the snapshot's `as_of_ts` — decision D11 + I3, issues #13/
    /// #21) and its **predicate chain** (`hit_reason` = the validated op list
    /// that composed the segment; in an AND-composed segment every op matched,
    /// so the chain is the selecting predicates). `as_of_ts` is the minimum
    /// source freshness (the data cut-off), not the materialisation wall-clock.
    ///
    /// # Errors
    /// - [`QueryError::InvalidDsl`] if the segment fails to compile or the DSL cannot be serialised
    ///   as `hit_reason`.
    /// - [`QueryError::Execution`] propagating ingestion/storage failures.
    pub async fn materialize(
        &self,
        q: &SegmentQuery,
        campaign_id: &str,
        tenant: &str,
    ) -> Result<String> {
        // A JIT Derive emits a metric, not a key set — it cannot materialise.
        if has_derive(q) {
            return Err(QueryError::InvalidDsl(
                "a JIT Derive returns a metric, not a segment; it cannot be materialised".into(),
            ));
        }
        // A Characterize emits a profile, not a key set — it cannot materialise.
        if has_characterize(q) {
            return Err(QueryError::InvalidDsl(
                "a Characterize returns a profile, not a segment; it cannot be materialised".into(),
            ));
        }
        // Best-effort EXPLAIN: validates the segment compiles under the read
        // alias and surfaces a bad-DSL failure early, but does NOT enforce row
        // budgets (large is the point of materialising). Errors from EXPLAIN
        // (a real compile error) are surfaced via `compile` below.
        let compiled = self.compile_and_check(q, tenant).await?;
        let est = explain_cost(
            &self.reader,
            &compiled,
            self.guardrails.statement_timeout_secs,
        )
        .await;
        tracing::info!(est_rows = est.est_rows, "materialise estimate");

        // Scalars.
        let snapshot_id = uuid::Uuid::now_v7().to_string();
        // I3 point-in-time bounding (issue #21): the snapshot's `as_of_ts` is
        // the MINIMUM source freshness over the segment's sources — the data
        // cut-off the snapshot reflects — not the materialisation wall-clock.
        // Sources without a registered epoch fall back to now (no signal).
        let now = now_epoch();
        let min_epoch = compiled
            .sources
            .iter()
            .filter_map(|d| self.freshness.get(&d.system, &d.entity))
            .map(|m| m.last_epoch_secs)
            .min();
        let as_of_epoch = min_epoch.unwrap_or(now);
        let as_of_ts = chrono::DateTime::from_timestamp(as_of_epoch, 0)
            .map(|d| d.to_rfc3339())
            .unwrap_or_else(|| chrono::Utc::now().to_rfc3339());
        // hit_reason: the per-predicate selection chain (D11, issue #13). The
        // validated op list is serialised to JSON text and **bound as a
        // parameter** — never interpolated into SQL (values inside ops come
        // from the agent).
        let hit_reason = serde_json::to_string(&q.ops)
            .map_err(|e| QueryError::InvalidDsl(format!("serialize hit_reason: {e}")))?;

        // Feature families known to the store (fresh read; the writer's
        // `feature_wide_*` views are at least as fresh).
        let families = self.feature_families(tenant).await?;

        // Write path: recompile under the writable alias so the writer's
        // `INSERT … SELECT` resolves `dl.raw_*`.
        let write = compile_with_opts(
            q,
            &CompileOptions {
                alias: WRITE_CATALOG_ALIAS,
                suppression: &self.suppression,
                derive_limit: None,
                tenant,
            },
        )?;

        // Wrap the compiled segment so the subquery emits `<key>, features,
        // hit_reason` per row (the writer selects exactly those columns).
        // Frozen features: LEFT JOIN a POINT-IN-TIME pivot per family (issue
        // #21): the EAV rows are bounded by `as_of_ts <= <as_of>` and the
        // value at the newest as_of within that window is frozen — the
        // latest-wins wide view would leak post-cut-off feature values (I3).
        // `json_object` omits NULLs, so a user's features JSON holds exactly
        // the values they had at selection time.
        let key = &q.key;
        let mut wrap_params = Vec::with_capacity(write.params.len() + 1 + families.len() * 2);
        // The wrap SELECT's `CAST(? AS JSON) AS hit_reason` placeholder precedes
        // every `?` inside the compiled subquery, so it binds first.
        wrap_params.push(Value::Text(hit_reason));
        wrap_params.extend(write.params);
        let mut joins = String::new();
        let mut json_pairs: Vec<String> = Vec::new();
        for (idx, (family, shorts)) in families.iter().enumerate() {
            let fa = format!("fs_{idx}");
            let mut pivot_cols = vec!["user_id".to_string(), "tenant_id".to_string()];
            for short in shorts {
                pivot_cols.push(format!(
                    "arg_max(num_value, as_of_ts) FILTER (WHERE feature_name = \
                     '{family}.{short}') AS {short}"
                ));
            }
            let pivot_select = pivot_cols.join(", ");
            joins.push_str(&format!(
                " LEFT JOIN (SELECT {pivot_select} FROM {WRITE_CATALOG_ALIAS}.feature_store WHERE \
                 starts_with(feature_name, '{family}.') AND as_of_ts <= CAST(? AS TIMESTAMPTZ) \
                 GROUP BY user_id, tenant_id) {fa} ON {fa}.user_id = s.{key} AND {fa}.tenant_id = \
                 ?"
            ));
            // SQL placeholder order: the pivot's as_of `?` then the join's
            // tenant `?`, both AFTER the compiled subquery's placeholders.
            wrap_params.push(Value::Text(as_of_ts.clone()));
            wrap_params.push(Value::Text(tenant.to_string()));
            for short in shorts {
                json_pairs.push(format!("'{family}.{short}', {fa}.{short}"));
            }
        }
        let features_expr = if json_pairs.is_empty() {
            "json_object()".to_string()
        } else {
            format!("json_object({})", json_pairs.join(", "))
        };
        let wrap_sql = format!(
            "SELECT s.{key} AS {key}, {features_expr} AS features, CAST(? AS JSON) AS hit_reason \
             FROM ({}) s{joins}",
            write.sql
        );
        let spec = SnapshotSpec {
            snapshot_id: snapshot_id.clone(),
            campaign_id: campaign_id.to_string(),
            as_of_ts,
        };
        self.ingestion
            .materialize_snapshot(&wrap_sql, wrap_params, &q.key, spec, tenant)
            .await?;
        Ok(format!("snap_{snapshot_id}"))
    }

    /// The distinct `(family, short)` feature names written to the store,
    /// grouped by family and sorted (the frozen-features projection joins one
    /// wide view per family).
    ///
    /// Best-effort: a missing `feature_store` table (a fresh engine with no
    /// features) degrades to an empty map — the snapshot is still valid, its
    /// rows just carry `features={}` (D11 / issue #13: frozen features are an
    /// enrichment, not a prerequisite; the server ensures the table at startup
    /// anyway).
    async fn feature_families(&self, tenant: &str) -> Result<BTreeMap<String, Vec<String>>> {
        let qr = match self
            .timed_read(
                "SELECT DISTINCT feature_name FROM dro.feature_store WHERE tenant_id = ?",
                vec![Value::Text(tenant.to_string())],
            )
            .await
        {
            Ok(qr) => qr,
            Err(e) => {
                tracing::warn!(error = %e, "feature_store unreadable; freezing no features");
                return Ok(BTreeMap::new());
            }
        };
        let mut families: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for row in &qr.rows {
            if let Some(name) = row.first().and_then(serde_json::Value::as_str)
                && let Ok((family, short)) = consumer_engine_core::split_feature_name(name)
            {
                families.entry(family).or_default().push(short);
            }
        }
        for shorts in families.values_mut() {
            shorts.sort();
            shorts.dedup();
        }
        Ok(families)
    }

    /// Read a snapshot's metadata via the read-only reader. Returns `None` if no
    /// rows exist for `snap_uuid`. `as_of_ts` and the JSON columns are cast to
    /// `VARCHAR` because `execution::value_to_json` maps TIMESTAMPTZ/JSON to
    /// null today.
    ///
    /// # Errors
    /// [`QueryError::Execution`] on reader failure.
    pub async fn snapshot_meta(
        &self,
        snap_uuid: &str,
        tenant: &str,
    ) -> Result<Option<SnapshotMeta>> {
        // IDOR closure (issue #22 / specs/21 I3): the metadata read is scoped to
        // the caller's tenant — a foreign tenant's snapshot resolves to None.
        let sql = "SELECT campaign_id, CAST(as_of_ts AS VARCHAR), count(*) FROM \
                   dro.audience_snapshot WHERE snapshot_id = CAST(? AS UUID) AND tenant_id = ? \
                   GROUP BY campaign_id, as_of_ts";
        let qr = self
            .timed_read(
                sql,
                vec![
                    Value::Text(snap_uuid.to_string()),
                    Value::Text(tenant.to_string()),
                ],
            )
            .await?;
        let row = match qr.rows.into_iter().next() {
            Some(r) => r,
            None => return Ok(None),
        };
        let campaign_id = row
            .first()
            .and_then(serde_json::Value::as_str)
            .map(str::to_string)
            .unwrap_or_default();
        let as_of_ts = row
            .get(1)
            .and_then(serde_json::Value::as_str)
            .map(str::to_string)
            .unwrap_or_default();
        let row_count = row.get(2).and_then(serde_json::Value::as_u64).unwrap_or(0);
        Ok(Some(SnapshotMeta {
            snapshot_id: snap_uuid.to_string(),
            campaign_id,
            as_of_ts,
            row_count,
        }))
    }
}

/// Metadata for one materialised snapshot, read back via the read-only reader.
#[derive(Debug, Clone, PartialEq)]
pub struct SnapshotMeta {
    /// The bare snapshot UUID (no `snap_` prefix).
    pub snapshot_id: String,
    /// Caller-supplied campaign id.
    pub campaign_id: String,
    /// Data cut-off reflected (ISO-8601 UTC string).
    pub as_of_ts: String,
    /// Number of users in the snapshot.
    pub row_count: u64,
}

/// Does the segment end in a JIT `Derive`?
fn has_derive(q: &SegmentQuery) -> bool {
    q.ops
        .iter()
        .any(|op| matches!(op, crate::ast::Op::Derive { .. }))
}

/// Does the segment end in a terminal `Characterize`?
fn has_characterize(q: &SegmentQuery) -> bool {
    q.ops
        .iter()
        .any(|op| matches!(op, crate::ast::Op::Characterize { .. }))
}

/// Assemble the structured profile JSON from the three characterize query
/// results: `{ segment, baseline, ratios }` covering monetary (AOV), frequency,
/// recency (days since last event) and category mix (specs/12 §4). Nulls are
/// treated as 0; ratios guard division by zero.
fn assemble_profile(
    metrics: &QueryResult,
    recency: &QueryResult,
    categories: &QueryResult,
) -> serde_json::Value {
    use serde_json::json;

    let cell = |row: &RowCells, i: usize| -> f64 {
        row.get(i)
            .and_then(serde_json::Value::as_f64)
            .unwrap_or(0.0)
    };
    let m = metrics.rows.first().cloned().unwrap_or_default();
    let r = recency.rows.first().cloned().unwrap_or_default();
    let seg_users = cell(&m, 0) as u64;
    let base_users = cell(&m, 1) as u64;
    let seg_aov = cell(&m, 2);
    let base_aov = cell(&m, 3);
    let seg_freq = cell(&m, 4);
    let base_freq = cell(&m, 5);
    // Total order counts (all categories) for correct shares even when the
    // category query is limited to the top-3.
    let seg_orders = cell(&m, 6);
    let base_orders = cell(&m, 7);
    let seg_recency = cell(&r, 0);
    let base_recency = cell(&r, 1);

    // Category mix: shares of the top categories (by segment count).
    let mut seg_mix: Vec<serde_json::Value> = Vec::new();
    let mut base_mix: Vec<serde_json::Value> = Vec::new();
    for row in &categories.rows {
        let category = row
            .first()
            .and_then(serde_json::Value::as_str)
            .unwrap_or("")
            .to_string();
        let seg_n = cell(row, 1);
        let base_n = cell(row, 2);
        seg_mix.push(json!({ "category": category, "orders": seg_n }));
        base_mix.push(json!({ "category": category, "orders": base_n }));
    }
    let share = |n: f64, total: f64| if total > 0.0 { n / total } else { 0.0 };
    for v in seg_mix.iter_mut() {
        let orders = v["orders"].as_f64().unwrap_or(0.0);
        v["share"] = json!(share(orders, seg_orders));
    }
    for v in base_mix.iter_mut() {
        let orders = v["orders"].as_f64().unwrap_or(0.0);
        v["share"] = json!(share(orders, base_orders));
    }

    let ratio = |seg: f64, base: f64| if base != 0.0 { seg / base } else { 0.0 };
    json!({
        "segment": {
            "users": seg_users,
            "averageOrderValue": seg_aov,
            "frequency": seg_freq,
            "recencyDays": seg_recency,
            "categoryMix": seg_mix,
        },
        "baseline": {
            "users": base_users,
            "averageOrderValue": base_aov,
            "frequency": base_freq,
            "recencyDays": base_recency,
            "categoryMix": base_mix,
        },
        "ratios": {
            "averageOrderValue": ratio(seg_aov, base_aov),
            "frequency": ratio(seg_freq, base_freq),
            "recencyDays": ratio(seg_recency, base_recency),
        },
    })
}

/// The narrowing segment: `q` with its terminal `Derive` op removed (parse
/// enforces the Derive is last, so this is the preceding B/F narrowing).
fn strip_derive(q: &SegmentQuery) -> SegmentQuery {
    SegmentQuery {
        source: q.source.clone(),
        key: q.key.clone(),
        as_of: q.as_of.clone(),
        ops: q
            .ops
            .iter()
            .filter(|op| !matches!(op, crate::ast::Op::Derive { .. }))
            .cloned()
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use consumer_engine_core::{FreshnessRegistry, GuardrailConfig, SuppressionRules};
    use consumer_engine_execution::{Reader, ReaderLimits};

    use super::*;

    #[tokio::test]
    async fn test_should_materialize_returns_snapshot_id() {
        let tmp = tempfile::tempdir().expect("tmp");
        let writer = consumer_engine_storage::Writer::attach(
            &tmp.path().join("cat.db"),
            &tmp.path().join("data"),
        )
        .expect("attach");
        writer
            .ingest_raw(
                "erp",
                "orders",
                &["user_id".into(), "sku".into()],
                &[
                    vec![Some("u1".into()), Some("A".into())],
                    vec![Some("u2".into()), Some("A".into())],
                    vec![Some("u3".into()), Some("B".into())],
                ],
            )
            .expect("ingest");

        let ingestion = consumer_engine_ingestion::IngestionHandle::start(
            writer,
            Arc::new(consumer_engine_ingestion::ProducerRegistry::new()),
        )
        .expect("start");
        let read_conn = consumer_engine_storage::open_reader(
            &tmp.path().join("cat.db"),
            &tmp.path().join("data"),
        )
        .expect("read attach");
        let attach_sql = consumer_engine_storage::read_only_attach_sql(
            &tmp.path().join("cat.db"),
            &tmp.path().join("data"),
        );
        let reader = Reader::start(read_conn, attach_sql, ReaderLimits::default()).expect("reader");

        let engine = QueryEngine::new(
            reader,
            ingestion.clone(),
            GuardrailConfig {
                enforce_catalogue: false,
                ..GuardrailConfig::default()
            },
            Arc::new(FreshnessRegistry::new()),
            SuppressionRules::default(),
        );

        let q = crate::parse::parse(serde_json::json!({
            "source": {"system":"erp","entity":"orders"},
            "key": "user_id",
            "ops": [
                {"kind":"filter","predicate":{"column":"sku","op":"eq","value":"A"}}
            ]
        }))
        .expect("parse");

        let snap = engine
            .materialize(&q, "c1", "default")
            .await
            .expect("materialize");
        assert!(
            snap.starts_with("snap_"),
            "snapshot id must be snap_-prefixed: {snap}"
        );

        let bare = snap.strip_prefix("snap_").expect("snap_ prefix");
        let meta = engine
            .snapshot_meta(bare, "default")
            .await
            .expect("snapshot_meta")
            .expect("snapshot exists");
        assert_eq!(meta.snapshot_id, bare);
        assert_eq!(meta.campaign_id, "c1");
        assert!(meta.row_count > 0, "row count must be > 0");

        ingestion.shutdown();
    }

    #[tokio::test]
    async fn test_should_report_worst_source_freshness() {
        use consumer_engine_core::SourceType;

        let tmp = tempfile::tempdir().expect("tmp");
        let writer = consumer_engine_storage::Writer::attach(
            &tmp.path().join("cat.db"),
            &tmp.path().join("data"),
        )
        .expect("attach");
        // Two sources: a stale batch source (orders) and a fresh cdc source
        // (events).
        writer
            .ingest_raw(
                "erp",
                "orders",
                &["user_id".into(), "sku".into()],
                &[vec![Some("u1".into()), Some("A".into())]],
            )
            .expect("ingest orders");
        writer
            .ingest_raw(
                "erp",
                "events",
                &["user_id".into(), "ts".into()],
                &[vec![
                    Some("u1".into()),
                    Some(chrono::Utc::now().to_rfc3339()),
                ]],
            )
            .expect("ingest events");

        let ingestion = consumer_engine_ingestion::IngestionHandle::start(
            writer,
            Arc::new(consumer_engine_ingestion::ProducerRegistry::new()),
        )
        .expect("start");
        let read_conn = consumer_engine_storage::open_reader(
            &tmp.path().join("cat.db"),
            &tmp.path().join("data"),
        )
        .expect("read attach");
        let attach_sql = consumer_engine_storage::read_only_attach_sql(
            &tmp.path().join("cat.db"),
            &tmp.path().join("data"),
        );
        let reader = Reader::start(read_conn, attach_sql, ReaderLimits::default()).expect("reader");

        let freshness = Arc::new(FreshnessRegistry::new());
        let base = now_epoch();
        // Batch source is 100s staler than the cdc source.
        freshness
            .set("erp", "orders", SourceType::Batch, base - 100)
            .expect("set orders");
        freshness
            .set("erp", "events", SourceType::Cdc, base)
            .expect("set events");

        let engine = QueryEngine::new(
            reader,
            ingestion.clone(),
            GuardrailConfig {
                enforce_catalogue: false,
                ..GuardrailConfig::default()
            },
            Arc::clone(&freshness),
            SuppressionRules::default(),
        );

        // A query touching both sources (orders base intersect events).
        let q = crate::parse::parse(serde_json::json!({
            "source": {"system":"erp","entity":"orders"},
            "key": "user_id",
            "ops": [
                {"kind":"setOp","op":"intersect",
                 "other":{"source":{"system":"erp","entity":"events"},"key":"user_id","ops":[]}}
            ]
        }))
        .expect("parse");
        let res = engine.run_sync(&q, "default").await.expect("run");
        assert_eq!(
            res.freshness.worst_source, "batch",
            "the stale batch source must be reported as worst (graded freshness, D5)"
        );
        assert!(
            res.freshness.lag_seconds >= 100,
            "batch lag must reflect the stale epoch: {}",
            res.freshness.lag_seconds
        );

        ingestion.shutdown();
    }

    /// Shared setup: a writer with an ingested `erp.orders` (columns
    /// user_id, sku, qty) whose catalogue holds exactly `{user_id, sku}`
    /// (written directly — this exercises enforcement, not profiling; `qty`
    /// exists in the raw table but is deliberately uncatalogued). Returns the
    /// tempdir (kept alive), the ingestion handle, and the engine.
    #[allow(clippy::type_complexity)]
    async fn setup_engine(
        guardrails: GuardrailConfig,
    ) -> (
        tempfile::TempDir,
        consumer_engine_ingestion::IngestionHandle,
        QueryEngine,
    ) {
        let tmp = tempfile::tempdir().expect("tmp");
        let writer = consumer_engine_storage::Writer::attach(
            &tmp.path().join("cat.db"),
            &tmp.path().join("data"),
        )
        .expect("attach");
        writer
            .ingest_raw(
                "erp",
                "orders",
                &["user_id".into(), "sku".into(), "qty".into()],
                &[vec![Some("u1".into()), Some("A".into()), Some("3".into())]],
            )
            .expect("ingest");
        writer
            .ensure_feature_store_table()
            .expect("ensure feature store");
        // Register one feature so the Feature-op guardrail can be exercised.
        writer
            .write_feature_rows(&[consumer_engine_core::FeatureRow {
                user_id: "u1".into(),
                feature_name: "cadence.regularity".into(),
                num_value: 1.0,
                as_of_ts: "2025-01-01T00:00:00Z".into(),
                producer_id: "cadence_sql".into(),
            }])
            .expect("write feature");
        // Mirror the producer flow: write rows, then refresh the wide view.
        writer
            .refresh_feature_wide_view("cadence", &["regularity".into()])
            .expect("refresh view");
        // Catalogue = {user_id, sku} only; qty is deliberately uncatalogued.
        let mut catalog = Vec::new();
        for col in ["user_id", "sku"] {
            catalog.push(consumer_engine_core::CatalogRow {
                entity_type: "column".into(),
                system: "erp".into(),
                table_name: "orders".into(),
                column_name: Some(col.into()),
                semantic_type: consumer_engine_core::SemanticType::Identifier,
                data_type: "VARCHAR".into(),
                description: format!("column {col}"),
                pii_flag: false,
                sample_values: serde_json::json!([]),
                embedding: vec![0.0; 4],
                source_epoch: 0,
            });
        }
        writer.write_catalog_rows(&catalog).expect("write catalog");

        let ingestion = consumer_engine_ingestion::IngestionHandle::start(
            writer,
            Arc::new(consumer_engine_ingestion::ProducerRegistry::new()),
        )
        .expect("start");
        let read_conn = consumer_engine_storage::open_reader(
            &tmp.path().join("cat.db"),
            &tmp.path().join("data"),
        )
        .expect("read attach");
        let attach_sql = consumer_engine_storage::read_only_attach_sql(
            &tmp.path().join("cat.db"),
            &tmp.path().join("data"),
        );
        let reader = Reader::start(read_conn, attach_sql, ReaderLimits::default()).expect("reader");
        let engine = QueryEngine::new(
            reader,
            ingestion.clone(),
            guardrails,
            Arc::new(FreshnessRegistry::new()),
            SuppressionRules::default(),
        );
        (tmp, ingestion, engine)
    }

    fn filter_on(column: &str) -> crate::ast::SegmentQuery {
        crate::parse::parse(serde_json::json!({
            "source": {"system":"erp","entity":"orders"},
            "key": "user_id",
            "ops": [{"kind":"filter","predicate":{"column":column,"op":"eq","value":"x"}}]
        }))
        .expect("parse")
    }

    #[tokio::test]
    async fn test_should_allow_catalogued_column() {
        let (_tmp, ingestion, engine) = setup_engine(GuardrailConfig::default()).await;
        let res = engine.run_sync(&filter_on("sku"), "default").await;
        assert!(
            res.is_ok(),
            "catalogued column must pass enforcement: {res:?}"
        );
        ingestion.shutdown();
    }

    #[tokio::test]
    async fn test_should_reject_uncatalogued_column() {
        let (_tmp, ingestion, engine) = setup_engine(GuardrailConfig::default()).await;
        let err = engine
            .run_sync(&filter_on("qty"), "default")
            .await
            .expect_err("uncatalogued column must be rejected");
        assert!(
            matches!(err, QueryError::InvalidDsl(_)),
            "expected InvalidDsl, got {err:?}"
        );
        ingestion.shutdown();
    }

    #[tokio::test]
    async fn test_should_warn_when_catalogue_stale() {
        // Spec 13 I5 / issue #18: a query referencing a column whose newest
        // catalogue entry predates the source's latest ingest must warn (not
        // reject — membership still passes). Capture the tracing output.
        use consumer_engine_core::{CatalogRow, SourceType};
        let tmp = tempfile::tempdir().expect("tmp");
        let writer = consumer_engine_storage::Writer::attach(
            &tmp.path().join("cat.db"),
            &tmp.path().join("data"),
        )
        .expect("attach");
        writer
            .ingest_raw(
                "erp",
                "orders",
                &["user_id".into(), "sku".into()],
                &[vec![Some("u1".into()), Some("A".into())]],
            )
            .expect("ingest");
        // Catalogue built at epoch 1 …
        let mut catalog = Vec::new();
        for col in ["user_id", "sku"] {
            catalog.push(CatalogRow {
                entity_type: "column".into(),
                system: "erp".into(),
                table_name: "orders".into(),
                column_name: Some(col.into()),
                semantic_type: consumer_engine_core::SemanticType::Identifier,
                data_type: "VARCHAR".into(),
                description: format!("column {col}"),
                pii_flag: false,
                sample_values: serde_json::json!([]),
                embedding: vec![0.0; 4],
                source_epoch: 1,
            });
        }
        writer.write_catalog_rows(&catalog).expect("catalog");

        let ingestion = consumer_engine_ingestion::IngestionHandle::start(
            writer,
            Arc::new(consumer_engine_ingestion::ProducerRegistry::new()),
        )
        .expect("start");
        let read_conn = consumer_engine_storage::open_reader(
            &tmp.path().join("cat.db"),
            &tmp.path().join("data"),
        )
        .expect("read attach");
        let attach_sql = consumer_engine_storage::read_only_attach_sql(
            &tmp.path().join("cat.db"),
            &tmp.path().join("data"),
        );
        let reader = Reader::start(read_conn, attach_sql, ReaderLimits::default()).expect("reader");
        // … while the source has since been ingested again (epoch 2).
        let freshness = Arc::new(FreshnessRegistry::new());
        freshness
            .set("erp", "orders", SourceType::Batch, 2)
            .expect("set");
        let engine = QueryEngine::new(
            reader,
            ingestion.clone(),
            GuardrailConfig::default(),
            Arc::clone(&freshness),
            SuppressionRules::default(),
        );

        struct VecWriter(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);
        impl std::io::Write for VecWriter {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0.lock().expect("lock").extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let buf = std::sync::Arc::new(std::sync::Mutex::new(Vec::<u8>::new()));
        let wbuf = std::sync::Arc::clone(&buf);
        let subscriber = tracing_subscriber::fmt()
            .with_writer(move || VecWriter(std::sync::Arc::clone(&wbuf)))
            .with_max_level(tracing::Level::INFO)
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        let q = crate::parse::parse(serde_json::json!({
            "source": {"system":"erp","entity":"orders"},
            "key": "user_id",
            "ops": [{"kind":"filter","predicate":{"column":"sku","op":"eq","value":"A"}}]
        }))
        .expect("parse");
        let res = engine.run_sync(&q, "default").await.expect("query runs");
        assert!(
            !res.rows.is_empty(),
            "stale catalogue must not block the query (warn, not reject)"
        );
        drop(_guard);
        let out = String::from_utf8(buf.lock().expect("buf").clone()).expect("utf8");
        assert!(
            out.contains("semantic catalogue older"),
            "stale catalogue must warn: {out}"
        );

        // Negative case: a FRESH catalogue (epochs equal) must NOT warn.
        freshness
            .set("erp", "orders", SourceType::Batch, 1)
            .expect("set fresh");
        let buf2 = std::sync::Arc::new(std::sync::Mutex::new(Vec::<u8>::new()));
        let wbuf2 = std::sync::Arc::clone(&buf2);
        let sub2 = tracing_subscriber::fmt()
            .with_writer(move || VecWriter(std::sync::Arc::clone(&wbuf2)))
            .with_max_level(tracing::Level::INFO)
            .finish();
        let _guard2 = tracing::subscriber::set_default(sub2);
        let _ = engine.run_sync(&q, "default").await.expect("query runs");
        drop(_guard2);
        let out2 = String::from_utf8(buf2.lock().expect("buf").clone()).expect("utf8");
        assert!(
            !out2.contains("semantic catalogue older"),
            "fresh catalogue must not warn: {out2}"
        );
        ingestion.shutdown();
    }

    #[tokio::test]
    async fn test_should_skip_enforcement_when_disabled() {
        // With enforcement off, the uncatalogued-but-present `qty` column runs
        // (the raw column exists, so the SQL is valid).
        let (_tmp, ingestion, engine) = setup_engine(GuardrailConfig {
            enforce_catalogue: false,
            ..GuardrailConfig::default()
        })
        .await;
        let res = engine.run_sync(&filter_on("qty"), "default").await;
        assert!(res.is_ok(), "enforcement off must not reject: {res:?}");
        ingestion.shutdown();
    }

    #[tokio::test]
    async fn test_should_reject_unregistered_feature() {
        // A Feature op must reference a feature a producer has actually written
        // (issue #6 AC#3: no invented feature names -> 400, not a 500 at SQL).
        let (_tmp, ingestion, engine) = setup_engine(GuardrailConfig::default()).await;
        let ok = crate::parse::parse(serde_json::json!({
            "source": {"system":"erp","entity":"orders"},
            "key": "user_id",
            "ops": [{"kind":"feature","name":"cadence.regularity","op":"gt","value":0.7}]
        }))
        .expect("parse");
        let res = engine.run_sync(&ok, "default").await;
        assert!(res.is_ok(), "registered feature must pass: {res:?}");

        let bad = crate::parse::parse(serde_json::json!({
            "source": {"system":"erp","entity":"orders"},
            "key": "user_id",
            "ops": [{"kind":"feature","name":"cadence.bogus","op":"gt","value":0.7}]
        }))
        .expect("parse");
        let err = engine
            .run_sync(&bad, "default")
            .await
            .expect_err("unregistered feature must be rejected");
        assert!(
            matches!(err, QueryError::InvalidDsl(_)),
            "expected InvalidDsl, got {err:?}"
        );
        ingestion.shutdown();
    }

    /// A JIT-derive fixture: `erp.orders` (user_id, amount) with `amount`
    /// catalogued (plus a `sku` column for narrowing filters, also catalogued).
    #[allow(clippy::type_complexity)]
    async fn derive_engine(
        guardrails: GuardrailConfig,
    ) -> (
        tempfile::TempDir,
        consumer_engine_ingestion::IngestionHandle,
        QueryEngine,
    ) {
        let tmp = tempfile::tempdir().expect("tmp");
        let writer = consumer_engine_storage::Writer::attach(
            &tmp.path().join("cat.db"),
            &tmp.path().join("data"),
        )
        .expect("attach");
        writer
            .ingest_raw(
                "erp",
                "orders",
                &["user_id".into(), "sku".into(), "amount".into()],
                &[
                    vec![Some("u1".into()), Some("A".into()), Some("10".into())],
                    vec![Some("u1".into()), Some("A".into()), Some("20".into())],
                ],
            )
            .expect("ingest");
        let mut catalog = Vec::new();
        for col in ["user_id", "sku", "amount"] {
            catalog.push(consumer_engine_core::CatalogRow {
                entity_type: "column".into(),
                system: "erp".into(),
                table_name: "orders".into(),
                column_name: Some(col.into()),
                semantic_type: consumer_engine_core::SemanticType::Identifier,
                data_type: "VARCHAR".into(),
                description: format!("column {col}"),
                pii_flag: false,
                sample_values: serde_json::json!([]),
                embedding: vec![0.0; 4],
                source_epoch: 0,
            });
        }
        writer.write_catalog_rows(&catalog).expect("write catalog");
        let ingestion = consumer_engine_ingestion::IngestionHandle::start(
            writer,
            Arc::new(consumer_engine_ingestion::ProducerRegistry::new()),
        )
        .expect("start");
        let read_conn = consumer_engine_storage::open_reader(
            &tmp.path().join("cat.db"),
            &tmp.path().join("data"),
        )
        .expect("read attach");
        let attach_sql = consumer_engine_storage::read_only_attach_sql(
            &tmp.path().join("cat.db"),
            &tmp.path().join("data"),
        );
        let reader = Reader::start(read_conn, attach_sql, ReaderLimits::default()).expect("reader");
        let engine = QueryEngine::new(
            reader,
            ingestion.clone(),
            guardrails,
            Arc::new(FreshnessRegistry::new()),
            SuppressionRules::default(),
        );
        (tmp, ingestion, engine)
    }

    #[tokio::test]
    async fn test_should_run_jit_derive_over_survivors() {
        let (_tmp, ingestion, engine) = derive_engine(GuardrailConfig::default()).await;
        let q = crate::parse::parse(serde_json::json!({
            "source": {"system":"erp","entity":"orders"},
            "key": "user_id",
            "ops": [
                {"kind":"filter","predicate":{"column":"sku","op":"eq","value":"A"}},
                {"kind":"derive","name":"total_revenue",
                 "metric":{"kind":"sum","event":{"system":"erp","entity":"orders"},"column":"amount"}}
            ]
        }))
        .expect("parse");
        let res = engine.run_sync(&q, "default").await.expect("run");
        // One row: [name, value]; the single survivor u1's amounts 10+20 = 30
        // (the survivor LIMIT may be 1 for a tiny table, so keep one survivor
        // to make the assertion deterministic).
        assert_eq!(res.columns, vec!["name", "value"]);
        assert_eq!(res.rows.len(), 1);
        assert_eq!(res.rows[0][0], serde_json::json!("total_revenue"));
        assert_eq!(res.rows[0][1], serde_json::json!(30.0));
        ingestion.shutdown();
    }

    #[tokio::test]
    async fn test_should_reject_derive_over_j_survivor_cap() {
        // 100 rows / 50 distinct users: EXPLAIN estimates > 1 survivor, so a
        // cap of 1 must reject the Derive (narrow first or precompute as F).
        let tmp = tempfile::tempdir().expect("tmp");
        let writer = consumer_engine_storage::Writer::attach(
            &tmp.path().join("cat.db"),
            &tmp.path().join("data"),
        )
        .expect("attach");
        let rows: Vec<Vec<Option<String>>> = (0..100)
            .map(|i| {
                vec![
                    Some(format!("u{}", i % 50)),
                    Some("A".into()),
                    Some("10".into()),
                ]
            })
            .collect();
        writer
            .ingest_raw(
                "erp",
                "orders",
                &["user_id".into(), "sku".into(), "amount".into()],
                &rows,
            )
            .expect("ingest");
        let mut catalog = Vec::new();
        for col in ["user_id", "sku", "amount"] {
            catalog.push(consumer_engine_core::CatalogRow {
                entity_type: "column".into(),
                system: "erp".into(),
                table_name: "orders".into(),
                column_name: Some(col.into()),
                semantic_type: consumer_engine_core::SemanticType::Identifier,
                data_type: "VARCHAR".into(),
                description: format!("column {col}"),
                pii_flag: false,
                sample_values: serde_json::json!([]),
                embedding: vec![0.0; 4],
                source_epoch: 0,
            });
        }
        writer.write_catalog_rows(&catalog).expect("write catalog");
        let ingestion = consumer_engine_ingestion::IngestionHandle::start(
            writer,
            Arc::new(consumer_engine_ingestion::ProducerRegistry::new()),
        )
        .expect("start");
        let read_conn = consumer_engine_storage::open_reader(
            &tmp.path().join("cat.db"),
            &tmp.path().join("data"),
        )
        .expect("read attach");
        let attach_sql = consumer_engine_storage::read_only_attach_sql(
            &tmp.path().join("cat.db"),
            &tmp.path().join("data"),
        );
        let reader = Reader::start(read_conn, attach_sql, ReaderLimits::default()).expect("reader");
        let engine = QueryEngine::new(
            reader,
            ingestion.clone(),
            GuardrailConfig {
                j_survivor_cap: 1,
                ..GuardrailConfig::default()
            },
            Arc::new(FreshnessRegistry::new()),
            SuppressionRules::default(),
        );
        // Narrowing + derive over ~50 survivors with cap=1 must reject.
        let q2 = crate::parse::parse(serde_json::json!({
            "source": {"system":"erp","entity":"orders"},
            "key": "user_id",
            "ops": [
                {"kind":"filter","predicate":{"column":"sku","op":"eq","value":"A"}},
                {"kind":"derive","name":"total_revenue",
                 "metric":{"kind":"sum","event":{"system":"erp","entity":"orders"},"column":"amount"}}
            ]
        }))
        .expect("parse");
        let err = engine
            .run_sync(&q2, "default")
            .await
            .expect_err("derive over 2 survivors with cap=1 must be rejected");
        assert!(
            matches!(err, QueryError::SurvivorUnbounded),
            "expected SurvivorUnbounded, got {err:?}"
        );
        ingestion.shutdown();
    }

    #[tokio::test]
    async fn test_should_emit_comparative_profile() {
        let tmp = tempfile::tempdir().expect("tmp");
        let writer = consumer_engine_storage::Writer::attach(
            &tmp.path().join("cat.db"),
            &tmp.path().join("data"),
        )
        .expect("attach");
        // u1 is a high spender (2 orders, one recent); u2 is baseline-only.
        writer
            .ingest_raw(
                "erp",
                "orders",
                &[
                    "user_id".into(),
                    "ts".into(),
                    "amount".into(),
                    "category".into(),
                ],
                &[
                    vec![
                        Some("u1".into()),
                        Some("2025-01-01T00:00:00Z".into()),
                        Some("100".into()),
                        Some("A".into()),
                    ],
                    vec![
                        Some("u1".into()),
                        Some("2025-01-02T00:00:00Z".into()),
                        Some("200".into()),
                        Some("B".into()),
                    ],
                    vec![
                        Some("u2".into()),
                        Some("2025-01-01T00:00:00Z".into()),
                        Some("10".into()),
                        Some("A".into()),
                    ],
                ],
            )
            .expect("ingest");
        let mut catalog = Vec::new();
        for col in ["user_id", "ts", "amount", "category"] {
            catalog.push(consumer_engine_core::CatalogRow {
                entity_type: "column".into(),
                system: "erp".into(),
                table_name: "orders".into(),
                column_name: Some(col.into()),
                semantic_type: consumer_engine_core::SemanticType::Identifier,
                data_type: "VARCHAR".into(),
                description: format!("column {col}"),
                pii_flag: false,
                sample_values: serde_json::json!([]),
                embedding: vec![0.0; 4],
                source_epoch: 0,
            });
        }
        writer.write_catalog_rows(&catalog).expect("write catalog");
        let ingestion = consumer_engine_ingestion::IngestionHandle::start(
            writer,
            Arc::new(consumer_engine_ingestion::ProducerRegistry::new()),
        )
        .expect("start");
        let read_conn = consumer_engine_storage::open_reader(
            &tmp.path().join("cat.db"),
            &tmp.path().join("data"),
        )
        .expect("read attach");
        let attach_sql = consumer_engine_storage::read_only_attach_sql(
            &tmp.path().join("cat.db"),
            &tmp.path().join("data"),
        );
        let reader = Reader::start(read_conn, attach_sql, ReaderLimits::default()).expect("reader");
        let engine = QueryEngine::new(
            reader,
            ingestion.clone(),
            GuardrailConfig::default(),
            Arc::new(FreshnessRegistry::new()),
            SuppressionRules::default(),
        );

        // Segment = users with an order on/after 2025-01-02 (only u1); the
        // profile compares u1 to the whole population (u1 + u2).
        let q = crate::parse::parse(serde_json::json!({
            "source": {"system":"erp","entity":"orders"},
            "key": "user_id",
            "ops": [
                {"kind":"filter","predicate":{"column":"ts","op":"ge","value":"2025-01-02T00:00:00Z"}},
                {"kind":"characterize",
                 "event":{"system":"erp","entity":"orders"},
                 "tsColumn":"ts","monetaryColumn":"amount","categoryColumn":"category"}
            ]
        }))
        .expect("parse");
        let res = engine.run_sync(&q, "default").await.expect("run");
        assert_eq!(res.columns, vec!["profile"]);
        let p = &res.rows[0][0];
        // Segment = {u1}; baseline = {u1, u2}.
        assert_eq!(p["segment"]["users"], serde_json::json!(1));
        assert_eq!(p["baseline"]["users"], serde_json::json!(2));
        // AOV: segment (100+200)/2 = 150; baseline (100+200+10)/3 ~ 103.33.
        assert!(
            (p["segment"]["averageOrderValue"].as_f64().unwrap() - 150.0).abs() < 0.01,
            "segment AOV: {}",
            p["segment"]["averageOrderValue"]
        );
        assert!((p["baseline"]["averageOrderValue"].as_f64().unwrap() - 103.3333).abs() < 0.01);
        // Ratio: 150 / 103.33 ~ 1.45x the baseline.
        assert!(
            (p["ratios"]["averageOrderValue"].as_f64().unwrap() - 1.4516).abs() < 0.01,
            "aov ratio: {}",
            p["ratios"]["averageOrderValue"]
        );
        // Frequency: segment 2 orders / 1 user = 2; baseline 3/2 = 1.5.
        assert!((p["segment"]["frequency"].as_f64().unwrap() - 2.0).abs() < 0.01);
        assert!((p["baseline"]["frequency"].as_f64().unwrap() - 1.5).abs() < 0.01);
        // Category mix is non-empty with shares summing to 1.
        let seg_mix = p["segment"]["categoryMix"].as_array().expect("mix");
        assert!(!seg_mix.is_empty());
        let share_sum: f64 = seg_mix
            .iter()
            .map(|v| v["share"].as_f64().unwrap_or(0.0))
            .sum();
        assert!(
            (share_sum - 1.0).abs() < 0.01,
            "segment category shares must sum to 1: {seg_mix:?}"
        );
        ingestion.shutdown();
    }
    #[tokio::test]
    async fn test_should_bound_snapshot_as_of_and_frozen_features() {
        // I3 (issue #21): the snapshot's `as_of_ts` = the MINIMUM source
        // freshness (not the materialisation wall-clock), and the frozen
        // features exclude values written AFTER the cut-off.
        use consumer_engine_core::{FeatureRow, SourceType};
        let tmp = tempfile::tempdir().expect("tmp");
        let writer = consumer_engine_storage::Writer::attach(
            &tmp.path().join("cat.db"),
            &tmp.path().join("data"),
        )
        .expect("attach");
        writer
            .ingest_raw(
                "erp",
                "orders",
                &["user_id".into(), "sku".into()],
                &[vec![Some("u1".into()), Some("A".into())]],
            )
            .expect("ingest");
        // Two feature values: an early one (0.9) and a late one (0.1) written
        // AFTER the source's freshness cut-off.
        writer
            .write_feature_rows(&[
                FeatureRow {
                    user_id: "u1".into(),
                    feature_name: "cadence.regularity".into(),
                    num_value: 0.9,
                    as_of_ts: "2025-01-01T00:00:00Z".into(),
                    producer_id: "cadence_sql".into(),
                },
                FeatureRow {
                    user_id: "u1".into(),
                    feature_name: "cadence.regularity".into(),
                    num_value: 0.1,
                    as_of_ts: "2025-06-01T00:00:00Z".into(),
                    producer_id: "cadence_sql".into(),
                },
            ])
            .expect("features");
        writer
            .refresh_feature_wide_view("cadence", &["regularity".into()])
            .expect("view");
        // Source cut-off = 2025-03-01 (between the two feature as_of times).
        let cut_off = chrono::DateTime::parse_from_rfc3339("2025-03-01T00:00:00Z")
            .expect("parse")
            .timestamp();
        let freshness = Arc::new(FreshnessRegistry::new());
        freshness
            .set("erp", "orders", SourceType::Batch, cut_off)
            .expect("set");
        let ingestion = consumer_engine_ingestion::IngestionHandle::start(
            writer,
            Arc::new(consumer_engine_ingestion::ProducerRegistry::new()),
        )
        .expect("start");
        let read_conn = consumer_engine_storage::open_reader(
            &tmp.path().join("cat.db"),
            &tmp.path().join("data"),
        )
        .expect("read attach");
        let attach_sql = consumer_engine_storage::read_only_attach_sql(
            &tmp.path().join("cat.db"),
            &tmp.path().join("data"),
        );
        let reader = Reader::start(read_conn, attach_sql, ReaderLimits::default()).expect("reader");
        let engine = QueryEngine::new(
            reader,
            ingestion.clone(),
            GuardrailConfig {
                enforce_catalogue: false,
                ..GuardrailConfig::default()
            },
            freshness,
            SuppressionRules::default(),
        );
        let q = crate::parse::parse(serde_json::json!({
            "source": {"system":"erp","entity":"orders"},
            "key": "user_id",
            "ops": [{"kind":"filter","predicate":{"column":"sku","op":"eq","value":"A"}}]
        }))
        .expect("parse");
        let snap = engine
            .materialize(&q, "c1", "default")
            .await
            .expect("materialize");
        let bare = snap.strip_prefix("snap_").expect("prefix");
        let meta = engine
            .snapshot_meta(bare, "default")
            .await
            .expect("meta")
            .expect("snapshot exists");
        assert!(
            meta.as_of_ts.starts_with("2025-03-01"),
            "snapshot as_of_ts must be the min source freshness, got {}",
            meta.as_of_ts
        );
        // The frozen feature must be the pre-cut-off value (0.9), NOT the
        // post-cut-off value (0.1) — I3: no post-as_of data in the snapshot.
        let r = consumer_engine_storage::open_reader(
            &tmp.path().join("cat.db"),
            &tmp.path().join("data"),
        )
        .expect("read attach 2");
        let features: String = r
            .query_row(
                "SELECT features FROM dro.audience_snapshot LIMIT 1",
                [],
                |row| row.get(0),
            )
            .expect("features cell");
        assert!(
            features.contains("0.9"),
            "frozen features must carry the pre-cut-off value: {features}"
        );
        assert!(
            !features.contains("0.1"),
            "frozen features must EXCLUDE the post-cut-off value (I3 leak): {features}"
        );
        ingestion.shutdown();
    }

    #[tokio::test]
    async fn test_should_feature_predicate_respect_as_of() {
        // Point-in-time (issue #21 / spec 10 I4): with an `asOf`, the Feature
        // predicate reads the EAV bounded by `as_of_ts <= asOf` and uses the
        // value at the newest as_of within that window — not the latest-wins
        // wide view.
        use consumer_engine_core::FeatureRow;
        let tmp = tempfile::tempdir().expect("tmp");
        let writer = consumer_engine_storage::Writer::attach(
            &tmp.path().join("cat.db"),
            &tmp.path().join("data"),
        )
        .expect("attach");
        writer
            .ingest_raw(
                "erp",
                "orders",
                &["user_id".into()],
                &[vec![Some("u1".into())]],
            )
            .expect("ingest");
        writer
            .write_feature_rows(&[
                FeatureRow {
                    user_id: "u1".into(),
                    feature_name: "cadence.regularity".into(),
                    num_value: 0.9,
                    as_of_ts: "2025-01-01T00:00:00Z".into(),
                    producer_id: "cadence_sql".into(),
                },
                FeatureRow {
                    user_id: "u1".into(),
                    feature_name: "cadence.regularity".into(),
                    num_value: 0.1,
                    as_of_ts: "2025-06-01T00:00:00Z".into(),
                    producer_id: "cadence_sql".into(),
                },
            ])
            .expect("features");
        writer
            .refresh_feature_wide_view("cadence", &["regularity".into()])
            .expect("view");
        let ingestion = consumer_engine_ingestion::IngestionHandle::start(
            writer,
            Arc::new(consumer_engine_ingestion::ProducerRegistry::new()),
        )
        .expect("start");
        let read_conn = consumer_engine_storage::open_reader(
            &tmp.path().join("cat.db"),
            &tmp.path().join("data"),
        )
        .expect("read attach");
        let attach_sql = consumer_engine_storage::read_only_attach_sql(
            &tmp.path().join("cat.db"),
            &tmp.path().join("data"),
        );
        let reader = Reader::start(read_conn, attach_sql, ReaderLimits::default()).expect("reader");
        let engine = QueryEngine::new(
            reader,
            ingestion.clone(),
            GuardrailConfig {
                enforce_catalogue: false,
                ..GuardrailConfig::default()
            },
            Arc::new(FreshnessRegistry::new()),
            SuppressionRules::default(),
        );
        // asOf = the early window → 0.9 > 0.5 → u1 matches.
        let early = crate::parse::parse(serde_json::json!({
            "source": {"system":"erp","entity":"orders"}, "key": "user_id",
            "asOf": "2025-03-01T00:00:00Z",
            "ops": [{"kind":"feature","name":"cadence.regularity","op":"gt","value":0.5}]
        }))
        .expect("parse");
        let res = engine
            .run_sync(&early, "default")
            .await
            .expect("early query");
        assert!(
            !res.rows.is_empty(),
            "asOf at the early window must match (0.9 > 0.5)"
        );
        // asOf = the late window → 0.1 <= 0.5 → u1 does NOT match (PIT wins
        // over the latest-wins wide view).
        let late = crate::parse::parse(serde_json::json!({
            "source": {"system":"erp","entity":"orders"}, "key": "user_id",
            "asOf": "2025-06-01T00:00:00Z",
            "ops": [{"kind":"feature","name":"cadence.regularity","op":"gt","value":0.5}]
        }))
        .expect("parse");
        let res = engine.run_sync(&late, "default").await.expect("late query");
        assert!(
            res.rows.is_empty(),
            "asOf at the late window must NOT match (0.1 <= 0.5)"
        );
        // A junk asOf is rejected at the boundary.
        let bad = crate::parse::parse(serde_json::json!({
            "source": {"system":"erp","entity":"orders"}, "key": "user_id",
            "asOf": "not-a-timestamp",
            "ops": [{"kind":"feature","name":"cadence.regularity","op":"gt","value":0.5}]
        }));
        assert!(bad.is_err(), "junk asOf must be rejected");
        ingestion.shutdown();
    }
}
