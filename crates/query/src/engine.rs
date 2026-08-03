//! The query engine: parse → compile → guard → run (sync), plus the Q2
//! materialise bridge (`materialize` → `snap_<uuid>`). The job lifecycle
//! (jobId mint, poll) lives in the ingress layer (`consumer_engine-ingress`);
//! here we only do the materialise work and snapshot metadata read.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use consumer_engine_core::{
    BoxError, Error, Freshness, FreshnessRegistry, GuardrailConfig, READ_ONLY_CATALOG_ALIAS,
    SnapshotSpec, WRITE_CATALOG_ALIAS,
};
use consumer_engine_execution::{QueryResult, Reader, RowCells};
use consumer_engine_ingestion::IngestionHandle;
use duckdb::types::Value;
use tokio::sync::Semaphore;

use crate::{
    ast::SegmentQuery,
    compiler::{CompiledQuery, compile, compile_with_alias},
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
    ) -> Self {
        let permits = guardrails.threads.max(1);
        Self {
            reader,
            ingestion,
            guardrails,
            inflight: Arc::new(Semaphore::new(permits)),
            freshness,
        }
    }

    /// Run a DSL JSON value end-to-end: parse/validate → compile → guard → run.
    ///
    /// # Errors
    /// Propagates parse, guardrail, and execution errors.
    pub async fn run(&self, dsl: serde_json::Value) -> Result<SyncResult> {
        let q = crate::parse::parse(dsl)?;
        self.run_sync(&q).await
    }

    /// Run a read-only catalogue/feature-store query under the statement
    /// timeout guardrail (AGENTS.md § Resource Limits: every storage read needs
    /// a timeout — a stalled probe must not block the single reader thread).
    async fn catalogue_read(&self, sql: &str, params: Vec<Value>) -> Result<QueryResult> {
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
    /// equally (a materialise IS a query, spec 12 §2a).
    async fn compile_and_check(&self, q: &SegmentQuery) -> Result<CompiledQuery> {
        let compiled = compile(q)?;
        self.enforce_catalogue(q).await?;
        Ok(compiled)
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
    async fn enforce_catalogue(&self, q: &SegmentQuery) -> Result<()> {
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
                .catalogue_read(
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
        }
        for name in crate::ast::referenced_features(q) {
            let qr = self
                .catalogue_read(
                    &format!(
                        "SELECT 1 FROM {READ_ONLY_CATALOG_ALIAS}.feature_store WHERE feature_name \
                         = ? LIMIT 1"
                    ),
                    vec![Value::Text(name.clone())],
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
    pub async fn plan(&self, q: &SegmentQuery) -> Result<Plan> {
        let (plan, _compiled) = self.prepare(q).await?;
        Ok(plan)
    }

    /// Compile, run the EXPLAIN pre-flight, enforce budgets, and choose a mode.
    /// Returns the [`Plan`] and the [`CompiledQuery`] (for execution).
    ///
    /// # Errors
    /// Propagates compile/guardrail errors.
    async fn prepare(&self, q: &SegmentQuery) -> Result<(Plan, CompiledQuery)> {
        let compiled = self.compile_and_check(q).await?;
        let est = explain_cost(&self.reader, &compiled).await;
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
    pub async fn run_sync(&self, q: &SegmentQuery) -> Result<SyncResult> {
        // Concurrency cap (acquired before the EXPLAIN pre-flight + execute).
        let _permit = self
            .inflight
            .acquire()
            .await
            .map_err(|e| QueryError::Execution {
                source: Error::Execution(BoxError::from(e)),
            })?;

        // Pre-flight: compile + EXPLAIN + enforce budgets (AC#3: over-budget is
        // rejected here, before the query executes).
        let (plan, compiled) = self.prepare(q).await?;
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

    /// Materialise a validated DSL segment into `audience_snapshot` via the
    /// single writer, returning the opaque snapshot id `snap_<uuidv7>`.
    ///
    /// This is the Q2 work half of the async path; the REST job lifecycle
    /// (jobId mint/poll) lives in `consumer_engine-ingress`. Guardrails are
    /// **not** enforced here — a large result set is the whole point of
    /// materialising — but the T2 `EXPLAIN` is run best-effort so a malformed
    /// segment fails fast with a clear error (`specs/20 I4`).
    ///
    /// `as_of_ts` is materialisation time in M2 (true I3 point-in-time bounding
    /// lands in T4); `features` is the non-null placeholder `{}` (Feature Store
    /// is T4); `hit_reason` is the serialised validated DSL — a faithful
    /// per-row selection reason for B-only segments.
    ///
    /// # Errors
    /// - [`QueryError::InvalidDsl`] if the segment fails to compile or the DSL cannot be serialised
    ///   as `hit_reason`.
    /// - [`QueryError::Execution`] propagating ingestion/storage failures.
    pub async fn materialize(&self, q: &SegmentQuery, campaign_id: &str) -> Result<String> {
        // Best-effort EXPLAIN: validates the segment compiles under the read
        // alias and surfaces a bad-DSL failure early, but does NOT enforce row
        // budgets (large is the point of materialising). Errors from EXPLAIN
        // (a real compile error) are surfaced via `compile` below.
        let compiled = self.compile_and_check(q).await?;
        let est = explain_cost(&self.reader, &compiled).await;
        tracing::info!(est_rows = est.est_rows, "materialise estimate");

        // Scalars.
        let snapshot_id = uuid::Uuid::now_v7().to_string();
        let as_of_ts = chrono::Utc::now().to_rfc3339();
        let features = "{}".to_string();
        let hit_reason = serde_json::to_string(q)
            .map_err(|e| QueryError::InvalidDsl(format!("serialize hit_reason: {e}")))?;

        // Write path: recompile under the writable alias so the writer's
        // `INSERT … SELECT` resolves `dl.raw_*`.
        let write = compile_with_alias(q, WRITE_CATALOG_ALIAS)?;
        let spec = SnapshotSpec {
            snapshot_id: snapshot_id.clone(),
            campaign_id: campaign_id.to_string(),
            as_of_ts,
            features,
            hit_reason,
        };
        self.ingestion
            .materialize_snapshot(&write.sql, write.params, &q.key, spec)
            .await?;
        Ok(format!("snap_{snapshot_id}"))
    }

    /// Read a snapshot's metadata via the read-only reader. Returns `None` if no
    /// rows exist for `snap_uuid`. `as_of_ts` and the JSON columns are cast to
    /// `VARCHAR` because `execution::value_to_json` maps TIMESTAMPTZ/JSON to
    /// null today.
    ///
    /// # Errors
    /// [`QueryError::Execution`] on reader failure.
    pub async fn snapshot_meta(&self, snap_uuid: &str) -> Result<Option<SnapshotMeta>> {
        const SQL: &str = "SELECT campaign_id, CAST(as_of_ts AS VARCHAR), count(*) FROM \
                           dro.audience_snapshot WHERE snapshot_id = CAST(? AS UUID) GROUP BY \
                           campaign_id, as_of_ts";
        let qr = self
            .reader
            .query_with_params(SQL, vec![Value::Text(snap_uuid.to_string())])
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

/// Current epoch seconds (0 if the clock is before the epoch).
fn now_epoch() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use consumer_engine_core::{FreshnessRegistry, GuardrailConfig};
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
        );

        let q = crate::parse::parse(serde_json::json!({
            "source": {"system":"erp","entity":"orders"},
            "key": "user_id",
            "ops": [
                {"kind":"filter","predicate":{"column":"sku","op":"eq","value":"A"}}
            ]
        }))
        .expect("parse");

        let snap = engine.materialize(&q, "c1").await.expect("materialize");
        assert!(
            snap.starts_with("snap_"),
            "snapshot id must be snap_-prefixed: {snap}"
        );

        let bare = snap.strip_prefix("snap_").expect("snap_ prefix");
        let meta = engine
            .snapshot_meta(bare)
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
        let res = engine.run_sync(&q).await.expect("run");
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
        let res = engine.run_sync(&filter_on("sku")).await;
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
            .run_sync(&filter_on("qty"))
            .await
            .expect_err("uncatalogued column must be rejected");
        assert!(
            matches!(err, QueryError::InvalidDsl(_)),
            "expected InvalidDsl, got {err:?}"
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
        let res = engine.run_sync(&filter_on("qty")).await;
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
        let res = engine.run_sync(&ok).await;
        assert!(res.is_ok(), "registered feature must pass: {res:?}");

        let bad = crate::parse::parse(serde_json::json!({
            "source": {"system":"erp","entity":"orders"},
            "key": "user_id",
            "ops": [{"kind":"feature","name":"cadence.bogus","op":"gt","value":0.7}]
        }))
        .expect("parse");
        let err = engine
            .run_sync(&bad)
            .await
            .expect_err("unregistered feature must be rejected");
        assert!(
            matches!(err, QueryError::InvalidDsl(_)),
            "expected InvalidDsl, got {err:?}"
        );
        ingestion.shutdown();
    }
}
