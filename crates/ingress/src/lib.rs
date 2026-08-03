//! REST ingress — the single trust boundary (decision D13).
//!
//! `POST /query` runs the DSL happy path (`{dsl}`) through `QueryEngine`;
//! raw SQL (`{sql}`) is the escape-hatch and is **rejected in M1** (approval
//! token wiring lands later). `POST /sources/onboard` and `GET /healthz` are
//! unchanged from T1. All boundary values are validated/capped (AGENTS.md §
//! Input Validation); query results carry a graded `freshness` label (D5).

#![forbid(unsafe_code)]
#![warn(rust_2024_compatibility, missing_docs, missing_debug_implementations)]

use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use consumer_engine_core::{Error, Freshness, FreshnessRegistry, SourceType, validate_ident};
use consumer_engine_execution::RowCells;
use consumer_engine_ingestion::IngestionHandle;
use consumer_engine_query::{QueryEngine, QueryError};
use consumer_engine_semantic::{IntentRag, Profiler};
use serde::{Deserialize, Serialize};

pub mod audience;
pub mod catalog;
pub mod jobs;
pub mod presign;
pub mod producers;
pub mod suppression;

pub use jobs::{JobRegistry, JobStatus};

/// Maximum rows accepted in a single onboard request.
const MAX_ONBOARD_ROWS: usize = 200_000;
/// Maximum number of columns in an onboard request.
const MAX_COLUMNS: usize = 1024;
/// Maximum size of a raw-SQL escape-hatch string, in bytes.
const MAX_SQL_BYTES: usize = 8_192;
/// Maximum size of a single onboarded cell value, in bytes.
const MAX_CELL_BYTES: usize = 4_096;
/// Request body limit.
const BODY_LIMIT: usize = 10 * 1024 * 1024;
/// Wall-clock budget for the onboarding profile step (spec 21 I5: bounded).
const PROFILE_TIMEOUT_SECS: u64 = 5;

/// Shared state injected into handlers.
#[derive(Clone, Debug)]
pub struct AppState {
    /// The single ingestion writer handle.
    pub ingestion: IngestionHandle,
    /// The query engine (DSL → guarded SQL).
    pub query_engine: QueryEngine,
    /// Graded per-source freshness registry (D5).
    pub freshness: Arc<FreshnessRegistry>,
    /// The L0 onboarding profiler (spec 13).
    pub profiler: Arc<Profiler>,
    /// The L1 Intent RAG retriever (spec 13).
    pub intent_rag: Arc<IntentRag>,
    /// Async-job registry for `POST /jobs` / `GET /jobs/:id`.
    pub jobs: Arc<JobRegistry>,
    /// Concurrency cap for materialisation jobs (bound in-flight work).
    pub materialise_slots: Arc<tokio::sync::Semaphore>,
    /// HMAC-SHA256 signing key for presigned export URLs (32 bytes of OS
    /// randomness; minted once at server startup).
    pub signing_key: Arc<[u8; 32]>,
}

/// `POST /sources/onboard` request body.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OnboardRequest {
    system: String,
    entity: String,
    columns: Vec<String>,
    rows: Vec<Vec<Option<String>>>,
    /// Source adapter kind (default `"batch"`; `"cdc"` for change-data-capture).
    #[serde(default)]
    source_type: Option<String>,
}

/// `POST /sources/onboard` response body.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OnboardResponse {
    rows_inserted: usize,
    /// Whether the L0 Profiler successfully profiled + catalogued the table.
    profiled: bool,
    /// The profiled column names (empty if profiling failed/timed out).
    columns: Vec<String>,
}

/// `POST /query` request body. Provide `dsl` (happy path) or `sql` (escape
/// hatch — rejected in M1 without a valid approval token).
#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct QueryRequest {
    /// The DSL segment query (raw JSON; parsed/validated by the query engine).
    #[serde(default)]
    dsl: Option<serde_json::Value>,
    /// Raw SQL escape hatch (M1: rejected).
    #[serde(default)]
    sql: Option<String>,
    /// Approval token for the escape hatch (M1: not honoured).
    #[serde(default)]
    #[allow(
        dead_code,
        reason = "forward-contract field for the escape-hatch approval gate (spec 21 §4)"
    )]
    approval_token: Option<String>,
}

/// Redacting `Debug` (specs/70, AC: no auth token in logs): the approval token
/// is never printed, only a `[REDACTED]` marker.
impl std::fmt::Debug for QueryRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryRequest")
            .field("dsl", &self.dsl)
            .field("sql", &self.sql)
            .field("approvalToken", &"[REDACTED]")
            .finish()
    }
}

/// `POST /query` response body.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryResponse {
    columns: Vec<String>,
    rows: Vec<RowCells>,
    count: u64,
    freshness: Freshness,
    query_id: String,
}

/// Build the router with `state` and a bounded request body.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/sources/onboard", post(onboard))
        .route("/query", post(query))
        .route("/catalog", get(catalog::get_catalog))
        .route("/producers/run", post(producers::run_producer))
        .route("/suppression", post(suppression::post_suppression))
        .route("/jobs", post(jobs::post_jobs))
        .route("/jobs/{id}", get(jobs::get_job))
        .route("/audience/{snapshot_id}", get(audience::get_audience))
        .route("/audience/{snapshot_id}/export", get(audience::get_export))
        .layer(axum::extract::DefaultBodyLimit::max(BODY_LIMIT))
        .with_state(state)
}

/// `GET /healthz`.
async fn healthz() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

/// `POST /sources/onboard`.
async fn onboard(
    State(state): State<AppState>,
    Json(req): Json<OnboardRequest>,
) -> Result<Json<OnboardResponse>, ApiError> {
    validate_ident(&req.system)?;
    validate_ident(&req.entity)?;
    if req.columns.is_empty() {
        return Err(Error::InvalidInput("columns must not be empty".into()).into());
    }
    if req.columns.len() > MAX_COLUMNS {
        return Err(Error::InvalidInput(format!("columns exceed cap of {MAX_COLUMNS}")).into());
    }
    for c in &req.columns {
        validate_ident(c)?;
    }
    if req.rows.len() > MAX_ONBOARD_ROWS {
        return Err(Error::InvalidInput(format!("rows exceed cap of {MAX_ONBOARD_ROWS}")).into());
    }
    let expected = req.columns.len();
    for (i, row) in req.rows.iter().enumerate() {
        if row.len() != expected {
            return Err(Error::InvalidInput(format!(
                "row {i} has {} cells, expected {expected}",
                row.len()
            ))
            .into());
        }
        for cell in row.iter().flatten() {
            if cell.len() > MAX_CELL_BYTES {
                return Err(Error::InvalidInput(format!(
                    "row {i} cell exceeds {MAX_CELL_BYTES} bytes"
                ))
                .into());
            }
        }
    }
    let n = state
        .ingestion
        .ingest_raw(&req.system, &req.entity, req.columns, req.rows)
        .await?;
    let source_type = parse_source_type(&req.source_type)?;
    state
        .freshness
        .set(&req.system, &req.entity, source_type, now_epoch())?;

    // Profile with a wall-clock budget (spec 21 I5); a timeout/failure degrades
    // to `profiled=false` rather than failing the whole onboard (the rows are
    // already ingested).
    let (profiled, columns) = match tokio::time::timeout(
        Duration::from_secs(PROFILE_TIMEOUT_SECS),
        state.profiler.onboard(&req.system, &req.entity),
    )
    .await
    {
        Ok(Ok(rows)) => {
            let cols: Vec<String> = rows.iter().filter_map(|r| r.column_name.clone()).collect();
            match state.ingestion.write_catalog(rows).await {
                Ok(_) => (true, cols),
                Err(e) => {
                    tracing::warn!(error = %e, "catalog write failed after profile");
                    (false, Vec::new())
                }
            }
        }
        Ok(Err(e)) => {
            tracing::warn!(error = %e, "profile failed");
            (false, Vec::new())
        }
        Err(_) => {
            tracing::warn!("profile timed out");
            (false, Vec::new())
        }
    };
    Ok(Json(OnboardResponse {
        rows_inserted: n,
        profiled,
        columns,
    }))
}

/// Parse the `sourceType` field into a [`SourceType`], defaulting to batch.
///
/// # Errors
/// [`ApiError::Core`] for an unknown source-type label (reject, don't coerce).
fn parse_source_type(s: &Option<String>) -> Result<SourceType, ApiError> {
    match s.as_deref() {
        None | Some("batch") => Ok(SourceType::Batch),
        Some("cdc") => Ok(SourceType::Cdc),
        Some(other) => Err(ApiError::Core(Error::InvalidInput(format!(
            "unknown sourceType {other:?}"
        )))),
    }
}

/// `POST /query`.
async fn query(
    State(state): State<AppState>,
    Json(req): Json<QueryRequest>,
) -> Result<Json<QueryResponse>, ApiError> {
    let res = match (req.dsl, req.sql) {
        (Some(dsl), _) => state.query_engine.run(dsl).await?,
        (None, Some(sql)) => {
            // Cap the escape-hatch payload even though it is rejected in M1.
            if sql.len() > MAX_SQL_BYTES {
                return Err(
                    Error::InvalidInput(format!("sql exceeds {MAX_SQL_BYTES} bytes")).into(),
                );
            }
            // M1: the raw-SQL escape hatch is closed. The approval-token gate
            // lands with the DSL's long tail (spec 21 §4).
            return Err(ApiError::Query(QueryError::InvalidDsl(
                "raw-SQL escape hatch is not enabled in M1; submit a 'dsl' instead".into(),
            )));
        }
        (None, None) => {
            return Err(Error::InvalidInput(
                "provide a 'dsl' (or 'sql' with an approval token — not enabled in M1)".into(),
            )
            .into());
        }
    };
    Ok(Json(QueryResponse {
        columns: res.columns,
        rows: res.rows,
        count: res.count,
        freshness: res.freshness,
        query_id: res.query_id,
    }))
}

/// Current epoch seconds (0 if the clock is before the epoch).
fn now_epoch() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// HTTP error wrapper for either a core [`Error`] or a [`QueryError`].
#[derive(Debug)]
pub enum ApiError {
    /// A core / ingress error.
    Core(Error),
    /// A query-engine error.
    Query(QueryError),
    /// The requested resource does not exist (job, snapshot).
    NotFound,
    /// Authentication / authorization failed (e.g. invalid presigned token).
    Unauthorized,
    /// The requested export format is not supported.
    UnsupportedFormat,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (code, msg): (StatusCode, String) = match self {
            ApiError::Core(Error::InvalidInput(m)) => (StatusCode::BAD_REQUEST, m),
            ApiError::Core(Error::WriterAlreadyHeld) => {
                (StatusCode::CONFLICT, "writer already held".into())
            }
            ApiError::Core(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
            ApiError::Query(QueryError::InvalidDsl(m)) => (StatusCode::BAD_REQUEST, m),
            ApiError::Query(QueryError::TooLarge) => (
                StatusCode::PAYLOAD_TOO_LARGE,
                "query too large for sync".into(),
            ),
            ApiError::Query(QueryError::Guardrail { rule, limit }) => (
                StatusCode::UNPROCESSABLE_ENTITY,
                format!("guardrail {rule} exceeded (limit {limit})"),
            ),
            ApiError::Query(QueryError::SurvivorUnbounded) => (
                StatusCode::UNPROCESSABLE_ENTITY,
                "JIT derive over unbounded survivor set".into(),
            ),
            ApiError::Query(QueryError::Execution { .. }) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "execution failure".into(),
            ),
            // QueryError is non_exhaustive; future variants map to 500.
            ApiError::Query(_) => (StatusCode::INTERNAL_SERVER_ERROR, "query failure".into()),
            ApiError::NotFound => (StatusCode::NOT_FOUND, "not found".into()),
            ApiError::Unauthorized => (StatusCode::UNAUTHORIZED, "unauthorized".into()),
            ApiError::UnsupportedFormat => (
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "unsupported format".into(),
            ),
        };
        (code, msg).into_response()
    }
}

impl From<Error> for ApiError {
    fn from(e: Error) -> Self {
        Self::Core(e)
    }
}

impl From<QueryError> for ApiError {
    fn from(e: QueryError) -> Self {
        Self::Query(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_should_redact_approval_token_in_debug() {
        // specs/70: no auth token in log output. The Debug representation must
        // never contain the token value, only a [REDACTED] marker.
        let req = QueryRequest {
            dsl: Some(serde_json::json!({ "source": {"system":"erp","entity":"users"} })),
            sql: None,
            approval_token: Some("super-secret-token-12345".into()),
        };
        let debug = format!("{req:?}");
        assert!(
            !debug.contains("super-secret-token-12345"),
            "approval token must never appear in Debug output: {debug}"
        );
        assert!(
            debug.contains("[REDACTED]"),
            "marker must be present: {debug}"
        );
    }

    #[test]
    fn test_should_not_leak_token_in_formatted_log_output() {
        // specs/70 I5: no auth token in *formatted log output*. Capture tracing
        // output while an error is logged with the request's Debug and assert
        // the token value is absent.
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
        let writer = std::sync::Arc::clone(&buf);
        let subscriber = tracing_subscriber::fmt()
            .with_writer(move || VecWriter(std::sync::Arc::clone(&writer)))
            .with_max_level(tracing::Level::INFO)
            .finish();
        let req = QueryRequest {
            dsl: None,
            sql: None,
            approval_token: Some("tok-leak-check-9876".into()),
        };
        tracing::subscriber::with_default(subscriber, || {
            tracing::error!(request = ?req, "handler rejected request");
        });
        let out = String::from_utf8(buf.lock().expect("buf").clone()).expect("utf8");
        assert!(
            !out.contains("tok-leak-check-9876"),
            "token must not appear in formatted log output: {out}"
        );
        assert!(out.contains("[REDACTED]"), "marker must appear: {out}");
    }
}
