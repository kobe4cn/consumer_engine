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
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use consumer_engine_core::{Error, Freshness, validate_ident};
use consumer_engine_execution::RowCells;
use consumer_engine_ingestion::IngestionHandle;
use consumer_engine_query::{QueryEngine, QueryError};
use serde::{Deserialize, Serialize};

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

/// Shared state injected into handlers.
#[derive(Clone, Debug)]
pub struct AppState {
    /// The single ingestion writer handle.
    pub ingestion: IngestionHandle,
    /// The query engine (DSL → guarded SQL).
    pub query_engine: QueryEngine,
    /// Epoch seconds of the last successful ingest (drives the freshness label).
    pub last_ingest_epoch: Arc<AtomicI64>,
}

/// `POST /sources/onboard` request body.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OnboardRequest {
    system: String,
    entity: String,
    columns: Vec<String>,
    rows: Vec<Vec<Option<String>>>,
}

/// `POST /sources/onboard` response body.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OnboardResponse {
    rows_inserted: usize,
}

/// `POST /query` request body. Provide `dsl` (happy path) or `sql` (escape
/// hatch — rejected in M1 without a valid approval token).
#[derive(Debug, Deserialize, Default)]
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
    state
        .last_ingest_epoch
        .store(now_epoch(), Ordering::Relaxed);
    Ok(Json(OnboardResponse { rows_inserted: n }))
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
            _ => (StatusCode::INTERNAL_SERVER_ERROR, "query failure".into()),
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
