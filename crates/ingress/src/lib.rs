//! REST ingress — the single trust boundary (decision D13).
//!
//! Exposes the T1 stand-in surface: `POST /sources/onboard` (batch ingest into
//! `raw_*`), `POST /query` (a trivial read-only SQL-over-REST, replaced by the
//! DSL in T2), and `GET /healthz`. Every value crossing this boundary is
//! validated — identifiers against `^[a-zA-Z0-9_-]{1,64}$`, and every external
//! string capped in bytes (AGENTS.md § Input Validation) — and query results
//! carry a graded `freshness` label (decision D5).

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
use consumer_engine_execution::{QueryResult, Reader};
use consumer_engine_ingestion::IngestionHandle;
use serde::{Deserialize, Serialize};

/// Maximum rows accepted in a single onboard request (defense against memory
/// exhaustion — a full body-size limit also applies via the router layer).
const MAX_ONBOARD_ROWS: usize = 200_000;

/// Maximum number of columns in an onboard request (DoS bound — a huge column
/// count builds a huge `CREATE TABLE`).
const MAX_COLUMNS: usize = 1024;

/// Maximum size of a `POST /query` SQL string, in bytes. DoS bound on the T1
/// stand-in query surface (the DSL in T2 replaces free SQL).
const MAX_SQL_BYTES: usize = 8_192;

/// Maximum size of a single onboarded cell value, in bytes. Per AGENTS.md § Input
/// Validation, every external string needs an explicit byte cap.
const MAX_CELL_BYTES: usize = 4_096;

/// Shared state injected into handlers.
#[derive(Clone, Debug)]
pub struct AppState {
    /// The single ingestion writer handle.
    pub ingestion: IngestionHandle,
    /// The read-only reader handle.
    pub reader: Reader,
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

/// `POST /query` request body.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct QueryRequest {
    /// Read-only SQL referencing the `dro` catalog alias.
    sql: String,
}

/// `POST /query` response body.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryResponse {
    columns: Vec<String>,
    rows: Vec<consumer_engine_execution::RowCells>,
    freshness: Freshness,
}

/// Build the T1 router with `state` and a 10 MB request-body limit.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/sources/onboard", post(onboard))
        .route("/query", post(query))
        .layer(axum::extract::DefaultBodyLimit::max(10 * 1024 * 1024))
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
    if req.sql.len() > MAX_SQL_BYTES {
        return Err(Error::InvalidInput(format!("sql exceeds {MAX_SQL_BYTES} bytes")).into());
    }
    let QueryResult { columns, rows, .. } = state.reader.query(&req.sql).await?;
    let lag = now_epoch() - state.last_ingest_epoch.load(Ordering::Relaxed);
    Ok(Json(QueryResponse {
        columns,
        rows,
        freshness: Freshness::batch(lag),
    }))
}

/// Current epoch seconds. Falls back to 0 if the clock is before the epoch
/// (cannot happen in practice).
fn now_epoch() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// HTTP error wrapper mapping [`Error`] to status codes.
#[derive(Debug)]
pub struct ApiError(pub Error);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (code, msg) = match &self.0 {
            Error::InvalidInput(_) => (StatusCode::BAD_REQUEST, self.0.to_string()),
            Error::WriterAlreadyHeld => (StatusCode::CONFLICT, self.0.to_string()),
            _ => (StatusCode::INTERNAL_SERVER_ERROR, self.0.to_string()),
        };
        (code, msg).into_response()
    }
}

impl From<Error> for ApiError {
    fn from(e: Error) -> Self {
        Self(e)
    }
}
