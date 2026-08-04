//! `POST /producers/run` — run a registered Feature Store producer (D9).
//!
//! Engineer-facing (elevated; auth is stubbed in M3): triggers a producer's
//! `run(as_of)` and persists its feature rows + wide views via the single
//! writer. The producer id and `asOf` are validated at the boundary.

use axum::{
    Json,
    extract::{Extension, State},
    http::StatusCode,
};
use consumer_engine_core::validate_feature_name;
use serde::{Deserialize, Serialize};

use crate::{ApiError, AppState, Tenant};

/// Maximum bytes accepted for the `asOf` cut-off string (§ Input Validation:
/// length limits on every external string, enforced in bytes).
const MAX_AS_OF_BYTES: usize = 64;

/// `POST /producers/run` request body.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RunProducerRequest {
    /// The registered producer id (validated as a feature name).
    producer_id: String,
    /// Point-in-time cut-off (`ISO-8601` UTC); defaults to now().
    #[serde(default)]
    as_of: Option<String>,
}

/// `POST /producers/run` response body.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RunProducerResponse {
    rows_written: usize,
}

/// `POST /producers/run`: run `producerId` at `asOf` (default now) and persist.
///
/// # Errors
/// - [`ApiError::Core`] on a bad producer id or an unknown producer.
pub async fn run_producer(
    State(st): State<AppState>,
    Extension(Tenant(tenant)): Extension<Tenant>,
    Json(req): Json<RunProducerRequest>,
) -> Result<(StatusCode, Json<RunProducerResponse>), ApiError> {
    validate_feature_name(&req.producer_id)?;
    if let Some(a) = &req.as_of
        && a.len() > MAX_AS_OF_BYTES
    {
        return Err(ApiError::Core(consumer_engine_core::Error::InvalidInput(
            format!("asOf exceeds {MAX_AS_OF_BYTES} bytes"),
        )));
    }
    let as_of = req.as_of.unwrap_or_else(|| chrono::Utc::now().to_rfc3339());
    let n = st
        .ingestion
        .run_producer(&req.producer_id, &as_of, &tenant)
        .await?;
    Ok((
        StatusCode::OK,
        Json(RunProducerResponse { rows_written: n }),
    ))
}
