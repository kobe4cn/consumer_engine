//! `POST /suppression` — the closed loop's writeback (E1).
//!
//! The external delivery system reports per-outcome outcomes (targeted /
//! delivered / converted / opted_out / bounced). The engine persists them via
//! the single writer (Q3) **idempotently**: the client supplies
//! `suppression_id`, and re-POSTing the same id writes nothing new
//! (specs/21 §4, specs/20 §5). This is the only external write path into the
//! engine.

use axum::{Json, extract::State, http::StatusCode};
use consumer_engine_core::{
    Error, SuppressionAction, SuppressionChannel, SuppressionRow, validate_ident,
};
use serde::{Deserialize, Serialize};

use crate::{ApiError, AppState};

/// Maximum bytes for a free-form string field (campaign/user ids, timestamps).
const MAX_FIELD_BYTES: usize = 256;

/// `POST /suppression` request body (specs/10 §4 wire shape).
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SuppressionRequest {
    /// Client-supplied dedupe key (UUID), REQUIRED — idempotent retry is only
    /// possible when the client controls the key (E1, specs/21 §4).
    suppression_id: String,
    /// The campaign the outcome belongs to.
    campaign_id: String,
    /// Pseudonymous subject id (D12).
    user_id: String,
    /// Delivery channel (`sms` / `email` / `push` / `ads`).
    channel: String,
    /// Outcome (`targeted` / `delivered` / `converted` / `opted_out` / `bounced`).
    action: String,
    /// When the outcome occurred, ISO-8601 UTC.
    occurred_ts: String,
}

/// `POST /suppression` response body.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SuppressionResponse {
    suppression_id: String,
}

/// `POST /suppression`: validate at the boundary, persist idempotently via Q3,
/// and return `201 { suppressionId }`.
///
/// # Errors
/// - [`ApiError::Core`] on invalid ids/channel/action/timestamp or write failure.
pub async fn post_suppression(
    State(st): State<AppState>,
    Json(req): Json<SuppressionRequest>,
) -> Result<(StatusCode, Json<SuppressionResponse>), ApiError> {
    validate_ident(&req.campaign_id)?;
    validate_ident(&req.user_id)?;
    let channel = SuppressionChannel::parse(&req.channel)?;
    let action = SuppressionAction::parse(&req.action)?;
    if chrono::DateTime::parse_from_rfc3339(&req.occurred_ts).is_err() {
        return Err(ApiError::Core(Error::InvalidInput(
            "occurredTs must be ISO-8601 UTC".into(),
        )));
    }
    for s in [&req.campaign_id, &req.user_id, &req.occurred_ts] {
        if s.len() > MAX_FIELD_BYTES {
            return Err(ApiError::Core(Error::InvalidInput(format!(
                "field exceeds {MAX_FIELD_BYTES} bytes"
            ))));
        }
    }

    // Client-supplied dedupe key is required (E1): a minted id would break
    // retry idempotency — a lost response + retry would write two rows.
    if uuid::Uuid::parse_str(&req.suppression_id).is_err() {
        return Err(ApiError::Core(Error::InvalidInput(
            "suppressionId must be a UUID (required for idempotent retry)".into(),
        )));
    }
    let suppression_id = req.suppression_id;

    let row = SuppressionRow {
        suppression_id: suppression_id.clone(),
        campaign_id: req.campaign_id,
        user_id: req.user_id,
        channel,
        action,
        occurred_ts: req.occurred_ts,
        received_ts: chrono::Utc::now().to_rfc3339(),
    };
    // Idempotent: a duplicate suppression_id inserts 0 rows but still returns
    // the (client-supplied) id — the client sees the same outcome either way.
    st.ingestion.write_suppression(vec![row]).await?;
    Ok((
        StatusCode::CREATED,
        Json(SuppressionResponse { suppression_id }),
    ))
}
