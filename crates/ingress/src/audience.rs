//! Audience snapshot metadata + presigned Parquet export.
//!
//! `GET /audience/:snapshot_id` returns snapshot metadata plus a short-lived,
//! presigned relative `downloadUrl`. `GET /audience/:snapshot_id/export`
//! verifies the presigned token (constant time; 401 on any mismatch), exports
//! the snapshot to a server-controlled temp Parquet via the single writer, and
//! streams the bytes back. Per `specs/21` I3/I4: bodies are bounded and export
//! URLs expire.

use std::path::PathBuf;

use axum::{
    Json,
    extract::{Path, Query, State},
    response::{IntoResponse, Response},
};
use consumer_engine_core::{BoxError, Error};
use serde::{Deserialize, Serialize};

use crate::{ApiError, AppState, presign};

/// `GET /audience/:id` response body.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AudienceResponse {
    snapshot_id: String,
    campaign_id: String,
    as_of_ts: String,
    row_count: u64,
    download_url: String,
}

/// `GET /audience/:id/export` query string.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExportQuery {
    format: Option<String>,
    token: Option<String>,
}

/// Strip the `snap_` prefix and validate the remainder is a UUID. Returns the
/// bare uuid string. `snap_` prefix is the opaque id minted by
/// [`QueryEngine::materialize`](consumer_engine_query::QueryEngine::materialize);
/// the bare uuid is what `audience_snapshot.snapshot_id` stores.
fn parse_snap_id(snap: &str) -> Result<String, ApiError> {
    let rest = snap
        .strip_prefix("snap_")
        .ok_or_else(|| ApiError::Core(Error::InvalidInput("invalid snapshot id".into())))?;
    if uuid::Uuid::parse_str(rest).is_err() {
        return Err(ApiError::Core(Error::InvalidInput(
            "invalid snapshot id".into(),
        )));
    }
    Ok(rest.to_string())
}

/// `GET /audience/:snapshot_id`: snapshot metadata + a relative presigned
/// `downloadUrl` (15-min TTL). The URL is relative because ingress does not know
/// its external host; the caller resolves it.
pub async fn get_audience(
    State(st): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<AudienceResponse>, ApiError> {
    let bare = parse_snap_id(&id)?;
    let meta = st
        .query_engine
        .snapshot_meta(&bare)
        .await?
        .ok_or(ApiError::NotFound)?;
    let token = presign::sign(st.signing_key.as_ref(), &bare, presign::EXPORT_TTL_SECS)
        .map_err(|_| ApiError::Core(Error::InvalidInput("signing failure".into())))?;
    let download_url = format!("/audience/{id}/export?format=parquet&token={token}");
    Ok(Json(AudienceResponse {
        snapshot_id: id,
        campaign_id: meta.campaign_id,
        as_of_ts: meta.as_of_ts,
        row_count: meta.row_count,
        download_url,
    }))
}

/// `GET /audience/:snapshot_id/export`: verify the presigned token, export the
/// snapshot to Parquet via the single writer, and stream the bytes.
pub async fn get_export(
    State(st): State<AppState>,
    Path(id): Path<String>,
    Query(q): Query<ExportQuery>,
) -> Result<Response, ApiError> {
    if q.format.as_deref() != Some("parquet") {
        return Err(ApiError::UnsupportedFormat);
    }
    let bare = parse_snap_id(&id)?;
    let token = q.token.as_deref().unwrap_or("");
    if !presign::verify(st.signing_key.as_ref(), &bare, token) {
        return Err(ApiError::Unauthorized);
    }

    // Server-controlled temp path (caller never chooses it): snapshot uuid +
    // fresh uuidv7 suffix avoids collisions between concurrent exports.
    let dest: PathBuf =
        std::env::temp_dir().join(format!("ce-export-{bare}-{}", uuid::Uuid::now_v7()));
    st.ingestion.export_parquet(&bare, dest.clone()).await?;
    let bytes = tokio::fs::read(&dest)
        .await
        .map_err(|e| ApiError::Core(Error::Execution(BoxError::from(e))))?;
    // Best-effort cleanup of the temp file; a failure here must not fail the
    // response (the bytes are already in hand).
    if let Err(e) = tokio::fs::remove_file(&dest).await {
        tracing::warn!(error = %e, "failed to remove temp export parquet");
    }

    // `bare` is a validated UUID string ⇒ always valid ASCII, so HeaderValue
    // construction cannot fail in practice; surface the (unreachable) error
    // rather than panic.
    let content_disposition =
        axum::http::HeaderValue::try_from(format!("attachment; filename=\"{bare}.parquet\""))
            .map_err(|e| ApiError::Core(Error::InvalidInput(format!("header build: {e}"))))?;

    Ok((
        [
            (
                axum::http::header::CONTENT_TYPE,
                axum::http::HeaderValue::from_static("application/vnd.apache.parquet"),
            ),
            (axum::http::header::CONTENT_DISPOSITION, content_disposition),
        ],
        axum::body::Body::from(bytes),
    )
        .into_response())
}
