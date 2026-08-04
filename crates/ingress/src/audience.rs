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
    extract::{Extension, Path, Query, State},
    response::{IntoResponse, Response},
};
use consumer_engine_core::{BoxError, Error};
use serde::{Deserialize, Serialize};

use crate::{ApiError, AppState, Tenant, presign};

/// `GET /audience/:id` response body.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AudienceResponse {
    snapshot_id: String,
    campaign_id: String,
    as_of_ts: String,
    row_count: u64,
    download_url: String,
}

/// Redacting `Debug` (specs/70 I5): `download_url` embeds the presigned export
/// token, so it is never printed — only a `[REDACTED]` marker.
impl std::fmt::Debug for AudienceResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AudienceResponse")
            .field("snapshot_id", &self.snapshot_id)
            .field("campaign_id", &self.campaign_id)
            .field("as_of_ts", &self.as_of_ts)
            .field("row_count", &self.row_count)
            .field("download_url", &"[REDACTED]")
            .finish()
    }
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
    Extension(Tenant(tenant)): Extension<Tenant>,
    Path(id): Path<String>,
) -> Result<Json<AudienceResponse>, ApiError> {
    let bare = parse_snap_id(&id)?;
    let meta = st
        .query_engine
        .snapshot_meta(&bare, &tenant)
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
    Extension(Tenant(tenant)): Extension<Tenant>,
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
    // IDOR closure (issue #22): even with a valid presigned token, the
    // snapshot must belong to the caller's tenant.
    if st
        .query_engine
        .snapshot_meta(&bare, &tenant)
        .await?
        .is_none()
    {
        return Err(ApiError::NotFound);
    }
    // Access logging (specs/21 §4: presigned access is logged). No token, no
    // query string — only the snapshot identity.
    tracing::info!(
        snapshot_id = %bare,
        format = "parquet",
        "presigned audience export accessed"
    );

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_should_redact_download_url_in_debug() {
        // The presigned download URL carries the export token; Debug must never
        // print it (specs/70 I5: no secrets in logs).
        let resp = AudienceResponse {
            snapshot_id: "snap_abc".into(),
            campaign_id: "c1".into(),
            as_of_ts: "2025-01-01T00:00:00Z".into(),
            row_count: 3,
            download_url: "/audience/snap_abc/export?format=parquet&token=SECRET_TOKEN".into(),
        };
        let debug = format!("{resp:?}");
        assert!(
            !debug.contains("SECRET_TOKEN"),
            "presigned token must not appear in Debug: {debug}"
        );
        assert!(
            debug.contains("[REDACTED]"),
            "marker must be present: {debug}"
        );
    }
}
