//! `GET /catalog` — L1 Intent RAG retrieval (`specs/13` §2).
//!
//! Embeds the operator utterance and returns a bounded candidate set of
//! tables/columns (`k` capped; spec 13 §3 I3).

use axum::{
    Json,
    extract::{Extension, Query, State},
    http::StatusCode,
};
use consumer_engine_core::{CatalogHit, Error, now_epoch, validate_ident};
use serde::Deserialize;

use crate::{ApiError, AppState, Tenant};

/// Maximum bytes accepted for the `q` query string (bounded boundary input).
const MAX_Q_BYTES: usize = 1024;
/// Default retrieval cap when `k` is absent (spec 13 §3 I3).
const DEFAULT_K: usize = 20;
/// Hard cap on `k` (defence-in-depth on top of the IntentRag's own bound).
const MAX_K: usize = 50;

/// `GET /catalog` query string.
#[derive(Debug, Default, Deserialize)]
pub struct CatalogQuery {
    /// The operator utterance to retrieve candidates for.
    #[serde(default)]
    q: Option<String>,
    /// Maximum number of candidates to return (1..=50; default 20).
    #[serde(default)]
    k: Option<usize>,
}

/// `GET /catalog`: retrieve a bounded candidate set for `q`.
///
/// # Errors
/// [`ApiError::Core`] if `q` exceeds the byte cap or retrieval fails.
pub async fn get_catalog(
    State(st): State<AppState>,
    Extension(Tenant(tenant)): Extension<Tenant>,
    Query(q): Query<CatalogQuery>,
) -> Result<Json<Vec<CatalogHit>>, ApiError> {
    let utterance = q.q.unwrap_or_default();
    if utterance.len() > MAX_Q_BYTES {
        return Err(ApiError::Core(Error::InvalidInput(format!(
            "q exceeds {MAX_Q_BYTES} bytes"
        ))));
    }
    // Reject, don't sanitise (AGENTS.md § Input Validation): an out-of-range
    // `k` is a caller error, not something to silently coerce.
    let k = match q.k {
        Some(k) if (1..=MAX_K).contains(&k) => k,
        Some(k) => {
            return Err(ApiError::Core(Error::InvalidInput(format!(
                "k must be 1..={MAX_K}, got {k}"
            ))));
        }
        None => DEFAULT_K,
    };
    let hits = st.intent_rag.retrieve(&utterance, k, &tenant).await?;
    Ok(Json(hits))
}

/// `PUT /catalog` request body — edit a column's description (spec 13 §4
/// editability, issue #23). Write-protected: only this endpoint (or
/// re-onboarding) can change a description; the agent's DSL path cannot.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EditCatalogRequest {
    /// Source system identifier.
    system: String,
    /// Source table (entity) identifier.
    table_name: String,
    /// The column whose description is edited.
    column_name: String,
    /// The new human-editable description (byte-capped at the boundary).
    description: String,
}

/// Maximum description bytes (AGENTS.md § Input Validation).
const MAX_DESCRIPTION_BYTES: usize = 2048;

/// `PUT /catalog` — replace a column's description, re-embed it, and append a
/// VERSIONED row (newer `source_epoch`) so retrieval picks the edit (the
/// original row is preserved — append-only versioning, spec 13 §4). The row's
/// semantics (type, PII flag, samples) are carried over untouched.
pub async fn put_catalog(
    State(st): State<AppState>,
    Extension(Tenant(tenant)): Extension<Tenant>,
    Json(req): Json<EditCatalogRequest>,
) -> Result<StatusCode, ApiError> {
    validate_ident(&req.system)?;
    validate_ident(&req.table_name)?;
    validate_ident(&req.column_name)?;
    if req.description.is_empty() || req.description.len() > MAX_DESCRIPTION_BYTES {
        return Err(Error::InvalidInput(format!(
            "description must be 1..={MAX_DESCRIPTION_BYTES} bytes"
        ))
        .into());
    }
    let mut row = st
        .intent_rag
        .catalogue_entry(&req.system, &req.table_name, &req.column_name, &tenant)
        .await?
        .ok_or(ApiError::NotFound)?;
    // Re-embed the description (I4: descriptions only — never PII values).
    let embedding = st
        .embed
        .embed(&req.description)
        .await
        .map_err(|e| ApiError::Core(Error::Execution(Box::from(format!("embed: {e}")))))?;
    row.description = req.description;
    row.embedding = embedding;
    // Versioned delta: STRICTLY newer than the original row's stamp, so
    // retrieval's QUALIFY (ORDER BY source_epoch DESC) deterministically picks
    // the edit even when the onboard and the edit land in the same wall-clock
    // second.
    row.source_epoch = now_epoch().max(row.source_epoch + 1);
    st.ingestion.write_catalog(vec![row], &tenant).await?;
    Ok(StatusCode::OK)
}
