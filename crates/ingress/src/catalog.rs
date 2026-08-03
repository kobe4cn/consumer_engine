//! `GET /catalog` — L1 Intent RAG retrieval (`specs/13` §2).
//!
//! Embeds the operator utterance and returns a bounded candidate set of
//! tables/columns (`k` capped; spec 13 §3 I3).

use axum::{
    Json,
    extract::{Query, State},
};
use consumer_engine_core::{CatalogHit, Error};
use serde::Deserialize;

use crate::{ApiError, AppState};

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
    Query(q): Query<CatalogQuery>,
) -> Result<Json<Vec<CatalogHit>>, ApiError> {
    let utterance = q.q.unwrap_or_default();
    if utterance.len() > MAX_Q_BYTES {
        return Err(ApiError::Core(Error::InvalidInput(format!(
            "q exceeds {MAX_Q_BYTES} bytes"
        ))));
    }
    let k = q.k.unwrap_or(DEFAULT_K).clamp(1, MAX_K);
    let hits = st.intent_rag.retrieve(&utterance, k).await?;
    Ok(Json(hits))
}
