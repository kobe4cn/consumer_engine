//! Feature Store DTOs shared across the producer/ingestion/write path.
//!
//! Per decision D9, the Feature Store is the ML-ready seam: producers emit
//! scalar features in entity-attribute-value (EAV) form; pivot views expose the
//! wide form for cheap scans. This module holds the plain row DTO that travels
//! `producer → ingestion → storage`. See `specs/10-data-model.md §2`.

use serde::{Deserialize, Serialize};

use crate::{Error, validate_ident};

/// Split a namespaced feature name `"family.short"` into validated parts.
///
/// Both parts must be valid identifiers (so exactly one `.`, with no nested
/// dots) — this is the invariant that lets a feature name map to a wide view
/// `feature_wide_{family}` whose column `{short}` is a sound SQL identifier.
///
/// # Errors
/// [`Error::InvalidInput`] if `name` has no `.`, or either part fails
/// [`crate::validate_ident`].
pub fn split_feature_name(name: &str) -> crate::Result<(String, String)> {
    let Some((family, short)) = name.split_once('.') else {
        return Err(Error::InvalidInput(format!(
            "feature name {name:?} must be namespaced 'family.short'"
        )));
    };
    validate_ident(family).map_err(|e| Error::InvalidInput(format!("feature family: {e}")))?;
    validate_ident(short).map_err(|e| Error::InvalidInput(format!("feature short name: {e}")))?;
    Ok((family.to_string(), short.to_string()))
}

/// One scalar feature value for one user, at a point in time (EAV form).
///
/// A newer `as_of_ts` supersedes an older one; the store is append-only (I4).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FeatureRow {
    /// Pseudonymous subject id (D12).
    pub user_id: String,
    /// Namespaced feature name `"{family}.{short}"` (e.g. `"cadence.regularity"`).
    pub feature_name: String,
    /// The scalar feature value.
    pub num_value: f64,
    /// Point-in-time the value is correct-for (ISO-8601 UTC string); anti-leakage.
    pub as_of_ts: String,
    /// Which producer wrote it (lineage; validated as a feature name).
    pub producer_id: String,
}
