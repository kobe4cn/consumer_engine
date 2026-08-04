//! Boundary identifier validation, shared across crates.
//!
//! Per `AGENTS.md` § Injection Prevention / § Input Validation, identifiers
//! (`system`, `entity`, column names) crossing the trust boundary are validated
//! against a strict allowlist before they ever reach SQL. Centralising this in
//! `core` keeps a single source of truth (DRY) for both `storage` and `ingress`.

use crate::{Error, Result};

/// Identifier allowlist: 1–64 chars of `[A-Za-z0-9_]`. Note: NO `-`/`.` —
/// identifiers are rendered **unquoted** into SQL (`dro.raw_{system}_{entity}`),
/// where `-` would break the token and `_` is a LIKE wildcard; the dot is
/// reserved for namespaced feature names (see [`FEATURE_NAME_RE`]).
// Hardcoded valid pattern; failure here is a programmer error, not external
// input, so `expect` is acceptable at this one static-init site.
#[allow(clippy::expect_used)]
static IDENT_RE: once_cell::sync::Lazy<regex::Regex> = once_cell::sync::Lazy::new(|| {
    regex::Regex::new(r"^[a-zA-Z0-9_]{1,64}$").expect("valid static regex")
});

/// Feature-name allowlist: 1–64 chars of `[A-Za-z0-9_.]` — note the `.` vs
/// [`IDENT_RE`], so namespaced `family.short` feature names and producer ids
/// are accepted. Still no `-` (feature names render into view/column names).
// Hardcoded valid pattern; see `IDENT_RE` justification.
#[allow(clippy::expect_used)]
static FEATURE_NAME_RE: once_cell::sync::Lazy<regex::Regex> = once_cell::sync::Lazy::new(|| {
    regex::Regex::new(r"^[a-zA-Z0-9_.]{1,64}$").expect("valid static regex")
});

/// Validate an identifier against the boundary allowlist `^[a-zA-Z0-9_]{1,64}$`.
///
/// # Errors
/// Returns [`Error::InvalidInput`] if `name` is empty, longer than 64 chars, or
/// contains a character outside the allowlist (including `-`, which would break
/// the unquoted SQL identifiers are interpolated into).
pub fn validate_ident(name: &str) -> Result<()> {
    if IDENT_RE.is_match(name) {
        Ok(())
    } else {
        Err(Error::InvalidInput(format!(
            "invalid identifier {name:?}: must match ^[a-zA-Z0-9_-]{{1,64}}$"
        )))
    }
}

/// Validate a feature name / producer id against the boundary allowlist
/// `^[a-zA-Z0-9_.-]{1,64}$` (note the `.` vs [`validate_ident`], so namespaced
/// `family.short` feature names are accepted).
///
/// # Errors
/// Returns [`Error::InvalidInput`] if `name` is empty, longer than 64 chars, or
/// contains a character outside the allowlist.
pub fn validate_feature_name(name: &str) -> Result<()> {
    if FEATURE_NAME_RE.is_match(name) {
        Ok(())
    } else {
        Err(Error::InvalidInput(format!(
            "invalid feature name {name:?}: must match ^[a-zA-Z0-9_.-]{{1,64}}$"
        )))
    }
}
