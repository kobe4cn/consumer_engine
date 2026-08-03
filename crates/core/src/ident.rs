//! Boundary identifier validation, shared across crates.
//!
//! Per `AGENTS.md` § Injection Prevention / § Input Validation, identifiers
//! (`system`, `entity`, column names) crossing the trust boundary are validated
//! against a strict allowlist before they ever reach SQL. Centralising this in
//! `core` keeps a single source of truth (DRY) for both `storage` and `ingress`.

use crate::{Error, Result};

/// Identifier allowlist: 1–64 chars of `[A-Za-z0-9_-]`.
// Hardcoded valid pattern; failure here is a programmer error, not external
// input, so `expect` is acceptable at this one static-init site.
#[allow(clippy::expect_used)]
static IDENT_RE: once_cell::sync::Lazy<regex::Regex> = once_cell::sync::Lazy::new(|| {
    regex::Regex::new(r"^[a-zA-Z0-9_-]{1,64}$").expect("valid static regex")
});

/// Validate an identifier against the boundary allowlist `^[a-zA-Z0-9_-]{1,64}$`.
///
/// # Errors
/// Returns [`Error::InvalidInput`] if `name` is empty, longer than 64 chars, or
/// contains a character outside the allowlist.
pub fn validate_ident(name: &str) -> Result<()> {
    if IDENT_RE.is_match(name) {
        Ok(())
    } else {
        Err(Error::InvalidInput(format!(
            "invalid identifier {name:?}: must match ^[a-zA-Z0-9_-]{{1,64}}$"
        )))
    }
}
