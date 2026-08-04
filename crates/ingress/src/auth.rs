//! Bearer-token authN middleware (specs/21 §3 I1: AuthN on every request).
//!
//! Every route except `/healthz` / `/readyz` requires `Authorization: Bearer
//! <token>`. The expected token is hashed (SHA-256) at startup; the incoming
//! token's hash is compared **in constant time** (AGENTS.md § Crypto) so a
//! timing side-channel cannot be used to probe it. A tokenless engine (no
//! `auth_token` configured) passes everything — a development convenience that
//! MUST NOT be used in production (an unauthenticated engine lets any caller
//! mint presigned exports — IDOR).

use axum::{
    extract::{Request, State},
    http::{StatusCode, header},
    middleware::Next,
    response::Response,
};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

use crate::AppState;

/// AuthN gate: run the request if the bearer token matches, else 401.
pub async fn require_auth(
    State(st): State<AppState>,
    req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    // Liveness/readiness probes never carry credentials.
    if matches!(req.uri().path(), "/healthz" | "/readyz") {
        return Ok(next.run(req).await);
    }
    // Tokenless engine (dev convenience, documented as unsafe in prod).
    let Some(expected) = &st.auth_token_hash else {
        return Ok(next.run(req).await);
    };
    let provided = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .ok_or(StatusCode::UNAUTHORIZED)?;
    let provided_hash = Sha256::digest(provided.as_bytes());
    if bool::from(expected.as_ref().ct_eq(provided_hash.as_slice())) {
        Ok(next.run(req).await)
    } else {
        Err(StatusCode::UNAUTHORIZED)
    }
}

/// SHA-256 hash of a bearer token, for constant-time comparison.
pub fn hash_token(token: &str) -> [u8; 32] {
    Sha256::digest(token.as_bytes()).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_should_hash_token_deterministically() {
        assert_eq!(hash_token("abc"), hash_token("abc"));
        assert_ne!(hash_token("abc"), hash_token("abd"));
    }
}
