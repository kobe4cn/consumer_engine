//! Bearer-token authN middleware (specs/21 §3 I1: AuthN on every request) that
//! also resolves the caller's **tenant** (issue #22 / specs/21 I2).
//!
//! Every route except `/healthz` / `/readyz` requires `Authorization: Bearer
//! <token>`. The expected tokens (the engine's own plus any configured
//! `tenants`) are hashed (SHA-256) at startup; incoming tokens are compared in
//! **constant time** (AGENTS.md § Crypto). On success the middleware inserts an
//! [`crate::Tenant`] extension that handlers use to scope reads/writes — the
//! tenant never comes from the caller's body, only from the verified token.
//! A tokenless engine (no `auth_token`, no `tenants`) passes everything as the
//! default tenant — a development convenience that MUST NOT be used in
//! production (an unauthenticated engine lets any caller mint presigned
//! exports — IDOR).

use axum::{
    extract::{Request, State},
    http::{StatusCode, header},
    middleware::Next,
    response::Response,
};
use sha2::{Digest, Sha256};

use crate::{AppState, Tenant};

/// AuthN + tenant-resolution gate: run the request as the resolved tenant, or
/// 401.
pub async fn require_auth(
    State(st): State<AppState>,
    mut req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    // Liveness/readiness probes never carry credentials.
    if matches!(req.uri().path(), "/healthz" | "/readyz") {
        return Ok(next.run(req).await);
    }
    // Tokenless engine (dev convenience, documented as unsafe in prod): every
    // caller is the default tenant.
    if st.auth_token_hash.is_none() && st.tenants.is_empty() {
        req.extensions_mut()
            .insert(Tenant(st.default_tenant.clone()));
        return Ok(next.run(req).await);
    }
    let provided = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .ok_or(StatusCode::UNAUTHORIZED)?;
    let tenant = st
        .resolve_tenant(Some(provided))
        .ok_or(StatusCode::UNAUTHORIZED)?;
    req.extensions_mut().insert(Tenant(tenant));
    Ok(next.run(req).await)
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
