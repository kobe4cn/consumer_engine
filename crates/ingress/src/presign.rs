//! Short-lived, snapshot-bound, constant-time-verified export tokens.
//!
//! Per AGENTS.md § Crypto, the audience-export URL is signed with HMAC-SHA256
//! over `"{snapshot_id}.{expiry}"` and verified in constant time via
//! [`subtle::ConstantTimeEq`]. The signing key is 32 bytes of OS randomness
//! (minted by the server via `getrandom`; see `apps/server`), and the token is
//! `"{expiry}.{hex_tag}"`. [`EXPORT_TTL_SECS`] (15 min) bounds the window
//! (`specs/21` I4 — presigned URLs expire).
//!
//! No `rand`/`hex` crate is pulled in: signing randomness lives in the server,
//! and hex (de)serialisation is inline and length-checked.

use std::time::{SystemTime, UNIX_EPOCH};

use hmac::{Hmac, Mac};
use sha2::Sha256;
use subtle::ConstantTimeEq;

/// HMAC-SHA256 type alias.
type HmacSha256 = Hmac<Sha256>;

/// Presigned-export token lifetime (15 minutes; `specs/21` I4).
pub const EXPORT_TTL_SECS: u64 = 15 * 60;

/// HMAC-SHA256 tag length, in bytes.
const TAG_LEN: usize = 32;

/// A signing failure. Opaque (private inner field): callers cannot construct
/// it, only receive it from [`sign`]. Mapped by the audience handler to a 500.
#[derive(Debug)]
pub struct PresignError(hmac::digest::InvalidLength);

impl std::fmt::Display for PresignError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "presign hmac key failure: {}", self.0)
    }
}

impl std::error::Error for PresignError {}

impl From<hmac::digest::InvalidLength> for PresignError {
    fn from(e: hmac::digest::InvalidLength) -> Self {
        Self(e)
    }
}

/// Sign `snapshot_id` for `ttl_secs`, returning `"{expiry}.{hex_tag}"`.
///
/// The key is fixed at 32 bytes, so [`Mac::new_from_slice`] cannot fail in
/// practice; the [`Result`] exists for soundness (it surfaces the
/// `InvalidLength` case rather than silently dropping it).
///
/// # Errors
/// [`PresignError`] only if the 32-byte key is somehow rejected by the MAC
/// initialiser.
pub fn sign(key: &[u8; 32], snapshot_id: &str, ttl_secs: u64) -> Result<String, PresignError> {
    let mut mac = <HmacSha256 as Mac>::new_from_slice(key)?;
    // TTL addition overflow is impossible for a wall-clock epoch + 15 minutes,
    // but use checked arithmetic rather than panicking on hostile clocks.
    let expiry = now_secs().saturating_add(ttl_secs);
    mac.update(format!("{snapshot_id}.{expiry}").as_bytes());
    Ok(format!(
        "{expiry}.{}",
        hex_encode(mac.finalize().into_bytes().as_slice())
    ))
}

/// Verify `token` against `snapshot_id` under `key`. Returns `false` for any
/// malformed, expired, tampered, or mismatched token; `true` only on an exact,
/// constant-time match.
#[must_use]
pub fn verify(key: &[u8; 32], snapshot_id: &str, token: &str) -> bool {
    let Some((expiry_str, tag_hex)) = token.split_once('.') else {
        return false;
    };
    let Ok(expiry) = expiry_str.parse::<u64>() else {
        return false;
    };
    // Expired (or exactly at expiry) → reject.
    if now_secs() >= expiry {
        return false;
    }
    let Some(tag) = hex_decode(tag_hex) else {
        return false;
    };
    if tag.len() != TAG_LEN {
        return false;
    }
    let mut mac = match <HmacSha256 as Mac>::new_from_slice(key) {
        Ok(m) => m,
        Err(_) => return false,
    };
    mac.update(format!("{snapshot_id}.{expiry}").as_bytes());
    let expected = mac.finalize().into_bytes();
    // Constant-time comparison (AGENTS.md § Crypto: token/MAC checks must not
    // leak via timing). Lengths are already equal (both TAG_LEN).
    bool::from(tag.as_slice().ct_eq(expected.as_slice()))
}

/// Encode bytes as lowercase hex (no `hex` crate dep).
fn hex_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

/// Decode a hex string, rejecting odd-length or non-hex input.
fn hex_decode(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    let mut out = Vec::with_capacity(s.len() / 2);
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        // The even-length check guarantees `i + 1` is in bounds; get() keeps
        // the no-indexing lint set clean (defensive None otherwise).
        let (Some(&hi_b), Some(&lo_b)) = (bytes.get(i), bytes.get(i + 1)) else {
            return None;
        };
        let hi = hex_nibble(hi_b)?;
        let lo = hex_nibble(lo_b)?;
        out.push(hi << 4 | lo);
        i += 2;
    }
    Some(out)
}

/// Map one hex character to its nibble value, or `None` if not hex.
fn hex_nibble(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

/// Current Unix epoch seconds (0 if the clock is before the epoch).
fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: [u8; 32] = [0xAB; 32];

    #[test]
    fn test_should_sign_then_verify_roundtrip() {
        let token = sign(&KEY, "snap-abc", EXPORT_TTL_SECS).expect("sign");
        assert!(verify(&KEY, "snap-abc", &token), "valid token must verify");
    }

    #[test]
    fn test_should_reject_tampered_token() {
        let token = sign(&KEY, "snap-abc", EXPORT_TTL_SECS).expect("sign");
        // Flip the last hex character of the tag.
        let mut tampered = token.clone();
        let last = tampered.len() - 1;
        let flipped = if tampered.as_bytes()[last] == b'0' {
            '1'
        } else {
            '0'
        };
        tampered.replace_range(last..=last, &flipped.to_string());
        assert!(
            !verify(&KEY, "snap-abc", &tampered),
            "tampered token must fail"
        );
    }

    #[test]
    fn test_should_reject_expired_token() {
        // ttl=0 ⇒ expiry == now ⇒ verify (now >= expiry) returns false.
        let token = sign(&KEY, "snap-abc", 0).expect("sign");
        // Wait past the (zero) TTL so now > expiry.
        std::thread::sleep(std::time::Duration::from_secs(1));
        assert!(!verify(&KEY, "snap-abc", &token), "expired token must fail");
    }

    #[test]
    fn test_should_reject_token_for_other_snapshot() {
        let token = sign(&KEY, "snap-A", EXPORT_TTL_SECS).expect("sign");
        assert!(
            !verify(&KEY, "snap-B", &token),
            "token bound to A must not verify for B"
        );
    }

    #[test]
    fn test_should_reject_malformed_token() {
        // No dot.
        assert!(!verify(&KEY, "snap-abc", "noseparatorhere"));
        // Non-numeric expiry.
        assert!(!verify(&KEY, "snap-abc", "notanumber.deadbeef"));
        // Odd-length hex tag.
        assert!(!verify(&KEY, "snap-abc", "9999999999.abc"));
    }
}
