//! Shared wall-clock helpers (AGENTS.md § DRY — one definition, not one per
//! crate).

/// Current Unix epoch seconds (`0` if the clock is before the epoch).
#[must_use]
pub fn now_epoch() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}
