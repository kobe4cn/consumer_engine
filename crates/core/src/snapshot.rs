//! The materialise DTO shared across `query → ingestion → storage`.
//!
//! Lives in the dependency root (`core`) so the materialise path does not create
//! a `query → storage` edge or a backwards `storage → ingestion` edge. All fields
//! are owned `String`s (no new `core` dependency). See `specs/10-data-model.md`
//! for the `audience_snapshot` schema.

/// The scalar payload of one `audience_snapshot` row, carried to the writer.
///
/// `features` and `hit_reason` are deliberately **not** here — they are
/// per-row values the materialise subquery emits (frozen feature values at
/// selection time, and the predicate chain that selected each user, decision
/// D11 / issue #13), so the snapshot write stays a single atomic
/// `INSERT … SELECT`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotSpec {
    /// Snapshot id (UUIDv7 string, e.g. `snap_<uuid>` — the prefix is added by
    /// the caller; this is the bare uuid string).
    pub snapshot_id: String,
    /// Caller-supplied campaign id.
    pub campaign_id: String,
    /// Data cut-off the snapshot reflects, ISO-8601 (UTC).
    pub as_of_ts: String,
}
