//! The materialise DTO shared across `query → ingestion → storage`.
//!
//! Lives in the dependency root (`core`) so the materialise path does not create
//! a `query → storage` edge or a backwards `storage → ingestion` edge. All fields
//! are owned `String`s (no new `core` dependency). See `specs/10-data-model.md`
//! for the `audience_snapshot` schema.

/// The scalar payload of one `audience_snapshot` row, carried to the writer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotSpec {
    /// Snapshot id (UUIDv7 string, e.g. `snap_<uuid>` — the prefix is added by
    /// the caller; this is the bare uuid string).
    pub snapshot_id: String,
    /// Caller-supplied campaign id.
    pub campaign_id: String,
    /// Data cut-off the snapshot reflects, ISO-8601 (UTC).
    pub as_of_ts: String,
    /// Frozen feature values at selection time (JSON text; non-null).
    pub features: String,
    /// Why each row was selected (JSON text; non-null).
    pub hit_reason: String,
}
