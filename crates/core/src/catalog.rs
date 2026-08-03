//! Catalog-attach aliases — the single source of truth so the read path, the
//! write path, and the reader's per-query `DETACH`/re-attach all agree.

/// Alias under which the **read-only** DuckLake catalog is attached
/// (reader thread, compiler read path, EXPLAIN).
pub const READ_ONLY_CATALOG_ALIAS: &str = "dro";

/// Alias under which the **writable** DuckLake catalog is attached (writer
/// actor; materialise `INSERT … SELECT` runs here).
pub const WRITE_CATALOG_ALIAS: &str = "dl";
