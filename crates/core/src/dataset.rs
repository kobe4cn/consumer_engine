//! Shared source-table reference type.
//!
//! [`Dataset`] names a raw table as `raw_{system}_{entity}`. It is the unit a
//! segment query reads from and the unit freshness is graded over (D5). Living
//! in the dependency root (`core`) lets both the query compiler and the
//! freshness registry share one type without `core → query` back-edge (DRY).

use serde::{Deserialize, Serialize};

/// A source table: compiles to `raw_{system}_{entity}`.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Dataset {
    /// Source system identifier (validated).
    pub system: String,
    /// Source entity (table) identifier (validated).
    pub entity: String,
}
